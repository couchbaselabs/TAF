#!/usr/bin/env python3
"""
Watches the tail block of live couchstore vbucket files (MB-65486).

Runs ON the Couchbase Server node (copied over by the test), using only the
python3 stdlib so it works with the server's bundled interpreter.

couchstore marks a commit point by a header block: the byte at a 4K aligned
offset is the block type (0x00 Data / 0x01 Header), followed by
  4 bytes big-endian length (includes the 4 crc bytes, high bit clear),
  4 bytes big-endian crc32c (crc32 for disk version 11),
  the header payload (first byte is the disk version).
Readers find the latest header by scanning back from EOF at 4K boundaries.

Before MB-65486 the header was written with type 0x01 straight away, so a
reader racing with the write (or a crash mid-write) could find a 0x01 block
with an incomplete payload -> 'Open error:checksum fail'.
With the fix, the header is first written with type 0x00 and fsync'ed, and
only then the single type byte is flipped to 0x01 and fsync'ed again.

Per sample, the tail block of each watched file is classified as,
  header_ok      - 0x01 block whose crc matches
  pending_header - 0x00 block holding a complete header with a valid crc.
                   Only the fixed commit protocol leaves such a block behind
  flip_confirmed - a block seen as pending_header, later seen as a valid
                   0x01 header with the same bytes (the one byte flip)
  torn_header    - 0x01 block failing crc / length / short-read checks,
                   which reads back fine on retry (transient checksum fail)
  bad_header     - 0x01 block that still fails on retry
  data_tail      - any other 0x00 block

With --kill-pid, the given pid (memcached) is SIGKILL'ed as soon as a
pending header is seen, leaving that header orphaned at the file tail.

Prints a single 'RESULT:<json>' line at the end.
"""

import argparse
import glob
import json
import os
import re
import signal
import struct
import sys
import time
import zlib

BLOCK_SIZE = 4096
MAX_DB_HEADER_SIZE = 1024
BLOCK_TYPE_DATA = 0x00
BLOCK_TYPE_HEADER = 0x01
MIN_DISK_VERSION = 11
# Re-glob for a newer file revision (compaction) this often
REFRESH_INTERVAL = 0.5
MAX_EXAMPLES = 10


def _crc32c_table():
    table = []
    for index in range(256):
        crc = index
        for _ in range(8):
            crc = (crc >> 1) ^ 0x82F63B78 if crc & 1 else crc >> 1
        table.append(crc)
    return table


CRC32C_TABLE = _crc32c_table()


def crc32c(data):
    crc = 0xFFFFFFFF
    for byte in data:
        crc = CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def read_skipping_prefixes(buf, offset, length):
    """
    Mirrors couchstore's read_skipping_prefixes(), 'offset' is relative to
    the start of 'buf' which itself starts at a block boundary.
    Returns (bytes or None if 'buf' is too short, next offset)
    """
    if offset % BLOCK_SIZE == 0:
        offset += 1
    out = bytearray()
    while length > 0:
        read_size = min(BLOCK_SIZE - (offset % BLOCK_SIZE), length)
        chunk = buf[offset : offset + read_size]
        if len(chunk) != read_size:
            return None, offset
        out += chunk
        offset += read_size
        length -= read_size
        if offset % BLOCK_SIZE == 0:
            offset += 1
    return bytes(out), offset


def parse_header(buf):
    """
    Parses 'buf' (starting at a block boundary) as a header block,
    ignoring the block type byte.
    Returns (status, raw_bytes) with status one of
      ok / checksum / corrupt / short
    'raw_bytes' covers the full header block when status is 'ok'
    """
    info, offset = read_skipping_prefixes(buf, 1, 8)
    if info is None:
        return "short", None
    chunk_len, crc = struct.unpack(">II", info)
    # Header length includes the crc and a header always has a payload
    if chunk_len & 0x80000000 or chunk_len <= 4 or chunk_len > MAX_DB_HEADER_SIZE:
        return "corrupt", None
    payload, end = read_skipping_prefixes(buf, offset, chunk_len - 4)
    if payload is None:
        return "short", None
    if crc32c(payload) != crc and (zlib.crc32(payload) & 0xFFFFFFFF) != crc:
        return "checksum", None
    if payload[0] < MIN_DISK_VERSION:
        return "corrupt", None
    return "ok", bytes(buf[:end])


def latest_vb_file(vb_dir, vb_num):
    latest = None
    for path in glob.glob(os.path.join(vb_dir, f"{vb_num}.couch.*")):
        match = re.search(r"\.couch\.(\d+)$", path)
        if match and (latest is None or int(match.group(1)) > latest[0]):
            latest = (int(match.group(1)), path)
    return latest[1] if latest else None


class WatchedFile:
    def __init__(self, vb_dir, vb_num):
        self.vb_dir = vb_dir
        self.vb_num = vb_num
        self.path = None
        self.fd = None
        self.last_refresh = 0
        # Header bytes already validated, avoids re-computing crc
        self.last_ok_header = None
        # (pos, header bytes) of the last pending header seen
        self.pending = None

    def refresh(self, stats):
        now = time.time()
        if self.fd is not None and now - self.last_refresh < REFRESH_INTERVAL:
            return
        self.last_refresh = now
        path = latest_vb_file(self.vb_dir, self.vb_num)
        if path is None or path == self.path:
            return
        self.close()
        try:
            self.fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        self.path = path
        self.last_ok_header = None
        self.pending = None
        stats["files_opened"] += 1

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
        self.fd = None

    def read_tail(self):
        """Returns (block aligned pos, bytes) of the block holding EOF"""
        size = os.fstat(self.fd).st_size
        if size < 2:
            return None, b""
        # Same start point as couchstore's find_header(), EOF - 2
        pos = (size - 2) - ((size - 2) % BLOCK_SIZE)
        return pos, os.pread(self.fd, 2 * BLOCK_SIZE, pos)


def add_example(result, kind, watched, pos, detail):
    if len(result["examples"]) < MAX_EXAMPLES:
        result["examples"].append(
            {"kind": kind, "vb": watched.vb_num, "file": watched.path, "pos": pos, "detail": detail}
        )


def check_sample(watched, pos, buf, result, stats):
    """Classifies one tail sample. Returns True if a pending header is seen"""
    stats["samples"] += 1
    block_type = buf[0]
    if block_type == BLOCK_TYPE_HEADER:
        last_ok = watched.last_ok_header
        if last_ok is not None and last_ok[0] == pos and buf[: len(last_ok[1])] == last_ok[1]:
            stats["header_ok"] += 1
            return False
        status, raw = parse_header(buf)
        if status == "ok":
            stats["header_ok"] += 1
            watched.last_ok_header = (pos, raw)
            pending = watched.pending
            if pending is not None and pending[0] == pos and pending[1][1:] == raw[1:]:
                stats["flip_confirmed"] += 1
                watched.pending = None
            return False
        # Re-read to tell a transient (torn) read from real corruption
        time.sleep(0.001)
        retry_buf = os.pread(watched.fd, 2 * BLOCK_SIZE, pos)
        retry_status = "not_a_header"
        if retry_buf and retry_buf[0] == BLOCK_TYPE_HEADER:
            retry_status, _ = parse_header(retry_buf)
        if retry_status == "ok":
            stats["torn_header"] += 1
            stats["torn_" + status] += 1
            add_example(result, "torn_header", watched, pos, status)
        else:
            stats["bad_header"] += 1
            add_example(result, "bad_header", watched, pos, f"{status}, retry={retry_status}")
        return False
    if block_type == BLOCK_TYPE_DATA:
        status, raw = parse_header(buf)
        if status == "ok":
            stats["pending_header"] += 1
            if watched.pending is None or watched.pending[0] != pos:
                stats["pending_positions"] += 1
                if result["first_pending"] is None:
                    result["first_pending"] = {
                        "vb": watched.vb_num,
                        "file": watched.path,
                        "pos": pos,
                        "header_len": len(raw),
                    }
            watched.pending = (pos, raw)
            return True
        stats["data_tail"] += 1
        return False
    stats["unknown_block_type"] += 1
    add_example(result, "unknown_block_type", watched, pos, block_type)
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--vb-dir", required=True, help="Bucket's data dir holding <vb>.couch.<rev>")
    parser.add_argument("--vbuckets", required=True, help="Comma separated vbucket numbers to watch")
    parser.add_argument("--duration", type=float, default=60, help="Seconds to watch for")
    parser.add_argument("--kill-pid", type=int, default=0, help="SIGKILL this pid on the first pending header")
    args = parser.parse_args()

    stats = dict.fromkeys(
        [
            "samples",
            "header_ok",
            "pending_header",
            "pending_positions",
            "flip_confirmed",
            "torn_header",
            "torn_checksum",
            "torn_corrupt",
            "torn_short",
            "bad_header",
            "data_tail",
            "unknown_block_type",
            "files_opened",
            "read_errors",
        ],
        0,
    )
    result = {"stats": stats, "examples": [], "first_pending": None, "killed": None}
    watched_files = [WatchedFile(args.vb_dir, int(vb_num)) for vb_num in args.vbuckets.split(",")]

    start = time.time()
    deadline = start + args.duration
    while time.time() < deadline and result["killed"] is None:
        for watched in watched_files:
            watched.refresh(stats)
            if watched.fd is None:
                continue
            try:
                pos, buf = watched.read_tail()
            except OSError:
                stats["read_errors"] += 1
                watched.close()
                continue
            if not buf:
                continue
            if check_sample(watched, pos, buf, result, stats) and args.kill_pid:
                os.kill(args.kill_pid, signal.SIGKILL)
                result["killed"] = {
                    "pid": args.kill_pid,
                    "vb": watched.vb_num,
                    "file": watched.path,
                    "pos": pos,
                    "header_hex": watched.pending[1].hex(),
                }
                break

    result["elapsed"] = round(time.time() - start, 2)
    for watched in watched_files:
        watched.close()
    sys.stdout.write(f"RESULT:{json.dumps(result)}\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
