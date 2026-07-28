"""
Encrypted-file-format validator for continuous-backup EaR tests.

Parses the 80-byte header defined in EncryptedFileFormat.md and returns
per-file + aggregate encryption status. Used by the H-family tests and as
a post-condition helper in the B / D / E rows of ear-test-cases.md.

Header layout (from EncryptedFileFormat.md):

    | offset | length | description                    |
    +--------+--------+--------------------------------+
    | 0      | 21     | magic: \0Couchbase Encrypted\0 |
    | 21     | 1      | version                        |
    | 22     | 1      | compression                    |
    | 23     | 1      | key derivation                 |
    | 24     | 3      | unused (should be 0)           |
    | 27     | 1      | id len                         |
    | 28     | 36     | id bytes                       |
    | 64     | 16     | salt (uuid)                    |
    Total 80 bytes
"""
import base64
import uuid


MAGIC = b"\x00Couchbase Encrypted\x00"  # 21 bytes
HEADER_LEN = 80

VALID_VERSIONS = {0, 1}
VALID_COMPRESSION = {0, 1, 2, 3, 4, 5}
VALID_KDF_METHODS = {0, 1, 2}


class HeaderParseError(Exception):
    pass


class EncryptedFileHeader:
    def __init__(self, path, raw):
        if len(raw) < HEADER_LEN:
            raise HeaderParseError(
                "%s: file shorter than header (%d bytes)" % (path, len(raw)))
        if raw[:21] != MAGIC:
            raise HeaderParseError(
                "%s: missing Couchbase Encrypted magic" % path)

        self.path = path
        self.raw = bytes(raw[:HEADER_LEN])
        self.version = self.raw[21]
        self.compression = self.raw[22]
        self.key_derivation_byte = self.raw[23]
        self.id_len = self.raw[27]
        self.id_bytes = self.raw[28:28 + self.id_len]
        self.salt_bytes = self.raw[64:80]

    @property
    def kdf_method(self):
        return self.key_derivation_byte & 0x0F

    @property
    def kdf_iteration_exponent(self):
        return (self.key_derivation_byte >> 4) & 0x0F

    @property
    def pbkdf2_iterations(self):
        # PBKDF2 iteration count is only defined when kdf_method == 2.
        if self.kdf_method != 2:
            return None
        return 1024 * (2 ** self.kdf_iteration_exponent)

    @property
    def salt_uuid(self):
        return uuid.UUID(bytes=bytes(self.salt_bytes))

    def assert_valid(self):
        """
        Raise AssertionError if any header field violates the spec.
        Returns self on success so callers can chain.
        """
        if self.version not in VALID_VERSIONS:
            raise AssertionError(
                "%s: version %d not in %s"
                % (self.path, self.version, VALID_VERSIONS))
        if self.compression not in VALID_COMPRESSION:
            raise AssertionError(
                "%s: compression byte %d not in %s"
                % (self.path, self.compression, VALID_COMPRESSION))
        if self.kdf_method not in VALID_KDF_METHODS:
            raise AssertionError(
                "%s: KDF method %d not in %s"
                % (self.path, self.kdf_method, VALID_KDF_METHODS))
        if self.id_len > 36:
            raise AssertionError(
                "%s: id_len %d > 36" % (self.path, self.id_len))
        if len(self.id_bytes) != self.id_len:
            raise AssertionError(
                "%s: id_bytes length %d != id_len %d"
                % (self.path, len(self.id_bytes), self.id_len))
        try:
            _ = self.salt_uuid
        except (ValueError, TypeError) as e:
            raise AssertionError(
                "%s: salt does not parse as UUID: %s" % (self.path, e))
        return self


def parse_header_bytes(path, raw):
    """
    Try to parse `raw` (bytes) as an encrypted-file header for logging as
    `path`. Returns EncryptedFileHeader on success, None if the file simply
    isn't encrypted (missing magic). Raises HeaderParseError on genuinely
    malformed input (too short after the magic).
    """
    if len(raw) < 21 or raw[:21] != MAGIC:
        return None
    return EncryptedFileHeader(path, raw)


def parse_local_file(path):
    """Read the first 80 bytes of a local file and parse the header."""
    with open(path, "rb") as fh:
        raw = fh.read(HEADER_LEN)
    return parse_header_bytes(path, raw)


def scan_local_directory(root, filename_pattern=None):
    """
    Walk a local directory and parse the header of every file.

    :param root: filesystem path to walk.
    :param filename_pattern: if given, a callable(name) -> bool that gates
                             which files are inspected. Default: inspect all.
    :return: dict {file_path: EncryptedFileHeader or None}.
    """
    import os
    results = {}
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if filename_pattern is not None and not filename_pattern(name):
                continue
            fpath = os.path.join(dirpath, name)
            try:
                results[fpath] = parse_local_file(fpath)
            except HeaderParseError:
                results[fpath] = None
    return results


def scan_remote_directory(shell, root, filename_pattern=None):
    """
    Walk a directory on `shell`'s remote node and parse the header of every
    file. Fetches header bytes via `head -c 80 | base64` per file.

    :param shell: RemoteMachineShellConnection.
    :param root: absolute path on the remote node.
    :param filename_pattern: callable(name) -> bool gate. Default: inspect all.
    :return: dict {file_path: EncryptedFileHeader or None}.
    """
    listing, _ = shell.execute_command("find %s -type f" % root)
    results = {}
    for line in listing:
        fpath = line.strip()
        if not fpath:
            continue
        if filename_pattern is not None:
            import os
            if not filename_pattern(os.path.basename(fpath)):
                continue
        b64, _ = shell.execute_command(
            "head -c %d %s | base64 -w 0" % (HEADER_LEN, fpath))
        try:
            raw = base64.b64decode("".join(b64).strip())
            results[fpath] = parse_header_bytes(fpath, raw)
        except HeaderParseError:
            results[fpath] = None
    return results


def aggregate_status(scan_result):
    """
    Reduce a scan result to `contbk info`'s encryption_status vocabulary.

    :return: one of "unencrypted" | "partial" | "full".
             "unencrypted" — no file has the magic.
             "full"        — every file has the magic.
             "partial"     — some but not all files have the magic.
    """
    if not scan_result:
        return "unencrypted"
    encrypted = sum(1 for header in scan_result.values() if header is not None)
    if encrypted == 0:
        return "unencrypted"
    if encrypted == len(scan_result):
        return "full"
    return "partial"


def assert_all_headers_valid(scan_result):
    """
    Assert every parsed header in `scan_result` passes assert_valid().
    Files that weren't encrypted (value is None) are skipped.
    """
    for header in scan_result.values():
        if header is not None:
            header.assert_valid()


def assert_no_headers_present(scan_result):
    """
    Assert no file in `scan_result` starts with the Couchbase Encrypted magic.
    Used by H-5 to prove an `--ear=False` run produced a genuinely unencrypted
    archive.
    """
    encrypted = [path for path, header in scan_result.items()
                 if header is not None]
    if encrypted:
        raise AssertionError(
            "Expected no encrypted files, found %d: %s"
            % (len(encrypted), encrypted[:5]))
