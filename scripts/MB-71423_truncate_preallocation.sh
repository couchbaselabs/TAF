
#!/bin/bash
#
# This script can be used to reclaim wasted disk space due to the sstable preallocation bug MB-71453.
# It can be run while Couchbase server is up and running.
#
# Algorithm:
# 0) Issues a sync to make sure modified time of all files is up to date
# 1) Finds sstables in given data directory along with their logical/physical sizes (initial snapshot)
# 2) Finds set of files that have active writers using lsof (writer set)
# 3) Refreshes the initial snapshot by once again finding all sstables and their logical/physical sizes (refreshed snapshot)
# 4) Files meeting the following conditions are candidate for truncation:
#   - logical size < physical size
#   - should not be in writer set
#   - should have modified time newer than (now - MAX_AGE_SECS)
#     This is to guard against the rare case where if memcached restarts between steps 3 and 4
#     and Magma reuses a SSTableID post recovery because it wasn't checkpointed, it would truncate the new file
#     at the size of the old file. MAX_AGE_SECS must be at least 2mins to be safe from this.
#
# Bug only exists on 8.0.0 and 8.0.1.
set -u
SECONDS=0
VERBOSE=false
DRY_RUN=false
FORCE=false
MAX_AGE_SECS=600
TARGET_DIR="."
SCANNED=0
MISMATCHES=0
TRUNCATED=0
SKIPPED_WRITERS=0
SKIPPED_DELETED=0
SKIPPED_AFTER_REFRESH=0
ERRORS=0
RECLAIMED_BYTES=0
while getopts "vdft:" opt; do
  case $opt in
    v)
      VERBOSE=true
      ;;
    d)
      DRY_RUN=true
      ;;
    f)
      FORCE=true
      ;;
    t)
      MAX_AGE_SECS=$OPTARG
      ;;
    \?)
      echo "Usage: $0 [-v] [-d] [-f] [-t SECONDS] [directory]"
      echo "  -v          verbose"
      echo "  -d          dry-run"
      echo "  -f          force: skip writer-FD check (use when memcached is stopped)"
      echo "  -t SECONDS  only consider files with mtime older than SECONDS (default: 600)"
      exit 1
      ;;
  esac
done
if ! [[ "$MAX_AGE_SECS" =~ ^[0-9]+$ ]]; then
    echo "FAILED: -t requires a non-negative integer (got '$MAX_AGE_SECS')"
    exit 1
fi
CUTOFF_EPOCH=$(( $(date +%s) - MAX_AGE_SECS ))
CUTOFF_HUMAN=$(date -d "@$CUTOFF_EPOCH" '+%Y-%m-%d %H:%M:%S %Z')
FIND_AGE_ARGS=( ! -newermt "@$CUTOFF_EPOCH" )
shift $((OPTIND - 1))
if [ -n "${1:-}" ]; then
    TARGET_DIR="$1"
fi
if [ ! -d "$TARGET_DIR" ]; then
    echo "FAILED: Directory '$TARGET_DIR' does not exist"
    exit 1
fi
if [ "$FORCE" = false ] && ! command -v lsof >/dev/null 2>&1; then
    echo "FAILED: lsof not found. Required for writer-skip safety check."
    echo "If memcached is stopped, pass -f to skip the writer check."
    exit 1
fi
if [ "$FORCE" = false ]; then
    # lsof can only see FDs of processes owned by the invoking uid (or all,
    # if root), and does so silently — running as the wrong user produces
    # an empty snapshot with no error, which would let the script truncate
    # under a live writer.
    EFFECTIVE_USER=$(id -un)
    if [ "$EFFECTIVE_USER" != "root" ] && [ "$EFFECTIVE_USER" != "couchbase" ]; then
        echo "FAILED: must run as root or 'couchbase' (needs lsof visibility into memcached FDs)."
        exit 1
    fi
    MEMCACHED_PIDS=$(pgrep -d, memcached || true)
    if [ -z "$MEMCACHED_PIDS" ]; then
        echo "FAILED: memcached not running. Cannot verify writer FDs."
        echo "If this is intentional (offline data dir), pass -f to skip the writer check."
        exit 1
    fi
else
    MEMCACHED_PIDS=""
fi
log() {
    if [ "$VERBOSE" = true ]; then
        echo -e "$1"
    fi
}
echo "Scanning: $TARGET_DIR (Verbose: $VERBOSE, Dry-run: $DRY_RUN, Force: $FORCE, MaxAgeSecs: $MAX_AGE_SECS, Cutoff: $CUTOFF_HUMAN [epoch $CUTOFF_EPOCH])"
if [ "$FORCE" = false ]; then
    echo "Memcached PIDs: $MEMCACHED_PIDS"
else
    echo "WARNING: -f set; skipping writer-FD check. Truncating all mismatches."
fi
echo "---------------------------------------------------------------"
echo "Measuring disk usage (before)..."
DU_PHYSICAL_BEFORE=$(du -hs    "$TARGET_DIR" 2>/dev/null | awk '{print $1}')
DU_LOGICAL_BEFORE=$( du -hs --apparent-size "$TARGET_DIR" 2>/dev/null | awk '{print $1}')
echo "  physical: $DU_PHYSICAL_BEFORE"
echo "  logical:  $DU_LOGICAL_BEFORE"
echo "---------------------------------------------------------------"
WRITER_SET=$(mktemp)
INITIAL_LIST=$(mktemp)
REFRESHED_LIST=$(mktemp)
CANDIDATES=$(mktemp)
# Note: trap is installed later, after TRUNCATE_LIST is also allocated, so
# all tmpfiles are cleaned up together on exit.
# Ensure mtime of all files are up to date.
echo "Syncing filesystem containing $TARGET_DIR..."
if ! sync -f "$TARGET_DIR" 2>/dev/null; then
    echo "FAILED: 'sync -f' not supported on this system (coreutils too old)."
    exit 4
fi
echo "Enumerating sstables (initial snapshot)..."
# `find -printf` emits logical size (%s), 512-block count (%b), and path
# (%p) for each match, NUL-delimited.
find "$TARGET_DIR" -type f -name "sstable*" "${FIND_AGE_ARGS[@]}" -printf '%s\t%b\t%p\0' > "$INITIAL_LIST"
if [ "$FORCE" = true ]; then
    # Skip the lsof snapshot entirely; leave WRITER_SET empty so the
    # per-file check below never matches.
    : > "$WRITER_SET"
else
echo "Enumerating memcached writer FDs..."
if ! lsof -c memcached -F0an >"$WRITER_SET.raw" 2>/tmp/lsof_err.$$; then
    : # lsof returns non-zero on partial output too; we still inspect stderr
fi
# Strip residual benign mount-table-scan warnings (tracefs, overlay/nsfs
# from Docker, /sys/kernel pseudo-fs) plus the "Output information may be
# incomplete" trailer lsof appends whenever any such warning fired. Real
# permission/visibility errors against memcached FDs are not in this list
# and will still get through.
LSOF_ERR=$(grep -v -e 'tracefs' \
                  -e 'Output information may be incomplete' \
                  -e "can't stat() .* file system /sys/kernel" \
                  -e "can't stat() overlay file system" \
                  -e "can't stat() nsfs file system" \
                  /tmp/lsof_err.$$ || true)
if [ -n "$LSOF_ERR" ]; then
    echo "FAILED: lsof reported errors during snapshot:"
    echo "$LSOF_ERR"
    echo "Aborting. Re-run as root or as the user owning memcached."
    exit 3
fi
python3 - "$WRITER_SET.raw" "$WRITER_SET" <<'PY'
import sys
writer_set_raw, writer_set = sys.argv[1:3]
paths = set()
mode = b""
path = None
def finishRecord():
    if mode == b"w" or mode == b"u":
        if path is not None:
            paths.add(path)
with open(writer_set_raw, "rb") as src:
    for field in src.read().split(b"\0"):
        if not field:
            continue
        tag = field[:1]
        value = field[1:]
        if tag == b"f":
            finishRecord()
            mode = b""
            path = None
        elif tag == b"a":
            mode = value[:1]
        elif tag == b"n":
            path = value
finishRecord()
with open(writer_set, "w", encoding="utf-8", errors="surrogateescape") as dst:
    for path in sorted(paths):
        print(path.decode("utf-8", "surrogateescape"), file=dst)
PY
rm -f "$WRITER_SET.raw"
WRITER_COUNT=$(wc -l < "$WRITER_SET")
echo "Number of memcached writer-FDs: $WRITER_COUNT"
fi  # end of -f branch
# This is done again to make sure we catch the final size of the files that could've been written to between initial snapshot and lsof.
echo "Enumerating sstables (refresh snapshot)..."
find "$TARGET_DIR" -type f -name "sstable*" "${FIND_AGE_ARGS[@]}" -printf '%s\t%b\t%p\0' > "$REFRESHED_LIST"
# Intersect: keep entries whose path is in both lists. Records are
# NUL-delimited. Post-refresh %s/%b are what flow into
# CANDIDATES, so the per-file truncate loop sees the freshest view.
python3 - "$INITIAL_LIST" "$REFRESHED_LIST" "$CANDIDATES" <<'PY'
import sys
def get_path(record):
    fields = record.split(b"\t", 2)
    if len(fields) != 3:
        raise ValueError("malformed snapshot record")
    return fields[2]
initial_list, refreshed_list, candidates = sys.argv[1:4]
try:
    with open(initial_list, "rb") as f:
        initial_paths = {
                get_path(record)
                for record in f.read().split(b"\0")
                if record
        }
    with open(refreshed_list, "rb") as src, open(candidates, "wb") as dst:
        for record in src.read().split(b"\0"):
            if record and get_path(record) in initial_paths:
                dst.write(record + b"\0")
except ValueError as e:
    print(f"FAILED: {e}", file=sys.stderr)
    sys.exit(5)
PY
status=$?
if [ "$status" -ne 0 ]; then
    exit "$status"
fi
# Count files by counting NUL terminators in each list.
INITIAL_COUNT=$(tr -cd '\0' < "$INITIAL_LIST" | wc -c | tr -d ' ')
REFRESHED_COUNT=$(tr -cd '\0' < "$REFRESHED_LIST" | wc -c | tr -d ' ')
KEPT_COUNT=$(tr -cd '\0' < "$CANDIDATES" | wc -c | tr -d ' ')
# Files dropped by the intersect, reported from the post-sync view: any
# file present after sync that was not present in the initial snapshot.
SKIPPED_AFTER_REFRESH=$((REFRESHED_COUNT - KEPT_COUNT))
echo "  initial snapshot:   $INITIAL_COUNT files"
echo "  refresh snapshot: $REFRESHED_COUNT files"
echo "  in both:   $KEPT_COUNT files (dropped $SKIPPED_AFTER_REFRESH)"
echo "---------------------------------------------------------------"
# Preload the writer-FD snapshot into a bash associative array so the
# per-file check is O(1) hash lookup instead of an O(N) `grep -F` reread.
declare -A WRITERS
while IFS= read -r WPATH; do
    WRITERS["$WPATH"]=1
done < "$WRITER_SET"
# Collect files that need a truncate into a batch list. We don't call
# truncate(1) per file: each call forks a process and that fork dominates
# wall time when mismatches are numerous. Instead we accumulate
# `<size>\t<path>\0` records and feed them to a single python worker
# after the scan, which calls os.truncate() directly via the syscall.
TRUNCATE_LIST=$(mktemp)
trap 'rm -f "$WRITER_SET" "$WRITER_SET.raw" "$INITIAL_LIST" "$REFRESHED_LIST" "$CANDIDATES" "$TRUNCATE_LIST" /tmp/lsof_err.$$' EXIT
# Scan the candidate files.
while IFS=$'\t' read -r -d '' LOGICAL_SIZE BLOCKS FILE; do
    SCANNED=$((SCANNED + 1))
    PHYSICAL_SIZE=$((BLOCKS * 512))
    if [ "$PHYSICAL_SIZE" -le "$LOGICAL_SIZE" ]; then
        continue
    fi
    MISMATCHES=$((MISMATCHES + 1))
    DIFF=$((PHYSICAL_SIZE - LOGICAL_SIZE))
    log "File: $FILE"
    log "  Difference: +$DIFF bytes (Physical: $PHYSICAL_SIZE | Logical: $LOGICAL_SIZE)"
    if [ -n "${WRITERS[$FILE]:-}" ]; then
        SKIPPED_WRITERS=$((SKIPPED_WRITERS + 1))
        log "  [SKIP] Open writer detected (snapshot). Not truncating."
        log "---------------------------------------------------------------"
        continue
    fi
    if [ "$DRY_RUN" = true ]; then
        log "  [SKIP] Dry-run mode: No truncate issued."
        log "---------------------------------------------------------------"
        continue
    fi
    # Queue for batched truncate. Record DIFF too so the parent can sum
    # reclaimed bytes for the files that python successfully truncates.
    printf '%s\t%s\t%s\0' "$LOGICAL_SIZE" "$DIFF" "$FILE" >> "$TRUNCATE_LIST"
done < "$CANDIDATES"
# Apply phase. Single python process drains the batch list and calls
# os.truncate per record (one syscall, no fork).
if [ "$DRY_RUN" = false ] && [ -s "$TRUNCATE_LIST" ]; then
    while read -r CODE BYTES; do
        case "$CODE" in
            TRUNCATED) TRUNCATED=$((TRUNCATED + 1)); RECLAIMED_BYTES=$((RECLAIMED_BYTES + BYTES)) ;;
            DELETED)   SKIPPED_DELETED=$((SKIPPED_DELETED + 1)) ;;
            ERROR)     ERRORS=$((ERRORS + 1)) ;;
        esac
    done < <(python3 -u -c '
import os, sys
buf = sys.stdin.buffer.read()
for rec in buf.split(b"\x00"):
    if not rec:
        continue
    try:
        size_s, diff_s, path_b = rec.split(b"\t", 2)
        size = int(size_s)
        diff = int(diff_s)
        path = path_b.decode("utf-8", "surrogateescape")
    except Exception:
        print("ERROR 0", flush=True)
        continue
    try:
        os.truncate(path, size)
        print(f"TRUNCATED {diff}", flush=True)
    except FileNotFoundError:
        print("DELETED 0", flush=True)
    except Exception as e:
        print("ERROR 0", flush=True)
        print(f"  truncate failed for {path}: {e}", file=sys.stderr)
' < "$TRUNCATE_LIST")
fi
# Snapshot disk usage after the apply phase.
echo "Measuring disk usage (after)..."
DU_PHYSICAL_AFTER=$(du -hs    "$TARGET_DIR" 2>/dev/null | awk '{print $1}')
DU_LOGICAL_AFTER=$( du -hs --apparent-size "$TARGET_DIR" 2>/dev/null | awk '{print $1}')
STATUS="OK"
exit_code=0
if [ "$ERRORS" -gt 0 ]; then
    STATUS="SCRIPT_FAILED"
    exit_code=2
elif [ "$DRY_RUN" = true ] && [ "$MISMATCHES" -gt 0 ]; then
    STATUS="MISMATCHES_FOUND"
    exit_code=1
elif [ "$TRUNCATED" -gt 0 ]; then
    STATUS="TRUNCATED"
fi
RECLAIMED_MB=$((RECLAIMED_BYTES / 1024 / 1024))
echo "========================================"
echo "Status:          $STATUS"
echo "Files scanned:   $SCANNED"
echo "Mismatches:      $MISMATCHES"
echo "Truncated:       $TRUNCATED"
echo "Skipped writers: $SKIPPED_WRITERS"
echo "Skipped deleted: $SKIPPED_DELETED"
echo "Skipped after refresh: $SKIPPED_AFTER_REFRESH (not present in initial snapshot)"
echo "Errors:          $ERRORS"
echo "Reclaimed bytes: $RECLAIMED_BYTES (${RECLAIMED_MB} MiB)"
echo "Disk usage before:  physical=$DU_PHYSICAL_BEFORE  logical=$DU_LOGICAL_BEFORE"
echo "Disk usage after:   physical=$DU_PHYSICAL_AFTER  logical=$DU_LOGICAL_AFTER"
ELAPSED_H=$((SECONDS / 3600))
ELAPSED_M=$(((SECONDS % 3600) / 60))
ELAPSED_S=$((SECONDS % 60))
printf "Elapsed:         %02d:%02d:%02d (%ds)\n" "$ELAPSED_H" "$ELAPSED_M" "$ELAPSED_S" "$SECONDS"
echo "========================================"
exit $exit_code
