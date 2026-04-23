#define _GNU_SOURCE
#include <dlfcn.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/syscall.h>

/* ---------------- configuration ---------------- */

#define CFG_DIR        "/run/faultinject"
#define CFG_ENABLE      CFG_DIR "/enable"
#define CFG_DELAY_MS    CFG_DIR "/delay_ms"
#define CFG_FAIL_RATE   CFG_DIR "/fail_rate"
#define CFG_FAIL_ERRNO  CFG_DIR "/fail_errno"
#define CFG_TARGET      CFG_DIR "/target_path"

/* Per-syscall enable flags (1 = intercept, 0 = pass through). Default 1. */
#define CFG_SC_WRITE    CFG_DIR "/sc_write"
#define CFG_SC_PWRITE   CFG_DIR "/sc_pwrite"
#define CFG_SC_READ     CFG_DIR "/sc_read"
#define CFG_SC_PREAD    CFG_DIR "/sc_pread"
#define CFG_SC_FSYNC      CFG_DIR "/sc_fsync"
#define CFG_SC_FDATASYNC  CFG_DIR "/sc_fdatasync"
#define CFG_SC_MMAP       CFG_DIR "/sc_mmap"

/*
 * Validation mode: log all write operations without injecting faults.
 * Used to verify that a program (e.g., magma_dump) is read-only.
 * When enabled, writes are logged to CFG_VALIDATE_LOG but still allowed.
 * Read-only opens are logged to CFG_VALIDATE_READONLY_LOG for reference.
 * Set to 1 to enable, 0 to disable. Independent of cfg_enable.
 */
#define CFG_VALIDATE_MODE  CFG_DIR "/validate_mode"
#define CFG_VALIDATE_LOG   "/tmp/validate.log"
#define CFG_VALIDATE_READONLY_LOG "/tmp/validate_readonly.log"

/*
 * Partial-write mode (torn-page simulation).
 * partial_rate: percentage (0-100) chance that a write()/pwrite() commits
 *   only (count - PARTIAL_BLOCK) bytes to disk but reports count back to
 *   the caller.  BasicFile::Write's retry loop sees a full-success return
 *   and exits without retrying; Magma advances flushOffset normally and
 *   returns Status::OK() — the short write goes undetected.  On restart,
 *   the missing trailing block produces a checksum failure in LogRec::Check,
 *   exercising the Corruption-tolerant WAL truncate path.
 *   Only fires when count > PARTIAL_BLOCK (4096); skipped for tiny writes.
 *   Independent of fail_rate — both may be set simultaneously.
 */
#define CFG_PARTIAL_RATE CFG_DIR "/partial_rate"

/*
 * O_DIRECT alignment unit.  Magma's WAL flushes are always a multiple of
 * this size, so (count - PARTIAL_BLOCK) is also aligned — required for
 * O_DIRECT to accept the shorter pwrite().
 */
#define PARTIAL_BLOCK 4096

#define CACHE_NS 100000000   // 100 ms

/* ---------------- helpers ---------------- */

static long last_refresh_ns = 0;
static int  cfg_enable = 0;
static int  cfg_delay_ms = 0;
static int  cfg_fail_rate = 0;
static int  cfg_fail_errno = EIO;   /* default: EIO (5) */
static char cfg_target[256] = "/data/dta";

/* Per-syscall intercept flags — all default to 1 (active) */
static int  cfg_sc_write  = 1;
static int  cfg_sc_pwrite = 1;
static int  cfg_sc_read   = 1;
static int  cfg_sc_pread  = 1;
static int  cfg_sc_fsync     = 1;
static int  cfg_sc_fdatasync = 1;
static int  cfg_sc_mmap      = 1;

static int  cfg_partial_rate = 0; /* 0 = disabled */

/* Validation mode: log writes without blocking them */
static int  cfg_validate_mode = 0;

static long now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000L + ts.tv_nsec;
}

static int read_int(const char* path, int def) {
    FILE* f = fopen(path, "r");
    if (!f) return def;
    int v = def;
    fscanf(f, "%d", &v);
    fclose(f);
    return v;
}

static void read_str(const char* path, char* buf, size_t len) {
    FILE* f = fopen(path, "r");
    if (!f) return;
    fgets(buf, len, f);
    buf[strcspn(buf, "\n")] = 0;
    fclose(f);
}

static void refresh_config(void) {
    long t = now_ns();
    if (t - last_refresh_ns < CACHE_NS)
        return;

    last_refresh_ns = t;
    cfg_enable     = read_int(CFG_ENABLE, 0);
    cfg_delay_ms   = read_int(CFG_DELAY_MS, 0);
    cfg_fail_rate  = read_int(CFG_FAIL_RATE, 0);
    cfg_fail_errno = read_int(CFG_FAIL_ERRNO, EIO);
    read_str(CFG_TARGET, cfg_target, sizeof(cfg_target));

    cfg_sc_write  = read_int(CFG_SC_WRITE,  1);
    cfg_sc_pwrite = read_int(CFG_SC_PWRITE, 1);
    cfg_sc_read   = read_int(CFG_SC_READ,   1);
    cfg_sc_pread  = read_int(CFG_SC_PREAD,  1);
    cfg_sc_fsync     = read_int(CFG_SC_FSYNC,     1);
    cfg_sc_fdatasync = read_int(CFG_SC_FDATASYNC, 1);
    cfg_sc_mmap      = read_int(CFG_SC_MMAP,      1);

    cfg_partial_rate = read_int(CFG_PARTIAL_RATE, 0);

    cfg_validate_mode = read_int(CFG_VALIDATE_MODE, 0);
}

static int path_matches(int fd) {
    if (cfg_target[0] == 0)
        return 1;

    char link[64], path[512];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, path, sizeof(path) - 1);
    if (n <= 0)
        return 0;
    path[n] = 0;

    return strstr(path, cfg_target) != NULL;
}

static int should_fail(void) {
    if (cfg_fail_rate <= 0)
        return 0;
    return (rand() % 100) < cfg_fail_rate;
}

static int should_partial(void) {
    if (cfg_partial_rate <= 0)
        return 0;
    return (rand() % 100) < cfg_partial_rate;
}

#define CFG_LOG "/tmp/injected.log"

static void _log_write(const char* msg, int len) {
    long logfd = syscall(SYS_open, CFG_LOG, O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (logfd >= 0) {
        syscall(SYS_write, (long)logfd, msg, (long)len);
        syscall(SYS_close, logfd);
    } else {
        syscall(SYS_write, (long)STDERR_FILENO, msg, (long)len);
    }
}

static void _log_validate(const char* msg, int len) {
    long logfd = syscall(SYS_open, CFG_VALIDATE_LOG, O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (logfd >= 0) {
        syscall(SYS_write, (long)logfd, msg, (long)len);
        syscall(SYS_close, logfd);
    } else {
        syscall(SYS_write, (long)STDERR_FILENO, msg, (long)len);
    }
}

static void _log_validate_readonly(const char* msg, int len) {
    long logfd = syscall(SYS_open, CFG_VALIDATE_READONLY_LOG, O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (logfd >= 0) {
        syscall(SYS_write, (long)logfd, msg, (long)len);
        syscall(SYS_close, logfd);
    } else {
        syscall(SYS_write, (long)STDERR_FILENO, msg, (long)len);
    }
}

static void log_validate_readonly_open(const char* sc, const char* path, int flags) {
    char msg[800];
    int len = snprintf(msg, sizeof(msg),
                       "VALIDATE_READONLY: syscall=%s path=%s flags=0x%x (O_RDONLY)\n",
                       sc, path, flags);
    _log_validate_readonly(msg, len);
}

static void log_validate_write(const char* sc, int fd, size_t count, off_t offset) {
    char link[64], path[512], msg[800];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, path, sizeof(path) - 1);
    if (n > 0) path[n] = 0;
    else snprintf(path, sizeof(path), "<fd=%d>", fd);

    int len = snprintf(msg, sizeof(msg),
                       "VALIDATE_WRITE: syscall=%s fd=%d path=%s count=%zu offset=%ld\n",
                       sc, fd, path, count, (long)offset);
    _log_validate(msg, len);
}

static void log_injection(const char* sc, int fd, const char* action) {
    char link[64], path[512], msg[700];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, path, sizeof(path) - 1);
    if (n > 0) path[n] = 0;
    else snprintf(path, sizeof(path), "<fd=%d>", fd);

    int len = snprintf(msg, sizeof(msg), "FAULTINJECT: %s fd=%d path=%s action=%s\n",
                       sc, fd, path, action);
    _log_write(msg, len);
}

static void log_partial(const char* sc, int fd, size_t bytes_requested, size_t bytes_written) {
    char link[64], path[512], msg[800];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, path, sizeof(path) - 1);
    if (n > 0) path[n] = 0;
    else snprintf(path, sizeof(path), "<fd=%d>", fd);

    int len = snprintf(msg, sizeof(msg),
                       "FAULTINJECT: %s fd=%d path=%s action=partial "
                       "bytes_requested=%zu bytes_written=%zu\n",
                       sc, fd, path, bytes_requested, bytes_written);
    _log_write(msg, len);
}

static void inject_error(void) {
    errno = cfg_fail_errno > 0 ? cfg_fail_errno : EIO;
}

static const char* current_syscall_name = NULL;
static int current_syscall_fd = -1;

static void maybe_delay(void) {
    if (cfg_delay_ms > 0) {
        log_injection(current_syscall_name, current_syscall_fd, "delay");
        usleep(cfg_delay_ms * 1000);
    }
}

/* ---------------- intercepted syscalls ---------------- */

static void log_validate_open(const char* sc, const char* path, int flags) {
    char msg[800];
    const char* flag_str = "";
    if (flags & O_WRONLY) flag_str = "O_WRONLY";
    else if (flags & O_RDWR) flag_str = "O_RDWR";
    else if (flags & O_CREAT) flag_str = "O_CREAT";
    else if (flags & O_TRUNC) flag_str = "O_TRUNC";
    else if (flags & O_APPEND) flag_str = "O_APPEND";

    int len = snprintf(msg, sizeof(msg),
                       "VALIDATE_OPEN: syscall=%s path=%s flags=0x%x (%s%s%s%s%s)\n",
                       sc, path, flags,
                       (flags & O_WRONLY) ? "O_WRONLY|" : "",
                       (flags & O_RDWR) ? "O_RDWR|" : "",
                       (flags & O_CREAT) ? "O_CREAT|" : "",
                       (flags & O_TRUNC) ? "O_TRUNC|" : "",
                       (flags & O_APPEND) ? "O_APPEND|" : "");
    _log_validate(msg, len);
}

static int path_str_matches(const char* path) {
    if (cfg_target[0] == 0)
        return 1;
    return strstr(path, cfg_target) != NULL;
}

int open(const char* pathname, int flags, ...) {
    static int (*real_open)(const char*, int, ...) = NULL;
    if (!real_open)
        real_open = dlsym(RTLD_NEXT, "open");

    refresh_config();

    /* Validation mode: log opens to target paths */
    if (cfg_validate_mode && path_str_matches(pathname)) {
        int write_flags = O_WRONLY | O_RDWR | O_CREAT | O_TRUNC | O_APPEND;
        if (flags & write_flags) {
            log_validate_open("open", pathname, flags);
        } else {
            /* Log read-only opens to separate file for reference */
            log_validate_readonly_open("open", pathname, flags);
        }
    }

    /* Handle variadic mode argument for O_CREAT */
    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, flags);
        mode_t mode = va_arg(ap, mode_t);
        va_end(ap);
        return real_open(pathname, flags, mode);
    }
    return real_open(pathname, flags);
}

int openat(int dirfd, const char* pathname, int flags, ...) {
    static int (*real_openat)(int, const char*, int, ...) = NULL;
    if (!real_openat)
        real_openat = dlsym(RTLD_NEXT, "openat");

    refresh_config();

    /* Validation mode: log opens to target paths */
    if (cfg_validate_mode && path_str_matches(pathname)) {
        int write_flags = O_WRONLY | O_RDWR | O_CREAT | O_TRUNC | O_APPEND;
        if (flags & write_flags) {
            log_validate_open("openat", pathname, flags);
        } else {
            /* Log read-only opens to separate file for reference */
            log_validate_readonly_open("openat", pathname, flags);
        }
    }

    /* Handle variadic mode argument for O_CREAT */
    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, flags);
        mode_t mode = va_arg(ap, mode_t);
        va_end(ap);
        return real_openat(dirfd, pathname, flags, mode);
    }
    return real_openat(dirfd, pathname, flags);
}

ssize_t write(int fd, const void* buf, size_t count) {
    static ssize_t (*real_write)(int, const void*, size_t) = NULL;
    if (!real_write)
        real_write = dlsym(RTLD_NEXT, "write");

    refresh_config();

    /* Validation mode: log write operations to target paths */
    if (cfg_validate_mode && path_matches(fd)) {
        log_validate_write("write", fd, count, -1);
        return real_write(fd, buf, count);
    }

    if (!cfg_enable || !cfg_sc_write || !path_matches(fd))
        return real_write(fd, buf, count);

    current_syscall_name = "write"; current_syscall_fd = fd;
    maybe_delay();

    if (should_partial() && count > PARTIAL_BLOCK) {
        /*
         * Write (count - PARTIAL_BLOCK) bytes and return count to the
         * caller.  BasicFile::Write's retry loop sees ret==count, exits
         * immediately, and returns Status::OK() — Magma never notices.
         * The final PARTIAL_BLOCK bytes are absent from disk, leaving a
         * torn record that WAL replay will detect via checksum failure.
         */
        size_t partial = count - PARTIAL_BLOCK;
        ssize_t ret = real_write(fd, buf, partial);
        if (ret < 0)
            return ret;
        log_partial("write", fd, count, partial);
        return (ssize_t)count;
    }

    if (should_fail()) {
        log_injection("write", fd, "error");
        inject_error();
        return -1;
    }

    return real_write(fd, buf, count);
}

ssize_t pwrite(int fd, const void* buf, size_t count, off_t off) {
    static ssize_t (*real_pwrite)(int, const void*, size_t, off_t) = NULL;
    if (!real_pwrite)
        real_pwrite = dlsym(RTLD_NEXT, "pwrite");

    refresh_config();

    /* Validation mode: log pwrite operations to target paths */
    if (cfg_validate_mode && path_matches(fd)) {
        log_validate_write("pwrite", fd, count, off);
        return real_pwrite(fd, buf, count, off);
    }

    if (!cfg_enable || !cfg_sc_pwrite || !path_matches(fd))
        return real_pwrite(fd, buf, count, off);

    current_syscall_name = "pwrite"; current_syscall_fd = fd;
    maybe_delay();

    if (should_partial() && count > PARTIAL_BLOCK) {
        /*
         * Same deception as write(): commit only (count - PARTIAL_BLOCK)
         * bytes at the given offset, but report count back to the caller.
         * PARTIAL_BLOCK is 4096 — the O_DIRECT alignment unit — so the
         * partial pwrite() itself is aligned and succeeds under O_DIRECT.
         * The missing trailing block is what the torn page leaves on disk.
         */
        size_t partial = count - PARTIAL_BLOCK;
        ssize_t ret = real_pwrite(fd, buf, partial, off);
        if (ret < 0)
            return ret;
        log_partial("pwrite", fd, count, partial);
        return (ssize_t)count;
    }

    if (should_fail()) {
        log_injection("pwrite", fd, "error");
        inject_error();
        return -1;
    }

    return real_pwrite(fd, buf, count, off);
}

ssize_t read(int fd, void* buf, size_t count) {
    static ssize_t (*real_read)(int, void*, size_t) = NULL;
    if (!real_read)
        real_read = dlsym(RTLD_NEXT, "read");

    refresh_config();

    if (!cfg_enable || !cfg_sc_read || !path_matches(fd))
        return real_read(fd, buf, count);

    current_syscall_name = "read"; current_syscall_fd = fd;
    maybe_delay();

    if (should_fail()) {
        log_injection("read", fd, "error");
        inject_error();
        return -1;
    }

    return real_read(fd, buf, count);
}

ssize_t pread(int fd, void* buf, size_t count, off_t off) {
    static ssize_t (*real_pread)(int, void*, size_t, off_t) = NULL;
    if (!real_pread)
        real_pread = dlsym(RTLD_NEXT, "pread");

    refresh_config();

    if (!cfg_enable || !cfg_sc_pread || !path_matches(fd))
        return real_pread(fd, buf, count, off);

    current_syscall_name = "pread"; current_syscall_fd = fd;
    maybe_delay();

    if (should_fail()) {
        log_injection("pread", fd, "error");
        inject_error();
        return -1;
    }

    return real_pread(fd, buf, count, off);
}

int fsync(int fd) {
    static int (*real_fsync)(int) = NULL;
    if (!real_fsync)
        real_fsync = dlsym(RTLD_NEXT, "fsync");

    refresh_config();

    if (!cfg_enable || !cfg_sc_fsync || !path_matches(fd))
        return real_fsync(fd);

    current_syscall_name = "fsync"; current_syscall_fd = fd;
    maybe_delay();

    if (should_fail()) {
        log_injection("fsync", fd, "error");
        inject_error();
        return -1;
    }

    return real_fsync(fd);
}

int fdatasync(int fd) {
    static int (*real_fdatasync)(int) = NULL;
    if (!real_fdatasync)
        real_fdatasync = dlsym(RTLD_NEXT, "fdatasync");

    refresh_config();

    if (!cfg_enable || !cfg_sc_fdatasync || !path_matches(fd))
        return real_fdatasync(fd);

    current_syscall_name = "fdatasync"; current_syscall_fd = fd;
    maybe_delay();

    if (should_fail()) {
        log_injection("fdatasync", fd, "error");
        inject_error();
        return -1;
    }

    return real_fdatasync(fd);
}

void* mmap(void* addr, size_t length, int prot, int flags, int fd, off_t offset) {
    static void* (*real_mmap)(void*, size_t, int, int, int, off_t) = NULL;
    if (!real_mmap)
        real_mmap = dlsym(RTLD_NEXT, "mmap");

    refresh_config();

    /* Anonymous mappings (fd == -1) are never storage I/O — skip them. */
    if (!cfg_enable || !cfg_sc_mmap || fd < 0 || !path_matches(fd))
        return real_mmap(addr, length, prot, flags, fd, offset);

    current_syscall_name = "mmap"; current_syscall_fd = fd;
    maybe_delay();

    if (should_fail()) {
        log_injection("mmap", fd, "error");
        inject_error();
        return MAP_FAILED;
    }

    return real_mmap(addr, length, prot, flags, fd, offset);
}

/* ---------------- init ---------------- */

__attribute__((constructor))
static void init_faultinject(void) {
    srand(time(NULL) ^ getpid());
    mkdir(CFG_DIR, 0755);
}
