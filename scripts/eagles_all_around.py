from com.jcraft.jsch import JSch
from org.python.core.util import FileUtil
import sys
import time
import traceback
from datetime import date

failed = []
skipped = []
exclude = "'Rollback point not found\|No space left on device\|Permission denied\|Already exists\|Unsupported key supplied'"

# A host that accepts the connection and then stalls used to block the run
# until Jenkins aborted the build. Reads carry a socket timeout and commands
# an upper bound on the host itself, so a stuck host raises and we move on.
CONNECT_TIMEOUT_MS = 10000
SOCKET_TIMEOUT_MS = 120000
CMD_TIMEOUT_SECS = 300
GDB_TIMEOUT_SECS = 900
# A multi-GB cbbackupmgr collectinfo takes well over the default budget to
# unpack, and every one of them used to abort the rest of that host's scan.
UNZIP_TIMEOUT_SECS = 900
# Consecutive command timeouts before a host is written off. A single slow
# zip is skipped; a host that times out repeatedly is not worth the wait.
MAX_TIMEOUTS_PER_HOST = 3
# A backup slave carries hundreds of collectinfo zips. Skipping a slow zip
# instead of abandoning the host meant those slaves got scanned in full, and
# the run hit the 1430-minute Jenkins timeout having reached 129 of 154
# slaves and no servers at all. Bound the time one host may take, and bound
# the phase, so the server scan - the only place a live crash dump is found
# - always gets to run.
HOST_BUDGET_SECS = 20 * 60
SLAVE_PHASE_BUDGET_SECS = 8 * 60 * 60
# A crash dump sitting in crash/ is the artifact worth reporting even when
# it is not fresh, and a node keeps its dumps until it is reimaged. Seven
# days was short enough that real dumps aged out before anyone saw them.
CRASH_MTIME_DAYS = 30


class CommandTimeout(Exception):
    """The remote command outlived its budget. The host is still answering."""
    pass


def sh_quote(command):
    return "'" + command.replace("'", "'\\''") + "'"


def run(command, session, timeout=CMD_TIMEOUT_SECS):
    output = []
    error = []
    _ssh_client = None
    if timeout:
        command = "timeout %s sh -c %s" % (timeout, sh_quote(command))
        # Keep the socket timeout above the command timeout so a slow but
        # healthy command is not mistaken for a dead host.
        socket_timeout = (timeout + 60) * 1000
    else:
        socket_timeout = SOCKET_TIMEOUT_MS
    try:
        session.setTimeout(socket_timeout)
        _ssh_client = session.openChannel("exec")
        _ssh_client.setInputStream(None)
        _ssh_client.setErrStream(None)

        instream = _ssh_client.getInputStream()
        errstream = _ssh_client.getErrStream()
        _ssh_client.setCommand(command)
        _ssh_client.connect()
        fu1 = FileUtil.wrap(instream)
        for line in fu1.readlines():
            output.append(line)
        fu1.close()

        fu2 = FileUtil.wrap(errstream)
        for line in fu2.readlines():
            error.append(line)
        fu2.close()

        # 124 is `timeout` killing the command. Partial output is not worth
        # trusting, but the host itself answered, so this is a CommandTimeout
        # the caller can skip past rather than a reason to drop the host.
        status = _ssh_client.getExitStatus()
        if status == 124:
            raise CommandTimeout("timed out after %ss: %s" % (timeout, command))
        # grep exits 1 with no match, so only a larger status is worth
        # reporting. Without this a command that never ran (a missing
        # binary, a bad path) looks exactly like "found nothing".
        if status > 1 and error:
            print("%s : exit %s from %s: %s"
                  % (session.getHost(), status, command,
                     "".join(error[:2]).rstrip()))
    except:
        # Once a host stops answering it will stall on every later command
        # too, so hand the failure to the caller rather than paying the
        # timeout again. What that costs depends on the failure: a dead
        # session drops the host, a CommandTimeout only drops the command.
        print("%s : command failed: %s" % (session.getHost(),
                                           sys.exc_info()[1]))
        raise
    finally:
        if _ssh_client is not None:
            try:
                _ssh_client.disconnect()
            except:
                pass
    return output, error


# A single corrupt file repeats the same CRITICAL or panic line tens of
# thousands of times, which buries every other node in the run. Keep the
# first few lines and say how many were dropped. The counting happens on the
# host, so only the trimmed output crosses the wire.
MAX_OUTPUT_LINES = 50
TRIM_OUTPUT = ("awk 'NR<=%d; END {if (NR>%d) printf \"... %%d more lines "
               "suppressed (%%d total)\\n\", NR-%d, NR}'"
               % (MAX_OUTPUT_LINES, MAX_OUTPUT_LINES, MAX_OUTPUT_LINES))


def trimmed(command):
    """Wrap a remote command so it prints at most MAX_OUTPUT_LINES lines."""
    return "%s | %s" % (command, TRIM_OUTPUT)


# memcached logs routine " ERROR " lines in the thousands (one per closed
# connection, say) while a crash writes a handful of CRITICAL ones. Grepping
# both severities in a single pass and keeping the first MAX_OUTPUT_LINES let
# the ERROR spam use up the whole budget, so the "Breakpad caught a crash"
# lines that come later in the file were suppressed and the crash went
# unreported. Scanning each severity on its own budget keeps a CRITICAL
# visible no matter how noisy ERROR is. Crash-level first, so it reads at the
# top of the block.
SEVERITIES = ["CRITICAL", " ERROR "]


def severity_scan(session, grep_args):
    """Grep grep_args for each severity separately, one output budget each.

    grep_args carries the files to search, so an empty one would leave grep
    reading stdin until the command timeout killed it.
    """
    if not grep_args.strip():
        return []
    messages = []
    for severity in SEVERITIES:
        # -H so a single-file search still says which file the line came from.
        cmd = "grep -H '%s' %s | grep -v %s" % (severity, grep_args, exclude)
        messages.extend(run(trimmed(cmd), session)[0])
    return messages


# Certificates are logged as PEM blobs whose base64 body regularly contains
# the substring "panic", which is not a crash. Blank the blobs out before
# grepping: the first expression handles a certificate escaped onto a single
# line, the second one spanning real newlines.
CERT_BEGIN = "-----BEGIN CERTIFICATE-----"
CERT_END = "-----END CERTIFICATE-----"
STRIP_CERTS = ('sed -e "s|%s.*%s|[certificate]|g" -e "/%s/,/%s/d"'
               % (CERT_BEGIN, CERT_END, CERT_BEGIN, CERT_END))


# Compressed payloads, such as the base64 NsMappingRecords blobs diag.log
# carries, are single lines tens of thousands of characters long whose body
# happens to contain the substring "panic". A real panic is always a short
# log line, so drop the oversized ones before grepping.
MAX_LINE_LEN = 1000
DROP_LONG_LINES = 'awk "length < %d"' % MAX_LINE_LEN

# Match "panic" as a whole word so it is not picked up from the middle of an
# identifier or an encoded payload that survived the filters above.
PANIC_RE = r"\bpanic\b"

# Files the panic grep never has anything to say about. couchbase.log and
# indexer_pprof.log are dumps of other logs and of profiler output.
# system_info.log lists sysctl, whose kernel.panic* keys are settings rather
# than crashes; it was 537 of the 739 blocks reported in build 421, the
# first run to reach the cbcontbk collections that carry it.
PANIC_EXCLUDES = ("! -iname 'couchbase.log*' ! -iname 'indexer_pprof.log*' "
                  "! -iname 'system_info.log*'")


def panic_grep(find_cmd):
    """Grep for panic in the files find_cmd prints, ignoring certificates.

    -A 5 keeps the 5 following lines so the timestamps and stack around the
    panic are visible. Files are filtered one at a time so a certificate at
    the end of one file cannot swallow the start of the next; the filename
    prefix grep adds for multiple files is put back by hand.
    """
    return trimmed(
        "%s -print0 | xargs -0 -r -I FILE sh -c "
        "'%s \"$1\" | %s | grep -i -E -A 5 \"%s\" "
        "| sed -e \"s|^|$1: |\"' _ FILE"
        % (find_cmd, STRIP_CERTS, DROP_LONG_LINES, PANIC_RE))


def connection(server):
    try:
        jsch = JSch()
        session = jsch.getSession("root", server, 22)
        session.setPassword("couchbase")
        session.setConfig("StrictHostKeyChecking", "no")
        session.connect(CONNECT_TIMEOUT_MS)
        session.setTimeout(SOCKET_TIMEOUT_MS)
        return session
    except:
        print("%s : ssh failed: %s" % (server, sys.exc_info()[1]))
        failed.append(server)
        return None


def disconnect(session):
    try:
        session.disconnect()
    except:
        pass


def give_up(server, phase):
    print("%s : giving up (%s): %s" % (server, phase, sys.exc_info()[1]))
    traceback.print_exc()
    skipped.append("%s (%s)" % (server, phase))


def scan_all_slaves():
    all_slaves = ["172.23.123.80","172.23.107.117","172.23.107.116","172.23.107.120","172.23.106.136","172.23.121.65",
                  "172.23.105.66","172.23.108.94","172.23.104.254",
                  "172.23.120.172","172.23.100.195","172.23.109.166","172.23.122.36","172.23.122.37",
                  "172.23.122.38","172.23.99.156","172.23.120.106","172.23.123.184","172.23.120.84",
                  "172.23.120.223","172.23.120.26","172.23.120.85","172.23.120.90","172.23.120.103",
                  "172.23.120.104","172.23.120.105","172.23.105.131","172.23.106.231","172.23.105.170",
                  "172.23.98.7","172.23.105.169","172.23.106.94","172.23.96.83","172.23.109.38",
                  "172.23.106.41","172.23.106.43","172.23.106.34","172.23.105.209","172.23.107.165",
                  "172.23.104.30","172.23.108.6","172.23.106.230","172.23.96.110","172.23.107.166",
                  "172.23.109.52","172.23.104.35","172.23.105.174","172.23.211.37","172.23.106.193",
                  "172.23.107.226","172.23.106.162","172.23.105.95","172.23.105.40","172.23.108.33",
                  "172.23.105.135","172.23.105.136","172.23.96.232","172.23.96.246","172.23.96.248",
                  "172.23.96.255","172.23.97.0","172.23.96.206","172.23.105.248","172.23.107.249",
                  "172.23.107.21","172.23.107.68","172.23.121.16","172.23.120.254","172.23.120.31",
                  "172.23.120.228","172.23.108.27","172.23.104.76","172.23.105.144","172.23.108.222",
                  "172.23.107.238","172.23.106.205","172.23.105.131","172.23.105.131","172.23.105.115",
                  "172.23.123.88","172.23.123.91","172.23.123.69","172.23.123.69","172.23.123.71",
                  "172.23.123.72","172.23.123.75","172.23.123.73","172.23.123.74","172.23.123.70",
                  "172.23.123.77","172.23.123.76","172.23.123.78","172.23.123.80",
                  "172.23.123.79","172.23.97.128","172.23.99.156","172.23.104.136","172.23.97.128",
                  "172.23.99.156","172.23.97.101","172.23.107.216","172.23.104.34",
                  "172.23.222.77","172.23.222.78","172.23.222.79","172.23.222.80","172.23.222.81","172.23.222.82",
                  "172.23.120.173","172.23.120.174","172.23.120.175","172.23.120.178",
                  "172.23.104.235",
                  "172.23.104.73","172.23.104.80","172.23.104.103","172.23.104.105","172.23.104.121","172.23.104.167",
                  "172.23.104.193","172.23.104.201","172.23.105.10",
                  "172.23.105.32","172.23.105.109","172.23.105.125",
                  "172.23.104.248","172.23.104.249","172.23.104.250","172.23.105.0",
                  "172.23.221.187","172.23.221.188","172.23.221.189","172.23.221.190",
                  "172.23.218.190","172.23.218.191","172.23.218.192","172.23.218.193","172.23.218.194","172.23.218.195",
                  "172.23.219.59","172.23.219.60","172.23.219.61","172.23.219.62","172.23.219.63","172.23.219.64",
                  "172.23.219.65","172.23.219.66",
                  "172.23.104.171","172.23.104.176","172.23.105.208",
                  "172.23.104.219","172.23.104.241","172.23.105.152","172.23.105.154"
                  ]
    count = 1
    deadline = time.time() + SLAVE_PHASE_BUDGET_SECS
    for server in all_slaves:
        if time.time() > deadline:
            print("slave phase budget spent, %s of %s slaves unscanned"
                  % (len(all_slaves) - count + 1, len(all_slaves)))
            break
        print("--+--+--+--+-- %s. CHECKING ON SLAVE: %s --+--+--+--+--" % (count, server))
        count += 1
        session = connection(server)
        if session is None:
            continue

        try:
            scan_slave(session)
        except:
            give_up(server, "slave scan")
        finally:
            disconnect(session)


def scan_patterns():
    if len(sys.argv) > 1:
        return [sys.argv[1]]
    # With no argument, scan the current year rather than a hardcoded one. In
    # January last year's workspaces are still around, so include them too.
    today = date.today()
    patterns = [str(today.year)]
    if today.month == 1:
        patterns.append(str(today.year - 1))
    return patterns


# Unpack into a directory of our own rather than into /root. The old code
# extracted in place and then grepped /root/cbcollect*/, so a zip that
# unpacked under any other name (cbbackupmgr-collectinfo-*.zip does) was
# never actually scanned, and its files were left behind for the next zip's
# unzip to stop and prompt about.
EXTRACT_DIR = "/root/cbcollect_scan"


def scan_zip(session, zip_path):
    """Report crash dumps, crash-level log lines and panics in one zip."""
    header = ["#######################",
              "checking: %s" % zip_path,
              "#######################"]

    def emit(body):
        # The header is printed once, by whichever check reports first.
        if header:
            print("\n".join(header))
            del header[:]
        print(body)

    # A .dmp inside the collection is the crash itself, so look before
    # unpacking and report it whatever else the logs do or do not say.
    log_files, _ = run("zipinfo -1 %s" % zip_path, session)
    for name in log_files:
        if name.rstrip().endswith("dmp"):
            emit(name.rstrip())
            break

    run("rm -rf %s" % EXTRACT_DIR, session)
    # Everything below reads <collection>/<file> and nothing deeper, so
    # unpacking the rest is pure cost: a cbbackupmgr collectinfo carries the
    # whole backup repository under it. -x '*/*/*' drops those members (an
    # unzip pattern matches across "/", unlike a shell glob). -o so a name
    # that survived the cleanup cannot leave unzip waiting on a prompt until
    # the command timeout kills it.
    run("unzip -o -q %s -x '*/*/*' -d %s" % (zip_path, EXTRACT_DIR), session,
        timeout=UNZIP_TIMEOUT_SECS)

    messages = severity_scan(
        session, "-r %s --include='memcached.log*'" % EXTRACT_DIR)
    if messages:
        emit("".join(messages))

    # Check all logs for panic.
    all_log = ("find %s -maxdepth 2 -type f %s"
               % (EXTRACT_DIR, PANIC_EXCLUDES))
    panics, _ = run(panic_grep(all_log), session)
    if panics:
        emit("=== panic found ===")
        print("".join(panics))


def scan_slave(session):
    cmds = []
    if len(sys.argv) > 1:
        for pattern in scan_patterns():
            cmds.append("find /data/workspace/ -iname '*collect*{}*.zip'".format(pattern))
            cmds.append("find /data/workspace/ -iname '*{}*diag*.zip'".format(pattern.replace("-", "")))
    else:
        # Scope the default scan to the last week; a whole year of old
        # workspaces makes the panic output unreadable. The job runs daily,
        # so every zip still gets seen.
        cmds.append("find /data/workspace/ -iname '*collect*.zip' -mtime -7")
        cmds.append("find /data/workspace/ -iname '*diag*.zip' -mtime -7")

    timeouts = 0
    deadline = time.time() + HOST_BUDGET_SECS
    for cmd in cmds:
        output, _ = run(cmd, session)
        for index, cbcollect_zips in enumerate(output):
            zip_path = cbcollect_zips.rstrip()
            if time.time() > deadline:
                print("%s : host budget spent, %s of %s zips unscanned for %s"
                      % (session.getHost(), len(output) - index, len(output),
                         cmd))
                break
            try:
                scan_zip(session, zip_path)
            except CommandTimeout:
                # Unpacking one oversized backup collection used to abort the
                # whole host, so every zip after it went unchecked. Skip the
                # zip instead, and only write the host off if it keeps
                # timing out.
                timeouts += 1
                print("skipped zip (timed out): %s" % zip_path)
                if timeouts >= MAX_TIMEOUTS_PER_HOST:
                    raise


def check_coredump_exist(server):
    binCb = "/opt/couchbase/bin/"
    libCb = "/opt/couchbase/var/lib/couchbase/"
    session = connection(server)

    if session is None:
        return

    try:
        check_server_logs(server, session, binCb, libCb)
    except:
        give_up(server, "server scan")
    finally:
        disconnect(session)


def check_server_logs(server, session, binCb, libCb):
    dmpmsg = ""

    def findIndexOf(strList, subString):
        for i in range(len(strList)):
            if subString in strList[i]:
                return i
        return -1

    def get_gdb(dmpPath, dmpName):
        dmpFile = dmpPath + dmpName
        coreFile = dmpPath + dmpName.strip(".dmp") + ".core"
        run("rm -rf " + coreFile, session)
        run("/" + binCb + "minidump-2-core " + dmpFile + " > " + coreFile, session)
        gdbOut = run("gdb --batch " + binCb + "memcached -c " + coreFile + " -ex \"bt full\" -ex quit", session, timeout=GDB_TIMEOUT_SECS)[0]
        index = findIndexOf(gdbOut, "Core was generated by")
        gdbOut = gdbOut[index:]
        gdbOut = " ".join(gdbOut)
        return gdbOut

    print(server + " : SSH Successful")
    print(server + " : Looking for crash dump files")
    crashDir = libCb + "crash/"
    dmpFiles = run("find {} -maxdepth 1 -iname '*.dmp' -mtime -{} -printf '%T@ %f\\n' | sort -rn | awk '{{print $2}}'".format(crashDir, CRASH_MTIME_DAYS), session)[0]
    dmpFiles = [f.strip("\n") for f in dmpFiles]
    if dmpFiles:
        print(run("cat /opt/couchbase/VERSION.txt", session)[0])
        msg = "Node %s - Core dump seen: %s" % (server, str(len(dmpFiles)))
        dmpmsg += msg + "\n"
        print(msg)
        # Name every dump, newest first. Only one of them gets a backtrace,
        # and the count alone does not say which crashes are on the node.
        for dmpFile in dmpFiles:
            print(server + " : " + crashDir + dmpFile)
        print(server + " : Stack Trace of first crash: " + dmpFiles[-1])
        print(get_gdb(crashDir, dmpFiles[-1]))
    else:
        print(server + " : No crash files found")

    print(server + " : Looking for CRITICAL messages in log")
    logsDir = libCb + "logs/"
    logFiles = run("find {} -maxdepth 1 -iname 'memcached.log.*' -mtime -7".format(logsDir), session)[0]
    logFiles = [f.strip("\n") for f in logFiles]
    if logFiles:
        # One grep across every rotated log, rather than one command per file
        # stopping at the first file with a hit: a crash in an older log used
        # to be hidden behind whichever newer log carried an ERROR line.
        criticalMessages = severity_scan(session, " ".join(logFiles))
        if criticalMessages:
            print(server + " : Found messages in " + logsDir)
            print("".join(criticalMessages))

    print(server + " : Looking for panic in logs")
    # Restrict to files touched in the last week so old panics don't keep
    # resurfacing.
    log_find = ("find {} -maxdepth 1 -type f -mtime -7 {}".format(
        logsDir, PANIC_EXCLUDES))
    panicMessages = run(panic_grep(log_find), session)[0]
    if panicMessages:
        version = run("cat /opt/couchbase/VERSION.txt", session)[0]
        version_str = "".join(version).strip()
        print(server + " : === panic found in " + version_str + " ===")
        print("".join(panicMessages))


def scan_all_servers():
    from java.time import Duration
    from com.couchbase.client.java import Cluster, ClusterOptions
    from com.couchbase.client.java.env import ClusterEnvironment
    from com.couchbase.client.core.env import TimeoutConfig, IoConfig
    cluster_env = ClusterEnvironment.builder().ioConfig(IoConfig.numKvConnections(25)).timeoutConfig(TimeoutConfig.builder().connectTimeout(Duration.ofSeconds(20)).kvTimeout(Duration.ofSeconds(10)))
    cluster_options = ClusterOptions.clusterOptions("Administrator", "esabhcuoc").environment(cluster_env.build())
    cluster = Cluster.connect("172.23.217.21", cluster_options)
    STATEMENT = "SELECT ipaddr FROM `QE-server-pool` WHERE os='debian';"
    result = cluster.query(STATEMENT)

    count = 1
    for server in result.rowsAsObject():
        print("--+--+--+--+-- %s. CHECKING ON SERVER: %s --+--+--+--+--"
              % (count, server.get("ipaddr")))
        count += 1
        check_coredump_exist(server.get("ipaddr"))


if __name__ == "__main__":
    for phase in [scan_all_slaves, scan_all_servers]:
        try:
            phase()
        except:
            print("ERROR: %s did not complete: %s"
                  % (phase.__name__, sys.exc_info()[1]))
            traceback.print_exc()

    if failed:
        for server in failed:
            print("ssh failed: %s" % server)

    if skipped:
        for server in skipped:
            print("skipped: %s" % server)
