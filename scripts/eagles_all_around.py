from com.jcraft.jsch import JSchException
from com.jcraft.jsch import JSch
from org.python.core.util import FileUtil
import sys
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
        # trusting, and the next command would most likely stall as well.
        if _ssh_client.getExitStatus() == 124:
            raise JSchException("timed out after %ss: %s" % (timeout, command))
    except:
        # Once a host stops answering it will stall on every later command
        # too, so give up on it rather than paying the timeout again.
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


# Certificates are logged as PEM blobs whose base64 body regularly contains
# the substring "panic", which is not a crash. Blank the blobs out before
# grepping: the first expression handles a certificate escaped onto a single
# line, the second one spanning real newlines.
CERT_BEGIN = "-----BEGIN CERTIFICATE-----"
CERT_END = "-----END CERTIFICATE-----"
STRIP_CERTS = ('sed -e "s|%s.*%s|[certificate]|g" -e "/%s/,/%s/d"'
               % (CERT_BEGIN, CERT_END, CERT_BEGIN, CERT_END))


def panic_grep(find_cmd):
    """Grep for panic in the files find_cmd prints, ignoring certificates.

    -A 5 keeps the 5 following lines so the timestamps and stack around the
    panic are visible. Files are filtered one at a time so a certificate at
    the end of one file cannot swallow the start of the next; the filename
    prefix grep adds for multiple files is put back by hand.
    """
    return trimmed(
        "%s -print0 | xargs -0 -r -I FILE sh -c "
        "'%s \"$1\" | grep -i -A 5 panic | sed -e \"s|^|$1: |\"' _ FILE"
        % (find_cmd, STRIP_CERTS))


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
    for server in all_slaves:
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


def scan_slave(session):
    cmds = []
    if len(sys.argv) > 1:
        for pattern in scan_patterns():
            cmds.append("find /data/workspace/ -iname '*collect*{}*.zip'".format(pattern))
            cmds.append("find /data/workspace/ -iname '*{}*diag*.zip'".format(pattern.replace("-", "")))
    else:
        # Scope the default scan to the last week; a whole year of old
        # workspaces makes the panic output unreadable.
        cmds.append("find /data/workspace/ -iname '*collect*.zip' -mtime -7")
        cmds.append("find /data/workspace/ -iname '*diag*.zip' -mtime -7")

    for cmd in cmds:
        output, _ = run(cmd, session)
        for cbcollect_zips in output:
            flag = True
            log_files, _ = run("zipinfo -1 {}".format(cbcollect_zips), session)
            for file in log_files:
                if file.rstrip().endswith("dmp"):
                    print "#######################"
                    print "checking: %s" % cbcollect_zips.rstrip()
                    print "#######################"
                    print file.rstrip()
                    flag = False
                    break
            run("rm -rf /root/cbcollect*", session)[0]
            run("unzip {}".format(cbcollect_zips), session)[0]
            memcached = "/root/cbcollect*/memcached.log*"
            o, _ = run(trimmed("grep 'CRITICAL\| ERROR ' {} | grep -v {}".format(memcached, exclude)), session)
            if o:
                if flag:
                    print "#######################"
                    print "checking: %s" % cbcollect_zips.rstrip()
                    print "#######################"
                print "".join(o)
            # Check all logs for panic.
            all_log = ("find /root/cbcollect*/ -maxdepth 1 -type f "
                       "! -iname 'couchbase.log*' ! -iname 'indexer_pprof.log*'")
            o, _ = run(panic_grep(all_log), session)
            if o:
                if flag:
                    print "#######################"
                    print "checking: %s" % cbcollect_zips.rstrip()
                    print "#######################"
                print "=== panic found ==="
                print "".join(o)


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
    dmpFiles = run("find {} -maxdepth 1 -iname '*.dmp' -mtime -7 -printf '%T@ %f\\n' | sort -rn | awk '{{print $2}}'".format(crashDir), session)[0]
    dmpFiles = [f.strip("\n") for f in dmpFiles]
    if dmpFiles:
        print(run("cat /opt/couchbase/VERSION.txt", session)[0])
        msg = "Node %s - Core dump seen: %s" % (server, str(len(dmpFiles)))
        dmpmsg += msg + "\n"
        print(msg)
        print(server + " : Stack Trace of first crash: " + dmpFiles[-1])
        print(get_gdb(crashDir, dmpFiles[-1]))
    else:
        print(server + " : No crash files found")

    print(server + " : Looking for CRITICAL messages in log")
    logsDir = libCb + "logs/"
    logFiles = run("find {} -maxdepth 1 -iname 'memcached.log.*' -mtime -7".format(logsDir), session)[0]
    for logFile in logFiles:
        criticalMessages = run(trimmed("grep -r 'CRITICAL\| ERROR ' {} | grep -v {}".format(logFile.strip("\n"), exclude)), session)[0]
        index = findIndexOf(criticalMessages, "Fatal error encountered during exception handling")
        if index != -1:
            criticalMessages = criticalMessages[:index]
        if (criticalMessages):
            print(server + " : Found message in " + logFile.strip("\n"))
            print("".join(criticalMessages))
            break

    print(server + " : Looking for panic in logs")
    # Restrict to files touched in the last week so old panics don't keep
    # resurfacing.
    log_find = ("find {} -maxdepth 1 -type f -mtime -7 ! -iname 'couchbase.log*' "
                "! -iname 'indexer_pprof.log*'".format(logsDir))
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
