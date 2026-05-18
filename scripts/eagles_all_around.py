from com.jcraft.jsch import JSchException
from com.jcraft.jsch import JSch
from org.python.core.util import FileUtil
import sys

failed = []
exclude = "'Rollback point not found\|No space left on device\|Permission denied\|Already exists\|Unsupported key supplied'"


def run(command, session):
    output = []
    error = []
    try:
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
        _ssh_client.disconnect()
    except JSchException as e:
        print("JSch exception on %s: %s" % (server, str(e)))
    return output, error


def connection(server):
    try:
        jsch = JSch()
        session = jsch.getSession("root", server, 22)
        session.setPassword("couchbase")
        session.setConfig("StrictHostKeyChecking", "no")
        session.connect(10000)
        return session
    except:
        failed.append(server)
        return None


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

        cmds = ["find /data/workspace/ -iname '*collect*2025*.zip'", "find /data/workspace/ -iname '*2025*diag*.zip'"]
        if len(sys.argv) > 1:
            cmds = ["find /data/workspace/ -iname '*collect*{}*.zip'".format(sys.argv[1]),
                    "find /data/workspace/ -iname '*{}*diag*.zip'".format(sys.argv[1].replace("-", ""))]

        for cmd in cmds:
            output, _ = run(cmd, session)
            try:
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
                    o, _ = run("grep 'CRITICAL\| ERROR ' {} | grep -v {}".format(memcached, exclude), session)
                    if o:
                        if flag:
                            print "#######################"
                            print "checking: %s" % cbcollect_zips.rstrip()
                            print "#######################"
                        print "".join(o)
            except:
                pass
        session.disconnect()


def check_coredump_exist(server):
    binCb = "/opt/couchbase/bin/"
    libCb = "/opt/couchbase/var/lib/couchbase/"
    dmpmsg = ""
    session = connection(server)

    if session is None:
        return

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
        gdbOut = run("gdb --batch " + binCb + "memcached -c " + coreFile + " -ex \"bt full\" -ex quit", session)[0]
        index = findIndexOf(gdbOut, "Core was generated by")
        gdbOut = gdbOut[index:]
        gdbOut = " ".join(gdbOut)
        return gdbOut

    print(server + " : SSH Successful")
    print(server + " : Looking for crash dump files")
    crashDir = libCb + "crash/"
    dmpFiles = run("ls -lt " + crashDir, session)[0]
    dmpFiles = [f for f in dmpFiles if ".core" not in f]
    dmpFiles = [f for f in dmpFiles if "total" not in f]
    dmpFiles = [f.split()[-1] for f in dmpFiles if ".core" not in f]
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
    logFiles = run("ls " + logsDir + "memcached.log.*", session)[0]
    for logFile in logFiles:
        criticalMessages = run("grep -r 'CRITICAL\| ERROR ' {} | grep -v {}".format(logFile.strip("\n"), exclude), session)[0]
        index = findIndexOf(criticalMessages, "Fatal error encountered during exception handling")
        criticalMessages = criticalMessages[:index]
        if (criticalMessages):
            print(server + " : Found message in " + logFile.strip("\n"))
            print("".join(criticalMessages))
            break

    session.disconnect()


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
    scan_all_slaves()
    scan_all_servers()

    if failed:
        for server in failed:
            print("ssh failed: %s" % server)
