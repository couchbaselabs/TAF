from com.jcraft.jsch import JSchException
from com.jcraft.jsch import JSch
from org.python.core.util import FileUtil
import os
import sys

failed = []
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
        session.connect(5000)
        return session
    except:
        failed.append(server)
        return None

def scan_all_slaves():
    magma_slaves = ["172.23.123.170","172.23.120.26","172.23.120.223","172.23.120.103","172.23.120.84",
                    "172.23.120.104","172.23.123.184","172.23.120.105","172.23.120.106","172.23.120.85"]

    all_slaves = ["172.23.106.136","172.23.121.65","172.23.105.66","172.23.108.94","172.23.104.254",
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
                  "172.23.123.77","172.23.123.80","172.23.123.76","172.23.123.78","172.23.123.80",
                  "172.23.123.79","172.23.97.128","172.23.99.156","172.23.104.136","172.23.97.128",
                  "172.23.99.156","172.23.97.101","172.23.107.216","172.23.104.34"
                  ]
    count = 1
    for server in all_slaves:
        print "--+--+--+--+-- {}. CHECKING ON SLAVE: {} --+--+--+--+--".format(count, server)
        count += 1
        session = connection(server)
        if session is None:
            continue

        cmd = "find /data/workspace/ -iname '*collect*2020*.zip'"
        if len(sys.argv) > 1:
            cmd = "find /data/workspace/ -iname '*collect*{}*.zip'".format(sys.argv[1])

        output, error = run(cmd, session)
        try:
            for cbcollect_zips in output:
                log_files, error = run("zipinfo -1 {}".format(cbcollect_zips), session)
                for file in log_files:
                    if file.rstrip().endswith("dmp"):
                        print cbcollect_zips.rstrip()
                        print file.rstrip()
                        run("rm -rf /root/cbcollect*", session)[0]
                        run("unzip {}".format(cbcollect_zips), session)[0]
                        exclude = "'Rollback point not found\|No space left on device'"
                        memcached = "/root/cbcollect*/memcached.log*"
                        print "".join(run("grep CRITICAL {} | grep -v {}".format(memcached, exclude), session)[0])
                        print "#######################"
                        break
        except:
            pass
        session.disconnect()


def check_coredump_exist(server):
    binCb = "/opt/couchbase/bin/"
    libCb = "/opt/couchbase/var/lib/couchbase/"
    crashDir = "/opt/couchbase/var/lib/couchbase/"
    crashDirWin = "c://CrashDumps"
    result = False
    dmpmsg = ""
    streammsg = ""
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
        print run("cat /opt/couchbase/VERSION.txt", session)[0]
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
        exclude = "'Rollback point not found\|No space left on device'"
        criticalMessages = run("grep -r 'CRITICAL' {} | grep -v {}".format(logFile.strip("\n"), exclude), session)[0]
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
    cluster = Cluster.connect("172.23.104.162", cluster_options)
    STATEMENT = "select meta().id from `QE-server-pool` where os='centos' and '12hrreg' in poolId or 'regression' in poolId or 'magmareg' in poolId;"
    result = cluster.query(STATEMENT);

    count = 1
    for server in result.rowsAsObject():
        print "--+--+--+--+-- {}. CHECKING ON SERVER: {} --+--+--+--+--".format(count, server.get("id"))
        count += 1
        check_coredump_exist(server.get("id"))

if __name__ == "__main__":
    scan_all_slaves()
    scan_all_servers()

    if failed:
        for server in failed:
            print "ssh failed: %s" % server