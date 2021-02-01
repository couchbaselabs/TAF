from com.jcraft.jsch import JSchException
from com.jcraft.jsch import JSch
from org.python.core.util import FileUtil
from java.time import Duration
from com.couchbase.client.java import Cluster, ClusterOptions
from com.couchbase.client.java.env import ClusterEnvironment
from com.couchbase.client.core.env import TimeoutConfig, IoConfig
import sys
failed = []


swapiness_cmd = "echo 0 > /proc/sys/vm/swappiness;echo \"net.ipv4.conf.all.arp_notify = 1\" > /etc/sysctl.conf;echo \"#Set swappiness to 0 to avoid swapping\" >> /etc/sysctl.conf;echo vm.swappiness = 0 >> /etc/sysctl.conf"
thp_cmd = "echo never >  /sys/kernel/mm/transparent_hugepage/enabled"
disable_firewall = "systemctl stop firewalld; systemctl disable firewalld; ls -l /data"
ulimit_cmd = "ulimit -n 500000;echo \"* soft nofile 500000\" > /etc/security/limits.conf;echo \"* hard nofile 500000\" >> /etc/security/limits.conf;"

def run(command, server):
    output = []
    error = []
    jsch = JSch()
    session = jsch.getSession("root", server, 22)
    session.setPassword("couchbase")
    session.setConfig("StrictHostKeyChecking", "no")
    try:
        session.connect(10000)
    except JSchException:
        failed.append(server)
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
        session.disconnect()
    except JSchException as e:
        print("JSch exception on %s: %s" % (server, str(e)))
    return output, error


def execute(cmd="free -m"):
    cluster_env = ClusterEnvironment.builder().ioConfig(IoConfig.numKvConnections(25)).timeoutConfig(TimeoutConfig.builder().connectTimeout(Duration.ofSeconds(20)).kvTimeout(Duration.ofSeconds(10)))
    cluster_options = ClusterOptions.clusterOptions("Administrator", "esabhcuoc").environment(cluster_env.build())
    cluster = Cluster.connect("172.23.104.162", cluster_options)
    STATEMENT = "select meta().id from `QE-server-pool` where os='centos' and '12hrreg' in poolId or 'regression' in poolId or 'magmareg' in poolId;"
    result = cluster.query(STATEMENT);

    count = 1
    for server in result.rowsAsObject():
        server = server.get("id")
        print "--+--+--+--+-- {}. SERVER: {} --+--+--+--+--".format(count, server)
        count += 1
        output, error = run(cmd, server)
        if output:
            print output
        if error:
            print error
try:
    param = sys.argv[1]
    if "thp" in param.lower():
        execute(thp_cmd)
    if "firewall" in param.lower():
        execute(disable_firewall)
    if "swapiness" in param.lower():
        execute(swapiness_cmd)
    if "ulimit" in param.lower():
        execute(ulimit_cmd)
except:
    execute()

if failed:
    for server in failed:
        print "ssh failed: %s" % server
