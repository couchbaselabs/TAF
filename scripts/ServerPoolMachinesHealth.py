from datetime import timedelta
import paramiko

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions)
import sys


failed = []


swapiness_cmd = "echo 0 > /proc/sys/vm/swappiness;echo \"net.ipv4.conf.all.arp_notify = 1\" > /etc/sysctl.conf;echo \"#Set swappiness to 0 to avoid swapping\" >> /etc/sysctl.conf;echo vm.swappiness = 0 >> /etc/sysctl.conf"
thp_cmd = "echo never >  /sys/kernel/mm/transparent_hugepage/enabled"
disable_firewall = "systemctl stop firewalld; systemctl disable firewalld; ls -l /data"
ulimit_cmd = "ulimit -n 500000;echo \"* soft nofile 500000\" > /etc/security/limits.conf;echo \"* hard nofile 500000\" >> /etc/security/limits.conf;"

def run(command, server):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server, username="root", password="couchbase", timeout=2)
        
        stdin, stdout, stderr = ssh.exec_command(command, timeout=2)
        output = stdout.read().decode('utf-8').strip()
        error = stderr.read().decode('utf-8').strip()
        
        ssh.close()
        return output, error
        
    except Exception as e:
        failed.append(server)
        return None, str(e)


def execute(cmd="df -h | grep data"):
    auth = PasswordAuthenticator("Administrator", "esabhcuoc")
    timeout_opts = ClusterTimeoutOptions(
            kv_timeout=timedelta(seconds=10),
            analytics_timeout=timedelta(seconds=1200),
            dns_srv_timeout=timedelta(seconds=10))
    cluster_opts = {
            "authenticator": auth,
            "enable_tls": False,
            "timeout_options": timeout_opts,
        }

    cluster_opts = ClusterOptions(**cluster_opts)
    connection_string = "{0}://{1}".format("couchbase", "172.23.104.162")
    cluster = Cluster.connect(connection_string,
                                cluster_opts)

    STATEMENT = "select meta().id, os from `QE-server-pool`"
    result = cluster.query(STATEMENT);

    count = 1
    for server in result.rows():
        print("--+--+--+--+-- %s. SERVER: %s --+--+--+--+--" % (count, server.get("id")))
        count += 1
        output, error = run("cat /etc/*-release | grep -e DISTRIB_DESCRIPTION -e PRETTY_NAME", server.get("id"))
        if output:
            print("Expected OS: %s, Actual OS: %s" % (server.get("os"), output))
        if error:
            print(error)
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
    else:
        execute()
except:
    execute()

if failed:
    for server in failed:
        print("ssh failed: %s" % server)
