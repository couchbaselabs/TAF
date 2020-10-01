from com.jcraft.jsch import JSchException
from com.jcraft.jsch import JSch
from org.python.core.util import FileUtil

servers = [
    "172.23.105.191",
    "172.23.105.192",
    "172.23.105.224",
    "172.23.105.231",
    "172.23.106.120",
    "172.23.106.122",
    "172.23.106.125",
    "172.23.106.155",
    "172.23.106.156",
    "172.23.106.159",
    "172.23.106.163",
    "172.23.106.164",
    "172.23.106.165",
    "172.23.106.168",
    "172.23.106.177",
    "172.23.106.196",
    "172.23.106.198",
    "172.23.106.199",
    "172.23.106.204",
    "172.23.106.211",
    "172.23.106.214",
    "172.23.106.215",
    "172.23.106.216",
    "172.23.106.217",
    "172.23.106.218",
    "172.23.106.219",
    "172.23.106.220",
    "172.23.106.222",
    "172.23.106.53",
    "172.23.106.56",
    "172.23.106.60",
    "172.23.106.62",
    "172.23.106.63",
    "172.23.106.69",
    "172.23.106.71",
    "172.23.106.87",
    "172.23.106.89",
    "172.23.106.92",
    "172.23.106.95",
    "172.23.106.99",
    "172.23.120.119",
    "172.23.120.120",
    "172.23.120.122",
    "172.23.120.123",
    "172.23.120.124",
    "172.23.120.125",
    "172.23.120.126",
    "172.23.120.127",
    "172.23.120.128",
    "172.23.120.129",
    "172.23.120.197",
    "172.23.120.201",
    "172.23.120.215",
    "172.23.120.222",
    "172.23.120.229",
    "172.23.120.230",
    "172.23.120.233",
    "172.23.120.237",
    "172.23.120.241",
    "172.23.120.249",
    "172.23.121.76",
    "172.23.121.79",
    "172.23.122.208",
    "172.23.122.221",
    "172.23.122.223",
    "172.23.122.224",
    "172.23.122.225",
    "172.23.122.226",
    "172.23.122.227",
    "172.23.122.228",
    "172.23.122.229",
    "172.23.122.231",
    "172.23.122.240",
    "172.23.122.241",
    "172.23.122.242",
    "172.23.122.243",
    "172.23.122.245",
    "172.23.122.246",
    "172.23.122.247",
    "172.23.122.248",
    "172.23.122.249",
    "172.23.123.10",
    "172.23.123.11",
    "172.23.123.12",
    "172.23.123.4",
    "172.23.123.5",
    "172.23.123.6",
    "172.23.123.7",
    "172.23.123.8",
    "172.23.123.9"
    ]
servers += [
    "172.23.120.61",
    "172.23.120.62",
    "172.23.120.63",
    "172.23.120.64",
    "172.23.120.65",
    "172.23.120.66",
    "172.23.120.67",
    "172.23.120.68",
    "172.23.120.69",
    "172.23.120.70"
    ]

servers += [
    "172.23.105.220",
    "172.23.105.221",
    "172.23.105.223",
    "172.23.105.225",
    "172.23.105.226",
    "172.23.105.227",
    "172.23.105.208"]

failed = []


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


def stop_firewall(nodes=None):
    global servers
    print "#######################"
    if nodes:
        servers = nodes
    for server in servers:
        print server
        output, error = run('systemctl stop firewalld; systemctl disable firewalld; ls -l /data', server)
        if output:
            print output
        if error:
            print error

def iptables(nodes=None):
    global servers
    print "#######################"
    if nodes:
        servers = nodes
    for server in servers:
        print server
        output, error = run("iptables -F", server)
        if output:
            print output
        if error:
            print error


stop_firewall()

if failed:
    for server in failed:
        print "ssh failed: %s" % server
