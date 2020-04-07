from com.jcraft.jsch import JSchException
from com.jcraft.jsch import JSch
from org.python.core.util import FileUtil

servers = [
    "172.23.122.222",
    "172.23.122.226",
    "172.23.121.79",
    "172.23.122.223",
    "172.23.121.76",
    "172.23.122.224",
    "172.23.122.225",
    "172.23.122.228",
    "172.23.122.227",
    "172.23.122.229",
    "172.23.122.231",
    "172.23.122.230",
    "172.23.122.232",
    "172.23.122.234",
    "172.23.122.233",
    "172.23.122.235",
    "172.23.122.236",
    "172.23.122.237",
    "172.23.122.238",
    "172.23.122.239",
    "172.23.122.221",
    "172.23.122.240",
    "172.23.122.242",
    "172.23.122.241",
    "172.23.122.243",
    "172.23.122.245",
    "172.23.122.247",
    "172.23.122.246",
    "172.23.122.248",
    "172.23.122.249",
    "172.23.122.250",
    "172.23.122.251",
    "172.23.122.252",
    "172.23.122.253",
    "172.23.122.254",
    "172.23.122.255",
    "172.23.123.2",
    "172.23.123.0",
    "172.23.123.3",
    "172.23.123.1",
    "172.23.122.208",
    "172.23.123.4",
    "172.23.123.5",
    "172.23.123.6",
    "172.23.123.7",
    "172.23.123.8",
    "172.23.123.9",
    "172.23.123.10",
    "172.23.123.12",
    "172.23.123.11",
    "172.23.123.13",
    "172.23.123.14",
    "172.23.123.16",
    "172.23.123.15",
    "172.23.123.17",
    "172.23.123.18",
    "172.23.123.19",
    "172.23.123.20",
    "172.23.123.21",
    "172.23.123.22"
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

servers = [
    "172.23.105.220",
    "172.23.105.221",
    "172.23.105.223",
    "172.23.105.225",
    "172.23.105.226",
    "172.23.105.227",
    "172.23.105.208"]

magma_vol = [
    "172.23.122.84",
    "172.23.122.85",
    "172.23.122.89",
    "172.23.122.184",
    "172.23.122.186",
    "172.23.122.202",
    "172.23.122.203",
    "172.23.122.204",
    "172.23.122.205",
    "172.23.122.209",
    "172.23.122.210",
    "172.23.122.211",
    "172.23.122.212",
    "172.23.122.213",
    "172.23.122.214",
    "172.23.122.215",
    "172.23.122.216",
    "172.23.122.217",
    "172.23.122.218",
    "172.23.122.219"
    ]

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


def crash_check():
    while True:
        print "#######################"
        for server in servers:
#             output, error = run("ls -l /opt/couchbase/var/lib/couchbase/crash/ | grep '.dmp' | wc -l", server)
            output, error = run("ls -l /data/kv", server)
            try:
                crashes = int(output[0].split("\n")[0])
            except:
                crashes = 0
                print error
            print server, crashes
            if crashes > 0:
                run("service couchbase-server stop", server)
                for _, server in enumerate(servers):
                    run("service couchbase-server stop", server)
                break


def iptables(nodes=None):
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


crash_check()
#iptables(magma_vol)

if failed:
    for server in failed:
        print "ssh failed: %s" % server
