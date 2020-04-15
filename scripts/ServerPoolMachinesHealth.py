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

failed = []
i = 1
for index, server in enumerate(servers):
    jsch = JSch()
    session = jsch.getSession("root", server, 22)
    session.setPassword("couchbase")
    session.setConfig("StrictHostKeyChecking", "no")
    try:
        session.connect(5000)
    except JSchException:
        failed.append(server)
        continue
    try:
        _ssh_client = session.openChannel("exec")
        _ssh_client.setInputStream(None)
        _ssh_client.setErrStream(None)

        instream = _ssh_client.getInputStream()
        errstream = _ssh_client.getErrStream()
        _ssh_client.setCommand("free -m")
        _ssh_client.connect()
        output = []
        error = []
        fu = FileUtil.wrap(instream)
        for line in fu.readlines():
            output.append(line)
        fu.close()

        fu1 = FileUtil.wrap(errstream)
        for line in fu1.readlines():
            error.append(line)
        fu1.close()
        try:
            print "{}) {}:{}".format(i, server, output[1].split()[1])
        except:
            print "{}: Not a linux machine".format(server)
        i += 1
        _ssh_client.disconnect()
        session.disconnect()
    except JSchException as e:
        print("JSch exception on %s: %s" % (server, str(e)))

if failed:
    print "ssh failed for: {}".format(failed)
