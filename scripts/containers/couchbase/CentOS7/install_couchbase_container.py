"""
Builds a 'couchbase-neo' docker image on a given VM from the corresponding Dockerfile
"""
import paramiko
import getopt
import sys


def build_image(ssh, build):
    # ToDo instead of assuming the dockerfile to be present on the VM...,
    # TODo ...copy the dockerfile from slave to the VM
    print("Building an image with name couchbase-neo")
    cmd = "cd /root/cb_container/CentOS7/; docker build --build-arg BUILD_NO=" + build + \
          " -t couchbase-neo ."
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    print(ssh_stdout.read(), ssh_stderr.read())


def remove_image(ssh):
    print("Removing any image with name couchbase-neo")
    cmd = "docker image rm couchbase-neo"
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    print(ssh_stdout.read(), ssh_stderr.read())


def stop_all_containers(ssh):
    print("Stopping all container")
    cmd = "docker stop $(docker ps -a -q)"
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    print(ssh_stdout.read(), ssh_stderr.read())


def remove_all_containers(ssh):
    print("Removing all containers")
    cmd = "docker rm $(docker ps -a -q)"
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    print(ssh_stdout.read(), ssh_stderr.read())


def main(argv):
    test_input = dict()
    try:
        opts, args = getopt.getopt(argv, "hn:b:")
    except getopt.GetoptError:
        print('python2 scripts/containers/couchbase/CentOS7/install_couchbase_container.py '
              '-n node_ip -b neo_build_number')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('python2 scripts/containers/couchbase/CentOS7/install_couchbase_container.py '
                  '-n node_ip -b neo_build_number')
            sys.exit()
        elif opt in "-n":
            test_input["node"] = arg
        elif opt in "-b":
            test_input["build"] = arg

    serv = test_input["node"]
    build = test_input["build"]
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(serv, username="root", password="couchbase")
    stop_all_containers(ssh)
    remove_all_containers(ssh)
    remove_image(ssh)
    build_image(ssh, build)
    ssh.close()


if __name__ == "__main__":
    arg = sys.argv[1:]
    main(argv=arg)
