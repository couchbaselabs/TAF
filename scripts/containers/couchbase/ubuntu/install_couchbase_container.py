"""
Builds a 'couchbase' docker image on a given VM from the corresponding Dockerfile
"""
import os

import paramiko
import getopt
import sys

CONTAINER_NAME = 'db'
IMAGE_NAME = 'couchbase'

def build_image(ssh, branch, version, build):
    # ToDo instead of assuming the dockerfile to be present on the VM...,
    # TODo ...copy the dockerfile from slave to the VM
    # TODo..change the image name from 'couchbase' to something more appropriate...
    # TODo...as we may not necessarily be building a neo image here
    print("Building an image with name couchbase")
    current_dir = os.getcwd()
    print("current directory is {}".format(current_dir))
    docker_file_dir = os.path.join(current_dir, "scripts", "containers", "couchbase", "ubuntu")
    cmd = "docker build" \
          " --build-arg FLAVOR=" + branch + \
          " --build-arg VERSION=" + version + \
          " --build-arg BUILD_NO=" + build + \
          " -t couchbase {}".format(docker_file_dir)
    print("Command to be run {}".format(cmd))
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    print(ssh_stdout.read(), ssh_stderr.read())


def remove_image(ssh):
    print("Removing any image with name {}".format(IMAGE_NAME))
    cmd = "docker image rm {}".format(IMAGE_NAME)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    print(ssh_stdout.read(), ssh_stderr.read())


def stop_all_containers(ssh):
    print("Stopping all containers with name {}".format(CONTAINER_NAME))
    cmd = "docker stop $(docker ps -f name={} -a -q)".format(CONTAINER_NAME)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    print(ssh_stdout.read(), ssh_stderr.read())


def remove_all_containers(ssh):
    print("Removing all containers matching name {}".format(CONTAINER_NAME))
    cmd = "docker rm $(docker ps -f name={} -a -q)".format(CONTAINER_NAME)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    print(ssh_stdout.read(), ssh_stderr.read())


def main(argv):
    test_input = dict()
    try:
        opts, args = getopt.getopt(argv, "hn:f:v:b:")
    except getopt.GetoptError:
        print('python3.7 scripts/containers/couchbase/ubuntu/install_couchbase_container.py '
              '-n node_ip -b build_number -c container_name')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('python3.7 scripts/containers/couchbase/ubuntu/install_couchbase_container.py '
                  '-n node_ip -b build_number  -c container_name')
            sys.exit()
        elif opt in "-n":
            test_input["node"] = arg
        elif opt in "-b":
            test_input["build"] = arg
    build = test_input["build"]
    version, build_num = build.split("-")[0], build.split("-")[1]
    serv = test_input["node"]
    if "7.6" in version:
        branch = "trinity"
    else:
        branch = "neo"
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(serv, username="root", password="couchbase")
    stop_all_containers(ssh)
    remove_all_containers(ssh)
    remove_image(ssh)
    build_image(ssh, branch, version, build_num)
    ssh.close()


if __name__ == "__main__":
    arg = sys.argv[1:]
    main(argv=arg)

