"""
Builds a 'couchbase-neo' docker image on a given VM from the corresponding Dockerfile
"""

try:
    import paramiko
except Exception as e:
    print("no paramiko library present")


def build_image():
    # ToDo instead of assuming the dockerfile to be present on the VM...,
    # TODo ...copy the dockerfile from slave to the VM
    print("Building an image with name couchbase-neo")
    cmd = "cd /root/cb_container/CentOS7/; docker build -t couchbase-neo ."
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    print(ssh_stdout.read(), ssh_stderr.read())


def remove_image():
    print("Removing any image with name couchbase-neo")
    cmd = "docker image rm couchbase-neo"
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    print(ssh_stdout.read(), ssh_stderr.read())


def stop_all_containers():
    print("Stopping all container")
    cmd = "docker stop $(docker ps -a -q)"
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    print(ssh_stdout.read(), ssh_stderr.read())


def remove_all_containers():
    print("Removing all containers")
    cmd = "docker rm $(docker ps -a -q)"
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    print(ssh_stdout.read(), ssh_stderr.read())


if __name__ == "__main__":
    # TODO take this from ini file
    serv = "172.23.122.100"
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(serv, username="root", password="couchbase")
    stop_all_containers()
    remove_all_containers()
    remove_image()
    build_image()
    ssh.close()
