import logging
import os
import weakref

import paramiko
import signal
import time
import uuid
from subprocess import Popen, PIPE
from time import sleep

from shell_util.common_api import CommonShellAPIs
from shell_util.remote_machine import RemoteMachineInfo, RemoteMachineProcess

log = logging.getLogger("shell_util")
log.setLevel("INFO")


class ShellConnection(CommonShellAPIs):
    connections = 0
    disconnections = 0
    __refs__ = list()

    @classmethod
    def get_instances(cls):
        for ins in cls.__refs__:
            yield ins

    @staticmethod
    def sleep(seconds, msg=""):
        if msg:
            log.info(msg)
        sleep(seconds)

    def __init__(self, test_server):
        super(ShellConnection, self).__init__()

        ShellConnection.__refs__.append(weakref.ref(self)())

        self.ip = test_server.ip
        self.port = test_server.port
        self.server = test_server
        self.remote = (self.ip != "localhost" and self.ip != "127.0.0.1")
        self.info = None
        self.log = log
        ShellConnection.connections += 1

        self._ssh_client = paramiko.SSHClient()
        self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def get_hostname(self):
        o, r = self.execute_command_raw('hostname', debug=False)
        if o:
            return o

    def get_domain(self, win_info=None):
        if win_info:
            o, _ = self.execute_batch_command('ipconfig')
            """ remove empty element """
            o = list(filter(None, o))
            suffix_dns_row = [
                row for row in o
                if row.find(" Connection-specific DNS Suffix") != -1
                and len(row.split(':')[1]) > 1]
            ret = ""
            if suffix_dns_row:
                ret = suffix_dns_row[0].split(':')[1].strip()
        else:
            ret = self.execute_command_raw('hostname -d', debug=False)
        return ret

    def get_cpu_info(self, win_info=None, mac=False):
        if win_info:
            if 'Processor(s)' not in win_info:
                win_info = self.create_windows_info()
            o = win_info['Processor(s)']
        elif mac:
            o, r = self.execute_command_raw(
                '/sbin/sysctl -n machdep.cpu.brand_string')
        else:
            o, r = self.execute_command_raw('cat /proc/cpuinfo', debug=False)
        if o:
            return o

    def get_ram_info(self, win_info=None, mac=False):
        if win_info:
            if 'Virtual Memory Max Size' not in win_info:
                win_info = self.create_windows_info()
            o = "Virtual Memory Max Size =" \
                + win_info['Virtual Memory Max Size'] + '\n' \
                + "Virtual Memory Available =" \
                + win_info['Virtual Memory Available'] + '\n' \
                + "Virtual Memory In Use =" + win_info['Virtual Memory In Use']
        elif mac:
            o, r = self.execute_command_raw(
                '/sbin/sysctl -n hw.memsize', debug=False)
        else:
            o, r = self.execute_command_raw('cat /proc/meminfo', debug=False)
        if o:
            return o

    def get_disk_info(self, win_info=None, mac=False, in_MB=False, path="/"):
        if win_info:
            if 'Total Physical Memory' not in win_info:
                win_info = self.create_windows_info()
            o = "Total Physical Memory =" + win_info['Total Physical Memory'] + '\n'
            o += "Available Physical Memory =" \
                 + win_info['Available Physical Memory']
        elif mac:
            o, r = self.execute_command_raw('df -hl', debug=False)
        else:
            if not in_MB:
                o, r = self.execute_command_raw('df -Thl', debug=False)
            else:
                o, r = self.execute_command_raw('df -lBM {}'.format(path),
                                                debug=False)
        if o:
            return o

    def connect_with_user(self, user="root"):
        self.ssh_connect_with_retries(self.ip, user, self.server.ssh_password,
                                      self.server.ssh_key)

    def ssh_connect_with_retries(self, ip, ssh_username, ssh_password, ssh_key,
                                 exit_on_failure=False, max_attempts_connect=5,
                                 backoff_time=10):
        # Retries with exponential backoff delay
        attempt = 0
        is_ssh_ok = False
        while not is_ssh_ok and attempt < max_attempts_connect:
            attempt += 1
            log.debug("SSH Connecting to {} with username:{}, attempt#{} of {}"
                      .format(ip, ssh_username, attempt, max_attempts_connect))
            try:
                if self.remote and ssh_key == '':
                    self._ssh_client.connect(
                        hostname=ip.replace('[', '').replace(']', ''),
                        username=ssh_username, password=ssh_password,
                        look_for_keys=False)
                elif self.remote:
                    self._ssh_client.connect(
                        hostname=ip.replace('[', '').replace(']', ''),
                        username=ssh_username, key_filename=ssh_key,
                        look_for_keys=False)
                is_ssh_ok = True
            except paramiko.BadHostKeyException as bhke:
                log.error("Can't establish SSH (Invalid host key) to {}: {}"
                          .format(ip, bhke))
                raise Exception(bhke)
            except Exception as e:
                log.error("Can't establish SSH (unknown reason) to {}: {}"
                          .format(ip, e, ssh_username, ssh_password))
                if attempt < max_attempts_connect:
                    log.info("Retrying with back off delay for {} secs."
                             .format(backoff_time))
                    self.sleep(backoff_time)
                    backoff_time *= 2

        if not is_ssh_ok:
            error_msg = ("-->No SSH connectivity to {} even after {} times!\n"
                         .format(self.ip, attempt))
            log.error(error_msg)
            if exit_on_failure:
                log.error("Exit on failure: killing process")
                os.kill(os.getpid(), signal.SIGKILL)
            else:
                log.error("No exit on failure, raise exception")
                raise Exception(error_msg)

    def reconnect_if_inactive(self):
        """
        If the SSH channel is inactive, retry the connection
        """
        tp = self._ssh_client.get_transport()
        if tp and not tp.active:
            log.warning("SSH connection to {} inactive, reconnecting..."
                        .format(self.ip))
            self.ssh_connect_with_retries(
                self.ip, self.server.ssh_username, self.server.ssh_password,
                self.server.ssh_key)

    def disconnect(self):
        ShellConnection.disconnections += 1
        self._ssh_client.close()

    def __find_windows_info(self):
        if self.remote:
            found = self.find_file("/cygdrive/c/tmp", "windows_info.txt")
            if isinstance(found, str):
                if self.remote:
                    sftp = self._ssh_client.open_sftp()
                    try:
                        f = sftp.open(found)
                        log.info("get windows information")
                        info = {}
                        for line in f:
                            (key, value) = line.split('=')
                            key = key.strip(' \t\n\r')
                            value = value.strip(' \t\n\r')
                            info[key] = value
                        return info
                    except IOError:
                        log.error("can not find windows info file")
                    sftp.close()
            else:
                return self.create_windows_info()
        else:
            try:
                txt = open(
                    "{0}/{1}".format("/cygdrive/c/tmp", "windows_info.txt"))
                log.info("get windows information")
                info = {}
                for line in txt.read():
                    (key, value) = line.split('=')
                    key = key.strip(' \t\n\r')
                    value = value.strip(' \t\n\r')
                    info[key] = value
                return info
            except IOError:
                log.error("can not find windows info file")

    def get_windows_system_info(self):
        try:
            info = {}
            o, _ = self.execute_batch_command('systeminfo')
            for line in o:
                line_list = line.split(':')
                if len(line_list) > 2:
                    if line_list[0] == 'Virtual Memory':
                        key = "".join(line_list[0:2])
                        value = " ".join(line_list[2:])
                    else:
                        key = line_list[0]
                        value = " ".join(line_list[1:])
                elif len(line_list) == 2:
                    (key, value) = line_list
                else:
                    continue
                key = key.strip(' \t\n\r')
                if key.find("[") != -1:
                    info[key_prev] += '|' + key + value.strip(' |')
                else:
                    value = value.strip(' |')
                    info[key] = value
                    key_prev = key
            return info
        except Exception as ex:
            log.error("error {0} appeared during getting  windows info".format(ex))

    def create_windows_info(self):
        systeminfo = self.get_windows_system_info()
        info = dict()
        info["os_name"] = "2k8"
        if "OS Name" in systeminfo:
            info["os"] = systeminfo["OS Name"].find("indows") and "windows" or "NONE"
        if systeminfo["OS Name"].find("2008 R2") != -1:
            info["os_name"] = 2008
        elif systeminfo["OS Name"].find("2016") != -1:
            info["os_name"] = 2016
        elif systeminfo["OS Name"].find("2019") != -1:
            info["os_name"] = 2019
        if "System Type" in systeminfo:
            info["os_arch"] = systeminfo["System Type"].find("64") and "x86_64" or "NONE"
        info.update(systeminfo)
        self.execute_batch_command("rm -rf  /cygdrive/c/tmp/windows_info.txt")
        self.execute_batch_command("touch  /cygdrive/c/tmp/windows_info.txt")
        sftp = self._ssh_client.open_sftp()
        try:
            f = sftp.open('/cygdrive/c/tmp/windows_info.txt', 'w')
            content = ''
            for key in sorted(info.keys()):
                content += '{0} = {1}\n'.format(key, info[key])
            f.write(content)
            log.info("/cygdrive/c/tmp/windows_info.txt was created with content: {0}".format(content))
        except IOError:
            log.error('Can not write windows_info.txt file')
        finally:
            sftp.close()
        return info

    def extract_remote_info(self):
        # initialize params
        os_distro = "linux"
        os_version = "default"
        is_linux_distro = True
        self.use_sudo = False
        is_mac = False
        self.reconnect_if_inactive()
        mac_check_cmd = "sw_vers | grep ProductVersion | awk '{ print $2 }'"
        if self.remote:
            stdin, stdout, stderro = self._ssh_client.exec_command(mac_check_cmd)
            stdin.close()
            ver, err = stdout.read(), stderro.read()
        else:
            p = Popen(mac_check_cmd, shell=True, stdout=PIPE, stderr=PIPE)
            ver, err = p.communicate()

        if not err and ver:
            os_distro = "Mac"
            try:
                ver = ver.decode()
            except AttributeError:
                pass
            os_version = ver
            is_linux_distro = True
            is_mac = True
            self.use_sudo = False
        elif self.remote:
            is_mac = False
            sftp = self._ssh_client.open_sftp()
            filenames = sftp.listdir('/etc/')
            os_distro = ''
            os_version = ''
            is_linux_distro = False
            for name in filenames:
                if name == 'os-release':
                    # /etc/os-release - likely standard across linux distros
                    filename = 'etc-os-release-{0}'.format(uuid.uuid4())
                    sftp.get(localpath=filename, remotepath='/etc/os-release')
                    file = open(filename)
                    line = file.readline()
                    is_version_id = False
                    is_pretty_name = False
                    os_pretty_name = ''
                    while line and (not is_version_id or not is_pretty_name):
                        log.debug(line)
                        if line.startswith('VERSION_ID'):
                            os_version = line.split('=')[1].replace('"', '')
                            os_version = os_version.rstrip('\n').rstrip(' ').rstrip('\\l').rstrip(
                                ' ').rstrip('\\n').rstrip(' ')
                            is_version_id = True
                        elif line.startswith('PRETTY_NAME'):
                            os_pretty_name = line.split('=')[1].replace('"', '')
                            is_pretty_name = True
                        line = file.readline()

                    os_distro_dict = {'ubuntu': 'Ubuntu', 'debian': 'Ubuntu',
                                      'mint': 'Ubuntu',
                                      'centos': 'CentOS',
                                      'openshift': 'CentOS',
                                      'amazon linux 2': 'CentOS',
                                      'amazon linux 2023': 'CentOS',
                                      'opensuse': 'openSUSE',
                                      'red': 'Red Hat',
                                      'suse': 'SUSE',
                                      'oracle': 'Oracle Linux',
                                      'almalinux': 'AlmaLinux OS',
                                      'rocky': 'Rocky Linux'}
                    os_shortname_dict = {'ubuntu': 'ubuntu', 'mint': 'ubuntu',
                                         'debian': 'debian',
                                         'centos': 'centos',
                                         'openshift': 'centos',
                                         'suse': 'suse',
                                         'opensuse': 'suse',
                                         'amazon linux 2': 'amzn2',
                                         'amazon linux 2023': 'al2023',
                                         'red': 'rhel',
                                         'oracle': 'oel',
                                         'almalinux': 'alma',
                                         'rocky': 'rocky'}
                    log.debug("os_pretty_name:" + os_pretty_name)
                    if os_pretty_name and "Amazon Linux 2" not in os_pretty_name:
                        os_name = os_pretty_name.split(' ')[0].lower()
                        os_distro = os_distro_dict[os_name]
                        if os_name != 'ubuntu':
                            os_version = os_shortname_dict[os_name] + " " + os_version.split('.')[0]
                        else:
                            os_version = os_shortname_dict[os_name] + " " + os_version
                        if os_distro:
                            is_linux_distro = True
                    log.info("os_distro: " + os_distro + ", os_version: " + os_version +
                             ", is_linux_distro: " + str(is_linux_distro))
                    file.close()
                    # now remove this file
                    os.remove(filename)
                    break
            else:
                os_distro = "linux"
                os_version = "default"
                is_linux_distro = True
                self.use_sudo = False
                is_mac = False
                filenames = []
            """ for Amazon Linux 2 only"""
            for name in filenames:
                if name == 'system-release' and os_distro == "":
                    # it's a amazon linux 2_distro . let's download this file
                    filename = 'amazon-linux2-release-{0}'.format(uuid.uuid4())
                    sftp.get(localpath=filename, remotepath='/etc/system-release')
                    file = open(filename)
                    etc_issue = ''
                    # let's only read the first line
                    for line in file:
                        # for SuSE that has blank first line
                        if line.rstrip('\n'):
                            etc_issue = line
                            break
                            # strip all extra characters
                    if etc_issue.lower().find('oracle linux') != -1:
                        os_distro = 'Oracle Linux'
                        for i in etc_issue:
                            if i.isdigit():
                                dist_version = i
                                break
                        os_version = "oel{}".format(dist_version)
                        is_linux_distro = True
                        break
                    elif etc_issue.lower().find('amazon linux 2') != -1 or \
                         etc_issue.lower().find('amazon linux release 2') != -1:
                        etc_issue = etc_issue.rstrip('\n').rstrip(' ').rstrip('\\l').rstrip(' ').rstrip('\\n').rstrip(
                            ' ')
                        os_distro = 'Amazon Linux 2'
                        os_version = etc_issue
                        is_linux_distro = True
                        file.close()
                        # now remove this file
                        os.remove(filename)
                        break
            """ for centos 7 or rhel8 """
            for name in filenames:
                if name == "redhat-release" and os_distro == "":
                    filename = 'redhat-release-{0}'.format(uuid.uuid4())
                    if self.remote:
                        sftp.get(localpath=filename, remotepath='/etc/redhat-release')
                    else:
                        p = Popen("cat /etc/redhat-release > {0}".format(filename), shell=True, stdout=PIPE, stderr=PIPE)
                        var, err = p.communicate()
                    file = open(filename)
                    redhat_release = ''
                    for line in file:
                        redhat_release = line
                        break
                    redhat_release = redhat_release.rstrip('\n').rstrip('\\l').rstrip('\\n')
                    """ in ec2: Red Hat Enterprise Linux Server release 7.2 """
                    if redhat_release.lower().find('centos') != -1 \
                         or redhat_release.lower().find('linux server') != -1 \
                         or redhat_release.lower().find('red hat') != -1:
                        if redhat_release.lower().find('release 7') != -1:
                            os_distro = 'CentOS'
                            os_version = "CentOS 7"
                            is_linux_distro = True
                        elif redhat_release.lower().find('release 8') != -1:
                            os_distro = 'CentOS'
                            os_version = "CentOS 8"
                            is_linux_distro = True
                        elif redhat_release.lower().find('red hat enterprise') != -1:
                            if "8.0" in redhat_release.lower():
                                os_distro = "Red Hat"
                                os_version = "rhel8"
                                is_linux_distro = True
                    else:
                        log.error("Could not find OS name."
                                  "It could be unsupport OS")
                    file.close()
                    os.remove(filename)
                    break

        if self.remote:
            if self.find_file("/cygdrive/c/Windows", "win.ini"):
                log.info("This is windows server!")
                is_linux_distro = False
        if not is_linux_distro:
            win_info = self.__find_windows_info()
            info = RemoteMachineInfo()
            info.type = win_info['os']
            info.windows_name = win_info['os_name']
            info.distribution_type = win_info['os']
            info.architecture_type = win_info['os_arch']
            info.ip = self.ip
            info.distribution_version = win_info['os']
            info.deliverable_type = 'msi'
            info.cpu = self.get_cpu_info(win_info)
            info.disk = self.get_disk_info(win_info)
            info.ram = self.get_ram_info(win_info)
            info.hostname = self.get_hostname()
            info.domain = self.get_domain(win_info)
            self.info = info
            return info
        else:
            # now run uname -m to get the architechtre type
            if self.remote:
                stdin, stdout, _ = self._ssh_client.exec_command('uname -m')
                stdin.close()
                os_arch = ''
                text = stdout.read().splitlines()
            else:
                p = Popen('uname -m', shell=True, stdout=PIPE, stderr=PIPE)
                text, err = p.communicate()
                os_arch = ''
            for line in text:
                try:
                    os_arch += line.decode("utf-8")
                except AttributeError:
                    os_arch += str(line)
                # at this point we should know if its a linux or windows ditro
            ext = {'Ubuntu': 'deb',
                   'CentOS': 'rpm',
                   'Red Hat': 'rpm',
                   'openSUSE': 'rpm',
                   'SUSE': 'rpm',
                   'Oracle Linux': 'rpm',
                   'Amazon Linux 2023': 'rpm',
                   'Amazon Linux 2': 'rpm',
                   'AlmaLinux OS': 'rpm',
                   'Rocky Linux': 'rpm',
                   'Mac': 'dmg',
                   'Debian': 'deb'}.get(os_distro, '')
            arch = {'i686': "x86",
                    'i386': "x86"}.get(os_arch, os_arch)

            info = RemoteMachineInfo()
            info.type = "Linux"
            info.distribution_type = os_distro
            info.architecture_type = arch
            info.ip = self.ip
            try:
                info.distribution_version = os_version.decode()
            except AttributeError:
                info.distribution_version = os_version
            info.deliverable_type = ext
            info.cpu = self.get_cpu_info(mac=is_mac)
            info.disk = self.get_disk_info(mac=is_mac)
            info.ram = self.get_ram_info(mac=is_mac)
            info.hostname = self.get_hostname()
            info.domain = self.get_domain()
            self.info = info
            log.info("%s - distribution_type: %s, distribution_version: %s"
                     % (self.server.ip, info.distribution_type,
                        info.distribution_version))
            return info

    def monitor_process(self, process_name, duration_in_seconds=120):
        # monitor this process and return if it crashes
        end_time = time.time() + float(duration_in_seconds)
        last_reported_pid = None
        while time.time() < end_time:
            process = self.is_process_running(process_name)
            if process:
                if not last_reported_pid:
                    last_reported_pid = process.pid
                elif not last_reported_pid == process.pid:
                    message = 'Process {0} restarted. PID Old: {1}, New: {2}'
                    log.info(message.format(process_name, last_reported_pid,
                                            process.pid))
                    return False
                    # check if its equal
            else:
                # we should have an option to wait for the process
                # to start during the timeout
                # process might have crashed
                log.info(
                    "{0}:process {1} is not running or it might have crashed!"
                    .format(self.ip, process_name))
                return False
            time.sleep(1)
        #            log.info('process {0} is running'.format(process_name))
        return True

    def monitor_process_memory(self, process_name, duration_in_seconds=180,
                               end=False):
        # monitor this process and return list of memories in 7 secs interval
        end_time = time.time() + float(duration_in_seconds)
        count = 0
        vsz = []
        rss = []
        while time.time() < end_time and not end:
            # get the process list
            process = self.is_process_running(process_name)
            if process:
                vsz.append(process.vsz)
                rss.append(process.rss)
            else:
                log.info("{0}:process {1} is not running.  Wait for 2 seconds"
                         .format(self.ip, process_name))
                count += 1
                self.sleep(2)
                if count == 5:
                    log.error("{0}:process {1} is not running at all."
                              .format(self.ip, process_name))
                    exit(1)
            log.info("sleep for 7 seconds before poll new processes")
            self.sleep(7)
        return vsz, rss

    def is_process_running(self, process_name):
        log.info("%s - Checking for process %s" % (self.ip, process_name))
        for process in self.get_running_processes():
            if process.name == process_name:
                log.info("%s - Process %s is running with pid %s"
                         % (self.ip, process_name, process.pid))
                return process
            elif process_name in process.args:
                log.debug("Process is running: %s" % process.args)
                return process

    def get_running_processes(self):
        # if its linux ,then parse each line
        # 26989 ?        00:00:51 pdflush
        # ps -Ao pid,comm
        processes = []
        output, error = self.execute_command('ps -Ao pid,comm,vsz,rss,args',
                                             debug=False)
        if output:
            for line in output:
                # split to words
                words = line.strip().split(' ')
                words = [_f for _f in words if _f]
                if len(words) >= 2:
                    process = RemoteMachineProcess()
                    process.pid = words[0]
                    process.name = words[1]
                    if words[2].isdigit():
                        process.vsz = int(words[2])//1024
                    else:
                        process.vsz = words[2]
                    if words[3].isdigit():
                        process.rss = int(words[3])//1024
                    else:
                        process.rss = words[3]
                    process.args = " ".join(words[4:])
                    processes.append(process)
        return processes
