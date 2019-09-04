import copy
import logging
import os
import re
import sys
import urllib
import uuid
import time
import stat
import json
import TestInput
from subprocess import Popen, PIPE

from builds.build_query import BuildQuery
from testconstants import VERSION_FILE
from testconstants import MEMBASE_VERSIONS
from testconstants import MISSING_UBUNTU_LIB
from testconstants import MV_LATESTBUILD_REPO, SHERLOCK_BUILD_REPO

from testconstants import CB_VERSION_NAME, CB_REPO, CB_RELEASE_APT_GET_REPO, \
                          CB_RELEASE_YUM_REPO
from testconstants import COUCHBASE_RELEASE_VERSIONS_3, CB_RELEASE_BUILDS
from testconstants import COUCHBASE_FROM_VERSION_3, COUCHBASE_FROM_VERSION_4,\
                          COUCHBASE_FROM_WATSON, COUCHBASE_FROM_SPOCK, \
                          COUCHBASE_VERSIONS, \
                          COUCHBASE_VERSION_2, COUCHBASE_VERSION_3

from testconstants import MAC_CB_PATH, MAC_COUCHBASE_BIN_PATH

from testconstants import LINUX_CAPI_INI, LINUX_DISTRIBUTION_NAME, \
                          LINUX_CB_PATH, LINUX_COUCHBASE_BIN_PATH, \
                          LINUX_STATIC_CONFIG, LINUX_LOG_PATH, \
                          LINUX_COUCHBASE_LOGS_PATH, LINUX_CONFIG_FILE, \
                          LINUX_MOXI_PATH

from testconstants import WIN_COUCHBASE_BIN_PATH, WIN_COUCHBASE_BIN_PATH_RAW, \
                          WIN_CB_PATH, WIN_PSSUSPEND, WIN_PROCESSES_KILLED, \
                          WIN_REGISTER_ID, WIN_TMP_PATH, WIN_TMP_PATH_RAW, \
                          WIN_UNZIP, WIN_COUCHBASE_LOGS_PATH, WIN_MB_PATH

from testconstants import RPM_DIS_NAME, SYSTEMD_SERVER
from testconstants import NR_INSTALL_LOCATION_FILE

from membase.api.rest_client import RestConnection, RestHelper
from com.jcraft.jsch import JSchException, JSchAuthCancelException, \
                            JSchPartialAuthException, SftpException

try:
    from com.jcraft.jsch import JSch
    from org.python.core.util import FileUtil
    from java.lang import System
except ImportError:
    log = logging.getLogger()
    log.warn("Warning: proceeding without importing "
             "paramiko due to import error. "
             "ssh connections to remote machines will fail!")

class RemoteMachineInfo(object):
    def __init__(self):
        self.type = ''
        self.ip = ''
        self.distribution_type = ''
        self.architecture_type = ''
        self.distribution_version = ''
        self.deliverable_type = ''
        self.ram = ''
        self.cpu = ''
        self.disk = ''
        self.hostname = ''


class RemoteMachineProcess(object):
    def __init__(self):
        self.pid = ''
        self.name = ''
        self.vsz = 0
        self.rss = 0
        self.args = ''


class RemoteMachineHelper(object):
    remote_shell = None

    def __init__(self, remote_shell):
        self.remote_shell = remote_shell
        self.infra_log = remote_shell.log

    def monitor_process(self, process_name,
                        duration_in_seconds=120):
        self.infra_log.debug("Monitor process {0} for {1} seconds"
                             .format(process_name, duration_in_seconds))
        # monitor this process and return if it crashes
        end_time = time.time() + float(duration_in_seconds)
        last_reported_pid = None
        while time.time() < end_time:
            # get the process list
            process = self.is_process_running(process_name)
            if process:
                if not last_reported_pid:
                    last_reported_pid = process.pid
                elif not last_reported_pid == process.pid:
                    message = '{0} - {1} restarted. P_ID old: {2}, new: {3}'
                    self.infra_log.error(message.format(self.remote_shell.ip,
                                                        process_name,
                                                        last_reported_pid,
                                                        process.pid))
                    return False
            else:
                # we should have an option to wait for the process
                # to start during the timeout
                # process might have crashed
                self.infra_log.error("%s - process %s not running or crashed"
                                     % (self.remote_shell.ip, process_name))
                return False
            time.sleep(1)
        return True

    def is_process_running(self, process_name):
        self.infra_log.debug("%s - Checking if %s is running or not"
                             % (self.remote_shell.ip, process_name))
        if getattr(self.remote_shell, "info", None) is None:
            self.remote_shell.info = self.remote_shell.extract_remote_info()

        if self.remote_shell.info.type.lower() == 'windows':
            output, error = self.remote_shell.execute_command(
                "tasklist| grep {0}".format(process_name), debug=False)
            if error or output == [""] or output == []:
                return None
            words = output[0].split(" ")
            words = filter(lambda x: x != "", words)
            process = RemoteMachineProcess()
            process.pid = words[1]
            process.name = words[0]
            self.infra_log.debug("%s - Processes '%s' are running"
                                 % (self.remote_shell.ip, words))
            return process
        else:
            processes = self.remote_shell.get_running_processes()
            for process in processes:
                if process.name == process_name:
                    return process
                elif process_name in process.args:
                    return process
            return None


class RemoteMachineShellConnection:
    connections = 0
    disconnections = 0

    def __init__(self, serverInfo):
        RemoteMachineShellConnection.connections += 1
        self.jsch = None
        self.session = None
        self.input = TestInput.TestInputParser.get_test_input(sys.argv)
        self.log = logging.getLogger("infra")
        self.test_log = logging.getLogger("test")

        self.ip = serverInfo.ip
        self.username = serverInfo.ssh_username
        self.password = serverInfo.ssh_password
        self.ssh_key = serverInfo.ssh_key
        self.port = serverInfo.port

        self.bin_path = LINUX_COUCHBASE_BIN_PATH
        self.cmd_ext = ""
        self.msi = False
        self.nonroot = False
        self.nr_home_path = "/home/%s/" % self.username
        self.use_sudo = True

        self.remote = (self.ip != "localhost" and self.ip != "127.0.0.1")
        if self.ip.find(":") != -1:
            self.ip = self.ip.replace('[', '').replace(']', '')

        if self.username == 'root':
            self.use_sudo = False
        elif self.username != "Administrator":
            self.use_sudo = False
            self.nonroot = True

        if self.nonroot:
            self.bin_path = self.nr_home_path + self.bin_path

        self.connect()
        self.extract_remote_info()
        if self.info.type.lower() == "windows":
            self.cmd_ext = ".exe"
            self.bin_path = WIN_COUCHBASE_BIN_PATH

    def connect(self):
        self.log.debug("Connecting to {0} with username: {1}, password: {2}"
                       .format(self.ip, self.username, self.password))
        self.jsch = JSch()
        self.session = self.jsch.getSession(self.username, self.ip, 22)
        self.session.setPassword(self.password)
        self.session.setConfig("StrictHostKeyChecking", "no")
        self.session.connect()

    def disconnect(self):
        self.log.debug("Disconnecting ssh_client for {0}".format(self.ip))
        self.session.disconnect()
        RemoteMachineShellConnection.disconnections += 1

    """
        In case of non root user, we need to switch to root to
        run command
    """
    def connect_with_user(self, user="root"):
        if self.info.distribution_type.lower() == "mac":
            self.log.debug("%s is Mac Server. Skip reconnect to it as %s"
                           % (self.ip, user))
            return
        max_attempts_connect = 2
        attempt = 0
        while True:
            try:
                self.log.debug("%s - Connect as user %s" % (self.ip, user))
                if self.remote and self.ssh_key == '':
                    self._ssh_client.connect(hostname=self.ip, username=user,
                                             password=self.password)
                break
            except JSchAuthCancelException:
                self.log.fatal("%s - Authentication for root failed" % self.ip)
                exit(1)
            except JSchPartialAuthException:
                self.log.fatal("%s - Invalid Host key" % self.ip)
                exit(1)
            except Exception as e:
                if str(e).find('PID check failed. RNG must be re-initialized') != -1 and\
                        attempt != max_attempts_connect:
                    self.log.error("{0} - Can't establish SSH session as root."
                                   "Exception {1}. Will try again in 1 sec"
                                   .format(self.ip, e))
                    attempt += 1
                    time.sleep(1)
                else:
                    self.log.fatal("{0} - Ssh connection failed: {1}"
                                   .format(self.ip, e))
                    exit(1)
        self.log.debug("%s - Connected as %s" % (self.ip, user))

    def sleep(self, timeout=1, message=""):
        self.log.info("{0}:sleep for {1} secs. {2} ..."
                      .format(self.ip, timeout, message))
        time.sleep(timeout)

    def get_running_processes(self):
        # if its linux ,then parse each line
        # 26989 ?        00:00:51 pdflush
        # ps -Ao pid,comm
        self.log.debug("%s - Getting running processes" % self.ip)
        processes = []
        output, error = self.execute_command('ps -Ao pid,comm', debug=False)
        if output:
            for line in output:
                # split to words
                words = line.strip().split(' ')
                if len(words) >= 2:
                    process = RemoteMachineProcess()
                    process.pid = words[0]
                    process.name = words[1]
                    processes.append(process)
        return processes

    def get_mem_usage_by_process(self, process_name):
        """Now only linux"""
        self.log.debug("%s - Getting memory usage by process" % self.ip)
        output, error = self.execute_command(
            "ps -e -o %mem,cmd|grep {0}".format(process_name), debug=False)
        if output:
            for line in output:
                if 'grep' not in line.strip().split(' '):
                    return float(line.strip().split(' ')[0])

    def stop_network(self, stop_time):
        """
        Stop the network for given time period and then restart the network
        on the machine.
        :param stop_time: Time duration for which the network service needs
        to be down in the machine
        :return: Nothing
        """
        self.log.debug("%s - Stopping network for %s seconds"
                       % (self.ip, stop_time))
        self.extract_remote_info()
        os_type = self.info.type.lower()
        if os_type == "unix" or os_type == "linux":
            if self.info.distribution_type.lower() == "ubuntu":
                command = "ifdown -a && sleep {} && ifup -a"
            else:
                command = "service network stop && sleep {} && " \
                          "service network start"
            output, error = self.execute_command(command.format(stop_time))
            self.log_command_output(output, error)
        elif os_type == "windows":
            command = "net stop Netman && timeout {} && net start Netman"
            output, error = self.execute_command(command.format(stop_time))
            self.log_command_output(output, error)

    def stop_membase(self):
        self.log.debug("%s - Stopping membase" % self.ip)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            self.log.debug("%s - STOP SERVER for windows" % self.ip)
            o, r = self.execute_command("net stop membaseserver")
            self.log_command_output(o, r)
            o, r = self.execute_command("net stop couchbaseserver")
            self.log_command_output(o, r)
            self.sleep(10, "Wait to stop service completely")
        if self.info.type.lower() == "linux":
            o, r = self.execute_command("/etc/init.d/membase-server stop")
            self.log_command_output(o, r)

    def start_membase(self):
        self.log.debug("%s - Starting membase" % self.ip)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("net start membaseserver")
            self.log_command_output(o, r)
        if self.info.type.lower() == "linux":
            o, r = self.execute_command("/etc/init.d/membase-server start")
            self.log_command_output(o, r)

    def get_number_of_cores(self):
        """
        Get number of cores on machine, that was invoked on.
        """
        self.log.debug("%s - Getting number of cores" % self.ip)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("cmd '/c echo %NUMBER_OF_PROCESSORS%' | sed 's/[^0-9]*//g'")
            self.log_command_output(o, r)
            return [o[0].rstrip()]
        elif self.info.distribution_type.lower() == 'mac':
            self.log.error('%s - Not implemented for Mac server' % self.ip)
        elif self.info.type.lower() == "linux":
            o, r = self.execute_command("getconf _NPROCESSORS_ONLN")
            self.log_command_output(o, r)
            return o

    def start_server(self, os="unix"):
        self.log.debug("%s - Starting couchbase server" % self.ip)
        self.extract_remote_info()
        os = self.info.type.lower()
        if not os or os == "centos":
            os = "unix"
        if os == "windows":
            o, r = self.execute_command("net start couchbaseserver")
            self.log_command_output(o, r)
        elif os == "unix" or os == "linux":
            if self.is_couchbase_installed():
                if self.nonroot:
                    self.log.debug("%s - Start couchbase-server as non root"
                                   % self.ip)
                    o, r = self.execute_command('%s%scouchbase-server \-- -noinput -detached '\
                                                % (self.nr_home_path, LINUX_COUCHBASE_BIN_PATH))
                    self.log_command_output(o, r)
                else:
                    fv, sv, bn = self.get_cbversion("linux")
                    if self.info.distribution_version.lower() in SYSTEMD_SERVER \
                            and sv in COUCHBASE_FROM_WATSON:
                        # from watson, systemd is used in centos 7, suse 12
                        self.log.info("%s - Running systemd command" % self.ip)
                        o, r = self.execute_command("systemctl start couchbase-server.service")
                        self.log_command_output(o, r)
                    else:
                        o, r = self.execute_command("/etc/init.d/couchbase-server start")
                        self.log_command_output(o, r)
        elif os == "mac":
            o, r = self.execute_command("open /Applications/Couchbase\ Server.app")
            self.log_command_output(o, r)
        else:
            self.log.error("%s - Unknown operating system or product version"
                           % self.ip)

    def stop_server(self):
        self.log.debug("%s - Stopping couchbase server" % self.ip)
        self.extract_remote_info()
        os = self.info.distribution_type.lower()
        if not os or os == "centos":
            os = "unix"
        if os == "windows":
            o, r = self.execute_command("net stop couchbaseserver")
            self.log_command_output(o, r)
        elif os == "unix" or os == "linux":
            if self.is_couchbase_installed():
                if self.nonroot:
                    o, r = self.execute_command("%s%scouchbase-server -k"
                                                % (self.nr_home_path,
                                                   LINUX_COUCHBASE_BIN_PATH))
                    self.log_command_output(o, r)
                else:
                    fv, sv, bn = self.get_cbversion("linux")
                    if self.info.distribution_version.lower() in SYSTEMD_SERVER \
                                                 and sv in COUCHBASE_FROM_WATSON:
                        # from watson, systemd is used in centos 7, suse 12
                        self.log.info("%s - Running systemd command" % self.ip)
                        o, r = self.execute_command("systemctl stop couchbase-server.service")
                        self.log_command_output(o, r)
                    else:
                        o, r = self.execute_command("/etc/init.d/couchbase-server stop",
                                                    use_channel=True)
            else:
                self.log.warning("%s - Couchbase Server not installed"
                                 % self.ip)
        elif os == "mac":
            cb_process = '/Applications/Couchbase\ Server.app/Contents/MacOS/Couchbase\ Server'
            cmd = "ps aux | grep {0} | awk '{{print $2}}' | xargs kill -9 " \
                  .format(cb_process)
            o, r = self.execute_command(cmd)
            self.log_command_output(o, r)
            o, r = self.execute_command("killall -9 epmd")
            self.log_command_output(o, r)
        else:
            self.log.error("%s - Unknown operating system or product version"
                           % self.ip)

    def restart_couchbase(self):
        """
        Restart the couchbase server on the machine.
        :return: Nothing
        """
        self.log.debug("%s - Restarting couchbase server" % self.ip)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("net stop couchbaseserver")
            self.log_command_output(o, r)
            o, r = self.execute_command("net start couchbaseserver")
            self.log_command_output(o, r)
        if self.info.type.lower() == "linux":
            fv, sv, bn = self.get_cbversion("linux")
            if "centos 7" in self.info.distribution_version.lower() \
                    and sv in COUCHBASE_FROM_WATSON:
                # from watson, systemd is used in centos 7
                self.log.debug("%s - this node is centos 7.x" % self.ip)
                o, r = self.execute_command("service couchbase-server restart")
                self.log_command_output(o, r)
            else:
                o, r = self.execute_command(
                    "/etc/init.d/couchbase-server restart")
                self.log_command_output(o, r)
        if self.info.distribution_type.lower() == "mac":
            o, r = self.execute_command(
                "open /Applications/Couchbase Server.app")
            self.log_command_output(o, r)

    def stop_schedule_tasks(self):
        self.log.debug("%s - STOP ALL SCHEDULE TASKS: installme, removeme and upgrademe"
                       % self.ip)
        output, error = self.execute_command("cmd /c schtasks /end /tn installme")
        self.log_command_output(output, error)
        output, error = self.execute_command("cmd /c schtasks /end /tn removeme")
        self.log_command_output(output, error)
        output, error = self.execute_command("cmd /c schtasks /end /tn upgrademe")
        self.log_command_output(output, error)

    def kill_erlang(self, os="unix"):
        self.log.debug("%s - Killing erlang process" % self.ip)
        if os == "windows":
            o, r = self.execute_command("taskkill /F /T /IM epmd.exe*")
            self.log_command_output(o, r)
            o, r = self.execute_command("taskkill /F /T /IM erl*")
            self.log_command_output(o, r)
            o, r = self.execute_command("tasklist | grep erl.exe")
            kill_all = False
            while len(o) >= 1 and not kill_all:
                self.execute_command("taskkill /F /T /IM erl*")
                o, r = self.execute_command("tasklist | grep erl.exe")
                if len(o) == 0:
                    kill_all = True
                    self.log.debug("%s - Erlang processes killed" % self.ip)
        else:
            o, r = self.execute_command("kill $(ps aux | grep 'beam.smp' | awk '{print $2}')")
            self.log_command_output(o, r)
        return o, r

    def kill_cbft_process(self):
        self.log.debug("%s - Killing cbft process" % self.ip)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("taskkill /F /T /IM cbft.exe*")
            self.log_command_output(o, r)
        else:
            o, r = self.execute_command("killall -9 cbft")
            self.log_command_output(o, r)
        return o, r

    def kill_java(self):
        self.log.debug("%s - Killing java process" % self.ip)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("taskkill /F /T /IM java*")
            self.log_command_output(o, r)
        else:
            self.log.debug("{0} - Java process list before killing: {1}"
                           .format(self.ip,
                                   self.execute_command("pgrep -l java")))
            o, r = self.execute_command("kill -9 $(ps aux | grep '/opt/couchbase/lib/cbas/runtime/bin/java' | awk '{print $2}')")
            self.log_command_output(o, r)
            self.log.debug("{0} - Java process list after killing: {1}"
                           .format(self.ip,
                                   self.execute_command("pgrep -l java")))
        return o, r

    def kill_process(self, process_name, service_name, signum=9):
        self.log.debug("%s - Sending %s signal to process: %s, service: %s"
                       % (self.ip, signum, process_name, service_name))
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("taskkill /F /T /IM %s*" % process_name)
            self.log_command_output(o, r)
        else:
            self.log.debug("{0} - Process info before sending signal: {1}"
                           .format(self.ip,
                                   self.execute_command("pgrep -l %s" % process_name)))
            o, r = self.execute_command("kill -%s $(pgrep %s)" % (signum, service_name))
            self.log_command_output(o, r)
            self.log.debug("{0} - Process info after sending signal: {1}"
                           .format(self.ip,
                                   self.execute_command("pgrep -l %s" % process_name)))
        return o, r

    def kill_multiple_process(self, processes, signum=9):
        self.log.debug("%s - Sending %s signal to processes: %s"
                       % (self.ip, signum, processes))
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            self.log.critical("%s - Not implemented for windows" % self.ip)
        else:
            process_list = ""
            for process in processes:
                process_list += "$(pgrep " + process + ") "
            o, r = self.execute_command("kill -%s %s" % (signum, process_list))
            self.log_command_output(o, r)
        return o, r

    def kill_memcached(self):
        return self.kill_process("memcached", "memcached", signum=9)

    def stop_memcached(self):
        return self.kill_process("memcached", "memcached", signum=19)

    def start_memcached(self):
        return self.kill_process("memcached", "memcached", signum=18)

    def kill_goxdcr(self):
        self.log.debug("%s - Killing goxdcr process" % self.ip)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("taskkill /F /T /IM goxdcr*")
            self.log_command_output(o, r)
        else:
            o, r = self.execute_command("killall -9 goxdcr")
            self.log_command_output(o, r)
        return o, r

    def change_log_level(self, new_log_level):
        self.log.debug("%s - Change LOG_LEVEL to %s"
                       % (self.ip, new_log_level))
        # ADD NON_ROOT user config_details
        output, error = self.execute_command(
            "sed -i '/loglevel_default, /c \\{loglevel_default, %s\}'. %s"
            % (new_log_level, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command(
            "sed -i '/loglevel_ns_server, /c \\{loglevel_ns_server, %s\}'. %s"
            % (new_log_level, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command(
            "sed -i '/loglevel_stats, /c \\{loglevel_stats, %s\}'. %s"
            % (new_log_level, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command(
            "sed -i '/loglevel_rebalance, /c \\{loglevel_rebalance, %s\}'. %s"
            % (new_log_level, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command(
            "sed -i '/loglevel_cluster, /c \\{loglevel_cluster, %s\}'. %s"
            % (new_log_level, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command(
            "sed -i '/loglevel_views, /c \\{loglevel_views, %s\}'. %s"
            % (new_log_level, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command(
            "sed -i '/loglevel_error_logger, /c \\{loglevel_error_logger, %s\}'. %s"
            % (new_log_level, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command(
            "sed -i '/loglevel_mapreduce_errors, /c \\{loglevel_mapreduce_errors, %s\}'. %s"
            % (new_log_level, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command(
            "sed -i '/loglevel_user, /c \\{loglevel_user, %s\}'. %s"
            % (new_log_level, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command(
            "sed -i '/loglevel_xdcr, /c \\{loglevel_xdcr, %s\}'. %s"
            % (new_log_level, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command(
            "sed -i '/loglevel_menelaus, /c \\{loglevel_menelaus, %s\}'. %s"
            % (new_log_level, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)

    def configure_log_location(self, new_log_location):
        mv_logs = LINUX_LOG_PATH + '/' + new_log_location
        error_log_tag = "error_logger_mf_dir"
        # ADD NON_ROOT user config_details
        self.log.debug("%s - CHANGE LOG LOCATION TO %s" % (self.ip, mv_logs))
        output, error = self.execute_command("rm -rf %s" % mv_logs)
        self.log_command_output(output, error)
        output, error = self.execute_command("mkdir %s" % mv_logs)
        self.log_command_output(output, error)
        output, error = self.execute_command("chown -R couchbase %s" % mv_logs)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/%s, /c \\{%s, \"%s\"\}.' %s"
                                             % (error_log_tag, error_log_tag, mv_logs,
                                                LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)

    def change_stat_periodicity(self, ticks):
        # ADD NON_ROOT user config_details
        self.log.debug("%s - CHANGE STAT PERIODICITY TO every %s seconds"
                       % (self.ip, ticks))
        output, error = self.execute_command("sed -i '$ a\{grab_stats_every_n_ticks, %s}.'  %s"
                                             % (ticks, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)

    def change_port_static(self, new_port):
        # ADD NON_ROOT user config_details
        self.log.debug("%s - CHANGE PORTS for REST: %s, MCCOUCH: %s,MEMCACHED: %s, MOXI: %s, CAPI: %s"
                       % (self.ip, new_port, new_port+1, new_port+2, new_port+3, new_port+4))
        output, error = self.execute_command("sed -i '/{rest_port/d' %s" % LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '$ a\{rest_port, %s}.' %s"
                                             % (new_port, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/{mccouch_port/d' %s" % LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '$ a\{mccouch_port, %s}.' %s"
                                             % (new_port + 1, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/{memcached_port/d' %s" % LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '$ a\{memcached_port, %s}.' %s"
                                             % (new_port + 2, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/{moxi_port/d' %s" % LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '$ a\{moxi_port, %s}.' %s"
                                             % (new_port + 3, LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/port = /c\port = %s' %s"
                                             % (new_port + 4, LINUX_CAPI_INI))
        self.log_command_output(output, error)
        output, error = self.execute_command("rm %s" % LINUX_CONFIG_FILE)
        self.log_command_output(output, error)
        output, error = self.execute_command("cat %s" % LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)

    def is_couchbase_installed(self):
        self.log.debug("%s - Checking if couchbase-server is installed"
                       % self.ip)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            if self.file_exists(WIN_CB_PATH, VERSION_FILE):
                self.log.debug("{0} - The version file {1} {2} exists"
                               .format(self.ip, WIN_CB_PATH, VERSION_FILE))
                # print running process on windows
                RemoteMachineHelper(self).is_process_running('memcached')
                RemoteMachineHelper(self).is_process_running('erl')
                return True
        elif self.info.distribution_type.lower() == 'mac':
            output, error = self.execute_command('ls %s%s'
                                                 % (MAC_CB_PATH, VERSION_FILE))
            self.log_command_output(output, error)
            for line in output:
                if line.find('No such file or directory') == -1:
                    return True
        elif self.info.type.lower() == "linux":
            if self.nonroot:
                if self.file_exists("/home/%s/" % self.username, NR_INSTALL_LOCATION_FILE):
                    output, error = self.execute_command("cat %s" % NR_INSTALL_LOCATION_FILE)
                    if output and output[0]:
                        self.log.debug("%s - Couchbase Server installed in non default path %s"
                                       % (self.ip, output[0]))
                        self.nr_home_path = output[0]
                file_path = self.nr_home_path + LINUX_CB_PATH
                if self.file_exists(file_path, VERSION_FILE):
                    self.log.debug("%s - Couchbase installed as non root"
                                   % self.ip)
                    return True
            else:
                if self.file_exists(LINUX_CB_PATH, VERSION_FILE):
                    self.log.debug("{0} - The version file {1} {2} exists"
                                   .format(self.ip, LINUX_CB_PATH, VERSION_FILE))
                    return True
        return False

    def is_moxi_installed(self):
        self.log.debug("%s - Checking if moxi installed or not" % self.ip)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            self.log.error('%s - Not implemented for Windows' % self.ip)
        elif self.info.distribution_type.lower() == 'mac':
            self.log.error('%s - Not implemented for Mac' % self.ip)
        elif self.info.type.lower() == "linux":
            if self.file_exists(LINUX_MOXI_PATH, 'moxi'):
                return True
        return False

    # /opt/moxi/bin/moxi -Z port_listen=11211 -u root -t 4 -O /var/log/moxi/moxi.log
    def start_moxi(self, ip, bucket, port, user=None, threads=4,
                   log_file="/var/log/moxi.log"):
        self.log.debug("%s - Starting moxi" % self.ip)
        if self.is_couchbase_installed():
            prod = "couchbase"
        else:
            prod = "membase"
        cli_path = "/opt/" + prod + "/bin/moxi"
        args = ""
        args += "http://{0}:8091/pools/default/bucketsStreaming/{1} " \
                .format(ip, bucket)
        args += "-Z port_listen={0} -u {1} -t {2} -O {3} -d" \
                .format(port, user or prod, threads, log_file)
        self.extract_remote_info()
        if self.info.type.lower() == "linux":
            o, r = self.execute_command("{0} {1}".format(cli_path, args))
            self.log_command_output(o, r)
        else:
            raise Exception("running standalone moxi is not supported for windows")

    def stop_moxi(self):
        self.log.debug("%s - Stopping moxi" % self.ip)
        self.extract_remote_info()
        if self.info.type.lower() == "linux":
            o, r = self.execute_command("killall -9 moxi")
            self.log_command_output(o, r)
        else:
            raise Exception("stopping standalone moxi is not supported on windows")

    def is_url_live(self, url):
        live_url = False
        self.log.debug("%s - Check if url %s is ok" % (self.ip, url))
        status = urllib.urlopen(url).getcode()
        if status == 200:
            self.log.info("%s - Url %s is live" % (self.ip, url))
            live_url = True
        else:
            mesg = "\n===============\n"\
                   "        This url {0} \n"\
                   "        is failed to connect.\n"\
                   "        Check version in params to make sure it correct pattern or build number.\n"\
                   "===============\n".format(url)
            self.stop_current_python_running(mesg)
        return live_url

    def is_ntp_installed(self):
        ntp_installed = False
        do_install = False
        os_version = ""
        self.log.info("%s - Check if ntp is installed" % self.ip)
        self.extract_remote_info()
        if self.info.type.lower() == 'linux':
            self.log.info("%s - OS version %s"
                          % (self.ip, self.info.distribution_version.lower()))
            if "centos 7" in self.info.distribution_version.lower():
                os_version = "centos 7"
                output, e = self.execute_command("systemctl status ntpd")
                for line in output:
                    if "Active: active (running)" in line:
                        ntp_installed = True
                if not ntp_installed:
                    self.log.info("%s - ntp not installed yet or not run.\n"
                                  "Let remove any old one and install ntp"
                                  % self.ip)
                    self.execute_command("yum erase -y ntp", debug=False)
                    self.execute_command("yum install -y ntp", debug=False)
                    self.execute_command("systemctl start ntpd", debug=False)
                    self.execute_command("systemctl enable ntpd", debug=False)
                    self.execute_command("firewall-cmd --add-service=ntp --permanent",
                                         debug=False)
                    self.execute_command("firewall-cmd --reload", debug=False)
                    do_install = True
                timezone, _ = self.execute_command("date")
                if "PST" not in timezone[0]:
                    self.execute_command("timedatectl set-timezone America/Los_Angeles",
                                         debug=False)
            elif "centos release 6" in self.info.distribution_version.lower():
                os_version = "centos 6"
                output, e = self.execute_command("/etc/init.d/ntpd status")
                if not output:
                    self.log.info("%s - ntp was not installed yet. "
                                  "Let install ntp on this server "
                                  % self.ip)
                    self.execute_command("yum install -y ntp ntpdate",
                                         debug=False)
                    self.execute_command("chkconfig ntpd on", debug=False)
                    self.execute_command("ntpdate pool.ntp.org", debug=False)
                    self.execute_command("/etc/init.d/ntpd start", debug=False)
                    do_install = True
                elif output and "ntpd is stopped" in output[0]:
                    self.log.info("%s - ntp is not running. Re-installing now"
                                  % self.ip)
                    self.execute_command("yum erase -y ntp", debug=False)
                    self.execute_command("yum install -y ntp ntpdate",
                                         debug=False)
                    self.execute_command("chkconfig ntpd on", debug=False)
                    self.execute_command("ntpdate pool.ntp.org", debug=False)
                    self.execute_command("/etc/init.d/ntpd start", debug=False)
                    do_install = True
                elif output and "is running..." in output[0]:
                    ntp_installed = True
                timezone, _ = self.execute_command("date")
                if "PST" not in timezone[0]:
                    self.execute_command("cp /etc/localtime /root/old.timezone",
                                         debug=False)
                    self.execute_command("rm -rf /etc/localtime", debug=False)
                    self.execute_command("ln -s /usr/share/zoneinfo/America/Los_Angeles "
                                         "/etc/localtime", debug=False)
            else:
                self.log.info("%s - Will add install in other os later, no set do install"
                              % self.ip)

        if do_install:
            if os_version == "centos 7":
                output, e = self.execute_command("systemctl status ntpd")
                for line in output:
                    if "Active: active (running)" in line:
                        self.log.info("%s - ntp is installed and running"
                                      % self.ip)
                        ntp_installed = True
                        break
            if os_version == "centos 6":
                output, e = self.execute_command("/etc/init.d/ntpd status")
                if output and " is running..." in output[0]:
                    self.log.info("%s - ntp is installed and running"
                                  % self.ip)
                    ntp_installed = True

        output, _ = self.execute_command("date", debug=False)
        self.log.info("\n%s - Date: %s" % (self.ip, output))
        if not ntp_installed and "centos" in os_version:
            mesg = "\n===============\n"\
                   "        This server {0} \n"\
                   "        failed to install ntp service.\n"\
                   "===============\n".format(self.ip)
            self.stop_current_python_running(mesg)

    def download_build(self, build):
        return self.download_binary(build.url, build.deliverable_type, build.name,
                                    latest_url=build.url_latest_build,
                                    version=build.product_version)

    def disable_firewall(self):
        self.extract_remote_info()
        if self.info.type.lower() == "windows":
            output, error = self.execute_command('netsh advfirewall set publicprofile state off')
            self.log_command_output(output, error)
            output, error = self.execute_command('netsh advfirewall set privateprofile state off')
            self.log_command_output(output, error)
            # for details see RemoteUtilHelper.enable_firewall for windows
            output, error = self.execute_command('netsh advfirewall firewall delete rule name="block erl.exe in"')
            self.log_command_output(output, error)
            output, error = self.execute_command('netsh advfirewall firewall delete rule name="block erl.exe out"')
            self.log_command_output(output, error)
        else:
            command_1 = "/sbin/iptables -F"
            command_2 = "/sbin/iptables -t nat -F"
            if self.nonroot:
                self.log.info("\n%s - Non root or non sudo has no right to disable firewall"
                              % self.ip)
                return
            output, error = self.execute_command(command_1)
            self.log_command_output(output, error)
            output, error = self.execute_command(command_2)
            self.log_command_output(output, error)
#             self.connect_with_user(user=self.username)

    def download_binary(self, url, deliverable_type, filename, latest_url=None,
                        version="", skip_md5_check=True):
        self.extract_remote_info()
        self.disable_firewall()
        file_status = False
        if self.info.type.lower() == 'windows':
            self.terminate_processes(self.info, [s for s in WIN_PROCESSES_KILLED])
            self.terminate_processes(self.info, [s + "-*" for s in COUCHBASE_FROM_VERSION_3])
            self.disable_firewall()
            remove_words = ["-rel", ".exe"]
            for word in remove_words:
                filename = filename.replace(word, "")
            """couchbase-server-enterprise_3.5.0-968-windows_amd64
               couchbase-server-enterprise_4.0.0-1655-windows_amd64
               sherlock changed from 3.5. to 4.0 """
            filename_version = ""
            if len(filename) > 40:
                if "enterprise" in filename:
                    filename_version = filename[28:33]
                elif "community" in filename:
                    filename_version = filename[27:32]
            if filename_version in COUCHBASE_FROM_VERSION_4:
                self.log.info("%s - Version is {0}" % (self.ip, filename_version))
                tmp = filename.split("_")
                version = tmp[1].replace("-windows", "")
            else:
                tmp = filename.split("-")
                tmp.reverse()
                version = tmp[1] + "-" + tmp[0]

            exist = self.file_exists('/cygdrive/c/tmp/', '{0}.exe'
                                     .format(version))
            command = "cd /cygdrive/c/tmp;cmd /c 'c:\\automation\\wget.exe " \
                      "--no-check-certificate" \
                      " -q {0} -O {1}.exe';ls -lh;".format(url, version)
            file_location = "/cygdrive/c/tmp/"
            deliverable_type = "exe"
            if not exist:
                output, error = self.execute_command(command)
                self.log_command_output(output, error)
                if not self.file_exists(file_location, '{0}.exe'
                                        .format(version)):
                    file_status = self.check_and_retry_download_binary(
                        command, file_location, version)
                return file_status
            else:
                self.log.info('%s - File %s.exe exist in tmp directory'
                              % (self.ip, version))
                return True

        elif self.info.distribution_type.lower() == 'mac':
            command = "cd ~/Downloads ; rm -rf couchbase-server* ;"\
                      " rm -rf Couchbase\ Server.app ; curl -O {0}".format(url)
            output, error = self.execute_command(command)
            self.log_command_output(output, error)
            output, error = self.execute_command('ls -lh  ~/Downloads/%s'
                                                 % filename)
            self.log_command_output(output, error)
            for line in output:
                if line.find('No such file or directory') == -1:
                    return True
            return False
        else:
        # try to push this build into
        # depending on the os
        # build.product has the full name
        # first remove the previous file if it exist ?
        # fix this :
            command1 = "cd /tmp ; D=$(mktemp -d cb_XXXX) ; mv {0} $D ; mv core.* $D ;"\
                       " rm -f * ; mv $D/* . ; rmdir $D".format(filename)
            command_root = "cd /tmp;wget -q -O {0} {1};cd /tmp;ls -lh" \
                           .format(filename, url)
            file_location = "/tmp"
            output, error = self.execute_command_raw_jsch(command1)
            self.log_command_output(output, error)
            if skip_md5_check:
                if self.nonroot:
                    output, error = self.execute_command("ls -lh ")
                    self.log_command_output(output, error)
                    self.log.info("%s - remove old couchbase server binary"
                                  % self.ip)
                    if self.file_exists("/home/%s/" % self.username,
                                        NR_INSTALL_LOCATION_FILE):
                        output, error = self.execute_command("cat %s" % NR_INSTALL_LOCATION_FILE)
                        if output and output[0]:
                            self.log.info("%s - Couchbase Server was installed in non default path %s"
                                          % (self.ip, output[0]))
                        self.nr_home_path = output[0]
                    self.execute_command_raw_jsch('cd %s;rm couchbase-server-*'
                                                  % self.nr_home_path)
                    if "nr_install_dir" in self.input.test_params and \
                            self.input.test_params["nr_install_dir"]:
                        self.nr_home_path = self.nr_home_path + self.input.test_params["nr_install_dir"]
                        op, er = self.execute_command("echo %s > %s" % (self.nr_home_path,
                                                      NR_INSTALL_LOCATION_FILE))
                        self.log_command_output(op, er)
                        op, er = self.execute_command("rm -rf %s" % self.nr_home_path)
                        self.log_command_output(op, er)
                        op, er = self.execute_command("mkdir %s" % self.nr_home_path)
                        self.log_command_output(op, er)
                    output, error = self.execute_command("ls -lh ")
                    self.log_command_output(output, error)
                    output, error = self.execute_command_raw_jsch('cd {2}; pwd;'
                                                                  ' wget -q -O {0} {1};ls -lh'
                                                                  .format(filename, url,
                                                                          self.nr_home_path))
                    self.log_command_output(output, error)
                else:
                    output, error = self.execute_command_raw_jsch(command_root)
                    self.log_command_output(output, error)
            else:
                self.log.info('%s - get md5sum for local and remote' % self.ip)
                output, error = self.execute_command_raw_jsch('cd /tmp ; rm -f *.md5 *.md5l ; wget -q -O {1}.md5 {0}.md5 ; md5sum {1} > {1}.md5l'.format(url, filename))
                self.log_command_output(output, error)
                if str(error).find('No such file or directory') != -1 and latest_url != '':
                    url = latest_url
                    output, error = self.execute_command_raw_jsch('cd /tmp ; rm -f *.md5 *.md5l ; wget -q -O {1}.md5 {0}.md5 ; md5sum {1} > {1}.md5l'.format(url, filename))
                self.log.info('%s - comparing md5sum and downloading if needed'
                              % self.ip)
                output, error = self.execute_command_raw_jsch('cd /tmp;diff {0}.md5 {0}.md5l || wget -q -O {0} {1};rm -f *.md5 *.md5l'.format(filename, url))
                self.log_command_output(output, error)
            # check if the file exists there now ?
            if self.nonroot:
                """ binary is saved at current user directory """
                return self.file_exists(self.nr_home_path, filename)
            else:
                file_status = self.file_exists(file_location, filename)
                if not file_status:
                    file_status = self.check_and_retry_download_binary(
                        command_root, file_location, version)
                return file_status
            # for linux environment we can just
            # figure out what version , check if /tmp/ has the
            # binary and then return True if binary is installed

    def get_file(self, remotepath, filename, todir="."):
        channel = self.session.openChannel("sftp")
        channel.connect()
        channelSftp = channel
        result = False
        try:
            channelSftp.cd(remotepath)
            if self.file_exists(remotepath, filename):
                channelSftp.get('{0}/{1}'.format(remotepath, filename), todir)
                channel.disconnect()
                result = True
            else:
                os.system("cp {0}/{1} {2}".format(remotepath, filename, todir))
        except:
            pass
        finally:
            channel.disconnect()

        return result

    def read_remote_file(self, remote_path, filename):
        if self.file_exists(remote_path, filename):
            if self.remote:
                out, err = self.execute_command_raw_jsch(
                    "cat %s" % os.path.join(remote_path, filename))
                return out
            else:
                txt = open('{0}/{1}'.format(remote_path, filename))
                return txt.read()
        return None

    def write_remote_file(self, remote_path, filename, lines):
        cmd = 'echo "%s" > %s/%s' % (''.join(lines), remote_path, filename)
        self.execute_command(cmd)

    def write_remote_file_single_quote(self, remote_path, filename, lines):
        cmd = 'echo \'%s\' > %s/%s' % (''.join(lines), remote_path, filename)
        self.execute_command(cmd)

    def create_whitelist(self, path, whitelist):
        if not os.path.exists(path):
            self.execute_command("mkdir %s" % path)
        filepath = os.path.join(path, "curl_whitelist.json")
        if os.path.exists(filepath):
            os.remove(filepath)
        file_data = json.dumps(whitelist)
        self.create_file(filepath, file_data)

    def remove_directory(self, remote_path):
        result = False
        if self.remote:
            channel = self.session.openChannel("sftp")
            channel.connect()
            channelSftp = channel
            try:
                channelSftp.rmdir(remote_path)
                result = True
            except:
                pass
            finally:
                channel.disconnect()
            return result
        else:
            try:
                p = Popen("rm -rf {0}".format(remote_path), shell=True,
                          stdout=PIPE, stderr=PIPE)
                stdout, stderro = p.communicate()
            except IOError:
                return False
        return True

    def rmtree(self, sftp, remote_path, level=0):
        for f in sftp.listdir_attr(remote_path):
            rpath = remote_path + "/" + f.filename
            if stat.S_ISDIR(f.st_mode):
                self.rmtree(sftp, rpath, level=(level + 1))
            else:
                rpath = remote_path + "/" + f.filename
                print('removing %s' % (rpath))
                sftp.remove(rpath)
        print('removing %s' % (remote_path))
        sftp.rmdir(remote_path)

    def remove_directory_recursive(self, remote_path):
        result = False
        if self.remote:
            sftp = self._ssh_client.open_sftp()
            try:
                self.log.info("%s - Removing {0} directory"
                              % (self.ip, remote_path))
                self.rmtree(sftp, remote_path)
                result = True
            except:
                pass
            finally:
                sftp.close()
        else:
            try:
                p = Popen("rm -rf {0}".format(remote_path), shell=True,
                          stdout=PIPE, stderr=PIPE)
                p.communicate()
            except IOError:
                result = False
        return result

    def list_files(self, remote_path):
        files = []
        if self.remote:
            channel = self.session.openChannel("sftp")
            channel.connect()
            channelSftp = channel
            try:
                channelSftp.cd(remote_path)
                filenames = channelSftp.ls(remote_path)
                for name in filenames:
                    files.append({'path': remote_path, 'file': name})
            except:
                pass
            finally:
                channel.disconnect()
            return files
        else:
            p = Popen("ls {0}".format(remote_path), shell=True,
                      stdout=PIPE, stderr=PIPE)
            files, stderro = p.communicate()
            return files

    def file_ends_with(self, remotepath, pattern):
        """
         Check if file ending with this pattern is present in remote machine
        """
        files_matched = []
        channel = self.session.openChannel("sftp")
        # errstream=channel.getErrStream()
        channel.connect()
        channelSftp = channel
        try:
            channelSftp.cd(remotepath)
            filenames = channelSftp.ls(remotepath)
            for name in filenames:
                if name.endswith(pattern):
                    files_matched.append("{0}/{1}".format(remotepath, name))
        except SftpException:
            pass
        channel.disconnect()

        if len(files_matched) > 0:
            self.log.info("%s - Found these files %s"
                          % (self.ip, files_matched))
        return files_matched

    # check if this file exists in the remote
    # machine or not
    def file_starts_with(self, remotepath, pattern):
        files_matched = []
        channel = self.session.openChannel("sftp")
        channel.connect()
        channelSftp = channel
        try:
            channelSftp.cd(remotepath)
            filenames = channelSftp.ls(remotepath)
            for name in filenames:
                if name.startswith(pattern):
                    files_matched.append("{0}/{1}".format(remotepath, name))
        except:
            pass
        finally:
            channel.disconnect()

        if len(files_matched) > 0:
            self.log.info("%s - Found these files %s"
                          % (self.ip, files_matched))
        return files_matched

    def file_exists(self, remotepath, filename, pause_time=30):
        channel = self.session.openChannel("sftp")
        channel.connect()
        channelSftp = channel
        result = False
        try:
            channelSftp.cd(remotepath)
            filenames = channelSftp.ls(remotepath)
            for name in filenames:
                if name.getFilename() == filename and int(name.getAttrs().getSize()) > 0:
                    channel.disconnect()
                    result = True
                elif name.getFilename() == filename and int(name.getAttrs().getSize()) == 0:
                    if name.getFilename() == NR_INSTALL_LOCATION_FILE:
                        continue
                    self.log.info("%s - Deleting file %s"
                                  % (self.ip, filename))
                    channel.rm(remotepath + filename)
        except:
            pass
        finally:
            channel.disconnect()
        return result

    def delete_file(self, remotepath, filename):
        channel = self.session.openChannel("sftp")
        channel.connect()
        channelSftp = channel
        delete_file = False
        try:
            filenames = channelSftp.ls(remotepath)
            for name in filenames:
                if name.filename == filename:
                    self.log.info("%s - Deleting file %s"
                                  % (self.ip, filename))
                    channelSftp.rm(remotepath + filename)
                    delete_file = True
                    break
            if delete_file:
                """ verify file is deleted """
                filenames = channelSftp.ls(remotepath)
                for name in filenames:
                    if name.filename == filename:
                        self.log.error("%s - Failed to remove %s"
                                       % (self.ip, filename))
                        delete_file = False
        except:
            pass
        finally:
            channel.disconnect()
        return delete_file

    def download_binary_in_win(self, url, version, msi_install=False):
        self.terminate_processes(self.info, [s for s in WIN_PROCESSES_KILLED])
        self.terminate_processes(self.info,
                                 [s + "-*" for s in COUCHBASE_FROM_VERSION_3])
        self.disable_firewall()
        version = version.replace("-rel", "")
        deliverable_type = "exe"
        if url and url.split(".")[-1] == "msi":
            deliverable_type = "msi"
        exist = self.file_exists('/cygdrive/c/tmp/', version)
        self.log.debug("%s - About to do the wget" % self.ip)
        command = "cd /cygdrive/c/tmp;cmd /c 'c:\\automation\\wget.exe "\
                  " --no-check-certificate"\
                  " -q {0} -O {1}.{2}';ls -lh;".format(url, version,
                                                       deliverable_type)
        file_location = "/cygdrive/c/tmp/"
        if not exist:
            output, error = self.execute_command(command)
            self.log_command_output(output, error)
        else:
            self.log.info('%s - File %s.%s exist in tmp directory'
                          % (self.ip, version, deliverable_type))
        file_status = self.file_exists('/cygdrive/c/tmp/', version)
        if not file_status:
            file_status = self.check_and_retry_download_binary(command, file_location, version)
        return file_status

    def check_and_retry_download_binary(self, command, file_location,
                                        version, time_of_try=3):
        count = 1
        file_status = self.file_exists(file_location, version, pause_time=60)
        while count <= time_of_try and not file_status:
            self.log.info("%s - Trying to download binary again %s time(s)"
                          % (self.ip, count))
            output, error = self.execute_command(command)
            self.log_command_output(output, error)
            file_status = self.file_exists(file_location, version,
                                           pause_time=60)
            count += 1
            if not file_status and count == 3:
                self.log.error("%s - Build {0} not downloaded completely"
                               % (self.ip, version))
                mesg = "stop job due to failure download build {0} at {1} " \
                       .format(version, self.ip)
                self.stop_current_python_running(mesg)
        return file_status

    def copy_file_local_to_remote(self, src_path, des_path):
        channel = self.session.openChannel("sftp")
        # errstream=channel.getErrStream()
        channel.connect()
        channelSftp = channel
        result = False
        try:
            channelSftp.put(src_path, des_path)
            result = True
        except:
            pass
        finally:
            channel.disconnect()

        return result

    def copy_file_remote_to_local(self, rem_path, des_path):
        channel = self.session.openChannel("sftp")
        channel.connect()
        channelSftp = channel
        result = False
        try:
            channelSftp.get(rem_path, des_path)
            result = True
        except:
            pass
        finally:
            channel.disconnect()
        return result

    def copy_files_local_to_remote(self, src_path, des_path):
        files = os.listdir(src_path)
        self.log.info("copy files from {0} to {1}".format(src_path, des_path))
        for tem_file in files:
            if tem_file.find("wget") != 1:
                a = ""
            full_src_path = os.path.join(src_path, tem_file)
            full_des_path = os.path.join(des_path, tem_file)
            self.copy_file_local_to_remote(full_src_path, full_des_path)

    # create a remote file from input string
    def create_file(self, remote_path, file_data):
        output, error = self.execute_command("echo '{0}' > {1}"
                                             .format(file_data, remote_path))

    def find_file(self, remote_path, file):
        try:
            files = self.execute_command_raw_jsch("ls %s" % remote_path)[0]
            if len(files) == 0:
                return

            for name in files:
                if name.rstrip('\n').rstrip(" ") == file:
                    found_it = os.path.join(remote_path, name)
                    self.log.info("File {0} was found".format(found_it))
                    return found_it
            else:
                self.log.error('File(s) name in {0}'.format(remote_path))
                for name in files:
                    self.log.info(name)
                self.log.error('Can not find {0}'.format(file))
        except IOError:
            pass

    def find_build_version(self, path_to_version, version_file, product):
        sftp = self._ssh_client.open_sftp()
        ex_type = "exe"
        if self.file_exists(WIN_CB_PATH, VERSION_FILE):
            path_to_version = WIN_CB_PATH
        else:
            path_to_version = WIN_MB_PATH
        try:
            self.log.info(path_to_version)
            f = sftp.open(os.path.join(path_to_version, VERSION_FILE), 'r+')
            tmp_str = f.read().strip()
            full_version = tmp_str.replace("-rel", "")
            if full_version == "1.6.5.4-win64":
                full_version = "1.6.5.4"
            build_name = short_version = full_version
            return build_name, short_version, full_version
        except IOError:
            self.log.error('Can not read version file')
        sftp.close()

    def find_windows_info(self):
        if self.remote:
            found = self.find_file("/cygdrive/c/tmp", "windows_info.txt")
            if isinstance(found, basestring):
                if self.remote:
                    try:
                        self.log.info("get windows information")
                        f = self.read_remote_file("/cygdrive/c/tmp", "windows_info.txt")
                        info = {}
                        for line in f:
                            (key, value) = line.split('=')
                            key = key.strip(' \t\n\r')
                            value = value.strip(' \t\n\r')
                            info[key] = value
                        return info
                    except IOError:
                        self.log.error("can not find windows info file")
            else:
                return self.create_windows_info()
        else:
            try:
                txt = open("{0}/{1}".format("/cygdrive/c/tmp", "windows_info.txt"))
                self.log.info("get windows information")
                info = {}
                for line in txt.read():
                    (key, value) = line.split('=')
                    key = key.strip(' \t\n\r')
                    value = value.strip(' \t\n\r')
                    info[key] = value
                return info
            except IOError:
                self.log.error("can not find windows info file")

    def create_windows_info(self):
            systeminfo = self.get_windows_system_info()
            info = {}
            info["os_name"] = "2k8"
            if "OS Name" in systeminfo:
                info["os"] = systeminfo["OS Name"].find("indows") and "windows" or "NONE"
            if systeminfo["OS Name"].find("2008 R2") != -1:
                info["os_name"] = 2008
            elif systeminfo["OS Name"].find("2016") != -1:
                info["os_name"] = 2016
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
                self.log.info("/cygdrive/c/tmp/windows_info.txt was created with content: {0}"
                         .format(content))
            except IOError:
                self.log.error('Can not write windows_info.txt file')
            finally:
                sftp.close()
            return info

    # Need to add new windows register ID in testconstant file when
    # new couchbase server version comes out.
    def create_windows_capture_file(self, task, product, version):
        src_path = "resources/windows/automation"
        des_path = "/cygdrive/c/automation"

        # remove dot in version (like 2.0.0 ==> 200)
        reg_version = version[0:5:2]
        reg_id = WIN_REGISTER_ID[reg_version]
        uuid_name = uuid.uuid4()

        if task == "install":
            template_file = "cb-install.wct"
            file = "{0}_{1}_install.iss".format(uuid_name, self.ip)
            # file = "{0}_install.iss".format(self.ip)
        elif task == "uninstall":
            template_file = "cb-uninstall.wct"
            file = "{0}_{1}_uninstall.iss".format(uuid_name, self.ip)
            # file = "{0}_uninstall.iss".format(self.ip)

        # create in/uninstall file from windows capture template (wct) file
        full_src_path_template = os.path.join(src_path, template_file)
        full_src_path = os.path.join(src_path, file)
        full_des_path = os.path.join(des_path, file)

        f1 = open(full_src_path_template, 'r')
        f2 = open(full_src_path, 'w')
        """ replace ####### with reg ID to install/uninstall """
        if "2.2.0-837" in version:
            reg_id = "2B630EB8-BBC7-6FE4-C9B8-D8843EB1EFFA"
        self.log.info("register ID: {0}".format(reg_id))
        for line in f1:
            line = line.replace("#######", reg_id)
            if product == "mb" and task == "install":
                line = line.replace("Couchbase", "Membase")
            f2.write(line)
        f1.close()
        f2.close()

        self.copy_file_local_to_remote(full_src_path, full_des_path)
        """ remove capture file from source after copy to destination """
        self.sleep(4, "wait for remote copy completed")
        """ need to implement to remove only at the end
            of installation """
        # os.remove(full_src_path)
        return uuid_name

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
            self.log.error("error {0} appeared during getting  windows info".format(ex))

    # this function used to modify bat file to run task schedule in windows
    def modify_bat_file(self, remote_path, file_name, name, version, task):
        found = self.find_file(remote_path, file_name)
        sftp = self._ssh_client.open_sftp()
        capture_iss_file = ""

        product_version = ""
        if version[:5] in MEMBASE_VERSIONS:
            product_version = version[:5]
            name = "mb"
        elif version[:5] in COUCHBASE_VERSIONS:
            product_version = version[:5]
            name = "cb"
        else:
            self.log.error('Windows automation does not support {0} version yet'
                      .format(version))
            sys.exit()

        if "upgrade" not in task:
            uuid_name = self.create_windows_capture_file(task, name, version)
        try:
            f = sftp.open(found, 'w')
            name = name.strip()
            version = version.strip()
            if task == "upgrade":
                content = 'c:\\tmp\{3}.exe /s -f1c:\\automation\{0}_{1}_{2}.iss' \
                          .format(name, product_version, task, version)
            else:
                content = 'c:\\tmp\{0}.exe /s -f1c:\\automation\{3}_{2}_{1}.iss' \
                          .format(version, task, self.ip, uuid_name)
            self.log.info("create {0} task with content:{1}".format(task, content))
            f.write(content)
            self.log.info('Successful write to {0}'.format(found))
            if "upgrade" not in task:
                capture_iss_file = '{0}_{1}_{2}.iss'.format(uuid_name, self.ip,
                                                            task)
        except IOError:
            self.log.error('Can not write build name file to bat file {0}'
                           .format(found))
        sftp.close()
        return capture_iss_file

    def compact_vbuckets(self, vbuckets, nodes, upto_seq, cbadmin_user="cbadminbucket",
                                                          cbadmin_password="password"):
        """
            compact each vbucket with cbcompact tools
        """
        for node in nodes:
            self.log.info("Purge delete keys in %s vbuckets.  It will take times "
                     % vbuckets)
            for vbucket in range(0, vbuckets):
                self.execute_command("%scbcompact%s %s:11210 compact %s --dropdeletes "
                                     " --purge-only-up-to-seq=%s"
                                     % (self.bin_path, self.cmd_ext, node,
                                        cbadmin_user, cbadmin_password,
                                        vbucket, upto_seq),
                                     debug=False)

    def set_vbuckets_win(self, vbuckets):
        bin_path = WIN_COUCHBASE_BIN_PATH
        bin_path = bin_path.replace("\\", "")
        src_file = bin_path + "service_register.bat"
        des_file = "/tmp/service_register.bat_{0}".format(self.ip)
        local_file = "/tmp/service_register.bat.tmp_{0}".format(self.ip)

        self.copy_file_remote_to_local(src_file, des_file)
        f1 = open(des_file, "r")
        f2 = open(local_file, "w")
        """ when install new cb server on windows, there is not
            env COUCHBASE_NUM_VBUCKETS yet.  We need to insert this
            env to service_register.bat right after  ERL_FULLSWEEP_AFTER 512
            like -env ERL_FULLSWEEP_AFTER 512 -env COUCHBASE_NUM_VBUCKETS vbuckets
            where vbucket is params passed to function when run install scripts """
        for line in f1:
            if "-env COUCHBASE_NUM_VBUCKETS " in line:
                tmp1 = line.split("COUCHBASE_NUM_VBUCKETS")
                tmp2 = tmp1[1].strip().split(" ")
                self.log.info("set vbuckets of node {0} to {1}" \
                                 .format(self.ip, vbuckets))
                tmp2[0] = vbuckets
                tmp1[1] = " ".join(tmp2)
                line = "COUCHBASE_NUM_VBUCKETS ".join(tmp1)
            elif "-env ERL_FULLSWEEP_AFTER 512" in line:
                self.log.info("set vbuckets of node {0} to {1}"
                         .format(self.ip, vbuckets))
                line = line.replace("-env ERL_FULLSWEEP_AFTER 512",
                                    "-env ERL_FULLSWEEP_AFTER 512 -env COUCHBASE_NUM_VBUCKETS {0}"
                                    .format(vbuckets))
            f2.write(line)
        f1.close()
        f2.close()
        self.copy_file_local_to_remote(local_file, src_file)

        """ re-register new setup to cb server """
        self.execute_command(WIN_COUCHBASE_BIN_PATH + "service_stop.bat")
        self.execute_command(WIN_COUCHBASE_BIN_PATH + "service_unregister.bat")
        self.execute_command(WIN_COUCHBASE_BIN_PATH + "service_register.bat")
        self.execute_command(WIN_COUCHBASE_BIN_PATH + "service_start.bat")
        self.sleep(10, "wait for cb server start completely after reset vbuckets!")

        """ remove temporary files on slave """
        os.remove(local_file)
        os.remove(des_file)

    def set_fts_query_limit_win(self, name, value):
        bin_path = WIN_COUCHBASE_BIN_PATH
        bin_path = bin_path.replace("\\", "")
        src_file = bin_path + "service_register.bat"
        des_file = "/tmp/service_register.bat_{0}".format(self.ip)
        local_file = "/tmp/service_register.bat.tmp_{0}".format(self.ip)

        self.copy_file_remote_to_local(src_file, des_file)
        f1 = open(des_file, "r")
        f2 = open(local_file, "w")
        """ when install new cb server on windows, there is not
            env CBFT_ENV_OPTIONS yet.  We need to insert this
            env to service_register.bat right after  ERL_FULLSWEEP_AFTER 512
            like -env ERL_FULLSWEEP_AFTER 512 -env CBFT_ENV_OPTIONS vbuckets
            where vbucket is params passed to function when run install scripts """
        for line in f1:
            if "-env CBFT_ENV_OPTIONS " in line:
                tmp1 = line.split("CBFT_ENV_OPTIONS")
                tmp2 = tmp1[1].strip().split(" ")
                self.log.info("set CBFT_ENV_OPTIONS of node {0} to {1}" \
                                 .format(self.ip, value))
                tmp2[0] = value
                tmp1[1] = " ".join(tmp2)
                line = "CBFT_ENV_OPTIONS ".join(tmp1)
            elif "-env ERL_FULLSWEEP_AFTER 512" in line:
                self.log.info("set CBFT_ENV_OPTIONS of node {0} to {1}" \
                                 .format(self.ip, value))
                line = line.replace("-env ERL_FULLSWEEP_AFTER 512", \
                  "-env ERL_FULLSWEEP_AFTER 512 -env {0} {1}" \
                                 .format(name, value))
            f2.write(line)
        f1.close()
        f2.close()
        self.copy_file_local_to_remote(local_file, src_file)

        """ re-register new setup to cb server """
        self.execute_command(WIN_COUCHBASE_BIN_PATH + "service_stop.bat")
        self.execute_command(WIN_COUCHBASE_BIN_PATH + "service_unregister.bat")
        self.execute_command(WIN_COUCHBASE_BIN_PATH + "service_register.bat")
        self.execute_command(WIN_COUCHBASE_BIN_PATH + "service_start.bat")
        self.sleep(10, "wait for cb server start completely after setting CBFT_ENV_OPTIONS")

        """ remove temporary files on slave """
        os.remove(local_file)
        os.remove(des_file)

    def create_directory(self, remote_path):
        channel = self.session.openChannel("sftp")
        channel.connect()
        channelSftp = channel
        result = False
        try:
            channelSftp.ls(remote_path)
            result = True
        except SftpException:
            try:
                channelSftp.mkdir(remote_path)
                result = True
            except SftpException:
                self.log.debug("Creating Directory...")
                complPath = remote_path.split("/")
                channelSftp.cd("/")
                for dir in complPath:
                    if dir:
                        try:
                            self.log.debug("Current Dir : " + channelSftp.pwd())
                            channelSftp.cd(dir)
                        except:
                            try:
                                channelSftp.mkdir(dir)
                                self.give_directory_permissions_to_couchbase(dir)
                                channelSftp.cd(dir)
                                result = True
                            except:
                                result = False
                                break
        except:
            result = False
            pass
        finally:
            channel.disconnect()
        return result

    # this function will remove the automation directory in windows
    def create_multiple_dir(self, dir_paths):
        sftp = self._ssh_client.open_sftp()
        try:
            for dir_path in dir_paths:
                if dir_path != '/cygdrive/c/tmp':
                    output = self.remove_directory('/cygdrive/c/automation')
                    if output:
                        self.log.info("{0} directory is removed.".format(dir_path))
                    else:
                        self.log.error("Can not delete {0} directory or directory {0} does not exist.".format(dir_path))
                self.create_directory(dir_path)
        except:
            pass
        finally:
            sftp.close()

    def couchbase_upgrade(self, build, save_upgrade_config=False, forcefully=False):
        # upgrade couchbase server
        self.extract_remote_info()
        self.log.info('deliverable_type : {0}'.format(self.info.deliverable_type))
        self.og.info('/tmp/{0} or /tmp/{1}'.format(build.name, build.product))
        command = ''
        if self.info.type.lower() == 'windows':
                print "build name in couchbase upgrade    ", build.product_version
                self.couchbase_upgrade_win(self.info.architecture_type,
                                           self.info.windows_name,
                                           build.product_version)
                self.log.info('********* continue upgrade process **********')

        elif self.info.deliverable_type == 'rpm':
            # run rpm -i to install
            if save_upgrade_config:
                self.couchbase_uninstall()
                install_command = 'rpm -i /tmp/{0}'.format(build.name)
                command = 'INSTALL_UPGRADE_CONFIG_DIR=/opt/couchbase/var/lib/membase/config {0}' \
                          .format(install_command)
            else:
                command = 'rpm -U /tmp/{0}'.format(build.name)
                if forcefully:
                    command = 'rpm -U --force /tmp/{0}'.format(build.name)
        elif self.info.deliverable_type == 'deb':
            if save_upgrade_config:
                self.couchbase_uninstall()
                install_command = 'dpkg -i /tmp/{0}'.format(build.name)
                command = 'INSTALL_UPGRADE_CONFIG_DIR=/opt/couchbase/var/lib/membase/config {0}' \
                          .format(install_command)
            else:
                command = 'dpkg -i /tmp/{0}'.format(build.name)
                if forcefully:
                    command = 'dpkg -i --force /tmp/{0}'.format(build.name)
        output, error = self.execute_command(command, use_channel=True)
        self.log_command_output(output, error)
        return output, error

    def couchbase_upgrade_win(self, architecture, windows_name, version):
        task = "upgrade"
        bat_file = "upgrade.bat"
        deleted = False
        self.modify_bat_file('/cygdrive/c/automation', bat_file, 'cb',
                             version, task)
        self.stop_schedule_tasks()
        self.remove_win_backup_dir()
        self.remove_win_collect_tmp()
        output, error = self.execute_command("cat '/cygdrive/c/Program Files/Couchbase/Server/VERSION.txt'")
        self.log.info("version to upgrade from: {0} to {1}".format(output, version))
        self.log.info('before running task schedule upgrademe')
        if '1.8.0' in str(output):
            """ run installer in second time as workaround for upgrade 1.8.0 only:
            #   Installer needs to update registry value in order to upgrade
            #   from the previous version.
            #   Please run installer again to continue."""
            output, error = self.execute_command("cmd /c schtasks /run /tn upgrademe")
            self.log_command_output(output, error)
            self.sleep(200, "because upgrade version is {0}".format(output))
            output, error = self.execute_command("cmd /c "
                                                 "schtasks /Query /FO LIST /TN upgrademe /V")
            self.log_command_output(output, error)
            self.stop_schedule_tasks()
        # run task schedule to upgrade Membase server
        output, error = self.execute_command("cmd /c schtasks /run /tn upgrademe")
        self.log_command_output(output, error)
        deleted = self.wait_till_file_deleted(WIN_CB_PATH, VERSION_FILE,
                                              timeout_in_seconds=600)
        if not deleted:
            self.log.error("Uninstall was failed at node {0}".format(self.ip))
            sys.exit()
        self.wait_till_file_added(WIN_CB_PATH, VERSION_FILE,
                                  timeout_in_seconds=600)
        self.log.info("installed version: {0}".format(version))
        output, error = self.execute_command("cat '/cygdrive/c/Program Files/Couchbase/Server/VERSION.txt'")
        ended = self.wait_till_process_ended(version[:10])
        if not ended:
            sys.exit("*****  Node %s failed to upgrade  *****" % (self.ip))
        self.sleep(10, "wait for server to start up completely")
        ct = time.time()
        while time.time() - ct < 10800:
            output, error = self.execute_command("cmd /c "
                                                 "schtasks /Query /FO LIST /TN upgrademe /V| findstr Status ")
            if "Ready" in str(output):
                self.log.info("upgrademe task complteted")
                break
            elif "Could not start":
                self.log.exception("Ugrade failed!!!")
                break
            else:
                self.log.info("upgrademe task still running:{0}".format(output))
                self.sleep(30)
        output, error = self.execute_command("cmd /c "
                                             "schtasks /Query /FO LIST /TN upgrademe /V")
        self.log_command_output(output, error)
        """ need to remove binary after done upgrade.  Since watson, current binary
            could not reused to uninstall or install cb server """
        self.delete_file(WIN_TMP_PATH, version[:10] + ".exe")

    """
        This method install Couchbase Server
    """
    def install_server(self, build, startserver=True, path='/tmp',
                       vbuckets=None, swappiness=10, force=False, openssl='',
                       upr=None, xdcr_upr=None, fts_query_limit=None,
                       enable_ipv6=None):

        self.log.info('*****install server ***')
        server_type = None
        success = True
        start_server_after_install = True
        track_words = ("warning", "error", "fail")
        if build.name.lower().find("membase") != -1:
            server_type = 'membase'
            abbr_product = "mb"
        elif build.name.lower().find("couchbase") != -1:
            server_type = 'couchbase'
            abbr_product = "cb"
        else:
            raise Exception("its not a membase or couchbase?")
        self.extract_remote_info()

        self.log.info('deliverable_type : {0}'.format(self.info.deliverable_type))
        if self.info.type.lower() == 'windows':
            self.log.info('***** Doing the windows install')
            self.terminate_processes(self.info,
                                     [s for s in WIN_PROCESSES_KILLED])
            self.terminate_processes(self.info,
                                     [s+"-*" for s in COUCHBASE_FROM_VERSION_3])
            # to prevent getting full disk let's delete some large files
            self.remove_win_backup_dir()
            self.remove_win_collect_tmp()
            output, error = self.execute_command("cmd /c schtasks /run /tn installme")
            success &= self.log_command_output(output, error, track_words)
            file_check = 'VERSION.txt'
            self.wait_till_file_added("/cygdrive/c/Program Files/{0}/Server/".format(server_type.title()),
                                      file_check, timeout_in_seconds=600)
            output, error = self.execute_command("cmd /c schtasks /Query /FO LIST /TN installme /V")
            self.log_command_output(output, error)
            ended = self.wait_till_process_ended(build.product_version[:10])
            if not ended:
                sys.exit("*****  Node %s failed to install  *****" % (self.ip))
            self.sleep(10, "wait for server to start up completely")
            if vbuckets and int(vbuckets) != 1024:
                self.set_vbuckets_win(vbuckets)
            if fts_query_limit:
                self.set_environment_variable(
                    name="CBFT_ENV_OPTIONS",
                    value="bleveMaxResultWindow={0}".format(int(fts_query_limit))
                )

            output, error = self.execute_command("rm -f /cygdrive/c/automation/*_{0}_install.iss"
                                                 .format(self.ip))
            self.log_command_output(output, error)
            self.delete_file(WIN_TMP_PATH, build.product_version[:10] + ".exe")
            # output, error = self.execute_command("cmd rm /cygdrive/c/tmp/{0}*.exe".format(build_name))
            # self.log_command_output(output, error)
        elif self.info.deliverable_type in ["rpm", "deb"]:
            if startserver and vbuckets is None:
                environment = ""
            else:
                environment = "INSTALL_DONT_START_SERVER=1 "
                start_server_after_install = False
            self.log.info('/tmp/{0} or /tmp/{1}'.format(build.name, build.product))

            # set default swappiness to 10 unless specify in params in all unix environment
            if self.nonroot:
                self.log.info("**** use root to run script/ssh.py to execute /sbin/sysctl vm.swappiness=0 "
                         "enable coredump cbenable_core_dumps.sh /tmp")
            else:
                output, error = self.execute_command('/sbin/sysctl vm.swappiness={0}'
                                                     .format(swappiness))
                success &= self.log_command_output(output, error, track_words,
                                                   debug=False)

            if self.info.deliverable_type == 'rpm':
                if self.nonroot:
                    op, er = self.execute_command('cd {0}; rpm2cpio {1} ' \
                                                  '|  cpio --extract --make-directories --no-absolute-filenames ' \
                                                  .format(self.nr_home_path, build.name),
                                                  debug=False)
                    self.log_command_output(op, er)
                    output, error = self.execute_command('cd {0}{1}; ./bin/install/reloc.sh `pwd` '
                                                         .format(self.nr_home_path, LINUX_CB_PATH),
                                                         debug=False)
                    self.log_command_output(output, error)
                    op, er = self.execute_command('cd {0};pwd'.format(self.nr_home_path))
                    self.log_command_output(op, er)
                    """ command to start Couchbase Server in non root
                        /home/nonroot_user/opt/couchbase/bin/couchbase-server \-- -noinput -detached
                    """
                    output, error = self.execute_command("ls -lh ")
                    self.log_command_output(output, error)
                    if start_server_after_install:
                        output, error = self.execute_command('%s%scouchbase-server '
                                                             '\-- -noinput -detached '
                                                             % (self.nr_home_path,
                                                                LINUX_COUCHBASE_BIN_PATH))
                else:
                    self.check_pkgconfig(self.info.deliverable_type, openssl)
                    if force:
                        output, error = self.execute_command('{0}rpm -Uvh --force /tmp/{1}'\
                                                             .format(environment, build.name),
                                                             debug=False)
                    else:
                        output, error = self.execute_command('{0}rpm -i /tmp/{1}'\
                                                             .format(environment, build.name),
                                                             debug=False)
            elif self.info.deliverable_type == 'deb':
                if self.nonroot:
                    op, er = self.execute_command('cd %s; dpkg-deb -x %s %s '
                                                  % (self.nr_home_path, build.name,
                                                     self.nr_home_path))
                    self.log_command_output(op, er)
                    output, error = self.execute_command('cd {0}{1}; ./bin/install/reloc.sh `pwd`'\
                                                         .format(self.nr_home_path, LINUX_CB_PATH))
                    self.log_command_output(output, error)
                    op, er = self.execute_command('pwd')
                    self.log_command_output(op, er)
                    """ command to start Couchbase Server in non root in ubuntu the same
                        as in centos above
                    """
                    if start_server_after_install:
                        output, error = self.execute_command('%s%scouchbase-server '
                                                             '\-- -noinput -detached '
                                                              % (self.nr_home_path,
                                                                 LINUX_COUCHBASE_BIN_PATH))
                else:
                    self.install_missing_lib()
                    if force:
                        output, error = self.execute_command('{0}dpkg --force-all -i /tmp/{1}'\
                                                             .format(environment, build.name),
                                                             debug=False)
                    else:
                        output, error = self.execute_command('{0}dpkg -i /tmp/{1}'\
                                                             .format(environment, build.name),
                                                             debug=False)

            if "SUSE" in self.info.distribution_type:
                if error and error[0] == 'insserv: Service network is missed in the runlevels 2'\
                                         ' 4 to use service couchbase-server':
                    self.log.info("Ignore this error for opensuse os")
                    error = []
            if output:
                server_ip = "\n\n**** Installing on server: {0} ****" \
                            .format(self.ip)
                output.insert(0, server_ip)
            if error:
                server_ip = "\n\n**** Installing on server: {0} ****" \
                            .format(self.ip)
                error.insert(0, server_ip)
            success &= self.log_command_output(output, error, track_words,
                                               debug=False)
            nonroot_path_start = ""
            if not self.nonroot:
                nonroot_path_start = "/"
                self.create_directory(path)
                output, error = self.execute_command('/opt/{0}/bin/{1}enable_core_dumps.sh  {2}'
                                                     .format(server_type, abbr_product, path),
                                                     debug=False)
                success &= self.log_command_output(output, error, track_words,
                                                   debug=False)

            if vbuckets:
                """
                   From spock, the file to edit is in /opt/couchbase/bin/couchbase-server
                """
                output, error = self.execute_command("sed -i 's/export ERL_FULLSWEEP_AFTER/"
                                                     "export ERL_FULLSWEEP_AFTER\\n"
                                                     "{0}_NUM_VBUCKETS={1}\\n"
                                                     "export {0}_NUM_VBUCKETS/' "
                                                     "{3}opt/{2}/bin/{2}-server".
                                                     format(server_type.upper(), vbuckets,
                                                            server_type, nonroot_path_start),
                                                     debug=False)
                success &= self.log_command_output(output, error, track_words)
            if upr is not None:
                protocol = "tap"
                if upr:
                    protocol = "upr"
                output, error = \
                    self.execute_command("sed -i 's/END INIT INFO/END INIT INFO\\nexport"
                                         " COUCHBASE_REPL_TYPE={1}/' {2}opt/{0}/etc/{0}_init.d"\
                                         .format(server_type, protocol,
                                                 nonroot_path_start))
                success &= self.log_command_output(output, error, track_words)
            if xdcr_upr is False:
                output, error = \
                    self.execute_command("sed -i 's/ulimit -c unlimited/ulimit "
                                         "-c unlimited\\n    export XDCR_USE_OLD_PATH={1}/'"
                                         "{2}opt/{0}/etc/{0}_init.d"
                                         .format(server_type, "true",
                                                 nonroot_path_start))
                success &= self.log_command_output(output, error, track_words)
            if fts_query_limit:
                output, error = \
                    self.execute_command("sed -i 's/export PATH/export PATH\\n"
                                         "export CBFT_ENV_OPTIONS=bleveMaxResultWindow={1},hideUI=false/'\
                                          {2}opt/{0}/bin/{0}-server"
                                         .format(server_type,
                                                 int(fts_query_limit),
                                                 nonroot_path_start))
                success &= self.log_command_output(output, error, track_words)
                startserver = True

            if enable_ipv6:
                output, error = \
                    self.execute_command("sed -i '/ipv6, /c \\{ipv6, true\}'. %s"
                                         % LINUX_STATIC_CONFIG)
                success &= self.log_command_output(output, error, track_words)
                startserver = True

            # skip output: [WARNING] couchbase-server is already started
            # dirname error skipping for CentOS-6.6 (MB-12536)
            track_words = ("error", "fail", "dirname")
            if startserver:
                self.start_couchbase()
                if (build.product_version.startswith("1.")
                    or build.product_version.startswith("2.0.0")) \
                        and build.deliverable_type == "deb":
                    # skip error '* Failed to start couchbase-server' for 1.* & 2.0.0 builds(MB-7288)
                    # fix in 2.0.1 branch Change-Id: I850ad9424e295bbbb79ede701495b018b5dfbd51
                    self.log.warn("Error '* Failed to start couchbase-server' "
                             "for 1.* builds will be skipped")
                    self.log_command_output(output, error, track_words)
                else:
                    success &= self.log_command_output(
                        output, error, track_words, debug=False)
        elif self.info.deliverable_type in ["zip"]:
            """ close Safari browser before install """
            self.terminate_process(self.info, "/Applications/Safari.app/Contents/MacOS/Safari")
            o, r = self.execute_command("ps aux | grep Archive | awk '{print $2}' | xargs kill -9")
            self.sleep(20)
            output, error = self.execute_command("cd ~/Downloads ; open couchbase-server*.zip")
            self.log_command_output(output, error)
            self.sleep(20)
            cmd1 = "mv ~/Downloads/couchbase-server*/Couchbase\ Server.app /Applications/"
            cmd2 = "sudo xattr -d -r com.apple.quarantine /Applications/Couchbase\ Server.app"
            cmd3 = "open /Applications/Couchbase\ Server.app"
            output, error = self.execute_command(cmd1)
            self.log_command_output(output, error)
            output, error = self.execute_command(cmd2)
            self.log_command_output(output, error)
            output, error = self.execute_command(cmd3)
            self.log_command_output(output, error)

        output, error = self.execute_command("rm -f *-diag.zip", debug=False)
        self.log_command_output(output, error, track_words, debug=False)
        return success

    def install_server_win(self, build, version, startserver=True,
                           vbuckets=None, fts_query_limit=None,
                           enable_ipv6=None, windows_msi=False):

        self.log.info('******start install_server_win ********')
        if windows_msi:
            if enable_ipv6:
                output, error = self.execute_command("cd /cygdrive/c/tmp;"
                                                     "msiexec /i {0}.msi USE_IPV6=true /qn "
                                                     .format(version))
            else:
                output, error = self.execute_command("cd /cygdrive/c/tmp;"
                                                     "msiexec /i {0}.msi /qn "
                                                     .format(version))
            self.log_command_output(output, error)
            if fts_query_limit:
                self.set_fts_query_limit_win(
                    name="CBFT_ENV_OPTIONS",
                    value="bleveMaxResultWindow={0}".format(int(fts_query_limit))
                )
            return len(error) == 0
        remote_path = None
        success = True
        track_words = ("warning", "error", "fail")
        if build.name.lower().find("membase") != -1:
            remote_path = WIN_MB_PATH
            abbr_product = "mb"
        elif build.name.lower().find("couchbase") != -1:
            remote_path = WIN_CB_PATH
            abbr_product = "cb"

        if remote_path is None:
            raise Exception("its not a membase or couchbase?")
        self.extract_remote_info()
        self.log.info('deliverable_type : {0}'.format(self.info.deliverable_type))
        if self.info.type.lower() == 'windows':
            task = "install"
            bat_file = "install.bat"
            dir_paths = ['/cygdrive/c/automation', '/cygdrive/c/tmp']
            capture_iss_file = ""
            # build = self.build_url(params)
            self.create_multiple_dir(dir_paths)
            self.copy_files_local_to_remote('resources/windows/automation',
                                            '/cygdrive/c/automation')
            # self.create_windows_capture_file(task, abbr_product, version)
            capture_iss_file = self.modify_bat_file('/cygdrive/c/automation',
                                                    bat_file, abbr_product,
                                                    version, task)
            self.stop_schedule_tasks()
            self.remove_win_backup_dir()
            self.remove_win_collect_tmp()
            self.log.info('sleep for 5 seconds before running task '
                     'schedule uninstall on {0}'.format(self.ip))

            """ the code below need to remove when bug MB-11985
                is fixed in 3.0.1 """
            if task == "install" and (version[:5] in COUCHBASE_VERSION_2 or
                                      version[:5] in COUCHBASE_FROM_VERSION_3):
                self.log.info("due to MB-11985, we need to delete below registry "
                         "before install version 2.x.x and 3.x.x")
                output, error = self.execute_command("reg delete \
                    'HKLM\Software\Wow6432Node\Ericsson\Erlang\ErlSrv' /f ")
                self.log_command_output(output, error)
            """ end remove code """

            """ run task schedule to install cb server """
            output, error = self.execute_command("cmd /c schtasks /run /tn installme")
            success &= self.log_command_output(output, error, track_words)
            file_check = 'VERSION.txt'
            self.wait_till_file_added(remote_path, file_check,
                                      timeout_in_seconds=600)
            if version[:3] != "2.5":
                ended = self.wait_till_process_ended(build.product_version[:10])
                if not ended:
                    sys.exit("*****  Node %s failed to install  *****"
                             % self.ip)
            if version[:3] == "2.5":
                self.sleep(20, "wait for server to start up completely")
            else:
                self.sleep(10, "wait for server to start up completely")
            output, error = self.execute_command("rm -f *-diag.zip")
            self.log_command_output(output, error, track_words)
            if vbuckets and int(vbuckets) != 1024:
                self.set_vbuckets_win(vbuckets)
            if fts_query_limit:
                self.set_fts_query_limit_win(
                    name="CBFT_ENV_OPTIONS",
                    value="bleveMaxResultWindow={0}".format(int(fts_query_limit))
                )

            if "4.0" in version[:5]:
                """  remove folder if it exists in work around of bub MB-13046 """
                self.execute_command("rm -rf \
                                     /cygdrive/c/Jenkins/workspace/sherlock-windows/couchbase/install/etc/security")
                """ end remove code for bug MB-13046 """
            if capture_iss_file:
                self.log.info("****Delete {0} in windows automation directory"
                             .format(capture_iss_file))
                output, error = self.execute_command("rm -f /cygdrive/c/automation/{0}"
                                                     .format(capture_iss_file))
                self.log_command_output(output, error)
                self.log.info("Delete {0} in slave resources/windows/automation dir"
                             .format(capture_iss_file))
                os.system("rm -f resources/windows/automation/{0}"
                          .format(capture_iss_file))
            self.delete_file(WIN_TMP_PATH, build.product_version[:10] + ".exe")
            self.log.info('***** done install_server_win *****')
            return success

    def install_server_via_repo(self, deliverable_type, cb_edition,
                                remote_client):
        success = True
        track_words = ("warning", "error", "fail")
        if cb_edition:
            cb_edition = "-" + cb_edition
        if deliverable_type == "deb":
            self.update_couchbase_release(remote_client, deliverable_type)
            output, error = self.execute_command("yes \
                   | apt-get install couchbase-server{0}".format(cb_edition))
            self.log_command_output(output, error)
            success &= self.log_command_output(output, error, track_words)
        elif deliverable_type == "rpm":
            self.update_couchbase_release(remote_client, deliverable_type)
            output, error = self.execute_command("yes \
                  | yum install couchbase-server{0}".format(cb_edition))
            self.log_command_output(output, error, track_words)
            success &= self.log_command_output(output, error, track_words)
        return success

    def update_couchbase_release(self, remote_client, deliverable_type):
        if deliverable_type == "deb":
            """ remove old couchbase-release package """
            self.log.info("remove couchbase-release at node {0}".format(self.ip))
            output, error = self.execute_command("dpkg --get-selections |\
                                                              grep couchbase")
            self.log_command_output(output, error)
            for str in output:
                if "couchbase-release" in str:
                    output, error = self.execute_command("apt-get \
                                                   purge -y couchbase-release")
            output, error = self.execute_command("dpkg --get-selections |\
                                                              grep couchbase")
            self.log_command_output(output, error)
            package_remove = True
            for str in output:
                if "couchbase-release" in str:
                    package_remove = False
                    self.log.info("couchbase-release is not removed at node {0}"
                             .format(self.ip))
                    sys.exit("** Node %s failed to remove couchbase-release **"
                             % (self.ip))
            """ install new couchbase-release package """
            self.log.info("install new couchbase-release repo at node {0}"
                     .format(self.ip))
            self.execute_command("rm -rf /tmp/couchbase-release*")
            self.execute_command("cd /tmp; wget {0}".format(CB_RELEASE_APT_GET_REPO))
            output, error = self.execute_command("yes | dpkg -i /tmp/couchbase-release*")
            self.log_command_output(output, error)
            output, error = self.execute_command("dpkg --get-selections |\
                                                              grep couchbase")
            package_updated = False
            for str in output:
                if "couchbase-release" in str:
                    package_updated = True
                    self.log.info("couchbase-release installed on node {0}"
                             .format(self.ip))
                    return package_updated
            if not package_updated:
                sys.exit("fail to install %s on node %s"
                         % (CB_RELEASE_APT_GET_REPO.rsplit("/", 1)[-1], self.ip))
        elif deliverable_type == "rpm":
            """ remove old couchbase-release package """
            self.log.info("remove couchbase-release at node {0}".format(self.ip))
            output, error = self.execute_command("rpm -qa | grep couchbase")
            self.log_command_output(output, error)
            for str in output:
                if "couchbase-release" in str:
                    output, error = self.execute_command("rpm -e couchbase-release")
            output, error = self.execute_command("rpm -qa | grep couchbase")
            self.log_command_output(output, error)
            package_remove = True
            for str in output:
                if "couchbase-release" in str:
                    package_remove = False
                    self.log.info("couchbase-release is not removed at node {0}"
                             .format(self.ip))
                    sys.exit("** Node %s failed to remove couchbase-release **"
                             % (self.ip))
            """ install new couchbase-release package """
            self.log.info("install new couchbase-release repo at node {0}"
                     .format(self.ip))
            self.execute_command("rm -rf /tmp/couchbase-release*")
            self.execute_command("cd /tmp; wget {0}".format(CB_RELEASE_YUM_REPO))
            output, error = self.execute_command("yes | rpm -i /tmp/couchbase-release*")
            self.log_command_output(output, error)
            output, error = self.execute_command("rpm -qa | grep couchbase")
            package_updated = False
            for str in output:
                if "couchbase-release" in str:
                    package_updated = True
                    self.log.info("couchbase-release installed on node {0}"
                             .format(self.ip))
                    return package_updated
            if not package_updated:
                sys.exit("fail to install %s on node %s"
                         % (CB_RELEASE_YUM_REPO.rsplit("/", 1)[-1], self.ip))

    def install_moxi(self, build):
        success = True
        track_words = ("warning", "error", "fail")
        self.extract_remote_info()
        self.log.info('deliverable_type : {0}'.format(self.info.deliverable_type))
        if self.info.type.lower() == 'windows':
            self.log.error('Not implemented')
        elif self.info.deliverable_type in ["rpm"]:
            output, error = self.execute_command('rpm -i /tmp/{0}'
                                                 .format(build.name))
            if error and ' '.join(error).find("ERROR") != -1:
                success = False
        elif self.info.deliverable_type == 'deb':
            output, error = self.execute_command('dpkg -i /tmp/{0}'
                                                 .format(build.name))
            if error and ' '.join(error).find("ERROR") != -1:
                success = False
        success &= self.log_command_output(output, '', track_words)
        return success

    def wait_till_file_deleted(self, remotepath, filename,
                               timeout_in_seconds=180):
        end_time = time.time() + float(timeout_in_seconds)
        deleted = False
        self.log.info("file {0} checked at {1}".format(filename, remotepath))
        while time.time() < end_time and not deleted:
            # get the process list
            exists = self.file_exists(remotepath, filename)
            if exists:
                self.log.error('at {2} file {1} still exists'
                          .format(remotepath, filename, self.ip))
                time.sleep(10)
            else:
                self.log.info('at {2} FILE {1} DOES NOT EXIST ANYMORE!'
                         .format(remotepath, filename, self.ip))
                deleted = True
        return deleted

    def wait_till_file_added(self, remotepath, filename,
                             timeout_in_seconds=180):
        end_time = time.time() + float(timeout_in_seconds)
        added = False
        self.log.info("file {0} checked at {1}".format(filename, remotepath))
        while time.time() < end_time and not added:
            # get the process list
            exists = self.file_exists(remotepath, filename)
            if not exists:
                self.log.error('at {2} file {1} does not exist'
                          .format(remotepath, filename, self.ip))
                time.sleep(10)
            else:
                self.log.info('at {2} FILE {1} EXISTS!'
                         .format(remotepath, filename, self.ip))
                added = True
        return added

    def wait_till_compaction_end(self, rest, bucket, timeout_in_seconds=60):
        end_time = time.time() + float(timeout_in_seconds)

        while time.time() < end_time:
            status, progress = rest.check_compaction_status(bucket)
            if status:
                self.log.info("Compaction progress is %s" % progress)
                time.sleep(1)
            else:
                # the compaction task has completed
                return True

        self.log.error("Auto compaction has not ended in {0} sec."
                       .format(str(timeout_in_seconds)))
        return False

    def wait_till_process_ended(self, process_name, timeout_in_seconds=600):
        if process_name[-1:] == "-":
            process_name = process_name[:-1]
        end_time = time.time() + float(timeout_in_seconds)
        process_ended = False
        process_running = False
        count_process_not_run = 0
        while time.time() < end_time and not process_ended:
            output, error = self.execute_command("tasklist | grep {0}"
                                                 .format(process_name))
            self.log_command_output(output, error)
            if output and process_name in output[0]:
                self.sleep(8, "wait for process ended!")
                process_running = True
            else:
                if process_running:
                    self.log.info("{1}: Alright, PROCESS {0} ENDED!"
                             .format(process_name, self.ip))
                    process_ended = True
                else:
                    if count_process_not_run < 5:
                        self.log.error("{1}: process {0} may not run"
                                  .format(process_name, self.ip))
                        self.sleep(5)
                        count_process_not_run += 1
                    else:
                        self.log.error("{1}: process {0} did not run after 25 secs"
                                  .format(process_name, self.ip))
                        mesg = "kill in/uninstall job due to process was not run" \
                               .format(process_name, self.ip)
                        self.stop_current_python_running(mesg)
        if time.time() >= end_time and not process_ended:
            self.log.info("Process {0} on node {1} is still running"
                     " after 10 minutes VERSION.txt file was removed"
                     .format(process_name, self.ip))
        return process_ended

    def terminate_processes(self, info, list):
        for process in list:
            type = info.distribution_type.lower()
            if type == "windows":
                # set debug=False if does not want to show log
                self.execute_command("taskkill /F /T /IM {0}".format(process),
                                     debug=False)
            elif type in LINUX_DISTRIBUTION_NAME:
                self.terminate_process(info, process, force=True)

    def remove_folders(self, list):
        for folder in list:
            output, error = self.execute_command("rm -rf {0}".format(folder),
                                                 debug=False)
            self.log_command_output(output, error)

    def couchbase_uninstall(self, windows_msi=False, product=None):
        self.log.info('{0} *****In couchbase uninstall****'.format(self.ip))
        linux_folders = ["/var/opt/membase", "/opt/membase",
                         "/etc/opt/membase", "/var/membase/data/*",
                         "/opt/membase/var/lib/membase/*",
                         "/opt/couchbase", "/data/*"]
        terminate_process_list = ["beam.smp", "memcached", "moxi",
                                  "vbucketmigrator", "couchdb", "epmd",
                                  "memsup", "cpu_sup", "goxdcr",
                                  "erlang", "eventing"]
        self.extract_remote_info()
        self.log.info(self.info.distribution_type)
        type = self.info.distribution_type.lower()
        fv, sv, bn = self.get_cbversion(type)
        if type == 'windows':
            product = "cb"
            query = BuildQuery()
            os_type = "exe"
            if windows_msi:
                os_type = "msi"
                self.info.deliverable_type = "msi"
            task = "uninstall"
            bat_file = "uninstall.bat"
            product_name = "couchbase-server-enterprise"
            version_path = "/cygdrive/c/Program Files/Couchbase/Server/"
            deleted = False
            capture_iss_file = ""
            self.log.info("kill any in/uninstall process from version 3 in node %s"
                     % self.ip)
            self.terminate_processes(self.info,
                                     [s + "-*" for s in COUCHBASE_FROM_VERSION_3])
            exist = self.file_exists(version_path, VERSION_FILE)
            self.log.info("Is VERSION file existed on {0}? {1}"
                     .format(self.ip, exist))
            if exist:
                cb_releases_version = []
                for x in CB_RELEASE_BUILDS:
                    if int(CB_RELEASE_BUILDS[x]):
                        cb_releases_version.append(x)

                build_name, short_version, full_version = \
                    self.find_build_version(version_path, VERSION_FILE, product)

                if "-" in full_version:
                    msi_build = full_version.split("-")
                    """
                       In spock from build 2924 and later release, we only support
                       msi installation method on windows
                    """
                    if msi_build[0] in COUCHBASE_FROM_SPOCK:
                        os_type = "msi"
                        windows_msi = True
                        self.info.deliverable_type = "msi"
                else:
                    mesg = " ***** ERROR: ***** \n" \
                           " Couchbase Server version format is not correct. \n" \
                           " It should be 0.0.0-DDDD format\n" \
                           % (self.ip, os.getpid())
                    self.stop_current_python_running(mesg)

                build_repo = MV_LATESTBUILD_REPO
                if full_version[:5] not in COUCHBASE_VERSION_2 and \
                   full_version[:5] not in COUCHBASE_VERSION_3:
                    if full_version[:3] in CB_VERSION_NAME:
                        build_repo = CB_REPO + CB_VERSION_NAME[full_version[:3]] + "/"
                    else:
                        sys.exit("version is not support yet")
                self.log.info("*****VERSION file exists."
                         "Start to uninstall {0} on {1} server"
                         .format(product, self.ip))
                if full_version[:3] == "4.0":
                    build_repo = SHERLOCK_BUILD_REPO
                self.log.info('Build name: {0}'.format(build_name))
                build_name = build_name.rstrip() + ".%s" % os_type
                self.log.info('Check if {0} is in tmp directory on {1} server'
                         .format(build_name, self.ip))
                exist = self.file_exists("/cygdrive/c/tmp/", build_name)
                if not exist:  # if not exist in tmp dir, start to download that version
                    if short_version[:5] in cb_releases_version:
                        build = query.find_couchbase_release_build(
                            product_name, self.info.deliverable_type,
                            self.info.architecture_type, short_version,
                            is_amazon=False,
                            os_version=self.info.distribution_version.lower())
                    elif short_version in COUCHBASE_RELEASE_VERSIONS_3:
                        build = query.find_membase_release_build(
                            product_name, self.info.deliverable_type,
                            self.info.architecture_type, short_version,
                            is_amazon=False,
                            os_version=self.info.distribution_version.lower())
                    else:
                        builds, changes = query.get_all_builds(
                            version=full_version,
                            deliverable_type=self.info.deliverable_type,
                            architecture_type=self.info.architecture_type,
                            edition_type=product_name, repo=build_repo,
                            distribution_version=self.info.distribution_version.lower())

                        build = query.find_build(
                            builds, product_name, os_type,
                            self.info.architecture_type, full_version,
                            distribution_version=self.info.distribution_version.lower(),
                            distribution_type=self.info.distribution_type.lower())
                    downloaded = self.download_binary_in_win(build.url, short_version)
                    if downloaded:
                        self.log.info('Successful download {0}.exe on {1} server'
                                 .format(short_version, self.ip))
                    else:
                        self.log.error('Download {0}.exe failed'
                                  .format(short_version))
                if not windows_msi:
                    dir_paths = ['/cygdrive/c/automation', '/cygdrive/c/tmp']
                    self.create_multiple_dir(dir_paths)
                    self.copy_files_local_to_remote('resources/windows/automation',
                                                    '/cygdrive/c/automation')
                    # modify bat file to run uninstall schedule task
                    # self.create_windows_capture_file(task, product, full_version)
                    capture_iss_file = self.modify_bat_file('/cygdrive/c/automation',
                                                            bat_file, product,
                                                            short_version, task)
                    self.stop_schedule_tasks()

                """ Remove this workaround when bug MB-14504 is fixed """
                self.log.info("Kill any un/install process leftover in sherlock")
                self.log.info("Kill any cbq-engine.exe in sherlock")
                self.execute_command('taskkill /F /T /IM cbq-engine.exe')
                self.log.info('sleep for 5 seconds before running task '
                         'schedule uninstall on {0}'.format(self.ip))
                """ End remove this workaround when bug MB-14504 is fixed """

                self.stop_couchbase()
                time.sleep(5)
                # run schedule task uninstall couchbase server
                if windows_msi:
                    self.log.info("******** uninstall via msi method ***********")
                    output, error = \
                        self.execute_command("cd /cygdrive/c/tmp; msiexec /x %s /qn"\
                                             % build_name)
                    self.log_command_output(output, error)
                    var_dir = "/cygdrive/c/Program\ Files/Couchbase/Server/var"
                    self.execute_command("rm -rf %s" % var_dir)
                else:
                    output, error = \
                        self.execute_command("cmd /c schtasks /run /tn removeme")
                    self.log_command_output(output, error)
                deleted = self.wait_till_file_deleted(
                    version_path, VERSION_FILE, timeout_in_seconds=600)
                if not deleted:
                    if windows_msi:
                        self.log.info("******** repair via msi method ***********")
                        output, error = \
                            self.execute_command("cd /cygdrive/c/tmp; msiexec /fa %s /norestart"\
                                                 % build_name)
                        self.log_command_output(output, error)
                        self.log.info("******** uninstall via msi method ***********")
                        output, error = \
                            self.execute_command("cd /cygdrive/c/tmp; msiexec /x %s /qn" \
                                                 % build_name)
                        self.log_command_output(output, error)
                        var_dir = "/cygdrive/c/Program\ Files/Couchbase/"
                        self.execute_command("rm -rf %s" % var_dir)
                        deleted = self.wait_till_file_deleted(
                            version_path, VERSION_FILE, timeout_in_seconds=300)
                        if not deleted:
                            self.log.error("Uninstall was failed at node {0}"
                                      .format(self.ip))
                            sys.exit()
                    else:
                        self.log.error("Uninstall was failed at node {0}"
                                  .format(self.ip))
                        sys.exit()
                if full_version[:3] != "2.5":
                    uninstall_process = full_version[:10]
                    if not windows_msi:
                        ended = self.wait_till_process_ended(uninstall_process)
                        if not ended:
                            sys.exit("****  Node %s failed to uninstall  ****"
                                     % self.ip)
                if full_version[:3] == "2.5":
                    self.sleep(20, "next step is to install")
                else:
                    self.sleep(10, "next step is to install")
                """ delete binary after uninstall """
                self.delete_file(WIN_TMP_PATH, build_name)
                """ the code below need to remove when bug MB-11328
                                                           is fixed in 3.0.1 """
                output, error = self.kill_erlang(os="windows")
                self.log_command_output(output, error)
                """ end remove code """

                """ the code below need to remove when bug MB-11985
                                                           is fixed in 3.0.1 """
                if full_version[:5] in COUCHBASE_VERSION_2 or \
                   full_version[:5] in COUCHBASE_FROM_VERSION_3:
                    self.log.info("due to bug MB-11985, we need to delete below registry")
                    output, error = self.execute_command("reg delete \
                            'HKLM\Software\Wow6432Node\Ericsson\Erlang\ErlSrv' /f ")
                    self.log_command_output(output, error)
                """ end remove code """
                if capture_iss_file and not windows_msi:
                    self.log.info("Delete {0} in windows automation directory" \
                             .format(capture_iss_file))
                    output, error = self.execute_command("rm -f \
                               /cygdrive/c/automation/{0}".format(capture_iss_file))
                    self.log_command_output(output, error)
                    self.log.info("Delete {0} in slave resources/windows/automation dir" \
                             .format(capture_iss_file))
                    os.system("rm -f resources/windows/automation/{0}" \
                              .format(capture_iss_file))
            else:
                self.log.info("*****No couchbase server on {0} server. Free to install" \
                         .format(self.ip))
        elif type in LINUX_DISTRIBUTION_NAME:
            """ check if couchbase server installed by root """
            if self.nonroot:
                test = self.file_exists(LINUX_CB_PATH, VERSION_FILE)
                if self.file_exists(LINUX_CB_PATH, VERSION_FILE):
                    mesg = " ***** ERROR: ***** \n"\
                           " Couchbase Server was installed by root user. \n"\
                           " Use root user to uninstall it at %s \n"\
                           " This python process id: %d will be killed to stop the installation"\
                           % (self.ip, os.getpid())
                    self.stop_current_python_running(mesg)
                if self.file_exists(self.nr_home_path, NR_INSTALL_LOCATION_FILE):
                    output, error = self.execute_command("cat %s"
                                                         % NR_INSTALL_LOCATION_FILE)
                    if output and output[0]:
                        self.log.info("Couchbase Server was installed in non default path %s"
                                 % output[0])
                        self.nr_home_path = output[0]
            # uninstallation command is different
            if type == "ubuntu":
                if self.nonroot:
                    """ check if old files from root install left in server """
                    if self.file_exists(LINUX_CB_PATH + "etc/couchdb/",
                                        "local.ini.debsave"):
                        print(" ***** ERROR: ***** \n"\
                              "Couchbase Server files was left by root install at %s .\n"
                              "Use root user to delete them all at server %s "
                              " (rm -rf /opt/couchbase) to remove all couchbase folder."
                              % (LINUX_CB_PATH, self.ip))
                        sys.exit(1)
                    self.stop_server()
                else:
                    if sv in COUCHBASE_FROM_VERSION_4:
                        if self.is_enterprise(type):
                            if product is not None and product == "cbas":
                                uninstall_cmd = "dpkg -r {0};dpkg --purge {1};" \
                                        .format("couchbase-server-analytics", "couchbase-server-analytics")
                            else:
                                uninstall_cmd = "dpkg -r {0};dpkg --purge {1};" \
                                        .format("couchbase-server", "couchbase-server")
                        else:
                            uninstall_cmd = "dpkg -r {0};dpkg --purge {1};" \
                                         .format("couchbase-server-community",
                                                 "couchbase-server-community")
                    else:
                        uninstall_cmd = "dpkg -r {0};dpkg --purge {1};" \
                                        .format("couchbase-server",
                                                "couchbase-server")
                    output, error = self.execute_command(uninstall_cmd)
                    self.log_command_output(output, error)
            elif type in RPM_DIS_NAME:
                """ Sometimes, vm left with unfinish uninstall/install process.
                    We need to kill them before doing uninstall """
                if self.nonroot:
                    """ check if old files from root install left in server """
                    if self.file_exists(LINUX_CB_PATH + "etc/couchdb/",
                                        "local.ini.rpmsave"):
                        print("Couchbase Server files was left by root install at %s.\n"
                              "Use root user to delete them all at server %s "
                              "(rm -rf /opt/couchbase) to remove all couchbase folder."
                              % (LINUX_CB_PATH, self.ip))
                        sys.exit(1)
                    self.stop_server()
                else:
                    output, error = self.execute_command("killall -9 rpm")
                    self.log_command_output(output, error)
                    output, error = self.execute_command("rm -f /var/lib/rpm/.rpm.lock")
                    self.log_command_output(output, error)
                    if sv in COUCHBASE_FROM_VERSION_4:
                        if self.is_enterprise(type):
                            if product is not None and product == "cbas":
                                uninstall_cmd = 'rpm -e {0}'.format("couchbase-server-analytics")
                            else:
                                uninstall_cmd = 'rpm -e {0}'.format("couchbase-server")
                        else:
                            uninstall_cmd = 'rpm -e {0}' \
                                          .format("couchbase-server-community")
                    else:
                        uninstall_cmd = 'rpm -e {0}'.format("couchbase-server")
                    self.log.info('running rpm -e to remove couchbase-server')
                    output, error = self.execute_command(uninstall_cmd)
                    if output:
                        server_ip = "\n\n**** Uninstalling on server: {0} ****".format(self.ip)
                        output.insert(0, server_ip)
                    if error:
                        server_ip = "\n\n**** Uninstalling on server: {0} ****".format(self.ip)
                        error.insert(0, server_ip)
                    self.log_command_output(output, error)
            self.terminate_processes(self.info, terminate_process_list)
            if not self.nonroot:
                self.remove_folders(linux_folders)
                self.kill_memcached()
                output, error = self.execute_command("ipcrm")
                self.log_command_output(output, error)
        elif self.info.distribution_type.lower() == 'mac':
            self.stop_server()
            """ close Safari browser before uninstall """
            self.terminate_process(self.info, "/Applications/Safari.app/Contents/MacOS/Safari")
            self.terminate_processes(self.info, terminate_process_list)
            output, error = self.execute_command("rm -rf /Applications/Couchbase\ Server.app")
            self.log_command_output(output, error)
            output, error = self.execute_command("rm -rf ~/Library/Application\ Support/Couchbase")
            self.log_command_output(output, error)
        if self.nonroot:
            if self.nr_home_path != "/home/%s/" % self.username:
                self.log.info("remove all non default install dir")
                output, error = self.execute_command("rm -rf %s"
                                                     % self.nr_home_path)
                self.log_command_output(output, error)
            else:
                self.log.info("Remove Couchbase Server directories opt etc and usr")
                output, error = self.execute_command("cd %s;rm -rf opt etc usr"
                                                     % self.nr_home_path)
                self.log_command_output(output, error)
                self.execute_command("cd %s;rm -rf couchbase-server-*"
                                     % self.nr_home_path)
                output, error = self.execute_command("cd %s;ls -lh"
                                                     % self.nr_home_path)
                self.log_command_output(output, error)
            if "nr_install_dir" not in self.input.test_params:
                self.nr_home_path = "/home/%s/" % self.username
                output, error = self.execute_command(" :> %s"
                                                     % NR_INSTALL_LOCATION_FILE)
                self.log_command_output(output, error)
            output, error = self.execute_command("ls -lh")
            self.log_command_output(output, error)

    def couchbase_win_uninstall(self, product, version, os_name, query):
        self.log.info('*****couchbase_win_uninstall****')
        builds, changes = query.get_all_builds(version=version)
        bat_file = "uninstall.bat"
        task = "uninstall"
        deleted = False

        self.extract_remote_info()
        ex_type = self.info.deliverable_type
        if self.info.architecture_type == "x86_64":
            os_type = "64"
        elif self.info.architecture_type == "x86":
            os_type = "32"
        if product == "cse":
            name = "couchbase-server-enterprise"
            version_path = "/cygdrive/c/Program Files/Couchbase/Server/"

        exist = self.file_exists(version_path, VERSION_FILE)
        if exist:
            # call uninstall function to install couchbase server
            # Need to detect csse or cse when uninstall.
            self.log.info("Start uninstall cb server on this server")
            build_name, rm_version = self.find_build_version(version_path,
                                                             VERSION_FILE)
            self.log.info('build needed to do auto uninstall {0}'
                     .format(build_name))
            # find installed build in tmp directory to match with currently installed version
            build_name = build_name.rstrip() + ".exe"
            self.log.info('Check if {0} is in tmp directory'.format(build_name))
            exist = self.file_exists("/cygdrive/c/tmp/", build_name)
            # if not exist in tmp dir, start to download that version build
            if not exist:
                build = query.find_build(builds, name, ex_type,
                                         self.info.architecture_type,
                                         rm_version)
                downloaded = self.download_binary_in_win(build.url, rm_version)
                if downloaded:
                    self.log.info('Successful download {0}.exe'.format(rm_version))
                else:
                    self.log.error('Download {0}.exe failed'.format(rm_version))
            # copy required files to automation directory
            dir_paths = ['/cygdrive/c/automation', '/cygdrive/c/tmp']
            self.create_multiple_dir(dir_paths)
            self.copy_files_local_to_remote('resources/windows/automation',
                                            '/cygdrive/c/automation')
            # modify bat file to run uninstall schedule task
            self.modify_bat_file('/cygdrive/c/automation', bat_file, product,
                                 rm_version, task)
            self.stop_couchbase()

            # the code below need to remove when bug MB-11328 is fixed in 3.0.1
            output, error = self.kill_erlang(os="windows")
            self.log_command_output(output, error)
            """ end remove code """

            self.sleep(5, "before running task schedule uninstall")
            # run schedule task uninstall couchbase server
            output, error = self.execute_command("cmd /c schtasks /run /tn removeme")
            self.log_command_output(output, error)
            deleted = self.wait_till_file_deleted(version_path, VERSION_FILE,
                                                  timeout_in_seconds=600)
            if not deleted:
                self.log.error("Uninstall was failed at node {0}".format(self.ip))
                sys.exit()
            ended = self.wait_till_process_ended(build_name[:10])
            if not ended:
                sys.exit("*****  Node %s failed to uninstall  *****"
                         % self.ip)
            self.sleep(10, "next step is to install")
            output, error = self.execute_command("rm -f \
                       /cygdrive/c/automation/*_{0}_uninstall.iss"
                       .format(self.ip))
            self.log_command_output(output, error)
            output, error = self.execute_command("cmd /c schtasks /Query /FO LIST /TN removeme /V")
            self.log_command_output(output, error)
            output, error = self.execute_command("rm -f /cygdrive/c/tmp/{0}"
                                                 .format(build_name))
            self.log_command_output(output, error)

            # the code below need to remove when bug MB-11328 is fixed in 3.0.1
            output, error = self.kill_erlang(os="windows")
            self.log_command_output(output, error)
            """ end remove code """
        else:
            self.log.info('No couchbase server on this server')

    def membase_uninstall(self, save_upgrade_config=False):
        linux_folders = ["/var/opt/membase",
                         "/opt/membase",
                         "/etc/opt/membase",
                         "/var/membase/data/*",
                         "/opt/membase/var/lib/membase/*"]
        terminate_process_list = ["beam", "memcached", "moxi",
                                  "vbucketmigrator", "couchdb", "epmd"]
        self.extract_remote_info()
        self.log.info(self.info.distribution_type)
        type = self.info.distribution_type.lower()
        if type == 'windows':
            product = "mb"
            query = BuildQuery()
            builds, changes = query.get_all_builds()
            os_type = "exe"
            task = "uninstall"
            bat_file = "uninstall.bat"
            deleted = False
            product_name = "membase-server-enterprise"
            version_path = "/cygdrive/c/Program Files/Membase/Server/"

            exist = self.file_exists(version_path, VERSION_FILE)
            self.log.info("Is VERSION file existed? {0}".format(exist))
            if exist:
                self.log.info("VERSION file exists.  Start to uninstall")
                build_name, short_version, full_version = self.find_build_version(version_path, VERSION_FILE, product)
                if "1.8.0" in full_version or "1.8.1" in full_version:
                    product_name = "couchbase-server-enterprise"
                    product = "cb"
                self.log.info('Build name: {0}'.format(build_name))
                build_name = build_name.rstrip() + ".exe"
                self.log.info('Check if {0} is in tmp directory'.format(build_name))
                exist = self.file_exists("/cygdrive/c/tmp/", build_name)
                # if not exist in tmp dir, start to download that version build
                if not exist:
                    build = query.find_build(builds, product_name, os_type,
                                             self.info.architecture_type,
                                             full_version)
                    downloaded = self.download_binary_in_win(build.url,
                                                             short_version)
                    if downloaded:
                        self.log.info('Successful download {0}_{1}.exe'
                                 .format(product, short_version))
                    else:
                        self.log.error('Download {0}_{1}.exe failed'
                                  .format(product, short_version))
                dir_paths = ['/cygdrive/c/automation', '/cygdrive/c/tmp']
                self.create_multiple_dir(dir_paths)
                self.copy_files_local_to_remote('resources/windows/automation',
                                                '/cygdrive/c/automation')
                # modify bat file to run uninstall schedule task
                # self.create_windows_capture_file(task, product, full_version)
                self.modify_bat_file('/cygdrive/c/automation', bat_file,
                                     product, short_version, task)
                self.stop_schedule_tasks()
                self.sleep(5, "before running task schedule uninstall")
                # run schedule task uninstall Couchbase server
                output, error = self.execute_command("cmd /c schtasks /run /tn removeme")
                self.log_command_output(output, error)
                deleted = self.wait_till_file_deleted(version_path, VERSION_FILE, timeout_in_seconds=600)
                if not deleted:
                    self.log.error("Uninstall was failed at node {0}".format(self.ip))
                    sys.exit()
                ended = self.wait_till_process_ended(full_version[:10])
                if not ended:
                    sys.exit("****  Node %s failed to uninstall  ****" % (self.ip))
                self.sleep(10, "next step is to install")
                output, error = self.execute_command("rm -f \
                       /cygdrive/c/automation/*_{0}_uninstall.iss".format(self.ip))
                self.log_command_output(output, error)
                output, error = self.execute_command("cmd /c schtasks /Query /FO LIST /TN removeme /V")
                self.log_command_output(output, error)
            else:
                self.log.info("No membase server on this server.  Free to install")
        elif type in LINUX_DISTRIBUTION_NAME:
            # uninstallation command is different
            if type == "ubuntu":
                uninstall_cmd = 'dpkg -r {0};dpkg --purge {1};' \
                                 .format('membase-server', 'membase-server')
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            elif type in RPM_DIS_NAME:
                """ Sometimes, vm left with unfinish uninstall/install process.
                    We need to kill them before doing uninstall """
                output, error = self.execute_command("killall -9 rpm")
                self.log_command_output(output, error)
                uninstall_cmd = 'rpm -e {0}'.format('membase-server')
                self.log.info('running rpm -e to remove membase-server')
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            self.terminate_processes(self.info, terminate_process_list)
            if not save_upgrade_config:
                self.remove_folders(linux_folders)

    def moxi_uninstall(self):
        terminate_process_list = ["moxi"]
        self.extract_remote_info()
        self.log.info(self.info.distribution_type)
        type = self.info.distribution_type.lower()
        if type == 'windows':
            self.log.error("Not implemented")
        elif type == "ubuntu":
            uninstall_cmd = "dpkg -r {0};dpkg --purge {1};" \
                            .format("moxi-server", "moxi-server")
            output, error = self.execute_command(uninstall_cmd)
            self.log_command_output(output, error)
        elif type in LINUX_DISTRIBUTION_NAME:
            uninstall_cmd = 'rpm -e {0}'.format("moxi-server")
            self.log.info('running rpm -e to remove couchbase-server')
            output, error = self.execute_command(uninstall_cmd)
            self.log_command_output(output, error)
        self.terminate_processes(self.info, terminate_process_list)

    def log_command_output(self, output, error, track_words=(), debug=True):
        # success means that there are no track_words in the output
        # and there are no errors at all, if track_words is not empty
        # if track_words=(), the result is not important, and we return True
        success = True
        for line in error:
            if debug:
                self.log.error(line)
            if track_words:
                if "Warning" in line and "hugepages" in line:
                    self.log.warn("There is a warning about transparent_hugepage "
                             "may be in used when install cb server. So we \
                              will disable transparent_hugepage in this vm")
                    output, error = self.execute_command("echo never > \
                                        /sys/kernel/mm/transparent_hugepage/enabled")
                    self.log_command_output(output, error)
                    success = True
                elif "Warning" in line and "systemctl daemon-reload" in line:
                    self.log.warn("Unit file of couchbase-server.service changed on disk,"
                             " we will run 'systemctl daemon-reload'")
                    output, error = self.execute_command("systemctl daemon-reload")
                    self.log_command_output(output, error)
                    success = True
                elif "Warning" in line and "RPMDB altered outside of yum" in line:
                    self.log.warn("Warning: RPMDB altered outside of yum")
                    success = True
                elif "dirname" in line:
                    self.log.warn("Ignore dirname error message during couchbase "
                             "startup/stop/restart for CentOS 6.6 (MB-12536)")
                    success = True
                elif "Created symlink from /etc/systemd/system" in line:
                    self.log.warn("This error is due to fix_failed_install.py "
                             "script that only happens in centos 7")
                    success = True
                elif "Created symlink /etc/systemd/system/multi-user.target.wants/couchbase-server.service" in line:
                    self.log.warn(line)
                    self.log.warn("This message comes only in debian8 and debian9 "
                             "during installation. This can be ignored.")
                    success = True
                else:
                    self.log.fatal("If couchbase server is running with this error."
                              "Go to log_command_output to add error mesg to "
                              "bypass it.")
                    success = False
        for line in output:
            self.log.debug(line)
            if any(s.lower() in line.lower() for s in track_words):
                if "Warning" in line and "hugepages" in line:
                    self.log.warn("There is a warning about transparent_hugepage "
                             "may be in used when install cb server. So we"
                             "will disable transparent_hugepage in this vm")
                    output, error = self.execute_command("echo never > /sys/kernel/mm/transparent_hugepage/enabled")
                    self.log_command_output(output, error, debug=debug)
                    success = True
                else:
                    success = False
                    self.log.fatal('something wrong happened on {0}!!! '
                              'output:{1}, error:{2}, track_words:{3}'
                              .format(self.ip, output, error, track_words))
        return success

    def execute_commands_inside(self, main_command, query, queries, bucket1,
                                password, bucket2, source, subcommands=[],
                                min_output_size=0, end_msg='', timeout=250):
        self.extract_remote_info()
        filename = "/tmp/test2"
        iswin = False

        if self.info.type.lower() == 'windows':
            iswin = True
            filename = "/cygdrive/c/tmp/test.txt"

        filedata = ""
        if not(query == ""):
            main_command = main_command + " -s=\"" + query + '"'
        elif (self.remote and not(queries == "")):
            sftp = self._ssh_client.open_sftp()
            filein = sftp.open(filename, 'w')
            for query in queries:
                filein.write(query)
                filein.write('\n')
            fileout = sftp.open(filename, 'r')
            filedata = fileout.read()
            fileout.close()
        elif not(queries == ""):
            f = open(filename, 'w')
            for query in queries:
                f.write(query)
                f.write('\n')
            f.close()
            fileout = open(filename, 'r')
            filedata = fileout.read()
            fileout.close()

        newdata = filedata.replace("bucketname", bucket2)
        newdata = newdata.replace("user", bucket1)
        newdata = newdata.replace("pass", password)
        newdata = newdata.replace("bucket1", bucket1)

        newdata = newdata.replace("user1", bucket1)
        newdata = newdata.replace("pass1", password)
        newdata = newdata.replace("bucket2", bucket2)
        newdata = newdata.replace("user2", bucket2)
        newdata = newdata.replace("pass2", password)

        if self.remote and not(queries == ""):
            f = sftp.open(filename, 'w')
            f.write(newdata)
            f.close()
        elif not(queries == ""):
            f = open(filename, 'w')
            f.write(newdata)
            f.close()
        if not(queries == ""):
            if (source):
                if iswin:
                    main_command = main_command + "  -s=\"\SOURCE " + 'c:\\\\tmp\\\\test.txt'
                else:
                    main_command = main_command + "  -s=\"\SOURCE " + filename+ '"'
            else:
                if iswin:
                    main_command = main_command + " -f=" + 'c:\\\\tmp\\\\test.txt'
                else:
                    main_command = main_command + " -f=" + filename

        self.log.debug("running command on {0}: {1}".format(self.ip, main_command))
        output = ""
        if self.remote:
            (stdin, stdout, stderro) = self._ssh_client.exec_command(main_command)
            time.sleep(10)
            count = 0
            for line in stdout.readlines():
                if (count == 0) and line.lower().find("error") > 0:
                    output = "status:FAIL"
                    break

                # if line.find("results") > 0 or line.find("status") > 0 or \
                #    line.find("metrics") or line.find("elapsedTime")> 0 or \
                #    line.find("executionTime")> 0 or line.find("resultCount"):
                if (count > 0):
                    output += line.strip()
                    output = output.strip()
                    if "Inputwasnotastatement" in output:
                        output = "status:FAIL"
                        break
                    if "timeout" in output:
                        output = "status:timeout"
                else:
                    count += 1
            stdin.close()
            stdout.close()
            stderro.close()
            """
            main_command = main_command + " < " + '/tmp/' + filename
            stdin,stdout, ssh_stderr = ssh.exec_command(main_command)
            stdin.close()
            output = []
            for line in stdout.read().splitlines():
                print(line)
                output = output.append(line)
            f.close()
            ssh.close()
            """
            # output = output + end_msg
        else:
            p = Popen(main_command, shell=True, stdout=PIPE, stderr=PIPE)
            stdout, stderro = p.communicate()
            output = stdout
            print output
            time.sleep(1)
        """
        for cmd in subcommands:
            self.log.info("running command {0} inside {1} ({2})"
                     .format(main_command, cmd, self.ip))
            stdin.channel.send("{0}\n".format(cmd))
            end_time = time.time() + float(timeout)
            while True:
                if time.time() > end_time:
                    raise Exception("no output in {3} sec running command \
                                    {0} inside {1} ({2})"
                                    .format(main_command, cmd, self.ip,
                                            timeout))
                output = stdout.channel.recv(1024)
                if output.strip().endswith(end_msg) and len(output) >= min_output_size:
                    break
                time.sleep(2)
            self.log.info("{0}:'{1}' -> '{2}' output\n: {3}".format(self.ip, main_command, cmd, output))
        stdin.close()
        stdout.close()
        stderro.close()
        """
        if self.remote and not(queries == ""):
            sftp.remove(filename)
            sftp.close()
        elif not(queries == ""):
            os.remove(filename)

        output = re.sub('\s+', '', output)
        return output

    def execute_command(self, command, info=None, debug=False,
                        use_channel=False):
        if getattr(self, "info", None) is None and info is not None:
            self.info = info
        else:
            self.extract_remote_info()

        if self.info.type.lower() == 'windows':
            self.use_sudo = False

        if self.use_sudo:
            command = "sudo " + command

        return self.execute_command_raw_jsch(command, debug=debug,
                                             use_channel=use_channel)

    def execute_command_raw_jsch(self, command, debug=True, use_channel=False):
        self.log.debug("Running command on {0}: {1}".format(self.ip, command))
        output = []
        error = []
        if not self.remote:
            p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
            output, error = p.communicate()

        else:
            try:
                self._ssh_client = self.session.openChannel("exec")
                self._ssh_client.setInputStream(None)
                self._ssh_client.setErrStream(None)

                instream = self._ssh_client.getInputStream()
                errstream = self._ssh_client.getErrStream()
                self._ssh_client.setCommand(command)
                self._ssh_client.connect()
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

                self._ssh_client.disconnect()
#                 self.session.disconnect()
            except JSchException as e:
                self.log.error("JSch exception on %s: %s" % (self.ip, str(e)))
        return output, error

    def execute_command_raw(self, command, debug=True, use_channel=False):
        if debug:
            self.log.info("running command.raw on {0}: {1}"
                     .format(self.ip, command))
        output = []
        error = []
        temp = ''
        if self.remote and self.use_sudo or use_channel:
            channel = self._ssh_client.get_transport().open_session()
            channel.get_pty()
            channel.settimeout(900)
            stdin = channel.makefile('wb')
            stdout = channel.makefile('rb')
            stderro = channel.makefile_stderr('rb')
            channel.exec_command(command)
            data = channel.recv(1024)
            while data:
                temp += data
                data = channel.recv(1024)
            channel.close()
            stdin.close()
        elif self.remote:
            stdin, stdout, stderro = self._ssh_client.exec_command(command)
            stdin.close()

        if not self.remote:
            p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
            output, error = p.communicate()

        if self.remote:
            for line in stdout.read().splitlines():
                output.append(line)
            for line in stderro.read().splitlines():
                error.append(line)
            if temp:
                line = temp.splitlines()
                output.extend(line)
            stdout.close()
            stderro.close()
        if debug:
            self.log.info('command executed successfully')
        return output, error

    def execute_non_sudo_command(self, command, info=None, debug=True,
                                 use_channel=False):
        info = info or self.extract_remote_info()
        self.info = info

        return self.execute_command_raw_jsch(command, debug=debug,
                                             use_channel=use_channel)

    def terminate_process(self, info=None, process_name='', force=False):
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("taskkill /F /T /IM {0}*"
                                        .format(process_name), debug=False)
            self.log_command_output(o, r)
        else:
            if (force == True):
                o, r = self.execute_command("kill -9 $(ps aux | grep '{0}' |  awk '{{print $2}}')"
                                            .format(process_name), debug=False)
                self.log_command_output(o, r)
            else:
                o, r = self.execute_command("kill $(ps aux | grep '{0}' |  awk '{{print $2}}')"
                                            .format(process_name), debug=False)
                self.log_command_output(o, r)

#     def disconnect(self):
#         if self._ssh_client:
#             log.info("Disconnecting ssh_client for {0}".format(self.ip))
#             self._ssh_client.disconnect()

    def extract_remote_info(self):
        # initialize params
        os_distro = "linux"
        os_version = "default"
        is_linux_distro = True
        self.use_sudo = False
        is_mac = False
        arch = "local"
        ext = "local"
        # use ssh to extract remote machine info
        # use sftp to if certain types exists or not
        if getattr(self, "info", None) is not None and isinstance(self.info,
                                                                  RemoteMachineInfo):
            return self.info
        mac_check_cmd = "sw_vers | grep ProductVersion | awk '{ print $2 }'"
        if self.remote:
            ver, err = self.execute_command_raw_jsch(mac_check_cmd)
        else:
            p = Popen(mac_check_cmd, shell=True, stdout=PIPE, stderr=PIPE)
            ver, err = p.communicate()
        if not err and ver:
            os_distro = "Mac"
            os_version = ver
            is_linux_distro = True
            is_mac = True
            self.use_sudo = False
        elif self.remote:
            is_mac = False
            filenames = self.execute_command_raw_jsch('ls /etc/')[0]
            os_distro = ""
            os_version = ""
            is_linux_distro = False
            for name in filenames:
                if name.rstrip('\n') == 'issue':
                    # it's a linux_distro . let's downlaod this file
                    # format Ubuntu 10.04 LTS \n \l
                    filename = 'etc-issue-{0}'.format(uuid.uuid4())
                    self.get_file('/etc', "issue", "./%s" % filename)
                    file = open(filename)
                    etc_issue = ''
                    # let's only read the first line
                    for line in file.xreadlines():
                        # for SuSE that has blank first line
                        if line.rstrip('\n'):
                            etc_issue = line
                            break
                        # strip all extra characters
                    etc_issue = etc_issue = etc_issue.rstrip('\n').rstrip(' ').rstrip('\\l').rstrip(' ').rstrip('\\n').rstrip(' ')
                    if etc_issue.lower().find('ubuntu') != -1:
                        os_distro = 'Ubuntu'
                        os_version = etc_issue
                        tmp_str = etc_issue.split()
                        if tmp_str and tmp_str[1][:2].isdigit():
                            os_version = "Ubuntu %s" % tmp_str[1][:5]
                        is_linux_distro = True
                    elif etc_issue.lower().find('debian') != -1:
                        os_distro = 'Ubuntu'
                        os_version = etc_issue
                        is_linux_distro = True
                    elif etc_issue.lower().find('mint') != -1:
                        os_distro = 'Ubuntu'
                        os_version = etc_issue
                        is_linux_distro = True
                    elif etc_issue.lower().find('amazon linux ami') != -1:
                        os_distro = 'CentOS'
                        os_version = etc_issue
                        is_linux_distro = True
                    elif etc_issue.lower().find('centos') != -1:
                        os_distro = 'CentOS'
                        os_version = etc_issue
                        is_linux_distro = True
                    elif etc_issue.lower().find('red hat') != -1:
                        os_distro = 'Red Hat'
                        os_version = etc_issue
                        is_linux_distro = True
                    elif etc_issue.lower().find('opensuse') != -1:
                        os_distro = 'openSUSE'
                        os_version = etc_issue
                        is_linux_distro = True
                    elif etc_issue.lower().find('suse linux') != -1:
                        os_distro = 'SUSE'
                        os_version = etc_issue
                        tmp_str = etc_issue.split()
                        if tmp_str and tmp_str[6].isdigit():
                            os_version = "SUSE %s" % tmp_str[6]
                        is_linux_distro = True
                    elif etc_issue.lower().find('oracle linux') != -1:
                        os_distro = 'Oracle Linux'
                        os_version = etc_issue
                        is_linux_distro = True
                    else:
                        self.log.debug("It could be other operating system. "
                                       "Go to check at other location")
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
                arch = "local"
                ext = "local"
                filenames = []
            """ for centos 7 only """
            for name in filenames:
                if name.rstrip('\n') == "redhat-release":
                    filename = 'redhat-release-{0}'.format(uuid.uuid4())
                    if self.remote:
                        self.get_file('/etc','redhat-release', "./%s" % filename)
                        # sftp.get(localpath=filename, remotepath='/etc/redhat-release')
                    else:
                        p = Popen("cat /etc/redhat-release > {0}".format(filename),
                                  shell=True, stdout=PIPE, stderr=PIPE)
                        var, err = p.communicate()
                    file = open(filename)
                    redhat_release = ''
                    for line in file.xreadlines():
                        redhat_release = line
                        break
                    redhat_release = redhat_release.rstrip('\n').rstrip('\\l').rstrip('\\n')
                    """ in ec2: Red Hat Enterprise Linux Server release 7.2 """
                    if redhat_release.lower().find('centos') != -1 \
                            or redhat_release.lower().find('linux server') != -1:
                        if redhat_release.lower().find('release 7') != -1:
                            os_distro = 'CentOS'
                            os_version = "CentOS 7"
                            is_linux_distro = True
                    else:
                        self.log.error("Could not find OS name."
                                  "It could be unsupport OS")
                    file.close()
                    os.remove(filename)
                    break

        if self.remote:
            if self.find_file("/cygdrive/c/Windows", "win.ini"):
                self.log.info("This is windows server!")
                is_linux_distro = False
        if not is_linux_distro:
            arch = ''
            os_version = 'unknown windows'
            win_info = self.find_windows_info()
            info = RemoteMachineInfo()
            info.type = win_info['os']
            info.windows_name = win_info['os_name']
            info.distribution_type = win_info['os']
            info.architecture_type = win_info['os_arch']
            info.ip = self.ip
            info.distribution_version = win_info['os']
            info.deliverable_type = 'exe'
            info.cpu = self.get_cpu_info(win_info)
            info.disk = self.get_disk_info(win_info)
            info.ram = self.get_ram_info(win_info)
            info.hostname = self.get_hostname(win_info)
            info.domain = self.get_domain(win_info)
            self.info = info
            return info
        else:
            # now run uname -m to get the architechtre type
            os_arch = ''
            if self.remote:
                text, stderro = self.execute_command_raw_jsch('uname -m')
            else:
                p = Popen('uname -m', shell=True, stdout=PIPE, stderr=PIPE)
                text, err = p.communicate()
                os_arch = ''
            for line in text:
                os_arch += line.rstrip()
                # at this point we should know if its a linux or windows ditro
            ext = {'Ubuntu': "deb",
                   'CentOS': "rpm",
                   'Red Hat': "rpm",
                   "Mac": "zip",
                   "Debian": "deb",
                   "openSUSE": "rpm",
                   "SUSE": "rpm",
                   "Oracle Linux": "rpm"}.get(os_distro, '')
            arch = {'i686': 'x86',
                    'i386': 'x86'}.get(os_arch, os_arch)

            info = RemoteMachineInfo()
            info.type = "Linux"
            info.distribution_type = os_distro
            info.architecture_type = arch
            info.ip = self.ip
            info.distribution_version = os_version
            info.deliverable_type = ext
            info.cpu = self.get_cpu_info(mac=is_mac)
            info.disk = self.get_disk_info(mac=is_mac)
            info.ram = self.get_ram_info(mac=is_mac)
            info.hostname = self.get_hostname()
            info.domain = self.get_domain()
            self.info = info
            return info

    def get_extended_windows_info(self):
        info = {}
        win_info = self.extend_windows_info()
        info['ram'] = self.get_ram_info(win_info)
        info['disk'] = self.get_disk_info(win_info)
        info['cpu'] = self.get_cpu_info(win_info)
        info['hostname'] = self.get_hostname()
        return info

    def get_hostname(self, win_info=None):
        if win_info:
            if 'Host Name' not in win_info:
                win_info = self.create_windows_info()
            o = win_info['Host Name']
        o, r = self.execute_command_raw_jsch('hostname', debug=False)
        if o:
            return o

    def get_domain(self, win_info=None):
        if win_info:
            o, _ = self.execute_batch_command('ipconfig')
            suffix_dns_row = [row for row in o
                              if row.find(" Connection-specific DNS Suffix") != -1 \
                              and len(row.split(':')[1]) > 1]
            if suffix_dns_row == []:
                # '   Connection-specific DNS Suffix  . : '
                ret = ""
            else:
                ret = suffix_dns_row[0].split(':')[1].strip()
        else:
            ret = self.execute_command_raw_jsch('hostname -d', debug=False)
        return ret

    def get_full_hostname(self):
        info = self.extract_remote_info()
        if not info.domain:
            return None
        self.log.info("hostname of this {0} is {1}"
                 .format(self.ip, info.hostname[0].strip()))
        if info.type.lower() == 'windows':
            self.log.info("domain name of this {0} is {1}"
                     .format(self.ip, info.domain))
            return '%s.%s' % (info.hostname[0].strip(), info.domain.strip())
        else:
            if info.domain[0]:
                if info.domain[0][0]:
                    self.log.info("domain name of this {0} is {1}"
                             .format(self.ip, info.domain[0][0].strip()))
                    return "{0}.{1}".format(info.hostname[0].strip(),
                                            info.domain[0][0].strip())
                else:
                    mesg = "Need to set domain name in server {0} like 'sc.couchbase.com'" \
                           .format(self.ip)
                    raise Exception(mesg)

    def get_cpu_info(self, win_info=None, mac=False):
        if win_info:
            if 'Processor(s)' not in win_info:
                win_info = self.create_windows_info()
            o = win_info['Processor(s)']
        elif mac:
            o, r = self.execute_command_raw_jsch('/sbin/sysctl -n machdep.cpu.brand_string')
        else:
            o, r = self.execute_command_raw_jsch('cat /proc/cpuinfo',
                                                 debug=False)
        if o:
            return o

    def get_ram_info(self, win_info=None, mac=False):
        if win_info:
            if 'Virtual Memory Max Size' not in win_info:
                win_info = self.create_windows_info()
            o = "Virtual Memory Max Size =" + win_info['Virtual Memory Max Size'] + '\n'
            o += "Virtual Memory Available =" + win_info['Virtual Memory Available'] + '\n'
            o += "Virtual Memory In Use =" + win_info['Virtual Memory In Use']
        elif mac:
            o, r = self.execute_command_raw_jsch('/sbin/sysctl -n hw.memsize',
                                                 debug=False)
        else:
            o, r = self.execute_command_raw_jsch('cat /proc/meminfo', debug=False)
        if o:
            return o

    def get_disk_info(self, win_info=None, mac=False, in_MB=False):
        if win_info:
            if 'Total Physical Memory' not in win_info:
                win_info = self.create_windows_info()
            o = "Total Physical Memory =" + win_info['Total Physical Memory'] + '\n'
            o += "Available Physical Memory =" + win_info['Available Physical Memory']
        elif mac:
            o, r = self.execute_command_raw_jsch('df -h', debug=False)
        else:
            if not in_MB:
                o, r = self.execute_command_raw_jsch('df -Th', debug=False)
            else:
                o, r = self.execute_command_raw_jsch('df -BM /', debug=False)
        if o:
            return o

    def get_memcache_pid(self):
        self.extract_remote_info()
        if self.info.type == 'Linux':
            o, _ = self.execute_command("ps -eo comm,pid | awk '$1 == \"memcached\" { print $2 }'")
            return o[0]
        elif self.info.type == 'windows':
            output, error = self.execute_command('tasklist| grep memcache',
                                                 debug=False)
            if error or output == [""] or output == []:
                return None
            words = output[0].split(" ")
            words = filter(lambda x: x != "", words)
            return words[1]

    def cleanup_data_config(self, data_path):
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            if "c:/Program Files" in data_path:
                data_path = data_path.replace("c:/Program Files",
                                              "/cygdrive/c/Program\ Files")
            o, r = self.execute_command("rm -rf ""{0}""/*".format(data_path))
            self.log_command_output(o, r)
            o, r = self.execute_command("rm -rf ""{0}""/*"
                                        .format(data_path.replace("data", "config")))
            self.log_command_output(o, r)
        else:
            o, r = self.execute_command("rm -rf {0}/*".format(data_path))
            self.log_command_output(o, r)
            o, r = self.execute_command("rm -rf {0}/*"\
                                        .format(data_path.replace("data", "config")))
            self.log_command_output(o, r)

    def stop_couchbase(self):
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("net stop couchbaseserver")
            self.log_command_output(o, r)
            self.sleep(10, "{0} - Wait to stop service completely"
                           .format(self.ip))
        if self.info.type.lower() == "linux":
            if self.nonroot:
                self.log.debug("{0} - stopping couchbase-server as non-root"
                               .format(self.ip))
                o, r = self.execute_command('%s%scouchbase-server -k '
                                            % (self.nr_home_path, LINUX_COUCHBASE_BIN_PATH))
                self.log_command_output(o, r)
            else:
                fv, sv, bn = self.get_cbversion("linux")
                if self.info.distribution_version.lower() in SYSTEMD_SERVER \
                        and sv in COUCHBASE_FROM_WATSON:
                    """from watson, systemd is used in centos 7, suse 12 """
                    self.log.debug("{0} - Running systemd command"
                                   .format(self.ip))
                    o, r = self.execute_command("systemctl stop couchbase-server.service")
                    self.log_command_output(o, r)
                else:
                    o, r = self.execute_command("/etc/init.d/couchbase-server stop",
                                                self.info, use_channel=True)
                    self.log_command_output(o, r)
        if self.info.distribution_type.lower() == "mac":
            cb_process = '/Applications/Couchbase\ Server.app/Contents/MacOS/Couchbase\ Server'
            cmd = "ps aux | grep {0} | awk '{{print $2}}' | xargs kill -9 " \
                  .format(cb_process)
            o, r = self.execute_command(cmd)
            self.log_command_output(o, r)
            o, r = self.execute_command("killall -9 epmd")
            self.log_command_output(o, r)

    def start_couchbase(self):
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("net start couchbaseserver")
            self.log_command_output(o, r)
        if self.info.type.lower() == "linux":
            if self.nonroot:
                self.log.info("Start Couchbase Server with non root method")
                o, r = self.execute_command('%s%scouchbase-server \-- -noinput -detached '\
                                            % (self.nr_home_path, LINUX_COUCHBASE_BIN_PATH))
                self.log_command_output(o, r)
            else:
                fv, sv, bn = self.get_cbversion("linux")
                if self.info.distribution_version.lower() in SYSTEMD_SERVER \
                        and sv in COUCHBASE_FROM_WATSON:
                    """from watson, systemd is used in centos 7, suse 12 """
                    self.log.debug("%s - Running systemd command" % self.ip)
                    o, r = self.execute_command("systemctl start couchbase-server.service")
                    self.log_command_output(o, r)
                else:
                    o, r = self.execute_command("/etc/init.d/couchbase-server start")
                    self.log_command_output(o, r)
        if self.info.distribution_type.lower() == "mac":
            o, r = self.execute_command("open /Applications/Couchbase\ Server.app")
            self.log_command_output(o, r)

    def pause_memcached(self, os="linux", timesleep=30):
        self.log.info("*** pause memcached process ***")
        if os == "windows":
            self.check_cmd("pssuspend")
            cmd = "pssuspend $(tasklist | grep  memcached | gawk '{printf $2}')"
            o, r = self.execute_command(cmd)
            self.log_command_output(o, [])
        else:
            o, r = self.execute_command("killall -SIGSTOP memcached")
            self.log_command_output(o, r)
        self.log.info("wait %s seconds to make node down." % timesleep)
        time.sleep(timesleep)

    def unpause_memcached(self, os="linux"):
        self.log.info("*** unpause memcached process ***")
        if os == "windows":
            cmd = "pssuspend -r $(tasklist | grep  memcached | gawk '{printf $2}')"
            o, r = self.execute_command(cmd)
            self.log_command_output(o, [])
        else:
            o, r = self.execute_command("killall -SIGCONT memcached")
            self.log_command_output(o, r)

    def pause_beam(self):
        o, r = self.execute_command("killall -SIGSTOP beam")
        self.log_command_output(o, r)

    def unpause_beam(self):
        o, r = self.execute_command("killall -SIGCONT beam")
        self.log_command_output(o, r)

    # TODO: Windows
    def flush_os_caches(self):
        self.extract_remote_info()
        if self.info.type.lower() == "linux":
            o, r = self.execute_command("sync")
            self.log_command_output(o, r)
            o, r = self.execute_command("/sbin/sysctl vm.drop_caches=3")
            self.log_command_output(o, r)

    def get_data_file_size(self, path=None):

        output, error = self.execute_command('du -b {0}'.format(path))
        if error:
            return 0
        else:
            for line in output:
                size = line.strip().split('\t')
                if size[0].isdigit():
                    print size[0]
                    return size[0]
                else:
                    return 0

    def get_process_statistics(self, process_name=None, process_pid=None):
        '''
        Gets process statistics for windows nodes
        WMI is required to be intalled on the node
        stats_windows_helper should be located on the node
        '''
        self.extract_remote_info()
        remote_command = "cd ~; /cygdrive/c/Python27/python stats_windows_helper.py"
        if process_name:
            remote_command.append(" " + process_name)
        elif process_pid:
            remote_command.append(" " + process_pid)

        if self.info.type.lower() == "windows":
            o, r = self.execute_command(remote_command, self.info)
            if r:
                self.log.error("Command didn't run successfully. Error: {0}".format(r))
            return o
        else:
            self.log.error("Function is implemented only for Windows OS")
            return None

    def get_process_statistics_parameter(self, parameter, process_name=None,
                                         process_pid=None):
        if not parameter:
            self.log.error("parameter cannot be None")

        parameters_list = self.get_process_statistics(process_name,
                                                      process_pid)

        if not parameters_list:
            self.log.error("no statistics found")
            return None
        parameters_dic = dict(item.split(' = ') for item in parameters_list)

        if parameter in parameters_dic:
            return parameters_dic[parameter]
        else:
            self.log.error("parameter '{0}' is not found".format(parameter))
            return None

    def set_environment_variable(self, name, value):
        """Request an interactive shell session, export custom variable and
        restart Couchbase server.

        Shell session is necessary because basic SSH client is stateless.
        """

        shell = self._ssh_client.invoke_shell()
        self.extract_remote_info()
        if self.info.type.lower() == "windows":
            shell.send('net stop CouchbaseServer\n')
            shell.send('set {0}={1}\n'.format(name, value))
            shell.send('net start CouchbaseServer\n')
        elif self.info.type.lower() == "linux":
            shell.send('export {0}={1}\n'.format(name, value))
            if "centos 7" in self.info.distribution_version.lower():
                """from watson, systemd is used in centos 7 """
                self.log.info("this node is centos 7.x")
                shell.send("systemctl restart couchbase-server.service\n")
            else:
                shell.send('/etc/init.d/couchbase-server restart\n')
        shell.close()

    def change_env_variables(self, dict):
        prefix = "\\n    "
        _, sv, _ = self.get_cbversion("linux")
        if sv in COUCHBASE_FROM_SPOCK:
            init_file = "couchbase-server"
            file_path = "/opt/couchbase/bin/"
        else:
            init_file = "couchbase_init.d"
            file_path = "/opt/couchbase/etc/"
        environmentVariables = ""
        self.extract_remote_info()
        if self.info.type.lower() == "windows":
            init_file = "service_start.bat"
            file_path = "\"/cygdrive/c/Program Files/Couchbase/Server/bin/\""
            prefix = "\\n"
        backupfile = file_path + init_file + ".bak"
        sourceFile = file_path + init_file
        o, r = self.execute_command("cp " + sourceFile + " " + backupfile)
        self.log_command_output(o, r)
        command = "sed -i 's/{0}/{0}".format("ulimit -l unlimited")
        """
        From spock, the file to edit is in /opt/couchbase/bin/couchbase-server
        """
        for key in dict.keys():
            o, r = self.execute_command("sed -i 's/{1}.*//' {0}"
                                        .format(sourceFile, key))
            self.log_command_output(o, r)
            if sv in COUCHBASE_FROM_SPOCK:
                o, r = self.execute_command("sed -i 's/export ERL_FULLSWEEP_AFTER/export ERL_FULLSWEEP_AFTER\\n{1}="
                                            "{2}\\nexport {1}/' {0}"
                                            .format(sourceFile, key, dict[key]))
                self.log_command_output(o, r)
        if self.info.type.lower() == "windows":
            command = "sed -i 's/{0}/{0}" \
                      .format("set NS_ERTS=%NS_ROOT%\erts-5.8.5.cb1\bin")

        for key in dict.keys():
            if self.info.type.lower() == "windows":
                environmentVariables += prefix + 'set {0}={1}' \
                                                 .format(key, dict[key])
            else:
                environmentVariables += prefix + 'export {0}={1}' \
                                                 .format(key, dict[key])

        command += environmentVariables + "/'" + " " + sourceFile
        o, r = self.execute_command(command)
        self.log_command_output(o, r)

        if self.info.type.lower() == "linux":
            if "centos 7" in self.info.distribution_version.lower():
                """from watson, systemd is used in centos 7 """
                self.log.info("this node is centos 7.x")
                o, r = self.execute_command("service couchbase-server restart")
                self.log_command_output(o, r)
            else:
                o, r = self.execute_command("/etc/init.d/couchbase-server restart")
                self.log_command_output(o, r)
        else:
            o, r = self.execute_command("net stop couchbaseserver")
            self.log_command_output(o, r)
            o, r = self.execute_command("net start couchbaseserver")
            self.log_command_output(o, r)

    def reset_env_variables(self):
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        """
        From spock, the file to edit is in /opt/couchbase/bin/couchbase-server
        """
        _, sv, _ = self.get_cbversion("linux")
        if sv in COUCHBASE_FROM_SPOCK:
            init_file = "couchbase-server"
            file_path = "/opt/couchbase/bin/"
        else:
            init_file = "couchbase_init.d"
            file_path = "/opt/couchbase/etc/"
        if self.info.type.lower() == "windows":
            init_file = "service_start.bat"
            file_path = "/cygdrive/c/Program\ Files/Couchbase/Server/bin/"
        backupfile = file_path + init_file + ".bak"
        sourceFile = file_path + init_file
        o, r = self.execute_command("mv " + backupfile + " " + sourceFile)
        self.log_command_output(o, r)
        if self.info.type.lower() == "linux":
            if "centos 7" in self.info.distribution_version.lower():
                """from watson, systemd is used in centos 7 """
                self.log.info("this node is centos 7.x")
                o, r = self.execute_command("service couchbase-server restart")
                self.log_command_output(o, r)
            else:
                o, r = self.execute_command("/etc/init.d/couchbase-server restart")
                self.log_command_output(o, r)
        else:
            o, r = self.execute_command("net stop couchbaseserver")
            self.log_command_output(o, r)
            o, r = self.execute_command("net start couchbaseserver")
            self.log_command_output(o, r)

    def set_node_name(self, name):
        """Edit couchbase-server shell script in place and set custom node name.
        This is necessary for cloud installations where nodes have both
        private and public addresses.

        It only works on Unix-like OS.

        Reference: http://bit.ly/couchbase-bestpractice-cloud-ip
        """

        # Stop server
        self.stop_couchbase()

        # Edit _start function
        cmd = r"sed -i 's/\(.*\-run ns_bootstrap.*\)/\1\n\t-name ns_1@{0} \\/' \
                /opt/couchbase/bin/couchbase-server".format(name)
        self.execute_command(cmd)

        # Cleanup
        for cmd in ('rm -fr /opt/couchbase/var/lib/couchbase/data/*',
                    'rm -fr /opt/couchbase/var/lib/couchbase/mnesia/*',
                    'rm -f /opt/couchbase/var/lib/couchbase/config/config.dat'):
            self.execute_command(cmd)

        # Start server
        self.start_couchbase()

    def execute_cluster_backup(self, login_info="Administrator:password",
                               backup_location="/tmp/backup",
                               command_options='', cluster_ip="",
                               cluster_port="8091", delete_backup=True):
        if self.nonroot:
            backup_command = "/home/%s%scbbackup" \
                             % (self.master.ssh_username,
                                LINUX_COUCHBASE_BIN_PATH)
        else:
            backup_command = "%scbbackup" % (LINUX_COUCHBASE_BIN_PATH)
        backup_file_location = backup_location
        # TODO: define WIN_COUCHBASE_BIN_PATH and implement a new function under RestConnectionHelper to use nodes/self info to get os info
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            backup_command = "\"%scbbackup.exe\"" % (WIN_COUCHBASE_BIN_PATH_RAW)
            backup_file_location = "C:%s" % (backup_location)
            output, error = self.execute_command("taskkill /F /T /IM cbbackup.exe")
            self.log_command_output(output, error)
        if self.info.distribution_type.lower() == 'mac':
            backup_command = "%scbbackup" % (MAC_COUCHBASE_BIN_PATH)

        command_options_string = ""
        if command_options is not '':
            if "-b" not in command_options:
                command_options_string = ' '.join(command_options)
            else:
                command_options_string = command_options
        cluster_ip = cluster_ip or self.ip
        cluster_port = cluster_port or self.port

        if '-m accu' not in command_options_string \
                and '-m diff' not in command_options_string and delete_backup:
            self.delete_files(backup_file_location)
            self.create_directory(backup_file_location)

        command = "%s %s%s@%s:%s %s %s" \
                  % (backup_command, "http://", login_info, cluster_ip,
                     cluster_port, backup_file_location,
                     command_options_string)
        if self.info.type.lower() == 'windows':
            command = "cmd /c START \"\" \"%s\" \"%s%s@%s:%s\" \"%s\" %s" \
                      % (backup_command, "http://", login_info, cluster_ip,
                         cluster_port, backup_file_location,
                         command_options_string)

        output, error = self.execute_command(command)
        self.log_command_output(output, error)

    def restore_backupFile(self, login_info, backup_location, buckets):
        restore_command = "%scbrestore" % (LINUX_COUCHBASE_BIN_PATH)
        backup_file_location = backup_location
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            restore_command = "\"%scbrestore.exe\"" \
                              % (WIN_COUCHBASE_BIN_PATH_RAW)
            backup_file_location = "C:%s" % (backup_location)
        if self.info.distribution_type.lower() == 'mac':
            restore_command = "%scbrestore" % (MAC_COUCHBASE_BIN_PATH)
        outputs = errors = []
        for bucket in buckets:
            command = "%s %s %s%s@%s:%s %s %s" \
                      % (restore_command, backup_file_location, "http://",
                         login_info, self.ip, self.port, "-b", bucket)
            if self.info.type.lower() == 'windows':
                command = "cmd /c \"%s\" \"%s\" \"%s%s@%s:%s\" %s %s" \
                          % (restore_command, backup_file_location, "http://",
                             login_info, self.ip, self.port, "-b", bucket)
            output, error = self.execute_command(command)
            self.log_command_output(output, error)
            outputs.extend(output)
            errors.extend(error)
        return outputs, errors

    def delete_files(self, file_location):
        command = "%s%s" % ("rm -rf ", file_location)
        output, error = self.execute_command(command)
        self.log_command_output(output, error)

    def get_data_map_using_cbtransfer(self, buckets, data_path=None,
                                      userId="Administrator",
                                      password="password", getReplica=False,
                                      mode="memory"):
        self.extract_remote_info()
        temp_path = "/tmp/"
        if self.info.type.lower() == 'windows':
            temp_path = WIN_TMP_PATH
        replicaOption = ""
        prefix = str(uuid.uuid1())
        fileName = prefix + ".csv"
        if getReplica:
            replicaOption = "  --source-vbucket-state=replica"

        source = "http://" + self.ip + ":8091"
        if mode == "disk":
            source = "couchstore-files://" + data_path
        elif mode == "backup":
            source = data_path
            fileName = ""
        # Initialize Output
        bucketMap = {}
        headerInfo = ""
        # Iterate per bucket and generate maps
        for bucket in buckets:
            if data_path is None:
                options = " -b " + bucket.name + " -u " + userId + " -p "+ password +" --single-node"
            else:
                options = " -b " + bucket.name + " -u " + userId + " -p" + password + replicaOption
            suffix = "_" + bucket.name + "_N%2FA.csv"
            if mode == "memory" or mode == "backup":
                suffix = "_" + bucket.name + "_" + self.ip + "%3A8091.csv"

            genFileName = prefix + suffix
            csv_path = temp_path + fileName
            if self.info.type.lower() == 'windows':
                csv_path = WIN_TMP_PATH_RAW + fileName
            path = temp_path + genFileName
            dest_path = "/tmp/" + fileName
            destination = "csv:" + csv_path
            self.log.info("Run cbtransfer to get data map")
            self.execute_cbtransfer(source, destination, options)
            file_existed = self.file_exists(temp_path, genFileName)
            if file_existed:
                self.copy_file_remote_to_local(path, dest_path)
                self.delete_files(path)
                content = []
                headerInfo = ""
                with open(dest_path) as f:
                    headerInfo = f.readline()
                    content = f.readlines()
                bucketMap[bucket.name] = content
                os.remove(dest_path)
        return headerInfo, bucketMap

    def execute_cbtransfer(self, source, destination, command_options=''):
        transfer_command = "%scbtransfer" % (LINUX_COUCHBASE_BIN_PATH)
        if self.nonroot:
            transfer_command = '/home/%s%scbtransfer' \
                               % (self.username, LINUX_COUCHBASE_BIN_PATH)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            transfer_command = "\"%scbtransfer.exe\"" \
                               % (WIN_COUCHBASE_BIN_PATH_RAW)
        if self.info.distribution_type.lower() == 'mac':
            transfer_command = "%scbtransfer" % (MAC_COUCHBASE_BIN_PATH)

        command = "%s %s %s %s" % (transfer_command, source, destination,
                                   command_options)
        if self.info.type.lower() == 'windows':
            command = "cmd /c \"%s\" \"%s\" \"%s\" %s" \
                      % (transfer_command, source, destination,
                         command_options)
        output, error = self.execute_command(command,
                                             use_channel=True)
        self.log_command_output(output, error)
        self.log.info("done execute cbtransfer")
        return output

    def execute_cbdocloader(self, username, password, bucket, memory_quota,
                            file):
        f, s, b = self.get_cbversion(self.info.type.lower())
        cbdocloader_command = "%scbdocloader" % (LINUX_COUCHBASE_BIN_PATH)
        cluster_flag = "-n"
        bucket_quota_flag = "-s"
        data_set_location_flag = " "
        if f[:5] in COUCHBASE_FROM_SPOCK:
            cluster_flag = "-c"
            bucket_quota_flag = "-m"
            data_set_location_flag = "-d"
        linux_couchbase_path = LINUX_CB_PATH
        if self.nonroot:
            cbdocloader_command = "/home/%s%scbdocloader" \
                                  % (self.username, LINUX_COUCHBASE_BIN_PATH)
            linux_couchbase_path = "/home/%s%s" % (self.username,
                                                   LINUX_CB_PATH)
        self.extract_remote_info()
        command = "%s -u %s -p %s %s %s:%s -b %s %s %s %s %ssamples/%s.zip" \
                  % (cbdocloader_command, username, password, cluster_flag,
                     self.ip, self.port, bucket, bucket_quota_flag,
                     memory_quota, data_set_location_flag,
                     linux_couchbase_path, file)
        if self.info.distribution_type.lower() == 'mac':
            cbdocloader_command = "%scbdocloader" % (MAC_COUCHBASE_BIN_PATH)
            command = "%s -u %s -p %s %s %s:%s -b %s %s %s %s %ssamples/%s.zip" \
                      % (cbdocloader_command, username, password, cluster_flag,
                         self.ip, self.port, bucket, bucket_quota_flag,
                         memory_quota, data_set_location_flag, MAC_CB_PATH,
                         file)

        if self.info.type.lower() == 'windows':
            cbdocloader_command = "%scbdocloader.exe" % WIN_COUCHBASE_BIN_PATH
            WIN_COUCHBASE_SAMPLES_PATH = "C:/Program\ Files/Couchbase/Server/samples/"
            command = "%s -u %s -p %s %s %s:%s -b %s %s %s %s %s%s.zip" \
                      % (cbdocloader_command, username, password, cluster_flag,
                         self.ip, self.port, bucket, bucket_quota_flag,
                         memory_quota, data_set_location_flag,
                         WIN_COUCHBASE_SAMPLES_PATH, file)
        output, error = self.execute_command(command)
        self.log_command_output(output, error)
        return output, error

    def execute_cbcollect_info(self, file, options=""):
        cbcollect_command = "%scbcollect_info" % (LINUX_COUCHBASE_BIN_PATH)
        if self.nonroot:
            cbcollect_command = "/home/%s%scbcollect_info" \
                                % (self.username, LINUX_COUCHBASE_BIN_PATH)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            cbcollect_command = "%scbcollect_info.exe" % WIN_COUCHBASE_BIN_PATH
        if self.info.distribution_type.lower() == 'mac':
            cbcollect_command = "%scbcollect_info" % (MAC_COUCHBASE_BIN_PATH)

        command = "%s %s %s" % (cbcollect_command, file, options)
        output, error = self.execute_command(command, use_channel=True)
        return output, error

    # works for linux only
    def execute_couch_dbinfo(self, file):
        couch_dbinfo_command = "%scouch_dbinfo" % (LINUX_COUCHBASE_BIN_PATH)
        cb_data_path = "/"
        if self.nonroot:
            couch_dbinfo_command = "/home/%s%scouch_dbinfo" \
                                   % (self.username, LINUX_COUCHBASE_BIN_PATH)
            cb_data_path = "/home/%s/" % self.username
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            couch_dbinfo_command = "%scouch_dbinfo.exe" \
                                   % (WIN_COUCHBASE_BIN_PATH)
        if self.info.distribution_type.lower() == 'mac':
            couch_dbinfo_command = "%scouch_dbinfo" % (MAC_COUCHBASE_BIN_PATH)

        command = couch_dbinfo_command + ' -i %sopt/couchbase/var/lib/couchbase/data/*/*[0-9] >' \
                                         % cb_data_path + file
        output, error = self.execute_command(command, use_channel=True)
        self.log_command_output(output, error)
        return output, error

    def execute_cbepctl(self, bucket, persistence, param_type, param, value,
                        cbadmin_user="cbadminbucket",
                        cbadmin_password="password"):
        cbepctl_command = "%scbepctl" % (LINUX_COUCHBASE_BIN_PATH)
        if self.nonroot:
            cbepctl_command = "/home/%s%scbepctl" % (self.username,
                                                     LINUX_COUCHBASE_BIN_PATH)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            cbepctl_command = "%scbepctl.exe" % (WIN_COUCHBASE_BIN_PATH)
        if self.info.distribution_type.lower() == 'mac':
            cbepctl_command = "%scbepctl" % (MAC_COUCHBASE_BIN_PATH)

        if bucket.saslPassword is None:
            bucket.saslPassword = ''
        if persistence != "":
            command = "%s %s:11210  -u %s -p %s -b %s %s" \
                      % (cbepctl_command, self.ip, cbadmin_user,
                         cbadmin_password, bucket.name, persistence)
        else:
            command = "%s %s:11210 -u %s -p %s -b %s %s %s %s" \
                      % (cbepctl_command, self.ip, cbadmin_user,
                         cbadmin_password, bucket.name, param_type, param,
                         value)
        output, error = self.execute_command(command)
        self.log_command_output(output, error)
        return output, error

    def execute_cbvdiff(self, bucket, node_str, password=None):
        cbvdiff_command = "%scbvdiff" % (LINUX_COUCHBASE_BIN_PATH)
        if self.nonroot:
            cbvdiff_command = "/home/%s%scbvdiff" % (self.username,
                                                     LINUX_COUCHBASE_BIN_PATH)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            cbvdiff_command = "%scbvdiff.exe" % (WIN_COUCHBASE_BIN_PATH)
        if self.info.distribution_type.lower() == 'mac':
            cbvdiff_command = "%scbvdiff" % (MAC_COUCHBASE_BIN_PATH)

        if not password:
            command = "%s -b %s %s " % (cbvdiff_command, bucket.name, node_str)
        else:
            command = "%s -b %s -p %s %s " % (cbvdiff_command, bucket.name,
                                              password, node_str)
        output, error = self.execute_command(command)
        self.log_command_output(output, error)
        return output, error

    def couchbase_cli(self, subcommand, cluster_host, options):
        cb_client = "%scouchbase-cli" % (LINUX_COUCHBASE_BIN_PATH)
        if self.nonroot:
            cb_client = "/home/%s%scouchbase-cli" \
                        % (self.username, LINUX_COUCHBASE_BIN_PATH)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            cb_client = "%scouchbase-cli.exe" % (WIN_COUCHBASE_BIN_PATH)
        if self.info.distribution_type.lower() == 'mac':
            cb_client = "%scouchbase-cli" % (MAC_COUCHBASE_BIN_PATH)

        # now we can run command in format where all parameters are optional
        # {PATH}/couchbase-cli [SUBCOMMAND] [OPTIONS]
        cluster_param = " -c {0}".format(cluster_host)
        command = cb_client + " " + subcommand + " " + cluster_param + " " + options

        output, error = self.execute_command(command, use_channel=True)
        self.log_command_output(output, error)
        return output, error

    def execute_couchbase_cli(self, cli_command, cluster_host='localhost',
                              options='', cluster_port=None,
                              user='Administrator', password='password'):
        cb_client = "%scouchbase-cli" % (LINUX_COUCHBASE_BIN_PATH)
        if self.nonroot:
            cb_client = "/home/%s%scouchbase-cli" \
                        % (self.username, LINUX_COUCHBASE_BIN_PATH)
        self.extract_remote_info()
        f, s, b = self.get_cbversion("unix")
        if self.info.type.lower() == 'windows':
            f, s, b = self.get_cbversion("windows")
            cb_client = "%scouchbase-cli.exe" % (WIN_COUCHBASE_BIN_PATH)
        if self.info.distribution_type.lower() == 'mac':
            cb_client = "%scouchbase-cli" % (MAC_COUCHBASE_BIN_PATH)

        cluster_param = (" -c http://{0}".format(cluster_host),
                         "")[cluster_host is None]
        if cluster_param is not None:
            cluster_param += (":{0}".format(cluster_port), "")[cluster_port is None]

        user_param = (" -u {0}".format(user), "")[user is None]
        passwd_param = (" -p {0}".format(password), "")[password is None]
        if f[:5] in COUCHBASE_FROM_SPOCK and cli_command == "cluster-init":
            user_param = (" --cluster-username {0}".format(user), "")[user is None]
            passwd_param = (" --cluster-password {0}".format(password), "")[password is None]
        # now we can run command in format where all parameters are optional
        # {PATH}/couchbase-cli [COMMAND] [CLUSTER:[PORT]] [USER] [PASWORD] [OPTIONS]
        command = cb_client + " " + cli_command + cluster_param + user_param + passwd_param + " " + options
        self.log.info("command to run: {0}".format(command))
        output, error = self.execute_command(command, debug=False, use_channel=True)
        return output, error

    def get_cluster_certificate_info(self, bin_path, cert_path,
                                     user, password, server_cert):
        """
            Get certificate info from cluster and store it in tmp dir
        """
        cert_file_location = cert_path + "cert.pem"
        cmd = "%s/couchbase-cli ssl-manage -c %s:8091 -u %s -p %s "\
              " --cluster-cert-info > %s" % (bin_path, server_cert.ip,
                                             user, password,
                                             cert_file_location)
        output, _ = self.execute_command(cmd)
        if output and "Error" in output[0]:
            raise("Failed to get CA certificate from cluster.")
        return cert_file_location

    def execute_cbworkloadgen(self, username, password, num_items, ratio,
                              bucket, item_size, command_options):
        cbworkloadgen_command = "%scbworkloadgen" % (LINUX_COUCHBASE_BIN_PATH)
        if self.nonroot:
            cbworkloadgen_command = "/home/%s%scbworkloadgen" \
                                    % (self.username, LINUX_COUCHBASE_BIN_PATH)
        self.extract_remote_info()

        if self.info.distribution_type.lower() == 'mac':
            cbworkloadgen_command = "%scbworkloadgen" \
                                    % (MAC_COUCHBASE_BIN_PATH)

        if self.info.type.lower() == 'windows':
            cbworkloadgen_command = "%scbworkloadgen.exe" \
                                    % (WIN_COUCHBASE_BIN_PATH)

        command = "%s -n %s:%s -r %s -i %s -b %s -s %s %s -u %s -p %s" \
                  % (cbworkloadgen_command, self.ip, self.port, ratio,
                     num_items, bucket, item_size, command_options,
                     username, password)

        output, error = self.execute_command(command)
        self.log_command_output(output, error)
        return output, error

    def execute_cbhealthchecker(self, username, password, command_options=None,
                                path_to_store=''):
        command_options_string = ""
        if command_options is not None:
            command_options_string = ' '.join(command_options)

        cbhealthchecker_command = "%scbhealthchecker" \
                                  % (LINUX_COUCHBASE_BIN_PATH)
        if self.info.distribution_type.lower() == 'mac':
            cbhealthchecker_command = "%scbhealthchecker" \
                                      % (MAC_COUCHBASE_BIN_PATH)

        if self.info.type.lower() == 'windows':
            cbhealthchecker_command = "%scbhealthchecker" \
                                      % (WIN_COUCHBASE_BIN_PATH)

        if path_to_store:
            self.execute_command('rm -rf %s; mkdir %s;cd %s'
                                 % (path_to_store, path_to_store,
                                    path_to_store))

        command = "%s -u %s -p %s -c %s:%s %s" \
                  % (cbhealthchecker_command, username, password, self.ip,
                     self.port, command_options_string)

        if path_to_store:
            command = "cd %s; %s -u %s -p %s -c %s:%s %s" \
                      % (path_to_store, cbhealthchecker_command, username,
                         password, self.ip, self.port, command_options_string)

        output, error = self.execute_command_raw_jsch(command)
        self.log_command_output(output, error)
        return output, error

    def execute_vbuckettool(self, keys, prefix=None):
        command = "%stools/vbuckettool" % (LINUX_COUCHBASE_BIN_PATH)
        if self.nonroot:
            command = "/home/%s%stools/vbuckettool" \
                      % (self.username, LINUX_COUCHBASE_BIN_PATH)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            command = "%stools/vbuckettool.exe" % (WIN_COUCHBASE_BIN_PATH)
        if prefix:
            command = prefix + command
        command = "%s - %s" % (command, ' '.join(keys))
        output, error = self.execute_command(command, use_channel=True)
        self.log_command_output(output, error)
        return output, error

    def execute_batch_command(self, command):
        remote_command = \
            "echo \"{0}\" > /tmp/cmd.bat; chmod u=rwx /tmp/cmd.bat; /tmp/cmd.bat" \
            .format(command)
        o, r = self.execute_command_raw_jsch(remote_command)
        if r and r != ['']:
            self.log.error("Command didn't run successfully. Error: {0}".format(r))
        return o, r

    def remove_win_backup_dir(self):
        win_paths = [WIN_CB_PATH, WIN_MB_PATH]
        for each_path in win_paths:
            backup_files = []
            files = self.list_files(each_path)
            for f in files:
                if f["file"].startswith("backup-"):
                    backup_files.append(f["file"])
            # keep the last one
            if len(backup_files) > 2:
                self.log.info("start remove previous backup directory")
                for f in backup_files[:-1]:
                    self.execute_command("rm -rf '{0}{1}'".format(each_path, f))

    def remove_win_collect_tmp(self):
        win_tmp_path = WIN_TMP_PATH
        self.log.info("start remove tmp files from directory %s" % win_tmp_path)
        self.execute_command("rm -rf '%stmp*'" % win_tmp_path)

    # ps_name_or_id means process name or ID will be suspended
    def windows_process_utils(self, ps_name_or_id, cmd_file_name, option=""):
        success = False
        files_path = "cygdrive/c/utils/suspend/"
        # check to see if suspend files exist in server
        file_existed = self.file_exists(files_path, cmd_file_name)
        if file_existed:
            command = "{0}{1} {2} {3}" \
                      .format(files_path, cmd_file_name, option, ps_name_or_id)
            o, r = self.execute_command(command)
            if not r:
                success = True
                self.log_command_output(o, r)
                self.sleep(30, "Wait for windows to execute completely")
            else:
                self.log.error("Command didn't run successfully. Error: {0}"
                          .format(r))
        else:
            o, r = self.execute_command("netsh advfirewall firewall add rule name=\"block erl.exe in\" dir=in action=block program=\"%ProgramFiles%\Couchbase\Server\\bin\erl.exe\"")
            if not r:
                success = True
                self.log_command_output(o, r)
            o, r = self.execute_command("netsh advfirewall firewall add rule name=\"block erl.exe out\" dir=out action=block program=\"%ProgramFiles%\Couchbase\Server\\bin\erl.exe\"")
            if not r:
                success = True
                self.log_command_output(o, r)
        return success

    def check_cmd(self, cmd):
        """
           Some tests need some commands to run.
           Copy or install command in server if they are not available
        """
        self.log.info("\n---> Run command %s to check if it is ready on server %s"
                 % (cmd, self.ip))
        out, err = self.execute_command(cmd)
        found_command = False
        download_command = ""
        command_output = ""
        if self.info.type.lower() == 'windows':
            wget = "/cygdrive/c/automation/wget.exe"
            if cmd == "unzip":
                download_command = WIN_UNZIP
                command_output = "UnZip 5.52 of 28 February 2005, by Info-ZIP"
                if out and command_output in out[0]:
                    self.log.info("unzip command is ready")
                    found_command = True
            if cmd == "pssuspend":
                download_command = WIN_PSSUSPEND
                command_output = "PsSuspend suspends or resumes processes"
                if out and command_output in out[0]:
                    self.log.info("PsSuspend command is ready to run")
                    found_command = True
            if not found_command and err and "command not found" in err[0]:
                self.execute_command("cd /bin; %s --no-check-certificate -q %s "
                                     % (wget, download_command))
                out, err = self.execute_command(cmd)
                if out and command_output in out[0]:
                    self.log.info("%s command is ready" % cmd)
                else:
                    mesg = "Failed to download %s " % cmd
                    self.stop_current_python_running(mesg)
        elif self.info.distribution_type.lower() == 'centos':
            if cmd == "unzip":
                command_output = "UnZip 6.00 of 20 April 2009"
                if out and command_output in out[0]:
                    self.log.info("unzip command is ready")
                    found_command = True
            if not found_command and err and "command not found" in err[0]:
                self.execute_command("yum install -y unzip")
                out, err = self.execute_command(cmd)
                if out and command_output in out[0]:
                    self.log.info("%s command is ready" % cmd)
                else:
                    mesg = "Failed to install %s " % cmd
                    self.stop_current_python_running(mesg)

    def check_openssl_version(self, deliverable_type, openssl, version):

        if "SUSE" in self.info.distribution_type:
            o, r = self.execute_command("zypper -n if openssl 2>/dev/null| grep -i \"Installed: Yes\"")
            self.log_command_output(o, r)

            if o == "":
                o, r = self.execute_command("zypper -n in openssl")
                self.log_command_output(o, r)
                o, r = self.execute_command("zypper -n if openssl 2>/dev/null| grep -i \"Installed: Yes\"")
                self.log_command_output(o, r)
                if o == "":
                    self.log.error("Could not install openssl in opensuse/SUSE")
        sherlock = ["3.5", "4.0"]
        if version[:3] not in sherlock and "SUSE" not in self.info.distribution_type:
            if self.info.deliverable_type == "deb":
                ubuntu_version = ["12.04", "13.04"]
                o, r = self.execute_command("lsb_release -r")
                self.log_command_output(o, r)
                if o[0] != "":
                    o = o[0].split(":")
                    if o[1].strip() in ubuntu_version and "1" in openssl:
                        o, r = self.execute_command("dpkg --get-selections | grep libssl")
                        self.log_command_output(o, r)
                        for s in o:
                            if "libssl0.9.8" in s:
                                o, r = self.execute_command("apt-get --purge remove -y {0}"
                                                            .format(s[:11]))
                                self.log_command_output(o, r)
                                o, r = self.execute_command("dpkg --get-selections | grep libssl")
                                self.log.info("package {0} should not appear below"
                                         .format(s[:11]))
                                self.log_command_output(o, r)
                    elif openssl == "":
                        o, r = self.execute_command("dpkg --get-selections | grep libssl")
                        self.log_command_output(o, r)
                        if not o:
                            # CBQE-36124: SSL stuff which is not needed anymore
                            pass
                            # o, r = self.execute_command("apt-get install -y libssl0.9.8")
                            # self.log_command_output(o, r)
                            # o, r = self.execute_command("dpkg --get-selections | grep libssl")
                            # self.log.info("package {0} should not appear below".format(o[:11]))
                            # self.log_command_output(o, r)
                        elif o:
                            for s in o:
                                if "libssl0.9.8" not in s:
                                    o, r = self.execute_command("apt-get install -y libssl0.9.8")
                                    self.log_command_output(o, r)
                                    o, r = self.execute_command("dpkg --get-selections | grep libssl")
                                    self.log.info("package {0} should not appear below".format(s[:11]))
                                    self.log_command_output(o, r)

    def check_pkgconfig(self, deliverable_type, openssl):
        if "SUSE" in self.info.distribution_type:
            o, r = self.execute_command("zypper -n if pkg-config 2>/dev/null| grep -i \"Installed: Yes\"")
            self.log_command_output(o, r)
            if o == "":
                o, r = self.execute_command("zypper -n in pkg-config")
                self.log_command_output(o, r)
                o, r = self.execute_command("zypper -n if pkg-config 2>/dev/null| grep -i \"Installed: Yes\"")
                self.log_command_output(o, r)
                if o == "":
                    self.log.error("Could not install pkg-config in suse")
        else:
            if self.info.deliverable_type == "rpm":
                centos_version = ["6.4", "6.5"]
                o, r = self.execute_command("cat /etc/redhat-release")
                self.log_command_output(o, r)

                if o is None:
                    # This must be opensuse, hack for now....
                    o, r = self.execute_command("cat /etc/SuSE-release")
                    self.log_command_output(o, r)
                if o[0] != "":
                    o = o[0].split(" ")
                    if o[2] in centos_version:
                        o, r = self.execute_command("rpm -qa | grep pkgconfig")
                        self.log_command_output(o, r)
                        if not o:
                            o, r = self.execute_command("yum install -y pkgconfig")
                            self.log_command_output(o, r)
                            o, r = self.execute_command("rpm -qa | grep pkgconfig")
                            self.log.info("package pkgconfig should appear below")
                            self.log_command_output(o, r)
                        elif o:
                            for s in o:
                                if "pkgconfig" not in s:
                                    o, r = self.execute_command("yum install -y pkgconfig")
                                    self.log_command_output(o, r)
                                    o, r = self.execute_command("rpm -qa | grep pkgconfig")
                                    self.log.info("package pkgconfig should appear below")
                                    self.log_command_output(o, r)
                    else:
                        self.log.info("no need to install pkgconfig")

    def check_man_page(self):
        self.log.info("check if man installed on vm?")
        info_cm = self.extract_remote_info()
        man_installed = False
        if info_cm.type.lower() == "linux" and not self.nonroot:
            if "centos 7" in info_cm.distribution_version.lower():
                out_cm, err_cm = self.execute_command("rpm -qa | grep 'man-db'")
                if out_cm:
                    man_installed = True
            elif "centos 6" in info_cm.distribution_version.lower():
                out_cm, err_cm = self.execute_command("rpm -qa | grep 'man-1.6'")
                if out_cm:
                    man_installed = True
            if not man_installed:
                self.log.info("man page does not install man page on vm %s"
                         " Let do install it now" % self.ip)
                self.execute_command("yum install -y man")

    def install_missing_lib(self):
        if self.info.deliverable_type == "deb":
            for lib_name in MISSING_UBUNTU_LIB:
                if lib_name != "":
                    self.log.info("prepare install library {0}".format(lib_name))
                    o, r = self.execute_command("apt-get install -y {0}"
                                                .format(lib_name))
                    self.log_command_output(o, r)
                    o, r = self.execute_command("dpkg --get-selections | grep {0}"
                                                .format(lib_name))
                    self.log_command_output(o, r)
                    self.log.info("lib {0} should appear around this line"
                             .format(lib_name))

    def is_enterprise(self, os_name):
        enterprise = False
        if os_name != 'windows':
            if self.file_exists("/opt/couchbase/etc/", "runtime.ini"):
                output = self.read_remote_file("/opt/couchbase/etc/", "runtime.ini")
                for x in output:
                    x = x.strip()
                    if x and "license = enterprise" in x:
                        enterprise = True
            else:
                self.log.info("couchbase server at {0} may not installed yet"
                         .format(self.ip))
        else:
            self.log.info("only check cb edition in unix enviroment")
        return enterprise

    def get_cbversion(self, os_name):
        """ fv = a.b.c-xxxx, sv = a.b.c, bn = xxxx """
        fv = sv = bn = tmp = ""
        nonroot_path_start = ""
        output = ""
        if not self.nonroot:
            nonroot_path_start = "/"
        if os_name != 'windows':
            if self.nonroot:
                if self.file_exists('/home/%s%s' % (self.username, LINUX_CB_PATH), VERSION_FILE):
                    output = self.read_remote_file(
                        '/home/%s%s' % (self.username, LINUX_CB_PATH),
                        VERSION_FILE)
                else:
                    self.log.info("couchbase server at {0} may not installed yet"
                             .format(self.ip))
            elif os_name == "mac":
                if self.file_exists(MAC_CB_PATH, VERSION_FILE):
                    output = self.read_remote_file(MAC_CB_PATH, VERSION_FILE)
            else:
                if self.file_exists(LINUX_CB_PATH, VERSION_FILE):
                    output = self.read_remote_file(LINUX_CB_PATH, VERSION_FILE)
                else:
                    self.log.info("couchbase server at {0} may not installed yet"
                             .format(self.ip))
        elif os_name == "windows":
            if self.file_exists(WIN_CB_PATH, VERSION_FILE):
                output = self.read_remote_file(WIN_CB_PATH, VERSION_FILE)
            else:
                self.log.info("couchbase server at {0} may not installed yet"
                         .format(self.ip))
        if output:
            for x in output:
                x = x.strip()
                if x and x[:5] in COUCHBASE_VERSIONS and "-" in x:
                    fv = x
                    tmp = x.split("-")
                    sv = tmp[0]
                    bn = tmp[1]
                break
        return fv, sv, bn

    def set_cbauth_env(self, server):
        """ from Watson, we need to set cbauth environment variables
            so cbq could connect to the host """
        ready = RestHelper(RestConnection(server)).is_ns_server_running(300)
        if ready:
            version = RestConnection(server).get_nodes_version()
        else:
            sys.exit("*****  Node %s failed to up in 5 mins **** " % server.ip)

        if version[:5] in COUCHBASE_FROM_WATSON:
            try:
                if self.input.membase_settings.rest_username:
                    rest_username = self.input.membase_settings.rest_username
                else:
                    self.log.info("** You need to set rest username at ini file **")
                    rest_username = "Administrator"
                if self.input.membase_settings.rest_password:
                    rest_password = self.input.membase_settings.rest_password
                else:
                    self.log.info("** You need to set rest password at ini file **")
                    rest_password = "password"
            except Exception, ex:
                if ex:
                    print ex
                pass
            self.extract_remote_info()
            if self.info.type.lower() != 'windows':
                self.log.info("***** set NS_SERVER_CBAUTH env in linux *****")
                # self.execute_command("/etc/init.d/couchbase-server stop")
                # self.sleep(15)
                self.execute_command('export NS_SERVER_CBAUTH_URL='
                                     '"http://{0}:8091/_cbauth"'
                                     .format(server.ip), debug=False)
                self.execute_command('export NS_SERVER_CBAUTH_USER="{0}"'
                                     .format(rest_username), debug=False)
                self.execute_command('export NS_SERVER_CBAUTH_PWD="{0}"'
                                     .format(rest_password), debug=False)
                self.execute_command('export NS_SERVER_CBAUTH_RPC_URL='
                                     '"http://{0}:8091/cbauth-demo"'.format(server.ip),
                                     debug=False)
                self.execute_command('export CBAUTH_REVRPC_URL='
                                     '"http://{0}:{1}@{2}:8091/query"'\
                                     .format(rest_username, rest_password,server.ip),
                                     debug=False)

    def change_system_time(self, time_change_in_seconds):
        # note that time change may be positive or negative
        # need to support Windows too
        output, error = self.execute_command("date +%s")
        if len(error) > 0:
            return False
        curr_time = int(output[-1])
        new_time = curr_time + time_change_in_seconds

        output, error = self.execute_command("date --date @" + str(new_time))
        if len(error) > 0:
            return False

        output, error = self.execute_command("date --set='" + output[-1] + "'")
        if len(error) > 0:
            return False
        else:
            return True

    def stop_current_python_running(self, mesg):
        os.system("ps aux | grep python | grep %d " % os.getpid())
        print mesg
        self.sleep(5, "Delay kill pid %d in 5 seconds to printout message"
                      % os.getpid())
        os.system('kill %d' % os.getpid())

    def enable_diag_eval_on_non_local_hosts(self, state=True):
        """
        Enable diag/eval to be run on non-local hosts.
        :return: Command output and error if any.
        """
        if self.input.membase_settings.rest_username:
            rest_username = self.input.membase_settings.rest_username
        else:
            self.log.info("*** You need to set rest username at ini file ***")
            rest_username = "Administrator"
        if self.input.membase_settings.rest_password:
            rest_password = self.input.membase_settings.rest_password
        else:
            self.log.info("*** You need to set rest password at ini file ***")
            rest_password = "password"
        command = "curl http://{0}:{1}@localhost:{2}/diag/eval -X POST -d " \
                  "'ns_config:set(allow_nonlocal_eval, {3}).'" \
                  .format(rest_username, rest_password, self.port,
                          state.__str__().lower())
        server = {"ip": self.ip, "username": rest_username,
                  "password": rest_password, "port": self.port}
        rest_connection = RestConnection(server)
        is_cluster_compatible = rest_connection.check_cluster_compatibility("5.5")
        if (not is_cluster_compatible):
            self.log.info("Enabling diag/eval on non-local hosts is available only post 5.5.2 or 6.0 releases")
            return None, "Enabling diag/eval on non-local hosts is available only post 5.5.2 or 6.0 releases"
        output, error = self.execute_command(command)
        return output, error

    def give_directory_permissions_to_couchbase(self, location):
        """
        Change the directory permission of the location mentioned
        to include couchbase as the user
        :param location: Directory location whoes permissions has to be changed
        :return: Nothing
        """
        command = "chown 'couchbase' {0}".format(location)
        output, error = self.execute_command(command)
        command = "chmod 777 {0}".format(location)
        output, error = self.execute_command(command)

    def create_new_partition(self, location, size=None):
        """
        Create a new partition at the location specified and of
        the size specified
        :param location: Location to create the new partition at.
        :param size: Size of the partition in MB
        :return: Nothing
        """
        command = "umount -l {0}".format(location)
        output, error = self.execute_command(command)
        command = "rm -rf {0}".format(location)
        output, error = self.execute_command(command)
        command = "rm -rf /usr/disk-img/disk-quota.ext3"
        output, error = self.execute_command(command)
        command = "mkdir -p {0}".format(location)
        output, error = self.execute_command(command)
        if size:
            count = (size * 1024 * 1024) / 512
        else:
            count = (5 * 1024 * 1024 * 1024) / 512
        command = "mkdir -p /usr/disk-img"
        output, error = self.execute_command(command)
        command = "dd if=/dev/zero of=/usr/disk-img/disk-quota.ext3 count={0}".format(count)
        output, error = self.execute_command(command)
        command = "/sbin/mkfs -t ext3 -q /usr/disk-img/disk-quota.ext3 -F"
        output, error = self.execute_command(command)
        command = "mount -o loop,rw,usrquota,grpquota /usr/disk-img/disk-quota.ext3 {0}".format(location)
        output, error = self.execute_command(command)
        command = "chown 'couchbase' {0}".format(location)
        output, error = self.execute_command(command)
        command = "chmod 777 {0}".format(location)
        output, error = self.execute_command(command)

    def mount_partition(self, location):
        """
        Mount a partition at the location specified
        :param location: Mount location
        :return: Output and error message from the mount command
        """
        command = "mount -o loop,rw,usrquota,grpquota /usr/disk-img/disk-quota.ext3 {0}; df -Th".format(location)
        output, error = self.execute_command(command)
        return output, error

    def unmount_partition(self, location):
        """
        Unmount the partition at the specified location.
        :param location: Location of the partition which has to be unmounted
        :return: Output and error message from the umount command
        """
        command = "umount -l {0}; df -Th".format(location)
        output, error = self.execute_command(command)
        return output, error

    def fill_disk_space(self, location, size):
        """
        Fill up the disk fully at the location specified.
        This method creates a junk file of the specified size in the location specified
        :param location: Location to fill the disk
        :param size: Size of disk space to fill up, in MB
        :return: Output and error message from filling up the disk.
        """
        count = (size * 1024 * 1024) / 512
        command = "dd if=/dev/zero of={0}/disk-quota.ext3 count={1}; df -Th".format(location, count)
        output, error = self.execute_command(command)
        return output, error

    def update_dist_type(self):
        output, error = self.execute_command("echo '{{dist_type,inet6_tcp}}.' > {0}".format(LINUX_DIST_CONFIG))
        self.log_command_output(output, error)


class RemoteUtilHelper(object):

    @staticmethod
    def enable_firewall(server, bidirectional=False, xdcr=False):
        """ Check if user is root or non root in unix """
        shell = RemoteMachineShellConnection(server)
        shell.info = shell.extract_remote_info()
        if shell.info.type.lower() == "windows":
            o, r = shell.execute_command('netsh advfirewall set publicprofile state on')
            shell.log_command_output(o, r)
            o, r = shell.execute_command('netsh advfirewall set privateprofile state on')
            shell.log_command_output(o, r)
            shell.test_log.debug("Enabled firewall on {0}".format(server))
            suspend_erlang = shell.windows_process_utils("pssuspend.exe",
                                                         "erl.exe", option="")
            if suspend_erlang:
                shell.test_log.log.debug("%s - Erlang process is suspended"
                                         % shell.ip)
            else:
                shell.test_log.log.error("%s - Erlang process failed to suspend"
                                         % shell.ip)
        else:
            copy_server = copy.deepcopy(server)
            command_1 = "/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:65535 -j REJECT"
            command_2 = "/sbin/iptables -A OUTPUT -p tcp -o eth0 --sport 1000:65535 -j REJECT"
            command_3 = "/sbin/iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT"
            if shell.info.distribution_type.lower() in LINUX_DISTRIBUTION_NAME \
                    and server.ssh_username != "root":
                copy_server.ssh_username = "root"
                shell.disconnect()
                shell.test_log.info("%s - Connect to server as %s "
                                    % (shell.ip, copy_server.ssh_username))
                shell = RemoteMachineShellConnection(copy_server)
                o, r = shell.execute_command("whoami")
                shell.log_command_output(o, r)
            # Reject incoming connections on port 1000->65535
            o, r = shell.execute_command(command_1)
            shell.log_command_output(o, r)
            # Reject outgoing connections on port 1000->65535
            if bidirectional:
                o, r = shell.execute_command(command_2)
                shell.log_command_output(o, r)
            if xdcr:
                o, r = shell.execute_command(command_3)
                shell.log_command_output(o, r)
            shell.test_log.debug("%s - Enabled firewall" % shell.ip)
            o, r = shell.execute_command("/sbin/iptables --list")
            shell.log_command_output(o, r)
        shell.disconnect()

    @staticmethod
    def common_basic_setup(servers):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            shell.start_couchbase()
            shell.disable_firewall()
            shell.unpause_memcached()
            shell.unpause_beam()
            shell.disconnect()
        time.sleep(10)

    @staticmethod
    def use_hostname_for_server_settings(server):
        shell = RemoteMachineShellConnection(server)
        info = shell.extract_remote_info()
        version = RestConnection(server).get_nodes_self().version
        time.sleep(5)
        hostname = shell.get_full_hostname()
        if version.startswith("1.8.") or version.startswith("2.0"):
            shell.stop_couchbase()
            if info.type.lower() == "windows":
                cmd = "'C:/Program Files/Couchbase/Server/bin/service_unregister.bat'"
                shell.execute_command_raw_jsch(cmd)
                cmd = 'cat "C:\\Program Files\\Couchbase\\Server\\bin\\service_register.bat"'
                old_reg = shell.execute_command_raw_jsch(cmd)
                new_start = '\n'.join(old_reg[0]).replace("ns_1@%IP_ADDR%", "ns_1@%s" % hostname)
                cmd = "echo '%s' > 'C:\\Program Files\\Couchbase\\Server\\bin\\service_register.bat'" % new_start
                shell.execute_command_raw_jsch(cmd)
                cmd = "'C:/Program Files/Couchbase/Server/bin/service_register.bat'"
                shell.execute_command_raw_jsch(cmd)
                cmd = 'rm -rf  "C:/Program Files/Couchbase/Server/var/lib/couchbase/mnesia/*"'
                shell.execute_command_raw_jsch(cmd)
            else:
                cmd = "cat  /opt/couchbase/bin/couchbase-server"
                old_start = shell.execute_command_raw_jsch(cmd)
                cmd = r"sed -i 's/\(.*\-run ns_bootstrap.*\)/\1\n\t-name ns_1@{0} \\/' \
                        /opt/couchbase/bin/couchbase-server".format(hostname)
                o, r = shell.execute_command(cmd)
                shell.log_command_output(o, r)
                time.sleep(2)
                cmd = 'rm -rf /opt/couchbase/var/lib/couchbase/data/*'
                shell.execute_command(cmd)
                cmd = 'rm -rf /opt/couchbase/var/lib/couchbase/mnesia/*'
                shell.execute_command(cmd)
                cmd = 'rm -rf /opt/couchbase/var/lib/couchbase/config/config.dat'
                shell.execute_command(cmd)
                cmd = 'echo "%s" > /opt/couchbase/var/lib/couchbase/ip' % hostname
                shell.execute_command(cmd)
        else:
            RestConnection(server).rename_node(hostname)
            shell.stop_couchbase()
        shell.start_couchbase()
        shell.disconnect()
        return hostname

    @staticmethod
    def is_text_present_in_logs(server, text_for_search,
                                logs_to_check='debug'):
        shell = RemoteMachineShellConnection(server)
        info = shell.extract_remote_info()
        if info.type.lower() != "windows":
            path_to_log = LINUX_COUCHBASE_LOGS_PATH
        else:
            path_to_log = WIN_COUCHBASE_LOGS_PATH
        log_files = []
        files = shell.list_files(path_to_log)
        for f in files:
            if f["file"].startswith(logs_to_check):
                log_files.append(f["file"])
        is_txt_found = False
        for f in log_files:
            o, r = shell.execute_command("cat {0}/{1} | grep '{2}'"
                                         .format(path_to_log, f,
                                                 text_for_search))
            if ' '.join(o).find(text_for_search) != -1:
                is_txt_found = True
                break
        shell.disconnect()
        return is_txt_found
