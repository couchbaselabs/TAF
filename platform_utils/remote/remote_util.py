import copy
import re
import time

from common_lib import sleep
from membase.api.rest_client import RestConnection
from platform_constants.os_constants import Linux, LinuxEnterpriseAnalytics, Windows
from shell_util.remote_connection import RemoteMachineShellConnection


class COMMAND:
    SHUTDOWN = "shutdown"
    REBOOT = "reboot"


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
            self.infra_log.debug("Sleep before monitor_process retry..")
            sleep(1, log_type="infra")
        return True

    def is_process_running(self, process_name):
        self.infra_log.debug("%s - Checking if %s is running or not"
                             % (self.remote_shell.ip, process_name))
        if getattr(self.remote_shell, "info", None) is None:
            self.remote_shell.info = self.remote_shell.extract_remote_info()

        if self.remote_shell.info.type.lower() == Windows.NAME:
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


class RemoteUtilHelper(object):

    @staticmethod
    def enable_firewall(
            server, bidirectional=False, xdcr=False, action_on_packet="REJECT",
            block_ips=[], all_interface=False, interface_names=["eth0"],
            destination_ports=None):
        """ Check if user is root or non root in unix """
        shell = RemoteMachineShellConnection(server)
        if shell.info.type.lower() == Windows.NAME:
            o, r = shell.execute_command('netsh advfirewall set publicprofile state on')
            shell.log_command_output(o, r)
            o, r = shell.execute_command('netsh advfirewall set privateprofile state on')
            shell.log_command_output(o, r)
            shell.log.debug("Enabled firewall on {0}".format(server))
            suspend_erlang = shell.windows_process_utils("pssuspend.exe",
                                                         "erl.exe", option="")
            if suspend_erlang:
                shell.log.debug(f"{shell.ip} - Erlang process is suspended")
            else:
                shell.log.error(
                    f"{shell.ip} - Erlang process failed to suspend")
        else:
            copy_server = copy.deepcopy(server)
            command_1 = "/sbin/iptables -A INPUT -p tcp "
            if not all_interface:
                command_1 += "-i {0} ".format(",".join(interface_names))
            if block_ips:
                command_1 += "-s {0} ".format(",".join(block_ips))
            if not destination_ports:
                destination_ports = "1000:65535"
            command_1 += "--dport {0} -j {1}".format(
                destination_ports, action_on_packet)
            command_2 = "/sbin/iptables -A OUTPUT -p tcp -o eth0 --sport 1000:65535 -j REJECT"
            command_3 = "/sbin/iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT"
            if shell.info.distribution_type.lower() in Linux.DISTRIBUTION_NAME \
                    and server.ssh_username != "root":
                copy_server.ssh_username = "root"
                shell.disconnect()
                shell.log.info("%s - Connect to server as %s "
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
            shell.log.debug("%s - Enabled firewall" % shell.ip)
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
        sleep(10, "Wait after common_basic_setup", log_type="infra")

    @staticmethod
    def use_hostname_for_server_settings(server):
        shell = RemoteMachineShellConnection(server)
        version = RestConnection(server).get_nodes_self().version
        hostname = shell.get_full_hostname()
        if version.startswith("1.8.") or version.startswith("2.0"):
            shell.stop_couchbase()
            if shell.info.type.lower() == Windows.NAME:
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
        if shell.info.type.lower() != Windows.NAME:
            if server.type == "analytics":
                path_to_log = LinuxEnterpriseAnalytics.COUCHBASE_LOGS_PATH
            else:
                path_to_log = Linux.COUCHBASE_LOGS_PATH
        else:
            path_to_log = Windows.COUCHBASE_LOGS_PATH
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

    @staticmethod
    def get_interface_info(server):
        interface_info = None
        shell = RemoteMachineShellConnection(server)
        if not shell.info.type.lower() == Windows.NAME:
            command_1 = "ifconfig -a"
            o, r = shell.execute_command(command_1)
            interface_info = dict()
            interface_name = None
            interface_regex = r"(.*):.*flags"
            ip_regex = r"inet(.*)netmask"
            for line in o:
                match1 = re.search(interface_regex, line)
                match2 = re.search(ip_regex, line)
                if match1:
                    interface_name = match1.group(1)
                    interface_info[interface_name] = None
                if match2:
                    interface_info[interface_name] = match2.group(1).strip(" ")
        shell.disconnect()
        return interface_info

    @staticmethod
    def set_upload_download_speed(server, upload=0, download=0):
        shell = RemoteMachineShellConnection(server)
        if not shell.info.type.lower() == Windows.NAME:
            interface_info = RemoteUtilHelper.get_interface_info(server)
            interface_to_use = list()
            for interface in interface_info:
                if not (interface == "lo" or not interface_info[interface]):
                    interface_to_use.append(interface)
            command = "wondershaper -a {0} "
            if upload:
                command += "-u {0} ".format(str(upload))
            if download:
                command += "-d {0} ".format(str(download))
            for interface in interface_to_use:
                o, r = shell.execute_command(command.format(interface))
        shell.disconnect()

    @staticmethod
    def clear_all_speed_restrictions(server):
        shell = RemoteMachineShellConnection(server)
        if not shell.info.type.lower() == Windows.NAME:
            interface_info = RemoteUtilHelper.get_interface_info(server)
            for interface in interface_info:
                shell.execute_command("wondershaper -ca {0}".format(interface))
