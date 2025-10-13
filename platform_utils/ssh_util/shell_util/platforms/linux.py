from shell_util.platforms.constants import LinuxConstants
from shell_util.shell_conn import ShellConnection


class Linux(ShellConnection, LinuxConstants):
    def __init__(self, test_server, info=None):
        super(Linux, self).__init__(test_server)
        self.nonroot = False
        self.use_sudo = False
        self.info = info

    def kill_process(self, process_name, service_name, signum=9):
        self.log.debug("{0} - Process info before sending signal: {1}"
                       .format(self.ip,
                               self.execute_command("pgrep -l %s" % process_name)))
        o, r = self.execute_command("kill -%s $(pgrep %s)" % (signum, service_name))
        self.log_command_output(o, r)
        self.log.debug("{0} - Process info after sending signal: {1}"
                       .format(self.ip,
                               self.execute_command("pgrep -l %s" % process_name)))
        return o, r

    def get_mem_usage_by_process(self, process_name):
        output, error = self.execute_command(
            'ps -e -o %mem,cmd|grep {0}'.format(process_name),
            debug=False)
        if output:
            for line in output:
                if not 'grep' in line.strip().split(' '):
                    return float(line.strip().split(' ')[0])

    def get_cbversion(self):
        output = ""
        fv = sv = bn = ""
        err_msg = "{} - Couchbase Server not found".format(self.ip)
        if self.nonroot:
            if self.file_exists('/home/%s/cb/%s' % (self.server.ssh_username,
                                                    self.cb_path), self.version_file):
                output = self.read_remote_file('/home/%s/cb/%s' % (self.server.ssh_username, self.cb_path),
                                               self.version_file)
            else:
                self.log.info(err_msg)
        else:
            if self.file_exists(self.cb_path, self.version_file):
                output = self.read_remote_file(self.cb_path, self.version_file)
            else:
                self.log.info(err_msg)
        if output:
            for x in output:
                x = x.strip()
                if x and x[:5] in CB_RELEASE_BUILDS.keys() and "-" in x:
                    fv = x
                    tmp = x.split("-")
                    sv = tmp[0]
                    bn = tmp[1]
                break
        return fv, sv, bn

    def is_couchbase_installed(self):
        if self.nonroot:
            if self.file_exists("/home/%s/" % self.server.ssh_username, NR_INSTALL_LOCATION_FILE):
                output, error = self.execute_command("cat %s" % NR_INSTALL_LOCATION_FILE)
                if output and output[0]:
                    self.log.info("Couchbase Server was installed in non default path %s"
                                  % output[0])
                    self.nr_home_path = output[0]
            file_path = self.nr_home_path + self.cb_path
            if self.file_exists(file_path, self.version_file):
                self.log.info("non root couchbase installed at %s " % self.ip)
                return True
        else:
            if self.file_exists(self.cb_path, self.version_file):
                self.log.info("{0} **** The linux version file {1} {2}  exists"
                              .format(self.ip, self.cb_path, self.version_file))
                return True
        return False

    def is_couchbase_running(self):
        o = self.is_process_running('beam.smp')
        if o is not None:
            return True
        return False

    def start_server(self):
        if self.is_couchbase_installed():
            if self.nonroot:
                cmd = '%s%scouchbase-server \-- -noinput -detached '\
                      % (self.nr_home_path, LINUX_COUCHBASE_BIN_PATH)
            else:
                cmd = "systemctl start couchbase-server.service"
            o, r = self.execute_command(cmd)
            self.log_command_output(o, r)

    def stop_server(self, os="unix"):
        if self.is_couchbase_installed():
            if self.nonroot:
                cmd = "%s%scouchbase-server -k" % (self.nr_home_path,
                                                   LINUX_COUCHBASE_BIN_PATH)
            else:
                cmd = "systemctl stop couchbase-server.service"
            o, r = self.execute_command(cmd)
            self.log_command_output(o, r)

    def restart_couchbase(self):
        o, r = self.execute_command("service couchbase-server restart")
        self.log_command_output(o, r)
        return o, r

    def enable_packet_loss(self):
        o, r = self.execute_command("tc qdisc add dev eth0 root netem loss 25%")
        self.log_command_output(o, r)
        return o, r

    def enable_network_delay(self):
        o, r = self.execute_command("tc qdisc add dev eth0 root netem delay 200ms")
        self.log_command_output(o, r)
        return o, r

    def enable_file_limit(self):
        o, r = self.execute_command("prlimit --nofile=100 --pid $(pgrep indexer)")
        self.log_command_output(o, r)
        return o, r

    def enable_file_size_limit(self):
        o, r = self.execute_command("prlimit --fsize=20480 --pid $(pgrep indexer)")
        self.log_command_output(o, r)
        return o, r

    def disable_file_size_limit(self):
        o, r = self.execute_command("prlimit --fsize=unlimited --pid $(pgrep indexer)")
        self.log_command_output(o, r)
        return o, r

    def enable_file_limit_desc(self):
        o, r = self.execute_command("sysctl -w fs.file-max=100;sysctl -p")
        self.log_command_output(o, r)
        return o, r

    def disable_file_limit(self):
        o, r = self.execute_command("prlimit --nofile=200000 --pid $(pgrep indexer)")
        self.log_command_output(o, r)
        return o, r

    def disable_file_limit_desc(self):
        o, r = self.execute_command("sysctl -w fs.file-max=1606494;sysctl -p")
        self.log_command_output(o, r)
        return o, r

    def delete_network_rule(self):
        o, r = self.execute_command("tc qdisc del dev eth0 root")
        self.log_command_output(o, r)
        return o, r

    def get_memcache_pid(self):
        o, _ = self.execute_command(
            "ps -eo comm,pid | awk '$1 == \"memcached\" { print $2 }'")
        return o[0]

    def kill_erlang(self, delay=0):
        if delay:
            time.sleep(delay)
        o, r = self.execute_command("killall -9 beam.smp")
        if r and r[0] and "command not found" in r[0]:
            o, r = self.execute_command("pkill beam.smp")
            self.log_command_output(o, r)
        self.log_command_output(o, r, debug=False)
        all_killed = False
        count = 0
        while not all_killed and count < 6:
            process_count = 0
            self.sleep(2, "wait for erlang processes terminated")
            out, _ = self.execute_command("ps aux | grep beam.smp")
            for idx, val in enumerate(out):
                if "/opt/couchbase" in val:
                    process_count += 1
            if process_count == 0:
                all_killed = True
            if count == 3:
                o, r = self.execute_command("killall -9 beam.smp")
                if r and r[0] and "command not found" in r[0]:
                    o, r = self.execute_command("pkill beam.smp")
                    self.log_command_output(o, r)
            count += 1
        if not all_killed:
            raise Exception("Could not kill erlang process")
        return o, r

    def kill_cbft_process(self):
        o, r = self.execute_command("killall -9 cbft")
        self.log_command_output(o, r)
        if r and r[0] and "command not found" in r[0]:
            o, r = self.execute_command("pkill cbft")
            self.log_command_output(o, r)
        return o, r

    def kill_memcached(self, num_retries=10, poll_interval=2):
        # Changed from kill -9 $(ps aux | grep 'memcached' | awk '{print $2}'
        # as grep was also returning eventing
        # process which was using memcached-cert
        o, r = self.execute_command("kill -9 $(ps aux | pgrep 'memcached')",
                                    debug=True)
        self.log_command_output(o, r, debug=False)
        while num_retries > 0:
            self.sleep(poll_interval, "waiting for memcached to start")
            out, err = self.execute_command('pgrep memcached')
            if out and out != "":
                self.log.info(f"memcached pid:{out} and err: {err}")
                break
            else:
                num_retries -= 1
        return o, r

    def start_memcached(self):
        o, r = self.execute_command("kill -SIGCONT $(pgrep memcached)")
        self.log_command_output(o, r, debug=False)
        return o, r

    def stop_memcached(self):
        o, r = self.execute_command("kill -SIGSTOP $(pgrep memcached)")
        self.log_command_output(o, r, debug=False)
        return o, r

    def start_indexer(self):
        o, r = self.execute_command("kill -SIGCONT $(pgrep indexer)")
        self.log_command_output(o, r)
        return o, r

    def stop_indexer(self):
        o, r = self.execute_command("kill -SIGSTOP $(pgrep indexer)")
        self.log_command_output(o, r, debug=False)
        return o, r

    def kill_goxdcr(self):
        o, r = self.execute_command("killall -9 goxdcr")
        self.log_command_output(o, r)
        return o, r

    def kill_eventing_process(self, name):
        o, r = self.execute_command(command="killall -9 {0}".format(name))
        self.log_command_output(o, r)
        return o, r

    def terminate_processes(self, info, p_list):
        for process in p_list:
            self.terminate_process(info, process, force=True)

    def reboot_node(self):
        o, r = self.execute_command("reboot")
        self.log_command_output(o, r)
        return o, r

    def change_log_level(self, new_log_level):
        self.log.info("CHANGE LOG LEVEL TO %s".format(new_log_level))
        # ADD NON_ROOT user config_details
        output, error = self.execute_command("sed -i '/loglevel_default, /c \\{loglevel_default, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_ns_server, /c \\{loglevel_ns_server, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_stats, /c \\{loglevel_stats, %s\}'. %s"
                                             % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_rebalance, /c \\{loglevel_rebalance, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_cluster, /c \\{loglevel_cluster, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_views, /c \\{loglevel_views, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_error_logger, /c \\{loglevel_error_logger, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_mapreduce_errors, /c \\{loglevel_mapreduce_errors, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_user, /c \\{loglevel_user, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_xdcr, /c \\{loglevel_xdcr, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_menelaus, /c \\{loglevel_menelaus, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)

    def configure_log_location(self, new_log_location):
        mv_logs = testconstants.LINUX_LOG_PATH + '/' + new_log_location
        print((" MV LOGS %s" % mv_logs))
        error_log_tag = "error_logger_mf_dir"
        # ADD NON_ROOT user config_details
        self.log.info("CHANGE LOG LOCATION TO %s".format(mv_logs))
        output, error = self.execute_command("rm -rf %s" % mv_logs)
        self.log_command_output(output, error)
        output, error = self.execute_command("mkdir %s" % mv_logs)
        self.log_command_output(output, error)
        output, error = self.execute_command("chown -R couchbase %s" % mv_logs)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/%s, /c \\{%s, \"%s\"\}.' %s"
                                             % (error_log_tag, error_log_tag, mv_logs, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)

    def change_stat_periodicity(self, ticks):
        # ADD NON_ROOT user config_details
        self.log.info("CHANGE STAT PERIODICITY TO every %s seconds" % ticks)
        output, error = self.execute_command("sed -i '$ a\{grab_stats_every_n_ticks, %s}.'  %s"
                                             % (ticks, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)

    def change_port_static(self, new_port):
        # ADD NON_ROOT user config_details
        self.log.info("=========CHANGE PORTS for REST: %s, MCCOUCH: %s,MEMCACHED: %s, CAPI: %s==============="
                      % (new_port, new_port + 1, new_port + 2, new_port + 4))
        output, error = self.execute_command("sed -i '/{rest_port/d' %s" % testconstants.LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '$ a\{rest_port, %s}.' %s"
                                             % (new_port, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/{mccouch_port/d' %s" % testconstants.LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '$ a\{mccouch_port, %s}.' %s"
                                             % (new_port + 1, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/{memcached_port/d' %s" % testconstants.LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '$ a\{memcached_port, %s}.' %s"
                                             % (new_port + 2, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/port = /c\port = %s' %s"
                                             % (new_port + 4, testconstants.LINUX_CAPI_INI))
        self.log_command_output(output, error)
        output, error = self.execute_command("rm %s" % testconstants.LINUX_CONFIG_FILE)
        self.log_command_output(output, error)
        output, error = self.execute_command("cat %s" % testconstants.LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)

    def disable_firewall(self):
        command_1 = "/sbin/iptables -F"
        command_2 = "/sbin/iptables -t nat -F"
        if self.nonroot:
            self.log.info("Non root user has no right to disable firewall, "
                          "switching over to root")
            self.connect_with_user(user="root")
            output, error = self.execute_command(command_1)
            self.log_command_output(output, error)
            output, error = self.execute_command(command_2)
            self.log_command_output(output, error)
            self.connect_with_user(user=self.server.ssh_username)
            return
        output, error = self.execute_command(command_1)
        self.log_command_output(output, error, debug=False)
        output, error = self.execute_command(command_2)
        self.log_command_output(output, error, debug=False)
        self.connect_with_user(user=self.server.ssh_username)

    def get_port_recvq(self, port):
        """
        Given a port, extracts address:port of services
        listening on that port (only ipv4)
        """
        command = "ss -4anpe | grep :%s | grep 'LISTEN' | awk -F ' ' '{print $5}'" % port
        o, r = self.execute_command(command)
        self.log_command_output(o, r)
        return o

    def start_couchbase(self):
        running = self.is_couchbase_running()
        retry = 0
        while not running and retry < 3:
            self.log.info("Starting couchbase server")
            if self.nonroot:
                self.log.info("Start Couchbase Server with non root method")
                o, r = self.execute_command(
                    '%s%scouchbase-server \-- -noinput -detached'
                    % (self.nr_home_path, LINUX_COUCHBASE_BIN_PATH))
                self.log_command_output(o, r)
            else:
                self.log.info("Running systemd command on this server")
                o, r = self.execute_command("systemctl start couchbase-server.service")
                self.log_command_output(o, r)
                self.sleep(5,"waiting for couchbase server to come up")
                o, r = self.execute_command("systemctl status couchbase-server.service | grep ExecStop=/opt/couchbase/bin/couchbase-server")
                self.log.info("Couchbase server status: {}".format(o))
            running = self.is_couchbase_running()
            retry = retry + 1
        if not running and retry >= 3:
            sys.exit("Failed to start Couchbase server on " + self.info.ip)

    def stop_couchbase(self, num_retries=5, poll_interval=10):
        if self.nonroot:
            self.log.info("Stop Couchbase Server with non root method")
            o, r = self.execute_command(
                '%s%scouchbase-server -k' % (self.nr_home_path,
                                             LINUX_COUCHBASE_BIN_PATH))
        else:
            o, r = self.execute_command("systemctl stop couchbase-server.service")
        self.log_command_output(o, r)

    def flush_os_caches(self):
        o, r = self.execute_command("sync")
        self.log_command_output(o, r)
        o, r = self.execute_command("/sbin/sysctl vm.drop_caches=3")
        self.log_command_output(o, r)

    def set_environment_variable(self, name, value):
        """Request an interactive shell session, export custom variable and
        restart Couchbase server.

        Shell session is necessary because basic SSH client is stateless.
        """
        shell = self._ssh_client.invoke_shell()
        shell.send('export {0}={1}\n'.format(name, value))
        if self.info.distribution_version.lower() in SYSTEMD_SERVER:
            """from watson, systemd is used in centos 7 """
            self.log.info("this node is centos 7.x")
            shell.send("systemctl restart couchbase-server.service\n")
        else:
            shell.send('/etc/init.d/couchbase-server restart\n')
        shell.close()

    def change_env_variables(self, dict):
        prefix = "\\n    "
        shell = self._ssh_client.invoke_shell()
        init_file = "couchbase-server"
        file_path = "/opt/couchbase/bin/"
        environmentVariables = ""
        backupfile = file_path + init_file + ".bak"
        sourceFile = file_path + init_file
        o, r = self.execute_command("cp " + sourceFile + " " + backupfile)
        self.log_command_output(o, r)
        command = "sed -i 's/{0}/{0}".format("ulimit -l unlimited")
        for key in list(dict.keys()):
            o, r = self.execute_command(
                "sed -i 's/{1}.*//' {0}".format(sourceFile, key))
            self.log_command_output(o, r)
            o, r = self.execute_command(
                "sed -i 's/export ERL_FULLSWEEP_AFTER/export "
                "ERL_FULLSWEEP_AFTER\\n{1}={2}\\nexport {1}/' {0}"
                .format(sourceFile, key, dict[key]))
            self.log_command_output(o, r)

        for key in list(dict.keys()):
            environmentVariables += prefix \
                 + 'export {0}={1}'.format(key, dict[key])

        command += environmentVariables + "/'" + " " + sourceFile
        o, r = self.execute_command(command)
        self.log_command_output(o, r)

        # Restart Couchbase
        o, r = self.execute_command("service couchbase-server restart")
        self.log_command_output(o, r)
        shell.close()

    def reset_env_variables(self):
        shell = self._ssh_client.invoke_shell()
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        init_file = "couchbase-server"
        file_path = "/opt/couchbase/bin/"
        backupfile = file_path + init_file + ".bak"
        sourceFile = file_path + init_file
        o, r = self.execute_command("mv " + backupfile + " " + sourceFile)
        self.log_command_output(o, r)

        # Restart Couchbase
        o, r = self.execute_command("service couchbase-server restart")
        self.log_command_output(o, r)
        shell.close()

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
            count = (size * 1024 * 1024) // 512
        else:
            count = (5 * 1024 * 1024 * 1024) // 512
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
        command = "mount -o loop,rw,usrquota,grpquota /usr/disk-img/disk-quota.ext3 {0}; df -Thl".format(location)
        output, error = self.execute_command(command)
        return output, error

    def mount_partition_ext4(self, location):
        """
        Mount a partition at the location specified
        :param location: Mount location
        :return: Output and error message from the mount command
        """
        command = "mount -o loop,rw,usrquota,grpquota /usr/disk-img/disk-quota.ext4 {0}; df -Thl".format(location)
        output, error = self.execute_command(command)
        return output, error
