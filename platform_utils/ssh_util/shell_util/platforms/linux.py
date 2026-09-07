from shell_util.platforms.constants import LinuxConstants
from shell_util.shell_conn import ShellConnection
from testconstants import NR_INSTALL_LOCATION_FILE


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
        # nftables-only hosts (e.g. Debian 12+) have no iptables binary, so
        # enable_firewall()'s rules live in nft - flush that too, or the
        # induced block never actually clears on those hosts.
        command_3 = "nft flush ruleset"
        if self.nonroot:
            self.log.info("Non root user has no right to disable firewall, "
                          "switching over to root")
            self.connect_with_user(user="root")
            output, error = self.execute_command(command_1)
            self.log_command_output(output, error)
            output, error = self.execute_command(command_2)
            self.log_command_output(output, error)
            output, error = self.execute_command(command_3)
            self.log_command_output(output, error)
            self.connect_with_user(user=self.server.ssh_username)
            return
        output, error = self.execute_command(command_1)
        self.log_command_output(output, error, debug=False)
        output, error = self.execute_command(command_2)
        self.log_command_output(output, error, debug=False)
        output, error = self.execute_command(command_3)
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

    def get_mount_source(self, location):
        """
        Return the device backing 'location' if it is currently a
        dedicated mountpoint distinct from the root filesystem, else None.
        Used to remember what to restore after create_new_partition()
        replaces 'location' with a loopback filesystem.

        'findmnt --target' prints one line per mount in the stack when
        something is mounted over 'location', oldest first, so the last
        line is the mount actually in effect. Taking the first instead
        reports the device buried underneath: on a node where /data is a
        real disk with this suite's loopback mounted over it, that answers
        '/dev/xvdb1' while the live filesystem is the loopback, which makes
        is_suite_device() say the mount is not ours, leaves it mounted, and
        so blocks every dm and loop release that follows.
        :param location: Path to check (e.g. /data)
        :return: Device path (str) or None
        """
        command = ("root_dev=$(findmnt -no SOURCE / | tail -1); "
                  "loc_dev=$(findmnt -no SOURCE --target {0} 2>/dev/null "
                  "| tail -1); "
                  "[ \"$loc_dev\" != \"$root_dev\" ] && echo \"$loc_dev\""
                  ).format(location)
        output, error = self.execute_command(command)
        if output and output[0].strip():
            return output[0].strip()
        return None

    @staticmethod
    def dm_device_name(location):
        """
        Deterministic device-mapper name for the disk backing 'location'.
        Deterministic so that a later run, or a hand cleanup, can find the
        device without having been told about it.
        :param location: Mountpoint the device backs, e.g. /data
        :return: Device-mapper name, e.g. taf_disk_data
        """
        return "taf_disk" + location.replace("/", "_").rstrip("_")

    def dm_device_path(self, location):
        """
        :param location: Mountpoint the device backs
        :return: /dev/mapper path of this suite's device for 'location'
        """
        return "/dev/mapper/" + self.dm_device_name(location)

    def suite_device_names(self, location):
        """
        Every spelling of this suite's device for 'location'.

        findmnt may report a device-mapper mount either as its /dev/mapper
        name or as the /dev/dm-N it resolves to, and both have to be
        recognised. Matching '/dev/dm-' generally would be unsafe: the
        root filesystem on these nodes is itself device-mapper
        (/dev/mapper/tmpl--deb10--vg-root), so only this suite's own
        device is ever treated as ours.
        :param location: Mountpoint the device backs
        :return: set of device paths that mean "this suite's device"
        """
        path = self.dm_device_path(location)
        names = {path}
        output, _ = self.execute_command(
            "readlink -f {0} 2>/dev/null".format(path))
        if output and output[0].strip():
            names.add(output[0].strip())
        return names

    def is_suite_device(self, device, location):
        """
        :param device: Device a mount reports, or None
        :param location: Mountpoint being considered
        :return: True when this suite created the device
        """
        if not device:
            return False
        return device.startswith("/dev/loop") \
            or device in self.suite_device_names(location)

    def get_dm_table_type(self, location):
        """
        Report what this suite's device-mapper device is currently mapped
        to: 'linear' while the disk is healthy, 'error' while a failure is
        injected, None when the device does not exist.
        :param location: Mountpoint the device backs
        :return: Target type as a string, or None
        """
        output, _ = self.execute_command(
            "dmsetup table {0} 2>/dev/null"
            .format(self.dm_device_name(location)))
        if not output or not output[0].strip():
            return None
        # "<logical_start> <sectors> <target_type> [target args]"
        fields = output[0].strip().split()
        return fields[2] if len(fields) > 2 else None

    def inject_disk_error(self, location):
        """
        Make every I/O to 'location' fail with EIO, by swapping the
        device-mapper table for an error target.

        This is the only injection that reaches a process which already
        holds files open for write, and memcached always does. Measured on
        172.23.104.173: 'umount -l' is lazy, so those descriptors keep
        working and ns_server is told of no disk problem at all;
        'mount -o remount,ro' is refused outright with "mount point is
        busy" (rc=32); 'blockdev --setro' is rejected by the block layer
        but absorbed by the page cache, so the application's write and
        fsync both still return success. Swapping the mapping underneath
        the filesystem gives a real "OSError errno 5 Input/output error".
        :param location: Mountpoint to fail
        :return: True once the mapping is an error target
        """
        dm_name = self.dm_device_name(location)
        output, _ = self.execute_command("dmsetup table {0}".format(dm_name))
        if not output or not output[0].strip():
            self.log.error("{0}: no device-mapper table for {1}; the disk "
                           "was not set up through one"
                           .format(self.ip, dm_name))
            return False
        sectors = output[0].strip().split()[1]
        # --noflush because a dying disk does not flush either, and
        # because flushing is what makes the reverse operation fail.
        for command in ("dmsetup suspend --noflush {0}".format(dm_name),
                        'dmsetup reload {0} --table "0 {1} error"'
                        .format(dm_name, sectors),
                        "dmsetup resume {0}".format(dm_name)):
            out, err = self.execute_command(command)
            self.log_command_output(out, err)
        return self.get_dm_table_type(location) == "error"

    def recover_disk_error(self, location,
                           backing_file="/usr/disk-img/disk-quota.ext3"):
        """
        Undo inject_disk_error() by mapping 'location' back to its loop
        device.

        The filesystem has to come off first. 'dmsetup suspend' flushes
        outstanding I/O, and with an error target live that flush itself
        fails - "suspend ioctl failed: Input/output error" - so the reload
        never happens and the mapping stays broken; measured on
        172.23.104.173. Unmounting first and suspending with --noflush
        avoids it. The filesystem also shuts itself down on the I/O errors
        and stays read-only through a plain remount, so it is checked and
        mounted fresh rather than remounted. Couchbase must already be
        stopped, so that nothing holds the filesystem open.
        :param location: Mountpoint to recover
        :param backing_file: Loopback image behind the device
        :return: True once the mapping is linear and 'location' is mounted
                 read-write again
        """
        dm_name = self.dm_device_name(location)
        output, _ = self.execute_command(
            "losetup -j {0} | cut -d: -f1".format(backing_file))
        loop_device = output[0].strip() \
            if output and output[0].strip() else None
        if not loop_device:
            self.log.error("{0}: no loop device is attached to {1}, cannot "
                           "map {2} back".format(self.ip, backing_file,
                                                 dm_name))
            return False
        output, _ = self.execute_command(
            "blockdev --getsz {0}".format(loop_device))
        sectors = output[0].strip() if output and output[0].strip() else None
        if not sectors:
            self.log.error("{0}: could not size {1}"
                           .format(self.ip, loop_device))
            return False
        dm_path = self.dm_device_path(location)
        for command in ("umount -l {0}".format(location),
                        "dmsetup suspend --noflush {0}".format(dm_name),
                        'dmsetup reload {0} --table "0 {1} linear {2} 0"'
                        .format(dm_name, sectors, loop_device),
                        "dmsetup resume {0}".format(dm_name),
                        "e2fsck -p {0}".format(dm_path),
                        "mount -o rw,usrquota,grpquota {0} {1}"
                        .format(dm_path, location)):
            out, err = self.execute_command(command)
            self.log_command_output(out, err)
        return self.get_dm_table_type(location) == "linear" \
            and self.get_mount_mode(location) == "rw"

    def clear_immutable(self, *paths):
        """
        Drop the immutable attribute from 'paths' and every component
        above them.

        A directory carrying the immutable flag refuses mkdir even to
        root, so couchbase cannot create its data directory and the node
        comes up with no data path at all. Measured on 172.23.220.121:
        /data was "----i---------e----", "mkdir /data/kv" failed with
        "Operation not permitted" as root, storage.hdd[0] came back {},
        the node sat 'unhealthy' while still 'active', and every
        subsequent test spent 360s waiting for a node that could never
        become healthy - then failed its add-node with HTTP 500 and ran a
        four-minute cb-collect. _prepare_node_paths() clears this for the
        same reason, but it never runs for these suites because
        skip_cluster_reset defaults to True.

        Deliberately never walks up to '/' itself.
        :param paths: Paths to clear, along with their parent components
        :return: Nothing
        """
        components = set()
        for path in paths:
            if not path:
                continue
            parts = [part for part in path.strip("/").split("/") if part]
            if not parts:
                # '/' or equivalent - clearing that is never intended.
                continue
            for i in range(1, len(parts) + 1):
                components.add("/" + "/".join(parts[:i]))
        if not components:
            return
        # A path that does not exist yet just reports an error, which is
        # why the result is logged rather than checked.
        command = "chattr -i {0}".format(" ".join(sorted(components)))
        output, error = self.execute_command(command)
        self.log_command_output(output, error)

    # Building the backing image and its filesystem is the only part of
    # this that scales with data_location_size, and 600s is not enough at
    # 24 GiB. Everything else here keeps the default.
    disk_build_timeout = 1800

    def create_new_partition(self, location, size=None):
        """
        Create a new partition at the location specified and of
        the size specified
        :param location: Location to create the new partition at.
        :param size: Size of the partition in MB
        :return: The device this unmounted from 'location' to make room
                 for the test filesystem, or None if there was none. The
                 caller must record this as the device to restore: its own
                 sample from before the call can differ, see below.
        """
        backing_file = "/usr/disk-img/disk-quota.ext3"
        # Release whatever an earlier run left mounted here or attached to
        # the image before touching either. 'dd' below truncates its output
        # file, so writing over an image a loop device still holds corrupts
        # a live filesystem, and deleting it makes the leak permanent:
        # 'losetup -j' matches by path and cannot see a '(deleted)' inode,
        # so nothing afterwards can find the device to detach it.
        still_attached = self._release_loopbacks(location, backing_file)
        if still_attached:
            raise Exception(
                "Cannot create a partition at {0}: loop device(s) {1} are "
                "still attached to {2}. Clear them on this node before "
                "reusing it."
                .format(location, ", ".join(still_attached), backing_file))

        # Take 'location's own device out of the way rather than mounting
        # over it. Where /data is a real disk (the deb12 pool VMs mount
        # /dev/xvdb1 there from /etc/fstab) mounting on top would stack the
        # loopback over a live filesystem: the 'rm -rf' below would wipe
        # that real volume, restore_partition() would mount the device back
        # on top of a mount that was never removed, and the stack would
        # grow by two every test until nothing could be released at all.
        # Unmounting first means the loopback replaces the device for the
        # duration of the test and restore_partition() puts the real one
        # back, leaving 'location' exactly as it was found.
        # Sampled HERE, after _release_loopbacks() has drained this
        # suite's leftovers, so it is whatever was underneath them - which
        # is not what the caller saw before calling. On a node an
        # interrupted run left with our loopback stacked over the real
        # disk, the caller samples the loopback, calls it a leftover and
        # records None, while this sees the real device. Unmounting that
        # against a recorded None means restore_partition() is asked to put
        # back nothing, remounts nothing, and its post-condition compares
        # None with None and passes - so the node's data disk is dropped
        # from the mount tree until the next reboot, silently, and every
        # later run repeats it while writing the image and the whole
        # dataset onto the root filesystem. The device actually unmounted
        # is therefore returned, and the caller records that instead.
        original_device = self.get_mount_source(location)
        unmounted_device = None
        if original_device and not self.is_suite_device(original_device,
                                                       location):
            self.log.info("{0}: unmounting {1} from {2} so the test "
                          "filesystem replaces it rather than stacking "
                          "on it".format(self.ip, original_device, location))
            unmounted_device = original_device
            if not self._unmount_device_everywhere(original_device,
                                                   only_target=location):
                raise Exception(
                    "Cannot create a partition at {0}: {1} is mounted there "
                    "and could not be unmounted. Stacking on it would "
                    "destroy its contents."
                    .format(location, original_device))

        # Everything from here on runs with 'location' unmounted. If any
        # of it fails the node is left with no filesystem there at all,
        # and nothing puts one back: tearDown does not run after a failing
        # setUp, so restore_partition() never gets the chance. Put the
        # device back before letting the failure out.
        try:
            # In MiB, written with a matching block size. Without an explicit
            # 'bs' dd defaults to 512-byte blocks, so a 5 GiB image is 10.5
            # million write syscalls: measured at over 620s on 172.23.220.135,
            # past execute_command()'s 600s timeout, which surfaced as a bare
            # socket.timeout whose str() is empty - "172.23.220.135: " with no
            # reason - and failed setUp, so tearDown never ran and the two
            # following tests failed on the unrestored cluster (job 259800,
            # test 11). At bs=1M it is a few thousand writes.
            count_mb = size if size else 5 * 1024

            # Before anything tries to remove or create under 'location':
            # an immutable flag there fails mkdir even as root.
            self.clear_immutable(location)
            for command in (
                    "rm -rf {0}".format(location),
                    "rm -rf {0}".format(backing_file),
                    "mkdir -p {0}".format(location),
                    "mkdir -p /usr/disk-img"):
                _, _ = self.execute_command(command)

            # Allocate the image rather than write it. 'dd' pushes every
            # byte through the kernel, and data_location_size is 24576 for
            # conf/magma/history_retention_failovers.conf - 24 GiB per
            # node, five nodes at once - which has run past
            # execute_command()'s default 600s on the slower nodes and
            # failed setUp with a bare socket.timeout. 'fallocate' reserves
            # the same blocks without writing them, so the disk-full
            # behaviour these tests rely on is unchanged, and it returns at
            # once. dd remains the fallback where fallocate is missing or
            # the filesystem cannot preallocate.
            _, _ = self.execute_command(
                "fallocate -l {0}M {1} 2>/dev/null "
                "|| dd if=/dev/zero of={1} bs=1M count={0}"
                .format(count_mb, backing_file),
                timeout=self.disk_build_timeout)
            _, _ = self.execute_command(
                "chown couchbase:couchbase {0}".format(backing_file))

            # The filesystem is mounted through a device-mapper linear target
            # rather than straight off the loop device, so that a disk failure
            # can be injected by swapping that mapping for an error target.
            # See inject_disk_error() for why nothing at the mount or
            # filesystem layer can fail a disk under a running memcached.
            output, error = self.execute_command(
                "losetup -f --show {0}".format(backing_file))
            self.log_command_output(output, error)
            loop_device = output[0].strip() \
                if output and output[0].strip() else None
            if not loop_device:
                raise Exception("Could not attach a loop device to {0} on {1}"
                                .format(backing_file, self.ip))
            output, error = self.execute_command(
                "blockdev --getsz {0}".format(loop_device))
            self.log_command_output(output, error)
            sectors = output[0].strip() if output and output[0].strip() else None
            if not sectors:
                raise Exception("Could not size {0} on {1}"
                                .format(loop_device, self.ip))

            dm_name = self.dm_device_name(location)
            dm_path = self.dm_device_path(location)
            _, _ = self.execute_command(
                'dmsetup create {0} --table "0 {1} linear {2} 0"'
                .format(dm_name, sectors, loop_device))
            # mkfs writes the inode tables, which on a 24 GiB ext3 is not
            # quick either, so it gets the same budget as the allocation.
            _, _ = self.execute_command(
                "/sbin/mkfs -t ext3 -q {0} -F".format(dm_path),
                timeout=self.disk_build_timeout)
            for command in (
                    "mount -o rw,usrquota,grpquota {0} {1}"
                    .format(dm_path, location),
                    "chown -R couchbase:couchbase {0}".format(location),
                    "chmod 777 {0}".format(location)):
                _, _ = self.execute_command(command)
        except Exception:
            if unmounted_device:
                self.log.error(
                    "{0}: failed to build the test filesystem at {1}; "
                    "remounting {2} so the node is not left without it"
                    .format(self.ip, location, unmounted_device))
                out, err = self.execute_command(
                    "mount {0} {1}".format(unmounted_device, location))
                self.log_command_output(out, err)
            raise
        return unmounted_device

    def _remove_dm_device(self, location):
        """
        Remove this suite's device-mapper device for 'location'.

        Retried, because it can report the device as busy for a moment
        after the filesystem above it has been unmounted, in the same way
        a loop device does.
        :param location: Mountpoint the device backs
        :return: True when the device is gone or was never there
        """
        if self.get_dm_table_type(location) is None:
            return True
        dm_name = self.dm_device_name(location)
        max_remove_attempts = 6
        for attempt in range(max_remove_attempts):
            output, error = self.execute_command(
                "dmsetup remove {0}".format(dm_name))
            self.log_command_output(output, error)
            if self.get_dm_table_type(location) is None:
                return True
            if attempt < max_remove_attempts - 1:
                self.sleep(5, "Waiting for {0} to be released"
                              .format(dm_name))
        self.log.error("{0}: could not remove device-mapper device {1}"
                       .format(self.ip, dm_name))
        return False

    def _mount_stack(self, location):
        """
        Every mount at 'location', bottom first, top last.

        get_mount_source() reports only the top, which is not enough to
        decide whether this suite has anything mounted here: a run from
        before restore_partition() stopped re-mounting an already-mounted
        device leaves the node's own disk stacked ON TOP of our loopback -
        /dev/xvdb1, taf_disk_data, /dev/xvdb1 - and judging by the top
        alone concludes nothing here is ours. The loopback then stays
        mounted and open, so the mapping cannot be removed, so the loop
        beneath it cannot be detached, and setUp fails on that node for
        good. Measured on 172.23.222.224 and .232.
        :param location: Mountpoint to inspect
        :return: List of device paths, bottom first
        """
        output, _ = self.execute_command(
            "findmnt -n -o SOURCE --mountpoint {0} 2>/dev/null"
            .format(location))
        return [line.strip() for line in output or [] if line.strip()]

    def _unmount_device_everywhere(self, device, only_target=None):
        """
        Unmount 'device' from the mountpoints it is currently mounted at.

        A loop device cannot be detached while any mount of it is live, and
        it need not be mounted at the location this suite cares about: a
        run that used a different data_location, or an interrupted one,
        leaves the image mounted somewhere else entirely (/mnt/disk_fo has
        been seen in the field). Nothing that only ever looks at 'location'
        can release that, so every later run on the node fails in setUp
        with "loop device(s) ... are still attached", permanently.

        '/' is never unmounted, whatever is asked. On the deb10 pool VMs
        /data sits on the root device, so 'findmnt --source' for it answers
        '/': one caller passing the wrong device would take the node down.
        get_mount_source() does return None for exactly that case, so this
        is not reachable today, but that guard is a string comparison in
        another function and the same device can be spelled two ways
        (/dev/dm-0 and /dev/mapper/vg-root), which would defeat it.
        :param device: Device to unmount, e.g. /dev/loop0
        :param only_target: Restrict to this mountpoint. Used where the
                            device is the node's own disk and only its
                            mount at 'location' is this suite's business;
                            left None for our own loop devices, which are
                            to be released wherever they turn up.
        :return: True when the device is no longer mounted where it matters
        """
        max_attempts = 5
        for attempt in range(max_attempts):
            output, _ = self.execute_command(
                "findmnt -n -o TARGET --source {0} 2>/dev/null"
                .format(device))
            targets = [line.strip() for line in output or [] if line.strip()]
            if only_target is not None:
                targets = [t for t in targets if t == only_target]
            unsafe = [t for t in targets if t == "/"]
            if unsafe:
                self.log.error(
                    "{0}: refusing to unmount {1} - it is the root "
                    "filesystem".format(self.ip, device))
                targets = [t for t in targets if t != "/"]
            if not targets:
                return not unsafe
            for target in targets:
                # 'umount -l <target>' pops whatever is topmost AT that
                # mountpoint, which is not necessarily this device's mount:
                # where two filesystems are stacked on one path, unmounting
                # for the lower one would take the upper one away instead -
                # possibly a real device the drain deliberately protected.
                # Only pop it when this device is the one on top.
                top, _ = self.execute_command(
                    "findmnt -n -o SOURCE --mountpoint {0} 2>/dev/null "
                    "| tail -1".format(target))
                top_source = top[0].strip() if top and top[0].strip() else None
                if top_source and top_source != device:
                    self.log.warning(
                        "{0}: not unmounting {1} - {2} is mounted over it "
                        "there".format(self.ip, target, top_source))
                    continue
                self.log.info("{0}: unmounting {1} from {2}"
                              .format(self.ip, device, target))
                command = "umount -l {0}".format(target)
                out, err = self.execute_command(command)
                self.log_command_output(out, err)
            if attempt < max_attempts - 1:
                self.sleep(2, "Waiting for {0} to be unmounted"
                              .format(device))
        self.log.error("{0}: {1} is still mounted after {2} attempts"
                       .format(self.ip, device, max_attempts))
        return False

    def _loopbacks_for(self, backing_file):
        """
        Every loop device currently bound to 'backing_file'.

        'losetup -j' matches by path and so cannot see an image that has
        already been unlinked, while 'losetup -a' still lists those as
        '(deleted)'. Both are consulted, because a device missed here is a
        device nothing can ever detach again.
        :param backing_file: Loopback image to look for
        :return: Sorted list of loop device paths
        """
        devices = set()
        # Exact, inode-based, but blind to an unlinked image.
        output, _ = self.execute_command(
            "losetup -j {0} | cut -d: -f1".format(backing_file))
        for line in output or []:
            if line.strip():
                devices.add(line.strip())
        # Catches the unlinked ones, but the backing file has to be
        # compared properly rather than searched for: a bare substring
        # test also matches disk-quota.ext3.bak, or a copy of the same
        # path inside a container root, and those devices would then be
        # unmounted and detached though they are nothing to do with us.
        output, _ = self.execute_command("losetup -a")
        for line in output or []:
            line = line.strip()
            open_paren, close_paren = line.find("("), line.rfind(")")
            if not line or open_paren < 0 or close_paren <= open_paren:
                continue
            backing = line[open_paren + 1:close_paren].strip()
            if backing.endswith("(deleted)"):
                backing = backing[:-len("(deleted)")].strip()
            if backing == backing_file:
                name = line.split(":")[0].strip()
                if name:
                    devices.add(name)
        return sorted(devices)

    def _release_loopbacks(self, location, backing_file):
        """
        Unmount every loopback this suite has stacked on 'location' and
        detach every loop device bound to 'backing_file'.

        A run that recovers a disk failure in its test body and again in
        teardown mounts the image twice - mount_partition() mounts without
        unmounting first - and a single 'umount -l' pops only the topmost,
        so the mountpoint has to be drained rather than unmounted once.
        Only a mount whose source is one of our loop devices is touched;
        the moment a real device is exposed the drain stops, because
        unmounting that is the damage restore_partition() exists to undo.

        :param location: Mountpoint the loopback image was mounted on
        :param backing_file: Loopback image create_new_partition() wrote
        :return: List of loop devices still attached afterwards; empty
                 when everything was released and the image is safe to
                 delete.
        """
        # Drain while ANY mount here is ours, not merely while the top one
        # is: see _mount_stack(). A device of the node's own that sits
        # above ours is popped too, but only when the very same device is
        # still mounted below it - that is what the stacking bug produced,
        # and popping a duplicate leaves 'location' with its filesystem.
        # Anything else stops the drain, exactly as before: unmounting a
        # real device with nothing to put back is the damage
        # restore_partition() exists to undo.
        max_unmount_attempts = 10
        loop_devices = []
        attempts = 0
        stack = []
        while attempts < max_unmount_attempts:
            stack = self._mount_stack(location)
            if not any(self.is_suite_device(d, location) for d in stack):
                break
            device = stack[-1]
            if not self.is_suite_device(device, location) \
                    and device not in stack[:-1]:
                self.log.error(
                    "{0}: {1} is mounted at {2} above one of ours and is "
                    "not a duplicate, so it cannot be popped; giving up on "
                    "draining".format(self.ip, device, location))
                break
            if device.startswith("/dev/loop") \
                    and device not in loop_devices:
                loop_devices.append(device)
            command = "umount -l {0}".format(location)
            output, error = self.execute_command(command)
            self.log_command_output(output, error)
            attempts += 1
        # 'stack' is the reading that ended the loop, so it does not have
        # to be fetched again.
        if any(self.is_suite_device(d, location) for d in stack):
            self.log.error(
                "{0}: {1} still has one of our devices mounted after {2} "
                "unmount attempts; giving up on draining it"
                .format(self.ip, location, max_unmount_attempts))

        # Remove the device-mapper device before detaching the loop
        # underneath it: the mapping holds that loop open, so the detach
        # cannot succeed while it exists.
        self._remove_dm_device(location)

        # Always ask the node as well, rather than trusting the drain
        # above to have seen everything. The drain only observes mounts of
        # 'location', and a loop bound to this image can be mounted
        # somewhere else entirely - by an earlier run with a different
        # data_location, or one that was interrupted - in which case the
        # drain sees nothing while the device is very much still held.
        for device in self._loopbacks_for(backing_file):
            if device not in loop_devices:
                loop_devices.append(device)

        if not loop_devices:
            return []

        # A loop device cannot be detached while any mount of it is live,
        # and 'losetup -d' on a busy device merely arms autoclear and
        # reports success-ish, so the detach loop below would spin for its
        # full 30s and still find the device attached. Take the mounts away
        # first, wherever they are; that alone releases a device whose
        # autoclear is set.
        for device in loop_devices:
            self._unmount_device_everywhere(device)

        # The disk_failure injection unmounts with 'umount -l', which keeps
        # the filesystem alive until the last reference closes, so a detach
        # straight after couchbase is stopped can fail with EBUSY while it
        # is still letting go. Retry rather than give up on the first pass.
        max_detach_attempts = 6
        still_attached = list(loop_devices)
        for attempt in range(max_detach_attempts):
            for loop_device in still_attached:
                command = "losetup -d {0}".format(loop_device)
                output, error = self.execute_command(command)
                self.log_command_output(output, error)
            # 'losetup -a' lists by device, so unlike 'losetup -j' it still
            # reports a loop whose backing file was unlinked - the shape
            # that made earlier leaks permanent and invisible.
            command = "losetup -a"
            output, error = self.execute_command(command)
            self.log_command_output(output, error)
            # 'losetup -a' prints '/dev/loop0: [65024]:1234 (/path)', so
            # compare against the parsed device field. A plain substring
            # test would also match /dev/loop1 inside /dev/loop10.
            attached_now = set()
            for line in output or []:
                name = line.split(":")[0].strip()
                if name:
                    attached_now.add(name)
            still_attached = [device for device in still_attached
                              if device in attached_now]
            if not still_attached:
                return []
            if attempt < max_detach_attempts - 1:
                self.sleep(5, "Waiting for {0} to be released"
                              .format(", ".join(still_attached)))
        return still_attached

    def restore_partition(self, location, original_device=None):
        """
        Undo create_new_partition(): drop the loopback quota filesystem
        mounted at 'location' and, if 'location' was backed by a real
        dedicated device before create_new_partition() was called,
        remount that device. The loopback image always lives on the
        root filesystem, so this never touches 'location's original
        device beyond a plain mount. Couchbase must already be stopped
        on this node before calling this, otherwise the unmount can be
        left busy and the loop device orphaned.
        :param location: Mount location previously passed to
                         create_new_partition()
        :param original_device: Device returned by get_mount_source()
                                before create_new_partition() was called,
                                or None if 'location' was not a dedicated
                                mountpoint at that time.
        :return: Nothing
        :raises Exception: If 'location' does not end up backed by
                           'original_device' (or by nothing, when
                           original_device is None) once this returns.
        """
        backing_file = "/usr/disk-img/disk-quota.ext3"
        # Only unmount the loopbacks we created, and drain all of them:
        # unmounting unconditionally detached 'location' from its real
        # device with nothing to put back, and a single 'umount -l' pops
        # only the topmost of a stack.
        still_attached = self._release_loopbacks(location, backing_file)

        if still_attached:
            # Deleting the image while a loop device still holds it strands
            # both the device and the GiBs it occupies on the root
            # filesystem, and hides them from the next run: 'losetup -j'
            # matches by path and cannot see a '(deleted)' inode. Leave the
            # image so the next create_new_partition() can release it, and
            # report once the restore itself is done.
            self.log.error(
                "Not deleting {0}: loop device(s) {1} are still attached "
                "to it".format(backing_file, ", ".join(still_attached)))
        else:
            command = "rm -f {0} /usr/disk-img/disk-quota.ext4".format(
                backing_file)
            output, error = self.execute_command(command)
            self.log_command_output(output, error)

        if original_device:
            # Only if it is not already there. Mounting a device onto a
            # mountpoint it is already mounted at does not fail, it stacks
            # a second mount, and two tests of that is a stack nothing can
            # unwind: the drain above stops at the first non-suite device
            # and leaves the rest in place forever.
            if self.get_mount_source(location) == original_device:
                self.log.info("{0}: {1} is already mounted at {2}, not "
                              "mounting it again"
                              .format(self.ip, original_device, location))
            else:
                command = "mount {0} {1}".format(original_device, location)
                output, error = self.execute_command(command)
                self.log_command_output(output, error)

        restored_device = self.get_mount_source(location)
        if restored_device != original_device:
            raise Exception(
                "Failed to restore {0}: expected device '{1}', found "
                "'{2}' after restore_partition()"
                .format(location, original_device, restored_device))

        if still_attached:
            raise Exception(
                "Restored {0} but could not detach loop device(s) {1} from "
                "{2}; {2} has been left in place for the next run to "
                "release. Clear them by hand if this repeats."
                .format(location, ", ".join(still_attached), backing_file))

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

    """
    EA START
    """

    def start_enterprise_analytics(self):
        o, r = self.execute_command(
            "systemctl start enterprise-analytics.service")
        self.log_command_output(o, r)
        return o, r

    def stop_enterprise_analytics(self):
        o, r = self.execute_command(
            "systemctl stop enterprise-analytics.service")
        self.log_command_output(o, r)
        return o, r

    """
    EA END
    """
