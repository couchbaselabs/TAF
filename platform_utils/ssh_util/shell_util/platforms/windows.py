from shell_util.platforms.constants import WindowsConstants
from shell_util.remote_machine import RemoteMachineProcess
from shell_util.shell_conn import ShellConnection


class Windows(ShellConnection, WindowsConstants):
    def __init__(self, test_server, info=None):
        super(Windows, self).__init__(test_server)
        self.nonroot = True
        self.info = info

        self.cmd_ext = ".exe"
        self.bin_path = "/cygdrive/c/Program\ Files/Couchbase/Server/bin/"

    def get_full_hostname(self):
        """Override method for windows"""
        if not self.info.domain:
            return None
        return '%s.%s' % (self.info.hostname[0], self.info.domain)

    def kill_process(self, process_name, service_name, signum=9):
        o, r = self.execute_command("taskkill /F /T /IM %s*" % process_name)
        self.log_command_output(o, r)
        return o, r

    def cpu_stress(self, stop_time):
        """Override method"""
        raise NotImplementedError

    def ram_stress(self, stop_time):
        """Override method"""
        raise NotImplementedError

    def enable_disk_readonly(self, disk_location):
        """Override method"""
        raise NotImplementedError

    def disable_disk_readonly(self, disk_location):
        """Override method"""
        raise NotImplementedError

    def stop_network(self, stop_time):
        command = "net stop Netman && timeout {} && net start Netman"
        output, error = self.execute_command(command.format(stop_time))
        self.log_command_output(output, error)

    def is_couchbase_installed(self):
        if self.file_exists(WIN_CB_PATH, VERSION_FILE):
            self.log.info("{0} - VERSION file {1} {2} exists"
                          .format(self.ip, WIN_CB_PATH, VERSION_FILE))
            return True
        return False

    def is_enterprise(self):
        """Override method for windows"""
        raise NotImplementedError

    def get_cbversion(self):
        fv = sv = bn = ""
        if self.file_exists(WIN_CB_PATH_PARA, VERSION_FILE):
            output = self.read_remote_file(WIN_CB_PATH_PARA, VERSION_FILE)
            if output:
                for x in output:
                    x = x.strip()
                    if x and x[:5] in CB_RELEASE_BUILDS.keys() and "-" in x:
                        fv = x
                        tmp = x.split("-")
                        sv = tmp[0]
                        bn = tmp[1]
                    break
        else:
            self.log.info("{} - Couchbase Server not found".format(self.ip))
        return fv, sv, bn

    def is_couchbase_running(self):
        o = self.is_process_running('erl.exe')
        if o is not None:
            return True
        return False

    def stop_membase(self, num_retries=10, poll_interval=1):
        o, r = self.execute_command("net stop membaseserver")
        self.log_command_output(o, r)
        o, r = self.execute_command("net stop couchbaseserver")
        self.log_command_output(o, r)
        retries = num_retries
        while retries > 0:
            if self.is_process_running('membaseserver') is None:
                break
            retries -= 1
            self.sleep(poll_interval)

    def start_membase(self):
        o, r = self.execute_command("net start membaseserver")
        self.log_command_output(o, r)

    def start_server(self):
        o, r = self.execute_command("net start couchbaseserver")
        self.log_command_output(o, r)

    def stop_server(self):
        o, r = self.execute_command("net stop couchbaseserver")
        self.log_command_output(o, r)

    def restart_couchbase(self):
        o, r = self.execute_command("net stop couchbaseserver")
        self.log_command_output(o, r)
        o, r = self.execute_command("net start couchbaseserver")
        self.log_command_output(o, r)

    def stop_schedule_tasks(self):
        self.log.info("STOP SCHEDULE TASKS: installme, removeme & upgrademe")
        for task in ["installme", "removeme", "upgrademe"]:
            output, error = self.execute_command("cmd /c schtasks /end /tn %s"
                                                 % task)
            self.log_command_output(output, error)

    def pause_memcached(self, timesleep=30, delay=0):
        """Override method for windows"""
        self.log.info("*** pause memcached process ***")
        if delay:
            self.sleep(delay)
        self.check_cmd("pssuspend")
        cmd = "pssuspend $(tasklist | grep  memcached | gawk '{printf $2}')"
        o, r = self.execute_command(cmd)
        self.log_command_output(o, [])
        self.log.info("wait %s seconds to make node down." % timesleep)
        self.sleep(timesleep)

    def unpause_memcached(self):
        self.log.info("*** unpause memcached process ***")
        cmd = "pssuspend -r $(tasklist | grep  memcached | gawk '{printf $2}')"
        o, r = self.execute_command(cmd)
        self.log_command_output(o, [])

    def pause_beam(self):
        """Override method"""
        raise NotImplementedError

    def unpause_beam(self):
        """Override method"""
        raise NotImplementedError

    def get_memcache_pid(self):
         output, error = self.execute_command('tasklist| grep memcache', debug=False)
         if error or output == [""] or output == []:
              return None
         words = output[0].split(" ")
         words = [x for x in words if x != ""]
         return words[1]

    def kill_erlang(self, os="unix", delay=0):
        if delay:
            time.sleep(delay)
        o, r = self.execute_command("taskkill /F /T /IM epmd.exe*")
        self.log_command_output(o, r)
        o, r = self.execute_command("taskkill /F /T /IM erl.exe*")
        self.log_command_output(o, r)
        o, r = self.execute_command("tasklist | grep erl.exe")
        kill_all = False
        count = 0
        while len(o) >= 1 and not kill_all:
            if o and "erl.exe" in o[0]:
                self.execute_command("taskkill /F /T /IM erl.exe*")
                self.sleep(1)
                o, r = self.execute_command("tasklist | grep erl.exe")
            if len(o) == 0:
                kill_all = True
                log.info("all erlang processes were killed")
            else:
                count += 1
            if count == 5:
                log.error("erlang process is not killed")
                break

    def kill_cbft_process(self):
        o, r = self.execute_command("taskkill /F /T /IM cbft.exe*")
        self.log_command_output(o, r)

    def kill_memcached(self, num_retries=10, poll_interval=2):
        o, r = self.execute_command("taskkill /F /T /IM memcached*")
        self.log_command_output(o, r, debug=False)

    def start_memcached(self):
        o, r = self.execute_command("taskkill /F /T /IM memcached")
        self.log_command_output(o, r, debug=False)

    def stop_memcached(self):
        o, r = self.execute_command("taskkill /F /T /IM memcached*")
        self.log_command_output(o, r, debug=False)

    def start_indexer(self):
        o, r = self.execute_command("taskkill /F /T /IM indexer*")
        self.log_command_output(o, r)

    def stop_indexer(self):
        o, r = self.execute_command("taskkill /F /T /IM indexer*")
        self.log_command_output(o, r, debug=False)

    def kill_goxdcr(self):
        o, r = self.execute_command("taskkill /F /T /IM goxdcr*")
        self.log_command_output(o, r)

    def kill_eventing_process(self, name):
        o, r = self.execute_command(command="taskkill /F /T /IM {0}*".format(name))
        self.log_command_output(o, r)

    def get_process_id(self, process_name):
        """Override method for windows"""
        raise NotImplementedError

    def terminate_processes(self, info, p_list):
        for process in p_list:
            # set debug=False if does not want to show log
            self.execute_command("taskkill /F /T /IM {0}"
                                 .format(process), debug=False)

    def terminate_process(self, info=None, process_name=None, force=False):
        """Override method for Windows"""
        if not process_name:
            log.info("Please specify process name to be terminated.")
            return
        o, r = self.execute_command("taskkill /F /T /IM {0}*"\
                                    .format(process_name), debug=False)
        self.log_command_output(o, r)

    def windows_process_utils(self, ps_name_or_id, cmd_file_name, option=""):
        success = False
        files_path = "cygdrive/c/utils/suspend/"
        # check to see if suspend files exist in server
        file_existed = self.file_exists(files_path, cmd_file_name)
        if file_existed:
            command = "{0}{1} {2} {3}".format(files_path, cmd_file_name,
                                              option, ps_name_or_id)
            o, r = self.execute_command(command)
            if not r:
                success = True
                self.log_command_output(o, r)
                self.sleep(30, "Wait for windows to execute completely")
            else:
                log.error(
                    "Command didn't run successfully. Error: {0}".format(r))
        else:
            o, r = self.execute_command(
                "netsh advfirewall firewall add rule name=\"block erl.exe in\" dir=in action=block program=\"%ProgramFiles%\Couchbase\Server\\bin\erl.exe\"")
            if not r:
                success = True
                self.log_command_output(o, r)
            o, r = self.execute_command(
                "netsh advfirewall firewall add rule name=\"block erl.exe out\" dir=out action=block program=\"%ProgramFiles%\Couchbase\Server\\bin\erl.exe\"")
            if not r:
                success = True
                self.log_command_output(o, r)
        return success

    def is_process_running(self, process_name):
        """Override method for windows"""
        self.log.info("%s - Checking for process %s" % (self.ip, process_name))
        output, error = self.execute_command(
            'tasklist | grep {0}'.format(process_name), debug=False)
        if error or output == [""] or output == []:
            return None
        words = output[0].split(" ")
        words = [x for x in words if x != ""]
        process = RemoteMachineProcess()
        process.pid = words[1]
        process.name = words[0]
        self.log.debug("Process is running: %s" % words)
        return process

    def reboot_node(self):
        o, r = self.execute_command("shutdown -r -f -t 0")
        self.log_command_output(o, r)

    def disable_firewall(self):
        output, error = self.execute_command('netsh advfirewall set publicprofile state off')
        self.log_command_output(output, error)
        output, error = self.execute_command('netsh advfirewall set privateprofile state off')
        self.log_command_output(output, error)
        # for details see RemoteUtilHelper.enable_firewall for windows
        output, error = self.execute_command('netsh advfirewall firewall delete rule name="block erl.exe in"')
        self.log_command_output(output, error)
        output, error = self.execute_command('netsh advfirewall firewall delete rule name="block erl.exe out"')
        self.log_command_output(output, error)

    def execute_commands_inside(self, main_command, query, queries,
                                bucket1, password, bucket2, source,
                                subcommands=[], min_output_size=0,
                                end_msg='', timeout=250):
        """
        Override method to handle windows specific file name
        """
        filename = "/cygdrive/c/tmp/test.txt"
        filedata = ""
        if not(query == ""):
            main_command = main_command + " -s=\"" + query+ '"'
        elif (self.remote and not(queries == "")):
            sftp = self._ssh_client.open_sftp()
            filein = sftp.open(filename, 'w')
            for query in queries:
                filein.write(query)
                filein.write('\n')
            fileout = sftp.open(filename, 'r')
            filedata = fileout.read()
            #print filedata
            fileout.close()
        elif not(queries==""):
            f = open(filename, 'w')
            for query in queries:
                f.write(query)
                f.write('\n')
            f.close()
            fileout = open(filename, 'r')
            filedata = fileout.read()
            fileout.close()

        if type(filedata) == bytes:
            filedata = filedata.decode()
        newdata = filedata.replace("bucketname",bucket2)
        newdata = newdata.replace("user",bucket1)
        newdata = newdata.replace("pass",password)
        newdata = newdata.replace("bucket1",bucket1)

        newdata = newdata.replace("user1",bucket1)
        newdata = newdata.replace("pass1",password)
        newdata = newdata.replace("bucket2",bucket2)
        newdata = newdata.replace("user2",bucket2)
        newdata = newdata.replace("pass2",password)

        if (self.remote and not(queries=="")) :
            f = sftp.open(filename,'w')
            f.write(newdata)
            f.close()
        elif not(queries==""):
            f = open(filename,'w')
            f.write(newdata)
            f.close()
        if not(queries==""):
            if (source):
                main_command = main_command + "  -s=\"\SOURCE " + 'c:\\\\tmp\\\\test.txt'
            else:
                main_command = main_command + " -f=" + 'c:\\\\tmp\\\\test.txt'

        log.info("running command on {0}: {1}".format(self.ip, main_command))
        output=""
        if self.remote:
            (stdin, stdout, stderro) = self._ssh_client.exec_command(main_command)
            time.sleep(10)
            count = 0
            for line in stdout.readlines():
                if (count == 0) and line.lower().find("error") > 0:
                   output = "status:FAIL"
                   break

                #if line.find("results") > 0 or line.find("status") > 0 or line.find("metrics") or line.find("elapsedTime")> 0 or  line.find("executionTime")> 0 or line.find("resultCount"):
                if (count > 0):
                    output+=line.strip()
                    output = output.strip()
                    if "Inputwasnotastatement" in output:
                        output = "status:FAIL"
                        break
                    if "timeout" in output:
                        output = "status:timeout"
                else:
                    count+=1
            stdin.close()
            stdout.close()
            stderro.close()
        else:
            p = Popen(main_command , shell=True, stdout=PIPE, stderr=PIPE)
            stdout, stderro = p.communicate()
            output = stdout
            print(output)
            time.sleep(1)
        if (self.remote and not(queries=="")) :
            sftp.remove(filename)
            sftp.close()
        elif not(queries==""):
            os.remove(filename)

        output = re.sub('\s+', '', output)
        return (output)

    def get_port_recvq(self, port):
        """
        Given a port, extracts address:port of services
        listening on that port (only ipv4)
        """
        command = "netstat -a -b -p tcp | grep :%s | grep 'LISTEN' " \
                  "| awk -F ' ' '{print $2}'" % port
        o, r = self.execute_command(command)
        self.log_command_output(o, r)
        return o

    def get_ip_address(self):
        """Override method"""
        raise NotImplementedError

    def get_processes_binding_to_ip_family(self, ip_family="ipv4"):
        """ Get all the processes binding to a particular ip family"""
        if ip_family == "ipv4":
            ip_family = "tcp"
        else:
            ip_family = "tcpv6"
        output_win, error = self.execute_command(
            "netstat -a -b -p {0} | grep exe | sort | uniq | sed \'s/\[//g; s/\]//g;\'".
            format(ip_family), debug=True)
        self.log_command_output(output_win, error, debug=True)
        output = list()
        for op in output_win:
            op = op.strip()
            if op in WIN_PROCESSES_SPAWNED:
                output.append(op)
        return output

    def cleanup_all_configuration(self, data_path):
        """
        Override method for windows
        Deletes the contents of the parent folder that holds the data and config directories.

        Args:
            data_path (str): The path key from the /nodes/self end-point which
            looks something like "/opt/couchbase/var/lib/couchbase/data" on
            Linux or "c:/Program Files/Couchbase/Server/var/lib/couchbase/data"
            on Windows.
        """
        path = data_path.replace("/data", "")
        if "c:/Program Files" in path:
            path = path.replace("c:/Program Files", "/cygdrive/c/Program\ Files")
        o, r = self.execute_command(f"rm -rf {path}/*")
        self.log_command_output(o, r)

    def cleanup_data_config(self, data_path):
        if "c:/Program Files" in data_path:
            data_path = data_path.replace("c:/Program Files",
                                          "/cygdrive/c/Program\ Files")
        o, r = self.execute_command("rm -rf ""{0}""/*".format(data_path))
        self.log_command_output(o, r)
        o, r = self.execute_command("rm -rf ""{0}""/*" \
                                    .format(
            data_path.replace("data", "config")))
        self.log_command_output(o, r)

    def __check_if_cb_service_stopped(self, service_name=None):
        if service_name:
            o, r = self.execute_command('sc query {0}'.format(service_name))
            for res in o:
                if "STATE" in res:
                    info = res.split(":")
                    is_stopped =  "STOPPED" in str(info[1])
                    return is_stopped

            log.error("Cannot identify service state for service {0}. "
                      "Host response is: {1}".format(service_name, str(o)))
            return True
        log.error("Service name is not specified!")
        return False

    def start_couchbase(self):
        retry = 0
        running = self.is_couchbase_running()
        while not running and retry < 3:
            log.info("Starting couchbase server")
            o, r = self.execute_command("net start couchbaseserver")
            self.log_command_output(o, r)
            self.sleep(5, "Waiting for 5 secs to start...on " + self.info.ip)
            running = self.is_couchbase_running()
            retry = retry + 1
        if not running and retry >= 3:
            sys.exit("Failed to start Couchbase server on " + self.info.ip)

    def stop_couchbase(self, num_retries=5, poll_interval=10):
        o, r = self.execute_command("net stop couchbaseserver")
        self.log_command_output(o, r)
        is_server_stopped = False
        retries = num_retries
        while not is_server_stopped and retries > 0:
            self.sleep(poll_interval, "Wait to stop service completely")
            is_server_stopped = self.__check_if_cb_service_stopped("couchbaseserver")
            retries -= 1

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

        o, r = self.execute_command(remote_command, self.info)
        if r:
            log.error("Command didn't run successfully. Error: {0}".format(r))
        return o

    def set_environment_variable(self, name, value):
        """Request an interactive shell session, export custom variable and
        restart Couchbase server.

        Shell session is necessary because basic SSH client is stateless.
        """
        shell = self._ssh_client.invoke_shell()
        shell.send('net stop CouchbaseServer\n')
        shell.send('set {0}={1}\n'.format(name, value))
        shell.send('net start CouchbaseServer\n')
        shell.close()

    def change_env_variables(self, dict):
        prefix = "\\n    "
        shell = self._ssh_client.invoke_shell()
        environmentVariables = ""
        init_file = "service_start.bat"
        file_path = "\"/cygdrive/c/Program Files/Couchbase/Server/bin/\""
        prefix = "\\n"
        backupfile = file_path + init_file + ".bak"
        sourceFile = file_path + init_file
        o, r = self.execute_command("cp " + sourceFile + " " + backupfile)
        self.log_command_output(o, r)
        for key in list(dict.keys()):
            o, r = self.execute_command("sed -i 's/{1}.*//' {0}"
                                        .format(sourceFile, key))
            self.log_command_output(o, r)
            o, r = self.execute_command(
                "sed -i 's/export ERL_FULLSWEEP_AFTER/export "
                "ERL_FULLSWEEP_AFTER\\n{1}={2}\\nexport {1}/' {0}"
                .format(sourceFile, key, dict[key]))
            self.log_command_output(o, r)

        for key in list(dict.keys()):
            environmentVariables += prefix + 'set {0}={1}'.format(key, dict[key])

        command = "sed -i 's/{0}/{0}".format("set NS_ERTS=%NS_ROOT%\erts-5.8.5.cb1\bin")
        command += environmentVariables + "/'" + " " + sourceFile
        o, r = self.execute_command(command)
        self.log_command_output(o, r)
        # Restart couchbase
        o, r = self.execute_command("net stop couchbaseserver")
        self.log_command_output(o, r)
        o, r = self.execute_command("net start couchbaseserver")
        self.log_command_output(o, r)
        shell.close()

    def reset_env_variables(self):
        shell = self._ssh_client.invoke_shell()
        init_file = "service_start.bat"
        file_path = "/cygdrive/c/Program\ Files/Couchbase/Server/bin/"
        backupfile = file_path + init_file + ".bak"
        sourceFile = file_path + init_file
        o, r = self.execute_command("mv " + backupfile + " " + sourceFile)
        self.log_command_output(o, r)
        # Restart couchbase
        o, r = self.execute_command("net stop couchbaseserver")
        self.log_command_output(o, r)
        o, r = self.execute_command("net start couchbaseserver")
        self.log_command_output(o, r)
        shell.close()
