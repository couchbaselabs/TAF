import os
from subprocess import PIPE, Popen
from typing import re

from shell_util.remote_machine import RemoteMachineProcess


class CommonShellAPIs(object):
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

    def cpu_stress(self, stop_time):
        o, r = self.execute_command("stress --cpu 20 --timeout {}".format(stop_time))
        self.log_command_output(o, r)

    def ram_stress(self, stop_time):
        o, r = self.execute_command("stress --vm 3 --vm-bytes 2.5G --timeout {}".format(stop_time))
        self.log_command_output(o, r)

    def enable_disk_readonly(self, disk_location):
        o, r = self.execute_command("chmod -R 444 {}".format(disk_location))
        self.log_command_output(o, r)

    def disable_disk_readonly(self, disk_location):
        o, r = self.execute_command("chmod -R 777 {}".format(disk_location))
        self.log_command_output(o, r)

    def stop_network(self, stop_time):
        """
        Stop the network for given time period and then restart the network
        on the machine.
        :param stop_time: Time duration for which the network service needs
        to be down in the machine
        :return: Nothing
        """
        command = "nohup service network stop && sleep {} " \
                  "&& service network start &"
        output, error = self.execute_command(command.format(stop_time))
        self.log_command_output(output, error)

    def wait_for_couchbase_started(self, num_retries=5, poll_interval=5,
                                   message="Waiting for couchbase startup finish."):
        while num_retries > 0:
            if self.is_couchbase_running():
                break
            self.sleep(timeout=poll_interval, message=message)
            num_retries -= 1
        else:
            self.log.error("Couchbase server is failed to start!")

    def get_file(self, remotepath, filename, todir):
        if self.file_exists(remotepath, filename):
            if self.remote:
                sftp = self._ssh_client.open_sftp()
                try:
                    filenames = sftp.listdir(remotepath)
                    for name in filenames:
                        if filename in name:
                            src_file = "{}/{}".format(remotepath, name)
                            dest_file = "{}/{}".format(todir, name)
                            self.log.info("Copying {} to {}"
                                          .format(src_file, dest_file))
                            sftp.get(src_file, dest_file)
                            sftp.close()
                            return True
                    sftp.close()
                    return False
                except IOError:
                    return False
            else:
                os.system("cp {0} {1}".format('{0}/{1}'.format(remotepath, filename), todir))

    def read_remote_file(self, remote_path, filename):
        if self.file_exists(remote_path, filename):
            if self.remote:
                sftp = self._ssh_client.open_sftp()
                remote_file = sftp.open('{0}/{1}'.format(remote_path, filename))
                try:
                    out = remote_file.readlines()
                finally:
                    remote_file.close()
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
        if self.remote:
            sftp = self._ssh_client.open_sftp()
            try:
                self.log.info("removing {0} directory...".format(remote_path))
                sftp.rmdir(remote_path)
            except IOError:
                return False
            finally:
                sftp.close()
        else:
            try:
                p = Popen("rm -rf {0}".format(remote_path), shell=True, stdout=PIPE, stderr=PIPE)
                stdout, stderro = p.communicate()
            except IOError:
                return False
        return True

    def rmtree(self, sftp, remote_path, level=0):
        count = 0
        for f in sftp.listdir_attr(remote_path):
            rpath = remote_path + "/" + f.filename
            if stat.S_ISDIR(f.st_mode):
                self.rmtree(sftp, rpath, level=(level + 1))
            else:
                rpath = remote_path + "/" + f.filename
                if count < 10:
                    print(('removing %s' % (rpath)))
                    count += 1
                    sftp.remove(rpath)
        print(('removing %s' % (remote_path)))
        sftp.rmdir(remote_path)

    def remove_directory_recursive(self, remote_path):
        if self.remote:
            sftp = self._ssh_client.open_sftp()
            try:
                self.log.info("removing {0} directory...".format(remote_path))
                self.rmtree(sftp, remote_path)
            except IOError:
                return False
            finally:
                sftp.close()
        else:
            try:
                p = Popen("rm -rf {0}".format(remote_path), shell=True, stdout=PIPE, stderr=PIPE)
                p.communicate()
            except IOError:
                return False
        return True

    def list_files(self, remote_path):
        if self.remote:
            sftp = self._ssh_client.open_sftp()
            files = []
            try:
                file_names = sftp.listdir(remote_path)
                for name in file_names:
                    files.append({'path': remote_path, 'file': name})
                sftp.close()
            except IOError:
                return []
            return files
        else:
            p = Popen("ls {0}".format(remote_path), shell=True, stdout=PIPE, stderr=PIPE)
            files, stderro = p.communicate()
            return files

    def file_ends_with(self, remotepath, pattern):
        """
         Check if file ending with this pattern is present in remote machine
        """
        sftp = self._ssh_client.open_sftp()
        files_matched = []
        try:
            file_names = sftp.listdir(remotepath)
            for name in file_names:
                if name.endswith(pattern):
                    files_matched.append("{0}/{1}".format(remotepath, name))
        except IOError:
            # ignore this error
            pass
        sftp.close()
        if len(files_matched) > 0:
            self.log.info("found these files : {0}".format(files_matched))
        return files_matched

    # check if this file exists in the remote
    # machine or not
    def file_starts_with(self, remotepath, pattern):
        sftp = self._ssh_client.open_sftp()
        files_matched = []
        try:
            file_names = sftp.listdir(remotepath)
            for name in file_names:
                if name.startswith(pattern):
                    files_matched.append("{0}/{1}".format(remotepath, name))
        except IOError:
            # ignore this error
            pass
        sftp.close()
        if len(files_matched) > 0:
            self.log.info("found these files : {0}".format(files_matched))
        return files_matched

    def file_exists(self, remotepath, filename, pause_time=30):
        sftp = self._ssh_client.open_sftp()
        try:
            if "Program" in remotepath:
                if "Program\\" in remotepath:
                    remotepath = remotepath.replace("Program\\", "Program")
                output, _ = self.execute_command("cat '{0}{1}'".format(remotepath, filename))
                if output and output[0]:
                    return True
                else:
                    return False

            filenames = sftp.listdir_attr(remotepath)
            for name in filenames:
                if filename in name.filename and int(name.st_size) > 0:
                    sftp.close()
                    return True
                elif filename in name.filename and int(name.st_size) == 0:
                    if name.filename == NR_INSTALL_LOCATION_FILE:
                        continue
                    self.log.info("File {0} will be deleted".format(filename))
                    if not remotepath.endswith("/"):
                        remotepath += "/"
                    self.execute_command("rm -rf {0}*{1}*".format(remotepath, filename))
                    self.sleep(pause_time, "** Network or sever may be busy. **"\
                                           "\nWait {0} seconds before executing next instrucion"\
                                                                             .format(pause_time))

            sftp.close()
            return False
        except IOError:
            return False

    def delete_file(self, remotepath, filename):
        sftp = self._ssh_client.open_sftp()
        delete_file = False
        try:
            filenames = sftp.listdir_attr(remotepath)
            for name in filenames:
                if name.filename == filename:
                    self.log.info("File {0} will be deleted".format(filename))
                    sftp.remove(remotepath + filename)
                    delete_file = True
                    break
            if delete_file:
                """ verify file is deleted """
                filenames = sftp.listdir_attr(remotepath)
                for name in filenames:
                    if name.filename == filename:
                        self.log.error("fail to remove file %s " % filename)
                        delete_file = False
                        break
            sftp.close()
            return delete_file
        except IOError:
            return False

    def copy_file_local_to_remote(self, src_path, des_path):
        result = True
        sftp = self._ssh_client.open_sftp()
        try:
            sftp.put(src_path, des_path)
        except IOError:
            self.log.error('Can not copy file')
            result = False
        finally:
            sftp.close()
        return result

    def copy_file_remote_to_local(self, rem_path, des_path):
        result = True
        sftp = self._ssh_client.open_sftp()
        try:
            sftp.get(rem_path, des_path)
        except IOError as e:
            self.log.error('Can not copy file', e)
            result = False
        finally:
            sftp.close()
        return result

    # copy multi files from local to remote server
    def copy_files_local_to_remote(self, src_path, des_path):
        files = os.listdir(src_path)
        self.log.info("copy files from {0} to {1}".format(src_path, des_path))
        # self.execute_batch_command("cp -r  {0}/* {1}".format(src_path, des_path))
        for file in files:
            if file.find("wget") != 1:
                a = ""
            full_src_path = os.path.join(src_path, file)
            full_des_path = os.path.join(des_path, file)
            self.copy_file_local_to_remote(full_src_path, full_des_path)

    # create a remote file from input string
    def create_file(self, remote_path, file_data):
        output, error = self.execute_command("echo '{0}' > {1}".format(file_data, remote_path))

    def find_file(self, remote_path, file):
        sftp = self._ssh_client.open_sftp()
        try:
            files = sftp.listdir(remote_path)
            for name in files:
                if name == file:
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
        sftp.close()

    def create_directory(self, remote_path):
        sftp = self._ssh_client.open_sftp()
        try:
            self.log.info("Checking if the directory {0} exists or not.".format(remote_path))
            sftp.stat(remote_path)
        except IOError as e:
            if e.errno == 2:
                self.log.info("Directory at {0} DOES NOT exist. We will create on here".format(remote_path))
                sftp.mkdir(remote_path)
                sftp.close()
                return False
            raise
        else:
            self.log.error("Directory at {0} DOES exist. Fx returns True".format(remote_path))
            return True

    def check_directory_exists(self, remote_path):
        sftp = self._ssh_client.open_sftp()
        try:
            self.log.info("Checking if the directory {0} exists or not.".format(remote_path))
            sftp.stat(remote_path)
        except IOError as e:
            self.log.info(f'Directory at {remote_path} DOES NOT exist.')
            sftp.close()
            return False
        self.log.info("Directory at {0} exist.")
        sftp.close()
        return True

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
            sftp.close()
        except IOError:
            pass

    def _check_output(self, word_check, output):
        found = False
        if len(output) >= 1:
            if isinstance(word_check, list):
                for ele in word_check:
                    for x in output:
                        if ele.lower() in str(x.lower()):
                            self.log.info("Found '{0} in output".format(ele))
                            found = True
                            break
            elif isinstance(word_check, str):
                for x in output:
                    if word_check.lower() in str(x.lower()):
                        self.log.info("Found '{0}' in output".format(word_check))
                        found = True
                        break
            else:
                self.log.error("invalid {0}".format(word_check))
        return found

    """
        This method install Couchbase Server
    """
    def wait_till_file_deleted(self, remotepath, filename, timeout_in_seconds=180):
        end_time = time.time() + float(timeout_in_seconds)
        deleted = False
        self.log.info("file {0} checked at {1}".format(filename, remotepath))
        while time.time() < end_time and not deleted:
            # get the process list
            exists = self.file_exists(remotepath, filename)
            if exists:
                self.log.error('at {2} file {1} still exists' \
                               .format(remotepath, filename, self.ip))
                time.sleep(2)
            else:
                self.log.info('at {2} FILE {1} DOES NOT EXIST ANYMORE!' \
                         .format(remotepath, filename, self.ip))
                deleted = True
        return deleted

    def wait_till_file_added(self, remotepath, filename, timeout_in_seconds=180):
        end_time = time.time() + float(timeout_in_seconds)
        added = False
        self.log.info("file {0} checked at {1}".format(filename, remotepath))
        while time.time() < end_time and not added:
            # get the process list
            exists = self.file_exists(remotepath, filename)
            if not exists:
                self.log.error('at {2} file {1} does not exist'
                               .format(remotepath, filename, self.ip))
                time.sleep(2)
            else:
                self.log.info('at {2} FILE {1} EXISTS!'
                              .format(remotepath, filename, self.ip))
                added = True
        return added

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
                        self.log.error("{1}: process {0} did not run after 25 seconds"
                                       .format(process_name, self.ip))
                        mesg = "kill in/uninstall job due to process was not run" \
                               .format(process_name, self.ip)
                        self.stop_current_python_running(mesg)
        if time.time() >= end_time and not process_ended:
            self.log.info("Process {0} on node {1} is still running"
                          " after 10 minutes VERSION.txt file was removed"
                          .format(process_name, self.ip))
        return process_ended

    def remove_folders(self, list):
        for folder in list:
            output, error = self.execute_command(
                "rm -rf {0}".format(folder), debug=False)
            self.log_command_output(output, error)

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
                    self.log.info(
                        "There is a warning about transparent_hugepage "
                        "may be in used when install cb server.\
                        So we will disable transparent_hugepage in this vm")
                    output, error = self.execute_command(
                        "echo never > "
                        "/sys/kernel/mm/transparent_hugepage/enabled")
                    self.log_command_output(output, error)
                    success = True
                elif "Warning" in line and "systemctl daemon-reload" in line:
                    self.log.info(
                        "Unit file of couchbase-server.service changed on "
                        "disk, we will run 'systemctl daemon-reload'")
                    output, error = self.execute_command("systemctl daemon-reload")
                    self.log_command_output(output, error)
                    success = True
                elif "Warning" in line and "RPMDB altered outside of yum" in line:
                    self.log.info("Warming: RPMDB altered outside of yum")
                    success = True
                elif "dirname" in line:
                    self.log.warning(
                        "Ignore dirname error message during couchbase "
                        "startup/stop/restart for CentOS 6.6 (MB-12536)")
                    success = True
                elif "Created symlink from /etc/systemd/system" in line:
                    self.log.info(
                        "This error is due to fix_failed_install.py script "
                        "that only happens in centos 7")
                    success = True
                elif "Created symlink /etc/systemd/system/multi-user.target.wants/couchbase-server.service" in line:
                    self.log.info(line)
                    self.log.info(
                        "This message comes only in debian8 and debian9 "
                        "during installation. This can be ignored.")
                    success = True
                else:
                    self.log.info(
                        "If couchbase server is running with this error. Go to"
                        " log_command_output to add error mesg to bypass it.")
                    success = False
        if self._check_output(list(track_words), output):
            success = False
            install_ok = False
            if self._check_output("hugepages", output):
                self.log.info(
                    "There is a warning about transparent_hugepage may be "
                    "in used when install cb server. So we will"
                    "So we will disable transparent_hugepage in this vm")
                output, error = self.execute_command(
                    "echo never > /sys/kernel/mm/transparent_hugepage/enabled")
                success = True
                install_ok = True
            if self._check_output("successfully installed couchbase server", output):
                success = True
                install_ok = True
            if not install_ok:
                self.log.error(
                    'something wrong happened on {0}!!! output:{1}, '
                    'error:{2}, track_words:{3}'
                    .format(self.ip, output, error, track_words))
        elif debug and output:
            for line in output:
                self.log.info(line)
        return success

    def execute_commands_inside(self, main_command, query, queries,
                                bucket1, password, bucket2, source,
                                subcommands=[], min_output_size=0,
                                end_msg='', timeout=250):
        filename = "/tmp/test2"
        filedata = ""
        if not(query == ""):
            main_command = main_command + " -s=\"" + query + '"'
        elif self.remote and not(queries == ""):
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

        if type(filedata) == bytes:
            filedata = filedata.decode()
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
            if source:
                main_command = main_command + " -s=\"\SOURCE " + filename + '"'
            else:
                main_command = main_command + " -f=" + filename

        self.log.info("%s - Running command: %s" % (self.ip, main_command))
        output = ""
        if self.remote:
            (stdin, stdout, stderro) = self._ssh_client.exec_command(main_command)
            self.sleep(10)
            count = 0
            for line in stdout.readlines():
                if (count == 0) and line.lower().find("error") > 0:
                    output = "status:FAIL"
                    break

                if count > 0:
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
        else:
            p = Popen(main_command, shell=True, stdout=PIPE, stderr=PIPE)
            stdout, stderro = p.communicate()
            output = stdout
            print(output)
            self.sleep(1)
        if self.remote and not(queries == ""):
            sftp.remove(filename)
            sftp.close()
        elif not(queries == ""):
            os.remove(filename)

        output = re.sub('\s+', '', output)
        return output

    def execute_command(self, command, info=None, debug=True,
                        use_channel=False, timeout=600, get_exit_code=False):
        if getattr(self, "info", None) is None and info is not None :
            self.info = info

        if self.info.type.lower() == 'windows':
            self.use_sudo = False

        if self.use_sudo:
            command = "sudo " + command

        return self.execute_command_raw(
            command, debug=debug, use_channel=use_channel,
            timeout=timeout, get_exit_code=get_exit_code)

    def reconnect_if_inactive(self):
        """
        If the SSH channel is inactive, retry the connection
        """
        tp = self._ssh_client.get_transport()
        if tp and not tp.active:
            self.log.warning("%s - SSH connection inactive" % self.ip)
            self.ssh_connect_with_retries(self.ip, self.username,
                                          self.password, self.ssh_key)

    def execute_command_raw(self, command, debug=True, use_channel=False,
                            timeout=600, get_exit_code=False):
        self.log.debug("%s - Running command.raw: %s" % (self.ip, command))
        self.reconnect_if_inactive()
        output = []
        error = []
        temp = ''
        p, stdout, exit_code = None, None, None
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
                temp += data.decode()
                data = channel.recv(1024)
            channel.close()
            stdin.close()
        elif self.remote:
            stdin, stdout, stderro = self._ssh_client.exec_command(
                command, timeout=timeout)
            stdin.close()

        if not self.remote:
            p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
            output, error = p.communicate()

        if get_exit_code:
            if stdout:
                exit_code = stdout.channel.recv_exit_status()
            if p:
                exit_code = p.returncode

        if self.remote:
            for line in stdout.read().splitlines():
                output.append(line.decode('utf-8', errors='replace'))
            for line in stderro.read().splitlines():
                error.append(line.decode('utf-8', errors='replace'))
            if temp:
                line = temp.splitlines()
                output.extend(line)
            stdout.close()
            stderro.close()
        if debug:
            if len(error):
                self.log.info('command executed with {} but got an error {} ...'.format(
                    self.server.ssh_username, str(error)[:400]))
        return (output, error, exit_code) if get_exit_code else (output, error)

    def execute_non_sudo_command(self, command, info=None, debug=True,
                                 use_channel=False):
        return self.execute_command_raw(command, debug=debug,
                                        use_channel=use_channel)

    def terminate_process(self, info=None, process_name=None, force=False):
        if not process_name:
            self.log.info("Please specify process name to be terminated.")
            return
        cmd = "kill "
        if force is True:
            cmd = "kill -9 "
        o, r = self.execute_command(
            "{} $(ps aux | grep '{}' |  awk '{{print $2}}')"
            .format(cmd, process_name), debug=False)
        self.log_command_output(o, r, debug=False)

    def get_aws_public_hostname(self):
        # AWS supported url to retrieve metadata like public hostname of an instance from shell
        output, _ = self.execute_command(
            "curl -s http://169.254.169.254/latest/meta-data/public-hostname")
        return output[0]

    def get_full_hostname(self):
        if not info.domain:
            return None
        self.log.info("%s - Hostname is %s" % (self.ip, info.hostname[0]))
        if info.domain[0]:
            if info.domain[0][0]:
                self.log.info("domain name of this {0} is {1}"
                     .format(self.ip, info.domain[0][0]))
                if info.domain[0][0] in info.hostname[0]:
                    return "{0}".format(info.hostname[0])
                else:
                    return "{0}.{1}".format(info.hostname[0], info.domain[0][0])
            else:
                mesg = "Need to set domain name in server {0} like 'sc.couchbase.com'"\
                                                                       .format(self.ip)
                raise Exception(mesg)
        else:
            return "{0}.{1}".format(info.hostname[0], 'sc.couchbase.com')

    def get_ram_info(self, win_info=None, mac=False):
        if win_info:
            if 'Virtual Memory Max Size' not in win_info:
                win_info = self.create_windows_info()
            o = "Virtual Memory Max Size =" + win_info['Virtual Memory Max Size'] + '\n'
            o += "Virtual Memory Available =" + win_info['Virtual Memory Available'] + '\n'
            o += "Virtual Memory In Use =" + win_info['Virtual Memory In Use']
        elif mac:
            o, r = self.execute_command_raw('/sbin/sysctl -n hw.memsize', debug=False)
        else:
            o, r = self.execute_command_raw('cat /proc/meminfo', debug=False)
        if o:
            return o

    def get_disk_info(self, win_info=None, mac=False):
        if win_info:
            if 'Total Physical Memory' not in win_info:
                win_info = self.create_windows_info()
            o = "Total Physical Memory =" + win_info['Total Physical Memory'] + '\n'
            o += "Available Physical Memory =" + win_info['Available Physical Memory']
        elif mac:
            o, r = self.execute_command_raw('df -hl', debug=False)
        else:
            o, r = self.execute_command_raw('df -Thl', debug=False)
        if o:
            return o

    def get_ip_address(self):
        ip_type = "inet \K[\d.]"
        ipv6_server = False
        if "ip6" in self.ip or self.ip.startswith("["):
            ipv6_server = True
            ip_type = "inet6 \K[0-9a-zA-Z:]"
        cmd = "ifconfig | grep -Po '{0}+'".format(ip_type)
        o, r = self.execute_command_raw(cmd)
        if ipv6_server:
            for x in range(len(o)):
                o[x] = "[{0}]".format(o[x])
        return o

    def get_processes_binding_to_ip_family(self, ip_family="ipv4"):
        """ Get all the processes binding to a particular ip family"""
        output, error = self.execute_command(
            "lsof -i -P -n | grep LISTEN | grep couchbase| grep -i {0}"
            .format(ip_family), debug=True)
        self.log_command_output(output, error, debug=True)
        return output

    def cleanup_all_configuration(self, data_path):
        """ Deletes the contents of the parent folder that holds the data and config directories.

        Args:
            data_path (str): The path key from the /nodes/self end-point which
            looks something like "/opt/couchbase/var/lib/couchbase/data" on
            Linux or "c:/Program Files/Couchbase/Server/var/lib/couchbase/data"
            on Windows.
        """
        # The path returned on both Linux and Windows by the /nodes/self end-point uses forward slashes.
        path = data_path.replace("/data", "")
        o, r = self.execute_command("rm -rf %s/*" % path)
        self.log_command_output(o, r)

    def cleanup_data_config(self, data_path):
        self.extract_remote_info()
        o, r = self.execute_command("rm -rf {0}/*".format(data_path))
        self.log_command_output(o, r)
        o, r = self.execute_command(
            "rm -rf {0}/*".format(data_path.replace("data", "config")))
        self.log_command_output(o, r)

    def pause_memcached(self, timesleep=30, delay=0):
        self.log.info("*** pause memcached process ***")
        if delay:
            time.sleep(delay)
        if self.nonroot:
            o, r = self.execute_command("killall -SIGSTOP memcached.bin")
        else:
            o, r = self.execute_command("killall -SIGSTOP memcached")
        self.log_command_output(o, r)
        self.log.info("wait %s seconds to make node down." % timesleep)
        time.sleep(timesleep)

    def unpause_memcached(self, os="linux"):
        self.log.info("*** unpause memcached process ***")
        if self.nonroot:
            o, r = self.execute_command("killall -SIGCONT memcached.bin")
        else:
            o, r = self.execute_command("killall -SIGCONT memcached")
        self.log_command_output(o, r)

    def pause_beam(self):
        o, r = self.execute_command("killall -SIGSTOP beam.smp")
        self.log_command_output(o, r)

    def unpause_beam(self):
        o, r = self.execute_command("killall -SIGCONT beam.smp")
        self.log_command_output(o, r)

    def get_data_file_size(self, path=None):
        output, error = self.execute_command('du -b {0}'.format(path))
        if error:
            return 0
        else:
            for line in output:
                size = line.strip().split('\t')
                if size[0].isdigit():
                    print((size[0]))
                    return size[0]
                else:
                    return 0

    def get_process_statistics_parameter(self, parameter,
                                         process_name=None, process_pid=None):
       if not parameter:
           self.log.error("parameter cannot be None")

       parameters_list = self.get_process_statistics(process_name, process_pid)

       if not parameters_list:
           self.log.error("no statistics found")
           return None
       parameters_dic = dict(item.split(' = ') for item in parameters_list)

       if parameter in parameters_dic:
           return parameters_dic[parameter]
       else:
           self.log.error("parameter '{0}' is not found".format(parameter))
           return None

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

    def delete_files(self, file_location, debug=False):
        command = "%s%s" % ("rm -rf ", file_location)
        output, error = self.execute_command(command, debug=debug)
        if debug:
            self.log_command_output(output, error)

    def execute_cbcollect_info(self, file, options=""):
        cbcollect_command = "%scbcollect_info" % (LINUX_COUCHBASE_BIN_PATH)
        if self.nonroot:
            cbcollect_command = "%scbcollect_info" % (LINUX_NONROOT_CB_BIN_PATH)
        self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            cbcollect_command = "%scbcollect_info.exe" % (WIN_COUCHBASE_BIN_PATH)
        if self.info.distribution_type.lower() == 'mac':
            cbcollect_command = "%scbcollect_info" % (MAC_COUCHBASE_BIN_PATH)

        command = "%s %s %s" % (cbcollect_command, file, options)
        output, error = self.execute_command(command, use_channel=True)
        return output, error

    def execute_batch_command(self, command):
        remote_command = "echo \"%s\" > /tmp/cmd.bat ; " \
                         "chmod u=rwx /tmp/cmd.bat; /tmp/cmd.bat" % command
        o, r = self.execute_command_raw(remote_command)
        if r and r!=['']:
            self.log.error("Command didn't run successfully. Error: {0}".format(r))
        return o, r

    def is_enterprise(self):
        enterprise = False
        runtime_file_path = ""
        if self.nonroot:
            if self.file_exists("%s/opt/couchbase/etc/" % self.nr_home_path,
                                "runtime.ini"):
                runtime_file_path = "%s/opt/couchbase/etc/" % self.nr_home_path
            else:
                self.log.info("{} - Couchbase server may not yet installed in nonroot server"
                              .format(self.ip))
        elif self.file_exists("/opt/couchbase/etc/", "runtime.ini"):
            runtime_file_path = "/opt/couchbase/etc/"
        else:
            self.log.info("{} - Couchbase server not found".format(self.ip))
        output = self.read_remote_file(runtime_file_path, "runtime.ini")
        for x in output:
            x = x.strip()
            if x and "license = enterprise" in x:
                enterprise = True
        return enterprise

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
        self.log.info(mesg)
        self.sleep(5, "==== delay kill pid %d in 5 seconds to printout message ==="\
                                                                      % os.getpid())
        os.system('kill %d' % os.getpid())

    def diag_eval(self, diag_eval_command):
        """ Executes a diag eval command
        Args:
            diag_eval_comand (str): A diag eval command
            e.g. "gen_server:cast(ns_cluster, leave)."
        """
        self.execute_command(
            "curl -X POST localhost:%s/diag/eval -d \"%s\" -u %s:%s"
            % (self.port, diag_eval_command,
               self.server.rest_username, self.server.rest_password))

    def enable_diag_eval_on_non_local_hosts(self, state=True):
        """
        Enable diag/eval to be run on non-local hosts.
        :return: Command output and error if any.
        """
        rest_username = self.server.rest_username
        rest_password = self.server.rest_password

        protocol = "-k https://" if str(self.port) == "18091" else "http://"
        command = "curl --silent --show-error {4}{0}:{1}@localhost:{2}/diag/eval -X POST -d " \
                  "'ns_config:set(allow_nonlocal_eval, {3}).'"\
            .format(rest_username, rest_password, self.port,
                    state.__str__().lower(), protocol)

        output, error = self.execute_command(command)
        self.log.info(output)
        try:
            output = output.decode()
        except AttributeError:
            pass
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

    def unmount_partition(self, location):
        """
        Unmount the partition at the specified location.
        :param location: Location of the partition which has to be unmounted
        :return: Output and error message from the umount command
        """
        command = "umount -l {0}; df -Th".format(location)
        output, error = self.execute_command(command)
        return output, error

    def fill_disk_space(self, location):
        """
        Fill up the disk fully at the location specified.
        This method creates a junk file of the specified size in the location specified
        :param location: Location to fill the disk
        :param size: Size of disk space to fill up, in MB
        :return: Output and error message from filling up the disk.
        """
        command = "dd if=/dev/zero of={0}/disk-quota.ext3 count={1}; df -Thl"\
            .format(location, 1024000000)
        output, error = self.execute_command(command)
        return output, error

    def _recover_disk_full_failure(self, location):
        delete_file = "{0}/disk-quota.ext3".format(location)
        output, error = self.execute_command("rm -f {0}".format(delete_file))
        return output, error

    def get_process_id(self, process_name):
        process_id, _ = self.execute_command(
            "ps -ef | grep \"%s \" | grep -v grep | awk '{print $2}'"
            % process_name)
        return process_id[0].strip()

    def update_dist_type(self):
        output, error = self.execute_command(
            "echo '{{dist_type,inet6_tcp}}.' > {0}".format(LINUX_DIST_CONFIG))
        self.log_command_output(output, error)

    def alt_addr_add_node(self, main_server=None, internal_IP=None,
                          server_add=None, user="Administrator",
                          passwd="password", services="kv", cmd_ext=""):
        """ in alternate address, we need to use curl to add node """
        if internal_IP is None:
            raise Exception("Need internal IP to add node.")
        if main_server is None:
            raise Exception("Need master IP to run")
        cmd = 'curl{0} -X POST -d "hostname={1}&user={2}&password={3}&services={4}" '\
            .format(cmd_ext, internal_IP, server_add.rest_username,
                    server_add.rest_password, services)
        cmd += '-u {0}:{1} https://{2}:18091/controller/addNode'\
            .format(main_server.rest_username, main_server.rest_password,
                    main_server.ip)
        output, error = self.execute_command(cmd)
        return output, error
