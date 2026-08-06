from shell_util.platforms.constants import UnixConstants
from shell_util.shell_conn import ShellConnection


class Unix(ShellConnection, UnixConstants):
    def __init__(self, test_server, info=None):
        super(Unix, self).__init__(test_server)
        self.nonroot = False
        self.info = info

    def connect_with_user(self, user="root"):
        """Override method since this is not required for Unix"""
        return

    def get_cbversion(self):
        """ fv = a.b.c-xxxx, sv = a.b.c, bn = xxxx """
        fv = sv = bn = ""
        if self.file_exists(self.cb_path, self.version_file):
            output = self.read_remote_file(self.cb_path, self.version_file)
            if output:
                for x in output:
                    x = x.strip()
                    if x and x[:5] in self.cb_release_builds.keys() \
                            and "-" in x:
                        fv = x
                        tmp = x.split("-")
                        sv = tmp[0]
                        bn = tmp[1]
                    break
        else:
            self.log.info("%s - Couchbase Server not found" % self.ip)
        return fv, sv, bn

    def is_couchbase_installed(self):
        output, error = self.execute_command('ls %s%s' % (self.cb_path,
                                                          self.version_file))
        self.log_command_output(output, error)
        for line in output:
            if line.find('No such file or directory') == -1:
                return True
        return False

    def is_couchbase_running(self):
        o = self.is_process_running('beam.smp')
        if o is not None:
            return True
        return False

    def stop_membase(self):
        """Override method"""
        raise NotImplementedError

    def start_server(self):
        o, r = self.execute_command("open /Applications/Couchbase\ Server.app")
        self.log_command_output(o, r)

    def stop_server(self, os="unix"):
        cb_process = '/Applications/Couchbase\ Server.app/Contents/MacOS/Couchbase\ Server'
        cmd = "ps aux | grep %s | awk '{{print $2}}' | xargs kill -9 " \
              % cb_process
        o, r = self.execute_command(cmd)
        self.log_command_output(o, r)
        o, r = self.execute_command("killall -9 epmd")
        self.log_command_output(o, r)

    def restart_couchbase(self):
        o, r = self.execute_command("open /Applications/Couchbase\ Server.app")
        self.log_command_output(o, r)

    def kill_cbft_process(self):
        o, r = self.execute_command("killall -9 cbft")
        self.log_command_output(o, r)
        if r and r[0] and "command not found" in r[0]:
            o, r = self.execute_command("pkill cbft")
            self.log_command_output(o, r)
        return o, r

    def get_memcache_pid(self):
        raise NotImplementedError

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
                self.log.info("memcached pid:{} and err: {}".format(out, err))
                break
            else:
                num_retries -= 1
        return o, r

    def start_memcached(self):
        o, r = self.execute_command("kill -SIGCONT $(pgrep memcached)")
        self.log_command_output(o, r, debug=False)

    def stop_memcached(self):
        o, r = self.execute_command("kill -SIGSTOP $(pgrep memcached)")
        self.log_command_output(o, r, debug=False)

    def start_indexer(self):
        o, r = self.execute_command("kill -SIGCONT $(pgrep indexer)")
        self.log_command_output(o, r)

    def stop_indexer(self):
        o, r = self.execute_command("kill -SIGSTOP $(pgrep indexer)")
        self.log_command_output(o, r, debug=False)

    def kill_goxdcr(self):
        o, r = self.execute_command("killall -9 goxdcr")
        self.log_command_output(o, r)

    def kill_eventing_process(self, name):
        o, r = self.execute_command(command="killall -9 {0}".format(name))
        self.log_command_output(o, r)

    def terminate_processes(self, info, p_list):
        raise NotImplementedError

    def get_port_recvq(self, port):
        raise NotImplementedError

    def start_couchbase(self):
        retry = 0
        running = self.is_couchbase_running()
        while not running and retry < 3:
            self.log.info("Starting couchbase server")
            o, r = self.execute_command("open /Applications/Couchbase\ Server.app")
            self.log_command_output(o, r)
            running = self.is_couchbase_running()
            retry = retry + 1
        if not running and retry >= 3:
            self.log.critical("%s - Server not started even after 3 retries" % self.info.ip)
            return False
        return True

    def stop_couchbase(self, num_retries=5, poll_interval=10):
        cb_process = '/Applications/Couchbase\ Server.app/Contents/MacOS/Couchbase\ Server'
        cmd = "ps aux | grep {0} | awk '{{print $2}}' | xargs kill -9 "\
            .format(cb_process)
        o, r = self.execute_command(cmd)
        self.log_command_output(o, r)
        o, r = self.execute_command("killall -9 epmd")
        self.log_command_output(o, r)
