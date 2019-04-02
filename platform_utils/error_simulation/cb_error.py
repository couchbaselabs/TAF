import logger
from cb_tools.cbepctl import Cbepctl

log = logger.Logger.get_logger()


class CouchbaseError(object):
    def __init__(self, shell_conn):
        self.shell_conn = shell_conn

    def __handle_shell_error(self, error):
        if len(error) != 0:
            raise("\n".join(error))

    def __interrupt_process(self, process_name, action):
        if action == "stop":
            # Send SIGSTOP signal
            return self.kill_process(process_name, process_name, signum=19)
            # Send SIGCONT signal
        elif action == "resume":
            return self.kill_process(process_name, process_name, signum=18)
        elif action == "kill":
            # Send SIGKILL signal
            return self.kill_process(process_name, process_name, signum=9)

    def create(self, action=None, bucket_name="default"):
        if action == "stop_memcached":
            _, error = self.__interrupt_process("memcached", "stop")
            self.__handle_shell_error(error)
        elif action == "kill_memcached":
            _, error = self.__interrupt_process("memcached", "kill")
            self.__handle_shell_error(error)
        elif action == "stop_persistence":
            cbepctl_obj = Cbepctl(self.shell_conn)
            cbepctl_obj.persistence(bucket_name, "stop")
        else:
            log.error("Unsupported action: '{0}'".format(action))

    def revert(self, action=None, bucket_name="default"):
        if action == "stop_memcached":
            _, error = self.__interrupt_process("memcached", "resume")
            self.__handle_shell_error(error)
        elif action == "start_persistence":
            cbepctl_obj = Cbepctl(self.shell_conn)
            cbepctl_obj.persistence(bucket_name, "start")
        else:
            log.error("Unsupported action to revert: '{0}'".format(action))
