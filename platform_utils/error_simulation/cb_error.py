from cb_tools.cbepctl import Cbepctl


class CouchbaseError:
    # Constants used within this class
    STOP_BEAMSMP = "stop_beam.smp"
    STOP_MEMCACHED = "stop_memcached"
    STOP_PERSISTENCE = "stop_persistence"
    STOP_SERVER = "stop_server"
    STOP_PROMETHEUS = "stop_prometheus"

    KILL_PROMETHEUS = "kill_prometheus"
    KILL_MEMCACHED = "kill_memcached"
    KILL_BEAMSMP = "kill_beam.smp"

    def __init__(self, logger, shell_conn):
        self.log = logger
        self.shell_conn = shell_conn

    def __handle_shell_error(self, error):
        if len(error) != 0:
            raise Exception("\n".join(error))

    def __interrupt_process(self, process_name, action):
        if action == "stop":
            # Send SIGSTOP signal
            return self.shell_conn.kill_process(process_name, process_name,
                                                signum=19)
            # Send SIGCONT signal
        elif action == "resume":
            return self.shell_conn.kill_process(process_name, process_name,
                                                signum=18)
        elif action == "kill":
            # Send SIGKILL signal
            return self.shell_conn.kill_process(process_name, process_name,
                                                signum=9)

    def create(self, action=None, bucket_name="default"):
        self.log.info("Simulating '{0}' in {1}".format(action,
                                                       self.shell_conn.ip))
        if action == CouchbaseError.STOP_MEMCACHED:
            _, error = self.__interrupt_process("memcached", "stop")
            self.__handle_shell_error(error)
        elif action == CouchbaseError.KILL_MEMCACHED:
            _, error = self.__interrupt_process("memcached", "kill")
            self.__handle_shell_error(error)
        elif action == CouchbaseError.STOP_BEAMSMP:
            _, error = self.__interrupt_process("beam.smp", "stop")
            self.__handle_shell_error(error)
        elif action == CouchbaseError.STOP_PROMETHEUS:
            _, error = self.__interrupt_process("prometheus", "stop")
            self.__handle_shell_error(error)
        elif action == CouchbaseError.KILL_BEAMSMP:
            _, error = self.__interrupt_process("beam.smp", "kill")
            self.__handle_shell_error(error)
        elif action == CouchbaseError.KILL_PROMETHEUS:
            _, error = self.__interrupt_process("prometheus", "kill")
            self.__handle_shell_error(error)
        elif action == CouchbaseError.STOP_SERVER:
            self.shell_conn.stop_server()
        elif action == CouchbaseError.STOP_PERSISTENCE:
            cbepctl_obj = Cbepctl(self.shell_conn)
            cbepctl_obj.persistence(bucket_name, "stop")
        else:
            self.log.error("Unsupported action: '{0}'".format(action))

    def revert(self, action=None, bucket_name="default"):
        self.log.info("Reverting '{0}' in {1}"
                      .format(action, self.shell_conn.ip))
        if action == CouchbaseError.STOP_MEMCACHED:
            _, error = self.__interrupt_process("memcached", "resume")
            self.__handle_shell_error(error)
        elif action == CouchbaseError.STOP_BEAMSMP:
            _, error = self.__interrupt_process("beam.smp", "resume")
            self.__handle_shell_error(error)
        elif action == CouchbaseError.STOP_PROMETHEUS:
            _, error = self.__interrupt_process("prometheus", "resume")
            self.__handle_shell_error(error)
        elif action == CouchbaseError.KILL_BEAMSMP \
                or action == CouchbaseError.STOP_SERVER:
            self.shell_conn.start_server()
        elif action == CouchbaseError.STOP_PERSISTENCE:
            cbepctl_obj = Cbepctl(self.shell_conn)
            cbepctl_obj.persistence(bucket_name, "start")
        else:
            self.log.error("Unsupported action to revert: '{0}'"
                           .format(action))
