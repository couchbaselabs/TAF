import logger

log = logger.Logger.get_logger()


class NetworkError():
    def __init__(self, shell_conn):
        self.shellConn = shell_conn
        if self.shellConn.extract_remote_info().type.lower() == 'windows':
            self.fail("Not implemented for windows")
        elif self.shellConn.extract_remote_info().type.lower() == 'mac':
            self.fail("Not implemented for mac")

    def __execute_cmd(self, cmd):
        """
        Executed the given command in the target shell
        Arguments:
        :cmd - Command to execute

        Returns:
        :output - Output for the command execution
        :error  - Buffer containing warnings/errors from the execution
        """
        log.info("Executing: {0}".format(cmd))
        return self.shellConn.execute_command(cmd)

    def limit_bandwidth_of_network_interface(self, downlink_speed, uplink_speed, ethernet_port="eth0"):
        cmd = "wondershaper {0} {1} {2}".format(ethernet_port, downlink_speed, uplink_speed)
        log.info("Executing: {0}".format(cmd))
        output, error = self.shellConn.execute_command(cmd)
        if "command not found" in output:
            self.fail("wondershaper not installed : Use 'yum install epel-release -y'",
                      "'yum install wondershaper -y' to install")
        else:
            log.info("Output : {0}".format(output))
            return output, error

    def clear_the_network_speed_limit(self, ethernet_port="eth0"):
        cmd = "wondershaper clear {0}".format(ethernet_port)
        log.info("Executing: {0}".format(cmd))
        return self.shellConn.execute_command(cmd)
