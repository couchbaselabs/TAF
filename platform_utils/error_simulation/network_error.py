class NetworkError:
    def __init__(self, logger, shell_conn, ethernet_port="eth0"):
        self.log = logger
        self.shellConn = shell_conn
        self.ethernet_port = ethernet_port
        unsupported_os = ['windows', 'mac']
        curr_os = self.shellConn.extract_remote_info().type.lower()
        if curr_os in [unsupported_os]:
            raise("NetworkError not supported for {0}".format(curr_os))

    def limit_bandwidth_of_network_interface(self, downlink_speed,
                                             uplink_speed):
        cmd = "wondershaper {0} {1} {2}".format(
            self.ethernet_port, downlink_speed, uplink_speed)
        self.log.info("Executing: {0}".format(cmd))
        output, error = self.shellConn.execute_command(cmd)
        if "command not found" in output:
            raise("wondershaper not installed."
                  " Try 'yum install epel-release -y'"
                  "     'yum install wondershaper -y' to install")
        else:
            self.log.info("Output: {0}".format(output))
        return output, error

    def clear_the_network_speed_limit(self):
        cmd = "wondershaper clear {0}".format(self.ethernet_port)
        return self.shellConn.execute_command(cmd)

    def revert(self, action=None):
        if action == "limit_bandwidth":
            self.clear_the_network_speed_limit()
