from cb_tools.cb_tools_base import CbCmdBase


class CbCollectInfo(CbCmdBase):
    def __init__(self, shell_conn, username="Administrator",
                 password="password"):

        CbCmdBase.__init__(self, shell_conn, "cbcollect_info",
                           username=username, password=password)

    def start_collection(self, file_name, options="", compress_output=False):
        log_redirection = ""
        if compress_output:
            log_redirection = "> /dev/null 2>&1"

        command = "%s %s %s %s" % (self.cbstatCmd, file_name, options,
                                   log_redirection)
        output, error = self._execute_cmd(command)
        return output, error
