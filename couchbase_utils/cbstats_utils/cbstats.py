import logging
import re

from testconstants import \
    LINUX_COUCHBASE_BIN_PATH, \
    LINUX_NONROOT_CB_BIN_PATH, WIN_COUCHBASE_BIN_PATH

log = logging.getLogger(__name__)


class Cbstats():
    def __init__(self, shell_conn, port=11210, username="Administrator",
                 password="password"):

        self.shellConn = shell_conn
        self.port = port
        self.username = username
        self.password = password

        self.binaryName = "cbstats"
        self.cbstatCmd = "%s%s" % (LINUX_COUCHBASE_BIN_PATH, self.binaryName)

        if self.shellConn.username != "root":
            self.cbstatCmd = "%s%s" % (LINUX_NONROOT_CB_BIN_PATH,
                                       self.binaryName)

        if self.shellConn.extract_remote_info().type.lower() == 'windows':
            self.cbstatCmd = "%s%s" % (WIN_COUCHBASE_BIN_PATH,
                                       self.binaryName)

    def get_stat(self, bucket_name, stat_name, field_to_grep):
        result = None
        cmd = "%s localhost:%s -u %s -p %s -b %s %s | grep %s" \
              % (self.cbstatCmd, self.port, self.username, self.password,
                 bucket_name, stat_name, field_to_grep)
        log.info("Executing: '%s'" % (cmd))
        output, err = self.shellConn.execute_command(cmd)
        if len(err) != 0:
            raise(err[0])

        for line in output:
            pattern = "[ \t]*{0}[ \t]*:[ \t]+([0-9]+)".format(field_to_grep)
            regexp = re.compile(pattern)
            match_result = regexp.match(line)
            if match_result:
                result = match_result.group(1)
                break

        return result
