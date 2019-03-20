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

    def get_stats(self, bucket_name, stat_name, field_to_grep=None):
        """
        Fetches stats using cbstat and greps for specific line.
        Uses command:
          cbstats localhost:port 'stat_name' | grep 'field_to_grep'

        Note: Function calling this API should take care of validating
        the outputs and handling the errors/warnings from execution.

        Arguments:
        :bucket_name   - Name of the bucket to get the stats
        :stat_name     - Any valid stat_command accepted by cbstats
        :field_to_grep - Target stat name string to grep.
                         Default=None, means fetch all data

        Returns:
        :output - Output for the cbstats command
        :err    - Exception/Warnings got from the command execution
        """

        cmd = "%s localhost:%s -u %s -p %s -b %s %s" \
              % (self.cbstatCmd, self.port, self.username, self.password,
                 bucket_name, stat_name)

        if field_to_grep:
            cmd = "%s | grep %s" % (cmd, field_to_grep)

        log.info("Executing: '%s'" % (cmd))
        output, err = self.shellConn.execute_command(cmd)

        return output, err

    def get_vbucket_stats(self, bucket_name, stat_name, vbucket_num,
                          field_to_grep=None):
        """
        Fetches failovers stats for specified vbucket
        and greps for specific stat.
        Uses command:
          cbstats localhost:port failovers '[vbucket_num]' | \
            grep '[field_to_grep]'

        Note: Function calling this API should take care of validating
        the outputs and handling the errors/warnings from execution.

        Arguments:
        :bucket_name   - Name of the bucket to get the stats
        :stat_name     - Any valid stat_command accepted by cbstats
        :vbucket_num   - Target vbucket number to fetch the stats
        :field_to_grep - Target stat name string to grep.
                         Default=None, means to fetch all stats related to
                         the selected vbucket stat

        Returns:
        :output - Output for the cbstats command
        :err    - Exception/Warnings got from the command execution
        """

        cmd = "%s localhost:%s -u %s -p %s -b %s %s %s" \
              % (self.cbstatCmd, self.port, self.username, self.password,
                 bucket_name, stat_name, vbucket_num)

        if field_to_grep:
            cmd = "%s | grep %s" % (cmd, field_to_grep)

        log.info("Executing: '%s'" % (cmd))
        output, err = self.shellConn.execute_command(cmd)

        return output, err

    # Below are wrapper functions for above command executor APIs
    def all_stats(self, bucket_name, field_to_grep):
        """
        Get a particular value of stat from the command,
          cbstats localhost:port all

        Arguments:
        :bucket_name   - Name of the bucket to get the stats
        :stat_name     - Any valid stat_command accepted by cbstats
        :field_to_grep - Target stat name string to grep.

        Returns:
        :result - Value of the 'field_to_grep' using regexp.
                  If not matched, 'None'

        Raise:
        :Exception returned from command line execution (if any)
        """

        result = None
        output, error = self.get_stats(bucket_name, "all",
                                       field_to_grep=field_to_grep)
        if len(error) != 0:
            raise("\n".join(error))

        pattern = "[ \t]*{0}[ \t]*:[ \t]+([0-9]+)".format(field_to_grep)
        regexp = re.compile(pattern)
        for line in output:
            match_result = regexp.match(line)
            if match_result:
                result = match_result.group(1)
                break

        return result

    def vbucket_list(self, bucket_name, vbucket_type="active"):
        """
        Get list of vbucket numbers as list.
        Uses command:
          cbstats localhost:port vbuckets

        Arguments:
        :bucket_name  - Name of the bucket to get the stats
        :vbucket_type - Type of vbucket (active/replica)
                        Default="active"

        Returns:
        :vb_list - List containing list of vbucket numbers matching
                   the :vbucket_type:

        Raise:
        :Exception returned from command line execution (if any)
        """

        vb_list = list()
        cmd = "%s localhost:%s -u %s -p %s -b %s vbucket" \
              % (self.cbstatCmd, self.port, self.username, self.password,
                 bucket_name)
        log.info("Executing: '%s'" % (cmd))
        output, err = self.shellConn.execute_command(cmd)
        if len(err) != 0:
            raise("\n".join(err))

        pattern = "[ \t]*vb_([0-9]+)[ \t]*:[ \t]+([a-zA-Z]+)"
        regexp = re.compile(pattern)
        for line in output:
            match_result = regexp.match(line)
            if match_result:
                curr_vb_type = match_result.group(2)
                if curr_vb_type == vbucket_type:
                    vb_num = match_result.group(1)
                    vb_list.append(vb_num)
                break

        return vb_list

    def vbucket_details(self, bucket_name, vbucket_num, field_to_grep):
        """
        Get a particular value of stat from the command,
          cbstats localhost:port vbucket-details

        Arguments:
        :bucket_name   - Name of the bucket to get the stats
        :vbucket_num   - Target vbucket_number to fetch the stats
        :field_to_grep - Target stat name string to grep

        Returns:
        :result - Value of the 'field_to_grep' using regexp.
                  If not matched, 'None'

        Raise:
        :Exception returned from command line execution (if any)
        """

        result = None
        output, error = self.get_vbucket_stats(bucket_name, "vbucket-details",
                                               vbucket_num,
                                               field_to_grep=field_to_grep)
        if len(error) != 0:
            raise("\n".join(error))

        pattern = "[ \t]*vb_{0}:{1}:[ \t]*:[ \t]+([_0-9a-zA-Z:\-\,\[\]\. ]+)" \
                  .format(vbucket_num, field_to_grep)
        regexp = re.compile(pattern)
        for line in output:
            match_result = regexp.match(line)
            if match_result:
                result = match_result.group(1)
                break

        return result

    def vbucket_seqno(self, bucket_name, vbucket_num, field_to_grep):
        """
        Get a particular value of stat from the command,
          cbstats localhost:port vbucket-seqno

        Arguments:
        :bucket_name   - Name of the bucket to get the stats
        :vbucket_num   - Target vbucket_number to fetch the stats
        :field_to_grep - Target stat name string to grep

        Returns:
        :result - Value of the 'field_to_grep' using regexp.
                  If not matched, 'None'

        Raise:
        :Exception returned from command line execution (if any)
        """

        result = None
        output, error = self.get_vbucket_stats(bucket_name, "vbucket-seqno",
                                               vbucket_num,
                                               field_to_grep=field_to_grep)
        if len(error) != 0:
            raise("\n".join(error))

        pattern = "[ \t]*vb_{0}:{1}:[ \t]*:[ \t]+([0-9]+)" \
                  .format(vbucket_num, field_to_grep)
        regexp = re.compile(pattern)
        for line in output:
            match_result = regexp.match(line)
            if match_result:
                result = match_result.group(1)
                break

        return result

    def verify_failovers_field_stat(self, bucket_name, field_to_grep,
                                    expected_value, vbuckets_list=None):
        """
        Verifies the given value against the failovers stats

        Arguments:
        :bucket_name    - Name of the bucket to get the stats
        :field_to_grep  - Target stat name string to grep
        :expected_value - Expected value against which the verification
                          needs to be done
        :vbuckets_list  - List of vbuckets to verify the values

        Returns:
        :is_stat_ok  - Boolean value saying whether it is okay or not

        Raise:
        :Exception returned from command line execution (if any)
        """
        # Local function to parse and verify the output lines
        def parse_failover_logs(output, error):
            is_ok = True
            if len(error) != 0:
                raise("\n".join(error))

            pattern = "[ \t]vb_[0-9]+:{0}:[ \t]+([0-9]+)".format(field_to_grep)
            regexp = re.compile(pattern)
            for line in output:
                match_result = regexp.match(line)
                if match_result is None:
                    is_ok = False
                    break
                else:
                    if match_result.group(1) != expected_value:
                        is_ok = False
                        break
            return is_ok

        is_stat_ok = True
        if vbuckets_list is None:
            output, error = self.get_stats(
                bucket_name, "failovers", field_to_grep=field_to_grep)
            try:
                is_stat_ok = parse_failover_logs(output, error)
            except Exception as err:
                raise(err)
        else:
            for tem_vb in vbuckets_list:
                output, error = self.get_vbucket_stats(
                    bucket_name, "failovers", vbucket_num=tem_vb,
                    field_to_grep=field_to_grep)
                try:
                    is_stat_ok = parse_failover_logs(output, error)
                    if not is_stat_ok:
                        break
                except Exception as err:
                    raise(err)
        return is_stat_ok
