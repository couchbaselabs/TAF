import re
import zlib
import json

from memcached.helper.data_helper import MemcachedClientHelper
from BucketLib.bucket import Bucket


class Cbstats:

    def __init__(self, server, username=None, password=None):
        self.server = server
        self.port = server.port
        self.mc_port = server.memcached_port
        self.username = username or server.rest_username
        self.password = password or server.rest_password

    def __calculate_vbucket_num(self, doc_key, total_vbuckets):
        """
        Calculates vbucket number based on the document's key

        Argument:
        :doc_key        - Document's key
        :total_vbuckets - Total vbuckets present in the bucket

        Returns:
        :vbucket_number calculated based on the 'doc_key'
        """
        return (((zlib.crc32(doc_key)) >> 16) & 0x7fff) & (total_vbuckets-1)

    def get_scopes(self, bucket):
        """
        Fetches list of scopes for the particular bucket
        Uses command:
          cbstats localhost:port scopes

        Arguments:
        :bucket - Bucket object to fetch the name

        Returns:
        :scope_data - Dict containing the scopes stat values
        """
        scope_data = dict()

        client = MemcachedClientHelper.direct_client(
            self.server, Bucket({"name": bucket.name}), 30,
            self.username, self.password)
        client.collections_supported = True
        collection_details = json.loads(client.get_collections()[2])
        collection_stats = client.stats("collections")
        client.close()
        scope_data["manifest_uid"] = int(collection_stats["manifest_uid"])
        scope_data["count"] = 0
        for s_details in collection_details["scopes"]:
            s_name = s_details["name"]
            s_id = s_details["uid"]
            scope_data["count"] += 1
            scope_data[s_name] = dict()
            scope_data[s_name]["collections"] = len(s_details["collections"])
            scope_data[s_name]["num_items"] = 0
            for col_details in s_details["collections"]:
                c_id = col_details["uid"]
                i_key = "0x%s:0x%s:items" % (s_id, c_id)
                scope_data[s_name]["num_items"] += int(collection_stats[i_key])

        return scope_data

    def get_collections(self, bucket):
        """
        Fetches list of collections from the server
        Uses command:
          cbstats localhost:port collections

        Arguments:
        :bucket - Bucket object to fetch the name

        Returns:
        :collection_data - Dict containing the collections stat values
        """
        collection_data = dict()

        client = MemcachedClientHelper.direct_client(
            self.server, Bucket({"name": bucket.name}), 30,
            self.username, self.password)
        client.collections_supported = True
        collection_details = json.loads(client.get_collections()[2])
        collection_stats = client.stats("collections")
        client.close()

        collection_data["count"] = 0
        collection_data["manifest_uid"] = collection_stats["manifest_uid"]

        for scope_details in collection_details["scopes"]:
            s_name = scope_details["name"]
            s_id = scope_details["uid"]
            collection_data[s_name] = dict()
            for col_details in scope_details["collections"]:
                c_name = col_details["name"]
                c_id = col_details["uid"]

                collection_data[s_name][c_name] = dict()
                scope_col_id = "0x%s:0x%s:" % (s_id, c_id)

                for stat, value in collection_stats.items():
                    if stat.startswith(scope_col_id):
                        stat = stat.split(':')[2]
                        # Convert to number if possible
                        try:
                            value = int(value)
                        except ValueError:
                            pass
                        collection_data[s_name][c_name][stat] = value
                collection_data["count"] += 1
        return collection_data

    def get_stats_memc(self, bucket_name, stat_name="", key=None):
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
        :error  - Buffer containing warnings/errors from the execution
        """
        # result = dict()
        if stat_name == "all":
            stat_name = ""
        client = MemcachedClientHelper.direct_client(
            self.server, Bucket({"name": bucket_name}), 30,
            self.username, self.password)
        output = client.stats(stat_name)
        client.close()
        return output if key is None else output[key]

    def get_timings(self, bucket_name, command="raw"):
        """
        Fetches timings stat
        :param bucket_name: Name of the bucket to get timings
        :param command: default="raw", to print in readable format
        :return output: Output for the cbstats command
        :return error:  Buffer containing warnings/errors from the execution
        """

        cmd = "%s localhost:%s %s -u %s -p %s -b %s timings" % (self.cbstatCmd,
                                                                self.mc_port,
                                                                command,
                                                                self.username,
                                                                self.password,
                                                                bucket_name)
        return self._execute_cmd(cmd)

    def get_kvtimings(self, command="raw"):
        """
        Fetches kvtiming using cbstats

        :param command: default="raw", to print in readable format
        :return output: Output for the cbstats command
        :return error:  Buffer containing warnings/errors from the execution
        """
        cmd = "%s localhost:%s %s -u %s -p %s kvtimings" % (self.cbstatCmd,
                                                            self.mc_port,
                                                            command,
                                                            self.username,
                                                            self.password)

        return self._execute_cmd(cmd)

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
        :error  - Buffer containing warnings/errors from the execution
        """
        client = MemcachedClientHelper.direct_client(
            self.server, Bucket(bucket_name), 30,
            self.username, self.password)
        output = client.stats("{} {}".format(stat_name, vbucket_num))
        client.close()
        return output

    # Below are wrapper functions for above command executor APIs
    def all_stats(self, bucket_name, stat_name=""):
        """
        Get a particular value of stat from the command,
          cbstats localhost:port all
        Arguments:
        :bucket_name   - Name of the bucket to get the stats
        :stat_name     - Any valid stat_command accepted by cbstats
        Returns:
        :result - Dict of stat_key:value
        Raise:
        :Exception returned from command line execution (if any)
        """
        if stat_name == "all":
            stat_name = ""
        result = self.get_stats_memc(bucket_name, stat_name)
        return result

    def checkpoint_stats(self, bucket_name, vb_num=None):
        """
        Fetches checkpoint stats for the given bucket_name

        :param bucket_name: Bucket name to fetch the stats for
        :param vb_num:      Target vbucket number to fetch the stats for
        :return result: Dictionary map of vb_num as inner dict

        Raise:
        :Exception returned from the command line execution (if any)
        """
        result = dict()
        stat = "checkpoint %s" % vb_num if vb_num is not None else "checkpoint"
        output = self.get_stats_memc(bucket_name, stat)
        pattern = \
            "[\t ]*vb_([0-9]+):([a-zA-Z0-9@.->:_]+)"
        pattern = re.compile(pattern)
        for key in output.keys():
            match_result = pattern.match(key)
            if match_result:
                vb_num = int(match_result.group(1))
                stat_name = match_result.group(2)
                stat_value = output.get(key)
                try:
                    stat_value = int(stat_value)
                except ValueError:
                    pass
                if vb_num not in result:
                    result[vb_num] = dict()
                result[vb_num][stat_name] = stat_value
        return result

    def magma_stats(self, bucket_name, stat_name="kvstore"):
        """
        Get a particular value of "kvstore" stat from the command,
        cbstats localhost:port kvstore
        kvstore stats are the stats for magma

        Arguments:
        :bucket_name   - Name of the bucket to get the stats
        :stat_name     - Any valid stat_command accepted by cbstats

        Returns:
        :result - Return a dict
        """
        output = self.get_stats_memc(bucket_name, stat_name)
        return output

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
        output = self.get_stats_memc(bucket_name, "vbucket")
        for key in output.keys():
            curr_vb_type = output[key]
            if curr_vb_type == vbucket_type:
                vb_num = key
                vb_list.append(int(vb_num.split("_")[1]))
        return vb_list

    def vbucket_details(self, bucket_name):
        """
        Get a particular value of stat from the command,
          cbstats localhost:port vbucket-details

        :param bucket_name:   - Name of the bucket to get the stats
        :returns vb_details:  - Dictionary of format dict[vb][stat_name]=value

        Raise:
        :Exception returned from command line execution (if any)
        """

        stats = dict()
        output = self.get_stats_memc(bucket_name, "vbucket-details")
        # In case of cluster_run, output is plain string due to direct exec
        if type(output) is str:
            output = output.split("\n")

        pattern = "[ \t]*vb_([0-9]+):([0-9a-zA-Z_]*)"
        regexp = re.compile(pattern)

        for key in output.keys():
            match_result = regexp.match(key)
            if not match_result:
                vb_num = key.split("_")[1]
                stat_name = "type"
                stat_value = output[key]
            else:
                vb_num = match_result.group(1)
                stat_name = match_result.group(2)
                stat_value = output[key]

            # Create a sub_dict to state vbucket level stats
            if vb_num not in stats:
                stats[vb_num] = dict()
            # Populate the values to the stats dictionary
            stats[vb_num][stat_name] = stat_value

        return stats

    def vkey_stat(self, bucket_name, doc_key,
                  vbucket_num=None, total_vbuckets=1024):
        """
        Get vkey stats from the command,
          cbstats localhost:port -b 'bucket_name' vkey 'doc_id' 'vbucket_num'

        Arguments:
        :bucket_name    - Name of the bucket to get the stats
        :doc_key        - Document key to validate for
        :vbucket_num    - Target vbucket_number to fetch the stats.
                          If 'None', calculate the vbucket_num locally
        :total_vbuckets - Total vbuckets configured for the bucket.
                          Default=1024

        Returns:
        :result - Dictionary of stat:values

        Raise:
        :Exception returned from command line execution (if any)
        """

        result = dict()
        if vbucket_num is None:
            vbucket_num = self.__calculate_vbucket_num(doc_key, total_vbuckets)

        cmd = "%s localhost:%s -u %s -p %s -b %s vkey %s %s" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket_name, doc_key, vbucket_num)

        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))

        # In case of cluster_run, output is plain string due to direct exec
        if type(output) is str:
            output = output.split("\n")

        pattern = "[ \t]*key_([a-zA-Z_]+)[: \t]+([0-9A-Za-z]+)"
        pattern = re.compile(pattern)
        for line in output:
            match_result = pattern.match(line)
            if match_result:
                result[match_result.group(1)] = match_result.group(2)

        return result

    def vbucket_seqno(self, bucket_name):
        """
        Get a particular value of stat from the command,
          cbstats localhost:port vbucket-seqno

        Arguments:
        :bucket_name   - Name of the bucket to get the stats

        Returns:
        :result - Dictionary of format stats["vb_num"]["stat_name"] = value

        Raise:
        :Exception returned from command line execution (if any)
        """

        stats = dict()
        output = self.get_stats_memc(bucket_name, "vbucket-seqno")

        pattern = "[ \t]*vb_([0-9]+):([0-9a-zA-Z_]+)"
        regexp = re.compile(pattern)
        for key in output.keys():
            match_result = regexp.match(key)
            if match_result:
                vb_num = match_result.group(1)
                stat_name = match_result.group(2)
                stat_value = output[key]

                # Create a sub_dict to state vbucket level stats
                if vb_num not in stats:
                    stats[vb_num] = dict()
                # Populate the values to the stats dictionary
                stats[vb_num][stat_name] = stat_value

        return stats

    def failover_stats(self, bucket_name):
        """
        Gets vbucket's failover stats. Uses command,
          cbstats localhost:port -b 'bucket_name' failovers

        Arguments:
        :bucket_name    - Name of the bucket to get the stats
        :field_to_grep  - Target stat name string to grep.
                          Default=None means fetch all stats for the vbucket

        Returns:
        :stats - Dictionary of format stats["vb_num"]["stat_name"] = value

        Raise:
        :Exception returned from command line execution (if any)
        """

        stats = dict()
        output = self.get_stats_memc(bucket_name, "failovers")

        pattern = "vb_([0-9]+):([0-9A-Za-z:_]+)"
        regexp = re.compile(pattern)

        for key, value in output.items():
            # Match the regexp to the line and populate the values
            match_result = regexp.match(key)
            vb_num = match_result.group(1)
            stat_name = match_result.group(2)

            # Create a sub_dict to state vbucket level stats
            if vb_num not in stats:
                stats[vb_num] = dict()
            # Populate the values to the stats dictionary
            stats[vb_num][stat_name] = value

        return stats

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
                raise Exception("\n".join(error))

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
                raise Exception(err)
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
                    raise Exception(err)
        return is_stat_ok

    def dcp_stats(self, bucket_name):
        return self.get_stats_memc(bucket_name, "dcp")

    def dcp_vbtakeover(self, bucket_name, vb_num, key):
        """
        Fetches dcp-vbtakeover stats for the target vb,key
          cbstats localhost:port -b bucket_name dcp-vbtakeover vb_num key

        Arguments:
        :bucket_name - Name of the bucket
        :vb_num - Target vBucket
        :key - Document key

        Returns:
        :stats - Dictionary of stats field::value

        Raise:
        :Exception returned from command line execution (if any)
        """

        stats = dict()
        cmd = "%s localhost:%s -u %s -p %s -b %s dcp-vbtakeover %s %s" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket_name, vb_num, key)

        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))

        for line in output:
            line = line.strip()
            list_all = line.rsplit(":", 1)
            stat = list_all[0]
            val = list_all[1].strip()
            try:
                val = int(val)
            except ValueError:
                pass
            stats[stat] = val

        return stats

    def warmup_stats(self, bucket_name):
        """
        Fetches warmup stats for the requested bucket.
        :param bucket_name: Name of the bucket to fetch the stats
        :return stats: Dict values of warmup stats
        """
        stats = dict()
        output, error = self.get_stats(bucket_name, "warmup")
        if len(error) != 0:
            raise Exception("\n".join(error))

        for line in output:
            line = line.strip()
            list_all = line.rsplit(":", 1)
            stat = list_all[0]
            val = list_all[1].strip()
            try:
                val = int(val)
            except ValueError:
                pass
            stats[stat] = val
        return stats
