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
          cbstats localhost:port 'stat_name'

        Note: Function calling this API should take care of validating
        the outputs and handling the errors/warnings from execution.

        Arguments:
        :bucket_name   - Name of the bucket to get the stats
        :stat_name     - Any valid stat_command accepted by cbstats
        :key           - Target stat name(string) value to return.
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

    def magma_stats(self, bucket_name, field_to_grep=None,
                    stat_name="kvstore"):
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
            try:
                stats[vb_num][stat_name] = int(stat_value)
            except ValueError:
                stats[vb_num][stat_name] = stat_value

        return stats

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


    def dcp_stats(self, bucket_name):
        return self.get_stats_memc(bucket_name, "dcp")
