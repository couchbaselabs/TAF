import re
from random import choice
from time import time

from Cb_constants import CbServer
from FtsLib.FtsOperations import FtsHelper
from bucket_collections.collections_base import CollectionBase
from couchbase_helper.tuq_helper import N1QLHelper
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from platform_constants.os_constants import Windows
from remote.remote_util import RemoteMachineShellConnection
from table_view import TableView

from java.lang import Exception as Java_base_exception


class ConfigPurging(CollectionBase):
    def setUp(self):
        super(ConfigPurging, self).setUp()
        is_windows = False

        for node in self.cluster.servers:
            shell = RemoteMachineShellConnection(node)
            if shell.info.type.lower() == Windows.NAME:
                is_windows = True
            shell.enable_diag_eval_on_non_local_hosts()
            shell.disconnect()

        self.cluster_util.update_cluster_nodes_service_list(self.cluster)

        # Default purger values
        self.default_run_interval = 60
        self.default_purge_age = 300

        self.time_stamp = time()
        self.num_index = self.input.param("num_index", 0)
        self.index_type = self.input.param("index_type", CbServer.Services.FTS)
        self.index_name_len = self.input.param("index_name_len", 10)
        self.fts_index_partition = self.input.param("fts_index_partition", 1)
        self.index_replicas = self.input.param("gsi_index_replicas", 1)
        self.fts_helper = FtsHelper(self.cluster.fts_nodes[0]) \
            if self.cluster.fts_nodes else None
        self.n1ql_helper = N1QLHelper(server=self.cluster.query_nodes[0],
                                      use_rest=True, log=self.log) \
            if self.cluster.query_nodes else None
        self.spare_node = self.servers[-1]
        self.couchbase_base_dir = "/opt/couchbase"
        if is_windows:
            self.couchbase_base_dir = \
                "/cygdrive/c/Program\\ Files/Couchbase/Server"

        # Param order:
        # fts_name, bucket_name, index_partitions, scope_name, collection_name
        self.fts_param_template = '{ \
          "type": "fulltext-index", \
          "name": "%s", \
          "sourceType": "gocbcore", \
          "sourceName": "%s", \
          "sourceUUID": "%s", \
          "planParams": { \
            "maxPartitionsPerPIndex": 1024, \
            "indexPartitions": %d \
          }, \
          "params": { \
            "doc_config": { \
              "docid_prefix_delim": "", \
              "docid_regexp": "", \
              "mode": "scope.collection.type_field", \
              "type_field": "type" \
            }, \
            "mapping": { \
              "analysis": {}, \
              "default_analyzer": "standard", \
              "default_datetime_parser": "dateTimeOptional", \
              "default_field": "_all", \
              "default_mapping": { \
                "dynamic": true, \
                "enabled": false \
              }, \
              "default_type": "_default", \
              "docvalues_dynamic": false, \
              "index_dynamic": true, \
              "store_dynamic": false, \
              "type_field": "_type", \
              "types": { \
                "%s.%s": { \
                  "dynamic": true, \
                  "enabled": true \
                } \
              } \
            }, \
            "store": { \
              "indexType": "scorch", \
              "segmentVersion": 15 \
            } \
          }, \
          "sourceParams": {} \
        }'

        self.gsi_index_name_template = "%s_%s_%s_%d"
        self.gsi_create_template = "CREATE PRIMARY INDEX `%s` " \
                                   "ON `%s`.`%s`.`%s` USING GSI " \
                                   "WITH {\"num_replica\": %d}"
        self.gsi_drop_template = "DROP INDEX `%s`.`%s` USING GSI"

        self.op_create = "key_create"
        self.op_remove = "key_delete"

        self.ts_during_start = self.__get_current_timestamps_from_debug_log()
        self.initial_tombstones = \
            self.cluster_util.get_metakv_dicts(self.cluster.master)
        self.log.info(self.ts_during_start)

    def tearDown(self):
        self.log_setup_status("ConfigPurging", "start", "tearDown")
        self.log.info("Resetting purger age value to default")
        rest = RestConnection(self.cluster.master)
        rest.run_tombstone_purger(self.default_purge_age)
        rest.enable_tombstone_purger()
        self.log_setup_status("ConfigPurging", "complete", "tearDown")
        super(ConfigPurging, self).tearDown()

    def __perform_meta_kv_op_on_rand_key(self, op_type, key_name, value=None):
        self.log.info("%s meta_kv key '%s'" % (op_type, key_name))
        if op_type == self.op_create:
            self.cluster_util.create_metakv_key(self.cluster.master,
                                                key_name, value)
        elif op_type == self.op_remove:
            self.cluster_util.delete_metakv_key(self.cluster.master, key_name)
        else:
            self.fail("Invalid operation: %s" % op_type)

    def __get_deleted_key_count(self, check_if_zero=False):
        deleted_keys = self.cluster_util.get_ns_config_deleted_keys_count(self.cluster)
        tbl = TableView(self.log.info)
        tbl.set_headers(["Node", "Deleted_key_count"])
        for t_ip, k_count in deleted_keys.items():
            tbl.add_row(["%s" % t_ip, "%s" % k_count])
        tbl.display("Tombstone count on cluster nodes:")

        if not check_if_zero:
            return
        for t_ip, k_count in deleted_keys.items():
            if k_count != 0:
                self.fail("%s Deleted key count %s != 0" % (t_ip, k_count))

    def __fts_index(self, op_type, index_name, bucket, scope, collection):
        self.log.info("%s fts index %s" % (op_type, index_name))
        if op_type == self.op_create:
            template = self.fts_param_template % (
                index_name, bucket.name, bucket.uuid,
                self.fts_index_partition, scope, collection)
            self.fts_helper.create_fts_index_from_json(index_name, template)
        elif op_type == self.op_remove:
            self.fts_helper.delete_fts_index(index_name)

    def __gsi_index(self, op_type, b_name, s_name, c_name, index_counter):
        query = None
        index_name = "%s_%s_%s_%d" % (b_name, s_name, c_name, index_counter)
        self.log.info("GSI %s: %s" % (op_type, index_name))
        if op_type == self.op_create:
            query = self.gsi_create_template % (index_name,
                                                b_name, s_name, c_name,
                                                self.index_replicas)
        elif op_type == self.op_remove:
            query = self.gsi_drop_template % (b_name, index_name)

        try:
            self.n1ql_helper.run_cbq_query(query)
        except Exception as e:
            self.log.critical(e)
        except Java_base_exception as e:
            self.log.critical(e)

    def __get_purged_tombstone_from_last_run(self, nodes=None):
        """
        :return last_purged_tombstones: Dict of format,
            { node_ip: {'count': N, 'keys': [k1, k2, ..] }, ...}
        """
        tail_cmd = "cat %s/var/lib/couchbase/logs/debug.log " \
                   % self.couchbase_base_dir \
                   + "| sed -n '/%s/,$p'"
        purged_ts_count_pattern = ".*Purged ([0-9]+) ns_config tombstone"
        meta_kv_keys_pattern = ".*{metakv,[ ]*<<\"/([0-9a-zA-Z_\-\.]+)\">>"
        start_of_line = "^\[ns_server:"

        start_of_line = re.compile(start_of_line)
        meta_kv_keys_pattern = re.compile(meta_kv_keys_pattern)
        purged_ts_count_pattern = re.compile(purged_ts_count_pattern)
        tbl_view = TableView(self.log.info)
        tbl_view.set_headers(["Node", "Purged Keys"])

        last_purged_tombstones = dict()
        if nodes is None:
            nodes = self.cluster_util.get_nodes_in_cluster(self.cluster)
        for node in nodes:
            self.log.info("Processing debug logs from %s" % node.ip)
            shell = RemoteMachineShellConnection(node)
            output, _ = shell.execute_command(
                tail_cmd % self.ts_during_start[node.ip])
            if not output:
                output, _ = shell.execute_command(
                    " ".join(tail_cmd.split(' ')[:2]))
            self.log.debug("Tail stdout:\n%s" % output)
            o_len = len(output)
            target_buffer = ""
            total_ts_purged = 0
            for index in range(o_len-1, -1, -1):
                line = output[index]
                if not start_of_line.match(line):
                    target_buffer = line + target_buffer
                elif "tombstone_agent:purge:" in line:
                    total_ts_purged = \
                        purged_ts_count_pattern.match(line).group(1)
                    break
                else:
                    target_buffer = ""

            last_purged_tombstones[node.ip] = dict()
            last_purged_tombstones[node.ip]["count"] = int(total_ts_purged)
            last_purged_tombstones[node.ip]["keys"] = \
                meta_kv_keys_pattern.findall(target_buffer)
            tbl_view.add_row([node.ip, total_ts_purged])
            shell.disconnect()

        tbl_view.display("Purged_keys:")
        self.log.debug("Purged keys :: %s" % last_purged_tombstones)
        return last_purged_tombstones

    def __get_current_timestamps_from_debug_log(self):
        cmd = "cat %s/var/lib/couchbase/logs/debug.log " \
              "| grep '\[ns_server:' | tail -1" % self.couchbase_base_dir
        timestamp_pattern = "\[ns_server:[a-zA-Z]+,([T0-9-:.]+),"
        timestamp_pattern = re.compile(timestamp_pattern)
        ts_result = dict()
        for server in self.cluster.servers:
            shell = RemoteMachineShellConnection(server)
            output, _ = shell.execute_command(cmd)
            if type(output) is list:
                output = output[0]
            ts_result[server.ip] = timestamp_pattern.match(output).group(1)
            shell.disconnect()
        return ts_result

    def test_meta_kv_resource_usage(self):
        """
        1. Records initial memory & disk usage stats
        2. Create huge number of meta_kv keys
        3. Records memory disk usage post creation
        4. Remove created keys to create tombstones
        5. Record memory & disk stats (with tombstones present)
        6. Wait for purger to cleanup the tombstones
        7. Validate the memory and disk space to reclaimed properly
        """
        def get_node_stats():
            for t_ip, t_conn in shell_conn.items():
                self.log.info("Node: %s" % t_ip)
                for t_cmd in commands_to_run:
                    self.log.info(
                        "".join(t_conn.execute_command(t_cmd)[0]))

        fts_generic_name = "fts_%s" % int(self.time_stamp) + "-%d"
        commands_to_run = [
            "free -h",
            "ls -lh %s/var/lib/couchbase/config/config.dat "
            "| awk '{print $5,$9}'" % self.couchbase_base_dir]

        rest = RestConnection(self.cluster.master)

        self.log.info("Stopping meta_kv purger autorun")
        rest.disable_tombstone_purger()
        rest.run_tombstone_purger(1)

        shell_conn = dict()
        # Open required ssh connections
        for node in self.cluster.nodes_in_cluster:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)

        self.log.info("Collecting initial cluster stats")
        self.__get_deleted_key_count(check_if_zero=True)
        get_node_stats()

        # Create required indexes
        self.log.info("Creating %s indexes" % self.num_index)
        for index in range(self.num_index):
            key = fts_generic_name % index
            if self.fts_helper is None:
                self.__perform_meta_kv_op_on_rand_key(self.op_create, key,
                                                      "dummy_val")
            else:
                self.__fts_index(self.op_create, key,
                                 self.cluster.buckets[0],
                                 CbServer.default_scope,
                                 CbServer.default_collection)
        self.log.info("Done creating indexes")

        self.log.info("Stats after index creation")
        get_node_stats()

        # Remove indexes to create tombstones
        self.log.info("Removing keys to create tombstones")
        for index in range(self.num_index):
            key = fts_generic_name % index
            if self.fts_helper is None:
                self.__perform_meta_kv_op_on_rand_key(self.op_create, key)
            else:
                self.__fts_index(self.op_remove, key,
                                 self.cluster.buckets[0],
                                 CbServer.default_scope,
                                 CbServer.default_collection)
        self.log.info("Done creating tombstones")

        if self.fts_helper is not None:
            self.sleep(30, "Wait for FTS delete to complete")

        self.log.info("Stats with active tombstones")
        self.__get_deleted_key_count()
        get_node_stats()

        self.log.info("Triggering tombstone purger")
        rest.run_tombstone_purger(1)
        self.sleep(10, "Wait for purger run to complete")

        self.log.info("Purged tombstone info:")
        self.log.info(self.__get_purged_tombstone_from_last_run())

        self.log.info("Post purge cluster stats")
        self.__get_deleted_key_count(check_if_zero=True)
        get_node_stats()

        # Close all ssh connections
        for _, conn in shell_conn.items():
            conn.disconnect()

    def test_meta_kv_key_purging(self):
        """
        1. Create meta_kv key entries for 'self.default_purge_age' seconds
        2. Wait for 'purge_time_interval' time and check the keys have been
           purged from the meta_kv or not
        """
        # Can take values custom / fts/ 2i
        i_count = 0
        created_keys = list()
        generic_name = "meta_kv_key-%s-%s"
        start_time = int(time())
        end_time = start_time + self.default_run_interval
        key_type = self.input.param("index_type", "custom")

        self.log.info("Creating meta_kv tombstones for %d seconds"
                      % self.default_run_interval)
        while int(time()) < end_time:
            index_name = generic_name \
                % (time(), str(i_count).zfill(self.index_name_len))
            if key_type == "custom":
                self.__perform_meta_kv_op_on_rand_key(
                    self.op_create, index_name, "value_dummy")
                self.__perform_meta_kv_op_on_rand_key(
                    self.op_remove, index_name)
            elif key_type == CbServer.Services.FTS:
                self.__fts_index(self.op_create, index_name,
                                 self.cluster.buckets[0],
                                 CbServer.default_scope,
                                 CbServer.default_collection)
                self.__fts_index(self.op_remove, index_name,
                                 self.cluster.buckets[0],
                                 CbServer.default_scope,
                                 CbServer.default_collection)
            created_keys.append(index_name)
            i_count += 1
        self.log.info("Created %d meta_kv_tombstones" % i_count)
        self.sleep(self.default_purge_age - self.default_run_interval,
                   "Wait for purging to run")

        # Validating purged keys with default settings
        total_keys_purged = dict()
        for iter in range(3):
            if iter == 0:
                #ignoring the first internal purge
                self.sleep(self.default_run_interval + 15,
                           "Wait for next purger run")
                continue
            ts_data = self.__get_purged_tombstone_from_last_run()
            for node_ip, purge_data in ts_data.items():
                if node_ip not in total_keys_purged:
                    total_keys_purged[node_ip] = 0
                total_keys_purged[node_ip] += purge_data["count"]
            self.sleep(self.default_run_interval + 15,
                       "Wait for next purger run")

        for node_ip, purged_count in total_keys_purged.items():
            if purged_count != i_count:
                self.fail("%s - Expected %s to be purged. Actual %s purged"
                          % (node_ip, i_count, purged_count))

    def test_create_meta_kv_keys_max_len(self):
        """
        Create meta_kv tombstones with huge key_size and validate purging
        """
        rest = RestConnection(self.cluster.master)
        self.log.info("Stopping meta_kv purger autorun")
        rest.disable_tombstone_purger()

        self.log.info("Creating %s indexes with length %s"
                      % (self.num_index, self.index_name_len))
        t_fts_name = "fts_%s" % int(self.time_stamp) \
                     + "-%0" + str(self.index_name_len) + "d"
        # Create req. number of index
        for index in range(self.num_index):
            fts_name = t_fts_name % index
            self.__fts_index(self.op_create, fts_name,
                             self.cluster.buckets[0],
                             CbServer.default_scope,
                             CbServer.default_collection)

        self.log.info("Deleting indexes to create tombstones")
        # Delete created indexes
        for index in range(self.num_index):
            fts_name = t_fts_name % index
            self.__fts_index(self.op_remove, fts_name,
                             self.cluster.buckets[0],
                             CbServer.default_scope,
                             CbServer.default_collection)

        deleted_keys = self.cluster_util.get_ns_config_deleted_keys_count(self.cluster)
        self.sleep(60, "Wait before triggering purger")
        rest.run_tombstone_purger(50)
        self.sleep(10, "Wait for purger to cleanup tombstones")

        self.log.info("Validating purged keys on each node")
        purged_ts_dict = self.__get_purged_tombstone_from_last_run()
        for node_ip, purged_data in purged_ts_dict.items():
            if purged_data["count"] != deleted_keys[node_ip]:
                self.log_failure("%s - keys deleted %s != %s purged count"
                                 % (node_ip, deleted_keys[node_ip],
                                    purged_data["count"]))

        self.validate_test_failure()

    def test_create_similar_tombstones_across_each_node(self):
        """
        1. Create meta_kv tombstone independently on each cluster node
        2. Make sure purger cleans up the proper key without any conflict
           across the nodes
        """

        rest = RestConnection(self.cluster.master)
        self.log.info("Stopping meta_kv purger autorun")
        rest.disable_tombstone_purger()

        rest_objects = dict()
        for node in self.cluster.nodes_in_cluster:
            rest_objects[node.ip] = RestConnection(node)

        t_key = "meta_kv_key-%d"
        value = "test_create_similar_tombstones_across_each_node"

        # Create tombstones with custom key on each cluster node
        self.log.info("Creating %s tombstones" % self.num_index)
        for index in range(self.num_index):
            meta_kv_key = t_key % index
            for _, rest_obj in rest_objects.items():
                rest_obj.create_metakv_key(meta_kv_key, value)
                rest_obj.delete_metakv_key(meta_kv_key)

        self.sleep(15, "Waiting to trigger purger with tombstone age > 15secs")
        rest.run_tombstone_purger(15)
        self.sleep(30, "Waiting for purger to run")

        # Validate tombstone purging
        self.log.info("Validating purged keys on each node")
        purged_keys_dict = self.__get_purged_tombstone_from_last_run()
        for node_ip, purged_data in purged_keys_dict.items():
            for index in range(self.num_index):
                meta_kv_key = t_key % index
                if meta_kv_key not in purged_data['keys']:
                    self.log_failure(
                        "%s :: tombstone %s missing in purged keys. "
                        "Purged keys -> %s"
                        % (node_ip, meta_kv_key, purged_data['keys']))

        self.validate_test_failure()

    def test_meta_kv_purger_timer(self):
        """
        1. Create a meta_kv key entry and delete the key
        2. Wait for few minutes
        3. Create new tombstone entry with new_key
        4. Wait till purger run
        5. Validate whether purger has removed only key
           from step#1 and key from step#3 is retained
        """

        t_key = "meta_kv_key-%s" % self.time_stamp + "-%s"
        value = "dummy_value"
        max_ts_to_create = \
            int(self.default_purge_age/self.default_run_interval)
        last_key_index = max_ts_to_create - 1

        rest = RestConnection(self.cluster.master)
        # Remove any old tombstones prior to the test
        rest.run_tombstone_purger(1)
        # Reset default purger run values
        rest.update_tombstone_purger_run_interval(self.default_run_interval)
        rest.update_tombstone_purge_age_for_removal(self.default_purge_age)
        rest.disable_tombstone_purger()

        # Create tombstones
        for index in range(max_ts_to_create):
            key = t_key % index
            self.log.info("Creating tombstone with key '%s'" % key)
            self.cluster_util.create_metakv_key(self.cluster.master,
                                                key, value)
            self.cluster_util.delete_metakv_key(self.cluster.master, key)
            if index != last_key_index:
                self.sleep(self.default_run_interval,
                           "Wait before creating next tombstone")

        self.log.info("Enabling ts-purger with default settings")
        rest.enable_tombstone_purger()
        rest.update_tombstone_purger_run_interval(self.default_run_interval)
        rest.update_tombstone_purge_age_for_removal(self.default_purge_age)

        self.sleep(40, "Wait for purger to start running")

        # Trigger purger with default values to validate the purging keys
        for index in range(max_ts_to_create):
            # Reset timestamp to track since the purger ran now
            self.ts_during_start = \
                self.__get_current_timestamps_from_debug_log()
            self.log.info("New timestamp reference: %s" % self.ts_during_start)

            key = t_key % index
            self.sleep(self.default_run_interval, "Wait for purger to run")
            self.log.info("Validating purger for key '%s'" % key)
            purged_ts = self.__get_purged_tombstone_from_last_run()
            for node_ip, purged_data in purged_ts.items():
                if not purged_data['keys']:
                    self.fail("%s No tombstone purged" % node_ip)
                elif purged_data['count'] > 1:
                    self.fail("%s - multiple meta_kv tombstones purged: %s"
                              % (node_ip, purged_data['keys']))
                elif purged_data['keys'][0] != key:
                    self.fail("%s - Tombstone '%s' missing in purger log"
                              % (node_ip, key))

    def test_create_remove_same_meta_kv_key(self):
        """
        1. Create and remove same key across the entire purger cycle
        2. Make sure no key is removed during the purger run
        """
        key = "meta_kv_key-%d" % self.time_stamp
        value = "dummy_value"
        max_ts_to_create = \
            int(self.default_purge_age/self.default_run_interval)
        last_key_index = max_ts_to_create - 1

        rest = RestConnection(self.cluster.master)
        # Remove any old tombstones prior to the test
        rest.run_tombstone_purger(1)
        # Reset default purger run values
        rest.update_tombstone_purger_run_interval(self.default_run_interval)
        rest.update_tombstone_purge_age_for_removal(self.default_purge_age)
        rest.disable_tombstone_purger()

        # Create tombstones
        for index in range(max_ts_to_create):
            self.log.info("Creating tombstone with key '%s'" % key)
            self.cluster_util.create_metakv_key(self.cluster.master,
                                                key, value)
            self.cluster_util.delete_metakv_key(self.cluster.master, key)
            if index != last_key_index:
                self.sleep(self.default_run_interval,
                           "Wait before creating next tombstone")

        self.log.info("Enabling ts-purger with default settings")
        rest.enable_tombstone_purger()
        rest.update_tombstone_purger_run_interval(self.default_run_interval)
        rest.update_tombstone_purge_age_for_removal(self.default_purge_age)
        self.sleep(20, "Wait for purger to start running")

        # Trigger purger with default values to validate the purging keys
        for index in range(max_ts_to_create):
            self.sleep(self.default_run_interval,
                       "Wait before validating for key %s" % key)
            purged_ts = self.__get_purged_tombstone_from_last_run()
            for node_ip, purged_data in purged_ts.items():
                if index == last_key_index:
                    if purged_data['count'] == 0:
                        self.fail("%s - No tombstones purged" % node_ip)
                    elif purged_data['count'] != 1:
                        self.fail("%s - multiple meta_kv tombstones purged: %s"
                                  % (node_ip, purged_data['keys']))
                    elif purged_data['keys'][0] != key:
                        self.fail("%s - Tombstone '%s' missing in purger log"
                                  % (node_ip, key))
                else:
                    if len(purged_data['keys']) != 0:
                        self.fail("%s - Some meta_kv key(s) purged: %s"
                                  % (node_ip, purged_data['keys']))

    def test_fail_node_during_purge_run(self):
        """
        1. Create and remove meta_kv key(s)
        2. Hard failover a random node (non-orchestrator)
           such that the purger gets triggered as expected
        3. Let the purger run complete
        """
        t_key = "fts_index-%s" % int(self.time_stamp) + "-%s"
        rest = RestConnection(self.cluster.master)
        self.log.info("Stopping meta_kv purger autorun")
        rest.disable_tombstone_purger()

        self.log.info("Creating fts_index tombstones")
        for index in range(self.num_index):
            key = t_key % index
            self.__fts_index(self.op_create, key,
                             self.cluster.buckets[0],
                             CbServer.default_scope,
                             CbServer.default_collection)
            self.__fts_index(self.op_remove, key,
                             self.cluster.buckets[0],
                             CbServer.default_scope,
                             CbServer.default_collection)

        deleted_keys = self.cluster_util.get_ns_config_deleted_keys_count(self.cluster)
        del_key_count = None
        for node_ip, curr_count in deleted_keys.items():
            if del_key_count is None:
                del_key_count = curr_count
            elif del_key_count != curr_count:
                self.fail(
                    "%s - Deleted keys count mismatch. Expected %s, got %s"
                    % (node_ip, del_key_count, curr_count))

        random_node = choice(self.cluster.servers[1:])
        shell = RemoteMachineShellConnection(random_node)
        cb_err = CouchbaseError(self.log, shell)

        if choice([True, False]):
            cb_err.create(cb_err.KILL_BEAMSMP)
        else:
            self.log.info("Stopping couchbase server gracefully on %s"
                          % random_node.ip)
            shell.stop_couchbase()

        self.sleep(15, "Wait before triggering purger")
        self.log.info("Triggering purger when a node is in failed state")
        rest.run_tombstone_purger(10)
        purged_keys_dict = self.__get_purged_tombstone_from_last_run()
        for node_ip, purged_data in purged_keys_dict.items():
            if purged_data['count'] or purged_data['count'] != 0:
                shell.disconnect()
                self.fail("%s - Some keys got purged when node is failed: %s"
                          % (node_ip, purged_data))

        self.log.info("Healing node with server restart")
        shell.restart_couchbase()
        shell.disconnect()
        self.sleep(60, "Wait for node to come online")

        self.log.info("Triggering purger when a node is in failed state")
        rest.run_tombstone_purger(10)
        self.sleep(10, "Wait for purger to run")
        purged_keys_dict = self.__get_purged_tombstone_from_last_run()
        for node_ip, purged_data in purged_keys_dict.items():
            if purged_data['count'] == 0:
                self.fail("%s - No keys purged: %s" % (node_ip, purged_data))
            if purged_data['count'] != del_key_count:
                self.fail("%s - Purged key count mismatch. Expected %s, got %s"
                          % (node_ip, del_key_count, purged_data['count']))

    def test_fail_node_during_meta_key_delete(self):
        """
        1. Create a meta_kv key entry
        2. Stop any random node and trigger a meta_kv key delete operation
        3. Recover back the node and make sure the purging goes through fine
        """
        def heal_the_node(shell):
            self.log.info("Healing the cluster node")
            shell.restart_couchbase()
            shell.disconnect()
            self.sleep(60, "Wait for node to come online")

        custom_meta_kv_key = "key_01_%s" % self.time_stamp
        fts_key = "fts_index_%s" % int(self.time_stamp)
        rest = RestConnection(self.cluster.master)

        random_node = choice(self.cluster.servers[1:self.nodes_init])
        if random_node.ip == self.cluster.fts_nodes[0].ip:
            self.fts_helper = FtsHelper(self.cluster.fts_nodes[1])

        self.log.info("Stopping meta_kv purger autorun")
        rest.disable_tombstone_purger()

        # Creating meta_kv keys
        self.__fts_index(self.op_create, fts_key,
                         self.cluster.buckets[0],
                         CbServer.default_scope,
                         CbServer.default_collection)
        self.__perform_meta_kv_op_on_rand_key(self.op_create,
                                              custom_meta_kv_key, "value")

        shell = RemoteMachineShellConnection(random_node)
        cb_err = CouchbaseError(self.log, shell)

        self.sleep(5, "Waiting for meta_kv sync to happen")

        # Stopping ns_server on random node
        if choice([True, False]):
            self.log.info("Stopping couchbase server gracefully on %s"
                          % random_node.ip)
            shell.stop_couchbase()
        else:
            cb_err.create(cb_err.KILL_BEAMSMP)

        # Removing meta_kv keys when one of the node is down
        self.__fts_index(self.op_remove, fts_key,
                         self.cluster.buckets[0],
                         CbServer.default_scope,
                         CbServer.default_collection)
        self.__perform_meta_kv_op_on_rand_key(self.op_remove,
                                              custom_meta_kv_key)

        self.sleep(15, "Wait before triggering purger")
        rest.run_tombstone_purger(10)

        purged_keys_dict = self.__get_purged_tombstone_from_last_run()
        for node_ip, purged_data in purged_keys_dict.items():
            if purged_data['count'] or purged_data['count'] != 0:
                heal_the_node(shell)
                self.fail("%s - Some keys got purged when node is failed: %s"
                          % (node_ip, purged_data))

        # Recover from the error condition
        heal_the_node(shell)

        deleted_keys = self.cluster_util.get_ns_config_deleted_keys_count(self.cluster)
        self.log.info(deleted_keys)
        del_key_count = None
        for node_ip, curr_count in deleted_keys.items():
            if del_key_count is None:
                del_key_count = curr_count
            elif del_key_count != curr_count:
                self.fail(
                    "%s - Deleted keys count mismatch. Expected %s, got %s"
                    % (node_ip, del_key_count, curr_count))

        rest.run_tombstone_purger(10)
        retry_count = 10
        # Validate the key has been deleted from meta_kv
        for i in range(retry_count):
            try:
                purged_keys_dict = self.__get_purged_tombstone_from_last_run()
                for node_ip, purged_data in purged_keys_dict.items():
                    if purged_data['count'] == 0:
                        self.fail(
                            "%s - No keys purged: %s" % (node_ip, purged_data))
                    if purged_data['count'] != del_key_count:
                        self.fail(
                            "%s - Purged key count mismatch. Expected %s, got %s"
                            % (node_ip, del_key_count, purged_data['count']))
                    if custom_meta_kv_key not in purged_data['keys']:
                        self.fail("%s - Key %s missing in purger: %s"
                                  % (node_ip, custom_meta_kv_key,
                                     purged_data['keys']))
                break
            except Exception as e:
                if i == retry_count-1:
                    raise e
                else:
                    self.sleep(1, "waiting 1 sec before retry")


    def test_node_add_back(self):
        """
        1. Create tombstones in meta_kv
        2. Rebalance random_node out the the cluster
        3. Add back the node within/after meta_kv purger interval
        4. Validate purging happening for prev. deleted keys
        """
        def run_purger_and_validate_purged_key(check_key="exists"):
            self.__get_deleted_key_count()
            self.log.info("Triggering meta_kv purger")
            rest.run_tombstone_purger(10)
            self.sleep(10, "Wait for purger to complete")

            # Validate purger runs on targeted node
            purged_keys_dict = self.__get_purged_tombstone_from_last_run()
            self.log.info(purged_keys_dict)
            for t_ip, purged_data in purged_keys_dict.items():
                if check_key == "exists" \
                        and custom_meta_kv_key not in purged_data['keys']:
                    self.fail("%s - keys not present as expected: %s"
                              % (t_ip, purged_data['keys']))
                elif check_key == "not_exists" \
                        and custom_meta_kv_key in purged_data['keys']:
                    self.fail("%s - keys purged again: %s"
                              % (t_ip, purged_data['keys']))

            self.__get_deleted_key_count(check_if_zero=True)

            # Reset timestamp to track since the purger ran now
            self.ts_during_start = \
                self.__get_current_timestamps_from_debug_log()
            self.log.info("New timestamp reference: %s" % self.ts_during_start)

        rest = RestConnection(self.cluster.master)
        custom_meta_kv_key = "metakv_key_%s" % int(self.time_stamp)
        fts_key = "fts_index_%s" % int(self.time_stamp)
        target_node_type = \
            self.input.param("target_node", CbServer.Services.KV)
        remove_node_method = \
            self.input.param("removal_method", "rebalance_out")
        add_back_node_timing = self.input.param("add_back_node",
                                                "within_purge_interval")

        self.log.info("Stopping meta_kv purger autorun")
        rest.disable_tombstone_purger()

        # Creating meta_kv keys
        self.__fts_index(self.op_create, fts_key,
                         self.cluster.buckets[0],
                         CbServer.default_scope,
                         CbServer.default_collection)
        self.__perform_meta_kv_op_on_rand_key(self.op_create,
                                              custom_meta_kv_key, "value")

        # Remove meta_kv keys for tombstone creation
        self.__fts_index(self.op_remove, fts_key,
                         self.cluster.buckets[0],
                         CbServer.default_scope,
                         CbServer.default_collection)
        self.__perform_meta_kv_op_on_rand_key(self.op_remove,
                                              custom_meta_kv_key)

        if target_node_type == CbServer.Services.KV:
            target_node = choice(self.cluster.kv_nodes)
        elif target_node_type == CbServer.Services.FTS:
            target_node = choice(self.cluster.fts_nodes)
        else:
            target_node = choice(self.cluster.nodes_in_cluster)

        self.__get_deleted_key_count()

        self.log.info("Removing node %s using %s"
                      % (target_node.ip, remove_node_method))

        # Remove the target node
        if remove_node_method == "rebalance_out":
            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[0:self.nodes_init], [], [target_node])
        elif remove_node_method == "swap_rebalance":
            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[0:self.nodes_init],
                [self.cluster.servers[self.nodes_init]], [target_node])
        elif remove_node_method == "failover":
            rebalance_task = self.task.async_failover(
                self.cluster.servers[0:self.nodes_init], [target_node],
                graceful=True)
        else:
            self.fail("Invalid remove method '%s'" % remove_node_method)

        self.task_manager.get_task_result(rebalance_task)
        self.assertTrue(rebalance_task.result, "Rebalance failed")
        self.sleep(15, "Wait after rebalance")

        self.__get_deleted_key_count()

        # Update new master node
        if target_node.ip == self.cluster.master.ip:
            self.cluster_util.find_orchestrator(self.cluster,
                                                self.cluster.servers[1])

        self.log.info("Current master node: %s" % self.cluster.master.ip)

        if add_back_node_timing == "before_purge_interval":
            run_purger_and_validate_purged_key(check_key="exists")

        # Add back the removed node
        self.log.info("Adding back node %s" % target_node.ip)
        if remove_node_method in ["rebalance_out", "swap_rebalance"]:
            self.cluster_util.add_node(self.cluster, target_node,
                                       [CbServer.Services.KV])
        elif remove_node_method == "failover":
            rest.set_recovery_type(otpNode="ns_1@"+target_node.ip,
                                   recoveryType=self.recovery_type)
            rebalance_result = self.task.rebalance(
                self.cluster_util.get_nodes_in_cluster(self.cluster), [], [])
            self.assertTrue(rebalance_result, "Add back rebalance failed")

        if add_back_node_timing == "within_purge_interval":
            run_purger_and_validate_purged_key(check_key="exists")
        elif add_back_node_timing == "before_purge_interval":
            run_purger_and_validate_purged_key(check_key="not_exists")

    def test_fts_2i_purging_with_cluster_ops(self):
        """
        MB-43291
        1. Cluster with 3 kv, 2 index+n1ql, 4 search nodes
        2. 6 buckets with 5000 docs each
        3. Built 200 GSI indexes with replica 1 (50 indexes on each bucket)
        4. Created 30 fts custom indexes (10 indexes on 3 buckets),
           just to add more entries to meta-kv
        5. Create and Drop 100 gsi indexes sequentially on 4 buckets
           (so this would be adding more entries of create/drop of 400 indexes)
        6. Create and drop 50 fts indexes on 3 buckets.
        7. Graceful Failover a node with kv service.
        8. Failover took 1hr 30 minutes

        Note: Test requires one spare node for performing swap/reb_in tests
        """

        self.num_gsi_index = self.input.param("num_gsi_index", 50)
        self.num_fts_index = self.input.param("num_fts_index", 10)
        self.fts_indexes_to_create_drop = \
            self.input.param("fts_indexes_to_create_drop", 50)
        self.gsi_indexes_to_create_drop = \
            self.input.param("gsi_index_to_create_drop", 200)

        self.num_nodes_to_run = self.input.param("num_nodes_to_run", 1)
        self.target_service_nodes = \
            self.input.param("target_nodes", "kv").split(";")
        self.cluster_actions = \
            self.input.param("cluster_action", "rebalance_in").split(";")
        self.recovery_type = self.input.param(
            "recovery_type", CbServer.Failover.RecoveryType.DELTA)

        fts_generic_name = "fts_%s" % int(self.time_stamp) + "_%s_%s_%s_%d"

        rest = RestConnection(self.cluster.master)
        # Set RAM quota and Index storage mode
        rest.set_service_mem_quota(
            {CbServer.Settings.KV_MEM_QUOTA: 1024,
             CbServer.Settings.INDEX_MEM_QUOTA: 4096,
             CbServer.Settings.FTS_MEM_QUOTA: 4096,
             CbServer.Settings.CBAS_MEM_QUOTA: 1024,
             CbServer.Settings.EVENTING_MEM_QUOTA: 256})
        rest.set_indexer_storage_mode(storageMode="plasma")

        # Open SDK for connection for running n1ql queries
        self.client = self.sdk_client_pool.get_client_for_bucket(
            self.cluster.buckets[0])

        # Create required GSI indexes
        for bucket in self.cluster.buckets[:2]:
            for _, scope in bucket.scopes.items():
                for c_name, _ in scope.collections.items():
                    for index in range(0, self.num_gsi_index):
                        self.__gsi_index(self.op_create, bucket.name,
                                         scope.name, c_name, index)
            self.log.info("Done creating GSI indexes for %s" % bucket.name)

        # Create required FTS indexes
        bucket = self.cluster.buckets[0]
        for _, scope in bucket.scopes.items():
            for c_name, _ in scope.collections.items():
                for index in range(0, self.num_fts_index):
                    fts_name = fts_generic_name % (bucket.name, scope.name,
                                                   c_name, index)
                    self.__fts_index(self.op_create, fts_name,
                                     bucket, scope.name, c_name)
        self.log.info("Done creating FTS indexes for %s" % bucket.name)

        # Create GSI tombstones
        for bucket in self.cluster.buckets[:2]:
            self.log.info("Create and drop %s GSI indexes on %s"
                          % (self.gsi_indexes_to_create_drop, bucket.name))
            for _, scope in bucket.scopes.items():
                for c_name, _ in scope.collections.items():
                    for index in range(self.num_gsi_index,
                                       self.num_gsi_index
                                       + self.gsi_indexes_to_create_drop):
                        self.__gsi_index(self.op_create, bucket.name,
                                         scope.name, c_name, index)
                        self.__gsi_index(self.op_remove, bucket.name,
                                         scope.name, c_name, index)

        # Create FTS tombstones
        bucket = self.cluster.buckets[0]
        for _, scope in bucket.scopes.items():
            for c_name, _ in scope.collections.items():
                for index in range(self.num_fts_index,
                                   self.num_fts_index
                                   + self.fts_indexes_to_create_drop):
                    fts_name = fts_generic_name % (bucket.name, scope.name,
                                                   c_name, index)
                    self.__fts_index(self.op_create, fts_name,
                                     bucket, scope.name, c_name)
                    self.__fts_index(self.op_remove, fts_name,
                                     bucket, scope.name, c_name)

        # Release the SDK client
        self.sdk_client_pool.release_client(self.client)

        nodes_involved = list()
        for service in self.target_service_nodes:
            num_nodes_run = 0
            if service == CbServer.Services.KV:
                nodes_to_play = self.cluster.kv_nodes
            elif service == CbServer.Services.INDEX:
                nodes_to_play = self.cluster.index_nodes
            elif service == CbServer.Services.N1QL:
                nodes_to_play = self.cluster.query_nodes
            elif service == CbServer.Services.INDEX:
                nodes_to_play = self.cluster.cbas_nodes
            elif service == CbServer.Services.FTS:
                nodes_to_play = self.cluster.fts_nodes
            elif service == CbServer.Services.EVENTING:
                nodes_to_play = self.cluster.eventing_nodes
            else:
                self.fail("Invalid service %s" % service)

            cluster_node = None
            for node in nodes_to_play:
                if node.ip in nodes_involved:
                    continue

                # Note affected nodes to avoid repeating action on same node
                # Happens when >1 service running on the node
                self.log.info("Master node: %s" % self.cluster.master.ip)
                nodes_involved.append(node.ip)

                for cluster_action in self.cluster_actions:
                    self.log.info("Performing '%s' on node %s"
                                  % (cluster_action, node.ip))
                    nodes_in_cluster = \
                        self.cluster_util.get_nodes_in_cluster(self.cluster)
                    if cluster_action == "rebalance_in":
                        rest = RestConnection(self.cluster.master)
                        self.task.rebalance(nodes_in_cluster, [node], [])
                        self.sleep(30, "Wait for rebalance to start")
                        self.assertTrue(
                            rest.monitorRebalance(stop_if_loop=True),
                            "Rebalance_in failed")
                    elif cluster_action == "rebalance_out":
                        rest = RestConnection(self.cluster.master)
                        self.task.rebalance(nodes_in_cluster, [], [node])
                        self.sleep(30, "Wait for rebalance to start")
                        self.assertTrue(
                            rest.monitorRebalance(stop_if_loop=True),
                            "Rebalance_out failed")
                        for t_node in nodes_in_cluster:
                            cluster_node = t_node
                            if cluster_node.ip != self.cluster.master.ip:
                                self.cluster_util.find_orchestrator(
                                    self.cluster, cluster_node)
                                break
                    elif cluster_action == "swap_rebalance":
                        rest = RestConnection(self.cluster.master)
                        self.task.rebalance(
                            nodes_in_cluster, [self.spare_node], [node],
                            services=None,
                            check_vbucket_shuffling=False)
                        self.sleep(30, "Wait for rebalance to start")
                        self.assertTrue(
                            rest.monitorRebalance(stop_if_loop=True),
                            "Swap_rebalance failed")
                        if node.ip == self.cluster.master.ip:
                            self.cluster.master = self.spare_node
                            self.cluster_util.find_orchestrator(
                                self.cluster, self.spare_node)
                        self.spare_node = node
                    elif cluster_action == "graceful_failover":
                        rest = None
                        for t_node in nodes_in_cluster:
                            if t_node.ip != node.ip:
                                rest = RestConnection(t_node)
                                cluster_node = t_node
                                break
                        rest.fail_over("ns_1@" + node.ip, graceful=True)
                        self.sleep(10, "Wait for failover to start")
                        self.assertTrue(
                            rest.monitorRebalance(stop_if_loop=True),
                            "Failover failed for node %s" % node.ip)
                    elif cluster_action == "rebalance_failover_node":
                        current_nodes = list()
                        for t_node in nodes_in_cluster:
                            if t_node.ip != node.ip:
                                current_nodes.append(t_node)

                        # Rebalance_out failed-over node
                        rest = RestConnection(self.cluster.master)
                        self.task.rebalance(current_nodes, [], [])
                        self.sleep(10, "Wait after cluster rebalance")
                        self.assertTrue(
                            rest.monitorRebalance(stop_if_loop=True),
                            "Rebalance failed with failover node %s" % node.ip)
                        self.cluster_util.find_orchestrator(self.cluster,
                                                            cluster_node)
                    elif cluster_action == "add_back_failover_node":
                        rest = RestConnection(self.cluster.master)
                        rest.set_recovery_type("ns_1@" + node.ip,
                                               self.recovery_type)
                        self.task.rebalance(nodes_in_cluster, [], [])
                        self.sleep(30)
                        self.assertTrue(
                            rest.monitorRebalance(stop_if_loop=True),
                            "Rebalance failed with failover node %s" % node.ip)
                        self.cluster_util.find_orchestrator(self.cluster,
                                                            cluster_node)

                # Break if max nodes to run has reached per service
                num_nodes_run += 1
                if num_nodes_run >= self.num_nodes_to_run:
                    break
