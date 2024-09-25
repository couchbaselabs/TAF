
import json
import os
from BucketLib.bucket import Bucket
from EventingLib.EventingOperations_Rest import EventingHelper
from FtsLib.FtsOperations_Rest import FtsHelper
from cluster_utils.cluster_ready_functions import CBCluster
from membase.api.rest_client import RestConnection
from ns_server.node_remap_base import NodeRemapBase
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import doc_generator
from fts_utils.fts_ready_functions import FTSUtils
from sdk_client3 import SDKClient

class NodeRemapTests(NodeRemapBase):

    def setUp(self):
        super(NodeRemapTests, self).setUp()

        self.init_data_load = self.input.param("init_data_load", True)
        self.node_specific_indexes = self.input.param("node_specific_indexes", False)
        self.run_post_remap_op = self.input.param("run_post_remap_op", True)

        # Enable shard affinity
        if len(self.cluster.index_nodes) > 0:
            idx_rest = RestConnection(self.cluster.index_nodes[0])
            idx_rest.set_indexer_params(redistributeIndexes='true',
                                        enableShardAffinity='true')

    def test_node_remap(self):

        self.pre_node_remap()
        self.cluster_util.print_cluster_stats(self.cluster)

        self.sleep(60, "Wait before performing the node remap procedure")
        self.run_node_remap()
        self.sleep(60, "Sleep after node re-mapping")

        self.setup_cloned_cluster()

        self.validation()

        if self.run_post_remap_op:
            self.post_node_remap()


    def run_node_remap(self):

        self.node_map_dict = dict()

        # Stop couchbase server on the source and destination nodes
        for node in self.cluster.nodes_in_cluster + self.dest_servers:
            self.log.info("Stopping Couchbase server on node: {}".format(node.ip))
            shell = RemoteMachineShellConnection(node)
            shell.stop_couchbase()
            shell.disconnect()

        dest_node_idx = 0
        # Copy data from source nodes to dest nodes
        for source_node in self.cluster.nodes_in_cluster:
            dest_node = self.dest_servers[dest_node_idx]
            self.log.info("Copying data from {0} to {1}".format(
                source_node.ip, dest_node.ip))
            self.node_map_dict[dest_node.ip] = source_node.ip
            self.copy_data_to_dest_node(source_node, dest_node, self.output_dir)
            dest_node_idx += 1

        # Remove and Overwrite existing couchbase data
        for node in self.dest_servers:
            shell = RemoteMachineShellConnection(node)
            shell.execute_command("rm -rf {}/*".format(self.couchbase_folder))
            shell.execute_command("yes | cp -Rf {0}/* {1}/".format(self.output_dir,
                                                            self.couchbase_folder))

        self.log.info("Performing node-remap")
        remap_script_cmd = self.build_config_remap_script_cmd(self.output_dir)
        self.log.info("Remap script cmd = {}".format(remap_script_cmd))

        # Running node remap script on all dest nodes
        for node in self.dest_servers:
            shell = RemoteMachineShellConnection(node)
            o, e = shell.execute_command(remap_script_cmd)
            shell.log_command_output(o, e)
            shell.disconnect()

        for node in self.dest_servers:
            # Config will be overwritten in the output dir by the script,
            # copy the changes to the main Couchbase folder
            shell = RemoteMachineShellConnection(node)
            shell.execute_command("yes | cp -Rf {0}/* {1}/".format(
                        self.output_dir, self.couchbase_folder))

            # Reset permissions
            shell.execute_command("chown -R couchbase {0} && chmod -R 0700 {0}".format(
                                    self.couchbase_folder))
            if self.alternate_data_idx_path:
                for path in [self.data_path, self.index_path]:
                    shell.execute_command("chown -R couchbase {0}" \
                                " && chmod -R 0700 {0}".format(path))

        # Start couchbase server on the source and destination nodes
        self.log.info("Starting Couchbase server on all nodes")
        for node in self.cluster.nodes_in_cluster + self.dest_servers:
            shell = RemoteMachineShellConnection(node)
            shell.start_couchbase()
            shell.disconnect()


    def pre_node_remap(self):

        # Load data into the buckets
        if self.init_data_load:
            for bucket in self.cluster.buckets:
                if self.large_docs is False:
                    self.data_load(self.cluster, bucket, 0,
                                   self.collection_items, "create")
                else:
                    self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                        self.collection_items, self.doc_size)

        # enable history on bucket
        if self.set_history_at_start is False and \
            (self.bucket_dedup_retention_seconds is not None or \
             self.bucket_dedup_retention_bytes is not None):
            for bucket in self.cluster.buckets:
                if bucket.storageBackend == Bucket.StorageBackend.magma:
                    self.bucket_util.update_bucket_property(
                        self.cluster.master, bucket,
                        history_retention_bytes=self.bucket_dedup_retention_bytes,
                        history_retention_seconds=self.bucket_dedup_retention_seconds)

        # Creating indexes
        self.index_count = 0
        self.indexes = []
        if len(self.cluster.index_nodes) > 0:
            field_names = ["body", "name", "mutation_type", "age"]
            replica = True
            if self.node_specific_indexes is True or len(self.cluster.index_nodes) < 2:
                replica = False
            for field in field_names[:2]:
                for bucket in self.cluster.buckets:
                    self.build_indexes(self.cluster, bucket, field, replica,
                                       self.node_specific_indexes)
            self.log.info("Number of indexes built = {}".format(self.index_count))
            self.log.info("Indexes = {}".format(self.indexes))

        if self.history_load:
            for bucket in self.cluster.buckets:
                if bucket.storageBackend == Bucket.StorageBackend.magma:
                    self.data_load(self.cluster, bucket, 0, self.collection_items, "update")

        if len(self.cluster.fts_nodes) > 0:
            fts_idx_prefix = "test_fts_index"
            self.fts_index_count = 0
            fts_util = FTSUtils(self.cluster, self.cluster_util, self.task)
            for bucket in self.cluster.buckets:
                for _, scope in bucket.scopes.items():
                    if scope.name == "_system":
                        continue
                    for _, collection in scope.collections.items():
                        fts_idx_name = fts_idx_prefix+str(self.fts_index_count)
                        status = fts_util.create_fts_indexes(bucket, scope,
                                                        collection, fts_idx_name)
                        self.log.info("Result of creation of fts index: " \
                                      "{0} = {1}".format(fts_idx_name, status))
                        self.fts_index_count += 1
            self.sleep(60, "Wait after creating FTS indexes")

        if len(self.cluster.eventing_nodes) > 0:
            self.eventing_funcs = 0
            self.eventing_func_dict = dict()
            deploy = True
            func_to_pause = None
            for bucket in self.cluster.buckets:
                func_name = "eventingfunc_" + bucket.name
                self.create_deploy_eventing_functions(self.cluster, bucket,
                                                      func_name, deploy=deploy)
                self.eventing_func_dict[func_name] = deploy
                self.eventing_funcs += 1
                if deploy and func_to_pause is None:
                    func_to_pause = func_name
                deploy = not deploy
            self.sleep(60, "Wait after creating/deploying eventing functions")
            # Pause an eventing function
            status, _ = EventingHelper(self.cluster.eventing_nodes[0]).\
                            pause_eventing_function(func_to_pause)
            if status:
                self.log.info("Eventing function {} paused".format(func_to_pause))
                self.eventing_func_dict[func_to_pause] = "paused"
            self.log.info("Eventing functions = {}".format(self.eventing_func_dict))

        if len(self.cluster.cbas_nodes) > 0:
            self.dataset_count = 0
            cbas_rest = RestConnection(self.cluster.cbas_nodes[0])
            dataverse_statement = "CREATE DATAVERSE mydataverse IF NOT EXISTS;"
            self.log.info("Running query on cbas: {}".format(dataverse_statement))
            content = cbas_rest.execute_statement_on_cbas(dataverse_statement)
            self.log.info("Content = {}".format(content))
            for bucket in self.cluster.buckets:
                dataset_name = "mydataset" + str(self.dataset_count)
                statement = "USE mydataverse; CREATE DATASET {} ON `{}`." \
                    "`_default`.`_default`;".format(dataset_name, bucket.name)
                self.log.info("Running query on cbas: {}".format(statement))
                content = cbas_rest.execute_statement_on_cbas(statement)
                self.log.info("Content = {}".format(content))
                self.dataset_count += 1
            connect_stmt = "CONNECT LINK Local;"
            self.log.info("Running query on cbas: {}".format(connect_stmt))
            content = cbas_rest.execute_statement_on_cbas(connect_stmt)
            self.log.info("Content = {}".format(content))

        self.bucket_util.print_bucket_stats(self.cluster)

    def data_load(self, cluster, bucket, start, end, op_type, key="test_docs"):

        doc_gen = doc_generator(key, start, end,
                                doc_size=self.doc_size,
                                randomize_value=True)
        self.log.info("Loading {0} items into each collection of bucket:{1}"\
                      .format(end-start, bucket.name))
        for _, scope in bucket.scopes.items():
            if scope.name == "_system":
                continue
            for _, collection in scope.collections.items():
                load_task = self.task.async_load_gen_docs(
                    cluster, bucket, doc_gen, op_type,
                    timeout_secs=self.sdk_timeout,
                    batch_size=self.batch_size,
                    scope=scope.name, collection=collection.name)
                self.task.jython_task_manager.get_task_result(load_task)

    def load_data_cbc_pillowfight(self, server, bucket, items, doc_size,
                                key_prefix="test_docs", threads=1, ops_rate=None):

        shell = RemoteMachineShellConnection(server)

        self.log.info("Loading {0} items of doc size: {1} into the bucket" \
                      " with cbc-pillowfight".format(items, doc_size))
        pillowfight_base_cmd = "/opt/couchbase/bin/cbc-pillowfight -U {0}/{1}" \
                                " -u Administrator -P password -I {2}" \
                                " -t {3} -m {4} -M {4} --populate-only --random-body" \
                                " --key-prefix={5} -Dtimeout=10"

        cmd = pillowfight_base_cmd.format(server.ip, bucket.name, items, threads,
                                          doc_size, key_prefix)

        if ops_rate is not None:
            cmd += " --rate-limit {}".format(ops_rate)

        self.log.info("Executing pillowfight command = {}".format(cmd))
        o, e = shell.execute_command(cmd)
        self.sleep(30, "Wait after executing pillowfight command")
        shell.disconnect()

    def build_indexes(self, cluster, bucket, field, replica=True,
                      node_specific=False):

        index_query_node = cluster.index_nodes[0]
        query_client = RestConnection(index_query_node)

        for _, scope in bucket.scopes.items():
            if scope.name == "_system":
                continue
            for _, collection in scope.collections.items():
                bucket_name = bucket.name
                idx_name = bucket_name + "#" + scope.name + "#" + collection.name \
                                + "#" + field
                index_create_query = 'CREATE INDEX `{0}` ON `{1}`.`{2}`.`{3}`' \
                    '(`{4}` include missing)' \
                    .format(idx_name, bucket.name, scope.name, collection.name, field)
                if replica:
                    index_create_query += ' WITH { "num_replica":1 }'
                if node_specific:
                    index_create_query += ' WITH {"nodes": ["{}:18091"]}'.format(
                        index_query_node.ip)
                index_create_query += ";"
                self.log.info("Building index with def: {}".format(index_create_query))
                result = query_client.query_tool(index_create_query)
                if result['status'] == 'success':
                    self.index_count += 1
                    self.indexes.append(idx_name)

    def create_deploy_eventing_functions(self, cluster, bucket, appname,
                                         meta_coll_name="eventing_metadata",
                                         deploy=True):

        eventing_helper = EventingHelper(cluster.eventing_nodes[0])
        self.log.info("Creating collection: {} for eventing".format(meta_coll_name))
        self.bucket_util.create_collection(
            cluster.master, bucket,
            "_default", {"name": meta_coll_name})
        self.sleep(2)

        eventing_functions_dir = os.path.join(os.getcwd(),
                                "pytests/eventing/exported_functions/volume_test")
        file = "bucket-op.json"
        fh = open(os.path.join(eventing_functions_dir, file), "r")
        body = fh.read()
        json_obj = json.loads(body)
        fh.close()

        json_obj["depcfg"].pop("buckets")
        json_obj["appcode"] = "function OnUpdate(doc, meta, xattrs) {\n    " \
                    "log(\"Doc created/updated\", meta.id);\n} \n\nfunction " \
                    "OnDelete(meta, options) {\n    log(\"Doc deleted/expired\", " \
                    "meta.id);\n}"
        json_obj["depcfg"]["source_bucket"] = bucket.name
        json_obj["depcfg"]["source_scope"] = "_default"
        json_obj["depcfg"]["source_collection"] = "_default"
        json_obj["depcfg"]["metadata_bucket"] = bucket.name
        json_obj["depcfg"]["metadata_scope"] = "_default"
        json_obj["depcfg"]["metadata_collection"] = meta_coll_name
        json_obj["appname"] = appname
        self.log.info("Creating Eventing function - {}".format(json_obj["appname"]))
        eventing_helper.create_function(appname, json_obj)
        if deploy:
            eventing_helper.deploy_eventing_function(appname)

    def setup_cloned_cluster(self):

        cloned_cluster_name = "C2"
        self.dest_cluster = CBCluster(name=cloned_cluster_name,
                                      servers=self.dest_servers,
                                      vbuckets=self.cluster.vbuckets)
        self.dest_cluster.master = self.dest_servers[0]
        self.dest_cluster.buckets = self.bucket_util.get_all_buckets(self.dest_cluster)

        for server in self.dest_servers:
            s_node_ip = self.node_map_dict[server.ip]
            for node in self.cluster.nodes_in_cluster:
                if node.ip == s_node_ip:
                    server.services = node.services
        self.dest_cluster.nodes_in_cluster = self.dest_servers
        self.dest_cluster.servers = self.cluster.servers[self.nodes_init:]
        self.cluster_util.update_cluster_nodes_service_list(self.dest_cluster)
        self.cluster_util.print_cluster_stats(self.dest_cluster)

    def validate_pools_default(self):

        self.log.info("Validating pools/default")
        dest_pools_default = self.dest_rest.get_pools_default()
        source_pools_default = self.rest.get_pools_default()

        source_node_dict = dict()
        for node in source_pools_default["nodes"]:
            source_node_dict[node["otpNode"][5:]] = node

        # check if nodes are healthy
        for node in dest_pools_default["nodes"]:
            self.log.info("{0}: {1}".format(node["hostname"], node["status"]))
            if node["status"] != "healthy":
                self.fail("Node {} is not in a healthy state".format(node["hostname"]))
            dest_services = node["services"]
            source_node_services = \
                source_node_dict[self.node_map_dict[node["otpNode"][5:]]]["services"]
            self.log.info("Services on source node = {}".format(source_node_services))
            self.log.info("Services on dest node = {}".format(dest_services))
            self.assertEqual(source_node_services, dest_services,
                             "Services do not match")

        # verify other fields in pools/default
        fields = ["clusterEncryptionLevel", "memoryQuota", "queryMemoryQuota",
                  "indexMemoryQuota", "ftsMemoryQuota", "cbasMemoryQuota",
                  "eventingMemoryQuota"]
        for field in fields:
            err_msg = "{} not matching".format(field)
            self.assertEqual(source_pools_default[field],
                             dest_pools_default[field], err_msg)

    def validate_sequence_numbers(self):

        if len(self.dest_cluster.eventing_nodes) > 0:
            return

        self.log.info("Validating sequence numbers")

        # record high_seq_no and history_start_seq_no on the source cluster
        self.vb_dict_list = dict()
        for bucket in self.cluster.buckets:
            if bucket.bucketType == Bucket.Type.EPHEMERAL:
                continue
            vb_dict = self.bucket_util.get_vb_details_for_bucket(
                            bucket, self.cluster.nodes_in_cluster)
            self.vb_dict_list[bucket.name] = vb_dict

        # record high_seq_no and history_start_seq_no on the cloned cluster
        self.vb_dict_list2 = dict()
        for bucket in self.cluster.buckets:
            if bucket.bucketType == Bucket.Type.EPHEMERAL:
                continue
            vb_dict = self.bucket_util.get_vb_details_for_bucket(
                                bucket, self.dest_servers)
            self.vb_dict_list2[bucket.name] = vb_dict

        for bucket in self.cluster.buckets:
            if bucket.bucketType == Bucket.Type.EPHEMERAL:
                continue
            bucket_name = bucket.name
            for vb_no in range(self.cluster.vbuckets):

                # Verifying active vbuckets
                source_vb_active_seq = self.vb_dict_list[bucket_name][vb_no]\
                                        ['active']['high_seqno']
                dest_vb_active_seq = self.vb_dict_list2[bucket_name][vb_no]\
                                        ['active']['high_seqno']
                err_msg = "Active sequence number mismatch for active vbucket {0}" \
                        "in bucket: {1} Expected: {2}, Actual: {3}" \
                        .format(vb_no, bucket_name, source_vb_active_seq,
                                dest_vb_active_seq)
                self.assertTrue(source_vb_active_seq == dest_vb_active_seq, err_msg)

                if bucket.storageBackend == Bucket.StorageBackend.magma:
                    source_vb_active_hist = self.vb_dict_list[bucket_name][vb_no]\
                        ['active']['history_start_seqno']
                    dest_vb_active_hist = self.vb_dict_list2[bucket_name][vb_no]\
                        ['active']['history_start_seqno']
                    err_msg = "History start seqno mismatch for active vbucket {0}" \
                            "in bucket: {1} Expected: {2}, Actual: {3}" \
                        .format(vb_no, bucket_name, source_vb_active_hist,
                                dest_vb_active_hist)
                    self.assertTrue(source_vb_active_hist == dest_vb_active_hist,
                                    err_msg)

                # Verifying replica vbuckets
                replica_list1 = self.vb_dict_list[bucket_name][vb_no]['replica']
                replica_list2 = self.vb_dict_list2[bucket_name][vb_no]['replica']
                for j in range(len(replica_list1)):
                    source_vb_replica_seq = replica_list1[j]['high_seqno']
                    dest_vb_replica_seq = replica_list2[j]['high_seqno']
                    err_msg = "Active sequence number mismatch for replica vbucket {0} " \
                        "in bucket: {1} Expected: {2}, Actual: {3}" \
                        .format(vb_no, bucket_name, source_vb_replica_seq,
                                dest_vb_replica_seq)
                    self.assertTrue(source_vb_replica_seq == dest_vb_replica_seq,
                                    err_msg)

                    if bucket.storageBackend == Bucket.StorageBackend.magma:
                        source_vb_replica_hist = replica_list1[j]['history_start_seqno']
                        dest_vb_replica_hist = replica_list2[j]['history_start_seqno']
                        err_msg = "History start seqno mismatch for replica vbucket {0} "\
                          "in bucket: {1} Expected: {2}, Actual: {3}" \
                          .format(vb_no, bucket_name, source_vb_replica_hist,
                                  dest_vb_replica_hist)
                        self.assertTrue(source_vb_replica_hist == dest_vb_replica_hist)

    def validate_bucket_properties(self):

        self.log.info("Validating bucket properties")
        bucket_properties = ["rank", "replicaNumber", "evictionPolicy",
            "durabilityMinLevel", "historyRetentionSeconds", "historyRetentionBytes",
            "maxTTL", "numVBuckets", "quota", "storageBackend"]

        for bucket in self.cluster.buckets:
            source_bucket = self.rest.get_bucket_details(bucket.name)
            dest_bucket = self.dest_rest.get_bucket_details(bucket.name)

            for property in bucket_properties:
                if property in source_bucket:
                    self.assertTrue(source_bucket[property] == dest_bucket[property],
                        "Bucket property: {0} mismatch. Expected: {1}, Actual: {2}"\
                        .format(property, source_bucket[property], dest_bucket[property]))

            # Validate bucket item count
            source_item_count = source_bucket["basicStats"]["itemCount"]
            dest_item_count = dest_bucket["basicStats"]["itemCount"]
            self.assertEqual(source_item_count, dest_item_count,
                            "Item count mismatch for bucket: {0}" \
                            "Expected: {1}, Actual: {2}" \
                            .format(bucket.name, source_item_count,
                                    dest_item_count))
            self.log.info("Item count verified for bucket: {}".format(bucket.name))

    def validation(self):

        self.dest_rest = RestConnection(self.dest_servers[0])
        self.validate_pools_default()
        self.validate_bucket_properties()

        # Perform validation like item count, index count
        self.log.info("Validating cluster UUID and cookie")
        _, content = self.rest.get_terse_cluster_info()
        terse_info = json.loads(content)
        source_cluster_uuid = terse_info["clusterUUID"]
        _, content = self.dest_rest.get_terse_cluster_info()
        terse_info = json.loads(content)
        dest_cluster_uuid = terse_info["clusterUUID"]
        self.assertTrue(source_cluster_uuid != dest_cluster_uuid,
                        "Cluster UUID hasn't been updated")

        for server in [self.cluster.master, self.dest_servers[0]]:
            RemoteMachineShellConnection(server).enable_diag_eval_on_non_local_hosts()
        _, source_cookie = self.rest.diag_eval("erlang:get_cookie()")
        _, dest_cookie = self.dest_rest.diag_eval("erlang:get_cookie()")
        self.assertTrue(source_cookie != dest_cookie, "Cookie hasn't been updated")

        self.validate_sequence_numbers()

        # Validate that auto-failover is disabled
        af_settings = RestConnection(self.dest_cluster.master).\
                            get_autofailover_settings()
        if not self.use_config_remap or \
            (self.use_config_remap and self.disable_af_config_remap):
            self.assertFalse(af_settings.enabled, "Auto-failover was not disabled")
        elif self.use_config_remap and not self.disable_af_config_remap:
            self.assertTrue(af_settings.enabled, "Auto-failover was disabled")

        # Validate that alternate addresses are removed
        content = RestConnection(self.dest_cluster.master).get_node_services()
        for node in content["nodesExt"]:
            self.assertTrue("alternateAddresses" not in node,
                            "Alternate address not removed for {}".\
                                format(node["hostname"]))

        # Validate number of indexes
        if len(self.dest_cluster.index_nodes) > 0:
            sdk_client = SDKClient([self.dest_cluster.index_nodes[0]], None)
            query = 'SELECT * FROM system:indexes WHERE `using` = "gsi";'
            result = sdk_client.cluster.query(query).rowsAsObject()
            self.assertTrue(len(result) == self.index_count,
                            "Index count mismatch. Expected: {0}, Actual: {1}" \
                            .format(self.index_count, len(result)))
            self.log.info("Index count verified")
            sdk_client.close()

        # Validate number of FTS indexes
        if len(self.dest_cluster.fts_nodes) > 0:
            self.fts_helper = FtsHelper(self.dest_cluster.fts_nodes[0])
            status, content = self.fts_helper.get_all_fts_indexes()
            if status:
                fts_indexes = json.loads(content)
                dest_fts_index_count = len(fts_indexes["indexDefs"]["indexDefs"].keys())
                self.assertTrue(self.fts_index_count == dest_fts_index_count,
                                "FTS index count mismatch. Expected = {0}, Actual = {1}." \
                                "List of indexes on the cloned cluster = {2}" \
                                .format(self.fts_index_count, dest_fts_index_count,
                                        fts_indexes["indexDefs"]["indexDefs"].keys()))
                self.log.info("FTS index count verified")

        # Validate analytics
        if len(self.dest_cluster.cbas_nodes) > 0:
            cbas_rest = RestConnection(self.dest_cluster.cbas_nodes[0])
            statement = 'USE mydataverse; SELECT VALUE d.DataverseName || "." ' \
                        '|| d.DatasetName FROM Metadata.`Dataset` d ' \
                        'WHERE d.DataverseName = "mydataverse";'
            self.log.info("Running query on cbas: {}".format(statement))
            content = cbas_rest.execute_statement_on_cbas(statement)
            json_content = json.loads(content)
            self.assertTrue(len(json_content["results"]) == self.dataset_count,
                            "Mismatch in the number of analytics datasets count")
            self.log.info("Content = {}".format(content))

        # Validate number of eventing functions
        if len(self.dest_cluster.eventing_nodes) > 0:
            eventing_helper = EventingHelper(self.dest_cluster.eventing_nodes[0])
            content = eventing_helper.get_all_functions()
            content = json.loads(content)
            # Validating number of eventing funcs
            self.assertTrue(len(content) == self.eventing_funcs,
                            "Count mismatch in eventing functions. " \
                            "Expected = {}, Actual = {}".format(
                            self.eventing_funcs, len(content)))
            # Validating status of eventing funcs
            for i in range(len(content)):
                func_content = content[i]
                func_name = func_content["appname"]
                deploy = func_content["settings"]["deployment_status"]
                state = func_content["settings"]["processing_status"]
                err_msg = "Eventing function state mismatch"
                if self.eventing_func_dict[func_name] is True:
                    self.assertTrue(deploy == state == True, err_msg)
                elif self.eventing_func_dict[func_name] is False:
                    self.assertTrue(deploy == state == False, err_msg)
                elif self.eventing_func_dict[func_name] == "paused":
                    self.assertTrue(deploy == True and state == False, err_msg)
            self.log.info("State of eventing functions validated")

    def post_node_remap(self):

        # CRUDs on the existing buckets
        start = self.collection_items
        end = self.collection_items + min(10000, self.collection_items)
        for bucket in self.dest_cluster.buckets:
            self.log.info("Performing CRUDs on bucket: {}".format(bucket.name))
            for op_type in ["create", "update", "read"]:
                self.log.info("Performing {} op workload".format(op_type))
                self.data_load(self.dest_cluster, bucket, start, end, op_type=op_type)
        self.bucket_util.print_bucket_stats(self.dest_cluster)

        # Creating new buckets on the cloned cluster
        bucket_prefix = "newbucket"
        for bucket_storage in ["magma", "couchstore"]:
            bucket_name = bucket_prefix+str(bucket_storage)
            self.log.info("Creating bucket: {}".format(bucket_name))
            self.bucket_util.create_default_bucket(
                    self.dest_cluster,
                    bucket_type=self.bucket_type,
                    ram_quota=256,
                    replica=self.num_replicas,
                    storage=bucket_storage,
                    eviction_policy=self.bucket_eviction_policy,
                    bucket_name=bucket_name)
        for bucket in self.dest_cluster.buckets:
            if bucket.name[:3] == "new":
                self.data_load(self.dest_cluster, bucket, 0, 10000, "create")
        self.bucket_util.print_bucket_stats(self.dest_cluster)

        # Create indexes on the cloned cluster
        if len(self.dest_cluster.index_nodes) > 0:
            for bucket in self.dest_cluster.buckets:
                if bucket.name[:3] == "new":
                    replica = len(self.dest_cluster.index_nodes) >= 2
                    self.build_indexes(self.dest_cluster, bucket, "age",
                                    replica=replica)

        # Rebalance-in a new node
        self.sleep(60, "Wait before rebalancing-in")
        spare_node = self.cluster.servers[self.nodes_init*2]
        rebalance_passed = self.task.rebalance(self.dest_cluster,
                                to_add=[spare_node], to_remove=[],
                                check_vbucket_shuffling=False,
                                services=["kv,index,n1ql,fts,eventing,cbas"],
                                retry_get_process_num=300)
        self.assertTrue(rebalance_passed, "Rebalance-in of node failed")
        self.cluster_util.print_cluster_stats(self.dest_cluster)

        # Rebalance-out a node
        self.sleep(60, "Wait before rebalancing-out")
        node_to_remove = self.dest_servers[1]
        rebalance_passed = self.task.rebalance(self.dest_cluster,
                                to_add=[], to_remove=[node_to_remove],
                                check_vbucket_shuffling=False,
                                retry_get_process_num=300)
        self.assertTrue(rebalance_passed, "Rebalance-out of node failed")
        self.cluster_util.print_cluster_stats(self.dest_cluster)
        spare_node = node_to_remove

        # Swap rebalance of nodes
        self.sleep(60, "Wait before swap rebalance")
        node_to_remove = self.dest_cluster.index_nodes[0]
        services = RestConnection(self.dest_cluster.master).get_nodes_services()
        services_on_target_node = services[(node_to_remove.ip + ":"
                                            + str(node_to_remove.port))]
        rebalance_passed = self.task.rebalance(self.dest_cluster,
                                to_add=[spare_node],
                                to_remove=[node_to_remove],
                                check_vbucket_shuffling=False,
                                services=[",".join(services_on_target_node)],
                                retry_get_process_num=300)
        self.assertTrue(rebalance_passed, "Swap rebalance of nodes failed")
        self.cluster_util.print_cluster_stats(self.dest_cluster)

        self.log.info("Creating new buckets post rebalance")
        bucket_prefix = "testbucket"
        for bucket_storage in ["magma", "couchstore"]:
            bucket_name = bucket_prefix+str(bucket_storage)
            self.log.info("Creating bucket: {}".format(bucket_name))
            self.bucket_util.create_default_bucket(
                    self.dest_cluster,
                    bucket_type=self.bucket_type,
                    ram_quota=256,
                    replica=self.num_replicas,
                    storage=bucket_storage,
                    eviction_policy=self.bucket_eviction_policy,
                    bucket_name=bucket_name)
        for bucket in self.dest_cluster.buckets:
            if bucket.name[:4] == "test":
                self.data_load(self.dest_cluster, bucket, 0, 10000, "create")
        self.bucket_util.print_bucket_stats(self.dest_cluster)

        # Verify that new buckets are created with the configured numVbuckets
        for bucket in self.dest_cluster.buckets:
            if bucket.name[:3] == "new" or bucket.name[:4] == "test":
                bucket_details = RestConnection(self.dest_cluster.master).\
                                    get_bucket_details(bucket.name)
                self.assertEqual(bucket_details["numVBuckets"], self.cluster.vbuckets,
                        "New buckets not created with configured numVbuckets")

        # Create indexes after rebalances
        if len(self.dest_cluster.index_nodes) > 0:
            for bucket in self.dest_cluster.buckets:
                if bucket.name[:4] == "test":
                    replica = len(self.dest_cluster.index_nodes) >= 2
                    self.build_indexes(self.dest_cluster, bucket,
                                    "name", replica=replica)

        self.log.info("Final index count = {}".format(self.index_count))
        self.log.info("Final list of indexes = {}".format(self.indexes))

        # Running queries using the indexes
        if len(self.dest_cluster.query_nodes) > 0:
            query_client = RestConnection(self.dest_cluster.query_nodes[0])
            for index in self.indexes:
                idx_split = index.split("#")
                bucket_name = idx_split[0]
                query = "SELECT {0} from `{1}`.`{2}`.`{3}` USE INDEX(`{4}`) " \
                    "limit 1000".format(idx_split[3], bucket_name, idx_split[1],
                                        idx_split[2], index)
                self.log.info("Running query = {}".format(query))
                result = query_client.query_tool(query)
                self.log.info("Status = {}".format(result["status"]))