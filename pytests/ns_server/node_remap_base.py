from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from cb_constants.CBServer import CbServer

class NodeRemapBase(BaseTestCase):

    def setUp(self):
        super(NodeRemapBase, self).setUp()

        if len(self.cluster.servers) < self.nodes_init * 2:
            self.fail("Not enough servers to perform cluster clone")

        self.rest = RestConnection(self.cluster.master)
        self.data_path = self.rest.get_data_path()
        node_info = self.rest.get_nodes_self()
        self.index_path = node_info.storage[0].get_index_path()

        self.bucket_ram_quota = self.input.param("bucket_ram_quota", None)
        self.bucket_util.add_rbac_user(self.cluster.master)
        self.magma_buckets = self.input.param("magma_buckets", 0)
        self.num_ephemeral_buckets = self.input.param("num_ephemeral_buckets", 0)
        self.bucket_eviction_policy = self.input.param("bucket_eviction_policy",
                                                Bucket.EvictionPolicy.FULL_EVICTION)
        self.num_collections = self.input.param("num_collections", 1)
        self.num_scopes = self.input.param("num_scopes", 1)
        self.collection_items = self.input.param("collection_items", 100000)
        self.output_dir = self.input.param("output_dir", "/output_dir")
        self.sdk_timeout = self.input.param("sdk_timeout", 60)
        self.couchbase_folder = "/opt/couchbase/var/lib/couchbase"
        self.set_history_at_start = self.input.param("set_history_at_start", True)
        self.history_load = self.input.param("history_load", False)
        self.alternate_data_idx_path = self.input.param("alternate_data_idx_path", False)
        self.large_docs = self.input.param("large_docs", False)
        self.alternate_address = self.input.param("alternate_address", False)
        self.alternate_ip = self.input.param("alternate_ip", "10.142.181.104")
        self.use_config_remap = self.input.param("use_config_remap", False)
        self.regenerate_bucket_uuid = self.input.param("regenerate_bucket_uuid", True)

        nodes_in = self.cluster.servers[1:self.nodes_init]
        self.dest_servers = self.cluster.servers[self.nodes_init:self.nodes_init*2]

        if self.services_init:
            self.services = self.cluster_util.get_services(
                [self.cluster.master], self.services_init, 0)

        self.kv_quota_mem = self.input.param("kv_quota_mem", 5000)
        self.index_quota_mem = self.input.param("index_quota_mem", 1024)
        self.fts_quota_mem = self.input.param("fts_quota_mem", 1024)
        self.cbas_quota_mem = self.input.param("cbas_quota_mem", 1024)
        self.eventing_quota_mem = self.input.param("eventing_quota_mem", 256)

        mem_quota_dict = {CbServer.Settings.KV_MEM_QUOTA: self.kv_quota_mem,
                CbServer.Settings.INDEX_MEM_QUOTA: self.index_quota_mem,
                CbServer.Settings.FTS_MEM_QUOTA: self.fts_quota_mem,
                CbServer.Settings.CBAS_MEM_QUOTA: self.cbas_quota_mem,
                CbServer.Settings.EVENTING_MEM_QUOTA: self.eventing_quota_mem}
        self.rest.set_service_mem_quota(mem_quota_dict)

        self.init_rebalance_skip = self.input.param("init_rebalance_skip", False)
        if not self.init_rebalance_skip:
            result = self.task.rebalance(self.cluster, nodes_in, [],
                                         services=self.services[1:])
            self.assertTrue(result, "Initial rebalance failed")
        for idx, node in enumerate(self.cluster.nodes_in_cluster):
            node.services = self.services[idx]

        if self.alternate_address:
            status = RestConnection(self.cluster.master).\
                set_alternate_addresses(self.alternate_ip)
            if status:
                self.log.info("Alternate IP set {} => {}".format(self.cluster.master.ip,
                                                                 self.alternate_ip))

        bucket_helper = BucketHelper(self.cluster.master)
        bucket_helper.update_memcached_settings(
            num_writer_threads="disk_io_optimized",
            num_reader_threads="disk_io_optimized",
            num_storage_threads="default")

        self.log.info("Creating {} buckets".format(self.standard_buckets))
        self.bucket_util.create_multiple_buckets(
            self.cluster, self.num_replicas,
            bucket_count=self.standard_buckets,
            bucket_type=self.bucket_type,
            eviction_policy=self.bucket_eviction_policy,
            storage={"couchstore": self.standard_buckets - self.magma_buckets,
                     "magma": self.magma_buckets},
            ram_quota=self.bucket_ram_quota)

        for i in range(self.num_ephemeral_buckets):
            bucket_name = "ephemeral" + str(i)
            self.log.info("Creating ephemeral bucket: {}".format(bucket_name))
            self.bucket_util.create_default_bucket(self.cluster, "ephemeral",
                ram_quota=self.bucket_ram_quota, replica=self.num_replicas,
                eviction_policy=self.bucket_eviction_policy,
                bucket_name=bucket_name)

        self.create_bucket_scope_collections()

        if self.set_history_at_start and \
            (self.bucket_dedup_retention_seconds is not None or \
             self.bucket_dedup_retention_bytes is not None):
            for bucket in self.cluster.buckets:
                if bucket.storageBackend == Bucket.StorageBackend.magma:
                    self.bucket_util.update_bucket_property(
                    self.cluster.master, bucket,
                    history_retention_bytes=self.bucket_dedup_retention_bytes,
                    history_retention_seconds=self.bucket_dedup_retention_seconds)

        self.bucket_util.print_bucket_stats(self.cluster)
        self.log.info("NodeRemapBase setup finished")

    def create_bucket_scope_collections(self):
        scope_prefix = "Scope"
        for bucket in self.cluster.buckets:
            for i in range(1, self.num_scopes):
                scope_name = scope_prefix + str(i)
                self.log.info("Creating bucket::scope {} {}\
                    ".format(bucket.name, scope_name))
                self.bucket_util.create_scope(self.cluster.master, bucket,
                                              {"name": scope_name})
                self.sleep(2)
        self.scopes = self.cluster.buckets[0].scopes.keys()
        self.log.info("Scopes list is {}".format(self.scopes))

        collection_prefix = "FunctionCollection"
        for bucket in self.cluster.buckets:
            for scope_name in self.scopes:
                if scope_name == CbServer.system_scope:
                    continue
                for i in range(len(bucket.scopes[scope_name].collections),
                               self.num_collections):
                    collection_name = collection_prefix + str(i)
                    self.log.info("Creating scope::collection {} {}\
                    ".format(scope_name, collection_name))
                    self.bucket_util.create_collection(
                        self.cluster.master, bucket,
                        scope_name, {"name": collection_name})
                    self.sleep(2)

        self.collections = \
            self.cluster.buckets[0].scopes[CbServer.default_scope].collections.keys()
        self.log.info("Collections list == {}".format(self.collections))


    def copy_data_to_dest_node(self, source_node, dest_node, output_dir):

        cmd = 'sshpass -p "{3}" scp -o StrictHostKeyChecking=no ' \
              '-r root@{0}:{1} {2}'.format(
              source_node.ip, self.couchbase_folder, output_dir,
              source_node.ssh_password)
        shell = RemoteMachineShellConnection(dest_node)

        # clear out the directory on dest node
        shell.execute_command("rm -rf {}".format(output_dir))

        o, e = shell.execute_command(cmd)
        shell.log_command_output(o, e)

        if self.alternate_data_idx_path:
            cmd = 'sshpass -p "{2}" scp -o StrictHostKeyChecking=no' \
                  ' -r root@{0}:{1}/* {1}'.format(source_node.ip, "/data",
                                                  source_node.ssh_password)
            o, e = shell.execute_command(cmd)

    def build_config_remap_script_cmd(self, output_dir):

        remap_args = ""
        for node_idx in range(self.nodes_init):
            source_node = self.cluster.nodes_in_cluster[node_idx]
            dest_node = self.dest_servers[node_idx]
            remap_args += " --remap ns_1@{0} ns_1@{1}".format(
                        source_node.ip, dest_node.ip)
            self.log.info("{} => {}".format(source_node.ip, dest_node.ip))

        if self.alternate_address:
            new_alternate_ip = ".".join(self.alternate_ip.split(".")[:3]) + \
                "." + str(int(self.alternate_ip.split(".")[-1]) + 1)
            remap_args += " --remap {} {}".format(self.alternate_ip, new_alternate_ip)

        if not self.use_config_remap:
            script_cmd = "/opt/couchbase/bin/node_remap --initargs " \
                        "/opt/couchbase/var/lib/couchbase/initargs" \
                        " --output-path {0}{1}".format(output_dir, remap_args)

        else:
            script_cmd = "/opt/couchbase/bin/escript /opt/couchbase/bin/escript-wrapper" \
                        " --initargs-path /opt/couchbase/var/lib/couchbase/initargs -- " \
                        "/opt/couchbase/bin/config_remap --initargs-path " \
                        "/opt/couchbase/var/lib/couchbase/initargs --output-path {0}{1}" \
                        " --regenerate-cookie --regenerate-cluster-uuid" \
                        .format(output_dir, remap_args)
            if self.regenerate_bucket_uuid:
                script_cmd += " --regenerate-bucket-uuids"

        return script_cmd
