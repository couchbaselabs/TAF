import math
import os
import random
import time

from Cb_constants.CBServer import CbServer
from cb_tools.cbstats import Cbstats
from remote.remote_util import RemoteMachineShellConnection
from storage.storage_base import StorageBase


class MagmaBaseTest(StorageBase):
    def setUp(self):
        super(MagmaBaseTest, self).setUp()

        # Update Magma/Storage Properties
        props = "magma"
        update_bucket_props = False

        self.disable_magma_commit_points = self.input.param(
            "disable_magma_commit_points", False)
        self.max_commit_points = self.input.param("max_commit_points", None)

        if self.disable_magma_commit_points:
            self.max_commit_points = 0

        if self.max_commit_points is not None:
            props += ";magma_max_checkpoints={}".format(self.max_commit_points)
            self.log.debug("props== {}".format(props))
            update_bucket_props = True

        if update_bucket_props:
            self.bucket_util.update_bucket_props(
                    "backend", props,
                    self.cluster.buckets)

        # Monitor Stats Params
        self.ep_queue_stats = self.input.param("ep_queue_stats", True)
        self.monitor_stats = ["doc_ops", "ep_queue_size"]
        if not self.ep_queue_stats:
            self.monitor_stats = ["doc_ops"]

        # Disk usage before data load
        self.empty_bucket_disk_usage = self.get_disk_usage(
            self.buckets[0], self.cluster.nodes_in_cluster)[0]
        self.log.info("Empty magma bucket disk usage: {}".format(
            self.empty_bucket_disk_usage))

        # self.thread_count is used to define number of thread use
        # to read same number of documents parallelly
        self.read_thread_count = self.input.param("read_thread_count", 4)
        self.disk_usage = dict()

        # Creating clients in SDK client pool
        self.sdk_timeout = self.input.param("sdk_timeout", 60)
        self.sdk_client_pool = True
        self.init_sdk_pool_object()
        self.log.info("Creating SDK clients for client_pool")
        max_clients = min(self.task_manager.number_of_threads, 20)
        clients_per_bucket = int(math.ceil(max_clients / self.standard_buckets))
        for bucket in self.cluster.buckets:
            self.sdk_client_pool.create_clients(
                bucket, [self.cluster.master],
                clients_per_bucket,
                compression_settings=self.sdk_compression)

        # Initial Data Load
        self.loader_dict = None
        self.init_loading = self.input.param("init_loading", True)
        if self.init_loading:
            if self.active_resident_threshold < 100:
                self.check_temporary_failure_exception = True
                self.create_start = 0
                self.create_end = self.init_items_per_collection
                self.generate_docs(doc_ops="create")
                self.load_buckets_in_dgm(self.gen_create, "create", 0)
            else:
                self.initial_load()
        self.log.info("==========Finished magma base setup========")

    def tearDown(self):
        super(MagmaBaseTest, self).tearDown()

    def validate_seq_itr(self):
        if self.dcp_services and self.num_collections == 1:
            index_build_q = "SELECT state FROM system:indexes WHERE name='{}';"
            start = time.time()
            result = False
            while start + 300 > time.time():
                result = self.query_client.query_tool(
                    index_build_q.format(self.initial_idx), timeout=60)
                if result["results"][0]["state"] == "online":
                    result = True
                    break
                self.sleep(5)
            self.assertTrue(result, "initial_idx Index warmup failed")
            self.final_idx = "final_idx"
            self.final_idx_q = "CREATE INDEX %s on default:`%s`.`%s`.`%s`(body) with \
                {\"defer_build\": false};" % (self.final_idx,
                                              self.buckets[0].name,
                                              CbServer.default_scope,
                                              self.collections[0])
            result = self.query_client.query_tool(self.final_idx_q, timeout=3600)
            start = time.time()
            if result["status"] != "success":
                while start + 300 > time.time():
                    result = self.query_client.query_tool(
                        index_build_q.format(self.final_idx), timeout=60)
                    if result["results"][0]["state"] == "online":
                        result = True
                        break
                    self.sleep(5)
                self.assertTrue(result, "final_idx Index warmup failed")
            else:
                self.assertTrue(result["status"] == "success", "Index query failed!")
            self.sleep(5)
            self.initial_count_q = "Select count(*) as items "\
                "from default:`{}`.`{}`.`{}` where meta().id like '%%';".format(
                    self.buckets[0].name, CbServer.default_scope, self.collections[0])
            self.final_count_q = "Select count(*) as items "\
                "from default:`{}`.`{}`.`{}` where body like '%%';".format(
                    self.buckets[0].name, CbServer.default_scope, self.collections[0])
            self.log.info(self.initial_count_q)
            self.log.info(self.final_count_q)
            initial_count, final_count = 0, 0
            kv_items = self.bucket_util.get_bucket_current_item_count(
                self.cluster, self.buckets[0])
            start = time.time()
            while start + 300 > time.time():
                kv_items = self.bucket_util.get_bucket_current_item_count(
                    self.cluster, self.buckets[0])
                self.log.info("Items in KV: %s" % kv_items)
                initial_count = self.query_client.query_tool(
                    self.initial_count_q)["results"][0]["items"]

                self.log.info("## Initial Index item count in %s:%s:%s == %s"
                              % (self.buckets[0].name,
                                 CbServer.default_scope, self.collections[0],
                                 initial_count))

                final_count = self.query_client.query_tool(self.final_count_q)["results"][0]["items"]
                self.log.info("## Final Index item count in %s:%s:%s == %s"
                              % (self.buckets[0].name,
                                 CbServer.default_scope, self.collections[0],
                                 final_count))

                if initial_count != kv_items or final_count != kv_items:
                    self.sleep(5)
                    continue
                break
#             self.assertTrue(initial_count == kv_items,
#                             "Indexer failed. KV:{}, Initial:{}".
#                             format(kv_items, initial_count))
#             self.assertTrue(final_count == kv_items,
#                             "Indexer failed. KV:{}, Final:{}".
#                             format(kv_items, final_count))

    def get_magma_stats(self, bucket, servers=None, field_to_grep=None):
        magma_stats_for_all_servers = dict()
        servers = servers or self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            result = dict()
            shell = RemoteMachineShellConnection(server)
            cbstat_obj = Cbstats(shell)
            result = cbstat_obj.magma_stats(bucket.name,
                                            field_to_grep=field_to_grep)
            shell.disconnect()
            magma_stats_for_all_servers[server.ip] = result
        return magma_stats_for_all_servers

    def get_disk_usage(self, bucket, servers=None):
        disk_usage = []
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        kvstore = 0
        wal = 0
        keyTree = 0
        seqTree = 0
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            kvstore += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(self.data_path,
                             bucket.name, "magma.*/kv*"))[0][0].split('\n')[0])
            wal += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(self.data_path,
                             bucket.name, "magma.*/wal"))[0][0].split('\n')[0])
            keyTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(self.data_path,
                             bucket.name, "magma.*/kv*/rev*/key*"))[0][0].split('\n')[0])
            seqTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(self.data_path,
                             bucket.name, "magma.*/kv*/rev*/seq*"))[0][0].split('\n')[0])
            shell.disconnect()
        self.log.info("Disk usage stats for bucekt {} is below".format(bucket.name))
        self.log.info("Total Disk usage for kvstore is {}MB".format(kvstore))
        self.get_bucket_dgm(bucket)
        self.log.debug("Total Disk usage for wal is {}MB".format(wal))
        self.log.debug("Total Disk usage for keyTree is {}MB".format(keyTree))
        self.log.debug("Total Disk usage for seqTree is {}MB".format(seqTree))
        disk_usage.extend([kvstore, wal, keyTree, seqTree])
        return disk_usage

    def check_fragmentation_using_magma_stats(self, bucket, servers=None):
        result = dict()
        time_end = time.time() + 60 * 5
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        while time.time() < time_end:
            stats = list()
            for server in servers:
                fragmentation_values = list()
                shell = RemoteMachineShellConnection(server)
                output = shell.execute_command(
                    "lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'"
                    )[0][0].split('\n')[0]
                self.log.debug("machine: {} - core(s): {}\
                ".format(server.ip, output))
                for i in range(min(int(output), 64)):
                    grep_field = "rw_{}:magma".format(i)
                    _res = self.get_magma_stats(
                        bucket, [server],
                        field_to_grep=grep_field)
                    fragmentation_values.append(
                        float(_res[server.ip][grep_field][
                            "Fragmentation"]))
                    stats.append(_res)
                result.update({server.ip: fragmentation_values})
            res = list()
            for value in result.values():
                res.append(max(value))
            if (max(res)) <= 1.1 * (float(self.fragmentation)/100):
                self.log.info("magma stats fragmentation result {} \
                ".format(result))
                self.log.debug(stats)
                return True
        self.log.info("magma stats fragmentation result {} \
        ".format(result))
        self.log.info(stats)
        return False

    def get_fragmentation_upsert_docs_list(self):
        """
         This function gives the list of "number of docs" need
         to be updated to touch the given fragmentation value
        """
        update_doc_count = int(math.ceil(float(
                    self.fragmentation * self.num_items) / (
                        100 - self.fragmentation)))

        upsert_doc_list = list()
        while update_doc_count > self.num_items:
            upsert_doc_list.append(self.num_items)
            update_doc_count -= self.num_items
        if update_doc_count > 0:
            upsert_doc_list.append(update_doc_count)
        self.log.info("Upsert list {}".format(upsert_doc_list))
        return upsert_doc_list

    def get_state_files(self, bucket, server=None):

        if server is None:
            server = self.cluster_util.cluster.master

        shell = RemoteMachineShellConnection(server)

        magma_path = os.path.join(self.data_path,
                                  bucket.name, "magma.0")
        kv_path = shell.execute_command("ls %s | grep kv | head -1" %
                                        magma_path)[0][0].split('\n')[0]
        path = os.path.join(magma_path, kv_path, "rev*/seqIndex")
        self.log.debug("SeqIndex path = {}".format(path))

        output = shell.execute_command("ls %s | grep state" % path)[0]
        self.log.debug("State files = {}".format(output))
        shell.disconnect()

        return output

    def get_random_keyIndex(self):
        shell = RemoteMachineShellConnection(self.cluster.master)
        keyIndex, _ = shell.execute_command("find {} -name keyIndex".format(self.data_path))
        shell.disconnect()
        return random.choice(keyIndex)

    def get_random_seqIndex(self):
        shell = RemoteMachineShellConnection(self.cluster.master)
        seqIndex, _ = shell.execute_command("find {} -name seqIndex".format(self.data_path))
        shell.disconnect()
        return random.choice(seqIndex)

    def get_random_wal(self):
        shell = RemoteMachineShellConnection(self.cluster.master)
        keyIndex, _ = shell.execute_command("find {} -name wal".format(self.data_path))
        shell.disconnect()
        return random.choice(keyIndex)

    def get_random_kvstore(self):
        shell = RemoteMachineShellConnection(self.cluster.master)
        keyIndex, _ = shell.execute_command("find {} -name kvstore-*".format(self.data_path))
        shell.disconnect()
        return random.choice(keyIndex)

    def get_tombstone_count_key(self, servers=[]):
        result = 0
        result_str = ""
        for server in servers:
            bucket = self.cluster.buckets[0]
            magma_path = os.path.join(self.data_path, bucket.name, "magma.{}")

            shell = RemoteMachineShellConnection(server)
            shards = shell.execute_command(
                        "lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'"
                        )[0][0].split('\n')[0]
            self.log.debug("machine: {} - core(s): {}".format(server.ip, shards))
            for shard in range(min(int(shards), 64)):
                magma = magma_path.format(shard)
                kvstores, _ = shell.execute_command("ls {} | grep kvstore".format(magma))
                cmd = '/opt/couchbase/bin/magma_dump {}'.format(magma)
                for kvstore in kvstores:
                    dump = cmd
                    kvstore_num = kvstore.split("-")[1].strip()
                    dump += ' --kvstore {} --tree key --treedata | grep Key |grep \'"deleted":true\' | wc -l'.format(kvstore_num)
                    ts_count = shell.execute_command(dump)[0][0].strip()
                    self.log.info("kvstore_num=={}, ts_count=={}".format(kvstore_num, ts_count))
                    result_str += str(ts_count) + "+"
                    result += int(ts_count)
        self.log.info("result_str is {}".format(result_str))
        return result

    def get_tombstone_count_seq(self, server=None, shard=0, kvstore=0):
        cmd = '/opt/couchbase/bin/magma_dump /data/kv/default/magma.{}/ \
        --kvstore {} --tree key --treedata | grep Seq| wc -l'.format(shard,
                                                                     kvstore)
        shell = RemoteMachineShellConnection(server)
        result = shell.execute_command(cmd)[0]
        return result

    def get_level_data_range(self, server=None, tree="key", shard=0, kvstore=0):
        cmd = '/opt/couchbase/bin/magma_dump /data/kv/default/magma.{}/ \
        --kvstore {} --tree {}'.format(shard, kvstore, tree)
        shell = RemoteMachineShellConnection(server)
        result = shell.execute_command(cmd)[0]
        return result
