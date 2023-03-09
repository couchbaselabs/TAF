import math
import os
import random
import time
import threading

from Cb_constants.CBServer import CbServer
from cb_tools.cbstats import Cbstats
from remote.remote_util import RemoteMachineShellConnection
from storage.storage_base import StorageBase
from storage_utils.magma_utils import MagmaUtils


class MagmaBaseTest(StorageBase):
    def setUp(self):
        super(MagmaBaseTest, self).setUp()

        # Update Magma/Storage Properties
        props = "magma"
        update_bucket_props = False

        self.disable_magma_commit_points = self.input.param(
            "disable_magma_commit_points", False)
        self.max_commit_points = self.input.param("max_commit_points", None)
        self.check_bloom_filters = self.input.param("check_bloom_filters", False)


        if self.disable_magma_commit_points:
            self.max_commit_points = 0

        if self.max_commit_points is not None:
            props += ";magma_max_checkpoints={}".format(self.max_commit_points)
            self.log.debug("props== {}".format(props))
            update_bucket_props = True

        if update_bucket_props:
            self.bucket_util.update_bucket_props(
                    "backend", props,
                    self.cluster, self.cluster.buckets)

        self.magma_utils = MagmaUtils()

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
                if self.check_bloom_filters:
                    self.totalBloomFilterMemUsed = self.get_magma_params(self.buckets[0], self.cluster.nodes_in_cluster)
                    self.memoryQuota = self.get_magma_params(self.buckets[0], self.cluster.nodes_in_cluster, param="MemoryQuota")
                    self.memory_quota = max([max(val) for val in self.memoryQuota.values()])
                    self.log.info("magma_memory_quota :: {}".format(self.memory_quota))
            self.dgm_prcnt = self.get_bucket_dgm(self.buckets[0])
            self.log.info("DGM percentage after init loading is {}".format(self.dgm_prcnt))
        if self.standard_buckets == 1 or self.standard_buckets == self.magma_buckets:
            for bucket in self.bucket_util.get_all_buckets(self.cluster):
                disk_usage = self.get_disk_usage(
                    bucket, self.cluster.nodes_in_cluster)
                self.disk_usage[bucket.name] = disk_usage[0]
                self.log.info(
                    "For bucket {} disk usage after initial creation is {}MB\
                    ".format(bucket.name,
                             self.disk_usage[bucket.name]))

        self.log.info("==========Finished magma base setup========")

    def tearDown(self):
        super(MagmaBaseTest, self).tearDown()

    def compute_docs_ranges(self, start=None, doc_ops=None):
        self.multiplier = self.input.param("multiplier", 1)
        doc_ops = doc_ops or self.doc_ops
        ops_len = len(doc_ops.split(":"))
        if "read" in doc_ops:
            self.read_start = 0
            self.read_end = self.init_items_per_collection
            if ops_len > 1:
                ops_len -= 1

        if "create" in doc_ops:
            ops_len -= 1
            self.create_start = start or self.init_items_per_collection
            if start:
                self.create_end = start + start * self.multiplier
            else:
                self.create_end = self.init_items_per_collection + self.init_num_items * self.multiplier
            self.num_items_per_collection += (self.create_end - self.create_start)
        if ops_len == 1:
            self.update_start = 0
            self.update_end = self.init_num_items
            self.expiry_start = 0
            self.expiry_end = self.init_num_items * self.multiplier
            self.delete_start = 0
            self.delete_end = self.init_num_items
        elif ops_len == 2:
            self.update_start = 0
            self.update_end = self.init_num_items // 2
            self.delete_start = self.init_num_items // 2
            self.delete_end = self.init_num_items

            if "expiry" in doc_ops:
                self.delete_start = 0
                self.delete_end = self.init_num_items // 2
                self.expiry_start = self.init_num_items // 2
                self.expiry_end = self.init_num_items * self.multiplier
        elif ops_len == 3:
            self.update_start = 0
            self.update_end = self.init_num_items // 3
            self.delete_start = self.init_num_items // 3
            self.delete_end = (2 * self.init_num_items) // 3
            self.expiry_start = (2 * self.init_num_items) // 3
            self.expiry_end = self.init_num_items * self.multiplier

        self.read_start = self.update_start
        self.read_end = self.update_end

        if "delete" in doc_ops:
            self.num_items_per_collection -= (self.delete_end - self.delete_start)
        if "expiry" in doc_ops:
            self.num_items_per_collection -= (self.expiry_end - self.expiry_start)

    def reset_doc_params(self, doc_ops=None):
        self.create_perc = 0
        self.delete_perc = 0
        self.update_perc = 0
        self.expiry_perc = 0
        self.read_perc = 0
        self.create_end = 0
        self.create_start = 0
        self.update_end = 0
        self.update_start = 0
        self.delete_end = 0
        self.delete_start = 0
        self.expire_end = 0
        self.expire_start = 0
        doc_ops = doc_ops or self.doc_ops
        doc_ops = doc_ops.split(":")
        perc = 100//len(doc_ops)
        if "update" in doc_ops:
            self.update_perc = perc
        if "create" in doc_ops:
            self.create_perc = perc
        if "read" in doc_ops:
            self.read_perc = perc
        if "expiry" in doc_ops:
            self.expiry_perc = perc
            self.maxttl = random.randint(5, 10)
            self.bucket_util._expiry_pager(self.cluster, 10000000000)
        if "delete" in doc_ops:
            self.delete_perc = perc

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
        return self.magma_utils.get_disk_usage(self.cluster, bucket,
                                               self.data_path, servers)

    def check_fragmentation_using_magma_stats(self, bucket, servers=None):
        result = dict()
        frag_factor = 1.1
        if self.fragmentation == 10:
            frag_factor = 1.2
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
                if not self.windows_platform:
                    output = shell.execute_command(
                        "lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'"
                        )[0][0].split('\n')[0]
                else:
                    output = shell.execute_command(
                        "cat /proc/cpuinfo | grep 'processor' | tail -1 | awk '{print $3}'"
                        )[0][0].split('\n')[0]
                    output = str(int(output) + 1)
                self.log.debug("%s - core(s): %s" % (server.ip, output))
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
            self.log.info("frag_factor is {}".format(frag_factor))
            if (max(res)) <= frag_factor * (float(self.fragmentation)/100):
                self.log.info("magma stats fragmentation result %s" % result)
                self.log.debug(stats)
                return True
        self.log.info("magma stats fragmentation result %s" % result)
        self.log.info("Fragmentation value that exceeds the configured value is ==> {}".format(max(res)))
        self.log.info(stats)
        return False

    def get_magma_params(self, bucket, servers=None, param="TotalBloomFilterMemUsed"):
        result = dict()
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        stats = list()
        for server in servers:
            if "kv" not in server.services.lower():
                continue
            param_values = list()
            shell = RemoteMachineShellConnection(server)
            if not self.windows_platform:
                output = shell.execute_command(
                    "lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'"
                    )[0][0].split('\n')[0]
            else:
                output = shell.execute_command(
                    "cat /proc/cpuinfo | grep 'processor' | tail -1 | awk '{print $3}'"
                    )[0][0].split('\n')[0]
                output = str(int(output) + 1)
            self.log.debug("%s - core(s): %s" % (server.ip, output))
            for i in range(int(output)):
                grep_field = "rw_{}:magma".format(i)
                _res = self.get_magma_stats(
                    bucket, [server],
                    field_to_grep=grep_field)
                param_values.append(
                    float(_res[server.ip][grep_field][
                        param]))
                stats.append(_res)
            result.update({server.ip: param_values})
        self.log.info("{} for each shard is {}".format(param, result))
        return result

    def bloomfilters(self):
        self.stop_stats = False
        self.stats_failure = False
        self.iteration = 0
        while not self.stop_stats:
            self.iteration += 1
            self.log.info("Stats iteration {}".format(self.iteration))
            self.totalBloomFilterMemUsed = self.get_magma_params(self.buckets[0], self.cluster.nodes_in_cluster)
            max_bloom_mem_used = max([max(val) for val in self.totalBloomFilterMemUsed.values()])
            self.log.info("max_bloom_mem_used ::{}".format(max_bloom_mem_used))
            time_end = time.time() + 20
            while max_bloom_mem_used > self.memory_quota and time.time() < time_end:
                self.totalBloomFilterMemUsed = self.get_magma_params(self.buckets[0], self.cluster.nodes_in_cluster)
                max_bloom_mem_used = max([max(val) for val in self.totalBloomFilterMemUsed.values()])

            if max_bloom_mem_used > self.memory_quota:
                self.log.info("exceeded max_bloom_mem_used is {}".format(max_bloom_mem_used))
                self.stats_failure = True
                self.stop_stats = True
            self.log.debug("total bloom filter mem used ::{}".format(self.totalBloomFilterMemUsed))

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
            server = self.cluster.master

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
        total_tombstones = {'final_count' : 0}
        ts_per_node = dict()
        for server in servers:
            ts_per_node[server.ip] = 0
        threads = []
        lock = threading.Lock()
        def count_tombstones(node, lock):
            result = 0
            result_str = ""
            bucket = self.cluster.buckets[0]
            magma_path = os.path.join(self.data_path, bucket.name, "magma.{}")
            shell = RemoteMachineShellConnection(node)
            shards = shell.execute_command(
                "lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'"
                )[0][0].split('\n')[0]
            self.log.debug("machine: {} - core(s): {}".format(node.ip, shards))
            for shard in range(min(int(shards), 64)):
                magma = magma_path.format(shard)
                kvstores, _ = shell.execute_command("ls {} | grep kvstore".format(magma))
                cmd = '/opt/couchbase/bin/magma_dump {}'.format(magma)
                for kvstore in kvstores:
                    dump = cmd
                    kvstore_num = kvstore.split("-")[1].strip()
                    dump += ' --kvstore {} --tree key --treedata | grep Key |grep \'"deleted":true\' | wc -l'.format(kvstore_num)
                    ts_count = shell.execute_command(dump)[0][0].strip()
                    self.log.debug("kvstore_num=={}, ts_count=={}".format(kvstore_num, ts_count))
                    result_str += str(ts_count) + "+"
                    result += int(ts_count)
            self.log.info("node={} and result={}".format(node, result))
            lock.acquire()
            increment_result(result)
            lock.release()
            ts_per_node[node.ip] = result

        def increment_result(result):
            total_tombstones['final_count'] += result

        for server in servers:
            th = threading.Thread(
                target=count_tombstones, args=[server, lock])
            th.start()
            threads.append(th)
        for th in threads:
            th.join()

        self.log.info("total_tombstones {}".format(total_tombstones['final_count']))
        self.log.info(" TombStones per node {}".format(ts_per_node))
        return total_tombstones['final_count']

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

    def start_stop_history_retention_for_collections(self):
        if self.bucket_dedup_retention_seconds or self.bucket_dedup_retention_bytes:
            self.crash_during_enable_disable_history = self.input.param("crash_during_enable_disable_history", False)
            self.stop_enable_disable_history = False
            self.loop_itr = 0

            def induce_crash():
                kill_count = 5
                count = kill_count
                nodes = self.cluster.nodes_in_cluster
                connections = dict()
                for node in nodes:
                    shell = RemoteMachineShellConnection(node)
                    connections.update({node: shell})
                for node, shell in connections.items():
                    if "kv" in node.services:
                        if self.graceful:
                            while count > 0:
                                shell.restart_couchbase()
                                self.sleep(3, "Sleep before restarting memcached on same node again.")
                                count -= 1
                        else:
                            while count > 0:
                                shell.kill_memcached()
                                self.sleep(3, "Sleep before killing memcached on same node again.")
                                count -= 1
                        count = kill_count
                for _, shell in connections.items():
                    shell.disconnect()

            while not self.stop_enable_disable_history:
                self.loop_itr += 1
                sleep = random.randint(10, 20)
                self.sleep(sleep,
                           "Iteration:{} waiting for {} sec to disable history retention".
                           format(self.loop_itr, sleep))
                for bucket in self.cluster.buckets:
                    for scope_name in self.scopes:
                        for collection_name in self.collections:
                            if collection_name == "_default" and scope_name == "_default":
                                continue
                            self.bucket_util.set_history_retention_for_collection(self.cluster.master,
                                                                                  bucket, scope_name,
                                                                                  collection_name,
                                                                                  "false")
                if self.crash_during_enable_disable_history:
                    induce_crash()

                sleep = random.randint(5, 10)
                self.sleep(sleep,
                           "Iteration:{} waiting for {} sec to enable history retention".
                           format(self.loop_itr, sleep))
                for bucket in self.cluster.buckets:
                    for scope_name in self.scopes:
                        for collection_name in self.collections:
                            if collection_name == "_default" and scope_name == "_default":
                                continue
                            self.bucket_util.set_history_retention_for_collection(self.cluster.master,
                                                                                  bucket, scope_name,
                                                                                  collection_name,
                                                                                  "true")
                if self.crash_during_enable_disable_history:
                    induce_crash()

    def change_history_retention_values(self, buckets=None):
        buckets = buckets or self.cluster.buckets
        self.change_history = False
        loop_itr = 0
        if type(buckets) is not list:
            buckets = [buckets]
        self.change_history_size_values = self.input.param("change_history_size_values", True)
        self.change_history_time_values = self.input.param("change_history_time_values", False)
        self.change_both_history_values = self.input.param("change_both_history_values", False)
        history_size_values = self.input.param("history_size_values", "5000").split(":")
        history_time_values = self.input.param("histoy_time_values", "1000").split(":")
        while not self.change_history:
            loop_itr += 1
            for bucket in buckets:
                if self.change_history_size_values:
                    print("type {} and val {}".format(type(history_size_values), history_size_values ))
                    for value in history_size_values:
                        sleep = random.randint(90, 120)
                        self.sleep(sleep, "Iteration:{} waiting for {} sec before setting history size values".
                                   format(loop_itr, sleep))
                        self.bucket_util.update_bucket_property(
                            self.cluster.master, bucket,
                            history_retention_seconds=self.bucket_dedup_retention_seconds,
                            history_retention_bytes=value)
                if self.change_history_time_values:
                    for value in history_time_values:
                        sleep = random.randint(90, 120)
                        self.sleep(sleep, "Iteration:{} waiting for {} sec before setting history time values".
                                   format(loop_itr, sleep))
                        self.bucket_util.update_bucket_property(
                            self.cluster.master, bucket,
                            history_retention_seconds=value,
                            history_retention_bytes=self.bucket_dedup_retention_bytes)
                if self.change_both_history_values:
                    for size in history_size_values:
                        for time in history_time_values:
                            sleep = random.randint(60, 90)
                            self.sleep(sleep, "Iteration:{} waiting for {} sec before setting history time and size values".
                                   format(loop_itr, sleep))
                            self.bucket_util.update_bucket_property(
                                self.cluster.master, bucket,
                                history_retention_seconds=time,
                                history_retention_bytes=size)

    def get_seqnumber_count(self, bucket=None):
        result = dict()
        for node in self.cluster.nodes_in_cluster:
            result["_".join(node.ip.split("."))] = dict()
        bucket = bucket or self.cluster.buckets[0]
        magma_path = os.path.join(self.data_path, bucket.name, "magma.{}")
        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            shards = shell.execute_command(
                "lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'"
                )[0][0].split('\n')[0]
            self.log.debug("machine: {} - core(s): {}".format(node.ip, shards))
            for shard in range(min(int(shards), 64)):
                magma = magma_path.format(shard)
                kvstores, _ = shell.execute_command("ls {} | grep kvstore".format(magma))
                cmd = '/opt/couchbase/bin/magma_dump {}'.format(magma)
                for kvstore in kvstores:
                    dump = cmd
                    kvstore_num = kvstore.split("-")[1].strip()
                    #dump += ' --kvstore {} --tree seq --treedata | grep  bySeqno | wc -l'.format(kvstore_num)
                    dump += ' --kvstore {} --docs-by-seq --history | wc -l'.format(kvstore_num)
                    seqnumber_count = shell.execute_command(dump)[0][0].strip()
                    result["_".join(node.ip.split("."))][kvstore] = seqnumber_count
        self.log.info("seqnumber_count/kvstore {}".format(result))
        seqnumber_count = 0
        for node in self.cluster.nodes_in_cluster:
            for count in result["_".join(node.ip.split("."))].values():
                seqnumber_count += int(count)
        self.log.info("seqnumber_count {}".format(seqnumber_count))
        return seqnumber_count

    def get_history_start_seq_for_each_vb(self):
        seq_stats = dict()
        for bucket in self.cluster.buckets:
            seq_stats[bucket] = self.bucket_util.get_vb_details_for_bucket(bucket,
                                                                           self.cluster.nodes_in_cluster)
        return seq_stats
