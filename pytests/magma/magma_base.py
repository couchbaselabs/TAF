import math
import os
import random

from BucketLib.bucket import Bucket
from Cb_constants.CBServer import CbServer
from basetestcase import BaseTestCase
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from BucketLib.BucketOperations import BucketHelper
import time


class MagmaBaseTest(BaseTestCase):
    def setUp(self):
        super(MagmaBaseTest, self).setUp()
        self.rest = RestConnection(self.cluster.master)
        self.bucket_ram_quota = self.input.param("bucket_ram_quota", None)
        self.check_temporary_failure_exception = False
        self.retry_exceptions = [SDKException.TimeoutException,
                                 SDKException.AmbiguousTimeoutException,
                                 SDKException.RequestCanceledException,
                                 SDKException.UnambiguousTimeoutException]
        self.ignore_exceptions = []

        # Create Cluster
        self.rest.init_cluster(username=self.cluster.master.rest_username,
                               password=self.cluster.master.rest_password)

        nodes_init = self.cluster.servers[1:self.nodes_init]
        self.services = ["kv"]*(self.nodes_init)

        self.dcp_services = self.input.param("dcp_services", None)
        self.dcp_servers = []
        if self.dcp_services:
            server = self.rest.get_nodes_self()
            self.rest.set_service_memoryQuota(
                service='indexMemoryQuota',
                memoryQuota=int(server.mcdMemoryReserved - 100))
            self.dcp_services = [service.replace(":", ",") for service in self.dcp_services.split("-")]
            self.services.extend(self.dcp_services)
            self.dcp_servers = self.cluster.servers[self.nodes_init:
                                                    self.nodes_init+len(self.dcp_services)]
        nodes_in = nodes_init + self.dcp_servers
        result = self.task.rebalance([self.cluster.master],
                                     nodes_in,
                                     [],
                                     services=self.services[1:])
        self.assertTrue(result, "Initial rebalance failed")
        self.cluster.nodes_in_cluster.extend(
            [self.cluster.master] + nodes_in)
        for idx, node in enumerate(self.cluster.nodes_in_cluster):
            node.services = self.services[idx]
        # Create Buckets
        self.bucket_storage = self.input.param("bucket_storage",
                                               Bucket.StorageBackend.magma)
        self.bucket_eviction_policy = self.input.param("bucket_eviction_policy",
                                                       Bucket.EvictionPolicy.FULL_EVICTION)
        self.bucket_util.add_rbac_user()
        self.bucket_name = self.input.param("bucket_name",
                                               None)

        self.magma_buckets = self.input.param("magma_buckets", 0)
        if self.standard_buckets > 10:
            self.bucket_util.change_max_buckets(self.standard_buckets)
        if self.standard_buckets == 1:
            self._create_default_bucket()
        else:
            self._create_multiple_buckets()

        self.buckets = self.bucket_util.buckets

        # sel.num_collections=1 signifies only default collection
        self.num_collections = self.input.param("num_collections", 1)
        self.num_scopes = self.input.param("num_scopes", 1)

        self.scope_name = CbServer.default_scope
        # Creation of scopes of num_scopes is > 1
        scope_prefix = "Scope"
        for bucket in self.bucket_util.buckets:
            for i in range(1, self.num_scopes):
                scope_name = scope_prefix + str(i)
                self.log.info("Creating bucket::scope {} {}\
                ".format(bucket.name, scope_name))
                self.bucket_util.create_scope(self.cluster.master,
                                          bucket,
                                          {"name": scope_name})
                self.sleep(2)
        self.scopes = self.buckets[0].scopes.keys()
        self.log.info("Scopes list is {}".format(self.scopes))

        collection_prefix = "FunctionCollection"
        # Creation of collection of num_collections is > 1
        for bucket in self.bucket_util.buckets:
            for scope_name in self.scopes:
                for i in range(1, self.num_collections):
                    collection_name = collection_prefix + str(i)
                    self.log.info("Creating scope::collection {} {}\
                    ".format(scope_name, collection_name))
                    self.bucket_util.create_collection(
                        self.cluster.master, bucket,
                        scope_name, {"name": collection_name})
                    self.sleep(2)
        self.collections = self.buckets[0].scopes[self.scope_name].collections.keys()
        self.log.debug("Collections list == {}".format(self.collections))

        if self.dcp_services and self.num_collections == 1:
            self.initial_idx = "initial_idx"
            self.initial_idx_q = "CREATE INDEX %s on default:`%s`.`%s`.`%s`(meta().id) with \
                {\"defer_build\": false};" % (self.initial_idx,
                                              self.buckets[0].name,
                                              self.scope_name,
                                              self.collections[0])
            self.query_client = RestConnection(self.dcp_servers[0])
            result = self.query_client.query_tool(self.initial_idx_q)
            self.assertTrue(result["status"] == "success", "Index query failed!")

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

        self.fragmentation = int(self.input.param("fragmentation", 50))
        if self.fragmentation != 50:
            props += ";magma_delete_frag_ratio=%s" % str(self.fragmentation/100.0)
            update_bucket_props = True

        if update_bucket_props:
            self.bucket_util.update_bucket_props(
                    "backend", props,
                    self.bucket_util.buckets)

        # Monitor Stats Params
        self.ep_queue_stats = self.input.param("ep_queue_stats", True)
        self.monitor_stats = ["doc_ops", "ep_queue_size"]
        if not self.ep_queue_stats:
            self.monitor_stats = ["doc_ops"]

        # Doc controlling params
        self.key = 'test_docs'
        if self.random_key:
            self.key = "random_keys"
        self.doc_ops = self.input.param("doc_ops", "create")
        self.key_size = self.input.param("key_size", 8)
        self.doc_size = self.input.param("doc_size", 2048)
        self.gen_create = None
        self.gen_delete = None
        self.gen_read = None
        self.gen_update = None
        self.create_perc = self.input.param("update_perc", 100)
        self.update_perc = self.input.param("update_perc", 0)
        self.delete_perc = self.input.param("delete_perc", 0)
        self.expiry_perc = self.input.param("expiry_perc", 0)
        self.start = 0
        self.end = 0
        self.create_start = None
        self.create_end = None
        self.update_start = None
        self.update_end = None
        self.delete_start = None
        self.delete_end = None
        self.read_start = None
        self.read_end = None
        self.expiry_start = None
        self.expiry_end = None
        self.mutate = 0
        self.init_items_per_collection = self.num_items

        # Common test params
        self.test_itr = self.input.param("test_itr", 4)
        self.update_itr = self.input.param("update_itr", 10)
        self.next_half = self.input.param("next_half", False)
        self.deep_copy = self.input.param("deep_copy", False)
        if self.active_resident_threshold < 100:
            self.check_temporary_failure_exception = True
        # self.thread_count is used to define number of thread use
        # to read same number of documents parallelly
        self.read_thread_count = self.input.param("read_thread_count", 4)
        self.disk_usage = dict()

        # Initial Data Load
        self.initial_load()
        self.log.info("==========Finished magma base setup========")

    def initial_load(self):
        self.create_start = 0
        self.create_end = self.init_items_per_collection
        if self.rev_write:
            self.create_start = -int(self.init_items_per_collection - 1)
            self.create_end = 1

        self.generate_docs(doc_ops="create")
        self.init_loading = self.input.param("init_loading", True)
        self.dgm_batch = self.input.param("dgm_batch", 5000)
        if self.init_loading:
            self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))

            tasks_info = dict()
            for collection in self.collections:
                self.generate_docs(doc_ops="create", target_vbucket=None)
                tem_tasks_info = self.loadgen_docs(
                    self.retry_exceptions,
                    self.ignore_exceptions,
                    scope=self.scope_name,
                    collection=collection,
                    _sync=False,
                    doc_ops="create")
                tasks_info.update(tem_tasks_info.items())
            for task in tasks_info:
                self.task_manager.get_task_result(task)
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            self.bucket_util._wait_for_stats_all_buckets(timeout=3600)
            if self.standard_buckets == 1 or self.standard_buckets == self.magma_buckets:
                for bucket in self.bucket_util.get_all_buckets():
                    disk_usage = self.get_disk_usage(
                        bucket, self.cluster.nodes_in_cluster)
                    self.disk_usage[bucket.name] = disk_usage[0]
                    self.log.info(
                        "For bucket {} disk usage after initial creation is {}MB\
                        ".format(bucket.name,
                                 self.disk_usage[bucket.name]))
            self.num_items = self.init_items_per_collection * self.num_collections
        self.read_start = 0
        self.read_end = self.init_items_per_collection

    def _create_default_bucket(self):
        self.bucket_util.create_default_bucket(
            bucket_type=self.bucket_type,
            ram_quota=self.bucket_ram_quota,
            replica=self.num_replicas,
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy)

    def _create_multiple_buckets(self):
        buckets_created = self.bucket_util.create_multiple_buckets(
            self.cluster.master,
            self.num_replicas,
            bucket_count=self.standard_buckets,
            bucket_type=self.bucket_type,
            storage={"couchstore": self.standard_buckets - self.magma_buckets,
                     "magma": self.magma_buckets},
            eviction_policy=self.bucket_eviction_policy,
            bucket_name=self.bucket_name)
        self.assertTrue(buckets_created, "Unable to create multiple buckets")

        for bucket in self.bucket_util.buckets:
            ready = self.bucket_util.wait_for_memcached(
                self.cluster.master,
                bucket)
            self.assertTrue(ready, msg="Wait_for_memcached failed")

    def tearDown(self):
        self.cluster_util.print_cluster_stats()
        dgm = None
        timeout = 65
        while dgm is None and timeout > 0:
            try:
                stats = BucketHelper(self.cluster.master).fetch_bucket_stats(
                    self.buckets[0].name)
                dgm = stats["op"]["samples"]["vb_active_resident_items_ratio"][
                    -1]
            except:
                self.log.debug("Fetching vb_active_resident_items_ratio(dgm) failed...retying")
                timeout -= 1
                time.sleep(1)
        self.log.info("## Active Resident Threshold of {0} is {1} ##".format(
            self.buckets[0].name, dgm))
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
                                              self.scope_name,
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
                    self.buckets[0].name, self.scope_name, self.collections[0])
            self.final_count_q = "Select count(*) as items "\
                "from default:`{}`.`{}`.`{}` where body like '%%';".format(
                    self.buckets[0].name, self.scope_name, self.collections[0])
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
                                 self.scope_name, self.collections[0],
                                 initial_count))

                final_count = self.query_client.query_tool(self.final_count_q)["results"][0]["items"]
                self.log.info("## Final Index item count in %s:%s:%s == %s"
                              % (self.buckets[0].name,
                                 self.scope_name, self.collections[0],
                                 final_count))

                if initial_count != kv_items or final_count != kv_items:
                    self.sleep(5)
                    continue
                break
            self.assertTrue(initial_count == kv_items,
                            "Indexer failed. KV:{}, Initial:{}".
                            format(kv_items, initial_count))
            self.assertTrue(final_count == kv_items,
                            "Indexer failed. KV:{}, Final:{}".
                            format(kv_items, final_count))

    def genrate_docs_basic(self, start, end, target_vbucket=None, mutate=0):
        return doc_generator(self.key, start, end,
                             doc_size=self.doc_size,
                             doc_type=self.doc_type,
                             target_vbucket=target_vbucket,
                             vbuckets=self.cluster_util.vbuckets,
                             key_size=self.key_size,
                             randomize_doc_size=self.randomize_doc_size,
                             randomize_value=self.randomize_value,
                             mix_key_size=self.mix_key_size,
                             mutate=mutate,
                             deep_copy=self.deep_copy)

    def generate_docs(self, doc_ops=None,
                      target_vbucket=None,
                      create_end=None, create_start=None,
                      create_mutate=0,
                      update_end=None, update_start=None,
                      update_mutate=0,
                      read_end=None, read_start=None,
                      read_mutate=0,
                      delete_end=None, delete_start=None,
                      expiry_end=None, expiry_start=None,
                      expiry_mutate=0):

        doc_ops = doc_ops or self.doc_ops

        if "update" in doc_ops:
            if update_start is not None:
                self.update_start = update_start
            if update_end is not None:
                self.update_end = update_end

            if self.update_start is None:
                self.update_start = self.start
            if self.update_end is None:
                self.update_end = self.end*self.update_perc/100

            self.mutate += 1
            self.gen_update = self.genrate_docs_basic(self.update_start,
                                                      self.update_end,
                                                      target_vbucket=target_vbucket,
                                                      mutate=self.mutate)
        if "delete" in doc_ops:
            if delete_start is not None:
                self.delete_start = delete_start
            if delete_end is not None:
                self.delete_end = delete_end

            if self.delete_start is None:
                self.delete_start = self.start
            if self.delete_end is None:
                self.delete_end = self.end*self.delete_perc/100

            self.gen_delete = self.genrate_docs_basic(self.delete_start,
                                                      self.delete_end,
                                                      target_vbucket=target_vbucket,
                                                      mutate=read_mutate)
        if "create" in doc_ops:
            if create_start is not None:
                self.create_start = create_start
            if self.create_start is None:
                self.create_start = self.end
            self.start = self.create_start

            if create_end is not None:
                self.create_end = create_end
            if self.create_end is None:
                self.create_end = self.start+self.num_items*self.create_perc/100
            self.end = self.create_end

            self.gen_create = self.genrate_docs_basic(self.create_start,
                                                      self.create_end,
                                                      target_vbucket=target_vbucket,
                                                      mutate=create_mutate)
        if "read" in doc_ops:
            if read_start is not None:
                self.read_start = read_start
            if read_end is not None:
                self.read_end = read_end

            self.gen_read = self.genrate_docs_basic(self.read_start,
                                                    self.read_end,
                                                    target_vbucket=target_vbucket,
                                                    mutate=read_mutate)
        if "expiry" in doc_ops:
            if expiry_start is not None:
                self.expiry_start = expiry_start
            elif self.expiry_start is None:
                self.expiry_start = self.start+(self.num_items *
                                                self.delete_perc)/100

            if expiry_end is not None:
                self.expiry_end = expiry_end
            elif self.expiry_end is None:
                self.expiry_end = self.start+self.num_items *\
                                  (self.delete_perc + self.expiry_perc)/100

            self.maxttl = self.input.param("maxttl", 10)
            self.gen_expiry = self.genrate_docs_basic(self.expiry_start,
                                                      self.expiry_end,
                                                      target_vbucket=target_vbucket,
                                                      mutate=expiry_mutate)

    def _load_all_buckets(self, cluster, kv_gen, op_type, exp, flag=0,
                          only_store_hash=True, batch_size=1000, pause_secs=1,
                          timeout_secs=30, compression=True, dgm_batch=5000,
                          skip_read_on_error=False,
                          suppress_error_table=False,
                          track_failures=True):

        retry_exceptions = self.retry_exceptions
        tasks_info = self.bucket_util.sync_load_all_buckets(
            cluster, kv_gen, op_type, exp, flag,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level, timeout_secs=timeout_secs,
            only_store_hash=only_store_hash, batch_size=batch_size,
            pause_secs=pause_secs, sdk_compression=compression,
            process_concurrency=self.process_concurrency,
            retry_exceptions=retry_exceptions,
            active_resident_threshold=self.active_resident_threshold,
            skip_read_on_error=skip_read_on_error,
            suppress_error_table=suppress_error_table,
            dgm_batch=dgm_batch,
            monitor_stats=self.monitor_stats,
            track_failures=track_failures)
        if self.active_resident_threshold < 100:
            for task, _ in tasks_info.items():
                self.num_items = task.doc_index
        self.assertTrue(self.bucket_util.doc_ops_tasks_status(tasks_info),
                        "Doc_ops failed in MagmaBase._load_all_buckets")
        return tasks_info

    def loadgen_docs(self,
                     retry_exceptions=[],
                     ignore_exceptions=[],
                     skip_read_on_error=False,
                     suppress_error_table=False,
                     scope=CbServer.default_scope,
                     collection=CbServer.default_collection,
                     _sync=True,
                     track_failures=True,
                     doc_ops=None):
        doc_ops = doc_ops or self.doc_ops

        tasks_info = dict()
        read_tasks_info = dict()
        read_task = False

        if self.check_temporary_failure_exception:
            retry_exceptions.append(SDKException.TemporaryFailureException)

        if "update" in doc_ops and self.gen_update is not None:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_update, "update", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures)
            tasks_info.update(tem_tasks_info.items())
        if "create" in doc_ops and self.gen_create is not None:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_create, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures)
            tasks_info.update(tem_tasks_info.items())
            self.num_items += (self.gen_create.end - self.gen_create.start)
        if "expiry" in doc_ops and self.gen_expiry is not None and self.maxttl:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_expiry, "update", self.maxttl,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures)
            tasks_info.update(tem_tasks_info.items())
            self.num_items -= (self.gen_expiry.end - self.gen_expiry.start)
        if "read" in doc_ops and self.gen_read is not None:
            read_tasks_info = self.bucket_util._async_validate_docs(
               self.cluster, self.gen_read, "read", 0,
               batch_size=self.batch_size,
               process_concurrency=self.process_concurrency,
               pause_secs=5, timeout_secs=self.sdk_timeout,
               retry_exceptions=retry_exceptions,
               ignore_exceptions=ignore_exceptions,
               scope=scope,
               collection=collection)
            read_task = True
        if "delete" in doc_ops and self.gen_delete is not None:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_delete, "delete", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures)
            tasks_info.update(tem_tasks_info.items())
            self.num_items -= (self.gen_delete.end - self.gen_delete.start)

        if _sync:
            for task in tasks_info:
                self.task_manager.get_task_result(task)

            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

        if read_task:
            # TODO: Need to converge read_tasks_info into tasks_info before
            #       itself to avoid confusions during _sync=False case
            tasks_info.update(read_tasks_info.items())
            if _sync:
                for task in read_tasks_info:
                    self.task_manager.get_task_result(task)

        return tasks_info

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
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/kv*"))[0][0].split('\n')[0])
            wal += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/wal"))[0][0].split('\n')[0])
            keyTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/kv*/rev*/key*"))[0][0].split('\n')[0])
            seqTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/kv*/rev*/seq*"))[0][0].split('\n')[0])
            shell.disconnect()
        self.log.info("Disk usage stats for bucekt {} is below".format(bucket.name))
        self.log.info("Total Disk usage for kvstore is {}MB".format(kvstore))
        self.log.debug("Total Disk usage for wal is {}MB".format(wal))
        self.log.debug("Total Disk usage for keyTree is {}MB".format(keyTree))
        self.log.debug("Total Disk usage for seqTree is {}MB".format(seqTree))
        disk_usage.extend([kvstore, wal, keyTree, seqTree])
        return disk_usage

    def change_swap_space(self, servers=None, disable=True):
        servers = servers or self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            if disable:
                _ = shell.execute_command("swapoff -a")
                self.sleep(5)
                output = shell.execute_command(
                    "free | tail -1 | awk '{print $2}'")[0][0].split('\n')[0]
                self.assertEqual(
                    int(output), 0,
                    msg="Failed to disable swap space on server {} having value {} \
                     ".format(server, output))
            else:
                _ = shell.execute_command("swapon -a")
                self.sleep(5)
                output = shell.execute_command(
                    "free | tail -1 | awk '{print $2}'")[0][0].split('\n')[0]
                self.assertNotEqual(
                    int(output), 0,
                    msg="Failed to enable swap space on server {} having value {} \
                    ".format(server, output))
        return

    def check_fragmentation_using_magma_stats(self, bucket, servers=None):
        result = dict()
        stats = list()
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            fragmentation_values = list()
            shell = RemoteMachineShellConnection(server)
            output = shell.execute_command(
                    "lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'"
                    )[0][0].split('\n')[0]
            self.log.debug("machine: {} - core(s): {}\
            ".format(server.ip, output))
            for i in range(int(output)):
                grep_field = "rw_{}:magma".format(i)
                _res = self.get_magma_stats(
                    bucket, [server],
                    field_to_grep=grep_field)
                fragmentation_values.append(
                    float(_res[server.ip][grep_field][
                        "Fragmentation"]))
                stats.append(_res)
            result.update({server.ip: fragmentation_values})
        self.log.info("magma stats fragmentation result {} \
        ".format(result))
        for value in result.values():
            if max(value) > self.fragmentation:
                self.log.info(stats)
                return False
        return True

    def check_fragmentation_using_bucket_stats(self, bucket, servers=None):
        # Disabling the check for time being
        return True
        result = dict()
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            frag_val = self.bucket_util.get_fragmentation_kv(
                bucket, server)
            self.log.debug("Current Fragmentation for node {} is {} \
            ".format(server.ip, frag_val))
            result.update({server.ip: frag_val})
        self.log.info("KV stats fragmentation values {}".format(result))
        for value in result.values():
            if value > self.fragmentation:
                return False
        return True

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

    def validate_data(self,  op_type, kv_gen, _sync=True):
        self.log.info("Validating Docs")
        validate_tasks_info = dict()
        for collection in self.collections:
            temp_tasks_info = self.bucket_util._async_validate_docs(
                self.cluster, kv_gen, op_type, 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                pause_secs=5, timeout_secs=self.sdk_timeout,
                scope=self.scope_name,
                collection=collection,
                retry_exceptions=self.retry_exceptions,
                ignore_exceptions=self.ignore_exceptions)
            validate_tasks_info.update(temp_tasks_info.items())
        if _sync:
            for task in validate_tasks_info:
                self.task_manager.get_task_result(task)
        else:
            return validate_tasks_info

    def sigkill_memcached(self, nodes=None, graceful=False):
        nodes = nodes or self.cluster.nodes_in_cluster
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            if graceful:
                shell.restart_couchbase()
            else:
                shell.kill_memcached()
            shell.disconnect()
        self.assertTrue(self.bucket_util._wait_warmup_completed(
            [self.cluster_util.cluster.master],
            self.bucket_util.buckets[0],
            wait_time=self.wait_timeout * 20))

    def crash(self, nodes=None, kill_itr=1, graceful=False,
              wait=True, force_collect=False):
        self.stop_crash = False
        count = kill_itr
        loop_itr = 0

        nodes = nodes or self.cluster.nodes_in_cluster

        connections = dict()
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            connections.update({node: shell})

        while not self.stop_crash:
            loop_itr += 1
            sleep = random.randint(30, 60)
            self.sleep(sleep,
                       "Iteration:{} waiting for {} sec to kill memcached on all nodes".
                       format(loop_itr, sleep))

            for node, shell in connections.items():
                if "kv" in node.services:
                    if graceful:
                        shell.restart_couchbase()
                    else:
                        while count > 0:
                            shell.kill_memcached()
                            self.sleep(3, "Sleep before killing memcached on same node again.")
                            count -= 1
                        count = kill_itr

            result = self.check_coredump_exist(self.cluster.nodes_in_cluster,
                                               force_collect=force_collect)
            if result:
                self.stop_crash = True
                self.task.jython_task_manager.abort_all_tasks()
                self.assertFalse(result, "CRASH | CRITICAL | WARN messages "
                                         "found in cb_logs")

            if wait:
                for node in nodes:
                    if "kv" in node.services:
                        result = self.bucket_util._wait_warmup_completed(
                                    [node],
                                    self.bucket_util.buckets[0],
                                    wait_time=self.wait_timeout * 5)
                        if not result:
                            self.stop_crash = True
                            self.task.jython_task_manager.abort_all_tasks()
                            self.assertFalse(result)

        for _, shell in connections.items():
            shell.disconnect()

    def get_state_files(self, bucket, server=None):

        if server is None:
            server = self.cluster_util.cluster.master

        shell = RemoteMachineShellConnection(server)

        magma_path = os.path.join(RestConnection(server).get_data_path(),
                                  bucket.name, "magma.0")
        kv_path = shell.execute_command("ls %s | grep kv | head -1" %
                                        magma_path)[0][0].split('\n')[0]
        path = os.path.join(magma_path, kv_path, "rev*/seqIndex")
        self.log.debug("SeqIndex path = {}".format(path))

        output = shell.execute_command("ls %s | grep state" % path)[0]
        self.log.debug("State files = {}".format(output))
        shell.disconnect()

        return output
