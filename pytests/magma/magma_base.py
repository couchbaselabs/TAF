import json
import os

from BucketLib.bucket import Bucket
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from sdk_exceptions import SDKException


class MagmaBaseTest(BaseTestCase):
    def setUp(self):
        super(MagmaBaseTest, self).setUp()
        self.rest = RestConnection(self.cluster.master)
        self.doc_ops = self.input.param("doc_ops", "create")
        self.key_size = self.input.param("key_size", 0)
        self.replica_to_update = self.input.param("new_replica", None)
        self.key = 'test_docs'.rjust(self.key_size, '0')
        self.items = self.num_items
        self.check_temporary_failure_exception = False
        self.dgm_batch = self.input.param("dgm_batch", 5000)
        node_ram_ratio = self.bucket_util.base_bucket_ratio(self.cluster.servers)
        info = self.rest.get_nodes_self()
        self.rest.init_cluster(username=self.cluster.master.rest_username,
                               password=self.cluster.master.rest_password)
        self.rest.init_cluster_memoryQuota(memoryQuota=int(info.mcdMemoryReserved*node_ram_ratio))
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        if nodes_init:
            result = self.task.rebalance([self.cluster.master], nodes_init, [])
            self.assertTrue(result, "Initial rebalance failed")
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.check_replica = self.input.param("check_replica", False)
        self.bucket_storage = self.input.param("bucket_storage",
                                               Bucket.StorageBackend.magma)
        self.bucket_eviction_policy = self.input.param(
            "bucket_eviction_policy",
            Bucket.EvictionPolicy.FULL_EVICTION)
        self.bucket_util.add_rbac_user()
        if self.standard_buckets > 10:
            self.bucket_util.change_max_buckets(self.standard_buckets)
        if self.standard_buckets == 1:
            self._create_default_bucket()
        else:
            self._create_multiple_buckets()
        self.disable_magma_commit_points = self.input.param("disable_magma_commit_points", False)
        if self.disable_magma_commit_points:
            self.bucket_util.update_bucket_props("backend", "magma;magma_max_commit_points=0",
                                                 self.bucket_util.buckets)
        self.gen_create = doc_generator(self.key, 0, self.num_items,
                                        doc_size=self.doc_size,
                                        doc_type=self.doc_type,
                                        target_vbucket=self.target_vbucket,
                                        vbuckets=self.cluster_util.vbuckets)
        if self.active_resident_threshold < 100:
            self.check_temporary_failure_exception = True
        self.result_task = self._load_all_buckets(self.cluster, self.gen_create,
                                                  "create", 0, batch_size=self.batch_size,
                                                  dgm_batch=self.dgm_batch )
        if self.active_resident_threshold == 100:
            self.log.info("Verifying num_items counts after doc_ops")
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
        else:
            for task in self.result_task.keys():
                num_items = task.doc_index;
                self.log.info("Verifying num_items counts after doc_ops")
                self.bucket_util._wait_for_stats_all_buckets()
                self.bucket_util.verify_stats_all_buckets(num_item

        # Initialize doc_generators
        self.active_resident_threshold = 100
        self.gen_create = None
        self.gen_delete = None
        self.gen_update = doc_generator(self.key, 0, self.num_items / 2,
                                        doc_size=self.doc_size,
                                        doc_type=self.doc_type,
                                        target_vbucket=self.target_vbucket,
                                        vbuckets=self.cluster_util.vbuckets)
        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()
        self.log.info("==========Finished rebalance base setup========")

    def _create_default_bucket(self):
        node_ram_ratio = self.bucket_util.base_bucket_ratio(self.servers)
        info = RestConnection(self.cluster.master).get_nodes_self()
        available_ram = int(info.memoryQuota * node_ram_ratio)
        if available_ram < 100:
            available_ram = 100
        self.bucket_util.create_default_bucket(
            ram_quota=available_ram,
            bucket_type=self.bucket_type,
            replica=self.num_replicas,
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy)

    def _create_multiple_buckets(self):
        buckets_created = self.bucket_util.create_multiple_buckets(
            self.cluster.master,
            self.num_replicas,
            bucket_count=self.standard_buckets,
            bucket_type=self.bucket_type,
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy)
        self.assertTrue(buckets_created, "Unable to create multiple buckets")

        for bucket in self.bucket_util.buckets:
            ready = self.bucket_util.wait_for_memcached(
                self.cluster.master,
                bucket)
            self.assertTrue(ready, msg="Wait_for_memcached failed")

    def tearDown(self):
        self.cluster_util.print_cluster_stats()
        super(MagmaBaseTest, self).tearDown()

    def _load_all_buckets(self, cluster, kv_gen, op_type, exp, flag=0,
                          only_store_hash=True, batch_size=1000, pause_secs=1,
                          timeout_secs=30, compression=True, dgm_batch=5000):

        tasks_info = self.bucket_util.sync_load_all_buckets(
            cluster, kv_gen, op_type, exp, flag,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level, timeout_secs=timeout_secs,
            only_store_hash=only_store_hash, batch_size=batch_size,
            pause_secs=pause_secs, sdk_compression=compression,
            process_concurrency=8,
            active_resident_threshold=self.active_resident_threshold,
            dgm_batch=dgm_batch)
        if self.active_resident_threshold < 100:
            for task, _ in tasks_info.items():
                self.num_items = task.doc_index
        self.assertTrue(self.bucket_util.doc_ops_tasks_status(tasks_info),
                        "Doc_ops failed in rebalance_base._load_all_buckets")
        return tasks_info

    def start_parallel_cruds(self,
                             retry_exceptions=[],
                             ignore_exceptions=[],
                             _sync=False):
        tasks_info = dict()
        if "update" in self.doc_ops and self.gen_update is not None:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_update, "update", 0, batch_size=20,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)
            tasks_info.update(tem_tasks_info.items())
        if "create" in self.doc_ops and self.gen_create is not None:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_create, "create", 0, batch_size=20,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)
            tasks_info.update(tem_tasks_info.items())
            self.num_items += (self.gen_create.end - self.gen_create.start)
        if "delete" in self.doc_ops and self.gen_delete is not None:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_delete, "delete", 0, batch_size=20,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)
            tasks_info.update(tem_tasks_info.items())
            self.num_items -= (self.gen_delete.end - self.gen_delete.start)

        if _sync:
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

        return tasks_info

    def loadgen_docs(self,
                     retry_exceptions=[],
                     ignore_exceptions=[],
                     _sync=False):
        retry_exceptions = list(set(retry_exceptions +
                                    [SDKException.TimeoutException,
                                     SDKException.AmbiguousTimeoutException,
                                     SDKException.RequestCanceledException]))

        if self.check_temporary_failure_exception:
            retry_exceptions.append(SDKException.TemporaryFailureException)
        loaders = self.start_parallel_cruds(retry_exceptions,
                                            ignore_exceptions,
                                            _sync)
        return loaders
