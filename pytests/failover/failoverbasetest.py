# -*- coding: utf-8 -*-
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from TestInput import TestInputSingleton
from basetestcase import BaseTestCase
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import doc_generator
from sdk_exceptions import SDKException
from shell_util.remote_connection import RemoteMachineShellConnection

retry_exceptions = SDKException.TimeoutException \
    + SDKException.RequestCanceledException \
    + SDKException.DurabilityAmbiguousException \
    + SDKException.DurabilityImpossibleException


class FailoverBaseTest(BaseTestCase):
    def setUp(self):
        self._cleanup_nodes = list()
        self._failed_nodes = list()
        super(FailoverBaseTest, self).setUp()
        default_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view_name = "default_view"
        self.default_view = View(self.default_view_name, default_map_func,
                                 None)
        self.failoverMaster = self.input.param("failoverMaster", False)
        self.total_vbuckets = self.input.param("total_vbuckets", 1024)
        self.compact = self.input.param("compact", False)
        self.std_vbucket_dist = self.input.param("std_vbucket_dist", 20)
        self.withMutationOps = self.input.param("withMutationOps", False)
        self.withViewsOps = self.input.param("withViewsOps", False)
        self.createIndexesDuringFailover = \
            self.input.param("createIndexesDuringFailover", False)
        self.upr_check = self.input.param("upr_check", True)
        self.withQueries = self.input.param("withQueries", False)
        self.numberViews = self.input.param("numberViews", False)
        self.gracefulFailoverFail = self.input.param("gracefulFailoverFail",
                                                     False)
        self.runRebalanceAfterFailover = \
            self.input.param("runRebalanceAfterFailover", True)
        self.failoverMaster = self.input.param("failoverMaster", False)
        self.check_verify_failover_type = \
            self.input.param("check_verify_failover_type", True)
        self.recoveryType = self.input.param("recoveryType", "delta")
        self.bidirectional = self.input.param("bidirectional", False)
        self.stopGracefulFailover = self.input.param("stopGracefulFailover",
                                                     False)
        self._value_size = self.input.param("value_size", 256)
        self.victim_type = self.input.param("victim_type", "other")
        self.victim_count = self.input.param("victim_count", 1)
        self.stopNodes = self.input.param("stopNodes", False)
        self.killNodes = self.input.param("killNodes", False)
        self.doc_ops = self.input.param("doc_ops", [])
        self.firewallOnNodes = self.input.param("firewallOnNodes", False)
        self.deltaRecoveryBuckets = self.input.param("deltaRecoveryBuckets",
                                                     None)
        self.max_verify = self.input.param("max_verify", None)
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(":")
        self.num_failed_nodes = self.input.param("num_failed_nodes", 0)
        self.target_vbucket_type = self.input.param("target_vbucket_type",
                                                    "active")
        self.dgm_run = self.input.param("dgm_run", True)
        self.add_back_flag = False
        self.during_ops = self.input.param("during_ops", None)
        self.graceful = self.input.param("graceful", True)
        self.failover_onebyone = self.input.param("failover_onebyone", False)
        self.new_replica = self.input.param("new_replica", None)
        self.test_abort_snapshot = self.input.param("test_abort_snapshot",
                                                    False)
        if self.recoveryType:
            self.recoveryType = self.recoveryType.split(":")
        if self.deltaRecoveryBuckets:
            self.deltaRecoveryBuckets = self.deltaRecoveryBuckets.split(":")

        # Definitions of Blob Generator used in tests
        self.key = "failover"
        self.gen_initial_create = doc_generator(self.key, 0, self.num_items)
        self.gen_create = doc_generator(
            self.key, self.num_items, self.num_items * 1.5)
        self.gen_update = doc_generator(
            self.key, self.num_items / 2, self.num_items)
        self.gen_delete = doc_generator(
            self.key, self.num_items / 4, self.num_items / 2 - 1)
        self.afterfailover_gen_create = doc_generator(
            self.key, self.num_items * 1.6, self.num_items * 2)
        self.afterfailover_gen_update = doc_generator(
            self.key, 1, self.num_items / 4)
        self.afterfailover_gen_delete = doc_generator(
            self.key, self.num_items * 0.5, self.num_items * 0.75)

        if self.cluster.vbuckets is not None \
                and self.cluster.vbuckets != self.total_vbuckets:
            self.total_vbuckets = self.cluster.vbuckets
        self.nodes_init = self.input.param("nodes_init", 1)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance(self.cluster, nodes_init, [])
        self.bucket_util.create_default_bucket(
            self.cluster,
            bucket_type=self.bucket_type,
            ram_quota=self.bucket_size,
            replica=self.num_replicas,
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy)
        self.bucket_util.add_rbac_user(self.cluster.master)

        if (self.load_docs_using == "default_loader" and
                self.cluster.sdk_client_pool):
            self.log.info("Creating SDK clients for client_pool")
            for bucket in self.cluster.buckets:
                self.cluster.sdk_client_pool.create_clients(
                    self.cluster, bucket,
                    req_clients=self.sdk_pool_capacity,
                    compression_settings=self.sdk_compression)
        elif self.load_docs_using == "sirius_java_sdk":
            for bucket in self.cluster.buckets:
                self.log.info(f"Creating Java SDK pool for {bucket.name}")
                SiriusCouchbaseLoader.create_clients_in_pool(
                    self.cluster.master, self.cluster.master.rest_username,
                    self.cluster.master.rest_password,
                    bucket.name, req_clients=self.sdk_pool_capacity)

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)
        self.buckets = self.bucket_util.get_all_buckets(self.cluster)
        self.log.info("== FailoverBaseTest setup finished for test #{0} {1} =="
                      .format(self.case_number, self._testMethodName))

    def __recover_failed_nodes(self, servers):
        self.log.info("Recovering failures on servers: %s" % servers)
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            shell.disable_firewall()
            shell.unpause_memcached()
            shell.unpause_beam()
            shell.start_couchbase()
            shell.disconnect()

    def tearDown(self):
        self.log.info(
            "=== FailoverBaseTest tearDown started for test #{0} {1} ==="
            .format(self.case_number, self._testMethodName))
        if hasattr(self, '_resultForDoCleanups') \
                and len(self._resultForDoCleanups.failures) > 0 \
                and 'stop-on-failure' in TestInputSingleton.input.test_params \
                and str(TestInputSingleton.input.test_params['stop-on-failure']).lower() == 'true':
            # supported starting with python2.7
            self.log.warn("CLEANUP WAS SKIPPED")
            self.cluster.shutdown(force=True)
        else:
            self.__recover_failed_nodes(self.cluster.servers)
            self.cluster_util.check_for_panic_and_mini_dumps(self.servers)
            super(FailoverBaseTest, self).tearDown()

    def subsequent_load_gen(self, retry_exceptions=[], ignore_exceptions=[]):
        subsequent_load_gen = doc_generator(self.key,
                                            self.num_items,
                                            self.num_items*2,
                                            key_size=self.key_size,
                                            doc_size=self.doc_size,
                                            doc_type=self.doc_type)
        return self.bucket_util._async_load_all_buckets(
            self.cluster, subsequent_load_gen, "create", 0, batch_size=20,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            retry_exceptions=retry_exceptions,
            ignore_exceptions=ignore_exceptions,
            load_using=self.load_docs_using)

    def async_load_all_buckets(self, kv_gen, op_type, exp, batch_size=20):
        tasks = list()
        for bucket in self.cluster.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, kv_gen, op_type, exp,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                batch_size=batch_size, timeout_secs=self.sdk_timeout,
                process_concurrency=8, retries=self.sdk_retries,
                durability=self.durability_level,
                load_using=self.load_docs_using)
            tasks.append(task)
        return tasks

    def start_parallel_cruds(self, retry_exceptions=[], ignore_exceptions=[],
                             task_verification=False):
        tasks_info = dict()
        if "update" in self.doc_ops:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_update, "update", 0, batch_size=20,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                load_using=self.load_docs_using)
            tasks_info.update(tem_tasks_info.items())
        if "create" in self.doc_ops:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_create, "create", 0, batch_size=20,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                load_using=self.load_docs_using)
            tasks_info.update(tem_tasks_info.items())
            self.num_items += (self.gen_create.end - self.gen_create.start)
        if "delete" in self.doc_ops:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_delete, "delete", 0, batch_size=20,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                load_using=self.load_docs_using)
            tasks_info.update(tem_tasks_info.items())
            self.num_items -= (self.gen_delete.end - self.gen_delete.start)

        if task_verification:
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster, load_using=self.load_docs_using)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

        return tasks_info

    def loadgen_docs(self, retry_exceptions=[], ignore_exceptions=[],
                     task_verification=False):
        retry_exceptions = \
            list(set(retry_exceptions +
                     SDKException.TimeoutException +
                     SDKException.RequestCanceledException +
                     SDKException.DurabilityImpossibleException +
                     SDKException.DurabilityAmbiguousException))

        loaders = self.start_parallel_cruds(retry_exceptions,
                                            ignore_exceptions,
                                            task_verification)
        return loaders
