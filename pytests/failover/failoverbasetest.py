from TestInput import TestInputSingleton
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.document import View
from remote.remote_util import RemoteUtilHelper


class FailoverBaseTest(BaseTestCase):

    def setUp(self):
        self._cleanup_nodes = []
        self._failed_nodes = []
        super(FailoverBaseTest, self).setUp()
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view_name = "default_view"
        self.default_view = View(self.default_view_name, self.defaul_map_func, None)
        self.failoverMaster = self.input.param("failoverMaster", False)
        self.total_vbuckets = self.input.param("total_vbuckets", 1024)
        self.compact = self.input.param("compact", False)
        self.std_vbucket_dist = self.input.param("std_vbucket_dist", 20)
        self.withMutationOps = self.input.param("withMutationOps", False)
        self.withViewsOps = self.input.param("withViewsOps", False)
        self.createIndexesDuringFailover = self.input.param("createIndexesDuringFailover", False)
        self.upr_check = self.input.param("upr_check", True)
        self.withQueries = self.input.param("withQueries", False)
        self.numberViews = self.input.param("numberViews", False)
        self.gracefulFailoverFail = self.input.param("gracefulFailoverFail", False)
        self.runRebalanceAfterFailover = self.input.param("runRebalanceAfterFailover", True)
        self.failoverMaster = self.input.param("failoverMaster", False)
        self.check_verify_failover_type = self.input.param("check_verify_failover_type", True)
        self.recoveryType = self.input.param("recoveryType", "delta")
        self.bidirectional = self.input.param("bidirectional", False)
        self.stopGracefulFailover = self.input.param("stopGracefulFailover", False)
        self._value_size = self.input.param("value_size", 256)
        self.victim_type = self.input.param("victim_type", "other")
        self.victim_count = self.input.param("victim_count", 1)
        self.stopNodes = self.input.param("stopNodes", False)
        self.killNodes = self.input.param("killNodes", False)
        self.doc_ops = self.input.param("doc_ops", [])
        self.firewallOnNodes = self.input.param("firewallOnNodes", False)
        self.deltaRecoveryBuckets = self.input.param("deltaRecoveryBuckets", None)
        self.max_verify = self.input.param("max_verify", None)
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(":")
        self.num_failed_nodes = self.input.param("num_failed_nodes", 0)
        self.target_vbucket_type = self.input.param("target_vbucket_type",
                                                    "active")
        self.dgm_run = self.input.param("dgm_run", True)
        credentials = self.input.membase_settings
        self.add_back_flag = False
        self.during_ops = self.input.param("during_ops", None)
        self.graceful = self.input.param("graceful", True)
        self.failover_onebyone = self.input.param("failover_onebyone", False)
        self.new_replica = self.input.param("new_replica", None)
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

        if self.vbuckets is not None and self.vbuckets != self.total_vbuckets:
            self.total_vbuckets = self.vbuckets
        self.nodes_init = self.input.param("nodes_init", 1)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.append(self.cluster.master)
        self.bucket_util.create_default_bucket(bucket_type=self.bucket_type,
                                               replica=self.num_replicas)
        self.bucket_util.add_rbac_user()
        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()
        self.buckets = self.bucket_util.get_all_buckets()
        self.log.info("== FailoverBaseTest setup finished for test #{0} {1} =="
                      .format(self.case_number, self._testMethodName))

    def tearDown(self):
        if hasattr(self, '_resultForDoCleanups') \
                and len(self._resultForDoCleanups.failures) > 0 \
                and 'stop-on-failure' in TestInputSingleton.input.test_params \
                and str(TestInputSingleton.input.test_params['stop-on-failure']).lower() == 'true':
            # supported starting with python2.7
            self.log.warn("CLEANUP WAS SKIPPED")
            self.cluster.shutdown(force=True)
        else:
            try:
                self.log.info("==============  tearDown was started for test #{0} {1} =============="\
                              .format(self.case_number, self._testMethodName))
                RemoteUtilHelper.common_basic_setup(self.cluster.servers)
                self.cluster_util.check_for_panic_and_mini_dumps(self.servers)
            finally:
                super(FailoverBaseTest, self).tearDown()