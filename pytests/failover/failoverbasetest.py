from TestInput import TestInputSingleton
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from couchbase_helper.document import View
from membase.api.rest_client import RestConnection
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
        self.vbuckets = self.input.param("vbuckets", 1024)
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
        self.wait_timeout = self.input.param("wait_timeout", 60)
        self.active_resident_threshold = int(self.input.param("active_resident_threshold", 0))
        self.max_verify = self.input.param("max_verify", None)
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(":")
        self.num_failed_nodes = self.input.param("num_failed_nodes", 0)
        self.dgm_run = self.input.param("dgm_run", True)
        credentials = self.input.membase_settings
        self.add_back_flag = False
        self.during_ops = self.input.param("during_ops", None)
        self.graceful = self.input.param("graceful", True)
        if self.recoveryType:
            self.recoveryType=self.recoveryType.split(":")
        if self.deltaRecoveryBuckets:
            self.deltaRecoveryBuckets=self.deltaRecoveryBuckets.split(":")
        # Defintions of Blod Generator used in tests
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        self.gen_initial_create = DocumentGenerator('failover', template, age, first, start=0,
                                       end=self.num_items)
        self.gen_create = DocumentGenerator('failover', template, age, first, start=self.num_items,
                                       end=self.num_items * 1.5)
        self.gen_update = DocumentGenerator('failover', template, age, first, start=self.num_items / 2,
                                       end=self.num_items)
        self.gen_delete = DocumentGenerator('failover', template, age, first, start=self.num_items / 4,
                                       end=self.num_items / 2 - 1)
        self.afterfailover_gen_create = DocumentGenerator('failover', template, age, first, start=self.num_items * 1.6, end=self.num_items * 2)
        self.afterfailover_gen_update = DocumentGenerator('failover', template, age, first, start=1,
                                                      end=self.num_items / 4)
        self.afterfailover_gen_delete = DocumentGenerator('failover', template, age, first, start=self.num_items * .5, end=self.num_items * 0.75)
        if self.vbuckets != None and self.vbuckets != self.total_vbuckets:
            self.total_vbuckets  = self.vbuckets
        self.nodes_init = self.input.param("nodes_init", 1)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.append(self.cluster.master)
        self.bucket_util.create_default_bucket()
        self.bucket_util.add_rbac_user()
        self.log.info("==============  FailoverBaseTest setup was finished for test #{0} {1} =============="\
                      .format(self.case_number, self._testMethodName))

    def tearDown(self):
        if hasattr(self, '_resultForDoCleanups') and len(self._resultForDoCleanups.failures) > 0 \
                    and 'stop-on-failure' in TestInputSingleton.input.test_params and \
                    str(TestInputSingleton.input.test_params['stop-on-failure']).lower() == 'true':
                    # supported starting with python2.7
                    self.log.warn("CLEANUP WAS SKIPPED")
                    self.cluster.shutdown(force=True)
                    self._log_finish()
        else:
            super(FailoverBaseTest, self).tearDown()
            # try:
            #     self.log.info("==============  tearDown was started for test #{0} {1} =============="\
            #                   .format(self.case_number, self._testMethodName))
            #     RemoteUtilHelper.common_basic_setup(self.cluster.servers)
            #     self.bucket_util.delete_all_buckets(self.cluster.servers)
            #     for node in self.cluster.servers:
            #         master = node
            #         try:
            #             self.cluster_util.cleanup_cluster()
            #         except:
            #             continue
            #     self.log.info("==============  tearDown was finished for test #{0} {1} =============="\
            #                   .format(self.case_number, self._testMethodName))
            # finally:
            #     super(FailoverBaseTest, self).tearDown()

    def vb_distribution_analysis(self, servers=[], buckets=[], total_vbuckets=0, std=1.0, type="rebalance",
                                 graceful=True):
        """
            Method to check vbucket distribution analysis after rebalance
        """
        self.log.info(" Begin Verification for vb_distribution_analysis")
        servers = self.cluster_util.get_kv_nodes(servers)
        if self.std_vbucket_dist != None:
            std = self.std_vbucket_dist
        if self.vbuckets != None and self.vbuckets != self.total_vbuckets:
            self.total_vbuckets = self.vbuckets
        active, replica = self.bucket_util.get_vb_distribution_active_replica(servers=servers, buckets=buckets)
        for bucket in active.keys():
            self.log.info(" Begin Verification for Bucket {0}".format(bucket))
            active_result = active[bucket]
            replica_result = replica[bucket]
            if graceful or type == "rebalance":
                self.assertTrue(active_result["total"] == total_vbuckets,
                                "total vbuckets do not match for active data set (= criteria), actual {0} expectecd {1}".format(
                                    active_result["total"], total_vbuckets))
            else:
                self.assertTrue(active_result["total"] <= total_vbuckets,
                                "total vbuckets do not match for active data set  (<= criteria), actual {0} expectecd {1}".format(
                                    active_result["total"], total_vbuckets))
            if type == "rebalance":
                rest = RestConnection(self.cluster.master)
                nodes = rest.node_statuses()
                if (len(nodes) - self.num_replicas) >= 1:
                    self.assertTrue(replica_result["total"] == self.num_replicas * total_vbuckets,
                                    "total vbuckets do not match for replica data set (= criteria), actual {0} expected {1}".format(
                                        replica_result["total"], self.num_replicas ** total_vbuckets))
                else:
                    self.assertTrue(replica_result["total"] < self.num_replicas * total_vbuckets,
                                    "total vbuckets do not match for replica data set (<= criteria), actual {0} expected {1}".format(
                                        replica_result["total"], self.num_replicas ** total_vbuckets))
            else:
                self.assertTrue(replica_result["total"] <= self.num_replicas * total_vbuckets,
                                "total vbuckets do not match for replica data set (<= criteria), actual {0} expected {1}".format(
                                    replica_result["total"], self.num_replicas ** total_vbuckets))
            self.assertTrue(active_result["std"] >= 0.0 and active_result["std"] <= std,
                            "std test failed for active vbuckets")
            self.assertTrue(replica_result["std"] >= 0.0 and replica_result["std"] <= std,
                            "std test failed for replica vbuckets")
        self.log.info(" End Verification for vb_distribution_analysis")
