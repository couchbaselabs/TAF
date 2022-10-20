import random
import string

from Cb_constants import DocLoading
from sdk_client3 import SDKClient
from LMT_base import LMT
from error_simulation.cb_error import CouchbaseError
from remote.remote_util import RemoteMachineShellConnection
from pytests.crash_test.constants import signum
from couchbase_helper.documentgenerator import \
    doc_generator

class ServerlessMetering(LMT):
    def setUp(self):
        super(ServerlessMetering, self).setUp()
        self.bucket = self.cluster.buckets[0]

    def tearDown(self):
        super(ServerlessMetering, self).tearDown()

    def stop_process(self, nodes, error_to_simulate):
        remote_node = []
        for node_i in nodes:
            print("node_i is %s" % node_i)
            remote = RemoteMachineShellConnection(node_i)
            error_sim = CouchbaseError(self.log, remote)
            # Induce the error condition
            error_sim.create(error_to_simulate)
            remote_node.append(remote)

        self.sleep(20, "Wait before reverting the error condition")
        for remote in remote_node:
            error_sim = CouchbaseError(self.log, remote)
            error_sim.revert(error_to_simulate)
            remote.disconnect()

    def test_stop_process(self):
        # set throttle limit to a very high value and load data
        error_to_simulate = self.input.param("simulate_error", None)
        self.nodes_to_affect = self.input.param("node_num", 1)
        self.buckets_to_affect = self.input.param("buckets_to_affect", 1)
        self.num_times_to_affect = self.input.param("num_times", 1)
        self.crash_other_node = self.input.param("crash_other_node", False)
        items = self.num_items
        target_vbucket_nodes = []
        start = 0
        end = items
        self.expected_wu = 0

        self.buckets = self.bucket_util.get_all_buckets(self.cluster)
        buckets_to_affect = random.sample(self.buckets, self.buckets_to_affect)

        for bucket in buckets_to_affect:
            target_vbucket = []
            try:
                nodes = random.sample(bucket.servers, self.nodes_to_affect)
            except:
                nodes = bucket.servers

            if self.crash_other_node:
                target_vbucket_nodes = set(bucket.servers) ^ set(nodes)
            if not target_vbucket_nodes:
                target_vbucket_nodes = nodes
            for node_i in target_vbucket_nodes:
                target_vbucket.extend(self.get_active_vbuckets(node_i, bucket))

            for _ in range(self.num_times_to_affect):
                self.generate_docs(doc_ops="create",
                                   create_start=start,
                                   create_end=end,
                                   target_vbucket=target_vbucket)
                _ = self.loadgen_docs(self.retry_exceptions,
                                      self.ignore_exceptions,
                                      _sync=True)
                self.log.info("Waiting for ep-queues to get drained")

                #kill/crash/restart memcached
                self.stop_process(nodes, error_to_simulate)

                # validation of stats
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets, timeout=100)
                self.bucket_util.get_all_buckets(self.cluster)
                self.log.info("num_items in bucket %s" % bucket.stats.itemCount)
                self.expected_wu = self.bucket_util.calculate_units(self.doc_size, 0) * bucket.stats.itemCount
                num_throttled, ru, wu = self.get_stat_from_metering(bucket)
                self.log.info("numthrottled:%s, ru:%s, wu:%s" % (num_throttled, ru, wu))
                if "kill" not in error_to_simulate or self.crash_other_node:
                    self.assertEqual(wu, self.expected_wu)
                elif wu > self.expected_wu:
                    self.log.fail("wu actual:%s, wu expected:%s"
                                  %(wu, self.expected_wu))
                if self.doc_size > 1000 and num_throttled < bucket.stats.itemCount/2:
                    self.log.fail("throttling didnt occur as expected")
                start += items
                end += items

            # perform load after the crash/stop process and check stats are working fine
            expected_num_throttled, expected_ru, self.expected_wu = self.get_stat_from_metering(bucket)
            self.generate_docs(doc_ops="create", create_start=start, create_end=end)
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util.get_all_buckets(self.cluster)
            expected_ru = items - (bucket.stats.itemCount - items)
            self.expected_wu += self.bucket_util.calculate_units(self.doc_size, 0) * (bucket.stats.itemCount - items)
            if self.doc_size > 1000:
                expected_num_throttled += bucket.stats.itemCount/2
            num_throttled, ru, wu = self.get_stat(bucket)
            if wu != self.expected_wu or ru != expected_ru or num_throttled < expected_num_throttled:
                self.fail("load after crash failed in stats "
                          "Actual:(ru:%s, wu:%s, num_throttled:%s),"
                          " expected:(ru:%s, wu:%s, num_throttled:%s)" %
                          (ru, wu, num_throttled, expected_ru, self.expected_wu, expected_num_throttled))
