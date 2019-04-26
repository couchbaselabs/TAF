import logger
import dcp.constants
from random import randint
from dcpbase import DCPBase
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection
from cb_tools.cbstats import Cbstats

log = logger.Logger.get_logger()


class DCPCrashTests(DCPBase):
    def test_stream_after_n_crashes(self):
        crashes = self.input.param("crash_num", 5)
        vbucket = randint(0, self.vbuckets)
        bucket = self.bucket_util.buckets[0]

        self.log.info("Chosen vbucket {0} for {1} crashes"
                      .format(vbucket, crashes))
        start = 0
        end = self.num_items

        nodeA = self.cluster.servers[0]
        shell_conn = RemoteMachineShellConnection(nodeA)
        cb_stat_obj = Cbstats(shell_conn)
        rest = RestHelper(RestConnection(nodeA))

        for _ in xrange(crashes):
            # Load data into the selected vbucket
            self.load_docs(bucket, vbucket, start, end, "create")
            self.assertTrue(self.stop_node(0), msg="Failed during stop_node")
            self.sleep(5, "Sleep after stop_node")
            self.assertTrue(self.start_node(0), msg="Failed during start_node")
            self.assertTrue(rest.is_ns_server_running(),
                            msg="Failed while is_ns_server_running check")
            self.sleep(5, "Waiting after ns_server started")

            # Fetch vbucket seqno stats
            vb_stat = cb_stat_obj.vbucket_seqno(bucket.name)
            dcp_client = self.dcp_client(nodeA, dcp.constants.PRODUCER)
            stream = dcp_client.stream_req(vbucket, 0, 0,
                                           vb_stat[vbucket]["high_seqno"],
                                           vb_stat[vbucket]["uuid"])
            stream.run()

            self.assertTrue(stream.last_by_seqno
                            == vb_stat[vbucket]["high_seqno"],
                            msg="Mismatch in high_seqno. {0} == {1}"
                            .format(vb_stat[vbucket]["high_seqno"],
                                    stream.last_by_seqno))

            # Update start/end values for next loop
            start = end
            end += self.num_items

        # Disconnect shell Connection for the node
        shell_conn.disconnect()

    def test_crash_while_streaming(self):
        bucket = self.bucket_util.buckets[0]
        vbucket = randint(0, self.vbuckets)
        nodeA = self.servers[0]
        self.load_docs(bucket, vbucket, 0, self.num_items, "create")

        shell_conn = RemoteMachineShellConnection(nodeA)
        cb_stat_obj = Cbstats(shell_conn)

        dcp_client = self.dcp_client(nodeA, dcp.constants.PRODUCER)
        _ = dcp_client.stream_req(vbucket, 0, 0, 2*self.num_items, 0)
        self.load_docs(nodeA, vbucket, self.num_items)
        self.assertTrue(self.stop_node(0), msg="Failed during stop_node")
        self.sleep(2, "Sleep after stop_node")
        self.assertTrue(self.start_node(0), msg="Failed during start_node")
        rest = RestHelper(RestConnection(nodeA))
        self.assertTrue(rest.is_ns_server_running(),
                        msg="Failed while is_ns_server_running check")
        self.sleep(30, "Sleep to wait for ns_server to run")

        vb_info = cb_stat_obj.vbucket_seqno(bucket.name)
        dcp_client = self.dcp_client(nodeA, dcp.constants.PRODUCER)
        stream = dcp_client.stream_req(vbucket, 0, 0,
                                       vb_info[vbucket]["high_seqno"], 0)
        stream.run()
        self.assertTrue(stream.last_by_seqno == vb_info[vbucket]["high_seqno"],
                        msg="Mismatch in high_seqno. {0} == {1}"
                        .format(vb_info[vbucket]["high_seqno"],
                                stream.last_by_seqno))

        # Disconnect shell Connection for the node
        shell_conn.disconnect()

    def test_crash_entire_cluster(self):
        self.cluster.rebalance([self.master], self.servers[1:], [])

        bucket = self.bucket_util.buckets[0]
        vbucket = randint(0, self.vbuckets)
        nodeA = self.servers[0]
        self.load_docs(bucket, vbucket, 0, self.num_items, "create")

        shell_conn = RemoteMachineShellConnection(nodeA)
        cb_stat_obj = Cbstats(shell_conn)

        dcp_client = self.dcp_client(nodeA, dcp.constants.PRODUCER)
        _ = dcp_client.stream_req(vbucket, 0, 0, 2*self.num_items, 0)
        self.load_docs(nodeA, vbucket, self.num_items)

        # stop all nodes
        node_range = range(len(self.servers))
        for i in node_range:
            self.assertTrue(self.stop_node(i), msg="Failed during stoip_node")
        self.sleep(2, "Wait after stop_node")

        # start all nodes in reverse order
        node_range.reverse()
        for i in node_range:
            self.assertTrue(self.start_node(i), msg="Failed during start_node")

        rest = RestHelper(RestConnection(nodeA))
        self.assertTrue(rest.is_ns_server_running(),
                        msg="Failed while is_ns_server_running check")

        vb_info = cb_stat_obj.vbucket_seqno(bucket.name)
        dcp_client = self.dcp_client(nodeA, dcp.constants.PRODUCER)
        stream = dcp_client.stream_req(vbucket, 0, 0,
                                       vb_info[vbucket]["high_seqno"], 0)
        stream.run()
        self.assertTrue(stream.last_by_seqno == vb_info[vbucket]["high_seqno"],
                        msg="Seq-no mismatch. {0} != {1}"
                        .format(stream.last_by_seqno,
                                vb_info[vbucket]["high_seqno"]))

        # Disconnect shell Connection for the node
        shell_conn.disconnect()
