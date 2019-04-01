import logger
from dcp.constants import PRODUCER
from dcpbase import DCPBase
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import doc_generator
from remote.remote_util import RemoteMachineShellConnection
from cb_cmd_utils.cbstats import Cbstats

log = logger.Logger.get_logger()


class DCPMultiBucket(DCPBase):
    def test_stream_all_buckets(self):
        doc_gen = doc_generator(self.key, 0, self.num_items)
        self._load_all_buckets(self.master, doc_gen, "create", 0)

        user_name = self.input.param("user_name", None)
        password = self.input.param("password", None)
        nodeA = self.servers[0]

        vbuckets = [vb for vb in range(self.vbuckets)]
        shell_conn = RemoteMachineShellConnection(nodeA)
        cb_stat_obj = Cbstats(shell_conn)

        for bucket in self.bucket_util.buckets:
            if user_name is not None:
                self.add_built_in_server_user(
                    [{'id': user_name, 'name': user_name,
                      'password': password}],
                    [{'id': user_name, 'name': user_name,
                      'roles': 'data_dcp_reader[default]'}],
                    self.master)
                dcp_client = self.dcp_client(
                    nodeA, PRODUCER, bucket_name=bucket.name,
                    auth_user=user_name, auth_password=password)
            else:
                dcp_client = self.dcp_client(nodeA, PRODUCER,
                                             bucket_name=bucket)

            for vb in vbuckets[0:16]:
                vbucket = vb.id
                vb_uuid, _, high_seqno = self.vb_info(cb_stat_obj, bucket.name,
                                                      vbucket)
                stream = dcp_client.stream_req(vbucket, 0, 0, high_seqno,
                                               vb_uuid)
                _ = stream.run()
                self.assertTrue(high_seqno == stream.last_by_seqno,
                                msg="Mismatch in high_seqno. {0} == {1}"
                                .format(high_seqno, stream.last_by_seqno))

        # Disconnect the shell_conn
        shell_conn.disconnect()

    def test_stream_after_warmup(self):
        nodeA = self.cluster.servers[0]
        bucket = self.bucket_util.buckets[1]

        shell_conn = RemoteMachineShellConnection(nodeA)
        cb_stat_obj = Cbstats(shell_conn)

        # load all buckets
        doc_gen = doc_generator(self.key, 0, self.num_items)
        self._load_all_buckets(self.master, doc_gen, "create", 0)
        self._wait_for_stats_all_buckets()

        # store expected vb seqnos
        originalVbInfo = self.all_vb_info(cb_stat_obj, bucket.name)

        # restart node
        self.assertTrue(self.stop_node(0), msg="Failed during stop_node")
        self.sleep(5, "Wait after stop_node")
        self.assertTrue(self.start_node(0), msg="Failed during start_node")
        rest = RestHelper(RestConnection(nodeA))
        self.assertTrue(rest.is_ns_server_running(),
                        msg="Failed while is_ns_server_running")
        self.sleep(2, "Wait after ns_server_start")

        # verify original vbInfo can be streamed
        dcp_client = self.dcp_client(nodeA, PRODUCER, bucket_name=bucket.name)
        for vbucket in originalVbInfo:
            vb_uuid, _, high_seqno = originalVbInfo[vbucket]
            stream = dcp_client.stream_req(vbucket, 0, 0, high_seqno, vb_uuid)
            _ = stream.run()
            self.assertTrue(high_seqno == stream.last_by_seqno,
                            msg="Mismatch in high_seqno. {0} == {1}"
                                .format(high_seqno, stream.last_by_seqno))

        # Disconnect the shell_conn
        shell_conn.disconnect()
