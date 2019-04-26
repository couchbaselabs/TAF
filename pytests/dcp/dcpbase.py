import time
from TestInput import TestInputSingleton

from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from dcp.constants import CONSUMER, DCP, DEFAULT_CONN_NAME, MCD, \
                          NOTIFIER, PRODUCER, SUCCESS
from dcp_bin_client import DcpClient
from mc_bin_client import MemcachedClient
from cluster_run_manager import CRManager
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class DCPBase(BaseTestCase):
    def __init__(self, args):
        super(DCPBase, self).__init__(args)
        self.is_setup = False
        self.crm = None
        self.input = TestInputSingleton.input
        self.use_cluster_run = self.input.param('dev', False)
        self.test = self.input.param('test', None)
        self.stopped_nodes = []

        self.doc_num = 0
        if self.use_cluster_run:
            num_nodes = self.input.param('num_nodes', 4)
            self.crm = CRManager(num_nodes, 0)

    def setUp(self):
        if self.test:
            if self.test != self._testMethodName:
                self.skipTest("disabled")

        self.is_setup = True

        if self.use_cluster_run:
            self.assertTrue(self.crm.clean(), msg="Failed during crm.clean")
            self.assertTrue(self.crm.start_nodes(),
                            msg="Failed during start_nodes")
            time.sleep(5)

        self.is_setup = False

        super(DCPBase, self).setUp()

        self.key = 'dcp_test_docs'.rjust(self.key_size, '0')
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)
        self.bucket_util.create_default_bucket(
            replica=self.num_replicas, compression_mode=self.compression_mode)
        self.bucket_util.add_rbac_user()
        self.log.info("==== Finished DcpBase base setup ====")

    def tearDown(self):
        for index in self.stopped_nodes:
            self.start_node(index)

        if self.use_cluster_run and not self.is_setup:
            self.assertTrue(self.crm.stop_nodes(),
                            msg="Failed during stop_nodes")
            self.cluster.shutdown(force=True)
        else:
            super(DCPBase, self).tearDown()
        self.cluster_util.cleanup_cluster(master=self.cluster.master)

    def load_docs(self, bucket, vbucket, start, end, op_type,
                  exp=0, flags=0):
        """ using direct mcd client to control vbucket seqnos.
            keeps track of vbucket and keys stored """

        gen_loader = doc_generator(self.key, start, end,
                                   target_vbucket=[vbucket])

        load_gen_task = self.task.async_load_gen_docs(
            self.cluster, bucket, gen_loader, op_type, exp=exp, flag=flags,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries)
        self.task.jython_task_manager.get_task_result(load_gen_task)

    def dcp_client(self, node, connection_type=PRODUCER, vbucket=None,
                   name=None, auth_user="cbadminbucket",
                   auth_password="password", bucket_name="default"):
        """
        Create an DCP client from node spec and
        opens connnection of specified type
        """
        client = self.client_helper(node, DCP, vbucket)
        if auth_user:
            client.sasl_auth_plain(auth_user, auth_password)
            client.bucket_select(bucket_name)

        self.assertTrue(connection_type in (PRODUCER, CONSUMER, NOTIFIER),
                        msg="connected_type not found")
        name = name or DEFAULT_CONN_NAME
        if connection_type == PRODUCER:
            response = client.open_producer(name)
        if connection_type == CONSUMER:
            response = client.open_consumer(name)
        if connection_type == NOTIFIER:
            response = client.open_notifier(name)

        self.assertTrue(response['status'] == SUCCESS,
                        msg="Status is not SUCCESS")
        return client

    def mcd_client(self, node, vbucket=None, auth_user=None,
                   auth_password=None):
        """
        Create a mcd client from Node spec
        """

        client = self.client_helper(node, MCD, vbucket)
        if auth_user:
            # admin_user='cbadminbucket',admin_pass='password'
            client.sasl_auth_plain('cbadminbucket', 'password')
            client.bucket_select('default')
        return client

    def client_helper(self, node, type_, vbucket):
        self.assertTrue(type_ in (MCD, DCP), msg="type not found")

        client = None
        ip = None
        port = None
        rest = RestConnection(node)

        if vbucket is not None:
            host = self.vbucket_host(rest, vbucket)
            ip = host.split(':')[0]
            port = int(host.split(':')[1])

        else:
            client_node = rest.get_nodes_self()
            ip = client_node.hostname.split(':')[0]
            port = client_node.memcached

        if type_ == MCD:
            client = MemcachedClient(ip, port)
        else:
            client = DcpClient(ip, port)

        return client

    def vbucket_host(self, rest, vbucket):
        info = rest.get_bucket_json()
        return info['vBucketServerMap']['serverList'] \
            [info['vBucketServerMap']['vBucketMap'][vbucket][0]]

    def vbucket_host_index(self, rest, vbucket):
        info = rest.get_bucket_json()
        host = self.vbucket_host(rest, vbucket)
        return info['vBucketServerMap']['serverList'].index(host)

    def flow_control_info(self, node, connection=None):

        connection = connection or DEFAULT_CONN_NAME
        mcd_client = self.mcd_client(node)
        stats = mcd_client.stats(DCP)

        acked = 'eq_dcpq:{0}:total_acked_bytes'.format(connection)
        unacked = 'eq_dcpq:{0}:unacked_bytes'.format(connection)
        sent = 'eq_dcpq:{0}:total_bytes_sent'.format(connection)
        return int(stats[acked]), int(stats[sent]), int(stats[unacked])

    def all_vb_info(self, cb_stat_obj, bucket_name, table_entry=0):
        vbInfoMap = dict()
        failover_stats = cb_stat_obj.failover_stats(bucket_name)
        id_key = '{0}:id'.format(table_entry)
        for vbucket in self.vbuckets:
            hi_seqno_pattern = 'vb_{0}:high_seqno'.format(vbucket)

            seqno = cb_stat_obj.vbucket_details(bucket_name, vbucket,
                                                hi_seqno_pattern)

            vb_uuid, high_seqno = (long(failover_stats[vbucket][id_key]),
                                   long(seqno))
            vbInfoMap[vbucket] = (vb_uuid, high_seqno)

        return vbInfoMap

    def stop_node(self, index):
        status = False
        if self.use_cluster_run:
            status = self.crm.stop(index)
        elif len(self.servers) >= index:
            node = self.servers[index]
            shell = RemoteMachineShellConnection(node)
            shell.stop_couchbase()
            shell.disconnect()
            status = True

        return status

    def start_node(self, index):
        status = False
        if self.use_cluster_run:
            status = self.crm.start(index)
        elif len(self.servers) >= index:
            node = self.servers[index]
            shell = RemoteMachineShellConnection(node)
            shell.start_couchbase()
            shell.disconnect()
            status = True
        return status
