'''
Created on 22-Jun-2021

@author: riteshagarwal
'''
from storage.magma.magma_base import MagmaBaseTest
from dcp_bin_client import DcpClient
from mc_bin_client import MemcachedError
import uuid
from dcp_new.constants import SUCCESS
from dcp_utils.dcp_ready_functions import DCPUtils
import json
import os
from dcp_new.dcp_data_persist import LogData


class DCPSeqItr(MagmaBaseTest):

    def setUp(self):
        MagmaBaseTest.setUp(self)
        self.vbuckets = range(1024)
        self.start_seq_no_list = self.input.param("start", [0] * len(self.vbuckets))
        self.end_seq_no = self.input.param("end", 0xffffffffffffffff)
        self.vb_uuid_list = self.input.param("vb_uuid_list", ['0'] * len(self.vbuckets))
        self.vb_retry = self.input.param("retry_limit", 10)
        self.filter_file = self.input.param("filter", None)
        self.stream_req_info = self.input.param("stream_req_info", False)
        self.failover_logging = self.input.param("failover_logging", False)
        self.keep_logs = self.input.param("keep_logs", False)
        self.log_path = self.input.param("log_path", None)
        self.keys = self.input.param("keys", True)
        self.docs = self.input.param("docs", False)
        self.output_string = list()
        self.initialise_cluster_connections()

    def tearDown(self):
        super(MagmaBaseTest, self).tearDown()

    def initialise_cluster_connections(self):
        self.dcp_client = DCPUtils.connect(self.cluster.master.ip,
                                           self.cluster.buckets[0])
        config_json = json.loads(DcpClient.get_config(self.dcp_client)[2])
        self.vb_map = config_json['vBucketServerMap']['vBucketMap']

        self.dcp_client_dict = dict()

        if self.log_path:
            self.log_path = os.path.normpath(self.log_path)
        self.dcp_log_data = LogData(self.log_path, self.vbuckets, self.keep_logs)

        # TODO: Remove globals and restructure (possibly into a class) to allow for multiple
        #       instances of the client (allowing multiple bucket connections)
        node_list = []
        for index, server in enumerate(config_json['vBucketServerMap']['serverList']):
            host = DCPUtils.check_valid_host(server.split(':')[0], 'Server Config Cluster Map')
            node_list.append(host)
            port = config_json['nodesExt'][index]['services'].get('kv')

            if port is not None:
                node = '{0}:{1}'.format(host, port)
                if 'thisNode' in config_json['nodesExt'][index]:
                    self.dcp_client_dict[index] = {'stream': self.dcp_client,
                                                   'node': node}
                else:
                    self.dcp_client_dict[index] = {'stream': DCPUtils.connect(server, self.cluster.buckets[0]),
                                                   'node': node}

    def test_dcp_duplication(self):
        self.log.info("trying DCP")
        pass