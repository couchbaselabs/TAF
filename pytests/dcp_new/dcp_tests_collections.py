from dcp_new.dcp_base import DCPBase
from dcp_new.constants import *
from dcp_bin_client import *
from mc_bin_client import MemcachedClient as McdClient
from memcached.helper.data_helper import MemcachedClientHelper
import unittest
from memcacheConstants import *
from threading import Thread
from remote.remote_util import RemoteMachineShellConnection
from datetime import datetime
from membase.api.rest_client import Node, RestConnection
from itertools import count

class DcpTestCase(DCPBase):
    def setUp(self):
        super(DcpTestCase, self).setUp()

    def test_stream_entire_bucket(self):
        streams = self.add_streams(self.vbuckets,
                                  self.start_seq_no_list,
                                  self.end_seq_no,
                                  self.vb_uuid_list,
                                  self.vb_retry, self.filter_file)
        output_string = self.process_dcp_traffic(streams)
        rest = RestConnection(self.cluster.master)
        expected_item_count = sum(rest.get_buckets_itemCount().values())
        actual_item_count = len(list(filter(lambda x: 'KEY' in x, output_string)))
        if expected_item_count != actual_item_count:
            self.log_failure("item count mismatch, expected %s actual %s"\
                             %(expected_item_count, actual_item_count))
        for client_stream in self.dcp_client_dict.values():
            client_stream['stream'].close()