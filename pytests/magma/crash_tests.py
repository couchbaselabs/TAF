'''
Created on Dec 12, 2019

@author: riteshagarwal
'''

from sdk_exceptions import SDKException
from magma_base import MagmaBaseTest
from remote.remote_util import RemoteMachineShellConnection


class MagmaBasicTests(MagmaBaseTest):
    def setUp(self):
        super(MagmaBasicTests, self).setUp()

    def tearDown(self):
        super(MagmaBasicTests, self).tearDown()

    def kill_magma_check_wal_file_size(self):
        nIter = 200
        while nIter > 0:
            shell = RemoteMachineShellConnection(self.cluster_util.cluster.master)
            shell.kill_memcached()
#             self.bucket_util._wait_warmup_completed()
            self.sleep(10, "sleep of 5s so that memcached can restart")
