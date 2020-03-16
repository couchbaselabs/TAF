import os
import random

from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from magma_base import MagmaBaseTest
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
import copy


class BasicCrudTests(MagmaBaseTest):
    def setUp(self):
        super(BasicCrudTests, self).setUp()

    def tearDown(self):
        super(BasicCrudTests, self).tearDown()

    def test_read_create_docs_parallelly(self):
        """
        Write and Read docs parallely , While reading we are using
        old doc generator (self.gen_create)
        using which we already created docs in magam_base
        for writing we are creating a new doc generator.
        Befor we start read, killing memcached to make sure,
        all reads happen from magma/storage
        """
        self.log.info("Loading and Reading docs parallel")
        count = 0
        while count < 3:
            for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()
                shell.disconnect()
            self.doc_ops = "create:read"
            start = self.num_items
            end = self.num_items+self.num_items
            if self.rev_write:
                start = -int(self.num_items+self.num_items - 1)
                end = -int(self.num_items - 1)
            self.gen_create = doc_generator(
                self.key, start, end,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size)
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.log.info("Verifying doc counts after create doc_ops")
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
            self.gen_delete = copy.deepcopy(self.gen_create)
            self.doc_ops = "delete"
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions)
            self.bucket_util.verify_stats_all_buckets(self.num_items)
            count += 1
        self.log.info("====test_read_create_docs_parallelly ends====")
