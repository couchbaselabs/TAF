from magma_basic_crud import BasicCrudTests
from remote.remote_util import RemoteMachineShellConnection


class BasicCreateTests(BasicCrudTests):
    def test_basic_create_read(self):
        """
        Test Focus: Perform create and read Doc-OPs parallely.

        STEPS:
           -- Create new items
           -- Read existing items
        """
        self.log.info("test_basic_create_read starts")
        count = 0
        init_items = self.num_items
        self.generate_docs(doc_ops="read")

        while count < self.test_itr:
            self.log.info("Iteration == {}".format(count))
            for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.restart_couchbase()
                shell.disconnect()

            self.doc_ops = "create:read"
            self.create_start = self.num_items
            self.create_end = self.num_items+init_items
            if self.rev_write:
                self.create_start = -int(self.num_items+init_items - 1)
                self.create_end = -int(self.num_items - 1)

            self.read_start = self.num_items
            self.read_end = self.num_items+init_items
            if self.rev_read:
                self.read_start = -int(self.num_items+init_items - 1)
                self.read_end = -int(self.num_items - 1)

            self.generate_docs(doc_ops="create")
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            self.generate_docs(doc_ops="read")
            # Check for doc size < 32 , not required
            #if self.doc_size <= 32:
            #    for bucket in self.bucket_util.get_all_buckets():
            #        disk_usage = self.get_disk_usage(
            #            bucket, self.cluster.nodes_in_cluster)
            #        msg = "Bucket={},Iteration= {},\
            #        SeqTree= {}MB > keyTree= {}MB"
            #        self.assertIs(
            #            disk_usage[2] > disk_usage[3], True,
            #            msg.format(bucket.name, count+1,
            #                       disk_usage[3], disk_usage[2]))
            count += 1

        self.log.info("====test_basic_create_read ends====")