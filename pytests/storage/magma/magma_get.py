from magma_basic_crud import BasicCrudTests
from remote.remote_util import RemoteMachineShellConnection


class BasicReadTests(BasicCrudTests):
    def test_read_docs_using_multithreads(self):
        """
        Test Focus : Read same items simultaneously
                    using MultiThreading.

                    Test update n items(calculated based on
                    fragmentation value), before
                    get operation
        """
        self.log.info("test_read_docs_using_multithreads starts")
        tasks_info = dict()
        upsert_doc_list = self.get_fragmentation_upsert_docs_list()

        for itr in upsert_doc_list:
            self.doc_ops = "update"
            self.update_start = 0
            self.update_end = itr
            self.mutate = -1
            self.generate_docs(doc_ops="update")

            update_task_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                _sync=False)
            tasks_info.update(update_task_info.items())

        count = 0
        self.doc_ops = "read"

        '''
          if self.next_half is true then one thread will read
           in ascending order and other in descending order
        '''

        if self.next_half:
            start = -int(self.num_items - 1)
            end = 1
            g_read = self.genrate_docs_basic(start, end)

        for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.restart_couchbase()
                shell.disconnect()

        while count < self.read_thread_count:
            read_task_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                _sync=False)
            tasks_info.update(read_task_info.items())
            count += 1
            if self.next_half and count < self.read_thread_count:
                read_task_info = self.bucket_util._async_validate_docs(
                    self.cluster, g_read, "read", 0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    timeout_secs=self.sdk_timeout,
                    retry_exceptions=self.retry_exceptions,
                    ignore_exceptions=self.ignore_exceptions)
                tasks_info.update(read_task_info.items())
                count += 1
            self.sleep(1,"Ensures all main read tasks will have unique names")

        for task in tasks_info:
                self.task_manager.get_task_result(task)

        self.log.info("Waiting for ep-queues to get drained")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster.buckets)
        self.log.info("test_read_docs_using_multithreads ends")
