from magma_basic_crud import BasicCrudTests
from remote.remote_util import RemoteMachineShellConnection


class SteadyStateTests(BasicCrudTests):
    def test_history_retention_for_n_update_iterations(self):
        self.PrintStep("test_history_retention_after_n_times_update starts")
        self.create_start = 0
        self.create_end = self.init_items_per_collection
        self.PrintStep("Step 1: Create %s items/collection: %s" % (self.init_items_per_collection,
                                                                   self.key_type))
        self.new_loader(wait=True)

        #MemCached Kill to ensure memtables get flushed
        self.PrintStep("Step 2: Kill Memcached on all Nodes")
        for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()
                shell.disconnect()
        self.sleep(30, "Sleep after killing memcached")

        self.PrintStep("Step 3: Sequence Number Count After initial Creates")
        init_seq_count = self.get_seqnumber_count()

        count = 1
        while count < self.test_itr+1:
            self.PrintStep("Step 4.%s: Update %s items/collection: %s" % (count, self.init_items_per_collection,
                                                                   self.key_type))
            self.doc_ops = "update"
            self.update_start = 0
            self.update_end = self.init_items_per_collection
            self.update_perc = 100
            self.create_perc = 0
            self.new_loader(wait=True)

            #MemCached Kill to ensure memtables get flushed
            self.PrintStep("Step 4.%s.2: Kill Memcached on all Nodes" % (count))
            for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()
                shell.disconnect()
            self.sleep(30, "Sleep after killing memcached")
            self.PrintStep("Step 4.%s.2: Sequence Number Count After Upsert"% (count))
            expected_count = (count+1) * (self.init_items_per_collection * (self.num_collections-1)) + ((self.num_collections -1) * self.vbuckets)
            seq_count = self.get_seqnumber_count()
            self.log.info("expected_count = {}".format(expected_count))
            self.assertEqual(seq_count, expected_count, "Not all sequence numbers are present")
            count += 1
