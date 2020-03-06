from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from dcpbase import DCPBase
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection

from memcached.helper.data_helper import VBucketAwareMemcached, \
                                         MemcachedClientHelper


class DCPRollBack(DCPBase):

    """
    # MB-21587 rollback recover is incorrect

    The scenario is as follows:
     1. 2 node cluster
     2. populate some kvs
     3. turn off persistence on both nodes
     4. modify less than 1/2 of the keys
     5. kill memcache on node 1. When node 2 reconnects to node 1
        it will ask for sequence numbers higher than
        those seen node 1 so node 1 will ask it to rollback
     6. failover to node 2
     7. Verify the keys that were modified and were active on node 1 have
        the values as set prior to step 4

    """

    def setUp(self):
        super(DCPRollBack, self).setUp()

    def tearDown(self):
        super(DCPRollBack, self).tearDown()

    def replicate_correct_data_after_rollback(self):
        '''
        @attention:
          This test case has some issue with docker runs.
          It passes without any issue on VMs.
        '''

        bucket = self.bucket_util.buckets[0]
        cluster = self.cluster

        gen_load = doc_generator(self.key, 0, self.num_items)
        for bucket in self.bucket_util.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gen_load, "create", 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries)
            self.task.jython_task_manager.get_task_result(task)

        # store the KVs which were modified and active on node 1
        modified_kvs_active_on_node1 = dict()
        vbucket_client = VBucketAwareMemcached(
            RestConnection(cluster.master), bucket.name)
        client = MemcachedClientHelper.direct_client(cluster.servers[0],
                                                     bucket.name)
        for i in range(self.num_items/100):
            keyname = 'keyname-' + str(i)
            vbId = self.bucket_util.get_vbucket_num_for_key(
                keyname,
                self.cluster_util.vbuckets)
            if vbucket_client.vBucketMap[vbId].split(':')[0] == cluster.servers[0].ip:
                rc = client.get(keyname)
                modified_kvs_active_on_node1[keyname] = rc[2]

        # Stop persistence
        for server in cluster.servers[:self.nodes_init]:
            # Create cbepctl command object
            node_shell_conn = RemoteMachineShellConnection(server)
            cbepctl_obj = Cbepctl(node_shell_conn)

            for bucket in self.bucket_util.buckets:
                cbepctl_obj.persistence(bucket.name, "stop")

            # Disconnect the shell_connection
            node_shell_conn.disconnect()

        # modify less than 1/2 of the keys
        gen_load = doc_generator(self.key, 0, self.num_items/100)
        rc = self.cluster.load_gen_docs(
            cluster.servers[0], bucket.name, gen_load,
            bucket.kvs[1], "create", exp=0, flag=0, batch_size=10,
            compression=self.sdk_compression)

        # kill memcached, when it comes back because persistence is disabled
        # it will have lost the second set of mutations
        shell = RemoteMachineShellConnection(cluster.servers[0])
        shell.kill_memcached()
        self.sleep(10, "Sleep after kill memcached")

        # Start persistence on the second node
        # Create cbepctl command object
        node_shell_conn = RemoteMachineShellConnection(cluster.servers[1])
        cbepctl_obj = Cbepctl(node_shell_conn)

        for bucket in self.bucket_util.buckets:
            cbepctl_obj.persistence(bucket.name, "start")

        # Disconnect the shell_connection
        node_shell_conn.disconnect()

        self.sleep(10, "Sleep after start persistence")

        # failover to the second node
        rc = self.cluster.failover(cluster.servers, cluster.servers[1:2],
                                   graceful=True)
        self.sleep(30, "Sleep after node failover triggered")

        # Values should be what they were prior to the second update
        client = MemcachedClientHelper.direct_client(
            cluster.servers[0], bucket.name)
        for k, v in modified_kvs_active_on_node1.iteritems():
            rc = client.get(k)
            self.assertTrue(v == rc[2], 'Expected {0}, actual {1}'
                                        .format(v, rc[2]))

        # need to rebalance the node back into the cluster
        # def rebalance(self, servers, to_add, to_remove, timeout=None,
        #               use_hostnames=False, services = None):

        rest_obj = RestConnection(cluster.servers[0])
        nodes_all = rest_obj.node_statuses()
        for node in nodes_all:
            if node.ip == cluster.servers[1].ip:
                break

        node_id_for_recovery = node.id
        status = rest_obj.add_back_node(node_id_for_recovery)
        if status:
            rest_obj.set_recovery_type(node_id_for_recovery,
                                       recoveryType='delta')
        rc = self.cluster.rebalance(cluster.servers[:self.nodes_init], [], [])

    """
    # MB-21568 sequence number is incorrect during a race between
    # persistence and failover.

    The scenario is as follows:
    1. Two node cluster
    2. Set 1000 KVs
    3. Stop persistence
    4. Set another 1000 non-intersecting KVs
    5. Kill memcached on node 1
    6. Verify that the number of items on both nodes is the same

    """
    def test_rollback_and_persistence_race_condition(self):
        cluster = self.cluster
        gen_load = doc_generator(self.key, 0, self.num_items)
        for bucket in self.bucket_util.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gen_load, "create", 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries)
            self.task.jython_task_manager.get_task_result(task)

        # Stop persistence
        for server in cluster.servers[:self.nodes_init]:
            # Create cbepctl command object
            node_shell_conn = RemoteMachineShellConnection(server)
            cbepctl_obj = Cbepctl(node_shell_conn)

            for bucket in self.bucket_util.buckets:
                cbepctl_obj.persistence(bucket.name, "stop")

            # Disconnect the shell_connection
            node_shell_conn.disconnect()

        self.sleep(10, "Wait after stop_persistence")

        # more (non-intersecting) load
        gen_load = doc_generator(self.key, 0, self.num_items, doc_size=64)
        for bucket in self.bucket_util.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gen_load, "create", 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries)
            self.task.jython_task_manager.get_task_result(task)

        shell = RemoteMachineShellConnection(cluster.servers[0])
        shell.kill_memcached()

        self.sleep(10, "Wait after kill memcached")

        node1_shell_conn = RemoteMachineShellConnection(cluster.servers[0])
        node2_shell_conn = RemoteMachineShellConnection(cluster.servers[1])
        node1_cb_stat_obj = Cbstats(node1_shell_conn)
        node2_cb_stat_obj = Cbstats(node2_shell_conn)

        node1_items = node1_cb_stat_obj.all_stats(bucket, "curr_items_tot")
        node2_items = node2_cb_stat_obj.all_stats(bucket, "curr_items_tot")

        # Disconnect the opened connections
        node1_shell_conn.disconnect()
        node2_shell_conn.disconnect()

        self.assertTrue(node1_items == node2_items,
                        'Node items not equal. Node 1:{0}, node 2:{1}'
                        .format(node1_items, node2_items))
