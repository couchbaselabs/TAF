from random import choice

from BucketLib.bucket import Bucket
from cb_constants import CbServer
from bucket_collections.collections_base import CollectionBase
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient
from shell_util.remote_connection import RemoteMachineShellConnection


class KvOsoBackfillTests(CollectionBase):
    def setUp(self):
        super(KvOsoBackfillTests, self).setUp()
        self.shell_conns = dict()
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        self.rest = RestConnection(self.cluster.master)
        for node in self.cluster.nodes_in_cluster:
            self.shell_conns[node.ip] = RemoteMachineShellConnection(node)

    def tearDown(self):
        # Closing shell connections
        [shell.disconnect() for shell in self.shell_conns.values()]
        super(KvOsoBackfillTests, self).tearDown()

    def test_dcp_backfill_config(self):
        """
        - Check default value is "auto"
        - Set 'oso_dcp_backfill' value using the method 'set_oso_config_using'
        - Create Indexes over the available collections
        - Run few queries on the created collections to validate the doc count
        """

        err_pattern = dict()
        err_itrs = self.input.param("err_itrs", 5)
        self.log.info("Disabling auto-failover")
        self.rest.update_autofailover_settings(False, 500)
        if self.simulate_error is not None:
            err_itrs = self.input.param("err_itrs", 5)
            for index in range(err_itrs):
                t_node = choice(self.cluster.kv_nodes)
                cb_err = CouchbaseError(self.log, self.shell_conns[t_node.ip])
                err_pattern[index] = (t_node, cb_err)

        sdk_client = SDKClient(self.cluster, self.cluster.buckets[0])
        self.log.info("Creating deferred indexes on collections")
        for bucket in self.cluster.buckets:
            for s_name, scope in bucket.scopes.items():
                for c_name, col in scope.collections.items():
                    query = 'create index `{0}_{1}_{2}_index` on ' \
                            '`{0}`.`{1}`.`{2}`(name) WITH ' \
                            '{{ "defer_build": true, "num_replica": 1 }};' \
                            .format(bucket.name, s_name, c_name)
                    self.log.debug("Creating index {}".format(query))
                    sdk_client.cluster.query(query)

        self.log.info("Building created indexes...")
        for bucket in self.cluster.buckets:
            for s_name, scope in bucket.scopes.items():
                for c_name, col in scope.collections.items():
                    query = "BUILD INDEX on `{0}`.`{1}`.`{2}`" \
                            "(`{0}_{1}_{2}_index`) USING GSI" \
                            .format(bucket.name, s_name, c_name)
                    try:
                        sdk_client.cluster.query(query)
                    except InternalServerFailureException as e:
                        if "Build Already In Progress" in str(e):
                            pass

        if err_pattern:
            self.log.info("Simulating error")
            for index in range(err_itrs):
                cb_err = err_pattern[index][1]
                try:
                    cb_err.create(self.simulate_error)
                except Exception as e:
                    self.log.critical(e)
                self.sleep(1)
                cb_err.revert(self.simulate_error)
                if index != err_itrs-1:
                    self.sleep(5)
                self.bucket_util.is_warmup_complete(self.cluster.buckets)

        self.log.info("Waiting for index build to complete")
        for bucket in self.cluster.buckets:
            for s_name, scope in bucket.scopes.items():
                for c_name, col in scope.collections.items():
                    i_name = '{0}_{1}_{2}_index'.format(bucket.name, s_name,
                                                        c_name)
                    query = "SELECT state FROM system:all_indexes WHERE " \
                            "name='{}'".format(i_name)
                    retry = 300
                    while retry > 0:
                        state = sdk_client.cluster.query(query)\
                            .rowsAsObject()[0].get("state")
                        if state == "online":
                            break
                        self.sleep(15, "Sleep before checking status for '{}'"
                                   .format(i_name))
                    else:
                        self.fail("{} - creation timed out".format(i_name))

        self.log.info("Validating num_items using created indexes")
        for _ in range(30):
            items_indexed = True
            for bucket in self.cluster.buckets:
                for s_name, scope in bucket.scopes.items():
                    if s_name == CbServer.system_scope:
                        # System query collection can have auto-generated docs
                        # due to the queries we run here. So skipping this
                        continue

                    for c_name, col in scope.collections.items():
                        query = "SELECT count(*) as num_items from " \
                                "`{0}`.`{1}`.`{2}`"\
                            .format(bucket.name, s_name, c_name)
                        doc_count = sdk_client.cluster.query(query)\
                            .rowsAsObject()[0].getNumber("num_items")
                        if err_pattern \
                                and bucket.bucketType == Bucket.Type.EPHEMERAL:
                            continue
                        if doc_count != col.num_items:
                            self.log.warn(
                                "Doc count mismatch. Exp {} != {} actual"
                                .format(col.num_items, doc_count))
                            items_indexed = False
                            break
            if items_indexed:
                break
            self.sleep(5, "Wait before next check")
        else:
            self.fail("Doc count mismatch in few index")
        # Close the SDK_client
        sdk_client.close()

        self.assertTrue(
            self.bucket_util.validate_oso_dcp_backfill_value(
                self.cluster.kv_nodes, self.cluster.buckets,
                self.oso_dcp_backfill),
            "oso_dcp_backfill value mismatch")
