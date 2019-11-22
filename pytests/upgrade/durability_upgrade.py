from couchbase_helper.documentgenerator import doc_generator
from sdk_exceptions import SDKException
from upgrade.upgrade_base import UpgradeBase


class UpgradeTests(UpgradeBase):
    def setUp(self):
        super(UpgradeTests, self).setUp()

    def tearDown(self):
        super(UpgradeTests, self).tearDown()

    def test_upgrade(self):
        create_batch_size = 10000
        update_task = None

        if self.upgrade_with_data_load:
            self.log.info("Starting async doc updates")
            update_task = self.task.async_continuous_update_docs(
                self.cluster, self.bucket, self.gen_load,
                process_concurrency=1,
                persist_to=1,
                replicate_to=1,
                timeout_secs=30)

        create_gen = doc_generator(self.key, self.num_items,
                                   self.num_items+create_batch_size)
        self.log.info("Upgrading cluster nodes to target version")
        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                     self.upgrade_version)
            try:
                self.cluster.update_master(self.cluster.servers[0])
            except:
                self.cluster.update_master(
                    self.cluster.servers[self.nodes_init-1])
            # Validate sync_write results after upgrade
            if self.atomicity:
                create_batch_size = 10
                create_gen = doc_generator(self.key, self.num_items,
                                   self.num_items+create_batch_size)
                sync_write_task = self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.bucket_util.buckets, create_gen, "create",
                    process_concurrency=1,
                    transaction_timeout=self.transaction_timeout,
                    record_fail=True)
            else:
                sync_write_task = self.task.async_load_gen_docs(
                    self.cluster, self.bucket, create_gen, "create",
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    process_concurrency=4,
                    skip_read_on_error=True,
                    suppress_error_table=True)
            self.task_manager.get_task_result(sync_write_task)

            node_to_upgrade = self.fetch_node_to_upgrade()
            if self.atomicity:
                self.sleep(10)
                current_items = self.bucket_util.get_bucket_current_item_count(
                    self.cluster, self.bucket)
                if node_to_upgrade is None:
                    if current_items < self.num_items+create_batch_size:
                        self.log_failure("Failures after cluster upgrade {} {}".format(current_items, self.num_items+create_batch_size))
                elif current_items > self.num_items :
                    self.log_failure("SyncWrite succeeded with mixed mode cluster")
            else:
                if node_to_upgrade is None:
                    if sync_write_task.fail.keys():
                        self.log_failure("Failures after cluster upgrade")
                elif len(sync_write_task.fail.keys()) != create_batch_size:
                    self.log_failure("SyncWrite succeeded with mixed mode cluster")
                else:
                    for doc_id, doc_result in sync_write_task.fail.items():
                        if SDKException.FeatureNotAvailableException \
                                not in str(doc_result["error"]):
                            self.log_failure("Invalid exception for %s: %s"
                                             % (doc_id, doc_result))

            self.cluster_util.print_cluster_stats()

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure is True:
                break

        if self.upgrade_with_data_load:
            # Wait for update_task to complete
            self.task_manager.stop_task(update_task)
            self.task_manager.get_task_result(update_task)

        self.log.info("Verifying async_writes")

        self.validate_test_failure()
