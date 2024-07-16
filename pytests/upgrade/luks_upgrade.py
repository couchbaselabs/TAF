from couchbase_helper.documentgenerator import doc_generator
from sdk_exceptions import SDKException
from constants.sdk_constants.java_client import SDKConstants
from upgrade.upgrade_base import UpgradeBase


class LuksUpgrade(UpgradeBase):
    def setUp(self):
        super(LuksUpgrade, self).setUp()

        # Install Couchbase server on LUKS nodes
        self.upgrade_helper.install_version_on_nodes(
            self.cluster.servers[self.nodes_init:],
            self.upgrade_version[0])

    def tearDown(self):
        super(LuksUpgrade, self).tearDown()

    def test_upgrade_to_luks_cluster(self):
        create_batch_size = 20000
        update_task = None

        if self.upgrade_with_data_load:
            self.log.info("Starting async doc updates")
            update_task = self.task.async_continuous_doc_ops(
                self.cluster, self.bucket, self.gen_load,
                op_type="update",
                process_concurrency=1,
                persist_to=1,
                replicate_to=1,
                durability=SDKConstants.DurabilityLevel.PERSIST_TO_MAJORITY,
                timeout_secs=30)

        create_gen = doc_generator(self.key, self.num_items,
                                   self.num_items+create_batch_size)
        self.log.info("Upgrading cluster nodes to target version")
        nodes_to_upgrade = self.cluster.servers[:self.nodes_init]
        total_nodes_to_upgrade = self.nodes_init
        for index, node_to_upgrade in enumerate(nodes_to_upgrade):
            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            self.spare_node = self.cluster.servers[index+self.nodes_init]
            self.upgrade_function[self.upgrade_type](
                self.cluster.servers[index],
                self.upgrade_version[0],
                install_on_spare_node=False)
            self.cluster_util.print_cluster_stats(self.cluster)

            # Validate sync_write results after upgrade
            if self.atomicity:
                create_batch_size = 10
                create_gen = doc_generator(
                    self.key,
                    self.num_items,
                    self.num_items+create_batch_size)
                sync_write_task = self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.cluster.buckets,
                    create_gen, "create",
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
                    suppress_error_table=True,
                    load_using=self.load_docs_using)
            self.task_manager.get_task_result(sync_write_task)

            if self.atomicity:
                self.sleep(10)
                current_items = self.bucket_util.get_buckets_item_count(
                    self.cluster, self.bucket.name)
                if index == total_nodes_to_upgrade-1:
                    if current_items < self.num_items+create_batch_size:
                        self.log_failure(
                            "Failures after cluster upgrade {} {}"
                            .format(current_items,
                                    self.num_items+create_batch_size))
                elif current_items > self.num_items:
                    self.log_failure(
                        "SyncWrite succeeded with mixed mode cluster")
            else:
                if index == total_nodes_to_upgrade-1:
                    if sync_write_task.fail.keys():
                        self.log_failure("Failures after cluster upgrade")
                else:
                    if sync_write_task.fail:
                        self.log.error("SyncWrite failed: %s"
                                       % sync_write_task.fail)
                        self.log_failure("SyncWrite failed during upgrade")
                    else:
                        self.num_items += create_batch_size
                        create_gen = doc_generator(
                            self.key,
                            self.num_items,
                            self.num_items + create_batch_size)
                if len(sync_write_task.fail.keys()) != create_batch_size:
                    self.log_failure(
                        "SyncWrite succeeded with mixed mode cluster")
                else:
                    for doc_id, doc_result in sync_write_task.fail.items():
                        if SDKException.FeatureNotAvailableException \
                                not in str(doc_result["error"]):
                            self.log_failure("Invalid exception for %s: %s"
                                             % (doc_id, doc_result))

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure is not None:
                break

        if self.upgrade_with_data_load:
            # Wait for update_task to complete
            update_task.end_task()
            self.task_manager.get_task_result(update_task)

        self.validate_test_failure()
