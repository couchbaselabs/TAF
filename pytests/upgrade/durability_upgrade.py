from random import choice
from threading import Thread

from BucketLib.bucket import Bucket
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper, \
    BucketDurability
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException
from upgrade.upgrade_base import UpgradeBase


class UpgradeTests(UpgradeBase):
    def setUp(self):
        super(UpgradeTests, self).setUp()
        self.durability_helper = DurabilityHelper(
            self.log,
            len(self.cluster.nodes_in_cluster))
        self.verification_dict = dict()
        self.verification_dict["ops_create"] = self.num_items
        self.verification_dict["ops_delete"] = 0

    def tearDown(self):
        super(UpgradeTests, self).tearDown()

    def __trigger_cbcollect(self, log_path):
        self.log.info("Triggering cb_collect_info")
        rest = RestConnection(self.cluster.master)
        nodes = rest.get_nodes()
        status = self.cluster_util.trigger_cb_collect_on_cluster(rest,
                                                                 nodes)

        if status is True:
            self.cluster_util.wait_for_cb_collect_to_complete(rest)
            status = self.cluster_util.copy_cb_collect_logs(
                rest, nodes, self.cluster, log_path)
            if status is False:
                self.log_failure("API copy_cb_collect_logs detected failure")
        else:
            self.log_failure("API perform_cb_collect returned False")
        return status

    def test_upgrade(self):
        create_batch_size = 10000
        update_task = None

        t_durability_level = ""
        if self.cluster_supports_sync_write:
            t_durability_level = Bucket.DurabilityLevel.MAJORITY

        if self.upgrade_with_data_load:
            self.log.info("Starting async doc updates")
            update_task = self.task.async_continuous_doc_ops(
                self.cluster, self.bucket, self.gen_load,
                op_type="update",
                process_concurrency=1,
                persist_to=1,
                replicate_to=1,
                durability=t_durability_level,
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
            self.cluster_util.print_cluster_stats()

            # Validate sync_write results after upgrade
            if self.atomicity:
                create_batch_size = 10
                create_gen = doc_generator(
                    self.key,
                    self.num_items,
                    self.num_items+create_batch_size)
                sync_write_task = self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.bucket_util.buckets,
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
                    suppress_error_table=True)
            self.task_manager.get_task_result(sync_write_task)

            node_to_upgrade = self.fetch_node_to_upgrade()
            if self.atomicity:
                self.sleep(10)
                current_items = self.bucket_util.get_bucket_current_item_count(
                    self.cluster, self.bucket)
                if node_to_upgrade is None:
                    if current_items < self.num_items+create_batch_size:
                        self.log_failure(
                            "Failures after cluster upgrade {} {}"
                            .format(current_items,
                                    self.num_items+create_batch_size))
                elif current_items > self.num_items:
                    self.log_failure(
                        "SyncWrite succeeded with mixed mode cluster")
            else:
                if node_to_upgrade is None:
                    if sync_write_task.fail.keys():
                        self.log_failure("Failures after cluster upgrade")
                elif self.cluster_supports_sync_write:
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
                elif len(sync_write_task.fail.keys()) != create_batch_size:
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

    def test_bucket_durability_upgrade(self):
        update_task = None
        self.sdk_timeout = 60
        create_batch_size = 10000
        if self.atomicity:
            create_batch_size = 10

        # To make sure sync_write can we supported by initial cluster version
        sync_write_support = True
        if float(self.initial_version[0:3]) < 6.5:
            sync_write_support = False

        if sync_write_support:
            self.verification_dict["rollback_item_count"] = 0
            self.verification_dict["sync_write_aborted_count"] = 0

        if self.upgrade_with_data_load:
            self.log.info("Starting async doc updates")
            update_task = self.task.async_continuous_doc_ops(
                self.cluster, self.bucket, self.gen_load,
                op_type="update",
                process_concurrency=1,
                persist_to=1,
                replicate_to=1,
                timeout_secs=30)

        self.log.info("Upgrading cluster nodes to target version")
        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                     self.upgrade_version)
            try:
                self.cluster.update_master(self.cluster.servers[0])
            except Exception:
                self.cluster.update_master(
                    self.cluster.servers[self.nodes_init-1])

            create_gen = doc_generator(self.key, self.num_items,
                                       self.num_items+create_batch_size)
            # Validate sync_write results after upgrade
            if self.atomicity:
                sync_write_task = self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.bucket_util.buckets,
                    create_gen, "create",
                    process_concurrency=1,
                    transaction_timeout=self.transaction_timeout,
                    record_fail=True)
            else:
                sync_write_task = self.task.async_load_gen_docs(
                    self.cluster, self.bucket, create_gen, "create",
                    timeout_secs=self.sdk_timeout,
                    process_concurrency=4,
                    skip_read_on_error=True,
                    suppress_error_table=True)
            self.task_manager.get_task_result(sync_write_task)
            self.num_items += create_batch_size

            retry_index = 0
            while retry_index < 5:
                self.sleep(3, "Wait for num_items to match")
                current_items = self.bucket_util.get_bucket_current_item_count(
                    self.cluster, self.bucket)
                if current_items == self.num_items:
                    break
                self.log.debug("Num_items mismatch. Expected: %s, Actual: %s"
                               % (self.num_items, current_items))
            # Doc count validation
            self.cluster_util.print_cluster_stats()

            self.verification_dict["ops_create"] += create_batch_size
            self.summary.add_step("Upgrade %s" % node_to_upgrade.ip)

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure:
                break

            node_to_upgrade = self.fetch_node_to_upgrade()

        if self.upgrade_with_data_load:
            # Wait for update_task to complete
            update_task.end_task()
            self.task_manager.get_task_result(update_task)
        else:
            self.verification_dict["ops_update"] = 0

        # Cb_stats vb-details validation
        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket_util.buckets[0],
            self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=self.verification_dict)
        if failed:
            self.log_failure("Cbstat vbucket-details validation failed")
        self.summary.add_step("Cbstats vb-details verification")

        self.validate_test_failure()

        possible_d_levels = dict()
        possible_d_levels[Bucket.Type.MEMBASE] = \
            self.bucket_util.get_supported_durability_levels()
        possible_d_levels[Bucket.Type.EPHEMERAL] = [
            Bucket.DurabilityLevel.NONE,
            Bucket.DurabilityLevel.MAJORITY]
        len_possible_d_levels = len(possible_d_levels[self.bucket_type]) - 1

        if not sync_write_support:
            self.verification_dict["rollback_item_count"] = 0
            self.verification_dict["sync_write_aborted_count"] = 0

        # Perform bucket_durability update
        key, value = doc_generator("b_durability_doc", 0, 1).next()
        client = SDKClient([self.cluster.master], self.bucket_util.buckets[0])
        for index, d_level in enumerate(possible_d_levels[self.bucket_type]):
            self.log.info("Updating bucket_durability=%s" % d_level)
            self.bucket_util.update_bucket_property(
                self.bucket_util.buckets[0],
                bucket_durability=BucketDurability[d_level])
            self.bucket_util.print_bucket_stats()

            buckets = self.bucket_util.get_all_buckets()
            if buckets[0].durability_level != BucketDurability[d_level]:
                self.log_failure("New bucket_durability not taken")

            self.summary.add_step("Update bucket_durability=%s" % d_level)

            self.sleep(10, "MB-39678: Bucket_d_level change to take effect")

            if index == 0:
                op_type = "create"
                self.verification_dict["ops_create"] += 1
            elif index == len_possible_d_levels:
                op_type = "delete"
                self.verification_dict["ops_delete"] += 1
            else:
                op_type = "update"
                if "ops_update" in self.verification_dict:
                    self.verification_dict["ops_update"] += 1

            result = client.crud(op_type, key, value,
                                 timeout=self.sdk_timeout)
            if result["status"] is False:
                self.log_failure("Doc_op %s failed on key %s: %s"
                                 % (op_type, key, result["error"]))
            self.summary.add_step("Doc_op %s" % op_type)
        client.close()

        # Cb_stats vb-details validation
        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket_util.buckets[0],
            self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=self.verification_dict)
        if failed:
            self.log_failure("Cbstat vbucket-details validation failed")
        self.summary.add_step("Cbstats vb-details verification")
        self.validate_test_failure()

    def test_transaction_doc_isolation(self):
        def run_transaction_updates():
            self.log.info("Starting transaction updates in parallel")
            while not stop_thread:
                commit_trans = choice([True, False])
                trans_update_task = self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.bucket_util.buckets,
                    self.gen_load, "update", exp=self.maxttl,
                    batch_size=50,
                    process_concurrency=3,
                    timeout_secs=self.sdk_timeout,
                    update_count=self.update_count,
                    transaction_timeout=self.transaction_timeout,
                    commit=commit_trans,
                    durability=self.durability_level,
                    sync=self.sync, defer=self.defer,
                    retries=0)
                self.task_manager.get_task_result(trans_update_task)

        stop_thread = False
        update_task = None
        self.sdk_timeout = 60

        self.log.info("Upgrading cluster nodes to target version")
        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            if self.upgrade_with_data_load:
                update_task = Thread(target=run_transaction_updates)
                update_task.start()

            self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                     self.upgrade_version)
            try:
                self.cluster.update_master(self.cluster.servers[0])
            except Exception:
                self.cluster.update_master(
                    self.cluster.servers[self.nodes_init-1])

            if self.upgrade_with_data_load:
                stop_thread = True
                update_task.join()

            self.cluster_util.print_cluster_stats()
            self.bucket_util.print_bucket_stats()

            self.summary.add_step("Upgrade %s" % node_to_upgrade.ip)

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure:
                break

            node_to_upgrade = self.fetch_node_to_upgrade()
            for bucket in self.bucket_util.get_all_buckets():
                tombstone_doc_supported = \
                    "tombstonedUserXAttrs" in bucket.bucketCapabilities
                if node_to_upgrade is None and not tombstone_doc_supported:
                    self.log_failure("Tombstone docs not added to %s "
                                     "capabilities" % bucket.name)
                elif node_to_upgrade is not None and tombstone_doc_supported:
                    self.log_failure("Tombstone docs supported for %s before "
                                     "cluster upgrade" % bucket.name)

        self.validate_test_failure()

        create_gen = doc_generator(self.key, self.num_items,
                                   self.num_items*2)
        # Start transaction load after node upgrade
        trans_task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.bucket_util.buckets,
            create_gen, "create", exp=self.maxttl,
            batch_size=50,
            process_concurrency=8,
            timeout_secs=self.sdk_timeout,
            update_count=self.update_count,
            transaction_timeout=self.transaction_timeout,
            commit=True,
            durability=self.durability_level,
            sync=self.sync, defer=self.defer,
            retries=0)
        self.task_manager.get_task_result(trans_task)

    def test_cbcollect_info(self):
        log_path = self.input.param("logs_folder")
        self.log.info("Starting update tasks")
        update_tasks = list()
        update_tasks.append(self.task.async_continuous_doc_ops(
            self.cluster, self.bucket, self.gen_load,
            op_type="update",
            persist_to=1,
            replicate_to=1,
            process_concurrency=1,
            batch_size=10,
            timeout_secs=30))
        update_tasks.append(self.task.async_continuous_doc_ops(
            self.cluster, self.bucket, self.gen_load,
            op_type="update",
            replicate_to=1,
            process_concurrency=1,
            batch_size=10,
            timeout_secs=30))
        update_tasks.append(self.task.async_continuous_doc_ops(
            self.cluster, self.bucket, self.gen_load,
            op_type="update",
            persist_to=1,
            process_concurrency=1,
            batch_size=10,
            timeout_secs=30))

        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            # Cbcollect with mixed mode cluster
            status = self.__trigger_cbcollect(log_path)
            if status is False:
                break

            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                     self.upgrade_version)
            self.cluster_util.print_cluster_stats()

            try:
                self.cluster.update_master(self.cluster.servers[0])
            except Exception:
                self.cluster.update_master(
                    self.cluster.servers[self.nodes_init-1])

            node_to_upgrade = self.fetch_node_to_upgrade()

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure is True:
                break

        # Cbcollect with fully upgraded cluster
        self.__trigger_cbcollect(log_path)

        for update_task in update_tasks:
            # Wait for update_task to complete
            update_task.end_task()
            self.task_manager.get_task_result(update_task)

        self.validate_test_failure()
