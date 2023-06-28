from basetestcase import ClusterSetup
from couchbase_helper.documentgenerator import doc_generator
from BucketLib.bucket import Bucket
from BucketLib.BucketOperations import BucketHelper
from membase.api.rest_client import RestConnection
from couchbase_helper.durability_helper import DurabilityHelper
from sdk_exceptions import SDKException


class BucketParamTest(ClusterSetup):
    def setUp(self):
        super(BucketParamTest, self).setUp()
        self.create_bucket(self.cluster)

        self.new_replica = self.input.param("new_replica", 1)
        self.src_bucket = self.bucket_util.get_all_buckets(self.cluster)

        # Reset active_resident_threshold to avoid further data load as DGM
        self.active_resident_threshold = 0
        self.def_bucket = self.bucket_util.get_all_buckets(self.cluster)[0]

        doc_create = doc_generator(self.key, 0, self.num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.cluster.vbuckets)

        if self.atomicity:
            task = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.cluster.buckets, doc_create,
                "create", 0, batch_size=20, process_concurrency=8,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                timeout_secs=self.sdk_timeout,
                transaction_timeout=self.transaction_timeout,
                commit=self.transaction_commit,
                durability=self.durability_level,
                sync=self.sync)
            self.task.jython_task_manager.get_task_result(task)
        else:
            for bucket in self.cluster.buckets:
                task = self.task.async_load_gen_docs(
                    self.cluster, bucket, doc_create, "create", 0,
                    persist_to=self.persist_to, replicate_to=self.replicate_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    batch_size=10, process_concurrency=8)
                self.task.jython_task_manager.get_task_result(task)

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Verify initial doc load count
        if not self.atomicity:
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                      self.num_items)
        self.log.info("==========Finished Bucket_param_test setup========")

    def tearDown(self):
        super(BucketParamTest, self).tearDown()

    def load_docs_atomicity(self, doc_ops, start_doc_for_insert, doc_count,
                            doc_update, doc_create, doc_delete):
        tasks = []
        if "update" in doc_ops:
            tasks.append(
                self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.cluster.buckets, doc_update,
                    "rebalance_only_update", 0, batch_size=20,
                    process_concurrency=8,
                    replicate_to=self.replicate_to,
                    persist_to=self.persist_to,
                    timeout_secs=self.sdk_timeout,
                    transaction_timeout=self.transaction_timeout,
                    update_count=self.update_count,
                    commit=self.transaction_commit,
                    durability=self.durability_level,
                    sync=self.sync))
            self.sleep(10, "To avoid overlap of multiple tasks in parallel")
        if "create" in doc_ops:
            tasks.append(
                self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.cluster.buckets, doc_create,
                    "create", 0, batch_size=20,
                    process_concurrency=8,
                    replicate_to=self.replicate_to,
                    persist_to=self.persist_to,
                    timeout_secs=self.sdk_timeout,
                    transaction_timeout=self.transaction_timeout,
                    commit=self.transaction_commit,
                    durability=self.durability_level,
                    sync=self.sync))
            doc_count += (doc_create.end - doc_create.start)
            start_doc_for_insert += self.num_items
        if "delete" in doc_ops:
            tasks.append(
                self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.cluster.buckets, doc_delete,
                    "rebalance_delete", 0, batch_size=20,
                    process_concurrency=8,
                    replicate_to=self.replicate_to,
                    persist_to=self.persist_to,
                    timeout_secs=self.sdk_timeout,
                    transaction_timeout=self.transaction_timeout,
                    commit=self.transaction_commit,
                    durability=self.durability_level,
                    sync=self.sync))
            doc_count -= (doc_delete.end - doc_delete.start)

        return tasks, doc_count, start_doc_for_insert

    def load_docs(self, doc_ops, start_doc_for_insert, doc_count, doc_update,
                  doc_create, doc_delete, suppress_error_table=False,
                  ignore_exceptions=[], retry_exceptions=[]):
        tasks_info = dict()
        if "create" in doc_ops:
            # Start doc create task in parallel with replica_update
            tasks_info.update(self.bucket_util._async_load_all_buckets(
                self.cluster, doc_create, "create", exp=0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=8,
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions,
                suppress_error_table=suppress_error_table))
            doc_count += (doc_create.end - doc_create.start)
            start_doc_for_insert += self.num_items
        if "update" in doc_ops:
            # Start doc update task in parallel with replica_update
            tasks_info.update(self.bucket_util._async_load_all_buckets(
                self.cluster, doc_update, "update", exp=0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=8,
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions,
                suppress_error_table=suppress_error_table))
        if "delete" in doc_ops:
            # Start doc update task in parallel with replica_update
            tasks_info.update(self.bucket_util._async_load_all_buckets(
                self.cluster, doc_delete, "delete", exp=0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=8,
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions,
                suppress_error_table=suppress_error_table))
            doc_count -= (doc_delete.end - doc_delete.start)

        return tasks_info, doc_count, start_doc_for_insert

    def doc_ops_operations(self, doc_ops, start_doc_for_insert, doc_count,
                           doc_update, doc_create, doc_delete,
                           suppress_error_table=False,
                           ignore_exceptions=[], retry_exceptions=[]):
        if self.atomicity:
            tasks, doc_count, start_doc_for_insert = self.load_docs_atomicity(
                doc_ops,
                start_doc_for_insert,
                doc_count,
                doc_update,
                doc_create,
                doc_delete)
        else:
            tasks, doc_count, start_doc_for_insert = self.load_docs(
                doc_ops,
                start_doc_for_insert,
                doc_count,
                doc_update,
                doc_create,
                doc_delete,
                suppress_error_table=suppress_error_table,
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)

        return tasks, doc_count, start_doc_for_insert

    def generic_replica_update(self, doc_count, doc_ops, bucket_helper_obj,
                               replicas_to_update, start_doc_for_insert):
        supported_d_levels = self.bucket_util.get_supported_durability_levels()
        for replica_num in replicas_to_update:
            # Creating doc creator to be used by test cases
            doc_create = doc_generator(self.key, start_doc_for_insert,
                                       start_doc_for_insert + self.num_items,
                                       key_size=self.key_size,
                                       doc_size=self.doc_size,
                                       doc_type=self.doc_type,
                                       vbuckets=self.cluster.vbuckets)

            # Creating doc updater to be used by test cases
            doc_update = doc_generator(
                self.key,
                start_doc_for_insert - (self.num_items/2),
                start_doc_for_insert,
                key_size=self.key_size,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                vbuckets=self.cluster.vbuckets)

            # Creating doc updater to be used by test cases
            doc_delete = doc_generator(
                self.key,
                start_doc_for_insert - self.num_items,
                start_doc_for_insert - (self.num_items/2),
                key_size=self.key_size,
                doc_size=self.doc_size, doc_type=self.doc_type,
                vbuckets=self.cluster.vbuckets)

            self.log.info("Updating replica count of bucket to {0}"
                          .format(replica_num))

            bucket_helper_obj.change_bucket_props(
                self.def_bucket, replicaNumber=replica_num)
            self.bucket_util.print_bucket_stats(self.cluster)

            d_impossible_exception = \
                SDKException.DurabilityImpossibleException
            ignore_exceptions = list()
            retry_exceptions = [SDKException.DurabilityAmbiguousException,
                                SDKException.AmbiguousTimeoutException]

            suppress_error_table = False
            if self.def_bucket.replicaNumber == 3 or replica_num == 3:
                doc_ops = "update"
                suppress_error_table = True
                ignore_exceptions = [d_impossible_exception]+retry_exceptions
                retry_exceptions = list()
            else:
                retry_exceptions.append(d_impossible_exception)

            tasks, doc_count, start_doc_for_insert = self.doc_ops_operations(
                doc_ops,
                start_doc_for_insert,
                doc_count,
                doc_update,
                doc_create,
                doc_delete,
                suppress_error_table=suppress_error_table,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)

            # Start rebalance task with doc_ops in parallel
            rebalance = self.task.async_rebalance(self.cluster, [], [])
            self.sleep(10, "Wait for rebalance to start")

            # Wait for all tasks to complete
            self.task.jython_task_manager.get_task_result(rebalance)
            if not rebalance.result:
                self.task_manager.abort_all_tasks()
                for task in tasks:
                    try:
                        for client in task.clients:
                            client.close()
                    except:
                        pass
                self.fail("Rebalance Failed")
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
            if not self.atomicity:
                # Wait for doc_ops to complete and retry & validate failures
                self.bucket_util.verify_doc_op_task_exceptions(tasks,
                                                               self.cluster)
                self.bucket_util.log_doc_ops_task_failures(tasks)

                for _, task_info in tasks.items():
                    self.assertFalse(
                        task_info["ops_failed"],
                        "Doc ops failure during %s" % task_info["op_type"])

            # Assert if rebalance failed
            self.assertTrue(rebalance.result,
                            "Rebalance failed after replica update")

            suppress_error_table = False
            if replica_num == 3:
                suppress_error_table = True

            ignore_exceptions = list()
            if replica_num == 3 and self.is_sync_write_enabled:
                ignore_exceptions.append(
                    SDKException.DurabilityImpossibleException)

            self.log.info("Performing doc_ops(update) after rebalance")
            tasks, _, _ = self.doc_ops_operations(
                "update",
                start_doc_for_insert,
                doc_count,
                doc_update,
                doc_create,
                doc_delete,
                suppress_error_table=suppress_error_table,
                ignore_exceptions=ignore_exceptions)

            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
            if not self.atomicity:
                # Wait for doc_ops to complete and retry & validate failures
                self.bucket_util.verify_doc_op_task_exceptions(tasks,
                                                               self.cluster)
                self.bucket_util.log_doc_ops_task_failures(tasks)

                for task, task_info in tasks.items():
                    if replica_num == 3:
                        if self.is_sync_write_enabled:
                            self.assertTrue(
                                len(task.fail.keys()) == (self.num_items/2),
                                "Few doc_ops succeeded while they should have failed.")
                        else:
                            self.assertTrue(
                                len(task.fail.keys()) == 0,
                                "Few doc_ops failed while they should have succeeded.")
                    self.assertFalse(
                        task_info["ops_failed"],
                        "Doc update failed after replica update rebalance")

            # Update the bucket's replica number
            self.def_bucket.replicaNumber = replica_num

            # Verify doc load count after each mutation cycle
            if not self.atomicity:
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets)
                self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                          doc_count)
        return doc_count, start_doc_for_insert

    def test_minimum_replica_update_with_non_admin_user(self):
        """ trying to set minimum replica setting of cluster from a non
        admin user"""
        self.test_user = None
        user_name = "Random"
        user_pass = "Random"
        permissions = 'data_writer[*],data_reader[*],query_insert[*],' \
                      'data_backup[*],query_select[*]'
        self.role_list = [{"id": user_name,
                           "name": user_name,
                           "roles": "{0}".format(permissions)}]
        self.test_user = [{'id': user_name, 'name': user_name,
                           'password': user_pass}]
        self.bucket_util.add_rbac_user(self.cluster.master,
                                       testuser=self.test_user,
                                       rolelist=self.role_list)
        rest = RestConnection(self.cluster.master)
        rest.username = self.test_user[0]["name"]
        rest.password = self.test_user[0]["password"]
        status = rest.set_minimum_bucket_replica_for_cluster(2)
        self.assertFalse(status[0], "Expected an exception while "
                                    "updating the min replica from a non "
                                    "admin user")

    def test_minimum_replica_setting(self):
        """
            Setting minimum bucket replica for cluster
            Creating bucket with different storage with replica = min_replica
            replica = min_replica + 1 and min_replica - 1

            Updating replicas of given buckets to  min_replica + 1 and  min_replica - 1
            while data loading, verifying expected error

           Setting minimum bucket replica for cluster again and verifying
           replica updates
        """
        def update_bucket_properties_and_re_balance(replica=None,
                                                 expected_fail=True,
                                                 history_retention_bytes=None,
                                                 history_retention_seconds=None,
                                                 ram_quota_mb=None,
                                                 compression_mode=Bucket.CompressionMode.ACTIVE,
                                                 max_ttl=50,
                                                 bucket_durability='none'):
            bucket_update_fail_count = 0
            for bucket in self.cluster.buckets:
                bucket_history_retention_bytes = None
                bucket_history_retention_seconds = None
                if bucket.replicaNumber == 3:
                    bucket_durability = 'none'
                if bucket.name == "magma":
                    bucket_history_retention_bytes = history_retention_bytes
                    bucket_history_retention_seconds = history_retention_seconds
                try:
                    self.bucket_util.update_bucket_property(
                        self.cluster.master, bucket, replica_number=replica,
                        history_retention_bytes=bucket_history_retention_bytes,
                        history_retention_seconds=bucket_history_retention_seconds,
                        ram_quota_mb=ram_quota_mb, max_ttl=max_ttl,
                        bucket_durability=bucket_durability,
                        compression_mode=compression_mode)
                except Exception as e:
                    if expected_fail:
                        bucket_update_fail_count += 1
                    else:
                        raise Exception(e)
                    continue
            rebalance = self.task.async_rebalance(self.cluster, [], [])
            self.task.jython_task_manager.get_task_result(rebalance)
            if expected_fail:
                self.assertTrue(
                    len(self.cluster.buckets) == bucket_update_fail_count,
                    "Bucket replica was not expected to be updated")

        buckets_properties = [
            {
                "type": "couchbase",
                "backend": Bucket.StorageBackend.magma,
                "replica": 3,
                "name": "magma"
            },
            {
                "type": "couchbase",
                "backend": Bucket.StorageBackend.magma,
                "replica": 1,
                "name": "magma1"
            },
            {
                "type": "couchbase",
                "backend": Bucket.StorageBackend.couchstore,
                "replica": 1,
                "name": "couchstore"
            },
            {
                "type": "ephemeral",
                "replica": 0,
                "backend": Bucket.StorageBackend.couchstore,
                "name": "ephemeral"
            }
        ]
        for bucket in buckets_properties:
            if self.minimum_bucket_replica is not None:
                self.num_replicas = self.minimum_bucket_replica
            else:
                self.num_replicas = self.num_replicas = bucket["replica"]
            self.bucket_storage = bucket["backend"]
            self.bucket_type = bucket["type"]
            self.create_bucket(self.cluster, bucket["name"])

        doc_create = doc_generator(self.key, 0, self.num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.cluster.vbuckets)
        data_load_task = []
        for bucket in self.cluster.buckets:
            task = \
                self.task.async_load_gen_docs(
                    self.cluster, bucket, doc_create, "create", 0,
                    persist_to=self.persist_to, replicate_to=self.replicate_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    batch_size=10, process_concurrency=8)
            data_load_task.append(task)

        if self.minimum_bucket_replica is not None:
            self.log.info("Creating buckets with different replicas")
            self.num_replicas = self.minimum_bucket_replica + 1
            for bucket in buckets_properties:
                self.bucket_storage = bucket["backend"]
                self.bucket_type = bucket["type"]
                self.create_bucket(self.cluster,
                                   bucket["name"] + "MoreReplica")

            self.num_replicas = self.minimum_bucket_replica - 1
            bucket_creation_fail_count = 0
            for bucket in buckets_properties:
                try:
                    self.bucket_storage = bucket["backend"]
                    self.bucket_type = bucket["type"]
                    self.create_bucket(self.cluster,
                                       bucket["name"] + "LessReplica")
                except Exception as e:
                    bucket_creation_fail_count += 1
                    continue
            self.assertTrue(
                len(buckets_properties) == bucket_creation_fail_count,
                "Bucket not expected to be created")
            self.log.info("Updating bucket replica")
            update_bucket_properties_and_re_balance(
                self.minimum_bucket_replica - 1)
            update_bucket_properties_and_re_balance(
                self.minimum_bucket_replica + 1, False)

        for task in data_load_task:
            self.task.jython_task_manager.get_task_result(task)

        self.log.info("Setting new minimum replica value for cluster")
        expect_min_replica_update_fail = self.input.param(
            "expect_min_replica_update_fail", False)
        new_minimum_replica = self.input.param("new_minimum_replica", 2)
        rest = RestConnection(self.cluster.master)
        status, content = rest.set_minimum_bucket_replica_for_cluster(
            new_minimum_replica)
        if expect_min_replica_update_fail:\
            self.assertFalse(status, "expected minimum replica setting "
                                     "update to fail")
        else:
            result = rest.get_minimum_bucket_replica_for_cluster()
            self.assertTrue(result == new_minimum_replica,
                            "minimum replica setting failed to update")
        data_load_task = []
        self.log.info("Data-load in old buckets post replica update")
        for bucket in self.cluster.buckets:
            task = \
                self.task.async_load_gen_docs(
                    self.cluster, bucket, doc_create, "update", 0,
                    persist_to=self.persist_to, replicate_to=self.replicate_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    batch_size=10, process_concurrency=8)
            data_load_task.append(task)
        for task in data_load_task:
            self.task.jython_task_manager.get_task_result(task)

        self.log.info("Updating bucket replica")
        update_bucket_properties_and_re_balance(ram_quota_mb=300,
                                                history_retention_bytes=2147483659,
                                                history_retention_seconds=2147483659,
                                                compression_mode=Bucket.CompressionMode.PASSIVE,
                                                max_ttl=100,
                                                bucket_durability='majority',
                                                expected_fail=False)
        if not expect_min_replica_update_fail:
            update_bucket_properties_and_re_balance(replica=new_minimum_replica - 1)
            update_bucket_properties_and_re_balance(
                replica=new_minimum_replica,
                expected_fail=False,
                ram_quota_mb=256,
                history_retention_bytes=2147483680,
                history_retention_seconds=2147483680,
                compression_mode=Bucket.CompressionMode.ACTIVE,
                max_ttl=50,
                bucket_durability='none')

    def test_replica_update(self):
        if self.atomicity:
            replica_count = 3
        else:
            replica_count = 4
        if self.nodes_init < 2:
            self.log.error("Test not supported for < 2 node cluster")
            return

        doc_ops = self.input.param("doc_ops", "")
        bucket_helper = BucketHelper(self.cluster.master)

        doc_count = self.num_items
        start_doc_for_insert = self.num_items

        self.is_sync_write_enabled = DurabilityHelper.is_sync_write_enabled(
            self.bucket_durability_level, self.durability_level)
        # Replica increment tests
        doc_count, start_doc_for_insert = self.generic_replica_update(
            doc_count,
            doc_ops,
            bucket_helper,
            range(1, min(replica_count, self.nodes_init)),
            start_doc_for_insert)

        # Replica decrement tests
        _, _ = self.generic_replica_update(
            doc_count,
            doc_ops,
            bucket_helper,
            range(min(replica_count, self.nodes_init)-2, -1, -1),
            start_doc_for_insert)

    def test_MB_34947(self):
        # Update already Created docs with async_writes
        load_gen = doc_generator(self.key, 0, self.num_items,
                                 key_size=self.key_size,
                                 doc_size=self.doc_size,
                                 doc_type=self.doc_type,
                                 vbuckets=self.cluster.vbuckets)
        task = self.task.async_load_gen_docs(
            self.cluster, self.def_bucket, load_gen, "update", 0,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            timeout_secs=self.sdk_timeout,
            batch_size=10, process_concurrency=8)
        self.task.jython_task_manager.get_task_result(task)

        # Update bucket replica to new value
        bucket_helper = BucketHelper(self.cluster.master)
        bucket_helper.change_bucket_props(
            self.def_bucket, replicaNumber=self.new_replica)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Start rebalance task
        rebalance = self.task.async_rebalance(self.cluster, [], [])
        self.sleep(10, "Wait for rebalance to start")

        # Wait for rebalance task to complete
        self.task.jython_task_manager.get_task_result(rebalance)

        # Assert if rebalance failed
        self.assertTrue(rebalance.result,
                        "Rebalance failed after replica update")
