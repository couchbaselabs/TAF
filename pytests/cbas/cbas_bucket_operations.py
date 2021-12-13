import random

from BucketLib.BucketOperations import BucketHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException
from cbas.cbas_base import CBASBaseTest
from collections_helper.collections_spec_constants import MetaCrudParams
from TestInput import TestInputSingleton
from com.couchbase.client.java.json import JsonObject
from couchbase_helper.documentgenerator import DocumentGenerator
from CbasLib.CBASOperations import CBASHelper


class CBASBucketOperations(CBASBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input
        if "cbas_spec" not in self.input.test_params:
            self.input.test_params.update(
                {"cbas_spec": "local_datasets"})

        if "bucket_spec" not in self.input.test_params:
            self.input.test_params.update(
                {"bucket_spec": "analytics.default"})
        self.input.test_params.update(
            {"cluster_kv_infra": "bkt_spec"})
        if "override_spec_params" not in self.input.test_params:
            self.input.test_params.update(
                {"override_spec_params": "num_items"})
        else:
            temp = self.input.test_params.get("override_spec_params")
            self.input.test_params.update(
                {"override_spec_params": temp + ";num_items"})

        super(CBASBucketOperations, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]
        self.disconnect_link = self.input.param('disconnect_link', False)

        self.setup_cbas_for_test()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(CBASBucketOperations, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def setup_cbas_for_test(self, update_spec={}, sub_spec_name="dataset"):
        if not update_spec:
            update_spec = {
                "no_of_dataverses": self.input.param('no_of_dv', 1),
                "no_of_datasets_per_dataverse": self.input.param('ds_per_dv', 1),
                "no_of_synonyms": 0,
                "no_of_indexes": self.input.param('no_of_idx', 1),
                "max_thread_count": self.input.param('no_of_threads', 1),
                "creation_methods": ["cbas_collection", "cbas_dataset"]}
        if self.cbas_spec_name:
            self.cbas_spec = self.cbas_util.get_cbas_spec(
                self.cbas_spec_name)
            if update_spec:
                self.cbas_util.update_cbas_spec(
                    self.cbas_spec, update_spec, sub_spec_name)
            cbas_infra_result = self.cbas_util.create_cbas_infra_from_spec(
                self.cluster, self.cbas_spec, self.bucket_util,
                wait_for_ingestion=True)
            if not cbas_infra_result[0]:
                self.fail(
                    "Error while creating infra from CBAS spec -- " +
                    cbas_infra_result[1])

    def CRUD_on_KV_docs(self, operations=["create"]):
        if self.disconnect_link:
            self.connect_disconnect_all_local_links(disconnect=True)
        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
            self.doc_spec_name)
        mutation_num = 0
        doc_loading_spec["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 0
        doc_loading_spec["doc_crud"][
            MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 0
        doc_loading_spec["doc_crud"][
            MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 0
        if "create" in operations:
            doc_loading_spec["doc_crud"][
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 100
        if "update_some" in operations:
            doc_loading_spec["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 50
            mutation_num = 1
        if "update_all" in operations:
            doc_loading_spec["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 100
            mutation_num = 1
        if "delete_some" in operations:
            doc_loading_spec["doc_crud"][
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 50
        if "delete_all" in operations:
            doc_loading_spec["doc_crud"][
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 100
        self.load_data_into_buckets(
            self.cluster, doc_loading_spec=doc_loading_spec, async_load=False,
            validate_task=True, mutation_num=mutation_num)

    def connect_disconnect_all_local_links(self, disconnect=False):
        if disconnect:
            for dataverse in self.cbas_util.dataverses:
                if not self.cbas_util.disconnect_link(
                        self.cluster, "{0}.Local".format(dataverse)):
                    self.fail("Error while disconnecting Local link for "
                              "dataverse {0}".format(dataverse))
        else:
            for dataverse in self.cbas_util.dataverses:
                if not self.cbas_util.connect_link(
                        self.cluster, "{0}.Local".format(dataverse)):
                    self.fail("Error while reconnecting Local link for "
                              "dataverse {0}".format(dataverse))

    def test_kv_doc_loading_before_dataset_disconnect(self):
        self.CRUD_on_KV_docs(["create"])
        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail(
                "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_kv_doc_loading_after_dataset_disconnect_and_then_reconnect_dataset(self):
        self.CRUD_on_KV_docs(["create"])
        self.connect_disconnect_all_local_links(disconnect=False)
        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail(
                "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_delete_some_docs_in_KV_before_dataset_disconnect(self):
        self.CRUD_on_KV_docs(["delete_some"])
        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail(
                "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_delete_some_docs_in_KV_after_dataset_disconnect_and_then_reconnect_dataset(self):
        self.CRUD_on_KV_docs(["delete_some"])
        self.connect_disconnect_all_local_links(disconnect=True)
        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail(
                "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_delete_all_docs_in_KV_before_dataset_disconnect(self):
        self.CRUD_on_KV_docs(["delete_all"])
        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail(
                "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_delete_all_docs_in_KV_after_dataset_disconnect_and_then_reconnect_dataset(self):
        self.CRUD_on_KV_docs(["delete_all"])
        self.connect_disconnect_all_local_links(disconnect=True)
        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail(
                "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_update_some_docs_in_KV_before_dataset_disconnect(self):
        self.CRUD_on_KV_docs(["update_some"])
        self.cbas_util.refresh_dataset_item_count(self.bucket_util)
        datasets = self.cbas_util.list_all_dataset_objs()

        for dataset in datasets:
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, dataset.num_of_items,
                    expected_mutated_count=dataset.num_of_items/2,
                    num_tries=12, timeout=300, analytics_timeout=300):
                self.fail(
                    "Dataset mutated doc count does not match the actual "
                    "mutated doc count in associated KV collection")

    def test_update_some_docs_in_KV_after_dataset_disconnect_and_then_reconnect_dataset(
            self):
        self.CRUD_on_KV_docs(["update_some"])
        self.connect_disconnect_all_local_links(disconnect=True)
        self.cbas_util.refresh_dataset_item_count(self.bucket_util)
        datasets = self.cbas_util.list_all_dataset_objs()

        for dataset in datasets:
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, dataset.num_of_items,
                    expected_mutated_count=dataset.num_of_items / 2,
                    num_tries=12, timeout=300, analytics_timeout=300):
                self.fail(
                    "Dataset mutated doc count does not match the actual "
                    "mutated doc count in associated KV collection")

    def test_update_all_docs_in_KV_before_dataset_disconnect(self):
        self.CRUD_on_KV_docs(["update_all"])
        self.cbas_util.refresh_dataset_item_count(self.bucket_util)
        datasets = self.cbas_util.list_all_dataset_objs()

        for dataset in datasets:
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, dataset.num_of_items,
                    expected_mutated_count=dataset.num_of_items,
                    num_tries=12, timeout=300, analytics_timeout=300):
                self.fail(
                    "Dataset mutated doc count does not match the actual "
                    "mutated doc count in associated KV collection")

    def test_update_all_docs_in_KV_after_dataset_disconnect_and_then_reconnect_dataset(
            self):
        self.CRUD_on_KV_docs(["update_all"])
        self.connect_disconnect_all_local_links(disconnect=True)
        self.cbas_util.refresh_dataset_item_count(self.bucket_util)
        datasets = self.cbas_util.list_all_dataset_objs()

        for dataset in datasets:
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, dataset.num_of_items,
                    expected_mutated_count=dataset.num_of_items,
                    num_tries=12, timeout=300, analytics_timeout=300):
                self.fail(
                    "Dataset mutated doc count does not match the actual "
                    "mutated doc count in associated KV collection")

    def test_CRUD_on_KV_before_dataset_disconnect(self):
        self.CRUD_on_KV_docs(["update_all", "delete_some", "create"])
        self.cbas_util.refresh_dataset_item_count(self.bucket_util)
        datasets = self.cbas_util.list_all_dataset_objs()
        for dataset in datasets:
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, dataset.num_of_items,
                    expected_mutated_count=dataset.num_of_items,
                    num_tries=12, timeout=300, analytics_timeout=300):
                self.fail(
                    "Dataset mutated doc count does not match the actual "
                    "mutated doc count in associated KV collection")

    def test_CRUD_on_KV_after_dataset_disconnect_and_then_reconnect_dataset(
            self):
        self.CRUD_on_KV_docs(["update_all", "delete_some", "create"])
        self.connect_disconnect_all_local_links(disconnect=True)
        self.cbas_util.refresh_dataset_item_count(self.bucket_util)
        datasets = self.cbas_util.list_all_dataset_objs()
        for dataset in datasets:
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, dataset.num_of_items,
                    expected_mutated_count=dataset.num_of_items,
                    num_tries=12, timeout=300, analytics_timeout=300):
                self.fail(
                    "Dataset mutated doc count does not match the actual "
                    "mutated doc count in associated KV collection")

    def test_KV_bucket_flush_before_dataset_disconnect(self):
        if self.input.param('flush_all_bucket', False):
            if not self.bucket_util.flush_all_buckets(self.cluster):
                self.fail("Flushing of buckets failed")
            datasets = self.cbas_util.list_all_dataset_objs()
        else:
            datasets = list()
            bucket = self.cluster.buckets[0]
            if not self.bucket_util.flush_bucket(self.cluster, bucket):
                self.fail("Flushing of buckets failed")
            for dataset in self.cbas_util.list_all_dataset_objs():
                if dataset.kv_bucket.name == bucket.name:
                    datasets.append(dataset)
        for dataset in datasets:
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, 0):
                self.fail(
                    "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_KV_bucket_flush_after_dataset_disconnect_and_then_reconnect_dataset(self):
        self.connect_disconnect_all_local_links(disconnect=False)
        if self.input.param('flush_all_bucket', False):
            if not self.bucket_util.flush_all_buckets(self.cluster):
                self.fail("Flushing of buckets failed")
            datasets = self.cbas_util.list_all_dataset_objs()
        else:
            datasets = list()
            bucket = self.cluster.buckets[0]
            if not self.bucket_util.flush_bucket(self.cluster, bucket):
                self.fail("Flushing of buckets failed")
            for dataset in self.cbas_util.list_all_dataset_objs():
                if dataset.kv_bucket.name == bucket.name:
                    datasets.append(dataset)
        self.connect_disconnect_all_local_links(disconnect=True)
        for dataset in datasets:
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, 0):
                self.fail(
                    "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_delete_kv_bucket_before_dataset_disconnect(self):
        if self.input.param('delete_all_bucket', False):
            try:
                self.bucket_util.delete_all_buckets(self.cluster)
            except Exception as e:
                self.fail(str(e))
            datasets = self.cbas_util.list_all_dataset_objs()
        else:
            datasets = list()
            bucket = self.cluster.buckets[0]
            if not self.bucket_util.delete_bucket(
                    self.cluster, bucket, wait_for_bucket_deletion=True):
                self.fail("Deleting of bucket failed")
            for dataset in self.cbas_util.list_all_dataset_objs():
                if dataset.kv_bucket.name == bucket.name:
                    datasets.append(dataset)
        for dataset in datasets:
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, 0):
                self.fail(
                    "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_delete_KV_bucket_after_dataset_disconnect_and_then_reconnect_dataset(
            self):
        self.connect_disconnect_all_local_links(disconnect=False)
        if self.input.param('delete_all_bucket', False):
            try:
                self.bucket_util.delete_all_buckets(self.cluster)
            except Exception as e:
                self.fail(str(e))
            datasets = self.cbas_util.list_all_dataset_objs()
        else:
            datasets = list()
            bucket = self.cluster.buckets[0]
            if not self.bucket_util.delete_bucket(
                    self.cluster, bucket, wait_for_bucket_deletion=True):
                self.fail("Deleting of bucket failed")
            for dataset in self.cbas_util.list_all_dataset_objs():
                if dataset.kv_bucket.name == bucket.name:
                    datasets.append(dataset)
        self.connect_disconnect_all_local_links(disconnect=True)
        for dataset in datasets:
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, 0):
                self.fail(
                    "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_delete_kv_collection_before_dataset_disconnect(self):
        datasets = self.cbas_util.list_all_dataset_objs()
        ds_to_be_deleted = random.sample(datasets, len(datasets)/2)
        for ds in ds_to_be_deleted:
            if not self.bucket_util.drop_collection(
                    self.cluster.master, ds.kv_bucket,
                    scope_name=ds.kv_scope.name,
                    collection_name=ds.kv_collection.name):
                self.fail("Failed to delete collection {0}.{1}.{2}".format(
                    ds.kv_bucket.name, ds.kv_scope.name, ds.kv_collection.name
                ))
        for dataset in ds_to_be_deleted:
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, 0):
                self.fail(
                    "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_delete_KV_collection_after_dataset_disconnect_and_then_reconnect_dataset(
            self):
        self.connect_disconnect_all_local_links(disconnect=True)
        datasets = self.cbas_util.list_all_dataset_objs()
        ds_to_be_deleted = random.sample(datasets, len(datasets) / 2)
        for ds in ds_to_be_deleted:
            if not self.bucket_util.drop_collection(
                    self.cluster.master, ds.kv_bucket,
                    scope_name=ds.kv_scope.name,
                    collection_name=ds.kv_collection.name):
                self.fail("Failed to delete collection {0}.{1}.{2}".format(
                    ds.kv_bucket.name, ds.kv_scope.name, ds.kv_collection.name
                ))
        self.connect_disconnect_all_local_links(disconnect=False)
        for dataset in ds_to_be_deleted:
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, 0):
                self.fail(
                    "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_compact_KV_bucket_before_dataset_disconnect(self):
        self.bucket_util._run_compaction(self.cluster, number_of_times=1)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail(
                "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_compact_kv_bucket_after_dataset_disconnect_and_then_reconnect_dataset(self):
        self.connect_disconnect_all_local_links(disconnect=True)
        self.bucket_util._run_compaction(self.cluster, number_of_times=1)
        self.connect_disconnect_all_local_links(disconnect=False)
        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail(
                "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_ingestion_after_kv_rollback(self):
        try:
            for kv_node in self.cluster.kv_nodes:
                for bucket in self.cluster.buckets:
                    self.log.info("Stopping persistence on {0} for bucket {1}"
                                  "".format(kv_node.ip, bucket.name))
                    mem_client = MemcachedClientHelper.direct_client(
                        kv_node, bucket)
                    mem_client.stop_persistence()

            # Perform Create, Update, Delete ops in the CB bucket
            self.log.info("Performing Mutations")
            first = ['james', 'sharon', 'dave', 'bill', 'mike', 'steve']
            profession = ['doctor', 'lawyer']

            template_obj = JsonObject.create()
            template_obj.put("number", 0)
            template_obj.put("first_name", "")
            template_obj.put("profession", "")
            template_obj.put("mutated", 0)
            template_obj.put("mutation_type", "ADD")
            doc_gen = DocumentGenerator(
                "test_docs", template_obj, start=0, end=1000,
                randomize=False, first_name=first, profession=profession,
                number=range(70))

            try:
                self.bucket_util.sync_load_all_buckets(
                    self.cluster, doc_gen, "create", 0,
                    batch_size=1000,
                    durability=self.durability_level,
                    suppress_error_table=True)
            except Exception as e:
                self.fail("Following error occurred while loading bucket - {"
                          "0}".format(str(e)))

            datasets = list()
            for dataset in self.cbas_util.list_all_dataset_objs():
                if dataset.kv_scope and dataset.kv_scope.name == \
                        "_default" and dataset.kv_collection.name\
                        == "_default":
                    dataset.num_of_items = dataset.num_of_items + 1000
                    datasets.append(dataset)

            for dataset in datasets:
                if not self.cbas_util.validate_cbas_dataset_items_count(
                        self.cluster, dataset.full_name, dataset.num_of_items):
                    raise Exception("Dataset doc count does not match the "
                                    "actual doc count in associated KV collection")

            # Kill memcached on Master Node so that another KV node becomes master
            self.log.info("Kill Memcached process on Master node")
            shell = RemoteMachineShellConnection(self.cluster.master)
            shell.kill_memcached()

            failover_nodes = list()
            # Start persistence on Node
            for kv_node in self.cluster.kv_nodes:
                if kv_node.ip != self.cluster.master.ip:
                    failover_nodes.append(kv_node)
                    for bucket in self.cluster.buckets:
                        self.log.info("Starting persistence on {0} for bucket {1}"
                                      "".format(kv_node.ip, bucket.name))
                        mem_client = MemcachedClientHelper.direct_client(
                            kv_node, bucket)
                        mem_client.start_persistence()

            # Failover Nodes on which persistence was started
            self.log.info("Failing over Nodes - {0}".format(str(failover_nodes)))
            result = self.task.failover(
                servers=self.cluster.servers, failover_nodes=failover_nodes,
                graceful=False, use_hostnames=False, wait_for_pending=0,
                allow_unsafe=False, all_at_once=False)
            self.log.info(str(result))

            # Wait for Failover & CBAS rollback to complete
            self.sleep(120)

            bucket_wise_collection_item_count = dict()
            for bucket in self.cluster.buckets:
                bucket_wise_collection_item_count[
                    bucket.name] = self.bucket_util.get_doc_count_per_collection(
                    self.cluster, bucket)
            self.log.info(str(bucket_wise_collection_item_count))

            # Verify the doc count in datasets
            for dataset in self.cbas_util.list_all_dataset_objs():
                if not self.cbas_util.validate_cbas_dataset_items_count(
                        self.cluster, dataset.full_name,
                        bucket_wise_collection_item_count[
                            dataset.kv_bucket.name][dataset.kv_scope.name][
                            dataset.kv_collection.name]["items"]):
                    raise Exception(
                        "Dataset doc count does not match the actual doc count "
                        "in associated KV collection after KV roll back")
        except Exception as e:
            for kv_node in self.cluster.kv_nodes:
                for bucket in self.cluster.buckets:
                    self.log.info("Starting persistence on {0} for bucket {1}"
                                  "".format(kv_node.ip, bucket.name))
                    mem_client = MemcachedClientHelper.direct_client(
                        kv_node, bucket)
                    mem_client.start_persistence()
            self.fail(str(e))


    def test_bucket_flush_while_index_are_created(self):

        self.connect_disconnect_all_local_links(disconnect=True)

        self.log.info('Create secondary index in Async')
        index_fields = self.input.param("index_fields", None)
        index_fields = index_fields.replace('-', ',')
        dataset = self.cbas_util.list_all_dataset_objs()[0]
        query = "create index sec_idx on {0}({1});" \
            .format(dataset.full_name, index_fields)
        create_index_task = self.task.async_cbas_query_execute(
            self.cluster, self.cbas_util, "/analytics/service", query)

        self.log.info('Flush bucket while index are getting created')
        # Flush the CB bucket
        if not self.bucket_util.flush_bucket(self.cluster, dataset.kv_bucket,
                                             skip_resetting_num_items=False):
            self.fail("Flushing of bucket failed")

        self.log.info('Get result on index creation')
        self.task_manager.get_task_result(create_index_task)

        self.log.info('Reconnect Links')
        self.connect_disconnect_all_local_links()

        self.log.info('Validate no. of items in CBAS dataset')
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cluster, dataset.full_name, 0):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the KV bucket")

    def test_kill_memcached_impact_on_bucket(self):

        self.log.info('Kill memcached service in all cluster nodes')
        for node in self.cluster.servers:
            RemoteMachineShellConnection(node).kill_memcached()

        self.log.info('Validate document count')
        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail(
                "Dataset doc count does not match the actual doc count in associated KV collection")

    def test_restart_kv_server_impact_on_bucket(self):

        self.log.info('Restart couchbase')
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.reboot_server_and_wait_for_cb_run(self.cluster_util,
                                                self.cluster.master)

        dataset = self.cbas_util.list_all_dataset_objs()[0]
        self.log.info('Validate document count')
        count_n1ql = self.cluster.rest.query_tool(
            'select count(*) from %s'
            % CBASHelper.format_name(dataset.kv_bucket.name))["results"][0][
            "$1"]
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cluster, dataset.full_name, count_n1ql):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the KV bucket")


class CBASEphemeralBucketOperations(CBASBucketOperations):

    def setUp(self):
        super(CBASEphemeralBucketOperations, self).setUp()
        self.bucket_ram = self.input.param("bucket_size", 100)
        self.document_ram_percentage = self.input.param(
            "document_ram_percentage", 0.90)
        self.bucket_name = self.cluster.buckets[0].name

    def load_document_until_ram_percentage(self):
        self.start = 0
        doc_batch_size = 5000
        self.end = doc_batch_size
        bucket_helper = BucketHelper(self.cluster.master)
        mem_cap = (self.document_ram_percentage
                   * self.bucket_ram
                   * 1000000)

        first = ['james', 'sharon', 'dave', 'bill', 'mike', 'steve']
        profession = ['doctor', 'lawyer']

        template_obj = JsonObject.create()
        template_obj.put("number", 0)
        template_obj.put("first_name", "")
        template_obj.put("profession", "")
        template_obj.put("mutated", 0)
        template_obj.put("mutation_type", "ADD")

        while True:
            self.log.info("Add documents to bucket")

            doc_gen = DocumentGenerator(
                "test_docs", template_obj, start=self.start, end=self.end,
                randomize=False, first_name=first, profession=profession,
                number=range(70))

            try:
                self.bucket_util.sync_load_all_buckets(
                    self.cluster, doc_gen, "create", 0, batch_size=doc_batch_size,
                    durability=self.durability_level, suppress_error_table=True)
            except Exception as e:
                self.fail("Following error occurred while loading bucket - {"
                          "0}".format(str(e)))

            self.log.info("Calculate available free memory")
            bucket_json = bucket_helper.get_bucket_json(self.bucket_name)
            mem_used = 0
            for node_stat in bucket_json["nodes"]:
                mem_used += node_stat["interestingStats"]["mem_used"]

            if mem_used < mem_cap:
                self.log.info("Memory used: %s < %s" % (mem_used, mem_cap))
                self.start = self.end
                self.end = self.end + doc_batch_size
                self.num_items = self.end
            else:
                break

    def test_no_eviction_impact_on_cbas(self):

        self.log.info("Add documents until ram percentage")
        self.load_document_until_ram_percentage()

        self.log.info("Fetch current document count")
        target_bucket = None
        buckets = self.bucket_util.get_all_buckets(self.cluster)
        for tem_bucket in buckets:
            if tem_bucket.name == self.bucket_name:
                target_bucket = tem_bucket
                break
        item_count = target_bucket.stats.itemCount
        self.log.info("Completed base load with %s items" % item_count)

        self.log.info("Load more until we are out of memory")
        client = SDKClient([self.cluster.master], target_bucket)
        i = item_count
        op_result = {"status": True}
        while op_result["status"] is True:
            op_result = client.crud(
                "create", "key-id" + str(i), '{"name":"dave"}',
                durability=self.durability_level)
            i += 1

        if SDKException.AmbiguousTimeoutException not in op_result["error"] \
                or SDKException.RetryReason.KV_TEMPORARY_FAILURE \
                not in op_result["error"]:
            client.close()
            self.fail("Invalid exception for OOM insert: %s" % op_result)

        self.log.info('Memory is full at {0} items'.format(i))
        self.log.info("As a result added more %s items" % (i - item_count))

        self.log.info("Fetch item count")
        target_bucket = None
        buckets = self.bucket_util.get_all_buckets(self.cluster)
        for tem_bucket in buckets:
            if tem_bucket.name == self.cb_bucket_name:
                target_bucket = tem_bucket
                break
        item_count_when_oom = target_bucket.stats.itemCount
        mem_when_oom = target_bucket.stats.memUsed
        self.log.info('Item count when OOM {0} and memory used {1}'
                      .format(item_count_when_oom, mem_when_oom))

        self.log.info("Validate document count on CBAS")
        count_n1ql = self.rest.query_tool(
            'select count(*) from %s'
            % self.bucket_name)['results'][0]['$1']

        dataset = self.cbas_util.list_all_dataset_objs()[0]
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cluster, dataset.full_name, count_n1ql):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the KV bucket")

    def test_nru_eviction_impact_on_cbas(self):

        self.log.info("Add documents until ram percentage")
        self.load_document_until_ram_percentage()

        self.log.info("Fetch current document count")
        target_bucket = None
        buckets = self.bucket_util.get_all_buckets(self.cluster)
        for tem_bucket in buckets:
            if tem_bucket.name == self.bucket_name:
                target_bucket = tem_bucket
                break

        item_count = target_bucket.stats.itemCount
        self.log.info("Completed base load with %s items" % item_count)

        self.log.info("Get initial inserted 100 docs, so they aren't removed")
        client = SDKClient([self.cluster.master], target_bucket)
        for doc_index in range(100):
            doc_key = "test_docs-" + str(doc_index)
            client.read(doc_key)

        self.log.info("Add 20% more items to trigger NRU")
        for doc_index in range(item_count, int(item_count * 1.2)):
            doc_key = "key_id-" + str(doc_index)
            op_result = client.crud("create",
                                    doc_key,
                                    '{"name":"dave"}',
                                    durability=self.durability_level)
            if op_result["status"] is False:
                self.log.warning("Insert failed for %s: %s"
                                 % (doc_key, op_result))

        # Disconnect the SDK client
        client.close()

        self.log.info("Validate document count on CBAS")
        count_n1ql = self.rest.query_tool(
            'select count(*) from %s'
            % self.bucket_name)['results'][0]['$1']
        dataset = self.cbas_util.list_all_dataset_objs()[0]
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cluster, dataset.full_name, count_n1ql):
            self.log.info("Document count mismatch might be due to ejection "
                          "of documents on KV. Retry again")
            count_n1ql = self.rest.query_tool(
                'select count(*) from %s'
                % self.bucket_name)['results'][0]['$1']
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, count_n1ql):
                self.fail("No. of items in CBAS dataset do not match "
                          "that in the KV bucket")
