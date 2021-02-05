import random
import traceback

from math import ceil

from BucketLib.bucket import TravelSample, BeerSample, Bucket
from basetestcase import BaseTestCase
from bucket_utils.bucket_ready_functions import BucketUtils
from cbas_utils.cbas_utils import CbasUtil
# TODO: Need to removed once the old CbasUtil is deprecated.
from cbas_utils.cbas_utils_v2 import CbasUtil as CbasUtilV2
from cluster_utils.cluster_ready_functions import ClusterUtils
from collections_helper.collections_spec_constants import \
    MetaConstants, MetaCrudParams
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.rest_client import RestHelper, RestConnection
from rbac_utils.Rbac_ready_functions import RbacUtils
from sdk_exceptions import SDKException
from testconstants import FTS_QUOTA, CBAS_QUOTA, INDEX_QUOTA, MIN_KV_QUOTA

from com.couchbase.client.java.json import JsonObject
from java.lang import Exception as Java_base_exception


class CBASBaseTest(BaseTestCase):
    def setUp(self, add_default_cbas_node=True):
        super(CBASBaseTest, self).setUp()

        if self._testMethodDoc:
            self.log.info("Starting Test: %s - %s"
                          % (self._testMethodName, self._testMethodDoc))
        else:
            self.log.info("Starting Test: %s" % self._testMethodName)

        invalid_ip = '10.111.151.109'
        self.cb_bucket_name = self.input.param('cb_bucket_name',
                                               'travel-sample')
        self.cbas_bucket_name = self.input.param('cbas_bucket_name', 'travel')
        self.cb_bucket_password = self.input.param('cb_bucket_password', None)
        self.cb_server_ip = self.input.param("cb_server_ip", None)
        self.cb_server_ip = \
            self.cb_server_ip.replace('INVALID_IP', invalid_ip) \
            if self.cb_server_ip is not None else None
        self.cbas_dataset_name = self.input.param("cbas_dataset_name",
                                                  'travel_ds')
        self.cbas_bucket_name_invalid = \
            self.input.param('cbas_bucket_name_invalid', self.cbas_bucket_name)
        self.cbas_dataset2_name = self.input.param('cbas_dataset2_name', None)
        self.skip_create_dataset = self.input.param('skip_create_dataset',
                                                    False)
        self.disconnect_if_connected = \
            self.input.param('disconnect_if_connected', False)
        self.cbas_dataset_name_invalid = \
            self.input.param('cbas_dataset_name_invalid',
                             self.cbas_dataset_name)
        self.skip_drop_connection = self.input.param('skip_drop_connection',
                                                     False)
        self.skip_drop_dataset = self.input.param('skip_drop_dataset', False)
        self.query_id = self.input.param('query_id', None)
        self.mode = self.input.param('mode', None)
        self.num_concurrent_queries = self.input.param('num_queries', 5000)
        self.concurrent_batch_size = self.input.param('concurrent_batch_size',
                                                      100)
        self.compiler_param = self.input.param('compiler_param', None)
        self.compiler_param_val = self.input.param('compiler_param_val', None)
        self.expect_reject = self.input.param('expect_reject', False)
        self.expect_failure = self.input.param('expect_failure', False)
        self.compress_dataset = self.input.param('compress_dataset', False)
        self.index_name = self.input.param('index_name', "NoName")
        self.index_fields = self.input.param('index_fields', None)
        if self.index_fields:
            self.index_fields = self.index_fields.split("-")
        self.retry_time = self.input.param("retry_time", 300)
        self.num_retries = self.input.param("num_retries", 1)
        self.sample_bucket_dict = {TravelSample().name: TravelSample(),
                                   BeerSample().name: BeerSample()}
        self.sample_bucket = None
        self.flush_enabled = Bucket.FlushBucket.ENABLED
        self.test_abort_snapshot = self.input.param("test_abort_snapshot",
                                                    False)
        self.cbas_spec_name = self.input.param("cbas_spec", None)

        if hasattr(self, "cluster"):
            self._cb_cluster = self.cluster
        else:
            self._cb_cluster = self.get_clusters()

        self.expected_error = self.input.param("error", None)

        self.bucket_spec = self.input.param("bucket_spec", None)
        self.doc_spec_name = self.input.param("doc_spec_name", "initial_load")
        self.set_cbas_memory_from_available_free_memory = self.input.param(
                'set_cbas_memory_from_available_free_memory', False)

        # Single cluster support
        if hasattr(self, "cluster"):
            for server in self.servers:
                if "cbas" in server.services:
                    self.cluster.cbas_nodes.append(server)
                if "kv" in server.services:
                    self.cluster.kv_nodes.append(server)
                rest = RestConnection(server)
                rest.set_data_path(data_path=server.data_path,
                                   index_path=server.index_path,
                                   cbas_path=server.cbas_path)
            if self.expected_error:
                self.expected_error = \
                    self.expected_error.replace("INVALID_IP", invalid_ip)
                self.expected_error = \
                    self.expected_error.replace("PORT",
                                                self.cluster.master.port)
            self.otpNodes = []
            self.cbas_path = server.cbas_path
            self.rest = RestConnection(self.cluster.master)
            if not self.set_cbas_memory_from_available_free_memory: 
                self.log.info(
                    "Setting the min possible memory quota so that adding "
                    "more nodes to the cluster wouldn't be a problem.")
                self.rest.set_service_memoryQuota(
                    service='memoryQuota', memoryQuota=MIN_KV_QUOTA)
                self.rest.set_service_memoryQuota(
                    service='ftsMemoryQuota', memoryQuota=FTS_QUOTA)
                self.rest.set_service_memoryQuota(
                    service='indexMemoryQuota', memoryQuota=INDEX_QUOTA)
                self.set_cbas_memory_from_available_free_memory = \
                    self.input.param(
                        'set_cbas_memory_from_available_free_memory', False)
    
                self.log.info("Setting %d memory quota for CBAS"
                              % CBAS_QUOTA)
                self.cbas_memory_quota = CBAS_QUOTA

                self.rest.set_service_memoryQuota(
                    service='cbasMemoryQuota', memoryQuota=CBAS_QUOTA)
            if self.expected_error:
                self.expected_error = \
                    self.expected_error.replace("INVALID_IP", invalid_ip)
                self.expected_error = \
                    self.expected_error.replace("PORT",
                                                self.cluster.master.port)

            self.cbas_util = None
            if self.cluster.cbas_nodes:
                self.cbas_node = self.cluster.cbas_nodes[0]
                if self.set_cbas_memory_from_available_free_memory:
                    self.set_memory_for_services(
                        self.rest, self.cluster_util, self.cbas_node, self.cbas_node.services)
                self.cbas_util = CbasUtil(self.cluster.master, self.cbas_node)
                self.cbas_util_v2 = CbasUtilV2(self.cluster.master, self.cbas_node)
                if "cbas" in self.cluster.master.services:
                    self.cleanup_cbas()
                if add_default_cbas_node:
                    if self.cluster.master.ip != self.cbas_node.ip:
                        self.otpNodes.append(
                            ClusterUtils(self.cluster, self.task_manager)
                            .add_node(self.cbas_node))
                    else:
                        self.otpNodes = self.rest.node_statuses()
                    ''' This cbas cleanup is actually not needed.
                        When a node is added to the cluster, 
                        it is automatically cleaned-up.'''
                    self.cleanup_cbas()
                    self.cluster.cbas_nodes.remove(self.cbas_node)
            if self.bucket_spec is not None:
                try:
                    self.collectionSetUp(self.cluster, self.bucket_util,
                                         self.cluster_util)
                except Java_base_exception as exception:
                    self.handle_collection_setup_exception(exception)
                except Exception as exception:
                    self.handle_collection_setup_exception(exception)
            else:
                if self.default_bucket:
                    self.bucket_util.create_default_bucket(
                        bucket_type=self.bucket_type,
                        ram_quota=self.bucket_size,
                        replica=self.num_replicas,
                        conflict_resolution=self.bucket_conflict_resolution_type,
                        replica_index=self.bucket_replica_index,
                        storage=self.bucket_storage,
                        eviction_policy=self.bucket_eviction_policy,
                        flush_enabled=self.flush_enabled)
                elif self.cb_bucket_name in self.sample_bucket_dict.keys():
                    self.sample_bucket = \
                        self.sample_bucket_dict[self.cb_bucket_name]

        else:
            # Multi Cluster Support
            for cluster in self._cb_cluster:

                cluster.cluster_util = ClusterUtils(cluster, self.task_manager)
                cluster.bucket_util = BucketUtils(cluster,
                                                  cluster.cluster_util,
                                                  self.task)

                for server in cluster.servers:
                    if "cbas" in server.services:
                        cluster.cbas_nodes.append(server)
                    if "kv" in server.services:
                        cluster.kv_nodes.append(server)
                    rest = RestConnection(server)
                    rest.set_data_path(data_path=server.data_path,
                                       index_path=server.index_path,
                                       cbas_path=server.cbas_path)

                if self.expected_error:
                    cluster.expected_error = \
                        self.expected_error.replace("INVALID_IP", invalid_ip)
                    cluster.expected_error = \
                        self.expected_error.replace("PORT",
                                                    cluster.master.port)

                cluster.otpNodes = list()
                cluster.cbas_path = server.cbas_path

                cluster.rest = RestConnection(cluster.master)
                
                if not self.set_cbas_memory_from_available_free_memory: 
                    self.log.info(
                        "Setting the min possible memory quota so that adding "
                        "more nodes to the cluster wouldn't be a problem.")
                    cluster.rest.set_service_memoryQuota(
                        service='memoryQuota', memoryQuota=MIN_KV_QUOTA)
                    cluster.rest.set_service_memoryQuota(
                        service='ftsMemoryQuota', memoryQuota=FTS_QUOTA)
                    cluster.rest.set_service_memoryQuota(
                        service='indexMemoryQuota', memoryQuota=INDEX_QUOTA)
                    cluster.set_cbas_memory_from_available_free_memory = \
                        self.input.param(
                            'set_cbas_memory_from_available_free_memory', False)
        
                    self.log.info("Setting %d memory quota for CBAS"
                                  % CBAS_QUOTA)
                    cluster.cbas_memory_quota = CBAS_QUOTA
    
                    cluster.rest.set_service_memoryQuota(
                        service='cbasMemoryQuota', memoryQuota=CBAS_QUOTA)

                cluster.cbas_util = None
                # Drop any existing buckets and datasets
                if cluster.cbas_nodes:
                    cluster.cbas_node = cluster.cbas_nodes[0]
                    if self.set_cbas_memory_from_available_free_memory:
                        self.set_memory_for_services(cluster.rest, cluster.cluster_util, 
                                                     cluster.cbas_node, cluster.cbas_node.services)
                    cluster.cbas_util = CbasUtil(cluster.master,
                                                 cluster.cbas_node,
                                                 self.task)
                    cluster.cbas_util_v2 = CbasUtilV2(cluster.master, cluster.cbas_node)
                    if "cbas" in cluster.master.services:
                        self.cleanup_cbas(cluster.cbas_util)
                    if add_default_cbas_node:
                        if cluster.master.ip != cluster.cbas_node.ip:
                            cluster.otpNodes.append(
                                cluster.cluster_util
                                .add_node(cluster.cbas_node))
                        else:
                            cluster.otpNodes = cluster.rest.node_statuses()
                        """
                        This cbas cleanup is actually not needed.
                        When a node is added to the cluster,
                        it is automatically cleaned-up.
                        """
                        self.cleanup_cbas(cluster.cbas_util)
                        cluster.cbas_nodes.remove(cluster.cbas_node)
                if self.bucket_spec is not None:
                    try:
                        self.collectionSetUp(cluster, cluster.bucket_util,
                                             cluster.cluster_util)
                    except Java_base_exception as exception:
                        self.handle_collection_setup_exception(exception)
                    except Exception as exception:
                        self.handle_collection_setup_exception(exception)
                else:
                    if self.default_bucket:
                        cluster.bucket_util.create_default_bucket(
                            bucket_type=self.bucket_type,
                            ram_quota=self.bucket_size,
                            replica=self.num_replicas,
                            conflict_resolution=self.bucket_conflict_resolution_type,
                            replica_index=self.bucket_replica_index,
                            storage=self.bucket_storage,
                            eviction_policy=self.bucket_eviction_policy,
                            flush_enabled=self.flush_enabled)
                    elif self.cb_bucket_name in self.sample_bucket_dict.keys():
                        self.sample_bucket = self.sample_bucket_dict[self.cb_bucket_name]

                cluster.bucket_util.add_rbac_user()
        self.log.info("=== CBAS_BASE setup was finished for test #{0} {1} ==="
                      .format(self.case_number, self._testMethodName))

    def tearDown(self):
        if hasattr(self, "cluster"):
            self.cbas_util.closeConn()
        else:
            for cluster in self._cb_cluster:
                if cluster.cbas_util:
                    cluster.cbas_util.closeConn()
        super(CBASBaseTest, self).tearDown()

    def cleanup_cbas(self, cbas_util=None):
        """
        Drops all connections, datasets and buckets from CBAS
        :param cbas_util: CbasUtil object.
        """
        if not cbas_util:
            cbas_util = self.cbas_util
        try:
            # Disconnect from all connected buckets
            cmd_get_buckets = "select Name from Metadata.`Bucket`;"
            status, metrics, errors, results, _ = cbas_util.execute_statement_on_cbas_util(cmd_get_buckets)
            if (results is not None) & (len(results) > 0):
                for row in results:
                    cbas_util.disconnect_from_bucket(row['Name'], disconnect_if_connected=True)
                    self.log.info("******* Disconnected all buckets *******")
            else:
                self.log.info("******* No buckets to disconnect *******")

            # Drop all datasets
            cmd_get_datasets = "select DatasetName from Metadata.`Dataset` where DataverseName != \"Metadata\";"
            status, metrics, errors, results, _ = cbas_util.execute_statement_on_cbas_util(cmd_get_datasets)
            if (results is not None) & (len(results) > 0):
                for row in results:
                    cbas_util.drop_dataset("`" + row['DatasetName'] + "`")
                    self.log.info("********* Dropped all datasets *********")
            else:
                self.log.info("********* No datasets to drop *********")

            # Drop all buckets
            status, metrics, errors, results, _ = cbas_util.execute_statement_on_cbas_util(cmd_get_buckets)
            if (results is not None) & (len(results) > 0):
                for row in results:
                    cbas_util.drop_cbas_bucket("`" + row['Name'] + "`")
                    self.log.info("********* Dropped all buckets *********")
            else:
                self.log.info("********* No buckets to drop *********")

            self.log.info("Drop Dataverse other than Default and Metadata")
            cmd_get_dataverse = 'select DataverseName from Metadata.`Dataverse` where DataverseName != "Metadata" and DataverseName != "Default";'
            status, metrics, errors, results, _ = cbas_util.execute_statement_on_cbas_util(cmd_get_dataverse)
            if (results is not None) & (len(results) > 0):
                for row in results:
                    cbas_util.disconnect_link("`" + row['DataverseName'] + "`" + ".Local")
                    cbas_util.drop_dataverse_on_cbas(dataverse_name="`" + row['DataverseName'] + "`")
                self.log.info("********* Dropped all dataverse except Default and Metadata *********")
            else:
                self.log.info("********* No dataverse to drop *********")
        except Exception as e:
            self.log.info(e.message)

    def perform_doc_ops_in_all_cb_buckets(self, operation,
                                          start_key=0, end_key=1000,
                                          batch_size=10, exp=0,
                                          _async=False,
                                          durability="",
                                          mutation_num=0,
                                          cluster=None,
                                          buckets=[],
                                          key=None):
        """
        Create/Update/Delete docs in all cb buckets
        :param operation: String - "create","update","delete"
        :param start_key: Doc Key to start the operation with
        :param end_key: Doc Key to end the operation with
        :param batch_size: Batch size of doc_ops
        :param exp: MaxTTL used for doc operations
        :param _async: Boolean to decide whether to start ops in parallel
        :param durability: Durability level to use for doc operation
        :param mutation_num: Mutation count to keep track per doc_loading
        :param cluster: cluster object for cluster on which this doc load
                        operation has to be performed.
        :param buckets: list of buckets on which doc load operation
                        has to be performed.
        :param key: key for the generated docs
        :return:
        """
        first = ['james', 'sharon', 'dave', 'bill', 'mike', 'steve']
        profession = ['doctor', 'lawyer']

        template_obj = JsonObject.create()
        template_obj.put("number", 0)
        template_obj.put("first_name", "")
        template_obj.put("profession", "")
        template_obj.put("mutated", mutation_num)
        template_obj.put("mutation_type", "ADD")

        if not key:
            key = "test_docs"

        doc_gen = DocumentGenerator(key, template_obj,
                                    start=start_key, end=end_key,
                                    randomize=False,
                                    first_name=first, profession=profession,
                                    number=range(70))
        if cluster:
            bucket_util = cluster.bucket_util
        else:
            cluster = self.cluster
            bucket_util = self.bucket_util
        try:
            if _async:
                if buckets:
                    for bucket in buckets:
                        return bucket_util.async_load_bucket(
                            cluster, bucket, doc_gen, operation, exp,
                            durability=durability,
                            batch_size=batch_size,
                            suppress_error_table=True)
                else:
                    return bucket_util._async_load_all_buckets(
                        cluster, doc_gen, operation, exp,
                        durability=durability,
                        batch_size=batch_size,
                        suppress_error_table=True)
            else:
                bucket_util.sync_load_all_buckets(
                    cluster, doc_gen, operation, exp,
                    durability=durability,
                    batch_size=batch_size,
                    suppress_error_table=True)
        except Exception as e:
            self.log.error(e.message)

    def remove_node(self, otpnode=None, wait_for_rebalance=True, rest=None):
        """
        Method to remove nodes from a cluster.
        :param otpnode: list of nodes to be removed.
        :param wait_for_rebalance: boolean, wait for rebalance to finish
                                   after removing the nodes.
        :param rest: RestConnection object
        """
        if not rest:
            rest = self.rest
        nodes = rest.node_statuses()
        '''This is the case when master node is running cbas service as well'''
        if len(nodes) <= len(otpnode):
            return

        helper = RestHelper(rest)
        try:
            removed = helper.remove_nodes(
                knownNodes=[node.id for node in nodes],
                ejectedNodes=[node.id for node in otpnode],
                wait_for_rebalance=wait_for_rebalance)
        except Exception:
            self.sleep(5, "Rebalance failed on Removal. Retry.. THIS IS A BUG")
            removed = helper.remove_nodes(
                knownNodes=[node.id for node in nodes],
                ejectedNodes=[node.id for node in otpnode],
                wait_for_rebalance=wait_for_rebalance)
        if wait_for_rebalance:
            self.assertTrue(removed,
                            "Rebalance operation failed while removing %s"
                            % otpnode)

    def create_dataverse_link_map(self, cbas_util, dataverse=0, link=0):
        """
        This function creates a hash map, depicting links in different dataverses.
        :param dataverse: Number of dataverses to be created. Default value of 0 will not create any dataverse,
         and any link if present will be associated with the "Default" dataverse.
        :param link: total number of links to be created.
        :returns hash map with dataverse names as keys and associated links as values.

        Sample dataverse map:
        Note - Default dataverse will always be present
        Note - 2 different dataverses can have links with same name.
        dataverse_map = {
                            "dataverse1": {
                                "link_1" : {
                                    "link_property_1": "value",
                                    "link_property_2": "value",
                                    ...
                                },
                                "link_2" : {
                                    "link_property_1": "value",
                                    "link_property_2": "value",
                                    ...
                                }
                            },
                            "Default": {
                                "link_1" : {
                                    "link_property_1": "value",
                                    "link_property_2": "value",
                                    ...
                                }
                            }
                        }
        """
        dataverse_map = dict()
        dataverse_map["Default"] = dict()
        link_created = 0
        for i in range(1, dataverse+1):
            dataverse_name = "dataverse_{0}".format(str(i))
            if cbas_util.create_dataverse_on_cbas(dataverse_name=dataverse_name):
                dataverse_map[dataverse_name] = dict()
                if link and (link_created < link):
                    for j in range(1, random.randint(0, link-link_created)+1):
                        link_name = "link_{0}".format(str(j))
                        dataverse_map[dataverse_name][link_name] = dict()
                        link_created += 1
            else:
                self.log.error("Creation of dataverse %s failed."
                               % dataverse_name)
                for key in dataverse_map.keys():
                    if key != "Dafault":
                        cbas_util.drop_dataverse_on_cbas(dataverse_name=key)
                    del dataverse_map[key]
                raise Exception("Dataverse creation failed")
        while link_created < link:
            dataverse_map["Default"]["link_{0}".format(str(link_created))] = dict()
            link_created += 1
        return dataverse_map

    def create_or_delete_users(self, rbac_util, rbac_users_created,
                               delete=False):
        """
        Creates all types of rbac users.
        """
        if delete:
            for user in rbac_users_created:
                try:
                    rbac_util._drop_user(user)
                    del(rbac_users_created[user])
                except:
                    pass
        else:
            for role in RbacUtils.cb_server_roles:
                if "[*]" in role:
                    user = role.replace("[*]", "")
                else:
                    user = role
                rbac_users_created[user] = role
                rbac_util._create_user_and_grant_role(user, role)

    def create_testcase_for_rbac_user(self, description, rbac_users_created):
        testcases = []
        for user in rbac_users_created:
            if user in ["admin", "analytics_admin", self.analytics_username]:
                test_params = {
                    "description": description.format(user),
                    "validate_error_msg": False
                    }
            elif user in ["security_admin_local", "security_admin_external", "query_external_access",
                          "query_system_catalog", "replication_admin",
                          "ro_admin", "bucket_full_access",
                          "replication_target", "mobile_sync_gateway",
                          "data_reader", "data_writer",
                          "data_dcp_reader", "data_monitoring",
                          "views_admin",  "views_reader",
                          "query_delete", "query_insert",
                          "query_manage_index",  "query_select",
                          "query_update", "fts_admin", "fts_searcher",
                          "cluster_admin","bucket_admin"]:
                test_params = {
                    "description": description.format(user),
                    "validate_error_msg": True,
                    "expected_error": "User must have permission",
                }
            else:
                test_params = {"description": description.format(user),
                               "validate_error_msg": True,
                               "expected_error": "Unauthorized user"}
            test_params["username"] = user
            testcases.append(test_params)
        return testcases

    def remove_and_return_new_list(self, itemlist, item_to_remove):
        try:
            itemlist.remove(item_to_remove)
        except Exception:
            pass
        finally:
            return itemlist

    def set_primary_index(self, rest, bucket_name):
        query = "CREATE PRIMARY INDEX ON `{0}`;".format(bucket_name)
        result = rest.query_tool(query)
        if result["status"] == "success":
            return True
        else:
            return False

    def convert_string_to_bool(self, value):
        if isinstance(value, str) or isinstance(value, unicode):
            if value.lower() == "true":
                return True
            elif value.lower() == "false":
                return False
            else:
                return value

    def handle_collection_setup_exception(self, exception_obj):
        if self.sdk_client_pool is not None:
            self.sdk_client_pool.shutdown()
        traceback.print_exc()
        raise exception_obj

    def collectionSetUp(self, cluster, bucket_util, cluster_util, load_data=True, buckets_spec=None, doc_loading_spec=None):
        """
        Setup the buckets, scopes and collecitons based on the spec passed.
        """
        self.over_ride_spec_params = self.input.param(
            "override_spec_params", "").split(";")
        self.remove_default_collection = self.input.param(
            "remove_default_collection", False)

        # Create bucket(s) and add rbac user
        bucket_util.add_rbac_user()
        if not buckets_spec:
            buckets_spec = bucket_util.get_bucket_template_from_package(
                self.bucket_spec)
        
        # Process params to over_ride values if required
        self.over_ride_bucket_template_params(buckets_spec)

        bucket_util.create_buckets_using_json_data(buckets_spec)
        bucket_util.wait_for_collection_creation_to_complete()

        # Prints bucket stats before doc_ops
        bucket_util.print_bucket_stats()

        # Init sdk_client_pool if not initialized before
        if self.sdk_client_pool is None:
            self.init_sdk_pool_object()

        # Create clients in SDK client pool
        if self.sdk_client_pool:
            self.log.info("Creating required SDK clients for client_pool")
            bucket_count = len(bucket_util.buckets)
            max_clients = self.task_manager.number_of_threads
            clients_per_bucket = int(ceil(max_clients / bucket_count))
            for bucket in bucket_util.buckets:
                self.sdk_client_pool.create_clients(
                    bucket,
                    [cluster.master],
                    clients_per_bucket,
                    compression_settings=self.sdk_compression)

        # TODO: remove this once the bug is fixed
        #self.sleep(120, "MB-38497")
        self.sleep(10, "MB-38497")
        cluster_util.print_cluster_stats()

        if load_data:
            self.load_data_into_buckets(cluster, bucket_util, doc_loading_spec)
    
    def load_data_into_buckets(self,cluster, bucket_util, doc_loading_spec=None):
        """
        Loads data into buckets using the data spec
        """
        if not doc_loading_spec:
            doc_loading_spec = bucket_util.get_crud_template_from_package(self.doc_spec_name)
        
        self.over_ride_doc_loading_template_params(doc_loading_spec)

        # MB-38438, adding CollectionNotFoundException in retry exception
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS].append(
            SDKException.CollectionNotFoundException)
        
        doc_loading_task = bucket_util.run_scenario_from_spec(
            self.task, cluster, bucket_util.buckets, doc_loading_spec, mutation_num=0, batch_size=self.batch_size)
        
        if doc_loading_task.result is False:
            self.fail("Initial reloading failed")
        
        ttl_buckets = [
            "multi_bucket.buckets_for_rebalance_tests_with_ttl",
            "multi_bucket.buckets_all_membase_for_rebalance_tests_with_ttl",
            "volume_templates.buckets_for_volume_tests_with_ttl"]

        # Verify initial doc load count
        bucket_util._wait_for_stats_all_buckets()
        if self.bucket_spec not in ttl_buckets:
            bucket_util.validate_docs_per_collections_all_buckets()

    def over_ride_bucket_template_params(self, bucket_spec):
        for over_ride_param in self.over_ride_spec_params:
            if over_ride_param == "replicas":
                bucket_spec[Bucket.replicaNumber] = self.num_replicas
            elif over_ride_param == "remove_default_collection":
                bucket_spec[MetaConstants.REMOVE_DEFAULT_COLLECTION] = \
                    self.remove_default_collection
            elif over_ride_param == "enable_flush":
                if self.input.param("enable_flush", False):
                    bucket_spec[Bucket.flushEnabled] = Bucket.FlushBucket.ENABLED
                else:
                    bucket_spec[Bucket.flushEnabled] = Bucket.FlushBucket.DISABLED
            elif over_ride_param == "num_buckets":
                bucket_spec[MetaConstants.NUM_BUCKETS] = int(
                    self.input.param("num_buckets", 1))
            elif over_ride_param == "bucket_size":
                if self.bucket_size == "auto":
                    cluster_info = self.rest.get_nodes_self()
                    kv_quota = cluster_info.__getattribute__("memoryQuota")
                    self.bucket_size = kv_quota // bucket_spec[MetaConstants.NUM_BUCKETS]
                bucket_spec[Bucket.ramQuotaMB] = self.bucket_size
            elif over_ride_param == "num_scopes":
                bucket_spec[MetaConstants.NUM_SCOPES_PER_BUCKET] = int(
                    self.input.param("num_scopes", 1))
            elif over_ride_param == "num_collections":
                bucket_spec[MetaConstants.NUM_COLLECTIONS_PER_SCOPE] = int(
                    self.input.param("num_collections", 1))
            elif over_ride_param == "num_items":
                bucket_spec[MetaConstants.NUM_ITEMS_PER_COLLECTION] = \
                    self.num_items

    def over_ride_doc_loading_template_params(self, target_spec):
        for over_ride_param in self.over_ride_spec_params:
            if over_ride_param == "durability":
                target_spec[MetaCrudParams.DURABILITY_LEVEL] = \
                    self.durability_level
            elif over_ride_param == "sdk_timeout":
                target_spec[MetaCrudParams.SDK_TIMEOUT] = self.sdk_timeout
            elif over_ride_param == "doc_size":
                target_spec[MetaCrudParams.DocCrud.DOC_SIZE] = self.doc_size
    
    def set_memory_for_services(self, master_rest, cluster_util, server, services):
        services = services.split(",")
        if len(services) > 0:
            service_mem_dict = {
                "kv": ["memoryQuota",MIN_KV_QUOTA],
                "fts": ["ftsMemoryQuota",FTS_QUOTA],
                "index": ["indexMemoryQuota",INDEX_QUOTA],
                "cbas": ["cbasMemoryQuota",CBAS_QUOTA]}
            
            if "n1ql" in services:
                services.remove("n1ql")
            
            # Get all services that are already running in cluster
            cluster_services = cluster_util.get_services_map()
            cluster_info = master_rest.get_nodes_self()
            
            rest = RestConnection(server)
            info = rest.get_nodes_self()
            memory_quota_available = info.mcdMemoryReserved
            
            if len(services) == 1:
                service = services[0]
                if service in cluster_services:
                    if service is not "kv":
                        self.log.info("Setting {0} memory quota for {1}".format(memory_quota_available, service))
                        property_name = service_mem_dict[service][0]
                        service_mem_in_cluster = cluster_info.__getattribute__(property_name)
                        # if service is already in cluster we cannot increase the RAM allocation, but we can reduce the RAM allocation if needed.
                        if memory_quota_available < service_mem_in_cluster:
                            if memory_quota_available > service_mem_dict[service][1]:
                                master_rest.set_service_memoryQuota(service=property_name, memoryQuota=memory_quota_available)
                            else:
                                self.fail("Error while setting service memory quota {0} for {1}".format(service_mem_dict[service][1], service))                                    
                else:
                    self.log.info("Setting {0} memory quota for {1}".format(memory_quota_available, service))
                    if memory_quota_available > service_mem_dict[service][1]:
                        master_rest.set_service_memoryQuota(service=service_mem_dict[service][0], memoryQuota=memory_quota_available)
                    else:
                        self.fail("Error while setting service memory quota {0} for {1}".format(service_mem_dict[service][1], service))
            else:
                # if KV is present, then don't change the KV memory quota
                # It is assumed that KV node will always be present in the master node of cluster.
                if "kv" in services:
                    services.remove("kv")
                    memory_quota_available -= cluster_info.__getattribute__("memoryQuota")
                
                set_cbas_mem = False
                if "cbas" in services:
                    services.remove("cbas")
                    set_cbas_mem = True
                
                for service in services:
                    # setting minimum possible memory for other services.
                    self.log.info("Setting {0} memory quota for {1}".format(service_mem_dict[service][1], service))
                    if memory_quota_available >= service_mem_dict[service][1]:
                        master_rest.set_service_memoryQuota(service=service_mem_dict[service][0], memoryQuota=service_mem_dict[service][1])
                        memory_quota_available -= service_mem_dict[service][1]
                    else:
                        self.fail("Error while setting service memory quota {0} for {1}".format(service_mem_dict[service][1], service))
                
                if set_cbas_mem and memory_quota_available >= service_mem_dict["cbas"][1]:
                    if "cbas" in cluster_services:
                        if cluster_info.__getattribute__("cbasMemoryQuota") >= memory_quota_available:
                            self.log.info("Setting {0} memory quota for CBAS".format(memory_quota_available))
                            master_rest.set_service_memoryQuota(service="cbasMemoryQuota", memoryQuota=memory_quota_available) 
                    else: 
                        self.log.info("Setting {0} memory quota for CBAS".format(memory_quota_available))
                        master_rest.set_service_memoryQuota(service="cbasMemoryQuota", memoryQuota=memory_quota_available)
                else:
                    self.fail("Error while setting service memory quota {0} for CBAS".format(memory_quota_available))