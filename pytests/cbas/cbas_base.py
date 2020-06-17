from BucketLib.bucket import TravelSample, BeerSample, Bucket
from basetestcase import BaseTestCase
from cbas_utils.cbas_utils import CbasUtil
from com.couchbase.client.java.json import JsonObject
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.rest_client import RestHelper, RestConnection
from testconstants import FTS_QUOTA, CBAS_QUOTA, INDEX_QUOTA, MIN_KV_QUOTA

from cluster_utils.cluster_ready_functions import ClusterUtils
from bucket_utils.bucket_ready_functions import BucketUtils

import random


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
        self.cb_server_ip = self.cb_server_ip.replace('INVALID_IP', invalid_ip) if self.cb_server_ip is not None else None
        self.cbas_dataset_name = self.input.param("cbas_dataset_name", 'travel_ds')
        self.cbas_bucket_name_invalid = self.input.param('cbas_bucket_name_invalid', self.cbas_bucket_name)
        self.cbas_dataset2_name = self.input.param('cbas_dataset2_name', None)
        self.skip_create_dataset = self.input.param('skip_create_dataset', False)
        self.disconnect_if_connected = self.input.param('disconnect_if_connected', False)
        self.cbas_dataset_name_invalid = self.input.param('cbas_dataset_name_invalid', self.cbas_dataset_name)
        self.skip_drop_connection = self.input.param('skip_drop_connection', False)
        self.skip_drop_dataset = self.input.param('skip_drop_dataset', False)
        self.query_id = self.input.param('query_id', None)
        self.mode = self.input.param('mode', None)
        self.num_concurrent_queries = self.input.param('num_queries', 5000)
        self.concurrent_batch_size = self.input.param('concurrent_batch_size', 100)
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

        if hasattr(self, "cluster"):
            self._cb_cluster = self.cluster
        else:
            self._cb_cluster = self.get_clusters()
            
        self.expected_error = self.input.param("error", None)
        
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
                self.expected_error = self.expected_error.replace("INVALID_IP",invalid_ip)
                self.expected_error = self.expected_error.replace("PORT",self.cluster.master.port)
            self.otpNodes = []
            self.cbas_path = server.cbas_path
            self.rest = RestConnection(self.cluster.master)
            self.log.info("Setting the min possible memory quota so that adding more nodes to the cluster wouldn't be a problem.")
            self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=MIN_KV_QUOTA)
            self.rest.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=FTS_QUOTA)
            self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=INDEX_QUOTA)
            self.set_cbas_memory_from_available_free_memory = self.input.param('set_cbas_memory_from_available_free_memory', False)
            if self.expected_error:
                self.expected_error = self.expected_error.replace("INVALID_IP",invalid_ip)
                self.expected_error = self.expected_error.replace("PORT",self.cluster.master.port)
            
            if self.set_cbas_memory_from_available_free_memory:
                info = self.rest.get_nodes_self()
                self.cbas_memory_quota = int((info.memoryFree // 1024 ** 2) * 0.9)
                self.log.info("Setting %d memory quota for CBAS" % self.cbas_memory_quota)
                self.rest.set_service_memoryQuota(service='cbasMemoryQuota', memoryQuota=self.cbas_memory_quota)
            else:
                self.log.info("Setting %d memory quota for CBAS" % CBAS_QUOTA)
                self.cbas_memory_quota = CBAS_QUOTA
                self.rest.set_service_memoryQuota(service='cbasMemoryQuota', memoryQuota=CBAS_QUOTA)
            self.cbas_util = None
            if self.cluster.cbas_nodes:
                self.cbas_node = self.cluster.cbas_nodes[0]
                self.cbas_util = CbasUtil(self.cluster.master, self.cbas_node)
                if "cbas" in self.cluster.master.services:
                    self.cleanup_cbas()
                if add_default_cbas_node:
                    if self.cluster.master.ip != self.cbas_node.ip:
                        self.otpNodes.append(ClusterUtils(self.cluster, self.task_manager).add_node(self.cbas_node))
                    else:
                        self.otpNodes = self.rest.node_statuses()
                    ''' This cbas cleanup is actually not needed.
                        When a node is added to the cluster, it is automatically cleaned-up.'''
                    self.cleanup_cbas()
                    self.cluster.cbas_nodes.remove(self.cbas_node)
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
                self.sample_bucket = self.sample_bucket_dict[self.cb_bucket_name]
        
        else:
            # Multi Cluster Support
            for cluster in self._cb_cluster:
                
                cluster.cluster_util = ClusterUtils(cluster, self.task_manager)
                cluster.bucket_util = BucketUtils(cluster, cluster.cluster_util, self.task)
                
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
                    cluster.expected_error = self.expected_error.replace("INVALID_IP", invalid_ip)
                    cluster.expected_error = self.expected_error.replace("PORT", cluster.master.port)
                
                cluster.otpNodes = list()
                cluster.cbas_path = server.cbas_path
                
                cluster.rest = RestConnection(cluster.master)
                self.log.info("Setting the min possible memory quota so that adding "
                              "more nodes to the cluster wouldn't be a problem.")
                cluster.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=MIN_KV_QUOTA)
                cluster.rest.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=FTS_QUOTA)
                cluster.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=INDEX_QUOTA)
                cluster.set_cbas_memory_from_available_free_memory = self.input.param('set_cbas_memory_from_available_free_memory', False)
        
                
                if cluster.set_cbas_memory_from_available_free_memory:
                    info = cluster.rest.get_nodes_self()
                    cluster.cbas_memory_quota = int((info.memoryFree // 1024 ** 2) * 0.9)
                    self.log.info("Setting %d memory quota for CBAS" % cluster.cbas_memory_quota)
                    cluster.rest.set_service_memoryQuota(service='cbasMemoryQuota', memoryQuota=cluster.cbas_memory_quota)
                else:
                    self.log.info("Setting %d memory quota for CBAS" % CBAS_QUOTA)
                    cluster.cbas_memory_quota = CBAS_QUOTA
                    cluster.rest.set_service_memoryQuota(service='cbasMemoryQuota', memoryQuota=CBAS_QUOTA)
                
                
                cluster.cbas_util = None
                # Drop any existing buckets and datasets
                if cluster.cbas_nodes:
                    cluster.cbas_node = cluster.cbas_nodes[0]
                    cluster.cbas_util = CbasUtil(cluster.master, cluster.cbas_node, self.task)
                    if "cbas" in cluster.master.services:
                        self.cleanup_cbas(cluster.cbas_util)
                    if add_default_cbas_node:
                        if cluster.master.ip != cluster.cbas_node.ip:
                            cluster.otpNodes.append(cluster.cluster_util.add_node(cluster.cbas_node))
                        else:
                            cluster.otpNodes = cluster.rest.node_statuses()
                        """
                        This cbas cleanup is actually not needed.
                        When a node is added to the cluster,
                        it is automatically cleaned-up.
                        """
                        self.cleanup_cbas(cluster.cbas_util)
                        cluster.cbas_nodes.remove(cluster.cbas_node)
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
            if (results != None) & (len(results) > 0):
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
        :param cluster: cluster object for cluster on which this doc load operation has to be performed.
        :param buckets: list of buckets on which doc load operation has to be performed.
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
        :param wait_for_rebalance: boolean, wait for rebalance to finish after removing the nodes.
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
    
    def create_dataverse_link_map(self, cluster, dataverse=0, link=0):
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
        for i in range(1,dataverse+1):
            dataverse_name = "dataverse_{0}".format(str(i))
            if cluster.cbas_util.create_dataverse_on_cbas(dataverse_name=dataverse_name):
                dataverse_map[dataverse_name] = dict()
                if link and (link_created < link):
                    for j in range(1, random.randint(0, link - link_created)+1):
                        link_name = "link_{0}".format(str(j))
                        dataverse_map[dataverse_name][link_name] = dict()
                        link_created += 1
            else:
                self.log.error("Creation of dataverse {0} failed.".format(dataverse_name))
                for key in dataverse_map.keys():
                    if key != "Dafault":
                        cluster.cbas_util.drop_dataverse_on_cbas(dataverse_name=key)
                    del dataverse_map[key]
                raise Exception("Dataverse creation failed")
        while link_created < link:
            dataverse_map["Default"]["link_{0}".format(str(link_created))] = dict()
            link_created += 1
        return dataverse_map
    
    
    def create_aws_links_from_dataverse_map(self, region_list=None, verify_link_creation=False):
        """
        This function creates external links to AWS based on the dataverse hash map.
        :param region_list: List of valid AWS regions, where the links have to be created. 
        If not passed, then it will generate region list from aws_config file.
        :param verify_link_creation: verify whether the link was created successfully or not.
        :returns True if all the links were created, False even if one of the link creation fails.
        """
        if not region_list:
            region_list = self.aws_util.aws_config["Buckets"].keys()
        try:
            for dataverse,links in self.dataverse_map.iteritems():
                for link in links:
                    self.dataverse_map[dataverse][link]["region"] = random.choice(region_list)
                    self.dataverse_map[dataverse][link]["accessKeyId"] = self.aws_util.aws_config["Users"][
                        self.awsUser]["accessKeyId"]
                    self.dataverse_map[dataverse][link]["serviceEndpoint"] = self.aws_util.aws_config["Users"][
                        self.awsUser]["serviceEndpoint"]
                    self.dataverse_map[dataverse][link]["type"] = "s3"
                    self.dataverse_map[dataverse][link]["secretAccessKey"] = self.aws_util.aws_config["Users"][
                        self.awsUser]["secretAccessKey"]
                    if self.cluster.cbas_util.create_external_link_on_cbas(
                        link_name=link, dataverse=dataverse, link_type="s3", 
                        accessKeyId=self.dataverse_map[dataverse][link]["accessKeyId"],
                        secretAccessKey=self.dataverse_map[dataverse][link]["secretAccessKey"],
                        region=self.dataverse_map[dataverse][link]["region"],
                        serviceEndpoint= self.dataverse_map[dataverse][link]["serviceEndpoint"]):
                        self.dataverse_map[dataverse][link]["created"] = True
                    else:
                        return False
                    if verify_link_creation:
                        response = self.cluster.cbas_util.get_link_info(link_name=link, dataverse=dataverse, link_type="s3")
                        assert(response[0]["accessKeyId"] == self.aws_util.aws_config["Users"][self.awsUser]["accessKeyId"], 
                               "Access key id for link {0} does not match. \n Actual - {1}\nExpected - {2}".format(
                                   link, response[0]["accessKeyId"],
                                   self.aws_util.aws_config["Users"][self.awsUser]["accessKeyId"]))
                        assert(response[0]["region"] == self.dataverse_map[dataverse][link]["region"],
                               "Region does not match. \nActual - {0}\nExpected - {1}".format(
                                   response[0]["region"],
                                   self.dataverse_map[dataverse][link]["region"]))
                        assert(response[0]["serviceEndpoint"] == self.aws_util.aws_config["Users"][self.awsUser][
                            "serviceEndpoint"], "Service Endpoints do not match.\n Actual - {0}\nExpected - {1}".format(
                                response[0]["serviceEndpoint"], self.aws_util.aws_config["Users"][
                                    self.awsUser]["serviceEndpoint"]))
            return True
        except Exception:
            return False
    
    
    def create_datasets_map(self, num_of_datasets, file_formats= "", file_path="", 
                            object_construction_def="", redact_warning=False, 
                            header=False, null_string=None, randomize=False):
        """
        This function creates a hash map, depicting properties of each of the datasets within.
        :param num_of_datasets: total number of datasets to be created.
        :param file_formats : List of file formats for which datasets have to be created.
        If not provided, then dataset will be created on one of the supported file formats.
        JSON, CSV or TSV.
        :param file_path : relative path to AWS bucket from where the files are to be 
        read while querying the dataset. If not provided, random path will be chosen from aws_config
        :param object_construction_def: str, required only if the file format is csv or tsv
        :param redact_warning : True/False
        :param header : True/False, required only if the file format is csv or tsv
        :param null_string : str, required only if the file format is csv or tsv
        :param randomize : True/False, sets True/False values randomly for header and redact_warning.
        :returns hash map with dataset names as keys and associated properties as values.
        
        Sample dataset map: 
        dataset_map = {
                        "dataset_1": {
                            "dataset_property_1": "value",
                            "dataset_property_2": "value",
                            ...
                            },
                        "dataset_2": {
                            "dataset_property_1": "value",
                            "dataset_property_2": "value",
                            ...
                            },
                        ...
                        }
        """
        dataset_map = dict()
        for i in range(1, num_of_datasets + 1):
            dataset_name = "dataset_{0}".format(str(i))
            dataset_map[dataset_name] = dict()
            # to avoid choosing a dataverse with no links.
            while True:
                dataverse_name = random.choice(self.dataverse_map.keys())
                if len(self.dataverse_map[dataverse_name].keys()):
                    break
            
            link_name = random.choice(self.dataverse_map[dataverse_name].keys())
            region = self.dataverse_map[dataverse_name][link_name]["region"]
            bucket_name = random.choice(self.aws_util.aws_config["Buckets"][region].keys())
            if file_formats:
                if isinstance(file_formats, str):
                    file_format = file_formats
                else:
                    file_format = random.choice(file_formats)
            else:
                file_format = random.choice(self.aws_util.aws_config["Buckets"][region][bucket_name].keys())
            if not file_path:
                file_path = random.choice(self.aws_util.aws_config["Buckets"][region][bucket_name][file_format].keys())
            if file_path == "/":
                file_path = ""
            if not object_construction_def:
                object_construction_def = self.aws_util.aws_config["Buckets"][region][bucket_name][
                    file_format][file_path].get("object_construction_def", None)
            if not null_string:
                null_string = self.aws_util.aws_config["Buckets"][region][bucket_name][
                    file_format][file_path].get("null_string", None)
            if randomize:
                header = random.choice([True,False])
                redact_warning = random.choice([True,False])
            
            dataset_map[dataset_name]["dataverse"] = dataset_name
            dataset_map[dataset_name]["link_name"] = link_name
            dataset_map[dataset_name]["aws_bucket_name"] = bucket_name
            dataset_map[dataset_name]["file_format"] = file_format
            dataset_map[dataset_name]["file_path"] = file_path
            dataset_map[dataset_name]["object_construction_def"] = object_construction_def
            dataset_map[dataset_name]["null_string"] = null_string
            dataset_map[dataset_name]["header"] = header
            dataset_map[dataset_name]["redact_warning"] = redact_warning
            dataset_map[dataset_name]["total_records"] = self.aws_util.aws_config["Buckets"][region][
                bucket_name][file_format][file_path].get("total_num_records", 0)
        return dataset_map
    
    
    def create_datasets_from_dataset_map(self, dataset_map=None, verify_dataset_creation=False):
        """
        Creates datasets based on dataset maps.
        :param dataset_map: dict()
        :param verify_dataset_creation: bool, verifies whether an entry for the dataset created is 
        present in Metadata.Dataset collection.
        :return bool
        """
        if not dataset_map:
            dataset_map = self.dataset_map
        
        if dataset_map:
            for dataset_name in dataset_map:
                if self.cluster.cbas_util.create_dataset_on_external_resource(
                    cbas_dataset_name=dataset_name, 
                    aws_bucket_name=dataset_map[dataset_name]["aws_bucket_name"], 
                    link_name=dataset_map[dataset_name]["link_name"],
                    object_construction_def=dataset_map[dataset_name]["object_construction_def"], 
                    dataverse=dataset_map[dataset_name]["dataverse"], 
                    path_on_aws_bucket=dataset_map[dataset_name]["file_path"], 
                    file_format=dataset_map[dataset_name]["file_format"], 
                    redact_warning=dataset_map[dataset_name]["redact_warning"],
                    header=dataset_map[dataset_name]["header"], 
                    null_string=dataset_map[dataset_name]["null_string"]):
                    if verify_dataset_creation:
                        if not self.cluster.cbas_util.validate_dataset_in_metadata_collection(
                            dataset_name=dataset_name, 
                            link_name=dataset_map[dataset_name]["link_name"], 
                            dataverse=dataset_map[dataset_name]["dataverse"], 
                            aws_bucket_name=dataset_map[dataset_name]["aws_bucket_name"], 
                            file_format=dataset_map[dataset_name]["file_format"]):
                            self.log.error("Dataset {0} creation Failed".format(dataset_name))
                            return False
                    dataset_map[dataset_name]["created"] = True
                else:
                    self.log.error("Dataset {0} creation Failed".format(dataset_name))
                    return False
        else:
            self.log.error("Dataset map needs to be created first")
            return False
