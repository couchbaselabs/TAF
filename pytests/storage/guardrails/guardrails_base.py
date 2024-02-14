import math

from cb_constants.CBServer import CbServer
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.tuq_helper import N1QLHelper
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient
from storage.storage_base import StorageBase


class GuardrailsBase(StorageBase):
    def setUp(self):
        super(GuardrailsBase, self).setUp()

        self.couchstore_max_data_per_node = self.input.param("couchstore_max_data_per_node", 1.6)
        self.magma_max_data_per_node = self.input.param("magma_max_data_per_node", 16)
        self.max_disk_usage = self.input.param("max_disk_usage", 85)
        self.autoCompactionDefined = str(self.input.param("autoCompactionDefined", "false")).lower()
        self.test_query_mutations = self.input.param("test_query_mutations", True)
        self.test_read_traffic = self.input.param("test_read_traffic", True)

        self.cluster.kv_nodes = self.cluster_util.get_kv_nodes(self.cluster,
                                                       self.cluster.nodes_in_cluster)
        self.log.info("KV nodes {}".format(self.cluster.kv_nodes))


    def check_resident_ratio(self, cluster):
        """
        This function returns a dictionary which contains resident ratios
        of all the buckets in the cluster across all nodes.
        key = bucket name
        value = list of resident ratios of the bucket on different nodes
        Ex:  bucket_rr = {'default': [100, 100], 'bucket-1': [45.8, 50.1]}
        """
        bucket_rr = dict()
        for server in cluster.kv_nodes:
            kv_ep_max_size = dict()
            _, res = RestConnection(server).query_prometheus("kv_ep_max_size")
            for item in res["data"]["result"]:
                bucket_name = item["metric"]["bucket"]
                kv_ep_max_size[bucket_name] = float(item["value"][1])

            _, res = RestConnection(server).query_prometheus("kv_logical_data_size_bytes")
            for item in res["data"]["result"]:
                if item["metric"]["state"] == "active":
                    bucket_name = item["metric"]["bucket"]
                    logical_data_bytes = float(item["value"][1])
                    resident_ratio = (kv_ep_max_size[bucket_name] / logical_data_bytes) * 100
                    resident_ratio = min(resident_ratio, 100)
                    if bucket_name not in bucket_rr:
                        bucket_rr[bucket_name] = [resident_ratio]
                    else:
                        bucket_rr[bucket_name].append(resident_ratio)

        return bucket_rr

    def check_if_rr_guardrail_breached(self, bucket, current_rr, threshold):

        rr_bucket = current_rr[bucket.name]
        for rr_val in rr_bucket:
            if rr_val < threshold:
                return True

        return False

    def check_cm_resource_limit_reached(self, cluster, metric):

        result = dict()
        for server in cluster.kv_nodes:
            _, res = RestConnection(server).query_prometheus("cm_resource_limit_reached")

            for item in res["data"]["result"]:
                if item["metric"]["resource"] == metric:
                    if metric != "disk_usage":
                        bucket_name = item["metric"]["bucket"]
                        if bucket_name not in result:
                            result[bucket_name] = [int(item["value"][1])]
                        else:
                            result[bucket_name].append(int(item["value"][1]))
                    else:
                        if "value" not in result:
                            result["value"] = [int(item["value"][1])]
                        else:
                            result["value"].append(int(item["value"][1]))

        return result

    def insert_new_docs_sdk(self, num_docs, bucket, doc_key="new_docs"):
        result = []
        self.document_keys = []
        self.log.info("Creating SDK client for inserting new docs")
        self.sdk_client = SDKClient([self.cluster.master],
                                    bucket,
                                    scope=CbServer.default_scope,
                                    collection=CbServer.default_collection)
        new_docs = doc_generator(key=doc_key, start=0,
                                end=num_docs,
                                doc_size=1024,
                                doc_type=self.doc_type,
                                vbuckets=self.cluster.vbuckets,
                                key_size=self.key_size,
                                randomize_value=True)
        self.log.info("Inserting {} documents".format(num_docs))
        for i in range(num_docs):
            key_obj, val_obj = new_docs.next()
            self.document_keys.append(key_obj)
            res = self.sdk_client.insert(key_obj, val_obj)
            result.append(res)

        self.sdk_client.close()
        return result

    def read_docs_sdk(self, bucket, doc_key="initial_docs"):
        self.log.info("Creating SDK client for reading docs")
        self.sdk_client = SDKClient([self.cluster.master],
                                    bucket,
                                    scope=CbServer.default_scope,
                                    collection=CbServer.default_collection)
        result = []
        for key in self.document_keys:
            res = self.sdk_client.read(key=key)
            result.append(res)

        self.sdk_client.close()
        return result

    def insert_new_docs_query(self, bucket):
        self.n1ql_server = self.cluster_util.get_nodes_from_services_map(
            cluster=self.cluster,
            service_type=CbServer.Services.N1QL)
        if self.n1ql_server is None:
            node_to_add = self.cluster.servers[self.nodes_init]
            self.log.info("Adding node for query service = {}".format(node_to_add.ip))
            rebalance_in_task = self.task.async_rebalance(self.cluster,
                                                to_add=[node_to_add],
                                                to_remove=[],
                                                check_vbucket_shuffling=False,
                                                services=[CbServer.Services.N1QL])
            self.task_manager.get_task_result(rebalance_in_task)
            self.assertTrue(rebalance_in_task.result, "Rebalance-in of query node failed")
            self.n1ql_server = self.cluster_util.get_nodes_from_services_map(
                                cluster=self.cluster,
                                service_type=CbServer.Services.N1QL)

        self.num_buckets = len(self.cluster.buckets)
        self.n1ql_helper = N1QLHelper(server=self.n1ql_server,
                                      use_rest=True,
                                      buckets=self.buckets,
                                      log=self.log,
                                      scan_consistency='REQUEST_PLUS',
                                      num_collection=self.num_collections,
                                      num_buckets=self.num_buckets)

        self.log.info("Inserting a doc through query on bucket {}".format(bucket.name))
        insert_query = "INSERT INTO `{}`.`_default`.`_default`".format(bucket.name) + \
            " ( KEY, VALUE ) VALUES ('k004', { 'id': '01', 'type': 'airline'})" \
            " RETURNING META().id as docid, *;"
        self.log.info("Insert query = {}".format(insert_query))

        try:
            result = self.n1ql_helper.run_cbq_query(query=insert_query, server=self.n1ql_server)
        except Exception as e:
            self.log.info(e)
            return False
        self.log.info("Insert query result = {}".format(result))
        return True

    def create_sdk_clients_for_buckets(self):
        self.init_sdk_pool_object()
        max_clients = min(self.task_manager.number_of_threads, 20)
        if self.standard_buckets > 20:
            max_clients = self.standard_buckets
        clients_per_bucket = int(math.ceil(max_clients / self.standard_buckets))
        for bucket in self.cluster.buckets:
            self.sdk_client_pool.create_clients(
                bucket, [self.cluster.master], clients_per_bucket,
                compression_settings=self.sdk_compression)


    def tearDown(self):
        self.cluster_util.print_cluster_stats(self.cluster)

        super(GuardrailsBase, self).tearDown()
