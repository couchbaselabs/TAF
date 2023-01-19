import math
import string
import time
import re
from random import choice, sample
from threading import Thread

from BucketLib.bucket import Bucket
from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from cb_tools.cbstats import Cbstats
from cluster_utils.cluster_ready_functions import Nebula
from collections_helper.collections_spec_constants import MetaConstants
from serverlessbasetestcase import OnCloudBaseTest
from Cb_constants import CbServer
from capellaAPI.capella.serverless.CapellaAPI import CapellaAPI

from com.couchbase.test.docgen import DocumentGenerator
from com.couchbase.test.sdk import Server
from com.couchbase.test.taskmanager import TaskManager

from java.util.concurrent import ExecutionException


class TenantMgmtOnCloud(OnCloudBaseTest):
    def setUp(self):
        super(TenantMgmtOnCloud, self).setUp()
        self.key_type = "SimpleKey"
        self.val_type = "SimpleValue"
        self.doc_loading_tm = TaskManager(self.thread_to_use)
        self.db_name = "TAF-TenantMgmtOnCloud"
        self.token = self.input.capella.get("token")
        self.sandbox_cleanup = self.input.param("sandbox_cleanup", False)
        self.with_data_load = self.input.param("with_data_load", False)
        self.capella_api =  self.serverless_util.capella_api
        self.validate_stat = self.input.param("validate_stat", False)

        self.weight_incr = {1: 30, 2: 15, 3: 10, 4: 7}
        self.weight_start = {1: 30, 2: 210, 3: 270, 4: 300}

    def tearDown(self):
        self.clean_sandbox()
        if self.sdk_client_pool:
            self.sdk_client_pool.shutdown()
        super(TenantMgmtOnCloud, self).tearDown()

    def _assert_if_not_sandbox_run(self):
        self.assertTrue("sandbox" in self.pod.url_public,
                        "Test supported only on sandbox env!")

    def _assert_num_dataplane_deployed(self, num_dataplane):
        self.log.info("Validate the num dataplanes are '%s'" % num_dataplane)
        resp = self.serverless_util.get_all_dataplanes()
        if num_dataplane == 0:
            req = {'errorType': 'NotFound', 'message': 'Not Found.'}
            self.assertEqual(resp, req, "Dataplanes present ! :: %s" % resp)
        else:
            self.assertTrue(
                isinstance(resp, list) and len(resp) == num_dataplane,
                "Deployed more than 1 dataplane")

    def get_random_weight_for_width(self, width=1):
        return self.weight_start[width] \
                 + (self.weight_incr[width] * choice(range(1, 13)))

    def validate_cluster_deployment(self, cluster, req_nodes_dict):
        req_kv_nodes = req_nodes_dict.get(CbServer.Services.KV)
        req_index_nodes = req_nodes_dict.get(CbServer.Services.INDEX, 0)
        req_n1ql_nodes = req_nodes_dict.get(CbServer.Services.N1QL, 0)
        req_cbas_nodes = req_nodes_dict.get(CbServer.Services.CBAS, 0)
        req_eventing_nodes = req_nodes_dict.get(CbServer.Services.EVENTING, 0)
        req_fts_nodes = req_nodes_dict.get(CbServer.Services.FTS, 0)
        req_backup_nodes = req_nodes_dict.get(CbServer.Services.BACKUP, 0)

        total_nodes = sum([req_kv_nodes, req_index_nodes, req_index_nodes,
                           req_cbas_nodes, req_eventing_nodes, req_fts_nodes,
                           req_backup_nodes])

        self.cluster_util.update_cluster_nodes_service_list(cluster)
        self.log.critical("Total nodes in the cluster: %s"
                          % len(cluster.servers))
        self.log.critical("KV Nodes: %s" % len(cluster.kv_nodes))
        self.log.critical("Index Nodes: %s" % len(cluster.index_nodes))
        self.log.critical("Query Nodes: %s" % len(cluster.query_nodes))
        self.log.critical("Fts Nodes: %s" % len(cluster.fts_nodes))
        self.log.critical("CBAS Nodes: %s" % len(cluster.cbas_nodes))
        self.log.critical("Eventing Nodes: %s" % len(cluster.eventing_nodes))
        self.log.critical("Backup Nodes: %s" % len(cluster.backup_nodes))

        self.assertEqual(req_kv_nodes, len(cluster.kv_nodes),
                         "Mismatch in KV nodes")
        self.assertEqual(req_index_nodes, len(cluster.index_nodes),
                         "Mismatch in index nodes")
        self.assertEqual(req_n1ql_nodes, len(cluster.query_nodes),
                         "Mismatch in query nodes")
        self.assertEqual(req_fts_nodes, len(cluster.fts_nodes),
                         "Mismatch in fts nodes")
        self.assertEqual(req_eventing_nodes, len(cluster.eventing_nodes),
                         "Mismatch in eventing nodes")
        self.assertEqual(req_cbas_nodes, len(cluster.cbas_nodes),
                         "Mismatch in cbas nodes")
        self.assertEqual(req_backup_nodes, len(cluster.backup_nodes),
                         "Mismatch in backup nodes")
        self.assertEqual(total_nodes, len(cluster.servers),
                         "Mismatch in total nodes")

    def get_bucket_spec(self, bucket_name_format="taf-tntMgmt-%s",
                        num_buckets=1, scopes_per_bucket=1,
                        collections_per_scope=1, items_per_collection=0):
        self.log.debug("Getting spec for %s buckets" % num_buckets)
        buckets = dict()
        for i in range(num_buckets):
            buckets[bucket_name_format % i] = dict()
        return {
            MetaConstants.NUM_BUCKETS: num_buckets,
            MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
            MetaConstants.NUM_SCOPES_PER_BUCKET: scopes_per_bucket,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: collections_per_scope,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: items_per_collection,
            MetaConstants.USE_SIMPLE_NAMES: True,

            Bucket.bucketType: Bucket.Type.MEMBASE,
            Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
            Bucket.ramQuotaMB: 256,
            Bucket.width: 1,
            Bucket.weight: 30,
            Bucket.maxTTL: 0,
            "buckets": buckets
        }

    def clean_sandbox(self):
        if self.sandbox_cleanup:
            dataplanes_details = self.serverless_util.get_all_dataplanes()
            if dataplanes_details != {'errorType': 'NotFound', 'message': 'Not Found.'}:
                databases = self.serverless_util.get_all_serverless_databases()
                databases_list = []
                for db in databases:
                    if "id" in db:
                        resp = self.serverless_util. \
                            delete_serverless_database(db["id"])
                        self.assertTrue(resp == 202, "Database Delete request "
                                                     "failed")
                        databases_list.append(db["id"])
                        self.log.info("Tagging Bucket :: %s, for deletion " % (
                            db["id"]))
                for data_base in databases_list:
                    self.serverless_util.wait_for_database_deleted(
                        self.tenant, data_base)
                for dataplane in dataplanes_details:
                    dataplane_id = dataplane["id"]
                    resp = self.serverless_util.delete_dataplane(dataplane_id)
                    self.assertTrue(resp.status_code == 202, "Dataplane "
                                                             "Delete "
                                                             "request failed")
                    self.serverless_util.wait_for_dataplane_deleted(dataplane_id)
            self.sleep(10, "Waiting for deleted dataplane to get reflected")

    def get_servers_for_databases(self, buckets=None):
        dataplanes = dict()
        if buckets:
            buckets_to_consider = buckets
        else:
            buckets_to_consider = self.cluster.buckets
        for bucket in buckets_to_consider:
            dataplane_id = self.serverless_util.get_database_dataplane_id(
                self.pod, bucket.name)
            self.log.info("dataplane_id is %s" % dataplane_id)
            if dataplane_id not in dataplanes:
                dataplanes[dataplane_id] = dict()
                _, dataplanes[dataplane_id]["node"], \
                    dataplanes[dataplane_id]["username"], \
                    dataplanes[dataplane_id]["password"] = \
                    self.serverless_util.bypass_dataplane(dataplane_id)
            self.cluster.nodes_in_cluster = \
                self.cluster_util.construct_servers_from_master_details(
                    dataplanes[dataplane_id]["node"],
                    dataplanes[dataplane_id]["username"],
                    dataplanes[dataplane_id]["password"])
            self.cluster.master = self.cluster.nodes_in_cluster[0]
            self.bucket_util.get_updated_bucket_server_list(self.cluster, bucket)

    def __get_single_bucket_scenarios(self, target_scenario):
        scenarios = list()
        bucket_name = choice(self.cluster.buckets).name
        if target_scenario == "single_bucket_width_change":
            for width in range(2, 4):
                scenarios.append({bucket_name: {Bucket.width: width}})
            scenarios = scenarios + scenarios[::-1][1:]
        elif target_scenario == "single_bucket_width_increment":
            for width in range(2, 4):
                scenarios.append({bucket_name: {Bucket.width: width}})
        elif target_scenario == "single_bucket_weight_change":
            for index in range(2, 14):
                scenarios.append({bucket_name: {Bucket.weight: index*30}})
            scenarios = scenarios + scenarios[::-1][1:]
        elif target_scenario == "single_bucket_width_weight_incremental":
            width = 1
            weight = 30
            while width <= 4:
                scenarios.append(
                    {bucket_name: {Bucket.width: width,
                                   Bucket.weight: weight}})
                weight += self.weight_incr[width]
                if weight > 390:
                    width += 1
                    if width in self.weight_start:
                        weight = self.weight_start[width]
            scenarios = scenarios + scenarios[::-1][1:]
        elif target_scenario == "single_bucket_width_weight_random":
            max_scenarios = 20
            # Creates 20 random scenarios of random width/weight update
            for scenario_index in range(max_scenarios):
                width = choice(range(1, 5))
                weight = self.get_random_weight_for_width(width)
                scenarios.append({bucket_name: {Bucket.width: width,
                                                Bucket.weight: weight}})
        return scenarios

    def __get_five_bucket_scenarios(self, target_scenario):
        scenarios = list()
        buckets = sample(self.cluster.buckets, 5)
        if target_scenario == "five_buckets_width_update":
            scenarios.append({
                buckets[0].name: {Bucket.width: 2},
                buckets[1].name: {Bucket.width: 2},
                buckets[2].name: {Bucket.width: 2},
                buckets[3].name: {Bucket.width: 2},
                buckets[4].name: {Bucket.width: 2},
            })
            scenarios.append({
                buckets[0].name: {Bucket.width: 3},
                buckets[1].name: {Bucket.width: 2},
                buckets[2].name: {Bucket.width: 1},
                buckets[3].name: {Bucket.width: 3},
                buckets[4].name: {Bucket.width: 3},
            })
            scenarios.append({
                buckets[0].name: {Bucket.width: 4},
                buckets[1].name: {Bucket.width: 1},
                buckets[2].name: {Bucket.width: 2},
                buckets[3].name: {Bucket.width: 4},
                buckets[4].name: {Bucket.width: 2},
            })
            scenarios.append({
                buckets[0].name: {Bucket.width: 1},
                buckets[1].name: {Bucket.width: 3},
                buckets[2].name: {Bucket.width: 4},
                buckets[3].name: {Bucket.width: 1},
                buckets[4].name: {Bucket.width: 4},
            })
            return scenarios
        if target_scenario == "five_buckets_weight_update":
            scenarios.append({
                buckets[0].name: {Bucket.weight: 60},
                buckets[1].name: {Bucket.weight: 120},
                buckets[2].name: {Bucket.weight: 90},
                buckets[3].name: {Bucket.weight: 60},
                buckets[4].name: {Bucket.weight: 60},
            })
            scenarios.append({
                buckets[0].name: {Bucket.weight: 90},
                buckets[1].name: {Bucket.weight: 240},
                buckets[2].name: {Bucket.weight: 330},
                buckets[3].name: {Bucket.weight: 180},
                buckets[4].name: {Bucket.weight: 150},
            })
            scenarios.append({
                buckets[0].name: {Bucket.weight: 120},
                buckets[1].name: {Bucket.weight: 390},
                buckets[2].name: {Bucket.weight: 30},
                buckets[3].name: {Bucket.weight: 330},
                buckets[4].name: {Bucket.weight: 360},
            })
            scenarios.append({
                buckets[0].name: {Bucket.weight: 240},
                buckets[1].name: {Bucket.weight: 390},
                buckets[2].name: {Bucket.weight: 240},
                buckets[3].name: {Bucket.weight: 30},
                buckets[4].name: {Bucket.weight: 9999},
            })
            scenarios.append({
                buckets[0].name: {Bucket.weight: 300},
                buckets[1].name: {Bucket.weight: 9999},
                buckets[2].name: {Bucket.weight: 9999},
                buckets[3].name: {Bucket.weight: 9999},
                buckets[4].name: {Bucket.weight: 30},
            })
            return scenarios
        if target_scenario == "five_buckets_width_weight_update":
            scenarios.append({
                buckets[0].name: {Bucket.width: 2, Bucket.weight: 210},
                buckets[1].name: {Bucket.width: 2, Bucket.weight: 240},
                buckets[2].name: {Bucket.width: 2, Bucket.weight: 390},
                buckets[3].name: {Bucket.width: 2, Bucket.weight: 210},
                buckets[4].name: {Bucket.width: 2, Bucket.weight: 255}
            })
            return scenarios

    def __get_ten_bucket_scenarios(self, target_scenario):
        scenarios = list()
        buckets = sample(self.cluster.buckets, 10)
        if target_scenario == "ten_buckets_width_update":
            scenarios.append({
                buckets[0].name: {Bucket.width: 2},
                buckets[1].name: {Bucket.width: 2},
                buckets[2].name: {Bucket.width: 2},
                buckets[3].name: {Bucket.width: 2},
                buckets[4].name: {Bucket.width: 2},
                buckets[5].name: {Bucket.width: 2},
                buckets[6].name: {Bucket.width: 2},
                buckets[7].name: {Bucket.width: 2},
                buckets[8].name: {Bucket.width: 2},
                buckets[9].name: {Bucket.width: 2}
            })
            scenarios.append({
                buckets[0].name: {Bucket.width: 3},
                buckets[1].name: {Bucket.width: 3},
                buckets[2].name: {Bucket.width: 3},
                buckets[3].name: {Bucket.width: 3},
                buckets[4].name: {Bucket.width: 3},
                buckets[5].name: {Bucket.width: 3},
                buckets[6].name: {Bucket.width: 3},
                buckets[7].name: {Bucket.width: 3},
                buckets[8].name: {Bucket.width: 3},
                buckets[9].name: {Bucket.width: 3}
            })
            scenarios.append({
                buckets[0].name: {Bucket.width: 4},
                buckets[1].name: {Bucket.width: 4},
                buckets[2].name: {Bucket.width: 4},
                buckets[3].name: {Bucket.width: 4},
                buckets[4].name: {Bucket.width: 4},
                buckets[5].name: {Bucket.width: 4},
                buckets[6].name: {Bucket.width: 4},
                buckets[7].name: {Bucket.width: 4},
                buckets[8].name: {Bucket.width: 4},
                buckets[9].name: {Bucket.width: 4}
            })
        elif target_scenario == "ten_buckets_weight_update":
            scenarios.append({
                buckets[0].name: {Bucket.weight: 240},
                buckets[1].name: {Bucket.weight: 390},
                buckets[2].name: {Bucket.weight: 240},
                buckets[3].name: {Bucket.weight: 30},
                buckets[4].name: {Bucket.weight: 353},
                buckets[5].name: {Bucket.weight: 240},
                buckets[6].name: {Bucket.weight: 390},
                buckets[7].name: {Bucket.weight: 240},
                buckets[8].name: {Bucket.weight: 30},
                buckets[9].name: {Bucket.weight: 9999},
            })
            scenarios.append({
                buckets[0].name: {Bucket.weight: 30},
                buckets[1].name: {Bucket.weight: 90},
                buckets[2].name: {Bucket.weight: 9999},
                buckets[3].name: {Bucket.weight: 420},
                buckets[4].name: {Bucket.weight: 383},
                buckets[5].name: {Bucket.weight: 503},
                buckets[6].name: {Bucket.weight: 310},
                buckets[7].name: {Bucket.weight: 120},
                buckets[8].name: {Bucket.weight: 60},
                buckets[9].name: {Bucket.weight: 225},
            })
            scenarios.append({
                buckets[0].name: {Bucket.weight: 330},
                buckets[1].name: {Bucket.weight: 900},
                buckets[2].name: {Bucket.weight: 30},
                buckets[3].name: {Bucket.weight: 308},
                buckets[4].name: {Bucket.weight: 375},
                buckets[5].name: {Bucket.weight: 435},
                buckets[6].name: {Bucket.weight: 503},
                buckets[7].name: {Bucket.weight: 90},
                buckets[8].name: {Bucket.weight: 9999},
                buckets[9].name: {Bucket.weight: 465},
            })
        elif target_scenario == "ten_buckets_width_weight_update":
            scenarios.append({
                buckets[0].name: {Bucket.width: 1, Bucket.weight: 90},
                buckets[1].name: {Bucket.width: 1, Bucket.weight: 120},
                buckets[2].name: {Bucket.width: 1, Bucket.weight: 150},
                buckets[3].name: {Bucket.width: 1, Bucket.weight: 180},
                buckets[4].name: {Bucket.width: 1, Bucket.weight: 390},
                buckets[5].name: {Bucket.width: 2, Bucket.weight: 210},
                buckets[6].name: {Bucket.width: 2, Bucket.weight: 315},
                buckets[7].name: {Bucket.width: 2, Bucket.weight: 375},
                buckets[8].name: {Bucket.width: 2, Bucket.weight: 390},
                buckets[9].name: {Bucket.width: 2, Bucket.weight: 240}
            })
            scenarios.append({
                buckets[0].name: {Bucket.width: 2, Bucket.weight: 270},
                buckets[1].name: {Bucket.width: 2, Bucket.weight: 300},
                buckets[2].name: {Bucket.width: 2, Bucket.weight: 375},
                buckets[3].name: {Bucket.width: 2, Bucket.weight: 225},
                buckets[4].name: {Bucket.width: 2, Bucket.weight: 300},
                buckets[5].name: {Bucket.width: 3, Bucket.weight: 390},
                buckets[6].name: {Bucket.width: 3, Bucket.weight: 370},
                buckets[7].name: {Bucket.width: 3, Bucket.weight: 270},
                buckets[8].name: {Bucket.width: 3, Bucket.weight: 320},
                buckets[9].name: {Bucket.width: 3, Bucket.weight: 340}
            })
            scenarios.append({
                buckets[0].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[1].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[2].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[3].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[4].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[5].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[6].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[7].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[8].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[9].name: {Bucket.width: 4, Bucket.weight: 398}
            })
        return scenarios

    def __get_twenty_bucket_scenarios(self, target_scenario):
        scenarios = list()
        buckets = sample(self.cluster.buckets, 20)
        if target_scenario == "twenty_buckets_width_update":
            scenarios.append({
                buckets[0].name: {Bucket.width: 2},
                buckets[1].name: {Bucket.width: 2},
                buckets[2].name: {Bucket.width: 2},
                buckets[3].name: {Bucket.width: 2},
                buckets[4].name: {Bucket.width: 2},
                buckets[5].name: {Bucket.width: 2},

                buckets[6].name: {Bucket.width: 3},
                buckets[7].name: {Bucket.width: 3},

                buckets[8].name: {Bucket.width: 4},
                buckets[9].name: {Bucket.width: 4}
            })
            scenarios.append({
                buckets[10].name: {Bucket.width: 2},
                buckets[11].name: {Bucket.width: 2},
                buckets[12].name: {Bucket.width: 2},
                buckets[13].name: {Bucket.width: 2},

                buckets[14].name: {Bucket.width: 3},
                buckets[15].name: {Bucket.width: 3},
                buckets[16].name: {Bucket.width: 3},

                buckets[17].name: {Bucket.width: 4},
                buckets[18].name: {Bucket.width: 4},
                buckets[19].name: {Bucket.width: 4}
            })
            scenarios.append({
                buckets[0].name: {Bucket.width: 3},
                buckets[1].name: {Bucket.width: 3},
                buckets[2].name: {Bucket.width: 3},
                buckets[3].name: {Bucket.width: 3},
                buckets[4].name: {Bucket.width: 3},
                buckets[5].name: {Bucket.width: 3},
                buckets[10].name: {Bucket.width: 4},
                buckets[11].name: {Bucket.width: 4},
                buckets[12].name: {Bucket.width: 4},
                buckets[13].name: {Bucket.width: 4},

                buckets[6].name: {Bucket.width: 4},
                buckets[7].name: {Bucket.width: 4},
                buckets[14].name: {Bucket.width: 4},
                buckets[15].name: {Bucket.width: 4},
            })
        if target_scenario == "twenty_buckets_weight_update":
            weights = range(30, 504)
            for n in range(3):
                scenario_dict = dict()
                for i in range(20):
                    scenario_dict[buckets[i].name] = {
                        Bucket.weight: choice(weights)}
                scenarios.append(scenario_dict)
            # Add scenario to set max weight for buckets
            scenario_dict = dict()
            for i in range(20):
                scenario_dict[buckets[i].name] = 503
            scenarios.append(scenario_dict)
        if target_scenario == "twenty_buckets_width_weight_update":
            scenarios.append({
                buckets[0].name: {Bucket.width: 1, Bucket.weight: 60},
                buckets[1].name: {Bucket.width: 1, Bucket.weight: 120},
                buckets[2].name: {Bucket.width: 1, Bucket.weight: 180},
                buckets[3].name: {Bucket.width: 1, Bucket.weight: 390},
                buckets[4].name: {Bucket.width: 1, Bucket.weight: 270},
                buckets[6].name: {Bucket.width: 1, Bucket.weight: 90},
                buckets[7].name: {Bucket.width: 1, Bucket.weight: 300},
                buckets[8].name: {Bucket.width: 1, Bucket.weight: 360},
                buckets[9].name: {Bucket.width: 1, Bucket.weight: 150},

                buckets[10].name: {Bucket.width: 2, Bucket.weight: 210},
                buckets[11].name: {Bucket.width: 2, Bucket.weight: 390},
                buckets[12].name: {Bucket.width: 2, Bucket.weight: 360},
                buckets[13].name: {Bucket.width: 2, Bucket.weight: 240},

                buckets[14].name: {Bucket.width: 3, Bucket.weight: 270},
                buckets[15].name: {Bucket.width: 3, Bucket.weight: 390},
                buckets[16].name: {Bucket.width: 3, Bucket.weight: 300},

                buckets[17].name: {Bucket.width: 4, Bucket.weight: 300},
                buckets[18].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[19].name: {Bucket.width: 4, Bucket.weight: 338}
            })
            scenarios.append({
                buckets[0].name: {Bucket.width: 1, Bucket.weight: 360},
                buckets[1].name: {Bucket.width: 1, Bucket.weight: 60},
                buckets[2].name: {Bucket.width: 1, Bucket.weight: 120},

                buckets[3].name: {Bucket.width: 2, Bucket.weight: 210},
                buckets[4].name: {Bucket.width: 2, Bucket.weight: 255},
                buckets[6].name: {Bucket.width: 2, Bucket.weight: 390},
                buckets[7].name: {Bucket.width: 2, Bucket.weight: 300},
                buckets[8].name: {Bucket.width: 2, Bucket.weight: 375},
                buckets[9].name: {Bucket.width: 2, Bucket.weight: 270},
                buckets[10].name: {Bucket.width: 2, Bucket.weight: 315},
                buckets[11].name: {Bucket.width: 2, Bucket.weight: 285},

                buckets[12].name: {Bucket.width: 3, Bucket.weight: 280},
                buckets[13].name: {Bucket.width: 3, Bucket.weight: 310},
                buckets[14].name: {Bucket.width: 3, Bucket.weight: 390},
                buckets[15].name: {Bucket.width: 3, Bucket.weight: 270},

                buckets[16].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[17].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[18].name: {Bucket.width: 4, Bucket.weight: 300},
                buckets[19].name: {Bucket.width: 4, Bucket.weight: 368}
            })
            scenarios.append({
                buckets[0].name: {Bucket.width: 1, Bucket.weight: 60},
                buckets[1].name: {Bucket.width: 1, Bucket.weight: 270},

                buckets[2].name: {Bucket.width: 2, Bucket.weight: 270},
                buckets[3].name: {Bucket.width: 2, Bucket.weight: 360},
                buckets[4].name: {Bucket.width: 2, Bucket.weight: 390},
                buckets[6].name: {Bucket.width: 2, Bucket.weight: 225},

                buckets[7].name: {Bucket.width: 3, Bucket.weight: 270},
                buckets[8].name: {Bucket.width: 3, Bucket.weight: 280},
                buckets[9].name: {Bucket.width: 3, Bucket.weight: 380},
                buckets[10].name: {Bucket.width: 3, Bucket.weight: 310},
                buckets[11].name: {Bucket.width: 3, Bucket.weight: 300},

                buckets[12].name: {Bucket.width: 4, Bucket.weight: 375},
                buckets[13].name: {Bucket.width: 4, Bucket.weight: 315},
                buckets[14].name: {Bucket.width: 4, Bucket.weight: 308},
                buckets[15].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[16].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[17].name: {Bucket.width: 4, Bucket.weight: 390},
                buckets[18].name: {Bucket.width: 4, Bucket.weight: 383},
                buckets[19].name: {Bucket.width: 4, Bucket.weight: 398}
            })
            scenarios.append({
                buckets[1].name: {Bucket.width: 1, Bucket.weight: 30},

                buckets[2].name: {Bucket.width: 2, Bucket.weight: 210},
                buckets[3].name: {Bucket.width: 2, Bucket.weight: 375},

                buckets[7].name: {Bucket.width: 3, Bucket.weight: 310},
                buckets[8].name: {Bucket.width: 3, Bucket.weight: 380},
                buckets[10].name: {Bucket.width: 3, Bucket.weight: 390},
                buckets[11].name: {Bucket.width: 3, Bucket.weight: 280},

                buckets[0].name: {Bucket.width: 4, Bucket.weight: 315},
                buckets[4].name: {Bucket.width: 4, Bucket.weight: 390},
                buckets[6].name: {Bucket.width: 4, Bucket.weight: 300},
                buckets[9].name: {Bucket.width: 4, Bucket.weight: 383},
                buckets[12].name: {Bucket.width: 4, Bucket.weight: 330},
                buckets[13].name: {Bucket.width: 4, Bucket.weight: 315},
                buckets[14].name: {Bucket.width: 4, Bucket.weight: 308},
                buckets[15].name: {Bucket.width: 4, Bucket.weight: 398},
                buckets[16].name: {Bucket.width: 4, Bucket.weight: 300},
                buckets[17].name: {Bucket.width: 4, Bucket.weight: 390},
                buckets[18].name: {Bucket.width: 4, Bucket.weight: 323},
                buckets[19].name: {Bucket.width: 4, Bucket.weight: 398}
            })
        return scenarios

    def get_serverless_bucket_obj(self, db_name, width, weight, num_vbs=None):
        self.log.debug("Creating server bucket_obj %s:%s:%s"
                       % (db_name, width, weight))
        bucket_obj = Bucket({
            Bucket.name: db_name,
            Bucket.bucketType: Bucket.Type.MEMBASE,
            Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
            Bucket.ramQuotaMB: 256,
            Bucket.storageBackend: Bucket.StorageBackend.magma,
            Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
            Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
            Bucket.width: width,
            Bucket.weight: weight})
        if num_vbs:
            bucket_obj.numVBuckets = num_vbs

        return bucket_obj

    def create_database(self, bucket=None, timeout=600):
        if not bucket:
            bucket = self.get_serverless_bucket_obj(
                self.db_name, self.bucket_width, self.bucket_weight)
        task = self.bucket_util.async_create_database(
            self.cluster, bucket, dataplane_id=self.dataplane_id, timeout=timeout)
        self.task_manager.get_task_result(task)
        self.assertTrue(task.result, "Database creation failed")

    def create_required_buckets(self, buckets_spec=None):
        if buckets_spec or self.spec_name:
            if buckets_spec is None:
                self.log.info("Creating buckets from spec: %s"
                              % self.spec_name)
                buckets_spec = \
                    self.bucket_util.get_bucket_template_from_package(
                        self.spec_name)
                buckets_spec[MetaConstants.USE_SIMPLE_NAMES] = True

            # Process params to over_ride values if required
            CollectionBase.over_ride_bucket_template_params(
                self, self.bucket_storage, buckets_spec)
            result = self.bucket_util.create_buckets_using_json_data(
                self.cluster, buckets_spec)
            self.assertTrue(result, "Bucket creation failed")
            self.sleep(5, "Wait for collections creation to complete")
        else:
            self.log.info("Creating '%s' buckets" % self.num_buckets)
            for _ in range(self.num_buckets):
                self.create_database()

    def create_sdk_client_pool(self, buckets, req_clients_per_bucket):
        for bucket in buckets:
            nebula = bucket.serverless.nebula_endpoint
            self.log.info("Using Nebula endpoint %s" % nebula.srv)
            server = Server(nebula.srv, nebula.port,
                            nebula.rest_username,
                            nebula.rest_password,
                            str(nebula.memcached_port))
            self.sdk_client_pool.create_clients(
                bucket.name, server, req_clients_per_bucket)
        self.sleep(5, "Wait for SDK client pool to warmup")

    def test_create_delete_database(self):
        """
        1. Loading initial buckets
        2. Start data loading to all buckets
        3. Create more buckets when data loading is running
        4. Delete the created database while initial load is still running
        :return:
        """
        self.db_name = "%s-testCreateDeleteDatabase" % self.db_name
        dynamic_buckets = self.input.param("other_buckets", 1)
        # Create Buckets
        self.create_required_buckets()

        loader_map = dict()
        # Create sdk_client_pool
        self.init_sdk_pool_object()
        self.create_sdk_client_pool(buckets=self.cluster.buckets,
                                    req_clients_per_bucket=1)

        for bucket in self.cluster.buckets:
            for scope in bucket.scopes.keys():
                for collection in bucket.scopes[scope].collections.keys():
                    if scope == CbServer.system_scope:
                        continue
                    work_load_settings = DocLoaderUtils.get_workload_settings(
                        key=self.key, key_size=self.key_size,
                        doc_size=self.doc_size,
                        create_perc=100, create_start=0,
                        create_end=self.num_items,
                        ops_rate=100)
                    dg = DocumentGenerator(work_load_settings,
                                           self.key_type, self.val_type)
                    loader_map.update({"%s:%s:%s" % (bucket.name, scope, collection): dg})

        DocLoaderUtils.perform_doc_loading(
            self.doc_loading_tm, loader_map,
            self.cluster, self.cluster.buckets,
            async_load=False, validate_results=False,
            sdk_client_pool=self.sdk_client_pool)
        result = DocLoaderUtils.data_validation(
            self.doc_loading_tm, loader_map, self.cluster,
            buckets=self.cluster.buckets,
            process_concurrency=self.process_concurrency,
            ops_rate=self.ops_rate, sdk_client_pool=self.sdk_client_pool)
        self.assertTrue(result, "Data validation failed")

        new_buckets = list()
        for _ in range(dynamic_buckets):
            bucket = self.get_serverless_bucket_obj(
                self.db_name, self.bucket_width, self.bucket_weight)
            task = self.bucket_util.async_create_database(self.cluster, bucket)
            self.task_manager.get_task_result(task)
            nebula = Nebula(task.srv, task.server)
            self.log.info("Populate Nebula object done!!")
            bucket.serverless.nebula_obj = nebula
            bucket.serverless.nebula_endpoint = nebula.endpoint
            bucket.serverless.dapi = \
                self.serverless_util.get_database_DAPI(self.pod, self.tenant,
                                                       bucket.name)
            self.bucket_util.update_bucket_nebula_servers(self.cluster, bucket)
            new_buckets.append(bucket)

        # TODO: Load docs to new buckets

        self.log.info("Removing '%s' buckets" % new_buckets)
        for bucket in new_buckets:
            self.serverless_util.delete_database(self.pod, self.tenant,
                                                 bucket.name)

    def test_recreate_database(self):
        """
        1. Create a database
        2. Remove the database immediately after create
        3. Recreate the database with the same name
        4. Check the recreate was successful
        :return:
        """
        max_itr = 5
        spl_chars = " -"
        char_set = string.ascii_letters + string.digits + spl_chars
        db_name = "tnt mgmt-max-name-size-"
        while len(db_name) != 48:
            db_name += choice(char_set)
        bucket = self.get_serverless_bucket_obj(
            db_name, self.bucket_width, self.bucket_weight)

        self.log.info("Testing with bucket name=%s" % bucket.name)
        task = self.bucket_util.async_create_database(self.cluster, bucket)
        self.task_manager.get_task_result(task)
        self.assertTrue(task.result, "Database creation failed")

        for itr in range(1, max_itr):
            self.log.info("Iteration :: %s" % itr)
            bucket_name = self.serverless_util.create_serverless_database(
                self.cluster.pod, self.cluster.tenant, db_name,
                "aws", "us-east-1",
                bucket.serverless.width, bucket.serverless.weight)
            self.log.info("Bucket %s created" % bucket_name)
            self.serverless_util.delete_database(
                self.pod, self.tenant, bucket_name)
            self.serverless_util.wait_for_database_deleted(
                self.tenant, bucket_name)
        bucket.name = db_name
        task = self.bucket_util.async_create_database(self.cluster, bucket)
        self.task_manager.get_task_result(task)
        self.assertTrue(task.result, "Database creation failed")

    def test_create_database_negative(self):
        """
        1. Create serverless database using invalid name and validate
        2. Create serverless db with name of one character
        :return:
        """
        invalid_char_err = \
            "{\"errorType\":\"ErrDatabaseNameInvalidChars\"," \
            "\"message\":\"Not able to create serverless database. " \
            "The name of the database contains one or more invalid " \
            "characters ','. Database names can only contain " \
            "alphanumeric characters, space, and hyphen.\"}"
        name_too_short_err = \
            "{\"errorType\":\"ErrDatabaseNameTooShort\"," \
            "\"message\":\"Not able to create serverless database. " \
            "The name provided must be at least two characters in length. " \
            "Please try again.\"}"
        name_too_long_err = \
            "Not able to create serverless database. The name provided " \
            "exceeds the maximum number of characters allowed. " \
            "Please reduce the size of the name to 93 characters or " \
            "less and try again."

        name_to_err_map = {"123,": invalid_char_err,
                           "1": name_too_short_err,
                           "a"*49: name_too_long_err}
        for name, expected_err in name_to_err_map.items():
            self.log.info("Trying to create database with name=%s" % name)
            bucket = self.get_serverless_bucket_obj(name, 1, 30)
            task = self.bucket_util.async_create_database(self.cluster, bucket)
            try:
                self.task_manager.get_task_result(task)
            except ExecutionException as exception:
                if expected_err not in str(exception):
                    self.fail("Exception mismatch. Got::%s" % exception)

    def __get_bucket_with_name(self, b_name):
        for b_obj in self.cluster.buckets:
            if b_obj.name == b_name:
                return b_obj

    def __trigger_bucket_param_updates(self, scenario):
        """
        :param scenario:
        :return:
        """
        to_track = list()
        for bucket_name, s_dict in scenario.items():
            bucket_obj = self.__get_bucket_with_name(bucket_name)
            db_info = {
                "cluster": self.cluster,
                "bucket": bucket_obj,
                "desired_ram_quota": None,
                "desired_width": None,
                "desired_weight": None
            }

            over_ride = dict()
            if Bucket.width in s_dict and (bucket_obj.serverless.width
                                           != s_dict[Bucket.width]):
                # TODO: Waiting for bucket_width decr support (AV-47047)
                if bucket_obj.serverless.width > s_dict[Bucket.width]:
                    continue
                over_ride[Bucket.width] = s_dict[Bucket.width]
                db_info["desired_width"] = s_dict[Bucket.width]
                bucket_obj.serverless.width = s_dict[Bucket.width]
            if Bucket.weight in s_dict and (bucket_obj.serverless.weight
                                            != s_dict[Bucket.weight]):
                over_ride[Bucket.weight] = s_dict[Bucket.weight]
                db_info["desired_weight"] = s_dict[Bucket.weight]
                bucket_obj.serverless.weight = s_dict[Bucket.weight]
            resp = self.capella_api.update_database(bucket_obj.name, over_ride)
            self.assertTrue(resp.status_code == 200, "Update Api failed")
            to_track.append(db_info)
        return to_track

    def test_bucket_scaling(self):
        """
        :return:
        """
        scenarios = None
        doc_loading_tasks = None
        bucket_name_format = "tntMgmtScaleTest-%s"
        target_scenario = self.input.param("target_scenario")

        spec = self.get_bucket_spec(
            bucket_name_format=bucket_name_format,
            num_buckets=self.num_buckets)
        self.create_required_buckets(buckets_spec=spec)
        if target_scenario.startswith("single_bucket_"):
            scenarios = self.__get_single_bucket_scenarios(target_scenario)
        elif target_scenario.startswith("five_buckets_"):
            scenarios = self.__get_five_bucket_scenarios(target_scenario)
        elif target_scenario.startswith("ten_buckets_"):
            scenarios = self.__get_ten_bucket_scenarios(target_scenario)
        elif target_scenario.startswith("twenty_buckets_"):
            scenarios = self.__get_twenty_bucket_scenarios(target_scenario)

        if self.validate_stat:
            self.get_servers_for_databases()
            self.expected_stat = self.bucket_util.get_initial_stats(
                self.cluster.buckets)
        if self.with_data_load:
            self.init_sdk_pool_object()
            self.create_sdk_client_pool(self.cluster.buckets, 1)
            loader_map = dict()
            for bucket in self.cluster.buckets:
                work_load_settings = DocLoaderUtils.get_workload_settings(
                    key=self.key, key_size=self.key_size,
                    doc_size=self.doc_size,
                    create_perc=100, create_start=0,
                    create_end=self.num_items,
                    ops_rate=100)
                dg = DocumentGenerator(work_load_settings,
                                       self.key_type, self.val_type)
                loader_map.update(
                    {"%s:%s:%s" % (bucket.name, CbServer.default_scope,
                                   CbServer.default_collection): dg})

            _, doc_loading_tasks = DocLoaderUtils.perform_doc_loading(
                self.doc_loading_tm, loader_map,
                self.cluster, self.cluster.buckets,
                async_load=True, validate_results=False,
                sdk_client_pool=self.sdk_client_pool)

        for scenario in scenarios:
            to_track = self.__trigger_bucket_param_updates(scenario)
            monitor_task = self.bucket_util.async_monitor_database_scaling(
                to_track, timeout=1800)
            self.task_manager.get_task_result(monitor_task)

        if self.with_data_load:
            DocLoaderUtils.wait_for_doc_load_completion(self.doc_loading_tm,
                                                        doc_loading_tasks)
            if self.validate_stat:
                for bucket in self.cluster.buckets:
                    self.expected_stat[bucket.name]["wu"] \
                        += self.bucket_util.calculate_units(
                            self.key_size, self.doc_size,
                            num_items=self.num_items)
        if self.validate_stat:
            self.get_servers_for_databases()
            self.bucket_util.validate_stats(self.cluster.buckets,
                                            self.expected_stat)

    def test_create_delete_db_during_bucket_scaling(self):
        """
        1. Deploy 'N' databases with width=1, weight=30
        2. Scale half of the DBs to width=2
        3. Scale one DB from width 1->2 and another from 2->3
           and CREATE & DELETE databases in parallel
        4. Scale one DB from width 2->3 and another from 3->2
           and CREATE & DELETE databases in parallel
        :return:
        """

        b_index = 0
        scenario = dict()
        bucket_name_format = "tntMgmtScaleTest-%s"
        scale_type = self.input.param("scale_type", Bucket.width)

        # Deploy initial databases
        spec = self.get_bucket_spec(num_buckets=self.num_buckets,
                                    bucket_name_format=bucket_name_format)
        self.create_required_buckets(buckets_spec=spec)

        # Initially scale first half of the buckets to width=2 and weight=300
        half_of_bucket_index = int(self.num_buckets/2)
        for index, bucket in enumerate(
                self.cluster.buckets[:half_of_bucket_index]):
            scenario[bucket.name] = {Bucket.width: 2,
                                     Bucket.weight: 300}
        to_track = self.__trigger_bucket_param_updates(scenario)
        monitor_task = self.bucket_util.async_monitor_database_scaling(
            to_track, timeout=1800)
        self.task_manager.get_task_result(monitor_task)

        # Perform scaling and perform new DB create/delete
        if scale_type == Bucket.width:
            scenario = {self.cluster.buckets[0].name: {Bucket.width: 3},
                        self.cluster.buckets[-1].name: {Bucket.width: 2}}
        elif scale_type == Bucket.weight:
            scenario = {self.cluster.buckets[0].name: {Bucket.weight: 390},
                        self.cluster.buckets[-1].name: {Bucket.weight: 210}}
        else:
            scenario = {self.cluster.buckets[0].name: {Bucket.width: 3,
                                                       Bucket.weight: 280},
                        self.cluster.buckets[-1].name: {Bucket.width: 2,
                                                        Bucket.weight: 300}}
        to_track = self.__trigger_bucket_param_updates(scenario)
        monitor_task = self.bucket_util.async_monitor_database_scaling(
            to_track, timeout=1800)
        # Create DB
        db_to_create = self.get_serverless_bucket_obj(
            db_name="tntMgmtCreateDeleteDB-%s" % b_index, width=1, weight=30)
        self.log.info("Creating DB: %s" % db_to_create.name)
        task = self.bucket_util.async_create_database(self.cluster,
                                                      db_to_create)
        # Drop DB
        db_to_drop = self.cluster.buckets[-2]
        self.log.info("Dropping DB: %s" % db_to_drop.name)
        self.serverless_util.delete_database(self.pod, self.tenant, db_to_drop)
        # Wait for create DB to complete
        self.log.info("Wait for %s creation to complete: %s" % db_to_drop.name)
        self.task_manager.get_task_result(task)
        self.task_manager.get_task_result(monitor_task)

        # Update width scaling for couple for DBs
        if scale_type == Bucket.width:
            scenario = {self.cluster.buckets[0].name: {Bucket.width: 2},
                        self.cluster.buckets[-1].name: {Bucket.width: 3}}
        elif scale_type == Bucket.weight:
            scenario = {self.cluster.buckets[0].name: {Bucket.weight: 225},
                        self.cluster.buckets[-1].name: {Bucket.weight: 375}}
        else:
            scenario = {self.cluster.buckets[0].name: {Bucket.width: 2,
                                                       Bucket.weight: 330},
                        self.cluster.buckets[-1].name: {Bucket.width: 3,
                                                        Bucket.weight: 390}}
        to_track = self.__trigger_bucket_param_updates(scenario)
        monitor_task = self.bucket_util.async_monitor_database_scaling(
            to_track, timeout=1800)
        # Drop the DB which was created earlier
        target_db = db_to_create
        self.log.info("Dropping DB: %s" % target_db.name)
        self.serverless_util.delete_database(self.pod, self.tenant, target_db)
        # Create DB
        target_db = db_to_drop
        self.log.info("Creating DB: %s" % target_db.name)
        task = self.bucket_util.async_create_database(self.cluster, target_db)
        self.task_manager.get_task_result(task)
        self.task_manager.get_task_result(monitor_task)

    def test_scaling_with_dgm_buckets(self):
        target_dgm = self.input.param("target_dgm", 90)
        num_dgm_buckets = self.input.param("num_dgm_buckets", 1)
        target_scenario = self.input.param("target_scenario")

        bucket_name_format = "tntMgmtScaleDgmBucket-%s"
        batch_size = 50000
        scenarios = list()

        if self.validate_stat:
            self.get_servers_for_databases()

        spec = self.get_bucket_spec(num_buckets=self.num_buckets,
                                    bucket_name_format=bucket_name_format)
        self.create_required_buckets(buckets_spec=spec)

        if target_scenario.startswith("single_bucket_"):
            scenarios = self.__get_single_bucket_scenarios(target_scenario)
        elif target_scenario.startswith("five_buckets_"):
            scenarios = self.__get_five_bucket_scenarios(target_scenario)
        elif target_scenario.startswith("ten_buckets_"):
            scenarios = self.__get_ten_bucket_scenarios(target_scenario)
        elif target_scenario.startswith("twenty_buckets_"):
            scenarios = self.__get_twenty_bucket_scenarios(target_scenario)

        work_load_settings = [DocLoaderUtils.get_workload_settings(
            key=self.key, key_size=self.key_size, doc_size=self.doc_size,
            create_perc=100, create_start=0, create_end=0,
            ops_rate=self.ops_rate) for _ in range(num_dgm_buckets)]
        doc_gens = [DocumentGenerator(ws, self.key_type, self.val_type)
                    for ws in work_load_settings]
        target_buckets = sample(self.cluster.buckets, num_dgm_buckets)
        loading_for_buckets = dict()
        for bucket in target_buckets:
            loading_for_buckets[bucket.name] = True

        self.init_sdk_pool_object()
        self.create_sdk_client_pool(target_buckets, 1)
        continue_data_load = True
        while continue_data_load:
            loading_tasks = list()
            continue_data_load = False
            for index, bucket in enumerate(target_buckets):
                if loading_for_buckets[bucket.name] is False:
                    continue
                continue_data_load = True
                loader_key = "%s:%s:%s" % (bucket.name, CbServer.default_scope,
                                           CbServer.default_collection)
                ws = work_load_settings[index]
                ws.dr.create_s = ws.dr.create_e
                ws.dr.create_e += batch_size
                _, tasks = DocLoaderUtils.perform_doc_loading(
                    self.doc_loading_tm, {loader_key: doc_gens[index]},
                    self.cluster, buckets=[bucket],
                    sdk_client_pool=self.sdk_client_pool)
                loading_tasks.extend(tasks)

            DocLoaderUtils.wait_for_doc_load_completion(
                self.doc_loading_tm, loading_tasks)

            for index, bucket in enumerate(target_buckets):
                if loading_for_buckets[bucket.name] is False:
                    continue
                for server in bucket.servers:
                    stat = Cbstats(server)
                    resident_mem = stat.get_stats_memc(bucket.name)[
                        "vb_active_perc_mem_resident"]
                    if int(resident_mem) <= int(target_dgm):
                        loading_for_buckets[bucket.name] = False
                        break
        if self.validate_stat:
            self.expected_stat = self.bucket_util.get_initial_stats(
                self.cluster.buckets)

        for scenario in scenarios:
            to_track = self.__trigger_bucket_param_updates(scenario)
            monitor_task = self.bucket_util.async_monitor_database_scaling(
                to_track, timeout=1800)
            self.task_manager.get_task_result(monitor_task)

        if self.validate_stat:
            self.get_servers_for_databases()
            self.bucket_util.validate_stats(self.cluster.buckets,
                                            self.expected_stat)

    def test_bucket_auto_ram_scaling(self):
        """
        :return:
        """
        start_index = 0
        batch_size = 50000
        self.create_required_buckets()
        bucket = self.cluster.buckets[0]

        work_load_settings = DocLoaderUtils.get_workload_settings(
            key=self.key, key_size=self.key_size, doc_size=self.doc_size,
            create_perc=100, create_start=start_index, create_end=start_index,
            ops_rate=self.ops_rate)
        dg = DocumentGenerator(work_load_settings,
                               self.key_type, self.val_type)

        loader_key = "%s:%s:%s" % (bucket.name, CbServer.default_scope,
                                   CbServer.default_collection)
        self.init_sdk_pool_object()
        self.create_sdk_client_pool([bucket], 1)
        dgm_index = 0
        storage_band = 1
        target_dgms = [3, 2.3, 2.0, 1.9, 1.8, 1.5]
        len_target_dgms = len(target_dgms)

        self.log.critical("Loading bucket till %s%% DGM"
                          % target_dgms[dgm_index])
        while dgm_index < len_target_dgms:
            work_load_settings.dr.create_s = work_load_settings.dr.create_e
            work_load_settings.dr.create_e += batch_size
            DocLoaderUtils.perform_doc_loading(
                self.doc_loading_tm, {loader_key: dg}, self.cluster,
                buckets=[bucket], async_load=False, validate_results=False,
                sdk_client_pool=self.sdk_client_pool)

            for server in bucket.servers:
                stat = Cbstats(server)
                resident_mem = stat.get_stats_memc(bucket.name)[
                    "vb_active_perc_mem_resident"]
                if int(resident_mem) <= target_dgms[dgm_index]:
                    dgm_index += 1
                    storage_band += 1
                    if dgm_index < len_target_dgms:
                        self.log.critical("Loading bucket till %s%% DGM"
                                          % target_dgms[dgm_index])

                    monitor_task = \
                        self.bucket_util.async_monitor_database_scaling(
                            self.cluster, bucket)
                    self.task_manager.get_task_result(monitor_task)

    def test_scope_collection_limit(self):
        """
        1. Create bucket with max scopes
        2. Create bucket with max collections under default scope
        3. Create bucket with default scopes/collections
           a. Create 100 scopes with no collections.
              Then create an extra scope to validate the failure.
           b. Create 1 collection under each of the prev. scope then create
              an extra collection to validate failure
           c. Drop 90 scopes now. then create 9 more collections such that
              we have 10 scopes and 10 collections per scope.
              Again try creating an extra collection to validate failure.
           d. Now recreate 90 scopes with no collections and validate

        Note: Scope / collection limit = 100 excluding the default scope/col.
              Meaning we can create 100 custom scope/collection
        """
        def create_scopes(b_obj, index_range):
            self.log.info("Bucket :: %s, create %s scopes"
                          % (b_obj.name, len(index_range)))
            try:
                for s_i in index_range:
                    self.bucket_util.create_scope(b_obj.servers[0], b_obj,
                                                  {"name": "scope-%s" % s_i})
            except Exception as err:
                self.log.critical("Create scope failed: %s" % err)
                self.task_failure = True

        def create_collections(b_obj, scope_name, index_range):
            self.log.info("Bucket::Scope - %s::%s, create %s collections"
                          % (b_obj.name, scope_name, len(index_range)))
            try:
                for c_i in index_range:
                    self.bucket_util.create_collection(
                        b_obj.servers[0], b_obj, scope_name,
                        {"name": "col-%s" % c_i})
            except Exception as err:
                self.log.critical("Create collection failed: %s" % err)
                self.task_failure = True

        self.task_failure = False
        scope_limit = collection_limit = 100
        bucket_name_format = "tntMgmtLimit-%s"

        self.log.info("Creating required buckets")
        b_spec = self.get_bucket_spec(num_buckets=3,
                                      bucket_name_format=bucket_name_format)
        self.create_required_buckets(b_spec)

        # Set bucket object references
        b1 = self.cluster.buckets[0]
        b2 = self.cluster.buckets[1]
        b3 = self.cluster.buckets[2]

        bucket_with_max_scopes_thread = Thread(target=create_scopes,
                                               args=(b1, range(scope_limit)))
        bucket_with_max_cols_thread = Thread(target=create_collections,
                                             args=(b2, CbServer.default_scope,
                                                   range(collection_limit)))
        bucket_with_max_scopes_thread.start()
        bucket_with_max_cols_thread.start()

        try:
            server = b3.servers[0]
            create_scopes(b3, range(scope_limit))
            self.log.info("Creating an extra scope in %s" % b3.name)
            try:
                self.bucket_util.create_scope(server, b3,
                                              {"name": "err_scope"})
            except Exception as exception:
                self.log.debug("Failed as expected: %s" % exception)
            else:
                self.fail("Scope created violating the limits")

            self.log.info("Creating %s custom collections" % collection_limit)
            for i in range(collection_limit):
                self.bucket_util.create_collection(
                    server, b3, "scope-%s" % i, {"name": "col-%s" % i})

            self.log.info("Trying to create an extra collection")
            for i in range(scope_limit):
                try:
                    self.bucket_util.create_collection(
                        server, b3, "scope-%s" % i, {"name": "err_col"})
                except Exception as e:
                    self.log.debug("Failed as expected" % e)
                else:
                    self.fail("Collection created violating the limits")

            self.log.info("Dropping 90 scopes from the bucket")
            for i in range(10, scope_limit):
                self.bucket_util.drop_scope(server, b3, "scope-%s" % i)

            self.log.info("Creating 10 collections per existing scope")
            for i in range(10):
                c_range = range(10)
                c_range.pop(i)
                create_collections(b3, "scope-%s" % i, c_range)

            self.log.info("Validate collections limit")
            for i in range(10):
                try:
                    self.bucket_util.create_collection(
                        server, b3, "scope-%s" % i, {"name": "errcol"})
                except Exception as e:
                    self.log.debug("Collection create failed as expected: %s"
                                   % e)
                else:
                    self.fail("Collection created violating the limits")

            self.log.info("Creating 90 scopes with 0 collections")
            create_scopes(b3, range(10, scope_limit))
            try:
                self.bucket_util.create_scope(server, b3, {"name": "errscope"})
            except Exception as e:
                self.log.debug("Scope create failed as expected: %s" % e)
            else:
                self.fail("Scope created violating the limits")
        finally:
            bucket_with_max_scopes_thread.join()
            bucket_with_max_cols_thread.join()
            self.assertFalse(self.task_failure, "Failure in create task")

    def test_initial_cluster_deployment_state(self):
        self._assert_if_not_sandbox_run()
        self.clean_sandbox()

        self._assert_num_dataplane_deployed(0)

        self.bucket_width = 1
        self.bucket_weight = 30
        self.db_name = "tntMgmt-initial-db"
        self.dataplane_id = None
        self.create_database(timeout=1400)
        bucket = self.cluster.buckets[0]

        self._assert_num_dataplane_deployed(1)

        dp_id = self.serverless_util.get_database_dataplane_id(self.pod,
                                                               bucket.name)
        self.log.info("Bucket :: %s, Dataplane :: %s" % (bucket.name, dp_id))
        try:
            _, host, u_name, pwd = self.serverless_util.bypass_dataplane(dp_id)
            self.cluster.servers = \
                self.cluster_util.construct_servers_from_master_details(
                    host, u_name, pwd)
            self.cluster.master = self.cluster.servers[0]
            for server in self.cluster.servers:
                server.port = CbServer.ssl_port
            validation_dict = {
                CbServer.Services.KV: 3,
                CbServer.Services.INDEX: 2,
                CbServer.Services.N1QL: 2,
                CbServer.Services.FTS: 2
            }
            self.validate_cluster_deployment(self.cluster, validation_dict)
            self.bucket_util.validate_serverless_buckets(self.cluster,
                                                         self.cluster.buckets)
        finally:
            self.log.info("Cleaning up database and dataplane")
            self.serverless_util.delete_database(
                self.pod, self.tenant, bucket.name)
            self.serverless_util.wait_for_database_deleted(
                self.tenant, bucket.name)
            self.cluster.buckets.remove(bucket)
            self.serverless_util.delete_dataplane(dp_id)

    def test_cluster_scaling_wrt_db_count(self):
        """
        1. Make sure there is no other dataplane present
        2. Create 19 DBs with default settings
        3. Validate cluster sizing
        4. Create 20th DB and make sure cluster scaling happens for KV
        5. Continue to create 40, 60, 80 DBs and repeat step 3, 4
        6. Thn scale upto 100 dbs and make sure we are able to accommodate all
           databases within the same cluster.
        :return:
        """
        self._assert_if_not_sandbox_run()
        self.clean_sandbox()
        self._assert_num_dataplane_deployed(0)

        self.bucket_width = 1
        self.bucket_weight = 30
        total_buckets = 0
        num_buckets = 19
        cluster_deployment_spec = {
            CbServer.Services.KV: 3,
            CbServer.Services.INDEX: 2,
            CbServer.Services.N1QL: 2,
            CbServer.Services.FTS: 2
        }

        self.log.info("Creating first %s databases" % num_buckets)
        b_name_format = "tntmgmt-set-1-%s"
        spec = self.get_bucket_spec(bucket_name_format=b_name_format,
                                    num_buckets=num_buckets)
        self.create_required_buckets(spec)
        self._assert_num_dataplane_deployed(1)
        total_buckets += num_buckets

        dp_id = self.serverless_util.get_database_dataplane_id(
            self.pod, self.cluster.buckets[0].name)

        self.log.info("Creating bypass for dataplane :: %s" % dp_id)
        _, host, u_name, pwd = self.serverless_util.bypass_dataplane(dp_id)
        self.cluster.servers = \
            self.cluster_util.construct_servers_from_master_details(
                host, u_name, pwd)
        self.cluster.master = self.cluster.servers[0]
        for server in self.cluster.servers:
            server.port = CbServer.ssl_port

        self.validate_cluster_deployment(self.cluster, cluster_deployment_spec)
        self.bucket_util.validate_serverless_buckets(self.cluster, self.cluster.buckets)

        for itr in [2, 3, 4]:
            cluster_deployment_spec[CbServer.Services.KV] = \
                itr * CbServer.Serverless.KV_SubCluster_Size

            b_num = (itr-1) * num_buckets + 1
            self.log.info("Creating %sth bucket" % (b_num + 1))
            bucket = self.get_serverless_bucket_obj(
                b_name_format % b_num, self.bucket_width, self.bucket_weight)
            self.create_database(bucket)
            total_buckets += 1

            self._assert_num_dataplane_deployed(1)
            self.cluster.servers = \
                self.cluster_util.construct_servers_from_master_details(
                    host, u_name, pwd)
            self.cluster.master = self.cluster.servers[0]
            for server in self.cluster.servers:
                server.port = CbServer.ssl_port
            self.validate_cluster_deployment(self.cluster,
                                             cluster_deployment_spec)
            self.bucket_util.validate_serverless_buckets(self.cluster, self.cluster.buckets)

            b_name_format = "tntmgmt-set-{0}-{1}".format(itr, "%s")
            self.log.info("Targeting %s databases in the dataplane"
                          % (itr*num_buckets))
            spec = self.get_bucket_spec(bucket_name_format=b_name_format,
                                        num_buckets=num_buckets)
            self.create_required_buckets(spec)
            self._assert_num_dataplane_deployed(1)
            total_buckets += num_buckets

            curr_dp_id = self.serverless_util.get_database_dataplane_id(
                self.pod, self.cluster.buckets[0].name)
            self.assertEqual(dp_id, curr_dp_id,
                             "Dataplane id changed to '%s'" % curr_dp_id)

            self.cluster.servers = \
                self.cluster_util.construct_servers_from_master_details(
                    host, u_name, pwd)
            for server in self.cluster.servers:
                server.port = CbServer.ssl_port
            self.cluster.master = self.cluster.servers[0]
            self.validate_cluster_deployment(self.cluster,
                                             cluster_deployment_spec)
            self.bucket_util.validate_serverless_buckets(self.cluster, self.cluster.buckets)
            self.log.info("Total buckets in the cluster: %s" % total_buckets)

        for b_num in range(total_buckets + 1, 100):
            self.log.info("Creating %sth bucket" % (b_num + 1))
            bucket = self.get_serverless_bucket_obj(
                b_name_format % b_num, self.bucket_width, self.bucket_weight)
            self.create_database(bucket)
            self._assert_num_dataplane_deployed(1)
            self.validate_cluster_deployment(self.cluster,
                                             cluster_deployment_spec)

    def test_defrag_dbaas(self):
        """
        creating given number of buckets on cloud
        expanding weight / deleting buckets from a target node
        waiting for re-balance to trigger by control plane
        asserting on bucket movements
        this method is only intended to use in a sandbox
        """
        if "sandbox" not in self.pod.url_public:
            self.log.info("test_defrag_dbaas case skipped as sandbox env not "
                          "detected in pod url")
            return
        self.clean_sandbox()
        delete_scenario = self.input.param("delete_scenario", False)
        weight_limit = self.input.param("weight_limit", 10000)

        def wait_for_defragmentation(node_dictionary, timeout=600):
            end_time = time.time() + timeout
            while time.time() < end_time:
                if not delete_scenario:
                    buckets_in_target = get_bucket_stats(
                        server_param=node_dictionary[target_node]["node"])
                    other_buckets = node_dictionary[target_node]["bucket"] -\
                        buckets_in_target[target_node]["bucket"]
                    other_bucket_dict = get_bucket_stats(buckets=list(
                        other_buckets))
                    if len(other_bucket_dict) > 0:
                        break
                else:
                    if len(node_dictionary[target_node]["bucket"]) > 0:
                        break
                self.sleep(10, "waiting for re-balance trigger")
            else:
                self.assertTrue(False, "Expected bucket movement after "
                                       "de-fragment re-balance")

        # creating and expanding single bucket with width = 2 to make sure
        # we have enough data nodes to create scenario
        def pre_test():
            pre_test_bucket_specs = self.get_bucket_spec(
                num_buckets=1, bucket_name_format="SingleBucket-%s")
            self.create_required_buckets(pre_test_bucket_specs)
            bucket_name = self.cluster.buckets[0].name
            scenarios = {bucket_name: {Bucket.width: 2}}
            track = self.__trigger_bucket_param_updates(scenarios)
            db_monitor_task = self.bucket_util.async_monitor_database_scaling(
                track, timeout=600)
            self.task_manager.get_task_result(db_monitor_task)
            return bucket_name

        # required for compare bucket movement before and after de-frag
        # rebalance
        def get_bucket_stats(buckets=None, server_param=None):
            if not buckets:
                buckets = self.cluster.buckets
            node_map = dict()
            for t_bucket in buckets:
                server = t_bucket.servers[0]
                if server_param:
                    server = server_param
                try:
                    stat = Cbstats(server)
                    status = stat.vbucket_details(t_bucket.name)
                except Exception as e:
                    self.log.warning(e)
                    continue
                for v_bucket in status:
                    for t_node in str(status[v_bucket]["topology"])\
                            .split("\""):
                        if "ns_1@" in t_node:
                            if t_node not in node_map.keys():
                                node_map[t_node] = dict()
                                node_map[t_node]["bucket"] = set()
                                node_map[t_node]["node"] = server
                            node_map[t_node]["bucket"].add(t_bucket)
            return node_map

        pre_test()
        # creating initial buckets
        bucket_specs = self.get_bucket_spec(
            num_buckets=self.num_buckets, bucket_name_format="DefragBucks-%s")
        self.create_required_buckets(bucket_specs)

        # assuming a target node
        node_dict = get_bucket_stats()
        target_node = None
        bucket_to_update = []
        for node in node_dict.keys():
            if len(node_dict[node]["bucket"]) > 2:
                for buck in node_dict[node]["bucket"]:
                    if buck.serverless.width <= 1:
                        bucket_to_update.append(buck)
                        target_node = node
            if target_node:
                break
        dynamic_scenarios = {}
        self.assertTrue(len(bucket_to_update) > 0, "Desired thershold "
                                                   "common bucket not found "
                                                   "on any single node")
        desired_weight = int(math.floor(weight_limit /
                                        len(bucket_to_update)))

        # updating weight / deleting buckets
        if not delete_scenario:
            for bucket in bucket_to_update:
                dynamic_scenarios[bucket.name] = \
                    {Bucket.weight: desired_weight}
            to_track = self.__trigger_bucket_param_updates(dynamic_scenarios)
            monitor_task = self.bucket_util.async_monitor_database_scaling(
                to_track, timeout=600)
            self.task_manager.get_task_result(monitor_task)
        else:
            for bucket in bucket_to_update:
                self.serverless_util.delete_database(self.pod, self.tenant,
                                                     bucket.name)
        # verification
        wait_for_defragmentation(node_dict)

    def test_auto_ebs_sacling(self):
        """
        :return:
        """
        self.clean_sandbox()
        loader_map = dict()
        self.create_required_buckets()
        self.get_servers_for_databases()
        storage_quota_init = self.bucket_util.get_storage_quota(self.cluster.buckets[0])

        for bucket in self.cluster.buckets:
            for scope in bucket.scopes.keys():
                for collection in bucket.scopes[scope].collections.keys():
                    if scope == CbServer.system_scope:
                        continue
                    work_load_settings = DocLoaderUtils.get_workload_settings(
                        key=self.key, key_size=self.key_size, doc_size=self.doc_size,
                        create_perc=100, create_start=0, create_end=self.num_items,
                        ops_rate=self.ops_rate)
                    dg = DocumentGenerator(work_load_settings,
                                           self.key_type, self.val_type)
                    loader_map.update(
                        {"%s:%s:%s" % (bucket.name, scope, collection): dg})

        self.init_sdk_pool_object()
        self.create_sdk_client_pool(buckets=self.cluster.buckets,
                                    req_clients_per_bucket=1)
        DocLoaderUtils.perform_doc_loading(
            self.doc_loading_tm, loader_map, self.cluster,
            self.cluster.buckets, async_load=False, validate_results=False,
            sdk_client_pool=self.sdk_client_pool)

        iteration = 1
        while iteration < 5:
            storage_quota = int(re.findall('\d+', str(self.bucket_util.get_storage_quota(self.cluster.buckets[0])))[0])
            self.log.info("Iteration =={} , storage_quota =={}".format(iteration, storage_quota))
            if storage_quota > storage_quota_init:
                break
            self.sleep(60, "60 second sleep before next iteration")
            iteration+=1
        self.assertTrue(storage_quota > storage_quota_init, "EBS scaling fails")

    def test_storage_limit(self):
        """
        1 creating n buckets, setting different storage limit for these buckets
        2 doc loading in these buckets in batches and flagging buckets whose limit is reached
        3 verifying if storage limit is honoured by doing a small doc load
          and verifying num items
        4 verifying disk used is within 1 gb range of the limit
        """
        # checking total disk usage of given buckets
        self.clean_sandbox()
        def get_total_disk_usage(bucket_array):
            total_disk_usage = 0
            for bucket in bucket_array:
                total_disk_usage += float(
                int(self.bucket_util.get_disk_used(bucket)))
            return total_disk_usage

        # checking breaking conditions before next batch data loading
        def check_conditions():
            for buck in range(len(self.cluster.buckets)):
                if not self.bucket_info[self.cluster.buckets[buck]][limit_reached]:
                    return True
            return False

        # method for doc generation and triggering data load
        def start_data_load(buckets=None):
            loader_map = dict()
            for bucket in buckets:
                for scope in bucket.scopes.keys():
                    for collection in bucket.scopes[
                        scope].collections.keys():
                        if scope == CbServer.system_scope:
                            continue
                    work_load_settings = DocLoaderUtils.get_workload_settings(
                        key=self.key, key_size=self.key_size,
                        doc_size=self.doc_size,
                        create_perc=100, create_start=self.bucket_info[
                            bucket][create_start],
                        create_end=self.bucket_info[
                            bucket][create_end],
                        ops_rate=self.ops_rate)
                    dg = DocumentGenerator(work_load_settings,
                                           self.key_type, self.val_type)
                    loader_map.update(
                        {"%s:%s:%s" % (
                        bucket.name, scope, collection): dg})
            self.init_sdk_pool_object()
            self.create_sdk_client_pool(buckets=buckets,
                                        req_clients_per_bucket=2)

            result, loading_tasks = DocLoaderUtils.perform_doc_loading(
                self.doc_loading_tm, loader_map, self.cluster,
                buckets, async_load=True, validate_results=False,
                sdk_client_pool=self.sdk_client_pool)
            return loading_tasks

        # test start
        self.log.info("============== test_storage_limit starts=============")
        self.create_required_buckets()
        self.get_servers_for_databases()
        self.data_load_during_rebalance = str(self.input.param(
            "data_load_during_rebalance", False))
        data_storage_limit = str(self.input.param("data_storage_limit", 1))
        '''
         Step 1: Set storage limit
        '''
        data_storage_limit = data_storage_limit.split(":")
        self.bucket_info = dict()
        create_start = "create_start"
        create_end = "create_end"
        disk_used_in_gb = "disk_used_in_gbs"
        prev_item_count = "prev_item_count"
        storage_limits = "storage_limits"
        limit_reached = "limit_reached"
        new_item_count = "new_item_count"
        for bucket in range(len(self.cluster.buckets)):
            if bucket >= len(data_storage_limit):
                limit = int(data_storage_limit[0])
            else:
                limit = int(data_storage_limit[bucket])
            self.bucket_info[self.cluster.buckets[bucket]] = dict()
            self.bucket_util.set_throttle_n_storage_limit(
                self.cluster.buckets[bucket], throttle_limit=5000, storage_limit=limit)
            self.bucket_info[self.cluster.buckets[bucket]][create_start] = 0
            self.bucket_info[self.cluster.buckets[bucket]][create_end] = 10000
            disk_usage = float(int(self.bucket_util.get_disk_used(
                self.cluster.buckets[bucket])))
            disk_usage_gb = disk_usage/1024/1024/1024
            self.bucket_info[self.cluster.buckets[bucket]][
                disk_used_in_gb] = disk_usage_gb
            self.bucket_info[self.cluster.buckets[bucket]][
                prev_item_count] = self.bucket_util.get_items_count(self.cluster.buckets[bucket])
            self.bucket_info[self.cluster.buckets[bucket]][
                limit_reached] = False
            storage_limit = self.bucket_util.get_storage_limit(self.cluster.buckets[bucket])
            self.bucket_info[self.cluster.buckets[bucket]][storage_limits] = storage_limit
            self.log.info("Storage Limit for {}  == {}".format(
                self.cluster.buckets[bucket].name, storage_limit))
            self.log.debug("disk_used_in_GBs {}".format(disk_usage_gb))

        '''
        Step 2: Start data load
        '''
        while check_conditions():
            buckets_to_load_data = []
            for data_load_bucket in self.cluster.buckets:
                if self.bucket_info[data_load_bucket][limit_reached]:
                    continue
                buckets_to_load_data.append(data_load_bucket)
            loading_tasks = start_data_load(buckets_to_load_data)
            self.doc_loading_tm.getAllTaskResult()
            # marking bucket to leave for next iteration if num items not
            # changed
            for task in loading_tasks:
                for optype, failures in task.failedMutations.items():
                    for failure in failures:
                        if failure is not None and failure.err().getClass().getSimpleName() == "CouchbaseException":
                            sleep_iterations = [90, 50, 40]
                            total_disk_usage_before_wait = \
                                get_total_disk_usage(buckets_to_load_data)
                            for i in range(len(sleep_iterations)):
                                self.sleep(sleep_iterations[i],
                                           "Sleep before next data load iteration")
                                if get_total_disk_usage(
                                        buckets_to_load_data) < \
                                        total_disk_usage_before_wait:
                                    break
                            for bucket in buckets_to_load_data:
                                # updating buckets info in the bucket info map
                                disk_used_in_byte = float(int(
                                    self.bucket_util.get_disk_used(bucket)))
                                self.bucket_info[bucket][disk_used_in_gb] = \
                                    disk_used_in_byte / 1024 / 1024 / 1024
                                if self.bucket_info[bucket][disk_used_in_gb] >= (
                                self.bucket_info[bucket][storage_limits]):
                                    self.bucket_info[bucket][
                                        limit_reached] = True
                            break
                    else:
                        continue
                    break
                else:
                    continue
                break

            for bucket in buckets_to_load_data:
                disk_used_in_byte = \
                    float(int(self.bucket_util.get_disk_used(bucket)))
                self.bucket_info[bucket][disk_used_in_gb] = \
                    disk_used_in_byte / 1024 / 1024 / 1024
                self.bucket_info[bucket][create_start] = \
                    self.bucket_info[bucket][create_end]
                self.bucket_info[bucket][create_end] = \
                    self.bucket_info[bucket][create_start] + 10000

        '''
        Case: test if storage limit honoured during/after re-balance 
        '''
        if self.data_load_during_rebalance:
            scenario = dict()
            for bucket in self.cluster.buckets:
                scenario[bucket.name] = {Bucket.width: 2}
            to_track = self.__trigger_bucket_param_updates(scenario)
            self.sleep(5, "Wait for re-balance to start")
            for bucket in self.cluster.buckets:
                self.bucket_info[bucket][create_start] = \
                    self.bucket_info[bucket][create_end]
                self.bucket_info[bucket][create_end] = \
                    self.bucket_info[bucket][create_start] + 500000
            start_data_load(self.cluster.buckets)
            monitor_task = self.bucket_util.async_monitor_database_scaling(
                to_track, timeout=600)
            self.task_manager.get_task_result(monitor_task)

        '''
         Step 3 : Data load for storage limit validation
        '''
        for bucket in self.cluster.buckets:
            item_count = self.bucket_util.get_items_count(bucket)
            self.bucket_info[bucket][prev_item_count] = item_count
            self.log.info("Num_items_count in {} == {}".format(
                bucket.name, item_count))
            self.bucket_info[bucket][create_start] = int(item_count)
            self.bucket_info[bucket][create_end] = self.bucket_info[
                bucket][create_start] + 10000
        start_data_load(self.cluster.buckets)
        self.doc_loading_tm.getAllTaskResult()

        '''
           Step 4: Storage Limit Validation
        '''
        for bucket in self.cluster.buckets:
            self.bucket_info[bucket][new_item_count] = \
                self.bucket_util.get_items_count(bucket)
            disk_used_in_bytes = float(int(
                self.bucket_util.get_disk_used(bucket)))
            disk_used_in_gbs = disk_used_in_bytes / 1024 / 1024 / 1024
            self.bucket_info[bucket][disk_used_in_gb] = disk_used_in_gbs
            if self.bucket_info[bucket][prev_item_count] != self.bucket_info[
                bucket][new_item_count]:
                self.log.info("disk_used after final load for {} = {}".
                              format(bucket.name, disk_used_in_gbs))
                if disk_used_in_gbs < (self.bucket_info[bucket][
                    storage_limits] + 0.2):
                    self.log.info(
                        "new_item_count {} >  item_count{} since disk used "
                        "< storage limit for bucket {}".format(
                            self.bucket_info[bucket][new_item_count],
                            self.bucket_info[bucket][prev_item_count], bucket.name))
                else:
                    self.assertTrue(self.bucket_info[bucket][new_item_count] ==
                                    self.bucket_info[bucket][prev_item_count],
                                    "new_item_count {} >  item_count{}".
                                    format(self.bucket_info[bucket][new_item_count],
                                           self.bucket_info[bucket][prev_item_count]))
            else:
                self.log.info("New_item_count {} == item_count{}".
                              format(self.bucket_info[bucket][new_item_count],
                                     self.bucket_info[bucket][prev_item_count]))
