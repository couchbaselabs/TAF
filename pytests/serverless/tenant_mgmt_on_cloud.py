import math
import time
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
        self.doc_loading_tm = TaskManager(2)
        self.db_name = "TAF-TenantMgmtOnCloud"
        self.token = self.input.capella.get("token")
        self.with_data_load = self.input.param("with_data_load", False)
        self.capella_api = CapellaAPI(self.pod.url_public, None, None,
                                      self.token)
        self.validate_stat = self.input.param("validate_stat", False)

    def tearDown(self):
        if self.sdk_client_pool:
            self.sdk_client_pool.shutdown()
        super(TenantMgmtOnCloud, self).tearDown()

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

    def get_servers_for_databases(self, cluster, pod):
        dataplanes = dict()
        for bucket in cluster.buckets:
            dataplane_id = self.serverless_util.get_database_dataplane_id(
                pod, bucket.name)
            self.log.info("dataplane_id is %s" % dataplane_id)
            if dataplane_id not in dataplanes:
                dataplanes[dataplane_id] = dict()
                dataplanes[dataplane_id]["node"], \
                    dataplanes[dataplane_id]["username"], \
                    dataplanes[dataplane_id]["password"] = \
                    self.serverless_util.bypass_dataplane(dataplane_id)
            cluster.nodes_in_cluster = \
                self.cluster_util.construct_servers_from_master_details(
                    dataplanes[dataplane_id]["node"],
                    dataplanes[dataplane_id]["username"],
                    dataplanes[dataplane_id]["password"])
            cluster.master = cluster.nodes_in_cluster[0]
            self.bucket_util.get_updated_bucket_server_list(cluster, bucket)

    def __get_single_bucket_scenarios(self, target_scenario):
        scenarios = list()
        bucket_name = choice(self.cluster.buckets).name
        weight_incr = {1: 30, 2: 15, 3: 10, 4: 7}
        weight_start = {1: 30, 2: 210, 3: 270, 4: 300}
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
                weight += weight_incr[width]
                if weight > 390:
                    width += 1
                    if width in weight_start:
                        weight = weight_start[width]
            scenarios = scenarios + scenarios[::-1][1:]
        elif target_scenario == "single_bucket_width_weight_random":
            max_scenarios = 20
            # Creates 20 random scenarios of random width/weight update
            for scenario_index in range(max_scenarios):
                width = choice(range(1, 5))
                weight = weight_start[width] \
                    + (weight_incr[width] * choice(range(1, 13)))
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

    def create_database(self, bucket=None):
        if not bucket:
            bucket = self.get_serverless_bucket_obj(
                self.db_name, self.bucket_width, self.bucket_weight)
        task = self.bucket_util.async_create_database(
            self.cluster, bucket, dataplane_id=self.dataplane_id)
        self.task_manager.get_task_result(task)

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
                    loader_map.update(
                        {"%s:%s:%s" % (bucket.name, scope, collection): dg})

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
        db_name = "tntmgmtrecreatedb"
        max_itr = 5
        bucket = self.get_serverless_bucket_obj(
            db_name, self.bucket_width, self.bucket_weight)

        for itr in range(1, max_itr):
            self.log.info("Iteration :: %s" % itr)
            bucket_name = self.serverless_util.create_serverless_database(
                self.cluster.pod, self.cluster.tenant, bucket.name,
                "aws", "us-east-1",
                bucket.serverless.width, bucket.serverless.weight)
            self.log.info("Bucket %s created" % bucket_name)
            self.serverless_util.delete_database(
                self.pod, self.tenant, bucket_name)
            self.serverless_util.wait_for_database_deleted(
                self.tenant, bucket_name)
        task = self.bucket_util.async_create_database(self.cluster, bucket)
        self.task_manager.get_task_result(task)

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
        bucket = self.get_serverless_bucket_obj("123,", 1, 30)
        task = self.bucket_util.async_create_database(self.cluster, bucket)
        try:
            self.task_manager.get_task_result(task)
        except ExecutionException as exception:
            if invalid_char_err not in str(exception):
                self.fail("Exception mismatch. Got::%s" % exception)

        bucket = self.get_serverless_bucket_obj("1", 1, 30)
        task = self.bucket_util.async_create_database(self.cluster, bucket)
        try:
            self.task_manager.get_task_result(task)
        except ExecutionException as exception:
            if name_too_short_err not in str(exception):
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
            self.get_servers_for_databases(self.cluster, self.pod)
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

            doc_loading_tasks = DocLoaderUtils.perform_doc_loading(
                self.doc_loading_tm, loader_map,
                self.cluster, self.cluster.buckets,
                async_load=True, validate_results=False,
                sdk_client_pool=self.sdk_client_pool)

        for scenario in scenarios:
            to_track = self.__trigger_bucket_param_updates(scenario)
            monitor_task = self.bucket_util.async_monitor_database_scaling(
                to_track, timeout=600)
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
            self.get_servers_for_databases(self.cluster, self.pod)
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
            to_track, timeout=600)
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
            to_track, timeout=600)
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
            to_track, timeout=600)
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
            self.get_servers_for_databases(self.cluster, self.pod)

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
                tasks = DocLoaderUtils.perform_doc_loading(
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
                to_track, timeout=600)
            self.task_manager.get_task_result(monitor_task)

        if self.validate_stat:
            self.get_servers_for_databases(self.cluster, self.pod)
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
        self.assertTrue("sandbox" in self.pod.url_public,
                        "Test supported only on sandbox env!")

        self.log.info("Making sure there are no active dataplanes deployed")
        resp = self.serverless_util.get_all_dataplanes()
        req = {'errorType': 'NotFound', 'message': 'Not Found.'}
        self.assertEqual(resp, req, "Dataplanes present ! :: %s" % resp)

        self.bucket_width = 1
        self.bucket_weight = 30
        self.db_name = "tntMgmt-initial-db"
        self.create_database()
        bucket = self.cluster.buckets[0]

        self.log.info("Validating num_dataplanes deployed")
        resp = self.serverless_util.get_all_dataplanes()
        self.assertTrue(isinstance(resp, list) and len(resp) == 1,
                        "Deployed more than 1 dataplane")

        dp_id = self.serverless_util.get_database_dataplane_id(self.pod,
                                                               bucket.name)
        self.log.info("Bucket :: %s, Dataplane :: %s" % (bucket.name, dp_id))
        try:
            host, u_name, pwd = self.serverless_util.bypass_dataplane(dp_id)
            self.cluster.servers = \
                self.cluster_util.construct_servers_from_master_details(
                    host, u_name, pwd)
            for server in self.cluster.servers:
                server.port = CbServer.ssl_port
            self.cluster.master = self.cluster.servers[0]
            self.cluster_util.update_cluster_nodes_service_list(self.cluster)
            self.log.critical("Total nodes in the cluster: %s"
                              % len(self.cluster.servers))
            self.log.critical("KV Nodes: %s" % len(self.cluster.kv_nodes))
            self.log.critical("Index Nodes: %s"
                              % len(self.cluster.index_nodes))
            self.log.critical("Query Nodes: %s"
                              % len(self.cluster.query_nodes))
            self.log.critical("Fts Nodes: %s" % len(self.cluster.fts_nodes))
            self.log.critical("CBAS Nodes: %s" % len(self.cluster.cbas_nodes))
            self.log.critical("Eventing Nodes: %s"
                              % len(self.cluster.eventing_nodes))
            self.log.critical("Backup Nodes: %s"
                              % len(self.cluster.backup_nodes))
            self.assertEqual(3, len(self.cluster.kv_nodes),
                             "Mismatch in KV nodes")
            self.assertEqual(2, len(self.cluster.index_nodes),
                             "Mismatch in index nodes")
            self.assertEqual(2, len(self.cluster.query_nodes),
                             "Mismatch in query nodes")
            self.assertEqual(2, len(self.cluster.fts_nodes),
                             "Mismatch in fts nodes")
            self.assertEqual(0, len(self.cluster.eventing_nodes),
                             "Mismatch in eventing nodes")
            self.assertEqual(0, len(self.cluster.cbas_nodes),
                             "Mismatch in cbas nodes")
            self.assertEqual(0, len(self.cluster.backup_nodes),
                             "Mismatch in backup nodes")
            self.assertEqual(9, len(self.cluster.servers),
                             "Mismatch in total nodes")
        finally:
            self.log.info("Cleaning up database and dataplane")
            self.serverless_util.delete_database(
                self.pod, self.tenant, bucket.name)
            self.serverless_util.wait_for_database_deleted(
                self.tenant, bucket.name)
            self.cluster.buckets.remove(bucket)
            self.serverless_util.delete_dataplane(dp_id)

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
