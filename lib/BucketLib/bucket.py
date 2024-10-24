from threading import Lock

import Jython_tasks
from cb_constants import CbServer
from common_lib import Counter


class BucketStats(object):
    def __init__(self):
        # Used for plotting Bucket status table
        self.itemCount = 0
        self.diskUsed = 0
        self.memUsed = 0
        self.ram = 0
        self.manifest_uid = 0
        self.expected_item_count = 0
        self.mutex = Lock()
        # Used to manage Bucket stats task
        self.stats_task = {"count": 0,
                           "object": None,
                           "lock": Lock()}

    def increment_manifest_uid(self):
        """
        Increments the manifest UID of bucket object.
        :return: None
        """
        # Uses Lock to safely update the uid during threaded execution
        self.mutex.acquire()
        self.manifest_uid += 1
        self.mutex.release()

    def manage_task(self, operation, task_manager,
                    cluster=None, bucket=None,
                    monitor_stats=list(), sleep=1):
        task_pkg = Jython_tasks.task
        if operation == "start":
            self.stats_task["lock"].acquire()
            if self.stats_task["object"] is None:
                self.stats_task["object"] = task_pkg.PrintBucketStats(
                    cluster, bucket,
                    monitor_stats=monitor_stats,
                    sleep=sleep)
                task_manager.add_new_task(self.stats_task["object"])
            self.stats_task["count"] += 1
            self.stats_task["lock"].release()
        elif operation == "stop":
            self.stats_task["lock"].acquire()
            if self.stats_task["count"] == 1:
                self.stats_task["object"].end_task()
                task_manager.get_task_result(self.stats_task["object"])
                self.stats_task["object"] = None
            self.stats_task["count"] -= 1
            self.stats_task["lock"].release()


class Scope(object):
    def __init__(self, scope_spec=dict()):
        self.name = scope_spec.get("name")
        self.collections = dict()

        # Meta data for test case validation
        self.is_dropped = False
        self.recreated = 0

    def __str__(self):
        return self.name

    def get_dict_object(self):
        return {"name": self.name}

    @staticmethod
    def recreated(scope_obj, scope_spec):
        # Update meta fields
        scope_obj.is_dropped = False
        scope_obj.recreated += 1


class Collection(object):
    def __init__(self, collection_spec=dict()):
        self.name = collection_spec.get("name")
        self.num_items = collection_spec.get("num_items", 0)
        self.maxTTL = collection_spec.get("maxTTL", -1)
        self.history = collection_spec.get("history", "false")

        # Meta data for test case validation
        self.is_dropped = False
        self.recreated = 0
        # Meta to preserve inserted doc index to support further CRUDs
        self.doc_index = (0, 0)
        self.sub_doc_index = (0, 0)

    def __str__(self):
        return self.name

    def get_dict_object(self):
        return {"name": self.name, "maxTTL": self.maxTTL,
                "history": self.history}

    @staticmethod
    def flushed(collection_obj, skip_resetting_num_items=False):
        """
        :collection_obj: collection_obj for which index, and num_items have to be set to 0
        :skip_resetting_num_items: Boolean on whether to skip resetting num_items
            this argument is to support cases where we need to skip resetting num_items,
            so that same number of items can be reloaded into the collection after bucket flush
        """
        if not skip_resetting_num_items:
            collection_obj.num_items = 0
        collection_obj.doc_index = (0, 0)
        collection_obj.sub_doc_index = (0, 0)

    @staticmethod
    def recreated(collection_obj, collection_spec):
        collection_obj.num_items = collection_spec.get("num_items", 0)
        collection_obj.maxTTL = collection_spec.get("maxTTL", -1)
        collection_obj.history = collection_spec.get("history", "false")

        # Update meta fields
        collection_obj.is_dropped = False
        collection_obj.recreated += 1
        collection_obj.doc_index = (0, 0)


class Serverless(object):
    def __init__(self):
        self.width = None
        self.weight = None
        self.nebula_obj = None
        self.nebula_endpoint = None
        self.dapi = None
        self.dataplane_id = None


class Bucket(object):
    name = "name"
    ramQuotaMB = "ramQuotaMB"
    bucketType = "bucketType"
    replicaNumber = "replicaNumber"
    evictionPolicy = "evictionPolicy"
    priority = "priority"
    rank = "rank"
    flushEnabled = "flushEnabled"
    conflictResolutionType = "conflictResolutionType"
    storageBackend = "storageBackend"
    maxTTL = "maxTTL"
    replicaIndex = "replicaIndex"
    threadsNumber = "threadsNumber"
    compressionMode = "compressionMode"
    uuid = "uuid"
    durabilityMinLevel = "durabilityMinLevel"
    purge_interval = "purge_interval"
    autoCompactionDefined = "autoCompactionDefined"
    fragmentationPercentage = "fragmentationPercentage"
    numVBuckets = "numVBuckets"
    width = "width"
    weight = "weight"
    historyRetentionCollectionDefault = "historyRetentionCollectionDefault"
    historyRetentionSeconds = "historyRetentionSeconds"
    historyRetentionBytes = "historyRetentionBytes"
    magmaKeyTreeDataBlockSize = "magmaKeyTreeDataBlockSize"
    magmaSeqTreeDataBlockSize = "magmaSeqTreeDataBlockSize"
    durabilityImpossibleFallback = "durabilityImpossibleFallback"

    # Tracks the last bucket/scope/collection counter created in the cluster
    bucket_counter = Counter()
    scope_counter = Counter()
    collection_counter = Counter()

    class Type(object):
        EPHEMERAL = "ephemeral"
        MEMBASE = "couchbase"

    class ReplicaNum(object):
        ZERO = 0
        ONE = 1
        TWO = 2
        THREE = 3

    class DurabilityMinLevel(object):
        NONE = "none"
        MAJORITY = "majority"
        MAJORITY_AND_PERSIST_ACTIVE = "majorityAndPersistActive"
        PERSIST_TO_MAJORITY = "persistToMajority"

    class EvictionPolicy(object):
        FULL_EVICTION = "fullEviction"
        NO_EVICTION = "noEviction"
        NRU_EVICTION = "nruEviction"
        VALUE_ONLY = "valueOnly"

    class ConflictResolution(object):
        SEQ_NO = "seqno"
        TIMESTAMP_BASED = "lww"

    class CompressionMode(object):
        ACTIVE = "active"
        PASSIVE = "passive"
        OFF = "off"

    class Priority(object):
        LOW = "low"
        HIGH = "high"

    class FlushBucket(object):
        DISABLED = 0
        ENABLED = 1

    class vBucket:
        ACTIVE = "active"
        REPLICA = "replica"
        MIN_VALUE = 16
        MAX_VALUE = 1024

        def __init__(self):
            self.master = ''
            self.replica = []
            self.id = -1

    class StorageBackend(object):
        magma = "magma"
        couchstore = "couchstore"

    def __init__(self, new_params=dict()):
        # Default values based on Couchbase document,
        # docs.couchbase.com/server/current/rest-api/rest-bucket-create.html

        self.name = new_params.get(Bucket.name, "default")
        self.bucketType = new_params.get(Bucket.bucketType,
                                         Bucket.Type.MEMBASE)
        self.replicaNumber = new_params.get(Bucket.replicaNumber,
                                            Bucket.ReplicaNum.ONE)
        self.ramQuotaMB = new_params.get(Bucket.ramQuotaMB, 256)
        self.replicaIndex = new_params.get(Bucket.replicaIndex, 1)
        self.storageBackend = new_params.get(Bucket.storageBackend,
                                             Bucket.StorageBackend.magma)
        self.rank = new_params.get(Bucket.rank, 0)
        self.priority = new_params.get(Bucket.priority, Bucket.Priority.LOW)
        self.uuid = None
        self.conflictResolutionType = \
            new_params.get(Bucket.conflictResolutionType,
                           Bucket.ConflictResolution.SEQ_NO)
        self.maxTTL = new_params.get(Bucket.maxTTL, 0)
        self.flushEnabled = new_params.get(Bucket.flushEnabled,
                                           Bucket.FlushBucket.DISABLED)
        self.compressionMode = new_params.get(
            Bucket.compressionMode,
            Bucket.CompressionMode.PASSIVE)
        self.durabilityMinLevel = new_params.get(
            Bucket.durabilityMinLevel,
            Bucket.DurabilityMinLevel.NONE)
        self.purge_interval = new_params.get(Bucket.purge_interval, 1)
        self.autoCompactionDefined = new_params.get(
            Bucket.autoCompactionDefined, "false")
        self.fragmentationPercentage = new_params.get(
            Bucket.fragmentationPercentage, False)
        self.historyRetentionCollectionDefault = new_params.get(
            Bucket.historyRetentionCollectionDefault, "true")
        self.historyRetentionBytes = new_params.get(
            Bucket.historyRetentionBytes, 0)
        self.historyRetentionSeconds = new_params.get(
            Bucket.historyRetentionSeconds, 0)
        self.magmaKeyTreeDataBlockSize = new_params.get(
            Bucket.magmaKeyTreeDataBlockSize, 4096)
        self.magmaSeqTreeDataBlockSize = new_params.get(
            Bucket.magmaSeqTreeDataBlockSize, 4096)
        self.durabilityImpossibleFallback = new_params.get(
            Bucket.durabilityImpossibleFallback, None)

        if self.bucketType == Bucket.Type.EPHEMERAL:
            self.evictionPolicy = new_params.get(
                Bucket.evictionPolicy,
                Bucket.EvictionPolicy.NO_EVICTION)
            if self.evictionPolicy not in [Bucket.EvictionPolicy.NRU_EVICTION,
                                           Bucket.EvictionPolicy.NO_EVICTION]:
                self.evictionPolicy = Bucket.EvictionPolicy.NO_EVICTION
        else:
            self.evictionPolicy = new_params.get(
                Bucket.evictionPolicy,
                Bucket.EvictionPolicy.FULL_EVICTION)

        num_vbs = new_params.get(Bucket.numVBuckets, None)

        self.bucketCapabilities = list()
        self.nodes = None
        self.stats = BucketStats()
        self.servers = list()
        self.numVBuckets = int(num_vbs) if num_vbs else None
        self.vbuckets = list()
        self.forward_map = list()
        self.scopes = dict()

        # Serverless
        self.serverless = None
        b_width = new_params.get(Bucket.width, None)
        b_weight = new_params.get(Bucket.weight, None)
        if b_weight is not None and b_width is not None:
            self.serverless = Serverless()
            self.serverless.width = b_width
            self.serverless.weight = b_weight

        # Create default scope-collection association
        hist_for_def_col = self.historyRetentionCollectionDefault \
            if self.storageBackend == Bucket.StorageBackend.magma else "false"
        scope = Scope({"name": CbServer.default_scope})
        collection = Collection({"name": CbServer.default_collection,
                                 "history": hist_for_def_col})
        scope.collections[CbServer.default_collection] = collection
        self.scopes[CbServer.default_scope] = scope

        scope = Scope({"name": CbServer.system_scope})
        self.scopes[CbServer.system_scope] = scope
        for c_name in [CbServer.query_collection,
                       CbServer.mobile_collection]:
            collection = Collection({"name": c_name, "maxTTL": 0})
            scope.collections[c_name] = collection

        # Only if Serverless profile is enabled on the cluster
        if CbServer.cluster_profile == "serverless":
            for c_name in [CbServer.eventing_collection,
                           CbServer.transaction_collection]:
                collection = Collection({"name": c_name, "maxTTL": 0})
                scope.collections[c_name] = collection

    def __str__(self):
        return self.name

    @staticmethod
    def get_params():
        param_list = list()
        for param in vars(Bucket).keys():
            if not (param.startswith("_")
                    or callable(getattr(Bucket, param))):
                param_list.append(param)
        return param_list

    @staticmethod
    def set_defaults(bucket):
        if bucket.storageBackend == Bucket.StorageBackend.magma:
            if bucket.historyRetentionCollectionDefault is None:
                bucket.historyRetentionCollectionDefault = "true"
            if bucket.historyRetentionSeconds is None:
                bucket.historyRetentionSeconds = 0
            if bucket.historyRetentionBytes is None:
                bucket.historyRetentionBytes = 0
            if bucket.magmaSeqTreeDataBlockSize is None:
                bucket.magmaSeqTreeDataBlockSize = 4096
            if bucket.magmaSeqTreeDataBlockSize is None:
                bucket.magmaKeyTreeDataBlockSize = 4096
            if bucket.durabilityImpossibleFallback is None:
                bucket.durabilityImpossibleFallback = "disabled"
            bucket.scopes[CbServer.default_scope].collections[
                CbServer.default_collection].history = bucket.historyRetentionCollectionDefault


class TravelSample(Bucket):
    def __init__(self):
        bucket_param = dict()
        bucket_param["name"] = "travel-sample"
        super(TravelSample, self).__init__(bucket_param)
        self.stats.expected_item_count = 63288

        # Update scope-collections with num_items
        self.scopes[CbServer.default_scope].collections[
            CbServer.default_collection].num_items = 31591
        s_name = "inventory"
        scope = Scope({"name": s_name})
        # Collection data format: (col_name, item_count)
        inventory_collections = [["airline", 187],
                                 ["airport", 1968],
                                 ["hotel", 917],
                                 ["landmark", 4495],
                                 ["route", 24024]]
        for c_data in inventory_collections:
            scope.collections[s_name] = Collection({"name": c_data[0],
                                                    "num_items": c_data[1]})
        self.scopes[s_name] = scope


class BeerSample(Bucket):
    def __init__(self):
        bucket_param = dict()
        bucket_param["name"] = "beer-sample"
        super(BeerSample, self).__init__(bucket_param)
        self.stats.expected_item_count = 7303
        # Update scope-collections with num_items
        self.scopes[CbServer.default_scope].collections[
            CbServer.default_collection].num_items = 7303


class GamesimSample(Bucket):
    def __init__(self):
        bucket_param = dict()
        bucket_param["name"] = "gamesim-sample"
        super(GamesimSample, self).__init__(bucket_param)
        self.stats.expected_item_count = 586
        # Update scope-collections with num_items
        self.scopes[CbServer.default_scope].collections[
            CbServer.default_collection].num_items = 586
