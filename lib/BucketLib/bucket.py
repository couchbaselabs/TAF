class Bucket(object):
    name = "name"
    replicas = "replicas"
    ramQuotaMB = "ramQuotaMB"
    bucketType = "bucketType"
    replicaNumber = "replicaNumber"
    replicaServers = "replicaServers"
    evictionPolicy = "evictionPolicy"
    priority = "priority"
    flushEnabled = "flushEnabled"
    lww = "lww"
    maxTTL = "maxTTL"
    replicaIndex = "replicaIndex"
    threadsNumber = "threadsNumber"
    compressionMode = "compressionMode"
    uuid = "uuid"

    class Type:
        EPHEMERAL = "ephemeral"
        MEMBASE = "membase"
        MEMCACHED = "memcached"

    class EvictionPolicy:
        FULL_EVICTION = "fullEviction"
        NO_EVICTION = "noEviction"
        NRU_EVICTION = "nruEviction"
        VALUE_ONLY = "valueOnly"

    class CompressionMode:
        ACTIVE = "active"
        PASSIVE = "passive"
        OFF = "off"

    class Priority:
        LOW = 3
        HIGH = 8

    class vBucket:
        def __init__(self):
            self.master = ''
            self.replica = []
            self.id = -1

    class BucketStats:
        def __init__(self):
            self.opsPerSec = 0
            self.itemCount = 0
            self.expected_item_count = 0
            self.diskUsed = 0
            self.memUsed = 0
            self.ram = 0

    def __init__(self, new_params=dict()):
        self.name = new_params.get(Bucket.name, "default")
        self.bucketType = new_params.get(Bucket.bucketType,
                                         Bucket.Type.MEMBASE)
        self.replicaNumber = new_params.get(Bucket.replicaNumber, 0)
        self.replicaServers = new_params.get(Bucket.replicaServers, [])
        self.ramQuotaMB = new_params.get(Bucket.ramQuotaMB, 100)

        if self.bucketType == Bucket.Type.EPHEMERAL:
            self.evictionPolicy = new_params.get(
                Bucket.evictionPolicy,
                Bucket.EvictionPolicy.NO_EVICTION)
        else:
            self.evictionPolicy = new_params.get(
                Bucket.evictionPolicy,
                Bucket.EvictionPolicy.VALUE_ONLY)

        self.replicaIndex = new_params.get(Bucket.replicaIndex, 0)
        self.priority = new_params.get(Bucket.priority, None)
        self.threadsNumber = new_params.get(Bucket.threadsNumber, 3)
        self.uuid = None
        self.lww = new_params.get(Bucket.lww, False)
        self.maxTTL = new_params.get(Bucket.maxTTL, 0)
        self.flushEnabled = new_params.get(Bucket.flushEnabled, 1)
        self.compressionMode = new_params.get(
            Bucket.compressionMode,
            Bucket.CompressionMode.PASSIVE)
        self.nodes = None
        self.stats = Bucket.BucketStats()
        self.servers = list()
        self.vbuckets = list()
        self.forward_map = list()

    def __str__(self):
        return self.name


class TravelSample(Bucket):
    def __init__(self):
        bucket_param = dict()
        bucket_param["name"] = "travel-sample"
        super(TravelSample, self).__init__(bucket_param)
        self.stats.expected_item_count = 31591


class BeerSample(Bucket):
    def __init__(self):
        bucket_param = dict()
        bucket_param["name"] = "beer-sample"
        super(BeerSample, self).__init__(bucket_param)
        self.stats.expected_item_count = 7303


class GamesimSample(Bucket):
    def __init__(self):
        bucket_param = dict()
        bucket_param["name"] = "gamesim-sample"
        super(GamesimSample, self).__init__(bucket_param)
        self.stats.expected_item_count = 586
