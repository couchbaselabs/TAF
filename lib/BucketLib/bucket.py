'''
Created on Sep 25, 2017

@author: riteshagarwal
'''
from memcached.helper.kvstore import KVStore

class Bucket(object):
    name = "name"
    replicas = "replicas"
    size = "size"
    type = "type"
    replicaIndex = "replicaIndex"
    eviction_policy = "eviction_policy"
    priority = "priority"
    flush_enabled = "flush_enabled"
    lww = "lww"
    maxTTL = "maxTTL"
    compressionMode = "compressionMode"
    uuid = "uuid"
    
    class bucket_type:
        MEMBASE = "membase"
        EPHEMERAL = "ephemeral"
        MEMCACHED = "memcached"
    
    class bucket_eviction_policy:
        VALUE_ONLY = "valueOnly"
        FULL_EVICTION = "fullEviction"
        NO_EVICTION = "noEviction"
        
    class bucket_compression_mode:
        ACTIVE = "active"
        PASSIVE = "passive"
        OFF = "off"

    class vBucket():
        def __init__(self):
            self.master = ''
            self.replica = []
            self.id = -1
    
    class BucketStats():
        def __init__(self):
            self.opsPerSec = 0
            self.itemCount = 0
            self.diskUsed = 0
            self.memUsed = 0
            self.ram = 0

    def __init__(self, new_params={}):
        self.name = new_params.get(Bucket.name, "default")
        self.type = new_params.get(Bucket.type, Bucket.bucket_type.MEMBASE)
        self.replicas = new_params.get(Bucket.replicas, 0)
        self.size = new_params.get(Bucket.size, 100)
        self.kvs = {1:KVStore()}
        self.eviction_policy = new_params.get(Bucket.eviction_policy, Bucket.bucket_eviction_policy.VALUE_ONLY)
        self.replicaIndex = new_params.get(Bucket.replicaIndex, 0)
        self.priority = new_params.get(Bucket.priority, None)
        self.uuid = None
        self.lww = new_params.get(Bucket.lww, False)
        self.maxTTL = new_params.get(Bucket.maxTTL, None)
        self.flush_enabled = new_params.get(Bucket.flush_enabled, 1)
        self.compressionMode = new_params.get(Bucket.compressionMode, Bucket.bucket_compression_mode.PASSIVE)
        self.nodes = None
        self.stats = None
        self.servers = []
        self.vbuckets = []
        self.forward_map = []

    def __str__(self):
        return self.params['name']

class vBucket():
    def __init__(self):
        self.master = ''
        self.replica = []
        self.id = -1

class BucketStats():
    def __init__(self):
        self.opsPerSec = 0
        self.itemCount = 0
        self.diskUsed = 0
        self.memUsed = 0
        self.ram = 0
        
