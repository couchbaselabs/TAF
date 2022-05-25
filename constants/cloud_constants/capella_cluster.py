class CloudCluster(object):
    class Services(object):
        KV = "data"

    class Bucket(object):
        name = "name"
        conflictResolutionType = "bucketConflictResolution"
        ramQuotaMB = "memoryAllocationInMb"
        flushEnabled = "flush"
        replicaNumber = "replicas"
        durabilityMinLevel = "durabilityLevel"
        maxTTL = "timeToLive"
