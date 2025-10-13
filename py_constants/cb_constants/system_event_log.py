class NsServer(object):
    """Mapping of ns_server events - event_id. Valid range 0-1023"""
    NodeAdded = 0
    ServiceStarted = 1
    RebalanceStarted = 2
    RebalanceComplete = 3
    RebalanceFailure = 4
    RebalanceInterrupted = 5
    GracefulFailoverStarted = 6
    GracefulFailoverComplete = 7
    GracefulFailoverFailed = 8
    GracefulFailoverInterrupted = 9
    HardFailoverStarted = 10
    HardFailoverComplete = 11
    HardFailoverFailed = 12
    HardFailoverInterrupted = 13
    AutoFailoverStarted = 14
    AutoFailoverComplete = 15
    AutoFailoverFailed = 16
    AutoFailoverWarning = 17
    MasterSelected = 18
    ServiceCrashed = 19


class KvEngine(object):
    """Mapping of KV related events - event_id. Valid range 8192:9215"""
    BucketCreated = 8192
    BucketDeleted = 8193
    ScopeCreated = 8194
    ScopeDropped = 8195
    CollectionCreated = 8196
    CollectionDropped = 8197
    BucketFlushed = 8198
    BucketOnline = 8199
    BucketOffline = 8200
    BucketConfigChanged = 8201
    MemcachedConfigChanged = 8202
    EphemeralAutoReprovision = 8203


class Security(object):
    """Mapping of Security related events - event_id. Valid range 9216:10239"""
    AuditEnabled = 9216
    AuditDisabled = 9217
    AuditSettingChanged = 9218
    LdapConfigChanged = 9219
    SecurityConfigChanged = 9220
    SasldAuthConfigChanged = 9221
    PasswordPolicyChanged = 9222
    UserAdded = 9223
    UserRemoved = 9224
    GroupAdded = 9225
    GroupRemoved = 9226


class Views(object):
    """Mapping of Views related events - event_id. Valid range 10240:11263"""


class Query(object):
    """Mapping of Query related events - event_id. Valid range 1024:2047"""


class Index(object):
    """Mapping of Index (2i) related events - event_id. Valid range 2048:3071"""


class Fts(object):
    """Mapping of FTS (search) related events - event_id. Valid range 3072:4095"""


class Eventing(object):
    """Mapping of Eventing related events - event_id. Valid range 4096:5119"""


class Analytics(object):
    """Mapping of Analytics related events - event_id. Valid range 5120:6143"""
    ProcessStarted = 5120
    ProcessCrashed = 5121
    ProcessExited = 5122
    TopologyChangeStarted = 5123
    TopologyChangeFailed = 5124
    TopologyChangeCompleted = 5125
    CollectionCreated = 5254
    CollectionMapped = 5255
    CollectionDropped = 5256
    CollectionDetached = 5257
    CollectionAttached = 5258
    CollectionRollback = 5259
    ScopeCreated = 5260
    ScopeDropped = 5261
    IndexCreated = 5262
    IndexDropped = 5263
    LinkCreated = 5264
    LinkAltered = 5265
    LinkDropped = 5266
    LinkConnected = 5267
    LinkDisconnected = 5268
    SettingChanged = 5269
    UserDefinedLibraryCreated = 5270
    UserDefinedLibraryReplaced = 5271
    UserDefinedLibraryDropped = 5272
    UserDefinedFunctionCreated = 5273
    UserDefinedFunctionReplaced = 5274
    UserDefinedFunctionDropped = 5275
    SynonymCreated = 5276
    SynonymDropped = 5277
    ViewCreated = 5278
    ViewReplaced = 5279
    ViewDropped = 5280
    BucketConnected = 5281
    BucketConnectFailed = 5282
    BucketDisconnected = 5283
    PartitionTopologyUpdated = 5284
    CollectionAnalyzed = 5287
    CollectionStatsDropped = 5288


class Xdcr(object):
    """Mapping of XDCR related events - event_id. Valid range 7168:8191"""
    CreateReplication = 0
    IncomingReplication = 0
    ModifyReplication = 0
    RemoveReplication = 0
    ProcessCrashed = 0
    SettingChanged = 0
    Paused = 0
    Resumed = 0


class Backup(object):
    """Mapping of Backup related events - event_id. Valid range 6143:7167"""
