class NsServer(object):
    """Mapping of ns_server events - event_id. Valid range 0-1023"""
    BabySitterRespawn = 0
    RebalanceStarted = 0
    RebalanceSuccess = 0
    RebalanceFailure = 0
    NodeAdded = 0
    NodeOffline = 0
    GracefulFailoverManual = 0
    AutoFailover = 0
    AutoFailoverFailure = 0
    HardFailoverManual = 0
    OrchestratorChange = 0
    TopologyChange = 0


class KvEngine(object):
    """Mapping of KV related events - event_id. Valid range 8192:9215"""
    BucketCreated = 8192
    BucketDeleted = 8193
    BucketFlushed = 8198
    BucketOffline = 8200
    BucketOnline = 8199
    BucketUpdated = 8201
    CollectedCreated = 8196
    CollectedDropped = 8197
    ScopeCreated = 8194
    ScopeDropped = 8195
    EphemeralAutoReprovision = 0
    MemcachedCrashed = 0
    MemcachedSettingChanged = 8202


class Security(object):
    """Mapping of Security related events - event_id. Valid range 9216:10239"""
    AuditEnabled = 9216
    AuditDisabled = 9217
    AuditSettingChanged = 9218
    GroupAdded = 0
    GroupRemoved = 0
    LdapEnabledDisabledForGroup = 0
    LdapEnabledDisabledForUsers = 0
    LdapSettingChanged = 9219
    TlsSettingChanged = 9220
    PamEnabledDisabled = 0
    PasswordPolicyChanged = 9222
    UserAdded = 0
    UserRemoved = 0


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
    NodeRestart = 0
    ProcessCrashed = 0
    DataSetCreated = 0
    DataSetDeleted = 0
    DateVerseCreated = 0
    DateVerseDeleted = 0
    IndexCreated = 0
    IndexDropped = 0
    LinkCreated = 0
    LinkDropped = 0
    SettingChanged = 0


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
