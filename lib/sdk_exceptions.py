# check_if_exception_exists matches the target exception to equivalent Exception from all possible SDK
def check_if_exception_exists(target_exception, *Exceptions):
    # type: (str, list) -> bool
    for exceptions in Exceptions:
        for e in exceptions:
            if e in str(target_exception):
                return True
    return False


class SDKException(object):
    CasMismatchException = \
        ["com.couchbase.client.core.error.CasMismatchException", "cas mismatch"]
    CollectionNotFoundException = \
        ["com.couchbase.client.core.error.CollectionNotFoundException", "collection not found"]
    CouchbaseException = \
        ["com.couchbase.client.core.error.CouchbaseException"]
    DecodingFailedException = \
        ["com.couchbase.client.core.error.DecodingFailedException", "decoding failure"]
    DocumentExistsException = \
        ["com.couchbase.client.core.error.DocumentExistsException", "document exists"]
    DocumentNotFoundException = \
        ["com.couchbase.client.core.error.DocumentNotFoundException", "document not found"]
    DocumentLockedException = \
        ["com.couchbase.client.core.error.DocumentLockedException", "document locked"]
    DurabilityAmbiguousException = \
        ["com.couchbase.client.core.error.DurabilityAmbiguousException", "durability ambiguous"]
    DurabilityImpossibleException = \
        ["com.couchbase.client.core.error.DurabilityImpossibleException", "durability impossible"]
    DurableWriteInProgressException = \
        ["com.couchbase.client.core.error.DurableWriteInProgressException", "durability write in progress"]
    FeatureNotAvailableException = \
        ["com.couchbase.client.core.error.FeatureNotAvailableException", "feature is not available"]
    ScopeNotFoundException = \
        ["com.couchbase.client.core.error.ScopeNotFoundException", "scope not found"]

    TimeoutException = \
        ["com.couchbase.client.core.error.TimeoutException", "operation has timed out"]
    AmbiguousTimeoutException = \
        ["com.couchbase.client.core.error.AmbiguousTimeoutException", "ambiguous timeout"]
    UnambiguousTimeoutException = \
        ["com.couchbase.client.core.error.UnambiguousTimeoutException", "unambiguous timeout"]

    PathNotFoundException = \
        ["com.couchbase.client.core.error.subdoc.PathNotFoundException", "path not found"]
    LookUpPathNotFoundException = \
        ["PATH_NOT_FOUND", "path invalid"]
    PathExistsException = \
        ["com.couchbase.client.java.error.subdoc.PathExistsException", "path exists"]
    RequestCanceledException = \
        ["com.couchbase.client.core.error.RequestCanceledException", "request canceled"]
    TemporaryFailureException = \
        ["com.couchbase.client.core.error.TemporaryFailureException", "temporary failure"]
    ValueTooLargeException = \
        ["com.couchbase.client.core.error.ValueTooLargeException", "value too large"]

    RetryExhaustedException = \
        ["com.couchbase.client.core.retry.reactor.RetryExhaustedException"]
    TransactionExpired = ["com.couchbase.transactions.error.TransactionExpired"]

    ServerOutOfMemoryException = \
        ["com.couchbase.client.core.error.ServerOutOfMemoryException"]

    class RetryReason(object):
        KV_SYNC_WRITE_IN_PROGRESS = \
            "KV_SYNC_WRITE_IN_PROGRESS"
        KV_SYNC_WRITE_IN_PROGRESS_NO_MORE_RETRIES = \
            "NO_MORE_RETRIES (KV_SYNC_WRITE_IN_PROGRESS)"
        KV_TEMPORARY_FAILURE = "KV_TEMPORARY_FAILURE"
        KV_COLLECTION_OUTDATED = "KV_COLLECTION_OUTDATED"
        COLLECTION_NOT_FOUND = "COLLECTION_NOT_FOUND"
        COLLECTION_MAP_REFRESH_IN_PROGRESS = \
            "COLLECTION_MAP_REFRESH_IN_PROGRESS"
