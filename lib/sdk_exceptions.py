class SDKException(object):
    AuthenticationException = "AuthenticationException"

    CasMismatchException = [
        "com.couchbase.client.core.error.CasMismatchException",
        "CasMismatchException",
    ]
    CollectionNotFoundException = [
        "com.couchbase.client.core.error.CollectionNotFoundException",
        "CollectionNotFoundException",
    ]
    CouchbaseException = [
        "com.couchbase.client.core.error.CouchbaseException",
        "CouchbaseException",
    ]
    DecodingFailedException = [
        "com.couchbase.client.core.error.DecodingFailedException",
        "DecodingFailedException",
    ]
    DocumentExistsException = [
        "com.couchbase.client.core.error.DocumentExistsException",
        "DocumentExistsException",
    ]
    DocumentNotFoundException = [
        "com.couchbase.client.core.error.DocumentNotFoundException",
        "DocumentNotFoundException",
    ]
    DocumentLockedException = [
        "com.couchbase.client.core.error.DocumentLockedException",
        "DocumentLockedException",
    ]
    DurabilityAmbiguousException = [
        "com.couchbase.client.core.error.DurabilityAmbiguousException",
        "DurabilityAmbiguousException",
    ]
    DurabilityImpossibleException = [
        "com.couchbase.client.core.error.DurabilityImpossibleException",
        "DurabilityImpossibleException",
    ]
    DurabilitySyncWriteAmbiguousException = [
        "DurabilitySyncWriteAmbiguousException"
    ]
    DurableWriteInProgressException = [
        "com.couchbase.client.core.error.DurableWriteInProgressException",
        "DurableWriteInProgressException"
    ]
    FeatureNotAvailableException = [
        "com.couchbase.client.core.error.FeatureNotAvailableException",
        "FeatureNotAvailableException",
    ]
    ScopeNotFoundException = [
        "com.couchbase.client.core.error.ScopeNotFoundException",
        "ScopeNotFoundException",
    ]

    TimeoutException = [
        "com.couchbase.client.core.error.TimeoutException",
        "TimeoutException",
    ]
    AmbiguousTimeoutException = [
        "com.couchbase.client.core.error.AmbiguousTimeoutException",
        "AmbiguousTimeoutException",
    ]
    UnambiguousTimeoutException = [
        "com.couchbase.client.core.error.UnambiguousTimeoutException",
        "UnambiguousTimeoutException",
    ]

    PathNotFoundException = [
        "com.couchbase.client.core.error.subdoc.PathNotFoundException",
        "PathNotFoundException",
    ]
    LookUpPathNotFoundException = [
        "PATH_NOT_FOUND",
    ]
    PathExistsException = [
        "com.couchbase.client.java.error.subdoc.PathExistsException",
    ]
    RequestCanceledException = [
        "com.couchbase.client.core.error.RequestCanceledException",
        "RequestCanceledException",
    ]
    TemporaryFailureException = [
        "com.couchbase.client.core.error.TemporaryFailureException",
        "TemporaryFailureException",
    ]
    ValueTooLargeException = [
        "com.couchbase.client.core.error.ValueTooLargeException",
        "Value is too big",
        "ValueTooLargeException",
    ]

    RetryExhaustedException = [
        "com.couchbase.client.core.retry.reactor.RetryExhaustedException",
        "RetryExhaustedException",
    ]
    TransactionExpired = [
        "com.couchbase.transactions.error.TransactionExpired",
        "TransactionExpired",
    ]

    ServerOutOfMemoryException = [
        "com.couchbase.client.core.error.ServerOutOfMemoryException",
        "ServerOutOfMemoryException",
    ]

    class RetryReason(object):
        SyncWriteAmbiguous = "SyncWriteAmbiguous"
        KV_Sync_Write_In_Progress = "key_value_sync_write_in_progress"
        # Below are Java related reasons
        KV_SYNC_WRITE_IN_PROGRESS = \
            "KV_SYNC_WRITE_IN_PROGRESS"
        KV_SYNC_WRITE_IN_PROGRESS_NO_MORE_RETRIES = \
            "NO_MORE_RETRIES (KV_SYNC_WRITE_IN_PROGRESS)"
        KV_TEMPORARY_FAILURE = "KV_TEMPORARY_FAILURE"
        KV_COLLECTION_OUTDATED = "KV_COLLECTION_OUTDATED"
        COLLECTION_NOT_FOUND = "COLLECTION_NOT_FOUND"
        COLLECTION_MAP_REFRESH_IN_PROGRESS = \
            "COLLECTION_MAP_REFRESH_IN_PROGRESS"

    @staticmethod
    def check_if_exception_exists(expected_exception, result):
        if isinstance(expected_exception, list):
            for exception in expected_exception:
                if exception in str(result):
                    return True
            return False
        # Expected Exception is a string
        return expected_exception in str(result)
