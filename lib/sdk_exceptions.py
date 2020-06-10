class SDKException:
    CasMismatchException = \
        "com.couchbase.client.core.error.CasMismatchException"
    CollectionDoesNotExistException = \
        "com.couchbase.client.core.error.CollectionDoesNotExistException"
    CollectionsNotAvailableException = \
        "com.couchbase.client.core.error.CollectionsNotAvailableException"
    DecodingFailedException = \
        "com.couchbase.client.core.error.DecodingFailedException"
    DocumentExistsException = \
        "com.couchbase.client.core.error.DocumentExistsException"
    DocumentNotFoundException = \
        "com.couchbase.client.core.error.DocumentNotFoundException"
    DocumentLockedException = \
        "com.couchbase.client.core.error.DocumentLockedException"
    DurabilityAmbiguousException = \
        "com.couchbase.client.core.error.DurabilityAmbiguousException"
    DurabilityImpossibleException = \
        "com.couchbase.client.core.error.DurabilityImpossibleException"
    DurableWriteInProgressException = \
        "com.couchbase.client.core.error.DurableWriteInProgressException"
    FeatureNotAvailableException = \
        "com.couchbase.client.core.error.FeatureNotAvailableException"

    TimeoutException = \
        "com.couchbase.client.core.error.TimeoutException"
    AmbiguousTimeoutException = \
        "com.couchbase.client.core.error.AmbiguousTimeoutException"
    UnambiguousTimeoutException = \
        "com.couchbase.client.core.error.UnambiguousTimeoutException"

    PathNotFoundException = \
        "com.couchbase.client.core.error.subdoc.PathNotFoundException"
    RequestCanceledException = \
        "com.couchbase.client.core.error.RequestCanceledException"
    TemporaryFailureException = \
        "com.couchbase.client.core.error.TemporaryFailureException"
    ValueTooLargeException = \
        "com.couchbase.client.core.error.ValueTooLargeException"

    RetryExhaustedException = \
        "com.couchbase.client.core.retry.reactor.RetryExhaustedException"
    TransactionExpired = "com.couchbase.transactions.error.TransactionExpired"

    def __init__(self):
        pass

    class RetryReason:
        KV_SYNC_WRITE_IN_PROGRESS = \
            "KV_SYNC_WRITE_IN_PROGRESS"
        KV_SYNC_WRITE_IN_PROGRESS_NO_MORE_RETRIES = \
            "NO_MORE_RETRIES (KV_SYNC_WRITE_IN_PROGRESS)"
        KV_TEMPORARY_FAILURE = "KV_TEMPORARY_FAILURE"

        def __init__(self):
            pass
