class ClientException:
    KeyExistsException = \
        "com.couchbase.client.core.error.KeyExistsException"
    DurabilityImpossibleException = \
        "com.couchbase.client.core.error.DurabilityImpossibleException"
    DurableWriteInProgressException = \
        "com.couchbase.client.core.error.DurableWriteInProgressException"
    DurabilityAmbiguousException = \
        "com.couchbase.client.core.error.DurabilityAmbiguousException"
    RequestTimeoutException = \
        "com.couchbase.client.core.error.RequestTimeoutException"
    TemporaryFailureException = \
        "com.couchbase.client.core.error.TemporaryFailureException"
    RequestCanceledException = \
        "com.couchbase.client.core.error.RequestCanceledException"
    ValueTooLargeException = \
        "com.couchbase.client.core.error.ValueTooLargeException"
    KeyNotFoundException = \
        "com.couchbase.client.core.error.KeyNotFoundException"
    PathNotFoundException = \
        "com.couchbase.client.core.error.subdoc.PathNotFoundException"
    DecodingFailedException = \
        "com.couchbase.client.core.error.DecodingFailedException"
    FeatureNotAvailableException = \
        "com.couchbase.client.core.error.FeatureNotAvailableException"

    class RetryReason:
        KV_SYNC_WRITE_IN_PROGRESS = \
            "KV_SYNC_WRITE_IN_PROGRESS"
        KV_SYNC_WRITE_IN_PROGRESS_NO_MORE_RETRIES = \
            "NO_MORE_RETRIES (KV_SYNC_WRITE_IN_PROGRESS)"
        KV_TEMPORARY_FAILURE = "KV_TEMPORARY_FAILURE"
