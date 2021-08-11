from com.couchbase.client.core.msg.kv import DurabilityLevel
from com.couchbase.client.core.retry import \
    BestEffortRetryStrategy, FailFastRetryStrategy

from com.couchbase.client.java.kv import \
    GetAllReplicasOptions, \
    GetOptions, \
    InsertOptions, \
    MutateInOptions, \
    PersistTo, \
    RemoveOptions, \
    ReplaceOptions, \
    ReplicateTo, \
    TouchOptions, \
    UpsertOptions

from java.time import Duration
from java.time.temporal import ChronoUnit

from BucketLib.bucket import Bucket
from constants.sdk_constants.java_client import SDKConstants


class SDKOptions(object):
    @staticmethod
    def get_duration(time, time_unit):
        time_unit = time_unit.lower()
        if time_unit == SDKConstants.TimeUnit.MILLISECONDS:
            temporal_unit = ChronoUnit.MILLIS
        elif time_unit == SDKConstants.TimeUnit.MINUTES:
            temporal_unit = ChronoUnit.MINUTES
        elif time_unit == SDKConstants.TimeUnit.HOURS:
            temporal_unit = ChronoUnit.HOURS
        elif time_unit == SDKConstants.TimeUnit.DAYS:
            temporal_unit = ChronoUnit.DAYS
        else:
            temporal_unit = ChronoUnit.SECONDS

        return Duration.of(time, temporal_unit)

    @staticmethod
    def get_persist_to(persist_to):
        try:
            persist_list = [PersistTo.NONE, PersistTo.ONE, PersistTo.TWO,
                            PersistTo.THREE, PersistTo.FOUR]
            return persist_list[persist_to]
        except IndexError:
            return PersistTo.ACTIVE

    @staticmethod
    def get_replicate_to(replicate_to):
        try:
            replicate_list = [ReplicateTo.NONE, ReplicateTo.ONE,
                              ReplicateTo.TWO, ReplicateTo.THREE]
            return replicate_list[replicate_to]
        except IndexError:
            return ReplicateTo.NONE

    @staticmethod
    def get_durability_level(durability_level):
        durability_level = durability_level.upper()
        if durability_level == Bucket.DurabilityLevel.MAJORITY:
            return DurabilityLevel.MAJORITY

        if durability_level == \
                Bucket.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE:
            return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE

        if durability_level == Bucket.DurabilityLevel.PERSIST_TO_MAJORITY:
            return DurabilityLevel.PERSIST_TO_MAJORITY

        return DurabilityLevel.NONE

    @staticmethod
    def set_options(options, cas=0,
                    exp=None, exp_unit=SDKConstants.TimeUnit.SECONDS,
                    persist_to=None, replicate_to=None,
                    durability=None,
                    timeout=None, timeunit=SDKConstants.TimeUnit.SECONDS,
                    store_semantics=None,
                    preserve_expiry=None, retry_strategy=None,
                    access_deleted=False, create_as_deleted=False):
        """
        Generic method to set options for all CRUD operations.
        Note: Value 'None' means don't set any thing explicitly.
              Default value should be taken implicitly
        """
        # Set CAS value in CRUD options (Mostly used in doc_replace)
        if cas > 0:
            options = options.cas(cas)

        # Set Expiry value
        if exp is not None:
            options = options.expiry(SDKOptions.get_duration(exp, exp_unit))

        # Set Preserve_expiry (Useful during doc/sub_doc updates)
        if preserve_expiry is not None:
            options = options.preserveExpiry(preserve_expiry)

        # Set Durability / Observe options
        if durability is not None:
            options = options.durability(
                SDKOptions.get_durability_level(durability))
        elif replicate_to is not None and persist_to is not None:
            options = options.durability(
                SDKOptions.get_persist_to(persist_to),
                SDKOptions.get_replicate_to(replicate_to))

        # Set TimeOut option
        options = options.timeout(SDKOptions.get_duration(timeout, timeunit))

        # Set Retry_Strategy
        if retry_strategy == SDKConstants.RetryStrategy.FAIL_FAST:
            options = \
                options.retryStrategy(FailFastRetryStrategy.INSTANCE)
        elif retry_strategy == SDKConstants.RetryStrategy.BEST_EFFORT:
            options = \
                options.retryStrategy(BestEffortRetryStrategy.INSTANCE)

        # Set access deleted
        if access_deleted:
            options = options.accessDeleted(access_deleted)

        # Set create as deleted
        if create_as_deleted:
            options = options.createAsDeleted(create_as_deleted)

        # Set Store_Semantics option (Only for sub_doc)
        if store_semantics is not None:
            options = options.storeSemantics(store_semantics)
        return options

    # Document operations' getOptions APIs
    @staticmethod
    def get_insert_options(exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                           persist_to=0, replicate_to=0,
                           timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                           durability="", sdk_retry_strategy=None):
        return SDKOptions.set_options(
            InsertOptions.insertOptions(),
            exp=exp, exp_unit=exp_unit,
            persist_to=persist_to, replicate_to=replicate_to,
            durability=durability, timeout=timeout, timeunit=time_unit,
            retry_strategy=sdk_retry_strategy)

    @staticmethod
    def get_read_options(timeout, time_unit=SDKConstants.TimeUnit.SECONDS,
                         sdk_retry_strategy=None):
        return SDKOptions.set_options(GetOptions.getOptions(),
                                      timeout=timeout, timeunit=time_unit,
                                      retry_strategy=sdk_retry_strategy)

    @staticmethod
    def get_upsert_options(exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                           persist_to=0, replicate_to=0,
                           timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                           durability="", preserve_expiry=None,
                           sdk_retry_strategy=None):
        return SDKOptions.set_options(
            UpsertOptions.upsertOptions(),
            exp=exp, exp_unit=exp_unit,
            replicate_to=replicate_to, persist_to=persist_to,
            durability=durability, timeout=timeout, timeunit=time_unit,
            preserve_expiry=preserve_expiry, retry_strategy=sdk_retry_strategy)

    @staticmethod
    def get_remove_options(persist_to=0, replicate_to=0,
                           timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                           durability="", cas=0, sdk_retry_strategy=None):
        return SDKOptions.set_options(
            RemoveOptions.removeOptions(),
            cas=cas,
            persist_to=persist_to, replicate_to=replicate_to,
            durability=durability,
            timeout=timeout, timeunit=time_unit,
            retry_strategy=sdk_retry_strategy)

    @staticmethod
    def get_replace_options(exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                            persist_to=0, replicate_to=0,
                            timeout=5,
                            time_unit=SDKConstants.TimeUnit.SECONDS,
                            durability="", cas=0,
                            preserve_expiry=None, sdk_retry_strategy=None):
        return SDKOptions.set_options(
            ReplaceOptions.replaceOptions(),
            cas=cas, exp=exp, exp_unit=exp_unit,
            persist_to=persist_to, replicate_to=replicate_to,
            durability=durability, timeout=timeout, timeunit=time_unit,
            preserve_expiry=preserve_expiry, retry_strategy=sdk_retry_strategy)

    @staticmethod
    def get_touch_options(timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                          sdk_retry_strategy=None):
        return SDKOptions.set_options(TouchOptions.touchOptions(),
                                      timeout=timeout, timeunit=time_unit,
                                      retry_strategy=sdk_retry_strategy)

    @staticmethod
    def get_mutate_in_options(exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                              persist_to=0, replicate_to=0, timeout=5,
                              time_unit=SDKConstants.TimeUnit.SECONDS,
                              durability="",
                              store_semantics=None,
                              preserve_expiry=None, sdk_retry_strategy=None,
                              access_deleted=False,
                              create_as_deleted=False):
        return SDKOptions.set_options(
            MutateInOptions.mutateInOptions(),
            exp=exp, exp_unit=exp_unit,
            persist_to=persist_to, replicate_to=replicate_to,
            timeout=timeout, timeunit=time_unit,
            durability=durability, store_semantics=store_semantics,
            preserve_expiry=preserve_expiry, retry_strategy=sdk_retry_strategy,
            access_deleted=access_deleted, create_as_deleted=create_as_deleted)
