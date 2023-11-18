class SDKConstants(object):
    class RetryStrategy(object):
        FAIL_FAST = "fail_fast"
        BEST_EFFORT = "best_effort"

    class TimeUnit(object):
        MILLISECONDS = "milliseconds"
        SECONDS = "seconds"
        MINUTES = "minutes"
        HOURS = "hours"
        DAYS = "days"

    class DurabilityLevel(object):
        NONE = "NONE"
        MAJORITY = "MAJORITY"
        MAJORITY_AND_PERSIST_TO_ACTIVE = "MAJORITY_AND_PERSIST_TO_ACTIVE"
        PERSIST_TO_MAJORITY = "PERSIST_TO_MAJORITY"
