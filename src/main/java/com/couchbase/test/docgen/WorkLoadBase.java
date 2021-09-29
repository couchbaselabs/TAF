package com.couchbase.test.docgen;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

public abstract class WorkLoadBase {
    public String keyPrefix = "test_docs-";
    public int keySize = 15;
    int min_key_size = 10;
    int max_key_size = min_key_size;

    public int docSize = 256;
    public AtomicInteger itr;

    public DurabilityLevel durability;
    public Duration timeout;
    public PersistTo persist_to;
    public ReplicateTo replicate_to;
    public RetryStrategy retryStrategy;

    public Duration getDuration(Integer timeout, String time_unit) {
        ChronoUnit chrono_unit = ChronoUnit.MILLIS;
        time_unit = time_unit.toLowerCase();
        switch(time_unit) {
            case "seconds":
                chrono_unit = ChronoUnit.SECONDS;
                break;
            case "ms":
                chrono_unit = ChronoUnit.MILLIS;
                break;
            case "min":
                chrono_unit = ChronoUnit.MINUTES;
                break;
            case "hr":
                chrono_unit = ChronoUnit.HOURS;
                break;
            case "days":
                chrono_unit = ChronoUnit.DAYS;
                break;
        }
        return Duration.of(timeout, chrono_unit);
    }

    public void setTimeoutDuration(Integer timeout, String time_unit) {
        if(timeout == null)
            return;

        this.timeout = this.getDuration(timeout, time_unit);
    }

    public void setRetryStrategy(String retryStrategy) {
        if (retryStrategy != null)
            retryStrategy = retryStrategy.toUpperCase();
        if (retryStrategy.equals("FAIL_FAST"))
            this.retryStrategy = FailFastRetryStrategy.INSTANCE;
        else
            this.retryStrategy = BestEffortRetryStrategy.INSTANCE;

    }

    public void setDurabilityLevel(String durabilityLevel) {
        switch(durabilityLevel) {
            case "NONE":
                this.durability = DurabilityLevel.NONE;
                break;
            case "MAJORITY":
                this.durability = DurabilityLevel.MAJORITY;
                break;
            case "MAJORITY_AND_PERSIST_TO_ACTIVE":
                this.durability = DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
                break;
            case "PERSIST_TO_MAJORITY":
                this.durability = DurabilityLevel.PERSIST_TO_MAJORITY;
                break;
        }
    }

    public void setPersistTo(int persist_to) {
        switch(persist_to) {
            case 0:
                this.persist_to = PersistTo.NONE;
                break;
            case 1:
                this.persist_to = PersistTo.ACTIVE;
                break;
            case 2:
                this.persist_to = PersistTo.TWO;
                break;
            case 3:
                this.persist_to = PersistTo.THREE;
                break;
            case 4:
                this.persist_to = PersistTo.FOUR;
                break;
        }
    }

    public void setReplicateTo(int replicate_to) {
        switch(replicate_to) {
            case 0:
                this.replicate_to = ReplicateTo.NONE;
                break;
            case 1:
                this.replicate_to = ReplicateTo.ONE;
                break;
            case 2:
                this.replicate_to = ReplicateTo.TWO;
                break;
            case 3:
                this.replicate_to = ReplicateTo.THREE;
                break;
        }
    }
}
