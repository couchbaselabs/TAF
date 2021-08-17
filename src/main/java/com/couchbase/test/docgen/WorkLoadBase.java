package com.couchbase.test.docgen;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.couchbase.client.core.msg.kv.DurabilityLevel;

public abstract class WorkLoadBase {
    public String keyPrefix = "test_docs-";
    public int keySize = 15;
	int min_key_size = 10;
	int max_key_size = min_key_size;

    public int docSize = 256;
    public AtomicInteger itr;

    public DurabilityLevel durability;
    public Duration timeout;

    public void setTimeoutDuration(Integer timeout, String time_unit) {
        if(timeout == null)
            return;

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

        this.timeout = Duration.of(timeout, chrono_unit);
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
}
