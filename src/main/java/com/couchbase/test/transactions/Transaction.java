package com.couchbase.test.transactions;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.EventSubscription;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;

import com.couchbase.transactions.TransactionAttempt;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.TransactionDurabilityLevel;
import com.couchbase.transactions.TransactionGetResult;
import com.couchbase.transactions.TransactionResult;
import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.log.LogDefer;
import com.couchbase.transactions.log.TransactionEvent;
import com.couchbase.transactions.log.TransactionCleanupAttempt;
import com.couchbase.transactions.log.TransactionCleanupEndRunEvent;

public class Transaction {
    public TransactionConfig createTransactionConfig(int trans_timeout, int kv_timeout, String durability) {
        TransactionConfigBuilder config = TransactionConfigBuilder.create().logDirectly(Event.Severity.VERBOSE);
        switch (durability) {
            case "MAJORITY":
                config.durabilityLevel(TransactionDurabilityLevel.MAJORITY);
                break;
            case "MAJORITY_AND_PERSIST_TO_ACTIVE":
                config.durabilityLevel(TransactionDurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE);
                break;
            case "PERSIST_TO_MAJORITY":
                config.durabilityLevel(TransactionDurabilityLevel.PERSIST_TO_MAJORITY);
                break;
            case "NONE":
                config.durabilityLevel(TransactionDurabilityLevel.NONE);
                break;
        }

        config = config.cleanupWindow(Duration.of(60, ChronoUnit.SECONDS));
        config = config.keyValueTimeout(Duration.of(kv_timeout, ChronoUnit.SECONDS));
        return config.expirationTime(Duration.of(trans_timeout, ChronoUnit.SECONDS)).build();
    }

    public Transactions createTransaction(Cluster cluster, TransactionConfig config) {
        Event.Severity logLevel = Event.Severity.INFO;
        cluster.environment().eventBus().subscribe(event -> {
            if (event instanceof TransactionEvent) {
                TransactionEvent te = (TransactionEvent) event;
                if (te.severity().ordinal() >= logLevel.ordinal()) {
                    System.out.println(te.getClass().getSimpleName() + ": " + event.description());

                    if (te.logs() != null) {
                        te.logs().forEach(log -> {
                            System.out.println(te.getClass().getSimpleName() + " log: " + log.toString());
                        });
                    }
                }
            }
        });
        return Transactions.create(cluster, config);
    }

    public static long get_cleanup_timeout(Transactions transaction, long start_time) {
        long elapsed_time, cleanup_timeout;
        // Curr_time - start_time
        elapsed_time = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS) - start_time;
        // Trans_expiry_time - Elapsed_trans_time
        cleanup_timeout = transaction.config().transactionExpirationTime().getSeconds() - elapsed_time;
        // (Trans_expiry_time - Elapsed_trans_time) + cleanupWindow
        cleanup_timeout += transaction.config().cleanupWindow().getSeconds();
        // Extra time buffer for cleanup to trigger
        cleanup_timeout += 15;
        return cleanup_timeout;
    }

    public EventSubscription record_cleanup_attempt_events(Cluster cluster, Set<String> attemptIds) {
        return cluster.environment().eventBus().subscribe(event -> {
            if (event instanceof TransactionCleanupAttempt) {
                String curr_attempt_id = ((TransactionCleanupAttempt)event).attemptId();
                if (((TransactionCleanupAttempt)event).success()) {
                    attemptIds.add(curr_attempt_id);
                }
            }
        });
    }

    public void waitForTransactionCleanupEvent(Cluster cluster, List<TransactionAttempt> attempts,
                                               Set<String> attemptIds, long trans_timeout) {
        Iterator<TransactionAttempt> it = attempts.iterator();
        long curr_time = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        long end_time = curr_time + trans_timeout;
        System.out.println("Cleanup timeout " + trans_timeout + " seconds");
        while(it.hasNext()) {
            String id_to_check = (it.next()).attemptId();
            System.out.println(curr_time + " Waiting for cleanup event for: " + id_to_check);
            while (curr_time <= end_time) {
                if (attemptIds.contains(id_to_check)) {
                    System.out.println(curr_time + " TransactionCleanupAttempt success: " + id_to_check);
                    break;
                }
                // Check for timeout case
                curr_time = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                if (curr_time > end_time) {
                    System.out.println(curr_time + " Timeout waiting for attemptId: " + id_to_check);
                    break;
                }
            }
        }
    }
}
