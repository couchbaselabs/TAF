package com.couchbase.test.transactions;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import com.couchbase.client.core.cnc.EventSubscription;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupAttemptEvent;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.transactions.config.TransactionOptions;

public class Transaction {
    public TransactionOptions get_transaction_options(DurabilityLevel d_level, Duration timeout) {
        TransactionOptions t_options = TransactionOptions.transactionOptions();
        t_options = t_options
            .durabilityLevel(d_level)
            .timeout(timeout);
        return t_options;
    }

    public static long get_cleanup_timeout(CoreTransactionsConfig tnx_config, long start_time) {
        long elapsed_time, cleanup_timeout;
        // Curr_time - start_time
        elapsed_time = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS) - start_time;
        // Trans_expiry_time - Elapsed_trans_time
        cleanup_timeout = tnx_config.transactionExpirationTime().getSeconds() - elapsed_time;
        // (Trans_expiry_time - Elapsed_trans_time) + cleanupWindow
        cleanup_timeout += tnx_config.cleanupConfig().cleanupWindow().getSeconds();
        // Extra time buffer for cleanup to trigger
        cleanup_timeout += 15;
        return cleanup_timeout;
    }

    public EventSubscription record_cleanup_attempt_events(Cluster cluster, Set<String> attemptIds) {
        return cluster.environment().eventBus().subscribe(event -> {
            if (event instanceof TransactionCleanupAttemptEvent) {
                String curr_attempt_id = ((TransactionCleanupAttemptEvent)event).attemptId();
                if (((TransactionCleanupAttemptEvent)event).success()) {
                    attemptIds.add(curr_attempt_id);
                }
            }
        });
    }

    public void waitForTransactionCleanupEvent(Cluster cluster, List<TransactionCleanupAttemptEvent> attempts,
                                               Set<String> attemptIds, long trans_timeout) {
        Iterator<TransactionCleanupAttemptEvent> it = attempts.iterator();
        long curr_time = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        long end_time = curr_time + trans_timeout;
        System.out.println("Cleanup timeout " + trans_timeout + " seconds");
        while(it.hasNext()) {
            String id_to_check = (it.next()).attemptId();
            System.out.println(curr_time + " Waiting for cleanup event for: " + id_to_check);
            while (curr_time <= end_time) {
                if (attemptIds.contains(id_to_check)) {
                    System.out.println(curr_time + " TransactionCleanupAttemptEvent success: " + id_to_check);
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
