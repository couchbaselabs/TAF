package com.couchbase.test.loadgen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import reactor.util.function.Tuple2;

import com.couchbase.client.core.cnc.EventSubscription;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.test.docgen.DocType.Person;
import com.couchbase.test.docgen.DocumentGenerator;
import com.couchbase.test.sdk.SDKClient;
import com.couchbase.test.taskmanager.Task;
import com.couchbase.test.transactions.Transaction;

import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.TransactionGetResult;
import com.couchbase.transactions.TransactionResult;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.log.LogDefer;

public class TransactionWorkLoadGenerate extends Task{
    int batch_size;
    int num_transactions;
    List<?> load_pattern;

    Cluster cluster;
    Bucket bucket;
    Transactions transaction;
    Transaction trans_helper;
    DocumentGenerator doc_gen;
    Boolean commit, rollback;

    public TransactionWorkLoadGenerate(
            String taskName, Cluster cluster, Bucket bucket,
            Transactions transaction, DocumentGenerator doc_gen,
            int batch_size, int num_transactions, List<?> load_pattern,
            Boolean commit, Boolean rollback, Transaction trans_helper) {
        super(taskName);
        this.cluster = cluster;
        this.bucket = bucket;
        this.transaction = transaction;
        this.doc_gen = doc_gen;
        this.batch_size = batch_size;
        this.num_transactions = num_transactions;
        this.load_pattern = load_pattern;
        this.trans_helper = trans_helper;
    }

    List<Tuple2<String, Object>> get_docs() {
        List<Tuple2<String, Object>> docs = new ArrayList<Tuple2<String, Object>>();
        for(int index=0; index < this.batch_size; index++) {
            docs.add(this.doc_gen.next());
        }
        return docs;
    }

    void run_transaction(Collection col_obj, List<Tuple2<String, Object>> docs, List<String> ops) {
        List<LogDefer> res = new ArrayList<LogDefer>();
        long start_time = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        Set<String> attempt_ids = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        EventSubscription cleanup_es = trans_helper.record_cleanup_attempt_events(this.cluster, attempt_ids);
        try {
            TransactionResult result = transaction.run(ctx -> {
                for (String op_type : ops) {
                    for (Tuple2<String, Object> doc : docs) {
                        TransactionGetResult trans_get_result;

                        String doc_key = doc.getT1();
                        Object doc_val = doc.getT2();

                        switch (op_type) {
                            case "C":
                                trans_get_result = ctx.insert(col_obj, doc_key, doc_val);
                                break;
                            case "U":
                                trans_get_result = ctx.get(col_obj, doc_key);
//                                 JsonObject content = trans_get_result.contentAs(JsonObject.class);
//                                 JsonObject gen_doc = (JsonObject)doc_val;
//                                 gen_doc.put("mutated", content.getInt("mutated")+1);
                                ctx.replace(trans_get_result, doc_val);
                                break;
                            case "R":
                                trans_get_result = ctx.get(col_obj, doc_key);
                                break;
                            case "D":
                                trans_get_result = ctx.get(col_obj, doc_key);
                                ctx.remove(trans_get_result);
                                break;
                        }
                    }
                }
                if (this.rollback != null && this.rollback == true)
                    ctx.rollback();
                else if (this.commit != null && this.commit)
                    ctx.commit();
            });

            if (this.commit != null && this.commit && !result.unstagingComplete()) {
                long cleanup_timeout = trans_helper.get_cleanup_timeout(transaction, start_time);
                trans_helper.waitForTransactionCleanupEvent(
                    this.cluster, result.attempts(), attempt_ids, cleanup_timeout);
            }
        }
        catch (TransactionFailed err) {
            System.out.println(err.getCause());
            res = err.result().log().logs();
            if (res.toString().contains("DurabilityImpossibleException"))
                System.out.println("DurabilityImpossibleException seen");
            else
                for (LogDefer e : ((TransactionFailed) err).result().log().logs())
                    System.out.println(e);
            long cleanup_timeout = trans_helper.get_cleanup_timeout(transaction, start_time);
            trans_helper.waitForTransactionCleanupEvent(
                this.cluster, err.result().attempts(), attempt_ids, cleanup_timeout);
        }
    }

    @Override
    public void run() {
        Collection col_obj;
        String scope_name = (String)this.load_pattern.get(0);
        String col_name = (String)this.load_pattern.get(1);
        List<String> ops = (List)this.load_pattern.get(2);
        List<Tuple2<String, Object>> docs;

        // Select target collection using SDKClient object
        if(col_name == "_default")
            col_obj = this.bucket.defaultCollection();
        else
            col_obj = this.bucket.scope(scope_name).collection(col_name);

        for(int transaction_index=0; transaction_index<this.num_transactions; transaction_index++) {
            docs = this.get_docs();
            this.run_transaction(col_obj, docs, ops);
        }
    }
}
