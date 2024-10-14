package com.couchbase.test.loadgen;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import elasticsearch.EsClient;

import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.ServerOutOfMemoryException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.test.docgen.DocType.Person;
import com.couchbase.test.docgen.DocumentGenerator;
import com.couchbase.test.sdk.DocOps;
import com.couchbase.test.sdk.SDKClient;
import com.couchbase.test.sdk.SDKClientPool;
import com.couchbase.test.taskmanager.Task;

import com.couchbase.test.sdk.Result;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class WorkLoadGenerate extends Task{
    DocumentGenerator dg;
    public SDKClient sdk;
    public DocOps docops;
    public String durability;
    public boolean trackFailures = true;
    public int retryTimes = 0;
    public int exp;
    public String exp_unit;
    public String retryStrategy;
    public UpsertOptions upsertOptions;
    public UpsertOptions expiryOptions;
    public InsertOptions setOptions;
    public RemoveOptions removeOptions;
    public GetOptions getOptions;
    public SDKClientPool sdkClientPool;
    public String bucket_name;
    public String scope = "_default";
    public String collection = "_default";
    public boolean stop_loading = false;
    public HashMap<String, List<Result>> failedMutations;
    private EsClient esClient = null;
    static Logger logger = LogManager.getLogger(WorkLoadGenerate.class);

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClient client, String durability) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.sdk = client;
        this.durability = durability;
        this.failedMutations = new HashMap<String, List<Result>>();
    }

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClientPool client_pool, String durability) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.sdkClientPool = client_pool;
        this.durability = durability;
        this.failedMutations = new HashMap<String, List<Result>>();
    }

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClient client, String durability,
            int exp, String exp_unit, boolean trackFailures, int retryTimes) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.sdk = client;
        this.durability = durability;
        this.trackFailures = trackFailures;
        this.retryTimes = retryTimes;
        this.exp = exp;
        this.exp_unit = exp_unit;
        this.failedMutations = new HashMap<String, List<Result>>();
    }

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClientPool client_pool, String durability,
            int exp, String exp_unit, boolean trackFailures, int retryTimes) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.sdkClientPool = client_pool;
        this.durability = durability;
        this.trackFailures = trackFailures;
        this.retryTimes = retryTimes;
        this.exp = exp;
        this.exp_unit = exp_unit;
        this.failedMutations = new HashMap<String, List<Result>>();
    }

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClient client, String durability,
            int exp, String exp_unit, boolean trackFailures, int retryTimes, String retryStrategy) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.sdk = client;
        this.durability = durability;
        this.trackFailures = trackFailures;
        this.retryTimes = retryTimes;
        this.exp = exp;
        this.exp_unit = exp_unit;
        this.retryStrategy = retryStrategy;
        this.failedMutations = new HashMap<String, List<Result>>();
    }

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClientPool client_pool, String durability,
            int exp, String exp_unit, boolean trackFailures, int retryTimes, String retryStrategy) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.sdkClientPool = client_pool;
        this.durability = durability;
        this.trackFailures = trackFailures;
        this.retryTimes = retryTimes;
        this.exp = exp;
        this.exp_unit = exp_unit;
        this.retryStrategy = retryStrategy;
        this.failedMutations = new HashMap<String, List<Result>>();
    }

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClient client, EsClient esClient,
            String durability, int exp, String exp_unit, boolean trackFailures, int retryTimes) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.sdk = client;
        this.esClient = esClient;
        this.durability = durability;
        this.trackFailures = trackFailures;
        this.retryTimes = retryTimes;
        this.exp = exp;
        this.exp_unit = exp_unit;
        this.failedMutations = new HashMap<String, List<Result>>();
    }

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClientPool client_pool, EsClient esClient,
            String durability, int exp, String exp_unit, boolean trackFailures, int retryTimes) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.sdkClientPool = client_pool;
        this.esClient = esClient;
        this.durability = durability;
        this.trackFailures = trackFailures;
        this.retryTimes = retryTimes;
        this.exp = exp;
        this.exp_unit = exp_unit;
        this.failedMutations = new HashMap<String, List<Result>>();
    }

    public void stop_work_load() {
        this.stop_loading = true;
    }

    public void set_collection_for_load(String bucket_name, String scope, String collection) {
        this.bucket_name = bucket_name;
        this.scope = scope;
        this.collection = collection;
    }

    @Override
    public void run() {
        System.out.println("Starting " + this.taskName);
        // Set timeout in WorkLoadSettings
        this.dg.ws.setTimeoutDuration(60, "seconds");
        // Set Durability in WorkLoadSettings
        this.dg.ws.setDurabilityLevel(this.durability);
        this.dg.ws.setRetryStrategy(this.retryStrategy);

        upsertOptions = UpsertOptions.upsertOptions()
                .timeout(this.dg.ws.timeout)
                .durability(this.dg.ws.durability)
                .retryStrategy(this.dg.ws.retryStrategy);
        expiryOptions = UpsertOptions.upsertOptions()
                .timeout(this.dg.ws.timeout)
                .durability(this.dg.ws.durability)
                .expiry(this.dg.ws.getDuration(this.exp, this.exp_unit))
                .retryStrategy(this.dg.ws.retryStrategy);
        setOptions = InsertOptions.insertOptions()
                .timeout(this.dg.ws.timeout)
                .durability(this.dg.ws.durability)
                .retryStrategy(this.dg.ws.retryStrategy);
        removeOptions = RemoveOptions.removeOptions()
                .timeout(this.dg.ws.timeout)
                .durability(this.dg.ws.durability)
                .retryStrategy(this.dg.ws.retryStrategy);
        getOptions = GetOptions.getOptions()
                .timeout(this.dg.ws.timeout)
                .retryStrategy(this.dg.ws.retryStrategy);
        int ops = 0;
        boolean flag = false;

        Instant trackFailureTime_start = Instant.now();
        while(! this.stop_loading) {
//            Instant st = Instant.now();
            if (this.sdkClientPool != null)
                this.sdk = this.sdkClientPool.get_client_for_bucket(this.bucket_name, this.scope, this.collection);
//            Instant en = Instant.now();
//            System.out.println(this.taskName + " Time Taken to get client: " + Duration.between(st, en).toMillis() + "ms");
            Instant trackFailureTime_end = Instant.now();
            Duration timeElapsed = Duration.between(trackFailureTime_start, trackFailureTime_end);
            if(timeElapsed.toMinutes() > 5) {
                for (Entry<String, List<Result>> optype: this.failedMutations.entrySet())
                    logger.info("Failed mutations count so far: " + optype.getKey() + " == " + optype.getValue().size());
                trackFailureTime_start = Instant.now();
            }
            Instant start = Instant.now();
            if(dg.ws.creates > 0) {
//                st = Instant.now();
                List<Tuple2<String, Object>> docs = dg.nextInsertBatch();
//                en = Instant.now();
//                System.out.println(this.taskName + " Time Taken to generate " + docs.size() + " Docs: " + Duration.between(st, en).toMillis() + "ms");
                if (docs.size()>0) {
                    flag = true;
//                    st = Instant.now();
                    List<Result> result = docops.bulkInsert(this.sdk.connection, docs, setOptions);
                    if(this.esClient != null) {
                        BulkResponse es_result = this.esClient.insertDocs(this.sdk.collection.replace("_", "").toLowerCase(), docs);
                        if(es_result.errors())
                            System.out.println(es_result);
                    }
//                    en = Instant.now();
//                    System.out.println(this.taskName + " Time Taken by Inserts: " + Duration.between(st, en).toMillis() + "ms");
                    ops += dg.ws.batchSize*dg.ws.creates/100;
                    if(trackFailures && result.size()>0)
                        try {
                            this.failedMutations.get("create").addAll(result);
                        } catch (Exception e) {
                            this.failedMutations.put("create", result);
                        }
                }
            }
            if(dg.ws.updates > 0) {
                List<Tuple2<String, Object>> docs = dg.nextUpdateBatch();
                if (docs.size()>0) {
                    flag = true;
                    List<Result> result = docops.bulkUpsert(this.sdk.connection, docs, upsertOptions);
                    if(this.esClient != null) {
                        this.esClient.insertDocs(this.sdk.collection.replace("_", "").toLowerCase(), docs);
                    }
                    ops += dg.ws.batchSize*dg.ws.updates/100;
                    if(trackFailures && result.size()>0)
                        try {
                            this.failedMutations.get("update").addAll(result);
                        } catch (Exception e) {
                            this.failedMutations.put("update", result);
                        }
                }
            }
            if(dg.ws.expiry > 0) {
                List<Tuple2<String, Object>> docs = dg.nextExpiryBatch();
                if (docs.size()>0) {
                    flag = true;
                    List<Result> result = docops.bulkUpsert(this.sdk.connection, docs, expiryOptions);
                    ops += dg.ws.batchSize*dg.ws.expiry/100;
                    if(trackFailures && result.size()>0)
                        try {
                            this.failedMutations.get("expiry").addAll(result);
                        } catch (Exception e) {
                            this.failedMutations.put("expiry", result);
                        }
                }
            }
            if(dg.ws.deletes > 0) {
                List<String> docs = dg.nextDeleteBatch();
                if (docs.size()>0) {
                    flag = true;
                    List<Result> result = docops.bulkDelete(this.sdk.connection, docs, removeOptions);
                    if(this.esClient != null) {
                        this.esClient.deleteDocs(this.sdk.collection.replace("_", "").toLowerCase(), docs);
                    }
                    ops += dg.ws.batchSize*dg.ws.deletes/100;
                    if(trackFailures && result.size()>0)
                        try {
                            this.failedMutations.get("delete").addAll(result);
                        } catch (Exception e) {
                            this.failedMutations.put("delete", result);
                        }
                }
            }
            if(dg.ws.reads > 0) {
                List<Tuple2<String, Object>> docs = dg.nextReadBatch();
                if (docs.size()>0) {
                    flag = true;
                    List<Tuple2<String, Object>> res = docops.bulkGets(this.sdk.connection, docs, getOptions);
                    if (this.dg.ws.validate) {
                        Map<Object, Object> trnx_res = res.stream().collect(Collectors.toMap(t -> t.get(0), t -> t.get(1)));
                        Map<Object, Object> trnx_docs = docs.stream().collect(Collectors.toMap(t -> t.get(0), t -> t.get(1)));
                        ObjectMapper om = new ObjectMapper();
                        om.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
                        for (Object name : trnx_docs.keySet()) {
                            try {
                                String a = om.writeValueAsString(trnx_res.get(name));
                                String b = om.writeValueAsString(trnx_docs.get(name));
                                if(this.dg.ws.expectDeleted) {
                                    if(!a.contains(DocumentNotFoundException.class.getSimpleName())) {
                                        logger.fatal("Validation failed for key: " + this.sdk.scope + ":" + this.sdk.collection + ":" + name);
                                        logger.fatal("Actual Value - " + a);
                                        logger.fatal("Expected Value - " + b);
                                        if(this.sdkClientPool != null)
                                            this.sdkClientPool.release_client(this.sdk);
                                        logger.info(this.taskName + " is completed!");
                                        return;
                                    }
                                } else if(!a.equals(b) && !a.contains("TimeoutException")){
                                    logger.fatal("Validation failed for key: " + this.sdk.scope + ":" + this.sdk.collection + ":" + name);
                                    logger.fatal("Actual Value - " + a);
                                    logger.fatal("Expected Value - " + b);
                                    if(this.sdkClientPool != null)
                                        this.sdkClientPool.release_client(this.sdk);
                                    logger.fatal(this.taskName + " is completed!");
                                    return;
                                }
                            } catch (JsonProcessingException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    ops += dg.ws.batchSize*dg.ws.reads/100;
                }
            }
            if(this.sdkClientPool != null)
                this.sdkClientPool.release_client(this.sdk);
            if(ops == 0)
                this.stop_loading = true;
            else if(ops < dg.ws.ops/dg.ws.workers && flag) {
                flag = false;
                continue;
            }
            ops = 0;
            Instant end = Instant.now();
            timeElapsed = Duration.between(start, end);
            if(!this.dg.ws.gtm && timeElapsed.toMillis() < 1000)
                try {
                    long i =  (long) ((1000-timeElapsed.toMillis()));
                    TimeUnit.MILLISECONDS.sleep(i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
        System.out.println(this.taskName + " is completed!");
        this.result = true;
        if (this.retryTimes > 0 && this.failedMutations.size() > 0) {
            logger.info(this.retryTimes);
            logger.info(this.failedMutations.size());
            this.retryTimes -= 1;
            if (this.sdkClientPool != null)
                this.sdk = this.sdkClientPool.get_client_for_bucket(this.bucket_name, this.scope, this.collection);
            for (Entry<String, List<Result>> optype: this.failedMutations.entrySet()) {
                for (Result r: optype.getValue()) {
                    logger.info("Loader Retrying: " + r.id() + " -> " + r.err().getClass().getSimpleName());
                    switch(optype.getKey()) {
                    case "create":
                        try {
                            docops.insert(r.id(), r.document(), this.sdk.connection, setOptions);
                            this.failedMutations.get(optype.getKey()).remove(r);
                        } catch (TimeoutException|ServerOutOfMemoryException e) {
                            logger.fatal("Retry Create failed for key: " + r.id());
                            this.result = false;
                        } catch (DocumentExistsException e) {
                            logger.fatal("Retry Create failed for key: " + r.id());
                        }
                    case "update":
                        try {
                            docops.upsert(r.id(), r.document(), this.sdk.connection, upsertOptions);
                            this.failedMutations.get(optype.getKey()).remove(r);
                        } catch (TimeoutException|ServerOutOfMemoryException e) {
                            logger.fatal("Retry update failed for key: " + r.id());
                            this.result = false;
                        }  catch (DocumentExistsException e) {
                            logger.fatal("Retry update failed for key: " + r.id());
                        }
                    case "delete":
                        try {
                            docops.delete(r.id(), this.sdk.connection, removeOptions);
                            this.failedMutations.get(optype.getKey()).remove(r);
                        } catch (TimeoutException|ServerOutOfMemoryException e) {
                            logger.fatal("Retry delete failed for key: " + r.id());
                            this.result = false;
                        } catch (DocumentNotFoundException e) {
                            logger.fatal("Retry delete failed for key: " + r.id());
                        }
                    }
                }
                if(this.sdkClientPool != null)
                    this.sdkClientPool.release_client(this.sdk);
            }
        }
    }
}
