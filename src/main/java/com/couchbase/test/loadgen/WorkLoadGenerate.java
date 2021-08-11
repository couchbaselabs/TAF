package com.couchbase.test.loadgen;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.test.docgen.DocType.Person;
import com.couchbase.test.docgen.DocumentGenerator;
import com.couchbase.test.sdk.DocOps;
import com.couchbase.test.sdk.SDKClient;
import com.couchbase.test.taskmanager.Task;

import reactor.util.function.Tuple2;

public class WorkLoadGenerate extends Task{
    DocumentGenerator dg;
    SDKClient sdk;
    DocOps docops;

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClient client) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.sdk = client;
    }

    @Override
    public void run() throws RuntimeException {
        UpsertOptions upsertOptions = UpsertOptions.upsertOptions().timeout(Duration.ofSeconds(10)).durability(DurabilityLevel.NONE);
        InsertOptions setOptions = InsertOptions.insertOptions().timeout(Duration.ofSeconds(10)).durability(DurabilityLevel.NONE);
        RemoveOptions removeOptions = RemoveOptions.removeOptions().timeout(Duration.ofSeconds(10)).durability(DurabilityLevel.NONE);
        GetOptions getOptions = GetOptions.getOptions().timeout(Duration.ofSeconds(10));
        int ops = 0;
        while(true) {
            Instant start = Instant.now();
            if(dg.ws.creates > 0) {
                List<Tuple2<String, Object>> docs = dg.nextInsertBatch();
                if (docs.size()>0) {
                    docops.bulkInsert(this.sdk.connection, docs, setOptions);
                    ops += dg.ws.batchSize*dg.ws.creates/100;
                }
            }
            if(dg.ws.updates > 0) {
                List<Tuple2<String, Object>> docs = dg.nextUpdateBatch();
                if (docs.size()>0) {
                    docops.bulkUpsert(this.sdk.connection, docs, upsertOptions);
                    ops += dg.ws.batchSize*dg.ws.updates/100;
                }
            }
            if(dg.ws.deletes > 0) {
                List<String> docs = dg.nextDeleteBatch();
                if (docs.size()>0) {
                    docops.bulkDelete(this.sdk.connection, docs, removeOptions);
                    ops += dg.ws.batchSize*dg.ws.updates/100;
                }
            }
            if(dg.ws.reads > 0) {
                List<Tuple2<String, Object>> docs = dg.nextReadBatch();
                if (docs.size()>0) {
                    List<Tuple2<String, Object>> res = docops.bulkGets(this.sdk.connection, docs, getOptions);
                    if (this.dg.ws.validate) {
                        Map<Object, Object> trnx_res = res.stream().collect(Collectors.toMap(t -> t.get(0), t -> t.get(1)));
                        Map<Object, Object> trnx_docs = docs.stream().collect(Collectors.toMap(t -> t.get(0), t -> t.get(1)));
                        ObjectMapper om = new ObjectMapper();
                        for (Object name : trnx_docs.keySet()) {
                            try {
                                String a = om.writeValueAsString(trnx_res.get(name));
                                String b = om.writeValueAsString(trnx_docs.get(name));
                                if(this.dg.ws.expectDeleted) {
                                    if(!a.contains(DocumentNotFoundException.class.getSimpleName())) {
                                        this.sdk.disconnectCluster();
                                        return;
                                    }
                                } else if(!a.equals(b)){
                                    System.out.println("Validation failed for key: " + name);
                                    System.out.println("Actual Value - " + a);
                                    System.out.println("Expected Value - " + b);
                                    this.sdk.disconnectCluster();
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
            if(ops == 0)
                break;
            else if(ops < dg.ws.ops/dg.ws.workers) {
                continue;
            }
            ops = 0;
            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            if(!this.dg.ws.gtm && timeElapsed.toMillis() < 1000)
                try {
                    long i =  (long) ((1000-timeElapsed.toMillis()));
                    TimeUnit.MILLISECONDS.sleep(i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
        this.result = true;
        this.sdk.disconnectCluster();
    }
}