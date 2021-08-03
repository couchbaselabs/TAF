package com.couchbase.test.loadgen;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.UpsertOptions;

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
    public void run() {
        UpsertOptions upsertOptions = UpsertOptions.upsertOptions().timeout(Duration.ofSeconds(10)).durability(DurabilityLevel.NONE);
        InsertOptions setOptions = InsertOptions.insertOptions().timeout(Duration.ofSeconds(10)).durability(DurabilityLevel.NONE);
        GetOptions getOptions = GetOptions.getOptions().timeout(Duration.ofSeconds(10));
        int ops = 0;
        while(dg.has_next()) {
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
            if(dg.ws.reads > 0) {
                List<Tuple2<String, Object>> docs = dg.nextUpdateBatch();
                if (docs.size()>0) {
                    docops.bulkGets(this.sdk.connection, docs, getOptions);
                    ops += dg.ws.batchSize*dg.ws.reads/100; 
                }
            }

            if(ops < dg.ws.ops/dg.ws.workers) {
                continue;
            }
            ops = 0;
            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            if(timeElapsed.toMillis() < 1000)
                try {
                    long i =  (long) ((1000-timeElapsed.toMillis()));
                    TimeUnit.MILLISECONDS.sleep(i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
        this.sdk.disconnectCluster();
    }
}