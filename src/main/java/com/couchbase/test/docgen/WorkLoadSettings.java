package com.couchbase.test.docgen;

import java.util.concurrent.atomic.AtomicInteger;
import com.couchbase.client.core.msg.kv.DurabilityLevel;

public class WorkLoadSettings {

    public String keyPrefix = "test_docs-";
    int start = 0;
    int end = 1000000;

    public AtomicInteger Itr = new AtomicInteger(start);
    AtomicInteger readItr = new AtomicInteger(0);
    AtomicInteger upsertItr = new AtomicInteger(0);
    AtomicInteger delItr = new AtomicInteger(0);

    public int workers = 10;
    public int ops = 40000;
    public int batchSize = ops/workers;
    public int keySize = 15;
    public int docSize = 256;
    int items = 0;

    public int creates = 0;
    public int reads = 0;
    public int updates = 0;
    public int deletes = 0;
    public int workingSet = 100;

    int persist_to = 0;
    int replicate_to = 0;
    DurabilityLevel d = DurabilityLevel.NONE;

    public String loadType;
    public String keyType;
    public String valueType;
    public boolean gtm;
    public boolean expectDeleted;
    public boolean validate;

    public WorkLoadSettings(){}

    public WorkLoadSettings(String keyPrefix, int itr, int start, int end,
            int keySize, int docSize, int c, int r, int u,
            int d, int workers, int ops, int items, String loadType, String keyType, String valueType,
            boolean validate, boolean gtm, boolean deleted) {
        super();
        this.keyPrefix = keyPrefix;
        this.start = start;
        this.end = end;
        this.Itr = new AtomicInteger(start);
        this.keySize = keySize;
        this.docSize = docSize;
        this.creates = c;
        this.reads = r;
        this.updates = u;
        this.deletes = d;
        this.workers = workers;
        this.ops = ops;

        this.batchSize = this.ops/this.workers;
        this.items = items;
        this.gtm = gtm;
        this.expectDeleted = deleted;
        this.validate = validate;
    };

}