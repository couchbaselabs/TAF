package com.couchbase.test.docgen;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.test.docgen.DocRange;

public class WorkLoadSettings {

    public String keyPrefix = "test_docs-";
    public int workers = 10;
    public int ops = 40000;
    public int batchSize = ops/workers;
    public int keySize = 15;
    public int docSize = 256;

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

    public DocRange dr;

    public WorkLoadSettings(){}

    public WorkLoadSettings(String keyPrefix,
            int keySize, int docSize, int c, int r, int u,
            int d, int workers, int ops, int items, String loadType, String keyType, String valueType,
            boolean validate, boolean gtm, boolean deleted) {
        super();
        this.keyPrefix = keyPrefix;
        this.keySize = keySize;
        this.docSize = docSize;
        this.creates = c;
        this.reads = r;
        this.updates = u;
        this.deletes = d;
        this.workers = workers;
        this.ops = ops;

        this.batchSize = this.ops/this.workers;
        this.gtm = gtm;
        this.expectDeleted = deleted;
        this.validate = validate;
    };

}