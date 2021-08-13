package com.couchbase.test.docgen;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.test.docgen.DocRange;

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;


public class WorkLoadSettings extends WorkLoadBase {
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

    public PersistTo persist_to;
    public ReplicateTo replicate_to;

    public String loadType;
    public String keyType;
    public String valueType;
    public boolean gtm;
    public boolean expectDeleted;
    public boolean validate;

    public DocRange dr;

    public WorkLoadSettings(){}

    public WorkLoadSettings(String keyPrefix,
            int keySize, int docSize, int c, int r, int u, int d,
            int workers, int ops, int items, String loadType,
            String keyType, String valueType,
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
