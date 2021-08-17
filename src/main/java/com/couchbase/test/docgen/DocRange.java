package com.couchbase.test.docgen;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import couchbase.test.docgen.DRConstants;

public class DocRange {
    public int create_s = 0;
    public int create_e = 0;

    public int read_s = 0;
    public int read_e = 0;

    public int update_s = 0;
    public int update_e = 0;

    public int delete_s = 0;
    public int delete_e = 0;

    public int expiry_s = 0;
    public int expiry_e = 0;

    public int touch_s = 0;
    public int touch_e = 0;

    public int replace_s = 0;
    public int replace_e = 0;

    AtomicInteger createItr;
    AtomicInteger readItr;
    AtomicInteger updateItr;
    AtomicInteger deleteItr;
    AtomicInteger expiryItr;
    AtomicInteger touchItr;
    AtomicInteger replaceItr;

    public DocRange(HashMap<String, Integer> ranges) {
        super();

        this.create_s = ranges.getOrDefault(DRConstants.create_s, 0);
        this.read_s = ranges.getOrDefault(DRConstants.read_s, 0);
        this.update_s = ranges.getOrDefault(DRConstants.update_s, 0);
        this.delete_s = ranges.getOrDefault(DRConstants.delete_s, 0);
        this.expiry_s = ranges.getOrDefault(DRConstants.expiry_s, 0);
        this.touch_s = ranges.getOrDefault(DRConstants.touch_s, 0);
        this.replace_s = ranges.getOrDefault(DRConstants.replace_s, 0);

        this.create_e = ranges.getOrDefault(DRConstants.create_e, 0);
        this.read_e = ranges.getOrDefault(DRConstants.read_e, 0);
        this.update_e = ranges.getOrDefault(DRConstants.update_e, 0);
        this.delete_e = ranges.getOrDefault(DRConstants.delete_e, 0);
        this.expiry_e = ranges.getOrDefault(DRConstants.expiry_e, 0);
        this.touch_e = ranges.getOrDefault(DRConstants.touch_e, 0);
        this.replace_e = ranges.getOrDefault(DRConstants.replace_e, 0);

        this.createItr = new AtomicInteger(this.create_s);
        this.readItr = new AtomicInteger(this.read_s);
        this.updateItr = new AtomicInteger(this.update_s);
        this.deleteItr = new AtomicInteger(this.delete_s);
        this.expiryItr = new AtomicInteger(this.expiry_s);
        this.touchItr = new AtomicInteger(this.touch_s);
        this.replaceItr = new AtomicInteger(this.replace_s);
    }

    public void resetAllItrs() {
        createItr = new AtomicInteger(create_s);
        readItr = new AtomicInteger(read_s);
        updateItr = new AtomicInteger(update_s);
        deleteItr = new AtomicInteger(delete_s);
        expiryItr = new AtomicInteger(expiry_s);
        touchItr = new AtomicInteger(touch_s);
        replaceItr = new AtomicInteger(replace_s);
    }
}
