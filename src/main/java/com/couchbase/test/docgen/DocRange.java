package com.couchbase.test.docgen;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import couchbase.test.docgen.DRConstants;

public class DocRange {
    public long create_s = 0;
    public long create_e = 0;

    public long read_s = 0;
    public long read_e = 0;

    public long update_s = 0;
    public long update_e = 0;

    public long delete_s = 0;
    public long delete_e = 0;

    public long expiry_s = 0;
    public long expiry_e = 0;

    public long touch_s = 0;
    public long touch_e = 0;

    public long replace_s = 0;
    public long replace_e = 0;

    AtomicLong createItr;
    AtomicLong readItr;
    AtomicLong updateItr;
    AtomicLong deleteItr;
    AtomicLong expiryItr;
    AtomicLong touchItr;
    AtomicLong replaceItr;

    public DocRange(HashMap<String, Number> ranges) {
        super();

        this.create_s = ranges.getOrDefault(DRConstants.create_s, 0).longValue();
        this.read_s = ranges.getOrDefault(DRConstants.read_s, 0).longValue();
        this.update_s = ranges.getOrDefault(DRConstants.update_s, 0).longValue();
        this.delete_s = ranges.getOrDefault(DRConstants.delete_s, 0).longValue();
        this.expiry_s = ranges.getOrDefault(DRConstants.expiry_s, 0).longValue();
        this.touch_s = ranges.getOrDefault(DRConstants.touch_s, 0).longValue();
        this.replace_s = ranges.getOrDefault(DRConstants.replace_s, 0).longValue();

        this.create_e = ranges.getOrDefault(DRConstants.create_e, 0).longValue();
        this.read_e = ranges.getOrDefault(DRConstants.read_e, 0).longValue();
        this.update_e = ranges.getOrDefault(DRConstants.update_e, 0).longValue();
        this.delete_e = ranges.getOrDefault(DRConstants.delete_e, 0).longValue();
        this.expiry_e = ranges.getOrDefault(DRConstants.expiry_e, 0).longValue();
        this.touch_e = ranges.getOrDefault(DRConstants.touch_e, 0).longValue();
        this.replace_e = ranges.getOrDefault(DRConstants.replace_e, 0).longValue();

        this.createItr = new AtomicLong(this.create_s);
        this.readItr = new AtomicLong(this.read_s);
        this.updateItr = new AtomicLong(this.update_s);
        this.deleteItr = new AtomicLong(this.delete_s);
        this.expiryItr = new AtomicLong(this.expiry_s);
        this.touchItr = new AtomicLong(this.touch_s);
        this.replaceItr = new AtomicLong(this.replace_s);
    }

    public void resetAllItrs() {
        createItr = new AtomicLong(create_s);
        readItr = new AtomicLong(read_s);
        updateItr = new AtomicLong(update_s);
        deleteItr = new AtomicLong(delete_s);
        expiryItr = new AtomicLong(expiry_s);
        touchItr = new AtomicLong(touch_s);
        replaceItr = new AtomicLong(replace_s);
    }
}
