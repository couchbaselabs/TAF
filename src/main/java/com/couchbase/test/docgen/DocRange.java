package com.couchbase.test.docgen;

import java.util.concurrent.atomic.AtomicInteger;

public class DocRange {
    public static int create_s = 0;
    public static int create_e = 0;

    public static int read_s = 0;
    public static int read_e = 0;

    public static int update_s = 0;
    public static int update_e = 0;

    public static int delete_s = 0;
    public static int delete_e = 0;

    public static int expiry_s = 0;
    public static int expiry_e = 0;

    public static int touch_s = 0;
    public static int touch_e = 0;

    public static int replace_s = 0;
    public static int replace_e = 0;

    AtomicInteger createItr;
    AtomicInteger readItr;
    AtomicInteger updateItr;
    AtomicInteger deleteItr;
    AtomicInteger expiryItr;
    AtomicInteger touchItr;
    AtomicInteger replaceItr;

    public DocRange() {
        super();
        createItr = new AtomicInteger(create_s);
        readItr = new AtomicInteger(read_s);
        updateItr = new AtomicInteger(update_s);
        deleteItr = new AtomicInteger(delete_s);
        expiryItr = new AtomicInteger(expiry_s);
        touchItr = new AtomicInteger(touch_s);
        replaceItr = new AtomicInteger(replace_s);
        System.out.println(DocRange.delete_e);
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
