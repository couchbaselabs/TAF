package com.couchbase.test.key;

import com.couchbase.test.docgen.WorkLoadSettings;

public class SimpleKey {
    public WorkLoadSettings ws;
    String padding = "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            + "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
    String alphabet = "";

    public SimpleKey() {
        super();
    }

    public SimpleKey(WorkLoadSettings ws) {
        super();
        this.ws = ws;
    }

    public String next(long doc_index) {
        int counterSize = Long.toString(Math.abs(doc_index)).length();
        int padd = this.ws.keySize - this.ws.keyPrefix.length() - counterSize;
        return this.ws.keyPrefix + this.padding.substring(0, padd) + Math.abs(doc_index);
    }
}
