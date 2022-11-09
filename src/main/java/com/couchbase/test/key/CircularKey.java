package com.couchbase.test.key;

import com.couchbase.test.docgen.WorkLoadSettings;

public class CircularKey extends SimpleKey {
    public CircularKey() {
        super();
    }

    public CircularKey(WorkLoadSettings ws) {
        super(ws);
    }

    public String next(long docIndex) {
        return super.next(docIndex);
    }
}
