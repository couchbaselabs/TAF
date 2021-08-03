package com.couchbase.test.docgen;

import reactor.util.function.Tuple2;

public class HotLoadGenerator extends KVGenerator {

    public HotLoadGenerator(WorkLoadSettings ws, String keyClass, String valClass) throws ClassNotFoundException {
        super(ws, keyClass, valClass);
        // TODO Auto-generated constructor stub
    }

    @Override
    Tuple2<String, Object> next() {
        // TODO Auto-generated method stub
        return null;
    }

}
