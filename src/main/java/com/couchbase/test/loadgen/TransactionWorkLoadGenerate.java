package com.couchbase.test.loadgen;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.test.docgen.DocType.Person;
import com.couchbase.test.docgen.DocumentGenerator;
import com.couchbase.test.sdk.SDKClient;
import com.couchbase.test.taskmanager.Task;

import com.couchbase.test.docgen.TransactionDocGenerator;

import reactor.util.function.Tuple2;

public class TransactionWorkLoadGenerate extends Task{
    TransactionDocGenerator dg;
    SDKClient sdk;

    public TransactionWorkLoadGenerate(String taskName, TransactionDocGenerator dg, SDKClient client) {
        super(taskName);
        this.dg = dg;
        this.sdk = client;
    }

    @Override
    public void run() {
    }
}
