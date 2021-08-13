package com.couchbase.test.docgen;

import java.lang.Math;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import reactor.util.function.Tuple2;

import com.couchbase.client.core.msg.kv.DurabilityLevel;

public class TransactionalWorkLoadSettings extends WorkLoadBase {
    int start = 0;
    int end = 1000;
    public int batchSize = 1;
    public List<List<?>> load_pattern;

    public AtomicInteger itr = new AtomicInteger(start);
    AtomicInteger readItr = new AtomicInteger(0);
    AtomicInteger upsertItr = new AtomicInteger(0);
    AtomicInteger delItr = new AtomicInteger(0);

    int items = 0;
    public int workers = 10;
    Boolean randomize_keys = false;
    Boolean transaction_on_same_keys = false;
    public Boolean commit;
    public Boolean rollback;

    public int creates = 0;
    public int reads = 0;
    public int updates = 0;
    public int deletes = 0;
    public int workingSet = 100;

    public TransactionDocGenerator doc_gen;

    private void create_transaction_load_pattern_per_worker(List<?> transaction_patterns) {
        int total_docs_to_be_mutated = (this.end - this.start);
        float max_iterations = (float)(total_docs_to_be_mutated/this.batchSize) / this.workers;
        int iterations_to_run = (int)Math.floor(max_iterations);
        int batch_with_extra_loops = ((int)Math.ceil(max_iterations*this.workers-this.workers)) % this.workers;
        List<Object> pattern_1 = (List<Object>)transaction_patterns.get(0);
        List<String> crud_pattern = (List<String>)pattern_1.get(2);
        int t_pattern_len = crud_pattern.size();

        int pattern_index = 0;
        this.load_pattern = new ArrayList<List<?>>();
        for(int index=0; index < this.workers; index++) {
            List t_pattern = new ArrayList<Object>();
            List load_pattern = new ArrayList<Object>();
            t_pattern.add(this.batchSize);
            if(index < batch_with_extra_loops)
                t_pattern.add(iterations_to_run+1);
            else
                t_pattern.add(iterations_to_run);

            load_pattern.add(pattern_1.get(0));
            load_pattern.add(pattern_1.get(1));
            load_pattern.add(crud_pattern.get(pattern_index));
            t_pattern.add(load_pattern);
            this.load_pattern.add(t_pattern);

            pattern_index++;
            if(pattern_index == t_pattern_len)
                pattern_index = 0;
        }
    }

    public TransactionalWorkLoadSettings(
            String keyPrefix, int start, int end, int batchSize,
            int min_key_size, int max_key_size, Boolean randomize_keys,
            int workers, int items, Boolean transaction_on_same_keys,
            List<?> transaction_patterns,
            Boolean commit, Boolean rollback) throws ClassNotFoundException {
         super();
         this.keyPrefix = keyPrefix;
         this.start = start;
         this.end = end;
         this.batchSize = batchSize;

         this.itr = new AtomicInteger(start);
         this.keySize = min_key_size;
         this.workers = workers;
         this.randomize_keys = randomize_keys;
         this.min_key_size = min_key_size;

         this.items = items;
         this.transaction_on_same_keys = transaction_on_same_keys;

         this.commit = commit;
         this.rollback = rollback;

        // Create document generator for the work_load
        this.doc_gen = new TransactionDocGenerator(this, "", "");

        this.create_transaction_load_pattern_per_worker(transaction_patterns);
    }
}
