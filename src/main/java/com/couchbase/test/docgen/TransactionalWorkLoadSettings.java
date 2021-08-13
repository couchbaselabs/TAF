package com.couchbase.test.docgen;

import java.util.List;

import java.util.concurrent.atomic.AtomicInteger;
import com.couchbase.client.core.msg.kv.DurabilityLevel;

public class TransactionalWorkLoadSettings extends WorkLoadBase {

	public String keyPrefix = "test_transaction-";
    int start = 0;
    int end = 1000;

    public AtomicInteger itr = new AtomicInteger(start);
    AtomicInteger readItr = new AtomicInteger(0);
    AtomicInteger upsertItr = new AtomicInteger(0);
    AtomicInteger delItr = new AtomicInteger(0);

    int items = 0;
    public int workers = 10;
    public int batchSize = 1;
    public int keySize = 15;
    public int docSize = 256;
	int min_key_size = 10;
	int max_key_size = min_key_size;
	Boolean randomize_keys = false;
	Boolean transaction_on_same_keys = false;
	List<?> trans_pattern;

    public int creates = 0;
    public int reads = 0;
    public int updates = 0;
    public int deletes = 0;
    public int workingSet = 100;

    public TransactionDocGenerator doc_gen;

	public TransactionalWorkLoadSettings(
	        String keyPrefix, int start, int end, int batchSize,
	        int min_key_size, int max_key_size, Boolean randomize_keys,
			int workers, int items, Boolean transaction_on_same_keys,
			List<?> transaction_pattern) throws ClassNotFoundException {
		super();
		this.keyPrefix = keyPrefix;
		this.start = start;
		this.end = end;
		this.batchSize = batchSize;
		this.itr = new AtomicInteger(start);
		this.keySize = keySize;
		this.workers = workers;
		this.randomize_keys = randomize_keys;
		this.min_key_size = min_key_size;
		this.trans_pattern = transaction_pattern;

		this.batchSize = 1;
		this.items = items;
		this.transaction_on_same_keys = transaction_on_same_keys;

        // Create document generator for loading phase
		this.doc_gen = new TransactionDocGenerator(this, null, null);
	};
}
