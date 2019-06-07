

package com.couchbase.test.transactions;

import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.log.LogDefer;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.sql.Time;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.transactions.AttemptContext;
import com.couchbase.transactions.TransactionDurabilityLevel;
import com.couchbase.transactions.TransactionJsonDocument;
import com.couchbase.transactions.Transactions;

public class SimpleTransaction {


	public Transactions createTansaction(Cluster cluster, TransactionConfig config) {
		return Transactions.create(cluster, config);
	}
	
	public TransactionConfig createTransactionConfig(int expiryTimeout, int durabilitylevel) {
		TransactionConfigBuilder config = TransactionConfigBuilder.create();
		switch (durabilitylevel) {
        case 0:
            config.durabilityLevel(TransactionDurabilityLevel.NONE);
            break;
        case 1:
            config.durabilityLevel(TransactionDurabilityLevel.MAJORITY);
            break;
        case 2:
            config.durabilityLevel(TransactionDurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER);
            break;
        case 3:
            config.durabilityLevel(TransactionDurabilityLevel.PERSIST_TO_MAJORITY);
            break;
        default:
        	config.durabilityLevel(TransactionDurabilityLevel.NONE);
    }
		return config.expirationTime(Duration.of(expiryTimeout, ChronoUnit.SECONDS)).build();
	}
	
	public ArrayList<LogDefer> RunTransaction(Transactions transaction, List<Collection> collections, List<Tuple2<String, Object>> Createkeys, List<String> Updatekeys, 
			List<String> Deletekeys, Boolean commit, boolean sync) {
		ArrayList<LogDefer> res = null;
//		synchronous API - transactions
		if (sync) {
			try {
				transaction.run(ctx -> {
	//				creation of docs
					for (Collection bucket:collections) {
						for (Tuple2<String, Object> document : Createkeys) {
							TransactionJsonDocument doc=ctx.insert(bucket, document.getT1(), document.getT2()); 
//							TransactionJsonDocument doc1=ctx.get(bucket, document.getT1()).get();
//							if (doc1 != doc) { System.out.println("Document not matched");	}
						}

					}
	//				update of docs
					for (String key: Updatekeys) {
						for (Collection bucket:collections) {
							if (ctx.get(bucket, key).isPresent()) {
								TransactionJsonDocument doc2=ctx.get(bucket, key).get();
								JsonObject content = doc2.contentAs(JsonObject.class);
								content.put("mutated", 1 );	
								ctx.replace(doc2, content); }
						}
					}
	//			   delete the docs
					for (String key: Deletekeys) {
						for (Collection bucket:collections) {
							if (ctx.get(bucket, key).isPresent()) {
								TransactionJsonDocument doc1=ctx.get(bucket, key).get();
								ctx.remove(doc1);
							}
						}
					}
	//				commit ot rollback the docs
					if (commit) {  ctx.commit(); }
					else { ctx.rollback(); 	 }
					
					transaction.close();
					
				}); }
			catch (TransactionFailed err) {
	            // This per-txn log allows the app to only log failures
				System.out.println("Transaction failed from runTransaction");
				err.result().log().logs().forEach(System.err::println);
	            res = err.result().log().logs();
	        }
			
		}
//		Asynchronous transactions API 
		else {
			System.out.println("IN ELSE PART");

		}
		return res;
		}
	
	public void nonTxnRemoves(Collection collection) {
	    String id = "collection_0";
	    JsonObject initial = JsonObject.create().put("val", 1);
	 
	    for (int i = 0; i < 100000; i ++) {
	        collection.insert(id, initial, InsertOptions.insertOptions().durabilityLevel(DurabilityLevel.MAJORITY));
	        collection.remove(id, RemoveOptions.removeOptions().durabilityLevel(DurabilityLevel.MAJORITY));
	    }
	}
	
}


