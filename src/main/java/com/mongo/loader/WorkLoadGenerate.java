package com.mongo.loader;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;

import com.couchbase.test.taskmanager.Task;

public class WorkLoadGenerate extends Task{
	DocumentGenerator dg;
	public MongoSDKClient sdk;
	public int exp;
	public String exp_unit;
	static Logger logger = LogManager.getLogger(WorkLoadGenerate.class);

	public WorkLoadGenerate(String taskName, DocumentGenerator dg, MongoSDKClient client) {
		super(taskName);
		this.dg = dg;
		this.sdk = client;
	}
	
    public BulkWriteResult bulkInsert(MongoCollection<Document> collection, List<InsertOneModel<Document>> documents) {
    	return collection.bulkWrite(documents);
      }

	@Override
	public void run() {
		System.out.println("Starting " + this.taskName);
		logger.info("Starting " + this.taskName);
		// Set timeout in WorkLoadSettings
		this.dg.ws.setTimeoutDuration(1, "seconds");
		int ops = 0;
		boolean flag = false;
		Instant trackFailureTime_start = Instant.now();
		while(true) {
			Instant trackFailureTime_end = Instant.now();
			Duration timeElapsed = Duration.between(trackFailureTime_start, trackFailureTime_end);
			Instant start = Instant.now();
			if(dg.ws.creates > 0) {
				List<InsertOneModel<Document>> docs = dg.nextInsertBatch();
				if (docs.size()>0) {
					flag = true;
					this.bulkInsert(this.sdk.collection, docs);
					ops += dg.ws.batchSize*dg.ws.creates/100;
				}
			}
			if(ops == 0)
				break;
			else if(ops < dg.ws.ops/dg.ws.workers && flag) {
				flag = false;
				continue;
			}
			ops = 0;
			Instant end = Instant.now();
			timeElapsed = Duration.between(start, end);
			if(!this.dg.ws.gtm && timeElapsed.toMillis() < 1000)
				try {
					long i =  (long) ((1000-timeElapsed.toMillis()));
					TimeUnit.MILLISECONDS.sleep(i);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
		logger.info(this.taskName + " is completed!");
		this.result = true;
	}
}
