package com.couchbase.test.bulk_doc_operations;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonDocument;

import rx.Observable;
import rx.functions.Func1;

public class doc_ops {

	/**
	 * @param bucket Bucket Object to be used to upsert documents
	 * @param documents List of JsonDocument documents to be upserted to the bucket
	 * @param replicateTo Replication constraint to watch
	 * @param persistTo Persistence constraint to watch
	 * @param timeOut Custom timeout
	 * @param timeUnit Unit for timeout
	 * @return
	 */
	public List<HashMap<String, Object>> bulkUpsert(final Bucket bucket, List<JsonDocument> documents, int persistTo, int replicateTo, long timeOut, String timeUnit){
		PersistTo persist = this.getPersistTo(persistTo);
		ReplicateTo replicate = this.getReplicateTo(replicateTo);
		TimeUnit time = this.getTimeUnit(timeUnit);
		List<HashMap<String, Object>> returnValues = Observable
				.from(documents)
				.flatMap(new Func1<JsonDocument, Observable<HashMap<String, Object>>>() {

					public Observable<HashMap<String, Object>> call(final JsonDocument docToInsert) {
						return bucket.async().upsert(docToInsert, persist, replicate, timeOut, time).flatMap(new Func1<JsonDocument, Observable<HashMap<String,Object>>>() {
							public Observable<HashMap<String, Object>> call(JsonDocument insertedDocument) {
								HashMap<String, Object> returnValue = new HashMap<String, Object>();
								returnValue.put("Document", insertedDocument);
								returnValue.put("Error", null);
								returnValue.put("Status", true);
								return Observable.just(returnValue);
							}
						})
						.onErrorReturn(new Func1<Throwable, HashMap<String,Object>>() {
							public HashMap<String, Object> call(Throwable error) {
								HashMap<String, Object> returnValue = new HashMap<String, Object>();
								returnValue.put("Document", docToInsert);
								returnValue.put("Error", error);
								returnValue.put("Status", false);
								return returnValue;
							}
						});
					}
				}).toList().toBlocking().single();
		return returnValues;
	}

	/**
	 * @param bucket Bucket Object to be used to insert documents
	 * @param documents List of JsonDocument documents to be inserted to the bucket
	 * @param replicateTo Replication constraint to watch
	 * @param persistTo Persistence constraint to watch
	 * @param timeOut Custom timeout
	 * @param timeUnit Unit for timeout
	 * @return
	 */
	public List<HashMap<String, Object>> bulkSet(final Bucket bucket, List<JsonDocument> documents, int persistTo, int replicateTo, long timeOut, String timeUnit){
		PersistTo persist = this.getPersistTo(persistTo);
		ReplicateTo replicate = this.getReplicateTo(replicateTo);
		TimeUnit time = this.getTimeUnit(timeUnit);
		List<HashMap<String, Object>> returnValues = Observable
		.from(documents)
		.flatMap(new Func1<JsonDocument, Observable<HashMap<String, Object>>>() {
			public Observable<HashMap<String, Object>> call(final JsonDocument docToInsert) {
				return bucket.async().insert(docToInsert, persist, replicate, timeOut, time).flatMap(new Func1<JsonDocument, Observable<HashMap<String,Object>>>() {
					public Observable<HashMap<String, Object>> call(JsonDocument insertedDocument) {
						HashMap<String, Object> returnValue = new HashMap<String, Object>();
						returnValue.put("Document", insertedDocument);
						returnValue.put("Error", null);
						returnValue.put("Status", true);
						return Observable.just(returnValue);
					}
				})
				.onErrorReturn(new Func1<Throwable, HashMap<String,Object>>() {
					public HashMap<String, Object> call(Throwable error) {
						HashMap<String, Object> returnValue = new HashMap<String, Object>();
						returnValue.put("Document", docToInsert);
						returnValue.put("Error", error);
						returnValue.put("Status", false);
						return returnValue;
					}
				});
			}
		}).toList().toBlocking().single();
		return returnValues;
	}

	public List<JsonDocument> bulkGet(final Bucket bucket, final List<String> ids) {
	    return Observable
	        .from(ids)
	        .flatMap(new Func1<String, Observable<JsonDocument>>() {
	            public Observable<JsonDocument> call(String id) {
	                return bucket.async().get(id);
	            }
	        })
	        .toList()
	        .toBlocking()
	        .single();
	}

	private PersistTo getPersistTo(int persistTo) {
		switch (persistTo) {
		case 0:
			return PersistTo.NONE;
		case 1:
			return PersistTo.ONE;
		case 2:
			return PersistTo.TWO;
		case 3:
			return PersistTo.THREE;
		case 4:
			return PersistTo.FOUR;
		default:
			return PersistTo.MASTER;

		}
	}

	private ReplicateTo getReplicateTo(int replicateTo) {
		switch (replicateTo) {
		case 0:
			return ReplicateTo.NONE;
		case 1:
			return ReplicateTo.ONE;
		case 2:
			return ReplicateTo.TWO;
		case 3:
			return ReplicateTo.THREE;
		default:
			return ReplicateTo.NONE;
		}
	}

	private TimeUnit getTimeUnit(String timeUnit) {
		if (timeUnit.equalsIgnoreCase("seconds")) {
			return TimeUnit.SECONDS;
		}
		else if (timeUnit.equalsIgnoreCase("milliseconds")) {
			return TimeUnit.MILLISECONDS;
		}
		else if (timeUnit.equalsIgnoreCase("minutes")) {
			return TimeUnit.MINUTES;
		}
		else {
			return TimeUnit.SECONDS;
		}
	}

}
