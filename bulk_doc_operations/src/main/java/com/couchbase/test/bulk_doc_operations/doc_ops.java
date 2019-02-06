package com.couchbase.test.bulk_doc_operations;

import java.util.HashMap;
import java.util.List;

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;

import rx.Observable;
import rx.functions.Func1;

public class doc_ops {
	
	public List<HashMap<String, Object>> bulkUpsert(final Bucket bucket, List<JsonDocument> documents){
		
		List<HashMap<String, Object>> returnValues = Observable
				.from(documents)
				.flatMap(new Func1<JsonDocument, Observable<HashMap<String, Object>>>() {

					public Observable<HashMap<String, Object>> call(final JsonDocument docToInsert) {
						return bucket.async().upsert(docToInsert).flatMap(new Func1<JsonDocument, Observable<HashMap<String,Object>>>() {
							

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
	
	public List<HashMap<String, Object>> bulkSet(final Bucket bucket, List<JsonDocument> documents){
		
		List<HashMap<String, Object>> returnValues = Observable
		.from(documents)
		.flatMap(new Func1<JsonDocument, Observable<HashMap<String, Object>>>() {

			public Observable<HashMap<String, Object>> call(final JsonDocument docToInsert) {
				return bucket.async().insert(docToInsert).flatMap(new Func1<JsonDocument, Observable<HashMap<String,Object>>>() {
					

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

}
