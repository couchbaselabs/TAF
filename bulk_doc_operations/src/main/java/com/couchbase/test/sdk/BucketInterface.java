package com.couchbase.test.sdk;

import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;

public class BucketInterface {
	private String bucketName;
	private Bucket bucket;
	private Cluster cluster;
	
	public BucketInterface(Cluster cluster, String bucketName) {
		this.cluster = cluster;
		this.bucketName = bucketName;
	}
	
	public void connect() {
		this.bucket = this.cluster.openBucket(bucketName);
	}
	
	public boolean close() {
		return this.bucket.close();
	}
	
	public boolean closeWithTimeout(long timeout, TimeUnit timeUnit) {
		return this.bucket.close(timeout, timeUnit);
	}
	
	public void reconnect() {
		this.close();
		this.connect();
	}
	
	public Bucket getBucketObj() {
		return this.bucket;
	}
	
	public CouchbaseEnvironment environment() {
		return this.bucket.environment();
	}
	
	/*
	// Append methods
	public Document<D> append(Document<D> doc) {
		return this.bucket.append(doc);
	}

	public Document<D> appendWithTimeouts(Document<D> doc, long timeout, TimeUnit timeUnit) {
		return this.bucket.append(doc, timeout, timeUnit);
	}

	public Document<D> appendWithPersistTo(Document<D> doc, PersistTo persistTo) {
		return this.bucket.append(doc, persistTo);
	}

	public Document<D> appendWithPersistToAndTimeout(Document<D> doc, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.append(doc, persistTo, timeout, timeUnit);
	}

	public Document<D> appendWithPersistToReplicateTo(Document<D> doc, PersistTo persistTo, ReplicateTo replicateTo) {
		return this.bucket.append(doc, persistTo, replicateTo);
	}

	public Document<D> appendWithPersistToReplicateToAndTimeout(Document<D> doc, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.append(doc, persistTo, replicateTo, timeout, timeUnit);
	}

	public Document<D> appendWithReplicateTo(Document<D> doc, ReplicateTo replicateTo) {
		return this.bucket.append(doc, replicateTo);
	}

	public Document<D> appendWithReplicateToAndTimeout(Document<D> doc, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.append(doc, replicateTo, timeout, timeUnit);
	}
	*/

	// Counter methods
	public JsonLongDocument counter(String id, long delta) {
		return this.bucket.counter(id, delta);
	}

	public JsonLongDocument counterWithInitial(String id, long delta, long initial) {
		return this.bucket.counter(id, delta, initial);
	}

	public JsonLongDocument counterWithInitialExpiry(String id, long delta, long initial, int expiry) {
		return this.bucket.counter(id, delta, initial, expiry);
	}

	public JsonLongDocument counterWithInitialExpiryAndTimeout(String id, long delta, long initial, int expiry, long timeout, TimeUnit timeUnit) {
		return this.bucket.counter(id, delta, initial, expiry, timeout, timeUnit);
	}

	public JsonLongDocument counterWithInitialExpiryPersistTo(String id, long delta, long initial, int expiry, PersistTo persistTo) {
		return this.bucket.counter(id, delta, initial, expiry, persistTo);
	}

	public JsonLongDocument counterWithInitialExpiryPersistToAndTimeout(String id, long delta, long initial, int expiry, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.counter(id, delta, initial, expiry, persistTo, timeout, timeUnit);
	}

	public JsonLongDocument counterWithInitialExpiryPersistToReplicateTo(String id, long delta, long initial, int expiry, PersistTo persistTo, ReplicateTo replicateTo) {
		return this.bucket.counter(id, delta, initial, expiry, persistTo, replicateTo);
	}

	public JsonLongDocument counterWithInitialExpiryPersistToReplicateToAndTimeout(String id, long delta, long initial, int expiry, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.counter(id, delta, initial, expiry, persistTo, replicateTo, timeout, timeUnit);
	}

	public JsonLongDocument counterWithInitialExpiryReplicateTo(String id, long delta, long initial, int expiry, ReplicateTo replicateTo) {
		return this.bucket.counter(id, delta, initial, expiry, replicateTo);
	}

	public JsonLongDocument counterWithInitialExpiryReplicateToAndTimeout(String id, long delta, long initial, int expiry, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.counter(id, delta, initial, expiry, replicateTo, timeout, timeUnit);
	}

	public JsonLongDocument counterWithInitialAndTimeout(String id, long delta, long initial, long timeout, TimeUnit timeUnit) {
		return this.bucket.counter(id, delta, initial, timeout, timeUnit);
	}

	public JsonLongDocument counterWithInitialPersistTo(String id, long delta, long initial, PersistTo persistTo) {
		return this.bucket.counter(id, delta, initial, persistTo);
	}

	public JsonLongDocument counterWithInitialPersistToAndTimeout(String id, long delta, long initial, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.counter(id, delta, initial, persistTo, timeout, timeUnit);
	}

	public JsonLongDocument counterWithInitialPersistToReplicateTo(String id, long delta, long initial, PersistTo persistTo, ReplicateTo replicateTo) {
		return this.bucket.counter(id, delta, initial, persistTo, replicateTo);
	}

	public JsonLongDocument counterWithInitialPersistToReplicateToAndTimeout(String id, long delta, long initial, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.counter(id, delta, initial, persistTo, replicateTo, timeout, timeUnit);
	}

	public JsonLongDocument counterWithInitialReplicateTo(String id, long delta, long initial, ReplicateTo replicateTo) {
		return this.bucket.counter(id, delta, initial, replicateTo);
	}

	public JsonLongDocument counterWithInitialReplicateToAndTimeout(String id, long delta, long initial, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.counter(id, delta, initial, replicateTo, timeout, timeUnit);
	}

	public JsonLongDocument counterWithTimeout(String id, long delta, long timeout, TimeUnit timeUnit) {
		return this.bucket.counter(id, delta, timeout, timeUnit);
	}

	public JsonLongDocument counterWithPersistTo(String id, long delta, PersistTo persistTo) {
		return this.bucket.counter(id, delta, persistTo);
	}

	public JsonLongDocument counterWithPersistToAndTimeout(String id, long delta, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.counter(id, delta, persistTo, timeout, timeUnit);
	}

	public JsonLongDocument counterWithPersistToReplicateTo(String id, long delta, PersistTo persistTo, ReplicateTo replicateTo) {
		return this.bucket.counter(id, delta, persistTo, replicateTo);
	}

	public JsonLongDocument counterWithPersistToReplicateToAndTimeout(String id, long delta, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.counter(id, delta, persistTo, replicateTo, timeout, timeUnit);
	}

	public JsonLongDocument counterWithReplicateTo(String id, long delta, ReplicateTo replicateTo) {
		return this.bucket.counter(id, delta, replicateTo);
	}

	public JsonLongDocument counterWithReplicateToAndTimeout(String id, long delta, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.counter(id, delta, replicateTo, timeout, timeUnit);
	}
	
	// Exists methods
	/*
	public boolean exists(Document<D> doc) {
		return this.bucket.exists(doc);
	}

	public boolean existsWithTimeout(Document<D> doc, long timeout, TimeUnit timeUnit) {
		return this.bucket.exists(doc, timeout, timeUnit);
	}
	*/

	public boolean existsWithId(String id) {
		return this.bucket.exists(id);
	}

	public boolean existsWithIdAndTimeout(String id, long timeout, TimeUnit timeUnit) {
		return this.bucket.exists(id, timeout, timeUnit);
	}

	// Get methods
	public Document getWithDoc(Document doc) {
		return this.bucket.get(doc);
	}

	/*
	public Document<D> getWithDocAndTimeout(Document<D> doc, long timeout, TimeUnit timeUnit) {
		return this.bucket.get(doc, timeout, timeUnit);
	}
	
	public JsonDocument get(String id) {
		return this.bucket.get(id);
	}

	public JsonDocument getWithTimeout(String id, long timeout, TimeUnit timeUnit) {
		return this.bucket.get(id, timeout, timeUnit);
	}
	*/

	/*
	// Need to implement based on the requirements
	D	get(String id, Class<D> target)
	D	get(String id, Class<D> target, long timeout, TimeUnit timeUnit)
	*/
	
	public void insert(Document doc) {
		this.bucket.insert(doc);
	}
	
	public void insertWithTimeout(Document doc, long timeout, TimeUnit timeUnit) {
		this.bucket.insert(doc, timeout, timeUnit);
	}

	public void insertWithPersistToReplicateToAndTimeout(Document doc, PersistTo persistTo, ReplicateTo replicateTo,
			long timeout, TimeUnit timeUnit) {
		this.bucket.insert(doc, persistTo, replicateTo, timeout, timeUnit);
	}

	/*
	D	insert(D document, PersistTo persistTo)
	Insert a Document if it does not exist already and watch for durability constraints with the default key/value timeout.
	<D extends Document<?>>
	D	insert(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit)
	Insert a Document if it does not exist already and watch for durability constraints with a custom timeout.
	<D extends Document<?>>
	D	insert(D document, PersistTo persistTo, ReplicateTo replicateTo)
	Insert a Document if it does not exist already and watch for durability constraints with the default key/value timeout.
	<D extends Document<?>>
	D	insert(D document, ReplicateTo replicateTo)
	Insert a Document if it does not exist already and watch for durability constraints with the default key/value timeout.
	<D extends Document<?>>
	D	insert(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit)
	*/

	// Remove document methods
	public JsonDocument	remove(String id) {
		return this.bucket.remove(id);
	}

	public JsonDocument removeWithTimeout(String id, long timeout, TimeUnit timeUnit) {
		return this.bucket.remove(id, timeout, timeUnit);
	}

	public JsonDocument removeWithPersistTo(String id, PersistTo persistTo) {
		return this.bucket.remove(id, persistTo);	
	}

	public JsonDocument removeWithPersistToAndTimeout(String id, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.remove(id, persistTo, timeout, timeUnit);
	}

	public JsonDocument removeWithPersistToReplicateTo(String id, PersistTo persistTo, ReplicateTo replicateTo) {
		return this.bucket.remove(id, persistTo, replicateTo);
	}

	public JsonDocument removeWithPersistToReplicateToAndTimeout(String id, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.remove(id, persistTo, replicateTo, timeout, timeUnit);
	}

	public JsonDocument removeWithReplicateTo(String id, ReplicateTo replicateTo) {
		return this.bucket.remove(id, replicateTo);
	}

	public JsonDocument removeWithReplicateToAndTimeout(String id, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
		return this.bucket.remove(id, replicateTo, timeout, timeUnit);
	}

	/*
	<D extends Document<?>>
	D	remove(D document)
	Removes a Document from the Server with the default key/value timeout.
	<D extends Document<?>>
	D	remove(D document, long timeout, TimeUnit timeUnit)
	Removes a Document from the Server with a custom timeout.
	<D extends Document<?>>
	D	remove(D document, PersistTo persistTo)
	Removes a Document from the Server and apply a durability requirement with the default key/value timeout.
	<D extends Document<?>>
	D	remove(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit)
	Removes a Document from the Server and apply a durability requirement with a custom timeout.
	<D extends Document<?>>
	D	remove(D document, PersistTo persistTo, ReplicateTo replicateTo)
	Removes a Document from the Server and apply a durability requirement with the default key/value timeout.
	<D extends Document<?>>
	D	remove(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit)
	Removes a Document from the Server and apply a durability requirement with a custom timeout.
	<D extends Document<?>>
	D	remove(D document, ReplicateTo replicateTo)
	Removes a Document from the Server and apply a durability requirement with the default key/value timeout.
	<D extends Document<?>>
	D	remove(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit)
	Removes a Document from the Server and apply a durability requirement with a custom timeout.
	<D extends Document<?>>
	D	remove(String id, Class<D> target)
	Removes a Document from the Server identified by its ID with the default key/value timeout.
	<D extends Document<?>>
	D	remove(String id, Class<D> target, long timeout, TimeUnit timeUnit)
	Removes a Document from the Server identified by its ID with a custom timeout.
	<D extends Document<?>>
	D	remove(String id, PersistTo persistTo, Class<D> target)
	Removes a Document from the Server by its ID and apply a durability requirement with the default key/value timeout.
	<D extends Document<?>>
	D	remove(String id, PersistTo persistTo, Class<D> target, long timeout, TimeUnit timeUnit)
	Removes a Document from the Server by its ID and apply a durability requirement with a custom timeout.
	<D extends Document<?>>
	D	remove(String id, PersistTo persistTo, ReplicateTo replicateTo, Class<D> target)
	Removes a Document from the Server by its ID and apply a durability requirement with the default key/value timeout.
	<D extends Document<?>>
	D	remove(String id, PersistTo persistTo, ReplicateTo replicateTo, Class<D> target, long timeout, TimeUnit timeUnit)
	Removes a Document from the Server by its ID and apply a durability requirement with a custom timeout.
	<D extends Document<?>>
	D	remove(String id, ReplicateTo replicateTo, Class<D> target)
	Removes a Document from the Server by its ID and apply a durability requirement with the default key/value timeout.
	<D extends Document<?>>
	D	remove(String id, ReplicateTo replicateTo, Class<D> target, long timeout, TimeUnit timeUnit)
	Removes a Document from the Server by its ID and apply a durability requirement with a custom timeout.
	*/
}
