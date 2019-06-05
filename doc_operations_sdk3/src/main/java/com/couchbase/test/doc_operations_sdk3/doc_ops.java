package com.couchbase.test.doc_operations_sdk3;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

public class doc_ops {

	public List<HashMap<String, Object>> bulkInsert(Collection collection, List<Tuple2<String, JsonObject>> documents,
			long expiry, final String expiryTimeUnit, final int persistTo, final int replicateTo,
			final String durabilityLevel, final long timeOut, final String timeOutTimeUnit) {
		final InsertOptions insertOptions = this.getInsertOptions(expiry, expiryTimeUnit, persistTo, replicateTo,
				timeOut, timeOutTimeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<HashMap<String, Object>> returnValue = Flux.fromIterable(documents)
				.flatMap(new Function<Tuple2<String, JsonObject>, Publisher<HashMap<String, Object>>>() {
					public Publisher<HashMap<String, Object>> apply(Tuple2<String, JsonObject> documentToInsert) {
						final String id = documentToInsert.getT1();
						final JsonObject content = documentToInsert.getT2();
						final HashMap<String, Object> returnValue = new HashMap<String, Object>();
						returnValue.put("document", content);
						returnValue.put("error", null);
						returnValue.put("cas", 0);
						returnValue.put("status", true);
						returnValue.put("id", id);
						return reactiveCollection.insert(id, content, insertOptions)
								.map(new Function<MutationResult, HashMap<String, Object>>() {
									public HashMap<String, Object> apply(MutationResult result) {
										returnValue.put("result", result);
										returnValue.put("cas", result.cas());
										return returnValue;
									}
								}).onErrorResume(new Function<Throwable, Mono<HashMap<String, Object>>>() {
									public Mono<HashMap<String, Object>> apply(Throwable error) {
										returnValue.put("error", error);
										returnValue.put("status", false);
										return Mono.just(returnValue);
									}
								});
					}
				}).subscribeOn(Schedulers.parallel()).collectList().block();
		return returnValue;
	}

	public List<HashMap<String, Object>> bulkInsert(Collection collection, List<Tuple2<String, JsonObject>> documents,
			long expiry, final String expiryTimeUnit, final PersistTo persistTo, final ReplicateTo replicateTo,
			final DurabilityLevel durabilityLevel, final long timeOut, final String timeOutTimeUnit) {
		final InsertOptions insertOptions = this.getInsertOptions(expiry, expiryTimeUnit, persistTo, replicateTo,
				timeOut, timeOutTimeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<HashMap<String, Object>> returnValue = Flux.fromIterable(documents)
				.flatMap(new Function<Tuple2<String, JsonObject>, Publisher<HashMap<String, Object>>>() {
					public Publisher<HashMap<String, Object>> apply(Tuple2<String, JsonObject> documentToInsert) {
						final String id = documentToInsert.getT1();
						final JsonObject content = documentToInsert.getT2();
						final HashMap<String, Object> returnValue = new HashMap<String, Object>();
						returnValue.put("document", content);
						returnValue.put("error", null);
						returnValue.put("cas", 0);
						returnValue.put("status", true);
						returnValue.put("id", id);
						return reactiveCollection.insert(id, content, insertOptions)
								.map(new Function<MutationResult, HashMap<String, Object>>() {
									public HashMap<String, Object> apply(MutationResult result) {
										returnValue.put("result", result);
										returnValue.put("cas", result.cas());
										return returnValue;
									}
								}).onErrorResume(new Function<Throwable, Mono<HashMap<String, Object>>>() {
									public Mono<HashMap<String, Object>> apply(Throwable error) {
										returnValue.put("error", error);
										returnValue.put("status", false);
										return Mono.just(returnValue);
									}
								});
					}
				}).subscribeOn(Schedulers.parallel()).collectList().block();
		return returnValue;
	}


	public List<HashMap<String, Object>> bulkUpsert(Collection collection, List<Tuple2<String, JsonObject>> documents,
			long expiry, final String expiryTimeUnit, final int persistTo, final int replicateTo,
			final String durabilityLevel, final long timeOut, final String timeUnit) {
		final UpsertOptions upsertOptions = this.getUpsertOptions(expiry, expiryTimeUnit, persistTo, replicateTo,
				timeOut, timeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<HashMap<String, Object>> returnValue = Flux.fromIterable(documents)
				.flatMap(new Function<Tuple2<String, JsonObject>, Publisher<HashMap<String, Object>>>() {
					public Publisher<HashMap<String, Object>> apply(Tuple2<String, JsonObject> documentToInsert) {
						final String id = documentToInsert.getT1();
						final JsonObject content = documentToInsert.getT2();
						final HashMap<String, Object> returnValue = new HashMap<String, Object>();
						returnValue.put("document", content);
						returnValue.put("error", null);
						returnValue.put("cas", 0);
						returnValue.put("status", true);
						returnValue.put("id", id);
						return reactiveCollection.upsert(id, content, upsertOptions)
								.map(new Function<MutationResult, HashMap<String, Object>>() {
									public HashMap<String, Object> apply(MutationResult result) {
										returnValue.put("result", result);
										returnValue.put("cas", result.cas());
										return returnValue;
									}
								}).onErrorResume(new Function<Throwable, Mono<HashMap<String, Object>>>() {
									public Mono<HashMap<String, Object>> apply(Throwable error) {
										returnValue.put("error", error);
										returnValue.put("status", false);
										return Mono.just(returnValue);
									}
								});
					}
				}).subscribeOn(Schedulers.parallel()).collectList().block();
		return returnValue;
	}

	public List<HashMap<String, Object>> bulkUpsert(Collection collection, List<Tuple2<String, JsonObject>> documents,
			long expiry, final String expiryTimeUnit, final PersistTo persistTo, final ReplicateTo replicateTo,
			final DurabilityLevel durabilityLevel, final long timeOut, final String timeUnit) {
		final UpsertOptions upsertOptions = this.getUpsertOptions(expiry, expiryTimeUnit, persistTo, replicateTo,
				timeOut, timeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<HashMap<String, Object>> returnValue = Flux.fromIterable(documents)
				.flatMap(new Function<Tuple2<String, JsonObject>, Publisher<HashMap<String, Object>>>() {
					public Publisher<HashMap<String, Object>> apply(Tuple2<String, JsonObject> documentToInsert) {
						final String id = documentToInsert.getT1();
						final JsonObject content = documentToInsert.getT2();
						final HashMap<String, Object> returnValue = new HashMap<String, Object>();
						returnValue.put("document", content);
						returnValue.put("error", null);
						returnValue.put("cas", 0);
						returnValue.put("status", true);
						returnValue.put("id", id);
						return reactiveCollection.upsert(id, content, upsertOptions)
								.map(new Function<MutationResult, HashMap<String, Object>>() {
									public HashMap<String, Object> apply(MutationResult result) {
										returnValue.put("result", result);
										returnValue.put("cas", result.cas());
										return returnValue;
									}
								}).onErrorResume(new Function<Throwable, Mono<HashMap<String, Object>>>() {
									public Mono<HashMap<String, Object>> apply(Throwable error) {
										returnValue.put("error", error);
										returnValue.put("status", false);
										return Mono.just(returnValue);
									}
								});
					}
				}).subscribeOn(Schedulers.parallel()).collectList().block();
		return returnValue;
	}

	public List<HashMap<String, Object>> bulkGet(Collection collection, List<String> keys) {
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<HashMap<String, Object>> returnValue = Flux.fromIterable(keys)
				.flatMap(new Function<String, Publisher<HashMap<String, Object>>>() {
					public Publisher<HashMap<String, Object>> apply(String key) {
						final HashMap<String, Object> retVal = new HashMap<String, Object>();
						retVal.put("id", key);
						retVal.put("cas", 0);
						retVal.put("content", null);
						retVal.put("error", null);
						retVal.put("status", false);
						return reactiveCollection.get(key).map(new Function<Optional<GetResult>, HashMap<String, Object>>(){
							public HashMap<String, Object> apply(Optional<GetResult> optionalResult) {
								if (optionalResult.isPresent()){
									GetResult result = optionalResult.get();
									retVal.put("cas", result.cas());
									retVal.put("content", result.contentAsObject());
									retVal.put("status", true);
								}
								return retVal;
							}
						}).onErrorResume(new Function<Throwable, Mono<HashMap<String, Object>>>() {
							public Mono<HashMap<String, Object>> apply(Throwable error) {
								retVal.put("error", error);
								return Mono.just(retVal);
							}
						}).defaultIfEmpty(retVal);
					}
				}).subscribeOn(Schedulers.parallel()).collectList().block();
		return returnValue;
	}

	public List<HashMap<String, Object>> bulkDelete(Collection collection, List<String> keys, final int persistTo, final int replicateTo,
			final String durabilityLevel, final long timeOut, final String timeUnit) {
		final RemoveOptions removeOptions = this.getRemoveOptions(persistTo, replicateTo, timeOut, timeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<HashMap<String, Object>> returnValue = Flux.fromIterable(keys)
				.flatMap(new Function<String, Publisher<HashMap<String, Object>>>() {
					public Publisher<HashMap<String, Object>> apply(String key){
						final HashMap<String, Object> retVal = new HashMap<String, Object>();
						retVal.put("id", key);
						retVal.put("cas", 0);
						retVal.put("error", null);
						retVal.put("status", false);
						return reactiveCollection.remove(key, removeOptions)
								.map(new Function<MutationResult, HashMap<String, Object>>() {
									public HashMap<String, Object> apply(MutationResult result){
										retVal.put("cas", result.cas());
										retVal.put("status", true);
										return retVal;
									}
								}).onErrorResume(new Function<Throwable, Mono<HashMap<String, Object>>>() {
									public Mono<HashMap<String, Object>> apply(Throwable error) {
										retVal.put("error", error);
										return Mono.just(retVal);
							}
								}).defaultIfEmpty(retVal);
					}
				}).subscribeOn(Schedulers.parallel()).collectList().block();
		return returnValue;

	}


	public List<HashMap<String, Object>> bulkDelete(Collection collection, List<String> keys, final PersistTo persistTo, final ReplicateTo replicateTo,
			final DurabilityLevel durabilityLevel, final long timeOut, final String timeUnit) {
		final RemoveOptions removeOptions = this.getRemoveOptions(persistTo, replicateTo, timeOut, timeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<HashMap<String, Object>> returnValue = Flux.fromIterable(keys)
				.flatMap(new Function<String, Publisher<HashMap<String, Object>>>() {
					public Publisher<HashMap<String, Object>> apply(String key){
						final HashMap<String, Object> retVal = new HashMap<String, Object>();
						retVal.put("id", key);
						retVal.put("cas", 0);
						retVal.put("error", null);
						retVal.put("status", false);
						return reactiveCollection.remove(key, removeOptions)
								.map(new Function<MutationResult, HashMap<String, Object>>() {
									public HashMap<String, Object> apply(MutationResult result){
										retVal.put("cas", result.cas());
										retVal.put("status", true);
										return retVal;
									}
								}).onErrorResume(new Function<Throwable, Mono<HashMap<String, Object>>>() {
									public Mono<HashMap<String, Object>> apply(Throwable error) {
										retVal.put("error", error);
										return Mono.just(retVal);
							}
								}).defaultIfEmpty(retVal);
					}
				}).subscribeOn(Schedulers.parallel()).collectList().block();
		return returnValue;
	}


	private InsertOptions getInsertOptions(long expiry, String expiryTimeUnit, int persistTo, int replicateTo,
			long timeOut, String timeUnit, String durabilityLevel) {
		PersistTo persistto = this.getPersistTo(persistTo);
		ReplicateTo replicateto = this.getReplicateTo(replicateTo);
		Duration exp = this.getDuration(expiry, expiryTimeUnit);
		Duration timeout = this.getDuration(timeOut, timeUnit);
		DurabilityLevel durabilitylevel = this.getDurabilityLevel(durabilityLevel);
		if (persistTo != 0 || replicateTo !=0) {
			return InsertOptions.insertOptions().durability(persistto, replicateto).timeout(timeout);
		}
		else {
			return InsertOptions.insertOptions().timeout(timeout).durabilityLevel(durabilitylevel);
		}
	}

	private InsertOptions getInsertOptions(long expiry, String expiryTimeUnit, PersistTo persistTo, ReplicateTo replicateTo,
			long timeOut, String timeUnit, DurabilityLevel durabilityLevel) {
		Duration exp = this.getDuration(expiry, expiryTimeUnit);
		Duration timeout = this.getDuration(timeOut, timeUnit);
		if (persistTo == PersistTo.NONE|| replicateTo == ReplicateTo.NONE) {
			return InsertOptions.insertOptions().durability(persistTo, replicateTo).timeout(timeout);
		}
		else {
			return InsertOptions.insertOptions().durabilityLevel(durabilityLevel).timeout(timeout);
		}
	}

	private UpsertOptions getUpsertOptions(long expiry, String expiryTimeUnit, int persistTo, int replicateTo,
			long timeOut, String timeUnit, String durabilityLevel) {
		PersistTo persistto = this.getPersistTo(persistTo);
		ReplicateTo replicateto = this.getReplicateTo(replicateTo);
		Duration exp = this.getDuration(expiry, expiryTimeUnit);
		Duration timeout = this.getDuration(timeOut, timeUnit);
		DurabilityLevel durabilitylevel = this.getDurabilityLevel(durabilityLevel);
		if (persistTo != 0 || replicateTo !=0) {
			return UpsertOptions.upsertOptions().durability(persistto, replicateto).timeout(timeout);
		}
		else {
			return UpsertOptions.upsertOptions().durabilityLevel(durabilitylevel).timeout(timeout);
		}
	}

	private UpsertOptions getUpsertOptions(long expiry, String expiryTimeUnit, PersistTo persistTo, ReplicateTo replicateTo,
			long timeOut, String timeUnit, DurabilityLevel durabilityLevel) {
		Duration exp = this.getDuration(expiry, expiryTimeUnit);
		Duration timeout = this.getDuration(timeOut, timeUnit);
		if (persistTo == PersistTo.NONE|| replicateTo == ReplicateTo.NONE) {
			return UpsertOptions.upsertOptions().durability(persistTo, replicateTo).timeout(timeout);
		}
		else {
			return UpsertOptions.upsertOptions().durabilityLevel(durabilityLevel).timeout(timeout);
		}
	}

	private RemoveOptions getRemoveOptions(int persistTo, int replicateTo,
			long timeOut, String timeUnit, String durabilityLevel) {
		PersistTo persistto = this.getPersistTo(persistTo);
		ReplicateTo replicateto = this.getReplicateTo(replicateTo);
		Duration timeout = this.getDuration(timeOut, timeUnit);
		DurabilityLevel durabilitylevel = this.getDurabilityLevel(durabilityLevel);
		if (persistTo != 0 || replicateTo !=0) {
			return RemoveOptions.removeOptions().durability(persistto, replicateto).timeout(timeout);
		}
		else {
			return RemoveOptions.removeOptions().durabilityLevel(durabilitylevel).timeout(timeout);
		}
	}

	private RemoveOptions getRemoveOptions(PersistTo persistTo, ReplicateTo replicateTo,
			long timeOut, String timeUnit, DurabilityLevel durabilityLevel) {
		Duration timeout = this.getDuration(timeOut, timeUnit);
		if (persistTo == PersistTo.NONE || replicateTo == ReplicateTo.NONE) {
			return RemoveOptions.removeOptions().durability(persistTo, replicateTo).timeout(timeout);
		}
		else {
			return RemoveOptions.removeOptions().durabilityLevel(durabilityLevel).timeout(timeout);
		}
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
			return PersistTo.ACTIVE;
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

	private DurabilityLevel getDurabilityLevel(String durabilityLevel) {
		if (durabilityLevel.equalsIgnoreCase("MAJORITY")) {
			return DurabilityLevel.MAJORITY;
		}
		if (durabilityLevel.equalsIgnoreCase("MAJORITY_AND_PERSIST_ON_MASTER")) {
			return DurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER;
		}
		if (durabilityLevel.equalsIgnoreCase("PERSIST_TO_MAJORITY")) {
			return DurabilityLevel.PERSIST_TO_MAJORITY;
		}

		return DurabilityLevel.NONE;
	}

	private Duration getDuration(long time, String timeUnit) {
		TemporalUnit temporalUnit;
		if (timeUnit.equalsIgnoreCase("seconds")) {
			temporalUnit = ChronoUnit.SECONDS;
		} else if (timeUnit.equalsIgnoreCase("milliseconds")) {
			temporalUnit = ChronoUnit.MILLIS;
		} else if (timeUnit.equalsIgnoreCase("minutes")) {
			temporalUnit = ChronoUnit.MINUTES;
		} else {
			temporalUnit = ChronoUnit.SECONDS;
		}
		return Duration.of(time, temporalUnit);
	}

}
