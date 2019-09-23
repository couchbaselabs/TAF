package com.couchbase.test.doc_operations_sdk3;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
//import java.util.Optional;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.TouchOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

public class doc_ops {

	public List<ConcurrentHashMap<String, Object>> bulkInsert(Collection collection, List<Tuple2<String, JsonObject>> documents,
			long expiry, String expiryTimeUnit, int persistTo, int replicateTo,
			String durabilityLevel, long timeOut, String timeOutTimeUnit) {
		InsertOptions insertOptions = this.getInsertOptions(expiry, expiryTimeUnit, persistTo, replicateTo,
				timeOut, timeOutTimeUnit, durabilityLevel);
		ReactiveCollection reactiveCollection = collection.reactive();
		List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(documents)
				.flatMap(new Function<Tuple2<String, JsonObject>, Publisher<ConcurrentHashMap<String, Object>>>() {
					public Publisher<ConcurrentHashMap<String, Object>> apply(Tuple2<String, JsonObject> documentToInsert) {
						String id = documentToInsert.getT1();
						JsonObject content = documentToInsert.getT2();
						final ConcurrentHashMap<String, Object> retValue = new ConcurrentHashMap<String, Object>();
						retValue.put("document", content);
						retValue.put("error", "");
						retValue.put("cas", 0);
						retValue.put("status", true);
						retValue.put("id", id);
						return reactiveCollection.insert(id, content, insertOptions)
								.map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
									public ConcurrentHashMap<String, Object> apply(MutationResult result) {
										retValue.put("result", result);
										retValue.put("cas", result.cas());
										return retValue;
									}
								}).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
									public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
										retValue.put("error", error);
										retValue.put("status", false);
										return Mono.just(retValue);
									}
								});
					}
				}).collectList().block();
		return returnValue;
	}

	public List<ConcurrentHashMap<String, Object>> bulkInsert(Collection collection, List<Tuple2<String, JsonObject>> documents,
			long expiry, final String expiryTimeUnit, final PersistTo persistTo, final ReplicateTo replicateTo,
			final DurabilityLevel durabilityLevel, final long timeOut, final String timeOutTimeUnit) {
		final InsertOptions insertOptions = this.getInsertOptions(expiry, expiryTimeUnit, persistTo, replicateTo,
				timeOut, timeOutTimeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(documents)
				.flatMap(new Function<Tuple2<String, JsonObject>, Publisher<ConcurrentHashMap<String, Object>>>() {
					public Publisher<ConcurrentHashMap<String, Object>> apply(Tuple2<String, JsonObject> documentToInsert) {
						final String id = documentToInsert.getT1();
						final JsonObject content = documentToInsert.getT2();
						final ConcurrentHashMap<String, Object> returnValue = new ConcurrentHashMap<String, Object>();
						returnValue.put("document", content);
						returnValue.put("error", "");
						returnValue.put("cas", 0);
						returnValue.put("status", true);
						returnValue.put("id", id);
						return reactiveCollection.insert(id, content, insertOptions)
								.map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
									public ConcurrentHashMap<String, Object> apply(MutationResult result) {
										returnValue.put("result", result);
										returnValue.put("cas", result.cas());
										return returnValue;
									}
								}).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
									public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
										returnValue.put("error", error);
										returnValue.put("status", false);
										return Mono.just(returnValue);
									}
								});
					}
				}).collectList().block();
		return returnValue;
	}

	public List<ConcurrentHashMap<String, Object>> bulkUpsert(Collection collection, List<Tuple2<String, JsonObject>> documents,
			long expiry, final String expiryTimeUnit, final int persistTo, final int replicateTo,
			final String durabilityLevel, final long timeOut, final String timeUnit) {
		final UpsertOptions upsertOptions = this.getUpsertOptions(expiry, expiryTimeUnit, persistTo, replicateTo,
				timeOut, timeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(documents)
				.flatMap(new Function<Tuple2<String, JsonObject>, Publisher<ConcurrentHashMap<String, Object>>>() {
					public Publisher<ConcurrentHashMap<String, Object>> apply(Tuple2<String, JsonObject> documentToInsert) {
						final String id = documentToInsert.getT1();
						final JsonObject content = documentToInsert.getT2();
						final ConcurrentHashMap<String, Object> returnValue = new ConcurrentHashMap<String, Object>();
						returnValue.put("document", content);
						returnValue.put("error", "");
						returnValue.put("cas", 0);
						returnValue.put("status", true);
						returnValue.put("id", id);
						return reactiveCollection.upsert(id, content, upsertOptions)
								.map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
									public ConcurrentHashMap<String, Object> apply(MutationResult result) {
										returnValue.put("result", result);
										returnValue.put("cas", result.cas());
										return returnValue;
									}
								}).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
									public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
										returnValue.put("error", error);
										returnValue.put("status", false);
										return Mono.just(returnValue);
									}
								});
					}
				}).collectList().block();
		return returnValue;
	}

	public List<ConcurrentHashMap<String, Object>> bulkUpsert(Collection collection, List<Tuple2<String, JsonObject>> documents,
			long expiry, final String expiryTimeUnit, final PersistTo persistTo, final ReplicateTo replicateTo,
			final DurabilityLevel durabilityLevel, final long timeOut, final String timeUnit) {
		final UpsertOptions upsertOptions = this.getUpsertOptions(expiry, expiryTimeUnit, persistTo, replicateTo,
				timeOut, timeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(documents)
				.flatMap(new Function<Tuple2<String, JsonObject>, Publisher<ConcurrentHashMap<String, Object>>>() {
					public Publisher<ConcurrentHashMap<String, Object>> apply(Tuple2<String, JsonObject> documentToInsert) {
						final String id = documentToInsert.getT1();
						final JsonObject content = documentToInsert.getT2();
						final ConcurrentHashMap<String, Object> returnValue = new ConcurrentHashMap<String, Object>();
						returnValue.put("document", content);
						returnValue.put("error", "");
						returnValue.put("cas", 0);
						returnValue.put("status", true);
						returnValue.put("id", id);
						return reactiveCollection.upsert(id, content, upsertOptions)
								.map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
									public ConcurrentHashMap<String, Object> apply(MutationResult result) {
										returnValue.put("result", result);
										returnValue.put("cas", result.cas());
										return returnValue;
									}
								}).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
									public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
										returnValue.put("error", error);
										returnValue.put("status", false);
										return Mono.just(returnValue);
									}
								});
					}
				}).collectList().block();
		return returnValue;
	}

	public List<ConcurrentHashMap<String, Object>> bulkReplace(Collection collection, List<Tuple2<String, JsonObject>> documents,
			long expiry, final String expiryTimeUnit, final int persistTo, final int replicateTo,
			final String durabilityLevel, final long timeOut, final String timeUnit) {
		final ReplaceOptions replaceOptions = this.getReplaceOptions(expiry, expiryTimeUnit, persistTo, replicateTo,
				timeOut, timeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(documents)
				.flatMap(new Function<Tuple2<String, JsonObject>, Publisher<ConcurrentHashMap<String, Object>>>() {
					public Publisher<ConcurrentHashMap<String, Object>> apply(Tuple2<String, JsonObject> documentToInsert) {
						final String id = documentToInsert.getT1();
						final JsonObject content = documentToInsert.getT2();
						final ConcurrentHashMap<String, Object> returnValue = new ConcurrentHashMap<String, Object>();
						returnValue.put("document", content);
						returnValue.put("error", "");
						returnValue.put("cas", 0);
						returnValue.put("status", true);
						returnValue.put("id", id);
						return reactiveCollection.replace(id, content, replaceOptions)
								.map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
									public ConcurrentHashMap<String, Object> apply(MutationResult result) {
										returnValue.put("result", result);
										returnValue.put("cas", result.cas());
										return returnValue;
									}
								}).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
									public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
										returnValue.put("error", error);
										returnValue.put("status", false);
										return Mono.just(returnValue);
									}
								});
					}
				}).collectList().block();
		return returnValue;
	}

	public List<ConcurrentHashMap<String, Object>> bulkReplace(Collection collection, List<Tuple2<String, JsonObject>> documents,
			long expiry, final String expiryTimeUnit, final PersistTo persistTo, final ReplicateTo replicateTo,
			final DurabilityLevel durabilityLevel, final long timeOut, final String timeUnit) {
		final ReplaceOptions replaceOptions = this.getReplaceOptions(expiry, expiryTimeUnit, persistTo, replicateTo,
				timeOut, timeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(documents)
				.flatMap(new Function<Tuple2<String, JsonObject>, Publisher<ConcurrentHashMap<String, Object>>>() {
					public Publisher<ConcurrentHashMap<String, Object>> apply(Tuple2<String, JsonObject> documentToInsert) {
						final String id = documentToInsert.getT1();
						final JsonObject content = documentToInsert.getT2();
						final ConcurrentHashMap<String, Object> returnValue = new ConcurrentHashMap<String, Object>();
						returnValue.put("document", content);
						returnValue.put("error", "");
						returnValue.put("cas", 0);
						returnValue.put("status", true);
						returnValue.put("id", id);
						return reactiveCollection.replace(id, content, replaceOptions)
								.map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
									public ConcurrentHashMap<String, Object> apply(MutationResult result) {
										returnValue.put("result", result);
										returnValue.put("cas", result.cas());
										return returnValue;
									}
								}).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
									public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
										returnValue.put("error", error);
										returnValue.put("status", false);
										return Mono.just(returnValue);
									}
								});
					}
				}).collectList().block();
		return returnValue;
	}

	public List<ConcurrentHashMap<String, Object>> bulkTouch(Collection collection, List<String> keys, final int exp, final int persistTo, final int replicateTo,
			final String durabilityLevel, final long timeOut, final String timeUnit) {
		final TouchOptions touchOptions = this.getTouchOptions(persistTo, replicateTo, timeOut, timeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		final Duration exp_duration = this.getDuration(exp, "seconds");
		List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(keys)
				.flatMap(new Function<String, Publisher<ConcurrentHashMap<String, Object>>>() {
					public Publisher<ConcurrentHashMap<String, Object>> apply(String key){
						final ConcurrentHashMap<String, Object> retVal = new ConcurrentHashMap<String, Object>();
						retVal.put("id", key);
						retVal.put("cas", 0);
						retVal.put("error", "");
						retVal.put("status", false);
						return reactiveCollection.touch(key, exp_duration, touchOptions)
								.map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
									public ConcurrentHashMap<String, Object> apply(MutationResult result){
										retVal.put("cas", result.cas());
										retVal.put("status", true);
										return retVal;
									}
								}).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
									public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
										retVal.put("error", error);
										return Mono.just(retVal);
							}
								}).defaultIfEmpty(retVal);
					}
				}).collectList().block();
		return returnValue;
	}

	public List<ConcurrentHashMap<String, Object>> bulkTouch(Collection collection, List<String> keys, final Duration exp, final PersistTo persistTo, final ReplicateTo replicateTo,
			final DurabilityLevel durabilityLevel, final long timeOut, final String timeUnit) {
		final TouchOptions touchOptions = this.getTouchOptions(persistTo, replicateTo, timeOut, timeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(keys)
				.flatMap(new Function<String, Publisher<ConcurrentHashMap<String, Object>>>() {
					public Publisher<ConcurrentHashMap<String, Object>> apply(String key){
						final ConcurrentHashMap<String, Object> retVal = new ConcurrentHashMap<String, Object>();
						retVal.put("id", key);
						retVal.put("cas", 0);
						retVal.put("error", "");
						retVal.put("status", false);
						return reactiveCollection.touch(key, exp, touchOptions)
								.map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
									public ConcurrentHashMap<String, Object> apply(MutationResult result){
										retVal.put("cas", result.cas());
										retVal.put("status", true);
										return retVal;
									}
								}).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
									public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
										retVal.put("error", error);
										return Mono.just(retVal);
							}
								}).defaultIfEmpty(retVal);
					}
				}).collectList().block();
		return returnValue;
	}

	public List<ConcurrentHashMap<String, Object>> bulkGet(Collection collection, List<String> keys,
			final long timeOut, final String timeUnit) {
		final ReactiveCollection reactiveCollection = collection.reactive();
		final GetOptions getOptions = this.getReadOptions(timeOut, timeUnit);
		List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(keys)
				.flatMap(new Function<String, Publisher<ConcurrentHashMap<String, Object>>>() {
					public Publisher<ConcurrentHashMap<String, Object>> apply(String key) {
						final ConcurrentHashMap<String, Object> retVal = new ConcurrentHashMap<String, Object>();
						retVal.put("id", key);
						retVal.put("cas", 0);
						retVal.put("content", "");
						retVal.put("error", "");
						retVal.put("status", false);
						return reactiveCollection.get(key, getOptions)
							.map(new Function<GetResult, ConcurrentHashMap<String, Object>>() {
								public ConcurrentHashMap<String, Object> apply(GetResult optionalResult) {
										retVal.put("cas", optionalResult.cas());
										retVal.put("content", optionalResult.contentAsObject());
										retVal.put("status", true);
									return retVal;
								}
						}).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
							public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
								retVal.put("error", error);
								return Mono.just(retVal);
							}
						}).defaultIfEmpty(retVal);
					}
				}).collectList().block();
		return returnValue;
	}

	public List<ConcurrentHashMap<String, Object>> bulkDelete(Collection collection, List<String> keys, final int persistTo, final int replicateTo,
			final String durabilityLevel, final long timeOut, final String timeUnit) {
		final RemoveOptions removeOptions = this.getRemoveOptions(persistTo, replicateTo, timeOut, timeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(keys)
				.flatMap(new Function<String, Publisher<ConcurrentHashMap<String, Object>>>() {
					public Publisher<ConcurrentHashMap<String, Object>> apply(String key){
						final ConcurrentHashMap<String, Object> retVal = new ConcurrentHashMap<String, Object>();
						retVal.put("id", key);
						retVal.put("cas", 0);
						retVal.put("error", "");
						retVal.put("status", false);
						return reactiveCollection.remove(key, removeOptions)
								.map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
									public ConcurrentHashMap<String, Object> apply(MutationResult result){
										retVal.put("cas", result.cas());
										retVal.put("status", true);
										return retVal;
									}
								}).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
									public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
										retVal.put("error", error);
										return Mono.just(retVal);
							}
								}).defaultIfEmpty(retVal);
					}
				}).collectList().block();
		return returnValue;
	}

	public List<ConcurrentHashMap<String, Object>> bulkDelete(Collection collection, List<String> keys, final PersistTo persistTo, final ReplicateTo replicateTo,
			final DurabilityLevel durabilityLevel, final long timeOut, final String timeUnit) {
		final RemoveOptions removeOptions = this.getRemoveOptions(persistTo, replicateTo, timeOut, timeUnit, durabilityLevel);
		final ReactiveCollection reactiveCollection = collection.reactive();
		List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(keys)
				.flatMap(new Function<String, Publisher<ConcurrentHashMap<String, Object>>>() {
					public Publisher<ConcurrentHashMap<String, Object>> apply(String key){
						final ConcurrentHashMap<String, Object> retVal = new ConcurrentHashMap<String, Object>();
						retVal.put("id", key);
						retVal.put("cas", 0);
						retVal.put("error", "");
						retVal.put("status", false);
						return reactiveCollection.remove(key, removeOptions)
								.map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
									public ConcurrentHashMap<String, Object> apply(MutationResult result){
										retVal.put("cas", result.cas());
										retVal.put("status", true);
										return retVal;
									}
								}).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
									public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
										retVal.put("error", error);
										return Mono.just(retVal);
							}
								}).defaultIfEmpty(retVal);
					}
				}).collectList().block();
		return returnValue;
	}

	private GetOptions getReadOptions(long timeOut, String timeUnit) {
		Duration timeout = this.getDuration(timeOut, timeUnit);
		return GetOptions.getOptions().timeout(timeout);
	}

	private InsertOptions getInsertOptions(long expiry, String expiryTimeUnit, int persistTo, int replicateTo,
			long timeOut, String timeUnit, String durabilityLevel) {
		PersistTo persistto = this.getPersistTo(persistTo);
		ReplicateTo replicateto = this.getReplicateTo(replicateTo);
		Duration exp = this.getDuration(expiry, expiryTimeUnit);
		Duration timeout = this.getDuration(timeOut, timeUnit);
		DurabilityLevel durabilitylevel = this.getDurabilityLevel(durabilityLevel);
		if (persistTo != 0 || replicateTo !=0) {
			return InsertOptions.insertOptions().expiry(exp).durability(persistto, replicateto).timeout(timeout);
		}
		else {
			return InsertOptions.insertOptions().expiry(exp).timeout(timeout).durability(durabilitylevel);
		}
	}

	private InsertOptions getInsertOptions(long expiry, String expiryTimeUnit, PersistTo persistTo, ReplicateTo replicateTo,
			long timeOut, String timeUnit, DurabilityLevel durabilityLevel) {
		Duration exp = this.getDuration(expiry, expiryTimeUnit);
		Duration timeout = this.getDuration(timeOut, timeUnit);
		if (persistTo == PersistTo.NONE|| replicateTo == ReplicateTo.NONE) {
			return InsertOptions.insertOptions().expiry(exp).durability(persistTo, replicateTo).timeout(timeout);
		}
		else {
			return InsertOptions.insertOptions().expiry(exp).durability(durabilityLevel).timeout(timeout);
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
			return UpsertOptions.upsertOptions().expiry(exp).durability(persistto, replicateto).timeout(timeout);
		}
		else {
			return UpsertOptions.upsertOptions().expiry(exp).durability(durabilitylevel).timeout(timeout);
		}
	}

	private UpsertOptions getUpsertOptions(long expiry, String expiryTimeUnit, PersistTo persistTo, ReplicateTo replicateTo,
			long timeOut, String timeUnit, DurabilityLevel durabilityLevel) {
		Duration exp = this.getDuration(expiry, expiryTimeUnit);
		Duration timeout = this.getDuration(timeOut, timeUnit);
		if (persistTo == PersistTo.NONE|| replicateTo == ReplicateTo.NONE) {
			return UpsertOptions.upsertOptions().expiry(exp).durability(persistTo, replicateTo).timeout(timeout);
		}
		else {
			return UpsertOptions.upsertOptions().expiry(exp).durability(durabilityLevel).timeout(timeout);
		}
	}

	private ReplaceOptions getReplaceOptions(long expiry, String expiryTimeUnit, int persistTo, int replicateTo,
			long timeOut, String timeUnit, String durabilityLevel) {
		PersistTo persistto = this.getPersistTo(persistTo);
		ReplicateTo replicateto = this.getReplicateTo(replicateTo);
		Duration exp = this.getDuration(expiry, expiryTimeUnit);
		Duration timeout = this.getDuration(timeOut, timeUnit);
		DurabilityLevel durabilitylevel = this.getDurabilityLevel(durabilityLevel);
		if (persistTo != 0 || replicateTo !=0) {
			return ReplaceOptions.replaceOptions().expiry(exp).durability(persistto, replicateto).timeout(timeout);
		}
		else {
			return ReplaceOptions.replaceOptions().expiry(exp).durability(durabilitylevel).timeout(timeout);
		}
	}

	private ReplaceOptions getReplaceOptions(long expiry, String expiryTimeUnit, PersistTo persistTo, ReplicateTo replicateTo,
			long timeOut, String timeUnit, DurabilityLevel durabilityLevel) {
		Duration exp = this.getDuration(expiry, expiryTimeUnit);
		Duration timeout = this.getDuration(timeOut, timeUnit);
		if (persistTo == PersistTo.NONE|| replicateTo == ReplicateTo.NONE) {
			return ReplaceOptions.replaceOptions().expiry(exp).durability(persistTo, replicateTo).timeout(timeout);
		}
		else {
			return ReplaceOptions.replaceOptions().expiry(exp).durability(durabilityLevel).timeout(timeout);
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
			return RemoveOptions.removeOptions().durability(durabilitylevel).timeout(timeout);
		}
	}

	private RemoveOptions getRemoveOptions(PersistTo persistTo, ReplicateTo replicateTo,
			long timeOut, String timeUnit, DurabilityLevel durabilityLevel) {
		Duration timeout = this.getDuration(timeOut, timeUnit);
		if (persistTo == PersistTo.NONE || replicateTo == ReplicateTo.NONE) {
			return RemoveOptions.removeOptions().durability(persistTo, replicateTo).timeout(timeout);
		}
		else {
			return RemoveOptions.removeOptions().durability(durabilityLevel).timeout(timeout);
		}
	}

	private ReplaceOptions getReplaceOptions(int persistTo, int replicateTo,
			long timeOut, String timeUnit, String durabilityLevel) {
		PersistTo persistto = this.getPersistTo(persistTo);
		ReplicateTo replicateto = this.getReplicateTo(replicateTo);
		Duration timeout = this.getDuration(timeOut, timeUnit);
		DurabilityLevel durabilitylevel = this.getDurabilityLevel(durabilityLevel);
		if (persistTo != 0 || replicateTo != 0) {
			return ReplaceOptions.replaceOptions().durability(persistto, replicateto).timeout(timeout);
		}
		else {
			return ReplaceOptions.replaceOptions().durability(durabilitylevel).timeout(timeout);
		}
	}

	private ReplaceOptions getReplaceOptions(PersistTo persistTo, ReplicateTo replicateTo,
			long timeOut, String timeUnit, DurabilityLevel durabilityLevel) {
		Duration timeout = this.getDuration(timeOut, timeUnit);
		if (persistTo == PersistTo.NONE || replicateTo == ReplicateTo.NONE) {
			return ReplaceOptions.replaceOptions().durability(persistTo, replicateTo).timeout(timeout);
		}
		else {
			return ReplaceOptions.replaceOptions().durability(durabilityLevel).timeout(timeout);
		}
	}

	private TouchOptions getTouchOptions(int persistTo, int replicateTo,
			long timeOut, String timeUnit, String durabilityLevel) {
		PersistTo persistto = this.getPersistTo(persistTo);
		ReplicateTo replicateto = this.getReplicateTo(replicateTo);
		Duration timeout = this.getDuration(timeOut, timeUnit);
		DurabilityLevel durabilitylevel = this.getDurabilityLevel(durabilityLevel);
		if (persistTo != 0 || replicateTo !=0) {
			return TouchOptions.touchOptions().durability(persistto, replicateto).timeout(timeout);
		}
		else {
			return TouchOptions.touchOptions().durability(durabilitylevel).timeout(timeout);
		}
	}

	private TouchOptions getTouchOptions(PersistTo persistTo, ReplicateTo replicateTo,
			long timeOut, String timeUnit, DurabilityLevel durabilityLevel) {
		Duration timeout = this.getDuration(timeOut, timeUnit);
		if (persistTo == PersistTo.NONE || replicateTo == ReplicateTo.NONE) {
			return TouchOptions.touchOptions().durability(persistTo, replicateTo).timeout(timeout);
		}
		else {
			return TouchOptions.touchOptions().durability(durabilityLevel).timeout(timeout);
		}
	}
	
	protected PersistTo getPersistTo(int persistTo) {
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

	protected ReplicateTo getReplicateTo(int replicateTo) {
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

	protected DurabilityLevel getDurabilityLevel(String durabilityLevel) {
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

	protected Duration getDuration(long time, String timeUnit) {
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
