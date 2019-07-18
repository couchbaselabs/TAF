package com.couchbase.test.doc_operations_sdk3;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInResult;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

public class SubDocOperations extends doc_ops {
    private MutateInOptions getMutateInOptions(long expiry, String expiryTimeUnit, PersistTo persistTo,
            ReplicateTo replicateTo, long timeOut, String timeUnit, DurabilityLevel durabilityLevel) {
        Duration exp = this.getDuration(expiry, expiryTimeUnit);
        Duration timeout = this.getDuration(timeOut, timeUnit);
        if (persistTo != PersistTo.NONE || replicateTo != ReplicateTo.NONE) {
            return MutateInOptions.mutateInOptions().durability(persistTo, replicateTo).expiry(exp).timeout(timeout);
        } else {
            return MutateInOptions.mutateInOptions().durabilityLevel(durabilityLevel).expiry(exp).timeout(timeout);
        }
    }

    public MutateInSpec getInsertMutateInSpec(String path, Object value,Boolean createPath, Boolean xattr) {
        MutateInSpec mutateInSpec;
        if (xattr) {
            if (createPath) {
                mutateInSpec = MutateInSpec.insert(path, value).createPath().xattr();
            }
            else {
                mutateInSpec = MutateInSpec.insert(path, value).createPath().xattr();
            }
        }
        else {
            if (createPath) {
                mutateInSpec = MutateInSpec.insert(path, value).createPath();
            }
            else {
                mutateInSpec = MutateInSpec.insert(path, value);
            }
        }
        return mutateInSpec;
    }

    public MutateInSpec getUpsertMutateInSpec(String path, Object value,Boolean createPath, Boolean xattr) {
        MutateInSpec mutateInSpec;
        if (xattr) {
            if (createPath) {
                mutateInSpec = MutateInSpec.upsert(path, value).createPath().xattr();
            }
            else {
                mutateInSpec = MutateInSpec.upsert(path, value).createPath().xattr();
            }
        }
        else {
            if (createPath) {
                mutateInSpec = MutateInSpec.upsert(path, value).createPath();
            }
            else {
                mutateInSpec = MutateInSpec.upsert(path, value);
            }
        }
        return mutateInSpec;
    }

    public MutateInSpec getRemoveMutateInSpec(String path, Boolean xattr) {
        MutateInSpec mutateInSpec;
        if (xattr) {
            mutateInSpec = MutateInSpec.remove(path).xattr();
        }
        else {
            mutateInSpec = MutateInSpec.remove(path);
        }
        return mutateInSpec;
    }

    public MutateInSpec getReplaceMutateInSpec(String path, Object value, Boolean xattr) {
        MutateInSpec mutateInSpec;
        if (xattr) {
            mutateInSpec = MutateInSpec.replace(path, value).xattr();
        }
        else {
            mutateInSpec = MutateInSpec.replace(path, value);
        }
        return mutateInSpec;
    }

    public MutateInSpec getIncrMutateInSpec(String path, long delta) {
        return MutateInSpec.increment(path, delta);
    }

    public LookupInSpec getLookUpInSpec(String path) {
        return LookupInSpec.get(path);
    }

    public List<HashMap<String, Object>> bulkSubDocOperation(Collection collection,
            List<Tuple2<String, List<MutateInSpec>>> mutateInSpecs, long expiry, final String expiryTimeUnit,
            final int persistTo, final int replicateTo, final String durabilityLevel, final long timeOut,
            final String timeUnit) {
        PersistTo persistto = this.getPersistTo(persistTo);
        ReplicateTo replicateto = this.getReplicateTo(replicateTo);
        DurabilityLevel durabilitylevel = this.getDurabilityLevel(durabilityLevel);
        return this.bulkSubDocOperation(collection, mutateInSpecs, expiry, expiryTimeUnit,
                                       persistto, replicateto, durabilitylevel, timeOut, timeUnit);
    }

    public List<HashMap<String, Object>> bulkSubDocOperation(Collection collection,
            List<Tuple2<String, List<MutateInSpec>>> mutateInSpecs, long expiry, final String expiryTimeUnit, final PersistTo persistTo, final ReplicateTo replicateTo,
            final DurabilityLevel durabilityLevel, final long timeOut, final String timeUnit) {
        final MutateInOptions mutateInOptions = this.getMutateInOptions(expiry, expiryTimeUnit, persistTo, replicateTo, timeOut, timeUnit, durabilityLevel);
        final ReactiveCollection reactiveCollection = collection.reactive();
        List<HashMap<String, Object>> returnValue = Flux.fromIterable(mutateInSpecs)
                .flatMap(new Function<Tuple2<String, List<MutateInSpec>>, Publisher<HashMap<String, Object>>>() {
                    public Publisher<HashMap<String, Object>> apply(Tuple2<String, List<MutateInSpec>> subDocOperations) {
                        final String id = subDocOperations.getT1();
                        final List<MutateInSpec> subDocOps = subDocOperations.getT2();
                        final HashMap<String, Object> returnValue = new HashMap<String, Object>();
                        returnValue.put("error", null);
                        returnValue.put("cas", 0);
                        returnValue.put("status", true);
                        returnValue.put("id", id);
                        return reactiveCollection.mutateIn(id, subDocOps, mutateInOptions)
                                .map(new Function<MutateInResult, HashMap<String, Object>>() {
                                    public HashMap<String, Object> apply(MutateInResult result) {
                                        returnValue.put("result", result);
                                        returnValue.put("cas", result.cas());
                                        return returnValue;
                                    }
                                }).onErrorResume(new Function<Throwable, Mono<HashMap<String, Object>>>(){
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

    public List<HashMap<String, Object>> bulkGetSubDocOperation(Collection collection, List<Tuple2<String, List<LookupInSpec>>> keys) {
        final ReactiveCollection reactiveCollection = collection.reactive();
        List<HashMap<String, Object>> returnValue = Flux.fromIterable(keys)
                .flatMap(new Function<Tuple2<String, List<LookupInSpec>>, Publisher<HashMap<String, Object>>>() {
                    public Publisher<HashMap<String, Object>> apply(Tuple2<String, List<LookupInSpec>> subDocOperations) {
                        final String id = subDocOperations.getT1();
                        final List<LookupInSpec> lookUpInSpecs = subDocOperations.getT2();
                        final HashMap<String, Object> retVal = new HashMap<String, Object>();
                        retVal.put("id", id);
                        retVal.put("cas", 0);
                        retVal.put("content", null);
                        retVal.put("error", null);
                        retVal.put("status", false);
                        return reactiveCollection.lookupIn(id, lookUpInSpecs)
                                .map(new Function<Optional<LookupInResult>, HashMap<String, Object>>() {
                                    public HashMap<String, Object> apply(Optional<LookupInResult> optionalResult) {
                                        if(optionalResult.isPresent()) {
                                            LookupInResult result = optionalResult.get();
                                            retVal.put("cas", result.cas());
                                            List<Object> content = new ArrayList<Object>();
                                            for (int i=0; i<lookUpInSpecs.size(); i++) {
                                                try {
                                                		content.add(result.contentAsObject(i));
                                                } catch (DecodingFailedException e1) {
                                                		try {
                                                			content.add(result.contentAsArray(i));
                                                		} catch (DecodingFailedException e2) {
                                                			try {
                                                				content.add(result.contentAs(i, Integer.class));
                                                			}
                                                			catch (DecodingFailedException e3) {
                                                				try {
                                                					content.add(result.contentAs(i, String.class));
                                                				} catch (Exception e4) {
                                                					content.add(null);
                                                				}
                                                			}
                                                		}
                                                } catch (PathNotFoundException e1) {
                                                		content.add("PATH_NOT_FOUND");
                                                }
                                            }
                                            retVal.put("status", true);
                                            retVal.put("content", content);
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
}
