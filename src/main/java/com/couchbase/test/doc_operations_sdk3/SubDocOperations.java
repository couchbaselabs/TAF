package com.couchbase.test.doc_operations_sdk3;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
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

    public LookupInSpec getLookUpInSpec(String path, Boolean xattr) {
        if (xattr) {
            return LookupInSpec.get(path).xattr();
        }
        else {
            return LookupInSpec.get(path);
        }
    }

    public List<HashMap<String, Object>> bulkSubDocOperation(Collection collection,
            List<Tuple2<String, List<MutateInSpec>>> mutateInSpecs, MutateInOptions t_mutateInOptions) {
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
                        returnValue.put("result", null);
                        return reactiveCollection.mutateIn(id, subDocOps, t_mutateInOptions)
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
                                .map(new Function<LookupInResult, HashMap<String, Object>>() {
                                    public HashMap<String, Object> apply(LookupInResult optionalResult) {
                                            retVal.put("cas", optionalResult.cas());
                                            List<Object> content = new ArrayList<Object>();
                                            for (int i=0; i<lookUpInSpecs.size(); i++) {
                                                try {
                                                		content.add(optionalResult.contentAsObject(i));
                                                } catch (DecodingFailureException e1) {
                                                		try {
                                                			content.add(optionalResult.contentAsArray(i));
                                                		} catch (DecodingFailureException e2) {
                                                			try {
                                                				content.add(optionalResult.contentAs(i, Integer.class));
                                                			}
                                                			catch (DecodingFailureException e3) {
                                                				try {
                                                					content.add(optionalResult.contentAs(i, String.class));
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
