package com.couchbase.test.sdk;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Publisher;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DurabilityAmbiguousException;
import com.couchbase.client.core.error.DurabilityImpossibleException;
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException;
import com.couchbase.client.core.error.DurableWriteInProgressException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.TouchOptions;
import com.couchbase.client.java.kv.UpsertOptions;

import com.couchbase.test.docgen.DocType.Person;
import com.couchbase.test.sdk.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;


public class DocOps {

    public List<Result> bulkInsert(Collection collection, List<Tuple2<String, Object>> documents, InsertOptions insertOptions) {
        ReactiveCollection reactiveCollection = collection.reactive();

        List<Result> out = new ArrayList();
        
        Flux.fromIterable(documents)
                .flatMap(documentToInsert -> {

                  String k = documentToInsert.getT1();
                  Object v = documentToInsert.getT2();

                  return reactiveCollection.insert(k, v, insertOptions)
                          .doOnError(error -> out.add(new Result(k, v, error, false)))
                          .onErrorResume(error -> Mono.empty());
                })
                .blockLast();

        return out;
      }

    public List<Result> bulkUpsert(Collection collection, List<Tuple2<String, Object>> documents,
            UpsertOptions upsertOptions) {
    	ReactiveCollection reactiveCollection = collection.reactive();
    	List<Result> out = new ArrayList();
        
        Flux.fromIterable(documents)
                .flatMap(documentToInsert -> {

                  String k = documentToInsert.getT1();
                  Object v = documentToInsert.getT2();

                  return reactiveCollection.upsert(k, v, upsertOptions)
                          .doOnError(error -> out.add(new Result(k, v, error, false)))
                          .onErrorResume(error -> Mono.empty());
                })
                .blockLast();

        return out;
    }
    
    public List<Tuple2<String, Object>> bulkGets(Collection collection, List<Tuple2<String, Object>> documents, GetOptions getOptions) {
        final ReactiveCollection reactiveCollection = collection.reactive();
        List<Tuple2<String, Object>> returnValue = Flux.fromIterable(documents)
                .flatMap(new Function<Tuple2<String, Object>, Publisher<Tuple2<String, Object>>>() {
                    public Publisher<Tuple2<String, Object>> apply(Tuple2<String, Object> documentToInsert) {
                        final String id = documentToInsert.getT1();
                        final Tuple2<String, Object> returnValue;
                        return reactiveCollection.get(id, getOptions)
                                .map(new Function<GetResult, Tuple2<String, Object>>() {
                                    public Tuple2<String, Object> apply(GetResult result) {
                                        return Tuples.of(id, result.contentAs(Person.class));
                                    }
                                }).onErrorResume(new Function<Throwable, Mono<Tuple2<String, Object>>>() {
                                    public Mono<Tuple2<String, Object>> apply(Throwable error) {
                                        return Mono.just(Tuples.of(id, error.getClass().getName()));
                                    }
                                });
                    }
                }).collectList().block();
        return returnValue;
    }

    public List<Result> bulkDelete(Collection collection, List<String> keys, RemoveOptions removeOptions) {
    	ReactiveCollection reactiveCollection = collection.reactive();

    	List<Result> out = new ArrayList();
        
        Flux.fromIterable(keys)
                .flatMap(key -> {

                  return reactiveCollection.remove(key, removeOptions)
                          .doOnError(error -> out.add(new Result(key, null, error, false)))
                          .onErrorResume(error -> Mono.empty());
                })
                .blockLast();

        return out;
    }
    
    public List<ConcurrentHashMap<String, Object>> bulkReplace(Collection collection, List<Tuple2<String, Object>> documents,
            ReplaceOptions replaceOptions) {
        final ReactiveCollection reactiveCollection = collection.reactive();
        List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(documents)
                .flatMap(new Function<Tuple2<String, Object>, Publisher<ConcurrentHashMap<String, Object>>>() {
                    public Publisher<ConcurrentHashMap<String, Object>> apply(Tuple2<String, Object> documentToInsert) {
                        final String id = documentToInsert.getT1();
                        final Object content = documentToInsert.getT2();
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

    public List<ConcurrentHashMap<String, Object>> bulkTouch(Collection collection, List<String> keys, final int exp,
            TouchOptions touchOptions, Duration exp_duration) {
        final ReactiveCollection reactiveCollection = collection.reactive();
        List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(keys)
                .flatMap(new Function<String, Publisher<ConcurrentHashMap<String, Object>>>() {
                    public Publisher<ConcurrentHashMap<String, Object>> apply(String key){
                        final ConcurrentHashMap<String, Object> returnValue = new ConcurrentHashMap<String, Object>();
                        returnValue.put("id", key);
                        returnValue.put("cas", 0);
                        returnValue.put("error", "");
                        returnValue.put("status", false);
                        return reactiveCollection.touch(key, exp_duration, touchOptions)
                                .map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
                                    public ConcurrentHashMap<String, Object> apply(MutationResult result){
                                        returnValue.put("cas", result.cas());
                                        returnValue.put("status", true);
                                        return returnValue;
                                    }
                                }).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
                                    public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
                                        returnValue.put("error", error);
                                        return Mono.just(returnValue);
                            }
                                }).defaultIfEmpty(returnValue);
                    }
                }).collectList().block();
        return returnValue;
    }

    public MutationResult insert(Tuple2<String, Object> documentToInsert, Collection collection, DurabilityLevel level){
        String k = documentToInsert.getT1();
        Object v = documentToInsert.getT2();
        try{
            MutationResult mutationResult = collection.insert(
                    k,
                    v,
                    InsertOptions.insertOptions()
                    .timeout(Duration.ofSeconds(10))
                    .durability(level)
                    );
            return mutationResult;
        }
        catch(DocumentExistsException|DurabilityImpossibleException|
                DurabilityLevelNotAvailableException|DurableWriteInProgressException|DurabilityAmbiguousException e){
            throw(e);
        }
    }

    public MutationResult upsert(String id, Person person, Collection collection, DurabilityLevel level){
        try{
            MutationResult mutationResult = collection.upsert(
                    id,
                    person,
                    UpsertOptions.upsertOptions()
                    .timeout(Duration.ofMinutes(1))
                    .expiry(Duration.ofDays(1))
                    .durability(level)
                    );
            return mutationResult;
        }
        catch(TimeoutException|DocumentExistsException|DurabilityImpossibleException|
                DurabilityLevelNotAvailableException|DurableWriteInProgressException|DurabilityAmbiguousException e){
            throw(e);
        }
    }

    public MutationResult replace(String id, Person person, Collection collection, DurabilityLevel level, long cas) {
        try {
                MutationResult mutationResult = collection.replace(
                        id,
                        person,
                        ReplaceOptions.replaceOptions()
                        .timeout(Duration.ofMinutes(1))
                        .durability(level)
                        .cas(cas));
                return mutationResult;
        }
        catch(DocumentExistsException|DurabilityImpossibleException|
                DurabilityLevelNotAvailableException|DurableWriteInProgressException|DurabilityAmbiguousException e){
            throw(e);
        }
    }

    public MutationResult delete(String id, Collection collection, DurabilityLevel level){
        try{
            MutationResult mutationResult = collection.remove(
                    id, 
                    RemoveOptions.removeOptions()
                    .durability(level)
                    .timeout(Duration.ofSeconds(10)));
            return mutationResult;
        }
        catch(DocumentExistsException|DurabilityImpossibleException|
                DurabilityLevelNotAvailableException|DurableWriteInProgressException|DurabilityAmbiguousException e){
            throw(e);
        }
    }

    public Person read(String id, Collection collection){
        GetResult getResult = collection.get(id, GetOptions.getOptions().timeout(Duration.ofSeconds(10)));
        Person p = null;
        p = getResult.contentAs(Person.class);
        return p;
    }

}
