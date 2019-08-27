

package com.couchbase.test.transactions;

import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.transactions.log.LogDefer;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;


import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.transactions.AttemptContextReactive;
import com.couchbase.transactions.TransactionDurabilityLevel;
import com.couchbase.transactions.TransactionJsonDocument;
import com.couchbase.transactions.TransactionResult;
import com.couchbase.transactions.Transactions;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.transactions.log.TransactionEvent;
import com.couchbase.transactions.util.TestAttemptContextFactory;
import com.couchbase.transactions.util.TransactionMock;

public class SimpleTransaction {


	public Transactions createTansaction(Cluster cluster, TransactionConfig config) {
		Event.Severity logLevel = Event.Severity.INFO;
		cluster.environment().eventBus().subscribe(event -> {
		    if (event instanceof TransactionEvent) {
		        TransactionEvent te = (TransactionEvent) event;
		        if (te.severity().ordinal() >= logLevel.ordinal()) {
		            System.out.println(te.getClass().getSimpleName() + ": " + event.description());
		            
		            if (te.hasLogs()) {
		                te.logs().forEach(log -> {
		                    System.out.println(te.getClass().getSimpleName() + " log: " + log.toString());
		                });
		            }
		        }
		    }
		});
		return Transactions.create(cluster, config);
	}
	Queue<String> queue=new LinkedList<>();

	public TransactionConfig createTransactionConfig(int expiryTimeout, int changedurability) {
		TransactionConfigBuilder config = TransactionConfigBuilder.create().logDirectlyCleanup(Event.Severity.VERBOSE);
		if (changedurability > 0) {
			switch (changedurability) {
				case 1:
					config.durabilityLevel(TransactionDurabilityLevel.MAJORITY);
					break;
				case 2:
					config.durabilityLevel(TransactionDurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER);
					break;
				case 3:
					config.durabilityLevel(TransactionDurabilityLevel.PERSIST_TO_MAJORITY);
					break;
				case 4:
					config.durabilityLevel(TransactionDurabilityLevel.NONE);
					break;
				default:
					config.durabilityLevel(TransactionDurabilityLevel.NONE);
			}
		}

		return config.expirationTime(Duration.of(expiryTimeout, ChronoUnit.SECONDS)).build();
	}
	public List<Tuple2<String, JsonObject>> ReadTransaction(Transactions transaction, List<Collection> collections, List<String> Readkeys) {
		List<Tuple2<String, JsonObject>> res = null;
		try {

			TransactionResult result = transaction.run(ctx -> {
				for (String key: Readkeys) {
					for (Collection bucket:collections) {
						if (ctx.get(bucket, key).isPresent()) {
							TransactionJsonDocument doc1=ctx.get(bucket, key).get();
							JsonObject content = doc1.contentAs(JsonObject.class);
							Tuple2<String, JsonObject>mp = Tuples.of(key, content);
							res.add(mp);
						}
					}
				}

			});

		}
		catch (TransactionFailed err) {
			// This per-txn log allows the app to only log failures
			System.out.println("Transaction failed from runTransaction");
			err.result().log().logs().forEach(System.err::println);
		}
		return res;
	}

	public ArrayList<LogDefer> MockRunTransaction(Cluster cluster, TransactionConfig config, Collection collection, List<Tuple2<String, 
		JsonObject>> Createkeys, Boolean commit, String operation)
	{
		AtomicInteger attempt = new AtomicInteger(0);
		AtomicBoolean first = new AtomicBoolean(true);
		ArrayList<LogDefer> res = null;

		try (Transactions transactions = Transactions.create(cluster, config)) {

		    TransactionMock mock = new TransactionMock();
		    TestAttemptContextFactory factory = new TestAttemptContextFactory(mock);
	    	transactions.reactive().setAttemptContextFactory(factory);

		    if (operation.equals("beforeAtrPending")) {
			    mock.beforeAtrPending = (ctx) -> {
			       if (attempt.get() == 1) return Mono.error(new TemporaryFailureException());
			       else return Mono.just(1);
			    };
		    }

		    if (operation.equals("afterAtrPending")) {
			    mock.afterAtrPending = (ctx) -> {
			       if (attempt.get() == 1) return Mono.error(new TemporaryFailureException());
			       else return Mono.just(1);
			    };
		    }

		    if (operation.equals("beforeAtrComplete")) {
			    mock.beforeAtrComplete = (ctx) -> {
			       if (attempt.get() == 1 && first.get()) {
			    	   first.set(false);
			    	   return Mono.error(new TemporaryFailureException());
			       }
			       else return Mono.just(1);
			    };

		    }

		    if (operation.equals("beforeAtrRolledBack")) {
			    mock.beforeAtrRolledBack = (ctx) -> {
			       if (attempt.get() == 1&& first.get()) {
			    	   first.set(false);
			    	   return Mono.error(new TemporaryFailureException());
			       }
			       else return Mono.just(1);
			    };
		    }

		    if (operation.equals("afterAtrCommit")) {
			    mock.afterAtrCommit = (ctx) -> {
			       if (attempt.get() == 1&& first.get()) {
			    	   first.set(false);
			    	   return Mono.error(new TemporaryFailureException());
			       }
			       else return Mono.just(1);
			    };
		    }

		    if (operation.equals("afterAtrComplete")) {
			    mock.afterAtrComplete = (ctx) -> {
			       if (attempt.get() == 1&& first.get()) {
			    	   first.set(false);
			    	   return Mono.error(new TemporaryFailureException());
			       }
			       else return Mono.just(1);
			    };
		    }

//		    if (operation.equals("afterAtrRolledBack")) {
//			    mock.afterAtrRolledBack = (ctx) -> {
//			       if (attempt.get() == 1&& first.get()) {
//			    	   first.set(false);
//			    	   return Mono.error(new TemporaryFailureException());
//			       }
//			       else return Mono.just(1);
//			    };
//		    }

		    TransactionResult result = transactions.run((ctx1) -> {
		        attempt.set(attempt.get() + 1);

		        for (Tuple2<String, JsonObject> document : Createkeys) {
					TransactionJsonDocument doc=ctx1.insert(collection, document.getT1(), document.getT2());
					}

		        if (commit) ctx1.commit();
			    else ctx1.rollback();

		    });

		    result.log().logs().forEach(System.err::println);
		    return res;		}
			catch (TransactionFailed err) {
				// This per-txn log allows the app to only log failures
				System.out.println("Transaction failed from runTransaction");
				err.result().log().logs().forEach(System.err::println);
				res = err.result().log().logs();
				return res ;
			}


	}

	public ArrayList<LogDefer> MockRunTransaction(Cluster cluster, TransactionConfig config, Collection collection, List<Tuple2<String, 
			JsonObject>> Createkeys, List<String> Updatekeys, List<String> Deletekeys, Boolean commit, String operation, String docId) 
		{
		ArrayList<LogDefer> res = null;
			try (Transactions transactions = Transactions.create(cluster, config)) {

				AtomicBoolean first = new AtomicBoolean(true);
			    TransactionMock mock = new TransactionMock();
			    TestAttemptContextFactory factory = new TestAttemptContextFactory(mock);
		    	transactions.reactive().setAttemptContextFactory(factory);

			    if (operation.equals("afterStagedInsertComplete")) {
                    mock.afterStagedInsertComplete = (ctx, id) -> {
                    if (first.get() && id.equals(docId))  {
                       first.set(false);
                       return Mono.error(new TemporaryFailureException());}
                    else return Mono.just(1);
                    };
                }

			    if (operation.equals("afterStagedReplaceComplete")) {
                    mock.afterStagedReplaceComplete = (ctx, id) -> {
                    if (first.get() && id.equals(docId))  {
                       first.set(false);
                       return Mono.error(new TemporaryFailureException());}
                    else return Mono.just(1);
                    };
                }

			    if (operation.equals("afterStagedRemoveComplete")) {
                    mock.afterStagedRemoveComplete = (ctx, id) -> {
                    if (first.get() && id.equals(docId))  {
                       first.set(false);
                       return Mono.error(new TemporaryFailureException());}
                    else return Mono.just(1);
                    };
                }

			    if (operation.equals("afterDocCommitted")) {
                    mock.afterDocCommitted = (ctx, id) -> {
                    if (first.get() && id.equals(docId))  {
                       first.set(false);
                       return Mono.error(new TemporaryFailureException());}
                    else return Mono.just(1);
                    };
                }

			    if (operation.equals("afterGetComplete")) {
                    mock.afterGetComplete = (ctx, id) -> {
                    if (first.get() && id.equals(docId))  {
                       first.set(false);
                       return Mono.error(new TemporaryFailureException());}
                    else return Mono.just(1);
                    };
                }

			    if (operation.equals("beforeDocCommitted")) {
                    mock.beforeDocCommitted = (ctx, id) -> {
                    if (first.get() && id.equals(docId))  {
                       first.set(false);
                       return Mono.error(new TemporaryFailureException());}
                    else return Mono.just(1);
                    };
                }

			    if (operation.equals("beforeStagedInsert")) {
                    mock.beforeStagedInsert = (ctx, id) -> {
                    if (first.get() && id.equals(docId))  {
                       first.set(false);
                       return Mono.error(new TemporaryFailureException());}
                    else return Mono.just(1);
                    };
                }

			    if (operation.equals("beforeStagedReplace")) {
                    mock.beforeStagedReplace = (ctx, id) -> {
                    if (first.get() && id.equals(docId))  {
                       first.set(false);
                       return Mono.error(new TemporaryFailureException());}
                    else return Mono.just(1);
                    };
                }

			    if (operation.equals("beforeStagedRemove")) {
                    mock.beforeStagedRemove = (ctx, id) -> {
                    if (first.get() && id.equals(docId))  {
                       first.set(false);
                       return Mono.error(new TemporaryFailureException());}
                    else return Mono.just(1);
                    };
                }

			    if (operation.equals("beforeDocRemoved")) {
                    mock.beforeDocRemoved = (ctx, id) -> {
                    if (first.get() && id.equals(docId))  {
                       first.set(false);
                       return Mono.error(new TemporaryFailureException());}
                    else return Mono.just(1);
                    };
                }

			    if (operation.equals("beforeDocRolledBack")) {
                    mock.beforeDocRolledBack = (ctx, id) -> {
                    if (first.get() && id.equals(docId))  {
                       first.set(false);
                       return Mono.error(new TemporaryFailureException());}
                    else return Mono.just(1);
                    };
                }

			    TransactionResult result = transactions.run((ctx1) -> {

			        for (Tuple2<String, JsonObject> document : Createkeys) {
						TransactionJsonDocument doc=ctx1.insert(collection, document.getT1(), document.getT2());
						 }

			        for (String key: Updatekeys) {
				        if (ctx1.get(collection, key).isPresent()) {
							TransactionJsonDocument doc2=ctx1.get(collection, key).get();
							JsonObject content = doc2.contentAs(JsonObject.class);
							content.put("mutated", 1 );
							ctx1.replace(doc2, content);
							}
						}

			        for (String key: Deletekeys) {
						if (ctx1.get(collection, key).isPresent()) {
							TransactionJsonDocument doc1=ctx1.get(collection, key).get();
							ctx1.remove(doc1);
						}

					}

			    });
			    result.log().logs().forEach(System.err::println);
			    return res;		}
			catch (TransactionFailed err) {
				// This per-txn log allows the app to only log failures
				System.out.println("Transaction failed from runTransaction");
				err.result().log().logs().forEach(System.err::println);
				res = err.result().log().logs();
				return res ;
			}
		}


	public ArrayList<LogDefer> RunTransaction(Transactions transaction, List<Collection> collections, List<Tuple2<String, JsonObject>> Createkeys, List<String> Updatekeys,
											  List<String> Deletekeys, Boolean commit, boolean sync, int updatecount) {
		ArrayList<LogDefer> res = null;
//		synchronous API - transactions
		if (sync) {
			try {

				TransactionResult result = transaction.run(ctx -> {
					//				creation of docs

					for (Collection bucket:collections) {
						for (Tuple2<String, JsonObject> document : Createkeys) {
							TransactionJsonDocument doc=ctx.insert(bucket, document.getT1(), document.getT2());
							TransactionJsonDocument doc1=ctx.get(bucket, document.getT1()).get();
//							JsonObject content = doc1.contentAs(JsonObject.class);
//							if (areEqual(content,document.getT2()));
//							if (content.equals(document.getT2()));
//							else {System.out.println("Document not matched");}
						}

					}
					//				update of docs
					for (String key: Updatekeys) {
						for (Collection bucket:collections) {
							if (ctx.get(bucket, key).isPresent()) {
								TransactionJsonDocument doc2=ctx.get(bucket, key).get();
								for (int i=1; i<=updatecount; i++) {
									JsonObject content = doc2.contentAs(JsonObject.class);
									content.put("mutated", i );
									ctx.replace(doc2, content);
//										TransactionJsonDocument doc1=ctx.get(bucket, key).get();
//										JsonObject read_content = doc1.contentAs(JsonObject.class);

								}
							}

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

//					transaction.close();


				});
				result.log().logs().forEach(System.err::println);

			}
			catch (TransactionFailed err) {
				// This per-txn log allows the app to only log failures
				System.out.println("Transaction failed from runTransaction");
				err.result().log().logs().forEach(System.err::println);
				res = err.result().log().logs();
			}

		}
		else {
			for (Collection collection:collections) {
				if (Createkeys.size() > 0) {
					res = multiInsertSingelTransaction(transaction, collection, Createkeys, commit); }
				if (Updatekeys.size() > 0) {
					multiUpdateSingelTransaction(transaction, collection, Updatekeys, commit);}
				if (Deletekeys.size() > 0) {
					multiDeleteSingelTransaction(transaction, collection, Deletekeys, commit);}
			}

		}
		return res;
	}

	public ArrayList<LogDefer>  multiInsertSingelTransaction(Transactions transaction, Collection collection, List<Tuple2<String, JsonObject>> createkeys, Boolean commit)
	{
		ArrayList<LogDefer> res = null;
		Tuple2<String, JsonObject> firstDoc = createkeys.get(0);
		List<Tuple2<String, JsonObject>> remainingDocs = createkeys.stream().skip(1).collect(Collectors.toList());
		ReactiveCollection rc = collection.reactive();

		TransactionResult result = transaction.reactive((ctx) -> {

			if (commit)
			{
				// The first mutation must be done in serial
				if (remainingDocs.size() == 1) {
					return ctx.insert(rc, firstDoc.getT1(), firstDoc.getT2()).then();
				}
				else {
					return ctx.insert(rc, firstDoc.getT1(), firstDoc.getT2())
							.flatMapMany(v -> Flux.fromIterable(remainingDocs)
											.flatMap(doc -> ctx.insert(rc, doc.getT1(), doc.getT2()),
													// Do all these inserts in parallel
													remainingDocs.size()
											)
	
									// There's an implicit commit so no need to call ctx.commit().  The .then()
									// converts to the
									// expected type
						).then();}
			}
			else
			{
				if (remainingDocs.size() == 1) {
					return ctx.insert(rc, firstDoc.getT1(), firstDoc.getT2()).then(ctx.rollback());
				}
				else {
				// The first mutation must be done in serial
				return ctx.insert(rc, firstDoc.getT1(), firstDoc.getT2())
						.flatMapMany(v -> Flux.fromIterable(remainingDocs)
										.flatMap(doc -> ctx.insert(rc, doc.getT1(), doc.getT2()),
												// Do all these inserts in parallel
												remainingDocs.size()
										)

								// There's an implicit commit so no need to call ctx.commit().  The .then()
								// converts to the
								// expected type
						).then(ctx.rollback());}
			}

		}).doOnError(err -> {

			for (LogDefer e : ((TransactionFailed) err).result().log().logs()) {
				// Optionally, log the result to your own logger
				res.add(e);
			}
		}).block();
		return res;
	}


	public ArrayList<LogDefer> multiUpdateSingelTransaction(Transactions transaction, Collection collection, List<String> ids, Boolean commit) {
		ArrayList<LogDefer> res = null;
		try {
			TransactionResult result = transaction.reactive((ctx) -> {
				return updateMulti(ctx, transaction, collection, ids, commit);
			}).block();
			//System.out.println("result: "+result.log().logs());
		} catch (TransactionFailed e) {
			System.out.println("Transaction " + e.result().transactionId() + " failed:");
			System.out.println("Error: " + e.result());
			for (LogDefer err : e.result().log().logs()) {
				res.add(err);
				if (err != null)
					System.out.println(err.toString());
			}
		}
		return res;
	}

	public Mono<Void> updateMulti(AttemptContextReactive acr, Transactions transaction, Collection collection, List<String> ids, Boolean commit){
		ReactiveCollection reactiveCollection=collection.reactive();
		List<String> docToUpdate=ids.parallelStream().collect(Collectors.toList());
		String id1 = docToUpdate.get(0);
		List<String> remainingDocs = docToUpdate.stream().skip(1).collect(Collectors.toList());
		//System.out.println("docs to be updated"+docToUpdate);
		if (commit) {
			if (remainingDocs.size() == 1) {
				return acr.getOrError(reactiveCollection, id1).flatMap(doc-> acr.replace(doc, doc.contentAs(JsonObject.class).put("mutated", 1))).then();
			}
			else {
				return acr.getOrError(reactiveCollection, id1).flatMap(doc-> acr.replace(doc, doc.contentAs(JsonObject.class).put("mutated", 1))).flatMapMany(
					v-> Flux.fromIterable(remainingDocs).flatMap(d -> acr.getOrError(reactiveCollection,d).flatMap(d1-> acr.replace(d1, d1.contentAs(JsonObject.class).put("mutated", 1))),
							remainingDocs.size())).then();}
			}
		else {
			if (remainingDocs.size() == 1) {
				return acr.getOrError(reactiveCollection, id1).flatMap(doc-> acr.replace(doc, doc.contentAs(JsonObject.class).put("mutated", 1))).then(acr.rollback());
			}
			else {
				return acr.getOrError(reactiveCollection, id1).flatMap(doc-> acr.replace(doc, doc.contentAs(JsonObject.class).put("mutated", 1))).flatMapMany(
						v-> Flux.fromIterable(remainingDocs).flatMap(d -> acr.getOrError(reactiveCollection,d).flatMap(d1-> acr.replace(d1, d1.contentAs(JsonObject.class).put("mutated", 1))),
								remainingDocs.size())).then(acr.rollback());
			}
			
		}
	}


	public void multiDeleteSingelTransaction(Transactions transaction, Collection collection, List<String> ids, Boolean commit) {
		try {
			TransactionResult result = transaction.reactive((ctx) -> {
				return deleteMulti(ctx, transaction, collection, ids, commit);
			}).block();
//	            System.out.println("result: ");
//	            for (LogDefer err : result.log().logs()) {
//	                if (err != null)
//	                    System.out.println(err.toString());
//	            }
		} catch (TransactionFailed e) {
			System.out.println("Transaction " + e.result().transactionId() + " failed:");
			System.out.println("Error: " + e.result());
			for (LogDefer err : e.result().log().logs()) {
				if (err != null)
					System.out.println(err.toString());
			}
		}
	}

	public Mono<Void> deleteMulti(AttemptContextReactive acr, Transactions transaction, Collection collection, List<String> ids, Boolean commit){
		ReactiveCollection reactiveCollection=collection.reactive();
		List<String> docToDelete=ids.parallelStream().collect(Collectors.toList());
		String id1 = docToDelete.get(0);
		List<String> remainingDocs = docToDelete.stream().skip(1).collect(Collectors.toList());
		//System.out.println("docs to be deleted"+docToDelete);
		if (commit) {
			if (remainingDocs.size() == 1) {
				return acr.getOrError(reactiveCollection, id1).flatMap(doc-> acr.remove(doc)).then();
			}
			else {
				return acr.getOrError(reactiveCollection, id1).flatMap(doc-> acr.remove(doc)).thenMany(
					Flux.fromIterable(remainingDocs).flatMap(d -> acr.getOrError(reactiveCollection,d).flatMap(d1-> acr.remove(d1)),
							remainingDocs.size())).then();}
		}
		else {
			if (remainingDocs.size() == 1) {
				return acr.getOrError(reactiveCollection, id1).flatMap(doc-> acr.remove(doc)).then(acr.rollback());
			}
			else {
				return acr.getOrError(reactiveCollection, id1).flatMap(doc-> acr.remove(doc)).thenMany(
						Flux.fromIterable(remainingDocs).flatMap(d -> acr.getOrError(reactiveCollection,d).flatMap(d1-> acr.remove(d1)),
								remainingDocs.size())).then(acr.rollback());
			}
			
		}
	}


	public List<String> getQueue(int n){
		return this.queue.stream().skip(queue.size() - n).collect(Collectors.toList());
	}


}


