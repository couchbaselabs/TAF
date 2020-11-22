import random
from threading import Thread

from pytests.N1qlTransaction.N1qlBase import N1qlBase
"""
Basic test cases with commit,rollback scenarios
"""

class N1ql_txn_negative(N1qlBase):
    def setUp(self):
        super(N1ql_txn_negative, self).setUp()
        self.bucket = self.bucket_util.buckets[0]

    def tearDown(self):
        super(N1ql_txn_negative, self).tearDown()

    def test_negative_txn(self):
        keyspace = self.n1ql_helper.get_collections()
        queries = []
        self.n1ql_helper.create_index(keyspace[0])
        query_params = self.n1ql_helper.create_txn()
        stmt = "DROP INDEX a1 on `%s` USING GSI" % self.bucket.name
        queries.append(stmt)
        stmt = "BUILD INDEX ON `%s` (`a1`) USING GSI" % self.bucket.name
        queries.append(stmt)
        stmt = "ALTER INDEX `%s`.idx1" % self.bucket.name
        queries.append(stmt)
        stmt = "create scope default:`%s`.%s" % (self.bucket.name, "a1")
        queries.append(stmt)
        stmt = "delete scope default:`%s`.%s" % (self.bucket.name, "a1")
        queries.append(stmt)
        stmt = "create collection default:`%s`.%s.%s" % (self.bucket.name,
                                                       "a1", "a2")
        queries.append(stmt)
        stmt = "delete collection default:`%s`.%s.%s" % (self.bucket.name,
                                                       "a1", "a2")
        queries.append(stmt)
        stmt = "CREATE FUNCTION celsius() " \
               "LANGUAGE INLINE AS (args[0] - 32) * 5/9"
        queries.append(stmt)
        stmt = "EXECUTE FUNCTION celsius(100)"
        queries.append(stmt)
        stmt = "DROP FUNCTION celsius"
        queries.append(stmt)
        stmt = "UPDATE STATISTICS for `%s`(name)" % self.bucket.name
        queries.append(stmt)
        stmt = "INFER `%s` WITH " % self.bucket.name + \
            "{'sample_size':10000,'num_sample_values':1," \
            " 'similarity_metric':0.0}"
        queries.append(stmt)
        stmt = "ADVISE SELECT * FROM `%s` a " \
               "WHERE a.type='hotel' AND a.country='France" % self.bucket.name
        queries.append(stmt)
        for stmt in queries:
            try:
                result = self.n1ql_helper.run_cbq_query(stmt, query_params=query_params)
                if result:
                    self.fail("stmt should fail %s" % stmt)
            except:
                pass
        self.n1ql_helper.end_txn(query_params, self.commit)
        self.process_value_for_verification_with_savepoint(bucket_col,
                                                           doc_gen_list,
                                                           "")

    def recreate_collection(self, doc_gen, bucket, scope, collection):
        self.sleep(10) # wait for queries to execute
        self.bucket_util.drop_collection(self.cluster.master,
                                           bucket,
                                           scope,
                                           collection)
        col_obj = \
            self.bucket.scopes[scope].collections[collection]
        self.bucket_util.create_collection(self.cluster.master,
                                           bucket, scope,
                                           col_obj.get_dict_object())
        self.sleep(60)
        task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen, "create", 0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            scope=scope,
            collection=collection)
        keyspace = self.get_collection_name(bucket.name, scope, collection)
        self.n1ql_helper.create_index(keyspace)
        self.task_manager.get_task_result(task)

    def execute_query(self, stmts, query_params, sleep=200):
        collection_savepoint, savepoints, queries = \
            self.full_execute_query(stmts, self.commit,
                        query_params, self.rollback_to_savepoint,
                        self.write_conflict, sleep)
        if isinstance(collection_savepoint, dict):
            self.fail("expected transaction to fail")
        else:
            self.log.info(collection_savepoint)

    def test_recreate_collection(self):
        """
        1. choose a collection
        2. start a txn on the collection with huge timeout
        3. recreate the collection with same data
        4. commit the txn and validate data
        # txntimeout = 5 mins
        """
        bucket_col = self.n1ql_helper.get_collections()
        for collection in bucket_col:
             if collection.split('.')[-1] != "_default":
                 break
        doc_gen_list = self.n1ql_helper.get_doc_gen_list(bucket_col)
        stmts = self.n1ql_helper.get_stmt(bucket_col)
        print("stmts are %s"%stmts)
        query_params = self.n1ql_helper.create_txn(5)
        # recreate the collection
        scope_name = collection.split('.')[-2]
        collection_name = collection.split('.')[-1]
        thread1 = Thread(target=self.recreate_collection, args=(
                        doc_gen_list[collection], self.bucket,
                        scope_name, collection_name))
        thread2 = Thread(target=self.execute_query, args=(
                        stmts, query_params))
        thread2.start()
        thread1.start()
        thread2.join()
        thread1.join()
        self.process_value_for_verification(bucket_col,
                                doc_gen_list, results="")

    def test_txn_timeout(self):
        '''
        let the txn timeout and retry the transaction
         and make sure it passes
        '''
        bucket_col = self.n1ql_helper.get_collections()
        stmt = self.n1ql_helper.get_stmt(bucket_col)
        query_params = self.n1ql_helper.create_txn(1)
        self.execute_query(stmt, query_params, 200)
        #retry the transaction
        self.execute_query_and_validate_results(stmt,
                                            bucket_col)

    def test_txn_duplicate_key_error(self):
        collections = self.n1ql_helper.get_collections()
        doc_type_list = self.n1ql_helper.get_doc_type_collection()
        modify_stmt = []
        for bucket_col in collections:
            stmts = self.clause.get_where_clause(
                doc_type_list[bucket_col], bucket_col,
                self.num_insert, 0, 0)
            self.n1ql_helper.process_index_to_create(stmts, bucket_col)
            modify_stmt.extend(stmts)
            modify_stmt.extend(stmts)
        modify_stmt = self.n1ql_helper.add_savepoints(modify_stmt)
        self.execute_query_and_validate_results(modify_stmt,
                                            collections)
