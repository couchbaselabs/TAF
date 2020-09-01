import random

from pytests.N1qlTransaction.N1qlBase import N1qlBase
"""
Basic test cases with commit,rollback scenarios
"""


class BasicOps(N1qlBase):
    def setUp(self):
        super(BasicOps, self).setUp()
        self.bucket = self.bucket_util.buckets[0]

    def tearDown(self):
        super(BasicOps, self).tearDown()

    def test_n1ql_query(self):
        """
        1. create an Index
        2. load to the bucket and collections
        3. run a simple query
        """
        results = ""
        bucket_col = self.create_primary_index()
        query_params = self.create_txn()
        stmt = self.get_stmt(bucket_col)
        collection_savepoint, savepoints, queries = \
            self.full_execute_query(stmt, self.commit, query_params,
                                    self.rollback_to_savepoint)
        doc_gen_list = self.get_doc_gen_list(bucket_col)
        if collection_savepoint != "txn failed":
            results = [[collection_savepoint, savepoints]]
        self.log.info("queries ran are %s" % queries)
        self.process_value_for_verification(bucket_col, doc_gen_list, results)

    def test_concurrent_txn(self):
        collections = self.create_primary_index()
        doc_type_list = self.get_doc_type_collection()
        doc_gen_list = self.get_doc_gen_list(collections)
        results, fail = self.get_stmt_for_threads(
            collections,
            doc_type_list,
            self.num_commit,
            self.num_rollback_to_savepoint,
            self.num_conflict)
        self.log.info("result is %s" % results)
        self.process_value_for_verification(collections, doc_gen_list, results)
        if fail:
            self.fail("One of the thread failed with unexpected errors")

    def test_negative_txn(self):
        self.create_primary_index()
        queries = []
        query_params = self.create_txn()
        stmt = "CREATE PRIMARY INDEX a1 on %s USING GSI" % self.bucket.name
        queries.append(stmt)
        stmt = "DROP INDEX a1 on %s USING GSI" % self.bucket.name
        queries.append(stmt)
        stmt = "BUILD INDEX ON `%s` (`a1`) USING GSI" % self.bucket.name
        queries.append(stmt)
        stmt = "ALTER INDEX `%s`.idx1" % self.bucket.name
        queries.append(stmt)
        stmt = "create scope default:%s.%s" % (self.bucket.name, "a1")
        queries.append(stmt)
        stmt = "delete scope default:%s.%s" % (self.bucket.name, "a1")
        queries.append(stmt)
        stmt = "create collection default:%s.%s.%s" % (self.bucket.name,
                                                       "a1", "a2")
        queries.append(stmt)
        stmt = "delete collection default:%s.%s.%s" % (self.bucket.name,
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
            result = self.run_cbq_query(stmt, query_params=query_params)
            if isinstance(result, str) or 'errors' in result:
                self.log.info("failed as expected")
            else:
                self.fail("stmt failed %s" % stmt)
        self.end_txn(query_params, self.commit)
        self.process_value_for_verification_with_savepoint(bucket_col,
                                                           doc_gen_list, 
                                                           "")

    def test_with_use_keys(self):
        collections = self.create_primary_index()
        stmts = []
        query_params = self.create_txn()
        doc_type_list = self.get_doc_type_collection()
        doc_gen_list = self.get_doc_gen_list(collections, True)
        modify_stmt = []
        for bucket_col in collections:
            modify_stmt.extend(self.clause.get_where_clause(
                doc_type_list[bucket_col], bucket_col, 1, 4, 2))
        for stmt in modify_stmt:
            clause = stmt.split(":")
            if clause[1] == "UPDATE" or clause[1] == "DELETE":
                keys_list = random.sample(doc_gen_list[clause[0]], 20)
                stmt = stmt + ": " + str(keys_list)
                stmts.append(stmt)
            else:
                stmts.append(stmt)

        stmts = self.add_savepoints(stmts)
        collection_savepoint, savepoints, queries = \
            self.full_execute_query(stmts, self.commit,
                                    query_params, False)

        self.log.info("executed queries are %s" % queries)
