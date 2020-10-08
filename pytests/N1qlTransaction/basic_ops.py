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
        bucket_col = self.get_collections()
        stmt = self.get_stmt(bucket_col)
        query_params = self.create_txn()
        collection_savepoint, savepoints, queries = \
            self.full_execute_query(stmt, self.commit, query_params,
                                    self.rollback_to_savepoint)
        doc_gen_list = self.get_doc_gen_list(bucket_col)
        if isinstance(collection_savepoint, dict):
            results = [[collection_savepoint, savepoints]]
            self.log.info("queries ran are %s" % queries)
            self.process_value_for_verification(bucket_col, doc_gen_list, results)
        else:
            self.fail(collection_savepoint)

    def test_concurrent_txn(self):
        collections = self.get_collections()
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

    def test_with_use_keys(self):
        collections = self.get_collections()
        doc_type_list = self.get_doc_type_collection()
        doc_gen_list = self.get_doc_gen_list(collections, True)
        modify_stmt = []
        for bucket_col in collections:
            self.get_random_number_stmt(self.num_stmt_txn)
            stmts = self.clause.get_where_clause(
                doc_type_list[bucket_col], bucket_col,
                self.num_insert, self.num_update, self.num_delete)
            self.process_index_to_create(stmts, bucket_col)
            modify_stmt.extend(stmts)
        stmts = []
        for stmt in modify_stmt:
            clause = stmt.split(":")
            if clause[1] == "UPDATE" or clause[1] == "DELETE":
                keys_list = random.sample(doc_gen_list[clause[0]], 20)
                stmt = stmt + ": " + str(keys_list)
                stmts.append(stmt)
            else:
                stmts.append(stmt)
        doc_gen_list = self.get_doc_gen_list(collections)
        stmts = self.add_savepoints(stmts)
        query_params = self.create_txn()
        collection_savepoint, savepoints, queries = \
            self.full_execute_query(stmts, self.commit,
                                    query_params, False)

        if isinstance(collection_savepoint, dict):
            results = [[collection_savepoint, savepoints]]
            self.log.info("queries ran are %s" % queries)
            self.process_value_for_verification(collections,
                                doc_gen_list, results)
        else:
            self.fail(collection_savepoint)
