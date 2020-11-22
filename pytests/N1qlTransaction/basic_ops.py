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
        bucket_col = self.n1ql_helper.get_collections()
        stmt = self.n1ql_helper.get_stmt(bucket_col)
        self.execute_query_and_validate_results(stmt,
                             bucket_col)

    def test_concurrent_txn(self):
        collections = self.n1ql_helper.get_collections()
        doc_type_list = self.n1ql_helper.get_doc_type_collection()
        doc_gen_list = self.n1ql_helper.get_doc_gen_list(collections)
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
        collections = self.n1ql_helper.get_collections()
        doc_type_list = self.n1ql_helper.get_doc_type_collection()
        doc_gen_list = self.n1ql_helper.get_doc_gen_list(collections, True)
        modify_stmt = []
        for bucket_col in collections:
            self.n1ql_helper.get_random_number_stmt(self.num_stmt_txn)
            stmts = self.clause.get_where_clause(
                doc_type_list[bucket_col], bucket_col,
                self.num_insert, self.num_update, self.num_delete)
            self.n1ql_helper.process_index_to_create(stmts, bucket_col)
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
        doc_gen_list = self.n1ql_helper.get_doc_gen_list(collections)
        stmts = self.n1ql_helper.add_savepoints(stmts)
        self.execute_query_and_validate_results(stmts,
                                            collections)

    def test_txn_same_collection_diff_scope(self):
        '''
        get 2 scopes and create same collection on both scopes,
        run quries and validate data
        execute test with single and multi bucket
        '''
        bucket_collections = []
        doc_gen_list = {}
        doc_gen = self.n1ql_helper.gen_docs("test_collections", 0,
                                          2000, type="employee")
        scope_considered = \
            self.bucket_util.get_random_scopes(self.buckets,
                                          2, "all")
        collection = self.bucket_util.get_random_name()
        for bucket_name, scope_dict in scope_considered.items():
            bucket = self.bucket_util.get_bucket_obj(self.bucket_util.buckets,
                                                bucket_name)
            for scope in scope_dict["scopes"].keys():
                self.bucket_util.create_collection(self.cluster.master,
                                              bucket,
                                              scope,
                                              {"name": collection})
                name = self.n1ql_helper.get_collection_name(
                    bucket_name, scope, collection)
                bucket_collections.append(name)
                doc_gen_list[name] = doc_gen
                task = self.task.async_load_gen_docs(
                    self.cluster, bucket, doc_gen, "create", 0,
                    batch_size=10, process_concurrency=8,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    durability=self.durability_level,
                    compression=self.sdk_compression,
                    timeout_secs=self.sdk_timeout,
                    scope=scope,
                    collection=collection)
                self.task_manager.get_task_result(task)
        stmts = self.n1ql_helper.get_stmt(bucket_collections)
        self.execute_query_and_validate_results(stmts,
                                            bucket_collections)

    def text_txn_same_collection_diff_bucket(self):
        '''
        create same scope and collection on different buckets
        execute query and validate data
        '''
        bucket_collections = []
        doc_gen_list = {}
        doc_gen = self.n1ql_helper.gen_docs("test_collections", 0,
                                          2000, type="employee")
        scope = self.bucket_util.get_random_name()
        collection = self.bucket_util.get_random_name()
        for bucket in self.bucket_util.buckets:
            self.bucket_util.create_scope(self.cluster.master,
                                      bucket,
                                      {"name": scope})
            self.bucket_util.create_collection(self.cluster.master,
                                              bucket,
                                              scope,
                                              {"name": collection})
            name = self.n1ql_helper.get_collection_name(
                bucket.name, scope, collection)
            bucket_collections.append(name)
            doc_gen_list[name] = doc_gen
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, doc_gen, "create", 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                compression=self.sdk_compression,
                timeout_secs=self.sdk_timeout,
                scope=scope,
                collection=collection)
            self.task_manager.get_task_result(task)
        stmts = self.n1ql_helper.get_stmt(bucket_collections)
        self.execute_query_and_validate_results(stmts,
                                            bucket_collections)

    def test_txn_same_keys(self):
        collections = self.n1ql_helper.get_collections()
        doc_type_list = self.n1ql_helper.get_doc_type_collection()
        doc_gen_list = self.n1ql_helper.get_doc_gen_list(collections, True)
        keys_list = random.sample(doc_gen_list[collections[0]], 20)
        modify_stmt = []
        for bucket_col in collections:
            stmts = self.clause.get_where_clause(
                doc_type_list[bucket_col], bucket_col,
                0, 6, 1)
            self.n1ql_helper.process_index_to_create(stmts, bucket_col)
            modify_stmt.extend(stmts)
        stmts = []
        for stmt in modify_stmt:
            clause = stmt.split(":")
            if clause[1] == "UPDATE" or clause[1] == "DELETE":
                stmt = stmt + ": " + str(keys_list)
                stmts.append(stmt)
            else:
                stmts.append(stmt)
        stmts = self.n1ql_helper.add_savepoints(stmts)
        self.execute_query_and_validate_results(stmts,
                                            collections)

    def test_basic_insert(self):
        queries = []
        collections = self.n1ql_helper.get_collections()
        self.n1ql_helper.create_index(collections[0])
        name = collections[0].split(".")
        query_params = self.n1ql_helper.create_txn()
        query1 = "INSERT INTO default:`%s`.`%s`.`%s` " %(name[0], name[1], name[2])
        query1 += "(KEY, VALUE) VALUES ( 'KEY', 'VALUE') "
        result = self.n1ql_helper.run_cbq_query(query1, query_params=query_params)
        query = "DELETE FROM default:`%s`.`%s`.`%s` WHERE meta().id = 'KEY'"\
                % (name[0], name[1], name[2])
        result = self.n1ql_helper.run_cbq_query(query, query_params=query_params)
        result = self.n1ql_helper.run_cbq_query(query1, query_params=query_params)
        self.n1ql_helper.end_txn(query_params, self.commit)
        query = "select * FROM default:`%s`.`%s`.`%s` WHERE meta().id = 'KEY'"\
                % (name[0], name[1], name[2])
        result = self.n1ql_helper.run_cbq_query(query)
        if result["results"][0][name[2]] != 'VALUE':
            self.fail("expected and actual values are different")

    def test_basic_update(self):
        collections = self.n1ql_helper.get_collections()
        self.n1ql_helper.create_index(collections[0])
        name = collections[0].split(".")
        query_params = self.n1ql_helper.create_txn()()
        query1 = "INSERT INTO default:`%s`.`%s`.`%s` " %(name[0], name[1], name[2])
        query1 += "(KEY, VALUE) VALUES ( 'KEY', 'VALUE') "
        result = self.n1ql_helper.run_cbq_query(query1, query_params=query_params)
        query = "UPDATE default:`%s`.`%s`.`%s` " %(name[0], name[1], name[2])
        query += "SET d=5 returning *"
        result = self.n1ql_helper.run_cbq_query(query, query_params=query_params)
        query = "select * FROM default:`%s`.`%s`.`%s` WHERE meta().id = 'KEY'"\
                % (name[0], name[1], name[2])
        result = self.n1ql_helper.run_cbq_query(query, query_params=query_params)
        self.n1ql_helper.end_txn(query_params, self.commit)
        print result

