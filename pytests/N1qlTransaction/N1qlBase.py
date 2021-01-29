import Queue
import copy
import json
import random
import string
import re
import testconstants
from threading import Thread

from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import BucketUtils, DocLoaderUtils
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.tuq_generators import JsonGenerator
from couchbase_helper.tuq_generators import TuqGenerators
from membase.api.rest_client import RestConnection
from n1ql_exceptions import N1qlException
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.random_query_template import WhereClause
from sdk_exceptions import SDKException
from com.couchbase.client.java.json import JsonObject
from couchbase_helper.tuq_helper import N1QLHelper
from global_vars import logger
from Cb_constants import CbServer
from couchbase_helper.documentgenerator import doc_generator


class N1qlBase(CollectionBase):
    def setUp(self):
        super(N1qlBase, self).setUp()
        self.scan_consistency = self.input.param("scan_consistency",
                                                 'REQUEST_PLUS')
        self.path = testconstants.LINUX_COUCHBASE_BIN_PATH
        self.use_rest = self.input.param("use_rest", True)
        self.hint_index = self.input.param("hint", None)
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.analytics = self.input.param("analytics", False)
        self.named_prepare = self.input.param("named_prepare", None)
        self.version = self.input.param("cbq_version", "sherlock")
        self.isprepared = False
        self.ipv6 = self.input.param("ipv6", False)
        self.trans = self.input.param("n1ql_txn", True)
        self.commit = self.input.param("commit", True)
        self.rollback_to_savepoint = self.input.param("rollback_to_savepoint", False)
        self.skip_index = self.input.param("skip_index", False)
        self.primary_indx_type = self.input.param("primary_indx_type", 'GSI')
        self.index_type = self.input.param("index_type", 'GSI')
        self.skip_primary_index = self.input.param("skip_primary_index", False)
        self.flat_json = self.input.param("flat_json", False)
        self.dataset = self.input.param("dataset", "sabre")
        self.array_indexing = self.input.param("array_indexing", False)
        self.max_verify = self.input.param("max_verify", None)
        self.num_stmt_txn = self.input.param("num_stmt_txn", 5)
        self.num_collection = self.input.param("num_collection", 1)
        self.num_savepoints = self.input.param("num_savepoints", 0)
        self.override_savepoint = self.input.param("override_savepoint", 0)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.prepare = self.input.param("prepare_stmt", False)
        self.num_txn = self.input.param("num_txn", 3)
        self.clause = WhereClause()
        self.buckets = self.bucket_util.get_all_buckets()
        self.collection_map = {}
        self.txtimeout = self.input.param("txntimeout", 0)
        self.atrcollection = self.input.param("atrcollection", False)
        load_spec = self.input.param("load_spec", self.data_spec_name)
        self.num_commit = self.input.param("num_commit", 3)
        self.num_rollback_to_savepoint = \
        self.input.param("num_rollback_to_savepoint", 0)
        self.num_conflict = self.input.param("num_conflict", 0)
        self.write_conflict = self.input.param("write_conflict", False)
        self.Kvtimeout = self.input.param("Kvtimeout", None)
        self.memory_quota = self.input.param("memory_quota", 1)
        self.n1ql_server = self.cluster_util.get_nodes_from_services_map(service_type="n1ql",
                                                                         get_all_nodes=True)
        self.n1ql_helper = N1QLHelper(server=self.n1ql_server,
                                      use_rest=True,
                                      buckets = self.buckets,
                                      log=self.log,
                                      scan_consistency='REQUEST_PLUS',
                                      num_collection=self.num_collection,
                                      num_buckets=self.num_buckets,
                                      num_savepoints=self.num_savepoints,
                                      override_savepoint=self.override_savepoint,
                                      num_stmt=self.num_stmt_txn,
                                      load_spec=self.data_spec_name)
        self.num_insert,self.num_update,self.num_delete,self.num_merge = \
                        self.n1ql_helper.get_random_number_stmt(self.num_stmt_txn)

    def tearDown(self):
        self.n1ql_helper.drop_index()
        super(N1qlBase, self).tearDown()
#
    def runTest(self):
        pass

    def _verify_results(self, actual_result, expected_result, sort_key=""):
        if self.max_verify is not None:
            actual_result = actual_result[:self.max_verify]
            expected_result = expected_result[:self.max_verify]
        self.assertTrue(len(actual_result) == len(expected_result))
        expected_result=sorted(expected_result, key = lambda i: i[sort_key])
        actual_result=sorted(actual_result, key = lambda i: i[sort_key])
        self.log.info(cmp(actual_result, expected_result))

    def validate_update_results(self, index, docs=[], dict_to_add="",
                                query_params={}, server=None):
        name = index.split('.')
        query = "SELECT  META().id,* from default:`%s`.`%s`.`%s` " \
                "WHERE META().id in %s" \
                % (name[0], name[1], name[2], docs)
        dict_to_add = dict_to_add.split("=")
        dict_to_add[1] = dict_to_add[1].replace('\'', '')
        result = self.n1ql_helper.run_cbq_query(query,
                                                query_params=query_params,
                                                server=server)
        collection = index.split('.')[-1]
        for doc in result["results"]:
            if "target" in dict_to_add[0]:
                new_add = dict_to_add[0].split(".")[1]
                dict_to_add[0] = new_add
            value = doc[collection].get(dict_to_add[0].encode())
            if isinstance(value, list):
                value = [x.encode('UTF8') for x in value]
                if isinstance(dict_to_add[1], str):
                    dict_to_add[1] = dict_to_add[1].strip("][").split(', ')
            else:
                value = str(value)
            if value != dict_to_add[1]:
                self.fail("actual %s and expected value %s are different"
                             % (value, dict_to_add[1]))

    def validate_insert_results(self, index, docs, query_params={}, server=None):
        # modify this to get values for list of docs
        keys = docs.keys()
        keys = [x.encode('UTF8') for x in keys]
        name = index.split('.')
        query = "SELECT  * from default:`%s`.`%s`.`%s` WHERE META().id in %s"\
                % (name[0], name[1], name[2], keys)
        result = self.n1ql_helper.run_cbq_query(query,
                                                query_params=query_params,
                                                server=server)
        for doc in result["results"]:
            t = doc.values()[0]
            if len(keys) > 1:
                if t != docs[t["name"]]:
                    self.log.info("expected value %s and actual value %s"
                                  % (t, docs[t["name"]]))
            else:
                if t != docs.values()[0]:
                    self.log.info("expected value %s and actual value %s"
                                  % (t, docs.values()[0]))
        if result["metrics"]["resultCount"] != len(keys):
            self.fail("Mismatch in result count %s and num keys_inserted %s"
                      % (result["metrics"]["resultCount"], len(keys)))

    def validate_delete_results(self, index, docs, query_params={}, server=None):
        # modify this to get values for list of docs
        name = index.split('.')
        query = "SELECT  META().id,* from default:`%s`.`%s`.`%s` " \
                "WHERE META().id in %s"\
                % (name[0], name[1], name[2], docs)
        result = self.n1ql_helper.run_cbq_query(query,
                                                query_params=query_params,
                                                server=server)
        self.log.info("delete result is %s"%(result["results"]))
        if result["results"]:
            self.fail("Deleted doc is present %s" %(result["results"]))

    def get_prepare_stmt(self, query, query_params):
        queries = []
        name = ""
        while name == "":
            name = self.get_random_name()
            if name in self.name_list:
                name=""
            else:
                self.name_list.append(name)
        if self.prepare:
            query = "PREPARE %s as %s"%(name, query)
            queries.append(query)
            _ = self.n1ql_helper.run_cbq_query(query, query_params=query_params)
            query = "EXECUTE %s"%(name)
            queries.append(query)
        else:
            queries.append(query)
        return queries

    def run_update_query(self, clause, query_params, server=None):
        name = clause[0].split('.')
        if len(clause) > 5:
            update_query = "UPDATE default:`%s`.`%s`.`%s` USE KEYS %s " \
                           "SET %s RETURNING meta().id"\
                            % (name[0], name[1], name[2], clause[5], clause[3])
        else:
            update_query = "UPDATE default:`%s`.`%s`.`%s` SET %s " \
                           "WHERE %s LIMIT 100 RETURNING meta().id"\
                           % (name[0], name[1], name[2], clause[3], clause[2])
        queries = self.get_prepare_stmt(update_query, query_params)
        result = self.n1ql_helper.run_cbq_query(queries[-1],
                                                query_params=query_params,
                                                server=server)
        if result["status"] == "success":
            list_docs = copy.deepcopy([d.get('id').encode() for d in result["results"]])
            self.validate_update_results(clause[0], list_docs, clause[3],
                                         query_params, server)
        else:
            self.fail("delete query failed %s"%queries[-1])
            list_docs = list()
        return list_docs, queries

    def run_insert_query(self, clause, query_params, server=None):
        docs = {}
        name = clause[0].split('.')
        if "name" in clause[-1]:
            queries = []
            docin = {}
            letters = string.ascii_lowercase
            value = [ ''.join(random.choice(letters) for i in range(self.doc_size))][0]
            docs["key1234"] = value
            query_params["memory_quota"] = self.memory_quota
            for key, value in docs.iteritems():
                queries.append("INSERT INTO default:`%s`.`%s`.`%s` " \
                        "(KEY, value) VALUES ( '%s', '%s') RETURNING *" \
                        % (name[0], name[1], name[2], key, value))
        else:
            select_query = "SELECT DISTINCT t.name AS k1,t " \
                           "FROM default:`%s`.`%s`.`%s` t WHERE %s LIMIT 10"\
                           % (name[0], name[1], name[2], clause[2])
            query = "INSERT INTO default:`%s`.`%s`.`%s` " \
                    "(KEY k1, value t) %s RETURNING *" \
                    % (name[0], name[1], name[2], select_query)
            queries = self.get_prepare_stmt(query, query_params)
        result = self.n1ql_helper.run_cbq_query(queries[-1],
                                                query_params=query_params,
                                                server=server)
        if result["status"] == "success":
            if "name" not in clause[-1]:
                for val in result["results"]:
                    t = val.values()[0]
                    key = t["name"]
                    docs[key] = t
            self.validate_insert_results(clause[0], docs, query_params,
                                         server)
        elif N1qlException.DocumentAlreadyExistsException \
                in str(result["errors"][0]["msg"]):
            docs = {}
        elif N1qlException.MemoryQuotaError \
                in str(result["errors"][0]["msg"]) and self.failure:
            docs = {}
        else:
            self.fail("insert query failed %s"%queries[-1])
        return docs, queries

    def run_delete_query(self, clause, query_params, server=None):
        name = clause[0].split('.')
        if len(clause) > 5:
            query = "DELETE FROM default:`%s`.`%s`.`%s` " \
                    "USE KEYS %s RETURNING meta().id"\
                    % (name[0], name[1], name[2], clause[5])
        else:
            query = "DELETE FROM default:`%s`.`%s`.`%s` " \
                    "WHERE %s LIMIT 200 RETURNING meta().id"\
                    % (name[0], name[1], name[2], clause[2])
        docs = list()
        queries = self.get_prepare_stmt(query, query_params)
        result = self.n1ql_helper.run_cbq_query(queries[-1],
                                                query_params=query_params,
                                                server=server)
        if result["status"] == "success":
            docs = [d.get('id').encode() for d in result["results"]]
            self.validate_delete_results(clause[0], docs, query_params,
                                         server)
        else:
            self.fail("delete query failed %s"%queries[-1])
        return docs, queries

    def run_merge_query(self, clause, query_params, server=None):
        # [bucket.scope.collection:MERGE:query]
        name = clause[0].split('.')
        match_or_not = ''
        inserted_docs = {}
        deleted_docs = list()
        tmp_query = None
        if clause[4] == 'DELETE':
            tmp_query = "DELETE WHERE {0} LIMIT 200 RETURNING meta().id".format(clause[3])
            match_or_not = 'WHEN MATCHED'

        elif clause[4] == 'UPDATE':
            tmp_query = "UPDATE SET {0} LIMIT 100 RETURNING meta().id".format(clause[3])
            match_or_not = 'WHEN MATCHED'

        elif clause[4] == 'INSERT':
            tmp_query ='INSERT (KEY UUID(), VALUE {0} ) RETURNING *'.format(clause[3])
            match_or_not = 'WHEN NOT MATCHED'

        # build query
        if tmp_query:
            query = 'MERGE INTO default:`{0}`.`{1}`.`{2}` target USING [{{"id":"21728", "month": 10, "year":2010, "day":25, "job":"Engineer"}},{{"id":"21730", "month": 7, "year":2008, "day":26, "job":"Support"}}] source ON {3} {4} THEN {5}'.format(
                name[0], name[1], name[2], clause[2], match_or_not, tmp_query)

            #execute query
            if self.memory_quota:
                query_params['memory_quota'] = 5
            result = self.n1ql_helper.run_cbq_query(query,
                                                    query_params=query_params,
                                                    server=server)
            # if success then validate and return results
            if result["status"] == "success":
                # in case of insert, send the list of inserted docs
                if clause[4] == 'INSERT':
                    for val in result["results"]:
                        t = val.values()[0]
                        key = t["name"]
                        inserted_docs[key] = t
                    self.validate_insert_results(clause[0], inserted_docs, query_params, server)
                    docs = inserted_docs
                elif clause[4] == 'UPDATE':
                    updated_docs = copy.deepcopy([d.get('id').encode() for d in result["results"]])
                    updated_val = clause[3].split('.')[-1]
                    # in case of update , get update docs and updated value
                    self.validate_update_results(clause[0], updated_docs, updated_val, query_params, server)
                    docs = {updated_val: updated_docs}
                elif clause[4] == 'DELETE':
                    deleted_docs = [d.get('id').encode() for d in result["results"]]
                    # in case of delete, get deelted docs in deleted_docs
                    self.validate_delete_results(clause[0], deleted_docs, query_params, server)
                    docs = deleted_docs
            else:
                self.fail("merge query failed %s" % query)
                docs = list()
            return docs, query
        else:
            return {},[]

    def run_savepoint_query(self, clause, query_params, server=None):
        query = "SAVEPOINT %s" % clause[1]
        result = self.n1ql_helper.run_cbq_query(query, query_params=query_params,
                                                server=server)
        if result["status"] != "success":
            self.fail("savepoint query failed %s"%query)
        return query

    def get_savepoint_to_verify(self, savepoint):
        savepoint_txn = random.choice(savepoint).split(":")[0]
        savepoint_txn = [key for key in savepoint if savepoint_txn in key][-1]
        index = savepoint.index(savepoint_txn) + 1
        return savepoint[:index]

    def full_execute_query(self, stmts, commit, query_params={},
                           rollback_to_savepoint=False, write_conflict=False,
                           issleep=0, N1qlhelper=None, prepare=False, server=None):
        """
        1. collection_map will store the values changed for a collection after savepoint
        it will be re-intialized after each savepoint and the values will be copied
        to collection_savepoint collection_map[collection] = {INSERT:{}, UPDATE:{}, DELETE:[]}
        2. collection savepoint will keep track of all changes and map it to savepoint
         collection savepoint = {savepoint: {
                                 collection1:{
                                 INSERT:{}, UPDATE:{}, DELETE:[]},..
                                 }..}
        3. savepoint list will have the order of savepoints
        """
        self.name_list = []
        self.prepare = prepare
        self.log = logger.get("test")
        collection_savepoint = dict()
        savepoint = list()
        collection_map = dict()
        txid = query_params.values()[0]
        rerun_thread = False
        if N1qlhelper:
            self.n1ql_helper = N1qlhelper
        queries = dict()
        queries[txid] = list()
        try:
            for stmt in stmts:
                query = "SELECT * FROM system:transactions"
                results = self.n1ql_helper.run_cbq_query(query)
                self.log.info(json.JSONEncoder().encode(results))
                clause = stmt.split(":")
                if clause[0] == "SAVEPOINT":
                    query = self.run_savepoint_query(clause, query_params, server=server)
                    if clause[1] in str(savepoint):
                        str1 = clause[1] + ":" + str(len(collection_savepoint.keys()))
                        collection_savepoint[str1] = copy.deepcopy(collection_map)
                        savepoint.append(str1)
                    else:
                        collection_savepoint[clause[1]] = copy.deepcopy(collection_map)
                        savepoint.append(clause[1])
                    queries[txid].append(query)
                    collection_map = {}
                    continue
                elif clause[0] not in collection_map.keys():
                    collection_map[clause[0]] = \
                                {"INSERT": {}, "UPDATE": {}, "DELETE":[]}
                if clause[1] == "UPDATE":
                    result, query = \
                        self.run_update_query(clause, query_params, server)
                    queries[txid].extend(query)
                    collection_map[clause[0]]["UPDATE"][clause[3]] = result
                if clause[1] == "INSERT":
                    result, query = self.run_insert_query(
                                            clause, query_params, server)
                    collection_map[clause[0]]["INSERT"].update(result)
                    queries[txid].extend(query)
                if clause[1] == "DELETE":
                    result, query = self.run_delete_query(
                                        clause, query_params, server)
                    collection_map[clause[0]]["DELETE"].extend(result)
                    queries[txid].extend(query)
                if clause[1] == "MERGE":
                    result, query = \
                            self.run_merge_query(clause, query_params, server)
                    if isinstance(result, list):
                        collection_map[clause[0]][clause[4]].extend(result)
                    elif result:
                        collection_map[clause[0]][clause[4]].update(result)
                    queries[txid].append(query)
            if issleep:
                self.sleep(issleep)
            if write_conflict:
                write_conflict_result = \
                    self.simulate_write_conflict(stmts, random.choice([True, False]))
            if rollback_to_savepoint and (len(savepoint) > 0):
                savepoint = self.get_savepoint_to_verify(savepoint)
                query, result = self.n1ql_helper.end_txn(query_params, commit,
                                             savepoint[-1].split(':')[0],
                                             server=server)
                queries[txid].append(query)
            if commit is False:
                savepoint = []
                collection_savepoint = {}
                query, result = self.n1ql_helper.end_txn(query_params, commit=False,
                                                         server=server)
                queries[txid].append(query)
            else:
                if (not rollback_to_savepoint) or len(savepoint) == 0:
                    collection_savepoint['last'] = copy.deepcopy(collection_map)
                    savepoint.append('last')
                query = "SELECT * FROM system:transactions"
                results = self.n1ql_helper.run_cbq_query(query)
                self.log.debug(results)
                queries[txid].append(query)
                query, result = self.n1ql_helper.end_txn(query_params, commit=True,
                                                         server=server)
                queries[txid].append(query)
                if isinstance(result, str) or 'errors' in result:
                    #retry the entire transaction
                    rerun_thread = self.validate_error_during_commit(result,
                                     collection_savepoint, savepoint)
                    savepoint = []
                    collection_savepoint = {}
            if write_conflict and write_conflict_result:
                collection_savepoint['last'] = copy.deepcopy(write_conflict_result)
                savepoint.append("last")
        except Exception as e:
            self.log.info(json.JSONEncoder().encode(e))
            collection_savepoint = e
        return collection_savepoint, savepoint, queries, rerun_thread

    def simulate_write_conflict(self, stmts, commit):
        collection_map = {}
        clause = list()
        for i in range(5):
            stmt = random.choice(stmts)
            clause = stmt.split(":")
            if clause[0] != "SAVEPOINT":
                collection_map[clause[0]] = {"INSERT": {},
                                             "UPDATE": {},
                                             "DELETE": []}
                break
        # create a txn
        query_params = self.n1ql_helper.create_txn()
        if clause[1] == "UPDATE":
            result, query = \
                self.run_update_query(clause, query_params)
            collection_map[clause[0]]["UPDATE"][clause[3]] = result
        if clause[1] == "INSERT":
            result, query = self.run_insert_query(
                                    clause, query_params)
            collection_map[clause[0]]["INSERT"].update(result)
        if clause[1] == "DELETE":
            result, query = self.run_delete_query(
                                clause, query_params)
            collection_map[clause[0]]["DELETE"].extend(result)

        # commit or rollback a txn
        if commit:
            self.n1ql_helper.end_txn(query_params, True)
        else:
            self.n1ql_helper.end_txn(query_params, False)
            collection_map = dict()
        return collection_map

    def get_random_name(self):
        char_set = string.ascii_letters
        name_len = random.randint(1, 20)
        rand_name = ""
        rand_name = ''.join(random.choice(char_set)
                                for _ in range(name_len))
        return rand_name

    def validate_keys(self, client, key_value, deleted_key):
        # create a client
        # get all the values and validate
        success, fail = client.get_multi(key_value.keys(), 120)
        for key, val in success.items():
            if type(key_value[key]) == JsonObject:
                expected_val = json.loads(key_value[key].toString())
            elif isinstance(key_value[key], dict):
                expected_val = key_value[key]
            else:
                expected_val = json.loads(key_value[key])
            actual_val = json.loads(val['value'].toString())
            if set(expected_val) != set(actual_val):
                self.fail("expected %s and actual %s for key %s are not equal"
                          % (expected_val, actual_val, key))
        for key, val in fail.items():
            if key in deleted_key and SDKException.DocumentNotFoundException \
                    in str(fail[key]["error"]):
                continue
        self.log.info("Expected keys: %s, Actual: %s, Deleted: %s"
                      % (len(success.keys()), len(key_value.keys()),
                         len(deleted_key)))
        DocLoaderUtils.sdk_client_pool.release_client(client)

    def validate_error_during_commit(self, result,
                                      collection_savepoint, savepoint):
        dict_to_verify = {}
        count = 0
        error_msg = result["errors"][0]["cause"]["cause"]
        if isinstance(error_msg, dict):
            error_msg = error_msg["error_description"]
        self.log.info("cause is %s"%result["errors"][0]["cause"])
        if N1qlException.CasMismatchException \
            in str(error_msg):
            return True
        elif N1qlException.DocumentExistsException \
            in str(error_msg):
            for key in savepoint:
                for index in collection_savepoint[key].keys():
                    keys = collection_savepoint[key][index]["INSERT"].keys()
                    try:
                        dict_to_verify[index].extend(keys)
                    except:
                        dict_to_verify[index] = keys
            for index, docs in dict_to_verify.items():
                name = index.split('.')
                docs = [d.encode() for d in docs]
                query = "SELECT  META().id,* from default:`%s`.`%s`.`%s` " \
                    "WHERE META().id in %s"\
                    % (name[0], name[1], name[2], docs)
                self.log.info("query is %s"%query)
                result = self.n1ql_helper.run_cbq_query(query)
                if result["metrics"]["resultCount"] == 0:
                    count += 1
            if count == len(dict_to_verify.keys()):
                self.log.info("txn failed with document exists")
                return True
            else:
                return False
        elif N1qlException.DocumentNotFoundException in \
            str(error_msg):
            for key in savepoint:
                for index in collection_savepoint[key].keys():
                    try:
                        dict_to_verify[index].extend(
                            collection_savepoint[key][index]["DELETE"])
                    except:
                        dict_to_verify[index] = \
                            collection_savepoint[key][index]["DELETE"]
            for index, docs in dict_to_verify.items():
                name = index.split('.')
                docs = [d.encode() for d in docs]
                query = "SELECT  META().id,* from default:`%s`.`%s`.`%s` " \
                        "WHERE META().id in %s"\
                        % (name[0], name[1], name[2], docs)
                self.log.info("query is %s"%query)
                result = self.n1ql_helper.run_cbq_query(query)
                if result["metrics"]["resultCount"] == len(docs):
                    count += 1
            if count == len(dict_to_verify.keys()):
                self.fail("got %s error when doc exist" %
                                N1qlException.DocumentNotFoundException)
                return False
            else:
                return True

    def process_value_for_verification(self, bucket_col, doc_gen_list,
                                       results, buckets=None):
        """
        1. get the collection
        2. get its doc_gen
        3. first validate deleted docs
        4. then check updated docs
        5. validate inserted docs
        """
        for collection in bucket_col:
            self.log.info("validation started for collection %s"%collection)
            gen_load = doc_gen_list[collection]
            self.validate_dict = {}
            self.deleted_key = []
            doc_gen = copy.deepcopy(gen_load)
            while doc_gen.has_next():
                key, val = next(doc_gen)
                self.validate_dict[key] = val
            for res in results:
                for savepoint in res[1]:
                    if collection in res[0][savepoint].keys():
                        for key in set(res[0][savepoint][collection]["DELETE"]):
                            self.deleted_key.append(key)
                        for key, val in res[0][savepoint][collection]["INSERT"].items():
                            self.validate_dict[key] = val
                        for key, val in res[0][savepoint][collection]["UPDATE"].items():
                            mutated = key.split("=")
                            for t_id in val:
                                try:
                                    self.validate_dict[t_id][mutated[0]] = \
                                        mutated[1]
                                except:
                                    self.validate_dict[t_id].put(mutated[0],
                                                                     mutated[1])
            bucket_collection = collection.split('.')
            if buckets:
                self.buckets = buckets
            else:
                self.buckets = self.bucket_util.buckets
            bucket = BucketUtils.get_bucket_obj(self.buckets,
                                                bucket_collection[0])
            client = \
                DocLoaderUtils.sdk_client_pool.get_client_for_bucket(
                    bucket, bucket_collection[1], bucket_collection[2])
            self.validate_keys(client, self.validate_dict, self.deleted_key)

    def thread_txn(self, args):
        stmt = args[0]
        query_params = args[1]
        commit = args[2]
        rollback_to_savepoint = args[3]
        write_conflict = args[4]
        server = args[5]
        self.log.info("values are %s %s %s %s" % (stmt, commit,
                                                  rollback_to_savepoint,
                                                  write_conflict))
        collection_savepoint, savepoints, queries, rerun_thread = \
            self.full_execute_query(stmt, commit, query_params,
                                    rollback_to_savepoint, write_conflict,
                                    server=server)
        if rerun_thread:
            query_params = self.n1ql_helper.create_txn(server=server, txtimeout=1)
            collection_savepoint, savepoints, queries, rerun_thread = \
            self.full_execute_query(stmt, commit, query_params,
                                    rollback_to_savepoint, write_conflict,
                                    server=server)
        self.log.info("queries executed in txn are %s" % queries)
        return [collection_savepoint, savepoints]

    def get_stmt_for_threads(self, collections, doc_type_list, num_commit,
                             num_rollback_to_savepoint=0, num_conflict=0):
        server=None
        que = Queue.Queue()
        fail = False
        self.threads = []
        self.results = []
        stmt = []
        for bucket_col in collections:
            self.num_insert, self.num_update, self.num_delete, self.num_merge = \
                        self.n1ql_helper.get_random_number_stmt(self.num_stmt_txn)
            self.log.info("insert, delete, update and merge %s %s %s %s"
                           % (self.num_insert, self.num_update,
                              self.num_delete, self.num_merge))
            self.num_merge = 0
            stmt.extend(self.clause.get_where_clause(
                doc_type_list[bucket_col], bucket_col,
                self.num_insert, self.num_update, self.num_delete, self.num_merge))
            self.n1ql_helper.process_index_to_create(stmt, bucket_col)
        random.shuffle(stmt)
        stmt_list = self.__chunks(stmt, int(len(stmt)/self.num_txn))

        for stmt in stmt_list:
            stmt = self.n1ql_helper.add_savepoints(stmt)
            random.seed(stmt[0])
            num_rollback_to_savepoint, rollback_to_savepoint = \
                self.num_count(num_rollback_to_savepoint)
            num_commit, commit = \
                self.num_count(num_commit)
            num_conflict, conflict = \
                self.num_count(num_conflict)
            if isinstance(self.n1ql_server, list):
                server = random.choice(self.n1ql_server)
            query_params = self.n1ql_helper.create_txn(server=server, txtimeout=1)
            self.threads.append(
                Thread(target=lambda q, arg1: q.put(self.thread_txn(arg1)),
                       args=(que, [stmt, query_params, commit,
                                   rollback_to_savepoint, conflict, server])))

        for thread in self.threads:
            thread.start()
        for thread in self.threads:
            thread.join()

        while not que.empty():
            result = que.get()
            if isinstance(result[0], dict):
                self.results.append(result)
            else:
                self.log.info(result[0])
                fail = True
        return self.results, fail

    @staticmethod
    def num_count(value):
        if value > 0:
            value = value - 1
            return value, True
        else:
            return 0, False

    @staticmethod
    def __chunks(i_list, n):
        """Yield successive n-sized chunks from input_list."""
        for i in range(0, len(i_list), n):
            yield i_list[i:i + n]

    def get_collection_for_atrcollection(self):
        collections = BucketUtils.get_random_collections(
                self.buckets, 1, "all", self.num_buckets)
        for bucket, scope_dict in collections.items():
            for s_name, c_dict in scope_dict["scopes"].items():
                for c_name, c_data in c_dict["collections"].items():
                    if random.choice([True, False]):
                        atrcollection = ("`%s`.`%s`.`%s`"%(bucket, s_name, c_name))
                    else:
                        atrcollection = ("`%s`.`%s`.`%s`"%(bucket,
                                     CbServer.default_scope,
                                     CbServer.default_collection))
        return atrcollection

    def execute_query_and_validate_results(self, stmt, bucket_col, doc_gen_list=None):
        atrcollection = ""
        if self.atrcollection:
            atrcollection = self.get_collection_for_atrcollection()
        query_params = self.n1ql_helper.create_txn(self.txtimeout, self.durability_level,
                                                   atrcollection, Kvtimeout=self.Kvtimeout)
        collection_savepoint, savepoints, queries, rerun = \
            self.full_execute_query(stmt, self.commit, query_params,
                                    self.rollback_to_savepoint)
        self.log.info("queries ran are %s" % queries)
        if not doc_gen_list:
            doc_gen_list = self.n1ql_helper.get_doc_gen_list(bucket_col)
        if isinstance(collection_savepoint, dict):
            results = [[collection_savepoint, savepoints]]
            self.process_value_for_verification(bucket_col,
                                 doc_gen_list, results)
        else:
            self.fail(collection_savepoint)