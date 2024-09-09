import json
import time
import random

from common_lib import sleep
from couchbase_helper.tuq_generators import TuqGenerators
from couchbase_helper.tuq_generators import JsonGenerator
from membase.api.rest_client import RestConnection
from platform_constants.os_constants import Linux, Windows
from n1ql_exceptions import N1qlException
from bucket_utils.bucket_ready_functions import BucketUtils
from couchbase_helper.random_query_template import WhereClause
from collections_helper.collections_spec_constants import MetaCrudParams
from shell_util.remote_connection import RemoteMachineShellConnection


class N1QLHelper:
    def __init__(self, version=None, server=None, shell=None,  max_verify=0, buckets=[], item_flag=0,
                 n1ql_port=8093, full_docs_list=[], log=None, input=None, database=None, use_rest=None,
                 scan_consistency="not_bounded", hint_index=False, num_collection=1, num_buckets=1,
                 num_savepoints=0, override_savepoint=0, num_stmt=3, load_spec=None):
        self.version = version
        self.shell = shell
        self.max_verify = max_verify
        self.buckets = buckets
        self.item_flag = item_flag
        self.n1ql_port = n1ql_port
        self.input = input
        self.log = log
        self.use_rest = use_rest
        self.full_docs_list = full_docs_list
        self.server = server
        self.database = database
        self.hint_index = hint_index
        self.analytics = False
        self.named_prepare = False
        self.num_collection = num_collection
        self.num_buckets = num_buckets
        self.clause = WhereClause()
        self.num_stmt_txn = num_stmt
        self.scan_consistency = scan_consistency
        self.num_savepoints = num_savepoints
        self.override_savepoint = override_savepoint
        self.load_spec = load_spec
        if self.full_docs_list and len(self.full_docs_list) > 0:
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        self.doc_gen_type = "default"
        if self.load_spec is not None:
            input_spec = \
                BucketUtils.get_crud_template_from_package(self.load_spec)
            self.doc_gen_type = \
                input_spec.get(MetaCrudParams.DOC_GEN_TYPE, "default")
        self.get_random_number_stmt(self.num_stmt_txn)
        self.index_map = {}
        self.name_list = []

    def killall_tuq_process(self):
        self.shell.execute_command("killall cbq-engine")
        self.shell.execute_command("killall tuqtng")
        self.shell.execute_command("killall indexer")

    def run_query_from_template(self, query_template):
        self.query = self.gen_results.generate_query(query_template)
        expected_result = self.gen_results.generate_expected_result()
        actual_result = self.run_cbq_query()
        return actual_result, expected_result

    def get_txid(self, records):
        try:
            txid = records["results"][0]["txid"]
        except:
            txid = ""
        return txid

    def create_txn(self, txtimeout=0, durability_level="", atrcollection="",
                   server=None, Kvtimeout=None):
        query_params = {}
        if durability_level:
            query_params["durability_level"] = durability_level
        if txtimeout:
            query_params["txtimeout"] = str(txtimeout) + "m"
        if Kvtimeout:
            query_params["Kvtimeout"] = str(Kvtimeout) + "s"
        if atrcollection:
            query_params["atrcollection"] = atrcollection
        stmt = "BEGIN WORK"
        results = self.run_cbq_query(stmt, query_params=query_params,
                                     server=server)
        txid = self.get_txid(results)
        return {'txid': txid}

    def end_txn(self, query_params, commit, savepoint="", server=None):
        # query_params = {'txid': txid}
        if savepoint:
            stmt = "ROLLBACK TO SAVEPOINT " + savepoint
        elif commit:
            stmt = "COMMIT WORK"
        else:
            stmt = "ROLLBACK WORK"
        self.log.info("commit work for txn %s"%query_params)
        result = self.run_cbq_query(stmt, query_params=query_params,
                                    server=server)
        return stmt, result

    def run_cbq_query(self, query=None, min_output_size=10,
                      server=None, query_params={}, is_prepared=False,
                      encoded_plan=None, username=None, password=None):
        if query is None:
            query = ""
        if server is None:
            if isinstance(self.server, list):
                server = self.server[0]
            else:
                server = self.server
        cred_params = {'creds': []}
        rest = RestConnection(server)
        if username is None and password is None:
            username = 'Administrator'
            password = 'password'
        cred_params['creds'].append({'user': username, 'pass': password})
        query_params.update(cred_params)

        if self.use_rest:
            query_params.update({'scan_consistency': self.scan_consistency})
            if hasattr(self, 'query_params') and self.query_params:
                query_params = self.query_params
            if self.hint_index and (query.lower().find('select') != -1):
                from_clause = re.sub(r'let.*', '',
                                     re.sub(r'.*from', '', re.sub(r'where.*', '', query)))
                from_clause = re.sub(r'LET.*', '',
                                     re.sub(r'.*FROM', '', re.sub(r'WHERE.*', '', from_clause)))
                from_clause = re.sub(r'select.*', '', re.sub(r'order by.*', '',
                                                             re.sub(r'group by.*', '',
                                                                    from_clause)))
                from_clause = re.sub(r'SELECT.*', '', re.sub(r'ORDER BY.*', '',
                                                             re.sub(r'GROUP BY.*', '',
                                                                    from_clause)))
                hint = ' USE INDEX (%s using %s) ' % (self.hint_index,
                                                      self.index_type)
                query = query.replace(from_clause, from_clause + hint)

            if not is_prepared:
                self.log.info('RUN QUERY %s' % query)

            if self.analytics:
                query = query + ";"
                for bucket in self.buckets:
                    query = query.replace(bucket.name, bucket.name + "_shadow")
                result1 = RestConnection(self.cluster.cbas_node) \
                    .execute_statement_on_cbas(query, "immediate")
                try:
                    result = json.loads(result1)
                except Exception as ex:
                    self.log.error("CANNOT LOAD QUERY RESULT IN JSON: %s"
                                   % ex.message)
                    self.log.error("INCORRECT DOCUMENT IS: " + str(result1))
            else:
                self.log.info("query_params is %s" % query_params)
                result = rest.query_tool(query, self.n1ql_port,
                                         query_params=query_params,
                                         is_prepared=is_prepared,
                                         named_prepare=self.named_prepare,
                                         encoded_plan=encoded_plan,
                                         servers=server, timeout=100)
        else:
            self.shell = RemoteMachineShellConnection(self.cluster.master)
            if self.version == "git_repo":
                output = self.shell.execute_commands_inside(
                    "$GOPATH/src/github.com/couchbase/query/"
                    + "shell/cbq/cbq ", "", "", "", "", "", "")
                self.shell.disconnect()
            else:
                if not self.isprepared:
                    query = query.replace('"', '\\"')
                    query = query.replace('`', '\\`')
                    if self.ipv6:
                        cmd = "%scbq  -engine=http://%s:%s/ -q -u %s -p %s" % (
                            self.path, server.ip, self.n1ql_port,
                            username, password)
                    else:
                        cmd = "%scbq  -engine=http://%s:%s/ -q -u %s -p %s" % (
                            self.path, server.ip, server.port,
                            username, password)

                    output = self.shell.execute_commands_inside(
                        cmd, query, "", "", "", "", "")
                    if not (output[0] == '{'):
                        output1 = '{%s' % output
                    else:
                        output1 = output
                    try:
                        result = json.loads(output1)
                    except Exception as ex:
                        self.log.error("CANNOT LOAD QUERY RESULT IN JSON: %s"
                                       % ex.message)
                        self.log.error("INCORRECT DOCUMENT IS: %s " % output1)
        if 'metrics' in result:
            self.log.info("TOTAL ELAPSED TIME: %s" % result["metrics"]["elapsedTime"])

        if isinstance(result, str) or 'errors' in result:
            for key, value in result["errors"][0].iteritems():
                if "cause" in key:
                    err = value
                else:
                    err = result["errors"][0]
            if "INDEX" in query:
                self.log.info("Index creation failed")
                result = None
            elif N1qlException.CasMismatchException \
                in str(err) \
                or N1qlException.DocumentExistsException \
                in str(err) or \
                N1qlException.DocumentNotFoundException in \
                str(err) or \
                N1qlException.DocumentAlreadyExistsException \
                in str(err):
                    self.log.info("txn failed with error %s"% json.JSONEncoder().encode(result))
                    return result
            else:
                raise Exception("txn failed with unexpected errors %s"%json.JSONEncoder().encode(result))
                result = None
        return result

    def get_collection_name(self, bucket="default",
                            scope="_default", collection="_default"):
        query_default_bucket = bucket + '.' \
                                + scope + '.' + collection
        return query_default_bucket

    def get_random_number_stmt(self, num_txn):
        self.num_insert = random.choice(range(num_txn))
        num_range = num_txn - self.num_insert
        if num_range > 0:
            self.num_update = random.choice(range(num_range))
        num_range = num_range - self.num_update
        self.num_delete = random.choice(range(num_range))
        self.num_merge = (num_range - self.num_delete)
        return self.num_insert, self.num_update, self.num_delete, self.num_merge

    def get_collections(self):
        keyspaces = []
        collections = BucketUtils.get_random_collections(
            self.buckets, self.num_collection, "all", self.num_buckets)
        for bucket, scope_dict in collections.items():
            for s_name, c_dict in scope_dict["scopes"].items():
                for c_name, c_data in c_dict["collections"].items():
                    keyspace = self.get_collection_name(bucket, s_name, c_name)
                    keyspaces.append(keyspace)
        return keyspaces

    def get_doc_type_collection(self):
        doc_type_list = {}
        for bucket in self.buckets:
            for s_name, scope in bucket.scopes.items():
                for c_name, c_dict in scope.collections.items():
                    collection = \
                        self.get_collection_name(bucket.name, s_name, c_name)
                    if isinstance(self.doc_gen_type, list):
                        random.seed(c_name)
                        doc_type_list[collection] = random.choice(self.doc_gen_type)
                    else:
                        doc_type_list[collection] = self.doc_gen_type
        return doc_type_list

    def process_index_to_create(self, stmts, collection):
        self.index_map[collection] = []
        all_index = []
        for stmt in stmts:
            index=""
            clause = stmt.split(":")
            if len(self.index_map[collection]) == 0:
                self.create_index(collection)
            while index == "":
                index = BucketUtils.get_random_name()
                if ('%' in index) or (index in all_index):
                    index=""
            result = self.create_index(collection, index, clause[-1])
            if result:
                self.index_map[collection].append(index)
                all_index.append(index)

    def create_index(self, collection, index=None, params=[]):
        name = collection.split('.')
        if params:
            query = "CREATE INDEX `%s` ON default:`%s`.`%s`.`%s`(%s)" \
                    " USING GSI" %(index, name[0],
                     name[1], name[2], params)
        else:
            query = "CREATE PRIMARY INDEX " \
              "on default:`%s`.`%s`.`%s` " \
              "USING GSI" \
              % (name[0], name[1], name[2])
        return self.run_cbq_query(query)

    def drop_index(self):
        for collection in self.index_map.keys():
            name = collection.split('.')
            for index in self.index_map[collection]:
                query = "DROP INDEX default:`%s`.`%s`.`%s`.`%s` "\
                        "USING GSI" %(name[0],name[1], name[2],
                                     index)
                _ = self.run_cbq_query(query)

    def get_stmt(self, collections):
        doc_type_list = self.get_doc_type_collection()
        stmt = []
        for bucket_col in collections:
            stmt.extend(self.clause.get_where_clause(
                doc_type_list[bucket_col], bucket_col,
                self.num_insert, self.num_update, self.num_delete,self.num_merge))
            self.process_index_to_create(stmt, bucket_col)
        stmt = random.sample(stmt, self.num_stmt_txn)
        stmt = self.add_savepoints(stmt)
        return stmt

    def add_savepoints(self, stmt):
        for i in range(self.num_savepoints):
            for _ in range(self.override_savepoint):
                save_stmt = "SAVEPOINT:a" + str(random.choice(
                    range(self.num_savepoints)))
                stmt.append(save_stmt)
            save_stmt = "SAVEPOINT:a" + str(i)
            stmt.append(save_stmt)
        random.shuffle(stmt)
        return stmt

    def create_full_stmts(self, stmts):
        queries = []
        for stmt in stmts:
            clause = stmt.split(":")
            name = clause[0].split('.')
            if clause[0] == "SAVEPOINT":
                queries.append("SAVEPOINT %s" % clause[1])
            elif clause[1] == "INSERT":
                select_query = "SELECT DISTINCT t.name AS k1,t " \
                       "FROM default:`%s`.`%s`.`%s` t WHERE %s LIMIT 10"\
                       % (name[0], name[1], name[2], clause[2])
                query = "INSERT INTO default:`%s`.`%s`.`%s` " \
                    "(KEY k1, value t) %s RETURNING *" \
                    % (name[0], name[1], name[2], select_query)
                queries.append(query)
            elif clause[1] == "DELETE":
                if len(clause) > 5:
                    query = "DELETE FROM default:`%s`.`%s`.`%s` " \
                            "USE KEYS %s RETURNING meta().id"\
                            % (name[0], name[1], name[2], clause[5])
                else:
                    query = "DELETE FROM default:`%s`.`%s`.`%s` " \
                            "WHERE %s LIMIT 200 RETURNING meta().id"\
                            % (name[0], name[1], name[2], clause[2])
                queries.append(query)
            elif clause[1] == "UPDATE":
                if len(clause) > 5:
                    update_query = "UPDATE default:`%s`.`%s`.`%s` USE KEYS %s " \
                                   "SET %s RETURNING meta().id"\
                                    % (name[0], name[1], name[2],
                                       clause[5], clause[3])
                else:
                    update_query = "UPDATE default:`%s`.`%s`.`%s` SET %s " \
                                   "WHERE %s LIMIT 100 RETURNING meta().id"\
                                   % (name[0], name[1], name[2], clause[3], clause[2])
                queries.append(update_query)
        return queries

    def get_tuq_results_object(self, gen_load):
        return TuqGenerators(self.log,
                             [self.generate_full_docs_list([gen_load])])

    def gen_docs(self, generic_key="", start=0, end=1, type="default"):
        json_generator = JsonGenerator()
        if type == "employee":
            gen_docs = json_generator.generate_docs_employee_more_field_types(
                            generic_key, docs_per_day=end, start=start)
        else:
            gen_docs = json_generator.generate_earthquake_doc(
                                generic_key, end=end, start=start)
        return gen_docs

    def get_key_from_doc(self, gen_load):
        key_list = []
        doc_gen = copy.deepcopy(gen_load)
        while doc_gen.has_next():
            key, val = next(doc_gen)
            key_list.append(key)
        return key_list

    def get_doc_gen_list(self, collections, keys=False):
        doc_gen_list = {}
        for bucket in self.buckets:
            for s_name, scope in bucket.scopes.items():
                for c_name, c_dict in scope.collections.items():
                    bucket_collection = \
                        self.get_collection_name(bucket.name, s_name, c_name)
                    if isinstance(self.doc_gen_type, list):
                        random.seed(c_name)
                        type = random.choice(self.doc_gen_type)
                    else:
                        type = self.doc_gen_type
                    if keys:
                        key = []
                        for i in range(c_dict.doc_index[0] ,c_dict.doc_index[1]):
                            key.append("test_collections-" + str(i))
                        doc_gen_list[bucket_collection] = key
                    else:
                        doc_gen_list[bucket_collection] = \
                            self.gen_docs("test_collections", c_dict.doc_index[0],
                                          c_dict.doc_index[1], type=type)
        for key, value in doc_gen_list.items():
            if key not in collections:
                doc_gen_list.pop(key)
        return doc_gen_list

    def generate_full_docs_list(self, gens_load=[], update=False):
        all_docs_list = dict()
        for gen_load in gens_load:
            doc_gen = copy.deepcopy(gen_load)
            while doc_gen.has_next():
                key, val = next(doc_gen)
                try:
                    val = json.loads(val)
                    if isinstance(val, dict) and 'mutated' not in list(val.keys()):
                        if update:
                            val['mutated'] = 1
                        else:
                            val['mutated'] = 0
                    else:
                        val['mutated'] += val['mutated']
                except TypeError:
                    pass
                all_docs_list[key]=val
        return all_docs_list

    def _verify_results(self, actual_result, expected_result, missing_count = 1, extra_count = 1):
        self.log.info("Analyzing Actual Result")
        actual_result = self._gen_dict(actual_result)
        self.log.info("Analyzing Expected Result")
        expected_result = self._gen_dict(expected_result)
        if len(actual_result) != len(expected_result):
            raise Exception("Results are incorrect.Actual num %s. Expected num: %s.\n" % (len(actual_result), len(expected_result)))
        msg = "The number of rows match but the results mismatch, please check"
        if actual_result != expected_result:
            raise Exception(msg)

    def _verify_results_rqg(self, subquery, aggregate=False, n1ql_result=[], sql_result=[], hints=["a1"], aggregate_pushdown=False):
        new_n1ql_result = []
        for result in n1ql_result:
            if result != {}:
                new_n1ql_result.append(result)

        n1ql_result = new_n1ql_result

        if self._is_function_in_result(hints):
            return self._verify_results_rqg_for_function(n1ql_result, sql_result, aggregate_pushdown=aggregate_pushdown)

        check = self._check_sample(n1ql_result, hints)
        actual_result = n1ql_result

        if actual_result == [{}]:
            actual_result = []
        if check:
            actual_result = self._gen_dict(n1ql_result)

        actual_result = sorted(actual_result)
        expected_result = sorted(sql_result)

        if len(actual_result) != len(expected_result):
            extra_msg = self._get_failure_message(expected_result, actual_result)
            raise Exception("Results are incorrect. Actual num %s. Expected num: %s. :: %s \n" % (len(actual_result), len(expected_result), extra_msg))

        msg = "The number of rows match but the results mismatch, please check"
        if subquery:
            for x, y in zip(actual_result, expected_result):
                if aggregate:
                    productId = x['ABC'][0]['$1']
                else:
                    productId = x['ABC'][0]['productId']
                if(productId != y['ABC']) or \
                                x['datetime_field1'] != y['datetime_field1'] or \
                                x['primary_key_id'] != y['primary_key_id'] or \
                                x['varchar_field1'] != y['varchar_field1'] or \
                                x['decimal_field1'] != y['decimal_field1'] or \
                                x['char_field1'] != y['char_field1'] or \
                                x['int_field1'] != y['int_field1'] or \
                                x['bool_field1'] != y['bool_field1']:
                    print("actual_result is %s" % actual_result)
                    print("expected result is %s" % expected_result)
                    extra_msg = self._get_failure_message(expected_result, actual_result)
                    raise Exception(msg+"\n "+extra_msg)
        else:
            if self._sort_data(actual_result) != self._sort_data(expected_result):
                extra_msg = self._get_failure_message(expected_result, actual_result)
                raise Exception(msg+"\n "+extra_msg)

    def _sort_data(self, result):
        new_data = []
        for data in result:
            new_data.append(sorted(data))
        return new_data

    def _verify_results_crud_rqg(self, n1ql_result=[], sql_result=[], hints=["primary_key_id"]):
        new_n1ql_result = []
        for result in n1ql_result:
            if result != {}:
                new_n1ql_result.append(result)
        n1ql_result = new_n1ql_result
        if self._is_function_in_result(hints):
            return self._verify_results_rqg_for_function(n1ql_result, sql_result)
        check = self._check_sample(n1ql_result, hints)
        actual_result = n1ql_result
        if actual_result == [{}]:
            actual_result = []
        if check:
            actual_result = self._gen_dict(n1ql_result)
        actual_result = sorted(actual_result)
        expected_result = sorted(sql_result)

        if len(actual_result) != len(expected_result):
            extra_msg = self._get_failure_message(expected_result, actual_result)
            raise Exception("Results are incorrect. Actual num %s. Expected num: %s.:: %s \n" % (len(actual_result), len(expected_result), extra_msg))
        if not self._result_comparison_analysis(actual_result, expected_result):
            msg = "The number of rows match but the results mismatch, please check"
            extra_msg = self._get_failure_message(expected_result, actual_result)
            raise Exception(msg+"\n "+extra_msg)

    def _get_failure_message(self, expected_result, actual_result):
        if expected_result is None:
            expected_result = []
        if actual_result is None:
            actual_result = []
        len_expected_result = len(expected_result)
        len_actual_result = len(actual_result)
        len_expected_result = min(5, len_expected_result)
        len_actual_result = min(5, len_actual_result)
        extra_msg = "mismatch in results :: expected :: {0}, actual :: {1} ".format(expected_result[0:len_expected_result], actual_result[0:len_actual_result])
        return extra_msg

    def _result_comparison_analysis(self, expected_result, actual_result):
        expected_map = {}
        actual_map = {}
        for data in expected_result:
            primary=None
            for key in data.keys():
                keys = key
                if keys.encode('ascii') == "primary_key_id":
                    primary = keys
            expected_map[data[primary]] = data
        for data in actual_result:
            primary = None
            for key in data.keys():
                keys = key
                if keys.encode('ascii') == "primary_key_id":
                    primary = keys
            actual_map[data[primary]] = data
        check = True
        for key in expected_map.keys():
            if sorted(actual_map[key]) != sorted(expected_map[key]):
                check= False
        return check

    def _analyze_for_special_case_using_func(self, expected_result, actual_result):
        if expected_result is None:
            expected_result = []
        if actual_result is None:
            actual_result = []
        if len(expected_result) == 1:
            value = expected_result[0].values()[0]
            if value is None or value == 0:
                expected_result = []
        if len(actual_result) == 1:
            value = actual_result[0].values()[0]
            if value is None or value == 0:
                actual_result = []
        return expected_result, actual_result

    def _is_function_in_result(self, result):
        if result == "FUN":
            return True
        return False

    def _verify_results_rqg_for_function(self, n1ql_result=[], sql_result=[], hints=["a1"], aggregate_pushdown=False):
        if not aggregate_pushdown:
            sql_result, n1ql_result = self._analyze_for_special_case_using_func(sql_result, n1ql_result)
        if len(sql_result) != len(n1ql_result):
            msg = "the number of results do not match :: sql = {0}, n1ql = {1}".format(len(sql_result), len(n1ql_result))
            extra_msg = self._get_failure_message(sql_result, n1ql_result)
            raise Exception(msg+"\n"+extra_msg)
        n1ql_result = self._gen_dict_n1ql_func_result(n1ql_result)
        n1ql_result = sorted(n1ql_result)
        sql_result = self._gen_dict_n1ql_func_result(sql_result)
        sql_result = sorted(sql_result)
        if len(sql_result) == 0 and len(n1ql_result) == 0:
            return
        if sql_result != n1ql_result:
            i = 0
            for sql_value, n1ql_value in zip(sql_result, n1ql_result):
                if sql_value != n1ql_value:
                    break
                i = i + 1
            num_results = len(sql_result)
            last_idx = min(i+5, num_results)
            msg = "mismatch in results :: result length :: {3}, first mismatch position :: {0}, sql value :: {1}, n1ql value :: {2} ".format(i, sql_result[i:last_idx], n1ql_result[i:last_idx], num_results)
            raise Exception(msg)

    def _convert_to_number(self, val):
        if not isinstance(val, str):
            return val
        value = -1
        try:
            if value == '':
                return 0
            value = int(val.split("(")[1].split(")")[0])
        except Exception as ex:
            self.log.info(ex)
        finally:
            return value

    def analyze_failure(self, actual, expected):
        missing_keys = []
        different_values = []
        for key in expected.keys():
            if key not in actual.keys():
                missing_keys.append(key)
            if expected[key] != actual[key]:
                different_values.append("for key {0}, expected {1} \n actual {2}".format(key, expected[key], actual[key]))
        self.log.info(missing_keys)
        if len(different_values) > 0:
            self.log.info(" number of such cases {0}".format(len(different_values)))
            self.log.info(" example key {0}".format(different_values[0]))

    def check_missing_and_extra(self, actual, expected):
        missing = []
        extra = []
        for item in actual:
            if not (item in expected):
                extra.append(item)
        for item in expected:
            if not (item in actual):
                missing.append(item)
        return missing, extra

    def build_url(self, version):
        info = self.shell.extract_remote_info()
        type = info.distribution_type.lower()
        if type in ["ubuntu", "centos", "red hat"]:
            url = "https://s3.amazonaws.com/packages.couchbase.com/releases/couchbase-query/dp1/"
            url += "couchbase-query_%s_%s_linux.tar.gz" % (version, info.architecture_type)
        # TODO for windows
        return url

    def _restart_indexer(self):
        couchbase_path = "/opt/couchbase/var/lib/couchbase"
        cmd = "rm -f {0}/meta;rm -f /tmp/log_upr_client.sock".format(couchbase_path)
        self.shell.execute_command(cmd)

    def _start_command_line_query(self, server):
        self.shell = RemoteMachineShellConnection(server)
        self._set_env_variable(server)
        if self.version == "git_repo":
            os = self.shell.extract_remote_info().type.lower()
            if os != Windows.NAME:
                gopath = Linux.GOPATH
            else:
                gopath = Windows.GOPATH
            if self.input.tuq_client and "gopath" in self.input.tuq_client:
                gopath = self.input.tuq_client["gopath"]
            if os == 'windows':
                cmd = "cd %s/src/github.com/couchbase/query/server/main; " % (gopath) +\
                "./cbq-engine.exe -datastore http://%s:%s/ >/dev/null 2>&1 &" % (server.ip, server.port)
            else:
                cmd = "cd %s/src/github.com/couchbase/query//server/main; " % (gopath) +\
                "./cbq-engine -datastore http://%s:%s/ >n1ql.log 2>&1 &" % (server.ip, server.port)
            self.shell.execute_command(cmd)
        elif self.version == "sherlock":
            os = self.shell.extract_remote_info().type.lower()
            if os != Windows.NAME:
                couchbase_path = Linux.COUCHBASE_BIN_PATH
            else:
                couchbase_path = Windows.COUCHBASE_BIN_PATH
            if self.input.tuq_client and "sherlock_path" in self.input.tuq_client:
                couchbase_path = "%s/bin" % self.input.tuq_client["sherlock_path"]
                print("PATH TO SHERLOCK: %s" % couchbase_path)
            if os == Windows.NAME:
                cmd = "cd %s; " % (couchbase_path) +\
                "./cbq-engine.exe -datastore http://%s:%s/ >/dev/null 2>&1 &" % (server.ip, server.port)
            else:
                cmd = "cd %s; " % (couchbase_path) +\
                "./cbq-engine -datastore http://%s:%s/ >n1ql.log 2>&1 &" % (server.ip, server.port)
                n1ql_port = self.input.param("n1ql_port", None)
                if server.ip == "127.0.0.1" and server.n1ql_port:
                    n1ql_port = server.n1ql_port
                if n1ql_port:
                    cmd = "cd %s; " % (couchbase_path) +\
                './cbq-engine -datastore http://%s:%s/ -http=":%s">n1ql.log 2>&1 &' % (server.ip, server.port, n1ql_port)
            self.shell.execute_command(cmd)
        else:
            os = self.shell.extract_remote_info().type.lower()
            if os != Windows.NAME:
                cmd = "cd /tmp/tuq;./cbq-engine -couchbase http://%s:%s/ >/dev/null 2>&1 &" % (server.ip, server.port)
            else:
                cmd = "cd /cygdrive/c/tuq;./cbq-engine.exe -couchbase http://%s:%s/ >/dev/null 2>&1 &" % (server.ip, server.port)
            self.shell.execute_command(cmd)
        self.shell.disconnect()

    def _parse_query_output(self, output):
        if output.find("cbq>") == 0:
            output = output[output.find("cbq>") + 4:].strip()
        if output.find("tuq_client>") == 0:
            output = output[output.find("tuq_client>") + 11:].strip()
        if output.find("cbq>") != -1:
            output = output[:output.find("cbq>")].strip()
        if output.find("tuq_client>") != -1:
            output = output[:output.find("tuq_client>")].strip()
        return json.loads(output)

    def sort_nested_list(self, result):
        actual_result = []
        for item in result:
            curr_item = {}
            for key, value in item.iteritems():
                if isinstance(value, list) or isinstance(value, set):
                    curr_item[key] = sorted(value)
                else:
                    curr_item[key] = value
            actual_result.append(curr_item)
        return actual_result

    def configure_gomaxprocs(self):
        max_proc = self.input.param("gomaxprocs", None)
        cmd = "export GOMAXPROCS=%s" % max_proc
        for server in self.servers:
            shell_connection = RemoteMachineShellConnection(server)
            shell_connection.execute_command(cmd)
            shell_connection.disconnect()

    def drop_primary_index(self, using_gsi = True, server = None):
        if server is None:
            server = self.server
        self.log.info("CHECK FOR PRIMARY INDEXES")
        for bucket in self.buckets:
            self.query = "DROP PRIMARY INDEX ON {0}".format(bucket.name)
            if using_gsi:
                self.query += " USING GSI"
            if not using_gsi:
                self.query += " USING VIEW "
            self.log.info(self.query)
            try:
                check = self._is_index_in_list(bucket.name, "#primary", server = server)
                if check:
                    self.run_cbq_query(query=self.query, server=server)
            except Exception as ex:
                self.log.error('ERROR during index creation %s' % str(ex))

    def create_primary_index(self, using_gsi=True, server=None):
        if server is None:
            server = self.server
        for bucket in self.buckets:
            self.query = "CREATE PRIMARY INDEX ON %s " % bucket.name
            if using_gsi:
                self.query += " USING GSI"
            if not using_gsi:
                self.query += " USING VIEW "
            if self.use_rest:
                try:
                    check = self._is_index_in_list(bucket.name, "#primary", server = server)
                    if not check:
                        self.run_cbq_query(query=self.query,server = server,query_params={'timeout' : '900s'})
                        check = self.is_index_online_and_in_list(bucket.name, "#primary", server = server)
                        if not check:
                            raise Exception(" Timed-out Exception while building primary index for bucket {0} !!!".format(bucket.name))
                    else:
                        raise Exception(" Primary Index Already present, This looks like a bug !!!")
                except Exception as ex:
                    self.log.error('ERROR during index creation %s' % str(ex))
                    raise ex

    def create_partitioned_primary_index(self, using_gsi=True, server=None):
        if server is None:
            server = self.server
        for bucket in self.buckets:
            self.query = "CREATE PRIMARY INDEX ON %s " % bucket.name
            if using_gsi:
                self.query += " PARTITION BY HASH(meta().id) USING GSI"
            if not using_gsi:
                self.query += " USING VIEW "
            if self.use_rest:
                try:
                    check = self._is_index_in_list(bucket.name, "#primary",
                                                   server=server)
                    if not check:
                        self.run_cbq_query(server=server,
                                           query_params={'timeout': '900s'})
                        check = self.is_index_online_and_in_list(bucket.name,
                                                                 "#primary",
                                                                 server=server)
                        if not check:
                            raise Exception(
                                " Timed-out Exception while building primary index for bucket {0} !!!".format(
                                    bucket.name))
                    else:
                        raise Exception(
                            " Primary Index Already present, This looks like a bug !!!")
                except Exception as ex:
                    self.log.error('ERROR during index creation %s' % str(ex))
                    raise ex

    def verify_index_with_explain(self, actual_result, index_name, check_covering_index=False):
        check = True
        if check_covering_index:
            if "covering" in str(actual_result):
                check = True
            else:
                check = False
        if index_name in str(actual_result):
            return True and check
        return False

    def run_query_and_verify_result(self, server=None, query=None, timeout=120.0, max_try=1, expected_result=None,
                                    scan_consistency=None, scan_vector=None, verify_results=True):
        check = False
        init_time = time.time()
        try_count = 0
        while not check:
            next_time = time.time()
            try:
                actual_result = self.run_cbq_query(query=query, server=server, scan_consistency=scan_consistency,
                                                   scan_vector=scan_vector)
                if verify_results:
                    self._verify_results(sorted(actual_result['results']), sorted(expected_result))
                else:
                    return "ran query with success and validated results", True
                check = True
            except Exception as ex:
                if next_time - init_time > timeout or try_count >= max_try:
                    return ex, False
            finally:
                try_count += 1
        return "ran query with success and validated results", check

    def get_index_names(self, server=None):
        query = "select distinct(name) from system:indexes where `using`='gsi'"
        index_names = []
        if server is None:
            server = self.server
        res = self.run_cbq_query(query=query, server=server)
        for item in res['results']:
            index_names.append(item['name'])
        return index_names

    def is_index_online_and_in_list(self, bucket, index_name, server=None,
                                    timeout=600.0):
        check = self._is_index_in_list(bucket, index_name, server=server)
        init_time = time.time()
        while not check:
            # Wait before checking index_in_list
            sleep(1)
            check = self._is_index_in_list(bucket, index_name, server=server)
            next_time = time.time()
            if check or (next_time - init_time > timeout):
                return check
        return check

    def is_index_ready_and_in_list(self, bucket, index_name, server=None,
                                   timeout=600.0):
        query = "SELECT * FROM system:indexes where name = \'%s\'" % index_name
        if server is None:
            server = self.server
        init_time = time.time()
        check = False
        while not check:
            res = self.run_cbq_query(query=query, server=server)
            for item in res['results']:
                if 'keyspace_id' not in item['indexes']:
                    check = False
                elif item['indexes']['keyspace_id'] == str(bucket) \
                        and item['indexes']['name'] == index_name \
                        and item['indexes']['state'] == "online":
                    check = True
            # Wait before running cbq_query again
            sleep(1)
            next_time = time.time()
            check = check or (next_time - init_time > timeout)
        return check

    def is_index_online_and_in_list_bulk(self, bucket, index_names=[], server=None, index_state="online", timeout=600.0):
        check, index_names = self._is_index_in_list_bulk(bucket, index_names, server=server, index_state=index_state)
        init_time = time.time()
        while not check:
            check, index_names = self._is_index_in_list_bulk(bucket, index_names, server=server, index_state=index_state)
            next_time = time.time()
            if check or (next_time - init_time > timeout):
                return check
        return check

    def gen_build_index_query(self, bucket="default", index_list=[]):
        return "BUILD INDEX on {0}({1}) USING GSI".format(bucket, ",".join(index_list))

    def gen_query_parameter(self, scan_vector=None, scan_consistency=None):
        query_params = {}
        if scan_vector:
            query_params.update("scan_vector", scan_vector)
        if scan_consistency:
            query_params.update("scan_consistency", scan_consistency)
        return query_params

    def _is_index_in_list(self, bucket, index_name, server=None, index_state=["pending", "building", "deferred"]):
        query = "SELECT * FROM system:indexes where name = \'{0}\'".format(index_name)
        if server is None:
            server = self.server
        res = self.run_cbq_query(query=query, server=server)
        for item in res['results']:
            if 'keyspace_id' not in item['indexes']:
                return False
            if item['indexes']['keyspace_id'] == str(bucket) and item['indexes']['name'] == index_name and item['indexes']['state'] not in index_state:
                return True
        return False

    def _is_index_in_list_bulk(self, bucket, index_names=[], server=None, index_state=["pending","building"]):
        query = "SELECT * FROM system:indexes"
        if server is None:
            server = self.server
        res = self.run_cbq_query(query=query, server=server)
        found_index_list = []
        for item in res['results']:
            if 'keyspace_id' not in item['indexes']:
                return False
            for index_name in index_names:
                if item['indexes']['keyspace_id'] == str(bucket) and item['indexes']['name'] == index_name and item['indexes']['state'] not in index_state:
                    found_index_list.append(index_name)
        if len(found_index_list) == len(index_names):
            return True, []
        return False, list(set(index_names) - set(found_index_list))

    def gen_index_map(self, server=None):
        query = "SELECT * FROM system:indexes"
        if server is None:
            server = self.server
        res = self.run_cbq_query(query=query, server=server)
        index_map = {}
        for item in res['results']:
            bucket_name = item['indexes']['keyspace_id'].encode('ascii', 'ignore')
            if bucket_name not in index_map.keys():
                index_map[bucket_name] = {}
            index_name = str(item['indexes']['name'])
            index_map[bucket_name][index_name] = {}
            index_map[bucket_name][index_name]['state'] = item['indexes']['state']
        return index_map

    def get_index_count_using_primary_index(self, buckets, server=None):
        query = "SELECT COUNT(*) FROM {0}"
        result_map = dict()
        if server is None:
            server = self.server
        for bucket in buckets:
            res = self.run_cbq_query(query=query.format(bucket.name), server=server)
            result_map[bucket.name] = int(res["results"][0]["$1"])
        return result_map

    def get_index_count_using_index(self, bucket, index_name, server=None):
        query = 'SELECT COUNT(*) FROM {0} USE INDEX ({1})'.format(bucket.name, index_name)
        if not server:
            server = self.server
        res = self.run_cbq_query(query=query, server=server)
        return int(res['results'][0]['$1'])

    def _gen_dict(self, result):
        result_set = []
        if result is not None and len(result) > 0:
            for val in result:
                for key in val.keys():
                    result_set.append(val[key])
        return result_set

    def _gen_dict_n1ql_func_result(self, result):
        result_set = [val[key] for val in result for key in val.keys()]
        new_result_set = []
        if len(result_set) > 0:
            for value in result_set:
                if isinstance(value, float):
                    new_result_set.append(round(value, 0))
                elif value == 'None':
                    new_result_set.append(None)
                else:
                    new_result_set.append(value)
        else:
            new_result_set = result_set
        return new_result_set

    def _check_sample(self, result, expected_in_key=None):
        if expected_in_key == "FUN":
            return False
        if expected_in_key is None or len(expected_in_key) == 0:
            return False
        if result is not None and len(result) > 0:
            sample = result[0]
            for key in sample.keys():
                for sample in expected_in_key:
                    if key in sample:
                        return True
        return False

    def old_gen_dict(self, result):
        result_set = []
        map = {}
        duplicate_keys = []
        try:
            if result is not None and len(result) > 0:
                for val in result:
                    for key in val.keys():
                        result_set.append(val[key])
            for val in result_set:
                if val["_id"] in map.keys():
                    duplicate_keys.append(val["_id"])
                map[val["_id"]] = val
            keys = map.keys()
            keys.sort()
        except Exception as ex:
            self.log.info(ex)
            raise
        if len(duplicate_keys) > 0:
            raise Exception(" duplicate_keys {0}".format(duplicate_keys))
        return map

    def _set_env_variable(self, server):
        self.shell.execute_command("export NS_SERVER_CBAUTH_URL=\"http://{0}:{1}/_cbauth\"".format(server.ip, server.port))
        self.shell.execute_command("export NS_SERVER_CBAUTH_USER=\"{0}\"".format(server.rest_username))
        self.shell.execute_command("export NS_SERVER_CBAUTH_PWD=\"{0}\"".format(server.rest_password))
        self.shell.execute_command("export NS_SERVER_CBAUTH_RPC_URL=\"http://{0}:{1}/cbauth-demo\"".format(server.ip, server.port))
        self.shell.execute_command("export CBAUTH_REVRPC_URL=\"http://{0}:{1}@{2}:{3}/query\"".format(server.rest_username, server.rest_password, server.ip, server.port))

    def verify_indexes_redistributed(self, map_before_rebalance, map_after_rebalance, stats_map_before_rebalance,
                                     stats_map_after_rebalance, nodes_in, nodes_out, swap_rebalance=False):
        # verify that number of indexes before and after rebalance are same
        no_of_indexes_before_rebalance = 0
        no_of_indexes_after_rebalance = 0
        for bucket in map_before_rebalance:
            no_of_indexes_before_rebalance += len(map_before_rebalance[bucket])
        for bucket in map_after_rebalance:
            no_of_indexes_after_rebalance += len(map_after_rebalance[bucket])
        self.log.info("Number of indexes before rebalance : {0}".format(no_of_indexes_before_rebalance))
        self.log.info("Number of indexes after rebalance  : {0}".format(no_of_indexes_after_rebalance))
        if no_of_indexes_before_rebalance != no_of_indexes_after_rebalance:
            self.log.info("some indexes are missing after rebalance")
            raise Exception("some indexes are missing after rebalance")

        # verify that index names before and after rebalance are same
        index_names_before_rebalance = []
        index_names_after_rebalance = []
        for bucket in map_before_rebalance:
            for index in map_before_rebalance[bucket]:
                index_names_before_rebalance.append(index)
        for bucket in map_after_rebalance:
            for index in map_after_rebalance[bucket]:
                index_names_after_rebalance.append(index)
        self.log.info("Index names before rebalance : {0}".format(sorted(index_names_before_rebalance)))
        self.log.info("Index names after rebalance  : {0}".format(sorted(index_names_after_rebalance)))
        if sorted(index_names_before_rebalance) != sorted(index_names_after_rebalance):
            self.log.info("number of indexes are same but index names don't match")
            raise Exception("number of indexes are same but index names don't match")

        # verify that rebalanced out nodes are not present
        host_names_before_rebalance = []
        host_names_after_rebalance = []
        for bucket in map_before_rebalance:
            for index in map_before_rebalance[bucket]:
                host_names_before_rebalance.append(map_before_rebalance[bucket][index]['hosts'])
        indexer_nodes_before_rebalance = sorted(set(host_names_before_rebalance))
        for bucket in map_after_rebalance:
            for index in map_after_rebalance[bucket]:
                host_names_after_rebalance.append(map_after_rebalance[bucket][index]['hosts'])
        indexer_nodes_after_rebalance = sorted(set(host_names_after_rebalance))
        self.log.info("Host names of indexer nodes before rebalance : {0}".format(indexer_nodes_before_rebalance))
        self.log.info("Host names of indexer nodes after rebalance  : {0}".format(indexer_nodes_after_rebalance))
        # indexes need to redistributed in case of rebalance out, not necessarily in case of rebalance in
        if nodes_out and indexer_nodes_before_rebalance == indexer_nodes_after_rebalance:
            self.log.info("Even after rebalance some of rebalanced out nodes still have indexes")
            raise Exception("Even after rebalance some of rebalanced out nodes still have indexes")
        for node_out in nodes_out:
            if node_out in indexer_nodes_after_rebalance:
                self.log.info("rebalanced out node still present after rebalance {0} : {1}".format(node_out,
                                                                                                   indexer_nodes_after_rebalance))
                raise Exception("rebalanced out node still present after rebalance")
        if swap_rebalance:
            for node_in in nodes_in:
                # strip of unnecessary data for comparison
                ip_address = str(node_in).replace("ip:", "").replace(" port", "").replace(" ssh_username:root", "").replace(" ssh_username:Administrator", "")
                if ip_address not in indexer_nodes_after_rebalance:
                    self.log.info("swap rebalanced in node is not distributed any indexes")
                    raise Exception("swap rebalanced in node is not distributed any indexes")

        # verify that items_count before and after rebalance are same
        items_count_before_rebalance = {}
        items_count_after_rebalance = {}
        for bucket in stats_map_before_rebalance:
            for index in stats_map_before_rebalance[bucket]:
                items_count_before_rebalance[index] = stats_map_before_rebalance[bucket][index][
                    "items_count"]
        for bucket in stats_map_after_rebalance:
            for index in stats_map_after_rebalance[bucket]:
                items_count_after_rebalance[index] = stats_map_after_rebalance[bucket][index]["items_count"]
        self.log.info("item_count of indexes before rebalance {0}".format(items_count_before_rebalance))
        self.log.info("item_count of indexes after rebalance {0}".format(items_count_after_rebalance))
        if cmp(items_count_before_rebalance, items_count_after_rebalance) != 0:
            self.log.info("items_count mismatch")
            raise Exception("items_count mismatch")

        # verify that index status before and after rebalance are same
        index_state_before_rebalance = {}
        index_state_after_rebalance = {}
        for bucket in map_before_rebalance:
            for index in map_before_rebalance[bucket]:
                index_state_before_rebalance[index] = map_before_rebalance[bucket][index]["status"]
        for bucket in map_after_rebalance:
            for index in map_after_rebalance[bucket]:
                index_state_after_rebalance[index] = map_after_rebalance[bucket][index]["status"]
        self.log.info("index status of indexes rebalance {0}".format(index_state_before_rebalance))
        self.log.info("index status of indexes rebalance {0}".format(index_state_after_rebalance))
        if cmp(index_state_before_rebalance, index_state_after_rebalance) != 0:
            self.log.info("index status mismatch")
            raise Exception("index status mismatch")

        # Rebalance is not guaranteed to achieve a balanced cluster.
        # The indexes will be distributed in a manner to satisfy the resource requirements of each index.
        # Hence printing the index distribution just for logging/debugging purposes
        index_distribution_map_before_rebalance = {}
        index_distribution_map_after_rebalance = {}
        for node in host_names_before_rebalance:
            index_distribution_map_before_rebalance[node] = index_distribution_map_before_rebalance.get(node, 0) + 1
        for node in host_names_after_rebalance:
            index_distribution_map_after_rebalance[node] = index_distribution_map_after_rebalance.get(node, 0) + 1
        self.log.info("Distribution of indexes before rebalance")
        for k, v in index_distribution_map_before_rebalance.iteritems():
            print(k, v)
        self.log.info("Distribution of indexes after rebalance")
        for k, v in index_distribution_map_after_rebalance.iteritems():
            print(k, v)

    def verify_replica_indexes(self, index_names, index_map, num_replicas, expected_nodes=None):
        # 1. Validate count of no_of_indexes
        # 2. Validate index names
        # 3. Validate index replica have the same id
        # 4. Validate index replicas are on different hosts

        nodes = []
        for index_name in index_names:
            index_host_name, index_id = self.get_index_details_using_index_name(index_name, index_map)
            nodes.append(index_host_name)

            for i in range(0, num_replicas):
                index_replica_name = index_name + " (replica {0})".format(str(i+1))

                try:
                    index_replica_hostname, index_replica_id = self.get_index_details_using_index_name(
                        index_replica_name, index_map)
                except Exception as ex:
                    self.log.info(str(ex))
                    raise Exception(str(ex))

                self.log.info("Hostnames : %s , %s" % (index_host_name, index_replica_hostname))
                self.log.info("Index IDs : %s, %s" % (index_id, index_replica_id))

                nodes.append(index_replica_hostname)

                if index_id != index_replica_id:
                    self.log.info("Index ID for main index and replica indexes not same")
                    raise Exception("index id different for replicas")

                if index_host_name == index_replica_hostname:
                    self.log.info("Index hostname for main index and replica indexes are same")
                    raise Exception("index hostname same for replicas")

        if expected_nodes:
            expected_nodes = expected_nodes.sort()
            nodes = nodes.sort()
            if not expected_nodes == nodes:
                self.fail("Replicas not created on expected hosts")

    def verify_replica_indexes_build_status(self, index_map, num_replicas, defer_build=False):

        index_names = self.get_index_names()

        for index_name in index_names:
            index_status, index_build_progress = self.get_index_status_using_index_name(index_name, index_map)
            if not defer_build and index_status != "Ready":
                self.log.info("Expected %s status to be Ready, but it is %s" % (index_name, index_status))
                raise Exception("Index status incorrect")
            elif defer_build and index_status != "Created":
                self.log.info(
                    "Expected %s status to be Created, but it is %s" % (index_name, index_status))
                raise Exception("Index status incorrect")
            else:
                self.log.info("index_name = %s, defer_build = %s, index_status = %s" % (index_name, defer_build, index_status))

            for i in range(1, num_replicas+1):
                index_replica_name = index_name + " (replica {0})".format(str(i))
                try:
                    index_replica_status, index_replica_progress = self.get_index_status_using_index_name(index_replica_name, index_map)
                except Exception as ex:
                    self.log.info(str(ex))
                    raise Exception(str(ex))

                if not defer_build and index_replica_status != "Ready":
                    self.log.info("Expected %s status to be Ready, but it is %s" % (index_replica_name, index_replica_status))
                    raise Exception("Index status incorrect")
                elif defer_build and index_replica_status != "Created":
                    self.log.info("Expected %s status to be Created, but it is %s" % (index_replica_name, index_replica_status))
                    raise Exception("Index status incorrect")
                else:
                    self.log.info("index_name = %s, defer_build = %s, index_replica_status = %s" % (index_replica_name, defer_build, index_status))

    def get_index_details_using_index_name(self, index_name, index_map):
        for key in index_map.iterkeys():
            if index_name in index_map[key].keys():
                return index_map[key][index_name]['hosts'], index_map[key][index_name]['id']
            else:
                raise Exception ("Index does not exist - {0}".format(index_name))

    def get_index_status_using_index_name(self, index_name, index_map):
        for key in index_map.iterkeys():
            if index_name in index_map[key].keys():
                return index_map[key][index_name]['status'], \
                       index_map[key][index_name]['progress']
            else:
                raise Exception("Index does not exist - {0}".format(index_name))
