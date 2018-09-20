from cbas.cbas_base import CBASBaseTest
from remote.remote_util import RemoteMachineShellConnection
from sdk_client import SDKClient


class CBASLimitPushdown(CBASBaseTest):

    def setUp(self):
        super(CBASLimitPushdown, self).setUp()

        self.log.info("Create a reference to SDK client")
        client = SDKClient(hosts=[self.master.ip], bucket=self.cb_bucket_name, password=self.master.rest_password)

        self.log.info("Insert documents in KV bucket")
        documents = [
            '{"name":"dave","age":19,"gender":"Male","salary":50.0, "married":false, "employed":""}',
            '{"name":"evan","age":25,"gender":"Female","salary":100.15, "married":true}',
            '{"name":"john","age":44,"gender":"Male","salary":150.55, "married":null}',
            '{"name": "sara", "age": 20, "gender": "Female", "salary": 200.34, "married":false}',
            '{"name":"tom","age":31,"gender":"Male","salary":250.99, "married":true}'
        ]
        client.insert_json_documents("id-", documents)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Create primary index on KV bucket")
        self.rest.query_tool("create primary index pri_idx on default")

        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Connect to Local link")
        self.cbas_util.connect_link()

        self.log.info("Wait for ingestion to complete")
        self.total_documents = self.rest.query_tool(CBASLimitQueries.BUCKET_COUNT_QUERY)['results'][0]['$1']
        self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name], self.total_documents)

        self.log.info("Fetch partitions")
        shell = RemoteMachineShellConnection(self.cbas_node)
        response = self.cbas_util.fetch_analytics_cluster_response(shell)
        if 'partitions' in response:
            self.partitions = len(response['partitions'])
            self.log.info("Number of data partitions on cluster %d" % self.partitions)
        else:
            self.fail(msg="Partitions not found. Failing early to avoid unexpected results")
    
    def run_queries_and_assert_results(self):
        
        query_errors = {}
        query_count = 0
        for query_object in CBASLimitQueries.LIMIT_QUERIES:
            query_count += 1
            self.log.info("-------------------------------------------- Running Query:%d ----------------------------------------------------------------------" % query_count)
            self.log.info("%s : %s" % (query_object['id'], query_object['query']))

            try:
                self.log.info("Execute query on N1QL")
                n1ql_result = self.rest.query_tool(query_object["query"])['results']

                self.log.info("Execute query on CBAS")
                status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util(query_object["query"])
                self.assertEquals(status, "success", msg="CBAS query failed")

                self.log.info("Assert on query statistics")
                if query_object['id'] != "limit":
                    if "skip_processed_count" not in query_object: 
                        self.assertTrue(metrics["processedObjects"] <= self.total_documents, msg="Processed object must be <= total documents. Actual %s" % (metrics["processedObjects"]))
                    else:
                        self.log.info("Skipping process object count check.In cases of sub query processed object count can be greater than total document count.")
                else:
                    self.assertEqual(self.partitions * query_object["limit_value"], metrics["processedObjects"], 
                                     msg="Processed Object count mismatch. Actual %s Expected %s" %(metrics["processedObjects"], self.partitions * query_object["limit_value"]))

                self.log.info("Assert query result")
                self.assertEqual(n1ql_result, cbas_result, msg="Query result mismatch.\n n1ql:%s \n cbas:%s " % (n1ql_result, cbas_result))

            except Exception as e:
                query_errors[query_object['id']] = e
            self.log.info("-------------------------------------------- Completed ------------------------------------------------------------------------------")

        self.log.info("LIMIT pushdown result summary")
        self.log.info("Total:%d Passed:%d Failed:%d" % (query_count, query_count-len(query_errors), len(query_errors)))
        
        if query_errors:
            for id in query_errors:
                self.log.info("******************************************** Failure summary ********************************************")
                self.log.info("%s" % id)
                self.log.info(query_errors[id])
            self.fail("Failing the test with above errors")
        
    """
    cbas.cbas_limit_pushdown.CBASLimitPushdown.test_cbas_limit_pushdown,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=default
    """
    def test_cbas_limit_pushdown(self):
                
        self.log.info("Execute LIMIT queries")
        self.run_queries_and_assert_results()
    
    """
    cbas.cbas_limit_pushdown.CBASLimitPushdown.test_cbas_limit_pushdown_with_index,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=default
    """
    def test_cbas_limit_pushdown_with_index(self):

        self.log.info("Disconnect to Local link")
        self.cbas_util.disconnect_link()

        self.log.info("Create secondary index")
        status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("create index idx_age on default(age:int)")
        self.assertEquals(status, "success", msg="Create secondary index on age failed")

        status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("create index idx_name on default(name:String)")
        self.assertEquals(status, "success", msg="Create secondary index on name failed")

        status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("create index idx_gender on default(gender:String)")
        self.assertEquals(status, "success", msg="Create secondary index gender failed")

        self.log.info("Connect to Local link")
        self.cbas_util.connect_link()
        
        self.log.info("Execute LIMIT queries")
        self.run_queries_and_assert_results()

    """
    cbas.cbas_limit_pushdown.CBASLimitPushdown.test_query_plan,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=default
    """
    def test_query_plan(self):

        self.log.info("Execute query fetch plan json")
        status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("explain select * from default limit 1")

        print(cbas_result)


    def tearDown(self):
        super(CBASLimitPushdown, self).tearDown()


class CBASLimitQueries:
    BUCKET_COUNT_QUERY = 'select count(*) from default'
    LIMIT_QUERIES = [
        {
            'id': 'limit',
            'query': 'select * from default limit 2',
            'limit_value': 2
        },
        {
            'id': 'limit+where+single+equals',
            'query': 'select * from default where age = 25 limit 1'
        },
        {
            'id': 'limit+where+not+equals',
            'query': 'select * from default where gender <> "Female" limit 2'
        },
        {
            'id': 'limit+where+greater+than',
            'query': 'select * from default where name > "f" limit 2'
        },
        {
            'id': 'limit+where+greater+than+equals',
            'query': 'select * from default where salary >= 100 limit 2'
        },
        {
            'id': 'limit+where+less+than',
            'query': 'select * from default where name < "f" limit 2'
        },
        {
            'id': 'limit+where+less+than+equals',
            'query': 'select * from default where salary <= 200.33 limit 3'
        },
        {
            'id': 'limit+between',
            'query': 'select * from default where age between 20 and 30 limit 1'
        },
        {
            'id': 'limit+not+between',
            'query': 'select * from default where age not between 19 and 30 limit 1'
        },
        {
            'id': 'limit+like',
            'query': 'select * from default where gender LIKE "%ale" limit 2'
        },
        {
            'id': 'limit+not+like',
            'query': 'select * from default where name not LIKE "%ave" limit 2'
        },
        {
            'id': 'limit+is+null',
            'query': 'select * from default where married is null limit 1'
        },
        {
            'id': 'limit+is+not+null',
            'query': 'select * from default where married is not null limit 3'
        },
        {
            'id': 'limit+is+missing',
            'query': 'select * from default where employed IS MISSING limit 2'
        },
        {
            'id': 'limit+is+not+missing',
            'query': 'select * from default where employed IS not MISSING limit 1'
        },
        {
            'id': 'limit+is+valued',
            'query': 'select * from default where married IS VALUED limit 3'
        },
        {
            'id': 'limit+is+not+valued',
            'query': 'select * from default where married IS NOT VALUED limit 1'
        },
        {
            'id': 'limit+where+and+different+keys',
            'query': 'select * from default where age >=20 and gender != "Male" limit 1'
        },
        {
            'id': 'limit+where+and+same+keys',
            'query': 'select * from default where age > 20 and age < 30 limit 1'
        },
        {
            'id': 'limit+where+or+different+keys',
            'query': 'select * from default where age >=20 or gender = "Male" limit 2'
        },
        {
            'id': 'limit+where+or+same+keys',
            'query': 'select * from default where age > 20 or age < 30 limit 1'
        },
        {
            'id': 'limit+where+or+and',
            'query': 'select * from default where age > 20 and gender = "Male" or married = "Female" limit 1'
        },
        {
            'id': 'limit+where+not',
            'query': 'select * from default where not married limit 2'
        },
        {
            'id': 'limit+in',
            'query': 'select * from default where name IN ["evan", "dave"] limit 1'
        },
        {
            'id': 'limit+equation',
            'query': 'select * from default limit 100/50'
        },
        {
            'id': 'limit+offset+same+value',
            'query': 'select * from default limit 2 offset 2'
        },
        {
            'id': 'limit+offset+different+value',
            'query': 'select * from default limit 2 offset 1'
        },
        {
            'id': 'limit+offset+negative',
            'query': 'select * from default limit 2 offset -3'
        },
        {
            'id': 'limit+negative',
            'query': 'select * from default limit -10'
        },
        {
            'id': 'limit+distinct',
            'query': 'select distinct * from default order by name limit 1'
        },
        {
            'id': 'limit+greater+than+document+count',
            'query': 'select * from default limit 100'
        },
        {
            'id': 'limit+offset+greater+than+document+count',
            'query': 'select * from default limit 1 offset 6'
        },
        {
            'id': 'limit+group+by+having',
            'query': 'select gender, sum(salary) from default group by gender order by gender desc limit 1'
        },
        {
            'id': 'limit+order+by',
            'query': 'select * from default order by name desc limit 2'
        },
        {
            'id': 'limit+select+where+on+same+key',
            'query': 'select name from default where name = "dave" limit 1'
        },
        {
            'id': 'limit+sub+query',
            'query': 'select name from default d where d.gender = (select RAW gender from default limit 1)[0] order by name limit 2',
            'skip_processed_count':True
        }
    ]

    def __init__(self):
        pass
