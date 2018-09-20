import time

from Rbac_utils.Rbac_ready_functions import rbac_utils
from cbas.cbas_base import CBASBaseTest
from remote.remote_util import RemoteMachineShellConnection


class CBASErrorValidator(CBASBaseTest):

    def setUp(self):
        super(CBASErrorValidator, self).setUp()

        self.log.info("Read input param : error id")
        self.error_id = self.input.param('error_id', None)
        self.error_response = CBASError(self.error_id).get_error()
        self.log.info("Test to validate error response :\ %s" % self.error_response)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Establish remote connection to CBAS node")
        self.shell = RemoteMachineShellConnection(self.cbas_node)
        self.shell_kv = RemoteMachineShellConnection(self.master)
        self.cbas_url = "http://{0}:{1}/analytics/service".format(self.cbas_node.ip, 8095)

    def create_dataset_connect_link(self):
        self.log.info("Create dataset on the CBAS")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)
        
        self.log.info("Connect to Local link")
        self.cbas_util.connect_link()

    def validate_error_response(self, status, errors, expected_error, expected_error_code):
        if errors is None:
            return False
        return self.cbas_util.validate_error_in_response(status, errors, expected_error, expected_error_code)

    """
    test_error_response,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds
    """
    def test_error_response(self):

        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()

        self.log.info("Execute queries and validate error response")
        error_response_mismatch = []
        count = 0

        for error_object in (x for x in CBASError.errors if "run_in_loop" in x):
            count += 1
            self.log.info("-------------------------------------------- Running --------------------------------------------------------------------------------")
            self.log.info(error_object)

            if "param" in error_object:
                status, _, errors, cbas_result, _ = self.cbas_util.execute_parameter_statement_on_cbas_util(error_object["query"], parameters=error_object["param"])

            else:
                time_out = error_object["time_out"] if "time_out" in error_object else 120
                time_unit = error_object["time_unit"] if "time_unit" in error_object else "s"
                status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(error_object["query"], analytics_timeout=time_out, time_out_unit=time_unit)

            if not self.validate_error_response(status, errors, error_object["msg"], error_object["code"]):
                error_response_mismatch.append([error_object['id']])

            self.log.info("-------------------------------------------- Completed ------------------------------------------------------------------------------")

        self.log.info("Run summary")
        self.log.info("Total:%d Passed:%d Failed:%d" % (count, count - len(error_response_mismatch), len(error_response_mismatch)))
        if len(error_response_mismatch):
            self.log.info(error_response_mismatch)
            self.fail("Failing test error msg/code mismatch.")

    """
    test_error_response_index_name_exist,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=index_already_exist
    """
    def test_error_response_index_name_exist(self):

        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        self.log.info("Disconnect Local link")
        self.assertTrue(self.cbas_util.disconnect_from_bucket(), msg="Failed to disconnect connected bucket")
        
        self.log.info("Create a secondary index")
        self.assertTrue(self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"]), msg="Failed to create secondary index")

        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
    
    """
    test_error_response_user_permission,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=user_permission
    """
    def test_error_response_user_permission(self):
        
        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        self.log.info("Create a user with analytics reader role")
        rbac_util = rbac_utils(self.master)
        rbac_util._create_user_and_grant_role("reader_admin", "analytics_reader")

        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"], username="reader_admin", password="password")
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
    
    """
    test_error_response_user_unauthorized,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=user_unauthorized
    """
    def test_error_response_user_unauthorized(self):
        
        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        self.log.info("Create remote connection and execute cbas query using curl")
        output, _ = self.shell.execute_command("curl -X POST {0} -u {1}:{2}".format(self.cbas_url, "Administrator", "pass"))

        self.log.info("Execute query and validate error response")
        self.assertTrue(self.error_response["msg"] in str(output), msg="Error message mismatch")
        self.assertTrue(str(self.error_response["code"]) in str(output), msg="Error code mismatch")
    
    """
    test_error_response_connect_link_failed,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=connect_link_fail
    """
    def test_error_response_connect_link_failed(self):
        
        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        self.log.info("Delete KV bucket")
        self.delete_bucket_or_assert(serverInfo=self.master)

        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])

    """
    test_error_response_drop_dataverse,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=dataverse_drop_link_connected
    """
    def test_error_response_drop_dataverse(self):

        self.log.info("Create dataverse")
        status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("create dataverse custom")
        self.assertEquals(status, "success", msg="Create dataverse query failed")

        self.log.info("Use dataverse")
        status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("use custom")
        self.assertEquals(status, "success", msg="Use dataverse query failed")

        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()

        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
    
    """
    test_analytics_service_tmp_unavailable,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=service_unavailable
    """
    def test_analytics_service_tmp_unavailable(self):

        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()

        self.log.info("Kill Java process")
        self.shell.execute_command("pkill java")

        self.log.info("Wait until we get into the state analytics service is unavailable")
        service_unavailable = False
        cluster_recovery_time = time.time()
        while time.time() < cluster_recovery_time + 120:
            output, error = self.shell.execute_command("curl -X POST {0} -u {1}:{2} -d 'statement={3}'".format(self.cbas_url, "Administrator", "password", self.error_response["query"]))
            self.log.info(output)
            self.log.info(error)
            if self.error_response["msg"][0] in str(output):
                self.log.info("Hit service unavailable condition")
                service_unavailable = True
                break

        self.log.info("Validate error response")
        self.assertTrue(service_unavailable, msg="Failed to get into the state analytics service is unavailable")
        self.assertTrue(self.error_response["msg"] in str(output), msg="Error message mismatch")
        self.assertTrue(str(self.error_response["code"]) in str(output), msg="Error code mismatch")
    
    """
    test_error_response_rebalance_in_progress,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=rebalance_in_progress,items=10000
    """
    def test_error_response_rebalance_in_progress(self):

        self.log.info("Load documents in KV bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items, batch_size=5000)

        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()

        self.log.info("Assert document count")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg="Count mismatch on CBAS")

        self.log.info("Rebalance in a cbas node")
        self.add_node(self.cbas_servers[0], wait_for_rebalance_completion=False)

        self.log.info("Execute query and validate error response")
        start_time = time.time()
        while time.time() < start_time + 120:
            status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
            if errors is not None:
               break 
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])

    """
    test_error_response_using_curl,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=job_requirement
    """    
    def test_error_response_using_curl(self):
        
        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        self.log.info("Execute query using CURL")
        output, _ = self.shell.execute_command("curl -X POST {0} -u {1}:{2} -d 'statement={3}'".format(self.cbas_url, "Administrator", "password", self.error_response["query"]))
            
        self.assertTrue(self.error_response["msg"] in str(output), msg="Error message mismatch")
        self.assertTrue(str(self.error_response["code"]) in str(output), msg="Error code mismatch")

    """
    test_error_response_memcached_bucket,default_bucket=False,cb_bucket_name=default,error_id=memcached_bucket
    """
    def test_error_response_memcached_bucket(self):

        self.log.info("create memcached bucket")
        self.shell_kv.execute_command(
            "curl 'http://{0}:8091/pools/default/buckets' --data 'name={1}&bucketType=memcached&ramQuotaMB=100' -u Administrator:password".format(self.master.ip, self.cb_bucket_name))
        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])

    """
    test_error_response_index_not_found,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=index_not_found
    """
    def test_error_response_index_not_found(self):

        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()

        self.log.info("Disconnect Local link")
        self.assertTrue(self.cbas_util.disconnect_link(), msg="Failed to disconnect connected bucket")

        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])

    """
    test_error_response_max_writable_dataset_exceeded,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=max_writable_datasets
    """
    def test_error_response_max_writable_dataset_exceeded(self):

        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()

        self.log.info("Disconnect Local link")
        self.assertTrue(self.cbas_util.disconnect_from_bucket(), msg="Failed to disconnect Local link")

        self.log.info("Create 8 more datasets on CBAS bucket")
        for i in range(1, 9):
            self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name + str(i)),
                            msg="Create dataset %s failed" % self.cbas_dataset_name + str(i))

        self.log.info("Connect back Local link and verify error response for max dataset exceeded")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])

    """
    test_error_response_for_bucket_uuid_change,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=bucket_uuid_change
    """
    def test_error_response_for_bucket_uuid_change(self):

        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()

        self.log.info("Disconnect link")
        self.cbas_util.disconnect_link()

        self.log.info("Delete KV bucket")
        self.delete_bucket_or_assert(serverInfo=self.master)

        self.log.info("Recreate KV bucket")
        self.create_default_bucket()

        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])

    """
    test_error_response_no_statement,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=no_statement
    """
    def test_error_response_no_statement(self):

        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()

        self.log.info("Execute query on CBAS")
        output, _ = self.shell.execute_command("curl -X POST {0} -u {1}:{2} ".format(self.cbas_url, "Administrator", "password"))

        self.assertTrue(self.error_response["msg"] in str(output), msg="Error message mismatch")
        self.assertTrue(str(self.error_response["code"]) in str(output), msg="Error code mismatch")

    def tearDown(self):
        super(CBASErrorValidator, self).tearDown()


class CBASError:
    errors = [
        # Error codes starting with 20xxx
        {
            "id": "user_unauthorized",
            "msg": "Unauthorized user",
            "code": 20000,
            "query": "select count(*) from ds"
        },
        {
            "id": "user_permission",
            "msg": "User must have permission (cluster.bucket[default].analytics!manage)",
            "code": 20001,
            "query": "drop dataset ds"
        },
        # Error codes starting with 21xxx
        {
            "id": "invalid_duration",
            "msg": 'Invalid duration "tos"',
            "code": 21000,
            "query": "select sleep(count(*), 2000) from ds",
            "time_out":"to",
            "run_in_loop": True
        },
        {
            "id": "unknown_duration",
            "msg": 'Unknown duration unit M',
            "code": 21001,
            "query": "select sleep(count(*), 2000) from ds",
            "time_unit": "M",
            "run_in_loop": True
        },
        {
            "id": "request_timeout",
            "msg": "Request timed out and will be cancelled",
            "code": 21002,
            "query": "select sleep(count(*), 2000) from ds",
            "time_out": 1,
            "run_in_loop": True
        },
        {
            "id": "multiple_statements",
            "msg": "Unsupported multiple statements.",
            "code": 21003,
            "query": "create dataset ds1 on default;connect link Local",
            "run_in_loop": True
        },
        {
            "id": "no_statement",
            "msg": "No statement provided",
            "code": 21004,
            "query": ""
        },
        # Error codes starting with 22xxx
        {
            "id": "connect_link_failed",
            "msg": "Connect link failed",
            "code": 22001,
            "query": "connect link Local"
        },
        {
            "id": "bucket_uuid_change",
            "msg": 'Connect link failed {\"Default.Local.default\" : \"Bucket UUID has changed\"}',
            "code": 22001,
            "query": "connect link Local"
        },
        {
            "id": "max_writable_datasets",
            "msg": 'Connect link failed {\"Default.Local.default\" : \"Maximum number of active writable datasets (8) exceeded\"}',
            "code": 22001,
            "query": "connect link Local"
        },
        {
            "id": "memcached_bucket",
            "msg": "Memcached buckets are not supported",
            "code": 22003,
            "query": "create dataset ds on default"
        },
        # Error codes starting with 23XXX
        {
            "id": "service_unavailable",
            "msg": "Analytics Service is temporarily unavailable",
            "code": 23000,
            "query": "set `import-private-functions` `true`;ping()"
        },
        {
            "id": "rebalance_in_progress",
            "msg": "Operation cannot be performed during rebalance",
            "code": 23003,
            "query": "connect link Local"
        },
        {
            "id": "dataverse_drop_link_connected",
            "msg": "Dataverse Default cannot be dropped while link Local is connected",
            "code": 23005,
            "query": "drop dataverse custom"
        },
        {
            "id": "dataset_create_link_connected",
            "msg": "Dataset cannot be created on default while link Local is connected",
            "code": 23006,
            "query": "create dataset ds1 on default",
            "run_in_loop": True
        },
        {
            "id": "job_requirement",
            "msg": 'exceeds capacity (memory: ',
            "code": 23008,
            "query": 'SET `compiler.groupmemory` "10GB";select sleep(count(*),500) from ds GROUP BY name'
        },
        {
            "id": "overflow",
            "msg": 'Overflow in numeric-multiply',
            "code": 23011,
            "query": 'select 23000000000000000 * 23000000000000000',
            "run_in_loop": True
        },
        {
            "id": "compare_non_primitive",
            "msg": 'Cannot compare non-primitive values',
            "code": 23021,
            "query": "select ARRAY_INTERSECT([2011,2012], [[2011]])",
            "run_in_loop": True
        },
        {
            "id": "dataset_drop_link_connected",
            "msg": 'Dataset cannot be dropped while link Local is connected',
            "code": 23022,
            "query": "drop dataset ds",
            "run_in_loop": True
        },
        {
            "id": "limit_non_integer",
            "msg": 'Expected integer value, got 1.1',
            "code": 23025,
            "query": 'select "a" limit 1.1',
            "run_in_loop": True
        },
        # Error codes starting with 24XXX
        {
            "id": "syntax_error",
            "msg": "Syntax error:",
            "code": 24000,
            "query": "selec 1",
            "run_in_loop": True
        },
        {
            "id": "compilation_error",
            "msg": "Compilation error:",
            "code": 24001,
            "query": "select count(*) ds",
            "run_in_loop": True
        },
        {
            "id": "index_on_type",
            "msg": "Cannot index field [click] on type date. Supported types: bigint, double, string",
            "code": 24002,
            "query": "create index idx on ds(click:date)",
            "run_in_loop": True
        },
        {
            "id": "kv_bucket_does_not_exist",
            "msg": "Bucket (default1) does not exist",
            "code": 24003,
            "query": "create dataset ds1 on default1",
            "run_in_loop": True
        },
        {
            "id": "unsupported_statement",
            "msg": "Unsupported statement (UPSERT)",
            "code": 24004,
            "query": "UPSERT INTO ds (SELECT VALUE name FROM ds)",
            "run_in_loop": True
        },
        {
            "id": "disallow_user_functions",
            "msg": "Unsupported statement (CREATE_FUNCTION)",
            "code": 24004,
            "query": "create function default.fun01(){'Test'}",
            "run_in_loop": True
        },
        {
            "id": "drop_link_not_exist",
            "msg": "Link Default.Local1 does not exist",
            "code": 24006,
            "query": "drop link Local1",
            "run_in_loop": True
        },
        {
            "id": "drop_local_link",
            "msg": "Local link cannot be dropped",
            "code": 24007,
            "query": "drop link Local",
            "run_in_loop": True
        },
        {
            "id": "type_mismatch",
            "msg": "Type mismatch: function contains expects its 2nd input parameter to be of type string, but the actual input type is bigint",
            "code": 24011,
            "query": 'SELECT CONTAINS("N1QL is awesome", 123) as n1ql',
            "run_in_loop": True
        },
        {
            "id": "duplicate_field",
            "msg": 'Duplicate field name \"a\"',
            "code": 24015,
            "query": 'select 1 as a, 2 as a',
            "run_in_loop": True
        },
        {
            "id": "dataset_not_found",
            "msg": "Cannot find dataset with name Bucket in dataverse Default",
            "code": 24025,
            "query": 'drop index Bucket.Bucket',
            "run_in_loop": True
        },
        {
            "id": "dataverse_not_found",
            "msg": "Cannot find dataverse with name custom",
            "code": 24034,
            "query": 'use custom',
            "run_in_loop": True
        },
        {
            "id": "dataverse_already_exist",
            "msg": "A dataverse with this name Default already exists",
            "code": 24039,
            "query": 'create dataverse Default',
            "run_in_loop": True
        },
        {
            "id": "create_dataset_that_exist",
            "msg": "A dataset with name ds already exists in dataverse Default",
            "code": 24040,
            "query": "create dataset ds on default",
            "run_in_loop": True
        },
        {
            "id": "dataset_not_found",
            "msg": "Cannot find dataset ds1 in dataverse Default nor an alias with name ds1!",
            "code": 24045,
            "query": 'select * from ds1',
            "run_in_loop": True
        },
        {
            "id": "index_not_found",
            "msg": "Cannot find index with name idx",
            "code": 24047,
            "query": 'drop index ds.idx'
        },
        {
            "id": "index_already_exist",
            "msg": "An index with this name sec_idx already exists",
            "code": 24048,
            "query": "create index sec_idx on ds(name:string)"
        },
        {
            "id": "no_value_for_param",
            "code": 24050,
            "msg": "No value for parameter: $1",
            "query": "SELECT * FROM ds where country = ? limit ?",
            "run_in_loop": True
        },
        {
            "id": "invalid_number_of_arguments",
            "code": 24051,
            "msg": "Invalid number of arguments for function array-intersect",
            "query": "select ARRAY_INTERSECT([2011,2012])",
            "run_in_loop": True
        },
        {
            "id": "incompatible_argument",
            "code": 24057,
            "msg": "Type mismatch: expected value of type integer, but got the value of type string",
            "query": "SELECT * FROM ds limit $1",
            "param": [{"args": ["United States"]}],
            "run_in_loop": True
        },
        # Error codes starting with 25XXX
        {
            "id": "internal_error",
            "msg": 'Internal error',
            "code": 25000,
            "query": 'SET `compiler.groupmemory` "100000GB";select sleep(count(*),500) from ds GROUP BY name',
            "run_in_loop": True
        },
    ]

    def __init__(self, error_id):
        self.error_id = error_id

    def get_error(self):
        for error in self.errors:
            if error['id'] == self.error_id:
                return error
        return None
