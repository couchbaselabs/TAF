import time
import random
from cbas_base import *
from rbac_utils.Rbac_ready_functions import RbacUtils
from remote.remote_util import RemoteMachineShellConnection
from couchbase_utils.cbas_utils.cbas_utils import CBASRebalanceUtil


class CBASErrorValidator(CBASBaseTest):
    def setUp(self):
        super(CBASErrorValidator, self).setUp()
        self.log.info("Read input param : error id")
        self.error_id = self.input.param('error_id', None)
        self.error_response = CBASError(self.error_id).get_error()
        self.log.info("Test to validate error response : %s" % self.error_response)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(CBASErrorValidator, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def setup_dataset(self, wait_for_ingestion=False):
        update_spec = {
            "no_of_dataverses": self.input.param('no_of_dv', 1),
            "no_of_datasets_per_dataverse": self.input.param('ds_per_dv', 1),
            "no_of_synonyms": 0,
            "no_of_indexes": 0,
            "max_thread_count": self.input.param('no_of_threads', 1),
            "dataset": {
                "creation_methods": ["cbas_collection", "cbas_dataset"],
            }
        }
        self.setup_for_test(update_spec, wait_for_ingestion)
        self.ds = self.cbas_util.list_all_dataset_objs()[0]
        self.dv = self.cbas_util.get_dataverses(self.cluster)[0]

    def test_error_response(self):
        self.setup_dataset()
        self.log.info("Execute queries and validate error response")
        error_response_mismatch = []
        count = 0
        for error_object in (x for x in CBASError.errors if "run_in_loop" in x):
            count += 1
            self.log.info("------------------------ Running ------------------------")
            self.log.info(error_object)
            if "param" in error_object:
                status, _, errors, _, _ = self.cbas_util.execute_parameter_statement_on_cbas_util(
                    self.cluster, error_object["query"].format(self.ds.name),
                    parameters=error_object["param"])
            else:
                time_out = error_object["time_out"] if "time_out" in error_object else 120
                time_unit = error_object["time_unit"] if "time_unit" in error_object else "s"
                status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.cluster, error_object["query"].format(self.ds.name),
                    analytics_timeout=time_out, time_out_unit=time_unit)
            self.log.info(status)
            self.log.info(errors)
            if not self.cbas_util.validate_error_in_response(
                    status, errors, error_object["msg"].format(self.ds.name),
                    error_object["code"]):
                error_response_mismatch.append([error_object['id']])
            self.log.info("------------------------ Completed ------------------------")
        self.log.info("Run summary")
        self.log.info("Total:%d , Passed:%d , Failed:%d" % (count, count - len(error_response_mismatch),
                                                            len(error_response_mismatch)))
        if len(error_response_mismatch):
            self.log.info(error_response_mismatch)
            self.fail("Failing test error msg/code mismatch.")

    def test_error_response_index_name_exist(self):
        self.setup_dataset()
        self.log.info("Disconnect Local link")
        if not self.cbas_util.disconnect_link(self.cluster, "Local"):
            self.fail("Failed to disconnect connected bucket.")
        self.log.info("Create a secondary index")
        if not self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, self.error_response["query"].format(self.ds.name)):
            self.fail("Failed to create secondary index.")
        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, self.error_response["query"].format(self.ds.name))
        if not self.cbas_util.validate_error_in_response(status, errors,
                                                         self.error_response["msg"],
                                                         self.error_response["code"]):
            self.fail("Error validation failed.")

    def test_error_response_user_permission(self):
        self.setup_dataset()
        self.log.info("Create a user with analytics reader role")
        rbac_util = RbacUtils(self.cluster.master)
        bucket_obj = self.ds.kv_bucket
        rbac_util._create_user_and_grant_role("reader_admin", "analytics_reader")
        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, self.error_response["query"].format(self.ds.name),
            username="reader_admin", password="password")
        if not self.cbas_util.validate_error_in_response(
                status, errors, self.error_response["msg"] % bucket_obj.name,
                self.error_response["code"]):
            self.fail("Error validation failed.")

    def test_error_response_user_unauthorized(self):
        self.setup_dataset()
        self.log.info("Create remote connection and execute cbas query using curl")
        rbac_util = RbacUtils(self.cluster.master)
        rbac_util._create_user_and_grant_role("reader_admin", "analytics_reader")
        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, self.error_response["query"].format(self.ds.name),
            username="reader_admin", password="pass")
        if not self.cbas_util.validate_error_in_response(status, errors,
                                                         self.error_response["msg"],
                                                         self.error_response["code"]):
            self.fail("Error validation failed.")

    def test_error_response_drop_dataverse(self):
        if not self.cbas_util.create_dataverse(self.cluster, "custom"):
            self.fail("Dataverse creation failed.")
        self.log.info("Use dataverse")
        status, _, _, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, "use custom")
        if status != "success":
            self.fail("Use dataverse query failed.")
        if not self.cbas_util.create_dataset(self.cluster, "ds", "lineitem", "custom"):
            self.fail("Dataset creation failed.")
        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, self.error_response["query"])
        if not self.cbas_util.validate_error_in_response(status, errors,
                                                         self.error_response["msg"],
                                                         self.error_response["code"]):
            self.fail("Error validation failed.")

    def test_analytics_service_tmp_unavailable(self):
        self.setup_dataset()
        selected_node = random.choice(list(set(self.cluster.cbas_nodes) - {self.cluster.cbas_cc_node}))
        self.log.info("SELECTED NODE : {0}".format(selected_node))
        self.log.info("CBAS_CC_NODE : {0}".format(self.cluster.cbas_cc_node.ip))
        shell = RemoteMachineShellConnection(selected_node)
        shell.stop_couchbase()
        service_unavailable = False
        while self.cbas_util.is_analytics_running(self.cluster, 60):
            pass
        remote_shell = RemoteMachineShellConnection(self.cluster.cbas_cc_node)
        output, _ = remote_shell.execute_command(
            """curl -X POST {0} -u {1}:{2} -d 'statement={3}'""".format(
                "http://{0}:{1}/analytics/service".format(self.cluster.cbas_cc_node.ip, 8095),
                "Administrator", "password", self.error_response["query"]))
        self.log.info("OUTPUT = {0}".format(str(output)))
        if self.error_response["msg"][0] in str(output):
            self.log.info("Hit service unavailable condition")
            service_unavailable = True
        self.log.info("Validate error response")
        shell.start_couchbase()
        shell.disconnect()
        if not service_unavailable:
            self.fail("Failed to get into the state analytics service is unavailable")
        if not self.error_response["msg"] in str(output):
            self.fail("Error message mismatch")
        if not str(self.error_response["code"]) in str(output):
            self.fail("Error code mismatch")
        remote_shell.disconnect()

    def test_error_response_rebalance_in_progress(self):
        self.setup_dataset()
        available_servers = []
        for server in self.servers:
            if server not in self.cluster.nodes_in_cluster:
                available_servers.append(server)
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task,
            vbucket_check=True, cbas_util=self.cbas_util)
        rebalance_task, _ = self.rebalance_util.rebalance(self.cluster, cbas_nodes_in=1,
                                      available_servers=available_servers)
        self.log.info("Execute query and validate error response")
        start_time = time.time()
        while time.time() < start_time + 120:
            status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, self.error_response["query"].format(self.ds.name))
            if errors is not None:
                break
        if not self.cbas_util.validate_error_in_response(status, errors,
                                                         self.error_response["msg"],
                                                         self.error_response["code"]):
            self.fail("Error validation failed.")

        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster):
            self.fail("Rebalancing in CBAS node failed.")

    def test_error_response_using_curl(self):
        self.setup_dataset()
        shell = RemoteMachineShellConnection(self.cluster.cbas_cc_node)
        self.log.info("Execute query using CURL on {0}".format(self.cluster.cbas_cc_node.ip))
        command = """curl -X POST {0} -u {1}:{2} --data-urlencode 'statement={3}'""".format(
            "http://{0}:8095/analytics/service".format(self.cluster.cbas_cc_node.ip),
            "Administrator", "password", self.error_response["query"].format(self.ds.name))
        output, _ = shell.execute_command(command)
        if not self.error_response["msg"] in str(output):
            self.fail("Error message mismatch")
        if not str(self.error_response["code"]) in str(output):
            self.fail("Error code mismatch")
        shell.disconnect()

    # Waiting on dev team for steps to simulate this.
    def test_error_response_type_mismatch_object(self):
        self.setup_dataset()
        self.log.info("Create a reference to SDK client")
        client = self.cluster.sdk_client_pool.clients["lineitem"][
            "idle_clients"][0]
        self.log.info("Insert documents in KV bucket")
        documents = ['{"address":{"city":"NY"}}']
        client.insert_json_documents("id-", documents)
        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, self.error_response["query"].format(self.ds.name))
        if not self.cbas_util.validate_error_in_response(status, errors,
                                                         self.error_response["msg"],
                                                         self.error_response["code"]):
            self.fail("Error validation failed.")

    def test_error_response_index_not_found(self):
        self.setup_dataset()
        self.log.info("Disconnect Local link")
        if not self.cbas_util.disconnect_link(self.cluster, "Local"):
            self.fail("Failed to disconnect connected bucket")
        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, self.error_response["query"].format(self.ds.name))
        if not self.cbas_util.validate_error_in_response(status, errors,
                                                         self.error_response["msg"],
                                                         self.error_response["code"]):
            self.fail("Error validation failed.")

    def test_error_response_no_statement(self):
        self.setup_dataset()
        self.log.info("Execute query and validate error response")
        shell = RemoteMachineShellConnection(self.cluster.cbas_cc_node)
        self.log.info("Execute query using CURL on {0}".format(self.cluster.cbas_cc_node.ip))
        output, _ = shell.execute_command("""curl -X POST {0} -u {1}:{2} """.format(
            "http://{0}:{1}/analytics/service".format(self.cluster.cbas_cc_node.ip, 8095),
            "Administrator", "password"))
        shell.disconnect()
        if not self.error_response["msg"] in str(output):
            self.fail("Error message mismatch")
        if not str(self.error_response["code"]) in str(output):
            self.fail("Error code mismatch")

    def test_error_response_for_kv_bucket_delete(self):
        self.setup_dataset()
        self.log.info("Disconnect Local link")
        if not self.cbas_util.disconnect_link(self.cluster, "Local"):
            self.fail("Failed to disconnect connected bucket")
        bucket_obj = self.ds.kv_bucket
        self.log.info("Delete KV bucket {0}".format(bucket_obj.name))
        self.bucket_util.delete_bucket(self.cluster, bucket_obj)
        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, self.error_response["query"].format(self.ds.name))
        if not self.cbas_util.validate_error_in_response(
                status, errors,
                self.error_response["msg"] % (bucket_obj.name, bucket_obj.name),
                self.error_response["code"]):
            self.fail("Error validation failed.")

    def test_error_response_index_on_meta_fields(self):
        self.setup_dataset()
        self.log.info("Disconnect Local link")
        if not self.cbas_util.disconnect_link(self.cluster, "Local"):
            self.fail("Failed to disconnect connected bucket")
        self.log.info("Create a secondary index")
        if not self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, self.error_response["query"].format(self.ds.name)):
            self.fail("Failed to create secondary index")
        self.log.info("Execute query and validate error response")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, self.error_response["query"].format(self.ds.name))
        if not self.cbas_util.validate_error_in_response(status, errors,
                                                         self.error_response["msg"],
                                                         self.error_response["code"]):
            self.fail("Error validation failed.")


class CBASError:
    errors = [
        # Error codes starting with 20xxx
        {
            "id": "user_unauthorized",
            "msg": "Unauthorized user",
            "code": 20000,
            "query": "select count(*) from {0}"
        },
        {
            "id": "user_permission",
            "msg": "User must have permission (cluster.bucket[%s].analytics!manage)",
            "code": 20001,
            "query": "drop dataset {0}"
        },
        # Error codes starting with 21xxx
        {
            "id": "invalid_duration",
            "msg": "Invalid value for parameter 'timeout': tos",
            "code": 21008,
            "query": "select sleep(count(*), 2000) from {0}",
            "time_out": "to",
            "run_in_loop": True
        },
        {
            "id": "request_timeout",
            "msg": "Request timed out and will be cancelled",
            "code": 21002,
            "query": "select sleep(count(*), 2000) from {0}",
            "time_out": 1,
            "run_in_loop": True
        },
        {
            "id": "multiple_statements",
            "msg": "Unsupported multiple statements.",
            "code": 21003,
            "query": "create dataset {0} on default;connect link Local",
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
            "id": "kv_bucket_does_not_exist",
            "msg": 'Connect link failed {\"Default.Local.%s\":\"Bucket (%s) does not exist\"}',
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
            "query": "create dataset {0} on default"
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
            "msg": "Analytics scope custom cannot be dropped while link Local is connected",
            "code": 23005,
            "query": "drop dataverse custom"
        },
        {
            "id": "job_requirement",
            "msg": 'exceeds capacity (memory: ',
            "code": 23008,
            "query": "SET `compiler.groupmemory` \"10GB\";select sleep(count(*),500) from {0} GROUP BY name"
        },
        {
            "id": "overflow",
            "msg": 'Overflow in numeric-multiply',
            "code": 23011,
            "query": 'select 23000000000000000 * 23000000000000000',
            "run_in_loop": True
        },
        {
            "id": "type_mismatch_for_object",
            "msg": 'Type mismatch: function field-access-by-name expects its 1st input parameter to be of type object, but the actual input type is string',
            "code": 24011,
            "query": "select address.city.name from {0} where address.city=\"NY\""
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
            "query": "select count(*) {0}",
            "run_in_loop": True
        },
        {
            "id": "index_on_meta_id",
            "msg": "Compilation error: Cannot create index on meta fields",
            "code": 24001,
            "query": "create index meta_idx on {0}(meta().id)"
        },
        {
            "id": "index_on_meta_cas",
            "msg": "Compilation error: Cannot create index on meta fields",
            "code": 24001,
            "query": "create index meta_idx on {0}(meta().cas)"
        },
        {
            "id": "composite_index_on_meta_and_document_field",
            "msg": "Compilation error: Cannot create index on meta fields",
            "code": 24001,
            "query": "create index comp_meta_idx on {0}(city:string, meta().id)"
        },
        {
            "id": "index_on_type",
            "msg": "Cannot index field [click] on type float. Supported types: bigint, double, string",
            "code": 24002,
            "query": "create index idx on {0}(click:float)",
            "run_in_loop": True
        },
        {
            "id": "kv_bucket_does_not_exist",
            "msg": "Bucket (default1) does not exist",
            "code": 24003,
            "query": "create dataset ds on default1",
            "run_in_loop": True
        },
        {
            "id": "unsupported_statement",
            "msg": "Feature supported only in Developer Preview mode",
            "code": 24122,
            "query": "UPSERT INTO {0} (SELECT VALUE name FROM {0})",
            "run_in_loop": True
        },
        {
            "id": "drop_link_not_exist",
            "msg": "Link Default.local1 does not exist",
            "code": 24006,
            "query": "drop link local1",
            "run_in_loop": True
        },
        {
            "id": "drop_local_link",
            "msg": "Operation cannot be performed on the Local link",
            "code": 24007,
            "query": "drop link Local",
            "run_in_loop": True
        },
        {
            "id": "duplicate_field",
            "msg": "Duplicate field name 'a'",
            "code": 24015,
            "query": 'select 1 as a, 2 as a',
            "run_in_loop": True
        },
        {
            "id": "dataset_not_found",
            "msg": "Cannot find analytics collection with name Bucket in analytics scope Default",
            "code": 24025,
            "query": 'drop index Bucket.Bucket',
            "run_in_loop": True
        },
        {
            "id": "dataverse_not_found",
            "msg": "Cannot find analytics scope with name custom",
            "code": 24034,
            "query": 'use custom',
            "run_in_loop": True
        },
        {
            "id": "dataverse_already_exist",
            "msg": "An analytics scope with this name Default already exists",
            "code": 24039,
            "query": 'create dataverse Default',
            "run_in_loop": True
        },
        {
            "id": "create_dataset_that_exist",
            "msg": "An analytics collection with name {0} already exists in analytics scope Default",
            "code": 24040,
            "query": "create dataset {0} on default",
            "run_in_loop": True
        },
        {
            "id": "ambiguous_alias",
            "msg": "Cannot resolve ambiguous alias reference for identifier IDENT",
            "code": 24042,
            "query": "select min(IDENT) from {0} c, {0} o where c.c_id = o.c_id",
            "run_in_loop": True
        },
        {
            "id": "dataset_not_found",
            "msg": "Cannot find analytics collection ds1 in analytics scope Default nor an alias with name ds1",
            "code": 24045,
            "query": 'select * from ds1',
            "run_in_loop": True
        },
        {
            "id": "index_not_found",
            "msg": "Cannot find index with name idx",
            "code": 24047,
            "query": 'drop index {0}.idx'
        },
        {
            "id": "index_already_exist",
            "msg": "An index with this name sec_idx already exists",
            "code": 24048,
            "query": "create index sec_idx on {0}(name:string)"
        },
        {
            "id": "no_value_for_param",
            "code": 24050,
            "msg": "No value for parameter: $1",
            "query": "SELECT * FROM {0} where country = ? limit ?",
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
            "query": "SELECT * FROM {0} limit $1",
            "param": {"args": ["United States"]},
            "run_in_loop": True
        },
        {
            "id": "reuse_variable",
            "code": 24057,
            "msg": "Type mismatch: expected value of type multiset or array, but got the value of type object",
            "query": "SELECT name FROM {0} WHERE rating = (SELECT MAX(rating) FROM {0})",
            "run_in_loop": True
        },
        # Error codes starting with 25XXX
        {
            "id": "invalid query parameter",
            "msg": 'Invalid query parameter compiler.groupmemory',
            "code": 24128,
            "query": 'SET `compiler.groupmemory` "100000GB";select sleep(count(*),500) from {0} GROUP BY name',
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
