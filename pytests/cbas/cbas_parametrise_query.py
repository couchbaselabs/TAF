from remote.remote_util import RemoteMachineShellConnection
from BucketLib.BucketOperations import BucketHelper
from CbasLib.CBASOperations_Rest import CBASHelper
from cbas.cbas_base import CBASBaseTest
from lib.couchbase_helper.tuq_helper import N1QLHelper
from pytests.query_tests_helper import QueryHelperTests
import testconstants
import json
import traceback
from testrunner import TestInputSingleton

class QueryParameterTest(CBASBaseTest):

    def setUp(self):
        
        self.input = TestInputSingleton.input
        self.input.test_params.update({"standard_buckets":0})
            
        super(QueryParameterTest,self).setUp()
        self.query_file = self.input.param("query_file","b/resources/analytics_query_with_parameter.txt")
        self.n1ql_server = self.get_nodes_from_services_map(service_type="n1ql")
        self.curl_path = "curl"
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        if type.lower() == 'windows':
            self.path = testconstants.WIN_COUCHBASE_BIN_PATH
            self.curl_path = "%scurl" % self.path
            self.n1ql_certs_path = "/cygdrive/c/Program\ Files/Couchbase/server/var/lib/couchbase/n1qlcerts"
        self.load_sample_buckets(servers=[self.master], bucketName="travel-sample",
                                 total_items=self.travel_sample_docs_count)
        self.cbas_util.createConn("travel-sample")

        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                         cb_bucket_password=self.cb_bucket_password)

        # Allow ingestion to complete
        self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name], self.travel_sample_docs_count, 300)

        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        self.shell = RemoteMachineShellConnection(self.master)
        self.item_flag = self.input.param("item_flag", 0)
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.n1ql_helper = N1QLHelper(shell=self.shell, max_verify=self.max_verify, buckets=self.buckets,
                                      item_flag=self.item_flag, n1ql_port=self.n1ql_port, log=self.log, input=self.input,
                                      master=self.master, use_rest=True)

    def curl_helper(self, statement):
        cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement={3}'". \
            format('Administrator', 'password', self.master.ip, statement, self.curl_path)
        return self.run_helper_cmd(cmd)

    def run_helper_cmd(self, cmd):
        shell = RemoteMachineShellConnection(self.master)
        output, error = shell.execute_command(cmd)
        new_list = [string.strip() for string in output]
        concat_string = ''.join(new_list)
        json_output = json.loads(concat_string)
        return json_output

    def verify_result_from_n1ql_cbas(self,cbas_result,n1ql_result,validate):
        self.log.info("cbas_result:{}".format(cbas_result))
        self.log.info("n1ql_result:{}".format(n1ql_result["results"]))
        if len(cbas_result) != len(n1ql_result["results"]):
            assert False, "cbas result set: %s not equal to n1ql result set: %s" %(len(cbas_result),len(n1ql_result["results"]))
        if validate:
            for  i in range(len(cbas_result))   :
                if "travel_ds" in cbas_result[i] :
                    a, b = json.dumps(cbas_result[i]["travel_ds"], sort_keys=True), json.dumps(
                        n1ql_result["results"][i]["travel-sample"],sort_keys=True)
                    assert a == b , "result from cbas: %s and from n1ql: %s not same" %(cbas_result[i]["travel_ds"],
                                                                                        n1ql_result["results"][i]["travel-sample"])
                else:
                    a, b = json.dumps(cbas_result[i], sort_keys=True), json.dumps(
                        n1ql_result["results"][i], sort_keys=True)
                    assert a == b, "result from cbas: %s and from n1ql: %s not same" % (
                    cbas_result[i], n1ql_result["results"][i])

    def excute_n1ql_query(self,query):
        n1ql_result = self.curl_helper(query)
        return n1ql_result

    def get_cbas_query(self,query):
        query = query.split("#")[0]
        args = query.split("&")
        cbas_query = args[0]
        params = []
        for i in range(1,len(args)):
            args_list=args[i].split("=")
            params.append({args_list[0]:eval(args_list[1])})
        if "%s" in args[0]:
            cbas_query = args[0].replace("%s",self.cbas_dataset_name)
        self.log.info("params:{}".format(params))
        return cbas_query,params

    def test_parametrise_query(self):
        success = 0
        fail = []
        with open(self.query_file) as f:
            query_list = f.readlines()
        for i in range(len(query_list)):
            try :
                self.log.info("=========== Running {} ===============".format(query_list[i]))
                if "validate" in query_list[i]:
                    validate=True
                else:
                    validate=False
                cbas_query,cbas_param=self.get_cbas_query(query_list[i].rstrip("\n"))
                status, _, _, cbas_result, _ = self.cbas_util.execute_parameter_statement_on_cbas_util(cbas_query,
                                                                                                       parameters=cbas_param)
                query_list[i]= query_list[i].split("#")[0]
                if "%s" in query_list[i]:
                    n1ql_query= query_list[i].replace("%s","`travel-sample`")
                else:
                    n1ql_query= query_list[i]
                n1ql_result = self.excute_n1ql_query(n1ql_query)
                self.verify_result_from_n1ql_cbas(cbas_result, n1ql_result,validate)
                success=success+1
                self.log.info("=========== Query passed {} ===============".format(query_list[i]))
            except Exception as e :
                print("Error:",traceback.format_exc())
                self.log.info("=========== Query failed {} ===============".format(query_list[i]))
                fail.append(query_list[i])
        self.log.info("Queries passed:{}".format(success))
        self.log.info("Queries failed:{}".format(len(fail)))
        assert len(fail) == 0 , "Following queries fail has mis match with n1ql {}".format(fail)
        assert success == len(query_list) , "All queries not executed"


    def test_parameter_queries_negative(self):
        cbas_query="SELECT * FROM `travel_ds` where country = ? limit ?"
        cbas_param=[{"args":["United States"]}]
        status, _, errors, cbas_result, _ = self.cbas_util.execute_parameter_statement_on_cbas_util(cbas_query,
                                                                                               parameters=cbas_param)
        print(errors)
        self.verify_error(errors,"No value for parameter: $2")

        cbas_query = "SELECT * FROM `travel_ds` where country = ? limit 2"
        cbas_param = []
        status, _, errors, cbas_result, _ = self.cbas_util.execute_parameter_statement_on_cbas_util(cbas_query,
                                                                                                    parameters=cbas_param)
        print(errors)
        self.verify_error(errors, "No value for parameter: $1")

        cbas_query = "SELECT * FROM `travel_ds` where country = $country limit 2"
        cbas_param = []
        status, _, errors, cbas_result, _ = self.cbas_util.execute_parameter_statement_on_cbas_util(cbas_query,
                                                                                                    parameters=cbas_param)
        print(errors)
        self.verify_error(errors, "No value for parameter: $country")

        cbas_query = "SELECT * FROM `travel_ds` where country = $country limit $li"
        cbas_param = [{"$country":"United States"}]
        status, _, errors, cbas_result, _ = self.cbas_util.execute_parameter_statement_on_cbas_util(cbas_query,
                                                                                                    parameters=cbas_param)
        print(errors)
        self.verify_error(errors, "No value for parameter: $li")

        cbas_query = "SELECT * FROM `travel_ds` where country = $country limit $li"
        cbas_param = [{"$country": "United States","$l":2}]
        status, _, errors, cbas_result, _ = self.cbas_util.execute_parameter_statement_on_cbas_util(cbas_query,
                                                                                                    parameters=cbas_param)
        print(errors)
        self.verify_error(errors, "No value for parameter: $li")

        ## fail because of MB-30620
        cbas_query = "SELECT * FROM `travel_ds` where country = ? limit $1"
        cbas_param = [{"args": ["United States", 2]}]
        status, _, errors, cbas_result, _ = self.cbas_util.execute_parameter_statement_on_cbas_util(cbas_query,
                                                                                                    parameters=cbas_param)
        print(errors)
        self.verify_error(errors, "Type mismatch: expected value of type integer, but got the value of type string",code=24057)

    def verify_error(self,errors,err_msg,code=24050):
        if err_msg in errors[0]["msg"]:
            assert True, "Error expected {}".format(err_msg)
            assert code == errors[0]["code"], "expeceted code {} but found {}".format(code,errors[0]["code"])