import json
import os

from common_lib import sleep
from global_vars import logger
from membase.api.rest_client import RestConnection
from EventingLib.EventingOperations_Rest import EventingHelper
from BucketLib.BucketOperations import BucketHelper
from shell_util.remote_connection import RemoteMachineShellConnection


class EventingUtils:
    def __init__(self, master, eventing_nodes, src_bucket_name='src_bucket', dst_bucket_name='dst_bucket',
                 metadata_bucket_name='metadata', dst_bucket_name1='dst_bucket_name1', eventing_log_level='INFO',
                 use_memory_manager=True, timer_storage_chan_size=10000, dcp_gen_chan_size=10000, is_sbm=False,
                 is_curl=False, hostname='https://postman-echo.com/', auth_type='no-auth',
                 curl_username=None, curl_password=None, cookies=False, print_eventing_handler_code_in_logs=True):
        self.log = logger.get("test")
        self.eventing_nodes = eventing_nodes
        self.master = master
        self.eventing_helper = EventingHelper(self.eventing_nodes[0])
        self.src_bucket_name = src_bucket_name
        self.dst_bucket_name = dst_bucket_name
        self.metadata_bucket_name = metadata_bucket_name
        self.dst_bucket_name1 = dst_bucket_name1
        self.eventing_log_level = eventing_log_level
        self.use_memory_manager = use_memory_manager
        self.timer_storage_chan_size = timer_storage_chan_size
        self.dcp_gen_chan_size = dcp_gen_chan_size
        self.is_sbm = is_sbm
        self.is_curl = is_curl
        self.hostname = hostname
        self.auth_type = auth_type
        self.curl_username = curl_username
        self.curl_password = curl_password
        self.cookies = cookies
        self.bucket_helper = BucketHelper(self.master)
        self.print_eventing_handler_code_in_logs = print_eventing_handler_code_in_logs

    def create_save_function_body(self, appname, appcode, description="Sample Description",
                                  checkpoint_interval=20000, cleanup_timers=False,
                                  dcp_stream_boundary="everything", deployment_status=True,
                                  skip_timer_threshold=86400,
                                  sock_batch_size=1, tick_duration=5000, timer_processing_tick_interval=500,
                                  timer_worker_pool_size=3, worker_count=3, processing_status=True,
                                  cpp_worker_thread_count=1, multi_dst_bucket=False, execution_timeout=20,
                                  data_chan_size=10000, worker_queue_cap=100000, deadline_timeout=62):
        body = {}
        body['appname'] = appname
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, appcode)
        fh = open(abs_file_path, "r")
        body['appcode'] = fh.read()
        fh.close()
        body['depcfg'] = {}
        body['depcfg']['buckets'] = []
        body['depcfg']['buckets'].append({"alias": self.dst_bucket_name, "bucket_name": self.dst_bucket_name,"access": "rw"})
        if multi_dst_bucket:
            body['depcfg']['buckets'].append({"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1})
        body['depcfg']['metadata_bucket'] = self.metadata_bucket_name
        body['depcfg']['source_bucket'] = self.src_bucket_name
        body['settings'] = {}
        body['settings']['checkpoint_interval'] = checkpoint_interval
        body['settings']['cleanup_timers'] = cleanup_timers
        body['settings']['dcp_stream_boundary'] = dcp_stream_boundary
        body['settings']['deployment_status'] = deployment_status
        body['settings']['description'] = description
        body['settings']['log_level'] = self.eventing_log_level
        body['settings']['skip_timer_threshold'] = skip_timer_threshold
        body['settings']['sock_batch_size'] = sock_batch_size
        body['settings']['tick_duration'] = tick_duration
        body['settings']['timer_processing_tick_interval'] = timer_processing_tick_interval
        body['settings']['timer_worker_pool_size'] = timer_worker_pool_size
        body['settings']['worker_count'] = worker_count
        body['settings']['processing_status'] = processing_status
        body['settings']['cpp_worker_thread_count'] = cpp_worker_thread_count
        body['settings']['execution_timeout'] = execution_timeout
        body['settings']['data_chan_size'] = data_chan_size
        body['settings']['worker_queue_cap'] = worker_queue_cap
        # See MB-27967, the reason for adding this config
        body['settings']['use_memory_manager'] = self.use_memory_manager
        # since deadline_timeout has to always greater than execution_timeout
        if execution_timeout != 3:
            deadline_timeout = execution_timeout + 1
        body['settings']['deadline_timeout'] = deadline_timeout
        body['settings']['timer_storage_chan_size'] = self.timer_storage_chan_size
        body['settings']['dcp_gen_chan_size'] = self.dcp_gen_chan_size
        if self.is_sbm:
            del body['depcfg']['buckets'][0]
            body['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name,"access": "rw"})
        body['depcfg']['curl'] = []
        if self.is_curl:
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,
                                           "allow_cookies": self.cookies})
            if self.auth_type=="bearer":
                body['depcfg']['curl'][0]['bearer_key']=self.bearer_key
        self.cb_version = RestConnection(self.master).get_nodes_version()
        body['settings']['language_compatibility']=self.cb_version[:5]
        return body

    def wait_for_bootstrap_to_complete(self, name, iterations=20):
        result = self.eventing_helper.get_deployed_eventing_apps()
        count = 0
        while name not in result and count < iterations:
            sleep(30, "Waiting for eventing node to complete bootstrap")
            count += 1
            result = self.eventing_helper.get_deployed_eventing_apps()
        if count == iterations:
            raise Exception(
                'Eventing took lot of time to come out of bootstrap state or did not successfully bootstrap')

    def wait_for_undeployment(self, name, iterations=20):
        sleep(30, "Waiting for undeployment of function...")
        result = self.eventing_helper.get_running_eventing_apps()
        count = 0
        while name in result and count < iterations:
            sleep(30, "Waiting for undeployment of function...")
            count += 1
            result = self.eventing_helper.get_running_eventing_apps()
        if count == iterations:
            raise Exception('Eventing took lot of time to undeploy')

    def verify_eventing_results(self, name, expected_dcp_mutations, doc_timer_events=False, on_delete=False,
                                skip_stats_validation=False, bucket=None, timeout=600):
        # This resets the rest server as the previously used rest server might be out of cluster due to rebalance
        num_nodes = self.refresh_rest_server()
        if bucket is None:
            bucket=self.dst_bucket_name
        if self.is_sbm:
            bucket=self.src_bucket_name
        if not skip_stats_validation:
            # we can't rely on dcp_mutation stats when doc timers events are set.
            # TODO : add this back when getEventProcessingStats works reliably for doc timer events as well
            if not doc_timer_events:
                count = 0
                if num_nodes <= 1:
                    stats = self.eventing_helper.get_event_processing_stats(name)
                else:
                    stats = self.eventing_helper.get_aggregate_event_processing_stats(name)
                if on_delete:
                    mutation_type = "dcp_deletion"
                else:
                    mutation_type = "dcp_mutation"
                actual_dcp_mutations = stats[mutation_type]
                # This is required when binary data is involved where dcp_mutation will have process DCP_MUTATIONS
                # but ignore it
                # wait for eventing node to process dcp mutations
                self.log.info("Number of {0} processed till now : {1}".format(mutation_type, actual_dcp_mutations))
                while actual_dcp_mutations != expected_dcp_mutations and count < 20:
                    sleep(timeout/20,
                          "Waiting for eventing to process all dcp mutations")
                    count += 1
                    if num_nodes <= 1:
                        stats = self.eventing_helper.get_event_processing_stats(name)
                    else:
                        stats = self.eventing_helper.get_aggregate_event_processing_stats(name)
                    actual_dcp_mutations = stats[mutation_type]
                    self.log.info("Number of {0} processed till now : {1}".format(mutation_type, actual_dcp_mutations))
                if count == 20:
                    raise Exception(
                        "Eventing has not processed all the {0}. Current : {1} Expected : {2}".format(mutation_type,
                                                                                                      actual_dcp_mutations,
                                                                                                      expected_dcp_mutations
                                                                                                      ))
        # wait for bucket operations to complete and verify it went through successfully
        count = 0
        stats_dst = self.bucket_helper.get_bucket_stats(bucket)
        while stats_dst["curr_items"] != expected_dcp_mutations and count < 20:
            message = "Waiting for handler code {2} to complete bucket operations... " \
                      "Current : {0} Expected : {1}".\
                      format(stats_dst["curr_items"], expected_dcp_mutations,name)
            sleep(timeout/20, message)
            curr_items = stats_dst["curr_items"]
            stats_dst = self.bucket_helper.get_bucket_stats(bucket)
            if curr_items == stats_dst["curr_items"]:
                count += 1
            else:
                count = 0
        try:
            stats_src = self.bucket_helper.get_bucket_stats(self.src_bucket_name)
            self.log.info("Documents in source bucket : {}".format(stats_src["curr_items"]))
        except :
            pass
        if stats_dst["curr_items"] != expected_dcp_mutations:
            total_dcp_backlog = 0
            timers_in_past = 0
            lcb = {}
            # TODO : Use the following stats in a meaningful way going forward. Just printing them for debugging.
            for eventing_node in self.eventing_nodes:
                rest_conn = EventingHelper(eventing_node)
                out = rest_conn.get_all_eventing_stats()
                total_dcp_backlog += out[0]["events_remaining"]["dcp_backlog"]
                if "TIMERS_IN_PAST" in out[0]["event_processing_stats"]:
                    timers_in_past += out[0]["event_processing_stats"]["TIMERS_IN_PAST"]
                total_lcb_exceptions= out[0]["lcb_exception_stats"]
                host=eventing_node.ip
                lcb[host]=total_lcb_exceptions
                full_out = rest_conn.get_all_eventing_stats(seqs_processed=True)
                self.log.debug("Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(out, sort_keys=True,
                                                                                          indent=4)))
                self.log.debug("Full Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(full_out,
                                                                                                sort_keys=True,
                                                                                                indent=4)))
            raise Exception(
                "Bucket operations from handler code took lot of time to complete or didn't go through. Current : {0} "
                "Expected : {1}  dcp_backlog : {2}  TIMERS_IN_PAST : {3} lcb_exceptions : {4}".format(stats_dst["curr_items"],
                                                                                 expected_dcp_mutations,
                                                                                 total_dcp_backlog,
                                                                                 timers_in_past,lcb))
        self.log.info("Final docs count... Current : {0} Expected : {1}".
                 format(stats_dst["curr_items"], expected_dcp_mutations))
        # TODO : Use the following stats in a meaningful way going forward. Just printing them for debugging.
        # print all stats from all eventing nodes
        # These are the stats that will be used by ns_server and UI
        for eventing_node in self.eventing_nodes:
            rest_conn = EventingHelper(eventing_node)
            out = rest_conn.get_all_eventing_stats()
            full_out = rest_conn.get_all_eventing_stats(seqs_processed=True)
            self.log.debug("Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(out, sort_keys=True,
                                                                                      indent=4)))
            self.log.debug("Full Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(full_out, sort_keys=True,
                                                                                            indent=4)))

    def eventing_stats(self):
        self.log.debug("Wait before get_all_eventing_stats() call")
        sleep(30)
        content = self.eventing_helper.get_all_eventing_stats()
        js = json.loads(content)
        self.log.debug("Execution stats: {0}".format(js))
        # for j in js:
        #     print j["function_name"]
        #     print j["execution_stats"]["on_update_success"]
        #     print j["failure_stats"]["n1ql_op_exception_count"]

    def deploy_function(self, body, deployment_fail=False, wait_for_bootstrap=True,pause_resume=False,pause_resume_number=1):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = True
        if self.print_eventing_handler_code_in_logs:
            self.log.info("Deploying the following handler code : {0} with {1}".format(body['appname'],body['depcfg']))
            self.log.info("\n{0}".format(body['appcode']))
        content1 = self.eventing_helper.create_function(body['appname'], body)
        self.log.info("deploy Application : {0}".format(content1))
        if deployment_fail:
            res = json.loads(content1)
            if not res["compile_success"]:
                return
            else:
                raise Exception("Deployment is expected to be failed but no message of failure")
        if wait_for_bootstrap:
            # wait for the function to come out of bootstrap state
            self.wait_for_handler_state(body['appname'], "deployed")
        if pause_resume and pause_resume_number > 0:
            self.pause_resume_n(body,pause_resume_number)

    def undeploy_and_delete_function(self, body):
        self.undeploy_function(body)
        self.log.debug("Wait between undeploy & before delete_function() call")
        sleep(5)
        self.delete_function(body)

    def undeploy_function(self, body):
        self.refresh_rest_server()
        content = self.eventing_helper.undeploy_function(body['appname'])
        self.log.info("Undeploy Application : {0}".format(body['appname']))
        self.wait_for_handler_state(body['appname'],"undeployed")
        return content

    def delete_function(self, body):
        content1 = self.eventing_helper.delete_single_function(body['appname'])
        self.log.info("Delete Application : {0}".format(body['appname']))
        return content1

    def pause_function(self, body,wait_for_pause=True):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = False
        self.refresh_rest_server()
        content1 = self.eventing_helper.set_settings_for_function(body['appname'], body['settings'])
        self.log.info("Pause Application : {0}".format(body['appname']))
        if wait_for_pause:
            self.wait_for_handler_state(body['appname'], "paused")

    def resume_function(self, body,wait_for_resume=True):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = True
        if "dcp_stream_boundary" in body['settings']:
            body['settings'].pop('dcp_stream_boundary')
        self.log.info("Settings after deleting dcp_stream_boundary : {0}".format(body['settings']))
        self.refresh_rest_server()
        content1 = self.eventing_helper.set_settings_for_function(body['appname'], body['settings'])
        self.log.info("Resume Application : {0}".format(body['appname']))
        if wait_for_resume:
            self.wait_for_handler_state(body['appname'], "deployed")

    def refresh_rest_server(self):
        self.restServer = self.eventing_nodes[0]
        self.rest = RestConnection(self.restServer)
        return len(self.eventing_nodes)

    def check_if_eventing_consumers_are_cleaned_up(self):
        array_of_counts = []
        command = "ps -ef | grep eventing-consumer | grep -v grep | wc -l"
        for eventing_node in self.eventing_nodes:
            shell = RemoteMachineShellConnection(eventing_node)
            count, error = shell.execute_non_sudo_command(command)
            if isinstance(count, list):
                count = int(count[0])
            else:
                count = int(count)
            self.log.info("Node : {0} , eventing_consumer processes running : {1}".format(eventing_node.ip, count))
            array_of_counts.append(count)
        count_of_all_eventing_consumers = sum(array_of_counts)
        if count_of_all_eventing_consumers != 0:
            return False
        return True

    """
        Checks if a string 'panic' is present in eventing.log on server and returns the number of occurrences
    """

    def check_eventing_logs_for_panic(self):
        # self.generate_map_nodes_out_dist()
        panic_str = "panic"
        if not self.eventing_nodes:
            return None
        for eventing_node in self.eventing_nodes:
            shell = RemoteMachineShellConnection(eventing_node)
            _, dir_name = RestConnection(eventing_node).diag_eval(
                'filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
            eventing_log = str(dir_name) + '/eventing.log*'
            count, err = shell.execute_command("zgrep \"{0}\" {1} | wc -l".
                                               format(panic_str, eventing_log))
            if isinstance(count, list):
                count = int(count[0])
            else:
                count = int(count)
            if count > self.panic_count:
                self.log.info("===== PANIC OBSERVED IN EVENTING LOGS ON SERVER {0}=====".format(eventing_node.ip))
                panic_trace, _ = shell.execute_command("zgrep \"{0}\" {1}".
                                                       format(panic_str, eventing_log))
                self.log.info("\n {0}".format(panic_trace))
                self.panic_count = count
            os_info = shell.extract_remote_info()
            if os_info.type.lower() == "windows":
                # This is a fixed path in all windows systems inside couchbase
                dir_name_crash = 'c://CrashDumps'
            else:
                dir_name_crash = str(dir_name) + '/../crash/'
            core_dump_count, err = shell.execute_command("ls {0}| wc -l".format(dir_name_crash))
            if isinstance(core_dump_count, list):
                core_dump_count = int(core_dump_count[0])
            else:
                core_dump_count = int(core_dump_count)
            if core_dump_count > 0:
                self.log.info("===== CORE DUMPS SEEN ON EVENTING NODES, SERVER {0} : {1} crashes seen =====".format(
                         eventing_node.ip, core_dump_count))
            shell.disconnect()

    def print_execution_and_failure_stats(self,name):
        out_event_execution = self.eventing_helper.get_event_execution_stats(name)
        self.log.debug("Event execution stats : {0}".format(out_event_execution))
        out_event_failure = self.eventing_helper.get_event_failure_stats(name)
        self.log.debug("Event failure stats : {0}".format(out_event_failure))

    def undeploy_delete_all_functions(self):
        content=self.eventing_helper.get_deployed_eventing_apps()
        res = content.keys()
        self.log.info("all keys {}".format(res))
        for a in res:
            self.eventing_helper.undeploy_function(a)
        for a in res:
            self.wait_for_handler_state(a, "undeployed")
        self.eventing_helper.delete_all_function()

    def cleanup_eventing(self):
        ev_rest = EventingHelper(self.eventing_nodes[0])
        self.log.info("Running eventing cleanup api...")
        ev_rest.cleanup_eventing()

    def print_eventing_stats_from_all_eventing_nodes(self):
        for eventing_node in self.eventing_nodes:
            rest_conn = EventingHelper(eventing_node)
            out = rest_conn.get_all_eventing_stats()
            self.log.info("Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(out, sort_keys=True,
                                                                                      indent=4)))

    def print_go_routine_dump_from_all_eventing_nodes(self):
        for eventing_node in self.eventing_nodes:
            rest_conn = EventingHelper(eventing_node)
            out = rest_conn.get_eventing_go_routine_dumps()
            self.log.info("Go routine dumps for Node {0} is \n{1} ======================================================"
                     "============================================================================================="
                     "\n\n".format(eventing_node.ip, out))

    # def verify_source_bucket_mutation(self,doc_count,deletes=False,timeout=600,bucket=None):
    #     if bucket == None:
    #         bucket=self.src_bucket_name
    #     # query = "create primary index on {}".format(self.src_bucket_name)
    #     # self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
    #     num_nodes = self.refresh_rest_server()
    #     count=0
    #     result=0
    #     while count <= 20 and doc_count != result:
    #         self.sleep(timeout / 20, message="Waiting for eventing to process all dcp mutations...")
    #         if deletes:
    #                 query="select raw(count(*)) from {} where doc_deleted = 1".format(bucket)
    #         else:
    #             query="select raw(count(*)) from {} where updated_field = 1".format(bucket)
    #         result_set=self.n1ql_helper.run_cbq_query(query=query,server=self.n1ql_node)
    #         result=result_set["results"][0]
    #         if deletes:
    #             self.log.info("deleted docs:{}  expected doc: {}".format(result,doc_count))
    #         else:
    #             self.log.info("updated docs:{}  expected doc: {}".format(result, doc_count))
    #         count=count+1
    #
    #     if count > 20 and doc_count != result:
    #         total_dcp_backlog = 0
    #         timers_in_past = 0
    #         lcb = {}
    #         # TODO : Use the following stats in a meaningful way going forward. Just printing them for debugging.
    #         for eventing_node in self.eventing_nodes:
    #             rest_conn = RestConnection(eventing_node)
    #             out = rest_conn.get_all_eventing_stats()
    #             total_dcp_backlog += out[0]["events_remaining"]["dcp_backlog"]
    #             if "TIMERS_IN_PAST" in out[0]["event_processing_stats"]:
    #                 timers_in_past += out[0]["event_processing_stats"]["TIMERS_IN_PAST"]
    #             total_lcb_exceptions = out[0]["lcb_exception_stats"]
    #             host = eventing_node.ip
    #             lcb[host] = total_lcb_exceptions
    #             full_out = rest_conn.get_all_eventing_stats(seqs_processed=True)
    #             log.info(
    #                 "Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(out, sort_keys=True, indent=4)))
    #             log.debug("Full Stats for Node {0} is \n{1} ".format(eventing_node.ip,
    #                                                                  json.dumps(full_out, sort_keys=True, indent=4)))
    #         raise Exception("Eventing has not processed all the mutation in expected time, docs:{}  expected doc: {}".format(result, doc_count))

    def pause_resume_n(self, body, num):
        for i in range(num):
            self.pause_function(body)
            self.log.debug("Wait between pause_function & "
                           "before resume_function() call")
            sleep(30)
            self.resume_function(body)

    def wait_for_handler_state(self, name, status, iterations=20):
        sleep(20, "Waiting for %s to %s..." % (name, status))
        _ = self.eventing_helper.get_composite_eventing_status()
        count = 0
        composite_status = None
        while composite_status != status and count < iterations:
            sleep(20, "Waiting for %s to %s..." % (name, status))
            result = self.eventing_helper.get_composite_eventing_status()
            for i in range(len(result['apps'])):
                if result['apps'][i]['name'] == name:
                    composite_status = result['apps'][i]['composite_status']
            count += 1
        if count == iterations:
            raise Exception('Eventing took lot of time for handler %s to %s'
                            % (name, status))

    def setup_curl(self):
        o = os.system('python scripts/curl_setup.py start')
        self.log.info("=== started docker container =======".format(o))
        sleep(10, "Wait for docker to start", log_type="infra")
        if o != 0:
            self.log.info("script result {}".format(o))
            raise Exception("unable to start docker")
        o = os.system('python scripts/curl_setup.py setup')
        self.log.info("=== setup done =======")
        if o != 0:
            self.log.info("script result {}".format(o))
            raise Exception("curl setup fail")

    def check_eventing_rebalance(self):
        status = self.eventing_helper.get_eventing_rebalance_status()
        self.log.info("Eventing rebalance status: {}".format(status))
        if status == "true":
            return True
        else:
            return False
