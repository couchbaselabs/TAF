from membase.api.rest_client import RestConnection
from testconstants import STANDARD_BUCKET_PORT
from eventing.eventing_constants import HANDLER_CODE
from eventing.eventing_base import EventingBaseTest
from membase.helper.cluster_helper import ClusterOperationHelper
from BucketLib.bucket import Bucket


class EventingSanity(EventingBaseTest):
    def setUp(self):
        super(EventingSanity, self).setUp()
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=700)
        if self.create_functions_buckets:
            self.bucket_size = 200
            self.log.info(self.bucket_size)
            bucket_params_src = Bucket({"name": self.src_bucket_name, "replicaNumber": self.num_replicas})
            src_bucket = self.bucket_util.create_bucket(bucket_params_src)
            self.src_bucket = self.bucket_util.get_all_buckets(self.cluster.master)[0]
            bucket_params_dst = Bucket({"name": self.dst_bucket_name, "replicaNumber": self.num_replicas})
            bucket_params_meta = Bucket({"name": self.metadata_bucket_name, "replicaNumber": self.num_replicas})
            bucket_dst = self.bucket_util.create_bucket(bucket_params_dst)
            bucket_meta = self.bucket_util.create_bucket(bucket_params_meta)
            self.buckets = self.bucket_util.get_all_buckets(self.cluster.master)
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.expiry = 3

    def tearDown(self):
        super(EventingSanity, self).tearDown()

    def test_create_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_delete_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_DELETE, worker_count=3)
        self.deploy_function(body)
        # delete all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, on_delete=True)
        self.undeploy_and_delete_function(body)

    def test_expiry_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 1, bucket=self.src_bucket_name)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_DELETE, worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the expiry mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, on_delete=True)
        self.undeploy_and_delete_function(body)

    def test_update_mutation_for_dcp_stream_boundary_from_now(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE,
                                              dcp_stream_boundary="from_now", sock_batch_size=1, worker_count=4,
                                              cpp_worker_thread_count=4)
        self.deploy_function(body)
        # update all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="update")
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_n1ql_query_execution_from_handler_code(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE, worker_count=3)
        # Enable this after MB-26527 is fixed
        # sock_batch_size=10, worker_count=4, cpp_worker_thread_count=4)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_doc_timer_events_from_handler_code_with_n1ql(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE_WITH_DOC_TIMER,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_cron_timer_events_from_handler_code_with_n1ql(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE_WITH_CRON_TIMER,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_doc_timer_events_from_handler_code_with_bucket_ops(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_cron_timer_events_from_handler_code_with_bucket_ops(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMER,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_delete_bucket_operation_from_handler_code(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        # delete all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_timers_without_context(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_TIMER_WITHOUT_CONTEXT,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_cancel_timers_with_timers_being_overwritten(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_TIMER_OVERWRITTEN,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_source_doc_mutations(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_DOC_MUTATION,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        #self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        #self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        # self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        # delete all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        # self.verify_source_bucket_mutation(self.docs_per_day * 2016,deletes=True,timeout=1200)
        self.undeploy_and_delete_function(body)

    def test_source_doc_mutations_with_timers(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_DOC_MUTATION_WITH_TIMERS,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        #self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        #self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        # self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        # delete all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        # self.verify_source_bucket_mutation(self.docs_per_day * 2016,deletes=True,timeout=1200)
        self.undeploy_and_delete_function(body)

    def test_source_bucket_mutations(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        #self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        #self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 4032, skip_stats_validation=True)
        # delete all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_source_bucket_mutations_with_timers(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_WITH_TIMERS,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        #self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        #self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 4032, skip_stats_validation=True)
        # delete all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_pause_resume_execution(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        self.deploy_function(body)
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        self.pause_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(60)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")
        self.gens_load = self.generate_docs(self.docs_per_day*2)
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        self.resume_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016*2,skip_stats_validation=True)
        self.undeploy_and_delete_function(body)


    def test_source_bucket_mutation_for_dcp_stream_boundary_from_now(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name,HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION ,
                                              dcp_stream_boundary="from_now", sock_batch_size=1, worker_count=4,
                                              cpp_worker_thread_count=4)
        self.deploy_function(body)
        # update all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="update")
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016*2)
        self.undeploy_and_delete_function(body)

    def test_compress_handler(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name,"handler_code/compress.js")
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
