from dcp_new.dcp_base import DCPBase
from dcp_new.constants import *
from dcp_bin_client import *
from memcacheConstants import *
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import Node, RestConnection
from itertools import count
from Cb_constants import CbServer
from couchbase_helper.documentgenerator import doc_generator
import json
from cb_tools.cbstats import Cbstats
from threading import Thread

class DcpTestCase(DCPBase):
    def setUp(self):
        super(DcpTestCase, self).setUp()

    def check_dcp_event(self, collection_name, output_string, event="create_collection", count=1):
        if event == "create_collection":
            cmd = "CollectionCREATED, name:"+ collection_name
            event_count = len(list(filter(lambda x: cmd in x, output_string))) * count
            if event_count == len(self.vbuckets):
                self.log.info("number of collection creation event matches %s"% event_count)
            else:
                self.log_failure("mismatch in collection creation event, " \
                "expected:%s but actual %s"% (len(self.vbuckets), event_count))

        if event == "drop_collection":
            event_count = len(list(filter(lambda x: "CollectionDROPPED" in x, output_string)))
            if event_count == len(self.vbuckets):
                self.log.info("number of collection drop event matches %s"% event_count)
            else:
                self.log_failure("mismatch in collection drop events, " \
                "expected:%s but actual %s"% (len(self.vbuckets), event_count))

        if event == "create_scope":
            cmd = "ScopeCREATED, name:"+ collection_name
            event_count = len(list(filter(lambda x: cmd in x, output_string)))
            if event_count == len(self.vbuckets):
                self.log.info("number of scope creation event matches %s"% event_count)
            else:
                self.log_failure("mismatch in scope creation event, " \
                "expected:%s but actual %s"% (len(self.vbuckets), event_count))

        if event == "drop_scope":
            event_count = len(list(filter(lambda x: "ScopeDROPPED" in x, output_string)))
            if event_count == len(self.vbuckets):
                self.log.info("number of Scope drop event matches %s"% event_count)
            else:
                self.log_failure("mismatch in Scope drop events, " \
                "expected:%s but actual %s"% (len(self.vbuckets), event_count))

    def get_dcp_event(self):
        streams = self.add_streams(self.vbuckets,
                                  self.start_seq_no_list,
                                  self.end_seq_no,
                                  self.vb_uuid_list,
                                  self.vb_retry, self.filter_file)
        output = self.process_dcp_traffic(streams)
        self.close_dcp_streams()
        return output

    def close_dcp_streams(self):
        for client_stream in self.dcp_client_dict.values():
            client_stream['stream'].close()

    def get_collection_id(self, bucket_name, scope_name, collection_name=None):
        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstats = Cbstats(shell)
        if collection_name:
            field = scope_name + ':' + collection_name + ':' + 'id:'
            cid_stat = cbstats.get_stats(bucket_name, "collections", field)[0][0]
        else:
            field = scope_name + ':id:'
            cid_stat = cbstats.get_stats(bucket_name, "scopes", field)[0][0]
        return cid_stat.split('0x')[-1].strip()

    def get_total_items_scope(self, bucket, scope_name):
        scope_items= 0
        scope_list = self.bucket_util.get_active_scopes(bucket)
        for scope in scope_list:
            if scope.name == scope_name:
                collection_list = scope.collections.values()
                for collection in collection_list:
                    scope_items += collection.num_items
        return scope_items

    def get_scope_name(self):
        retry =5
        while retry > 0:
            scope_dict = self.bucket_util.get_random_scopes(
                                self.bucket_util.buckets, 1, 1)
            scope_name = scope_dict[self.bucket.name]["scopes"].keys()[0]
            if scope_name != CbServer.default_scope:
                break
            retry -= 1
        return scope_name

    def rebalance_in(self):
        servs_in = [self.cluster.servers[0 + self.nodes_init]]
        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, [])
        self.sleep(10)
        self.task_manager.get_task_result(rebalance_task)
        if rebalance_task.result is False:
                self.fail("Rebalance failed")

    def drop_scope(self):
        scope_name = self.get_scope_name()
        if scope_name != CbServer.default_scope:
            self.bucket_util.drop_scope(
                self.cluster.master, self.bucket, scope_name)
        return scope_name

    def drop_collection(self):
        collections = self.bucket_util.get_random_collections(
                                    [self.bucket], 1, 1, 1)
        scope_dict = collections[self.bucket.name]["scopes"]
        scope_name = scope_dict.keys()[0]
        collection_name = scope_dict[scope_name]["collections"].keys()[0]
        self.bucket_util.drop_collection(self.cluster.master,
                                        self.bucket,
                                        scope_name,
                                        collection_name)
        return scope_name, collection_name

    def __perform_operation(self):
        if self.operation == "rebalance":
            self.rebalance_in()

        elif self.operation == "replica_update":
            self.rebalance_in()
            self.sleep(10)
            self.bucket_util.update_all_bucket_replicas(1)

        elif self.operation == "drop_scope":
            self.scope_name = self.drop_scope()

        elif self.operation == "recreate_scope":
            self.scope_name = self.drop_scope()
            print self.scope_name
            self.bucket_util.create_scope(self.cluster.master,
                                         self.bucket,
                                         {"name": self.scope_name})

        elif self.operation == "drop_collection":
            self.drop_collection()

        elif self.operation == "recreate_collection":
            self.scope_name, self.collection_name = self.drop_collection()
            print(self.scope_name, self.collection_name)
            self.bucket_util.create_collection(self.cluster.master,
                                           self.bucket_util.buckets[0],
                                           self.scope_name,
                                           {"name": self.collection_name})

        elif self.operation == "load_data":
            doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package("def_load_random_collection")

            self.bucket_util.run_scenario_from_spec(self.task,
                                                    self.cluster,
                                                    self.bucket_util.buckets,
                                                    doc_loading_spec,
                                                    mutation_num=0)

        elif self.operation == "kill_memcached":
            self.remote_shell = RemoteMachineShellConnection(self.cluster.master)
            if self.remote_shell.info.type.lower()== 'windows':
                self._execute_command('taskkill /F /T /IM memcached*')
            else:
                self._execute_command('killall -9 memcached')
            # wait for server to be up
            self.wait_for_warmup(self.cluster.master.ip,  self.cluster.master.port)

    def verify_operation(self, operation, mutation_count):
        self.dcp_client = self.initialise_cluster_connections()
        output_string = self.get_dcp_event()
        actual_item_count = len(list(filter(
                    lambda x: 'CMD_MUTATION' in x, output_string)))

        if operation == "drop_scope":
            self.check_dcp_event(self.scope_name,
                                    output_string, "drop_scope")

        if operation == "drop_collection":
            self.check_dcp_event(self.collection_name,
                                    output_string, "drop_collection")

        if operation == "recreate_scope":
            self.check_dcp_event(self.scope_name,
                                    output_string, "drop_scope")
            self.check_dcp_event(self.scope_name,
                                   output_string, "create_scope")

        if operation == "recreate_collection":
            self.check_dcp_event(self.collection_name,
                                    output_string, "drop_collection")
            self.check_dcp_event(self.collection_name,
                                    output_string, "create_collection", 2)

        if operation == "load_data":
            if mutation_count == actual_item_count:
                self.log_failure("mutation count not changed")
        else:
            if mutation_count != actual_item_count:
                self.log_failure("mutation count same as expected")

    def test_stream_entire_bucket(self):
        # get all the scopes
        scope_list = self.bucket_util.get_active_scopes(self.bucket)

        # drop scope before streaming dcp events
        scope_name = self.get_scope_name()

        if scope_name != CbServer.default_scope:
            self.collection_list = \
                self.bucket_util.get_active_collections(self.bucket, scope_name)
            self.bucket_util.drop_scope(self.cluster.master, self.bucket, scope_name)

        # stream dcp events and verify events
        output_string = self.get_dcp_event()
        rest = RestConnection(self.cluster.master)
        expected_item_count = sum(rest.get_buckets_itemCount().values())
        actual_item_count = len(list(filter(lambda x: 'CMD_MUTATION' in x, output_string)))
        if expected_item_count != actual_item_count:
            self.log_failure("item count mismatch, expected %s actual %s"\
                             %(expected_item_count, actual_item_count))

        for scope in scope_list:
            if scope.name != CbServer.default_scope:
                self.check_dcp_event(scope.name, output_string, "create_scope")
            collection_list = scope.collections.values()
            for collection in collection_list:
                if collection.name != CbServer.default_collection:
                    self.check_dcp_event(collection.name, output_string)

        if scope_name != CbServer.default_scope:
            self.check_dcp_event(scope_name, output_string, "drop_scope")
            for collection in self.collection_list:
                self.check_dcp_event(collection.name, output_string, "drop_collection")

        self.validate_test_failure()

    def test_stream_drop_default_collection(self):
        # drop default collection 
        self.bucket_util.drop_collection(self.cluster.master,
                                         self.bucket,
                                         CbServer.default_scope,
                                         CbServer.default_collection)
        #get dcp events
        output_string = self.get_dcp_event()
        self.check_dcp_event(CbServer.default_collection,
                                output_string, "drop_collection")
        self.validate_test_failure()

    def test_stream_specific_collection(self):
        # get random collection
        self.num_items = 1000
        collections = self.bucket_util.get_random_collections(
                                    [self.bucket], 1, 1, 1)
        scope_dict = collections[self.bucket.name]["scopes"]
        scope_name = scope_dict.keys()[0]
        collection_name = scope_dict[scope_name]["collections"].keys()[0]
        bucket = self.bucket_util.get_bucket_obj(self.bucket_util.buckets,
                                                self.bucket.name)
        bucket.scopes[scope_name] \
                            .collections[collection_name].num_items \
                            += self.num_items

        # load to the collection
        load_gen = doc_generator('test_drop_default',
                                 0, self.num_items,
                                 mutate=0,
                                 target_vbucket=self.target_vbucket)
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, load_gen, "create", self.maxttl,
            batch_size=10, process_concurrency=2,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            scope=scope_name,
            collection=collection_name,
            suppress_error_table=True)
        self.task_manager.get_task_result(task)

        # get the uid and stream dcp data for that collection
        cid = self.get_collection_id(self.bucket.name, scope_name, collection_name)
        self.filter_file = {"collections": [cid]}
        self.filter_file = json.dumps(self.filter_file)
        output_string = self.get_dcp_event()

        # verify item count
        actual_item_count = len(list(filter(lambda x: 'CMD_MUTATION' in x, output_string)))
        if actual_item_count != self.bucket.scopes[scope_name] \
                            .collections[collection_name].num_items:
            self.log_failure("item count mismatch, expected %s actual %s"\
                             %(self.bucket.scopes[scope_name] \
                               .collections[collection_name].num_items,
                               actual_item_count))
        self.validate_test_failure()

    def test_stream_scope(self):
        self.create_collection = self.input.param("create_collection", False)
        # get a random scope
        scope_dict = self.bucket_util.get_random_scopes(
                                self.bucket_util.buckets, 1, 1)
        scope_name = scope_dict[self.bucket.name]["scopes"].keys()[0]

        if self.create_collection:
            collection_name = self.bucket_util.get_random_name()
            self.bucket_util.create_collection(self.cluster.master,
                                           self.bucket,
                                           scope_name,
                                           collection_name)

        # get scope id and create a filter file
        scope_id = self.get_collection_id(self.bucket.name, scope_name)
        self.filter_file = {"scope": scope_id}
        self.filter_file = json.dumps(self.filter_file)
        output_string = self.get_dcp_event()

        # verify the item count
        total_items_scope = self.get_total_items_scope(self.bucket, scope_name)
        actual_item_count = len(list(filter(lambda x: 'CMD_MUTATION' in x, output_string)))
        if actual_item_count != total_items_scope:
            self.log_failure("item count mismatch, expected %s actual %s"\
                             %(total_items_scope, actual_item_count))

        if self.create_collection:
            self.check_dcp_event(collection_name, output_string)
        self.validate_test_failure()

    def test_stream_multiple_collections(self):
        self.num_collection_stream = self.input.param("num_collection", 2)
        collections = self.bucket_util.get_random_collections(
                                    [self.bucket], "all", "all",
                                    self.num_collection_stream)

        list_uid = []
        total_items = 0
        for self.bucket_name, scope_dict in collections.iteritems():
            bucket = self.bucket_util.get_bucket_obj(self.bucket_util.buckets,
                                                self.bucket_name)
            scope_dict = scope_dict["scopes"]
            for scope_name, collection_dict in scope_dict.items():
                collection_dict = collection_dict["collections"]
                for c_name, _ in collection_dict.items():
                    list_uid.append(self.get_collection_id(self.bucket_name,
                                                 scope_name, c_name))
                    total_items += bucket.scopes[scope_name] \
                            .collections[c_name].num_items

        self.filter_file = {"collections": list_uid}
        self.filter_file = json.dumps(self.filter_file)
        output_string = self.get_dcp_event()

        # verify item count
        actual_item_count = len(list(filter(lambda x: 'CMD_MUTATION' in x, output_string)))
        if actual_item_count != total_items:
            self.log_failure("item count mismatch, expected %s actual %s"\
                             %(total_items, actual_item_count))
        self.validate_test_failure()

    def test_dcp_stream_check(self):
        # load to specific vbucket
        self.vbuckets = [100]
        self.operation = self.input.param("operation", "load_data")
        stream = self.dcp_client.stream_req(100, 0, 0, 10, 0)
        assert stream.status is SUCCESS
        stream.run()
        self.mutation_count = stream.mutation_count

        self.__perform_operation()
        self.verify_operation(self.operation, stream.mutation_count)
        self.validate_test_failure()

    def test_dcp_stream_disconnect(self):
        # needs single vbucket load
        self.operation = self.input.param("operation", "load_data")
        proc1 = Thread(target=self.get_dcp_event,
                       args=())

        proc2 = Thread(target=self.__perform_operation,
                       args=())

        proc1.start()
        proc2.start()
        self.sleep(2)
        proc2.join()
        proc1.join()

        rest = RestConnection(self.cluster.master)
        expected_item_count = sum(rest.get_buckets_itemCount().values())
        self.verify_operation(self.operation, expected_item_count)
        self.validate_test_failure()

