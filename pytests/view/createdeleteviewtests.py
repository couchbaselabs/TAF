from basetestcase import BaseTestCase
from couchbase_helper.document import View, DesignDocument
from membase.api.exception import ReadDocumentException
from membase.api.rest_client import RestConnection
from couchbase_helper.documentgenerator import DocumentGenerator


class CreateDeleteViewTests(BaseTestCase):
    def setUp(self):
        try:
            super(CreateDeleteViewTests, self).setUp()
            self.bucket_ddoc_map = {}
            self.ddoc_ops = self.input.param("ddoc_ops", None)
            self.boot_op = self.input.param("boot_op", None)
            self.nodes_in = self.input.param("nodes_in", 1)
            self.nodes_out = self.input.param("nodes_out", 1)
            self.test_with_view = self.input.param("test_with_view", False)
            self.num_views_per_ddoc = self.input.param("num_views_per_ddoc", 1)
            self.num_ddocs = self.input.param("num_ddocs", 1)
            self.gen = None
            self.default_design_doc_name = "Doc1"
            self.default_map_func = 'function (doc) { emit(doc.age, doc.first_name);}'
            self.updated_map_func = 'function (doc) { emit(null, doc);}'
            self.default_view = View("View", self.default_map_func, None, False)
            self.fragmentation_value = self.input.param("fragmentation_value", 80)
            self.nodes_init = self.input.param("nodes_init", 1)
            self.nodes_in = self.input.param("nodes_in", 1)
            self.nodes_out = self.input.param("nodes_out", 1)
            self.wait_timeout = self.input.param("wait_timeout", 60)
            nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
            self.task.rebalance([self.cluster.master], nodes_init, [])
            self.cluster.nodes_in_cluster.append(self.cluster.master)
            self.bucket_util.create_default_bucket()
            self.bucket_util.add_rbac_user()
        except Exception as ex:
            self.input.test_params["stop-on-failure"] = True
            self.log.error("SETUP WAS FAILED. ALL TESTS WILL BE SKIPPED")
            self.fail(ex)

    def tearDown(self):
        super(CreateDeleteViewTests, self).tearDown()


    def _execute_ddoc_ops(self, ddoc_op_type, test_with_view, num_ddocs,
                          num_views_per_ddoc, prefix_ddoc="dev_ddoc",
                          prefix_view="views", start_pos_for_mutation=0,
                          bucket="default", check_replication=True):
        """
        Synchronously execute create/update/delete operations on a bucket and
        create an internal dictionary of the objects created. For update/delete operation,
        number of ddocs/views to be updated/deleted with be taken from sequentially from the position specified by
        start_pos_for_mutation.

        :param ddoc_op_type: Operation Type (create/update/delete). (String)
        :param test_with_view: If operations need to be executed on views. (Boolean)
        :param num_ddocs: Number of Design Documents to be created/updated/deleted. (Number)
        :param num_views_per_ddoc: Number of Views per DDoc to be created/updated/deleted. (Number)
        :param prefix_ddoc: Prefix for the DDoc name. (String)
        :param prefix_view: Prefix of the View name. (String)
        :param start_pos_for_mutation: Start index for the update/delete operation
        :param bucket: The name of the bucket on which to execute the operations. (String)
        :param check_replication: Check for replication (Boolean)
        :return: None
        """
        if ddoc_op_type == "create":
            self.log.info("Processing Create DDoc Operation On Bucket {0}".format(bucket))
            # if there are ddocs already, add to that else start with an empty map
            ddoc_view_map = self.bucket_ddoc_map.pop(bucket, {})
            for ddoc_count in xrange(num_ddocs):
                design_doc_name = prefix_ddoc + str(ddoc_count)
                view_list = []
                # Add views if flag is true
                if test_with_view:
                    # create view objects as per num_views_per_ddoc
                    view_list = self.bucket_util.make_default_views(self.default_view, prefix_view, num_views_per_ddoc)
                # create view in the database
                self.bucket_util.create_views(self.cluster.master, design_doc_name, view_list, bucket, self.wait_timeout * 2,
                                  check_replication=check_replication)
                # store the created views in internal dictionary
                ddoc_view_map[design_doc_name] = view_list
            # store the ddoc-view dict per bucket
            self.bucket_ddoc_map[bucket] = ddoc_view_map
        elif ddoc_op_type == "update":
            self.log.info("Processing Update DDoc Operation On Bucket {0}".format(bucket))
            # get the map dict for the bucket
            ddoc_view_map = self.bucket_ddoc_map[bucket]
            ddoc_map_loop_cnt = 0
            # iterate for all the ddocs
            for ddoc_name, view_list in ddoc_view_map.items():
                if ddoc_map_loop_cnt < num_ddocs:
                    # Update views if flag is true
                    if test_with_view:
                        # iterate and update all the views as per num_views_per_ddoc
                        for view_count in xrange(num_views_per_ddoc):
                            # create new View object to be updated
                            updated_view = View(view_list[start_pos_for_mutation + view_count].name,
                                                self.updated_map_func, None, False)
                            self.bucket_util.create_view(self.cluster.master, ddoc_name, updated_view, bucket,
                                                     self.wait_timeout * 2, check_replication=check_replication)
                    else:
                        # update the existing design doc(rev gets updated with this call)
                        self.bucket_util.create_view(self.cluster.master, ddoc_name, None, bucket, self.wait_timeout * 2,
                                                 check_replication=check_replication)
                    ddoc_map_loop_cnt += 1
        elif ddoc_op_type == "delete":
            self.log.info("Processing Delete DDoc Operation On Bucket {0}".format(bucket))
            # get the map dict for the bucket
            ddoc_view_map = self.bucket_ddoc_map[bucket]
            ddoc_map_loop_cnt = 0
            # iterate for all the ddocs
            for ddoc_name, view_list in ddoc_view_map.items():
                if ddoc_map_loop_cnt < num_ddocs:
                    # Update views if flag is true
                    if test_with_view:
                        for view_count in xrange(num_views_per_ddoc):
                            # iterate and update all the views as per num_views_per_ddoc
                            self.bucket_util.delete_view(self.cluster.master, ddoc_name,
                                                     view_list[start_pos_for_mutation + view_count], bucket,
                                                     self.wait_timeout * 2)
                        # store the updated view list
                        ddoc_view_map[ddoc_name] = view_list[:start_pos_for_mutation] + view_list[
                                                                                        start_pos_for_mutation + num_views_per_ddoc:]
                    else:
                        # delete the design doc
                        self.bucket_util.delete_view(self.cluster.master, ddoc_name, None, bucket, self.wait_timeout * 2)
                        # remove the ddoc_view_map
                        del ddoc_view_map[ddoc_name]
                    ddoc_map_loop_cnt += 1
            # store the updated ddoc dict
            self.bucket_ddoc_map[bucket] = ddoc_view_map
        else:
            self.log.exception("Invalid ddoc operation {0}. No execution done.".format(ddoc_op_type))

    def _verify_ddoc_ops_all_buckets(self):
        """
        Verify number of Design Docs/Views on all buckets
        comparing with the internal dictionary of the create/update/delete ops
        :return:
        """
        self.log.info("DDoc Validation Started")
        rest = RestConnection(self.cluster.master)
        #Iterate over all the DDocs/Views stored in the internal dictionary
        for bucket, self.ddoc_view_map in self.bucket_ddoc_map.items():
            for ddoc_name, view_list in self.ddoc_view_map.items():
                try:
                    #fetch the DDoc information from the database
                    ddoc_json, header = rest.get_ddoc(bucket, ddoc_name)
                    self.log.info('Database Document {0} details : {1}'.format(ddoc_name, json.dumps(ddoc_json)))
                    ddoc = DesignDocument._init_from_json(ddoc_name, ddoc_json)
                    for view in view_list:
                        if view.name not in [v.name for v in ddoc.views]:
                            self.fail("Validation Error: View - {0} in Design Doc - {1} and Bucket - {2} is missing from database".format(view.name, ddoc_name, bucket))

                except ReadDocumentException:
                    self.fail("Validation Error: Design Document - {0} is missing from Bucket - {1}".format(ddoc_name, bucket))

        self.log.info("DDoc Validation Successful")

    def _verify_ddoc_data_all_buckets(self):
        """
        Verify the number of Documents stored in DDoc/Views for all buckets
        :return:
        """
        rest = RestConnection(self.cluster.master)
        query = {"stale" : "false", "full_set" : "true", "connection_timeout" : 60000}
        for bucket, self.ddoc_view_map in self.bucket_ddoc_map.items():
            num_items = sum([len(kv_store) for kv_store in bucket.kvs.values()])
            self.log.info("DDoc Data Validation Started on bucket {0}. Expected Data Items {1}".format(bucket, num_items))
            for ddoc_name, view_list in self.ddoc_view_map.items():
                for view in view_list:
                    result = self.bucket_util.query_view(self.cluster.master, ddoc_name, view.name, query, num_items, bucket)
                    if not result:
                        self.fail("DDoc Data Validation Error: View - {0} in Design Doc - {1} and Bucket - {2}".format(view.name, ddoc_name, bucket))
        self.log.info("DDoc Data Validation Successful")

    def test_view_ops(self):
        tasks = []
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "mutated" : 0, "age": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator('test_docs', template, age, first,
                                     start=0,
                                     end=self.num_items,
                                     key_size=self.key_size,
                                     doc_size=self.doc_size,
                                     doc_type=self.doc_type)
        for bucket in self.bucket_util.buckets:
            tasks.append(self.task.async_load_gen_docs(self.cluster, bucket, gen_load, "create", 0,
                                                       batch_size=20, persist_to=self.persist_to,
                                                       replicate_to=self.replicate_to,
                                                       pause_secs=5, timeout_secs=self.sdk_timeout,
                                                       retries=self.sdk_retries))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc, bucket=bucket)
            if self.ddoc_ops in ["update", "delete"]:
                self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view,
                                       self.num_ddocs / 2, self.num_views_per_ddoc / 2, bucket=bucket)

        self.bucket_util._wait_for_stats_all_buckets()
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()