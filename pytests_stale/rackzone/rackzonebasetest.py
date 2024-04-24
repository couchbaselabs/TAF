from basetestcase import ClusterSetup
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from membase.api.rest_client import RestConnection
from platform_constants.os_constants import Linux, Mac, Windows
from remote.remote_util import RemoteMachineShellConnection


class RackzoneBaseTest(ClusterSetup):

    def setUp(self):
        super(RackzoneBaseTest, self).setUp()
        self.product = self.input.param("product", "cb")
        self.version = self.input.param("version", "2.5.1-1082")
        self.type = self.input.param('type', 'enterprise')
        self.doc_ops = self.input.param("doc_ops", None)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        self.default_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"

        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        self.doc_ops = self.input.param("doc_ops", "create")
        # define the data that will be used to test
        self.blob_generator = self.input.param("blob_generator", False)
        server_info = self.servers[0]
        rest = RestConnection(server_info)
        if not rest.is_enterprise_edition():
            raise Exception("This couchbase server is not Enterprise Edition.\
                  This RZA feature requires Enterprise Edition to work")
        if self.blob_generator:
            # gen_load data is used for upload before each test
            self.gen_load = BlobGenerator('test', 'test-', self.doc_size,
                                          end=self.num_items)
            # gen_update is used for doing mutation for 1/2th of uploaded data
            self.gen_update = BlobGenerator('test', 'test-',
                                            self.doc_size,
                                            end=(self.num_items / 2 - 1))
            # upload data before each test
            tasks = []
            for bucket in self.cluster.buckets:
                tasks.append(self.task.async_load_gen_docs(
                    self.cluster, bucket, self.gen_load, "create", 0,
                    batch_size=20, persist_to=self.persist_to,
                    replicate_to=self.replicate_to,
                    pause_secs=5, timeout_secs=self.sdk_timeout,
                    retries=self.sdk_retries,
                    load_using=self.load_docs_using))
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
        else:
            tasks = []
            age = range(5)
            first = ['james', 'sharon']
            template = '{{ "mutated" : 0, "age": {0}, "first_name": "{1}" }}'
            self.gen_load = DocumentGenerator('test_docs', template, age,
                                              first, start=0,
                                              end=self.num_items)
            for bucket in self.cluster.buckets:
                tasks.append(self.task.async_load_gen_docs(
                    self.cluster, bucket, self.gen_load, "create", 0,
                    batch_size=20, persist_to=self.persist_to,
                    replicate_to=self.replicate_to,
                    pause_secs=5, timeout_secs=self.sdk_timeout,
                    retries=self.sdk_retries,
                    load_using=self.load_docs_using))
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
        shell = RemoteMachineShellConnection(self.cluster.master)
        s_type = shell.extract_remote_info().distribution_type
        shell.disconnect()
        self.os_name = Linux.NAME
        self.is_linux = True
        self.cbstat_command = "%scbstats" % Linux.COUCHBASE_BIN_PATH
        if s_type.lower() == Windows.NAME:
            self.is_linux = False
            self.os_name = Windows.NAME
            self.cbstat_command = "%scbstats.exe" % Windows.COUCHBASE_BIN_PATH
        elif s_type.lower() == Mac.NAME:
            self.cbstat_command = "%scbstats" % Mac.COUCHBASE_BIN_PATH
        if self.nonroot:
            self.cbstat_command = "/home/%s%scbstats" \
                                  % (self.cluster.master.ssh_username,
                                     Linux.COUCHBASE_BIN_PATH)

    def tearDown(self):
        """ Some test involve kill couchbase server.  If the test steps failed
            right after kill erlang process, we need to start couchbase server
            in teardown so that the next test will not be false failed """
        super(RackzoneBaseTest, self).tearDown()
        self.cluster_util.cleanup_cluster(self.cluster,
                                          master=self.cluster.master)
        for server in self.cluster.servers:
            shell = RemoteMachineShellConnection(server)
            shell.start_couchbase()
            self.sleep(7, "Wait for couchbase server start")
        server_info = self.servers[0]
        rest = RestConnection(server_info)
        zones = rest.get_zone_names()
        for zone in zones:
            if zone != "Group 1":
                rest.delete_zone(zone)
