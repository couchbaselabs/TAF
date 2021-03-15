from random import choice, sample
from ruamel.yaml import YAML

from Cb_constants import DocLoading
from bucket_collections.app.app_basetest import AppBase
from bucket_collections.app.constants import global_vars
from bucket_collections.app.lib import query_util
from bucket_collections.app.scenarios.airline import Airline
from bucket_collections.app.scenarios.guest import Guest
from bucket_collections.app.scenarios.hotel import Hotel
from bucket_collections.app.scenarios.user import User

from com.couchbase.client.java.json import JsonObject


class TravelSampleApp(AppBase):
    def setUp(self):
        super(TravelSampleApp, self).setUp()

        self.log_setup_status("TravelSampleApp", "started")
        self.monitor_ops_rate = self.input.param("monitor_ops_rate", False)
        self.playbook = self.input.param("playbook", "steady_state")
        self.activities = list()

        # Start monitoring doc_ops
        if self.monitor_ops_rate:
            self.bucket.stats.manage_task("start", self.task_manager,
                                          cluster=self.cluster,
                                          bucket=self.bucket,
                                          monitor_stats=["doc_ops"],
                                          sleep=1)
        # Fetch all tenants from the bucket (Scope will collection "meta_data")
        self.tenants = list()
        for scope_name, scope in self.bucket.scopes.items():
            for c_name, _ in scope.collections.items():
                if c_name == "meta_data":
                    self.tenants.append(scope_name)
                    break

        if self.initial_load:
            self.__load_initial_data()

        self.app_iteration = self.input.param("iteration", 1)
        global_vars.app_current_date = \
            query_util.CommonUtil.get_current_date(self.tenants[0])

        with open(self.app_path + "/scenarios/" + self.playbook + ".yaml",
                  "r") as fp:
            self.activities = YAML().load(fp.read())["activities"]

        self.log_setup_status("TravelSampleApp", "complete")

    def tearDown(self):
        # Stop monitoring doc_ops
        if self.monitor_ops_rate:
            self.bucket.stats.manage_task("stop", self.task_manager,
                                          cluster=self.cluster,
                                          bucket=self.bucket,
                                          monitor_stats=["doc_ops"],
                                          sleep=1)

        # Start tearDown process
        super(TravelSampleApp, self).tearDown()

    def __load_initial_data(self):
        # Create collection meta_data document
        sdk_client = self.sdk_clients["bucket_data_writer"]
        for tenant in self.tenants:
            sdk_client.select_collection(scope_name=tenant,
                                         collection_name="meta_data")
            app_data = JsonObject.create()
            app_data.put("date", "2001-01-01")
            result = sdk_client.crud(DocLoading.Bucket.DocOps.CREATE,
                                     "application", app_data)
            self.assertTrue(result["status"], "App_meta creation failed")
            self.bucket.scopes[self.tenants[0]].collections["meta_data"]\
                .num_items += 1

            create_users = User(self.bucket,
                                scope=tenant,
                                op_type="scenario_user_registration",
                                num_items=10000)
            create_users.start()
            create_users.join()

    def run_app(self):
        default_op_count = 10
        random_op = "random"
        all_tenants = "all"

        itr_index = 1
        while itr_index <= self.app_iteration:
            self.log.info("#### Iteration :: %d ####" % itr_index)
            tasks = list()
            for activity in self.activities:
                task = None
                tenants = self.tenants
                activity_type = activity.get("type")
                op_type = activity.get("op_type", random_op)
                op_count = activity.get("op_count", default_op_count)
                num_tenant = activity.get("tenants", all_tenants)
                if type(num_tenant) is int:
                    tenants = sample(self.tenants, activity["tenants"])
                if activity_type == "user":
                    for tenant in tenants:
                        task = User(self.bucket, tenant,
                                    op_type=op_type,
                                    op_count=op_count)
                elif activity_type == "guest":
                    for tenant in tenants:
                        task = Guest(self.bucket, tenant,
                                     op_type=op_type,
                                     op_count=op_count)
                elif activity_type == "hotel":
                    for tenant in tenants:
                        task = Hotel(self.bucket, tenant,
                                     op_type=op_type,
                                     op_count=op_count)
                elif activity_type == "airline":
                    for tenant in tenants:
                        task = Airline(self.bucket, tenant,
                                       op_type=op_type,
                                       op_count=op_count)
                else:
                    self.fail("Unsupported activity_type: %s" % activity_type)

                # Start the activity
                task.start()
                # Append the task to the list for tracking
                tasks.append(task)

            # Wait for threads to complete
            for task in tasks:
                task.join()

            # for task in tasks:
            #     if task.exception:
            #         self.fail(task.exception)

            # Print current iteration summary (Possible values)
            # Backup and restore

            # Check for core dumps / critical messages in logs
            result = self.check_coredump_exist(self.servers,
                                               force_collect=True)
            self.assertFalse(result, "CRASH | CRITICAL | WARN messages "
                                     "found in cb_logs")

            if choice(range(0, 9)) == 10:
                query_util.CommonUtil.incr_date(self.tenants)

            itr_index += 1
