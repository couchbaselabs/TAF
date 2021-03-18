from random import choice

from bucket_collections.app.app_basetest import AppBase
from bucket_collections.app.constants import global_vars
from bucket_collections.app.lib import query_util
from bucket_collections.app.scenarios.airline import Airline
from bucket_collections.app.scenarios.guest import Guest
from bucket_collections.app.scenarios.hotel import Hotel
from bucket_collections.app.scenarios.user import User


class TravelSampleApp(AppBase):
    def setUp(self):
        super(TravelSampleApp, self).setUp()

        self.log_setup_status("TravelSampleApp", "started")
        self.monitor_ops_rate = self.input.param("monitor_ops_rate", False)

        # Start monitoring doc_ops
        if self.monitor_ops_rate:
            self.bucket.stats.manage_task("start", self.task_manager,
                                          cluster=self.cluster,
                                          bucket=self.bucket,
                                          monitor_stats=["doc_ops"],
                                          sleep=1)

        if self.initial_load:
            self.__load_intial_data()

        self.app_iteration = self.input.param("iteration", 1)
        global_vars.app_current_date = query_util.CommonUtil.get_current_date()
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

    def __load_intial_data(self):
        pass

    def run_app(self):
        random_op = "random"

        itr_index = 1
        while itr_index <= self.app_iteration:
            self.log.info("#### Iteration :: %d ####" % itr_index)
            tasks = list()

            user_activity_1 = User(self.bucket, op_type=random_op,
                                   op_count=100)
            user_activity_2 = User(self.bucket, op_type=random_op,
                                   op_count=100)
            guest_activity_1 = Guest(self.bucket, op_type=random_op,
                                     op_count=20)
            guest_activity_2 = Guest(self.bucket, op_type=random_op,
                                     op_count=20)
            hotel_activity_1 = Hotel(self.bucket, op_type=random_op,
                                     op_count=20)
            airline_activity_1 = Airline(self.bucket, op_type=random_op,
                                         op_count=20)

            tasks.append(user_activity_1)
            tasks.append(guest_activity_1)
            tasks.append(guest_activity_2)
            tasks.append(user_activity_2)
            tasks.append(hotel_activity_1)
            tasks.append(airline_activity_1)

            # Start all threads
            for task in tasks:
                task.start()

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
                query_util.CommonUtil.incr_date()

            itr_index += 1
