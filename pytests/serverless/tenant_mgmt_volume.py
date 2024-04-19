from random import choice, sample

from BucketLib.bucket import Bucket
from cb_constants import CbServer
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from serverless.tenant_mgmt_on_cloud import TenantMgmtOnCloud

from table_view import TableView


class TenantMgmtVolumeTest(TenantMgmtOnCloud):
    def setUp(self):
        super(TenantMgmtVolumeTest, self).setUp()

        self.step_no = 1
        self.banner = "#" * 70

        config_name = self.input.param("test_config", "default")
        self.__set_run_config(config_name)

        # Variable deciding the loading pattern
        """
        self.cluster.buckets index mapping:
        0 . . . . . . . . . . . . . . . . . . .. . . . . . . . . . . . . . . N
        <- idle - cont_updates - exp_load   ->< variable dbs sample(load_docs)>
        <self.num_buckets - no manual scaling><-Manual scaling->
        """
        self.max_buckets = \
            self.input.param("max_buckets", self.num_buckets)
        self.num_buckets_idle = self.input.param("num_buckets_idle", 0)
        self.num_buckets_for_expiry_load = \
            self.input.param("num_buckets_for_expiry_load", 0)
        self.num_buckets_for_cont_updates = \
            self.input.param("num_buckets_for_doc_updates", 0)
        self.num_buckets_to_load = \
            self.input.param("num_buckets_to_load_docs", 0)
        # End of doc_load pattern variables

        self.buckets_eligible_for_data_load = list()

        self.doc_ops_bucket_map = dict()
        self.idle_buckets = list()
        self.doc_ops_buckets = list()
        self.scaling_buckets = list()

        # Placeholder for doc_loading tasks (To clean it up in tearDown)
        self.cont_update_tasks = list()

        # Force initialize the pool object
        self.cluster.sdk_client_pool = \
            self.bucket_util.initialize_java_sdk_client_pool()
        self.generic_tbl = TableView(self.log.info)

    def tearDown(self):
        self.log.info("Stopping doc_ops for all buckets")
        for bucket_name in self.doc_ops_bucket_map.keys():
            self.__stop_doc_ops_for_bucket(
                self.__get_bucket_obj_with_name(bucket_name))

        delete_retry = 5
        while delete_retry > 0:
            delete_retry -= 1
            self.log.info("Cleaning up all buckets")
            try :
                for bucket in self.cluster.buckets:
                    name = bucket.name
                    self.log.info("%s - Closing sdk_clients" % name)
                    self.cluster.sdk_client_pool.force_close_clients_for_bucket(name)
                    self.log.info("%s - Triggering delete API" % name)
                    self.serverless_util.delete_database(self.pod, self.tenant,
                                                         bucket)
                    self.serverless_util.wait_for_database_deleted(
                        self.tenant, name)
                break
            except Exception as e:
                self.log.critical(e)
                self.sleep(15, "Wait before next retry")
            self.log.critical("Retry remaining: %s" % delete_retry)

        super(TenantMgmtVolumeTest, self).tearDown()

    def __set_run_config(self, config_name):
        test_config = dict()
        test_config["mini_run"] = {
            # Total steps: 5
            "dbs_create_pattern": [5, 5, 5, 5, 5, 0],
            "dbs_delete_with_width_1": [0, 1, 1, 1, 1],
            "dbs_delete_with_width_2": [0, 0, 1, 1, 1],
            "dbs_delete_with_width_3": [0, 0, 0, 1, 0],
            "dbs_delete_with_width_4": [0, 0, 0, 0, 1],
            "dbs_update_width_from_1": [1, 2, 2, 2, 2],
            "dbs_update_width_from_2": [0, 1, 2, 1, 2],
            "dbs_update_width_from_3": [0, 0, 1, 1, 2],
            "dbs_update_both_with_width_1": [1, 1, 2, 1, 1],
            "dbs_update_both_with_width_2": [0, 1, 1, 1, 0],
            "dbs_update_both_with_width_3": [0, 0, 0, 1, 0],
            "dbs_update_weight_with_width_1": [1] * 5,
            "dbs_update_weight_with_width_2": [1] * 5,
            "dbs_update_weight_with_width_3": [1] * 5,
            "dbs_update_weight_with_width_4": [1] * 5,
        }
        test_config["default"] = {
            # Total steps: 10
            "dbs_create_pattern": [5, 10, 10, 15, 15, 15, 15, 15, 25, 0],
            "dbs_delete_with_width_1": [0, 2, 1, 1, 1, 3, 3, 10, 10, 0],
            "dbs_delete_with_width_2": [0, 0, 1, 1, 1, 1, 1, 1, 2, 0],
            "dbs_delete_with_width_3": [0, 0, 0, 1, 0, 0, 0, 1, 1, 0],
            "dbs_delete_with_width_4": [0, 0, 0, 0, 0, 1, 0, 1, 1, 0],
            "dbs_update_width_from_1": [2, 5, 5, 5, 8, 7, 7, 7, 7, 5],
            "dbs_update_width_from_2": [0, 1, 2, 1, 2, 5, 5, 5, 5, 5],
            "dbs_update_width_from_3": [0, 0, 1, 1, 2, 1, 2, 2, 2, 5],
            "dbs_update_both_with_width_1": [1, 1, 2, 1, 1, 0, 0, 1, 1, 4],
            "dbs_update_both_with_width_2": [0, 1, 1, 1, 0, 0, 5, 1, 1, 2],
            "dbs_update_both_with_width_3": [0, 0, 0, 1, 0, 1, 1, 1, 1, 1],
            "dbs_update_weight_with_width_1":
                [2, 2, 5, 4, 7, 10, 10, 12, 15, 18, 20],
            "dbs_update_weight_with_width_2":
                [0, 1, 1, 2, 5, 5, 7, 9, 10, 10],
            "dbs_update_weight_with_width_3":
                [0, 0, 0, 0, 0, 1, 2, 4, 5, 10],
            "dbs_update_weight_with_width_4":
                [0, 0, 0, 1, 3, 4, 6, 8, 10, 13],
        }
        # Variables deciding the bucket create/delete/update pattern
        test_config = test_config[config_name]
        self.dbs_create_pattern = test_config["dbs_create_pattern"]
        self.dbs_delete_with_width_1 = test_config["dbs_delete_with_width_1"]
        self.dbs_delete_with_width_2 = test_config["dbs_delete_with_width_2"]
        self.dbs_delete_with_width_3 = test_config["dbs_delete_with_width_3"]
        self.dbs_delete_with_width_4 = test_config["dbs_delete_with_width_4"]
        self.dbs_update_width_from_1 = test_config["dbs_update_width_from_1"]
        self.dbs_update_width_from_2 = test_config["dbs_update_width_from_2"]
        self.dbs_update_width_from_3 = test_config["dbs_update_width_from_3"]
        self.dbs_update_weight_with_width_1 = \
            test_config["dbs_update_weight_with_width_1"]
        self.dbs_update_weight_with_width_2 = \
            test_config["dbs_update_weight_with_width_2"]
        self.dbs_update_weight_with_width_3 = \
            test_config["dbs_update_weight_with_width_3"]
        self.dbs_update_weight_with_width_4 = \
            test_config["dbs_update_weight_with_width_4"]
        self.dbs_update_both_with_width_1 = \
            test_config["dbs_update_both_with_width_1"]
        self.dbs_update_both_with_width_2 = \
            test_config["dbs_update_both_with_width_2"]
        self.dbs_update_both_with_width_3 = \
            test_config["dbs_update_both_with_width_3"]
        # End of bucket edit specific variables


    def __get_bucket_obj_with_name(self, b_name):
        for bucket in self.cluster.buckets:
            if bucket.name == b_name:
                return bucket

    def __add_task_to_bucket_map(self, work_load_generate_task):
        b_name = work_load_generate_task.bucket_name
        if b_name not in self.doc_ops_bucket_map.keys():
            self.doc_ops_bucket_map[b_name] = list()
        self.doc_ops_bucket_map[b_name].append(work_load_generate_task)

    def __print_step(self, msg):
        self.log.critical("\n{0}\n {1}. {2}\n{0}"
                          .format(self.banner, self.step_no, msg))
        self.step_no += 1

    def __get_req_buckets(self, buckets, width=None, weight=None,
                          limit=1, exclude_buckets=None):
        """
        Filter function to get bucket objects with target width/weight
        :param buckets: List of Buckets to consider
        :param width: Target width to filter
        :param weight: Target weight to filter
        :param limit: Number of max buckets to return
        :param exclude_buckets: List of buckets to avoid considering
        :return:
        """
        result = list()
        if limit == 0:
            return result

        self.log.debug("Filtering %s buckets" % limit)
        if exclude_buckets:
            exclude_buckets = [b.name for b in exclude_buckets]
        for bucket in buckets:
            if exclude_buckets is None or bucket.name not in exclude_buckets:
                if width and (weight is None):
                    if bucket.serverless.width == width:
                        result.append(bucket)
                elif weight and (width is None):
                    if bucket.serverless.weight == weight:
                        result.append(bucket)
                elif weight and width:
                    if bucket.serverless.width == width \
                            and bucket.serverless.weight == weight:
                        result.append(bucket)
                else:
                    result.append(bucket)
        return sample(result, min(limit, len(result)))

    def __create_databases(self, num_buckets, b_name_format,
                           create_sdk_client=True):
        self.__print_step("Creating %s buckets" % num_buckets)
        bucket_specs = self.get_bucket_spec(
            num_buckets=num_buckets,
            bucket_name_format=b_name_format)
        self.create_required_buckets(bucket_specs)
        created_buckets = self.cluster.buckets[-num_buckets:]

        # Update the idle buckets variable for future use
        self.idle_buckets.extend(created_buckets)

        if create_sdk_client:
            self.log.info("Creating SDK clients for %s buckets"
                          % len(created_buckets))
            self.create_sdk_client_pool(created_buckets,
                                        req_clients_per_bucket=1)

    def __delete_database(self, bucket):
        self.__stop_doc_ops_for_bucket(bucket)
        self.log.info("%s - Force closing sdk_clients" % bucket.name)
        self.cluster.sdk_client_pool.force_close_clients_for_bucket(
            bucket.name)
        self.log.info("%s - Triggering delete API" % bucket.name)
        self.serverless_util.delete_database(self.pod, self.tenant,
                                             bucket.name)
        self.log.info("%s - Removing from cluster.buckets" % bucket.name)
        self.cluster.buckets.remove(bucket)

    def __stop_doc_ops_for_bucket(self, bucket):
        if bucket.name not in self.doc_ops_bucket_map.keys():
            return
        self.log.info("Stopping doc-ops for bucket '%s'" % bucket.name)
        for task in self.doc_ops_bucket_map[bucket.name]:
            self.log.debug("Stopping loader_task '%s'" % task.taskName)
            task.stop_work_load()
            self.doc_loading_tm.getTaskResult(task)
        self.doc_ops_bucket_map.pop(bucket.name)
        if bucket in self.doc_ops_buckets:
            self.doc_ops_buckets.remove(bucket)

    def __load_initial_docs(self, buckets, num_items, ops_rate=5000):
        self.__print_step("Loading initial load of %s docs in each bucket"
                          % num_items)
        loader_map = dict()
        for bucket in buckets:
            self.log.info("Loading %s items in %s" % (num_items, bucket.name))
            wl_settings = self.bucket_util.get_workload_settings(
                key=self.key, key_size=self.key_size, doc_size=self.doc_size,
                create_perc=100, create_start=0, create_end=num_items,
                ops_rate=ops_rate)
            dg = DocumentGenerator(wl_settings, self.key_type, self.val_type)
            loader_map.update(
                {"%s:%s:%s" % (bucket.name, CbServer.default_scope,
                               CbServer.default_collection): dg})
            # Update collection level num_items/doc_index for tracking
            bucket.scopes[CbServer.default_scope].collections[
                CbServer.default_collection].num_items = num_items
            bucket.scopes[CbServer.default_scope].collections[
                CbServer.default_collection].doc_index = (0, num_items)

        result, tasks = self.bucket_util.perform_doc_loading(
            self.doc_loading_tm, loader_map,
            self.cluster, self.cluster.buckets,
            async_load=True, track_failures=False)
        self.assertTrue(result, "Failed to start doc_ops")
        return loader_map, tasks

    def __subsequent_data_load(self, buckets):
        loader_map = dict()
        if not buckets:
            return loader_map, []
        doc_loading_rates = [100, 200, 300, 400, 500, 800,
                             1000, 1500, 2000, 2500, 3000,
                             3500, 4000, 4500, 5000]
        load_patterns = [
            "create", "update", "read", "delete", "expiry",
            "create:update", "create:delete", "update:delete",
            "create:update:delete", "create:update:read"]
        self.log.info("Loading docs into %s buckets" % len(buckets))
        for bucket in buckets:
            def_collection = bucket.scopes[CbServer.default_scope].collections[
                CbServer.default_collection]
            start, end = def_collection.doc_index
            load_pattern = choice(load_patterns)
            self.log.info("Load pattern for %s - %s"
                          % (bucket.name, load_pattern))
            ops_rate = choice(doc_loading_rates)
            params = {
                "key": self.key, "key_size": self.key_size,
                "doc_size": self.doc_size, "ops_rate": ops_rate,
                "process_concurrency": 1,
                "key_type": self.key_type, "value_type": self.val_type,
            }
            num_op_types = len(load_pattern.split(":"))
            ops_perc = int(100/num_op_types)
            remaining_perc = 100 % num_op_types
            if "delete" in load_pattern:
                doc_end = int(start/3)
                params["delete_perc"] = ops_perc
                params["delete_start"] = start
                params["delete_end"] = doc_end
                start = doc_end
                def_collection.num_items -= (doc_end - start)
                if self.validate_stat:
                    self.expected_stat[bucket.name]["wu"] += self.bucket_util.calculate_units(
                        self.key_size, self.doc_size, num_items=doc_end - start)
            if "create" in load_pattern:
                doc_end = end + 50000
                params["create_perc"] = ops_perc + remaining_perc
                params["create_start"] = end
                params["create_end"] = doc_end
                end = doc_end
                remaining_perc = 0
                def_collection.num_items -= (doc_end - start)
                if self.validate_stat:
                    self.expected_stat[bucket.name]["wu"] += self.bucket_util.calculate_units(
                        self.key_size, self.doc_size, num_items=doc_end - start)
            if "update" in load_pattern:
                params["update_perc"] = ops_perc + remaining_perc
                remaining_perc = 0
                params["update_start"] = int(end/2)
                params["update_end"] = end
                if self.validate_stat:
                    self.expected_stat[bucket.name]["wu"] += self.bucket_util.calculate_units(
                        self.key_size, self.doc_size, num_items=params["update_end"] - params["update_start"])
            if "read" in load_pattern:
                params["read_perc"] = ops_perc + remaining_perc
                params["read_start"] = int(end/2)
                params["read_end"] = end
                if self.validate_stat:
                    self.expected_stat[bucket.name]["ru"] += self.bucket_util.calculate_units(
                        self.key_size, self.doc_size, read=True,
                        num_items=params["read_end"]-params["read_start"])
            if "expiry" in load_pattern:
                params["expiry_perc"] = ops_perc
                params["expiry_start"] = 0
                params["expiry_end"] = 10000
                params["key"] = "expiry_doc"
                if self.validate_stat:
                    self.expected_stat[bucket.name]["wu"] += self.bucket_util.calculate_units(
                        self.key_size, self.doc_size, num_items=params["expiry_end"] - params["expiry_start"])

            def_collection.doc_index = (start, end)

            self.log.info("%s - %s" % (bucket.name, params))
            wl_settings = self.bucket_util.get_workload_settings(**params)
            dg = DocumentGenerator(wl_settings, self.key_type, self.val_type)
            loader_map.update(
                {"%s:%s:%s" % (bucket.name, CbServer.default_scope,
                               CbServer.default_collection): dg})

        result, loading_tasks = self.bucket_util.perform_doc_loading(
            self.doc_loading_tm, loader_map, self.cluster, buckets,
            async_load=True, track_failures=False)
        self.assertTrue(result, "Starting doc_ops failed")
        return loader_map, loading_tasks

    def __wait_for_doc_ops_and_validate(self, tasks, loader_map,
                                        validate_results=True, ops_rate=3000):
        self.log.info("Waiting for doc_ops to complete")
        DocLoaderUtils.wait_for_doc_load_completion(self.doc_loading_tm, tasks)

        if validate_results:
            if not loader_map:
                self.log.warning("Loader map empty. Returning without "
                                 "validation")
                return
            self.log.info("Validating doc_ops result")
            result = DocLoaderUtils.data_validation(
                self.doc_loading_tm, loader_map,
                self.cluster, self.cluster.buckets,
                process_concurrency=1, ops_rate=ops_rate)
            self.assertTrue(result, "Doc validation failed")

    def __init_setup(self):
        """
        - Create 'self.num_buckets' with requested collections
        - Load all buckets with diff DGM level to prepare for future testing
        - Validate doc_counts / collections matches the test
        - All buckets are with width=1 and weight=30
        - Start continuous doc_ops on few buckets with defined data_patterns
        """
        self.__create_databases(self.num_buckets, "tntMgmtVol-Step-0-%s",
                                create_sdk_client=False)

        self.buckets_eligible_for_data_load = \
            self.cluster.buckets[self.num_buckets_idle:]
        self.log.info("Creating SDK clients for %s buckets"
                      % len(self.buckets_eligible_for_data_load))
        self.create_sdk_client_pool(self.buckets_eligible_for_data_load,
                                    req_clients_per_bucket=1)

        # get initial stats
        if self.validate_stat:
            self.get_servers_for_databases(self.buckets_eligible_for_data_load)
            self.expected_stat = self.bucket_util.get_initial_stats(
                self.cluster.buckets)

        # Load initial docs
        loader_map, tasks = self.__load_initial_docs(
            self.buckets_eligible_for_data_load, self.num_items)
        self.__wait_for_doc_ops_and_validate(tasks, loader_map)

        # validate meter stats
        if self.validate_stat:
            for bucket in self.buckets_eligible_for_data_load:
                # items_loaded = self.bucket_util.get_actual_items_loaded_to_calculate_wu(
                #     self.num_items, self.expected_stat[bucket.name]["total_items"], bucket)
                items_loaded = self.num_items
                units = self.bucket_util.calculate_units(self.key_size, self.doc_size,
                                                         num_items=items_loaded)
                self.bucket_util.update_stat_on_buckets([bucket], self.expected_stat, units)
                self.bucket_util.validate_stats([bucket], self.expected_stat)
                self.expected_stat[bucket.name]["total_items"] = items_loaded

        # self.bucket_util.validate_serverless_buckets(self.cluster,
        #                                              self.cluster.buckets)

        loader_map = dict()
        self.__print_step("Starting cont. update load on %s buckets"
                          % self.num_buckets_for_cont_updates)
        for bucket in self.buckets_eligible_for_data_load[
                      :self.num_buckets_for_cont_updates]:
            wl_settings = self.bucket_util.get_workload_settings(
                key=self.key, key_size=self.key_size, doc_size=self.doc_size,
                update_perc=100, update_start=0, update_end=self.num_items,
                ops_rate=1000)
            dg = DocumentGenerator(wl_settings, "CircularKey", self.val_type)
            loader_map.update(
                {"%s:%s:%s" % (bucket.name, CbServer.default_scope,
                               CbServer.default_collection): dg})

        _, self.cont_update_tasks = self.bucket_util.perform_doc_loading(
            self.doc_loading_tm, loader_map,
            self.cluster, self.cluster.buckets,
            async_load=True, track_failures=False)
        for task in self.cont_update_tasks:
            self.__add_task_to_bucket_map(task)

        self.bucket_util.print_bucket_stats(self.cluster)

    def __incremental_steps(self):
        """
        - Create 'N' buckets based on the value in 'self.create_bucket_pattern'
        - Start initial doc_load on the new buckets
        - Scale width / weight based on the values in,
            > self.num_buckets_to_update_width
            > self.num_buckets_to_update_weight
            > self.num_buckets_to_update_width_weight
        - Trigger bucket_deletion in parallel
        """
        get_b_names = lambda l: [_.name for _ in l]
        for step_index, num_dbs in enumerate(self.dbs_create_pattern):
            self.log.critical("Incremental step :: %s" % (step_index+1))
            # Select required buckets for all operations
            exclude_buckets = list()
            scaling_to_track = list()
            buckets_to_consider = self.cluster.buckets[self.num_buckets:]
            self.log.info("bucket considered %s"% buckets_to_consider)

            # Pick buckets to delete
            try:
                buckets_to_del = self.__get_req_buckets(
                    buckets_to_consider, width=1,
                    limit=self.dbs_delete_with_width_1[step_index])
                buckets_to_del.extend(self.__get_req_buckets(
                    buckets_to_consider, width=2,
                    limit=self.dbs_delete_with_width_2[step_index]))
                buckets_to_del.extend(self.__get_req_buckets(
                    buckets_to_consider, width=3,
                    limit=self.dbs_delete_with_width_3[step_index]))
                buckets_to_del.extend(self.__get_req_buckets(
                    buckets_to_consider, width=4,
                    limit=self.dbs_delete_with_width_4[step_index]))
                exclude_buckets.extend(buckets_to_del)
                self.log.info("Buckets to del: %s" % buckets_to_del)
            except:
                buckets_to_del = []

            # Pick buckets to perform width scaling (incr by 1)
            try:
                scale_from_width_1 = self.__get_req_buckets(
                    buckets_to_consider, width=1,
                    limit=self.dbs_update_width_from_1[step_index],
                    exclude_buckets=exclude_buckets)
                exclude_buckets.extend(scale_from_width_1)
                self.log.info("Width scale with width=1 %s"
                              % get_b_names(scale_from_width_1))
            except:
                scale_from_width_1 = []

            try:
                scale_from_width_2 = self.__get_req_buckets(
                    buckets_to_consider, width=2,
                    limit=self.dbs_update_width_from_2[step_index],
                    exclude_buckets=exclude_buckets)
                exclude_buckets.extend(scale_from_width_2)
                self.log.info("Width scale with width=2 %s"
                              % get_b_names(scale_from_width_2))
            except:
                scale_from_width_2 = []

            try:
                scale_from_width_3 = self.__get_req_buckets(
                    buckets_to_consider, width=3,
                    limit=self.dbs_update_width_from_3[step_index],
                    exclude_buckets=exclude_buckets)
                exclude_buckets.extend(scale_from_width_3)
                self.log.info("Width scale with width=3 %s"
                              % get_b_names(scale_from_width_3))
            except:
                scale_from_width_3 = []

            # Pick buckets to perform width/weight scaling (incr by 1)
            try:
                scale_width_weight_from_width_1 = self.__get_req_buckets(
                    buckets_to_consider, width=1,
                    limit=self.dbs_update_both_with_width_1[step_index],
                    exclude_buckets=exclude_buckets)
                exclude_buckets.extend(scale_width_weight_from_width_1)
                self.log.info("Both scaling with width=1 %s"
                              % get_b_names(scale_width_weight_from_width_1))
            except:
                scale_width_weight_from_width_1 = []

            try:
                scale_width_weight_from_width_2 = self.__get_req_buckets(
                    buckets_to_consider, width=2,
                    limit=self.dbs_update_both_with_width_2[step_index],
                    exclude_buckets=exclude_buckets)
                exclude_buckets.extend(scale_width_weight_from_width_2)
                self.log.info("Both scaling with width=2 %s"
                              % get_b_names(scale_width_weight_from_width_2))
            except:
                scale_width_weight_from_width_2 = []

            try:
                scale_width_weight_from_width_3 = self.__get_req_buckets(
                    buckets_to_consider, width=3,
                    limit=self.dbs_update_both_with_width_3[step_index],
                    exclude_buckets=exclude_buckets)
                exclude_buckets.extend(scale_width_weight_from_width_3)
                self.log.info("Both scaling with width=3 %s"
                              % get_b_names(scale_width_weight_from_width_3))
            except:
                scale_width_weight_from_width_3 = []

            # Pick buckets to perform weight scaling
            try:
                scale_weight_in_width_1 = self.__get_req_buckets(
                    buckets_to_consider, width=1,
                    limit=self.dbs_update_weight_with_width_1[step_index],
                    exclude_buckets=exclude_buckets)
                exclude_buckets.extend(scale_weight_in_width_1)
                self.log.info("Weight scaling with width=1 %s"
                              % get_b_names(scale_weight_in_width_1))
            except:
                scale_weight_in_width_1 = []

            try:
                scale_weight_in_width_2 = self.__get_req_buckets(
                    buckets_to_consider, width=2,
                    limit=self.dbs_update_weight_with_width_2[step_index],
                    exclude_buckets=exclude_buckets)
                exclude_buckets.extend(scale_weight_in_width_2)
                self.log.info("Weight scaling with width=2 %s"
                              % get_b_names(scale_weight_in_width_2))
            except:
                scale_weight_in_width_2 = []

            try:
                scale_weight_in_width_3 = self.__get_req_buckets(
                    buckets_to_consider, width=3,
                    limit=self.dbs_update_weight_with_width_3[step_index],
                    exclude_buckets=exclude_buckets)
                exclude_buckets.extend(scale_weight_in_width_3)
                self.log.info("Weight scaling with width=3 %s"
                              % get_b_names(scale_weight_in_width_3))
            except:
                scale_weight_in_width_3 = []

            try:
                scale_weight_in_width_4 = self.__get_req_buckets(
                    buckets_to_consider, width=4,
                    limit=self.dbs_update_weight_with_width_4[step_index],
                    exclude_buckets=exclude_buckets)
                exclude_buckets.extend(scale_weight_in_width_4)
                self.log.info("Weight scaling with width=4 %s"
                              % get_b_names(scale_weight_in_width_4))
            except:
                scale_weight_in_width_4 = []

            # Pick buckets to load docs
            buckets_to_load_docs = self.__get_req_buckets(
                buckets_to_consider, limit=self.num_buckets_to_load,
                exclude_buckets=buckets_to_del)
            if self.validate_stat:
                self.expected_stat.update(self.bucket_util.get_initial_stats(
                    buckets_to_load_docs))
            loader_map, data_load_tasks = \
                self.__subsequent_data_load(buckets_to_load_docs)
            self.log.info("Subsequent load into buckets: %s"
                          % get_b_names(buckets_to_load_docs))
            self.log.info("Loader map: %s" % loader_map)
            self.log.info("Data load tasks: %s" % data_load_tasks)

            # Create buckets
            b_name_format = "tntMgmtVol-Step-%s" % (step_index+1)
            self.__create_databases(num_dbs, b_name_format + "-%s")

            # Start initial load on new buckets
            new_buckets = self.cluster.buckets[-num_dbs:]
            if self.validate_stat:
                self.expected_stat.update(self.bucket_util.get_initial_stats(new_buckets))
            new_buckets_loader_map, new_bucket_loading_tasks = \
                self.__load_initial_docs(new_buckets, self.num_items)
            self.log.info("Load into new buckets: %s"
                          % get_b_names(new_buckets))
            self.log.info("Loader map: %s" % new_buckets_loader_map)
            self.log.info("Data load tasks: %s" % new_bucket_loading_tasks)

            # Delete req. number of buckets
            db_del_threads = list()
            self.generic_tbl.headers = ["Buckets to be deleted"]
            self.generic_tbl.rows = list()
            self.log.info(self.doc_ops_bucket_map)
            for tem_bucket in buckets_to_del:
                self.generic_tbl.add_row([tem_bucket.name])
                self.__delete_database(tem_bucket)
                # thread = Thread(target=self.__delete_database,
                #                 args=(tem_bucket,))
                # db_del_threads.append(thread)
                # thread.start()
            self.generic_tbl.display("")
            self.log.info(self.doc_ops_bucket_map)

            # Trigger bucket scaling operation
            self.generic_tbl.headers = ["Bucket", "Width change",
                                        "Weight change"]
            self.generic_tbl.rows = list()
            for index, buckets in enumerate([scale_from_width_1,
                                             scale_from_width_2,
                                             scale_from_width_3]):
                width = index + 1
                new_width = width + 1
                for b_obj in buckets:
                    if b_obj.serverless.width != width:
                        self.fail("Failed to get bucket with width=%s" % width)
                    db_info = {"cluster": self.cluster,
                               "bucket": b_obj,
                               "desired_width": new_width}

                    width_update_info = "%s -> %s" % (width, new_width)
                    self.generic_tbl.add_row([b_obj.name, width_update_info,
                                              "-"])
                    over_ride = {Bucket.width: new_width}
                    b_obj.serverless.width = new_width
                    self.log.info("Updating %s: %s" % (b_obj.name, over_ride))
                    resp = self.capella_api.update_database(b_obj.name,
                                                            over_ride)
                    self.assertTrue(resp.status_code == 200,
                                    "Width update failed")
                    scaling_to_track.append(db_info)

            for index, buckets in enumerate(
                    [scale_width_weight_from_width_1,
                     scale_width_weight_from_width_2,
                     scale_width_weight_from_width_3]):
                width = index + 1
                new_width = width + 1
                for b_obj in buckets:
                    if b_obj.serverless.width != width:
                        self.fail("Failed to get bucket with width=%s" % width)
                    new_weight = self.get_random_weight_for_width(width)
                    db_info = {"cluster": self.cluster,
                               "bucket": b_obj,
                               "desired_width": new_width,
                               "desired_weight": new_weight}

                    width_update_info = "%s -> %s" % (width, new_width)
                    weight_update_info = "%s -> %s" % (b_obj.serverless.weight,
                                                       new_weight)
                    self.generic_tbl.add_row([b_obj.name, width_update_info,
                                              weight_update_info])
                    over_ride = {Bucket.width: new_width,
                                 Bucket.weight: new_weight}
                    b_obj.serverless.width = new_width
                    b_obj.serverless.weight = new_weight
                    self.log.info("Updating %s: %s" % (b_obj.name, over_ride))
                    resp = self.capella_api.update_database(b_obj.name,
                                                            over_ride)
                    self.assertTrue(resp.status_code == 200,
                                    "Width/Weight update failed")
                    scaling_to_track.append(db_info)

            for index, buckets in enumerate(
                    [scale_weight_in_width_1, scale_weight_in_width_2,
                     scale_weight_in_width_3, scale_weight_in_width_4]):
                width = index + 1
                for b_obj in buckets:
                    if b_obj.serverless.width != width:
                        self.fail("Failed to get bucket with width=%s" % width)
                    new_weight = self.get_random_weight_for_width(width)
                    db_info = {"cluster": self.cluster,
                               "bucket": b_obj,
                               "desired_weight": new_weight}

                    weight_update_info = "%s -> %s" % (b_obj.serverless.weight,
                                                       new_weight)
                    self.generic_tbl.add_row([b_obj.name, "-",
                                              weight_update_info])
                    over_ride = {Bucket.weight: new_weight}
                    b_obj.serverless.weight = new_weight
                    self.log.info("Updating %s: %s" % (b_obj.name, over_ride))
                    resp = self.capella_api.update_database(b_obj.name,
                                                            over_ride)
                    self.assertTrue(resp.status_code == 200,
                                    "Weight update failed")
                    scaling_to_track.append(db_info)
            self.generic_tbl.display("Bucket scaling info:")

            # Start tracking the scaling activity
            monitor_task = self.bucket_util.async_monitor_database_scaling(
                scaling_to_track, timeout=600)
            self.task_manager.get_task_result(monitor_task)

            # Validate bucket deletion
            for thread in db_del_threads:
                thread.join(10)
            for tem_bucket in buckets_to_del:
                self.serverless_util.wait_for_database_deleted(
                    self.tenant, tem_bucket.name)

            # Wait for doc_load to complete
            self.__wait_for_doc_ops_and_validate(new_bucket_loading_tasks,
                                                 new_buckets_loader_map)
            self.__wait_for_doc_ops_and_validate(data_load_tasks, loader_map)
            self.bucket_util.print_bucket_stats(self.cluster)
            if self.validate_stat:
                for buckets in [new_buckets, buckets_to_load_docs, scale_width_weight_from_width_1,
                                scale_width_weight_from_width_2, scale_width_weight_from_width_3]:
                    self.get_servers_for_databases(buckets)
                for buckets in [new_buckets, buckets_to_load_docs]:
                    for bucket in buckets:
                        # items_loaded = self.bucket_util.get_actual_items_loaded_to_calculate_wu(
                        #     self.num_items, self.expected_stat[bucket.name]["total_items"], bucket)
                        items_loaded = self.num_items
                        units = self.bucket_util.calculate_units(self.key_size, self.doc_size,
                                                                 num_items=items_loaded)
                        self.bucket_util.update_stat_on_buckets([bucket], self.expected_stat, units)
                        self.bucket_util.validate_stats([bucket], self.expected_stat)
                        self.expected_stat[bucket.name]["total_items"] = items_loaded
                self.bucket_util.validate_stats(scale_width_weight_from_width_1, self.expected_stat)
                self.bucket_util.validate_stats(scale_width_weight_from_width_2, self.expected_stat)
                self.bucket_util.validate_stats(scale_width_weight_from_width_3, self.expected_stat)

    def __run_scenario(self):
        """
        - Cont. update bucket's width / weight (incr/decrement)
        - Update data loading patterns
        - Create / recreate few buckets to maintain overall load in the cluster
        """
        buckets_to_load_docs = self.__get_req_buckets(
            self.cluster.buckets, limit=self.num_buckets_to_load)
        loader_map, data_load_tasks = \
            self.__subsequent_data_load(buckets_to_load_docs)
        self.__wait_for_doc_ops_and_validate(data_load_tasks, loader_map)
        if self.validate_stat:
            self.bucket_util.validate_stats(buckets_to_load_docs, self.expected_stat)

    def test_volume(self):
        """
        1. Start with self.num_buckets
        2. Load few docs (self.num_items) on each bucket
        3. Keep self.num_buckets_idle buckets in idle state (No data load)
        4. Run the scenario 'run_scenario()' in a loop decided by 'iterations'
        :return:
        """
        iterations = self.input.param("iterations", 1)

        self.__init_setup()
        self.__incremental_steps()
        for itr in range(0, iterations+1):
            self.log.critical("Iteration :: %s" % itr)
            self.__run_scenario()

    def test_ram_scaling_volume(self):
        """
        1. Start with self.num_buckets with width = 1, self.num_buckets with width = 2, self.num_buckets with width = 3, self.num_buckets with width = 4
        2. Change the storage limit to 1500 in each and turn throttling off
        3. Create 10 collections each for a databand
        4. Load data into a collection until the RAM is scaled.
        5. Load data into different collection after
        6. Delete the collection and observe the RAM down scaling
        :return:
        """

        #serverless.tenant_mgmt_volume.TenantMgmtVolumeTest.test_ram_scaling_volume,runtype=serverless,num_buckets=4,ops_rate=50000,key_size=20,doc_size=1024,dynamic_throttling=False,

        self.data_storage_limit = self.input.param("data_storage_limit", 1500)

        self.bucket_width = 1
        self.bucket_weight = 30
        self.create_required_buckets()
        self.bucket_width = 2
        self.create_required_buckets()
        self.bucket_width = 3
        self.create_required_buckets()
        self.bucket_width = 4
        self.create_required_buckets()
        self.get_servers_for_databases()

        self.cluster.sdk_client_pool = \
            self.bucket_util.initialize_java_sdk_client_pool()
        self.create_sdk_client_pool(self.cluster.buckets, 1)

        start_index = 0
        batch_size = 5000000
        disk = [0, 50, 100, 150, 200, 250, 300, 350, 400, 450]
        ram = [256, 256, 384, 512, 640, 768, 896, 1024, 1152, 1280]

        work_load_settings = DocLoaderUtils.get_workload_settings(
            key=self.key, key_size=self.key_size, doc_size=self.doc_size,
            create_perc=100, create_start=start_index, create_end=start_index,
            ops_rate=self.ops_rate, process_concurrency=20)
        loader_map = dict()
        to_track = list()
        for bucket in self.cluster.buckets:
            #Set storage and throttle limit
            self.bucket_util.set_throttle_n_storage_limit(
                bucket, throttle_limit=-1, storage_limit=self.data_storage_limit)

            #Create 10 collections
            for i in range(10):
                self.bucket_util.create_collection(
                                bucket.servers[0], bucket, CbServer.default_scope,
                                {"name": "col-{}".format(i)})

            #Set the band for each bucket
            if bucket.serverless.width == 1:
                bucket.band = 10
            elif bucket.serverless.width == 2:
                bucket.band = 8
            elif bucket.serverless.width == 3:
                bucket.band = 6
            elif bucket.serverless.width == 4:
                bucket.band = 4

            #Create a loader for first collection
            bucket.curr_collection = 0
            dg = DocumentGenerator(work_load_settings,
                        self.key_type, self.val_type)
            loader_key = "{}:{}:{}".format(bucket.name, CbServer.default_scope,"col-{}".format(bucket.curr_collection))
            bucket.loader_key = loader_key
            loader_map.update({loader_key:dg})

            #Add the bucket as part of RAM Scaling tracker
            db_info = {
                    "cluster": self.cluster,
                    "bucket": bucket,
                    "desired_ram_quota": 256,
                    "desired_width": None,
                    "desired_weight": None
                }
            to_track.append(db_info)

        while len(loader_map) != 0:
            monitor_task = self.bucket_util.async_monitor_database_scaling(
                to_track, timeout=300, ignore_undesired_updates=False)
            self.task_manager.get_task_result(monitor_task)

            work_load_settings.dr.create_s = work_load_settings.dr.create_e
            work_load_settings.dr.create_e += batch_size

            for loader_key in loader_map:
                self.log.info("Initializing doc loading on {}".format(loader_key))

            DocLoaderUtils.perform_doc_loading(
                self.doc_loading_tm, loader_map, self.cluster,
                buckets=self.cluster.buckets, async_load=False,
                validate_results=False, track_failures=False,
                process_concurrency=20)

            self.sleep(10)
            to_track = list()
            for bucket in self.cluster.buckets:
                logical_data, _ , _ = self.bucket_util.get_logical_data(bucket)
                logical_data = logical_data / (1024 * 1024 * 1024)
                self.log.info("Bucket {} Active Logical Data = {} GB".format(bucket.name, logical_data))

                desired_ram_quota = ram[0]
                count = 0
                while count < len(disk) and logical_data >= disk[count]:
                    desired_ram_quota = ram[count]
                    count += 1

                db_info = {
                    "cluster": self.cluster,
                    "bucket": bucket,
                    "desired_ram_quota": desired_ram_quota,
                    "desired_width": None,
                    "desired_weight": None
                }
                to_track.append(db_info)

                if bucket.curr_collection == bucket.band:
                    continue
                if logical_data > disk[bucket.curr_collection]:
                    bucket.curr_collection += 1
                    loader_map.pop(bucket.loader_key)
                if bucket.curr_collection != bucket.band:
                    dg = DocumentGenerator(work_load_settings,
                                           self.key_type, self.val_type)
                    loader_key = "{}:{}:{}".format(bucket.name, CbServer.default_scope,"col-{}".format(bucket.curr_collection))
                    bucket.loader_key = loader_key
                    loader_map.update({loader_key : dg})

        monitor_task = self.bucket_util.async_monitor_database_scaling(
                to_track, timeout=300, ignore_undesired_updates=False)
        self.task_manager.get_task_result(monitor_task)

        #TODO - Delete collections and check for RAM downscaling - CBQE-7929
        self.fail("RAM Downscaling has to be implemented - CBQE-7929")
