import time
from copy import deepcopy
from random import choice
from ruamel.yaml import YAML

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import TravelSample
from Cb_constants import CbServer, DocLoading
from SecurityLib.rbac import RbacUtil
from backup_lib.backup import BackupHelper
from basetestcase import BaseTestCase
from bucket_collections.app.constants import global_vars
from bucket_collections.app.constants.collection_spec import collection_spec
from bucket_collections.app.constants.query import CREATE_INDEX_QUERIES
from bucket_collections.app.constants.rbac import rbac_data
from bucket_collections.app.lib import query_util
from bucket_collections.app.scenarios.guest import Guest
from bucket_collections.app.scenarios.user import User
from cb_tools.cbstats import Cbstats
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient

from com.couchbase.client.java.json import JsonObject


class TravelSampleApp(BaseTestCase):
    def setUp(self):
        super(TravelSampleApp, self).setUp()

        self.log_setup_status("TravelSampleApp", "started")
        self.step_num = 1
        self.app_iteration = self.input.param("iteration", 1)

        self.rbac_util = RbacUtil()
        self.sdk_clients = global_vars.sdk_clients

        cluster_yaml = "pytests/bucket_collections/app/config/cluster.yaml"
        with open(cluster_yaml, "r") as fp:
            self.cluster_config = YAML().load(fp.read())

        # Override nodes_init, services_init from yaml data
        self.nodes_init = self.cluster_config["cb_cluster"]["nodes_init"]
        self.services_init = self.cluster_config["cb_cluster"]["services"]

        if not self.skip_setup_cleanup:
            # Rebalance_in required nodes
            nodes_init = self.cluster.servers[1:self.nodes_init] \
                if self.nodes_init != 1 else []
            self.task.rebalance([self.cluster.master], nodes_init, [],
                                services=self.services_init[1:])
            self.cluster.nodes_in_cluster.extend(
                [self.cluster.master] + nodes_init)

            # Load travel_sample bucket
            ts_bucket = TravelSample()
            if self.bucket_util.load_sample_bucket(ts_bucket) is False:
                self.fail("Failed to load sample bucket")

            self.bucket = self.bucket_util.buckets[0]
            self.bucket_util.update_bucket_property(
                self.bucket, ram_quota_mb=self.bucket_size)

            # Create required scope/collections
            self.create_scope_collections()
            self.bucket.scopes[
                CbServer.default_scope].collections[
                CbServer.default_collection].num_items \
                = ts_bucket.scopes[CbServer.default_scope].collections[
                    CbServer.default_collection].num_items
            self.sleep(20, "Wait for num_items to get updated")
            self.bucket_util.validate_docs_per_collections_all_buckets()

            # Create RBAC users
            self.create_rbac_users()

            #  Opening required clients
            self.create_sdk_clients()

            # Loading initial data into created collections
            self.load_initial_collection_data()
            self.bucket_util.validate_docs_per_collections_all_buckets()

            # Configure backup settings
            self.configure_bucket_backups()

            # Create required indexes
            self.create_indexes()
        else:
            self.bucket = self.bucket_util.buckets[0]
            self.map_collection_data()

            #  Opening required clients
            self.create_sdk_clients()

        global_vars.app_current_date = query_util.CommonUtil.get_current_date()
        self.log_setup_status("TravelSampleApp", "complete")

    def tearDown(self):
        self.shutdown_sdk_clients()
        super(TravelSampleApp, self).tearDown()

    def __print_step(self, message):
        message = "  %s. %s" % (self.step_num, message)
        line_delimiter = "#"*60
        self.log.info("\n{1}\n{0}\n{1}".format(message, line_delimiter))
        self.step_num += 1

    def map_collection_data(self):
        cb_stat_objects = list()
        collection_data = None

        for node in self.cluster_util.get_kv_nodes():
            shell = RemoteMachineShellConnection(node)
            cb_stat_objects.append(Cbstats(shell))

        for cb_stat in cb_stat_objects:
            tem_collection_data = cb_stat.get_collections(self.bucket)
            if collection_data is None:
                collection_data = tem_collection_data
            else:
                for key, value in tem_collection_data.items():
                    if type(value) is dict:
                        for col_name, c_data in value.items():
                            collection_data[key][col_name]['items'] \
                                += c_data['items']

        for s_name, s_data in collection_data.items():
            if type(s_data) is not dict:
                continue
            self.bucket_util.create_scope_object(self.bucket,
                                                 {"name": s_name})
            for c_name, c_data in s_data.items():
                if type(c_data) is not dict:
                    continue
                self.bucket_util.create_collection_object(
                    self.bucket, s_name,
                    {"name": c_name, "num_items": c_data["items"],
                     "maxTTL": c_data.get("maxTTL", 0)})

        # Close shell connections
        for cb_stat in cb_stat_objects:
            cb_stat.shellConn.disconnect()

    def create_rbac_users(self):
        def populate_users_roles(user_id, user_role_data):
            u_name, password = user_role_data["auth"][0], \
                               user_role_data["auth"][1]
            users.append({'id': user_id, 'name': u_name, 'password': password})
            roles.append({'id': user_id, 'name': u_name,
                          'roles': user_role_data["roles"]})

        self.__print_step("Creating RBAC users")
        master = self.cluster.master
        rest_conn = RestConnection(master)

        # Create cluster_admin first
        users = list()
        roles = list()
        self.log.info("Creating cluster_admin user")
        populate_users_roles("cluster_admin", rbac_data["cluster_admin"])
        RbacUtil().create_user_source(users, 'builtin', master)
        _ = RbacUtil().add_user_role(roles, rest_conn, 'builtin')

        # Create rbac-security_admin using cluster_admin
        users = list()
        roles = list()
        self.log.info("Creating rbac_admin user")
        populate_users_roles("rbac_admin", rbac_data["rbac_admin"])
        RbacUtil().create_user_source(users, 'builtin', master)
        _ = RbacUtil().add_user_role(roles, rest_conn, 'builtin')

        # Update rest_conn credentials to use rbac_user
        master = deepcopy(master)
        master.rest_username = rbac_data["rbac_admin"]["auth"][0]
        master.rest_password = rbac_data["rbac_admin"]["auth"][1]

        # Create rest of the users using rbac_admin
        users = list()
        roles = list()
        self.log.info("Creating other users using rbac_admin")
        for u_id, u_data in rbac_data.items():
            if u_id in ["cluster_admin", "rbac_admin"]:
                # User already created
                continue
            populate_users_roles(u_id, u_data)

        RbacUtil().create_user_source(users, 'builtin', master)
        _ = RbacUtil().add_user_role(roles, RestConnection(master), 'builtin')

    def create_sdk_clients(self):
        self.__print_step("Creating required SDK clients")
        for u_id, u_data in rbac_data.items():
            u_name, password = u_data["auth"][0], u_data["auth"][1]
            self.sdk_clients[u_id] = SDKClient([self.cluster.master],
                                               self.bucket,
                                               username=u_name,
                                               password=password)

    def shutdown_sdk_clients(self):
        self.__print_step("Closing SDK client connections")
        for _, client in self.sdk_clients.items():
            client.close()

    def create_scope_collections(self):
        self.__print_step("Creating required scope/collections")
        BucketHelper(self.cluster.master).import_collection_using_manifest(
            self.bucket.name,
            str(collection_spec).replace("'", '"'))
        self.bucket.stats.increment_manifest_uid()

        for scope in collection_spec["scopes"]:
            self.bucket_util.create_scope_object(self.bucket, scope)
            for collection in scope["collections"]:
                self.bucket_util.create_collection_object(self.bucket,
                                                          scope["name"],
                                                          collection)

    def load_initial_collection_data(self):
        self.__print_step("Loading initial data into collections")
        # Pairs of scope, collection name
        type_collection_map = dict()
        type_collection_map["airline"] = ("airlines", "airline")
        type_collection_map["airport"] = ("airlines", "airport")
        type_collection_map["route"] = ("airlines", "routes")
        type_collection_map["hotel"] = ("hotels", "hotels")
        type_collection_map["landmark"] = ("hotels", "landmark")

        meta_data = dict()
        for scope in collection_spec["scopes"]:
            meta_data[scope["name"]] = dict()
            for collection in scope["collections"]:
                meta_data[scope["name"]][collection["name"]] = dict()
                meta_data[scope["name"]][collection["name"]]["doc_counter"] = 0
                meta_data[scope["name"]][collection["name"]]["num_items"] = 0

        hotel_review_rows = list()
        sdk_client = self.sdk_clients["bucket_data_writer"]
        query = "SELECT * FROM `travel-sample`.`_default`.`_default` " \
                "WHERE type='%s'"
        for d_type, collection_info in type_collection_map.items():
            s_name, c_name = collection_info[0], collection_info[1]

            sdk_client.select_collection(s_name, c_name)
            query_result = sdk_client.cluster.query(query % d_type)
            rows_inserted = 0
            for row in query_result.rowsAsObject():
                value = row.getObject("travel-sample").removeKey("type")

                doc_id = value.getInt("id")
                if doc_id > meta_data[s_name][c_name]["doc_counter"]:
                    meta_data[s_name][c_name]["doc_counter"] = doc_id

                if d_type == "hotel":
                    # Segregate 'reviews' from hotel collection
                    review = JsonObject.create()
                    review.put("id", doc_id)
                    review.put("reviews", value.getArray("reviews"))
                    hotel_review_rows.append(review)
                    value = value.removeKey("reviews")
                key = d_type + "_" + str(doc_id)
                result = sdk_client.crud(DocLoading.Bucket.DocOps.CREATE,
                                         key, value)
                if result["status"] is False:
                    self.fail("Loading collections failed")
                rows_inserted += 1

            self.bucket.scopes[collection_info[0]].collections[
                collection_info[1]].num_items += rows_inserted
            meta_data[s_name][c_name]["num_items"] = rows_inserted

        # Write hotel reviews into respective collection
        rows_inserted = 0
        s_name, c_name = "hotels", "reviews"
        sdk_client.select_collection(s_name, c_name)
        for review in hotel_review_rows:
            doc_id = review.getInt("id")
            if doc_id > meta_data[s_name][c_name]["doc_counter"]:
                meta_data[s_name][c_name]["doc_counter"] = doc_id

            key = "review_" + str(doc_id)
            result = sdk_client.crud(DocLoading.Bucket.DocOps.CREATE,
                                     key, review)
            if result["status"] is False:
                self.fail("Loading reviews collection failed")
            rows_inserted += 1

        self.bucket.scopes[s_name].collections[
            c_name].num_items += rows_inserted
        meta_data[s_name][c_name]["num_items"] = rows_inserted

        # Create collection meta_data document
        sdk_client.select_collection(scope_name=CbServer.default_scope,
                                     collection_name="meta_data")
        app_data = JsonObject.create()
        app_data.put("date", "2001-01-01")
        result = sdk_client.crud(DocLoading.Bucket.DocOps.CREATE,
                                 "application", app_data)
        self.assertTrue(result["status"], "App_meta creation failed")
        self.bucket.scopes[CbServer.default_scope].collections[
            "meta_data"].num_items += 1

        for s_name, scope_data in meta_data.items():
            for c_name, c_data in scope_data.items():
                c_meta = JsonObject.create()
                for meta_key, meta_val in c_data.items():
                    c_meta.put(meta_key, meta_val)
                key = "%s.%s" % (s_name, c_name)
                result = sdk_client.crud(DocLoading.Bucket.DocOps.CREATE,
                                         key, c_meta)
                if result["status"] is False:
                    self.fail("Meta creation failed")
                self.bucket.scopes[CbServer.default_scope].collections[
                    "meta_data"].num_items += 1

        create_users = User(self.bucket, op_type="scenario_user_registration",
                            num_items=10000)
        create_users.start()
        create_users.join()

        self.sleep(30, "Wait for num_items to get updated")

    def create_indexes(self):
        self.__print_step("Creating required Indexes")

        self.log.info("Dropping default indexes on _default collection")
        select_result = self.sdk_clients["bucket_admin"].cluster.query(
            "SELECT name FROM system:indexes "
            "WHERE keyspace_id='travel-sample'")
        for row in select_result.rowsAsObject():
            drop_result = self.sdk_clients["bucket_admin"].cluster.query(
                "DROP INDEX `%s` on `travel-sample`" % row.get("name"))
            if drop_result.metaData().status().toString() != "SUCCESS":
                self.fail("Drop index '%s' failed: %s" % (row.get("name"),
                                                          drop_result))

        self.log.info("Creating collection specific indexes")
        for query in CREATE_INDEX_QUERIES:
            result = self.sdk_clients["bucket_admin"].cluster.query(query)
            if result.metaData().status().toString() != "SUCCESS":
                self.fail("Create index '%s' failed: %s" % (query, result))

        self.log.info("Building deferred indexes")
        indexes_to_build = dict()
        result = self.sdk_clients["bucket_admin"].cluster.query(
            'SELECT * FROM system:indexes '
            'WHERE bucket_id="travel-sample" AND state="deferred"')
        for row in result.rowsAsObject():
            row = row.get("indexes")
            bucket = self.bucket.name
            scope = row.get("scope_id")
            collection = row.get("keyspace_id")
            if bucket not in indexes_to_build:
                indexes_to_build[bucket] = dict()
            if scope not in indexes_to_build[bucket]:
                indexes_to_build[bucket][scope] = dict()
            if collection not in indexes_to_build[bucket][scope]:
                indexes_to_build[bucket][scope][collection] = list()

            indexes_to_build[bucket][scope][collection].append(row.get("name"))

        for bucket, b_data in indexes_to_build.items():
            for scope, s_data in b_data.items():
                for collection, indexes in s_data.items():
                    build_res = self.sdk_clients["bucket_admin"].cluster.query(
                        "BUILD INDEX on `%s`.`%s`.`%s`(%s)"
                        % (bucket, scope, collection, ",".join(indexes)))
                    if build_res.metaData().status().toString() != "SUCCESS":
                        self.fail("Build index failed for %s: %s"
                                  % (indexes, build_res))

        self.log.info("Waiting for indexes to become online")
        start_time = time.time()
        stop_time = start_time + 300

        for row in result.rowsAsObject():
            query = "SELECT state FROM system:indexes WHERE name='%s'" \
                    % row.get("indexes").get("name")
            while True:
                state = self.sdk_clients["bucket_admin"].cluster.query(
                    query).rowsAsObject()[0].get("state")
                if state == "online":
                    break
                if time.time() > stop_time:
                    self.fail("Index availability timeout")

    def configure_bucket_backups(self):
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        backup_node = self.cluster.backup_nodes[0]
        backup_helper = BackupHelper(backup_node)

        self.log.info("Creating permissions for backup folder")
        backup_configs = self.cluster_config["cb_cluster"]["backup"]
        shell = RemoteMachineShellConnection(backup_node)
        for backup_config in backup_configs:
            plan_params = dict()
            repo_params = dict()
            if "plan" in backup_config:
                plan_params["plan"] = backup_config["plan"]
                repo_params["plan"] = backup_config["plan"]
            if "description" in backup_config:
                plan_params["description"] = backup_config["description"]
            if "archive_path" in backup_config:
                repo_params["archive"] = backup_config["archive_path"]
                shell.execute_command("mkdir -p {0} ; chmod 777 {0}"
                                      .format(backup_config["archive_path"]))
            if "bucket" in backup_config:
                repo_params["bucket"] = backup_config["bucket"]

            if plan_params["plan"] not in ["_hourly_backups",
                                           "_daily_backups"]:
                self.log.info("Updating custom plan %s" % plan_params["plan"])
                status = backup_helper.create_edit_plan("create", plan_params)
                if status is False:
                    self.fail("Backup %s create failed" % backup_config)

            # Create repo
            status = backup_helper.create_repo(backup_config["repo_id"],
                                               repo_params)
            if status is False:
                self.fail("Create repo failed for %s" % backup_config)
        shell.disconnect()

    def run_app(self):
        random_op = "random"

        itr_index = 1
        while itr_index <= self.app_iteration:
            self.log.info("#### Iteration :: %d ####" % itr_index)
            tasks = list()
            # Start monitoring doc_ops
            # self.bucket.stats.manage_task("start", self.task_manager,
            #                               cluster=self.cluster,
            #                               bucket=self.bucket,
            #                               monitor_stats=["doc_ops"],
            #                               sleep=1)

            user_activity_1 = User(self.bucket, op_type=random_op,
                                   op_count=100)
            user_activity_2 = User(self.bucket, op_type=random_op,
                                   op_count=100)
            guest_activity_1 = Guest(self.bucket, op_type=random_op,
                                     op_count=20)
            guest_activity_2 = Guest(self.bucket, op_type=random_op,
                                     op_count=20)

            tasks.append(user_activity_1)
            tasks.append(guest_activity_1)
            tasks.append(guest_activity_2)
            tasks.append(user_activity_2)

            # Start all threads
            for task in tasks:
                task.start()

            # Wait for threads to complete
            for task in tasks:
                task.join()

            # Stop monitoring doc_ops
            # self.bucket.stats.manage_task("stop", self.task_manager,
            #                               cluster=self.cluster,
            #                               bucket=self.bucket,
            #                               monitor_stats=["doc_ops"],
            #                               sleep=1)

            for task in tasks:
                if task.exception:
                    self.fail(task.exception)

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
