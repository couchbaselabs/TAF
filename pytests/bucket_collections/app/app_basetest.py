import json
import time
from copy import deepcopy
from ruamel.yaml import YAML

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import TravelSample, BeerSample, GamesimSample, Bucket
from Cb_constants import CbServer, DocLoading
from SecurityLib.rbac import RbacUtil
from backup_lib.backup import BackupHelper
from basetestcase import BaseTestCase
from bucket_collections.app.constants import global_vars
from bucket_collections.app.constants.query import CREATE_INDEX_QUERIES
from bucket_collections.app.scenarios.user import User
from cb_tools.cbstats import Cbstats
from cbas_utils.cbas_utils import CbasUtil
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient

from com.couchbase.client.java.json import JsonObject
from com.couchbase.client.core.error import IndexFailureException, \
    InternalServerFailureException


class AppBase(BaseTestCase):
    def setUp(self):
        super(AppBase, self).setUp()
        self.log_setup_status("AppBase", "started")

        self.step_num = 1
        self.initial_load = False
        self.cluster_conf = self.input.param("cluster_conf", None)
        self.bucket_conf = self.input.param("bucket_conf", None)
        self.service_conf = self.input.param("service_conf", None)
        self.rbac_conf = self.input.param("rbac_conf", None)

        self.rbac_util = RbacUtil()
        self.sdk_clients = global_vars.sdk_clients
        self.config_path = "pytests/bucket_collections/app/config/"

        if self.cluster_conf is not None:
            with open(self.config_path + self.cluster_conf, "r") as fp:
                self.cluster_conf = YAML().load(fp.read())

            self.__init_rebalance_with_rbac_setup()

        # Update cluster node-service map and create cbas_util
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        self.cbas_util = CbasUtil(self.cluster.master,
                                  self.cluster.cbas_nodes[0])

        if self.rbac_conf is not None:
            with open(self.config_path + self.rbac_conf, "r") as fp:
                self.rbac_conf = YAML().load(fp.read())

        if self.bucket_conf is not None:
            with open(self.config_path + self.bucket_conf, "r") as fp:
                self.bucket_conf = YAML().load(fp.read())

            self.__setup_buckets()
        else:
            self.map_collection_data()

        self.bucket = self.bucket_util.buckets[0]
        for rbac_roles in self.rbac_conf["rbac_roles"]:
            self.create_sdk_clients(rbac_roles["roles"])

        if self.bucket_conf is not None:
            # Loading initial data into created collections
            self.initial_load = True
            for bucket in self.bucket_conf["buckets"]:
                if bucket["name"] == self.bucket.name:
                    self.load_initial_collection_data(bucket["scopes"])
                    break
            self.bucket_util.validate_docs_per_collections_all_buckets()

        if self.service_conf is not None:
            with open(self.config_path + self.service_conf, "r") as fp:
                self.service_conf = YAML().load(fp.read())

            # Configure backup settings
            self.configure_bucket_backups()

            # Create required GSIs
            self.create_indexes()

            # Create required CBAS data-sets
            self.create_cbas_indexes()

        self.log_setup_status("AppBase", "complete")

    def tearDown(self):
        self.shutdown_sdk_clients()
        super(AppBase, self).tearDown()

    def __print_step(self, message):
        message = "  %s. %s" % (self.step_num, message)
        line_delimiter = "#"*60
        self.log.info("\n{1}\n{0}\n{1}".format(message, line_delimiter))
        self.step_num += 1

    def __init_rebalance_with_rbac_setup(self):
        # Override nodes_init, services_init from yaml data
        self.nodes_init = self.cluster_conf["cb_cluster"]["nodes_init"]
        self.services_init = self.cluster_conf["cb_cluster"]["services"]

        # Rebalance_in required nodes
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [],
                            services=self.services_init[1:])
        self.cluster.nodes_in_cluster.extend(
            [self.cluster.master] + nodes_init)

        # Create RBAC users
        self.create_rbac_users(
            self.cluster.master.rest_username,
            self.cluster.master.rest_password,
            self.cluster_conf["cb_cluster"]["rbac_users"])

    def __setup_buckets(self):
        for bucket in self.bucket_conf["buckets"]:
            b_name = bucket["name"]
            s_bucket = None
            if bucket["sample_bucket"] is True:
                if b_name == "travel-sample":
                    s_bucket = TravelSample()
                elif b_name == "beer-sample":
                    s_bucket = BeerSample()
                elif b_name == "gamesim-sample":
                    s_bucket = GamesimSample()
                else:
                    self.fail("Invalid sample bucket '%s'" % b_name)

                if self.bucket_util.load_sample_bucket(s_bucket) is False:
                    self.fail("Failed to load sample bucket")
            else:
                self.bucket_util.create_default_bucket(
                    bucket_name=b_name,
                    bucket_type=bucket.get(Bucket.bucketType,
                                           Bucket.Type.MEMBASE),
                    ram_quota=bucket.get(Bucket.ramQuotaMB, None),
                    replica=bucket.get(Bucket.replicaNumber,
                                       Bucket.ReplicaNum.ONE),
                    maxTTL=bucket.get(Bucket.maxTTL, 0),
                    storage=bucket.get(Bucket.storageBackend,
                                       Bucket.StorageBackend.couchstore),
                    eviction_policy=bucket.get(
                        Bucket.evictionPolicy,
                        Bucket.EvictionPolicy.VALUE_ONLY),
                    bucket_durability=bucket.get(Bucket.durabilityMinLevel,
                                                 Bucket.DurabilityLevel.NONE))

            bucket_obj = self.bucket_util.buckets[-1]
            self.__print_step("Creating required scope/collections")
            scope_dict_data = dict()
            scope_dict_data["scopes"] = list()
            for scope in bucket["scopes"]:
                scope_dict = dict()
                scope_dict["name"] = scope["name"]
                scope_dict["collections"] = \
                    [dict(collection) for collection in
                     scope["collections"]]
                scope_dict_data["scopes"].append(scope_dict)

            BucketHelper(self.cluster.master).import_collection_using_manifest(
                bucket["name"],
                json.dumps(scope_dict_data).replace("'", '"'))
            bucket_obj.stats.increment_manifest_uid()

            for scope in bucket["scopes"]:
                self.bucket_util.create_scope_object(bucket_obj, scope)
                for collection in scope["collections"]:
                    self.bucket_util.create_collection_object(bucket_obj,
                                                              scope["name"],
                                                              collection)
            # Create RBAC users
            for t_bucket in self.rbac_conf["rbac_roles"]:
                if t_bucket["bucket"] == b_name:
                    self.create_rbac_users("rbac_admin", "rbac_admin",
                                           t_bucket["roles"])
                    break
            # Create required scope/collections
            if bucket["sample_bucket"] is True:
                bucket_obj.scopes[CbServer.default_scope].collections[
                    CbServer.default_collection].num_items \
                    = s_bucket.scopes[CbServer.default_scope].collections[
                    CbServer.default_collection].num_items

        self.bucket_util.buckets = self.bucket_util.get_all_buckets()

    def create_rbac_users(self, rest_username, rest_password, user_role_list):
        self.__print_step("Creating RBAC users")
        master = deepcopy(self.cluster.master)
        master.rest_username = rest_username
        master.rest_password = rest_password
        rest_conn = RestConnection(master)

        users = list()
        roles = list()
        for user_role in user_role_list:
            u_name = user_role["user_name"]
            password = user_role["password"]
            user_roles = ",".join(user_role["roles"])
            self.log.debug("Create user_role for %s" % u_name)
            users.append({'id': u_name, 'name': u_name, 'password': password})
            roles.append({'id': u_name, 'name': u_name,
                          'roles': user_roles})
        self.rbac_util.create_user_source(users, 'builtin', master)
        _ = self.rbac_util.add_user_role(roles, rest_conn, 'builtin')

    def create_sdk_clients(self, rbac_roles):
        self.__print_step("Creating required SDK clients")
        for role in rbac_roles:
            u_name, password = role["user_name"], role["password"]
            bucket = self.bucket
            if "select_bucket" in role and role["select_bucket"] is False:
                bucket = None
            self.sdk_clients[u_name] = SDKClient([self.cluster.master],
                                                 bucket,
                                                 username=u_name,
                                                 password=password)

    def shutdown_sdk_clients(self):
        self.__print_step("Closing SDK client connections")
        for _, client in self.sdk_clients.items():
            client.close()

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
                    try:
                        build_res = \
                            self.sdk_clients["bucket_admin"].cluster.query(
                                "BUILD INDEX on `%s`.`%s`.`%s`(%s)"
                                % (bucket, scope, collection,
                                   ",".join(indexes)))
                        if build_res.metaData().status().toString() \
                                != "SUCCESS":
                            self.fail("Build index failed for %s: %s"
                                      % (indexes, build_res))
                    except InternalServerFailureException as err:
                        if "will retry building in the background" \
                                not in str(err):
                            raise err

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

    def create_cbas_indexes(self):
        if CbServer.Services.CBAS not in self.service_conf:
            return

        client = self.sdk_clients["cbas_admin"]
        cbas_conf = self.service_conf[CbServer.Services.CBAS]
        for data_verse in cbas_conf["dataverses"]:
            query = "CREATE DATAVERSE %s" % data_verse["name"]
            result = client.cluster.analyticsQuery(query)
            if result.metaData().status().toString() != "SUCCESS":
                self.fail("Failure during analytics query: %s" % result)
        for data_set in cbas_conf["datasets"]:
            query = "CREATE DATASET `%s`.`%s` ON %s " \
                    % (data_set["dataverse"], data_set["name"],
                       data_set["on"])
            if "where" in data_set:
                query += "WHERE %s" % data_set["where"]

            result = client.cluster.analyticsQuery(query)
            if result.metaData().status().toString() != "SUCCESS":
                self.fail("Failure during analytics query: %s" % result)

    def configure_bucket_backups(self):
        if CbServer.Services.BACKUP not in self.service_conf:
            return

        backup_node = self.cluster.backup_nodes[0]
        backup_helper = BackupHelper(backup_node)

        self.log.info("Creating permissions for backup folder")
        backup_configs = self.service_conf[CbServer.Services.BACKUP]
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

    def load_initial_collection_data(self, collection_spec):
        self.__print_step("Loading initial data into collections")
        # Pairs of scope, collection name
        type_collection_map = dict()
        type_collection_map["airline"] = ("airlines", "airline")
        type_collection_map["airport"] = ("airlines", "airport")
        type_collection_map["route"] = ("airlines", "routes")
        type_collection_map["hotel"] = ("hotels", "hotels")
        type_collection_map["landmark"] = ("hotels", "landmark")

        meta_data = dict()
        for scope in collection_spec:
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
            # TODO: Remove this retry logic once MB-41535 is fixed
            retry_index = 0
            query_result = None
            while retry_index < 5:
                try:
                    query_result = sdk_client.cluster.query(query % d_type)
                    break
                except IndexFailureException:
                    retry_index += 1
                    self.sleep(5, "Retrying due to IndexFailure (MB-41535)")
                    continue
            rows_inserted = 0
            for row in query_result.rowsAsObject():
                value = row.getObject(CbServer.default_collection) \
                    .removeKey("type")

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
