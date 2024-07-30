import time
import yaml
from copy import deepcopy

from couchbase.exceptions import InternalServerFailureException
from couchbase.logic.analytics import AnalyticsStatus
from couchbase.logic.n1ql import QueryStatus

from BucketLib.bucket import TravelSample, BeerSample, GamesimSample, Bucket
from backup_utils.backup_utils import BackupUtil
from cb_constants import CbServer
from SecurityLib.rbac import RbacUtil
from basetestcase import BaseTestCase
from bucket_collections.app.constants import global_vars
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.index.index_api import IndexRestAPI
from cb_server_rest_util.security.security_api import SecurityRestAPI
from cb_tools.cbstats import Cbstats
from cbas_utils.cbas_utils import CbasUtil
from sdk_client3 import SDKClient
from shell_util.remote_connection import RemoteMachineShellConnection


class AppBase(BaseTestCase):
    def setUp(self):
        super(AppBase, self).setUp()
        self.log_setup_status("AppBase", "started")

        self.step_num = 1
        self.initial_load = self.input.param("initial_load", False)
        self.cluster_conf = self.input.param("cluster_conf", None)
        self.bucket_conf = self.input.param("bucket_conf", None)
        self.service_conf = self.input.param("service_conf", None)
        self.rbac_conf = self.input.param("rbac_conf", None)

        self.index_storage_mode = self.input.param("index_storage_mode",
                                                   "plasma")
        self.rbac_util = RbacUtil()
        self.sdk_clients = global_vars.sdk_clients
        self.app_path = "pytests/bucket_collections/app/"
        self.config_path = self.app_path + "config/"
        self.capella_run = self.input.param("capella_run", False)

        if self.cluster_conf is not None:
            with open(self.config_path+self.cluster_conf+".yaml", "r") as fp:
                self.cluster_conf = yaml.safe_load(fp.read())

            self.__init_rebalance_with_rbac_setup()

        # Update cluster node-service map and create cbas_util
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        self.cbas_util = CbasUtil(self.task)

        # Load bucket conf
        if self.bucket_conf is not None:
            with open(self.config_path+self.bucket_conf+".yaml", "r") as fp:
                self.bucket_conf = yaml.safe_load(fp.read())

        # Load RBAC conf
        if self.rbac_conf is not None:
            with open(self.config_path + self.rbac_conf + ".yaml", "r") as fp:
                self.rbac_conf = yaml.safe_load(fp.read())

        if self.bucket_conf is not None:
            self.__setup_buckets()
        self.bucket = self.cluster.buckets[0]

        if self.rbac_conf is not None:
            for rbac_roles in self.rbac_conf["rbac_roles"]:
                self.create_sdk_clients(rbac_roles["roles"])

        if self.service_conf is not None:
            with open(self.config_path+self.service_conf+".yaml", "r") as fp:
                self.service_conf = yaml.safe_load(fp.read())["services"]

            if not self.capella_run:
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

        rest = ClusterRestAPI(self.cluster.master)
        # Set cluster settings
        for setting in self.cluster_conf["cb_cluster"]["settings"]:
            if setting["name"] == "memory_quota":
                setting.pop("name")
                rest.configure_memory(setting)

        # Rebalance_in required nodes
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        result = self.task.rebalance(self.cluster, nodes_init, [],
                                     services=self.services_init[1:])
        self.assertTrue(result, "Initial rebalance failed")
        self.cluster.nodes_in_cluster.extend(
            [self.cluster.master] + nodes_init)

        if not self.capella_run:
            # Create RBAC users
            self.create_on_prem_rbac_users(
                self.cluster.master.rest_username,
                self.cluster.master.rest_password,
                self.cluster_conf["cb_cluster"]["rbac_users"])

    def __setup_buckets(self):
        self.cluster.buckets = self.bucket_util.get_all_buckets(self.cluster)
        for bucket in self.bucket_conf["buckets"]:
            bucket_obj = None
            # Skip bucket creation if already exists in cluster
            # Note: Useful while running instances for multi-tenant case
            for existing_bucket in self.cluster.buckets:
                if existing_bucket.name == bucket["name"]:
                    bucket_obj = existing_bucket
                    break
            if bucket_obj is None:
                if bucket["sample_bucket"] is True:
                    if bucket["name"] == "travel-sample":
                        s_bucket = TravelSample()
                    elif bucket["name"] == "beer-sample":
                        s_bucket = BeerSample()
                    elif bucket["name"] == "gamesim-sample":
                        s_bucket = GamesimSample()
                    else:
                        self.fail("Invalid sample bucket '%s'"
                                  % bucket["name"])

                    if self.capella_run:
                        CapellaAPI.load_sample_bucket(self.pod, self.tenant,
                                                      self.cluster.id,
                                                      "travel-sample")
                        self.sleep(15, "Wait for bucket to get warmed up")
                        retry_count = 600
                        sleep_time = 5
                        buckets = \
                            self.bucket_util.get_all_buckets(self.cluster)
                        for c_bucket in buckets:
                            if c_bucket.name == s_bucket.name:
                                # Append loaded bucket into buckets list
                                self.cluster.buckets.append(c_bucket)
                                break
                        while retry_count > 0:
                            docs = self.bucket_util.get_buckets_item_count(
                                self.cluster)
                            if docs[s_bucket.name] == \
                                    s_bucket.stats.expected_item_count:
                                break
                            self.sleep(sleep_time, "Bucket still loading")
                            retry_count -= sleep_time
                        else:
                            self.fail("Sample bucket '%s' not loaded" %
                                      s_bucket.name)
                    elif self.bucket_util.load_sample_bucket(
                            self.cluster, s_bucket) is False:
                        self.fail("Failed to load sample bucket")
                    # if Bucket.ramQuotaMB in bucket:
                    #     BucketHelper(self.cluster.master).change_bucket_props(
                    #         self.cluster.buckets[-1],
                    #         ramQuotaMB=bucket[Bucket.ramQuotaMB])
                else:
                    self.bucket_util.create_default_bucket(
                        cluster=self.cluster,
                        bucket_name=bucket["name"],
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
                        bucket_durability=bucket.get(
                            Bucket.durabilityMinLevel,
                            Bucket.DurabilityMinLevel.NONE))

                bucket_obj = self.cluster.buckets[-1]

            self.map_collection_data(bucket_obj)
            self.__print_step("Creating required scope/collections")
            for scope in bucket["scopes"]:
                if scope["name"] in bucket_obj.scopes.keys():
                    self.log.debug("Scope %s already exists for bucket %s"
                                   % (scope["name"], bucket_obj.name))
                else:
                    self.bucket_util.create_scope(self.cluster.master,
                                                  bucket_obj,
                                                  scope)
                    bucket_obj.stats.increment_manifest_uid()
                for collection in scope["collections"]:
                    if collection["name"] in \
                            bucket_obj.scopes[scope["name"]].collections:
                        self.log.debug("Collection %s :: %s exists"
                                       % (scope["name"], collection["name"]))
                    else:
                        self.bucket_util.create_collection(self.cluster.master,
                                                           bucket_obj,
                                                           scope["name"],
                                                           collection)
                        bucket_obj.stats.increment_manifest_uid()

            # Create RBAC users
            for t_bucket in self.rbac_conf["rbac_roles"]:
                if t_bucket["bucket"] == bucket["name"]:
                    if self.capella_run:
                        self.create_capella_users(t_bucket["roles"])
                    else:
                        self.create_on_prem_rbac_users(
                            "rbac_admin", "Rbac_admin@123", t_bucket["roles"])
                    break

    def create_on_prem_rbac_users(self, rest_username, rest_password,
                                  user_role_list):
        self.__print_step("Creating RBAC users")
        master = deepcopy(self.cluster.master)
        master.rest_username = rest_username
        master.rest_password = rest_password
        rest_conn = SecurityRestAPI(master)

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

    def create_capella_users(self, user_role_list):
        for user_role in user_role_list:
            u_name = user_role["user_name"]
            password = user_role["password"]
            self.log.debug("Create user %s" % u_name)
            CapellaAPI.create_db_user(self.pod, self.tenant, self.cluster.id,
                                      u_name, password)

    def create_sdk_clients(self, rbac_roles):
        self.__print_step("Creating required SDK clients")
        for role in rbac_roles:
            u_name, password = role["user_name"], role["password"]
            bucket = self.bucket
            if "select_bucket" in role and role["select_bucket"] is False:
                bucket = None
            self.sdk_clients[u_name] = SDKClient(self.cluster, bucket,
                                                 username=u_name,
                                                 password=password)

    def shutdown_sdk_clients(self):
        self.__print_step("Closing SDK client connections")
        for _, client in self.sdk_clients.items():
            client.close()

    def create_indexes(self):
        self.__print_step("Creating required Indexes")

        if not self.capella_run:
            self.log.info("Setting index storage mode=gsi")
            status, content = (IndexRestAPI(self.cluster.master).
                set_gsi_settings({'storageMode': self.index_storage_mode}))
            self.assertTrue(status, f"Failed to set storageMode: {content}")

        self.log.info("Dropping default indexes on _default collection")
        select_result = self.sdk_clients["bucket_admin"].cluster.query(
            "SELECT name,scope_id,keyspace_id FROM system:indexes "
            "WHERE bucket_id='travel-sample'")
        for row in select_result.rows():
            drop_result = self.sdk_clients["bucket_admin"].cluster.query(
                "DROP INDEX `%s` on `travel-sample`.`%s`.`%s`"
                % (row["name"], row["scope_id"], row["keyspace_id"]))
            if drop_result.metadata().status() != QueryStatus.SUCCESS:
                self.fail("Drop index '%s' failed: %s" % (row["name"],
                                                          drop_result))

        self.log.info("Creating collection specific indexes")
        for query in self.service_conf["indexes"]:
            result = self.sdk_clients["bucket_admin"].cluster.query(query)
            for _ in result.rows(): pass
            if result.metadata().status() != QueryStatus.SUCCESS:
                self.fail("Create index '%s' failed: %s" % (query, result))

        self.log.info("Building deferred indexes")
        index_names = list()
        indexes_to_build = dict()
        result = self.sdk_clients["bucket_admin"].cluster.query(
            'SELECT * FROM system:indexes '
            'WHERE bucket_id="travel-sample" AND state="deferred"')
        for row in result.rows():
            index_names.append(row["name"])
            row = row["indexes"]
            bucket = self.bucket.name
            scope = row["scope_id"]
            collection = row["keyspace_id"]
            if bucket not in indexes_to_build:
                indexes_to_build[bucket] = dict()
            if scope not in indexes_to_build[bucket]:
                indexes_to_build[bucket][scope] = dict()
            if collection not in indexes_to_build[bucket][scope]:
                indexes_to_build[bucket][scope][collection] = list()

            indexes_to_build[bucket][scope][collection].append(row["name"])

        for bucket, b_data in indexes_to_build.items():
            for scope, s_data in b_data.items():
                for collection, indexes in s_data.items():
                    try:
                        build_res = \
                            self.sdk_clients["bucket_admin"].cluster.query(
                                "BUILD INDEX on `%s`.`%s`.`%s`(%s)"
                                % (bucket, scope, collection,
                                   ",".join(indexes)))
                        for _ in build_res.rows(): pass
                        if build_res.metadata().status() != QueryStatus.SUCCESS:
                            self.fail("Build index failed for %s: %s"
                                      % (indexes, build_res))
                    except InternalServerFailureException as err:
                        if "will retry building in the background" \
                                not in str(err):
                            raise err

        self.log.info("Waiting for indexes to become online")
        start_time = time.time()
        stop_time = start_time + 300

        for i_name in index_names:
            query = f"SELECT state FROM system:indexes WHERE name='{i_name}'"
            while True:
                result = self.sdk_clients["bucket_admin"].cluster.query(query)
                states = list(set([row["state"] for row in result.rows()]))
                if len(states) == 1 and states[0] == "online":
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
            result = client.cluster.analytics_query(query)
            for _ in result.rows(): pass
            if result.metadata().status() != AnalyticsStatus.SUCCESS:
                self.fail("Failure during analytics query: %s" % result)
        for data_set in cbas_conf["datasets"]:
            query = "CREATE DATASET `%s`.`%s` ON %s " \
                    % (data_set["dataverse"], data_set["name"],
                       data_set["on_collection"])
            if "where" in data_set:
                query += "WHERE %s" % data_set["where"]

            result = client.cluster.analytics_query(query)
            for _ in result.rows(): pass
            if result.metadata().status() != AnalyticsStatus.SUCCESS:
                self.fail("Failure during analytics query: %s" % result)

    def configure_bucket_backups(self):
        if CbServer.Services.BACKUP not in self.service_conf:
            return

        backup_node = self.cluster.backup_nodes[0]
        backup_util = BackupUtil(backup_node)

        # Remove old repos (if any)
        backup_util.archive_all_repos()
        backup_util.delete_all_archive_repos(remove_repository=True)

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
                status, _ = backup_util.rest.create_plan(plan_params["name"],
                                                         plan_params)
                if status is False:
                    self.fail("Backup %s create failed" % backup_config)

            # Create repo
            status, content = backup_util.rest.create_repository(
                backup_config["repo_id"], repo_params)
            self.assertTrue(status,
                            f"Create repo failed for {repo_params}: {content}")
        shell.disconnect()

    def map_collection_data(self, bucket):
        cb_stat_objects = list()
        collection_data = None

        for node in self.cluster_util.get_kv_nodes(self.cluster):
            cb_stat_objects.append(Cbstats(node))

        for cb_stat in cb_stat_objects:
            tem_collection_data = cb_stat.get_collections(bucket)
            cb_stat.disconnect()
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
            self.bucket_util.create_scope_object(bucket,
                                                 {"name": s_name})
            for c_name, c_data in s_data.items():
                if type(c_data) is not dict:
                    continue
                self.bucket_util.create_collection_object(
                    bucket, s_name,
                    {"name": c_name, "num_items": c_data["items"],
                     "maxTTL": c_data.get("maxTTL", 0)})
