"""
Created on 13-May-2020

@author: umang.agrawal
"""
from TestInput import TestInputSingleton
from cbas.cbas_base import CBASBaseTest
import random, json, copy, time
from threading import Thread
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from rbac_utils.Rbac_ready_functions import RbacUtils
from membase.api.rest_client import RestHelper
from CbasLib.CBASOperations import CBASHelper
from cbas_utils.cbas_utils import CBASRebalanceUtil
from collections_helper.collections_spec_constants import MetaConstants, MetaCrudParams
from security_utils.security_utils import SecurityUtils

rbac_users_created = {}


class CBASExternalLinks(CBASBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input

        if "num_of_clusters" not in self.input.test_params:
            self.input.test_params.update(
                {"num_of_clusters": "2"})

        if "services_init" not in self.input.test_params:
            self.input.test_params.update(
                {"services_init": "kv:n1ql:index-cbas|kv:n1ql:index-kv:n1ql:index"})
        if "nodes_init" not in self.input.test_params:
            self.input.test_params.update(
                {"nodes_init": "2|2"})

        if self.input.param('setup_infra', True):
            if "bucket_spec" not in self.input.test_params:
                self.input.test_params.update(
                    {'bucket_spec': "analytics.single_bucket"})
            if "cluster_kv_infra" not in self.input.test_params:
                self.input.test_params.update(
                    {"cluster_kv_infra": "None|bkt_spec"})

        # Randomly choose name cardinality of links and datasets.
        if "link_cardinality" not in self.input.test_params:
            self.input.test_params.update(
                {"link_cardinality": random.randrange(1,4)})
        if "dataset_cardinality" not in self.input.test_params:
            self.input.test_params.update(
                {"dataset_cardinality": random.randrange(1,4)})

        self.encryption = self.input.param("encryption", "none")
        if self.encryption in ["full1", "full2"]:
            self.setup_cert = True
        else:
            self.setup_cert = False

        super(CBASExternalLinks, self).setUp()

        self.client_cert_state = self.input.param("client_cert_state", "enable")
        self.cert_paths = self.input.param('paths',
                                       "subject.cn:san.dnsname:san.uri")
        self.prefixs = self.input.param('prefixs', 'www.cb-:us.:www.')
        self.delimeter = self.input.param('delimeter', '.:.:.')
        self.dns = self.input.param('dns', None)
        self.cert_uri = self.input.param('uri', None)
        self.encryption_type = self.input.param('encryption_type', "")
        self.key_length = self.input.param("key_length", 1024)

        self.use_new_ddl_format = self.input.param('use_new_ddl_format', False)

        self.security_util = SecurityUtils(
            self.log, client_cert_state=self.client_cert_state,
            paths=self.cert_paths, prefixs=self.prefixs,
            delimeter=self.delimeter, client_ip="172.16.1.174",
            dns=self.dns, uri=self.cert_uri, ssltype="openssl",
            encryption_type=self.encryption_type, key_length=self.key_length)
        self.setup_certs_status = False

        """
        Assumptions:
        1) Only 1 Analytics cluster.
        2) No cbas node on any other cluster apart from analytics cluster.
        """
        self.to_clusters = list()
        for cluster in self.cb_clusters.values():
            if hasattr(cluster, "cbas_cc_node"):
                self.analytics_cluster = cluster
            else:
                self.to_clusters.append(cluster)
            cluster.rbac_util = RbacUtils(cluster.master)

        # Create analytics admin user on Analytics cluster
        self.analytics_username = "admin_analytics"
        self.analytics_cluster.rbac_util._create_user_and_grant_role(
            self.analytics_username, "analytics_admin")
        rbac_users_created[self.analytics_username] = "analytics_admin"

        self.invalid_value = "invalid"
        self.invalid_ip = "172.19.202.132"
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        """
        If certs were setup, then remove those certs and restore default certs.
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        self.create_or_delete_users(self.analytics_cluster.rbac_util,
                                    rbac_users_created, delete=True)

        for cluster in self.cb_clusters.values():
            self.cluster_util.stop_firewall_on_node(cluster, cluster.master)
            if self.setup_certs_status:
                self.security_util.teardown_x509_certs(
                    cluster.servers, CA_cert_file_path=cluster.CACERTFILEPATH)

        super(CBASExternalLinks, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def setup_certs(self, cluster):
        """
        Setup method for setting up root, node and client certs for all the clusters.
        """
        self.log.info("Setting up certificates")
        self.security_util._reset_original(cluster.nodes_in_cluster)
        self.security_util.generate_x509_certs(cluster)
        self.sleep(60)
        self.security_util.upload_x509_certs(
            cluster=cluster, setup_once=self.input.param("setup_once", True))
        self.setup_certs_status = True

    def read_file(self, file_path):
        try:
            with open(file_path, "r") as fh:
                return fh.read()
        except Exception as err:
            self.log.error(str(err))
            return None

    def setup_for_test(self, to_cluster=None, encryption=None, hostname=None,
                       setup_cert=False, create_link_objs=True,
                       create_links=True, create_remote_kv_infra=False,
                       create_dataset_objs=True, for_all_kv_entities=False,
                       same_dv_for_link_and_dataset=True,
                       create_datasets=False, connect_link=False,
                       wait_for_ingestion=False, rebalance_util=False,
                       username=None):

        self.log.info("Selecting remote cluster")
        if not to_cluster:
            to_cluster = random.choice(self.to_clusters)

        if not username:
            username = self.analytics_username

        if not encryption:
            encryption = random.choice(["none", "half", "full1", "full2"])

        if not hostname:
            hostname = random.choice(to_cluster.servers)

        if create_link_objs:
            self.cbas_util.create_link_obj(
                self.analytics_cluster, "couchbase",
                link_cardinality=self.input.param("link_cardinality", 1),
                hostname=hostname.ip,
                username=hostname.rest_username,
                password=hostname.rest_password, encryption=encryption,
                certificate=None, clientCertificate=None, clientKey=None,
                no_of_objs=self.input.param("no_of_links", 1))

        link_objs = self.cbas_util.list_all_link_objs(link_type="couchbase")

        if setup_cert:
            self.setup_certs(to_cluster)
            if encryption in ["full1", "full2"]:
                for link in link_objs:
                    link.properties["certificate"] = self.read_file(
                        to_cluster.root_ca_path)
                    link.properties["encryption"] = "full"
                    if encryption == "full2":
                        link.properties["clientCertificate"] = self.read_file(
                            to_cluster.client_certs[hostname.ip]["cert_pem"])
                        link.properties["clientKey"] = self.read_file(
                            to_cluster.client_certs[hostname.ip]["cert_key"])
                        link.properties["username"] = None
                        link.properties["password"] = None

        if create_links:
            for link_obj in link_objs:
                if not self.cbas_util.create_link(
                    self.analytics_cluster, link_obj.properties,
                    username=username):
                    self.fail("link creation failed")

        if create_remote_kv_infra:
            self.log.info("Loading bucket, scopes and collections on remote cluster")
            self.collectionSetUp(to_cluster)

        if create_dataset_objs:
            self.cbas_util.create_dataset_obj(
                self.analytics_cluster, self.bucket_util,
                dataset_cardinality=self.input.param("dataset_cardinality", 1),
                bucket_cardinality=self.input.param("bucket_cardinality", 1),
                enabled_from_KV=False, for_all_kv_entities=for_all_kv_entities,
                remote_dataset=True, link=None,
                same_dv_for_link_and_dataset=same_dv_for_link_and_dataset,
                name_length=30, fixed_length=False, exclude_bucket=[],
                exclude_scope=[], exclude_collection=[],
                no_of_objs=self.input.param("no_of_datasets", 1))

        if create_datasets:
            for dataset_obj in self.cbas_util.list_all_dataset_objs():
                if not self.cbas_util.create_dataset(
                        self.analytics_cluster, dataset_obj.name,
                        kv_entity=dataset_obj.full_kv_entity_name,
                        dataverse_name=dataset_obj.dataverse_name,
                        link_name=dataset_obj.link_name,
                        username=username,
                        analytics_collection=self.use_new_ddl_format):
                    self.fail("Dataset creation failed")

        if connect_link:
            for dataset_obj in self.cbas_util.list_all_dataset_objs():
                if not self.cbas_util.connect_link(
                    self.analytics_cluster, dataset_obj.link_name,
                    username=username):
                    self.fail("Error while connecting remote link - {0}".format(
                        dataset_obj.link_name))

        if wait_for_ingestion:
            self.log.info("Waiting for data ingestion to complete")
            if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.analytics_cluster, self.bucket_util):
                self.fail("Data Ingestion did not complete")

        if rebalance_util:
            self.rebalance_util = CBASRebalanceUtil(
                self.cluster_util, self.bucket_util, self.task, True, self.cbas_util)

        return to_cluster

    def test_create_remote_link(self):
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption="none", hostname=None, setup_cert=True,
            create_links=False, create_remote_kv_infra=False,
            create_dataset_objs=False, for_all_kv_entities=False,
            same_dv_for_link_and_dataset=True, create_datasets=False,
            connect_link=False, wait_for_ingestion=False, rebalance_util=False)

        if self.input.param("link_cardinality", 1) - 1 > 1:
            invalid_dv = "invalid.invalid"
        else:
            invalid_dv = "invalid"

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster.rbac_util,
                                    rbac_users_created)
        link = self.cbas_util.list_all_link_objs()[0]

        testcases = [
            {
                "description": "Create a link with a non-existent dataverse",
                "dataverse": invalid_dv,
                "expected_error": "Cannot find analytics scope with name {0}".format(
                    invalid_dv),
                "create_dataverse": False
            },
            {
                "description": "Create a link with an invalid hostname",
                "hostname": self.invalid_ip,
                "expected_error": "Cannot connect to host {0}".format(
                    self.invalid_ip),
                "timeout": 600
            },
            {
                "description": "Create a link with an invalid credentials",
                "password": self.invalid_value,
                "expected_error": "Invalid credentials for link {0}".format(
                    CBASHelper.unformat_name(link.full_name))
            },
            {
                "description": "Create a link with an invalid encryption value",
                "encryption": self.invalid_value,
                "expected_error": "Unknown encryption value"
            },
            {
                "description": "Create a link with an invalid root certificate",
                "encryption": "full",
                "certificate": self.read_file(
                    self.analytics_cluster.root_ca_path),
                "expected_error": "Cannot connect to host"
            },
            {
                "description": "Create a link with an invalid client certificate",
                "encryption": "full",
                "certificate": self.read_file(to_cluster.root_ca_path),
                "clientKey": self.read_file(
                    to_cluster.client_certs[link.properties["hostname"]][
                        "cert_key"]),
                "username": None,
                "password": None,
                "clientCertificate": self.read_file(
                    self.analytics_cluster.client_certs[
                        self.analytics_cluster.master.ip]["cert_pem"]),
                "expected_error": "Cannot connect to host {0}".format(
                    link.properties["hostname"])
            },
            {
                "description": "Create a link with an invalid client key",
                "encryption": "full",
                "clientKey": self.read_file(
                    self.analytics_cluster.client_certs[
                        self.analytics_cluster.master.ip]["cert_key"]),
                "certificate": self.read_file(to_cluster.root_ca_path),
                "clientCertificate": self.read_file(
                    to_cluster.client_certs[link.properties["hostname"]][
                        "cert_pem"]),
                "username": None,
                "password": None,
                "expected_error": "Cannot connect to host {0}".format(
                    link.properties["hostname"])
            },
            {
                "description": "Create a link with a name that already exists in the dataverse",
                "recreate_link": True,
                "validate_error_msg": False,
                "expected_error": "Link {0} already exists".format(
                    CBASHelper.unformat_name(link.full_name))
            },
            {
                "description": "Create a link with a name of form Local*",
                "name": "Local123",
                "expected_error": 'Links starting with \"Local\" are reserved by the system'
            },
            {
                "description": "Create a link when remote host is unreachable",
                "stop_server": True,
                "expected_error": 'Cannot connect to host'
            },
            {
                "description": "Create a link with half encryption",
                "encryption": "half",
                "validate_error_msg": False,
            },
            {
                "description": "Create a link with full encryption using root certificate, username and password",
                "encryption": "full",
                "certificate": self.read_file(to_cluster.root_ca_path),
                "validate_error_msg": False,
            },
            {
                "description": "Create a link with full encryption using root certificate, client certificate and client key",
                "encryption": "full",
                "certificate": self.read_file(to_cluster.root_ca_path),
                "clientCertificate": self.read_file(
                    to_cluster.client_certs[link.properties["hostname"]][
                        "cert_pem"]),
                "clientKey": self.read_file(
                    to_cluster.client_certs[link.properties["hostname"]][
                        "cert_key"]),
                "username": None,
                "password": None,
                "validate_error_msg": False,
            }
        ]

        rbac_testcases = self.create_testcase_for_rbac_user(
            "Create a link using {0} user", rbac_users_created,
            users_with_permission=[self.analytics_username])

        for testcase in rbac_testcases:
            if testcase["validate_error_msg"]:
                testcase["expected_error"] = "Access denied: user lacks necessary permission(s) to access resource"

        testcases = testcases + rbac_testcases
        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])
                link_properties = copy.deepcopy(link.properties)
                # Copying link property values for the testcase, to link_property dict, to create a new link.
                for key, value in testcase.iteritems():
                    if key == "username" and not testcase[key]:
                        link_properties[key] = value
                    elif key in link_properties and key != "username":
                        link_properties[key] = value

                if testcase.get("stop_server", False):
                    self.cluster_util.stop_server(to_cluster, to_cluster.master)

                if not self.cbas_util.create_link(
                    self.analytics_cluster, link_properties,
                    username=testcase.get("username", self.analytics_username),
                    validate_error_msg=testcase.get("validate_error_msg", True),
                    expected_error=testcase.get("expected_error", None),
                    create_dataverse=testcase.get("create_dataverse", True)):
                    raise Exception("Error while creating link")

                if testcase.get("stop_server", False):
                    self.cluster_util.start_server(to_cluster, to_cluster.master)
                    helper = RestHelper(to_cluster.rest)
                    service_up = False
                    count = 0
                    while not service_up:
                        count += 1
                        self.sleep(10, "waiting for couchbase service to come up")
                        try:
                            if helper.is_cluster_healthy():
                                service_up = True
                        except Exception as err:
                            self.log.error(str(err))
                            if count > 12:
                                raise Exception(
                                    "Couchbase service did not start even after 2 mins.")

                if testcase.get("recreate_link", False):
                    testcase["validate_error_msg"] = True
                    if not self.cbas_util.create_link(
                        self.analytics_cluster, link_properties,
                        username=testcase.get("username", self.analytics_username),
                        validate_error_msg=testcase.get("validate_error_msg", True),
                        expected_error=testcase.get("expected_error", None)):
                        raise Exception("Error message is different than expected.")
                    testcase["validate_error_msg"] = False

                if not testcase.get("validate_error_msg", True):
                    self.cbas_util.drop_link(
                        self.analytics_cluster, link.full_name,
                        username=self.analytics_username)
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))

    def test_create_and_drop_multiple_remote_links(self):
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption,
            hostname=None, setup_cert=self.setup_cert, create_links=False,
            create_remote_kv_infra=False, create_dataset_objs=False,
            for_all_kv_entities=False, same_dv_for_link_and_dataset=True,
            create_datasets=False, connect_link=False, wait_for_ingestion=False,
            rebalance_util=False)

        for link in self.cbas_util.list_all_link_objs():
            if self.cbas_util.create_link(
                self.analytics_cluster, link.properties,
                username=self.analytics_username):
                response = self.cbas_util.get_link_info(
                    self.analytics_cluster, link.dataverse_name, link.name,
                    link.properties["type"], username=self.analytics_username)
                if not response[0]["bootstrapHostname"] == link.properties[
                    "hostname"]:
                    self.fail("Hostname does not match. Expected - {0}\tActual - {1}".format(
                        link.properties["hostname"],
                        response[0]["bootstrapHostname"]))
            else:
                self.fail("link creation failed")

        for link in self.cbas_util.list_all_link_objs():
            if not self.cbas_util.drop_link(
                self.analytics_cluster, link.full_name):
                self.fail("Dropping link {0} failed".format(link.full_name))

    def test_list_remote_links(self):
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_links=True,
            create_remote_kv_infra=False, create_dataset_objs=True,
            for_all_kv_entities=False, same_dv_for_link_and_dataset=False,
            create_datasets=False, connect_link=False, wait_for_ingestion=False,
            rebalance_util=False)

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster.rbac_util,
                                    rbac_users_created)

        link = self.cbas_util.list_all_link_objs()[0]

        testcases = [
            {"expected_hits": 1,
             "description": "Parameters Passed - None",
             },
            {"dataverse": CBASHelper.metadata_format(link.dataverse_name),
             "expected_hits": 1,
             "description": "Parameters Passed - Dataverse",
             },
            {"dataverse": CBASHelper.metadata_format(link.dataverse_name),
             "name": CBASHelper.unformat_name(link.name),
             "expected_hits": 1,
             "description": "Parameters Passed - Dataverse, Name",
             },
            {"type": "couchbase",
             "expected_hits": 1,
             "description": "Parameters Passed - Type",
             },
            {"name": CBASHelper.unformat_name(link.name),
             "expected_hits": 0,
             "description": "Parameters Passed - Name",
             "validate_error_msg": True,
             "expected_error": "Cannot find analytics scope with name {0}".format(
                 CBASHelper.unformat_name(link.name))
             },
            {"dataverse": CBASHelper.metadata_format(link.dataverse_name),
             "name": CBASHelper.unformat_name(link.name),
             "type": "couchbase",
             "expected_hits": 1,
             "description": "Parameters Passed - Dataverse, Name and Type",
             }
        ]

        rbac_testcases = self.create_testcase_for_rbac_user(
            "List external links using {0} user", rbac_users_created,
            users_with_permission=[self.analytics_username])

        for testcase in rbac_testcases:
            testcase["dataverse"] = CBASHelper.metadata_format(
                link.dataverse_name)
            testcase["name"] = CBASHelper.unformat_name(link.name)
            testcase["type"] = "couchbase"
            if testcase["username"] in ["admin_analytics"]:
                testcase["validate_error_msg"] = False
            if testcase["validate_error_msg"]:
                testcase["expected_hits"] = 0
                testcase["expected_error"] = "Access denied: user lacks necessary permission(s) to access resource"
            else:
                testcase["expected_hits"] = 1

        testcases = testcases + rbac_testcases

        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])

                response = self.cbas_util.get_link_info(
                    self.analytics_cluster,
                    dataverse=testcase.get("dataverse", None),
                    link_name=testcase.get("name", None),
                    link_type=testcase.get("type", None),
                    validate_error_msg=testcase.get("validate_error_msg",
                                                    False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get("username", self.analytics_username))

                if testcase.get("validate_error_msg", False):
                    if not response:
                        raise Exception(
                            "Error message is different than expected.")
                else:
                    if not len(response) == testcase["expected_hits"]:
                        raise Exception(
                            "Expected links - {0} \t Actual links - {1}".format(
                                testcase["expected_hits"], len(response)))
                    if not response[0]["scope"] == testcase.get(
                        "dataverse", CBASHelper.metadata_format(
                            link.dataverse_name)):
                        raise Exception(
                            "Expected - {0} \t Actual- {1}".format(
                                testcase.get(
                                    "dataverse", CBASHelper.metadata_format(
                                        link.dataverse_name)),
                                response[0]["scope"]))
                    if not response[0]["name"] == testcase.get(
                        "name", CBASHelper.unformat_name(link.name)):
                        raise Exception(
                            "Expected - {0} \t Actual- {1}".format(
                                testcase.get("name", CBASHelper.unformat_name(
                                    link.name)), response[0]["name"]))
                    if not response[0]["type"] == "couchbase":
                        raise Exception(
                            "Expected - {0} \t Actual- {1}".format(
                                "couchbase", response[0]["type"]))
                    if not response[0]["bootstrapHostname"] == link.properties["hostname"]:
                        raise Exception(
                            "Expected - {0} \t Actual- {1}".format(
                                link.properties["hostname"],
                                response[0]["bootstrapHostname"]))
                    if not response[0]["username"] == link.properties["username"]:
                        raise Exception(
                            "Expected - {0} \t Actual- {1}".format(
                                link.properties["username"],
                                response[0]["username"]))
                    if not response[0]["encryption"] == link.properties[
                        "encryption"]:
                        raise Exception(
                            "Expected - {0} \t Actual- {1}".format(
                                link.properties["encryption"],
                                response[0]["encryption"]))
                    if not response[0]["certificate"] == link.properties.get(
                        "certificate", None):
                        raise Exception(
                            "Expected - {0} \t Actual- {1}".format(
                                link.properties["certificate"],
                                response[0]["certificate"]))
                    if not response[0]["clientCertificate"] == link.properties.get(
                        "clientCertificate", None):
                        raise Exception(
                            "Expected - {0} \t Actual- {1}".format(
                                link.properties["clientCertificate"],
                                response[0]["clientCertificate"]))
                    if link.properties["encryption"] == "full":
                        if not response[0]["clientKey"] == "<redacted sensitive entry>":
                            raise Exception(
                                "Expected - {0} \t Actual- {1}".format(
                                    "<redacted sensitive entry>",
                                    response[0]["clientKey"]))
                        if not response[0]["password"] == link.properties["password"]:
                            raise Exception(
                                "Expected - {0} \t Actual- {1}".format(
                                    link.properties["password"],
                                    response[0]["password"]))
                    else:
                        if not response[0]["password"] == "<redacted sensitive entry>":
                            raise Exception(
                                "Expected - {0} \t Actual- {1}".format(
                                    "<redacted sensitive entry>",
                                    response[0]["password"]))
                        if not response[0]["clientKey"] == link.properties.get(
                            "clientKey", None):
                            raise Exception(
                                "Expected - {0} \t Actual- {1}".format(
                                    link.properties["clientKey"],
                                    response[0]["clientKey"]))
                self.log.info("Test Passed")
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))

    def restore_link_to_original(self, link):
        self.log.info("Resetting link to original state")
        if not self.cbas_util.disconnect_link(
            self.analytics_cluster, link.full_name):
            raise Exception("Error while Disconnecting the link ")
        if not self.cbas_util.update_external_link_properties(
            self.analytics_cluster, link.properties):
            raise Exception("Error while restoring link to original config")
        if not self.cbas_util.connect_link(
            self.analytics_cluster, link.full_name, with_force=True):
            raise Exception("Error while connecting the link ")

    def validate_alter_link(self, to_cluster, link_properties, dataset,
                            operation="update", timeout=300):

        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
                self.doc_spec_name)

        if operation == "create":
            self.load_data_into_buckets(
                to_cluster, doc_loading_spec=doc_loading_spec,
                async_load=False, validate_task=True)
            if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.analytics_cluster, self.bucket_util):
                raise Exception("New data loaded on KV was not ingested successfully")
        elif operation == "update":
            mutation_num = random.randint(0, 100)
            doc_loading_spec["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 100
            self.load_data_into_buckets(
                to_cluster, doc_loading_spec=doc_loading_spec, async_load=False,
                validate_task=True, mutation_num=mutation_num)

            end_time = time.time() + timeout
            while time.time() < end_time:
                total_items, mutated_items = self.cbas_util.get_num_items_in_cbas_dataset(
                    self.analytics_cluster, dataset.full_name, timeout=300,
                    analytics_timeout=300)
                if total_items == mutated_items:
                    break
                else:
                    self.sleep(10, "Will retry CBAS query after sleep")

        response = self.cbas_util.get_link_info(
            self.analytics_cluster, dataverse=link_properties["dataverse"],
            link_name=link_properties["name"])[0]
        if not (response["bootstrapHostname"] == link_properties["hostname"]):
            raise Exception(
                "Hostname does not match. Expected - {0}\nActual - {1}".format(
                    link_properties["hostname"],
                    response["bootstrapHostname"]))
        if not (response["username"] == link_properties["username"]):
            raise Exception(
                "Hostname does not match. Expected - {0}\nActual - {1}".format(
                    link_properties["username"], response["username"]))
        return True

    def test_alter_link_properties(self):

        if self.input.param("link_cardinality", 1) - 1 > 1:
            invalid_dv = "invalid.invalid"
        else:
            invalid_dv = "invalid"

        to_cluster_1 = None
        to_cluster_2 = None
        for cluster in self.to_clusters:
            if len(cluster.servers) > 1 and not to_cluster_1:
                to_cluster_1 = cluster
            else:
                to_cluster_2 = cluster

        to_cluster_1 = self.setup_for_test(
            to_cluster=to_cluster_1, encryption="none", hostname=None,
            setup_cert=True, create_links=True, create_remote_kv_infra=True,
            create_dataset_objs=True, for_all_kv_entities=False,
            same_dv_for_link_and_dataset=True, create_datasets=True,
            connect_link=True, wait_for_ingestion=True, rebalance_util=False)

        link = self.cbas_util.list_all_link_objs()[0]
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster.rbac_util,
                                    rbac_users_created)

        ip_list = []
        for server in to_cluster_1.servers:
            if server.ip != to_cluster_1.master.ip:
                ip_list.append(server.ip)

        testcases = [
            {
                "description": "Changing dataverse to another existing dataverse",
                "validate_error_msg": True,
                "expected_error": "Link {0}.{1} does not exist",
                "new_dataverse": True
            },
            {
                "description": "Changing dataverse to a non-existing dataverse",
                "invalid_dataverse": invalid_dv,
                "new_dataverse": True,
                "validate_error_msg": True,
                "expected_error": "Cannot find analytics scope with name {0}".format(
                    invalid_dv)
            },
            {
                "description": "Changing link type",
                "type": "s3",
                "validate_error_msg": True,
                "expected_error": "Link type for an existing link cannot be changed."
            },
            {
                "description": "Changing hostname to another node of same cluster",
                "hostname": ip_list[0],
            },
            {
                "description": "Changing hostname to another cluster",
                "hostname": to_cluster_2.master.ip,
                "load_bucket": True
            },
            {
                "description": "Changing hostname to another cluster with force flag on connect link",
                "hostname": to_cluster_2.master.ip,
                "load_bucket": True,
                "with_force": True
            },
            {
                "description": "Changing hostname to an invalid hostname",
                "hostname": self.invalid_ip,
                "validate_error_msg": True,
                "expected_error": "Cannot connect to host {0}".format(
                    self.invalid_ip)
            },
            {
                "description": "Changing credentials to invalid credentials",
                "password": self.invalid_value,
                "validate_error_msg": True,
                "expected_error": "Invalid credentials for link {0}".format(
                    CBASHelper.unformat_name(link.full_name))
            },
            {
                "description": "Changing credentials to another set of valid credentials",
                "new_user": "bucket_full_access[*]",
            },
            {
                "description": "Changing credentials to a user which has less than minimum role required",
                "new_user": "analytics_reader",
                "validate_connect_error": True,
                "expected_connect_error": "Access denied: user lacks necessary permission(s) to access resource"
            },
            {
                "description": "Changing encryption type to half",
                "encryption": "half"
            },
            {
                "description": "Changing encryption type to full, with valid root certificate",
                "encryption": "full",
                "certificate": self.read_file(to_cluster_1.root_ca_path)
            },
            {
                "description": "Changing encryption type to full, with invalid root certificate",
                "encryption": "full",
                "certificate": self.read_file(
                    self.analytics_cluster.root_ca_path),
                "validate_error_msg": True,
                "expected_error": "Cannot connect to host"
            },
            {
                "description": "Changing encryption type to full, with valid root certificate, clientcertificate and client key",
                "encryption": "full",
                "certificate": self.read_file(to_cluster_1.root_ca_path),
                "clientCertificate": self.read_file(
                    to_cluster_1.client_certs[link.properties["hostname"]][
                        "cert_pem"]),
                "clientKey": self.read_file(
                    to_cluster_1.client_certs[link.properties["hostname"]][
                        "cert_key"]),
                "username": None,
                "password": None
            },
            {
                "description": "Changing encryption type to full, with valid root certificate and clientcertificate and invalid client key",
                "encryption": "full",
                "username": None,
                "password": None,
                "certificate": self.read_file(to_cluster_1.root_ca_path),
                "clientCertificate": self.read_file(
                    to_cluster_1.client_certs[link.properties["hostname"]][
                        "cert_pem"]),
                "clientKey": self.read_file(
                    self.analytics_cluster.client_certs[
                        self.analytics_cluster.master.ip]["cert_key"]),
                "validate_error_msg": True,
                "expected_error": "Cannot connect to host {0}".format(
                    link.properties["hostname"])
            },
            {
                "description": "Changing encryption type to full, with valid root certificate and clientKey and invalid clientcertificate",
                "encryption": "full",
                "username": None,
                "password": None,
                "certificate": self.read_file(to_cluster_1.root_ca_path),
                "clientKey": self.read_file(
                    to_cluster_1.client_certs[link.properties["hostname"]][
                        "cert_key"]),
                "clientCertificate": self.read_file(
                    self.analytics_cluster.client_certs[
                        self.analytics_cluster.master.ip]["cert_pem"]),
                "validate_error_msg": True,
                "expected_error": "Cannot connect to host {0}".format(
                    link.properties["hostname"])
            }
        ]

        rbac_testcases = self.create_testcase_for_rbac_user(
            "Altering external link properties using {0} user",
            rbac_users_created, users_with_permission=[self.analytics_username])

        for testcase in rbac_testcases:
            testcase["encryption"] = "half"
            if testcase["validate_error_msg"]:
                testcase["expected_error"] = "Access denied: user lacks necessary permission(s) to access resource"

        testcases = testcases + rbac_testcases
        failed_testcases = list()

        reset_original = False
        for testcase in testcases:
            try:
                if reset_original:
                    self.restore_link_to_original(link)

                self.log.info(testcase["description"])

                link_properties = copy.deepcopy(link.properties)
                # Copying link property values for the testcase, to link_property dict, to create a new link.
                for key, value in testcase.iteritems():
                    if key == "username" and not testcase[key]:
                        link_properties[key] = value
                    elif key in link_properties and key != "username":
                        link_properties[key] = value

                # disconnect link before altering
                self.log.info(
                    "Disconnecting link before altering it's properties")
                if not self.cbas_util.disconnect_link(
                    self.analytics_cluster, link.full_name):
                    raise Exception("Error while Disconnecting the link ")

                if testcase.get("load_bucket", False):
                    self.collectionSetUp(to_cluster_2)
                elif testcase.get("new_dataverse", False):
                    if testcase.get("invalid_dataverse", None):
                        link_properties["dataverse"] = testcase.get(
                            "invalid_dataverse")
                    else:
                        link_properties["dataverse"] = "dataverse2"
                        if not self.cbas_util.create_dataverse(
                            self.analytics_cluster, link_properties["dataverse"],
                            username=self.analytics_username):
                            raise Exception("Dataverse creation failed")
                    testcase["expected_error"] = testcase["expected_error"].format(
                        link_properties["dataverse"], link_properties["name"])
                elif testcase.get("new_user", None):
                    link_properties["username"] = testcase["new_user"].replace(
                        "[*]", "")
                    to_cluster_1.rbac_util._create_user_and_grant_role(
                        testcase["new_user"].replace("[*]", ""),
                        testcase["new_user"])

                # Altering link
                self.log.info("Altering link properties")
                response = self.cbas_util.update_external_link_properties(
                    self.analytics_cluster, link_properties,
                    validate_error_msg=testcase.get("validate_error_msg",
                                                    False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get("username", self.analytics_username))

                if testcase.get("validate_error_msg", False) or testcase.get(
                        "validate_connect_error", False) or testcase.get(
                        "with_force", False):
                    if not response:
                        raise Exception(
                            "Error message is different than expected.")

                    if testcase.get("new_dataverse", False):
                        if not testcase.get("invalid_dataverse", None) and not \
                                self.cbas_util.drop_dataverse(
                                    self.analytics_cluster,
                                    link_properties["dataverse"],
                                    username=self.analytics_username):
                            raise Exception("Dropping Dataverse {0} failed".format(
                                link_properties["dataverse"]))
                        link_properties["dataverse"] = link.dataverse_name

                    self.log.info(
                        "Connecting link after altering link properties")
                    if self.cbas_util.connect_link(
                        self.analytics_cluster, link_name="{0}.{1}".format(
                            link_properties["dataverse"],
                            link_properties["name"]),
                        with_force=testcase.get("with_force", False),
                        validate_error_msg=testcase.get(
                            "validate_connect_error", False),
                        expected_error=testcase.get(
                            "expected_connect_error", None)):

                        if testcase.get("with_force", False):
                            if not self.validate_alter_link(
                                to_cluster_2, link_properties, dataset, "create"):
                                raise Exception(
                                    "Validating data and link property after altering link failed")
                        reset_original = True
                    else:
                        raise Exception("Error while connecting the link ")
                else:
                    self.log.info(
                        "Connecting link after altering link properties")
                    if not self.cbas_util.connect_link(
                        self.analytics_cluster,
                        link_name="{0}.{1}".format(
                            link_properties["dataverse"],
                            link_properties["name"])):
                        raise Exception("Error while connecting the link ")
                    if not self.validate_alter_link(
                        to_cluster_1, link_properties, dataset, "update"):
                        raise Exception(
                            "Validating data and link property after altering link failed")
                    if not response:
                        raise Exception("Altering link properties failed.")

                if testcase.get("new_user", None):
                    to_cluster_1.rbac_util._drop_user(
                        testcase["new_user"].replace("[*]", ""))

                if testcase.get("load_bucket", False) \
                        and not self.bucket_util.delete_bucket(
                            to_cluster_2, to_cluster_2.buckets[0], True):
                    raise Exception("Error while deleting {0} bucket in remote cluster".format(
                        to_cluster_2.buckets[0].name))

                self.log.info("Test Passed")
            except Exception as err:
                self.log.error(str(err))
                if testcase.get("load_bucket", False) \
                    and not self.bucket_util.delete_bucket(
                        to_cluster_2, to_cluster_2.buckets[0], True):
                    raise Exception(
                        "Error while deleting {0} bucket in remote cluster".format(
                            to_cluster_2.buckets[0].name))
                reset_original = True
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))

    def test_connect_link(self):
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_links=True,
            create_remote_kv_infra=False, create_dataset_objs=False,
            for_all_kv_entities=False, same_dv_for_link_and_dataset=True,
            create_datasets=False, connect_link=False, wait_for_ingestion=False,
            rebalance_util=False)

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster.rbac_util,
                                    rbac_users_created)
        link = self.cbas_util.list_all_link_objs()[0]

        testcases = [
            {"name": link.full_name,
             "description": "Connect a valid link",
             },
            {"name": self.invalid_value,
             "description": "Connect an invalid link",
             "validate_error_msg": True,
             "expected_error": "Link {0}.{1} does not exist".format(
                 link.properties["dataverse"], self.invalid_value)
             }
        ]

        rbac_testcases = self.create_testcase_for_rbac_user(
            "Connect external link using {0} user", rbac_users_created,
            users_with_permission=[self.analytics_username])

        for testcase in rbac_testcases:
            testcase["name"] = link.properties["name"]

        testcases = testcases + rbac_testcases
        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])
                if self.cbas_util.connect_link(
                    self.analytics_cluster,
                    link_name="{0}.{1}".format(
                        link.properties["dataverse"], testcase["name"]),
                    validate_error_msg=testcase.get("validate_error_msg", False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get("username", self.analytics_username)):
                    if not testcase.get("validate_error_msg", False) and not self.cbas_util.disconnect_link(
                        self.analytics_cluster, link.full_name):
                        raise Exception("Error while Disconnecting the link ")
                else:
                    raise Exception("Error while connecting the link ")

                self.log.info("Test Passed")
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))

    def test_disconnect_link(self):
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_links=True,
            create_remote_kv_infra=False, create_dataset_objs=False,
            for_all_kv_entities=False, same_dv_for_link_and_dataset=True,
            create_datasets=False, connect_link=True, wait_for_ingestion=False,
            rebalance_util=False)

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster.rbac_util,
                                    rbac_users_created)
        link = self.cbas_util.list_all_link_objs()[0]

        testcases = [
            {"name": link.full_name,
             "description": "Disconnect a valid link",
             }
        ]

        rbac_testcases = self.create_testcase_for_rbac_user(
            "Disconnect external link using {0} user", rbac_users_created,
            users_with_permission=[self.analytics_username])
        for testcase in rbac_testcases:
            testcase["name"] = link.full_name

        testcases = testcases + rbac_testcases

        failed_testcases = list()

        connect_link = False

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])
                if connect_link and not self.cbas_util.connect_link(
                    self.analytics_cluster, testcase["name"]):
                    raise Exception("Error while connecting the link ")
                if self.cbas_util.disconnect_link(
                    self.analytics_cluster, testcase["name"],
                    validate_error_msg=testcase.get("validate_error_msg", False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get("username", self.analytics_username)):
                    connect_link = True
                else:
                    connect_link = False
                    raise Exception("Error while Disconnecting the link ")
                self.log.info("Test Passed")
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))

    def test_create_dataset_for_all_rbac_roles(self):
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_links=True, create_remote_kv_infra=True,
            create_dataset_objs=True, for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True), create_datasets=False,
            connect_link=False, wait_for_ingestion=False, rebalance_util=False)

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster.rbac_util,
                                    rbac_users_created)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        rbac_testcases = self.create_testcase_for_rbac_user(
            "Creating dataset on external link using {0} user",
            rbac_users_created, users_with_permission=[self.analytics_username])

        failed_testcases = list()

        for testcase in rbac_testcases:
            try:
                self.log.info(testcase["description"])
                if not self.cbas_util.create_dataset(
                        self.analytics_cluster, dataset.name,
                        kv_entity=dataset.full_kv_entity_name,
                        dataverse_name=dataset.dataverse_name,
                        link_name=dataset.link_name,
                        validate_error_msg=testcase.get("validate_error_msg", False),
                        expected_error=testcase.get("expected_error", None),
                        username=testcase.get("username", self.analytics_username),
                        analytics_collection=self.use_new_ddl_format):
                    raise Exception("Error while creating dataset")

                if not testcase.get("validate_error_msg", False):
                    if not self.cbas_util.validate_dataset_in_metadata(
                            self.analytics_cluster. dataset.name,
                            dataset.dataverse_name, link_name=dataset.link_name,
                            bucket_name=dataset.kv_bucket.name):
                        raise Exception(
                            "Dataset entry not present in Metadata.Dataset collection")
                    if not self.cbas_util.drop_dataset(
                        self.analytics_cluster, dataset.full_name):
                        raise Exception("Error while dropping dataset")
                self.log.info("Test Passed")
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))

    def test_query_dataset_for_all_rbac_roles(self):
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_links=True, create_remote_kv_infra=True,
            create_dataset_objs=True, for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True), create_datasets=True,
            connect_link=True, wait_for_ingestion=True, rebalance_util=False)

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster.rbac_util,
                                    rbac_users_created)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        rbac_testcases = self.create_testcase_for_rbac_user(
            "Querying dataset on external link using {0} user",
            rbac_users_created, users_with_permission=[self.analytics_username])

        failed_testcases = list()

        for testcase in rbac_testcases:
            try:
                self.log.info(testcase["description"])
                if testcase.get("username") in ["data_backup",
                                                "analytics_reader"]:
                    testcase["validate_error_msg"] = False
                    testcase["expected_error"] = None
                if testcase.get("username") in ["analytics_admin",
                                                "admin_analytics"]:
                    testcase["validate_error_msg"] = True
                    testcase["expected_error"] = "User must have permission"
                status, metrics, errors, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.analytics_cluster,
                    statement="select count(*) from {0}".format(dataset.full_name),
                    username=testcase.get("username", self.analytics_username))
                if testcase.get("validate_error_msg", False):
                    if not self.cbas_util.validate_error_in_response(
                        status, errors,
                        expected_error=testcase.get("expected_error", None)):
                        raise Exception(
                            "Error msg is different from expected error msg")
                elif not cbas_result[0][dataset.full_name] == dataset.num_of_items:
                    raise Exception(
                        "Result returned from analytics query does not match the result of N1Ql query")
                self.log.info("Test passed")
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))

    def test_drop_dataset_for_all_rbac_roles(self):
        """
        add test case for dropping dataset while link is connected
        """
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_links=True, create_remote_kv_infra=True,
            create_dataset_objs=True, for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True), create_datasets=True,
            connect_link=False, wait_for_ingestion=False, rebalance_util=False)

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster.rbac_util,
                                    rbac_users_created)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        rbac_testcases = self.create_testcase_for_rbac_user(
            "Creating dataset on external link using {0} user",
            rbac_users_created, users_with_permission=[self.analytics_username])

        failed_testcases = list()

        create_dataset = False

        for testcase in rbac_testcases:
            try:
                self.log.info(testcase["description"])
                if create_dataset:
                    if not self.cbas_util.create_dataset(
                            self.analytics_cluster, dataset.name,
                            kv_entity=dataset.full_kv_entity_name,
                            dataverse_name=dataset.dataverse_name,
                            link_name=dataset.link_name,
                            analytics_collection=self.use_new_ddl_format):
                        raise Exception("Error while creating dataset")
                    else:
                        create_dataset = False

                if not self.cbas_util.drop_dataset(
                    self.analytics_cluster, dataset.full_name,
                    validate_error_msg=testcase.get("validate_error_msg", False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get("username", self.analytics_username)):
                    raise Exception("Error while dropping dataset")

                if not testcase.get("validate_error_msg", False):
                    create_dataset = True
                self.log.info("Test Passed")
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))

    def test_effect_of_rbac_role_change_on_remote_link(self):
        """
        This testcase checks, effect of user role change on data ingestion,
        when the role of the user who created the link and dataset is changed to a role,
        which does not have permission to create link or datasets.
        """
        new_username = "user1"
        original_user_role = "analytics_admin"
        new_user_role = "analytics_reader"
        self.analytics_cluster.rbac_util._create_user_and_grant_role(
            new_username, original_user_role)
        rbac_users_created[new_username] = None

        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_links=True, create_remote_kv_infra=True,
            create_dataset_objs=True, for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True), create_datasets=True,
            connect_link=True, wait_for_ingestion=True, rebalance_util=False,
            username=new_username)

        self.analytics_cluster.rbac_util._create_user_and_grant_role(
            new_username, new_user_role)

        self.load_data_into_buckets(
            to_cluster, doc_loading_spec=None, async_load=False, validate_task=True)
        if not self.cbas_util.wait_for_ingestion_all_datasets(
            self.analytics_cluster, self.bucket_util):
            self.fail("Data Ingestion did not complete")

    def test_data_ingestion_after_reducing_and_then_restoring_remote_user_permission(
            self):
        to_cluster = random.choice(self.to_clusters)

        # Create a new high privileged role remote user
        new_username = "user1"
        original_user_role = "admin"
        new_user_role = "query_external_access"
        to_cluster.rbac_util._create_user_and_grant_role(new_username,
                                                         original_user_role)
        rbac_users_created[new_username] = None

        # Set user name to the remote user created above in link properties
        to_cluster = self.setup_for_test(
            to_cluster=to_cluster, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True, create_links=False,
            create_remote_kv_infra=False, create_dataset_objs=False,
            for_all_kv_entities=False, same_dv_for_link_and_dataset=True,
            create_datasets=False, connect_link=False, wait_for_ingestion=False,
            rebalance_util=False, username=None)

        link = self.cbas_util.list_all_link_objs()[0]
        link.properties["username"] = new_username

        to_cluster = self.setup_for_test(
            to_cluster=to_cluster, encryption="none", hostname=None,
            setup_cert=False, create_link_objs=False, create_links=True,
            create_remote_kv_infra=True, create_dataset_objs=True,
            for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True),
            create_datasets=True, connect_link=True, wait_for_ingestion=True,
            rebalance_util=False, username=None)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        # Assign the remote user created before a low privilege role
        self.log.info("Assigning Low priviledge role to remote user")
        to_cluster.rbac_util._create_user_and_grant_role(new_username,
                                                         new_user_role)

        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
            self.doc_spec_name)
        self.over_ride_doc_loading_template_params(doc_loading_spec)
        doc_loading_task = self.bucket_util.run_scenario_from_spec(
            self.task, to_cluster, to_cluster.buckets, doc_loading_spec,
            mutation_num=0, batch_size=self.batch_size,
            async_load=True, validate_task=False)

        if self.input.param("after_timeout", False):
            # Restoring remote user permission after timeout

            # Wait until timeout happens and dataset is disconnected
            retry = 0
            while retry < 30:
                retry += 1
                status, content, response = self.cbas_util.fetch_bucket_state_on_cbas(
                    self.analytics_cluster)
                if status and json.loads(content)["buckets"][0][
                    "state"] != "connected":
                    break
                self.sleep(10)

            self.log.info(
                "Validate not all new data has been ingested into dataset")
            self.cbas_util.refresh_dataset_item_count(self.bucket_util)
            if self.cbas_util.validate_cbas_dataset_items_count(
                self.analytics_cluster, dataset.full_name, dataset.num_of_items,
                num_tries=1):
                self.fail(
                    "New data was ingested into dataset even after remote user permission was revoked.")

            to_cluster.rbac_util._create_user_and_grant_role(new_username,
                                                             original_user_role)
            # Reconnecting link.
            if not self.cbas_util.connect_link(
                self.analytics_cluster, dataset.link_name,
                username=self.analytics_username):
                self.fail("Error while connecting link")

        else:
            self.log.info("Restoring remote user permission")
            # Restoring remote user permission before timeout
            to_cluster.rbac_util._create_user_and_grant_role(new_username,
                                                             original_user_role)

        if doc_loading_task.result is False:
            self.fail("Reloading data failed")

        if not self.cbas_util.wait_for_ingestion_all_datasets(
            self.analytics_cluster, self.bucket_util):
            self.fail("Data Ingestion did not complete")

    def test_data_ingestion_after_daleting_and_then_recreating_remote_user(
            self):
        to_cluster = random.choice(self.to_clusters)

        # Create a new high privileged role remote user
        new_username = "user1"
        original_user_role = "bucket_full_access[*]"

        timeout = 120
        analytics_timeout = 120

        if self.input.param("has_bucket_access", False) and self.input.param(
                "same_role", False):
            new_role = original_user_role
        elif self.input.param("has_bucket_access",
                              False) and not self.input.param("same_role",
                                                              False):
            new_role = "admin"
        else:
            new_role = "analytics_admin"
            timeout = 900
            analytics_timeout = 900

        to_cluster.rbac_util._create_user_and_grant_role(new_username,
                                                         original_user_role)
        rbac_users_created[new_username] = None

        to_cluster = self.setup_for_test(
            to_cluster=to_cluster, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True, create_links=False,
            create_remote_kv_infra=False, create_dataset_objs=False,
            for_all_kv_entities=False, same_dv_for_link_and_dataset=True,
            create_datasets=False, connect_link=False, wait_for_ingestion=False,
            rebalance_util=False, username=None)

        link = self.cbas_util.list_all_link_objs()[0]
        link.properties["username"] = new_username

        to_cluster = self.setup_for_test(
            to_cluster=to_cluster, encryption="none", hostname=None,
            setup_cert=False, create_link_objs=False, create_links=True,
            create_remote_kv_infra=True, create_dataset_objs=True,
            for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True),
            create_datasets=True, connect_link=True, wait_for_ingestion=True,
            rebalance_util=False, username=None)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        # Delete the remote user created before.
        to_cluster.rbac_util._drop_user(new_username)

        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
            self.doc_spec_name)
        self.over_ride_doc_loading_template_params(doc_loading_spec)
        doc_loading_task = self.bucket_util.run_scenario_from_spec(
            self.task, to_cluster, to_cluster.buckets, doc_loading_spec,
            mutation_num=0, batch_size=self.batch_size,
            async_load=True, validate_task=False)

        if self.input.param("after_timeout", False):
            # Restoring remote user permission after timeout

            # Wait until timeout happens and dataset is disconnected
            retry = 0
            while retry < 30:
                retry += 1
                status, content, response = self.cbas_util.fetch_bucket_state_on_cbas(
                    self.analytics_cluster)
                if status and json.loads(content)["buckets"][0][
                    "state"] != "connected":
                    break
                self.sleep(10, "waiting for link to auto disconnect")

            self.log.info(
                "Validate not all new data has been ingested into dataset")
            if self.cbas_util.validate_cbas_dataset_items_count(
                self.analytics_cluster, dataset.full_name, dataset.num_of_items,
                num_tries=1):
                self.fail(
                    "New data was ingested into dataset even after remote user permission was revoked.")

            to_cluster.rbac_util._create_user_and_grant_role(new_username,
                                                             new_role)

            # Reconnecting link.
            if not self.cbas_util.connect_link(
                self.analytics_cluster, dataset.link_name,
                username=self.analytics_username,
                validate_error_msg=self.input.param("validate_error_msg", False),
                expected_error=self.input.param("error_msg", None),
                timeout=timeout, analytics_timeout=analytics_timeout):
                self.fail("Error while connecting link")

        else:
            # Restoring remote user permission before timeout
            to_cluster.rbac_util._create_user_and_grant_role(new_username,
                                                             new_role)

        if doc_loading_task.result is False:
            self.fail("Reloading data failed")

        # Allow ingestion to complete
        result = self.cbas_util.wait_for_ingestion_complete(
            self.analytics_cluster, dataset.full_name, dataset.num_of_items)

        if not self.input.param("has_bucket_access", False) and result:
            self.fail("Data Ingestion started when it should not.")
        elif not result:
            self.fail("Data Ingestion did not complete")

    def test_dataset_creation_when_network_down(self):
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True, create_links=True,
            create_remote_kv_infra=True, create_dataset_objs=True,
            for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True),
            create_datasets=False, connect_link=False, wait_for_ingestion=False,
            rebalance_util=False, username=None)
        link = self.cbas_util.list_all_link_objs()[0]
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        RemoteUtilHelper.enable_firewall(
            server=to_cluster.master, all_interface=True, action_on_packet="DROP")

        self.sleep(10, "Waiting after applying firewall rule")

        def sleep_and_bring_network_up():
            time.sleep(25)
            self.cluster_util.stop_firewall_on_node(to_cluster, to_cluster.master)

        if self.input.param("network_up_before_timeout", False):
            thread = Thread(target=sleep_and_bring_network_up,
                            name="connect_link_thread")
            thread.start()
        else:
            end_time = time.time() + 600
            while time.time() < end_time:
                if self.cbas_util.is_link_active(
                    self.analytics_cluster, link.name, link.dataverse_name,
                    "couchbase"):
                    self.sleep(30, "Waiting for link to be disconnected")
                else:
                    break

        result = self.cbas_util.create_dataset(
            self.analytics_cluster, dataset.name,
            kv_entity=dataset.full_kv_entity_name,
            dataverse_name=dataset.dataverse_name, link_name=dataset.link_name,
            validate_error_msg=self.input.param("validate_error", False),
            expected_error=self.input.param("expected_error", None),
            analytics_collection=self.use_new_ddl_format)

        self.cluster_util.stop_firewall_on_node(to_cluster, to_cluster.master)
        if not result:
            self.fail("Dataset creation failed")

    def test_connect_link_when_network_up_before_timeout(self):
        "This testcase deals with the scenario, when network comes back up before timeout happens for connect link DDL"
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True, create_links=True,
            create_remote_kv_infra=True, create_dataset_objs=True,
            for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True),
            create_datasets=True, connect_link=False, wait_for_ingestion=False,
            rebalance_util=False, username=None)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        RemoteUtilHelper.enable_firewall(
            server=to_cluster.master, action_on_packet="DROP",
            block_ips=[self.analytics_cluster.master.ip], all_interface=True)

        link_connected = False

        def get_connect_link_result():
            if self.cbas_util.connect_link(
                self.analytics_cluster, dataset.link_name,
                username=self.analytics_username):
                link_connected = True

        thread = Thread(target=get_connect_link_result,
                        name="connect_link_thread")
        thread.start()

        self.cluster_util.stop_firewall_on_node(to_cluster, to_cluster.master)

        thread.join()

        if not link_connected:
            self.fail("Link connection Failed.")

        if not self.cbas_util.wait_for_ingestion_complete(
            self.analytics_cluster, dataset.full_name, dataset.num_of_items):
            self.fail("Data Ingestion did not resume")

    def test_data_ingestion_resumes_when_network_up_before_timeout(self):
        "This testcase deals with the scenario, when network comes back up before timeout happens for an exsiting dataset"
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True, create_links=True,
            create_remote_kv_infra=True, create_dataset_objs=True,
            for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True),
            create_datasets=True, connect_link=True, wait_for_ingestion=True,
            rebalance_util=False, username=None)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        RemoteUtilHelper.enable_firewall(
            server=to_cluster.master, action_on_packet="DROP",
            block_ips=[self.analytics_cluster.master.ip], all_interface=True)

        # Wait for link to get disconnected
        retry = 0
        while retry < 30:
            retry += 1
            status, content, response = self.cbas_util.fetch_bucket_state_on_cbas(
                self.analytics_cluster)
            if status and json.loads(content)["buckets"][0][
                "state"] != "connected":
                break
            self.sleep(10)

        self.load_data_into_buckets(to_cluster)

        self.cluster_util.stop_firewall_on_node(to_cluster, to_cluster.master)

        # Allow ingestion to complete
        if not self.cbas_util.wait_for_ingestion_complete(
            self.analytics_cluster, dataset.full_name, dataset.num_of_items):
            self.fail("Data Ingestion did not resume")

    def test_reconnecting_link_after_timeout_due_to_network_failure(self):
        """
        This testcase deals with the scenario, when network comes back up after timeout happens.
        The link connection should fail after the timeout happens. But a subsequent link connection
        request once the network is up should succeed.
        """
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True, create_links=True,
            create_remote_kv_infra=True, create_dataset_objs=True,
            for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True),
            create_datasets=True, connect_link=False, wait_for_ingestion=False,
            rebalance_util=False, username=None)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        RemoteUtilHelper.enable_firewall(
            server=to_cluster.master, action_on_packet="DROP",
            block_ips=[self.analytics_cluster.master.ip], all_interface=True)

        # Wait for connect link to timeout. It take approx 10 mins to timeout.
        if not self.cbas_util.connect_link(
            self.analytics_cluster, dataset.link_name,
            username=self.analytics_username, validate_error_msg=True,
            expected_error="Connect link failed", timeout=900, analytics_timeout=900):
            self.fail("Expected a different error while connecting link")

        self.cluster_util.stop_firewall_on_node(to_cluster, to_cluster.master)
        if not self.cbas_util.connect_link(
            self.analytics_cluster, dataset.link_name,
            username=self.analytics_username):
            self.fail("Link connection failed")

        if not self.cbas_util.wait_for_ingestion_complete(
            self.analytics_cluster, dataset.full_name, dataset.num_of_items):
            self.fail("Data Ingestion did not resume")

    def test_dataset_behaviour_on_remote_bucket_deletion(self):
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True, create_links=True,
            create_remote_kv_infra=True, create_dataset_objs=True,
            for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True),
            create_datasets=True, connect_link=True, wait_for_ingestion=True,
            rebalance_util=False, username=None)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        if not self.bucket_util.delete_bucket(
            cluster=to_cluster, bucket=dataset.kv_bucket):
            self.fail("Deletion of bucket failed")

        # Wait for link to get disconnected
        retry = 0
        while retry < 30:
            retry += 1
            status, content, response = self.cbas_util.fetch_bucket_state_on_cbas(
                self.analytics_cluster)
            if status and json.loads(content)["buckets"][0][
                "state"] != "connected":
                break
            self.sleep(10)

        if not self.cbas_util.wait_for_ingestion_complete(
            self.analytics_cluster, dataset.full_name, 0):
            self.fail("Data Ingestion did not resume")

    def test_dataset_behaviour_on_remote_bucket_deletion_and_recreation(self):
        """
        This testcase deals with the scenario when remote bucket is deleted and another bucket with same name
        is recreated on remote cluster.
        """
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True, create_links=True,
            create_remote_kv_infra=True, create_dataset_objs=True,
            for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True),
            create_datasets=True, connect_link=True, wait_for_ingestion=True,
            rebalance_util=False, username=None)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        if not self.bucket_util.delete_bucket(
            cluster=to_cluster, bucket=dataset.kv_bucket):
            self.fail("Deletion of bucket failed")

        # Wait for link to get disconnected
        retry = 0
        while retry < 30:
            retry += 1
            status, content, response = self.cbas_util.fetch_bucket_state_on_cbas(
                self.analytics_cluster)
            if status and json.loads(content)["buckets"][0][
                "state"] != "connected":
                break
            self.sleep(10)

        self.log.info("Recreating bucket, scopes and collections on remote cluster")
        self.collectionSetUp(to_cluster)

        with_force_flag = self.input.param("with_force_flag", False)

        if with_force_flag:
            validate_err_msg = False
            expected_error_msg = None
        else:
            validate_err_msg = True
            expected_error_msg = "Bucket UUID has changed"

        if not self.cbas_util.connect_link(
            self.analytics_cluster, dataset.link_name,
            validate_error_msg=validate_err_msg, with_force=with_force_flag,
            username=self.analytics_username, expected_error=expected_error_msg):
            self.fail("Error while connecting link")
        if with_force_flag and not self.cbas_util.validate_cbas_dataset_items_count(
            self.analytics_cluster, dataset.full_name, dataset.num_of_items):
            self.fail(
                "Mismatch between number of documents in dataset and remote bucket")

    def test_data_ingestion_for_data_updation_in_remote_cluster(self):
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True, create_links=True,
            create_remote_kv_infra=True, create_dataset_objs=True,
            for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True),
            create_datasets=True, connect_link=True, wait_for_ingestion=True,
            rebalance_util=False, username=None)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
                self.doc_spec_name)
        mutation_num = 1
        doc_loading_spec["doc_crud"][
            MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 100
        self.load_data_into_buckets(
            to_cluster, doc_loading_spec=doc_loading_spec, async_load=False,
            validate_task=True, mutation_num=mutation_num)

        self.cbas_util.refresh_dataset_item_count(self.bucket_util)
        if not self.cbas_util.wait_for_ingestion_complete(
            self.analytics_cluster, dataset.full_name, dataset.num_of_items):
            self.fail("Data Ingestion did not complete")

        query_statement = "select count(*) from {0} where mutated>0;"
        count_n1ql = to_cluster.rest.query_tool(
            query_statement.format(dataset.full_kv_entity_name))['results'][0][
            '$1']

        if not self.cbas_util.validate_cbas_dataset_items_count(
            self.analytics_cluster, dataset.full_name, dataset.num_of_items,
            expected_mutated_count=count_n1ql,
            num_tries=12, timeout=300, analytics_timeout=300):
            self.fail("Mutated record count in KV and CBAS does not match")

    def test_data_ingestion_for_data_deletion_in_remote_cluster(self):
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True, create_links=True,
            create_remote_kv_infra=True, create_dataset_objs=True,
            for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True),
            create_datasets=True, connect_link=True, wait_for_ingestion=True,
            rebalance_util=False, username=None)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
                self.doc_spec_name)
        doc_loading_spec["doc_crud"][
            MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 100
        self.load_data_into_buckets(
            to_cluster, doc_loading_spec=doc_loading_spec, async_load=False,
            validate_task=True)

        self.cbas_util.refresh_dataset_item_count(self.bucket_util)
        if not self.cbas_util.wait_for_ingestion_complete(
            self.analytics_cluster, dataset.full_name, dataset.num_of_items):
            self.fail("Data Ingestion did not complete")

    def test_analytics_cluster_while_rebalancing_remote_cluster(self):
        '''
        1. We have 2 clusters, local and remote, each with 2 nodes.
        2. Swap / rebalance-in / rebalance-out a KV node on remote cluster.

        Data mutation is happening on remote cluster while local cluster is rebalancing.
        '''

        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True, create_links=True,
            create_remote_kv_infra=True, create_dataset_objs=True,
            for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True),
            create_datasets=True, connect_link=True, wait_for_ingestion=True,
            rebalance_util=True, username=None)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
            self.doc_spec_name)
        doc_loading_task = self.rebalance_util.data_load_collection(
            to_cluster, doc_loading_spec, True, async_load=True,
            skip_read_success_results=True, percentage_per_collection=100,
            durability_level=None)

        run_query = self.input.param("run_query", False)
        if run_query:
            self.log.info("Run concurrent queries to simulate busy system")
            statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(
                dataset.full_name)
            handles = self.cbas_util._run_concurrent_queries(
                self.analytics_cluster, statement, "async",
                self.input.param("num_queries", 0), wait_for_execution=False)

        if self.input.param("rebalance_type", "swap"):
            kv_nodes_in = 1
            kv_nodes_out = 1
        elif self.input.param("rebalance_type", "rebalance-in"):
            kv_nodes_in = 1
            kv_nodes_out = 0
        elif self.input.param("rebalance_type", "rebalance-out"):
            kv_nodes_in = 0
            kv_nodes_out = 1

        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            to_cluster, kv_nodes_in=kv_nodes_in, kv_nodes_out=kv_nodes_out,
            cbas_nodes_in=0, cbas_nodes_out=0,
            available_servers=self.available_servers, exclude_nodes=[])

        if not self.rebalance_util.wait_for_rebalance_task_to_complete(rebalance_task):
            self.fali("{0} on remote cluster failed".format(
                self.input.param("rebalance_type", "swap").upper()))

        self.log.info("Get KV ops result")
        if not self.rebalance_util.wait_for_data_load_to_complete(doc_loading_task, False):
            self.fail("Error while loading docs in remote cluster while rebalancing")

        if run_query:
            self.log.info("Log concurrent query status")
            self.cbas_util.log_concurrent_query_outcome(
                self.analytics_cluster, self.analytics_cluster.master, handles)

        self.cbas_util.refresh_dataset_item_count(self.bucket_util)

        if not self.cbas_util.validate_cbas_dataset_items_count(
            self.analytics_cluster, dataset.full_name, dataset.num_of_items):
            self.fail(
                "Number of items in dataset do not match number of items in bucket")

    def test_analytics_cluster_while_rebalancing(self):
        '''
        1. We have 2 clusters, local and remote, each with 2 nodes.
        2. Swap / rebalance-in / rebalance-out a CBAS node on analytics cluster.

        Data mutation is happening on remote cluster while local cluster is rebalancing.
        '''
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True, create_links=True,
            create_remote_kv_infra=True, create_dataset_objs=True,
            for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True),
            create_datasets=True, connect_link=True, wait_for_ingestion=True,
            rebalance_util=True, username=None)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
            self.doc_spec_name)
        doc_loading_task = self.rebalance_util.data_load_collection(
            to_cluster, doc_loading_spec, True, async_load=True,
            skip_read_success_results=True, percentage_per_collection=100,
            durability_level=None)

        run_query = self.input.param("run_query", False)
        if run_query:
            self.log.info("Run concurrent queries to simulate busy system")
            statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(
                dataset.full_name)
            handles = self.cbas_util._run_concurrent_queries(
                self.analytics_cluster, statement, "async",
                self.input.param("num_queries", 0), wait_for_execution=False)

        if self.input.param("rebalance_type", "swap"):
            cbas_nodes_in = 1
            cbas_nodes_out = 1
        elif self.input.param("rebalance_type", "rebalance-in"):
            cbas_nodes_in = 1
            cbas_nodes_out = 0
        elif self.input.param("rebalance_type", "rebalance-out"):
            cbas_nodes_in = 0
            cbas_nodes_out = 1

        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.analytics_cluster, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=cbas_nodes_in, cbas_nodes_out=cbas_nodes_out,
            available_servers=self.available_servers,
            exclude_nodes=[self.analytics_cluster.cbas_cc_node])

        if not self.rebalance_util.wait_for_rebalance_task_to_complete(rebalance_task):
            self.fali("{0} on remote cluster failed".format(
                self.input.param("rebalance_type", "swap").upper()))

        self.log.info("Get KV ops result")
        if not self.rebalance_util.wait_for_data_load_to_complete(doc_loading_task, False):
            self.fail("Error while loading docs in remote cluster while rebalancing")

        if run_query:
            self.log.info("Log concurrent query status")
            self.cbas_util.log_concurrent_query_outcome(
                self.analytics_cluster, self.analytics_cluster.master, handles)

        self.cbas_util.refresh_dataset_item_count(self.bucket_util)

        if not self.cbas_util.validate_cbas_dataset_items_count(
            self.analytics_cluster, dataset.full_name, dataset.num_of_items):
            self.fail(
                "Number of items in dataset do not match number of items in bucket")

    def test_fail_over_cbas_node_followed_by_rebalance_out_or_add_back(self):
        '''
        1. We have 2 clusters, local and remote, each with 2 nodes.
        2. Swap / rebalance-in / rebalance-out a CBAS node on analytics cluster.

        Data mutation is happening on remote cluster while local cluster is rebalancing.
        '''
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True, create_links=True,
            create_remote_kv_infra=True, create_dataset_objs=True,
            for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True),
            create_datasets=True, connect_link=True, wait_for_ingestion=True,
            rebalance_util=True, username=None)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
            self.doc_spec_name)
        doc_loading_task = self.rebalance_util.data_load_collection(
            to_cluster, doc_loading_spec, True, async_load=True,
            skip_read_success_results=True, percentage_per_collection=100,
            durability_level=None)

        run_query = self.input.param("run_query", False)
        if run_query:
            self.log.info("Run concurrent queries to simulate busy system")
            statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(
                dataset.full_name)
            handles = self.cbas_util._run_concurrent_queries(
                self.analytics_cluster, statement, "async",
                self.input.param("num_queries", 0), wait_for_execution=False)

        self.available_servers = self.rebalance_util.failover(
            self.analytics_cluster, failover_type="Hard",
            action=self.input.param("action_on_failover", "FullRecovery"),
            service_type="cbas", timeout=7200,
            available_servers=self.available_servers,
            exclude_nodes=[])

        self.log.info("Get KV ops result")
        if not self.rebalance_util.wait_for_data_load_to_complete(doc_loading_task, False):
            self.fail("Error while loading docs in remote cluster while rebalancing")

        if run_query:
            self.log.info("Log concurrent query status")
            self.cbas_util.log_concurrent_query_outcome(
                self.analytics_cluster, self.analytics_cluster.master, handles)

        self.cbas_util.refresh_dataset_item_count(self.bucket_util)

        if not self.cbas_util.validate_cbas_dataset_items_count(
            self.analytics_cluster, dataset.full_name, dataset.num_of_items):
            self.fail(
                "Number of items in dataset do not match number of items in bucket")

    """def test_scan_consistency_with_kv_mutations(self):
        
        to_cluster = self.setup_datasets(wait_for_ingestion=True)
        
        self.log.info("Perform async doc operations on KV")
        
        kv_task = self.perform_doc_ops_in_cb_buckets(to_cluster, self.sample_bucket, "create", key=self.key, end_key=self.num_items * 4, 
                                           batch_size=5000)
        
        self.log.info('Validate count')
        query = 'select count(*) from %s' % self.cbas_dataset_name
        dataset_count=0
        import time
        start_time = time.time()
        output_with_scan = []
        output_without_scan = []
        while time.time() < start_time + 120:
            try:
                response_with_scan, _, _, results_with_scan, _ = self.analytics_cluster.cbas_util.execute_statement_on_cbas_util(query, 
                                                                                                                            scan_consistency="request_plus", 
                                                                                                                            scan_wait="1m")
                self.assertEqual(response_with_scan, "success", "Query failed...")
                output_with_scan.append(results_with_scan[0]['$1'])
                
                response_without_scan, _, _, results_without_scan, _ = self.analytics_cluster.cbas_util.execute_statement_on_cbas_util(query, 
                                                                                                                                  scan_consistency='not_bounded')
                self.assertEqual(response_without_scan, "success", "Query failed...")
                output_without_scan.append(results_without_scan[0]['$1'])
                
                if results_without_scan[0]['$1'] == self.num_items * 4:
                    break
            except Exception as e:
                self.log.info('Try again neglect failures...')
        
        self.log.info("Get KV ops result")
        kv_task.get_result()
        
        self.log.info('Compare the output result length of count query with scan and with scan parameters')
        self.assertTrue(len(set(output_with_scan)) < len(set(output_without_scan)), msg='Select query with scan consistency must take fewer results')
        cbas_datasets = sorted(list(set(output_with_scan)))
        count_n1ql = self.rest.query_tool('select count(*) from %s' % self.cb_bucket_name)['results'][0]['$1']
        self.assertEqual(cbas_datasets[len(cbas_datasets)-1], count_n1ql, msg='KV-CBAS count mismatch. Actual %s, expected %s' % (dataset_count, count_n1ql))"""

    def test_create_query_drop_remote_dataset(self):
        """
        This testcase validates creation of dataset on
        remote link, connection of remote link, validation of data ingestion into dataset,
        dropping of dataset, disconnecting of remote link and dropping of remote link
        :testparam encryption:
        :testparam link_cardinality
        :testparam dataset_cardinality
        :testparam bucket_cardinality
        :testparam error
        :testparam invalid_kv_collection
        :testparam invalid_kv_scope
        :testparam invalid_dataverse
        :testparam name_length
        :testparam no_dataset_name
        :testparam remove_default_collection
        :testparam connect_invalid_link
        :testparam validate_link_error
        :testparam dataset_creation_method
        :testparam validate_error
        """
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_links=True, create_remote_kv_infra=True,
            create_dataset_objs=True, for_all_kv_entities=False,
            same_dv_for_link_and_dataset=self.input.param(
                "same_dv_for_link_and_dataset", True), create_datasets=False,
            connect_link=False, wait_for_ingestion=False, rebalance_util=False)

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster.rbac_util,
                                    rbac_users_created)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        # Negative scenario
        if self.input.param('error', None):
            error_msg = self.input.param('error', None)
        else:
            error_msg = None

        original_link_name = ""

        if self.input.param('invalid_kv_collection', False):
            dataset_obj.full_kv_entity_name = dataset_obj.get_fully_qualified_kv_entity_name(
                2) + "invalid"
            error_msg = error_msg.format(CBASHelper.unformat_name(
                dataset_obj.full_kv_entity_name))
        elif self.input.param('invalid_kv_scope', False):
            dataset_obj.full_kv_entity_name = dataset_obj.kv_bucket.name + "invalid" + dataset_obj.kv_collection.name
            error_msg = error_msg.format(CBASHelper.unformat_name(
                dataset_obj.full_kv_entity_name))
        elif self.input.param('invalid_dataverse', False):
            dataset_obj.full_name = "invalid" + dataset_obj.name
            error_msg = error_msg.format("invalid")
        elif self.input.param('name_length', 0):
            dataset_obj.full_name = dataset_obj.dataverse_name + self.cbas_util.generate_name(
                name_cardinality=1,
                max_length=self.input.param('name_length', 0),
                fixed_length=True, name_key=None)
        elif self.input.param('no_dataset_name', False):
            dataset_obj.name = ''
        elif self.input.param('remove_default_collection', False):
            error_msg = error_msg.format(
                CBASHelper.unformat_name(
                    dataset_obj.get_fully_qualified_kv_entity_name(2),
                    "_default"))
        elif self.input.param("connect_invalid_link", False):
            original_link_name = dataset_obj.link_name
            dataset_obj.link_name = "invalid"
            error_msg = "Link {0} does not exist".format(dataset_obj.link_name)
        # Negative scenario ends

        self.log.info("Creating dataset on link to remote cluster")
        if not self.cbas_util.create_dataset(
                self.analytics_cluster, dataset_obj.name,
                kv_entity=dataset_obj.full_kv_entity_name,
                dataverse_name=dataset_obj.dataverse_name,
                link_name=dataset_obj.link_name,
                validate_error_msg=self.input.param('validate_error', False),
                expected_error=error_msg,
                analytics_collection=self.use_new_ddl_format):
            raise Exception("Error while creating dataset")

        if self.input.param('validate_error', False):
            dataset_created = False
        else:
            dataset_created = True

        self.log.info("Connecting remote link")
        if not self.cbas_util.connect_link(
            self.analytics_cluster, dataset_obj.link_name,
            validate_error_msg=self.input.param("validate_link_error", False),
            with_force=False, username=None, password=None,
            expected_error=error_msg, expected_error_code=None, timeout=120,
            analytics_timeout=120):
            self.fail("Error while connecting the link")

        if not self.input.param("connect_invalid_link",
                                False) and dataset_created:
            if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.analytics_cluster, self.bucket_util):
                self.fail("Expected data does not match actual data")

        if dataset_created:
            self.log.info("Dropping dataset created on link to remote cluster")
            if not self.cbas_util.drop_dataset(
            self.analytics_cluster, dataset_obj.full_name):
                self.fail("Error while creating dataset")

        if not self.input.param("connect_invalid_link", False):
            self.log.info("Disconnecting remote link")
            if not self.cbas_util.disconnect_link(
                self.analytics_cluster, dataset_obj.link_name):
                self.fail("Error while disconnecting the link")

        if original_link_name:
            dataset_obj.link_name = original_link_name

        self.log.info(
            "Dropping remote link with {0} encryption".format(self.encryption))
        if not self.cbas_util.drop_link(
            self.analytics_cluster, dataset_obj.link_name):
            self.fail("Dropping remote link with encryption {0} failed".format(
                self.encryption))

        self.log.info("Test Passed")

        """
        1. Setup cluster, Create a few remote datasets in analytics and Make sure the ingestion happens and is continous
        2. Disable autofailover on remote cluster
        3. Disable autofailover on analytics cluster

        4. Now enable n2n and encrytionLevel = control. Enable client cert as well (x509 certs) on remote cluster.
        5. Now create new remote dataset/delete and edit, ingestion should continue.
        6. Make sure older data continue to ingest.
        7. rebalance in/out for KV node on remote cluster and analytics nodes on local cluster.
        8. Make sure older data continue to ingest.

        9. Now enable n2n and encrytionLevel = control. Enable client cert as well (x509 certs) on analytics cluster.
        10. Now create new remote dataset/delete and edit, ingestion should continue.
        11. Make sure older data continue to ingest.
        12. rebalance in/out for KV node on remote cluster and analytics nodes on local cluster.
        13. Make sure older data continue to ingest.

        14. Now change the encryptionLevel = all on remote cluster.
        15. Now create new remote dataset/delete and edit, ingestion should continue.
        16. Make sure older data continue to ingest.
        17. rebalance in/out for KV node on remote cluster and analytics nodes on local cluster.
        18. Make sure older data continue to ingest.

        19. Now change the encryptionLevel = all on local cluster.
        20. Now create new remote dataset/delete and edit, ingestion should continue.
        21. Make sure older data continue to ingest.
        22. rebalance in/out for KV node on remote cluster and analytics nodes on local cluster.
        23. Make sure older data continue to ingest.

        24. Now enable n2n and encrytionLevel = control on remote cluster.
        25. Now create new remote dataset/delete and edit, ingestion should continue.
        26. Make sure older data continue to ingest.
        27. rebalance in/out for KV node on remote cluster and analytics nodes on local cluster.
        28. Make sure older data continue to ingest.

        29. Now enable n2n and encrytionLevel = control. Enable client cert as well (x509 certs) on analytics cluster.
        30. Now create new remote dataset/delete and edit, ingestion should continue.
        31. Make sure older data continue to ingest.
        32. rebalance in/out for KV node on remote cluster and analytics nodes on local cluster.
        33. Make sure older data continue to ingest.

        34. Now disable n2n and client cert authentication on remote cluster.
        35. Now create new remote dataset/delete and edit, ingestion should continue.
        36. Make sure older data continue to ingest.
        37. rebalance in/out for KV node on remote cluster and analytics nodes on local cluster.
        38. Make sure older data continue to ingest.

        39. Now disable n2n and client cert authentication on analytics cluster.
        40. Now create new remote dataset/delete and edit, ingestion should continue.
        41. Make sure older data continue to ingest.
        42. rebalance in/out for KV node on remote cluster and analytics nodes on local cluster.
        43. Make sure older data continue to ingest.

        44. Now enable n2n and encrytionLevel = all on remote cluster.
        45. Now create new remote dataset/delete and edit, ingestion should continue.
        46. Make sure older data continue to ingest.
        47. rebalance in/out for KV node on remote cluster and analytics nodes on local cluster.
        48. Make sure older data continue to ingest.

        49. Now enable n2n and encrytionLevel = all on analytics cluster.
        50. Now create new remote dataset/delete and edit, ingestion should continue.
        51. Make sure older data continue to ingest.
        52. rebalance in/out for KV node on remote cluster and analytics nodes on local cluster.
        53. Make sure older data continue to ingest.

        54. Now client cert auth on remote cluster.
        55. Now create new remote dataset/delete and edit, ingestion should continue.
        56. Make sure older data continue to ingest.
        57. rebalance in/out for KV node on remote cluster and analytics nodes on local cluster.
        58. Make sure older data continue to ingest.

        59. Now client cert auth on analytics cluster.
        60. Now create new remote dataset/delete and edit, ingestion should continue.
        61. Make sure older data continue to ingest.
        62. rebalance in/out for KV node on remote cluster and analytics nodes on local cluster.
        63. Make sure older data continue to ingest.
        """

    def test_cbas_with_n2n_encryption_and_client_cert_auth(self):

        def load_data(cluster):
            doc_loading_spec = \
                self.bucket_util.get_crud_template_from_package("initial_load")
            doc_loading_spec["doc_crud"][
                "create_percentage_per_collection"] = 25
            self.load_data_into_buckets(
                cluster, doc_loading_spec=doc_loading_spec,
                async_load=False, validate_task=True, mutation_num=0)

        def create_dataset():
            dataset_obj = self.cbas_util.create_dataset_obj(
                self.analytics_cluster, self.bucket_util,
                dataset_cardinality=self.input.param("dataset_cardinality", 1),
                bucket_cardinality=self.input.param("bucket_cardinality", 1),
                enabled_from_KV=False,for_all_kv_entities=False,
                remote_dataset=True, link=None,
                same_dv_for_link_and_dataset=False,
                name_length=30, fixed_length=False, exclude_bucket=[],
                exclude_scope=[], exclude_collection=[], no_of_objs=1)[0]

            if not self.cbas_util.create_dataset(
                    self.analytics_cluster, dataset_obj.name,
                    kv_entity=dataset_obj.full_kv_entity_name,
                    dataverse_name=dataset_obj.dataverse_name,
                    link_name=dataset_obj.link_name):
                self.fail("Error while creating dataset {0}".format(
                    dataset_obj.full_name))

        def perform_rebalance(remoteIN=True, remoteOUT=False,
                              analyticsIN=True, analyticsOUT=False):
            rebalance_tasks = list()
            if remoteIN or remoteOUT:
                kv_nodes_in = 0
                kv_nodes_out = 0
                if remoteIN:
                    kv_nodes_in = 1
                if remoteOUT:
                    kv_nodes_out = 1
                rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                    to_cluster, kv_nodes_in=kv_nodes_in,
                    kv_nodes_out=kv_nodes_out, cbas_nodes_in=0,
                    cbas_nodes_out=0, available_servers=self.available_servers,
                    exclude_nodes=[])
                rebalance_tasks.append(rebalance_task)
            if analyticsIN or analyticsOUT:
                cbas_nodes_in = 0
                cbas_nodes_out = 0
                if analyticsIN:
                    cbas_nodes_in = 1
                if analyticsOUT:
                    cbas_nodes_out = 1
                rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                    self.analytics_cluster, kv_nodes_in=0, kv_nodes_out=0,
                    cbas_nodes_in=cbas_nodes_in, cbas_nodes_out=cbas_nodes_out,
                    available_servers=self.available_servers, exclude_nodes=[])
                rebalance_tasks.append(rebalance_task)

            for rebalance_tsk in rebalance_tasks:
                if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                        rebalance_tsk):
                    return False
            return True

        step_count = 1
        do_rebalance = self.input.param("do_rebalance", False)
        self.log.info("Step {0}: Setting up KV and CBAS infra".format(
            step_count))
        step_count += 1
        to_cluster = self.setup_for_test(
            to_cluster=None, encryption=self.encryption, hostname=None,
            setup_cert=self.setup_cert, create_link_objs=True,
            create_links=True, create_remote_kv_infra=True,
            create_dataset_objs=True, for_all_kv_entities=False,
            same_dv_for_link_and_dataset=False, create_datasets=True,
            connect_link=True, wait_for_ingestion=True, rebalance_util=True)

        self.log.info("Step {0}: Disabling Auto-Failover on remote "
                      "cluster".format(step_count))
        step_count += 1
        if not to_cluster.rest.update_autofailover_settings(False, 120, False):
            self.fail("Disabling Auto-Failover on remote cluster failed")

        self.log.info("Step {0}: Disabling Auto-Failover on analytics "
                      "cluster".format(step_count))
        step_count += 1
        if not self.analytics_cluster.rest.update_autofailover_settings(
                False, 120, False):
            self.fail("Disabling Auto-Failover on analytics cluster failed")

        self.log.info("Step {0}: Setting node to node encryption level to "
                      "control on remote cluster".format(step_count))
        step_count += 1
        self.security_util.set_n2n_encryption_level_on_nodes(
            to_cluster.nodes_in_cluster, level="control")

        self.log.info("Step {0}: Setting up certificates on remote "
                      "cluster".format(step_count))
        step_count += 1
        self.setup_certs(to_cluster)

        self.log.info("Step (0): Loading more docs on remote cluster".format(
            step_count))
        step_count += 1
        load_data(to_cluster)

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        create_dataset()

        if do_rebalance:
            self.log.info("Step {0}: Rebalancing-IN KV node on remote "
                          "cluster and CBAS node on analytics cluster".format(
                step_count))
            step_count += 1
            if not perform_rebalance(
                    remoteIN=True, remoteOUT=False, analyticsIN=True,
                    analyticsOUT=False):
                self.fali("Rebalancing-IN KV node on remote cluster and "
                          "CBAS node on analytics cluster Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(
                self.analytics_cluster, self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Setting node to node encryption level to "
                      "control on analytics cluster".format(step_count))
        step_count += 1
        self.security_util.set_n2n_encryption_level_on_nodes(
            self.analytics_cluster.nodes_in_cluster, level="control")
        if not self.cbas_util.wait_for_cbas_to_recover(
                self.analytics_cluster, 300):
            self.fail("Analytics service Failed to recover")

        self.log.info("Step {0}: Setting up certificates on analytics "
                      "cluster".format(step_count))
        step_count += 1
        self.setup_certs(self.analytics_cluster)

        self.log.info("Step (0): Loading more docs on remote cluster".format(
            step_count))
        step_count += 1
        load_data(to_cluster)

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        create_dataset()

        if do_rebalance:
            self.log.info("Step {0}: Rebalancing-OUT KV node on remote "
                          "cluster and CBAS node on analytics cluster".format(
                step_count))
            step_count += 1
            if not perform_rebalance(
                    remoteIN=False, remoteOUT=True, analyticsIN=False,
                    analyticsOUT=True):
                self.fali("Rebalancing-OUT KV node on remote cluster and "
                          "CBAS node on analytics cluster Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(
                self.analytics_cluster, self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Setting node to node encryption level to "
                      "all on remote cluster".format(step_count))
        step_count += 1
        self.security_util.set_n2n_encryption_level_on_nodes(
            to_cluster.nodes_in_cluster, level="all")

        self.log.info("Step (0): Loading more docs on remote cluster".format(
            step_count))
        step_count += 1
        load_data(to_cluster)

        self.log.info("Step {0}: Dropping dataset".format(step_count))
        step_count += 1
        dataset_to_drop = random.choice(self.cbas_util.list_all_dataset_objs())
        if not self.cbas_util.drop_dataset(
            self.analytics_cluster, dataset_to_drop.full_name):
            self.fail("Error while dropping dataset {0}".format(
                dataset_to_drop.full_name))

        if do_rebalance:
            self.log.info("Step {0}: Rebalancing-IN KV node on remote "
                          "cluster and CBAS node on analytics cluster".format(
                step_count))
            step_count += 1
            if not perform_rebalance(
                    remoteIN=True, remoteOUT=False, analyticsIN=True,
                    analyticsOUT=False):
                self.fali("Rebalancing-IN KV node on remote cluster and "
                          "CBAS node on analytics cluster Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(
                self.analytics_cluster, self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Setting node to node encryption level to "
                      "all on analytics cluster".format(step_count))
        step_count += 1
        self.security_util.set_n2n_encryption_level_on_nodes(
            self.analytics_cluster.nodes_in_cluster, level="all")
        if not self.cbas_util.wait_for_cbas_to_recover(
                self.analytics_cluster, 300):
            self.fail("Analytics service Failed to recover")

        self.log.info("Step (0): Loading more docs on remote cluster".format(
            step_count))
        step_count += 1
        load_data(to_cluster)

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        create_dataset()

        if do_rebalance:
            self.log.info("Step {0}: Rebalancing-OUT KV node on remote "
                          "cluster and CBAS node on analytics cluster".format(
                step_count))
            step_count += 1
            if not perform_rebalance(
                    remoteIN=False, remoteOUT=True, analyticsIN=False,
                    analyticsOUT=True):
                self.fali("Rebalancing-OUT KV node on remote cluster and "
                          "CBAS node on analytics cluster Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(
                self.analytics_cluster, self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Setting node to node encryption level to "
                      "control on remote cluster".format(step_count))
        step_count += 1
        self.security_util.set_n2n_encryption_level_on_nodes(
            to_cluster.nodes_in_cluster, level="control")

        self.log.info("Step (0): Loading more docs on remote cluster".format(
            step_count))
        step_count += 1
        load_data(to_cluster)

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        create_dataset()

        if do_rebalance:
            self.log.info("Step {0}: Rebalancing-IN KV node on remote "
                          "cluster and CBAS node on analytics cluster".format(
                step_count))
            step_count += 1
            if not perform_rebalance(
                    remoteIN=True, remoteOUT=False, analyticsIN=True,
                    analyticsOUT=False):
                self.fali("Rebalancing-IN KV node on remote cluster and "
                          "CBAS node on analytics cluster Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(
                self.analytics_cluster, self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Setting node to node encryption level to "
                      "control on analytics cluster".format(step_count))
        step_count += 1
        self.security_util.set_n2n_encryption_level_on_nodes(
            self.analytics_cluster.nodes_in_cluster, level="control")
        if not self.cbas_util.wait_for_cbas_to_recover(
                self.analytics_cluster, 300):
            self.fail("Analytics service Failed to recover")

        self.log.info("Step (0): Loading more docs on remote cluster".format(
            step_count))
        step_count += 1
        load_data(to_cluster)

        self.log.info("Step {0}: Dropping dataset".format(step_count))
        step_count += 1
        dataset_to_drop = random.choice(self.cbas_util.list_all_dataset_objs())
        if not self.cbas_util.drop_dataset(
                self.analytics_cluster, dataset_to_drop.full_name):
            self.fail("Error while dropping dataset {0}".format(
                dataset_to_drop.full_name))

        if do_rebalance:
            self.log.info("Step {0}: Rebalancing-OUT KV node on remote "
                          "cluster and CBAS node on analytics cluster".format(
                step_count))
            step_count += 1
            if not perform_rebalance(
                    remoteIN=False, remoteOUT=True, analyticsIN=False,
                    analyticsOUT=True):
                self.fali("Rebalancing-OUT KV node on remote cluster and "
                          "CBAS node on analytics cluster Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(
                self.analytics_cluster, self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Disabling node to node encryption on remote "
                      "cluster".format(step_count))
        step_count += 1
        self.security_util.disable_n2n_encryption_cli_on_nodes(
            to_cluster.nodes_in_cluster)

        self.log.info("Step {0}: Disabling certs on remote cluster".format(step_count))
        step_count += 1
        self.security_util.teardown_x509_certs(to_cluster.nodes_in_cluster,
                                               to_cluster.CACERTFILEPATH)

        self.log.info("Step (0): Loading more docs on remote cluster".format(
            step_count))
        step_count += 1
        load_data(to_cluster)

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        create_dataset()

        if do_rebalance:
            self.log.info("Step {0}: Rebalancing-IN KV node on remote "
                          "cluster and CBAS node on analytics cluster".format(
                step_count))
            step_count += 1
            if not perform_rebalance(
                    remoteIN=True, remoteOUT=False, analyticsIN=True,
                    analyticsOUT=False):
                self.fali("Rebalancing-IN KV node on remote cluster and "
                          "CBAS node on analytics cluster Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(
                self.analytics_cluster, self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Disabling node to node encryption on "
                      "analytics cluster".format(step_count))
        step_count += 1
        self.security_util.disable_n2n_encryption_cli_on_nodes(
            self.analytics_cluster.nodes_in_cluster)

        if not self.cbas_util.wait_for_cbas_to_recover(
                self.analytics_cluster, 300):
            self.fail("Analytics service Failed to recover")

        self.log.info(
            "Step {0}: Disabling certs on analytics cluster".format(
                step_count))
        step_count += 1
        self.security_util.teardown_x509_certs(
            self.analytics_cluster.nodes_in_cluster,
            self.analytics_cluster.CACERTFILEPATH)

        self.log.info("Step (0): Loading more docs on remote cluster".format(
            step_count))
        step_count += 1
        load_data(to_cluster)

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        create_dataset()

        if do_rebalance:
            self.log.info("Step {0}: Rebalancing-OUT KV node on remote "
                          "cluster and CBAS node on analytics cluster".format(
                step_count))
            step_count += 1
            if not perform_rebalance(
                    remoteIN=False, remoteOUT=True, analyticsIN=False,
                    analyticsOUT=True):
                self.fali("Rebalancing-OUT KV node on remote cluster and "
                          "CBAS node on analytics cluster Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(
                self.analytics_cluster, self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Setting node to node encryption level to "
                      "all on remote cluster".format(step_count))
        step_count += 1
        self.security_util.set_n2n_encryption_level_on_nodes(
            to_cluster.nodes_in_cluster, level="all")

        self.log.info("Step (0): Loading more docs on remote cluster".format(
            step_count))
        step_count += 1
        load_data(to_cluster)

        self.log.info("Step {0}: Dropping dataset".format(step_count))
        step_count += 1
        dataset_to_drop = random.choice(self.cbas_util.list_all_dataset_objs())
        if not self.cbas_util.drop_dataset(
                self.analytics_cluster, dataset_to_drop.full_name):
            self.fail("Error while dropping dataset {0}".format(
                dataset_to_drop.full_name))

        if do_rebalance:
            self.log.info("Step {0}: Rebalancing-IN KV node on remote "
                          "cluster and CBAS node on analytics cluster".format(
                step_count))
            step_count += 1
            if not perform_rebalance(
                    remoteIN=True, remoteOUT=False, analyticsIN=True,
                    analyticsOUT=False):
                self.fali("Rebalancing-IN KV node on remote cluster and "
                          "CBAS node on analytics cluster Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(
                self.analytics_cluster, self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Setting node to node encryption level to "
                      "all on analytics cluster".format(step_count))
        step_count += 1
        self.security_util.set_n2n_encryption_level_on_nodes(
            self.analytics_cluster.nodes_in_cluster, level="all")
        if not self.cbas_util.wait_for_cbas_to_recover(
                self.analytics_cluster, 300):
            self.fail("Analytics service Failed to recover")

        self.log.info("Step (0): Loading more docs on remote cluster".format(
            step_count))
        step_count += 1
        load_data(to_cluster)

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        create_dataset()

        if do_rebalance:
            self.log.info("Step {0}: Rebalancing-OUT KV node on remote "
                          "cluster and CBAS node on analytics cluster".format(
                step_count))
            step_count += 1
            if not perform_rebalance(
                    remoteIN=False, remoteOUT=True, analyticsIN=False,
                    analyticsOUT=True):
                self.fali("Rebalancing-OUT KV node on remote cluster and "
                          "CBAS node on analytics cluster Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(
                self.analytics_cluster, self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Setting up certificates on remote "
                      "cluster".format(step_count))
        step_count += 1
        self.setup_certs(to_cluster)

        self.log.info("Step (0): Loading more docs on remote cluster".format(
            step_count))
        step_count += 1
        load_data(to_cluster)

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        create_dataset()

        if do_rebalance:
            self.log.info("Step {0}: Rebalancing-IN KV node on remote "
                          "cluster and CBAS node on analytics cluster".format(
                step_count))
            step_count += 1
            if not perform_rebalance(
                    remoteIN=True, remoteOUT=False, analyticsIN=True,
                    analyticsOUT=False):
                self.fali("Rebalancing-IN KV node on remote cluster and "
                          "CBAS node on analytics cluster Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(
                self.analytics_cluster, self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Setting up certificates on analytics "
                      "cluster".format(step_count))
        step_count += 1
        self.setup_certs(self.analytics_cluster)

        self.log.info("Step (0): Loading more docs on remote cluster".format(
            step_count))
        step_count += 1
        load_data(to_cluster)

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        create_dataset()

        if do_rebalance:
            self.log.info("Step {0}: Rebalancing-OUT KV node on remote "
                          "cluster and CBAS node on analytics cluster".format(
                step_count))
            step_count += 1
            if not perform_rebalance(
                    remoteIN=False, remoteOUT=True, analyticsIN=False,
                    analyticsOUT=True):
                self.fali("Rebalancing-OUT KV node on remote cluster and "
                          "CBAS node on analytics cluster Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(
                self.analytics_cluster, self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.security_util.teardown_x509_certs(to_cluster.nodes_in_cluster,
                                               to_cluster.CACERTFILEPATH)
        self.security_util.teardown_x509_certs(
            self.analytics_cluster.nodes_in_cluster,
            self.analytics_cluster.CACERTFILEPATH)

