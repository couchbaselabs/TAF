'''
Created on 13-May-2020

@author: umang.agrawal
'''
from TestInput import TestInputSingleton
from cbas.cbas_base import CBASBaseTest
import random, json, copy, datetime
from threading import Thread
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from rbac_utils.Rbac_ready_functions import RbacUtils
from security_utils.x509main import x509main
from shutil import copytree

cb_server_roles = ["admin", "analytics_admin", "analytics_reader", "cluster_admin",
                   "query_external_access", "query_system_catalog", "replication_admin",
                   "ro_admin", "security_admin", "analytics_manager[*]", "analytics_select[*]",
                   "bucket_admin[*]", "bucket_full_access[*]", "data_backup[*]", "data_dcp_reader[*]",
                   "data_monitoring[*]", "data_reader[*]", "data_writer[*]", "fts_admin[*]",
                   "fts_searcher[*]", "mobile_sync_gateway[*]", "query_delete[*]", "query_insert[*]",
                   "query_manage_index[*]", "query_select[*]", "query_update[*]", "replication_target[*]",
                   "views_admin[*]", "views_reader[*]"]
rbac_users_created = {}


class CBASExternalLinks(CBASBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket":False})
        super(CBASExternalLinks, self).setUp()
        self.setup_certs_status = False
        self.dataset_created = False
        self.link_created = False

        """
        Assumptions: 
        1) Only 1 Analytics cluster.
        2) No cbas node on any other cluster apart from analytics cluster.
        """
        self.to_clusters = list()
        self.link_info = dict()
        for cluster in self._cb_cluster:
            if hasattr(cluster,"cbas_node"):
                self.analytics_cluster = cluster
            else:
                self.to_clusters.append(cluster)
            cluster.rbac_util = RbacUtils(cluster.master)

        # Test parameters
        self.no_of_dataverse = int(self.input.test_params.get("dataverse", 0))
        self.no_of_link = int(self.input.test_params.get("link", 1))
        self.no_of_datasets = int(self.input.test_params.get("dataset", 0))

        # Create analytics admin user on Analytics cluster
        self.analytics_username = "admin_analytics"
        self.analytics_cluster.rbac_util._create_user_and_grant_role(self.analytics_username, "analytics_admin")
        rbac_users_created[self.analytics_username] = "analytics_admin"

        self.invalid_value = "invalid"
        self.invalid_ip = "127.0.0.99"

    def tearDown(self):
        """
        If certs were setup, then remove those certs and restore default certs.
        """
        if self.setup_certs_status:
            self.teardown_certs()

        if self.dataset_created:
            self.analytics_cluster.cbas_util.drop_dataset(cbas_dataset_name=self.cbas_dataset_name,
                                                     dataverse=self.link_info["dataverse"])

        if self.link_created:
            self.analytics_cluster.cbas_util.drop_link_on_cbas(link_name= "{0}.{1}".format(
                self.link_info["dataverse"],
                self.link_info["name"]))

        self.create_or_delete_users(self.analytics_cluster, delete=True)

        for cluster in self.to_clusters:
            cluster.cluster_util.stop_firewall_on_node(cluster.master)

        super(CBASExternalLinks, self).tearDown()

    def setup_certs(self):
        """
        Setup method for setting up root, node and client certs for all the clusters.
        """

        self._reset_original()
        self.ip_address = '172.16.1.174'
        SSLtype = "openssl"
        encryption_type = self.input.param('encryption_type',"")
        key_length=self.input.param("key_length",1024)

        #Input parameters for state, path, delimeters and prefixes
        self.client_cert_state = self.input.param("client_cert_state","enable")
        self.paths = self.input.param('paths',"subject.cn:san.dnsname:san.uri").split(":")
        self.prefixs = self.input.param('prefixs','www.cb-:us.:www.').split(":")
        self.delimeters = self.input.param('delimeter','.:.:.') .split(":")
        self.setup_once = self.input.param("setup_once",True)

        self.dns = self.input.param('dns',None)
        self.uri = self.input.param('uri',None)

        for cluster in self._cb_cluster:

            cluster.x509 = x509main(cluster.master)
            cluster.CACERTFILEPATH = x509main.CACERTFILEPATH.rstrip("/") + cluster.name + "/"
            cluster.root_ca_path = cluster.CACERTFILEPATH + x509main.CACERTFILE
            cluster.client_cert_pem = cluster.CACERTFILEPATH + self.ip_address + ".pem"
            cluster.client_cert_key = cluster.CACERTFILEPATH + self.ip_address + ".key"

            copy_servers = copy.deepcopy(cluster.servers)

            #Generate cert and pass on the client ip for cert generation
            if (self.dns is not None) or (self.uri is not None):
                cluster.x509._generate_cert(copy_servers,type=SSLtype,encryption=encryption_type,key_length=key_length,client_ip=self.ip_address,alt_names='non_default',dns=self.dns,uri=self.uri)
            else:
                cluster.x509._generate_cert(copy_servers,type=SSLtype,encryption=encryption_type,key_length=key_length,client_ip=self.ip_address)
            self.log.info(" Path is {0} - Prefixs - {1} -- Delimeters - {2}".format(self.paths, self.prefixs, self.delimeters))

            if (self.setup_once):
                cluster.x509.setup_master(self.client_cert_state, self.paths, self.prefixs, self.delimeters)
                cluster.x509.setup_cluster_nodes_ssl(cluster.servers)
            copytree(cluster.x509.CACERTFILEPATH, cluster.CACERTFILEPATH)
            self.log.info (" list of server {0}".format(copy_servers))
        self.setup_certs_status = True

    def teardown_certs(self):
        """
        Teardown method for removing all the certs created and setting all root cert on all the servers to Default.
        """
        self._reset_original()
        shell = RemoteMachineShellConnection(x509main.SLAVE_HOST)
        shell.execute_command("rm -rf " + x509main.CACERTFILEPATH)
        for cluster in self._cb_cluster:
            shell.execute_command("rm -rf " + cluster.CACERTFILEPATH)

    def _reset_original(self):
        """
        Reverting to original state - regenerating certificate and removing inbox folder
        """
        self.log.info ("Reverting to original state - regenerating certificate and removing inbox folder")
        tmp_path = "/tmp/abcd.pem"
        for servers in self.servers:
            cli_command = "ssl-manage"
            remote_client = RemoteMachineShellConnection(servers)
            options = "--regenerate-cert={0}".format(tmp_path)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options,
                                                                cluster_host=servers.ip, user="Administrator",
                                                                password="password")
            x509main(servers)._delete_inbox_folder()

    def get_link_property_dict(self, to_cluster, encryption="none"):
        """
        Creates a dict of all the properties required to create external link to remote
        couchbase cluster.
        :param to_cluster : remote cluster to which the link has to be created.
        """
        self.link_info = dict()
        self.link_info["dataverse"] = "Default"
        self.link_info["name"] = "newLink"
        self.link_info["type"] = "couchbase"
        self.link_info["hostname"] = to_cluster.master.ip
        self.link_info["username"] = to_cluster.master.rest_username
        self.link_info["password"] = to_cluster.master.rest_password
        self.link_info["encryption"] = encryption
        if encryption == "full":
            self.setup_certs()
            try:
                with open(to_cluster.root_ca_path, "r") as rca:
                    self.link_info["certificate"] = rca.read()
                with open(to_cluster.client_cert_pem, "r") as ccp:
                    self.link_info["clientCertificate"] = ccp.read()
                with open(to_cluster.client_cert_key, "r") as cck:
                    self.link_info["clientKey"] = cck.read()
                self.link_info["username"] = None
                self.link_info["password"] = None
            except Exception:
                self.link_info["certificate"] = None
                self.link_info["clientCertificate"] = None
                self.link_info["clientKey"] = None

    def create_or_delete_users(self, cluster, delete=False):
        """
        Creates all types of rbac users.
        """
        if delete:
            for user in rbac_users_created:
                try:
                    cluster.rbac_util._drop_user(user)
                    del(rbac_users_created[user])
                except:
                    pass
        else:
            for role in cb_server_roles:
                if "[*]" in role:
                    user = role.replace("[*]","")
                else:
                    user = role
                rbac_users_created[user] = role
                cluster.rbac_util._create_user_and_grant_role(user, role)


    def create_testcase_for_rbac_user(self, cluster, description):
        testcases = []
        for user in rbac_users_created:
            if user in ["admin", "analytics_admin", "cluster_admin"]:
                test_params = {
                    "description": description,
                    "validate_error_msg": False
                    }
            elif user in ["security_admin", "query_external_access", "query_system_catalog",
                          "replication_admin", "ro_admin", "bucket_full_access",
                          "replication_target", "mobile_sync_gateway", "data_reader", "data_writer",
                          "data_dcp_reader", "data_monitoring", "views_admin", "views_reader",
                          "query_delete", "query_insert", "query_manage_index", "query_select",
                          "query_update", "fts_admin", "fts_searcher", ]:
                test_params = {
                    "description": description,
                    "validate_error_msg": True,
                    "expected_error": "User must have permission",
                    }
            else:
                test_params = {
                    "description": description,
                    "validate_error_msg": True,
                    "expected_error": "Unauthorized user",
                    }
            test_params["username"] = user
            test_params["description"] = (description.replace("{0}",user))
            testcases.append(test_params)
        return testcases

    def setup_datasets(self, username=None, to_cluster=None, get_link_info=True,
                       connect_link=True, wait_for_ingestion=True, set_primary_index=True):

        if not username:
            username = self.analytics_username

        if not to_cluster:
            to_cluster = random.choice(self.to_clusters)

        if get_link_info:
            self.get_link_property_dict(to_cluster)


        if not self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = self.link_info, username=username):
            self.fail("link cration failed")
        self.link_created = True

        if not to_cluster.bucket_util.load_sample_bucket(self.sample_bucket):
            self.fail("Error while loading {0} bucket in remote cluster".format(self.sample_bucket.name))

        if not self.analytics_cluster.cbas_util.create_dataset_on_bucket(cbas_bucket_name= self.sample_bucket.name,
                                                                         cbas_dataset_name= self.cbas_dataset_name,
                                                                         dataverse=self.link_info["dataverse"],
                                                                         link_name="{0}.{1}".format(
                                                                             self.link_info["dataverse"],
                                                                             self.link_info["name"]),
                                                                         username=username):
            self.fail("Error while creating dataset")
        self.dataset_created = True

        if connect_link:
            if not self.analytics_cluster.cbas_util.connect_link("{0}.{1}".format(self.link_info["dataverse"],
                                                                             self.link_info["name"]),
                                                                             username=username):
                self.fail("Error while connecting link")
            if wait_for_ingestion:
                if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                                    self.sample_bucket.stats.expected_item_count,
                                                                                    300):
                    self.fail("Data Ingestion did not complete")
                if set_primary_index and not self.set_primary_index(to_cluster, self.sample_bucket.name):
                    self.fail("Creating Primary index on bucket {0} FAILED".format(self.sample_bucket.name))

        return to_cluster


    def set_primary_index(self, cluster, bucket_name):
        query = "CREATE PRIMARY INDEX ON `{0}`;".format(bucket_name)
        result = cluster.rest.query_tool(query)
        if result["status"] == "success" and not result["results"]:
            return True
        else:
            return False


    def perform_setup_and_add_new_docs(self, number_of_docs, wait_for_ingestion=True, username=None):
        to_cluster = self.setup_datasets(wait_for_ingestion=wait_for_ingestion, username=username)

        task = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=number_of_docs, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)
        result = self.task.jython_task_manager.get_task_result(task)

        if wait_for_ingestion:
            assert(self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                           self.sample_bucket.stats.expected_item_count + number_of_docs,
                                                                           300), "Data Ingestion did not complete")
        return to_cluster

    def test_create_external_link(self):
        self.setup_certs()
        # Check for link creation failure scenario
        if self.no_of_link == 1:
            to_cluster = random.choice(self.to_clusters)
            self.get_link_property_dict(to_cluster)

            # Create users with all RBAC roles.
            self.create_or_delete_users(self.analytics_cluster)

            testcases = [
                {
                    "description": "Create a link with a non-existent dataverse",
                    "dataverse": self.invalid_value,
                    "expected_error": "Cannot find dataverse with name {0}".format(self.invalid_value)
                },
                {
                    "description": "Create a link with an invalid hostname",
                    "hostname": self.invalid_ip,
                    "expected_error": "Invalid credentials for link Default.newLink"
                },
                {
                    "description": "Create a link with an invalid credentials",
                    "password": self.invalid_value,
                    "expected_error": "Invalid credentials for link {0}.{1}".format(self.link_info["dataverse"],
                                                                                    self.link_info["name"])
                },
                {
                    "description": "Create a link with an invalid encryption value",
                    "encryption": self.invalid_value,
                    "expected_error": "Invalid link specification"
                },
                {
                    "description": "Create a link with an invalid root certificate",
                    "encryption": "full",
                    "certificate": self.analytics_cluster.root_ca_path,
                    "expected_error": "failed to respond"
                },
                {
                    "description": "Create a link with an invalid client certificate",
                    "encryption": "full",
                    "clientCertificate": self.analytics_cluster.client_cert_pem,
                    "expected_error": "failed to respond"
                },
                {
                    "description": "Create a link with an invalid client key",
                    "encryption": "full",
                    "clientKey": self.analytics_cluster.client_cert_key,
                    "expected_error": "failed to respond"
                },
                {
                    "description": "Create a link with a name that already exists in the dataverse",
                    "recreate_link": True,
                    "validate_error_msg": False,
                    "expected_error": "Link {0}.{1} already exists".format(self.link_info["dataverse"],
                                                                           self.link_info["name"])
                },
                {
                    "description": "Create a link with a name of form Local*",
                    "name": "Local123",
                    "expected_error": 'Links starting with \"Local\" are reserved by the system'
                },
                {
                    "description": "Create a link when remote host is unreachable",
                    "stop_server": True,
                    "expected_error": 'Connection refused'
                }
                ]

            rbac_testcases = self.create_testcase_for_rbac_user(self.analytics_cluster,
                                                                "Create a link using (0) user")
            for testcase in rbac_testcases:
                if testcase["validate_error_msg"]:
                    testcase["expected_error"] = "Unauthorized user"

            testcases = testcases + rbac_testcases

            for testcase in testcases:
                self.log.info(testcase["description"])
                link_properties = copy.deepcopy(self.link_info)
                # Copying link property values for the testcase, to link_property dict, to create a new link.
                for key, value in testcase.iteritems():
                    if key in link_properties:
                        link_properties[key] = value

                if testcase.has_key("stop_server"):
                    to_cluster.cluster_util.stop_server(to_cluster.master)

                if not self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = link_properties,
                                                                                     validate_error_msg=testcase.get(
                                                                                         "validate_error_msg", True),
                                                                                     expected_error=testcase.get(
                                                                                         "expected_error", None),
                                                                                     username=testcase.get("username",
                                                                                                           self.analytics_username)):
                    self.fail("Error message is different than expected.")

                if testcase.has_key("stop_server"):
                    to_cluster.cluster_util.start_server(to_cluster.master)

                if testcase.has_key("recreate_link"):
                    testcase["validate_error_msg"] = True
                    if not self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = link_properties,
                                                                                         validate_error_msg=testcase.get(
                                                                                             "validate_error_msg", True),
                                                                                         expected_error=testcase.get(
                                                                                             "expected_error", None),
                                                                                         username=testcase.get("username",
                                                                                                               self.analytics_username)):
                        self.fail("Error message is different than expected.")
                    testcase["validate_error_msg"] = False

                if not testcase.get("validate_error_msg", True):
                    self.analytics_cluster.cbas_util.drop_link_on_cbas(link_name="{0}.{1}".format(link_properties["dataverse"],
                                                                                             link_properties["name"]),
                                                                                             username=self.analytics_username)
        else:
            self.dataverse_map = self.create_dataverse_link_map(self.analytics_cluster, self.no_of_dataverse,
                                                                self.no_of_link)
            # Check for multiple link creation scenario
            to_cluster = random.choice(self.to_clusters)
            ip_list = []
            for server in to_cluster.servers:
                ip_list.append(server.ip)

            try:
                for dataverse, links in self.dataverse_map.iteritems():
                    for link in links:
                        self.dataverse_map[dataverse][link]["dataverse"] = dataverse
                        self.dataverse_map[dataverse][link]["name"] = link
                        self.dataverse_map[dataverse][link]["type"] = "couchbase"
                        self.dataverse_map[dataverse][link]["hostname"] = random.choice(ip_list)
                        self.dataverse_map[dataverse][link]["encryption"] = random.choice(["none", "half", "full"])
                        self.dataverse_map[dataverse][link]["username"] = to_cluster.username
                        self.dataverse_map[dataverse][link]["password"] = to_cluster.password
                        if self.dataverse_map[dataverse][link]["encryption"] == "full":
                            with open(to_cluster.root_ca_path, "r") as rcp:
                                self.dataverse_map[dataverse][link]["certificate"] = rcp.read()
                            self.dataverse_map[dataverse][link]["use_only_certs"] = random.choice([True,False])
                            if self.dataverse_map[dataverse][link]["use_only_certs"]:
                                with open(to_cluster.client_cert_pem, "r") as ccp:
                                    self.dataverse_map[dataverse][link]["clientCertificate"] = ccp.read()
                                with open(to_cluster.client_cert_key, "r") as cck:
                                    self.dataverse_map[dataverse][link]["clientKey"] = cck.read()
                                self.dataverse_map[dataverse][link]["username"] = None
                                self.dataverse_map[dataverse][link]["password"] = None
                        if self.analytics_cluster.cbas_util.create_external_link_on_cbas(
                            link_properties = self.dataverse_map[dataverse][link],
                            username= self.analytics_username):
                            response = self.analytics_cluster.cbas_util.get_link_info(link_name=link,
                                                                                 dataverse=dataverse,
                                                                                 link_type="couchbase",
                                                                                 username=self.analytics_username)
                            if not (response[0]["bootstrapHostname"] == self.dataverse_map[dataverse][link]["hostname"]):
                                self.fail("Hostname does not match. Expected - {0}\nActual - {1}".format(self.dataverse_map[dataverse][link]["hostname"],
                                                                                                         response[0]["bootstrapHostname"]))
                            if not (response[0]["username"] == self.dataverse_map[dataverse][link]["username"]):
                                self.fail("Hostname does not match. Expected - {0}\nActual - {1}".format(self.dataverse_map[dataverse][link]["username"],
                                                                                                         response[0]["username"]))
                        else:
                            self.fail("Link creation failed.")
            except Exception as err:
                self.fail("Exception Occured - " + str(err))

    def test_list_external_links(self):
        to_cluster = random.choice(self.to_clusters)
        if self.input.test_params.get("encryption", "none") == "full":
            self.get_link_property_dict(to_cluster,encryption="full")
        else:
            self.get_link_property_dict(to_cluster)

        if self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = self.link_info,
                                                                    username=self.analytics_username):
            self.link_created = True
            # Create users with all RBAC roles.
            self.create_or_delete_users(self.analytics_cluster)

            if self.link_info["encryption"] == "full":
                testcases = [
                    {"expected_hits": 1,
                     "description": "Parameters Passed - None",
                        }
                    ]
            else:
                testcases = [
                    {"expected_hits": 1,
                     "description": "Parameters Passed - None",
                        },
                    {"dataverse": self.link_info["dataverse"],
                     "expected_hits": 1,
                     "description": "Parameters Passed - Dataverse",
                        },
                    {"dataverse": self.link_info["dataverse"],
                     "name": self.link_info["name"],
                     "expected_hits": 1,
                     "description": "Parameters Passed - Dataverse, Name",
                        },
                    {"type": "couchbase",
                     "expected_hits": 1,
                     "description": "Parameters Passed - Type",
                        },
                    {"name": self.link_info["name"],
                     "expected_hits": 0,
                     "description": "Parameters Passed - Name",
                     "validate_error_msg": True,
                     "expected_error": "You must specify 'dataverse' if 'name' is specified"
                        },
                    {"dataverse": self.link_info["dataverse"],
                     "name": self.link_info["name"],
                     "type": "couchbase",
                     "expected_hits": 1,
                     "description": "Parameters Passed - Dataverse, Name and Type",
                        }
                    ]
                rbac_testcases = self.create_testcase_for_rbac_user(self.analytics_cluster,
                                                                           "List external links using (0) user")
                for testcase in rbac_testcases:
                    testcase["dataverse"] = self.link_info["dataverse"]
                    testcase["name"] = self.link_info["name"]
                    testcase["type"] = "couchbase"
                    if testcase["username"] in ["analytics_manager"]:
                        testcase["validate_error_msg"] = False
                    if testcase["validate_error_msg"]:
                        testcase["expected_hits"] = 0
                        testcase["expected_error"] = "Unauthorized user"
                    else:
                        testcase["expected_hits"] = 1

                testcases = testcases + rbac_testcases

            for testcase in testcases:
                self.log.info(testcase["description"])
                response = self.analytics_cluster.cbas_util.get_link_info(link_name=testcase.get("name", None),
                                                                          dataverse=testcase.get("dataverse",None),
                                                                          link_type=testcase.get("type",None),
                                                                          validate_error_msg=testcase.get("validate_error_msg", False),
                                                                          expected_error=testcase.get("expected_error",None),
                                                                          username=testcase.get("username", self.analytics_username))
                if testcase.get("validate_error_msg", False):
                    if not response:
                        self.fail("Error message is different than expected.")
                else:
                    if not (len(response) == testcase["expected_hits"]):
                        self.fail("Expected links - {0} \t Actual links - {1}".format(testcase["expected_hits"], len(response)))
                    if not (response[0]["dataverse"] == self.link_info["dataverse"]):
                        self.fail("Expected - {0} \t Actual- {1}".format(self.link_info["dataverse"], response[0]["dataverse"]))
                    if not (response[0]["name"] == self.link_info["name"]):
                        self.fail("Expected - {0} \t Actual- {1}".format(self.link_info["name"], response[0]["name"]))
                    if not (response[0]["type"] == self.link_info["type"]):
                        self.fail("Expected - {0} \t Actual- {1}".format(self.link_info["type"], response[0]["type"]))
                    if not (response[0]["bootstrapHostname"] == self.link_info["hostname"]):
                        self.fail("Expected - {0} \t Actual- {1}".format(self.link_info["hostname"], response[0]["bootstrapHostname"]))
                    if not (response[0]["username"] == self.link_info["username"]):
                        self.fail("Expected - {0} \t Actual- {1}".format(self.link_info["username"], response[0]["username"]))
                    if not (response[0]["encryption"] == self.link_info["encryption"]):
                        self.fail("Expected - {0} \t Actual- {1}".format(self.link_info["encryption"], response[0]["encryption"]))
                    if not (response[0]["certificate"] == self.link_info.get("certificate", None)):
                        self.fail("Expected - {0} \t Actual- {1}".format(self.link_info["certificate"], response[0]["certificate"]))
                    if not (response[0]["clientCertificate"] == self.link_info.get("clientCertificate",None)):
                        self.fail("Expected - {0} \t Actual- {1}".format(self.link_info["clientCertificate"], response[0]["clientCertificate"]))
                    if self.link_info["encryption"] == "full":
                        if not (response[0]["clientKey"] == "<redacted sensitive entry>"):
                            self.fail("Expected - {0} \t Actual- {1}".format("<redacted sensitive entry>", response[0]["clientKey"]))
                        if not (response[0]["password"] == self.link_info["password"]):
                            self.fail("Expected - {0} \t Actual- {1}".format(self.link_info["password"], response[0]["password"]))
                    else:
                        if not (response[0]["password"] == "<redacted sensitive entry>"):
                            self.fail("Expected - {0} \t Actual- {1}".format("<redacted sensitive entry>", response[0]["password"]))
                        if not (response[0]["clientKey"] == self.link_info.get("clientKey",None)):
                            self.fail("Expected - {0} \t Actual- {1}".format(self.link_info["clientKey"], response[0]["clientKey"]))
                self.log.info("Test Passed")
        else:
            self.fail("Link creation failed")

    def test_alter_link_properties(self):
        to_cluster = random.choice(self.to_clusters)
        self.get_link_property_dict(to_cluster)
        if self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = self.link_info):
            self.link_created = True

            def remove_and_return_new_list(itemlist, item_to_remove):
                itemlist.remove(item_to_remove)
                return itemlist

            # Create users with all RBAC roles.
            self.create_or_delete_users(self.analytics_cluster)

            self.setup_certs()

            new_dataverse = "dataverse2"
            if not self.analytics_cluster.cbas_util.create_dataverse_on_cbas(dataverse_name=new_dataverse,
                                                                             username=self.analytics_username):
                self.fail("Dataverse creation failed")

            ip_list = []
            for server in to_cluster.servers:
                ip_list.append(server.ip)

            ip_list = remove_and_return_new_list(ip_list, to_cluster.master.ip)

            testcases = [
                {"description": "Changing dataverse to another existing dataverse",
                 "dataverse": new_dataverse,
                 "validate_error_msg": True,
                 "expected_error": "Link {0} does not exist".format(self.link_info["name"])
                    },
                {"description": "Changing dataverse to a non-existing dataverse",
                 "dataverse": self.invalid_value,
                 "validate_error_msg": True,
                 "expected_error": "Dataverse {0} does not exist".format(self.invalid_value)
                    },
                {"description": "Changing link type",
                 "type": "s3",
                 "validate_error_msg": True,
                 "expected_error": "Link type for an existing link cannot be changed. \
                 Existing link type is: COUCHBASE, provided link type is: s3"
                    },
                {"description": "Changing hostname to another node of same cluster",
                 "hostname": ip_list[0],
                    },
                {"description": "Changing hostname to another cluster",
                 "hostname": random.choice(remove_and_return_new_list(self.to_clusters, to_cluster)[0].master.ip),
                 "encryption": "none",
                    },
                {"description": "Changing hostname to an invalid hostname",
                 "hostname": self.invalid_ip,
                 "validate_error_msg": True,
                 "expected_error": "No route to host"
                    },
                {"description": "Changing credentials to invalid credentials",
                 "password": self.invalid_value,
                 "validate_error_msg": True,
                 "expected_error": "Invalid credentials for link {0}.{1}".format(self.link_info["dataverse"],
                                                                                 self.link_info["name"])
                    },
                {"description": "Changing credentials to another set of valid credentials",
                 "username": "cluster_admin",
                    },
                {"description": "Changing credentials to a user which has less than minimum role required",
                 "username": "analytics_reader",
                 "validate_error_msg": True,
                 "expected_error": "Invalid credentials for link {0}.{1}".format(self.link_info["dataverse"],
                                                                                 self.link_info["name"])
                    },
                {"description": "Changing encryption type to half",
                 "encryption": "half"
                    },
                {"description": "Changing encryption type to full, with valid root certificate",
                 "encryption": "full",
                 "clientCertificate": None,
                 "clientKey": None
                    },
                {"description": "Changing encryption type to full, with invalid root certificate",
                 "encryption": "full",
                 "certificate": self.analytics_cluster.root_ca_path,
                 "clientCertificate": None,
                 "clientKey": None,
                 "validate_error_msg": True,
                 "expected_error": ""
                    },
                {"description": "Changing encryption type to full, with valid root certificate, clientcertificate\
                and client key",
                 "encryption": "full",
                 "username": None,
                 "pasword": None
                    },
                {"description": "Changing encryption type to full, with valid root certificate and clientcertificate\
                and invalid client key",
                 "encryption": "full",
                 "username": None,
                 "pasword": None,
                 "clientKey": self.analytics_cluster.client_cert_key,
                 "validate_error_msg": True,
                 "expected_error": ""
                    },
                {"description": "Changing encryption type to full, with valid root certificate and clientKey\
                and invalid clientcertificate",
                 "encryption": "full",
                 "username": None,
                 "pasword": None,
                 "clientCertificate": self.analytics_cluster.client_cert_pem,
                 "validate_error_msg": True,
                 "expected_error": ""
                    },
                ]

            rbac_testcases = self.create_testcase_for_rbac_user(self.analytics_cluster,
                                                                       "Altering external link properties using (0) user")
            for testcase in rbac_testcases:
                testcase["encryption"] = "half"
                if testcase["validate_error_msg"]:
                    testcase["expected_error"] = "Unauthorized user"

            testcases = testcases + rbac_testcases

            for testcase in testcases:
                self.log.info(testcase["description"])
                link_properties = copy.deepcopy(self.link_info)
                # Copying link property values for the testcase, to link_property dict, to create a new link.
                for key, value in testcase.iteritems():
                    if key in link_properties:
                        link_properties[key] = value
                response = self.cluster.cbas_util.update_external_link_properties(
                    link_properties, validate_error_msg=testcase.get("validate_error_msg", False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get("username", self.analytics_username))

                if testcase["validate_error_msg"]:
                    assert(response, "Error message is different than expected.")
                else:
                    response2 = self.cluster.cbas_util.get_link_info(link_name=link_properties["name"],
                                                                     dataverse=link_properties["dataverse"])[0]
                    assert(response2.viewitems() <= link_properties,
                           "Actual link properties - \n{0}\n Expected Link properties - \n{1}\n".format(json.dumps(response2),
                                                                                                        json.dumps(link_properties)))
                self.log.info("Test Passed")
            self.analytics_cluster.cbas_util.drop_link_on_cbas(link_name= "{0}.{1}".format(
                self.link_info["dataverse"], self.link_info["name"]))
            assert(self.analytics_cluster.cbas_util.drop_dataverse_on_cbas(dataverse_name=new_dataverse,
                                                                        username=self.analytics_username),
                                                                        "Dataverse creation failed")


        else:
            assert(False, "Link creation failed")

    def test_connect_link(self):
        to_cluster = random.choice(self.to_clusters)
        self.get_link_property_dict(to_cluster)
        if self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = self.link_info):
            self.link_created = True

            # Create users with all RBAC roles.
            self.create_or_delete_users(self.analytics_cluster)

            testcases = [
                {"name": self.link_info["name"],
                 "description": "Connect a valid link",
                    },
                {"name": self.invalid_value,
                 "description": "Connect an invalid link",
                 "validate_error_msg": True,
                 "expected_error": "Link {0}.{1} does not exist".format(self.link_info["dataverse"], self.invalid_value)
                    }
                ]

            rbac_testcases = self.create_testcase_for_rbac_user(self.analytics_cluster,
                                                                "Connect external link using (0) user")
            for testcase in rbac_testcases:
                testcase["name"] = self.link_info["name"]

            testcases = testcases + rbac_testcases
            for testcase in testcases:
                self.log.info(testcase["description"])
                if not self.analytics_cluster.cbas_util.connect_link(
                    link_name="{0}.{1}".format(self.link_info["dataverse"], testcase["name"]),
                    validate_error_msg=testcase.get("validate_error_msg", False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get("username", self.analytics_username)):
                    self.fail("Error while connecting the link ")
                self.log.info("Test Passed")

        else:
            self.fail("Link creation failed")

    def test_disconnect_link(self):
        to_cluster = random.choice(self.to_clusters)
        self.get_link_property_dict(to_cluster)
        if self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = self.link_info):
            self.link_created = True

            # Create users with all RBAC roles.
            self.create_or_delete_users(self.analytics_cluster)
            testcases = [
                {"name": self.link_info["name"],
                 "description": "Disconnect a valid link",
                    },
                {"name": self.invalid_value,
                 "description": "Disconnect an invalid link",
                 "validate_error_msg": True,
                 "expected_error": "Link {0}.{1} does not exist".format(self.link_info["dataverse"], self.invalid_value)
                    }
                ]

            rbac_testcases = self.create_testcase_for_rbac_user(self.analytics_cluster,
                                                                       "Disconnect external link using (0) user")
            for testcase in rbac_testcases:
                testcase["name"] = self.link_info["name"]

            testcases = testcases + rbac_testcases

            for testcase in testcases:
                self.log.info(testcase["description"])
                if not self.analytics_cluster.cbas_util.disconnect_link(
                    link_name="{0}.{1}".format(self.link_info["dataverse"], testcase["name"]),
                    validate_error_msg=testcase.get("validate_error_msg", False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get("username", self.analytics_username)):
                    self.fail( "Error while Disconnecting the link ")
                self.log.info("Test Passed")

        else:
            self.fail("Link creation failed")

    def test_create_dataset(self):
        to_cluster = random.choice(self.to_clusters)
        self.get_link_property_dict(to_cluster)

        if not self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = self.link_info):
            self.fail("link creation failed")
        self.link_created = True

        if not to_cluster.bucket_util.load_sample_bucket(self.sample_bucket):
            self.fail("Error while loading {0} bucket in remote cluster".format(self.sample_bucket))

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster)

        testcases = [
            {
                "description": "Creating a valid dataset",
                "dataset_name": self.cbas_dataset_name,
                "bucket_name": self.sample_bucket.name
            },
            {
                "description": "Creating a dataset on non-existent bucket on remote cluster",
                "dataset_name": self.cbas_dataset_name,
                "bucket_name": self.invalid_value,
                "validate_error_msg": True,
                "expected_error": "Bucket ({0}) does not exist".format(self.invalid_value)
            },
        ]

        rbac_testcases = self.create_testcase_for_rbac_user(self.analytics_cluster,
                                                            "Creating dataset on external link using (0) user")
        for testcase in rbac_testcases:
            testcase["dataset_name"] = self.cbas_dataset_name
            testcase["bucket_name"] = self.sample_bucket.name

        testcases = testcases + rbac_testcases
        for testcase in testcases:
            self.log.info(testcase["description"])
            if not self.analytics_cluster.cbas_util.create_dataset_on_bucket(cbas_bucket_name= testcase["bucket_name"],
                                                                             cbas_dataset_name= testcase["dataset_name"],
                                                                             validate_error_msg=testcase.get("validate_error_msg",
                                                                                                             False),
                                                                             expected_error=testcase.get("expected_error",
                                                                                                         None),
                                                                             dataverse=self.link_info["dataverse"],
                                                                             compress_dataset=False,
                                                                             link_name="{0}.{1}".format(
                                                                                 self.link_info["dataverse"],
                                                                                 self.link_info["name"]),
                                                                             username=testcase.get("username",
                                                                                                   self.analytics_username)):
                self.fail("Error while creating dataset")
            if not testcase.get("validate_error_msg", False):
                if not self.analytics_cluster.cbas_util.validate_dataset_in_metadata_collection(dataset_name=testcase["dataset_name"],
                                                                                                link_name=self.link_info["name"],
                                                                                                dataverse=self.link_info["dataverse"],
                                                                                                bucket_name=testcase["bucket_name"]):
                    self.fail("Dataset entry not present in Metadata.Dataset collection")
                self.analytics_cluster.cbas_util.drop_dataset(cbas_dataset_name=testcase["dataset_name"],
                                                         dataverse=self.link_info["dataverse"])
            self.log.info("Test Passed")

    def test_query_dataset(self):
        to_cluster = self.setup_datasets()

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster)

        testcases = self.create_testcase_for_rbac_user(self.analytics_cluster,
                                                       "Querying dataset on external link using (0) user")

        query_statement = "select * from `{0}` where `id` <= 10;"

        n1ql_result = to_cluster.rest.query_tool(query_statement.format(self.sample_bucket.name))[
            'results'][0][self.sample_bucket.name]

        for testcase in testcases:
            status, metrics, errors, cbas_result, _ = self.analytics_cluster.cbas_util.execute_statement_on_cbas_util(
                statement=query_statement.format(self.cbas_dataset_name),
                username=testcase.get("username", self.analytics_username))
            if testcase["validate_error_msg"]:
                if not self.analytics_cluster.cbas_util.validate_error_in_response(status, errors,
                                                                                   expected_error=testcase["expected_error"]):
                    self.fail("Error msg is different from expected error msg")
            elif not cbas_result[0][self.cbas_dataset_name] == n1ql_result:
                self.fail("Result returned from analytics query does not match the result of N1Ql query")

    def test_effect_of_rbac_role_change_on_external_link(self):
        """
        This testcase checks, effect of user role change on data ingestion,
        when the role of the user who created the link and dataset is changed to a role,
        which does not have permission to create link or datasets.
        """
        new_username = "user1"
        original_user_role = "analytics_admin"
        new_user_role = "analytics_reader"
        self.analytics_cluster.rbac_util._create_user_and_grant_role(new_username, original_user_role)
        rbac_users_created[new_username] = None

        to_cluster = self.setup_datasets(username=new_username)

        self.analytics_cluster.rbac_util._create_user_and_grant_role(new_username, new_user_role)

        task = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=1000, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)

        result = self.task.jython_task_manager.get_task_result(task)

        # Allow ingestion to complete
        if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                            self.sample_bucket.stats.expected_item_count + 1000, 300):
            self.fail("Data Ingestion did not complete")

    def test_data_ingestion_after_reducing_and_then_restoring_remote_user_permission(self):
        to_cluster = random.choice(self.to_clusters)

        # Create a new high privileged role remote user
        new_username = "user1"
        original_user_role = "admin"
        new_user_role = "query_external_access"
        to_cluster.rbac_util._create_user_and_grant_role(new_username, original_user_role)
        rbac_users_created[new_username] = None

        # Set user name to the remote user created above in link properties
        self.get_link_property_dict(to_cluster)
        self.link_info["username"] = new_username

        to_cluster = self.setup_datasets(to_cluster=to_cluster, get_link_info=False)

        # Assign the remote user created before a low privilege role
        to_cluster.rbac_util._create_user_and_grant_role(new_username, new_user_role)

        task = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=self.num_items, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)

        result = self.task.jython_task_manager.get_task_result(task)

        if self.input.param("after_timeout", False):
            # Restoring remote user permission after timeout

            # Wait until timeout happens and dataset is disconnected
            retry = 0
            while retry < 30:
                retry += 1
                status, content, response = self.analytics_cluster.cbas_util.fetch_bucket_state_on_cbas()
                if status and json.loads(content)["buckets"][0]["state"] != "connected":
                    break
                self.sleep(10)

            self.log.info("Validate not all new data has been ingested into dataset")
            if self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                  self.sample_bucket.stats.expected_item_count + self.num_items,
                                                                                  num_tries=1):
                self.fail("New data was ingested into dataset even after remote user permission was revoked.")

            to_cluster.rbac_util._create_user_and_grant_role(new_username, original_user_role)
            # Reconnecting link.
            if not self.analytics_cluster.cbas_util.connect_link("{0}.{1}".format(self.link_info["dataverse"],
                                                                             self.link_info["name"]),
                                                                             username=self.analytics_username):
                self.fail("Error while connecting link")

        else:
            # Restoring remote user permission before timeout
            to_cluster.rbac_util._create_user_and_grant_role(new_username, original_user_role)

        # Allow ingestion to complete
        if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                            self.sample_bucket.stats.expected_item_count + self.num_items,
                                                                            300):
            self.fail("Data Ingestion did not complete")

    def test_data_ingestion_after_daleting_and_then_recreating_remote_user(self):
        to_cluster = random.choice(self.to_clusters)

        # Create a new high privileged role remote user
        new_username = "user1"
        original_user_role = "analytics_admin"

        validate_error_msg = False
        error_msg = None
        timeout = 120
        analytics_timeout = 120

        if self.input.param("has_bucket_access", False) and self.input.param("same_role", False):
            new_role = "analytics_admin"
        elif self.input.param("has_bucket_access", False) and not self.input.param("same_role", False):
            new_role = "admin"
        else:
            new_role = "query_external_access"
            validate_error_msg = True
            error_msg = "Connect link failed"
            timeout = 900
            analytics_timeout = 900

        to_cluster.rbac_util._create_user_and_grant_role(new_username, original_user_role)
        rbac_users_created[new_username] = None

        # Set user name to the remote user created above in link properties
        self.get_link_property_dict(to_cluster)
        self.link_info["username"] = new_username

        to_cluster = self.setup_datasets(to_cluster=to_cluster, get_link_info=False)

        # Delete the remote user created before.
        to_cluster.rbac_util._drop_user(new_username)

        task = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=self.num_items, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)

        result = self.task.jython_task_manager.get_task_result(task)

        if self.input.param("after_timeout", False):
            # Restoring remote user permission after timeout

            # Wait until timeout happens and dataset is disconnected
            retry = 0
            while retry < 30:
                retry += 1
                status, content, response = self.analytics_cluster.cbas_util.fetch_bucket_state_on_cbas()
                if status and json.loads(content)["buckets"][0]["state"] != "connected":
                    break
                self.sleep(10)

            self.log.info("Validate not all new data has been ingested into dataset")
            if self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                  self.sample_bucket.stats.expected_item_count + self.num_items,
                                                                                  num_tries=1):
                self.fail("New data was ingested into dataset even after remote user permission was revoked.")

            to_cluster.rbac_util._create_user_and_grant_role(new_username, new_role)
            # Reconnecting link.
            if not self.analytics_cluster.cbas_util.connect_link("{0}.{1}".format(self.link_info["dataverse"],
                                                                                  self.link_info["name"]),
                                                                 username=self.analytics_username,
                                                                 validate_error_msg=validate_error_msg,
                                                                 expected_error=error_msg,
                                                                 timeout=timeout, analytics_timeout=analytics_timeout):
                self.fail("Error while connecting link")

        else:
            # Restoring remote user permission before timeout
            to_cluster.rbac_util._create_user_and_grant_role(new_username, new_role)

        # Allow ingestion to complete
        if not self.input.param("has_bucket_access", False):
            if self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                            self.sample_bucket.stats.expected_item_count + self.num_items,
                                                                            300):
                self.fail("Data Ingestion started when it should not.")
        else:
            if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                                self.sample_bucket.stats.expected_item_count + self.num_items,
                                                                                300):
                self.fail("Data Ingestion did not complete")


    def test_dataset_creation_when_network_down(self):
        to_cluster = random.choice(self.to_clusters)
        self.get_link_property_dict(to_cluster)
        if not self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = self.link_info):
            self.fail("link cration failed")
        self.link_created = True

        if not to_cluster.bucket_util.load_sample_bucket(self.sample_bucket):
            self.fail("Error while loading {0} bucket in remote cluster".format(self.sample_bucket.name))

        RemoteUtilHelper.enable_firewall(server=to_cluster.master, block_ips=[self.analytics_cluster.master.ip],
                                         all_interface=True)

        if not self.analytics_cluster.cbas_util.create_dataset_on_bucket(cbas_bucket_name= self.sample_bucket.name,
                                                                         cbas_dataset_name= self.cbas_dataset_name,
                                                                         dataverse=self.link_info["dataverse"],
                                                                         link_name="{0}.{1}".format(
                                                                             self.link_info["dataverse"],
                                                                             self.link_info["name"]),
                                                                         validate_error_msg=True,
                                                                         expected_error="Internal error"):
            self.fail("Error while creating dataset")
        to_cluster.cluster_util.stop_firewall_on_node(to_cluster.master)

    def test_connect_link_when_network_up_before_timeout(self):
        "This testcase deals with the scenario, when network comes back up before timeout happens for connect link DDL"
        to_cluster = self.setup_datasets(connect_link=False)

        RemoteUtilHelper.enable_firewall(server=to_cluster.master, action_on_packet="DROP",
                                         block_ips=[self.analytics_cluster.master.ip],
                                         all_interface=True)
        link_connected = [False]

        def get_connect_link_result():
            if self.analytics_cluster.cbas_util.connect_link("{0}.{1}".format(self.link_info["dataverse"],
                                                                              self.link_info["name"]),
                                                                              username=self.analytics_username):
                link_connected[0] = True

        thread = Thread(target=get_connect_link_result,
                        name="connect_link_thread")
        thread.start()

        to_cluster.cluster_util.stop_firewall_on_node(to_cluster.master)

        thread.join()

        if not link_connected[0]:
            self.fail("Link connection Failed.")

        if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                            self.sample_bucket.stats.expected_item_count, 300):
            self.fail("Data Ingestion did not resume")

    def test_data_ingestion_resumes_when_network_up_before_timeout(self):
        "This testcase deals with the scenario, when network comes back up before timeout happens for an exsiting dataset"
        to_cluster = self.setup_datasets()

        RemoteUtilHelper.enable_firewall(server=to_cluster.master, action_on_packet="DROP",
                                         block_ips=[self.analytics_cluster.master.ip],
                                         all_interface=True)

        # Wait for link to get disconnected
        retry = 0
        while retry < 30:
            retry += 1
            status, content, response = self.analytics_cluster.cbas_util.fetch_bucket_state_on_cbas()
            if status and json.loads(content)["buckets"][0]["state"] != "connected":
                break
            self.sleep(10)

        task = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=self.num_items, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)

        result = self.task.jython_task_manager.get_task_result(task)

        to_cluster.cluster_util.stop_firewall_on_node(to_cluster.master)

        # Allow ingestion to complete
        if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                            self.sample_bucket.stats.expected_item_count + self.num_items,
                                                                            300):
            self.fail("Data Ingestion did not complete")

    def test_reconnecting_link_after_timeout_due_to_network_failure(self):
        """
        This testcase deals with the scenario, when network comes back up after timeout happens.
        The link connection should fail after the timeout happens. But a subsequent link connection
        request once the network is up should succeed.
        """
        to_cluster = self.setup_datasets(connect_link=False)

        RemoteUtilHelper.enable_firewall(server=to_cluster.master, action_on_packet="DROP",
                                         block_ips=[self.analytics_cluster.master.ip],
                                         all_interface=True)

        #Wait for connect link to timeout. It take approx 10 mins to timeout.
        if not self.analytics_cluster.cbas_util.connect_link("{0}.{1}".format(self.link_info["dataverse"],
                                                                              self.link_info["name"]),
                                                                              username=self.analytics_username,
                                                                              validate_error_msg=True,
                                                                              expected_error="Connect link failed",
                                                                              timeout=900, analytics_timeout=900):
            self.fail("Expected a different error while connecting link")

        to_cluster.cluster_util.stop_firewall_on_node(to_cluster.master)
        if not self.analytics_cluster.cbas_util.connect_link("{0}.{1}".format(self.link_info["dataverse"],
                                                                              self.link_info["name"]),
                                                                              username=self.analytics_username):
            self.fail("Link connection failed")
        if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                            self.sample_bucket.stats.expected_item_count,
                                                                            300):
            self.fail("Data Ingestion did not resume")

    def test_dataset_behaviour_on_remote_bucket_deletion(self):
        to_cluster = self.setup_datasets()

        if not to_cluster.bucket_util.delete_bucket(serverInfo=to_cluster.master,
                                                    bucket=self.sample_bucket):
            self.fail("Deletion of bucket failed")

        # Wait for link to get disconnected
        retry = 0
        while retry < 30:
            retry += 1
            status, content, response = self.analytics_cluster.cbas_util.fetch_bucket_state_on_cbas()
            if status and json.loads(content)["buckets"][0]["state"] != "connected":
                break
            self.sleep(10)

        if not self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                  self.sample_bucket.stats.expected_item_count):
            self.fail("Mismatch between number of documents in dataset and remote bucket")

    def test_dataset_behaviour_on_remote_bucket_deletion_and_recreation(self):
        """
        This testcase deals with the scenario when remote bucket is deleted and another bucket with same name
        is recreated on remote cluster.
        """
        to_cluster = self.perform_setup_and_add_new_docs(10, wait_for_ingestion=True)

        if not to_cluster.bucket_util.delete_bucket(serverInfo=to_cluster.master,
                                                    bucket=self.sample_bucket):
            self.fail("Deletion of bucket failed")

        # Wait for link to get disconnected
        retry = 0
        while retry < 30:
            retry += 1
            status, content, response = self.analytics_cluster.cbas_util.fetch_bucket_state_on_cbas()
            if status and json.loads(content)["buckets"][0]["state"] != "connected":
                break
            self.sleep(10)

        if not to_cluster.bucket_util.load_sample_bucket(self.sample_bucket):
            self.fail("Error while loading {0} bucket in remote cluster".format(self.sample_bucket.name))

        with_force_flag = self.input.param("with_force_flag", False)

        if with_force_flag:
            validate_err_msg = False
            expected_error_msg = None
        else:
            validate_err_msg = True
            expected_error_msg = "Bucket UUID has changed"

        if not self.analytics_cluster.cbas_util.connect_link("{0}.{1}".format(self.link_info["dataverse"],
                                                                              self.link_info["name"]),
                                                                              validate_error_msg=validate_err_msg,
                                                                              with_force=with_force_flag,
                                                                              username=self.analytics_username,
                                                                              expected_error=expected_error_msg):
            self.fail("Error while connecting link")
        if with_force_flag and not self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                                      self.sample_bucket.stats.expected_item_count):
            self.fail("Mismatch between number of documents in dataset and remote bucket")

    def test_data_ingestion_for_data_updation_in_remote_cluster(self):
        to_cluster = self.perform_setup_and_add_new_docs(10, wait_for_ingestion=False)

        task = self.perform_doc_ops_in_all_cb_buckets(operation="update", end_key=10, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key,
                                                      mutation_num=1)
        result = self.task.jython_task_manager.get_task_result(task)

        if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                            self.sample_bucket.stats.expected_item_count + 10,
                                                                            300):
            self.fail("Data Ingestion did not complete")

        query_statement = "select count(*) from `{0}` where mutated=1;"
        count_n1ql = to_cluster.rest.query_tool(query_statement.format(self.sample_bucket.name))['results'][0]['$1']

        status, metrics, errors, cbas_result, _ = self.analytics_cluster.cbas_util.execute_statement_on_cbas_util(
            statement=query_statement.format(self.cbas_dataset_name))
        if status == "success":
            cbas_result = cbas_result[0]['$1']
            if not cbas_result == count_n1ql:
                self.fail("Result returned from analytics query does not match the result of N1Ql query")
        else:
            self.fail("Unable to execute query on analytics dataset")

    def test_data_ingestion_for_data_deletion_in_remote_cluster(self):
        to_cluster = self.perform_setup_and_add_new_docs(10, wait_for_ingestion=False)

        task = self.perform_doc_ops_in_all_cb_buckets(operation="delete", end_key=10, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)
        result = self.task.jython_task_manager.get_task_result(task)

        if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                            self.sample_bucket.stats.expected_item_count,
                                                                            300):
            self.fail("Data Ingestion did not complete")

    def get_rebalance_servers(self, to_cluster):
        rebalanceServers = list()
        used_servers = list()
        for cserver in (self.analytics_cluster.servers + to_cluster.servers):
            used_servers.append(cserver.ip)
        for server in self.servers:
            if not server.ip in used_servers:
                rebalanceServers.append(server)
        return rebalanceServers

    def test_analytics_cluster_while_remote_cluster_swap_rebalacing(self):
        '''
        1. We have 2 clusters, local cluster, remote cluster and 4 nodes - 101, 102, 103, 104.
        2, Post initial setup - local cluster - 1 node with cbas, remote cluster - 1 node with KV and query running
        3. As part of test add an extra KV node that we will swap rebalance later - Adding 103 and rebalance
        4. select the node added in #3 for remove and 104 to add during swap

        Data mutation is happening on remote cluster while local cluster is rebalancing.
        '''

        node_services = ["kv", "n1ql"]

        self.log.info("Setup CBAS")
        to_cluster = self.setup_datasets()

        rebalanceServers = self.get_rebalance_servers(to_cluster)

        self.log.info("Rebalance in remote cluster, this node will be removed during swap")
        to_cluster.cluster_util.add_node(node=rebalanceServers[0], services=node_services)


        self.log.info("Run KV ops in async while rebalance is in progress")
        tasks = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=self.num_items, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)

        run_query = self.input.param("run_query", False)
        if run_query:
            self.log.info("Run concurrent queries to simulate busy system")
            statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.cbas_dataset_name)
            handles = self.analytics_cluster.cbas_util._run_concurrent_queries(statement,
                                                                          "async",
                                                                          self.num_concurrent_queries,
                                                                          wait_for_execution=False)

        self.log.info("Fetch node to remove during rebalance")
        out_nodes = []
        nodes = to_cluster.rest.node_statuses()
        for node in nodes:
            if node.ip == rebalanceServers[0].ip:
                out_nodes.append(node)

        self.log.info("Swap rebalance KV nodes")
        to_cluster.cluster_util.add_node(node=rebalanceServers[1], services=node_services, rebalance=False)
        self.remove_node([out_nodes[0]], wait_for_rebalance=False, rest=to_cluster.rest)

        if not to_cluster.rest.monitorRebalance():
            self.fail("Rebalance failed")

        self.log.info("Get KV ops result")
        self.task_manager.get_task_result(tasks)

        self.log.info("Log concurrent query status")
        self.analytics_cluster.cbas_util.log_concurrent_query_outcome(self.analytics_cluster.cluster.master, handles)

        if not self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                  self.sample_bucket.stats.expected_item_count +
                                                                                  self.num_items):
            self.fail("Number of items in dataset do not match number of items in bucket")


    def test_analytics_cluster_swap_rebalacing(self):
        '''
        1. We have 2 clusters, local cluster, remote cluster and 4 nodes - 101, 102, 103, 104.
        2, Post initial setup - local cluster - 1 node with cbas, remote cluster - 1 node with KV and query running
        3. As part of test add an extra cbas node that we will swap rebalance later - Adding 103 and rebalance
        4. select the node added in #3 for remove and 104 to add during swap

        Data mutation is happening on remote cluster while local cluster is rebalancing.
        '''

        node_services = ["cbas"]

        self.log.info("Setup CBAS")
        to_cluster = self.setup_datasets()

        rebalanceServers = self.get_rebalance_servers(to_cluster)

        self.log.info("Rebalance in local cluster, this node will be removed during swap")
        self.analytics_cluster.cluster_util.add_node(node=rebalanceServers[0], services=node_services)

        self.log.info("Run KV ops in async while rebalance is in progress")
        tasks = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=self.num_items, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)

        run_query = self.input.param("run_query", False)
        if run_query:
            self.log.info("Run concurrent queries to simulate busy system")
            statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.cbas_dataset_name)
            handles = self.analytics_cluster.cbas_util._run_concurrent_queries(statement,
                                                                          "async",
                                                                          self.num_concurrent_queries,
                                                                          wait_for_execution=False)

        self.log.info("Fetch node to remove during rebalance")
        out_nodes = []
        nodes = self.analytics_cluster.rest.node_statuses()
        for node in nodes:
            if node.ip == rebalanceServers[0].ip:
                out_nodes.append(node)

        self.log.info("Swap rebalance CBAS nodes")
        self.analytics_cluster.cluster_util.add_node(node=rebalanceServers[1], services=node_services, rebalance=False)
        self.remove_node([out_nodes[0]], wait_for_rebalance=True, rest=self.analytics_cluster.rest)

        self.log.info("Get KV ops result")
        self.task_manager.get_task_result(tasks)

        self.log.info("Log concurrent query status")
        self.analytics_cluster.cbas_util.log_concurrent_query_outcome(self.analytics_cluster.master, handles)

        if not self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                  self.sample_bucket.stats.expected_item_count
                                                                                  + self.num_items):
            self.fail("Number of items in dataset do not match number of items in bucket")


    def test_analytics_cluster_when_rebalacing_in_cbas_node(self):
        '''
        1. We have 2 clusters, local cluster, remote cluster and 4 nodes - 101, 102, 103, 104.
        2, Post initial setup - local cluster - 1 node with cbas, remote cluster - 1 node with KV and query running
        3. As part of test add an extra cbas nodes and rebalance

        Data mutation is happening on remote cluster while local cluster is rebalancing.
        '''
        node_services = ["cbas"]

        self.log.info("Setup CBAS")
        to_cluster = self.setup_datasets()

        rebalanceServers = self.get_rebalance_servers(to_cluster)

        self.log.info("Run KV ops in async while rebalance is in progress")
        tasks = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=self.num_items, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)

        run_query = self.input.param("run_query", False)
        if run_query:
            self.log.info("Run concurrent queries to simulate busy system")
            statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.cbas_dataset_name)
            handles = self.analytics_cluster.cbas_util._run_concurrent_queries(statement, "async",
                                                                          self.num_concurrent_queries,
                                                                          wait_for_execution=False)

        self.log.info("Rebalance in CBAS nodes")
        self.analytics_cluster.cluster_util.add_node(node=rebalanceServers[0], services=node_services,
                                                rebalance=False, wait_for_rebalance_completion=False)
        self.analytics_cluster.cluster_util.add_node(node=rebalanceServers[1], services=node_services,
                                                rebalance=True, wait_for_rebalance_completion=True)

        self.log.info("Get KV ops result")
        self.task_manager.get_task_result(tasks)

        self.log.info("Log concurrent query status")
        self.analytics_cluster.cbas_util.log_concurrent_query_outcome(self.analytics_cluster.cluster.master, handles)

        if not self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                  self.sample_bucket.stats.expected_item_count
                                                                                  + self.num_items, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    def test_analytics_cluster_when_rebalacing_out_cbas_node(self):
        '''
        1. We have 2 clusters, local cluster, remote cluster and 4 nodes - 101, 102, 103, 104.
        2, Post initial setup - local cluster - 1 node with cbas, remote cluster - 1 node with KV and query running
        3. As part of test add extra cbas nodes that we will rebalance out later - Adding 103, 104 and rebalance
        4. select the nodes added in #3 for remove

        Data mutation is happening on remote cluster while local cluster is rebalancing.
        '''
        node_services = ["cbas"]

        self.log.info("Setup CBAS")
        to_cluster = self.setup_datasets()

        rebalanceServers = self.get_rebalance_servers(to_cluster)

        self.log.info("Rebalance in CBAS nodes")
        self.analytics_cluster.cluster_util.add_node(node=rebalanceServers[0], services=node_services,
                                                rebalance=False, wait_for_rebalance_completion=False)
        self.analytics_cluster.cluster_util.add_node(node=rebalanceServers[1], services=node_services,
                                                rebalance=True, wait_for_rebalance_completion=True)


        self.log.info("Run KV ops in async while rebalance is in progress")
        tasks = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=self.num_items, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)

        run_query = self.input.param("run_query", False)
        if run_query:
            self.log.info("Run concurrent queries to simulate busy system")
            statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.cbas_dataset_name)
            handles = self.analytics_cluster.cbas_util._run_concurrent_queries(statement, "async",
                                                                          self.num_concurrent_queries,
                                                                          wait_for_execution=False)


        self.log.info("Fetch node to remove during rebalance")
        out_nodes = []
        nodes = self.analytics_cluster.rest.node_statuses()
        for node in nodes:
            if node.ip == rebalanceServers[0].ip or node.ip == rebalanceServers[1].ip:
                out_nodes.append(node)

        self.log.info("Rebalance out CBAS nodes %s %s" % (out_nodes[0].ip, out_nodes[1].ip))
        self.analytics_cluster.cluster_util.remove_all_nodes_then_rebalance([out_nodes[0],out_nodes[1]])

        self.log.info("Get KV ops result")
        self.task_manager.get_task_result(tasks)

        self.log.info("Log concurrent query status")
        self.cbas_util.log_concurrent_query_outcome(self.cluster.master, handles)

        if not self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                  self.sample_bucket.stats.expected_item_count
                                                                                  + self.num_items, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")


    def test_fail_over_cbas_node_followed_by_rebalance_out_or_add_back(self):
        """
        1. We have 2 clusters, local cluster, remote cluster and 4 nodes - 101, 102, 103, 104.
        2, Post initial setup - local cluster - 1 node with cbas, remote cluster - 1 node with KV and query running
        3. Add a node that will be failed over - CBAS
        4. Create CBAS buckets and dataset
        5. Fail over the cbas node based on graceful_failover parameter specified
        6. Rebalance out/add back based on rebalance_out parameter specified
        7. Perform doc operations
        8. run concurrent queries
        9. Verify document count on dataset post failover
        """
        node_services = ["cbas"]

        self.log.info("Setup CBAS")
        to_cluster = self.setup_datasets()

        rebalanceServers = self.get_rebalance_servers(to_cluster)

        self.log.info("Add an extra node to fail-over")
        self.analytics_cluster.cluster_util.add_node(node=rebalanceServers[0], services=node_services,
                                                     rebalance=True, wait_for_rebalance_completion=True)

        self.log.info("Read the failure out type to be performed")
        graceful_failover = self.input.param("graceful_failover", True)

        self.log.info("Run KV ops in async while rebalance is in progress")
        tasks = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=self.num_items, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)

        run_query = self.input.param("run_query", False)
        if run_query:
            self.log.info("Run concurrent queries to simulate busy system")
            statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.cbas_dataset_name)
            handles = self.analytics_cluster.cbas_util._run_concurrent_queries(statement, "async",
                                                                          self.num_concurrent_queries,
                                                                          wait_for_execution=False)

        self.log.info("fail-over the node")
        fail_task = self.analytics_cluster.async_failover(self.analytics_cluster.servers,
                                                          [rebalanceServers[0]], graceful_failover)
        self.task_manager.get_task_result(fail_task)

        self.log.info("Read input param to decide on add back or rebalance out")
        self.rebalance_out = self.input.param("rebalance_out", False)
        if self.rebalance_out:
            self.log.info("Rebalance out the fail-over node")
            result = self.analytics_cluster.cluster_util.rebalance()
            self.assertTrue(result, "Rebalance operation failed")
        else:
            self.recovery_strategy = self.input.param("recovery_strategy", "full")
            self.log.info("Performing %s recovery" % self.recovery_strategy)
            success = False
            end_time = datetime.datetime.now() + datetime.timedelta(minutes=int(1))
            while datetime.datetime.now() < end_time or not success:
                try:
                    self.sleep(10, message="Wait for fail over complete")
                    self.analytics_cluster.rest.set_recovery_type('ns_1@' + rebalanceServers[0].ip, self.recovery_strategy)
                    success = True
                except Exception:
                    self.log.info("Fail over in progress. Re-try after 10 seconds.")
                    pass
            if not success:
                self.fail("Recovery %s failed." % self.recovery_strategy)
            self.analytics_cluster.rest.add_back_node('ns_1@' + rebalanceServers[0].ip)
            result = self.analytics_cluster.cluster_util.rebalance()
            self.assertTrue(result, "Rebalance operation failed")

        self.log.info("Get KV ops result")
        self.task_manager.get_task_result(tasks)

        self.log.info("Log concurrent query status")
        self.cbas_util.log_concurrent_query_outcome(self.analytics_cluster.master, handles)

        self.log.info("Validate dataset count on CBAS")
        count_n1ql = to_cluster.rest.query_tool('select count(*) from `%s`' % self.cb_bucket_name)['results'][0]['$1']
        if not self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                  count_n1ql, 0,
                                                                                  timeout=400,
                                                                                  analytics_timeout=400):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
    
    
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