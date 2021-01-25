'''
Created on 13-May-2020

@author: umang.agrawal
'''
from TestInput import TestInputSingleton
from cbas.cbas_base import CBASBaseTest
import random, json, copy, time
from threading import Thread
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from rbac_utils.Rbac_ready_functions import RbacUtils
from security_utils.x509main import x509main
from shutil import copytree
from membase.api.rest_client import RestHelper
from cbas_utils.cbas_utils import Dataset
from Cb_constants import CbServer

rbac_users_created = {}


class CBASExternalLinks(CBASBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket": False})
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
            try:
                cluster.cluster_util.add_all_nodes_then_rebalance(cluster.servers)
            except Exception as err:
                if "already added to this cluster" in str(err):
                    self.log.info("Ignoring this error as nodes have already been added to cluster")
                else:
                    raise Exception(str(err))

        # Create analytics admin user on Analytics cluster
        self.analytics_username = "admin_analytics"
        self.analytics_cluster.rbac_util._create_user_and_grant_role(self.analytics_username, "analytics_admin")
        rbac_users_created[self.analytics_username] = "analytics_admin"

        self.invalid_value = "invalid"
        self.invalid_ip = "172.19.202.132"
        self.log.info("================================================================")
        self.log.info("SETUP has finished")
        self.log.info("================================================================")

    def tearDown(self):
        """
        If certs were setup, then remove those certs and restore default certs.
        """
        self.log.info("================================================================")
        self.log.info("TEARDOWN has started")
        self.log.info("================================================================")
        if hasattr(self, "dataverse_map"):
            for dataverse, links in self.dataverse_map.iteritems():
                for link in links:
                    self.analytics_cluster.cbas_util.drop_link_on_cbas(link_name="{0}.{1}".format(
                        dataverse, link))
                self.analytics_cluster.cbas_util.drop_dataverse_on_cbas(dataverse)
        if self.setup_certs_status:
            self.teardown_certs()

        if self.dataset_created:
            self.analytics_cluster.cbas_util.drop_dataset(cbas_dataset_name=self.cbas_dataset_name,
                                                     dataverse=self.link_info["dataverse"])

        if self.link_created:
            self.analytics_cluster.cbas_util.drop_link_on_cbas(link_name= "{0}.{1}".format(
                self.link_info["dataverse"],
                self.link_info["name"]))

        self.create_or_delete_users(self.analytics_cluster.rbac_util, rbac_users_created, delete=True)

        for cluster in self._cb_cluster:
            cluster.cluster_util.stop_firewall_on_node(cluster.master)

        super(CBASExternalLinks, self).tearDown()
        self.log.info("================================================================")
        self.log.info("Teardown has finished")
        self.log.info("================================================================")

    def setup_certs(self):
        """
        Setup method for setting up root, node and client certs for all the clusters.
        """

        self.log.info("Setting up certificates")
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

            self.log.info("Certificate creation started on cluster - {0}".format(cluster.name))
            cluster.x509 = x509main(cluster.master)
            cluster.CACERTFILEPATH = x509main.CACERTFILEPATH.rstrip("/") + cluster.name + "/"
            cluster.root_ca_path = cluster.CACERTFILEPATH + x509main.CACERTFILE
            cluster.client_certs = dict()
            for server in cluster.servers:
                cluster.client_certs[server.ip] = {
                    "cert_pem": cluster.CACERTFILEPATH + "long_chain" + self.ip_address + ".pem",
                    "cert_key": cluster.CACERTFILEPATH + self.ip_address + ".key",
                    }

            copy_servers = copy.deepcopy(cluster.servers)

            #Generate cert and pass on the client ip for cert generation
            if (self.dns is not None) or (self.uri is not None):
                cluster.x509._generate_cert(copy_servers,type=SSLtype,encryption=encryption_type,key_length=key_length,client_ip=self.ip_address,alt_names='non_default',dns=self.dns,uri=self.uri)
            else:
                cluster.x509._generate_cert(copy_servers,type=SSLtype,encryption=encryption_type,key_length=key_length,client_ip=self.ip_address)
            self.log.info(" Path is {0} - Prefixs - {1} -- Delimeters - {2}".format(self.paths, self.prefixs, self.delimeters))

            if self.setup_once:
                self.sleep(5, "Sleeping before uploading root certs.")
                cluster.x509.setup_master(self.client_cert_state, self.paths, self.prefixs, self.delimeters)
                self.sleep(5, "Sleeping before uploading node certs.")
                cluster.x509.setup_cluster_nodes_ssl(cluster.servers,reload_cert=True)
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

    def get_link_property_dict(self, to_cluster, encryption="none",
                               dataverse_name="Default"):
        """
        Creates a dict of all the properties required to create external link to remote
        couchbase cluster.
        :param to_cluster : remote cluster to which the link has to be created.
        """
        self.link_info = dict()
        self.link_info["dataverse"] = dataverse_name
        self.link_info["name"] = "newLink"
        self.link_info["type"] = "couchbase"
        self.link_info["hostname"] = to_cluster.master.ip
        self.link_info["username"] = to_cluster.master.rest_username
        self.link_info["password"] = to_cluster.master.rest_password
        self.link_info["encryption"] = encryption
        self.link_info["certificate"] = None
        self.link_info["clientCertificate"] = None
        self.link_info["clientKey"] = None
        if encryption == "full":
            self.setup_certs()
            self.link_info["certificate"] = self.read_file(to_cluster.root_ca_path)
            self.link_info["clientCertificate"] = self.read_file(to_cluster.client_certs[to_cluster.master.ip]["cert_pem"])
            self.link_info["clientKey"] = self.read_file(to_cluster.client_certs[to_cluster.master.ip]["cert_key"])
            self.link_info["username"] = None
            self.link_info["password"] = None

    def read_file(self, file_path):
        try:
            with open(file_path, "r") as fh:
                return fh.read()
        except Exception as err:
            self.log.error(str(err))
            return None

    def setup_datasets(self, username=None, to_cluster=None, get_link_info=True,
                       connect_link=True, wait_for_ingestion=True, set_primary_index=True):

        if not username:
            username = self.analytics_username

        self.log.info("Selecting remote cluster")
        if not to_cluster:
            to_cluster = random.choice(self.to_clusters)

        if get_link_info:
            self.get_link_property_dict(to_cluster)

        self.log.info("Creating link to remote cluster")
        if not self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = self.link_info, username=username):
            self.fail("link cration failed")
        self.link_created = True

        self.log.info("Loading sample bucket")
        if not to_cluster.bucket_util.load_sample_bucket(self.sample_bucket):
            self.fail("Error while loading {0} bucket in remote cluster".format(self.sample_bucket.name))

        self.log.info("Creating dataset on link to remote cluster")
        if not self.analytics_cluster.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name= self.sample_bucket.name,
            cbas_dataset_name= "{0}.{1}".format(self.link_info["dataverse"],self.cbas_dataset_name),
            link_name="{0}.{1}".format(self.link_info["dataverse"],self.link_info["name"]), 
            username=username):
            self.fail("Error while creating dataset")
        self.dataset_created = True

        if connect_link:
            self.log.info("Connectin remtoe link")
            if not self.analytics_cluster.cbas_util.connect_link(
                "{0}.{1}".format(self.link_info["dataverse"], self.link_info["name"]),
                username=username):
                self.fail("Error while connecting link")
            if wait_for_ingestion:
                self.log.info("Waiting for data ingestion to complete")
                if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete(
                    ["{0}.{1}".format(self.link_info["dataverse"],self.cbas_dataset_name)],
                    self.sample_bucket.scopes[CbServer.default_scope].collections[
                        CbServer.default_collection].num_items, 300):
                    self.fail("Data Ingestion did not complete")
                self.log.info("Setting primary index on dataset")
                if set_primary_index and not self.set_primary_index(to_cluster.rest, self.sample_bucket.name):
                    self.fail("Creating Primary index on bucket {0} FAILED".format(self.sample_bucket.name))

        return to_cluster

    def perform_setup_and_add_new_docs(self, number_of_docs, wait_for_ingestion=True, username=None):
        to_cluster = self.setup_datasets(wait_for_ingestion=wait_for_ingestion, username=username)

        task = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=number_of_docs, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)
        result = self.task.jython_task_manager.get_task_result(task)

        if wait_for_ingestion:
            assert(self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                           self.sample_bucket.scopes[
                                                                               CbServer.default_scope].collections[
                                                                                   CbServer.default_collection].num_items + number_of_docs,
                                                                           300), "Data Ingestion did not complete")
        return to_cluster

    def test_create_external_link(self):
        # Test parameters
        no_of_dataverse = int(self.input.test_params.get("dataverse", 0))
        no_of_link = int(self.input.test_params.get("link", 1))
        self.setup_certs()
        # Check for link creation failure scenario
        if no_of_link == 1:
            to_cluster = random.choice(self.to_clusters)
            if self.input.param("multipart_dataverse", False):
                dataverse_name = Dataset.create_name_with_cardinality(2)
                if not self.analytics_cluster.cbas_util.validate_dataverse_in_metadata(
                    dataverse_name) and not self.analytics_cluster.cbas_util.create_dataverse_on_cbas(
                        dataverse_name=Dataset.format_name(dataverse_name)):
                    self.fail("Creation of Dataverse {0} failed".format(
                        dataverse_name))
                invalid_dv = "invalid.invalid"
            else:
                dataverse_name = "Default"
                invalid_dv = "invalid"
            
            self.get_link_property_dict(to_cluster,
                                        dataverse_name=Dataset.format_name(dataverse_name))
            
            # Create users with all RBAC roles.
            self.create_or_delete_users(self.analytics_cluster.rbac_util, rbac_users_created)

            testcases = [
                {
                    "description": "Create a link with a non-existent dataverse",
                    "dataverse": invalid_dv,
                    "expected_error": "Cannot find dataverse with name {0}".format(invalid_dv)
                },
                {
                    "description": "Create a link with an invalid hostname",
                    "hostname": self.invalid_ip,
                    "expected_error": "Cannot connect to host for link {0}".format(
                        Dataset.format_name_for_error(True,
                                                      self.link_info["dataverse"], 
                                                      self.link_info["name"]))
                },
                {
                    "description": "Create a link with an invalid credentials",
                    "password": self.invalid_value,
                    "expected_error": "Invalid credentials for link {0}".format(
                        Dataset.format_name_for_error(True,
                                                      self.link_info["dataverse"], 
                                                       self.link_info["name"]))
                },
                {
                    "description": "Create a link with an invalid encryption value",
                    "encryption": self.invalid_value,
                    "expected_error": "Unknown encryption value: '{0}'".format(self.invalid_value)
                },
                {
                    "description": "Create a link with an invalid root certificate",
                    "encryption": "full",
                    "certificate": self.read_file(self.analytics_cluster.root_ca_path),
                    "expected_error": "Cannot connect to host for link {0}".format(
                        Dataset.format_name_for_error(True,
                                                      self.link_info["dataverse"], 
                                                      self.link_info["name"]))
                },
                {
                    "description": "Create a link with an invalid client certificate",
                    "encryption": "full",
                    "certificate": self.read_file(to_cluster.root_ca_path),
                    "clientKey": self.read_file(to_cluster.client_certs[self.link_info["hostname"]]["cert_key"]),
                    "username": None,
                    "password": None,
                    "clientCertificate": self.read_file(
                        self.analytics_cluster.client_certs[self.analytics_cluster.master.ip]["cert_pem"]),
                    "expected_error": "Cannot connect to host for link {0}".format(
                        Dataset.format_name_for_error(True,
                                                      self.link_info["dataverse"], 
                                                      self.link_info["name"]))
                },
                {
                    "description": "Create a link with an invalid client key",
                    "encryption": "full",
                    "clientKey": self.read_file(
                        self.analytics_cluster.client_certs[self.analytics_cluster.master.ip]["cert_key"]),
                    "certificate": self.read_file(to_cluster.root_ca_path),
                    "clientCertificate": self.read_file(
                        to_cluster.client_certs[self.link_info["hostname"]]["cert_pem"]),
                    "username": None,
                    "password": None,
                    "expected_error": "Cannot connect to host for link {0}".format(
                        Dataset.format_name_for_error(True,
                                                      self.link_info["dataverse"], 
                                                      self.link_info["name"]))
                },
                {
                    "description": "Create a link with a name that already exists in the dataverse",
                    "recreate_link": True,
                    "validate_error_msg": False,
                    "expected_error": "Link {0} already exists".format(
                        Dataset.format_name_for_error(True,
                                                      self.link_info["dataverse"], 
                                                      self.link_info["name"]))
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
                        to_cluster.client_certs[self.link_info["hostname"]]["cert_pem"]),
                    "clientKey": self.read_file(
                        to_cluster.client_certs[self.link_info["hostname"]]["cert_key"]),
                    "username": None,
                    "password": None,
                    "validate_error_msg": False,
                }
                ]

            rbac_testcases = self.create_testcase_for_rbac_user("Create a link using {0} user",
                                                                rbac_users_created)
            for testcase in rbac_testcases:
                if testcase["validate_error_msg"]:
                    testcase["expected_error"] = "Unauthorized user"

            testcases = testcases + rbac_testcases

            failed_testcases = list()

            for testcase in testcases:
                try:
                    self.log.info(testcase["description"])
                    link_properties = copy.deepcopy(self.link_info)
                    # Copying link property values for the testcase, to link_property dict, to create a new link.
                    for key, value in testcase.iteritems():
                        if key == "username" and not testcase[key]:
                            link_properties[key] = value
                        elif key in link_properties and key != "username":
                            link_properties[key] = value

                    if testcase.get("stop_server",False):
                        to_cluster.cluster_util.stop_server(to_cluster.master)

                    if not self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = link_properties,
                                                                                         validate_error_msg=testcase.get(
                                                                                             "validate_error_msg", True),
                                                                                         expected_error=testcase.get(
                                                                                             "expected_error", None),
                                                                                         username=testcase.get("username",
                                                                                                               self.analytics_username)):
                        raise Exception("Error message is different than expected.")

                    if testcase.get("stop_server",False):
                        to_cluster.cluster_util.start_server(to_cluster.master)
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
                                    raise Exception("Couchbase service did not start even after 2 mins.")

                    if testcase.get("recreate_link",False):
                        testcase["validate_error_msg"] = True
                        if not self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = link_properties,
                                                                                             validate_error_msg=testcase.get(
                                                                                                 "validate_error_msg", True),
                                                                                             expected_error=testcase.get(
                                                                                                 "expected_error", None),
                                                                                             username=testcase.get("username",
                                                                                                                   self.analytics_username)):
                            raise Exception("Error message is different than expected.")
                        testcase["validate_error_msg"] = False

                    if not testcase.get("validate_error_msg", True):
                        self.analytics_cluster.cbas_util.drop_link_on_cbas(link_name="{0}.{1}".format(link_properties["dataverse"],
                                                                                                 link_properties["name"]),
                                                                                                 username=self.analytics_username)
                except Exception as err:
                    self.log.error(str(err))
                    failed_testcases.append(testcase["description"])
            if failed_testcases:
                self.fail("Following testcases failed - {0}".format(str(failed_testcases)))
        else:
            self.dataverse_map = self.create_dataverse_link_map(self.analytics_cluster.cbas_util,
                                                                no_of_dataverse,
                                                                no_of_link)
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
                            self.dataverse_map[dataverse][link]["certificate"] = self.read_file(to_cluster.root_ca_path)
                            if random.choice([True,False]):
                                self.dataverse_map[dataverse][link]["clientCertificate"] = self.read_file(to_cluster.client_certs[self.dataverse_map[dataverse][link]["hostname"]]["cert_pem"])
                                self.dataverse_map[dataverse][link]["clientKey"] = self.read_file(to_cluster.client_certs[self.dataverse_map[dataverse][link]["hostname"]]["cert_key"])
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
        
        if self.input.param("multipart_dataverse", False):
            dataverse_name = Dataset.create_name_with_cardinality(2)
            if not self.analytics_cluster.cbas_util.validate_dataverse_in_metadata(
                dataverse_name) and not self.analytics_cluster.cbas_util.create_dataverse_on_cbas(
                    dataverse_name=Dataset.format_name(dataverse_name)):
                self.fail("Creation of Dataverse {0} failed".format(
                    dataverse_name))
        else:
            dataverse_name = "Default"
        
        encryption = self.input.test_params.get("encryption", "none")
            
        self.get_link_property_dict(to_cluster,encryption=encryption,
                                    dataverse_name=Dataset.format_name(dataverse_name))

        if self.analytics_cluster.cbas_util.create_external_link_on_cbas(
            link_properties = self.link_info, username=self.analytics_username):
            self.link_created = True
            # Create users with all RBAC roles.
            self.create_or_delete_users(self.analytics_cluster.rbac_util, rbac_users_created)

            if self.link_info["encryption"] in ["full","half"]:
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
                rbac_testcases = self.create_testcase_for_rbac_user("List external links using {0} user",
                                                                    rbac_users_created)
                for testcase in rbac_testcases:
                    testcase["dataverse"] = self.link_info["dataverse"]
                    testcase["name"] = self.link_info["name"]
                    testcase["type"] = "couchbase"
                    if testcase["username"] in ["admin_analytics"]:
                        testcase["validate_error_msg"] = False
                    if testcase["validate_error_msg"]:
                        testcase["expected_hits"] = 0
                        testcase["expected_error"] = "Unauthorized user"
                    else:
                        testcase["expected_hits"] = 1

                testcases = testcases + rbac_testcases

            failed_testcases = list()

            for testcase in testcases:
                try:
                    self.log.info(testcase["description"])
                    response = self.analytics_cluster.cbas_util.get_link_info(link_name=testcase.get("name", None),
                                                                              dataverse=testcase.get("dataverse",None),
                                                                              link_type=testcase.get("type",None),
                                                                              validate_error_msg=testcase.get("validate_error_msg", False),
                                                                              expected_error=testcase.get("expected_error",None),
                                                                              username=testcase.get("username", self.analytics_username))
                    if testcase.get("validate_error_msg", False):
                        if not response:
                            raise Exception("Error message is different than expected.")
                    else:
                        if not (len(response) == testcase["expected_hits"]):
                            raise Exception("Expected links - {0} \t Actual links - {1}".format(testcase["expected_hits"], len(response)))
                        if not (response[0]["dataverse"] == Dataset.format_name_for_error(True,self.link_info["dataverse"])):
                            raise Exception("Expected - {0} \t Actual- {1}".format(self.link_info["dataverse"], response[0]["dataverse"]))
                        if not (response[0]["name"] == self.link_info["name"]):
                            raise Exception("Expected - {0} \t Actual- {1}".format(self.link_info["name"], response[0]["name"]))
                        if not (response[0]["type"] == self.link_info["type"]):
                            raise Exception("Expected - {0} \t Actual- {1}".format(self.link_info["type"], response[0]["type"]))
                        if not (response[0]["bootstrapHostname"] == self.link_info["hostname"]):
                            raise Exception("Expected - {0} \t Actual- {1}".format(self.link_info["hostname"], response[0]["bootstrapHostname"]))
                        if not (response[0]["username"] == self.link_info["username"]):
                            raise Exception("Expected - {0} \t Actual- {1}".format(self.link_info["username"], response[0]["username"]))
                        if not (response[0]["encryption"] == self.link_info["encryption"]):
                            raise Exception("Expected - {0} \t Actual- {1}".format(self.link_info["encryption"], response[0]["encryption"]))
                        if not (response[0]["certificate"] == self.link_info.get("certificate", None)):
                            raise Exception("Expected - {0} \t Actual- {1}".format(self.link_info["certificate"], response[0]["certificate"]))
                        if not (response[0]["clientCertificate"] == self.link_info.get("clientCertificate",None)):
                            raise Exception("Expected - {0} \t Actual- {1}".format(self.link_info["clientCertificate"], response[0]["clientCertificate"]))
                        if self.link_info["encryption"] == "full":
                            if not (response[0]["clientKey"] == "<redacted sensitive entry>"):
                                raise Exception("Expected - {0} \t Actual- {1}".format("<redacted sensitive entry>", response[0]["clientKey"]))
                            if not (response[0]["password"] == self.link_info["password"]):
                                raise Exception("Expected - {0} \t Actual- {1}".format(self.link_info["password"], response[0]["password"]))
                        else:
                            if not (response[0]["password"] == "<redacted sensitive entry>"):
                                raise Exception("Expected - {0} \t Actual- {1}".format("<redacted sensitive entry>", response[0]["password"]))
                            if not (response[0]["clientKey"] == self.link_info.get("clientKey",None)):
                                raise Exception("Expected - {0} \t Actual- {1}".format(self.link_info["clientKey"], response[0]["clientKey"]))
                    self.log.info("Test Passed")
                except Exception as err:
                    self.log.error(str(err))
                    failed_testcases.append(testcase["description"])
            if failed_testcases:
                self.fail("Following testcases failed - {0}".format(str(failed_testcases)))
        else:
            self.fail("Link creation failed")

    def restore_link_to_original(self):
        self.log.info("Resetting link to original state")
        if not self.analytics_cluster.cbas_util.disconnect_link(
            link_name="{0}.{1}".format(self.link_info["dataverse"], self.link_info["name"])):
            raise Exception( "Error while Disconnecting the link ")
        if not self.analytics_cluster.cbas_util.update_external_link_properties(self.link_info):
            raise Exception( "Error while restoring link to original config")
        if not self.analytics_cluster.cbas_util.connect_link(
            link_name="{0}.{1}".format(self.link_info["dataverse"], self.link_info["name"]), with_force=True):
            raise Exception( "Error while connecting the link ")

    def validate_alter_link(self,to_cluster, link_properties, operation="update", timeout=300):

        mutation_num = random.randint(0,100)

        task = self.perform_doc_ops_in_all_cb_buckets(
            operation=operation, end_key=10, _async=True,
            cluster=to_cluster, buckets=[self.sample_bucket], key=self.key, mutation_num=mutation_num)
        result = self.task.jython_task_manager.get_task_result(task)

        if operation == "create":
            if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete(
                ["{0}.{1}".format(self.link_info["dataverse"],self.cbas_dataset_name)],
                self.sample_bucket.scopes[CbServer.default_scope].collections[
                    CbServer.default_collection].num_items + 10, 300):
                raise Exception("Data Ingestion did not complete")
        else:
            end_time = time.time() + timeout

            while time.time() < end_time:
                cmd_get_num_mutated_items = "select count(*) from {0}.{1} where mutated={2};".format(
                    self.link_info["dataverse"],self.cbas_dataset_name, str(mutation_num))
                status, metrics, errors, results, _ = self.analytics_cluster.cbas_util.execute_statement_on_cbas_util(
                    cmd_get_num_mutated_items, timeout=timeout, analytics_timeout=timeout)
                if status != "success":
                    self.log.error("Query failed")
                    raise Exception("CBAS query failed")
                else:
                    if results[0]['$1'] == 10:
                        break
                    else:
                        self.sleep(10, "Will retry CBAS query after sleep")

        response2 = self.analytics_cluster.cbas_util.get_link_info(
            link_name=link_properties["name"],
            dataverse=link_properties["dataverse"])[0]
        if not (response2["bootstrapHostname"] == link_properties["hostname"]):
            raise Exception("Hostname does not match. Expected - {0}\nActual - {1}".format(
                link_properties["hostname"], response2["bootstrapHostname"]))
        if not (response2["username"] == link_properties["username"]):
            raise Exception("Hostname does not match. Expected - {0}\nActual - {1}".format(
                link_properties["username"], response2["username"]))
        return True

    def test_alter_link_properties(self):
        
        if self.input.param("multipart_dataverse", False):
            dataverse_name = Dataset.create_name_with_cardinality(2)
            if not self.analytics_cluster.cbas_util.validate_dataverse_in_metadata(
                dataverse_name) and not self.analytics_cluster.cbas_util.create_dataverse_on_cbas(
                    dataverse_name=Dataset.format_name(dataverse_name)):
                self.fail("Creation of Dataverse {0} failed".format(
                    dataverse_name))
            invalid_dv = "invalid.invalid"
        else:
            dataverse_name = "Default"
            invalid_dv = "invalid"
            
        to_cluster = None
        while not to_cluster:
            to_cluster = random.choice(self.to_clusters)
            if len(to_cluster.servers) > 1:
                break
            else:
                to_cluster = None
        self.get_link_property_dict(to_cluster,
                                    dataverse_name=Dataset.format_name(dataverse_name))
        self.setup_datasets(to_cluster=to_cluster,get_link_info=False)
        
        task = self.perform_doc_ops_in_all_cb_buckets(
            operation="create", end_key=10, _async=True,
            cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)
        result = self.task.jython_task_manager.get_task_result(task)
        if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete(
            ["{0}.{1}".format(self.link_info["dataverse"],self.cbas_dataset_name)],
            self.sample_bucket.scopes[CbServer.default_scope].collections[
                CbServer.default_collection].num_items + 10, 300):
                raise Exception("Data Ingestion did not complete")

        # Create users with all RBAC roles.
        #self.create_or_delete_users(self.analytics_cluster.rbac_util, rbac_users_created)

        self.setup_certs()

        ip_list = []
        for server in to_cluster.servers:
            ip_list.append(server.ip)

        ip_list = self.remove_and_return_new_list(ip_list, to_cluster.master.ip)

        testcases = [
            {
                "description": "Changing dataverse to another existing dataverse",
                "validate_error_msg": True,
                "expected_error": "Link {0} does not exist".format(self.link_info["name"]),
                "new_dataverse": True
                },
            {
                "description": "Changing dataverse to a non-existing dataverse",
                "invalid_dataverse": invalid_dv,
                "new_dataverse": True,
                "validate_error_msg": True,
                "expected_error": "Cannot find dataverse with name {0}".format(invalid_dv)
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
                "hostname": self.remove_and_return_new_list(self.to_clusters, to_cluster)[0].master.ip,
                "load_sample_bucket": True,
                "validate_connect_error": True,
                "expected_connect_error": "Bucket UUID has changed"
                },
            {
                "description": "Changing hostname to another cluster with force flag on connect link",
                "hostname": self.remove_and_return_new_list(self.to_clusters, to_cluster)[0].master.ip,
                "load_sample_bucket": True,
                "with_force": True
                },
            {
                "description": "Changing hostname to an invalid hostname",
                "hostname": self.invalid_ip,
                "validate_error_msg": True,
                "expected_error": "Cannot connect to host for link {0}".format(
                        Dataset.format_name_for_error(True,
                                                      self.link_info["dataverse"], 
                                                      self.link_info["name"]))
                },
            {
                "description": "Changing credentials to invalid credentials",
                "password": self.invalid_value,
                "validate_error_msg": True,
                "expected_error": "Invalid credentials for link {0}".format(
                    Dataset.format_name_for_error(True,
                                                  self.link_info["dataverse"], 
                                                  self.link_info["name"]))
                },
            {
                "description": "Changing credentials to another set of valid credentials",
                "new_user": "bucket_full_access[*]",
                },
            {
                "description": "Changing credentials to a user which has less than minimum role required",
                "new_user": "analytics_reader",
                "validate_connect_error": True,
                "expected_connect_error": "Connect link failed"
                },
            {
                "description": "Changing encryption type to half",
                "encryption": "half"
                },
            {
                "description": "Changing encryption type to full, with valid root certificate",
                "encryption": "full",
                "certificate": self.read_file(to_cluster.root_ca_path)
                },
            {
                "description": "Changing encryption type to full, with invalid root certificate",
                "encryption": "full",
                "certificate": self.read_file(self.analytics_cluster.root_ca_path),
                "validate_error_msg": True,
                "expected_error": "Cannot connect to host for link {0}".format(
                        Dataset.format_name_for_error(True,
                                                      self.link_info["dataverse"], 
                                                      self.link_info["name"]))
                },
            {
                "description": "Changing encryption type to full, with valid root certificate, clientcertificate and client key",
                "encryption": "full",
                "certificate": self.read_file(to_cluster.root_ca_path),
                "clientCertificate": self.read_file(to_cluster.client_certs[self.link_info["hostname"]]["cert_pem"]),
                "clientKey": self.read_file(to_cluster.client_certs[self.link_info["hostname"]]["cert_key"]),
                "username": None,
                "password": None
                },
            {
                "description": "Changing encryption type to full, with valid root certificate and clientcertificate and invalid client key",
                "encryption": "full",
                "username": None,
                "password": None,
                "certificate": self.read_file(to_cluster.root_ca_path),
                "clientCertificate": self.read_file(to_cluster.client_certs[self.link_info["hostname"]]["cert_pem"]),
                "clientKey": self.read_file(
                    self.analytics_cluster.client_certs[self.analytics_cluster.master.ip]["cert_key"]),
                "validate_error_msg": True,
                "expected_error": "Cannot connect to host for link {0}".format(
                        Dataset.format_name_for_error(True,
                                                      self.link_info["dataverse"], 
                                                      self.link_info["name"]))
                },
            {
                "description": "Changing encryption type to full, with valid root certificate and clientKey and invalid clientcertificate",
                "encryption": "full",
                "username": None,
                "password": None,
                "certificate": self.read_file(to_cluster.root_ca_path),
                "clientKey": self.read_file(to_cluster.client_certs[self.link_info["hostname"]]["cert_key"]),
                "clientCertificate": self.read_file(self.analytics_cluster.client_certs[self.analytics_cluster.master.ip]["cert_pem"]),
                "validate_error_msg": True,
                "expected_error": "Cannot connect to host for link {0}".format(
                        Dataset.format_name_for_error(True, self.link_info["dataverse"], self.link_info["name"]))
                }
            ]

        rbac_testcases = self.create_testcase_for_rbac_user("Altering external link properties using {0} user",
                                                            rbac_users_created)
        for testcase in rbac_testcases:
            testcase["encryption"] = "half"
            if testcase["validate_error_msg"]:
                testcase["expected_error"] = "Unauthorized user"

        testcases = testcases + rbac_testcases
        failed_testcases = list()

        reset_original = False
        for testcase in testcases:
            try:
                if reset_original:
                    self.restore_link_to_original()

                self.log.info(testcase["description"])

                link_properties = copy.deepcopy(self.link_info)
                # Copying link property values for the testcase, to link_property dict, to create a new link.
                for key, value in testcase.iteritems():
                    if key == "username" and not testcase[key]:
                        link_properties[key] = value
                    elif key in link_properties and key != "username":
                        link_properties[key] = value

                # disconnect link before altering
                self.log.info("Disconnecting link before altering it's properties")
                if not self.analytics_cluster.cbas_util.disconnect_link(
                    link_name="{0}.{1}".format(link_properties["dataverse"], link_properties["name"])):
                    raise Exception( "Error while Disconnecting the link ")

                if testcase.get("load_sample_bucket", False):
                    if not self.to_clusters[0].bucket_util.load_sample_bucket(self.sample_bucket):
                        raise Exception("Error while loading {0} bucket in remote cluster".format(self.sample_bucket.name))
                elif testcase.get("new_dataverse",False):
                    if testcase.get("invalid_dataverse",None):
                        link_properties["dataverse"] = testcase.get("invalid_dataverse")
                    else:
                        link_properties["dataverse"] = "dataverse2"
                        if not self.analytics_cluster.cbas_util.create_dataverse_on_cbas(
                            dataverse_name=link_properties["dataverse"], username=self.analytics_username):
                            raise Exception("Dataverse creation failed")
                elif testcase.get("new_user",None):
                    link_properties["username"] = testcase["new_user"].replace("[*]","")
                    to_cluster.rbac_util._create_user_and_grant_role(testcase["new_user"].replace("[*]",""),
                                                                     testcase["new_user"])

                #Altering link
                self.log.info("Altering link properties")
                response = self.analytics_cluster.cbas_util.update_external_link_properties(
                    link_properties, validate_error_msg=testcase.get("validate_error_msg", False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get("username", self.analytics_username))

                if testcase.get("validate_error_msg",False) or testcase.get("validate_connect_error",False) or testcase.get("with_force",False):
                    if not response:
                        raise Exception("Error message is different than expected.")

                    if testcase.get("new_dataverse", False):
                        if not testcase.get("invalid_dataverse",None) and not \
                        self.analytics_cluster.cbas_util.drop_dataverse_on_cbas(
                            dataverse_name=link_properties["dataverse"], username=self.analytics_username):
                            raise Exception("Dataverse creation failed")
                        link_properties["dataverse"] = self.link_info["dataverse"]

                    self.log.info("Connecting link after altering link properties")
                    if self.analytics_cluster.cbas_util.connect_link(
                        link_name="{0}.{1}".format(link_properties["dataverse"], link_properties["name"]),
                        with_force=testcase.get("with_force",False),
                        validate_error_msg=testcase.get("validate_connect_error",False),
                        expected_error=testcase.get("expected_connect_error",None)):

                        if testcase.get("with_force",False):
                            self.validate_alter_link(self.to_clusters[0], link_properties, "create")
                        if testcase.get("load_sample_bucket", False) and not self.to_clusters[0].bucket_util.delete_bucket(
                            self.to_clusters[0].master, self.sample_bucket):
                            raise Exception("Error while deleting {0} bucket in remote cluster".format(self.sample_bucket.name))
                        reset_original = True
                    else:
                        raise Exception( "Error while connecting the link ")
                else:
                    self.log.info("Connecting link after altering link properties")
                    if not self.analytics_cluster.cbas_util.connect_link(
                        link_name="{0}.{1}".format(link_properties["dataverse"], link_properties["name"])):
                        raise Exception( "Error while connecting the link ")
                    self.validate_alter_link(to_cluster, link_properties)
                    if not response:
                        raise Exception("Altering link properties failed.")

                if testcase.get("new_user",None):
                    to_cluster.rbac_util._drop_user(testcase["new_user"].replace("[*]",""))

                self.log.info("Test Passed")
            except Exception as err:
                self.log.error(str(err))
                reset_original = True
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))

    def test_connect_link(self):
        to_cluster = self.setup_datasets(connect_link=False)

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster.rbac_util, rbac_users_created)

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

        rbac_testcases = self.create_testcase_for_rbac_user("Connect external link using {0} user",
                                                            rbac_users_created)
        for testcase in rbac_testcases:
            testcase["name"] = self.link_info["name"]

        testcases = testcases + rbac_testcases
        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])
                if self.analytics_cluster.cbas_util.connect_link(
                    link_name="{0}.{1}".format(self.link_info["dataverse"], testcase["name"]),
                    validate_error_msg=testcase.get("validate_error_msg", False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get("username", self.analytics_username)):
                    if not testcase.get("validate_error_msg", False) and not self.analytics_cluster.cbas_util.disconnect_link(
                        link_name="{0}.{1}".format(self.link_info["dataverse"], testcase["name"])):
                        raise Exception( "Error while Disconnecting the link ")
                else:
                    raise Exception("Error while connecting the link ")

                self.log.info("Test Passed")
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))

    def test_disconnect_link(self):
        to_cluster = self.setup_datasets(connect_link=False)

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster.rbac_util, rbac_users_created)

        testcases = [
            {"name": self.link_info["name"],
             "description": "Disconnect a valid link",
                }
            ]

        rbac_testcases = self.create_testcase_for_rbac_user("Disconnect external link using {0} user",
                                                            rbac_users_created)
        for testcase in rbac_testcases:
            testcase["name"] = self.link_info["name"]

        testcases = testcases + rbac_testcases

        failed_testcases = list()

        connect_link = True

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])
                if connect_link and not self.analytics_cluster.cbas_util.connect_link(
                    link_name="{0}.{1}".format(self.link_info["dataverse"], testcase["name"])):
                    raise Exception("Error while connecting the link ")
                if self.analytics_cluster.cbas_util.disconnect_link(
                    link_name="{0}.{1}".format(self.link_info["dataverse"], testcase["name"]),
                    validate_error_msg=testcase.get("validate_error_msg", False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get("username", self.analytics_username)):
                    connect_link = True
                else:
                    connect_link = False
                    raise Exception( "Error while Disconnecting the link ")
                self.log.info("Test Passed")
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))

    def test_create_dataset(self):
        to_cluster = random.choice(self.to_clusters)
        self.get_link_property_dict(to_cluster)

        if not self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = self.link_info):
            self.fail("link creation failed")
        self.link_created = True

        if not to_cluster.bucket_util.load_sample_bucket(self.sample_bucket):
            self.fail("Error while loading {0} bucket in remote cluster".format(self.sample_bucket))

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster.rbac_util, rbac_users_created)

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

        rbac_testcases = self.create_testcase_for_rbac_user("Creating dataset on external link using {0} user",
                                                            rbac_users_created)
        for testcase in rbac_testcases:
            testcase["dataset_name"] = self.cbas_dataset_name
            testcase["bucket_name"] = self.sample_bucket.name

        testcases = testcases + rbac_testcases

        failed_testcases = list()

        for testcase in testcases:
            try:
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
                    raise Exception("Error while creating dataset")
                if not testcase.get("validate_error_msg", False):
                    if not self.analytics_cluster.cbas_util.validate_dataset_in_metadata(dataset_name=testcase["dataset_name"],
                                                                                         dataverse=self.link_info["dataverse"],
                                                                                         LinkName=self.link_info["name"],
                                                                                         BucketName=testcase["bucket_name"]):
                        raise Exception("Dataset entry not present in Metadata.Dataset collection")
                    self.analytics_cluster.cbas_util.drop_dataset(cbas_dataset_name=testcase["dataset_name"],
                                                             dataverse=self.link_info["dataverse"])
                self.log.info("Test Passed")
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))

    def test_query_dataset(self):
        to_cluster = self.setup_datasets()

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.analytics_cluster.rbac_util, rbac_users_created)

        testcases = self.create_testcase_for_rbac_user("Querying dataset on external link using {0} user",
                                                       rbac_users_created)

        query_statement = "select * from `{0}` where `id` <= 10;"

        n1ql_result = to_cluster.rest.query_tool(query_statement.format(self.sample_bucket.name))[
            'results'][0][self.sample_bucket.name]

        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])
                if testcase.get("username") in ["data_backup", "analytics_reader"]:
                    testcase["validate_error_msg"] = False
                    testcase["expected_error"] = None
                if testcase.get("username") in ["analytics_admin", "admin_analytics"]:
                    testcase["validate_error_msg"] = True
                    testcase["expected_error"] = "Unauthorized user"
                status, metrics, errors, cbas_result, _ = self.analytics_cluster.cbas_util.execute_statement_on_cbas_util(
                    statement=query_statement.format(self.cbas_dataset_name),
                    username=testcase.get("username", self.analytics_username))
                if testcase.get("validate_error_msg",False):
                    if not self.analytics_cluster.cbas_util.validate_error_in_response(
                        status, errors, expected_error=testcase.get("expected_error", None)):
                        raise Exception("Error msg is different from expected error msg")
                elif not cbas_result[0][self.cbas_dataset_name] == n1ql_result:
                    raise Exception("Result returned from analytics query does not match the result of N1Ql query")
                self.log.info("Test passed")
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))

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
                                                                            self.sample_bucket.scopes[
                                                                                CbServer.default_scope].collections[
                                                                                    CbServer.default_collection].num_items + 1000, 300):
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
                                                                                  self.sample_bucket.scopes[
                                                                                      CbServer.default_scope].collections[
                                                                                          CbServer.default_collection].num_items + self.num_items,
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
                                                                            self.sample_bucket.scopes[
                                                                                CbServer.default_scope].collections[
                                                                                    CbServer.default_collection].num_items + self.num_items,
                                                                            300):
            self.fail("Data Ingestion did not complete")

    def test_data_ingestion_after_daleting_and_then_recreating_remote_user(self):
        to_cluster = random.choice(self.to_clusters)

        # Create a new high privileged role remote user
        new_username = "user1"
        original_user_role = "bucket_full_access[*]"

        timeout = 120
        analytics_timeout = 120

        if self.input.param("has_bucket_access", False) and self.input.param("same_role", False):
            new_role = original_user_role
        elif self.input.param("has_bucket_access", False) and not self.input.param("same_role", False):
            new_role = "admin"
        else:
            new_role = "analytics_admin"
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
                self.sleep(10, "waiting for link to auto disconnect")

            self.log.info("Validate not all new data has been ingested into dataset")
            if self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                  self.sample_bucket.scopes[
                                                                                      CbServer.default_scope].collections[
                                                                                          CbServer.default_collection].num_items + self.num_items,
                                                                                  num_tries=1):
                self.fail("New data was ingested into dataset even after remote user permission was revoked.")

            to_cluster.rbac_util._create_user_and_grant_role(new_username, new_role)
            # Reconnecting link.
            if not self.analytics_cluster.cbas_util.connect_link("{0}.{1}".format(self.link_info["dataverse"],
                                                                                  self.link_info["name"]),
                                                                 username=self.analytics_username,
                                                                 validate_error_msg=self.input.param("validate_error_msg",
                                                                                                     False),
                                                                 expected_error=self.input.param("error_msg", None),
                                                                 timeout=timeout, analytics_timeout=analytics_timeout):
                self.fail("Error while connecting link")

        else:
            # Restoring remote user permission before timeout
            to_cluster.rbac_util._create_user_and_grant_role(new_username, new_role)

        # Allow ingestion to complete
        if not self.input.param("has_bucket_access", False):
            if self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                            self.sample_bucket.scopes[
                                                                                CbServer.default_scope].collections[
                                                                                    CbServer.default_collection].num_items + self.num_items,
                                                                            300):
                self.fail("Data Ingestion started when it should not.")
        else:
            if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                                self.sample_bucket.scopes[
                                                                                    CbServer.default_scope].collections[
                                                                                        CbServer.default_collection].num_items + self.num_items,
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
                                         all_interface=True,action_on_packet="DROP")

        def sleep_and_bring_network_up():
            time.sleep(60)
            to_cluster.cluster_util.stop_firewall_on_node(to_cluster.master)

        if self.input.param("network_up_before_timeout", False):
            thread = Thread(target=sleep_and_bring_network_up,
                            name="connect_link_thread")
            thread.start()

        if not self.analytics_cluster.cbas_util.create_dataset_on_bucket(cbas_bucket_name= self.sample_bucket.name,
                                                                         cbas_dataset_name= self.cbas_dataset_name,
                                                                         dataverse=self.link_info["dataverse"],
                                                                         link_name="{0}.{1}".format(
                                                                             self.link_info["dataverse"],
                                                                             self.link_info["name"]),
                                                                         validate_error_msg=self.input.param("validate_error",
                                                                                                             False),
                                                                         expected_error=self.input.param("expected_error", None),
                                                                         timeout=360, analytics_timeout=360):
            to_cluster.cluster_util.stop_firewall_on_node(to_cluster.master)
            self.fail("Error while creating dataset")
        to_cluster.cluster_util.stop_firewall_on_node(to_cluster.master)
        self.dataset_created = True

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
                                                                            self.sample_bucket.scopes[
                                                                                CbServer.default_scope].collections[
                                                                                    CbServer.default_collection].num_items, 300):
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
                                                                            self.sample_bucket.scopes[
                                                                                CbServer.default_scope].collections[
                                                                                    CbServer.default_collection].num_items + self.num_items,
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
                                                                            self.sample_bucket.scopes[
                                                                                CbServer.default_scope].collections[
                                                                                    CbServer.default_collection].num_items,
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
                                                                                  self.sample_bucket.scopes[
                                                                                      CbServer.default_scope].collections[
                                                                                          CbServer.default_collection].num_items):
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
                                                                                                      self.sample_bucket.scopes[
                                                                                                          CbServer.default_scope].collections[
                                                                                                              CbServer.default_collection].num_items):
            self.fail("Mismatch between number of documents in dataset and remote bucket")

    def test_data_ingestion_for_data_updation_in_remote_cluster(self):
        to_cluster = self.perform_setup_and_add_new_docs(10, wait_for_ingestion=False)

        task = self.perform_doc_ops_in_all_cb_buckets(operation="update", end_key=10, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key,
                                                      mutation_num=1)
        result = self.task.jython_task_manager.get_task_result(task)

        if not self.analytics_cluster.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],
                                                                            self.sample_bucket.scopes[
                                                                                CbServer.default_scope].collections[
                                                                                    CbServer.default_collection].num_items + 10,
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
                                                                            self.sample_bucket.scopes[
                                                                                CbServer.default_scope].collections[
                                                                                    CbServer.default_collection].num_items,
                                                                            300):
            self.fail("Data Ingestion did not complete")

    def get_rebalance_servers(self):
        node_ip = list()
        for cluster in self._cb_cluster:
            node_ip.extend([i.ip for i in cluster.rest.node_statuses()])
        rebalanceServers = list()
        for server in self.servers:
            if not (server.ip in node_ip):
                rebalanceServers.append(server)
        return rebalanceServers

    def test_analytics_cluster_while_remote_cluster_swap_rebalancing(self):
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

        rebalanceServers = self.get_rebalance_servers()

        self.log.info("Rebalance in remote cluster, this node will be removed during swap")
        to_cluster.cluster_util.add_node(node=rebalanceServers[0], services=node_services)


        self.log.info("Run KV ops in async while rebalance is in progress")
        tasks = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=self.num_items, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)

        run_query = self.input.param("run_query", False)
        if run_query:
            self.log.info("Run concurrent queries to simulate busy system")
            statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.cbas_dataset_name)
            handles = self.analytics_cluster.cbas_util._run_concurrent_queries(statement, "async",
                                                                               int(self.input.param("num_queries", 0)),
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

        if run_query:
            self.log.info("Log concurrent query status")
            self.analytics_cluster.cbas_util.log_concurrent_query_outcome(self.analytics_cluster.master, handles)

        if not self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                  self.sample_bucket.scopes[
                                                                                      CbServer.default_scope].collections[
                                                                                          CbServer.default_collection].num_items +
                                                                                  self.num_items):
            self.fail("Number of items in dataset do not match number of items in bucket")


    def test_analytics_cluster_when_rebalancing_in_cbas_node(self):
        '''
        1. We have 2 clusters, local cluster, remote cluster and 4 nodes - 101, 102, 103, 104.
        2, Post initial setup - local cluster - 1 node with cbas, remote cluster - 1 node with KV and query running
        3. As part of test add an extra cbas nodes and rebalance

        Data mutation is happening on remote cluster while local cluster is rebalancing.
        '''
        node_services = ["cbas"]

        self.log.info("Setup CBAS")
        to_cluster = self.setup_datasets()

        rebalanceServers = self.get_rebalance_servers()

        self.log.info("Run KV ops in async while rebalance is in progress")
        tasks = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=self.num_items, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)

        run_query = self.input.param("run_query", False)
        if run_query:
            self.log.info("Run concurrent queries to simulate busy system")
            statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.cbas_dataset_name)
            handles = self.analytics_cluster.cbas_util._run_concurrent_queries(statement, "async",
                                                                               int(self.input.param("num_queries", 0)),
                                                                               wait_for_execution=False)

        self.log.info("Rebalance in CBAS nodes")
        self.analytics_cluster.cluster_util.add_node(node=rebalanceServers[0], services=node_services,
                                                rebalance=False, wait_for_rebalance_completion=False)
        self.analytics_cluster.cluster_util.add_node(node=rebalanceServers[1], services=node_services,
                                                rebalance=True, wait_for_rebalance_completion=True)

        self.log.info("Get KV ops result")
        self.task_manager.get_task_result(tasks)

        if run_query:
            self.log.info("Log concurrent query status")
            self.analytics_cluster.cbas_util.log_concurrent_query_outcome(self.analytics_cluster.master, handles)

        if not self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                  self.sample_bucket.scopes[
                                                                                      CbServer.default_scope].collections[
                                                                                          CbServer.default_collection].num_items
                                                                                  + self.num_items, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")


    def test_analytics_cluster_swap_rebalancing(self):
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

        rebalanceServers = self.get_rebalance_servers()

        self.log.info("Rebalance in local cluster, this node will be removed during swap")
        self.analytics_cluster.cluster_util.add_node(node=rebalanceServers[0], services=node_services)

        self.log.info("Run KV ops in async while rebalance is in progress")
        tasks = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=self.num_items, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)

        run_query = self.input.param("run_query", False)
        if run_query:
            self.log.info("Run concurrent queries to simulate busy system")
            statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.cbas_dataset_name)
            handles = self.analytics_cluster.cbas_util._run_concurrent_queries(statement, "async",
                                                                               int(self.input.param("num_queries", 0)),
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

        if run_query:
            self.log.info("Log concurrent query status")
            self.analytics_cluster.cbas_util.log_concurrent_query_outcome(self.analytics_cluster.master, handles)

        if not self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                  self.sample_bucket.scopes[
                                                                                      CbServer.default_scope].collections[
                                                                                          CbServer.default_collection].num_items
                                                                                  + self.num_items):
            self.fail("Number of items in dataset do not match number of items in bucket")

    def test_analytics_cluster_when_rebalancing_out_cbas_node(self):
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

        rebalanceServers = self.get_rebalance_servers()

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
                                                                               int(self.input.param("num_queries", 0)),
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

        if run_query:
            self.log.info("Log concurrent query status")
            self.analytics_cluster.cbas_util.log_concurrent_query_outcome(self.analytics_cluster.master, handles)

        if not self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                                                  self.sample_bucket.scopes[
                                                                                      CbServer.default_scope].collections[
                                                                                          CbServer.default_collection].num_items
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

        rebalanceServers = self.get_rebalance_servers()

        self.log.info("Add an extra node to fail-over")
        self.analytics_cluster.cluster_util.add_node(node=rebalanceServers[0], services=node_services,
                                                     rebalance=True, wait_for_rebalance_completion=True)

        self.log.info("Run KV ops in async while rebalance is in progress")
        tasks = self.perform_doc_ops_in_all_cb_buckets(operation="create", end_key=self.num_items, _async=True,
                                                      cluster=to_cluster, buckets=[self.sample_bucket], key=self.key)

        run_query = self.input.param("run_query", False)
        if run_query:
            self.log.info("Run concurrent queries to simulate busy system")
            statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.cbas_dataset_name)
            handles = self.analytics_cluster.cbas_util._run_concurrent_queries(statement, "async",
                                                                               int(self.input.param("num_queries", 0)),
                                                                               wait_for_execution=False)

        self.log.info("fail-over the node")
        if not self.task.failover(self.analytics_cluster.servers, failover_nodes=[rebalanceServers[0]],
                                  graceful=False, wait_for_pending=300):
            self.fail("Error while node failover")

        self.log.info("Read input param to decide on add back or rebalance out")
        self.rebalance_out = self.input.param("rebalance_out", False)
        self.sleep(10, "Sleeping before removing or adding back the failed over node")
        if self.rebalance_out:
            self.log.info("Rebalance out the fail-over node")
            result = self.analytics_cluster.cluster_util.rebalance()
            self.assertTrue(result, "Rebalance operation failed")
        else:
            self.recovery_strategy = self.input.param("recovery_strategy", "full")
            self.analytics_cluster.rest.set_recovery_type('ns_1@' + rebalanceServers[0].ip, self.recovery_strategy)
            result = self.analytics_cluster.cluster_util.rebalance()
            self.assertTrue(result, "Rebalance operation failed")

        self.log.info("Get KV ops result")
        self.task_manager.get_task_result(tasks)

        if run_query:
            self.log.info("Log concurrent query status")
            self.analytics_cluster.cbas_util.log_concurrent_query_outcome(self.analytics_cluster.master, handles)

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
        
    def test_remote_link_lifecycle_with_multi_part_dataset_and_kv_entity_name(self):
        """
        This testcase validates creation of remote link, creation of dataset on 
        remote link, connection of remote link, validation of data ingestion into dataset,
        dropping of dataset, disconnecting of remote link and dropping of remote link, with 
        2 part dataverse name with different remote link encryption types.
        :testparam encryption:
        :testparam cardinality
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
        to_cluster = random.choice(self.to_clusters)
        
        self.log.info("Loading bucket, scopes and collections")
        self.bucket_spec = "analytics.single_bucket"
        self.collectionSetUp(to_cluster, to_cluster.bucket_util, to_cluster.cluster_util)
        
        dataset_obj = Dataset(
            bucket_util=to_cluster.bucket_util,
            cbas_util=self.analytics_cluster.cbas_util,
            consider_default_KV_scope=True, 
            consider_default_KV_collection=True,
            dataset_name_cardinality=int(self.input.param('cardinality', 1)),
            bucket_cardinality=int(self.input.param('bucket_cardinality', 3)),
            random_dataset_name=True
            )
        
        encryption = self.input.param("encryption","none")
        
        if encryption in ["full1","full2"]:
            encryption = "full"
        
        self.get_link_property_dict(to_cluster,encryption=encryption,
                                    dataverse_name=Dataset.format_name(dataset_obj.dataverse))
        
        full_link_name = ".".join([self.link_info["dataverse"],self.link_info["name"]])
        
        if not self.analytics_cluster.cbas_util.validate_dataverse_in_metadata(
            dataset_obj.dataverse) and not self.analytics_cluster.cbas_util.create_dataverse_on_cbas(
                dataverse_name=self.link_info["dataverse"]):
            self.fail("Creation of Dataverse {0} failed".format(self.link_info["dataverse"]))
        
        if self.input.param("encryption","none") == "full1":
            self.link_info["username"] = to_cluster.master.rest_username
            self.link_info["password"] = to_cluster.master.rest_password
            self.link_info["clientCertificate"] = None
            self.link_info["clientKey"] = None
        
        self.log.info("Create remote link with {0} encryption".format(encryption))
        if not self.analytics_cluster.cbas_util.create_external_link_on_cbas(link_properties = self.link_info):
            self.fail("Creation of remote link with encryption {0} failed".format(encryption))
        
        # Negative scenario 
        if self.input.param('error', None):
            error_msg = self.input.param('error', None)
        else:
            error_msg = None
        
        original_link_name = ""
        
        if self.input.param('invalid_kv_collection', False):
            dataset_obj.kv_collection_obj.name = "invalid"
            error_msg = error_msg.format(
                Dataset.format_name_for_error(
                    True, dataset_obj.get_fully_quantified_kv_entity_name(
                        dataset_obj.bucket_cardinality)))
        elif self.input.param('invalid_kv_scope', False):
            dataset_obj.kv_scope_obj.name = "invalid"
            error_msg = error_msg.format(
                Dataset.format_name_for_error(
                    True, dataset_obj.get_fully_quantified_kv_entity_name(2)))
        elif self.input.param('invalid_dataverse', False):
            dataset_obj.dataverse  = "invalid"
            error_msg = error_msg.format("invalid")
        elif self.input.param('name_length', 0):
            dataset_obj.dataverse, dataset_obj.name = dataset_obj.split_dataverse_dataset_name(
                dataset_obj.create_name_with_cardinality(
                    1, int(self.input.param('name_length', 0)), True))
        elif self.input.param('no_dataset_name', False):
            dataset_obj.name = ''
        elif self.input.param('remove_default_collection', False):
            error_msg = error_msg.format(
                Dataset.format_name_for_error(True,
                                              dataset_obj.get_fully_quantified_kv_entity_name(2),
                                              "._default"))
        elif self.input.param("connect_invalid_link", False):
            original_link_name = self.link_info["name"]
            self.link_info["name"] = "invalid"
            error_msg = "Link {0} does not exist".format(
                Dataset.format_name_for_error(
                    True, self.link_info["dataverse"], self.link_info["name"]))
        # Negative scenario ends
        
        self.log.info("Creating dataset on link to remote cluster")
        if not dataset_obj.setup_dataset(
            dataset_creation_method=self.input.param('dataset_creation_method', "cbas_dataset"),
            validate_metadata=True, validate_doc_count=False, create_dataverse=False, 
            validate_error=self.input.param('validate_error', False), 
            error_msg=error_msg, username=None, password=None, timeout=120, analytics_timeout=120,
            link_name=full_link_name):
            self.fail("Error while creating dataset")
        
        if self.input.param('validate_error', False):
            dataset_created = False
        else:
            dataset_created = True
            
        self.log.info("Connecting remote link")
        if not self.analytics_cluster.cbas_util.connect_link(
            link_name=full_link_name,
            validate_error_msg=self.input.param("validate_link_error", False), expected_error=error_msg):
            self.fail("Error while connecting the link")
        
        if not self.input.param("connect_invalid_link", False) and dataset_created:
            doc_count = dataset_obj.get_item_count_in_collection(
                dataset_obj.bucket_util,dataset_obj.kv_bucket_obj, 
                dataset_obj.kv_scope_obj.name, dataset_obj.kv_collection_obj.name)
            if not self.analytics_cluster.cbas_util.validate_cbas_dataset_items_count(
                dataset_obj.full_dataset_name, doc_count,
                timeout=300, analytics_timeout=300):
                self.fail("Expected data does not match actual data")
        
        if dataset_created:
            self.log.info("Dropping dataset created on link to remote cluster")
            if not self.analytics_cluster.cbas_util.drop_dataset(
                cbas_dataset_name=dataset_obj.full_dataset_name):
                self.fail("Error while creating dataset")
        
        if not self.input.param("connect_invalid_link", False):
            self.log.info("Disconnecting remote link")
            if not self.analytics_cluster.cbas_util.disconnect_link(
                link_name=full_link_name):
                self.fail("Error while disconnecting the link")
        
        if original_link_name:
            self.link_info["name"] = original_link_name

        self.log.info("Dropping remote link with {0} encryption".format(encryption))
        if not self.analytics_cluster.cbas_util.drop_link_on_cbas(
            link_name=full_link_name,
            username=self.analytics_username):
            self.fail("Dropping remote link with encryption {0} failed".format(encryption))

        self.log.info("Test Passed")
        
        
        