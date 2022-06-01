from SecurityLib.rbac import RbacUtil
from basetestcase import ClusterSetup
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class AllowedHosts(ClusterSetup):
    def setUp(self):
        super(AllowedHosts, self).setUp()
        self.users_list = list()
        self.allowedhosts = "[\"*.couchbase.com\",\"10.112.0.0/16\",\"172.23.0.0/16\"]"
        self.shell = RemoteMachineShellConnection(self.cluster.master)

    def tearDown(self):
        self.shell.disconnect()
        super(AllowedHosts, self).tearDown()

    def __create_user(self, uname, password, roles, writer=True):
        user = [{'id': uname, 'name': uname, 'password': password}]
        role_list = [{'id': uname, 'name': uname, 'roles': roles}]

        rbac_util = RbacUtil()
        rest = RestConnection(self.cluster.master)

        self.log.debug("Dropping user %s if exists" % uname)
        rbac_util.remove_user_role([uname], rest)

        self.log.info("Creating user %s" % uname)
        rbac_util.create_user_source(user, 'builtin', self.cluster.master)
        status = rbac_util.add_user_role(role_list, rest, 'builtin')
        self.assertTrue(status, "User '%s' creation failed" % uname)
        user[0].update({'Writer': writer})
        self.users_list.append(user[0])

    def test_allowed_hosts_rest_apis(self):
        self.__create_user("user1", "password", "data_writer[*]:admin")
        self.__create_user("user2", "password", "data_writer[*]", False)
        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            for user in self.users_list:
                if user["Writer"]:
                    output = shell.set_allowedhosts("localhost", user["name"], user["password"],
                                                    self.allowedhosts)
                    if len(output[0]) > 2:
                        self.fail("Allowed hosts is not changed and error is {0}".format(output))
                    output = shell.get_allowedhosts(user["name"], user["password"])
                    self.assertEqual(output, self.allowedhosts)
            shell.disconnect()
        self.add_node_and_rebalance()

    def add_node_and_rebalance(self, expect_failure=False):
        rebalance_task = self.task.async_rebalance(
            self.cluster, [self.cluster.servers[self.nodes_init]], [])
        self.task.jython_task_manager.get_task_result(rebalance_task)
        if not rebalance_task.result and not expect_failure:
            self.fail("rebalance did not progress")

    def test_endpoints_with_unauthorised_users(self):
        # check for forbidden user
        error_msg1 = [
            '{"message":"Forbidden. User needs the following permissions","permissions":["cluster.admin.security!write"]}']
        error_msg2 = ['{"errors":["allowedHosts - can be modified from localhost only for security reasons"]}']
        self.__create_user("user1", "password", "ro_admin", False)
        self.__create_user("user2", "password", "data_reader[*]", False)
        self.__create_user("user3", "password", "data_dcp_reader[*]", False)
        self.__create_user("user4", "password", "query_manage_index[*]", False)
        self.__create_user("user5", "password", "data_writer[*]:admin")
        self.__create_user("user6", "password", "bucket_admin[*]:admin")
        self.__create_user("user6", "password", "cluster_admin:admin")

        for user in self.users_list:
            error = error_msg2
            if not user["Writer"]:
                error = error_msg1
                output = self.shell.set_allowedhosts("localhost", user["name"], user["password"],
                                                self.allowedhosts)
                self.assertEqual(output, error)
            output = self.shell.set_allowedhosts(self.cluster.master.ip, user["name"], user["password"],
                                            self.allowedhosts)
            self.assertEqual(output, error)
        self.add_node_and_rebalance()

    def test_invalid_entries(self):
        """ check all the invalid entries"""
        invalid_entries = ["-274.0.0", "56FE::2159:5BBC::6594", "10.-1.0.0/9", "10.0.0.0/145", "56GE::2159:5BBC::6594",
                           "*foo*.example.com", "*.xn--kcry6tjko*.example.org", "192.*.12.156",
                           "fe80::*:48ff:fe00:1122"]
        self.__create_user("user5", "password", "data_writer[*]:admin")
        for user in self.users_list:
            for host in invalid_entries:
                host = str("[\"") + str(host) + str("\"]")
                output = self.shell.set_allowedhosts("localhost", user["name"], user["password"], host)
                if "errors" not in output[0]:
                    self.fail("Invalid address should fail, address {0}".format(host))
        self.add_node_and_rebalance()

    def test_max_entries(self):
        """ check with more entries (50*255) entries in the allowed hosts,
         We can have as many entries as possible """
        hosts = "[\"10.112.0.0/16\""
        for i in range(50):
            for j in range(255):
                host = ",\"172.23." + str(i) + "." + str(j) + "\""
                hosts += host
        hosts += ",\"172.23.0.0/16\"" + "]"
        self.__create_user("user1", "password", "cluster_admin:admin")
        self.shell.set_allowedhosts("localhost", "user1", "password", hosts)
        self.add_node_and_rebalance()

    def test_host_not_allowed(self):
        """ check if the host is not allowed first and the
        change allowed hosts settings and validate
        if the node can be added successfully """
        self.__create_user("user1", "password", "cluster_admin:admin")
        host = str("[\"") + str(self.cluster.master.ip) + str("\"]")
        self.shell.set_allowedhosts("localhost", "user1", "password", host)
        self.add_node_and_rebalance(expect_failure=True)
        host = str("[\"*\"]")
        self.shell.set_allowedhosts("localhost", "user1", "password", host)
        self.add_node_and_rebalance(expect_failure=False)

    def test_wild_card_characters(self):
        """ this tests case mostly validates host names"""
        self.__create_user("user1", "password", "cluster_admin:admin")
        hosts = "[\"s*-ip6.qe.couchbase.com\", \"172.23.0.0/16\", \"*.qe.couchbase.com\"]"
        self.shell.set_allowedhosts("localhost", "user1", "password", hosts)
        self.add_node_and_rebalance()

    def test_cluster_init(self):
        """
        check if we can set allowed hosts through cluster init
        """
        node = self.cluster.servers[self.nodes_init]
        self.shell = RemoteMachineShellConnection(node)
        # expect cluster init to fail
        cmd = "curl -X POST %s:8091/clusterInit -d 'allowedHosts=%s'" \
              " -d username=%s -d port=8091 -d password=%s -d services=\"kv\" " % \
              (node.ip, "10.112.0.0", "Administrator", "password")
        output, _ = self.shell.execute_command(cmd)
        if "errors" not in output[0]:
            raise Exception("Expected allowedhosts to fail during clusterInit")
        # expect cluster init to pass
        cmd = "curl -X POST %s:8091/clusterInit -d 'allowedHosts=%s'" \
              " -d username=%s -d port=8091 -d password=%s -d services=\"kv\" " % \
              (node.ip, "10.112.0.0/16,172.23.0.0/16", "Administrator", "password")
        output, _ = self.shell.execute_command(cmd)
        if "errors" in output[0]:
            raise Exception("Not able to set allowedhosts during clusterInit")
        self.add_node_and_rebalance()
