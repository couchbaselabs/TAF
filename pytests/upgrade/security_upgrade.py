from BucketLib.bucket import TravelSample, BeerSample, GamesimSample, Bucket
from membase.api.rest_client import RestConnection
from rbac_utils.Rbac_ready_functions import RbacUtils

from upgrade.upgrade_base import UpgradeBase

class SecurityUpgradeTests(UpgradeBase):

    def setUp(self):
        super(SecurityUpgradeTests, self).setUp()
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)

        self.rbac_util = RbacUtils(self.cluster.master)

        # cbadminbucket user is added in OnPremBase setup
        self.users = {
            "cbadminbucket": {'name': 'cbadminbucket', 'password': 'password', 'roles': ['admin']},
        }

    def tearDown(self):
        super(SecurityUpgradeTests, self).tearDown()

    def _create_ro_admin_users_during_upgrade(self):
        user_count = len(self.users)

        for i in range(user_count, user_count + 5):
            user = "ro_admin_user_{}".format(i)
            password = "password"
            roles = "ro_admin"
            self.users[user] = {'name': user, 'password': password, 'roles': [roles]}

            self.log.info("Creating user: {0} with password {1} and roles: {2}".format(user, password, roles))
            self.rbac_util._create_user_and_grant_role(user, roles)


    def _update_users_during_upgrade(self, scenario):
        user_roles = {
            "Node 1 Upgrade": ["admin", "security_admin_local", "cluster_admin"],
            "Node 2 Upgrade": ["eventing_admin", "backup_admin", "external_stats_reader"],
            "Node 3 Upgrade": ["bucket_admin[*]", "data_reader[*]", "data_writer[*]"],
            "Post Upgrade": ["ro_security_admin", "security_admin", "ro_security_admin"]
        }

        # Update first user completely (remove ro_admin)
        user_to_update = "ro_admin_user_{}".format((len(self.users) - 5) + 1)
        role_to_update = user_roles[scenario][0]
        self.log.info("Updating user: {0} from roles {1} to roles: {2}".format(
            user_to_update, self.users[user_to_update]['roles'], [role_to_update]))
        self.rbac_util._update_user_roles(user_to_update, role_to_update)
        self.users[user_to_update]['roles'] = [role_to_update]

        # Update second user to have ro_admin and another role
        user_to_update = "ro_admin_user_{}".format((len(self.users) - 5) + 2)
        role_to_update = "ro_admin:" + user_roles[scenario][1]
        self.log.info("Updating user: {0} from roles {1} to roles: {2}".format(
            user_to_update, self.users[user_to_update]['roles'], [role_to_update]))
        self.rbac_util._update_user_roles(user_to_update, role_to_update)
        self.users[user_to_update]['roles'] = role_to_update.split(":")

        # Update third user to have ro_admin and another role
        user_to_update = "ro_admin_user_{}".format((len(self.users) - 5) + 3)
        role_to_update = "ro_admin:" + user_roles[scenario][2]
        self.log.info("Updating user: {0} from roles {1} to roles: {2}".format(
            user_to_update, self.users[user_to_update]['roles'], [role_to_update]))
        self.rbac_util._update_user_roles(user_to_update, role_to_update)
        self.users[user_to_update]['roles'] = role_to_update.split(":")

    def _delete_users_during_upgrade(self):
        # Delete the last user
        user = "ro_admin_user_{}".format(len(self.users) - 1)
        self.log.info("Deleting user: {0}".format(user))
        self.rbac_util._drop_user(user)
        self.users[user]['deleted'] = True

    def _validate_rbac(self):
        resp = self.rest.retrieve_user_roles()

        # Validating if all users are created/updated/deleted properly
        for user in self.users:
            if 'deleted' in self.users[user] and self.users[user]['deleted']:
                user_found = False
                for rbac_user in resp:
                    if rbac_user['id'] == user:
                        user_found = True
                        break
                self.assertFalse(user_found, msg="User {0} not deleted".format(user))
            else:
                user_found = False
                for rbac_user in resp:
                    if rbac_user['id'] == user:
                        user_found = True
                        user_roles = [str(role["role"]) for role in rbac_user['roles']]
                        self.log.info("Validating user: {0} expected roles: {1} actual roles: {2}".format(user, self.users[user]['roles'], user_roles))
                        self.assertTrue(sorted(user_roles) == sorted(self.users[user]['roles']),
                                        msg="Roles mismatch for user {0}".format(user))
                        break

                self.assertTrue(user_found, msg="User {0} not found".format(user))

        # Validating if there are no unexpected users
        for rbac_user in resp:
            self.assertTrue(rbac_user['id'] in self.users,
                            msg="Unexpected user {0} found".format(rbac_user['id']))

    def _validate_ro_admin_permissions(self, upgrade_complete=False):
        for user in self.users:
            if 'deleted' in self.users[user] and self.users[user]['deleted']:
                continue
            self.log.info("Validating user permissions for user {} and role {}".format(user, self.users[user]['roles']))
            if upgrade_complete:
                if "ro_admin" in self.users[user]['roles'] and "ro_security_admin" in self.users[user]['roles'] and len(self.users[user]['roles']) == 2:
                    resp = self.rest.check_user_permission(user, "password", "cluster.admin.security!all,cluster.admin.users!all,cluster.admin.security!read,cluster.admin.users!read")
                    if not resp['cluster.admin.security!read'] or not resp['cluster.admin.users!read'] or resp['cluster.admin.users!all'] or resp['cluster.admin.security!all']:
                        self.fail("Permission incorrect for read only admin and security read only admin after upgrade. Error: {}".format(resp))
                elif "ro_admin" in self.users[user]['roles'] and len(self.users[user]['roles']) == 1:
                    resp = self.rest.check_user_permission(user, "password", "cluster.admin.security!all,cluster.admin.users!all,cluster.admin.security!read,cluster.admin.users!read")
                    if resp['cluster.admin.security!read'] or resp['cluster.admin.users!read'] or resp['cluster.admin.users!all'] or resp['cluster.admin.security!all']:
                        self.fail("Permission incorrect for read only admin post upgrade. Error: {}".format(resp))
                elif "ro_security_admin" in self.users[user]['roles'] and len(self.users[user]['roles']) == 1:
                    resp = self.rest.check_user_permission(user, "password", "cluster.admin.security!all,cluster.admin.users!all,cluster.admin.security!read,cluster.admin.users!read")
                    if not resp['cluster.admin.security!read'] or not resp['cluster.admin.users!read'] or resp['cluster.admin.users!all'] or resp['cluster.admin.security!all']:
                        self.fail("Permission incorrect for security read only admin. Error: {}".format(resp))
            elif "ro_admin" in self.users[user]['roles'] and len(self.users[user]['roles']) == 1:
                resp = self.rest.check_user_permission(user, "password", "cluster.admin.security!all,cluster.admin.users!all,cluster.admin.security!read,cluster.admin.users!read")
                if not resp['cluster.admin.security!read'] or resp['cluster.admin.users!read'] or resp['cluster.admin.users!all'] or resp['cluster.admin.security!all']:
                    self.fail("Permission incorrect for read only admin before complete upgrade. Error: {}".format(resp))

    def test_upgrade_ro_admin(self):
        '''
            Ref : MB-67164

            1. Create a 3 node cluster with 5 ro_admin users.
            2. Start upgrade of cluster nodes one by one using any method.
            3. After node 1 upgrade, create 3 new users with ro_admin role.
            4. After node 1 upgrade, update 1 user with ro_admin role to ro_admin and security_admin roles.
            5. After node 1 upgrade, delete 1 user with ro_admin role.
            6. After upgrade, create 3 new users with ro_admin role.
            7. After upgrade, update users with ro_admin role.
            8. After upgrade, delete users with ro_admin role.
            9. Validate RBAC operations at each step.

            This test is specifically for upgrades from 7.2.x and 7.6.x to 8.x.x. It does not apply to 8.x.x --> 8.x.x upgrades.
        '''

        #Loading Travel Sample Bucket
        travelSampleBucket = TravelSample()
        load_success=self.bucket_util.load_sample_bucket(self.cluster, travelSampleBucket)
        self.assertTrue(load_success,
                        msg="Travel Sample Bucket could not be loaded")
        self.log.info("Travel Sample Bucket Loaded")

        #Loading Beer Sample Bucket
        load_success=self.bucket_util.load_sample_bucket(self.cluster, BeerSample())
        self.assertTrue(load_success,
                        msg = "Beer Sample Bucket could not be loaded")
        self.log.info("Beer Sample Bucket Loaded")

        #Loading Gamesim Sample Bucket
        load_success=self.bucket_util.load_sample_bucket(self.cluster, GamesimSample())
        self.assertTrue(load_success,
                        msg = "Gamesim Sample Bucket could not be loaded")
        self.log.info("Gamesim Sample Bucket Loaded")

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        self._create_ro_admin_users_during_upgrade()
        self.sleep(10, "Wait for 10 seconds before validating rbac")
        self._validate_rbac()
        self._validate_ro_admin_permissions(upgrade_complete=False)

        self.PrintStep("Starting Upgrades")
        # Start upgrades here
        for upgrade_version in self.upgrade_chain[1:]:
            self.initial_version = self.upgrade_version
            self.upgrade_version = upgrade_version

            ### Fetching the first node to upgrade ###
            node_to_upgrade = self.fetch_node_to_upgrade()
            itr = 0

            ### Each node in the cluster is upgraded iteratively ###
            while node_to_upgrade is not None:
                itr += 1
                ### The upgrade procedure starts ###
                self.log.info("Selected node for upgrade: {}".format(node_to_upgrade.ip))

                ### Based on the type of upgrade, the upgrade function is called ###
                if self.upgrade_type in ["failover_delta_recovery",
                                            "failover_full_recovery"]:
                    if "kv" not in node_to_upgrade.services.lower():
                        upgrade_type = "failover_full_recovery"
                        self.upgrade_function[upgrade_type](node_to_upgrade, graceful=False)
                    else:
                        self.upgrade_function[self.upgrade_type](node_to_upgrade)
                elif self.upgrade_type == "full_offline":
                    self.upgrade_function[self.upgrade_type](self.cluster.nodes_in_cluster, self.upgrade_version)
                else:
                    self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                            self.upgrade_version)

                # Upgrade of node
                self.cluster_util.print_cluster_stats(self.cluster)

                if self.test_failure is not None:
                    self.fail("Test failed during upgrade")

                self.log.info("Upgrade of node {0} : {1} complete".format(itr, node_to_upgrade.ip))

                # Updating the rest connection to the orchestrator node which may have changed after upgrade
                self.rest = RestConnection(self.cluster.master)
                self.rbac_util = RbacUtils(self.cluster.master)

                ### Fetching the next node to upgrade ##
                node_to_upgrade = self.fetch_node_to_upgrade()

                if node_to_upgrade is not None:
                    # Create update and delete users during upgrade
                    self._update_users_during_upgrade(scenario="Node {} Upgrade".format(itr))
                    self._delete_users_during_upgrade()
                    self._create_ro_admin_users_during_upgrade()
                    self.sleep(10, "Wait for 10 seconds before validating rbac")
                    self._validate_rbac()
                    self._validate_ro_admin_permissions(upgrade_complete=False)

            self.cluster.version = upgrade_version
            self.cluster_features = \
                self.upgrade_helper.get_supported_features(self.cluster.version)

        ### Printing cluster stats after the upgrade of the whole cluster ###
        self.cluster_util.print_cluster_stats(self.cluster)
        self.PrintStep("Upgrade of the whole cluster to {0} complete".format(
                                                        self.upgrade_version))

        # Update all ro_admin users to have ro_security_admin role
        # Update all security_admin_local roles to security_admin and user_admin_local roles
        for user in self.users:
            if "ro_admin" in self.users[user]['roles']:
                self.users[user]['roles'].append("ro_security_admin")
            if "security_admin_local" in self.users[user]['roles']:
                self.users[user]['roles'].remove("security_admin_local")
                self.users[user]['roles'].append("security_admin")
                self.users[user]['roles'].append("user_admin_local")

        self._validate_rbac()
        self._validate_ro_admin_permissions(upgrade_complete=True)

        # Create update and delete users after upgrade
        self._update_users_during_upgrade(scenario="Post Upgrade")
        self._create_ro_admin_users_during_upgrade()
        self._delete_users_during_upgrade()
        self.sleep(10, "Wait for 10 seconds before validating rbac")
        self._validate_rbac()
        self._validate_ro_admin_permissions(upgrade_complete=True)

        # Create a user with ro_security_admin role and validate permissions
        self.log.info("Creating user: ro_security_admin with ro_security_admin role")
        self.rbac_util._create_user_and_grant_role("ro_security_admin", "ro_security_admin")
        self._validate_rbac()
        self._validate_ro_admin_permissions(upgrade_complete=True)

