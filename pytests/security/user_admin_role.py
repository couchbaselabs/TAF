import json
import random
import string

from membase.api.rest_client import RestConnection
from pytests.basetestcase import ClusterSetup
from rbac_utils.Rbac_ready_functions import RbacUtils
from internal_user import InternalUser

class UserAdminRole(ClusterSetup):
    def setUp(self):
        super(UserAdminRole, self).setUp()
        self.rbac_util = RbacUtils(self.cluster.master)
        self.expected_error = "Forbidden. User needs the following permissions"
        self.rest = RestConnection(self.cluster.master)
        self.security_users = ["user_admin_local", "user_admin_external", "ro_security_admin", "security_admin"]
        self.user_permission_map = {
            "create_local_user": ["user_admin_local"],
            "get_local_user": ["user_admin_local", "security_admin", "ro_security_admin"],
            "list_local_users": ["user_admin_local", "security_admin", "ro_security_admin"],
            "delete_local_user": ["user_admin_local"],
            "create_external_user": ["user_admin_external"],
            "get_external_user": ["user_admin_external", "security_admin", "ro_security_admin"],
            "list_external_users": ["user_admin_external", "security_admin", "ro_security_admin"],
            "delete_external_user": ["user_admin_external"],
            "create_group": ["user_admin_local", "user_admin_external"],
            "get_group": ["user_admin_local", "user_admin_external", "security_admin", "ro_security_admin"],
            "delete_group": ["user_admin_local", "user_admin_external"],
            "list_groups": ["user_admin_local", "user_admin_external", "security_admin", "ro_security_admin"],
            "backup_users": ["user_admin_local", "user_admin_external", "security_admin", "ro_security_admin"],
            "restore_users": ["user_admin_local", "user_admin_external"],
            "get_audit_settings": ["security_admin", "ro_security_admin"],
            "set_audit_settings": ["security_admin"],
            "get_security_settings": ["security_admin", "ro_security_admin"],
            "set_security_settings": ["security_admin"]
        }

    def tearDown(self):
        super(UserAdminRole, self).tearDown()


    def get_rest_object_for_user(self, username, password="password"):

        server_info = {"ip": self.cluster.master.ip, "port": self.cluster.master.port,
                       "username": username, "password": password}
        rest_obj = RestConnection(server_info)

        return rest_obj

    def validate_error_message(self, returned_err_msg):

        try:
            err = json.loads(str(returned_err_msg))
            err_msg = err["message"]
        except ValueError:
            err_msg = str(returned_err_msg)

        if err_msg != self.expected_error:
            self.fail("Error message don't match. Expected: {}. Error: {}".format(self.expected_error,
                                                                                  err_msg))

    @staticmethod
    def get_sample_users(num_users=1, usernames=[], passwords=[]):

        if len(usernames) == 0:
            for _ in range(num_users):
                base_name = "security_user"
                # Generate a random string of lowercase letters and digits
                random_suffix = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))

                # Combine the base name with the random suffix
                username = "{}_{}".format(base_name, random_suffix)
                usernames.append(username)

        if len(passwords) == 0:
            for _ in range(num_users):
                password = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))
                passwords.append(password)

        users = []
        for uname, psswd in zip(usernames, passwords):
            user_dict = {
                'id': uname,
                'password': psswd,
                'name': uname
            }
            users.append(user_dict)

        return users

    def create_external_user(self, user_dict, username="Administrator", verify_error=False,
                             roles=[]):
        if len(roles) > 0:
            roles = ",".join(roles)
        else:
            roles = "external_stats_reader"
        rest = self.get_rest_object_for_user(username)
        try:
            resp = rest.add_external_user(user_dict['id'], roles)
            if verify_error:
                self.fail("Expected error but creating external user for {} returned response: {}".
                          format(username, resp))
        except Exception as err:
            if not verify_error:
                self.fail("{} user failed to create external users. Error: {}".format(username, err))

            self.validate_error_message(err)


    def create_local_user(self, user_dict, username="Administrator", verify_error=False,
                          roles=[]):
        roles = ",".join(roles)
        payload = "name=" + user_dict['name'] + "&roles=" + roles + "&password=" + user_dict['password']
        rest = self.get_rest_object_for_user(username)
        try:
            resp = rest.add_set_builtin_user(user_dict['id'], payload)
            if verify_error:
                self.fail("Expected error but creating local user for {} returned response: {}".
                           format(username, resp))
        except Exception as err:
            if not verify_error:
                self.fail("{} user failed to create local users. Error: {}".format(username, err))

            self.validate_error_message(err)

    def get_external_user(self, user_id=None, username=None, verify_error=False):
        try:
            rest = self.get_rest_object_for_user(username)
            resp = rest.get_external_user(user_id)
            if verify_error:
                self.fail("Expected error but getting local user for {} returned response: {}".
                           format(username, resp))
        except Exception as err:
            if not verify_error:
                self.fail("{} user failed to get local user. Error: {}".format(username, err))

            self.validate_error_message(err)

    def get_local_user(self, user_id=None, username=None, verify_error=False):
        # Get Specific user
        try:
            rest = self.get_rest_object_for_user(username)
            resp = rest.get_builtin_user(user_id)
            if verify_error:
                self.fail("Expected error but getting local user for {} returned response: {}".
                           format(username, resp))
        except Exception as err:
            if not verify_error:
                self.fail("{} user failed to get local user. Error: {}".format(username, err))

            self.validate_error_message(err)

    def list_users(self, username=None, verify_error=False):
        # Get all users
        try:
            rest = self.get_rest_object_for_user(username)
            resp = rest.retrieve_user_roles()
            if verify_error:
                # Check if the list is emtpy or not
                if len(resp) > 0:
                    self.fail("Expected no users in list for {} returned response: {}".
                               format(username, resp))

            if (not verify_error) and len(resp) == 0:
                self.fail("No users in list for {} but expected users in list users response".format(username))

            for user in resp:
                user_domain = user['domain']
                if username == "user_admin_local":
                    if user_domain != "local":
                        self.fail("Local user admin able to list non local users. User: {}".
                                  format(user))
                elif username == "user_admin_external":
                    if user_domain != "external":
                        self.fail("External user admin able to list non external users. User: {}".
                                  format(user))

        except Exception as err:
            if not verify_error:
                self.fail("{} user failed to list users. Error: {}".format(username, err))

            self.validate_error_message(err)

    def delete_external_user(self, user_id=None, username=None, verify_error=False):
        try:
            rest = self.get_rest_object_for_user(username)
            resp = rest.delete_external_user(user_id)
            if verify_error:
                self.fail("Expected error for deleting local user for {} but got response {}".
                          format(username, resp))
        except Exception as err:
            if not verify_error:
                self.fail("{} failed to delete local user. Err: {}".format(username, err))

            self.validate_error_message(err)

    def delete_local_user(self, user_id=None, username=None, verify_error=False):
        try:
            rest = self.get_rest_object_for_user(username)
            resp = rest.delete_builtin_user(user_id)
            if verify_error:
                self.fail("Expected error for deleting local user for {} but got response {}".
                          format(username, resp))
        except Exception as err:
            if not verify_error:
                self.fail("{} failed to delete local user. Err: {}".format(username, err))

            self.validate_error_message(err)

    def create_group(self, group_name, roles= [], username=None, verify_error=False):

        try:
            payload = "roles=" + ",".join(roles)
            rest = self.get_rest_object_for_user(username)
            resp = rest.add_set_bulitin_group(group_name, payload)
            if verify_error:
                self.fail("Expected error for creating group for {} but got response {}".
                          format(username, resp))
        except Exception as err:
            if not verify_error:
                self.fail("{} failed to create group. Err: {}".format(username, err))

            self.validate_error_message(err)

    def get_group(self, group_name, username=None, verify_error=None):

        try:
            rest = self.get_rest_object_for_user(username)
            resp = rest.get_builtin_group(group_name)
            if verify_error:
                self.fail("Expected error for get group for {} but got response {}".
                          format(username, resp))
        except Exception as err:
            if not verify_error:
                self.fail("{} failed to get group. Err: {}".format(username, err))

            self.validate_error_message(err)

    def list_groups(self, username=None, verify_error=None):

        try:
            rest = self.get_rest_object_for_user(username)
            resp = rest.list_groups()
            if verify_error:
                self.fail("Expected error for list groups for {} but got response {}".
                          format(username, resp))
        except Exception as err:
            if not verify_error:
                self.fail("{} failed to list groups. Err: {}".format(username, err))

            self.validate_error_message(err)

    def delete_group(self, group_name, username=None, verify_error=None):

        try:
            rest = self.get_rest_object_for_user(username)
            resp = rest.delete_builtin_group(group_name)
            if verify_error:
                self.fail("Expected error for delete group for {} but got response {}".
                          format(username, resp))
        except Exception as err:
            if not verify_error:
                self.fail("{} failed to delete group. Err: {}".format(username, err))

            self.validate_error_message(err)

    def backup_users(self, username, verify_error=False):
        try:
            rest = self.get_rest_object_for_user(username)
            resp = rest.backup_users()
            if verify_error:
                self.fail("Expected error for backup for {} but got response {}".
                          format(username, resp))
        except Exception as err:
            if not verify_error:
                self.fail("{} failed to backup. Err: {}".format(username, err))

            self.validate_error_message(err)

    def restore_users(self, username, backup_data, verify_error=False):
        try:
            rest = self.get_rest_object_for_user(username)
            resp = rest.restore_users(backup_data)
            if verify_error:
                self.fail("Expected error for restore {} but got response {}".
                          format(username, resp))
        except Exception as err:
            if not verify_error:
                self.fail("{} failed to backup. Err: {}".format(username, err))
            self.validate_error_message(err)

    def get_audit_settings(self, username, verify_error=False):
        try:
            rest = self.get_rest_object_for_user(username)
            resp = rest.getAuditSettings()
        except Exception as err:
            if not verify_error:
                self.fail("{} failed to get audit setting. Err: {}".format(username, err))
            self.validate_error_message(err)

    def set_audit_settings(self, username, verify_error=False):
        try:
            rest = self.get_rest_object_for_user(username)
            resp = rest.setAuditSettings()
        except Exception as err:
            if not verify_error:
                self.fail("{} failed to set audit setting. Err: {}".format(username, err))
            self.validate_error_message(err)

    def get_security_settings(self, username, verify_error=False):
        try:
            rest = self.get_rest_object_for_user(username)
            resp = rest.get_security_settings()
        except Exception as err:
            if not verify_error:
                self.fail("{} failed to get security setting. Err: {}".format(username, err))
            self.validate_error_message(err)

    def set_security_settings(self, username, verify_error=False):
        try:
            rest = self.get_rest_object_for_user(username)
            settings = {"tlsMinVersion": "tlsv1.3", "clusterEncryptionLevel": "all"}
            resp = rest.set_security_settings(settings)
        except Exception as err:
            if not verify_error:
                self.fail("{} failed to get security setting. Err: {}".format(username, err))
            self.validate_error_message(err)

    def test_local_user_management(self):
        # Create all security users
        # Users will have the same name as the role
        for user in self.security_users:
            self.rbac_util._create_user_and_grant_role(user, user)

        # Get sample users
        sample_users = self.get_sample_users(num_users=len(self.security_users))

        for idx, user in enumerate(self.security_users):
            ## CREATE LOCAL USERS
            self.log.info("Test user creation for {}".format(user))
            self.create_local_user(sample_users[idx], user,
                                   user not in self.user_permission_map["create_local_user"])

            # If user does not exist, create it with Administrator user
            try:
                rest = self.get_rest_object_for_user("Administrator")
                _ = rest.get_builtin_user(sample_users[idx])
            except Exception as err:
                # User does not exist, create it
                self.create_local_user(sample_users[idx], username="Administrator")

            ## GET LOCAL USER
            self.log.info("Testing get user details for {}".format(user))
            self.get_local_user(sample_users[idx]['id'], user,
                                user not in self.user_permission_map["get_local_user"])

            ## LIST ALL USERS
            self.log.info("Testing list users for {}".format(user))
            self.list_users(user, user not in self.user_permission_map["list_local_users"])

            ## DELETE LOCAL USER
            self.log.info("Testing delete user for {}".format(user))
            self.delete_local_user(sample_users[idx]['id'], user,
                                   user not in self.user_permission_map["delete_local_user"])

    def test_group_management(self):
        # Create all security users
        # Users will have the same name as the role
        for user in self.security_users:
            self.rbac_util._create_user_and_grant_role(user, user)

        groups = ["security_group_1", "security_group_2", "security_group_3", "security_group_4"]

        for idx, user in enumerate(self.security_users):

            self.log.info("Testing create group {}".format(user))
            self.create_group(groups[idx], username=user,
                              verify_error=user not in self.user_permission_map["create_group"])

            # If group does not exist, create it with Administrator user
            try:
                rest = self.get_rest_object_for_user("Administrator")
                _ = rest.get_builtin_group(groups[idx])
            except Exception as err:
                # Group does not exist, create it
                self.create_group(groups[idx], username="Administrator")

            self.log.info("Testing get group for {}".format(user))
            self.get_group(groups[idx], username=user,
                           verify_error=user not in self.user_permission_map["get_group"])

            self.log.info("Testing list groups for {}".format(user))
            self.list_groups(username=user, verify_error=user not in self.user_permission_map["list_groups"])

            self.log.info("Testing delete group for {}".format(user))
            self.delete_group(groups[idx], username=user,
                              verify_error=user not in self.user_permission_map["delete_group"])

    def test_external_user_management(self):

        # Create all security users
        # Users will have the same name as the role
        for user in self.security_users:
            self.rbac_util._create_user_and_grant_role(user, user)

        # Get sample users
        users = self.get_sample_users(num_users=len(self.security_users))

        for idx, user in enumerate(self.security_users):
            # CREATE EXTERNAL USERS
            self.log.info("Test external user creation for {}".format(user))
            self.create_external_user(users[idx], user,
                                      user not in self.user_permission_map["create_external_user"])

            # If user does not exist, create it with Administrator user
            try:
                rest = self.get_rest_object_for_user("Administrator")
                _ = rest.get_external_user(users[idx])
            except Exception as err:
                # User does not exist, create it
                self.create_external_user(users[idx], username="Administrator")

            # GET EXTERNAL USER
            self.log.info("Testing get external user details for {}".format(user))
            self.get_external_user(users[idx]['id'], user,
                                   user not in self.user_permission_map["get_external_user"])

            # LIST EXTERNAL USERS
            self.log.info("Testing list external users for {}".format(user))
            self.list_users(user, user not in self.user_permission_map["list_external_users"])

            # DELETE EXTERNAL USER
            self.log.info("Testing delete external user for local user admin")
            self.delete_external_user(users[idx]['id'], user,
                                      user not in self.user_permission_map["delete_external_user"])

    def test_privilege_escalation(self):

        # Create both local and external user admin
        self.rbac_util._create_user_and_grant_role("user_admin_local", "user_admin_local")
        self.rbac_util._create_user_and_grant_role("user_admin_external", "user_admin_external")

        # Test that both the user cannot create any users with admin roles
        admin_roles = ["admin", "ro_admin" , "security_admin" , "user_admin_local" ,
                       "user_admin_external" , "cluster_admin", "ro_security_admin"]
        sample_users = self.get_sample_users(len(admin_roles) * 2)

        for idx, admin in enumerate(admin_roles):
            if admin != "user_admin_local":
                self.log.info("Testing {} role creation using local user admin".format(admin))
                self.create_local_user(sample_users[idx], "user_admin_local",
                                       verify_error=True, roles=[admin])

            if admin != "user_admin_external":
                self.log.info("Testing {} role creation using external user admin".format(admin))
                self.create_external_user(sample_users[idx + len(admin_roles)], "user_admin_external",
                                          verify_error=True, roles=[admin])


        # Test that both local and external admin users cannot change their own roles
        roles = admin_roles + ["query_external_access", "replication_admin", "bucket_admin[*]"]

        for role in roles:
            if role != "user_admin_local":
                self.log.info("Changing role of local user admin to {}".format(role))
                user_dict = {
                    'id': "user_admin_local",
                    'name': "user_admin_external",
                    'password': "password"
                }
                self.create_local_user(user_dict, "user_admin_local",
                                       verify_error=True, roles=[role])

            if role != "user_admin_external":
                self.log.info("Changing role of external user admin to {}".format(role))
                user_dict = {
                    'id': "user_admin_external",
                    'name': "user_admin_external",
                    'password': "password"
                }
                self.create_external_user(user_dict, "user_admin_external",
                                          verify_error=True, roles=[role])

    def test_user_backup_restore(self):

        # Create all security users
        # Users will have the same name as the role
        for user in self.security_users:
            self.rbac_util._create_user_and_grant_role(user, user)

        backup_data = self.rest.backup_users()

        # Check backup/restore user permissions
        for idx, user in enumerate(self.security_users):
            # Backup users
            self.log.info("Test backup permissions for {}".format(user))
            self.backup_users(user, user not in self.user_permission_map["backup_users"])

            # Restore users
            self.log.info("Test restore permissions for {}".format(user))
            self.restore_users(user, backup_data, user not in self.user_permission_map["restore_users"])

        for user in self.security_users:
            # Deleting the security users created
            self.log.info("Deleting security user: {}".format(user))
            self.delete_local_user(user, "Administrator")

        self.rbac_util._create_user_and_grant_role("user_admin_local", "user_admin_local")

        # Test if local_user_admin is only able to restore local users and also test that it cannot restore admin users
        # Create some local and external users as well as all the admin users
        self.log.info("Testing backup/restore for local user admin")
        admin_roles = ["admin", "ro_admin", "security_admin", "user_admin_local",
                       "user_admin_external", "ro_security_admin"]
        sample_users = self.get_sample_users(4 + len(admin_roles))
        sample_local_users = sample_users[:2]
        sample_external_users = sample_users[2:4]
        sample_admin_users = sample_users[4:]

        self.log.info("Creating sample local users")
        for user in sample_local_users:
            self.log.info("Creating local user: {}".format(user))
            self.create_local_user(user)
        self.log.info("Creating sample external users")
        for user in sample_external_users:
            self.log.info("Creating external user: {}".format(user))
            self.create_external_user(user)
        self.log.info("Creating admin users")
        for user, role in zip(sample_admin_users, admin_roles):
            self.log.info("Creating admin user: {} with role: {}".format(user, role))
            self.create_local_user(user, roles=[role])

        # Backup users using local user admin
        self.log.info("Backup all users using local user admin")
        rest = self.get_rest_object_for_user("user_admin_local")
        try:
            backup_data_local_user_admin = rest.backup_users()
        except Exception as err:
            self.fail("Failed to backup data using local user admin. Error: {}".format(err))

        # Delete all users created
        self.log.info("Deleting all the created sample users and admin users")
        local_users_to_delete = sample_local_users + sample_admin_users
        for user in local_users_to_delete:
            self.delete_local_user(user['id'], 'Administrator')

        for user in sample_external_users:
            self.delete_external_user(user['id'], 'Administrator')

        # Restore back the users using local user admin
        self.log.info("Restoring users using local user admin")
        try:
            restore_response = rest.restore_users(backup_data_local_user_admin)
        except Exception as err:
            self.fail("Failed to to restore data using local user admin. Error: {}".format(err))

        # List all users and check only local non admin users are restored
        self.log.info("Verifying only local non admin users are restored")
        restored_users_list = self.rest.retrieve_user_roles()
        for restored_user in restored_users_list:
            user_domain = restored_user['domain']
            if user_domain != "local":
                self.fail("local user admin restore a non local user. User restored: {}".format(restored_user))
            user_roles = restored_user['roles']
            user_id = restored_user['id']
            for role in user_roles:
                role_name = role['role']
                if (role_name in admin_roles) and (user_id in [admin['id'] for admin in sample_admin_users]):
                    self.fail("local user admin restored an admin user. User restored: {}".format(restored_user))

        self.delete_local_user("user_admin_local", "Administrator")
        self.delete_local_user("cbadminbucket", "Administrator")
        self.rbac_util._create_user_and_grant_role("user_admin_external", "user_admin_external")

        # Test if external user admin is only able to restore external users and also test that it cannot restore admin users
        # Create some local and external users as well as all the admin users
        self.log.info("Testing backup/restore for external user admin")
        admin_roles = ["admin", "ro_admin", "security_admin", "user_admin_local",
                       "user_admin_external", "ro_security_admin"]

        self.log.info("Creating sample external users")
        for user in sample_external_users:
            self.create_external_user(user)
        self.log.info("Creating admin users")
        for user, role in zip(sample_admin_users, admin_roles):
            self.create_external_user(user, roles=[role])

        # Backup users using local user admin
        self.log.info("Backup all users using external user admin")
        rest = self.get_rest_object_for_user("user_admin_external")
        try:
            backup_data_external_user_admin = rest.backup_users()
        except Exception as err:
            self.fail("Failed to backup data using local user admin. Error: {}".format(err))

        # Delete all users created
        self.log.info("Deleting all the created sample users and admin users")
        local_users_to_delete = sample_local_users
        for user in local_users_to_delete:
            self.delete_local_user(user['id'], 'Administrator')
        external_users_to_delete = sample_external_users + sample_admin_users
        for user in external_users_to_delete:
            self.delete_external_user(user['id'], 'Administrator')

        # Restore back the users using local user admin
        self.log.info("Restoring users using external user admin")
        try:
            restore_response = rest.restore_users(backup_data_external_user_admin)
        except Exception as err:
            self.fail("Failed to to restore data using local user admin. Error: {}".format(err))

        # List all users and check only local non admin users are restored
        self.log.info("Verifying only external non admin users are restored")
        restored_users_list = self.rest.retrieve_user_roles()
        for restored_user in restored_users_list:
            if restored_user['id'] == "user_admin_external":
                continue
            user_domain = restored_user['domain']
            if user_domain != "external":
                self.fail("external user admin restored a non local user. User restored: {}".format(restored_user))
            user_roles = restored_user['roles']
            user_id = restored_user['id']
            for role in user_roles:
                role_name = role['role']
                if (role_name in admin_roles) and (user_id in [admin['id'] for admin in sample_admin_users]):
                    self.fail("external user admin restored an admin user. User restored: {}".format(restored_user))

    def test_security_admin_permissions(self):

        # Test that security admin has cluster.admin.security!all:True and cluster.admin.user!all:False

        #Create a security admin user
        self.rbac_util._create_user_and_grant_role("security_admin", "security_admin")

        resp = self.rest.check_user_permission("security_admin", "password", "cluster.admin.security!all,cluster.admin.users!all")

        if not resp['cluster.admin.security!all'] or resp['cluster.admin.users!all']:
            self.fail("Permission incorrect for security admin. Error: {}".format(resp))

    def test_security_admin_general_security_apis(self):
        # Create all security users
        # Users will have the same name as the role
        for user in self.security_users:
            self.rbac_util._create_user_and_grant_role(user, user)

        for idx, user in enumerate(self.security_users):

            self.log.info("Test get audit settings for {}".format(user))
            self.get_audit_settings(user, user not in self.user_permission_map["get_audit_settings"])

            self.log.info("Test set audit settings for {}".format(user))
            self.set_audit_settings(user, user not in self.user_permission_map["set_audit_settings"])

            self.log.info("Test get security setting for {}".format(user))
            self.get_security_settings(user, user not in self.user_permission_map["get_security_settings"])

            self.log.info("Test set security settings for {}".format(user))
            self.set_security_settings(user, user not in self.user_permission_map["set_security_settings"])

    def test_ro_security_admin_permissions(self):

        # Test that security read only admin has cluster.admin.security!read:True and cluster.admin.user!all:False and cluster.admin.security!all:False

        #Create a security read only admin user
        self.rbac_util._create_user_and_grant_role("ro_security_admin", "ro_security_admin")

        resp = self.rest.check_user_permission("ro_security_admin", "password", "cluster.admin.security!all,cluster.admin.users!all,cluster.admin.security!read,cluster.admin.users!read")

        if not resp['cluster.admin.security!read'] or not resp['cluster.admin.users!read'] or resp['cluster.admin.users!all'] or resp['cluster.admin.security!all']:
            self.fail("Permission incorrect for security read only admin. Error: {}".format(resp))
