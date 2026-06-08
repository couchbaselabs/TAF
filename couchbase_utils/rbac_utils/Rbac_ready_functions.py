'''
Created on Dec 1, 2017

@author: riteshagarwal
'''
import logging

from cb_server_rest_util.security.security_api import SecurityRestAPI
from security.rbac_base import RbacBase
from membase.api.rest_client import RestConnection


class RbacUtils():

    cb_server_roles = ["admin", "analytics_admin", "analytics_reader", "cluster_admin",
                       "query_external_access", "query_system_catalog", "replication_admin",
                       "ro_admin", "security_admin_local", "security_admin_external", "analytics_manager[*]",
                       "analytics_select[*]", "bucket_admin[*]", "bucket_full_access[*]", "data_backup[*]",
                       "data_dcp_reader[*]", "data_monitoring[*]", "data_reader[*]", "data_writer[*]", "fts_admin[*]",
                       "fts_searcher[*]", "mobile_sync_gateway[*]", "query_delete[*]", "query_insert[*]",
                       "query_manage_index[*]", "query_select[*]", "query_update[*]", "replication_target[*]",
                       "views_admin[*]", "views_reader[*]"]

    def __init__(self, master):
        self.master = master
        self.rest = RestConnection(master)
        self.security_rest = SecurityRestAPI(master)
        self.log = logging.getLogger("test")

    def _create_user_and_grant_role(self, username, role, source='builtin',
                                    password="password"):
        user = [{'id': username, 'password': password, 'name': 'Some Name'}]
        _ = RbacBase().create_user_source(user, source, self.master)
        user_role_list = [{'id': username, 'name': 'Some Name', 'roles': role}]
        _ = RbacBase().add_user_role(user_role_list, self.rest, source)

    def _drop_user(self, username):
        user = [username]
        _ = RbacBase().remove_user_role(user, self.rest)

    def _create_user_with_exclusion_role(self, username, role_name, bucket,
                                         scope, excluded_collection,
                                         password="password",
                                         enable_custom_roles=True,
                                         additional_roles=None):
        """
        Creates a custom RBAC role that grants all access to every collection
        in `bucket:scope` except `excluded_collection`, then creates a new
        local user assigned to that role.

        Permission format verified on CB 8.1.0:
          cluster.collection[b:s:.]        -> "all"   (wildcard: every collection)
          cluster.collection[b:s:col_name] -> "none"  (explicit deny on one collection)

        Requires custom_roles_enabled (provisioned / Capella dedicated profile).
        additional_roles: list of built-in role strings to append (e.g. ["data_writer[bucket]"])
        Returns True if role + user were created successfully, False otherwise.
        """
        if enable_custom_roles:
            # on-prem clusters have custom_roles_enabled off by default;
            # provisioned/dedicated clusters already have it enabled
            self.rest.diag_eval(
                "persistent_term:put(config_profile, "
                "[{custom_roles_enabled, true} | "
                "persistent_term:get(config_profile, [])]).")
        permissions = {
            "cluster.collection[{b}:{s}:.]".format(b=bucket, s=scope): "all",
            "cluster.collection[{b}:{s}:{c}]".format(
                b=bucket, s=scope, c=excluded_collection): "none",
        }
        status, resp = self.security_rest.create_custom_role(
            role_name, "Exclusion role - " + role_name, permissions)
        if not status:
            self.log.warning(
                "create_custom_role failed (custom_roles_enabled?): %s" % resp)
            return False
        roles = role_name
        if additional_roles:
            roles = ",".join([role_name] + additional_roles)
        params = "name={u}&password={p}&roles={r}".format(
            u=username, p=password, r=roles)
        status, content = self.security_rest.create_local_user(username, params)
        if not status:
            self.security_rest.delete_custom_role(role_name)
            self.log.warning("create_local_user failed: %s" % content)
            return False
        return True

    def _check_user_permissions(self, username, password, permissions):
        """Check permissions for a specific user via checkPermissions REST API."""
        user_sec_rest = SecurityRestAPI(self.master)
        user_sec_rest.username = username
        user_sec_rest.password = password
        return user_sec_rest.check_permissions(permissions)

    def _drop_user_and_custom_role(self, username, role_name):
        """Cleans up the exclusion-syntax user and its custom role."""
        self.security_rest.delete_local_user(username)
        status, content = self.security_rest.delete_custom_role(role_name)
        if not status:
            self.log.warning("Failed to delete custom role '%s': %s",
                             role_name, content)
