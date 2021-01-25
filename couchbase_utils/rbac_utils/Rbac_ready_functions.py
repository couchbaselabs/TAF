'''
Created on Dec 1, 2017

@author: riteshagarwal
'''
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

    def _create_user_and_grant_role(self, username, role, source='builtin'):
        user = [{'id': username, 'password': 'password', 'name': 'Some Name'}]
        _ = RbacBase().create_user_source(user, source, self.master)
        user_role_list = [{'id': username, 'name': 'Some Name', 'roles': role}]
        _ = RbacBase().add_user_role(user_role_list, self.rest, source)

    def _drop_user(self, username):
        user = [username]
        _ = RbacBase().remove_user_role(user, self.rest)
