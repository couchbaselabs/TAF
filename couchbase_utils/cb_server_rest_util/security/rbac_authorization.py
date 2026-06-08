import json

from cb_server_rest_util.connection import CBRestConnection


class RbacAuthorization(CBRestConnection):
    def __init__(self):
        super(RbacAuthorization, self).__init__()

    def list_roles(self):
        """
        GET :: /settings/rbac/roles
        docs.couchbase.com/server/current/rest-api/rbac.html
        """

    def list_current_users_and_roles(self):
        """
        GET:: /settings/rbac/users
        docs.couchbase.com/server/current/rest-api/rbac.html
        """
        url = "/settings/rbac/users"
        api = self.base_url + url
        status, content, _ = self.request(api, self.GET)
        return status, content

    def check_permissions(self, permissions):
        """
        POST :: /pools/default/checkPermissions
        docs.couchbase.com/server/current/rest-api/rbac.html
        permissions: list of permission strings, e.g.
          ["cluster.collection[b:s:c1]!write", "cluster.collection[b:s:c2]!write"]
        Authenticated as self.username/self.password.
        Returns (status, dict) mapping permission -> bool.
        """
        api = self.base_url + "/pools/default/checkPermissions"
        body = ",".join(permissions)
        status, content, _ = self.request(api, self.POST, params=body)
        return status, content

    def create_local_user(self, user_name, params):
        """
        PUT :: /settings/rbac/users/local/<new-username>
        docs.couchbase.com/server/current/rest-api/rbac.html
        """
        api = self.base_url + f"/settings/rbac/users/local/{user_name}"
        status, content, _ = self.request(api, self.PUT, params=params)
        return status, content

    def update_local_user(self):
        """
        PATCH :: /settings/rbac/users/local/<exiting-username>
        docs.couchbase.com/server/current/rest-api/rbac.html
        """

    def delete_local_user(self, user_name):
        """
        DELETE :: /settings/rbac/users/local/<local-username>
        docs.couchbase.com/server/current/rest-api/rbac.html
        """
        api = self.base_url + f"/settings/rbac/users/local/{user_name}"
        status, content, _ = self.request(api, self.DELETE)
        return status, content

    def create_external_user(self):
        """
        PUT :: /settings/rbac/users/local/<new-username>
        docs.couchbase.com/server/current/rest-api/rbac.html
        """

    def delete_external_user(self):
        """
        DELETE :: /settings/rbac/users/external/<external-username>
        docs.couchbase.com/server/current/rest-api/rbac.html
        """

    def create_new_group(self):
        """
        PUT :: /settings/rbac/groups/<new-group_name>
        docs.couchbase.com/server/current/rest-api/rbac.html
        """

    def delete_group(self):
        """
        DELETE :: /settings/rbac/groups/<group_name>
        docs.couchbase.com/server/current/rest-api/rbac.html
        """

    def create_custom_role(self, role_name, display_name, permissions):
        """
        PUT :: /settings/rbac/customRoles/<role_name>
        Requires custom_roles_enabled (provisioned profile).
        permissions: dict mapping permission key -> "all"|"none"
          e.g. {"cluster.collection[b:s:.]": "all",
                "cluster.collection[b:s:col]": "none"}
        Returns (status, content).
        """
        api = self.base_url + f"/settings/rbac/customRoles/{role_name}"
        body = json.dumps({"name": display_name, "permissions": permissions})
        headers = self.get_headers_for_content_type_json()
        status, content, _ = self.request(api, self.PUT, params=body,
                                          headers=headers)
        return status, content

    def delete_custom_role(self, role_name):
        """DELETE :: /settings/rbac/customRoles/<role_name>"""
        api = self.base_url + f"/settings/rbac/customRoles/{role_name}"
        status, content, _ = self.request(api, self.DELETE)
        return status, content
