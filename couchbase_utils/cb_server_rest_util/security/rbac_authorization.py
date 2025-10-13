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

    def check_permissions(self):
        """
        POST :: /pools/default/checkPermissions
        docs.couchbase.com/server/current/rest-api/rbac.html
        """

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
