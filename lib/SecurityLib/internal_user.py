from SecurityLib.user_base_abc import UserBase
from cb_server_rest_util.security.security_api import SecurityRestAPI
from global_vars import logger
from membase.api.rest_client import RestConnection


class InternalUser(UserBase):
    def __init__(self, user_id=None, payload=None, host=None):
        self.user_id = user_id
        self.password = ""
        self.payload = payload
        self.host = host
        self.log = logger.get("infra")

    '''
    payload=name=<nameofuser>&roles=admin,cluster_admin&password=<password>
    if roles=<empty> user will be created with no roles
    '''
    def create_user(self):
        rest = SecurityRestAPI(self.host)
        status, response = rest.create_local_user(self.user_id, self.payload)
        if status is False:
            self.log.critical(f"Create user failed with msg: {response}")
        return response

    def delete_user(self):
        status = False
        try:
            rest = SecurityRestAPI(self.host)
            status, resp = rest.delete_local_user(self.user_id)
            if status is False:
                raise Exception(f"Delete user '{self.user_id}' failed: '{resp}'")
        except Exception as e:
            self.log.error(f"Exception while deleting user. {str(e)}")
        return status

    def change_password(self, user_id=None, password=None, host=None):
        if user_id:
            self.user_id = user_id
        if password:
            self.password = password
        if host:
            self.host = host

        rest = RestConnection(self.host)
        response = rest.change_password_builtin_user(self.user_id,
                                                     self.password)
        return response

    def exists_users(self):
        json_output = dict()
        try:
            rest = SecurityRestAPI(self.host)
            status, json_output = rest.list_current_users_and_roles()
            if status is False:
                raise Exception(str(json_output))
        except Exception as e:
            self.log.error(f"Get users API failed: {str(e)}")
        return json_output

    def user_setup(self, user_id=None, host=None, payload=None):
        if user_id:
            self.user_id = user_id
        if host:
            self.host = host
        if payload:
            self.payload = payload
        # rest = RestConnection(self.host)
        # cluster_compatibility = rest.check_cluster_compatibility("5.0")
        # if cluster_compatibility is None:
        #     pre_spock = True
        # else:
        #     pre_spock = not cluster_compatibility
        # if pre_spock:
        #     self.log.info("At least one of the node in the cluster is on "
        #                   "pre-spock version. Not creating user since "
        #                   "RBAC is a spock feature."
        #     return
        # check if the atleast some users exist before running
        json_output = self.exists_users()
        if json_output:
            for user_role in json_output:
                if user_role["id"] == self.user_id:
                    self.delete_user()
        self.create_user()
