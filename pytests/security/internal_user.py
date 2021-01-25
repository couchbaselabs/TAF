from global_vars import logger
from user_base_abc import UserBase
from membase.api.rest_client import RestConnection


class InternalUser(UserBase):
    def __init__(self, user_id=None, payload=None, host=None):
        self.user_id = user_id
        self.password = ""
        self.payload = payload
        self.host = host
        self.log = logger["test"]

    '''
    payload=name=<nameofuser>&roles=admin,cluster_admin&password=<password>
    if roles=<empty> user will be created with no roles
    '''
    def create_user(self):
        rest = RestConnection(self.host)
        response = rest.add_set_builtin_user(self.user_id, self.payload)
        return response

    def delete_user(self):
        try:
            rest = RestConnection(self.host)
            response = rest.delete_builtin_user(self.user_id)
        except Exception as e:
            self.log.info("Exception while deleting user. Exception is -{0}"
                          .format(e))
            response = False
        return response

    def change_password(self, user_id=None, password=None, host=None):
        if user_id:
            self.user_id = user_id
        if password:
            self.password = password
        if host:
            self.host = host

        rest = RestConnection(self.host)
        _ = rest.change_password_builtin_user(self.user_id, self.password)

    def user_setup(self, user_id=None, host=None, payload=None):
        if user_id:
            self.user_id = user_id
        if host:
            self.host = host
        if payload:
            self.payload = payload
        rest = RestConnection(self.host)
        versions = rest.get_nodes_versions()
        pre_spock = False
        for version in versions:
            if "5" > version:
                pre_spock = True
        if pre_spock:
            self.log.info(
                "At least one of the node in the cluster is on pre-spock "
                "version. Not creating user since RBAS is a spock feature.")
            return
        self.delete_user()
        self.create_user()
