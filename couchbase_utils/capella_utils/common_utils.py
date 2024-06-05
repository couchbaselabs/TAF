'''
Created on Aug 10, 2022

@author: ritesh.agarwal
'''
from capellaAPI.capella.common.CapellaAPI import CommonCapellaAPI
from global_vars import logger
import json
import time


class Fleet:
    def __init__(self, tenants, clusters, serverless_dp):
        self.num_tenants = tenants
        self.num_provisioned = clusters
        self.num_serverless_dp = serverless_dp


class Pod:
    counter = 0
    def __init__(self, url_public, TOKEN_FOR_INTERNAL_SUPPORT=None, signup_token=None,
                 override_token=None):
        self.url_public = url_public
        self.TOKEN = TOKEN_FOR_INTERNAL_SUPPORT
        self.signup_token = signup_token
        self.override_key = override_token
        self.log = logger.get("test")
        
    def create_tenants(self, num_tenants, email="random.user@couchbase.com"):
        if num_tenants == 0:
            return

        def seed_email(email):
            a, b = email.split("@")
            Pod.counter += 1
            return "{}".format(a), "{}+{}@{}".format(a, Pod.counter, b)

        self.commonAPI = CommonCapellaAPI(
            self.url_public, None, None, None, None)

        self.log.info("Singup token: {0}".format(self.signup_token))
        tenants = list()
        for _ in range(num_tenants):
            full_name, seed_mail = seed_email(email)
            seed_pwd = "Couch@123"
            resp = self.commonAPI.signup_user(full_name, seed_mail, seed_pwd,
                                              full_name, self.signup_token)
            resp.raise_for_status()
            verify_token = resp.headers.get("Vnd-project-Avengers-com-e2e-token", None)
            user_id = resp.json()["userId"]
            tenant_id = resp.json()["tenantId"]
            if verify_token:
                resp = self.commonAPI.verify_email(verify_token)
                resp.raise_for_status()
            self.log.info("Tenant Created - tenantID: {}, user: {}, pwd: {}".format(
                tenant_id, seed_mail, seed_pwd))
            tenant = Tenant(tenant_id,
                            seed_mail,
                            seed_pwd)
            tenant.name = full_name
            tenants.append(tenant)
        return tenants

    def wait_for_tenant_activation(self, tenant, snaplogic_token):
        self.commonAPI = CommonCapellaAPI(
            self.url_public, None, None, None, None,
            TOKEN_FOR_SNAPLOGIC=snaplogic_token)
        self.log.info("Waiting for tenant activation: %s" % tenant.id)
        while True:
            resp = self.commonAPI.tenant_activation()
            if resp.status_code != 200:
                resp.raise_for_status()
            tenants = json.loads(resp.content)
            _tenant = [_tenant for _tenant in tenants if _tenant["tenantId"] == tenant.id]
            if _tenant:
                if _tenant[0]["status"] == "Active":
                    self.log.info("Tenant {} is ACTIVE".format(tenant.id))
                    return
                else:
                    self.log.info("Tenant {} is {}".format(tenant.id), _tenant[0]["status"])
            time.sleep(10)

    def activate_resources(self, tenant, accountID):
        self.commonAPI = CommonCapellaAPI(
            self.url_public, None, None, None, None,
            TOKEN_FOR_INTERNAL_SUPPORT=self.TOKEN)
        body = {
            "tenantId": tenant.id,
            "accountId": accountID,
        }
        self.log.info("Activating aws container resources for tenant: %s" % tenant.id)
        resp = self.commonAPI.activate_resource_container("aws", body)
        if resp.status_code != 204:
            resp.raise_for_status()

class Tenant:
    def __init__(self, id, user, pwd,
                 secret=None, access=None):
        self.id = id
        self.user = user
        self.pwd = pwd
        self.api_secret_key = secret
        self.api_access_key = access
        # This will be used to destroy the key in teardown.
        self.api_key_id = None
        self.projects = []
        self.clusters = list()
        self.columnar_instances = list()
        self.xdcr_clusters = list()
        self.users = [User(self.user, self.pwd)]

class User:
    def __init__(self, email, pwd):
        self.email = email
        self.pwd = pwd
