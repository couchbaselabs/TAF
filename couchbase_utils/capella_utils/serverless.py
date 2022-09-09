
'''
Created on Aug 10, 2022

@author: ritesh.agarwal
'''


# -*- coding: utf-8 -*-
# Generic/Built-in
import logging

import json
from capellaAPI.capella.serverless.CapellaAPI import CapellaAPI
import time


class CapellaUtils:
    log = logging.getLogger(__name__)

    def __init__(self, cluster):
        self.capella_api = CapellaAPI(cluster.pod.url_public,
                                      cluster.tenant.user,
                                      cluster.tenant.pwd,
                                      cluster.pod.TOKEN)

    def create_serverless_dataplane(self, pod, config=dict()):
        resp = self.capella_api.create_serverless_dataplane(config)
        if resp.status_code != 202:
            CapellaUtils.log.critical("Data plane creation failed with status\
            code:{} and error message:{}".format(resp.status_code, resp.content))
            CapellaUtils.log.critical("Response Json:{}".format(resp))

        return json.loads(resp.content)["dataplaneId"]

    def get_dataplane_deployment_status(self, pod, dataplane_id):
        resp = self.capella_api.get_dataplane_deployment_status(dataplane_id)
        if resp.status_code != 200:
            CapellaUtils.log.critical("Get Data plane status failed with status\
            code:{} and error message:{}".format(resp.status_code, resp.content))
        return (json.loads(resp.content)["status"]["state"])

    def create_serverless_database(self, pod, tenant, database_name,
                                   provider, region, width=None, weight=None,
                                   dataplane_id=None):
        database_config = {
            "name": database_name,
            "projectId": tenant.project_id,
            "provider": provider,
            "region": region,
        }
        if width or weight or dataplane_id:
            database_config.update({
                "tenantId": tenant.id,
                "overRide": {
                    "weight": weight,
                    "width": width,
                    "dataplaneId": dataplane_id
                }})
            resp = self.capella_api.create_serverless_database_overRide(database_config)
        else:
            resp = self.capella_api.create_serverless_database(tenant.id, database_config)
        if resp.status_code != 202:
            raise Exception("Create database failed: {}".
                            format(resp.content))
        databaseId = json.loads(resp.content)['databaseId']
        return databaseId

    def is_database_ready(self, pod, tenant, database_id):
        resp = self.capella_api.get_serverless_db_info(tenant.id, tenant.project_id,
                                                  database_id)
        if resp.status_code != 200:
            raise Exception("Fetch database details failed: {}".
                            format(resp.content))
        state = json.loads(resp.content).get("data").get("status").get("state")
        end_time = time.time() + 120
        while state != "healthy" and time.time() < end_time:
            CapellaUtils.log.info("Wait for database {} to be ready to use: {}".
                                  format(database_id, state))
            time.sleep(5)
            resp = self.capella_api.get_serverless_db_info(tenant.id, tenant.project_id,
                                                      database_id)
            state = json.loads(resp.content).get("data").get("status").get("state")
        if state != "healthy":
            raise Exception("Deploying a serverless database failed!")
        CapellaUtils.log.info("Database {} is ready to use: {}".
                              format(database_id, state))
        return json.loads(resp.content).get("data").get("status").get("state")

    def get_database_details(self, pod, tenant, database_id):
        resp = self.capella_api.get_serverless_db_info(tenant.id, tenant.project_id,
                                                  database_id)
        if resp.status_code != 200:
            raise Exception("Fetch database details failed: {}".
                            format(resp.content))
        return json.loads(resp.content).get("data")

    def allow_my_ip(self, pod, tenant, database_id):
        resp = self.capella_api.allow_my_ip(tenant.id, tenant.project_id,
                                       database_id)
        if resp.status_code != 200:
            result = json.loads(resp.content)
            if result["errorType"] == "ErrAllowListsCreateDuplicateCIDR":
                CapellaUtils.log.warn("IP is already added: %s" % result["message"])
                return
            CapellaUtils.log.critical(resp.content)
            raise Exception("Adding allowed IP failed.")

    def get_database_nebula_endpoint(self, pod, tenant, database_id):
        return self.get_database_details(pod, tenant,
                                                 database_id)['connect']['srv']

    def get_database_debug_info(self, pod, database_id):
        resp = self.capella_api.get_serverless_database_debugInfo(database_id)
        if resp.status_code != 200:
            raise Exception("Get database debugging info failed: {}".
                            format(resp.content))
        return json.loads(resp.content)

    def get_database_dataplane_id(self, pod, database_id):
        resp = self.get_database_debug_info(pod, database_id)
        return resp["database"]["config"]["dataplaneId"]

    def get_database_DAPI(self, pod, tenant, database_id):
        return self.get_database_details(pod, tenant,
                                                 database_id)['connect']['dataApi']

    def get_database_deployment_status(self, database_id, tenant_id, project_id):
        all_databases = self.list_all_databases(tenant_id, project_id)
        for database in all_databases:
            if database['data']['id'] == database_id:
                return database['data']['status']['state']

    def list_all_databases(self, pod, tenant):
        resp = self.capella_api.list_all_databases(tenant.id, tenant.project_id)
        CapellaUtils.log.info(resp)
        all_databases = json.loads(resp.content)['data']
        return all_databases

    def add_ip_allowlists(self, pod, tenant, database_id, cidr):
        allowlist_config = {"create": [{"cidr": cidr}]}
        self.capella_api.add_ip_allowlists(tenant.id, database_id,
                                           tenant.project_id,
                                           allowlist_config)

    def generate_keys(self, pod, tenant, database_id):
        resp = self.capella_api.generate_keys(tenant.id, tenant.project_id,
                                              database_id)
        if resp.status_code != 201:
            raise Exception("Create database keys failed: {}".
                            format(resp.content))
        keys = json.loads(resp.content)
        return keys["access"], keys["secret"]

    def delete_database(self, pod, tenant, database_id):
        resp = self.capella_api.delete_database(tenant.id, tenant.project_id,
                                                database_id)
        return resp

    def delete_dataplane(self, dataplane_id):
        resp = self.capella_api.delete_dataplane(dataplane_id)
        CapellaUtils.log.info("delete_serverless_dataplane response:{}".
                              format(resp))
        return resp

    def bypass_dataplane(self, pod, dataplane_id):
        resp = self.capella_api.get_access_to_serverless_dataplane_nodes(dataplane_id)
        CapellaUtils.log.info("bypass DN response:{}".
                              format(resp))
        if resp.status_code != 200:
            raise Exception("Bypass DN failed: {}".
                            format(resp.content))
        data = json.loads(resp.content)["couchbaseCreds"]
        return data["username"], data["password"], data["srv"]
