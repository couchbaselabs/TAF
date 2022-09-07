
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

    @staticmethod
    def create_serverless_dataplane(pod, config=dict()):
        capella_api = CapellaAPI(pod.url_public, None, None, pod.TOKEN)
        resp = capella_api.create_serverless_dataplane(config)
        if resp.status_code != 200:
            CapellaUtils.log.critical("Data plane creation failed with status\
            code:{} and error message:{}".format(resp.status_code, resp.content))
            CapellaUtils.log.critical("Response Json:{}".format(resp))

        return resp.content["dataplaneId"]

    @staticmethod
    def get_dataplane_deployment_status(pod, dataplane_id):
        capella_api = CapellaAPI(pod.url_public, None, None, pod.TOKEN)
        resp = capella_api.get_dataplane_deployment_status(dataplane_id)
        CapellaUtils.log.info(resp)
        return (resp["status"]["state"])

    @staticmethod
    def create_serverless_database(pod, tenant, database_name,
                                   provider, region, width=1, weight=30,
                                   dataplane_id=""):
        database_config = {
            "name": database_name,
            "projectId": tenant.project_id,
            "provider": provider,
            "region": region,
            "overRide": {
                "weight": weight,
                "width": width,
                "dataplaneId": dataplane_id
                }
        }
        capella_api = CapellaAPI(pod.url_public, tenant.user, tenant.pwd)
        resp = capella_api.create_serverless_database(tenant.id, database_config)
        if resp.status_code != 202:
            raise Exception("Create database failed: {}".
                            format(resp.content))
        databaseId = json.loads(resp.content)['databaseId']
        return databaseId

    @staticmethod
    def is_database_ready(pod, tenant, database_id):
        capella_api = CapellaAPI(pod.url_public, tenant.user, tenant.pwd)
        resp = capella_api.get_serverless_db_info(tenant.id, tenant.project_id,
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
            resp = capella_api.get_serverless_db_info(tenant.id, tenant.project_id,
                                                      database_id)
            state = json.loads(resp.content).get("data").get("status").get("state")
        if state != "healthy":
            raise Exception("Deploying a serverless database failed!")
        CapellaUtils.log.info("Database {} is ready to use: {}".
                              format(database_id, state))
        return json.loads(resp.content).get("data").get("status").get("state")

    @staticmethod
    def get_database_details(pod, tenant, database_id):
        capella_api = CapellaAPI(pod.url_public, tenant.user, tenant.pwd)
        resp = capella_api.get_serverless_db_info(tenant.id, tenant.project_id,
                                                  database_id)
        if resp.status_code != 200:
            raise Exception("Fetch database details failed: {}".
                            format(resp.content))
        return json.loads(resp.content).get("data")

    @staticmethod
    def allow_my_ip(pod, tenant, database_id):
        capella_api = CapellaAPI(pod.url_public, tenant.user, tenant.pwd)
        resp = capella_api.allow_my_ip(tenant.id, tenant.project_id,
                                       database_id)
        if resp.status_code != 200:
            result = json.loads(resp.content)
            if result["errorType"] == "ErrAllowListsCreateDuplicateCIDR":
                CapellaUtils.log.warn("IP is already added: %s" % result["message"])
                return
            CapellaUtils.log.critical(resp.content)
            raise Exception("Adding allowed IP failed.")

    @staticmethod
    def get_database_nebula_endpoint(pod, tenant, database_id):
        return CapellaUtils.get_database_details(pod, tenant,
                                                 database_id)['connect']['srv']

    @staticmethod
    def get_database_debug_info(pod, database_id):
        capella_api = CapellaAPI(pod.url_public, None, None, pod.TOKEN)
        resp = capella_api.get_serverless_database_debugInfo(database_id)
        if resp.status_code != 200:
            raise Exception("Get database debugging info failed: {}".
                            format(resp.content))
        return json.loads(resp.content)

    @staticmethod
    def get_database_dataplane_id(pod, database_id):
        resp = CapellaUtils.get_database_debug_info(pod, database_id)
        return resp["database"]["config"]["dataplaneId"]

    @staticmethod
    def get_database_DAPI(pod, tenant, database_id):
        return CapellaUtils.get_database_details(pod, tenant,
                                                 database_id)['connect']['dataApi']

    @staticmethod
    def get_database_deployment_status(database_id, tenant_id, project_id):
        all_databases = CapellaUtils.list_all_databases(tenant_id, project_id)
        for database in all_databases:
            if database['data']['id'] == database_id:
                return database['data']['status']['state']

    @staticmethod
    def list_all_databases(pod, tenant):
        capella_api = CapellaAPI(pod.url_public, tenant.user, tenant.pwd)
        resp = capella_api.list_all_databases(tenant.id, tenant.project_id)
        CapellaUtils.log.info(resp)
        all_databases = json.loads(resp.content)['data']
        return all_databases

    @staticmethod
    def add_ip_allowlists(pod, tenant, database_id, cidr):
        allowlist_config = {"create": [{"cidr": cidr}]}
        capella_api = CapellaAPI(pod.url_public, tenant.user, tenant.pwd)
        capella_api.add_ip_allowlists(tenant.id, database_id,
                                      tenant.project_id,
                                      allowlist_config)

    @staticmethod
    def generate_keys(pod, tenant, database_id):
        capella_api = CapellaAPI(pod.url_public, tenant.user, tenant.pwd)
        resp = capella_api.generate_keys(tenant.id, tenant.project_id,
                                         database_id)
        if resp.status_code != 201:
            raise Exception("Create database keys failed: {}".
                            format(resp.content))
        keys = json.loads(resp.content)
        return keys["access"], keys["secret"]

    @staticmethod
    def delete_database(pod, tenant, database_id):
        capella_api = CapellaAPI(pod.url_public, tenant.user, tenant.pwd)
        resp = capella_api.delete_database(tenant.id, tenant.project_id,
                                           database_id)
        return resp

    @staticmethod
    def delete_dataplane(pod, dataplane_id):
        capella_api = CapellaAPI(pod.url_public, None, None, pod.TOKEN)
        resp = capella_api.delete_dataplane(dataplane_id)
        CapellaUtils.log.info("delete_serverless_dataplane response:{}".
                              format(resp))
        return resp

    @staticmethod
    def bypass_dataplane(pod, dataplane_id):
        capella_api = CapellaAPI(pod.url_public, None, None, pod.TOKEN)
        resp = capella_api.get_access_to_serverless_dataplane_nodes(dataplane_id)
        CapellaUtils.log.info("bypass DN response:{}".
                              format(resp))
        if resp.status_code != 200:
            raise Exception("Bypass DN failed: {}".
                            format(resp.content))
        data = json.loads(resp.content)["couchbaseCreds"]
        return data["username"], data["password"], data["srv"]
