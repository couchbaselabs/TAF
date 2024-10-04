# -*- coding: utf-8 -*-
# Generic/Built-in
"""
Created on 04-October-2024

@author: Abhay Aggrawal

This utility is for performing actions on Molo17 connector.
"""

from capellaAPI.capella.lib.APIRequests import APIRequests
from  kafka_util.confluent_utils import ConfluentCloudAPIs

class Molo17APIs(object):
    """
        This class contains collection of API to access GlueSync Molo17.
    """

    def __init__(self, url, username=None, password=None):
        self.username = username
        self.password = password
        self.api_request = APIRequests(url)
        self.authentication_endpoint = "/authentication"
        self.pipeline_endpoints = "/pipelines"
        self.set_authentication_keys(username, password)
        self.set_bearer_token(username, password)

    def set_bearer_token(self, username, password):
        url = self.authentication_endpoint + "/login"
        payload = {
            "username": username,
            "password": password
        }
        response = self.api_request.api_post(url, payload)
        if response.status_code == 200:
            resp = response.json()
            self.api_request.bearer_token = resp["apiToken"]
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def set_authentication_keys(self, access, secret):
        self.api_request.ACCESS = access
        self.api_request.SECRET = secret

    def generate_auth_header(self):
        api_request_headers = {
            'Authorization': 'Bearer ' + self.api_request.bearer_token,
            'Content-Type': 'application/json'
        }
        # Return the header
        return api_request_headers

    def get_all_pipelines(self):
        """
        Method to get all pipelines
        """
        url = self.pipeline_endpoints
        response = self.api_request.api_get(
            url, headers=self.generate_auth_header())
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def get_pipeline(self, pipeline_id):
        """
        Method to get specific pipeline
        """
        url = self.pipeline_endpoints + "/{0}".format(pipeline_id)
        response = self.api_request.api_get(url, headers=self.generate_auth_header())
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def create_pipeline(self, name, description):
        """
        Method to create new pipeline
        """
        url = self.pipeline_endpoints
        payload = {
            "name": name,
            "description": description
        }
        response = self.api_request.api_post(url, payload, headers=self.generate_auth_header())
        if response.status_code == 200:
            resp = response.json()
            return resp["pipelineId"]
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def get_unassigned_agents(self):
        """
        Method to get all unassigned agents
        """
        url = self.api_request.API_BASE_URL + "/unassigned-agent"
        response = self.api_request.api_get(url, headers=self.generate_auth_header())
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def delete_pipelines(self, pipeline_id):
        """
        Method to delete a pipeline
        """
        url = self.pipeline_endpoints + "/{0}".format(pipeline_id)
        response = self.api_request.api_del(url, headers=self.generate_auth_header())
        if response.status_code == 200:
            return True
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def update_pipelines(self, pipeline_id, name=None, description=None):
        """
        Method to update name and description in pipeline
        """
        url = self.pipeline_endpoints + "/{0}".format(pipeline_id)
        payload = {}
        if name:
            payload["name"] = name
        if description:
            payload["description"] = description
        response = self.api_request.api_put(url, payload, headers=self.generate_auth_header())
        if response.status_code == 200:
            return response.json()["pipelineId"]
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def get_pipeline_agents(self, pipeline_id):
        """
        Method to get all agents associated to pipeline
        """
        url = self.pipeline_endpoints + "/{0}/agents".format(pipeline_id)
        response = self.api_request.api_get(url, headers=self.generate_auth_header())
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def delete_agent_from_pipeline(self, pipeline_id, agent_id):
        """
        Method to de-associate an agent from pipeline
        """
        url = self.pipeline_endpoints + "/{0}/agents/{1}".format(pipeline_id, agent_id)
        response = self.api_request.api_del(url, headers=self.generate_auth_header())
        if response == 200:
            return True
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def get_specific_agent_from_pipeline(self, pipeline_id, agent_id):
        """
        Method to get information of specific agent in pipeline
        """
        url = self.pipeline_endpoints + "/{0}/agents/{1}".format(pipeline_id, agent_id)
        response = self.api_request.api_get(url, headers=self.generate_auth_header())
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def assign_agent_to_pipeline(self, pipeline_id, agent_id):
        """
        Method to associate an agent to a pipeline
        """
        url = self.pipeline_endpoints + "/{0}/agents/{1}".format(pipeline_id, agent_id)
        response =  self.api_request.api_put(url, request_body={}, headers=self.generate_auth_header())
        if response.status_code == 200:
            return True
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def configure_agent_credentials(self, pipeline_id, agent_id, connection_name="", host=None, port=0,
                                    database_name="", username=None, password=None, disable_auth=False,
                                    enable_tls=False, trust_server_certificate=False, tls_name=None,
                                    trust_store_path=None, trust_store_password=None, key_store_path=None,
                                    key_store_password=None, certificate_path=None, certificate_password=None,
                                    additional_hosts=None, max_connections_count=100, custom_key=None,
                                    custom_key_value=None, custom_value=None, custom_value_value=None,
                                    connection_string=""):
        """
        Method to configure credentials of an agent
        """
        url = self.pipeline_endpoints + "/{0}/agents/{1}".format(pipeline_id, agent_id) + "/configure/credentials"
        payload = {"hostCredentials": {"connectionName":connection_name, "connectionString":connection_string,
                                       "databaseName": database_name, "disableAuth": disable_auth,
                                       "enableTls": enable_tls, "host":host,
                                       "maxConnectionsCount": max_connections_count, "password": password,
                                       "username":username, "port":port,
                                       "trustServerCertificate": trust_server_certificate,
                                       "additionalHosts":additional_hosts if additional_hosts else []},
                   "customHostCredentials": {}}
        if enable_tls and trust_server_certificate:
            payload["hostCredentials"]["tlsName"] = tls_name
            payload["hostCredentials"]["trustStorePath"] = trust_store_path
            payload["hostCredentials"]["trustStorePassword"] = trust_store_password
            payload["hostCredentials"]["keyStorePath"] = key_store_path
            payload["hostCredentials"]["keyStorePassword"] = key_store_password
            payload["hostCredentials"]["certificatePath"]  = certificate_path
            payload["hostCredentials"]["certificatePassword"] = certificate_password
        if custom_key and custom_value:
            payload["customHostCredentials"]= {custom_key:custom_key_value, custom_value:custom_value_value}

        response = self.api_request.api_put(url, payload, headers=self.generate_auth_header())
        if response.status_code == 202:
            return
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def configure_agent_specific(self, pipeline_id, agent_id, key, value):
        """
        Method to specify specifics of an agent
        """
        url = self.pipeline_endpoints + "/{0}/agents/{1}".format(pipeline_id, agent_id) + "/configure/specific"
        payload= {"key": key, "value": value}
        response = self.api_request.api_put(url, payload, headers=self.generate_auth_header())
        if response.status_code == 202:
            return
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def get_columns_of_table(self, pipeline_id, agent_id, table_schema, table_name):
        """
        Get table columns of a specific schema of a table
        """
        url = self.pipeline_endpoints + "/{0}/agents/{1}".format(pipeline_id, agent_id) + "/discovery/columns"
        params = {"tableschema": table_schema, "tablename": table_name}
        response = self.api_request.api_get(url, params=params, headers=self.generate_auth_header())
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def get_schema_of_agent(self, pipeline_id, agent_id):
        """
        Method of get schema of the agent
        """
        url = self.pipeline_endpoints + "/{0}/agents/{1}".format(pipeline_id, agent_id) + "/discovery/schemas"
        response = self.api_request.api_get(url, headers=self.generate_auth_header())
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def get_tables_from_schema(self, pipeline_id, agent_id, table_schema):
        """
        Method to get tables from the schema
        """
        url = self.pipeline_endpoints + "/{0}/agents/{1}".format(pipeline_id, agent_id) + "/discovery/tables"
        params = {"schema": table_schema}
        response = self.api_request.api_get(url, params=params, headers=self.generate_auth_header())
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def upsert_entities_of_pipeline_agent(self, pipeline_id, entity_name, source_entity, target_entity):
        """
        Upsert entity of an agent in pipeline
        """
        url = self.pipeline_endpoints + ("/{0}/config/entities".format(pipeline_id))
        payload = {"entities":[{"entityId":"","entityName":entity_name,"agentEntities":[source_entity, target_entity]}]}
        response = self.api_request.api_put(url, payload, headers=self.generate_auth_header())
        if response.status_code == 200:
            return True
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def get_pipeline_config(self, pipeline_id):
        """
        Method to get all config of pipeline
        """
        url = self.pipeline_endpoints + ("/{0}/config".format(pipeline_id))
        response = self.api_request.api_get(url, headers=self.generate_auth_header())
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def set_pipeline_config(self, pipeline_id, body):
        """
        Method to add all configs to a pipeline
        """
        url = self.pipeline_endpoints + ("/{0}/config".format(pipeline_id))
        response = self.api_request.api_put(url, body, headers=self.generate_auth_header())
        if response.status_code == 202:
            return True
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def delete_entity(self, pipeline_id, entity_id):
        """
        Method to delete a pipeline entity
        """
        url = self.pipeline_endpoints + ("/{0}/config".format(pipeline_id)) + "/entities/{0}".format(entity_id)
        response = self.api_request.api_del(url, headers=self.generate_auth_header())
        if response.status_code == 200:
            return True
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def get_all_entities(self, pipeline_id):
        """
        Method to get all entities in a pipeline
        """
        url = self.pipeline_endpoints + "{0}/entities".format(pipeline_id)
        response = self.api_request.api_get(url, headers=self.generate_auth_header())
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def get_specific_entity(self, pipeline_id, entity_id):
        """
        Method to get a specific entity in a pipeline
        """
        url = self.pipeline_endpoints + "{0}/entities/{1}".format(pipeline_id, entity_id)
        response = self.api_request.api_get(url, headers=self.generate_auth_header())
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )

    def complete_pipeline_configuration(self, pipeline_id, name, description):
        """
        Method to specify that a pipeline configuration is complete
        """
        url = self.pipeline_endpoints + "/{0}".format(pipeline_id)
        payload = {"name":name, "description": description, "configurationCompleted":True}
        response = self.api_request.api_put(url, payload, headers=self.generate_auth_header())
        if response.status_code == 200:
            return response.json()["pipelineId"]
        else:
            raise Exception(
                "Following error occurred while creating apiToken - "
                "{0}".format(ConfluentCloudAPIs.parse_error(response))
            )
