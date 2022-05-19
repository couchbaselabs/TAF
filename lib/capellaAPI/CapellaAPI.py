# -*- coding: utf-8 -*-
# Generic/Built-in
import logging

from .CapellaAPIRequests import CapellaAPIRequests
import json
import base64


class CapellaAPI(CapellaAPIRequests):

    def __init__(self, url, sceret, access, user, pwd):
        super(CapellaAPI, self).__init__(url, sceret, access)
        self.user = user
        self.pwd = pwd
        self._log = logging.getLogger(__name__)
        self.perPage = 100

    def get_authorization_internal(self, url):
        basic = base64.encodestring('{}:{}'.format(self.user, self.pwd)).strip("\n")
        header = {'Authorization': 'Basic %s' % basic}
        resp = self._urllib_request(
            "{}/sessions".format(url), method="POST",
            headers=header)
        jwt = json.loads(resp.content).get("jwt")
        cbc_api_request_headers = {
           'Authorization': 'Bearer %s' % jwt,
           'Content-Type': 'application/json'
        }
        return cbc_api_request_headers

    def set_logging_level(self, level):
        self._log.setLevel(level)

    # Cluster methods
    def get_clusters(self):
        capella_api_response = self.capella_api_get('/v3/clusters')

        return (capella_api_response)

    def get_cluster_info(self, cluster_id):
        capella_api_response = self.capella_api_get('/v3/clusters/' + cluster_id)

        return (capella_api_response)

    def get_cluster_status(self, cluster_id):
        capella_api_response = self.capella_api_get('/v3/clusters/' + cluster_id + '/status')

        return (capella_api_response)

    def create_cluster(self, cluster_configuration):
        capella_api_response = self.capella_api_post('/v3/clusters', cluster_configuration)

        return (capella_api_response)

    def update_cluster_servers(self, cluster_id, new_cluster_server_configuration):
        capella_api_response = self.capella_api_put('/v3/clusters' + '/' + cluster_id + '/servers',
                                                    new_cluster_server_configuration)

        return (capella_api_response)

    def get_cluster_servers(self, cluster_id):
        response_dict = None

        capella_api_response = self.get_cluster_info(True, cluster_id)
        # Did we get the info back ?
        if capella_api_response.status_code == 200:
            # Do we have JSON response ?
            if capella_api_response.headers['content-type'] == 'application/json':
                # Is there anything in it?
                # We use response.text as this is a string
                # response.content is in bytes which we use for json.loads
                if len(capella_api_response.text) > 0:
                    response_dict = capella_api_response.json()['place']

        # return just the servers bit
        return (response_dict)

    def delete_cluster(self, cluster_id):
        capella_api_response = self.capella_api_del('/v3/clusters' + '/' + cluster_id)
        return (capella_api_response)

    def get_cluster_users(self, cluster_id):
        capella_api_response = self.capella_api_get('/v3/clusters' + '/' + cluster_id +
                                                    '/users')
        return (capella_api_response)

    def delete_cluster_user(self, cluster_id, cluster_user):
        capella_api_response = self.capella_api_del('/v3/clusters' + '/' + cluster_id +
                                                    '/users/'+ cluster_user)
        return (capella_api_response)

    # Cluster certificate
    def get_cluster_certificate(self, cluster_id):
        capella_api_response = self.capella_api_get('/v3/clusters' + '/' + cluster_id +
                                                    '/certificate')
        return (capella_api_response)

    # Cluster buckets
    def get_cluster_buckets(self, cluster_id):
        capella_api_response = self.capella_api_get('/v2/clusters' + '/' + cluster_id +
                                                    '/buckets')
        return (capella_api_response)

    def create_cluster_bucket(self, cluster_id, bucket_configuration):
        capella_api_response = self.capella_api_post('/v2/clusters' + '/' + cluster_id +
                                                     '/buckets', bucket_configuration)
        return (capella_api_response)

    def update_cluster_bucket(self, cluster_id, bucket_id, new_bucket_configuration):
        capella_api_response = self.capella_api_put('/v2/clusters' + '/' + cluster_id +
                                                    '/buckets/' + bucket_id , new_bucket_configuration)
        return (capella_api_response)

    def delete_cluster_bucket(self, cluster_id, bucket_configuration):
        capella_api_response = self.capella_api_del('/v2/clusters' + '/' + cluster_id +
                                                    '/buckets', bucket_configuration)
        return (capella_api_response)

    # Cluster Allow lists
    def get_cluster_allowlist(self, cluster_id):
        capella_api_response = self.capella_api_get('/v2/clusters' + '/' + cluster_id +
                                                    '/allowlist')
        return (capella_api_response)

    def delete_cluster_allowlist(self, cluster_id, allowlist_configuration):
        capella_api_response = self.capella_api_del('/v2/clusters' + '/' + cluster_id +
                                                    '/allowlist', allowlist_configuration)
        return (capella_api_response)

    def create_cluster_allowlist(self, cluster_id, allowlist_configuration):
        capella_api_response = self.capella_api_post('/v2/clusters' + '/' + cluster_id +
                                                     '/allowlist', allowlist_configuration)
        return (capella_api_response)

    def update_cluster_allowlist(self, cluster_id, new_allowlist_configuration):
        capella_api_response = self.capella_api_put('/v2/clusters' + '/' + cluster_id +
                                                    '/allowlist', new_allowlist_configuration)
        return (capella_api_response)

    # Cluster user
    def create_cluster_user(self, cluster_id, cluster_user_configuration):
        capella_api_response = self.capella_api_post('/v3/clusters' + '/' + cluster_id +
                                                     '/users', cluster_user_configuration)
        return (capella_api_response)

    # Capella Users
    def get_users(self):
        capella_api_response = self.capella_api_get('/v2/users?perPage=' + str(self.perPage))
        return (capella_api_response)

    def create_project(self, url, tenant_id, name):
        capella_header = self.get_authorization_internal(url)
        project_details = {"name": name, "tenantId": tenant_id}

        url = '{}/v2/organizations/{}/projects'.format(url, tenant_id)
        capella_api_response = self._urllib_request(url, method="POST",
                                                    params=json.dumps(project_details),
                                                    headers=capella_header)
        return capella_api_response

    def delete_project(self, url, tenant_id, project_id):
        capella_header = self.get_authorization_internal(url)
        url = '{}/v2/organizations/{}/projects/{}'.format(url, tenant_id,
                                                          project_id)
        capella_api_response = self._urllib_request(url, method="DELETE",
                                                    params='',
                                                    headers=capella_header)
        return capella_api_response

    def create_bucket(self, url, tenant_id, project_id, cluster_id,
                      bucket_params):
        capella_header = self.get_authorization_internal(url)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(url, tenant_id, project_id, cluster_id)
        url = '{}/buckets'.format(url)
        default = {"name": "default", "bucketConflictResolution": "seqno",
                   "memoryAllocationInMb": 100, "flush": False, "replicas": 0,
                   "durabilityLevel": "none", "timeToLive": None}
        default.update(bucket_params)
        resp = self._urllib_request(url, method="POST",
                                    params=json.dumps(default),
                                    headers=capella_header)
        return resp

    def get_buckets(self, url, tenant_id, project_id, cluster_id):
        capella_header = self.get_authorization_internal(url)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(url, tenant_id, project_id, cluster_id)
        url = '{}/buckets'.format(url)
        resp = self._urllib_request(url, method="GET", params='',
                                    headers=capella_header)
        return resp

    def flush_bucket(self, url, tenant_id, project_id, cluster_id, bucket_id):
        capella_header = self.get_authorization_internal(url)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(url, tenant_id, project_id, cluster_id)
        url = url + "/" + bucket_id + "/flush"
        resp = self._urllib_request(url, method="POST",
                                    headers=capella_header)
        return resp

    def delete_bucket(self, url, tenant_id, project_id, cluster_id,
                      bucket_id):
        capella_header = self.get_authorization_internal(url)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(url, tenant_id, project_id, cluster_id)
        url = '{}/buckets/{}'.format(url, bucket_id)
        resp = self._urllib_request(url, method="DELETE",
                                    headers=capella_header)
        return resp

    def update_bucket_settings(self, url, tenant_id, project_id, cluster_id,
                               bucket_id, bucket_params):
        capella_header = self.get_authorization_internal(url)
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/{}" \
            .format(url, tenant_id, project_id,
                    cluster_id, bucket_id)
        resp = self._urllib_request(url, method="PUT", headers=capella_header,
                                    params=json.dumps(bucket_params))
        return resp

    def jobs(self, url, project_id, tenant_id, cluster_id):
        capella_header = self.get_authorization_internal(url)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(url, tenant_id, project_id, cluster_id)
        url = '{}/jobs'.format(url)
        resp = self._urllib_request(url, method="GET", params='',
                                    headers=capella_header)
        return resp

    def get_cluster_internal(self, url, tenant_id, project_id, cluster_id):
        capella_header = self.get_authorization_internal(url)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(url, tenant_id, project_id, cluster_id)

        resp = self._urllib_request(url, method="GET",
                                    params='', headers=capella_header)
        return resp

    def get_nodes(self, url, tenant_id, project_id, cluster_id):
        capella_header = self.get_authorization_internal(url)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(url, tenant_id, project_id, cluster_id)
        url = '{}/nodes'.format(url)
        resp = self._urllib_request(url, method="GET", params='',
                                    headers=capella_header)
        return resp

    def get_db_users(self, url, tenant_id, project_id, cluster_id,
                     page=1, limit=100):
        capella_header = self.get_authorization_internal(url)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
              .format(url, tenant_id, project_id, cluster_id)
        url = url + '/users?page=%s&perPage=%s' % (page, limit)
        resp = self._urllib_request(url, method="GET", headers=capella_header)
        return resp

    def delete_db_user(self, url, tenant_id, project_id, cluster_id, user_id):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/users/{}" \
              .format(url, tenant_id, project_id, cluster_id,
                      user_id)
        print(url)

    def create_db_user(self, url, tenant_id, project_id, cluster_id,
                       user, pwd):
        capella_header = self.get_authorization_internal(url)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(url, tenant_id, project_id, cluster_id)
        body = {"name": user, "password": pwd,
                "permissions": {"data_reader": {}, "data_writer": {}}}
        url = '{}/users'.format(url)
        resp = self._urllib_request(url, method="POST",
                                    params=json.dumps(body),
                                    headers=capella_header)
        return resp

    def add_allowed_ip(self, url, tenant_id, project_id, cluster_id):
        capella_header = self.get_authorization_internal(url)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(url, tenant_id, project_id, cluster_id)
        resp = self._urllib_request("https://ifconfig.me", method="GET")
        if resp.status_code != 200:
            raise Exception("Fetch public IP failed!")
        body = {"create": [{"cidr": "{}/32".format(resp.content),
                            "comment": ""}]}
        url = '{}/allowlists-bulk'.format(url)
        resp = self._urllib_request(url, method="POST",
                                    params=json.dumps(body),
                                    headers=capella_header)
        return resp

    def load_sample_bucket(self, url, tenant_id, project_id, cluster_id,
                           bucket_name):
        capella_header = self.get_authorization_internal(url)
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/samples"\
              .format(url, tenant_id, project_id, cluster_id)
        param = {'name': bucket_name}
        resp = self._urllib_request(url, method="POST",
                                    params=json.dumps(param),
                                    headers=capella_header)
        return resp
