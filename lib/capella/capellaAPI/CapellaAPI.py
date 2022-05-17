# -*- coding: utf-8 -*-
# Generic/Built-in
import logging

from .CapellaAPIRequests import CapellaAPIRequests


class CapellaAPI(CapellaAPIRequests):

    def __init__(self, url, sceret, access):
        super(CapellaAPI, self).__init__(url, sceret, access)
        self._log = logging.getLogger(__name__)
        self.perPage = 100

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
