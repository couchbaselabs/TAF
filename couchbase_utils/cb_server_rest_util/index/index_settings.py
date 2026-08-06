"""
https://docs.couchbase.com/server/current/rest-api/rest-index-service.html
"""

from cb_server_rest_util.connection import CBRestConnection


class IndexSettings(CBRestConnection):
    def __init__(self):
        super(IndexSettings).__init__()

    def get_gsi_settings(self):
        """
        GET :: /settings/indexes
        docs.couchbase.com/server/current/rest-api/get-settings-indexes.html
        """

    def set_gsi_settings(self, redistributeIndexes='false',
                         numReplica=0, enablePageBloomFilter='false',
                         indexerThreads=0, memorySnapshotInterval=200,
                         stableSnapshotInterval=5000, maxRollbackPoints=2,
                         logLevel="info", storageMode='plasma', enableShardAffinity=None):
        """
        POST :: /settings/indexes
        """

        param_dict = {
            'redistributeIndexes': redistributeIndexes,
            'numReplica': numReplica,
            'enablePageBloomFilter': enablePageBloomFilter,
            'indexerThreads': indexerThreads,
            'memorySnapshotInterval': memorySnapshotInterval,
            'stableSnapshotInterval': stableSnapshotInterval,
            'maxRollbackPoints': maxRollbackPoints,
            'logLevel': logLevel,
            'storageMode': storageMode
        }
        if enableShardAffinity is not None:
            param_dict['enableShardAffinity'] = enableShardAffinity

        api = self.base_url + '/settings/indexes'
        status, content, _ = self.request(api, 'POST', param_dict)
        return status, content

    def get_node_statistics(self):
        """
        GET :: /api/v1/stats
        """

    def get_keyspace_statistics(self, key_space):
        """
        GET :: /api/v1/stats/{keyspace}
        """

    def get_index_statistics(self, key_space, index_name):
        """
        GET :: /api/v1/stats/{keyspace}/{index}
        """
