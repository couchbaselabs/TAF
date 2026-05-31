"""
Created on May 25, 2026

@author: Automation
"""

import copy

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class ReplicationBase(GetCluster):

    def setUp(self, nomenclature="Replication_Base"):
        GetCluster.setUp(self, nomenclature)
        self.replication_id = self.input.param(
            "replication_id", "00000000-0000-0000-0000-000000000000")
        self.job_id = self.input.param(
            "job_id", "00000000-0000-0000-0000-000000000000")
        self.endpoint_id = self.input.param(
            "endpoint_id", "vpce-079a0245094731925")
        self.replication_payload = {
            "direction": "one-way",
            "sourceBucket": self.input.param("source_bucket", "ZGVmYXVsdA=="),
            "target": {
                "cluster": self.input.param("target_cluster_id", self.cluster_id),
                "bucket": self.input.param("target_bucket", "ZGVmYXVsdA=="),
                "scopes": []
            },
            "settings": {
                "priority": "medium",
                "networkUsageLimit": 500
            }
        }

    def tearDown(self):
        super(ReplicationBase, self).tearDown()

    def api_call_with_retry(self, method, *args, **kwargs):
        result = method(*args, **kwargs)
        if result.status_code == 429:
            self.handle_rate_limit(int(result.headers["Retry-After"]))
            result = method(*args, **kwargs)
        return result

    def payload_copy(self):
        return copy.deepcopy(self.replication_payload)
