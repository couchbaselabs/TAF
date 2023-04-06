import ast
import base64

from capella_utils.dedicated import CapellaUtils as CapellaAPI
from membase.api.rest_client import RestConnection

class DoctorXDCR:

    def __init__(self, source_cluster, destination_cluster, source_bucket, destination_bucket,
                 pod, tenant):
        self.source_cluster = source_cluster
        self.destination_cluster = destination_cluster
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.pod = pod
        self.tenant = tenant

    def create_payload(self, all_scopes=True, **kwargs):
        """
        sample params:
        create_payload(direction="one-way", source_bucket="source_bucket_name", target_cluster="target_cluster_id",
        target_bucket= "target_bucket_name", scope_map={"scope-1;['coll-1']": "target-scope-1;['target-coll-1']"})
        """
        direction = kwargs.get('direction', "one-way")
        source_bucket = kwargs.get('source_bucket')
        target_cluster = kwargs.get('target_cluster')
        target_bucket = kwargs.get('target_bucket')
        scope_map = kwargs.get('scope_map')
        payload = {"direction": direction, "sourceBucket": base64.b64encode(source_bucket), "target": {
            "cluster": target_cluster,
            "bucket": base64.b64encode(target_bucket),
        }}
        if not all_scopes:
            payload['scopes'] = []
            for item, value in scope_map.iteritems():
                source_scope, source_coll_list = item.split(";")[0], ast.literal_eval(item.split(";")[1])
                destination_scope, destination_coll_list = value.split(";")[0], ast.literal_eval(value.split(";")[1])
                for i in range(len(source_coll_list)):
                    payload['scopes'].append({
                        "source": source_scope,
                        "target": destination_scope,
                        "collections": [
                            {
                                "source": source_coll_list[i],
                                "target": destination_coll_list[i]
                            }
                        ]
                    })

        if kwargs.has_key("settings"):
            payload['settings'] = kwargs.get("settings")
        else:
            payload['settings'] = {"priority": "medium"}
        return payload

    def set_up_replication(self):
        payload = self.create_payload(direction="one-way", source_bucket=self.source_bucket,
                                      target_cluster=self.destination_cluster.id,
                                      target_bucket=self.destination_bucket,
                                      all_scopes=True)
        CapellaAPI.create_xdcr_replication(pod=self.pod,
                                           tenant=self.tenant,
                                           cluster_id=self.source_cluster.id,
                                           payload=payload)

    def is_replication_complete(self, cluster, item_count, bucket_name):
        rest = RestConnection(cluster.master)
        bucket_info = rest.get_bucket_details(bucket_name=bucket_name)
        if bucket_info['basicStats']['itemCount'] == item_count:
            return True
        return False

