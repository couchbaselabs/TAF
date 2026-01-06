from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.xdcr.xdcr_api import XdcrRestAPI


class XDCRUtils(object):
    def __init__(self, server):
        self.server = server
        self.cluster_rest = ClusterRestAPI(server)
        self.xdcr_rest = XdcrRestAPI(server)

    def get_replications(self):
        replications = list()
        status, content = self.cluster_rest.cluster_tasks()
        for item in content:
            if not isinstance(item, dict):
                msg = f"Error while retrieving pools/default/tasks: {content}"
                raise Exception(msg)
            if item["type"] == "xdcr":
                replications.append(item)
        return replications

    def stop_replication(self, replication_uri):
        self.xdcr_rest.delete_replication(replication_uri)

    def remove_all_replications(self):
        for replication in self.get_replications():
            uri = replication["cancelURI"].split("/")[-1]
            self.stop_replication(uri)

    def remove_all_remote_clusters(self):
        _, remote_clusters = self.xdcr_rest.get_remote_references()
        for remote_cluster in remote_clusters or []:
            # Handle case where remote_cluster is a string (cluster name) instead of dict
            if isinstance(remote_cluster, dict):
                cluster_name = remote_cluster["name"]
                try:
                    if remote_cluster["deleted"] is False:
                        self.xdcr_rest.delete_remote_reference(cluster_name)
                except KeyError:
                    # goxdcr cluster references will not contain "deleted" field
                    self.xdcr_rest.delete_remote_reference(cluster_name)
            else:
                # If it's a string, use it directly as cluster name
                self.xdcr_rest.delete_remote_reference(remote_cluster)

    def remove_all_recoveries(self):
        recoveries = list()
        status, content = self.cluster_rest.cluster_tasks()
        for item in content:
            if item["type"] == "recovery":
                recoveries.append(item)
        for recovery in recoveries:
            uri = recovery["stopURI"]
            status, content = self.xdcr_rest.stop_recovery(uri)
            if status is False:
                raise Exception(f"Failed to stop recovery using uri: {uri}")
