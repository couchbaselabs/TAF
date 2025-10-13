from cb_server_rest_util.cluster_nodes.auto_failover import AutoFailoverAPI
from cb_server_rest_util.cluster_nodes.auto_reprovision import \
    AutoReprovisionAPI
from cb_server_rest_util.cluster_nodes.cb_logging import Logging
from cb_server_rest_util.cluster_nodes.cluster_init_provision \
    import ClusterInitializationProvision
from cb_server_rest_util.cluster_nodes.manual_failover import ManualFailoverAPI
from cb_server_rest_util.cluster_nodes.node_add_remove import \
    NodeAdditionRemoval
from cb_server_rest_util.cluster_nodes.rebalance import RebalanceRestAPI
from cb_server_rest_util.cluster_nodes.settings_and_connections import \
    SettingsAndConnectionsAPI
from cb_server_rest_util.cluster_nodes.statistics import StatisticsAPI
from cb_server_rest_util.cluster_nodes.status_and_events import \
    StatusAndEventsAPI
from cb_server_rest_util.cluster_nodes.diag_eval import DiagEvalAPI


class ClusterRestAPI(AutoFailoverAPI,
                     AutoReprovisionAPI,
                     ClusterInitializationProvision,
                     DiagEvalAPI,
                     Logging,
                     ManualFailoverAPI,
                     NodeAdditionRemoval,
                     RebalanceRestAPI,
                     SettingsAndConnectionsAPI,
                     StatisticsAPI,
                     StatusAndEventsAPI):
    def __init__(self, server):
        """
        Main gateway for all Cluster Rest Operations
        """
        super(ClusterRestAPI, self).__init__()

        self.set_server_values(server)
        self.set_endpoint_urls(server)
        self.check_if_couchbase_is_active(self, max_retry=5)
