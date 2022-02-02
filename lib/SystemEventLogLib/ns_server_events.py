from SystemEventLogLib.Events import Event
from cb_constants.system_event_log import NsServer


class NsServerEvents(object):
    @staticmethod
    def rebalance_started(node, active_nodes, keep_nodes,
                          eject_nodes, delta_nodes, failed_nodes):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.RebalanceStarted,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Rebalance initiated",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {
                "nodes_info": {
                    "active_nodes": active_nodes,
                    "keep_nodes": keep_nodes,
                    "eject_nodes": eject_nodes,
                    "delta_nodes": delta_nodes,
                    "failed_nodes": failed_nodes
                }
            }
        }

    @staticmethod
    def rebalance_success(node, active_nodes, keep_nodes, eject_nodes,
                          delta_nodes, failed_nodes):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.RebalanceComplete,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Rebalance completed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {
                "completion_message": "Rebalance completed successfully.",
                "nodes_info": {
                    "active_nodes": active_nodes,
                    "keep_nodes": keep_nodes,
                    "eject_nodes": eject_nodes,
                    "delta_nodes": delta_nodes,
                    "failed_nodes": failed_nodes
                }
            }
        }

    @staticmethod
    def rebalance_failed(node, active_nodes, keep_nodes, eject_nodes,
                         delta_nodes, failed_nodes):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.RebalanceFailure,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Rebalance failed",
            Event.Fields.SEVERITY: Event.Severity.ERROR,
            Event.Fields.EXTRA_ATTRS: {
                "nodes_info": {
                    "active_nodes": active_nodes,
                    "keep_nodes": keep_nodes,
                    "eject_nodes": eject_nodes,
                    "delta_nodes": delta_nodes,
                    "failed_nodes": failed_nodes
                }
            }
        }

    @staticmethod
    def node_added(node, node_added, node_services):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.NodeAdded,
            Event.Fields.DESCRIPTION: "Node successfully joined the cluster",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {"node_added": node_added,
                                       "services": node_services}
        }

    @staticmethod
    def service_started(node, extra_attrs=None):
        event = {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.ServiceStarted,
            Event.Fields.DESCRIPTION: "Service started",
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.SEVERITY: Event.Severity.INFO,
        }
        if extra_attrs:
            event[Event.Fields.EXTRA_ATTRS] = extra_attrs
        return event

    @staticmethod
    def node_offline(node, new_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.NodeOffline,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {"node": new_node}
        }

    @staticmethod
    def auto_failover_started(node, active_nodes, failover_nodes, master_node,
                              failover_threshold, failover_reason,
                              allow_unsafe=False):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.AutoFailoverStarted,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Auto failover initiated",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {
                "nodes_info": {
                    "active_nodes": active_nodes,
                    "failover_nodes": failover_nodes,
                    "master_node": master_node
                },
                "failover_threshold": failover_threshold,
                "failover_reason": failover_reason,
                "allow_unsafe": allow_unsafe
            }
        }

    @staticmethod
    def auto_failover_complete(node, active_nodes, failover_nodes,
                               master_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.AutoFailoverComplete,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Auto failover completed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {
                "nodes_info": {
                    "active_nodes": active_nodes,
                    "failover_nodes": failover_nodes,
                    "master_node": master_node
                },
                "completion_message": "Failover completed successfully."
            }
        }

    @staticmethod
    def auto_failover_failure(node, active_nodes, failover_nodes,
                              master_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.AutoFailoverFailed,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Auto failover failed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {
                "nodes_info": {
                    "active_nodes": active_nodes,
                    "failover_nodes": failover_nodes,
                    "master_node": master_node
                }
            }
        }

    @staticmethod
    def graceful_failover_started(node, active_nodes, failover_nodes,
                                  master_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.GracefulFailoverStarted,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Graceful failover initiated",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {
                "nodes_info": {
                    "active_nodes": active_nodes,
                    "failover_nodes": failover_nodes,
                    "master_node": master_node
                }
            }
        }

    @staticmethod
    def graceful_failover_complete(node, active_nodes, failover_nodes,
                                   master_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.GracefulFailoverComplete,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Graceful failover completed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {
                "completion_message": "Graceful failover "
                                      "completed successfully.",
                "nodes_info": {
                    "active_nodes": active_nodes,
                    "failover_nodes": failover_nodes,
                    "master_node": master_node
                }
            }
        }

    @staticmethod
    def graceful_failover_failed(node, active_nodes, failover_nodes,
                                 master_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.GracefulFailoverFailed,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Graceful failover failed",
            Event.Fields.SEVERITY: Event.Severity.ERROR,
            Event.Fields.EXTRA_ATTRS: {
                "nodes_info": {
                    "active_nodes": active_nodes,
                    "failover_nodes": failover_nodes,
                    "master_node": master_node
                }
            }
        }

    @staticmethod
    def hard_failover_started(node, active_nodes, failover_nodes,
                              master_node, allow_unsafe=False):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.HardFailoverStarted,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Hard failover initiated",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {
                "nodes_info": {
                    "active_nodes": active_nodes,
                    "failover_nodes": failover_nodes,
                    "master_node": master_node
                },
                "allow_unsafe": allow_unsafe
            }
        }

    @staticmethod
    def hard_failover_complete(node, active_nodes, failover_nodes,
                               master_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.HardFailoverComplete,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Hard failover completed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {
                "nodes_info": {
                    "active_nodes": active_nodes,
                    "failover_nodes": failover_nodes,
                    "master_node": master_node
                },
                "completion_message": "Failover completed successfully."
            }
        }

    @staticmethod
    def hard_failover_failed(node, active_nodes, failover_nodes,
                             master_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.HardFailoverComplete,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Hard failover failed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {
                "nodes_info": {
                    "active_nodes": active_nodes,
                    "failover_nodes": failover_nodes,
                    "master_node": master_node
                }
            }
        }

    @staticmethod
    def orchestrator_change(old_orchestrator, new_orchestrator):
        return {
            Event.Fields.EVENT_ID: NsServer.MasterSelected,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Master selected",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {"old_master": old_orchestrator,
                                       "new_master": new_orchestrator}
        }

    @staticmethod
    def topology_update(node, old_topology, new_topology):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.TopologyChange,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {"old_topology": old_topology,
                                       "new_topology": new_topology}
        }

    @staticmethod
    def service_crashed(node, process_name, process_id, exit_status):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.ServiceCrashed,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Service crashed",
            Event.Fields.SEVERITY: Event.Severity.ERROR,
            Event.Fields.EXTRA_ATTRS: {
                "name": process_name,
                "os_pid": process_id,
                "exit_status": exit_status
            }
        }
