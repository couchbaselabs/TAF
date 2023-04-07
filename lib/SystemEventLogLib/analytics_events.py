from SystemEventLogLib.Events import Event
from cb_constants.system_event_log import Analytics


class AnalyticsEvents(object):

    @staticmethod
    def process_started(node, process_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.ProcessStarted,
            Event.Fields.DESCRIPTION: "Analytics Process Started",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {"process_name": process_name}
        }

    @staticmethod
    def process_crashed(node, process_name):
        """
        Currently this event is only generated for java process, an issue
        has been filed for capturing this event for cbas process as well.
        MB-49920
        """
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.ERROR,
            Event.Fields.EVENT_ID: Analytics.ProcessCrashed,
            Event.Fields.DESCRIPTION: "Analytics Process Crashed",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {"process_name": process_name}
        }

    @staticmethod
    def process_exited(node, process_name):
        """
        This event can be triggered by calling analytics cluster restart
        endpoint.
        Currently generated only for java process
        """
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.ProcessExited,
            Event.Fields.DESCRIPTION: "Analytics Process Exited",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {"process_name": process_name}
        }

    @staticmethod
    def topology_change_started(node, node_in, node_out):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.TopologyChangeStarted,
            Event.Fields.DESCRIPTION: "Analytics Topology Change Started",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "topology": {
                    "num_eject_nodes": node_out,
                    "num_keep_nodes": node_in
                }
            }
        }

    @staticmethod
    def topology_change_failed(node, node_in, node_out):
        """
        This event can be triggered by creating a firewall between the cbas
        controller node and incoming/outgoing CBAS node during rebalance.
        """
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.ERROR,
            Event.Fields.EVENT_ID: Analytics.TopologyChangeFailed,
            Event.Fields.DESCRIPTION: "Analytics Topology Change Failed",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "topology": {
                    "num_eject_nodes": node_out,
                    "num_keep_nodes": node_in,
                }
            }
        }

    @staticmethod
    def topology_change_completed(node, node_in, node_out):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.TopologyChangeCompleted,
            Event.Fields.DESCRIPTION: "Analytics Topology Change Completed",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "topology": {
                    "num_eject_nodes": node_out,
                    "num_keep_nodes": node_in
                }
            }
        }

    @staticmethod
    def collection_created(node, scope_name, collection_name,
                           link_scope_name, link_name, kv_bucket_name,
                           kv_scope_name, kv_collection_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.CollectionCreated,
            Event.Fields.DESCRIPTION: "Analytics Collection Created",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "collection_name": collection_name,
                "link_scope_name": link_scope_name,
                "link_name": link_name,
                "source": {
                    "bucket_name": kv_bucket_name,
                    "scope_name": kv_scope_name,
                    "collection_name": kv_collection_name
                }
            }
        }

    @staticmethod
    def collection_mapped(node, kv_bucket_name, kv_scope_name,
                          kv_collection_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.CollectionMapped,
            Event.Fields.DESCRIPTION: "Analytics Collection Mapped",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "bucket_name": kv_bucket_name,
                "scope_name": kv_scope_name,
                "collection_name": kv_collection_name
            }
        }

    @staticmethod
    def collection_dropped(node, scope_name, collection_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.CollectionDropped,
            Event.Fields.DESCRIPTION: "Analytics Collection Dropped",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "collection_name": collection_name
            }
        }

    @staticmethod
    def collection_detached(node, scope_name, collection_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.WARN,
            Event.Fields.EVENT_ID: Analytics.CollectionDetached,
            Event.Fields.DESCRIPTION: "Analytics Collection Detached",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "collection_name": collection_name
            }
        }

    @staticmethod
    def collection_attached(node, scope_name, collection_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.CollectionAttached,
            Event.Fields.DESCRIPTION: "Analytics Collection Attached",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "collection_name": collection_name
            }
        }

    @staticmethod
    def collection_rollback():
        pass

    @staticmethod
    def scope_created(node, scope_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.ScopeCreated,
            Event.Fields.DESCRIPTION: "Analytics Scope Created",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name
            }
        }

    @staticmethod
    def scope_dropped(node, scope_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.ScopeDropped,
            Event.Fields.DESCRIPTION: "Analytics Scope Dropped",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name
            }
        }

    @staticmethod
    def index_created(node, scope_name, index_name, collection_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.IndexCreated,
            Event.Fields.DESCRIPTION: "Analytics Index Created",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "index_name": index_name,
                "collection_name": collection_name
            }
        }

    @staticmethod
    def index_dropped(node, scope_name, index_name, collection_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.IndexDropped,
            Event.Fields.DESCRIPTION: "Analytics Index Dropped",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "index_name": index_name,
                "collection_name": collection_name
            }
        }

    @staticmethod
    def link_created(node, attributes):
        """
        Attributes -
        For S3 links -
        {
            "scope_name": "",
            "link_name": "",
            "link_type": "s3",
            "region": "",
            "service_endpoint": null / ""
        }
        For Couchbase links -
        {
            "scope_name": "",
            "link_name": "",
            "link_type": "couchbase",
            "encryption": "none/half/full",
            "hostname": "10.112.205.103",
        }
        For Azure Blob links -
        {}
        """
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.LinkCreated,
            Event.Fields.DESCRIPTION: "Analytics Link Created",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: attributes
        }

    @staticmethod
    def link_altered(node, attributes):
        """
        Attributes -
        For S3 links -
        {
            "scope_name": "",
            "link_name": "",
            "link_type": "s3",
            "region": "",
            "service_endpoint": null / ""
        }
        For Couchbase links -
        {
            "scope_name": "",
            "link_name": "",
            "link_type": "couchbase",
            "encryption": "none/half/full",
            "hostname": "10.112.205.103",
        }
        For Azure Blob links -
        {}
        """
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.LinkAltered,
            Event.Fields.DESCRIPTION: "Analytics Link Altered",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: attributes
        }

    @staticmethod
    def link_dropped(node, scope_name, link_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.LinkDropped,
            Event.Fields.DESCRIPTION: "Analytics Link Dropped",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "link_name": link_name
            }
        }

    @staticmethod
    def link_connected(node, scope_name, link_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.LinkConnected,
            Event.Fields.DESCRIPTION: "Analytics Link Connected",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "link_name": link_name
            }
        }

    @staticmethod
    def link_disconnected(node, scope_name, link_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.LinkDisconnected,
            Event.Fields.DESCRIPTION: "Analytics Link Disconnected",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "link_name": link_name
            }
        }

    @staticmethod
    def setting_changed(node, option_name, old_value, new_value):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.SettingChanged,
            Event.Fields.DESCRIPTION: "Analytics Settings Changed",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope": "service",
                "option_name": option_name,
                "old_value": old_value,
                "new_value": new_value
            }
        }

    @staticmethod
    def user_defined_library_created():
        pass

    @staticmethod
    def user_defined_library_replaced():
        pass

    @staticmethod
    def user_defined_library_dropped():
        pass

    @staticmethod
    def user_defined_function_created(node, scope_name, udf_name, arity):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.UserDefinedFunctionCreated,
            Event.Fields.DESCRIPTION: "Analytics User-Defined Function Created",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "udf_name": udf_name,
                "arity": arity
            }
        }

    @staticmethod
    def user_defined_function_replaced(node, scope_name, udf_name, arity):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.UserDefinedFunctionReplaced,
            Event.Fields.DESCRIPTION: "Analytics User-Defined Function Replaced",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "udf_name": udf_name,
                "arity": arity
            }
        }

    @staticmethod
    def user_defined_function_dropped(node, scope_name, udf_name, arity):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.UserDefinedFunctionDropped,
            Event.Fields.DESCRIPTION: "Analytics User-Defined Function Dropped",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "udf_name": udf_name,
                "arity": arity
            }
        }

    @staticmethod
    def synonym_created(node, scope_name, synonym_name, target_scope_name,
                        target_collection_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.SynonymCreated,
            Event.Fields.DESCRIPTION: "Analytics Synonym Created",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "synonym_name": synonym_name,
                "target_scope_name": target_scope_name,
                "target_collection_name": target_collection_name
            }
        }

    @staticmethod
    def synonym_dropped(node, scope_name, synonym_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.SynonymDropped,
            Event.Fields.DESCRIPTION: "Analytics Synonym Dropped",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "synonym_name": synonym_name,
            }
        }

    @staticmethod
    def view_created(node, scope_name, view_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.ViewCreated,
            Event.Fields.DESCRIPTION: "Analytics View Created",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {"scope_name": scope_name,
                                       "view_name": view_name}
        }

    @staticmethod
    def view_dropped(node, scope_name, view_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.ViewDropped,
            Event.Fields.DESCRIPTION: "Analytics View Dropped",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {"scope_name": scope_name,
                                       "view_name": view_name}
        }

    @staticmethod
    def view_replaced(node, scope_name, view_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.ViewReplaced,
            Event.Fields.DESCRIPTION: "Analytics View Replaced",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {"scope_name": scope_name,
                                       "view_name": view_name}
        }

    @staticmethod
    def bucket_connected(node, scope_name, link_name, bucket_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.BucketConnected,
            Event.Fields.DESCRIPTION: "Analytics Bucket Connected",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name, "link_name": link_name,
                "bucket_name": bucket_name}
        }

    @staticmethod
    def bucket_connect_failed(node, scope_name, link_name, bucket_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.ERROR,
            Event.Fields.EVENT_ID: Analytics.BucketConnectFailed,
            Event.Fields.DESCRIPTION: "Analytics Bucket Connect Failed",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name, "link_name": link_name,
                "bucket_name": bucket_name}
        }

    @staticmethod
    def bucket_disconnected(node, scope_name, link_name, bucket_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.BucketDisconnected,
            Event.Fields.DESCRIPTION: "Analytics Bucket Disconnected",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name, "link_name": link_name,
                "bucket_name": bucket_name}
        }

    @staticmethod
    def partition_topology_updated(node, num_replicas):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.PartitionTopologyUpdated,
            Event.Fields.DESCRIPTION: "Analytics Partitions Topology Updated",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {"num_replicas": num_replicas}
        }

    @staticmethod
    def collection_analyzed(node, scope_name, collection_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.CollectionAnalyzed,
            Event.Fields.DESCRIPTION: "Analytics Collection Analyzed",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "collection_name": collection_name
            }
        }

    @staticmethod
    def collection_stats_dropped(node, scope_name, collection_name):
        return {
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Analytics.CollectionStatsDropped,
            Event.Fields.DESCRIPTION: "Analytics Collection Statistics Dropped",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {
                "scope_name": scope_name,
                "collection_name": collection_name
            }
        }
