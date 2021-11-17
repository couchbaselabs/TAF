from datetime import datetime
from random import choice

from BucketLib.bucket import Bucket
from Cb_constants import DocLoading, CbServer
from SystemEventLogLib.Events import Event
from SystemEventLogLib.SystemEventOperations import SystemEventRestHelper
from SystemEventLogLib.data_service_events import DataServiceEvents
from membase.api.rest_client import RestConnection
from upgrade.upgrade_base import UpgradeBase


class SystemEventLogs(UpgradeBase):
    def setUp(self):
        super(SystemEventLogs, self).setUp()

    def tearDown(self):
        super(SystemEventLogs, self).tearDown()

    def test_upgrade(self):
        update_task = None
        t_durability_level = choice([Bucket.DurabilityLevel.NONE,
                                     Bucket.DurabilityLevel.MAJORITY])

        if self.upgrade_with_data_load:
            self.log.info("Continuous doc updates with durability=%s"
                          % t_durability_level)
            update_task = self.task.async_continuous_doc_ops(
                self.cluster, self.bucket, self.gen_load,
                op_type=DocLoading.Bucket.DocOps.UPDATE, timeout_secs=30,
                process_concurrency=1, durability=t_durability_level)

        self.log.info("Upgrading cluster nodes to target version")
        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                     self.upgrade_version)
            self.cluster_util.print_cluster_stats(self.cluster)

            node_to_upgrade = self.fetch_node_to_upgrade()

            # Create custom event to test system event logs
            event = DataServiceEvents.bucket_online(
                self.cluster.master.ip, self.bucket.name, self.bucket.uuid)
            event[Event.Fields.UUID] = self.system_events.get_rand_uuid()
            event[Event.Fields.TIMESTAMP] = \
                self.system_events.get_timestamp_format(datetime.now())
            nodes_in_cluster = \
                RestConnection(self.cluster.master).get_nodes()

            if node_to_upgrade is None \
                    or self.cluster_supports_system_event_logs:
                # Cluster fully upgraded / already supports system event logs
                self.log.info("Creating events for validation")
                self.system_events.set_test_start_time()
                self.system_events.events = list()
                col_name = "collection_1"
                self.bucket_util.create_collection(
                    self.cluster.master, self.bucket,
                    collection_spec={"name": col_name})
                self.bucket_util.drop_collection(
                    self.cluster.master, self.bucket, collection_name=col_name)

                self.system_events.add_event(
                    DataServiceEvents.collection_created(
                        self.cluster.master.ip, self.bucket.name,
                        self.bucket.uuid, CbServer.default_scope, col_name))
                self.system_events.add_event(
                    DataServiceEvents.collection_dropped(
                        self.cluster.master.ip, self.bucket.name,
                        self.bucket.uuid, CbServer.default_scope, col_name))
                event_helper = SystemEventRestHelper([self.cluster.master])
                status, content = event_helper.create_event(event)
                if status is False:
                    self.log_failure("Failed to create event: %s" % content)

                for node in nodes_in_cluster:
                    self.log.info("Validating events on %s" % node.ip)
                    self.system_events.validate(node)
            else:
                # Mixed mode cluster testing
                self.log.info("Creating custom event to validate "
                              "and trying to fetch events from cluster")
                for node in nodes_in_cluster:
                    event_helper = SystemEventRestHelper([node])
                    status, content = event_helper.create_event(event)
                    if float(node.version[:3]) >= 7.1:
                        if not status:
                            self.log_failure("Event creation returned '%s':%s"
                                             % (status, content))
                        events = event_helper.get_events(events_count=-1)
                        if len(events) != 0:
                            self.log_failure("Events created in mixed mode: %s"
                                             % events)

                        status, content = event_helper.update_max_events(
                            max_event_count=CbServer.sys_event_max_logs)
                        if status:
                            self.log_failure(
                                "Mixed mode - able to update max_events: %s"
                                % content)
                        elif content != "Not supported in mixed mode error":
                            self.log_failure("Mismatch in mix-mode error: %s"
                                             % content)

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure is not None:
                break

        if self.upgrade_with_data_load:
            # Wait for update_task to complete
            update_task.end_task()
            self.task_manager.get_task_result(update_task)

        # Perform final collection/doc_count validation
        self.validate_test_failure()
