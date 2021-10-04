import uuid
from datetime import datetime
from random import choice

from SystemEventLogLib.Events import Event, EventHelper
from SystemEventLogLib.SystemEventOperations import SystemEventRestHelper
from basetestcase import ClusterSetup


class SystemEventLogs(ClusterSetup):
    def setUp(self):
        super(SystemEventLogs, self).setUp()

    def tearDown(self):
        super(SystemEventLogs, self).tearDown()

    def test_event_id_range(self):
        start_time = EventHelper.get_timestamp_format(datetime.now())
        component = self.input.param("component", Event.Component.NS_SERVER)
        event_id_range = \
            self.input.param("event_id_range", "0:1023").split(":")
        is_range_valid = self.input.param("is_range_valid", True)

        event_severity = [Event.Severity.INFO, Event.Severity.ERROR,
                          Event.Severity.WARN, Event.Severity.FATAL]
        event_rest_helper = \
            SystemEventRestHelper(self.cluster.nodes_in_cluster)
        for event_id in range(int(event_id_range[0]),
                              int(event_id_range[1])+1):
            target_node = choice(self.cluster.nodes_in_cluster)
            timestamp = EventHelper.get_timestamp_format(datetime.now())
            uuid_val = str(uuid.uuid4())
            severity = choice(event_severity)
            description = "test"
            event_dict = {
                Event.Fields.TIMESTAMP: timestamp,
                Event.Fields.COMPONENT: component,
                Event.Fields.SEVERITY: severity,
                Event.Fields.EVENT_ID: event_id,
                Event.Fields.UUID: uuid_val,
                Event.Fields.DESCRIPTION: description
            }

            # Send server request to create an event
            status, content = event_rest_helper.create_event(event_dict)
            if not status and is_range_valid:
                self.fail("Event creation failed: %s" % content)

            if is_range_valid:
                # Add events for later validation
                self.events.add_event(
                    uuid=uuid, event_id=event_id, component=component,
                    severity=severity, timestamp=timestamp,
                    description=description, node_name=target_node.ip)

        failures = self.events.validate(since_time=start_time)
        if failures:
            self.fail(failures)

    def test_event_fields(self):
        pass

    def test_max_events(self):
        pass
