import uuid
from copy import deepcopy
from datetime import datetime
from random import choice

from Cb_constants import CbServer
from SystemEventLogLib.Events import Event, EventHelper
from SystemEventLogLib.SystemEventOperations import SystemEventRestHelper
from basetestcase import ClusterSetup
from cb_constants.system_event_log import NsServer, KvEngine


class SystemEventLogs(ClusterSetup):
    def setUp(self):
        super(SystemEventLogs, self).setUp()

    def tearDown(self):
        super(SystemEventLogs, self).tearDown()

    def __validate(self, start_time=None):
        for server in self.cluster.nodes_in_cluster:
            self.log.info("Validating events from node %s" % server.ip)
            failures = self.system_events.validate(server=server,
                                                   since_time=start_time)
            if failures:
                self.fail(failures)

    def test_event_id_range(self):
        """
        Create custom events for 'component' using the event-ids
        provided by the 'event_id_range'.
        'is_range_valid' determines the provided values are right or not.
        is_range_valid - Basically to test negative scenarios
        """
        start_time = EventHelper.get_timestamp_format(datetime.now())
        component = self.input.param("component", Event.Component.NS_SERVER)
        event_id_range = \
            self.input.param("event_id_range", "0:1023").split(":")
        is_range_valid = self.input.param("is_range_valid", True)

        event_severity = Event.Severity.values()
        event_rest_helper = \
            SystemEventRestHelper(self.cluster.nodes_in_cluster)
        for event_id in range(int(event_id_range[0]),
                              int(event_id_range[1])+1):
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
                Event.Fields.DESCRIPTION: description,
                Event.Fields.NODE_NAME: self.cluster.master.ip
            }

            # Send server request to create an event
            status, content = event_rest_helper.create_event(event_dict)
            if not status and is_range_valid:
                self.fail("Event creation failed: %s" % content)

            if is_range_valid:
                # Add events for later validation
                self.system_events.add_event(event_dict)
        self.__validate(start_time)

    def test_event_fields_missing(self):
        """
        Create custom events with a missing mandatory event field.
        Validates the response is always False with a valid error message.
        """
        def get_invalid_event():
            temp_event = deepcopy(event_dict)
            temp_event.pop(mandatory_field)
            return temp_event

        expected_err_msg = "The value must be supplied"
        start_time = EventHelper.get_timestamp_format(datetime.now())
        event_rest_helper = \
            SystemEventRestHelper(self.cluster.nodes_in_cluster)
        uuid_val = str(uuid.uuid4())
        severity = Event.Severity.INFO
        description = "test"
        timestamp = EventHelper.get_timestamp_format(datetime.now())
        event_dict = {
            Event.Fields.TIMESTAMP: timestamp,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.SEVERITY: severity,
            Event.Fields.EVENT_ID: NsServer.NodeAdded,
            Event.Fields.UUID: uuid_val,
            Event.Fields.DESCRIPTION: description
        }
        for mandatory_field in Event.Fields.values(only_mandatory_fields=True):
            invalid_event = get_invalid_event()
            status, content = event_rest_helper.create_event(invalid_event)
            if status:
                self.fail("Event with the missing '%s' field accepted: %s"
                          % (mandatory_field, event_dict))
            content = content["errors"]

            if content[mandatory_field] != expected_err_msg:
                self.fail("Invalid error message for '%s': %s"
                          % (mandatory_field, content))
        self.__validate(start_time)

    def test_max_events(self):
        """
        1. Load 10K events under random components and corresponding event_ids
        2. Validate last 10K events are there in the cluster
        3. Load more event to make sure the first event is rolled over correctly
        """
        def generate_random_event():
            timestamp = EventHelper.get_timestamp_format(datetime.now())
            uuid_val = str(uuid.uuid4())
            severity = choice(event_severity)
            description = "Add event %s" % event_count
            component = choice(components)
            return {
                Event.Fields.TIMESTAMP: timestamp,
                Event.Fields.COMPONENT: choice(components),
                Event.Fields.SEVERITY: severity,
                Event.Fields.EVENT_ID: choice(range(id_range[component][0],
                                                    id_range[component][1])),
                Event.Fields.UUID: uuid_val,
                Event.Fields.DESCRIPTION: description,
                Event.Fields.NODE_NAME: self.cluster.master.ip
            }

        event_rest_helper = \
            SystemEventRestHelper(self.cluster.nodes_in_cluster)
        components = Event.Component.values()
        event_severity = Event.Severity.values()
        id_range = {
            Event.Component.NS_SERVER: (0, 1024),
            Event.Component.DATA: (8192, 9216),
            Event.Component.SECURITY: (9216, 10240),
            Event.Component.VIEWS: (10240, 11264),
            Event.Component.QUERY: (1024, 2048),
            Event.Component.INDEXING: (2048, 3072),
            Event.Component.SEARCH: (3072, 4096),
            Event.Component.EVENTING: (4096, 120),
            Event.Component.ANALYTICS: (512, 6144),
            Event.Component.XDCR: (7168, 8192),
            Event.Component.BACKUP: (6143, 7168)
        }
        for event_count in range(0, CbServer.max_sys_event_logs):
            event_dict = generate_random_event()
            # Send server request to create an event
            status, _ = event_rest_helper.create_event(event_dict)
            if not status:
                self.fail("Failed to add event: %s" % event_dict)
            # Add events for later validation
            self.system_events.add_event(event_dict)
            self.__validate()

        # Add one more event to check roll-over
        event_dict = generate_random_event()
        # Send server request to create an event
        status, _ = event_rest_helper.create_event(event_dict)
        if not status:
            self.fail("Failed to add event: %s" % event_dict)
        # Add events for later validation
        self.system_events.add_event(event_dict)
        self.__validate()

    def test_event_description_field(self):
        """
        Test event_description with,
        1. NULL value
        2. Max length (80 chars)
        Validates the response string is correct
        """

        expected_err_format = "Name length (%d) must be in the range " \
                              "from 1 to 80, inclusive"
        start_time = EventHelper.get_timestamp_format(datetime.now())
        event_rest_helper = \
            SystemEventRestHelper(self.cluster.nodes_in_cluster)
        target_node = choice(self.cluster.nodes_in_cluster)

        # Event dictionary format
        event_dict = {
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EVENT_ID: KvEngine.BucketCreated,
            Event.Fields.TIMESTAMP:
                EventHelper.get_timestamp_format(datetime.now()),
            Event.Fields.UUID: str(uuid.uuid4()),
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.DESCRIPTION: "",
            Event.Fields.NODE_NAME: target_node.ip
        }

        # Event description test with variable str lengths
        for desc_len in [0, 81, 100]:
            self.log.info("Testing with description string len=%d" % desc_len)
            event_dict[Event.Fields.DESCRIPTION] = "a" * desc_len
            status, content = event_rest_helper.create_event(event_dict)
            if status:
                self.fail("Event created with NULL description value")
            if content["errors"][Event.Fields.DESCRIPTION] \
                    != expected_err_format % desc_len:
                self.fail("Description error message mismatch: %s" % content)

        # Success case
        desc_len = choice(range(1, 81))
        self.log.info("Testing with description string len=%d" % desc_len)
        event_dict[Event.Fields.DESCRIPTION] = "a" * desc_len
        status, content = event_rest_helper.create_event(event_dict,
                                                         server=target_node)
        if not status:
            self.fail("Event creation failed with valid description len")
        self.system_events.add_event(event_dict)

        self.__validate(start_time)

    def test_event_size(self):
        """
        1. Create events with minimum fields and minimum size as possible
        2. Create event which exceeds max allowed size (3072bytes)
           and validate the event creation fails as expected
        """

    def test_duplicate_events(self):
        """

        """
