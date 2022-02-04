import os.path
import zipfile
from copy import deepcopy
from datetime import datetime, timedelta
from random import choice, randint
from threading import Thread
from time import time

from BucketLib.BucketOperations_Rest import BucketHelper
from BucketLib.bucket import Bucket
from Cb_constants import CbServer
from SecurityLib.rbac import RbacUtil
from SystemEventLogLib.Events import Event, EventHelper
from SystemEventLogLib.SystemEventOperations import SystemEventRestHelper
from SystemEventLogLib.data_service_events import DataServiceEvents
from SystemEventLogLib.ns_server_events import NsServerEvents
from basetestcase import ClusterSetup
from cb_constants.system_event_log import NsServer, KvEngine
from cb_tools.cb_collectinfo import CbCollectInfo
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import BucketDurability
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection, OS
from table_view import TableView


class SystemEventLogs(ClusterSetup):
    def setUp(self):
        super(SystemEventLogs, self).setUp()

        self.log_setup_status("SystemEventLogs", "started")

        # Rebalance in and out a node to update node value from cb.local
        # to actual IPs/hostnames
        # Ref: MB-50641
        if self.case_number == self.nodes_init == 1:
            node_in_out = [self.cluster.servers[self.nodes_init]]
            result = self.task.rebalance(self.cluster.nodes_in_cluster,
                                         to_add=node_in_out, to_remove=[])
            self.assertTrue(result, msg="Rebalance in failed")
            result = self.task.rebalance(self.cluster.nodes_in_cluster,
                                         to_add=[], to_remove=node_in_out)
            self.assertTrue(result, msg="Rebalance out failed")

        self.create_bucket(self.cluster)
        self.max_event_count = CbServer.sys_event_def_logs
        self.event_rest_helper = \
            SystemEventRestHelper(self.cluster.nodes_in_cluster)

        self.id_range = {
            Event.Component.NS_SERVER: (0, 1024),
            Event.Component.DATA: (8192, 9216),
            Event.Component.SECURITY: (9216, 10240),
            Event.Component.VIEWS: (10240, 11264),
            Event.Component.QUERY: (1024, 2048),
            Event.Component.INDEXING: (2048, 3072),
            Event.Component.SEARCH: (3072, 4096),
            Event.Component.EVENTING: (4096, 5120),
            Event.Component.ANALYTICS: (5120, 6144),
            Event.Component.XDCR: (7168, 8192),
            Event.Component.BACKUP: (6144, 7168)
        }
        self.log_setup_status("SystemEventLogs", "completed")

    def tearDown(self):
        self.log_setup_status("SystemEventLogs", "started", stage="tearDown")
        # Update max event counter to default value
        self.event_rest_helper.update_max_events(server=self.cluster.master)
        # Reset max events in EventHelper class
        EventHelper.max_events = CbServer.sys_event_def_logs
        self.log_setup_status("SystemEventLogs", "completed", stage="tearDown")

        super(SystemEventLogs, self).tearDown()

    def __reset_events(self):
        self.system_events.events = list()
        self.system_events._set_counter(0)
        self.system_events.set_test_start_time()

    def __validate(self, start_time=None):
        for server in self.cluster.nodes_in_cluster:
            self.log.info("Validating events from node %s" % server.ip)
            failures = self.system_events.validate(server=server,
                                                   since_time=start_time,
                                                   events_count=-1)
            if failures:
                self.fail(failures)

    def __generate_random_event(self, components, severities, description):
        component = choice(components)
        return {
            Event.Fields.TIMESTAMP:
                EventHelper.get_timestamp_format(datetime.utcnow()),
            Event.Fields.COMPONENT: component,
            Event.Fields.SEVERITY: choice(severities),
            Event.Fields.EVENT_ID:
                choice(range(self.id_range[component][0],
                             self.id_range[component][1])),
            Event.Fields.UUID: self.system_events.get_rand_uuid(),
            Event.Fields.DESCRIPTION: description,
            Event.Fields.NODE_NAME: self.cluster.master.ip
        }

    def get_process_id(self, shell, process_name):
        self.log.debug("Fetching process_id for %s" % process_name)
        process_id, _ = shell.execute_command(
            "ps -ef | grep \"%s \" | grep -v grep | awk '{print $2}'"
            % process_name)
        return process_id[0].strip()

    def get_last_event_from_cluster(self):
        return self.event_rest_helper.get_events(
            server=self.cluster.master, events_count=-1)["events"][-1]

    def test_event_id_range(self):
        """
        Create custom events for 'component' using the event-ids
        provided by the 'event_id_range'.
        'is_range_valid' - Determines the provided values are right or not.
                           (Used for -ve test cases)
        """
        component = self.input.param("component", Event.Component.NS_SERVER)
        is_range_valid = self.input.param("is_range_valid", True)

        self.__reset_events()
        timestamp = datetime.utcnow()

        if is_range_valid:
            # event_id_range is a range. Eg: 0:1023
            event_id_range = range(self.id_range[component][0],
                                   self.id_range[component][1])
        else:
            # Construct a list of event_ids from the other components
            # event_id_range to validate the negative scenarios
            event_id_range = list()
            for component_key, v_range in self.id_range.items():
                if component_key == component:
                    continue
                for _ in range(5):
                    event_id_range.append(randint(v_range[0], v_range[1]-1))

        event_severity = Event.Severity.values()
        self.log.info("Creating %s events in cluster" % component)
        for event_id in event_id_range:
            timestamp += timedelta(seconds=1)
            uuid_val = self.system_events.get_rand_uuid()
            severity = choice(event_severity)
            description = "test event_id_range: %s"
            event_dict = {
                Event.Fields.TIMESTAMP:
                    EventHelper.get_timestamp_format(timestamp),
                Event.Fields.COMPONENT: component,
                Event.Fields.SEVERITY: severity,
                Event.Fields.EVENT_ID: event_id,
                Event.Fields.UUID: uuid_val,
                Event.Fields.DESCRIPTION: description % event_id,
                Event.Fields.NODE_NAME: self.cluster.master.ip
            }

            # Send server request to create an event
            status, content = self.event_rest_helper.create_event(event_dict)
            if not status and is_range_valid:
                self.fail("Event creation failed: %s" % content)

            if is_range_valid:
                # Add events for later validation
                self.system_events.add_event(event_dict)

        self.log.info("Validating events")
        self.__validate(self.system_events.test_start_time)

    def test_event_fields_missing(self):
        """
        Create custom events with a missing mandatory event field.
        Validates the response is always False with a valid error message.
        """
        def get_invalid_event():
            temp_event = deepcopy(event_dict)
            temp_event.pop(mandatory_field)
            return temp_event

        self.__reset_events()
        expected_err_msg = "The value must be supplied"
        start_time = EventHelper.get_timestamp_format(datetime.utcnow())
        uuid_val = self.system_events.get_rand_uuid()
        severity = Event.Severity.INFO
        description = "test"
        timestamp = EventHelper.get_timestamp_format(datetime.utcnow())
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
            status, content = self.event_rest_helper.create_event(invalid_event)
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
        self.__reset_events()

        components = Event.Component.values()
        event_severity = Event.Severity.values()
        self.log.info("Creating %s events in cluster" % self.max_event_count)
        for event_count in range(0, self.max_event_count):
            event_dict = self.__generate_random_event(
                components, event_severity, "Adding event %s" % event_count)
            # Send server request to create an event
            status, _ = self.event_rest_helper.create_event(event_dict)
            if not status:
                self.fail("Failed to add event: %s" % event_dict)
            # Add events for later validation
            self.system_events.add_event(event_dict)

            # Extra logging to make sure the events are being added
            if event_count % 500 == 0:
                self.log.info("Added %s events" % event_count)

        self.__validate()

        # Add one more event to check roll-over
        self.log.info("Create event to trigger rollover")
        event_dict = self.__generate_random_event(components, event_severity,
                                                  "Roll over event")
        # Send server request to create an event
        status, _ = self.event_rest_helper.create_event(event_dict)
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
        target_node = choice(self.cluster.nodes_in_cluster)
        c_time = datetime.utcnow()

        # Event dictionary format
        event_dict = {
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EVENT_ID: KvEngine.BucketCreated,
            Event.Fields.TIMESTAMP: EventHelper.get_timestamp_format(c_time),
            Event.Fields.UUID: self.system_events.get_rand_uuid(),
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.DESCRIPTION: "",
            Event.Fields.NODE_NAME: target_node.ip
        }

        # Event description test with variable str lengths
        for desc_len in [0, 81, 100]:
            self.log.info("Testing with description string len=%d" % desc_len)
            event_dict[Event.Fields.DESCRIPTION] = "a" * desc_len
            status, content = self.event_rest_helper.create_event(event_dict)
            if status:
                self.fail("Event created with NULL description value")
            if content["errors"][Event.Fields.DESCRIPTION] \
                    != expected_err_format % desc_len:
                self.fail("Description error message mismatch: %s" % content)

        # Success case
        desc_len = choice(range(1, 81))
        self.log.info("Testing with description string len=%d" % desc_len)
        event_dict[Event.Fields.DESCRIPTION] = "a" * desc_len
        status, content = \
            self.event_rest_helper.create_event(event_dict, server=target_node)
        if not status:
            self.fail("Event creation failed with valid description len")
        self.system_events.add_event(event_dict)

        self.__validate(self.system_events.test_start_time)

    def test_event_size(self):
        """
        1. Create events with minimum fields and minimum size as possible
        2. Create event which exceeds max allowed size (3072bytes)
           and validate the event creation fails as expected
        """
        self.log.info("Adding event with 1byte description")
        event_dict = {
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EVENT_ID: KvEngine.BucketCreated,
            Event.Fields.TIMESTAMP: EventHelper.get_timestamp_format(
                datetime.utcnow()),
            Event.Fields.UUID: self.system_events.get_rand_uuid(),
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.DESCRIPTION: "a"
        }
        target_node = choice(self.cluster.nodes_in_cluster)
        status, _ = self.event_rest_helper.create_event(event_dict,
                                                        server=target_node)
        if status is False:
            self.fail("Event creation failed: %s" % event_dict)
        event_dict[Event.Fields.NODE_NAME] = target_node.ip
        self.system_events.add_event(event_dict)

        # Event with empty extra_attr
        self.log.info("Adding event with empty extra_attr field")
        event_dict.pop(Event.Fields.NODE_NAME)
        event_dict[Event.Fields.EXTRA_ATTRS] = ""
        event_dict[Event.Fields.UUID] = self.system_events.get_rand_uuid()
        event_dict[Event.Fields.TIMESTAMP] = \
            EventHelper.get_timestamp_format(datetime.utcnow())
        target_node = choice(self.cluster.nodes_in_cluster)
        status, _ = self.event_rest_helper.create_event(event_dict,
                                                        server=target_node)
        if status is False:
            self.fail("Event creation failed: %s" % event_dict)
        event_dict[Event.Fields.NODE_NAME] = target_node.ip
        self.system_events.add_event(event_dict)

        # Event with max size extra_attr
        self.log.info("Adding event with max sized extra attribute field")
        event_dict.pop(Event.Fields.NODE_NAME)
        event_dict[Event.Fields.EXTRA_ATTRS] = \
            "a" * (CbServer.sys_event_log_max_size - 179)

        event_dict[Event.Fields.UUID] = self.system_events.get_rand_uuid()
        event_dict[Event.Fields.TIMESTAMP] = \
            EventHelper.get_timestamp_format(datetime.utcnow())
        target_node = choice(self.cluster.nodes_in_cluster)
        status, _ = self.event_rest_helper.create_event(event_dict,
                                                        server=target_node)
        if status is False:
            self.fail("Event creation failed: %s" % event_dict)

        event_dict[Event.Fields.NODE_NAME] = target_node.ip
        self.system_events.add_event(event_dict)

        self.log.info("Adding event size greater than allowed size")
        event_dict[Event.Fields.EXTRA_ATTRS] = \
            event_dict[Event.Fields.EXTRA_ATTRS] + "a"
        status, _ = self.event_rest_helper.create_event(event_dict,
                                                        server=target_node)
        event_dict.pop(Event.Fields.NODE_NAME)
        if status:
            self.fail("Event greater than allowed size got created: %s"
                      % event_dict)

        self.__validate(self.system_events.test_start_time)

    def test_duplicate_events(self):
        """
        1. Raise duplicate events back to back to check the
           redundant events are handled
        2. Raise duplicate events back to back but separated
           by time interval of 60sec to make sure the duplicate events are
           recorded by ns_server
        """
        curr_time = datetime.utcnow()
        target_node = choice(self.cluster.nodes_in_cluster)
        uuid_to_test = self.system_events.get_rand_uuid()

        self.log.info("Add duplicate events within 1min time frame")
        # Create duplicate events back to back within same component
        # Event-1 definition
        event_1 = NsServerEvents.service_started(target_node.ip)
        event_1[Event.Fields.UUID] = uuid_to_test
        event_1[Event.Fields.TIMESTAMP] = \
            EventHelper.get_timestamp_format(curr_time)

        # Add valid event to the list for validation
        self.system_events.add_event(event_1)

        # Event-2 definition
        event_2 = NsServerEvents.node_added(target_node.ip,
                                            "new_nodes.ip",
                                            CbServer.Services.KV)
        event_2.pop(Event.Fields.EXTRA_ATTRS)
        event_2.pop(Event.Fields.NODE_NAME)
        event_2[Event.Fields.UUID] = uuid_to_test
        event_2[Event.Fields.TIMESTAMP] = \
            EventHelper.get_timestamp_format(datetime.utcnow())

        self.log.info("Adding events with similar UUID")
        event_1 = deepcopy(event_1)
        event_1.pop(Event.Fields.NODE_NAME)
        self.event_rest_helper.create_event(event_1, server=target_node)
        self.event_rest_helper.create_event(event_2)

        last_event = self.get_last_event_from_cluster()
        if last_event[Event.Fields.UUID] != uuid_to_test \
                or (last_event[Event.Fields.DESCRIPTION]
                    != event_1[Event.Fields.DESCRIPTION]):
            self.fail("Event-id mismatch. Cluster event: %s" % last_event)

        # Create required event dictionaries for testing
        event_1 = DataServiceEvents.scope_created(target_node.ip, "bucket_1",
                                                  "b_uuid", "scope_1")
        event_1[Event.Fields.UUID] = uuid_to_test
        event_1.pop(Event.Fields.NODE_NAME)

        # Add valid event to the list for validation
        self.log.info("Adding event with same uuid")
        end_time = curr_time + timedelta(
            seconds=CbServer.sys_event_log_uuid_uniqueness_time - 1)
        event_1[Event.Fields.TIMESTAMP] = \
            EventHelper.get_timestamp_format(datetime.utcnow())

        self.log.info("Add duplicate events across allowed time frame")
        while curr_time <= end_time:
            curr_time = curr_time + timedelta(seconds=1)
            event_1[Event.Fields.TIMESTAMP] = \
                EventHelper.get_timestamp_format(curr_time)
            self.event_rest_helper.create_event(event_1)

        curr_time = curr_time + timedelta(milliseconds=1)

        # Validate the event
        last_event = self.get_last_event_from_cluster()
        if last_event[Event.Fields.DESCRIPTION] != "Service started":
            self.fail("Last cluster event mismatch. Cluster event: %s"
                      % last_event)

        self.log.info("Adding same event_id after the allowed time frame")
        event_2[Event.Fields.TIMESTAMP] = \
            EventHelper.get_timestamp_format(curr_time)
        status, _ = self.event_rest_helper.create_event(event_2,
                                                        server=target_node)
        if not status:
            self.fail("Event creation failed: %s" % event_2)

        event_2[Event.Fields.NODE_NAME] = target_node.ip
        self.system_events.add_event(event_2)

        last_event = self.get_last_event_from_cluster()
        if last_event[Event.Fields.UUID] != uuid_to_test \
                or (last_event[Event.Fields.DESCRIPTION]
                    != event_2[Event.Fields.DESCRIPTION]):
            self.fail("UUID / desc mismatch. Cluster event: %s, "
                      "Expected: %s" % (last_event, event_2))

        # Validate the events
        self.__validate(self.system_events.test_start_time)

    def test_invalid_values(self):
        """
        Create events with invalid field values.
        Expect the event creation to fail
        """
        c_time = datetime.utcnow()
        valid_event = {
            Event.Fields.EVENT_ID: 0,
            Event.Fields.UUID: self.system_events.get_rand_uuid(),
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.TIMESTAMP: EventHelper.get_timestamp_format(c_time),
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.DESCRIPTION: "test_invalid_values"
        }

        self.log.info("Testing event with type(event_id) = string")
        expected_error = "The value must be an integer"
        invalid_event = deepcopy(valid_event)
        invalid_event[Event.Fields.EVENT_ID] = "string"
        status, content = self.event_rest_helper.create_event(invalid_event)
        if status:
            self.fail("Event creation succeeded with type(event_id)=str")
        if content["errors"][Event.Fields.EVENT_ID] != expected_error:
            self.fail("Invalid error message: %s" % content)

        self.log.info("Testing event with type(uuid) = invalid string")
        expected_error = "The value must be a valid v4 UUID"
        invalid_event = deepcopy(valid_event)
        invalid_event[Event.Fields.UUID] = "invalid_uuid"
        status, content = self.event_rest_helper.create_event(invalid_event)
        if status:
            self.fail("Event creation succeeded with uuid=invalid")
        if content["errors"][Event.Fields.UUID] != expected_error:
            self.fail("Invalid error message: %s" % content)

        self.log.info("Testing event with type(uuid) = int")
        expected_error = "Value must be json string"
        invalid_event = deepcopy(valid_event)
        invalid_event[Event.Fields.UUID] = 1
        status, content = self.event_rest_helper.create_event(invalid_event)
        if status:
            self.fail("Event creation succeeded with type(uuid)=int")
        if content["errors"][Event.Fields.UUID] != expected_error:
            self.fail("Invalid error message: %s" % content)

        self.log.info("Testing event with invalid component")
        expected_error = "The value must be one of the following: " \
                         "[ns_server,query,indexing,search,eventing," \
                         "analytics,backup,xdcr,data,security,views]"
        invalid_event = deepcopy(valid_event)
        invalid_event[Event.Fields.COMPONENT] = "invalid"
        status, content = self.event_rest_helper.create_event(invalid_event)
        if status:
            self.fail("Event creation succeeded with invalid component")
        if content["errors"][Event.Fields.COMPONENT] != expected_error:
            self.fail("Invalid error message: %s" % content)

        self.log.info("Testing event with invalid timestamp")
        expected_error = "The value must be a valid ISO 8601 UTC"
        invalid_event = deepcopy(valid_event)
        invalid_event[Event.Fields.TIMESTAMP] = "invalid"
        status, content = self.event_rest_helper.create_event(invalid_event)
        if status:
            self.fail("Event creation succeeded with invalid timestamp")
        if content["errors"][Event.Fields.TIMESTAMP] != expected_error:
            self.fail("Invalid error message: %s" % content)

        self.log.info("Testing event with invalid timestamp format")
        invalid_event = deepcopy(valid_event)
        invalid_event[Event.Fields.TIMESTAMP] = \
            EventHelper.get_timestamp_format(datetime.utcnow())[:-1]
        status, content = self.event_rest_helper.create_event(invalid_event)
        if status:
            self.fail("Event creation succeeded with invalid timestamp format")
        if content["errors"][Event.Fields.TIMESTAMP] != expected_error:
            self.fail("Invalid error message: %s" % content)

        self.log.info("Testing event with invalid severity level")
        expected_error = "The value must be one of the following: " \
                         "[info,error,warn,fatal]"
        for value in ["debug", "warning", "critical"]:
            invalid_event = deepcopy(valid_event)
            invalid_event[Event.Fields.SEVERITY] = value
            status, content = self.event_rest_helper.create_event(invalid_event)
            if status:
                self.fail("Event creation succeeded with severity=%s" % value)
            if content["errors"][Event.Fields.SEVERITY] != expected_error:
                self.fail("Invalid error message: %s" % content)

        self.log.info("Testing event with type(description) = int")
        expected_error = "Value must be json string"
        invalid_event = deepcopy(valid_event)
        invalid_event[Event.Fields.DESCRIPTION] = 100
        status, content = self.event_rest_helper.create_event(invalid_event)
        if status:
            self.fail("Event creation succeeded with type(desc)=int")
        if content["errors"][Event.Fields.DESCRIPTION] != expected_error:
            self.fail("Invalid error message: %s" % content)

        self.log.info("Testing event with node value given explicitly")
        invalid_event = deepcopy(valid_event)
        invalid_event[Event.Fields.NODE_NAME] = "ns_serv_accepts_any_str"
        status, _ = self.event_rest_helper.create_event(invalid_event)
        if not status:
            self.fail("Event creation failed with node value")

        last_event = self.get_last_event_from_cluster()
        if last_event[Event.Fields.NODE_NAME] != "ns_serv_accepts_any_str":
            self.fail("Value mismatch in last_event: %s" % last_event)

    def test_logs_in_cbcollect(self):
        """
        Trigger cbcollect on nodes and make sure the following exists,
        - event_log file exists
        - Logs present in diag.log
        """
        def run_cb_collect_info(server):
            self.log.info("%s - Running cb_collect_info" % server.ip)
            remote_path = node_data[server.ip]["cb_collect_file"].split('/')
            file_name = remote_path[-1]
            dir_name = file_name.split('.')[0]
            remote_path = remote_path[:-1].join('/')
            CbCollectInfo(node_data[node.ip]["shell"]).start_collection(
                file_name=node_data[server.ip]["cb_collect_file"],
                compress_output=True)
            self.log.info("%s - cb_collect_info completed" % server.ip)

            self.log.info("%s - Fetching cbcollect file" % server.ip)
            result = node_data[node.ip]["shell"].get_file(remote_path,
                                                          file_name)
            self.log.info("%s - Cbcollect file transfered" % server.ip)

            # Remove cb_collect remote file
            node_data[node]["shell"].execute_command(
                "rm -f %s" % node_data[node]["cb_collect_file"])

            if result is False:
                node_data["error"] = "%s - get_file failed" % server.ip
                return

            # Extract the copied zip file
            with zipfile.ZipFile(file_name, 'r') as zip_fp:
                zip_fp.extractall(dir_name)

            self.log.info("%s - Starting validation" % server.ip)

            # Event_log file exists check
            if not os.path.exists(os.path.join(dir_name, event_log_file_name)):
                node_data["error"] = "%s - event log file missing" % server.ip

            result = int(os.system("grep '{\"timestamp\":' %s | wc -l "
                                   "| awk '{print $1}'"
                                   % os.path.join(dir_name, diags_file_name)))

            self.log.info("%s - Validation complete" % server.ip)

        file_ext = ".zip"
        file_generic_name = "/tmp/cbcollect_info_test-"
        cbcollect_timeout = 300
        event_log_file_name = "event_log"
        diags_file_name = "diag.log"
        diags_pattern_start = "Event Logs:"

        self.log.info("Starting cbcollect-info")
        # Dict to store server-shell connections
        node_data = dict()
        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            # Update file path in case of windows
            if shell.info.type.lower() == OS.WINDOWS:
                file_generic_name = "C:\\\\tmp\\\\cbcollect_info_test-"
                cbcollect_timeout = cbcollect_timeout * 5
            node_data[node.ip] = dict()
            node_data[node.ip]["shell"] = shell
            node_data[node.ip]["cb_collect"] = CbCollectInfo(shell)
            node_data[node.ip]["cb_collect_file"] = \
                file_generic_name + node.ip.replace('.', '_') + file_ext
            node_data[node.ip]["error"] = None

            # Start cb_collect_info in a separate thread
            tem_thread = Thread(target=run_cb_collect_info,
                                args=node)
            tem_thread.start()
            node_data[node.ip]["thread"] = tem_thread

        self.log.info("Waiting for cbcollect_info thread(s) to complete")
        for node in self.cluster.nodes_in_cluster:
            # Wait for cb_collect_info to complete
            node_data[node.ip]["thread"].join(timeout=cbcollect_timeout)

            # Close shell connections for respective node
            node_data[node.ip]["shell"].disconnect()

    def test_non_admin_access(self):
        """
        1. Create event using non-admin user
        2. Fetch events using non-admin user
        Both should fail with http response 401:unauthorised
        """
        def check_http_response(status_bool, content_dict):
            if status_bool:
                self.fail("Event creation with non-admin user succeeded")
            if content_dict["message"] != expected_err:
                self.fail("Unexpected error message: %s"
                          % content_dict["message"])

        timestamp = EventHelper.get_timestamp_format(datetime.utcnow())
        uuid_val = self.system_events.get_rand_uuid()
        severity = Event.Severity.INFO
        description = "Add event as non-admin"
        component = Event.Component.DATA
        event_dict = {
            Event.Fields.TIMESTAMP: timestamp,
            Event.Fields.COMPONENT: component,
            Event.Fields.SEVERITY: severity,
            Event.Fields.EVENT_ID: KvEngine.ScopeCreated,
            Event.Fields.UUID: uuid_val,
            Event.Fields.DESCRIPTION: description,
            Event.Fields.NODE_NAME: self.cluster.master.ip
        }
        expected_err = "Forbidden. User needs the following permissions"
        rest_connection = RestConnection(self.cluster.master)
        rbac_util = RbacUtil()

        bucket_admin = {'id': 'bucket_admin', 'name': 'bucket_admin',
                        'password': 'password'}
        bucket_admin_roles = {'id': 'bucket_admin', 'name': 'bucket_admin',
                              'roles': 'bucket_admin[*]'}

        ro_admin = {'id': 'ro_admin', 'name': 'ro_admin',
                    'password': 'password'}
        ro_admin_roles = {'id': 'ro_admin', 'name': 'ro_admin',
                          'roles': 'ro_admin'}

        users = [bucket_admin, ro_admin]
        roles = [bucket_admin_roles, ro_admin_roles]

        rbac_util.remove_user_role([user['id'] for user in users],
                                   rest_connection)
        self.log.info("Creating required users for testing")
        rbac_util.create_user_source(users, 'builtin', self.cluster.master)
        status = rbac_util.add_user_role(roles, rest_connection, 'builtin')
        if status is False:
            self.fail("User creation failed")

        # Create event using non-admin user
        self.log.info("Trying to create event_log using created users")
        status, content = self.event_rest_helper.create_event(
            event_dict, username=bucket_admin['id'],
            password=ro_admin['password'])
        check_http_response(status, content)

        event_dict[Event.Fields.UUID] = uuid_val
        status, content = self.event_rest_helper.create_event(
            event_dict, username=ro_admin['id'], password=ro_admin['password'])
        check_http_response(status, content)

        self.log.info("Fetching logs using cluster_ro admin")
        events = self.event_rest_helper.get_events(
            server=self.cluster.master, username=ro_admin['id'],
            password=ro_admin['password'])
        if isinstance(events, list) and len(events) == 0:
            self.fail("No events found")

        self.log.info("Fetching logs using bucket_admin")
        events = self.event_rest_helper.get_events(
            server=self.cluster.master,
            username=bucket_admin['id'], password=bucket_admin['password'])
        if isinstance(events, list) and len(events) == 0:
            self.fail("No events found")

        self.__validate(self.system_events.test_start_time)

    def test_process_crash(self):
        """
        Crash services to make sure we get respective sys-events generated
        """
        def crash_process(process_name, service_nodes):
            """
            Crash process on node
            """
            self.log.info("Testing %s crash" % process_name)
            target_node = choice(service_nodes)
            shell = RemoteMachineShellConnection(target_node)
            process_id = self.get_process_id(shell, process_name)
            self.log.debug("Pid of '%s'=%s" % (process_name, process_id))
            shell.execute_command("kill -9 %s" % process_id)
            shell.disconnect()
            self.sleep(5, "Sleep for %s to recover" % p_name)
            return target_node.ip, int(process_id)

        # To segregate the nodes based on the service they run
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        exit_status = 137

        # Crash memcached and xdcr
        for p_name in ["memcached", "goxdcr"]:
            node_ip, pid = crash_process(p_name, self.cluster.kv_nodes)
            self.system_events.add_event(
                NsServerEvents.service_crashed(node_ip, p_name, pid,
                                               exit_status))
            self.system_events.add_event(
                NsServerEvents.service_started(node_ip, {"name": p_name}))

        # Crash cbq-engine
        p_name = "cbq-engine"
        node_ip, pid = crash_process(p_name, self.cluster.query_nodes)
        self.system_events.add_event(
            NsServerEvents.service_crashed(node_ip, CbServer.Services.N1QL,
                                           pid, exit_status))
        self.system_events.add_event(
            NsServerEvents.service_started(node_ip,
                                           {"name": CbServer.Services.N1QL}))

        # Crash cbas
        p_name = "cbas"
        node_ip, pid = crash_process(p_name, self.cluster.cbas_nodes)
        self.system_events.add_event(
            NsServerEvents.service_crashed(node_ip, p_name, pid, exit_status))
        self.system_events.add_event(
            NsServerEvents.service_started(node_ip, {"name": p_name}))

        # Crash indexer
        p_name = "indexer"
        node_ip, pid = crash_process(p_name, self.cluster.index_nodes)
        self.system_events.add_event(
            NsServerEvents.service_crashed(node_ip, CbServer.Services.INDEX,
                                           pid, exit_status))
        self.system_events.add_event(
            NsServerEvents.service_started(node_ip,
                                           {"name": CbServer.Services.INDEX}))

        # Crash cbft
        p_name = "cbft"
        node_ip, pid = crash_process(p_name, self.cluster.fts_nodes)
        self.system_events.add_event(
            NsServerEvents.service_crashed(node_ip, CbServer.Services.FTS,
                                           pid, exit_status))
        self.system_events.add_event(
            NsServerEvents.service_started(node_ip,
                                           {"name": CbServer.Services.FTS}))

        # Crash backup
        p_name = CbServer.Services.BACKUP
        node_ip, pid = crash_process(p_name, self.cluster.backup_nodes)
        self.system_events.add_event(
            NsServerEvents.service_crashed(node_ip, p_name, pid, exit_status))
        self.system_events.add_event(
            NsServerEvents.service_started(node_ip, {"name": p_name}))

        # Crash eventing-producer
        p_name = "eventing-producer"
        node_ip, pid = crash_process(p_name, self.cluster.eventing_nodes)
        self.system_events.add_event(
            NsServerEvents.service_crashed(node_ip, CbServer.Services.EVENTING,
                                           pid, exit_status))
        self.system_events.add_event(
            NsServerEvents.service_started(
                node_ip, {"name": CbServer.Services.EVENTING}))

    def test_update_max_event_settings(self):
        """
        1. Update max_event_settings to 3000 >= random value >= 20000
        2. Create events such that the max_event counter is reached
        3. Decrease the max_event_settings so that the events get truncated
           in cluster
        4. Verify that the events are truncated
        5. Increase the value again to some higher value
        6. Create new events and validate the event log settings is reflected
        """

        def update_lib_counters(value):
            # Update cluster with new max_event_settings
            self.event_rest_helper.update_max_events(
                value, server=self.cluster.master)
            # Update max events in EventHelper class to help validation
            EventHelper.max_events = value
            self.max_event_count = value

        initial_val = choice(range(CbServer.sys_event_min_logs+10,
                                   CbServer.sys_event_max_logs))
        new_val = choice(range(CbServer.sys_event_min_logs,
                               CbServer.sys_event_min_logs+10))

        self.log.info("Updating max_event_counter=%s" % initial_val)
        update_lib_counters(initial_val)
        self.test_max_events()

        # Update to new value lower than the prev. value
        self.log.info("Updating max_event_counter=%s" % new_val)
        update_lib_counters(new_val)

        # Truncate values in system_event_events to trigger validation
        self.system_events.events = self.system_events.events[-new_val:]
        self.__validate()

        self.test_max_events()

    def test_update_max_event_settings_negative(self):
        """
        Update max_event_settings to 3000 < random value < 20000
        Rest call should fail with appropriate error message
        """
        event_values = [2999, 20001]
        expected_err = "eventLogsLimit - " \
                       "The value must be between 3000 and 20000."
        for value in event_values:
            status, content = self.event_rest_helper.update_max_events(
                value, server=self.cluster.master)
            if status:
                self.fail("Able to set max_event_count=%s" % value)
            if content["errors"][0] != expected_err:
                self.fail("Mismatch in expected error. Got %s" % content)

    def test_event_creation_during_rebalance(self):
        """
        1. Cluster with bucket created
        2. Perform rebalance operation
        3. Create events when rebalance is still in progress
        4. Wait for rebalance completion and validate the events
        """
        rebalance_type = self.input.param("rebalance_type", "in")
        events_to_create = self.input.param("num_events_to_create", 1000)
        involve_master = self.input.param("involve_master_node", False)
        doc_loading = self.input.param("with_doc_loading", False)
        failure = None
        components = Event.Component.values()
        event_severity = Event.Severity.values()

        nodes_in_cluster = deepcopy(self.cluster.nodes_in_cluster)
        if doc_loading:
            doc_gen = doc_generator(self.key, 0, self.num_items)
            doc_loading = self.task.async_continuous_doc_ops(
                self.cluster, self.cluster.buckets[0], doc_gen,
                exp=self.maxttl)

        if rebalance_type == "in":
            nodes_in = self.cluster.servers[
                       self.nodes_init:self.nodes_init+self.nodes_in]

            # Update nodes_in_cluster
            self.cluster.nodes_in_cluster.extend(nodes_in)

            # Start rebalance
            rebalance_task = self.task.async_rebalance(
                nodes_in_cluster, nodes_in, [])
        elif rebalance_type == "out":
            nodes_out = list()
            if involve_master:
                nodes_out.append(self.cluster.nodes_in_cluster.pop(0))
                self.nodes_out -= 1
                # Update master node
                self.cluster.master = self.cluster.nodes_in_cluster[0]
                self.log.info("Updated master - %s" % self.cluster.master.ip)
            while self.nodes_out:
                nodes_out.append(self.cluster.nodes_in_cluster.pop(-1))
                self.nodes_out -= 1

            # Start rebalance
            rebalance_task = self.task.async_rebalance(
                nodes_in_cluster, [], nodes_out)
        elif rebalance_type == "swap":
            nodes_in = self.cluster.servers[
                       self.nodes_init:self.nodes_init+self.nodes_in]
            nodes_out = list()
            if involve_master:
                nodes_out.append(self.cluster.nodes_in_cluster.pop(0))
                self.nodes_out -= 1
                # Update master node
                self.cluster.master = self.cluster.nodes_in_cluster[0]
                self.log.info("Updated master - %s" % self.cluster.master.ip)
            while self.nodes_out:
                nodes_out.append(self.cluster.nodes_in_cluster.pop(-1))
                self.nodes_out -= 1

            # Update nodes_in_cluster
            self.cluster.nodes_in_cluster.extend(nodes_in)

            # Start rebalance
            rebalance_task = self.task.async_rebalance(
                nodes_in_cluster, nodes_in, nodes_out,
                check_vbucket_shuffling=False)
        else:
            self.fail("Invalid rebalance type")

        ref_time = time() + 60
        while time() < ref_time:
            if rebalance_task.state == "running":
                break
        else:
            self.fail("Rebalance task is not running")

        self.log.info("Creating events with rebalance")
        for index in range(events_to_create):
            event = self.__generate_random_event(components, event_severity,
                                                 "Rebalance_event %s" % index)
            event.pop(Event.Fields.NODE_NAME)
            target_node = choice(self.cluster.nodes_in_cluster)
            status, _ = \
                self.event_rest_helper.create_event(event, server=target_node)
            if status is False:
                failure = "Event creation failed for: %s" % event
                self.log.critical(failure)
                break

            # Add correct node ip to event and append for validation
            event[Event.Fields.NODE_NAME] = target_node.ip
            self.system_events.add_event(event)

        self.log.info("Waiting for rebalance to complete")
        self.task_manager.get_task_result(rebalance_task)

        if doc_loading:
            self.log.info("Waiting for doc_loading to complete")
            doc_loading.end_task()
            self.task_manager.get_task_result(doc_loading)

        # Validate events
        self.__validate(self.system_events.test_start_time)
        if failure:
            self.fail(failure)

    def test_rebalance_out_and_in_nodes(self):
        """
        1. Rebalance out node with any service
        2. Add back node to the cluster
        3. Add back node should have log files from prev out operation
        """

        num_events = 100
        components = Event.Component.values()
        severities = Event.Severity.values()

        self.log.info("Loading few events before rebalance operation")
        for index in range(num_events):
            event = self.__generate_random_event(
                components, severities, description="Event %s" % index)
            event.pop(Event.Fields.NODE_NAME)

        nodes = self.cluster.servers[-self.nodes_out:]
        self.log.info("Rebalancing out %s nodes" % self.nodes_out)
        rebalance_task = self.task.async_rebalance(
            self.cluster.nodes_in_cluster, to_remove=nodes)

        # Update nodes_in_cluster
        self.cluster.nodes_in_cluster = self.cluster.servers[:-self.nodes_out]

        # Wait for rebalance to complete
        self.task_manager.get_task_result(rebalance_task)

        self.log.info("Loading few more events after rebalance out operation")
        for index in range(num_events, 2*num_events):
            event = self.__generate_random_event(
                components, severities, description="Event %s" % index)
            event.pop(Event.Fields.NODE_NAME)

        self.log.info("Rebalance in the nodes back to cluster")
        rebalance_task = self.task.async_rebalance(
            self.cluster.nodes_in_cluster, to_add=nodes)

        # Update nodes_in_cluster
        self.cluster.nodes_in_cluster += nodes

        # Wait for rebalance to complete
        self.task_manager.get_task_result(rebalance_task)

        self.__validate(self.system_events.test_start_time)

    def test_failover_recovery_events(self):
        """
        Trigger failover and recovery of nodes
        Validate respective events in system-logs
        """
        def set_recovery_and_rebalance(recovery_type):
            self.log.info("Waiting for failover to complete")
            rebalance_outcome = rest.monitorRebalance()
            if not rebalance_outcome:
                self.fail("Rebalance failed")

            if recovery_type:
                self.log.info("Add back node using '%s' recovery"
                              % recovery_type)
                status = rest.set_recovery_type(otp_node, recovery_type)
                if not status:
                    self.fail("Unable to set recovery type")

            result = self.task.rebalance(self.cluster.nodes_in_cluster,
                                         to_add=empty_list,
                                         to_remove=empty_list)
            if not result:
                self.fail("Rebalance operation failed after recovery")

            delta_nodes = failed_nodes = empty_list
            new_active_nodes = active_nodes
            if recovery_type is None:
                failed_nodes = [otp_node]
                new_active_nodes = [t_n.id for t_n in rest.node_statuses()
                                    if t_n.ip != target_node.ip]
            elif recovery_type == CbServer.Failover.RecoveryType.DELTA:
                delta_nodes = [otp_node]

            # Rebalance started event
            self.system_events.add_event(
                NsServerEvents.rebalance_started(
                    self.cluster.master.ip, new_active_nodes,
                    new_active_nodes, empty_list, delta_nodes, failed_nodes))
            if recovery_type is not None:
                # Bucket online event
                self.system_events.add_event(
                    DataServiceEvents.bucket_online(target_node.ip,
                                                    bucket.name, bucket.uuid))
            # Rebalance completed event
            self.system_events.add_event(
                NsServerEvents.rebalance_success(
                    self.cluster.master.ip, new_active_nodes,
                    new_active_nodes, empty_list, delta_nodes, failed_nodes))

        def do_event_validation(scenario_name):
            failures = \
                self.system_events.validate(self.cluster.master,
                                            self.system_events.test_start_time)
            if failures:
                self.fail("Failed to validate events for %s scenario"
                          % scenario_name)
        fo_type = self.input.param("failover_type",
                                   CbServer.Failover.Type.AUTO)
        auto_fo_threshold = 5
        auto_fo_reason = "The data service did not respond for the duration " \
                         "of the auto-failover threshold. Either none of " \
                         "the buckets have warmed up or there is an " \
                         "issue with the data service. "
        rest = RestConnection(self.cluster.master)
        if fo_type == "auto":
            self.log.info("Setting failover timeout = 5 seconds")
            rest.update_autofailover_settings(True, auto_fo_threshold, True)

        bucket = self.cluster.buckets[0]
        target_node = self.cluster.nodes_in_cluster[-1]
        active_nodes = list()
        otp_master = otp_node = None
        for node in rest.node_statuses():
            active_nodes.append(node.id)
            if node.ip == self.cluster.master.ip:
                otp_master = node.id
            if node.ip == target_node.ip:
                otp_node = node.id

        empty_list = list()
        otp_nodes = [otp_node]
        fo_reason_dict = {otp_node: auto_fo_reason}
        self.log.info("Node to failover: %s" % target_node.ip)

        self.log.info("1/3 Testing graceful-failover")
        rest.fail_over(otp_node, graceful=True)
        self.system_events.add_event(
            NsServerEvents.graceful_failover_started(
                self.cluster.master.ip, active_nodes, otp_nodes, otp_master))
        self.system_events.add_event(
            NsServerEvents.graceful_failover_complete(
                self.cluster.master.ip, active_nodes, otp_nodes, otp_master))
        set_recovery_and_rebalance(CbServer.Failover.RecoveryType.DELTA)
        do_event_validation("graceful failover")

        self.log.info("2/3 Testing hard-failover")
        rest.fail_over(otp_node, graceful=False)
        self.system_events.add_event(
            NsServerEvents.hard_failover_started(
                self.cluster.master.ip, active_nodes, otp_nodes, otp_master))
        self.system_events.add_event(
            NsServerEvents.hard_failover_complete(
                self.cluster.master.ip, active_nodes, otp_nodes, otp_master))
        set_recovery_and_rebalance(CbServer.Failover.RecoveryType.FULL)
        do_event_validation("hard failover")

        self.log.info("3/3 Testing auto-failover")
        shell = RemoteMachineShellConnection(target_node)
        cb_err = CouchbaseError(self.log, shell)
        cb_err.create(CouchbaseError.STOP_MEMCACHED)
        self.sleep(10, "Wait for auto_failover to trigger")
        cb_err.revert(CouchbaseError.STOP_MEMCACHED)
        shell.disconnect()
        self.system_events.add_event(
            NsServerEvents.auto_failover_started(
                self.cluster.master.ip, active_nodes, otp_nodes,
                otp_master, fo_reason_dict))
        set_recovery_and_rebalance(recovery_type=None)
        do_event_validation("auto-failover")

    def test_kill_event_log_server(self):
        """
        - Kill event_log_server immediately after writing a log
        - Restart event_log_server
        - Make sure the last written log is served from disk
        """
        diag_eval_cmd = "supervisor:%s(ns_server_sup, event_log_server)"
        terminate_cmd = diag_eval_cmd % "terminate_child"
        restart_cmd = diag_eval_cmd % "restart_child"

        # Create generic event template for testing
        event_format = {
            Event.Fields.EVENT_ID: 0,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.DESCRIPTION: "Crash test event %s"
        }

        # Reset events in test case for validation
        event_list = list()
        self.__reset_events()

        # Enable diag/eval on non_local_host
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        # Create required number of events
        self.log.info("Creating required event objects")
        base_time_stamp = datetime.utcnow()
        for index in range(100):
            base_time_stamp += timedelta(milliseconds=1)
            event = deepcopy(event_format)
            event[Event.Fields.UUID] = self.system_events.get_rand_uuid()
            event[Event.Fields.TIMESTAMP] = \
                self.system_events.get_timestamp_format(base_time_stamp)
            event[Event.Fields.DESCRIPTION] = \
                event[Event.Fields.DESCRIPTION] % index
            event_list.append(event)
            self.system_events.add_event(event)

        # Create events on cluster
        self.log.info("Creating events on cluster")
        for event in event_list:
            self.event_rest_helper.create_event(event,
                                                server=self.cluster.master)
        rest = RestConnection(self.cluster.master)
        self.log.info("Terminating event_log_server")
        rest.diag_eval(terminate_cmd)

        self.sleep(5, "Wait before restarting the event_log_server back")
        rest.diag_eval(restart_cmd)
        self.sleep(5, "Wait for event_log_server to become fully operational")

        # Validate events
        self.__validate(self.system_events.test_start_time)

    def test_event_log_replication(self):
        """
        - Stop event_log_server on replica node
        - Create events on other nodes in the cluster
        - Start event_log_server on replica node
        - Validate all logs are synchronised on all nodes after restart
        """
        diag_eval_cmd = "supervisor:%s(ns_server_sup, event_log_server)"
        terminate_cmd = diag_eval_cmd % "terminate_child"
        restart_cmd = diag_eval_cmd % "restart_child"

        failures = None
        gossip_timeout = 120
        base_time_stamp = datetime.utcnow()
        target_node = choice(self.cluster.nodes_in_cluster)
        non_affected_nodes = [node for node in self.cluster.nodes_in_cluster
                              if node.ip != target_node.ip]

        # Create generic event template for testing
        event_format = {
            Event.Fields.EVENT_ID: 0,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.DESCRIPTION: "Crash test event %s"
        }

        # Reset events in test case for validation
        self.__reset_events()

        # Enable diag/eval on non_local_host
        shell = RemoteMachineShellConnection(target_node)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        self.log.info("Stopping event_log_server on %s" % target_node.ip)
        rest = RestConnection(target_node)
        rest.diag_eval(terminate_cmd)

        self.log.info("Creating event on other nodes")
        for index in range(100):
            event_target = choice(non_affected_nodes)
            base_time_stamp += timedelta(milliseconds=1)
            event = deepcopy(event_format)
            event[Event.Fields.UUID] = self.system_events.get_rand_uuid()
            event[Event.Fields.TIMESTAMP] = \
                self.system_events.get_timestamp_format(base_time_stamp)
            event[Event.Fields.DESCRIPTION] = \
                event[Event.Fields.DESCRIPTION] % index
            self.system_events.add_event(event)

            self.event_rest_helper.create_event(event,
                                                server=event_target)

        node = None
        for node in non_affected_nodes:
            self.log.info("Validating events from node %s" % node.ip)
            failures = self.system_events.validate(
                server=node, since_time=self.system_events.test_start_time,
                events_count=-1)
            if failures:
                # Will make test fail after restarting event_log_server
                break

        self.log.info("Starting event_log_server on %s" % target_node.ip)
        rest.diag_eval(restart_cmd)

        if failures:
            self.fail("Event log replication failed for node %s" % node.ip)

        self.log.info("Validating events from node %s" % target_node.ip)
        for sec in range(gossip_timeout):
            failures = self.system_events.validate(
                server=target_node,
                since_time=self.system_events.test_start_time,
                events_count=-1)
            if not failures:
                self.log.critical("Events synced after %s seconds" % sec)
                # All events are replicated
                break
            self.sleep(1, "Wait before next check")
        else:
            self.fail("Events not synced up on %s: %s"
                      % (target_node.ip, failures))

    # KV / Data related test cases
    def test_bucket_related_event_logs(self):
        """
        - Create bucket
        - Create scope/collections
        - Drop scope/collections
        - Flush bucket
        - Delete bucket
        - Validate all above events
        """

        def bucket_events():
            event_helper = EventHelper()
            event_helper.set_test_start_time()
            kv_node = choice(self.cluster.nodes_in_cluster)

            bucket_name = self.bucket_util.get_random_name()
            bucket_type = choice([Bucket.Type.EPHEMERAL, Bucket.Type.MEMBASE])
            bucket_size = randint(512, 600)
            num_replicas = choice([0, 1, 2])
            bucket_ttl = choice([0, 1000, 50000, 2147483647])
            compression_mode = choice([Bucket.CompressionMode.ACTIVE,
                                       Bucket.CompressionMode.PASSIVE,
                                       Bucket.CompressionMode.OFF])
            bucket_storage = choice([Bucket.StorageBackend.couchstore,
                                     Bucket.StorageBackend.magma])
            flush_enabled = choice([0, 1])
            if bucket_type == Bucket.Type.EPHEMERAL:
                bucket_durability = choice(
                    [BucketDurability[Bucket.DurabilityLevel.NONE],
                     BucketDurability[Bucket.DurabilityLevel.MAJORITY]])
            else:
                bucket_durability = \
                    choice([value for _, value in BucketDurability.items()])

            tbl = TableView(self.log.critical)
            tbl.set_headers(["Field", "Value"])
            tbl.add_row(["Bucket Type", bucket_type])
            tbl.add_row(["Bucket size", str(bucket_size)])
            tbl.add_row(["Replicas", str(num_replicas)])
            tbl.add_row(["TTL", str(bucket_ttl)])
            tbl.add_row(["Compression mode", compression_mode])
            tbl.add_row(["Storage backend", bucket_storage])
            tbl.add_row(["Flush enabled", str(flush_enabled)])
            tbl.display("Creating bucket %s:" % bucket_name)

            try:
                self.bucket_util.create_default_bucket(
                    self.cluster,
                    bucket_type=bucket_type,
                    ram_quota=bucket_size,
                    replica=num_replicas,
                    maxTTL=bucket_ttl,
                    compression_mode=compression_mode,
                    wait_for_warmup=True,
                    conflict_resolution=Bucket.ConflictResolution.SEQ_NO,
                    replica_index=self.bucket_replica_index,
                    storage=bucket_storage,
                    eviction_policy=self.bucket_eviction_policy,
                    flush_enabled=flush_enabled,
                    bucket_durability=bucket_durability,
                    purge_interval=self.bucket_purge_interval,
                    autoCompactionDefined="false",
                    fragmentation_percentage=50,
                    bucket_name=bucket_name)
            except Exception as e:
                test_failures.append(str(e))
                return

            buckets = self.bucket_util.get_all_buckets(self.cluster)
            bucket = [bucket for bucket in buckets
                      if bucket.name == bucket_name][0]

            bucket_create_event = DataServiceEvents.bucket_create(
                self.cluster.master.ip, bucket_type,
                bucket.name, bucket.uuid,
                {'compression_mode': compression_mode,
                 'max_ttl': bucket_ttl,
                 'storage_mode': bucket_storage,
                 'conflict_resolution_type': Bucket.ConflictResolution.SEQ_NO,
                 'eviction_policy': self.bucket_eviction_policy,
                 'purge_interval': 'undefined',
                 'durability_min_level': self.bucket_durability_level,
                 'num_replicas': num_replicas})
            if bucket_create_event[Event.Fields.EXTRA_ATTRS]['bucket_props'][
                    'eviction_policy'] == Bucket.EvictionPolicy.VALUE_ONLY:
                bucket_create_event[Event.Fields.EXTRA_ATTRS][
                    'bucket_props']['eviction_policy'] = 'value_only'
            if self.bucket_type == Bucket.Type.EPHEMERAL:
                bucket_create_event[Event.Fields.EXTRA_ATTRS][
                    'bucket_props']['storage_mode'] = Bucket.Type.EPHEMERAL
                bucket_create_event[Event.Fields.EXTRA_ATTRS][
                    'bucket_props'].pop('purge_interval', None)
                bucket_create_event[Event.Fields.EXTRA_ATTRS][
                    'bucket_props']['eviction_policy'] = "no_eviction"
            self.system_events.add_event(bucket_create_event)

            scope = self.bucket_util.get_random_name(
                max_length=CbServer.max_scope_name_len)
            collection = self.bucket_util.get_random_name(
                max_length=CbServer.max_collection_name_len)

            try:
                self.log.info("%s - Creating scope" % bucket_name)
                self.bucket_util.create_scope(kv_node, bucket,
                                              {"name": scope})
                event_helper.add_event(
                    DataServiceEvents.scope_created(kv_node.ip, bucket_name,
                                                    scope))
                self.log.info("%s - Creating collection" % bucket_name)
                self.bucket_util.create_collection(kv_node, bucket, scope,
                                                   {"name": collection})
                event_helper.add_event(
                    DataServiceEvents.collection_created(
                        kv_node.ip, bucket_name, scope, collection))

                if flush_enabled:
                    self.bucket_util.flush_bucket(self.cluster, bucket)
                    event_helper.add_event(
                        DataServiceEvents.bucket_flushed(
                            master_ip, bucket_name, bucket.uuid))
                    self.sleep(5, "%s - Wait after flush" % bucket_name)

                self.log.info("%s - Dropping collection" % bucket_name)
                self.bucket_util.drop_collection(kv_node, bucket,
                                                 scope, collection)
                event_helper.add_event(
                    DataServiceEvents.collection_dropped(
                        kv_node.ip,  bucket_name, scope, collection))

                self.log.info("%s - Dropping scope" % bucket_name)
                self.bucket_util.drop_scope(kv_node, bucket, scope)
                event_helper.add_event(
                    DataServiceEvents.scope_dropped(kv_node.ip, bucket_name,
                                                    scope))

                self.log.info("%s - Deleting bucket" % bucket_name)
                self.bucket_util.delete_bucket(self.cluster, bucket)
                event_helper.add_event(
                    DataServiceEvents.bucket_dropped(master_ip, bucket_name,
                                                     bucket.uuid))

                # Validation
                for node in self.cluster.nodes_in_cluster:
                    failures = event_helper.validate(
                        node, since_time=event_helper.test_start_time)
                    if failures:
                        test_failures.extend(failures)
                        break
            except Exception as e:
                test_failures.append(str(e))
                return

        # Test starts here
        index = 0
        max_loops = 5
        num_threads = 4
        test_failures = list()
        while index < max_loops:
            self.log.info("Loop index %s" % index)
            bucket_threads = list()
            for _ in range(num_threads):
                thread = Thread(target=bucket_events)
                thread.start()
                bucket_threads.append(thread)

            for thread in bucket_threads:
                thread.join(60)

            if test_failures:
                self.fail(test_failures)
            index += 1
            self.sleep(5, "Wait for all buckets to get deleted")

    def test_update_bucket_params(self):
        """
        Update all possible bucket specific params
        Validate the respective system_event log for the updated params
        """
        bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
        old_settings = {
            "compression_mode": bucket.compressionMode,
            "max_ttl": bucket.maxTTL,
            "storage_mode": bucket.storageBackend,
            "conflict_resolution_type": bucket.conflictResolutionType,
            "num_threads": bucket.threadsNumber,
            "flush_enabled": True if bucket.flushEnabled else False,
            "durability_min_level": bucket.durability_level,
            "replica_index": bucket.replicaIndex,
            "num_replicas": bucket.replicaNumber
        }
        if bucket.evictionPolicy == Bucket.EvictionPolicy.VALUE_ONLY:
            old_settings["eviction_policy"] = 'value_only'
        if bucket.bucketType == Bucket.Type.EPHEMERAL:
            old_settings["eviction_policy"] = "no_eviction"
            old_settings["storage_mode"] = Bucket.Type.EPHEMERAL
            old_settings.pop("replica_index", None)

        # Add BucketOnline event in case of single node cluster
        # (In multi-node we may not know the order of events across the nodes)
        if self.nodes_init == 1:
            self.system_events.add_event(DataServiceEvents.bucket_online(
                self.cluster.master.ip, bucket.name, bucket.uuid))

        # Validate pre-bucket update events
        self.__validate(self.system_events.test_start_time)

        # Update bucket RAM quota
        self.log.info("Updating bucket_ram quota")
        self.bucket_util.update_bucket_property(
            self.cluster.master, bucket,
            ram_quota_mb=self.bucket_size+1)

        # Get the last bucket update event
        event = self.get_last_event_from_cluster()
        bucket_updated_event = DataServiceEvents.bucket_updated(
            self.cluster.master.ip, bucket.name, bucket.uuid,
            bucket.bucketType, dict(), dict())
        for param, value in bucket_updated_event.items():
            if param == Event.Fields.EXTRA_ATTRS:
                continue
            if event[param] != value:
                self.fail("Value mismatch for '%s'. Expected %s != %s Actual"
                          % (param, value, event[param]))

        # Test other mandatory fields
        event_keys = event.keys()
        for param in [Event.Fields.TIMESTAMP, Event.Fields.UUID]:
            if param not in event_keys:
                self.fail("%s key missing in bucket update event" % param)

        # Test Extra Attributes fields
        for param in ["bucket", "bucket_uuid", "type"]:
            exp_val = bucket_updated_event[Event.Fields.EXTRA_ATTRS][param]
            act_val = event[Event.Fields.EXTRA_ATTRS][param]
            if act_val != exp_val:
                self.fail("Mismatch in %s. Expected %s != %s Actual"
                          % (param, exp_val, act_val))

        act_val = event[Event.Fields.EXTRA_ATTRS]["old_settings"]
        for param in ["ram_quota", "purge_interval"]:
            if param not in act_val.keys():
                self.fail("'%s' missing in old_settings: %s"
                          % (param, act_val))
            act_val.pop(param)
        if old_settings != act_val:
            self.fail("Old settings' value mismatch. Expected %s != %s Actual"
                      % (old_settings, act_val))

        expected_keys = 11 if bucket.bucketType == Bucket.Type.EPHEMERAL \
            else 12
        act_val = event[Event.Fields.EXTRA_ATTRS]["new_settings"]
        act_val_keys = act_val.keys()
        if len(act_val_keys) != expected_keys \
                or 'ram_quota' not in act_val_keys:
            self.fail("Mismatch in new-setting params: %s" % act_val_keys)

        self.num_replicas += 1
        self.log.info("Updating bucket_replica=%s" % self.num_replicas)
        self.bucket_util.update_bucket_property(
            self.cluster.master, bucket,
            replica_number=self.num_replicas)

        event = self.get_last_event_from_cluster()
        act_val = event[Event.Fields.EXTRA_ATTRS]["new_settings"]
        act_val_keys = act_val.keys()
        if len(act_val_keys) != expected_keys \
                or 'num_replicas' not in act_val_keys:
            self.fail("Mismatch in new-setting params: %s" % act_val_keys)
        if event[Event.Fields.EXTRA_ATTRS]["old_settings"]["num_replicas"] \
                != self.num_replicas-1:
            self.fail("Mismatch in old replica val. Expected %s != %s Actual"
                      % (self.num_replicas-1,
                         event[Event.Fields.EXTRA_ATTRS][
                             "old_settings"]["num_replicas"]))
        if act_val["num_replicas"] != self.num_replicas:
            self.fail("Mismatch in replica value. Expected %s != %s Actual"
                      % (self.num_replicas, act_val["num_replicas"]))

    def test_update_memcached_settings(self):
        """
        Update memcached settings and validate

        Refer MB-49631 for other valid fields
        """
        bucket_helper = BucketHelper(self.cluster.master)
        bucket_helper.update_memcached_settings(max_connections=2000)
        bucket_helper.update_memcached_settings(max_connections=2001)
        last_event = self.get_last_event_from_cluster()
        # Check and remove fields with dynamic values
        for field in [Event.Fields.UUID, Event.Fields.TIMESTAMP]:
            if field not in last_event:
                self.fail("'%s' field missing: %s" % (field, last_event))
            last_event.pop(field)

        event = DataServiceEvents.memcached_settings_changed(
            self.cluster.master.ip,
            {"max_connections": 2000},
            {"max_connections": 2001})
        event["otp_node"] = "ns_1@" + self.cluster.master.ip
        if last_event != event:
            self.fail("Mismatch in event. Expected %s != %s Actual"
                      % (event, last_event))

    def test_auto_reprovisioning(self):
        """
        - Create Ephemeral bucket
        - Trigger auto-reprovisioning and validate the system event logs
        """
        def validate_rebalance_event_fields(r_type, cluster_event_dict):
            for field in [Event.Fields.UUID, Event.Fields.TIMESTAMP]:
                if field not in cluster_event_dict:
                    self.fail("Field '%s' missing in rebalance event: %s"
                              % (field, cluster_event_dict))
                cluster_event_dict.pop(field)

            fields = ['operation_id']
            if r_type != "reb_start":
                fields.append('time_taken')
            if r_type == "reb_failed":
                fields.append('completion_message')
            for field in fields:
                if field not in \
                        cluster_event_dict[Event.Fields.EXTRA_ATTRS]:
                    self.fail("Field '%s' missing in rebalance extra_attr: %s"
                              % (field, cluster_event_dict))
                cluster_event_dict[Event.Fields.EXTRA_ATTRS].pop(field)

        def sort_nodes(t_event):
            for n_type in ["active_nodes", "keep_nodes"]:
                t_event[Event.Fields.EXTRA_ATTRS]["nodes_info"][n_type].sort()

        rebalance_failure = self.input.param("with_rebalance", False)
        rebalance_task = None
        cluster_event = dict()
        bucket = self.cluster.buckets[0]
        active_nodes = ['ns_1@' + node.ip
                        for node in self.cluster.nodes_in_cluster]
        eject_nodes = [self.cluster.nodes_in_cluster[-1]]
        eject_ns_nodes = ['ns_1@' + eject_nodes[0].ip]
        keep_nodes = ['ns_1@' + node.ip
                      for node in self.cluster.nodes_in_cluster
                      if node.ip != self.cluster.nodes_in_cluster[-1].ip]
        empty_list = list()
        memcached_process = "memcached"
        if self.bucket_type != Bucket.Type.EPHEMERAL:
            self.fail("Test valid only for ephemeral bucket")

        node = choice(self.cluster.nodes_in_cluster)
        self.log.info("Target node: %s" % node.ip)
        shell = RemoteMachineShellConnection(node)
        p_id = int(self.get_process_id(shell, memcached_process))
        self.log.critical("Memcached pid=%s" % p_id)

        if rebalance_failure:
            rebalance_task = self.task.async_rebalance(
                self.cluster.nodes_in_cluster,
                to_add=[], to_remove=eject_nodes)
            self.system_events.add_event(
                NsServerEvents.rebalance_started(
                    self.cluster.master.ip, active_nodes=active_nodes,
                    keep_nodes=keep_nodes, eject_nodes=eject_ns_nodes,
                    delta_nodes=empty_list, failed_nodes=empty_list))
            self.sleep(5, "Wait for rebalance to start")
            cluster_event["reb_start"] = self.get_last_event_from_cluster()

        shell.execute_command("kill -9 %s" % p_id)
        shell.disconnect()

        # Add required event to validate
        restarted_nodes = [node.ip]
        self.system_events.add_event(NsServerEvents.service_crashed(
            node.ip, memcached_process, p_id, 137))
        self.system_events.add_event(
            NsServerEvents.service_started(node.ip,
                                           {"name": memcached_process}))
        if rebalance_failure:
            reb_fail_event = None
            max_retry_time = time() + 5
            while time() < max_retry_time and reb_fail_event is None:
                reb_fail_event = self.get_last_event_from_cluster()
                if reb_fail_event["event_id"] != NsServer.RebalanceFailure:
                    reb_fail_event = None
            cluster_event["reb_failed"] = reb_fail_event

        if rebalance_failure:
            self.task_manager.get_task_result(rebalance_task)
            self.system_events.add_event(
                NsServerEvents.rebalance_failed(
                    self.cluster.master.ip,  active_nodes=active_nodes,
                    keep_nodes=keep_nodes, eject_nodes=eject_ns_nodes,
                    delta_nodes=empty_list, failed_nodes=empty_list))

        self.system_events.add_event(
            DataServiceEvents.ephemeral_auto_reprovision(
                self.cluster.master.ip, bucket.name, bucket.uuid,
                nodes=restarted_nodes, restarted_on=restarted_nodes))
        self.system_events.add_event(
            DataServiceEvents.bucket_online(node.ip, bucket.name,
                                            bucket.uuid))

        self.sleep(30, "Wait for bucket online before rebalancing")
        rebalance_task = self.task.async_rebalance(
            self.cluster.nodes_in_cluster, [], [])
        self.sleep(2, "Wait for rebalance to start")
        if not rebalance_failure:
            cluster_event["reb_start"] = self.get_last_event_from_cluster()
        self.task_manager.get_task_result(rebalance_task)

        self.system_events.add_event(
            NsServerEvents.rebalance_started(
                self.cluster.master.ip, active_nodes=active_nodes,
                keep_nodes=active_nodes, eject_nodes=empty_list,
                delta_nodes=empty_list, failed_nodes=empty_list))
        self.system_events.add_event(
            NsServerEvents.rebalance_success(
                self.cluster.master.ip, active_nodes=active_nodes,
                keep_nodes=active_nodes, eject_nodes=empty_list,
                delta_nodes=empty_list, failed_nodes=empty_list))
        cluster_event["reb_success"] = self.get_last_event_from_cluster()

        # Validate all cluster events before validating event specific fields
        self.__validate(self.system_events.test_start_time)

        self.log.info("Validating rebalance specific events")
        # Validate fields existence in event dict
        for reb_type, event in cluster_event.items():
            validate_rebalance_event_fields(reb_type, event)

        # Validate cluster event with local event dict
        reb_event = NsServerEvents.rebalance_started(
            self.cluster.master.ip,
            active_nodes=active_nodes, keep_nodes=keep_nodes,
            eject_nodes=eject_ns_nodes, delta_nodes=empty_list,
            failed_nodes=empty_list)
        reb_event["otp_node"] = "ns_1@%s" % self.cluster.master.ip
        if not rebalance_failure:
            reb_event[Event.Fields.EXTRA_ATTRS][
                "nodes_info"]["keep_nodes"] = active_nodes
            reb_event[Event.Fields.EXTRA_ATTRS][
                "nodes_info"]["eject_nodes"] = empty_list

        for tem_event in [reb_event,
                          cluster_event["reb_start"],
                          cluster_event["reb_failed"],
                          cluster_event["reb_success"]]:
            sort_nodes(tem_event)

        if reb_event != cluster_event["reb_start"]:
            self.fail("Rebalance start event is not as expected. "
                      "Expected: %s, Actual: %s"
                      % (reb_event, cluster_event["reb_start"]))

        if 'reb_failed' in cluster_event:
            reb_event = NsServerEvents.rebalance_failed(
                self.cluster.master.ip,
                active_nodes=active_nodes, keep_nodes=keep_nodes,
                eject_nodes=eject_ns_nodes, delta_nodes=empty_list,
                failed_nodes=empty_list)
            reb_event["otp_node"] = "ns_1@%s" % self.cluster.master.ip
            sort_nodes(reb_event)
            if reb_event != cluster_event["reb_failed"]:
                self.fail("Rebalance failed event is not as expected. "
                          "Expected: %s, Actual: %s"
                          % (reb_event, cluster_event["reb_failed"]))

        reb_event = NsServerEvents.rebalance_success(
            self.cluster.master.ip,
            active_nodes=active_nodes, keep_nodes=active_nodes,
            eject_nodes=[], delta_nodes=[], failed_nodes=[])
        reb_event["otp_node"] = "ns_1@%s" % self.cluster.master.ip
        sort_nodes(reb_event)
        if reb_event != cluster_event["reb_success"]:
            self.fail("Rebalance success event is not as expected. "
                      "Expected: %s, Actual: %s"
                      % (reb_event, cluster_event["reb_success"]))
