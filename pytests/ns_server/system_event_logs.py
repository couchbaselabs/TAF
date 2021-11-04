import os.path
import zipfile
from copy import deepcopy
from datetime import datetime, timedelta
from random import choice, randint
from threading import Thread

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
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection, OS


class SystemEventLogs(ClusterSetup):
    def setUp(self):
        super(SystemEventLogs, self).setUp()

        self.log_setup_status("SystemEventLogs", "started")
        self.max_event_count = \
            self.input.param("max_event_count",
                             CbServer.sys_event_def_logs)
        self.event_rest_helper = \
            SystemEventRestHelper(self.cluster.nodes_in_cluster)

        if self.max_event_count != CbServer.sys_event_def_logs:
            # Update max event logs in cluster to new value
            self.event_rest_helper.update_max_events(self.max_event_count)
            # Update max events in EventHelper class to help validation
            EventHelper.max_events = self.max_event_count

        self.id_range = {
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
        self.log_setup_status("SystemEventLogs", "completed")

    def tearDown(self):
        self.log_setup_status("SystemEventLogs", "started", stage="tearDown")
        # Update max event counter to default value
        self.event_rest_helper.update_max_events()
        # Reset max events in EventHelper class
        EventHelper.max_events = CbServer.sys_event_def_logs
        self.log_setup_status("SystemEventLogs", "completed", stage="tearDown")

        super(SystemEventLogs, self).tearDown()

    def __validate(self, start_time=None):
        for server in self.cluster.nodes_in_cluster:
            self.log.info("Validating events from node %s" % server.ip)
            failures = self.system_events.validate(server=server,
                                                   since_time=start_time)
            if failures:
                self.fail(failures)

    def __generate_random_event(self, components, severities, description):
        timestamp = EventHelper.get_timestamp_format(datetime.now())
        uuid_val = self.system_events.get_rand_uuid()
        severity = choice(severities)
        component = choice(components)
        return {
            Event.Fields.TIMESTAMP: timestamp,
            Event.Fields.COMPONENT: choice(components),
            Event.Fields.SEVERITY: severity,
            Event.Fields.EVENT_ID:
                choice(range(self.id_range[component][0],
                             self.id_range[component][1])),
            Event.Fields.UUID: uuid_val,
            Event.Fields.DESCRIPTION: description,
            Event.Fields.NODE_NAME: self.cluster.master.ip
        }

    def test_event_id_range(self):
        """
        Create custom events for 'component' using the event-ids
        provided by the 'event_id_range'.
        'is_range_valid' determines the provided values are right or not.
        is_range_valid - Basically to test negative scenarios
        """
        start_time = EventHelper.get_timestamp_format(datetime.now())
        component = self.input.param("component", Event.Component.NS_SERVER)
        event_id_range = self.input.param("event_id_range", "0:1023")
        is_range_valid = self.input.param("is_range_valid", True)

        if is_range_valid:
            # event_id_range is a range. Eg: 0:1023
            event_id_range = event_id_range.split(":")
            event_id_range = range(int(event_id_range[0]),
                                   int(event_id_range[1]) + 1)
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
        for event_id in event_id_range:
            timestamp = EventHelper.get_timestamp_format(datetime.now())
            uuid_val = self.system_events.get_rand_uuid()
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
            status, content = self.event_rest_helper.create_event(event_dict)
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
        uuid_val = self.system_events.get_rand_uuid()
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
        start_time = EventHelper.get_timestamp_format(datetime.now())
        target_node = choice(self.cluster.nodes_in_cluster)

        # Event dictionary format
        event_dict = {
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EVENT_ID: KvEngine.BucketCreated,
            Event.Fields.TIMESTAMP:
                EventHelper.get_timestamp_format(datetime.now()),
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

        self.__validate(start_time)

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
            Event.Fields.TIMESTAMP:
                EventHelper.get_timestamp_format(datetime.now()),
            Event.Fields.UUID: self.system_events.get_rand_uuid(),
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.DESCRIPTION: "a"
        }
        target_node = choice(self.cluster.nodes_in_cluster)
        self.event_rest_helper.create_event(event_dict, server=target_node)
        event_dict[Event.Fields.NODE_NAME] = target_node.ip
        self.system_events.add_event(event_dict)
        self.log.info("Size: %s" % len(str(event_dict)))

        # Event with empty extra_attr
        self.log.info("Adding event with empty extra_attr field")
        event_dict.pop(Event.Fields.NODE_NAME)
        event_dict[Event.Fields.EXTRA_ATTRS] = ""
        event_dict[Event.Fields.UUID] = self.system_events.get_rand_uuid()
        target_node = choice(self.cluster.nodes_in_cluster)
        self.event_rest_helper.create_event(event_dict, server=target_node)
        event_dict[Event.Fields.NODE_NAME] = target_node.ip
        self.system_events.add_event(event_dict)
        self.log.info("Size: %s" % len(str(event_dict)))

        # Event with max size extra_attr
        self.log.info("Adding event with empty extra_attr field")
        event_dict.pop(Event.Fields.NODE_NAME)
        event_dict[Event.Fields.EXTRA_ATTRS] = \
            "a" * CbServer.sys_event_log_max_size

        event_dict[Event.Fields.UUID] = self.system_events.get_rand_uuid()
        target_node = choice(self.cluster.nodes_in_cluster)
        self.event_rest_helper.create_event(event_dict, server=target_node)
        event_dict[Event.Fields.NODE_NAME] = target_node.ip
        self.system_events.add_event(event_dict)
        self.log.info("Size: %s" % len(str(event_dict)))

        self.__validate(self.system_events.test_start_time)

    def test_duplicate_events(self):
        """
        1. Raise duplicate events back to back to check the
           redundant events are handled
        2. Raise duplicate events back to back but separated
           by time interval of 60sec to make sure the duplicate events are
           recorded by ns_server
        """
        dummy_pid = 1000
        target_node = choice(self.cluster.nodes_in_cluster)
        uuid_to_test = self.system_events.get_rand_uuid()

        self.log.info("Add duplicate events within 1min time frame")
        # Create duplicate events back to back within same component
        # Event-1 definition
        event_1 = NsServerEvents.baby_sitter_respawn(target_node.ip,
                                                     dummy_pid)
        event_1.pop(Event.Fields.EXTRA_ATTRS)
        event_1[Event.Fields.UUID] = uuid_to_test
        event_1[Event.Fields.TIMESTAMP] = \
            EventHelper.get_timestamp_format(datetime.now())

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
            EventHelper.get_timestamp_format(datetime.now())

        self.log.info("Adding events with similar UUID")
        event_1 = deepcopy(event_1)
        event_1.pop(Event.Fields.NODE_NAME)
        self.event_rest_helper.create_event(event_1, server=target_node)
        self.event_rest_helper.create_event(event_2)

        self.__validate(self.system_events.test_start_time)

        # Get new UUID to test
        uuid_to_test = self.system_events.get_rand_uuid()
        curr_time = datetime.now()

        # Create required event dictionaries for testing
        event_1 = DataServiceEvents.scope_created(target_node.ip,
                                                  "bucket", "scope_1")
        event_1.pop(Event.Fields.EXTRA_ATTRS)
        event_1[Event.Fields.UUID] = uuid_to_test
        event_1[Event.Fields.TIMESTAMP] = \
            EventHelper.get_timestamp_format(curr_time)
        event_2[Event.Fields.UUID] = uuid_to_test

        # Add valid event to the list for validation
        self.system_events.add_event(deepcopy(event_1))
        self.log.info("Adding event with unique uuid")
        end_time = curr_time + timedelta(
            seconds=CbServer.sys_event_log_uuid_uniqueness_time - 1)
        event_1.pop(Event.Fields.NODE_NAME)
        event_1[Event.Fields.TIMESTAMP] = \
            EventHelper.get_timestamp_format(datetime.now())
        self.log.info(event_1)
        self.event_rest_helper.create_event(event_1)

        self.log.info("Add duplicate events across allowed time frame")
        while curr_time <= end_time:
            curr_time = curr_time + timedelta(seconds=1)
            event_2[Event.Fields.TIMESTAMP] = \
                EventHelper.get_timestamp_format(curr_time)
            self.event_rest_helper.create_event(event_2)
            self.log.info(event_2)

        curr_time = curr_time + timedelta(seconds=1)
        self.log.info(end_time)
        self.log.info(curr_time)

        # Validate the events
        self.__validate(self.system_events.test_start_time)

        self.log.info("Adding same event_id after the allowed time frame")
        event_2[Event.Fields.TIMESTAMP] = \
            EventHelper.get_timestamp_format(curr_time)
        self.log.info(event_2[Event.Fields.TIMESTAMP])
        self.event_rest_helper.create_event(event_2, server=target_node)
        self.log.info(event_2)
        event_2[Event.Fields.NODE_NAME] = target_node.ip
        self.system_events.add_event(event_2)

        # Validate the events
        self.__validate(self.system_events.test_start_time)

    def test_invalid_values(self):
        """
        Create events with invalid field values.
        Expect the event creation to fail
        """
        valid_event = {
            Event.Fields.EVENT_ID: 0,
            Event.Fields.UUID: self.system_events.get_rand_uuid(),
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.TIMESTAMP: EventHelper.get_timestamp_format(
                datetime.now()),
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
            EventHelper.get_timestamp_format(datetime.now())[:-1]
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
        expected_error = "Unexpected field"
        invalid_event = deepcopy(valid_event)
        invalid_event[Event.Fields.NODE_NAME] = "1.1.1.1"
        status, content = self.event_rest_helper.create_event(invalid_event)
        if status:
            self.fail("Event creation succeeded with node value given")
        if content["errors"][Event.Fields.NODE_NAME] != expected_error:
            self.fail("Invalid error message: %s" % content)

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

        timestamp = EventHelper.get_timestamp_format(datetime.now())
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
        if isinstance(list, events) and len(events) == 0:
            self.fail("No events found")

        self.log.info("Fetching logs using bucket_admin")
        events = self.event_rest_helper.get_events(
            server=self.cluster.master,
            username=bucket_admin['id'], password=bucket_admin['password'])
        if isinstance(list, events) and len(events) == 0:
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
            process_id, _ = shell.execute_command(
                "ps -ef | grep \"%s \" | grep -v grep | awk '{print $2}'"
                % process_name)
            self.log.debug("Pid of '%s'=%s" % (process_name, process_id))
            shell.execute_command("kill -9 %s" % process_id)
            shell.disconnect()
            return target_node.ip, int(process_id)

        # To segrigate the nodes based on the service they run
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)

        # Crash memcached
        node_ip, pid = crash_process("memcached", self.cluster.kv_nodes)
        self.system_events.add_event(
            DataServiceEvents.memcached_crashed(node_ip, pid))

        # Crash goxdcr
        node_ip, pid = crash_process("goxdcr", self.cluster.kv_nodes)
        self.system_events.add_event(
            DataServiceEvents.memcached_crashed(node_ip, pid))

        # Crash cbq-engine
        node_ip, pid = crash_process("cbq-engine", self.cluster.query_nodes)
        self.system_events.add_event(
            DataServiceEvents.memcached_crashed(node_ip, pid))

        # Crash cbas
        node_ip, pid = crash_process("cbas", self.cluster.cbas_nodes)
        self.system_events.add_event(
            DataServiceEvents.memcached_crashed(node_ip, pid))

        # Crash indexer
        node_ip, pid = crash_process("indexer", self.cluster.index_nodes)
        self.system_events.add_event(
            DataServiceEvents.memcached_crashed(node_ip, pid))

        # Crash cbft
        node_ip, pid = crash_process("cbft", self.cluster.fts_nodes)
        self.system_events.add_event(
            DataServiceEvents.memcached_crashed(node_ip, pid))

        # Crash backup
        node_ip, pid = crash_process("backup", self.cluster.backup_nodes)
        self.system_events.add_event(
            DataServiceEvents.memcached_crashed(node_ip, pid))

        # Crash eventing-producer
        node_ip, pid = crash_process("eventing-producer",
                                     self.cluster.eventing_nodes)
        self.system_events.add_event(
            DataServiceEvents.memcached_crashed(node_ip, pid))

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

        initial_val = choice(range(CbServer.sys_event_min_logs+1000,
                                   CbServer.sys_event_max_logs))
        new_val = choice(range(CbServer.sys_event_min_logs,
                               CbServer.sys_event_min_logs+1000))

        self.log.info("Updating max_event_counter=%s" % initial_val)
        self.event_rest_helper.update_max_events(initial_val,
                                                 server=self.cluster.master)
        self.max_event_count = initial_val
        self.test_max_events()

        # Update to new value lower than the prev. value
        self.log.info("Updating max_event_counter=%s" % new_val)
        self.event_rest_helper.update_max_events(new_val,
                                                 server=self.cluster.master)
        self.system_events._set_counter(new_val)
        self.max_event_count = new_val

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

        # Reset test start time to avoid test failures due to time difference
        self.system_events.set_test_start_time()

        if doc_loading:
            doc_gen = doc_generator(self.key, 0, self.num_items)
            doc_loading = self.task.async_continuous_doc_ops(
                self.cluster, self.cluster.buckets[0], doc_gen,
                exp=self.maxttl)

        if rebalance_type == "in":
            nodes_in = self.cluster.servers[self.nodes_init:self.nodes_in]

            # Update nodes_in_cluster
            self.cluster.nodes_in_cluster.extend(nodes_in)

            # Start rebalance
            rebalance_task = self.task.async_rebalance(
                self.cluster.nodes_in_cluster, nodes_in, [])
        elif rebalance_type == "out":
            nodes_out = list()
            if involve_master:
                nodes_out = [self.cluster.master]
                nodes_out -= 1
                # Update master node
                self.cluster.master = self.cluster.servers[1]
                self.log.info("Updated master - %s" % self.cluster.master.ip)
            nodes_out.extend(self.cluster.nodes_in_cluster[-self.nodes_out:])

            # Update nodes_in_cluster
            start_index = 0
            if involve_master:
                start_index = 1
            self.cluster.nodes_in_cluster = \
                self.cluster.nodes_in_cluster[start_index:-self.nodes_out]

            # Start rebalance
            rebalance_task = self.task.async_rebalance(
                self.cluster.nodes_in_cluster, [], nodes_out)
        elif rebalance_type == "swap":
            nodes_out = list()
            if involve_master:
                nodes_out = [self.cluster.master]
                nodes_out -= 1
                # Update master node
                self.cluster.master = self.cluster.servers[1]
                self.log.info("Updated master - %s" % self.cluster.master.ip)
            nodes_out.extend(self.cluster.nodes_in_cluster[-self.nodes_out:])
            nodes_in = self.cluster.servers[self.nodes_init:self.nodes_in]

            # Update nodes_in_cluster
            start_index = 0
            if involve_master:
                start_index = 1
            self.cluster.nodes_in_cluster = \
                self.cluster.nodes_in_cluster[start_index:-self.nodes_out] \
                + nodes_in

            # Start rebalance
            rebalance_task = self.task.async_rebalance(
                self.cluster.nodes_in_cluster, nodes_in, nodes_out)
        else:
            self.fail("Invalid rebalance type")

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
        self.system_events.set_test_start_time()
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
