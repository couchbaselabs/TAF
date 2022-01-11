from basetestcase import ClusterSetup

from SystemEventLogLib.Events import Event
from SystemEventLogLib.SystemEventOperations import SystemEventRestHelper
from SystemEventLogLib.security_events import SecurityEvents
from membase.api.rest_client import RestConnection


class SystemEventLogs(ClusterSetup):
    def setUp(self):
        super(SystemEventLogs, self).setUp()
        self.rest = RestConnection(self.cluster.master)
        self.event_rest_helper = \
            SystemEventRestHelper(self.cluster.nodes_in_cluster)
        self.log_setup_status("SystemEventLogs", "started")

    def tearDown(self):
        self.log_setup_status("SystemEventLogs", "started", stage="tearDown")
        self.log_setup_status("SystemEventLogs", "completed", stage="tearDown")
        super(SystemEventLogs, self).tearDown()

    def get_last_event_from_cluster(self):
        return self.event_rest_helper.get_events(
            server=self.cluster.master, events_count=-1)["events"][-1]

    def generic_fields_check(self, event):
        event_keys = event.keys()
        for param in [Event.Fields.TIMESTAMP, Event.Fields.UUID]:
            if param not in event_keys:
                self.fail("%s key missing in event" % param)

    def test_user_crud_events(self):
        domain = "local"
        username = "cbadminbucket"
        payload = "name=" + username + "&roles=admin"

        for action in ["create", "delete"]:
            if action == "create":
                self.log.info("Creating user")
                self.rest.add_set_builtin_user(username, payload)
                user_event = SecurityEvents.user_added(self.cluster.master.ip, "local")
            else:
                self.log.info("Deleting user")
                self.rest.delete_builtin_user(username)
                user_event = SecurityEvents.user_deleted(self.cluster.master.ip, "local")

            # Get the last user created/deleted event
            event = self.get_last_event_from_cluster()


            # Test NON Extra Attributes fields & NON generic fields
            for param, value in user_event.items():
                if param == Event.Fields.EXTRA_ATTRS:
                    continue
                if event[param] != value:
                    self.fail("Value mismatch for '%s'. Expected %s != %s Actual"
                              % (param, value, event[param]))

            self.generic_fields_check(event)

            # Test Extra Attributes fields
            if event[Event.Fields.EXTRA_ATTRS]["user"] == username:
                self.fail("Username got printed in system event log")
            if event[Event.Fields.EXTRA_ATTRS]["domain"] != domain:
                self.fail("Domain mismatch. Expected %s != %s Actual"
                          % (event[Event.Fields.EXTRA_ATTRS]["domain"], domain))

    def test_group_crud_events(self):
        groupname = "cbadmingroup"
        payload = "roles=admin"

        for action in ["create", "delete"]:
            if action == "create":
                self.log.info("Creating group")
                self.rest.add_set_bulitin_group(groupname, payload)
                user_event = SecurityEvents.group_added(self.cluster.master.ip)
            else:
                self.log.info("Deleting group")
                self.rest.delete_builtin_group(groupname)
                user_event = SecurityEvents.group_deleted(self.cluster.master.ip)

            # Get the last user created/deleted event
            event = self.get_last_event_from_cluster()

            # Test NON Extra Attributes fields & NON generic fields
            for param, value in user_event.items():
                if param == Event.Fields.EXTRA_ATTRS:
                    continue
                if event[param] != value:
                    self.fail("Value mismatch for '%s'. Expected %s != %s Actual"
                              % (param, value, event[param]))

            self.generic_fields_check(event)

            # Test Extra Attributes fields
            if event[Event.Fields.EXTRA_ATTRS]["group"] == groupname:
                self.fail("Username got printed in system event log")