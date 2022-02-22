import copy

from basetestcase import ClusterSetup

from SystemEventLogLib.Events import Event
from SystemEventLogLib.SystemEventOperations import SystemEventRestHelper
from SystemEventLogLib.security_events import SecurityEvents
from couchbase_utils.cb_tools.cb_cli import CbCli
from membase.api.rest_client import RestConnection
from platform_utils.remote.remote_util import RemoteMachineShellConnection


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

    def get_event_from_cluster(self, last=True):
        events = self.event_rest_helper.get_events(
            server=self.cluster.master, events_count=-1)["events"]
        if last:
            return events[-1]
        else:
            return events

    def generic_fields_check(self, event):
        event_keys = event.keys()
        for param in [Event.Fields.TIMESTAMP, Event.Fields.UUID]:
            if param not in event_keys:
                self.fail("%s key missing in event" % param)

    @staticmethod
    def get_default_password_settings():
        return {
            "minLength": 6,
            "must_present": []
        }

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
            event = self.get_event_from_cluster()

            # Test NON Extra Attributes fields & NON generic fields
            for param, value in user_event.items():
                if param == Event.Fields.EXTRA_ATTRS:
                    continue
                if event[param] != value:
                    self.fail("Value mismatch for '%s'. Expected %s != %s Actual"
                              % (param, value, event[param]))

            # Test generic fields
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

            # Get the last event
            event = self.get_event_from_cluster()

            # Test NON Extra Attributes fields & NON generic fields
            for param, value in user_event.items():
                if param == Event.Fields.EXTRA_ATTRS:
                    continue
                if event[param] != value:
                    self.fail("Value mismatch for '%s'. Expected %s != %s Actual"
                              % (param, value, event[param]))

            # Test generic fields
            self.generic_fields_check(event)

            # Test Extra Attributes fields
            if event[Event.Fields.EXTRA_ATTRS]["group"] == groupname:
                self.fail("Groupname got printed in system event log")

    def test_password_policy_changed_event(self):
        default_settings = self.get_default_password_settings()
        status, content = self.rest.change_password_policy(min_length=8, enforce_digits="true")
        if not status:
            self.fail("Changing password policy failed with {0}".format(content))
        new_settings = copy.deepcopy(default_settings)
        new_settings["minLength"] = 8
        new_settings["must_present"] = ["digits"]

        # Get the last event
        event = self.get_event_from_cluster()
        user_event = SecurityEvents.password_policy_changed(self.cluster.master.ip,
                                                            old_min_length=
                                                            default_settings["minLength"],
                                                            old_must_present=
                                                            default_settings["must_present"],
                                                            new_min_length=
                                                            new_settings["minLength"],
                                                            new_must_present=
                                                            new_settings["must_present"])

        # Test NON Extra Attributes fields & NON generic fields
        for param, value in user_event.items():
            if param == Event.Fields.EXTRA_ATTRS:
                continue
            if event[param] != value:
                self.fail("Value mismatch for '%s'. Expected %s != %s Actual"
                          % (param, value, event[param]))

        # Test generic fields
        self.generic_fields_check(event)

        # Test Extra Attributes fields
        for param in ["old_settings", "new_settings"]:
            exp_val = user_event[Event.Fields.EXTRA_ATTRS][param]
            act_val = event[Event.Fields.EXTRA_ATTRS][param]
            if act_val != exp_val:
                self.fail("Mismatch in %s. Expected %s != %s Actual"
                          % (param, exp_val, act_val))

    def test_ldap_config_changed_event(self):

        def validate_ldap_event(old_settings, new_settings):
            old_settings_copy = copy.deepcopy(old_settings)
            new_settings_copy = copy.deepcopy(new_settings)
            old_settings_copy["clientTLSCert"] = "redacted"
            old_settings_copy["cacert"] = "redacted"
            old_settings_copy["bindDN"] = "redacted"
            new_settings_copy["clientTLSCert"] = "redacted"
            new_settings_copy["cacert"] = "redacted"
            new_settings_copy["bindDN"] = "redacted"
            for key, val in old_settings_copy.items():
                if val == "false":
                    old_settings_copy[key] = False
                elif val == "true":
                    old_settings_copy[key] = True
            for key, val in new_settings_copy.items():
                if val == "false":
                    new_settings_copy[key] = False
                elif val == "true":
                    new_settings_copy[key] = True
            # Get the last event
            event = self.get_event_from_cluster()
            user_event = SecurityEvents.ldap_config_changed(self.cluster.master.ip,
                                                            old_settings_copy, new_settings_copy)

            # Test NON Extra Attributes fields & NON generic fields
            for param, value in user_event.items():
                if param == Event.Fields.EXTRA_ATTRS:
                    continue
                if event[param] != value:
                    self.fail("Value mismatch for '%s'. Expected %s != %s Actual"
                              % (param, value, event[param]))

            # Test generic fields
            self.generic_fields_check(event)

            # Test Extra Attributes fields
            for param in ["old_settings", "new_settings"]:
                exp_val = user_event[Event.Fields.EXTRA_ATTRS][param]
                act_val = event[Event.Fields.EXTRA_ATTRS][param]
                if act_val != exp_val:
                    self.fail("Mismatch in %s. Expected %s != %s Actual"
                              % (param, exp_val, act_val))

        old_setting = {"authenticationEnabled": "false", "authorizationEnabled": "false",
                       "bindDN": "", "bindPass": "", "cacheValueLifetime": 300000,
                       "encryption": "None", "failOnMaxDepth": "false",
                       "hosts": [], "maxCacheSize": 10000,
                       "maxParallelConnections": 100, "nestedGroupsEnabled": "false",
                       "nestedGroupsMaxDepth": 10, "port": 389, "requestTimeout": 5000,
                       "serverCertValidation": "true", "userDNMapping": "None"}
        new_setting = {"authenticationEnabled": "true", "authorizationEnabled": "true",
                       "bindDN": "", "bindPass": "", "cacheValueLifetime": 300000,
                       "encryption": "None", "failOnMaxDepth": "false",
                       "hosts": [], "maxCacheSize": 10000,
                       "maxParallelConnections": 100, "nestedGroupsEnabled": "false",
                       "nestedGroupsMaxDepth": 10, "port": 390, "requestTimeout": 5000,
                       "serverCertValidation": "true", "userDNMapping": "None"}
        ldap_settings = {"authenticationEnabled": "true", "authorizationEnabled": "true",
                         "port": 390}
        status, content, _ = self.rest.configure_ldap_settings(ldap_settings)
        if not status:
            self.fail("Setting LDAP failed")
        validate_ldap_event(old_setting, new_setting)

        # revert to old ldap settings and check if event is generated
        ldap_settings = {"authenticationEnabled": "false", "authorizationEnabled": "false",
                         "port": 389}
        status, content, _ = self.rest.configure_ldap_settings(ldap_settings)
        if not status:
            self.fail("Reverting LDAP Setting failed")
        validate_ldap_event(new_setting, old_setting)

    def test_saslauthd_config_changed_event(self):

        def validate_saslauthd_event(event, user_event):
            # Test NON Extra Attributes fields & NON generic fields
            for param, value in user_event.items():
                if param == Event.Fields.EXTRA_ATTRS:
                    continue
                if event[param] != value:
                    self.fail("Value mismatch for '%s'. Expected %s != %s Actual"
                              % (param, value, event[param]))

            # Test generic fields
            self.generic_fields_check(event)

            # Test Extra Attributes fields
            for param in ["old_settings", "new_settings"]:
                exp_val = user_event[Event.Fields.EXTRA_ATTRS][param]
                act_val = event[Event.Fields.EXTRA_ATTRS][param]
                if act_val != exp_val:
                    self.fail("Mismatch in %s. Expected %s != %s Actual"
                              % (param, exp_val, act_val))

        # enable sasl authd
        settings = {"enabled": "true", "admins": "alice,barry", "roAdmins": "clair,daniel"}
        status, content, _ = self.rest.configure_sasl_authd(settings)
        if not status:
            self.fail("Changing saslauthd settings failed {0}".format(content))

        # Get the last event
        actual_event = self.get_event_from_cluster()
        expected_event = SecurityEvents.sasldauth_config_changed(self.cluster.master.ip,
                                                                 old_enabled=False,
                                                                 new_enabled=True)
        validate_saslauthd_event(event=actual_event, user_event=expected_event)

        # disable the saslauthd
        status, content, _ = self.rest.configure_sasl_authd({"enabled": "false"})
        if not status:
            self.fail("disabling saslauthd failed {0}".format(content))

        # Get the last event
        actual_event = self.get_event_from_cluster()
        expected_event = SecurityEvents.sasldauth_config_changed(self.cluster.master.ip,
                                                                 old_enabled=True,
                                                                 new_enabled=False)
        validate_saslauthd_event(event=actual_event, user_event=expected_event)

    def test_security_config_changed_event(self):
        _ = self.rest.update_autofailover_settings(False, 120, False)
        shell_conn = RemoteMachineShellConnection(self.cluster.master)
        cb_cli = CbCli(shell_conn)
        o = cb_cli.enable_n2n_encryption()
        self.log.info(o)
        shell_conn.disconnect()
        self.rest.set_encryption_level(level="control")

        settings = {"tlsMinVersion": "tlsv1.1", "clusterEncryptionLevel": "all"}
        self.rest.set_security_settings(settings)

        old_settings = {"ssl_minimum_protocol": "tlsv1.2", "cluster_encryption_level": "control"}
        new_settings = {"ssl_minimum_protocol": "tlsv1.1", "cluster_encryption_level": "all"}
        # Get the last event
        event = self.get_event_from_cluster()
        user_event = SecurityEvents.security_config_changed(self.cluster.master.ip,
                                                            old_settings, new_settings)

        # Test NON Extra Attributes fields & NON generic fields
        for param, value in user_event.items():
            if param == Event.Fields.EXTRA_ATTRS:
                continue
            if event[param] != value:
                self.fail("Value mismatch for '%s'. Expected %s != %s Actual"
                          % (param, value, event[param]))

        # Test generic fields
        self.generic_fields_check(event)

        # Test Extra Attributes fields
        for param in ["old_settings", "new_settings"]:
            expected_settings = user_event[Event.Fields.EXTRA_ATTRS][param]
            actual_settings = event[Event.Fields.EXTRA_ATTRS][param]
            for i_param in ["ssl_minimum_protocol", "cluster_encryption_level"]:
                act_val = actual_settings[i_param]
                exp_val = expected_settings[i_param]
                if act_val != exp_val:
                    self.fail("Mismatch in %s. Expected %s != %s Actual"
                              % (param, exp_val, act_val))

    def test_audit_enabled_event(self):
        status = self.rest.setAuditSettings(enabled='true')
        if not status:
            self.fail("Enabling audit failed")

        # Get the audit event
        events = self.get_event_from_cluster(last=False)
        events.reverse()
        event = {}
        for eve in events:
            self.log.info("description {0}".format(eve["description"]))
            if eve["description"] == "Audit enabled":
                event = eve
                break
        user_event = SecurityEvents.audit_enabled(self.cluster.master.ip, enabled_audit_ids=[],
                                                  log_path="/opt/couchbase/var/lib/couchbase/logs",
                                                  rotate_interval=86400,
                                                  rotate_size=20971520)
        # Test NON Extra Attributes fields & NON generic fields
        for param, value in user_event.items():
            if param == Event.Fields.EXTRA_ATTRS:
                continue
            if event[param] != value:
                self.fail("Value mismatch for '%s'. Expected %s != %s Actual"
                          % (param, value, event[param]))

        # Test generic fields
        self.generic_fields_check(event)

        # Test Extra Attributes fields
        # check if mandatory keys in new settings are present
        for param in ["enabled_audit_ids", "log_path", "rotate_interval", "rotate_size"]:
            if param not in event[Event.Fields.EXTRA_ATTRS]["new_settings"].keys():
                self.fail("Param: {0} not present in new settings".format(param))

    def test_audit_disabled_event(self):
        status = self.rest.setAuditSettings(enabled='true')
        if not status:
            self.fail("Enabling audit failed")
        status = self.rest.setAuditSettings(enabled='false')
        if not status:
            self.fail("Disabling audit failed")

        # Get the audit event
        events = self.get_event_from_cluster(last=False)
        events.reverse()
        event = {}
        for eve in events:
            if eve["description"] == "Audit disabled":
                event = eve
                break
        user_event = SecurityEvents.audit_disabled(self.cluster.master.ip,
                                                   enabled_audit_ids=[],
                                                   log_path="/opt/couchbase/var/lib/couchbase/logs",
                                                   rotate_interval=86400,
                                                   rotate_size=524288000,
                                                   sync=[])
        # Test NON Extra Attributes fields & NON generic fields
        for param, value in user_event.items():
            if param == Event.Fields.EXTRA_ATTRS:
                continue
            if event[param] != value:
                self.fail("Value mismatch for '%s'. Expected %s != %s Actual"
                          % (param, value, event[param]))

        # Test generic fields
        self.generic_fields_check(event)

        # Test Extra Attributes fields
        # check if mandatory keys in old settings are present
        for param in ["enabled_audit_ids", "log_path", "rotate_interval", "rotate_size", "sync"]:
            if param not in event[Event.Fields.EXTRA_ATTRS]["old_settings"].keys():
                self.fail("Param: {0} not present in new settings".format(param))

    def test_audit_config_changed_event(self):
        status = self.rest.setAuditSettings(enabled='true')
        if not status:
            self.fail("Enabling audit failed")
        status = self.rest.setAuditSettings(enabled='true',
                                            rotateInterval=172800)
        if not status:
            self.fail("Setting audit failed")

        # Get the audit event
        events = self.get_event_from_cluster(last=False)
        events.reverse()
        event = {}
        for eve in events:
            if eve["description"] == "Audit configuration changed":
                event = eve
                break
        user_event = SecurityEvents.audit_setting_changed(self.cluster.master.ip,
                                                          old_enabled_audit_ids=[],
                                                          old_log_path="",
                                                          old_rotate_interval=86400,
                                                          old_rotate_size="", sync=[],
                                                          new_enabled_audit_ids=[],
                                                          new_log_path="",
                                                          new_rotate_interval=172800,
                                                          new_rotate_size="")
        # Test NON Extra Attributes fields & NON generic fields
        for param, value in user_event.items():
            if param == Event.Fields.EXTRA_ATTRS:
                continue
            if event[param] != value:
                self.fail("Value mismatch for '%s'. Expected %s != %s Actual"
                          % (param, value, event[param]))

        # Test generic fields
        self.generic_fields_check(event)

        # Test Extra Attributes fields
        for param in ["old_settings", "new_settings"]:
            expected_settings = user_event[Event.Fields.EXTRA_ATTRS][param]
            actual_settings = event[Event.Fields.EXTRA_ATTRS][param]
            for i_param in ["rotate_interval"]:
                act_val = actual_settings[i_param]
                exp_val = expected_settings[i_param]
                if act_val != exp_val:
                    self.fail("Mismatch in %s. Expected %s != %s Actual"
                              % (param, exp_val, act_val))
