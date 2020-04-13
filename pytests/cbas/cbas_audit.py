import json

from cbas.cbas_base import CBASBaseTest
from security_utils.audit_ready_functions import audit
from rbac_utils.Rbac_ready_functions import RbacUtils


class CBASAuditLogs(CBASBaseTest):
    
    actual_service_parameter_dict = {}
    actual_node_parameter_dict = {}
    expected_service_parameter_dict = {}
    expected_node_parameter_dict = {}
    
    def build_expected_service_parameter_dict(self):
        self.log.info("Fetch configuration service parameters")
        status, content, _ = self.cbas_util.fetch_service_parameter_configuration_on_cbas()
        self.assertTrue(status, msg="Response status incorrect for GET request")

        self.log.info("Create server configuration expected dictionary")
        CBASAuditLogs.actual_service_parameter_dict = json.loads(content)
        for key in CBASAuditLogs.actual_service_parameter_dict:
            CBASAuditLogs.expected_service_parameter_dict["config_before:" + key] = CBASAuditLogs.actual_service_parameter_dict[key]
            CBASAuditLogs.expected_service_parameter_dict["config_after:" + key] = CBASAuditLogs.actual_service_parameter_dict[key]
    
    def build_expected_node_parameter_dict(self):
        self.log.info("Fetch configuration node parameters")
        status, content, _ = self.cbas_util.fetch_node_parameter_configuration_on_cbas()
        self.assertTrue(status, msg="Response status incorrect for GET request")

        self.log.info("Create node configuration expected dictionary")
        CBASAuditLogs.actual_node_parameter_dict = json.loads(content)
        for key in CBASAuditLogs.actual_node_parameter_dict:
            CBASAuditLogs.expected_node_parameter_dict["config_before:" + key] = CBASAuditLogs.actual_node_parameter_dict[key]
            CBASAuditLogs.expected_node_parameter_dict["config_after:" + key] = CBASAuditLogs.actual_node_parameter_dict[key]

    def setUp(self):
        super(CBASAuditLogs, self).setUp()

        self.log.info("Enable audit on cluster")
        audit_obj = audit(host=self.cluster.master)
        current_state = audit_obj.getAuditStatus()
        if current_state:
            self.log.info("Audit already enabled, disabling and re-enabling to remove previous settings")
            audit_obj.setAuditEnable('false')
        audit_obj.setAuditEnable('true')

        self.log.info("Build service configuration expected dictionary object")
        self.build_expected_service_parameter_dict()

        self.log.info("Build node configuration expected dictionary object")
        self.build_expected_node_parameter_dict()

    def validate_audit_event(self, event_id, host, expected_audit):
        auditing = audit(eventID=event_id, host=host)
        _, audit_match = auditing.validateEvents(expected_audit)
        self.assertTrue(audit_match, "Values for one of the fields mismatch, refer test logs for mismatch value")

    """
    cbas.cbas_audit.CBASAuditLogs.test_successful_service_configuration_updates_are_audited,default_bucket=False,audit_id=36865
    """
    def test_successful_service_configuration_updates_are_audited(self):

        self.log.info("Read audit input id")
        self.audit_id = self.input.param("audit_id")

        self.log.info("Create a configuration map that will be passed as JSON body for service configuration")
        update_configuration_map = {}
        for key in CBASAuditLogs.actual_service_parameter_dict:
            if isinstance(CBASAuditLogs.actual_service_parameter_dict[key], (int, long)) and CBASAuditLogs.actual_service_parameter_dict[key] != 1:
                update_configuration_map[key] = CBASAuditLogs.actual_service_parameter_dict[key] - 1

        self.log.info("Update service configuration")

        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas({'jobHistorySize' : 20})
        self.assertTrue(status, msg="Incorrect status for configuration service PUT request")

        self.log.info("Update expected dictionary")
        expected_dict = CBASAuditLogs.expected_service_parameter_dict
        key = "jobHistorySize"
        value = 20

        expected_dict["config_after:" + key] = value

        self.log.info("Validate audit log for service configuration update")
        self.validate_audit_event(self.audit_id, self.cbas_node, expected_dict)

    """
    cbas.cbas_audit.CBASAuditLogs.test_successful_node_configuration_updates_are_audited,default_bucket=False,audit_id=36866
    """
    def test_successful_node_configuration_updates_are_audited(self):

        self.log.info("Read audit input id")
        self.audit_id = self.input.param("audit_id")

        self.log.info("Create a configuration map that will be passed as JSON body for node configuration")
        update_configuration_map = {}
        for key in CBASAuditLogs.actual_node_parameter_dict:
            if isinstance(CBASAuditLogs.actual_node_parameter_dict[key], (int, long)):
                update_configuration_map[key] = CBASAuditLogs.actual_node_parameter_dict[key] - 1

        self.log.info("Update node configuration")
        status, _, _ = self.cbas_util.update_node_parameter_configuration_on_cbas(update_configuration_map)
        self.assertTrue(status, msg="Incorrect status for configuration service PUT request")

        self.log.info("Update expected dictionary")
        expected_dict = CBASAuditLogs.expected_node_parameter_dict
        for key in update_configuration_map:
            expected_dict["config_after:" + key] = update_configuration_map[key]

        self.log.info("Validate audit log for service configuration update")
        self.validate_audit_event(self.audit_id, self.cbas_node, expected_dict)

    """
    cbas.cbas_audit.CBASAuditLogs.test_unsuccessful_service_configuration_updates_are_not_audited,default_bucket=False,audit_id=36865
    """
    def test_unsuccessful_service_configuration_updates_are_not_audited(self):

        self.log.info("Read configuration audit id")
        self.audit_id = self.input.param("audit_id")

        self.log.info("Update configuration service parameters: logLevel with incorrect value")
        service_configuration_map = {"logLevel": "Invalid"}
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(service_configuration_map)
        self.assertFalse(status, msg="Incorrect status for service configuration PUT request")

        self.log.info("Verify audit log event is not generated since service configuration update failed")
        audit_obj = audit(eventID=self.audit_id, host=self.cbas_node)
        self.assertFalse(audit_obj.check_if_audit_event_generated(), msg="Audit event must not be generated")
    
    """
    cbas.cbas_audit.CBASAuditLogs.test_unsuccessful_node_configuration_updates_are_not_audited,default_bucket=False,audit_id=36866
    """
    def test_unsuccessful_node_configuration_updates_are_not_audited(self):

        self.log.info("Read configuration audit id")
        self.audit_id = self.input.param("audit_id")

        self.log.info("Update configuration service parameters: storageBuffercacheSize with incorrect value")
        node_configuration_map = {"storageBuffercacheSize": "bulk"}
        status, _, _ = self.cbas_util.update_node_parameter_configuration_on_cbas(node_configuration_map)
        self.assertFalse(status, msg="Incorrect status for node configuration PUT request")

        self.log.info("Validate audit logs are not generated for unsuccessful node configuration update")
        audit_obj = audit(eventID=self.audit_id, host=self.cbas_node)
        self.assertFalse(audit_obj.check_if_audit_event_generated(), msg="Audit event must not be generated")

    """
    cbas.cbas_audit.CBASAuditLogs.test_toggling_service_audit_filter_component,default_bucket=False,audit_id=36865
    """
    def test_toggling_service_audit_filter_component(self):

        self.log.info("Read configuration audit id")
        self.audit_id = self.input.param("audit_id")

        self.log.info("Disable audit logging for service configuration change")
        audit_obj = audit(host=self.cluster.master)
        audit_obj.setAuditFeatureDisabled(str(self.audit_id))

        self.log.info("Update configuration service parameters: logLevel")
        service_configuration_map = {"logLevel": "TRACE"}
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(service_configuration_map)
        self.assertTrue(status, msg="Incorrect status for service configuration PUT request")

        self.log.info("Validate audit logs are not generated for service configuration update")
        service_audit_obj = audit(eventID=self.audit_id, host=self.cbas_node)
        self.assertFalse(service_audit_obj.check_if_audit_event_generated(), msg="Audit event must not be generated")

        self.log.info("Enable audit logging for service configuration change")
        audit_obj.setAuditFeatureDisabled('')

        self.log.info("Update configuration service parameters: logLevel")
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(service_configuration_map)
        self.assertTrue(status, msg="Incorrect status for service configuration PUT request")

        self.log.info("Validate audit logs are generated for service configuration update")
        self.assertTrue(service_audit_obj.check_if_audit_event_generated(), msg="Audit event must be generated")

    """
    cbas.cbas_audit.CBASAuditLogs.test_toggling_node_audit_filter_component,default_bucket=False,audit_id=36866
    """
    def test_toggling_node_audit_filter_component(self):

        self.log.info("Read configuration audit id")
        self.audit_id = self.input.param("audit_id")

        self.log.info("Disable audit logging for node configuration change")
        audit_obj = audit(host=self.cluster.master)
        audit_obj.setAuditFeatureDisabled(str(self.audit_id))

        self.log.info("Update configuration node parameters: storageBuffercacheSize")
        node_configuration_map = {"storageBuffercacheSize": 1}
        status, _, _ = self.cbas_util.update_node_parameter_configuration_on_cbas(node_configuration_map)
        self.assertTrue(status, msg="Incorrect status for node configuration PUT request")

        self.log.info("Validate audit logs are not generated for node configuration update")
        node_audit_obj = audit(eventID=self.audit_id, host=self.cbas_node)
        self.assertFalse(node_audit_obj.check_if_audit_event_generated(), msg="Audit event must not be generated")

        self.log.info("Enable audit logging for node configuration change")
        audit_obj.setAuditFeatureDisabled('')

        self.log.info("Update configuration node parameters: storageBuffercacheSize")
        status, _, _ = self.cbas_util.update_node_parameter_configuration_on_cbas(node_configuration_map)
        self.assertTrue(status, msg="Incorrect status for node configuration PUT request")

        self.log.info("Validate audit logs are generated for service configuration update")
        self.assertTrue(node_audit_obj.check_if_audit_event_generated(), msg="Audit event must be generated")

    """
    cbas.cbas_audit.CBASAuditLogs.test_no_audits_events_if_analytics_filter_component_is_disabled,default_bucket=False,service_audit_id=36865,node_audit_id=36866
    """
    def test_no_audits_events_if_analytics_filter_component_is_disabled(self):
        
        self.log.info("Read configuration audit ids")
        self.service_audit_id = self.input.param("service_audit_id")
        self.node_audit_id = self.input.param("node_audit_id")

        self.log.info("Disable audit logging for service & node configuration change")
        audit_obj = audit(host=self.cluster.master)
        audit_obj.setAuditFeatureDisabled(str(self.service_audit_id) + "," + str(self.node_audit_id))

        self.log.info("Update service configuration service parameter: logLevel")
        service_configuration_map = {"logLevel": "TRACE"}
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(service_configuration_map)
        self.assertTrue(status, msg="Incorrect status for service configuration PUT request")

        self.log.info("Update node configuration service parameters: storageBuffercacheSize")
        node_configuration_map = {"storageBuffercacheSize": 1}
        status, _, _ = self.cbas_util.update_node_parameter_configuration_on_cbas(node_configuration_map)
        self.assertTrue(status, msg="Incorrect status for node configuration PUT request")

        self.log.info("Validate audit logs are not generated for service configuration update")
        service_audit_obj = audit(eventID=self.service_audit_id, host=self.cbas_node)
        self.assertFalse(service_audit_obj.check_if_audit_event_generated(), msg="Audit event must not be generated")

        self.log.info("Validate audit logs are not generated for node configuration update")
        node_audit_obj = audit(eventID=self.node_audit_id, host=self.cbas_node)
        self.assertFalse(node_audit_obj.check_if_audit_event_generated(), msg="Audit event must not be generated")
    
    """
    cbas.cbas_audit.CBASAuditLogs.test_audit_logs_with_filtered_user_list,default_bucket=False,audit_id=36865
    """
    def test_audit_logs_with_filtered_user_list(self):

        self.log.info("Create a user with role cluster admin")
        rbac_util = RbacUtils(self.cluster.master)
        rbac_util._create_user_and_grant_role("cbas_admin", "cluster_admin")

        self.log.info("Read configuration audit ids")
        self.audit_id = self.input.param("audit_id")

        self.log.info("Disabled audit logs for user")
        audit_obj = audit(host=self.cluster.master)
        audit_obj.setWhiteListUsers("cbas_admin/local")

        self.log.info("Update service configuration service parameter: logLevel")
        service_configuration_map = {"logLevel": "TRACE"}
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(service_configuration_map, username="cbas_admin")
        self.assertTrue(status, msg="Incorrect status for service configuration PUT request")

        self.log.info("Verify audit logs are not generated as cbas_admin is whitelisted")
        server_audit_obj = audit(eventID=self.audit_id, host=self.cbas_node)
        self.assertFalse(server_audit_obj.check_if_audit_event_generated(), msg="Audit event must not be generated")

        self.log.info("Remove whitelabel user")
        audit_obj.setWhiteListUsers()

        self.log.info("Update service configuration service parameter: logLevel")
        service_configuration_map = {"logLevel": "TRACE"}
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(service_configuration_map, username="cbas_admin")
        self.assertTrue(status, msg="Incorrect status for service configuration PUT request")

        self.log.info("Verify audit logs are not generated as cbas_admin is whitelisted")
        server_audit_obj = audit(eventID=self.audit_id, host=self.cbas_node)
        self.assertTrue(server_audit_obj.check_if_audit_event_generated(), msg="Audit event must be generated")

    def tearDown(self):
        super(CBASAuditLogs, self).tearDown()
