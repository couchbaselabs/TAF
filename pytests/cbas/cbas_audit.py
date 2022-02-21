'''
Created on 30-Aug-2021

@author: couchbase
'''
import json

from cbas.cbas_base import CBASBaseTest
from security_utils.audit_ready_functions import audit
from rbac_utils.Rbac_ready_functions import RbacUtils
from TestInput import TestInputSingleton


class CBASAuditLogs(CBASBaseTest):

    actual_service_parameter_dict = {}
    actual_node_parameter_dict = {}
    expected_service_parameter_dict = {}
    expected_node_parameter_dict = {}

    def setUp(self):
        self.input = TestInputSingleton.input

        super(CBASAuditLogs, self).setUp()
        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]

        self.log.info("Enable audit on cluster")
        self.audit_obj = audit(host=self.cluster.master)
        current_state = self.audit_obj.getAuditStatus()
        if current_state:
            self.log.info(
                "Audit already enabled, disabling and re-enabling to remove previous settings")
            self.audit_obj.setAuditEnable('false')
        self.audit_obj.setAuditEnable('true')

        self.log.info("Build service configuration expected dictionary object")
        self.build_expected_service_parameter_dict()

        self.log.info("Build node configuration expected dictionary object")
        self.build_expected_node_parameter_dict()

        self.rbac_util = RbacUtils(self.cluster.master)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(CBASAuditLogs, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def build_expected_service_parameter_dict(self):
        self.log.info("Fetch configuration service parameters")
        status, content, _ = self.cbas_util.fetch_service_parameter_configuration_on_cbas(
            self.cluster)
        self.assertTrue(status, msg="Response status incorrect for GET request")

        self.log.info("Create server configuration expected dictionary")
        CBASAuditLogs.actual_service_parameter_dict = content
        for key in CBASAuditLogs.actual_service_parameter_dict:
            CBASAuditLogs.expected_service_parameter_dict[
                "config_before:" + key] = CBASAuditLogs.actual_service_parameter_dict[key]
            CBASAuditLogs.expected_service_parameter_dict[
                "config_after:" + key] = CBASAuditLogs.actual_service_parameter_dict[key]

    def build_expected_node_parameter_dict(self):
        self.log.info("Fetch configuration node parameters")
        status, content, _ = self.cbas_util.fetch_node_parameter_configuration_on_cbas(
            self.cluster)
        self.assertTrue(status, msg="Response status incorrect for GET request")

        self.log.info("Create node configuration expected dictionary")
        CBASAuditLogs.actual_node_parameter_dict = json.loads(content)
        for key in CBASAuditLogs.actual_node_parameter_dict:
            CBASAuditLogs.expected_node_parameter_dict[
                "config_before:" + key] = CBASAuditLogs.actual_node_parameter_dict[key]
            CBASAuditLogs.expected_node_parameter_dict[
                "config_after:" + key] = CBASAuditLogs.actual_node_parameter_dict[key]

    def validate_audit_event(self, event_id, host, expected_audit):
        auditing = audit(eventID=event_id, host=host)
        _, audit_match = auditing.validateEvents(expected_audit)
        self.assertTrue(
            audit_match,
            "Values for one of the fields mismatch, refer test logs for mismatch value")

    """
    cbas.cbas_audit.CBASAuditLogs.test_successful_service_configuration_updates_are_audited,default_bucket=False,audit_id=36865
    """
    def test_successful_service_configuration_updates_are_audited(self):

        self.log.info("Read audit input id")
        self.audit_id = self.input.param("audit_id")

        self.log.info(
            "Create a configuration map that will be passed as JSON body for service configuration")
        update_configuration_map = {}
        for key in CBASAuditLogs.actual_service_parameter_dict:
            if isinstance(CBASAuditLogs.actual_service_parameter_dict[key],
                          (int, long)) and CBASAuditLogs.actual_service_parameter_dict[key] != 1:
                update_configuration_map[key] = CBASAuditLogs.actual_service_parameter_dict[key] - 1

        self.log.info("Update service configuration")

        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(
            self.cluster, {'jobHistorySize' : 20})
        self.assertTrue(
            status, msg="Incorrect status for configuration service PUT request")

        self.log.info("Update expected dictionary")
        expected_dict = CBASAuditLogs.expected_service_parameter_dict

        expected_dict["config_after:jobHistorySize"] = 20

        self.log.info("Validate audit log for service configuration update")
        self.validate_audit_event(self.audit_id, self.cluster.cbas_cc_node, expected_dict)

    """
    cbas.cbas_audit.CBASAuditLogs.test_successful_node_configuration_updates_are_audited,default_bucket=False,audit_id=36866
    """
    def test_successful_node_configuration_updates_are_audited(self):

        self.log.info("Read audit input id")
        self.audit_id = self.input.param("audit_id")

        self.log.info(
            "Create a configuration map that will be passed as JSON body for node configuration")
        update_configuration_map = {}
        for key in CBASAuditLogs.actual_node_parameter_dict:
            if isinstance(CBASAuditLogs.actual_node_parameter_dict[key], (int, long)):
                update_configuration_map[key] = CBASAuditLogs.actual_node_parameter_dict[key] - 1

        self.log.info("Update node configuration")
        status, _, _ = self.cbas_util.update_node_parameter_configuration_on_cbas(
            self.cluster, update_configuration_map)
        self.assertTrue(
            status, msg="Incorrect status for configuration service PUT request")

        self.log.info("Update expected dictionary")
        expected_dict = CBASAuditLogs.expected_node_parameter_dict
        for key in update_configuration_map:
            expected_dict["config_after:" + key] = update_configuration_map[key]

        self.log.info("Validate audit log for service configuration update")
        self.validate_audit_event(self.audit_id, self.cluster.cbas_cc_node, expected_dict)

    """
    cbas.cbas_audit.CBASAuditLogs.test_unsuccessful_service_configuration_updates_are_not_audited,default_bucket=False,audit_id=36865
    """
    def test_unsuccessful_service_configuration_updates_are_not_audited(self):

        self.log.info("Read configuration audit id")
        self.audit_id = self.input.param("audit_id")

        self.log.info("Update configuration service parameters: logLevel with incorrect value")
        service_configuration_map = {"logLevel": "Invalid"}
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(
            self.cluster, service_configuration_map)
        self.assertFalse(
            status, msg="Incorrect status for service configuration PUT request")

        self.sleep(5, "Waiting for audit logs to be generated")
        self.log.info(
            "Verify audit log event is not generated since service configuration update failed")
        audit_obj = audit(eventID=self.audit_id, host=self.cluster.cbas_cc_node)
        self.assertFalse(audit_obj.check_if_audit_event_generated(),
                         msg="Audit event must not be generated")

    """
    cbas.cbas_audit.CBASAuditLogs.test_unsuccessful_node_configuration_updates_are_not_audited,default_bucket=False,audit_id=36866
    """
    def test_unsuccessful_node_configuration_updates_are_not_audited(self):

        self.log.info("Read configuration audit id")
        self.audit_id = self.input.param("audit_id")

        self.log.info("Update configuration service parameters: storageBuffercacheSize with incorrect value")
        node_configuration_map = {"storageBuffercacheSize": "bulk"}
        status, _, _ = self.cbas_util.update_node_parameter_configuration_on_cbas(
            self.cluster, node_configuration_map)
        self.assertFalse(
            status, msg="Incorrect status for node configuration PUT request")

        self.sleep(5, "Waiting for audit logs to be generated")
        self.log.info("Validate audit logs are not generated for unsuccessful node configuration update")
        audit_obj = audit(eventID=self.audit_id, host=self.cluster.cbas_cc_node)
        self.assertFalse(audit_obj.check_if_audit_event_generated(),
                         msg="Audit event must not be generated")

    """
    cbas.cbas_audit.CBASAuditLogs.test_toggling_service_audit_filter_component,default_bucket=False,audit_id=36865
    """
    def test_toggling_service_audit_filter_component(self):

        self.log.info("Read configuration audit id")
        self.audit_id = self.input.param("audit_id")

        self.log.info("Disable audit logging for service configuration change")
        self.audit_obj.setAuditFeatureDisabled(str(self.audit_id))

        self.log.info("Update configuration service parameters: logLevel")
        service_configuration_map = {"logLevel": "TRACE"}
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(
            self.cluster, service_configuration_map)
        self.assertTrue(
            status, msg="Incorrect status for service configuration PUT request")

        self.sleep(5, "Waiting for audit logs to be generated")
        self.log.info("Validate audit logs are not generated for service configuration update")
        service_audit_obj = audit(eventID=self.audit_id,
                                  host=self.cluster.cbas_cc_node)
        self.assertFalse(
            service_audit_obj.check_if_audit_event_generated(),
            msg="Audit event must not be generated")

        self.log.info("Enable audit logging for service configuration change")
        self.audit_obj.setAuditFeatureDisabled('')
        self.sleep(5, "Sleeping after enabling audit for configuration")

        self.log.info("Update configuration service parameters: logLevel")
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(
            self.cluster, service_configuration_map)
        self.assertTrue(
            status, msg="Incorrect status for service configuration PUT request")

        self.sleep(5, "Waiting for audit logs to be generated")
        self.log.info("Validate audit logs are generated for service configuration update")
        self.assertTrue(service_audit_obj.check_if_audit_event_generated(),
                        msg="Audit event must be generated")

    """
    cbas.cbas_audit.CBASAuditLogs.test_toggling_node_audit_filter_component,default_bucket=False,audit_id=36866
    """
    def test_toggling_node_audit_filter_component(self):

        self.log.info("Read configuration audit id")
        self.audit_id = self.input.param("audit_id")

        self.log.info("Disable audit logging for node configuration change")
        self.audit_obj.setAuditFeatureDisabled(str(self.audit_id))

        self.log.info("Update configuration node parameters: storageBuffercacheSize")
        node_configuration_map = {"storageBuffercacheSize": 1}
        status, _, _ = self.cbas_util.update_node_parameter_configuration_on_cbas(
            self.cluster, node_configuration_map)
        self.assertTrue(
            status, msg="Incorrect status for node configuration PUT request")

        self.sleep(5, "Waiting for audit logs to be generated")
        self.log.info("Validate audit logs are not generated for node configuration update")
        node_audit_obj = audit(eventID=self.audit_id,
                               host=self.cluster.cbas_cc_node)
        self.assertFalse(node_audit_obj.check_if_audit_event_generated(),
                         msg="Audit event must not be generated")

        self.log.info("Enable audit logging for node configuration change")
        self.audit_obj.setAuditFeatureDisabled('')
        self.sleep(5, "Sleeping after enabling audit for configuration")

        self.log.info("Update configuration node parameters: storageBuffercacheSize")
        status, _, _ = self.cbas_util.update_node_parameter_configuration_on_cbas(
            self.cluster, node_configuration_map)
        self.assertTrue(
            status, msg="Incorrect status for node configuration PUT request")

        self.sleep(5, "Waiting for audit logs to be generated")
        self.log.info("Validate audit logs are generated for service configuration update")
        self.assertTrue(node_audit_obj.check_if_audit_event_generated(),
                        msg="Audit event must be generated")

    """
    cbas.cbas_audit.CBASAuditLogs.test_no_audits_events_if_analytics_filter_component_is_disabled,default_bucket=False,service_audit_id=36865,node_audit_id=36866
    """
    def test_no_audits_events_if_analytics_filter_component_is_disabled(self):

        self.log.info("Read configuration audit ids")
        self.service_audit_id = self.input.param("service_audit_id")
        self.node_audit_id = self.input.param("node_audit_id")

        self.log.info("Disable audit logging for service & node configuration change")
        self.audit_obj.setAuditFeatureDisabled(
            str(self.service_audit_id) + "," + str(self.node_audit_id))

        self.log.info("Update service configuration service parameter: logLevel")
        service_configuration_map = {"logLevel": "TRACE"}
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(
            self.cluster, service_configuration_map)
        self.assertTrue(
            status, msg="Incorrect status for service configuration PUT request")

        self.log.info("Update node configuration service parameters: storageBuffercacheSize")
        node_configuration_map = {"storageBuffercacheSize": 1}
        status, _, _ = self.cbas_util.update_node_parameter_configuration_on_cbas(
            self.cluster, node_configuration_map)
        self.assertTrue(
            status, msg="Incorrect status for node configuration PUT request")

        self.sleep(5, "Waiting for audit logs to be generated")
        self.log.info("Validate audit logs are not generated for service configuration update")
        service_audit_obj = audit(eventID=self.service_audit_id,
                                  host=self.cluster.cbas_cc_node)
        self.assertFalse(service_audit_obj.check_if_audit_event_generated(),
                         msg="Audit event must not be generated")

        self.sleep(5, "Waiting for audit logs to be generated")
        self.log.info("Validate audit logs are not generated for node configuration update")
        node_audit_obj = audit(eventID=self.node_audit_id,
                               host=self.cluster.cbas_cc_node)
        self.assertFalse(node_audit_obj.check_if_audit_event_generated(),
                         msg="Audit event must not be generated")

    """
    cbas.cbas_audit.CBASAuditLogs.test_audit_logs_with_filtered_user_list,default_bucket=False,audit_id=36865
    """
    def test_audit_logs_with_filtered_user_list(self):

        self.log.info("Create a user with role Analytics admin")
        self.rbac_util._create_user_and_grant_role("cbas_admin", "analytics_admin")

        self.log.info("Read configuration audit ids")
        self.audit_id = self.input.param("audit_id")

        self.log.info("Disabled audit logs for user")
        self.audit_obj.setWhiteListUsers("cbas_admin/local")

        self.log.info("Update service configuration service parameter: logLevel")
        service_configuration_map = {"logLevel": "TRACE"}
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(
            self.cluster, service_configuration_map, username="cbas_admin")
        self.assertTrue(
            status, msg="Incorrect status for service configuration PUT request")

        self.sleep(5, "Waiting for audit logs to be generated")
        self.log.info("Verify audit logs are not generated as user is whitelisted")
        server_audit_obj = audit(eventID=self.audit_id,
                                 host=self.cluster.cbas_cc_node)
        self.assertFalse(server_audit_obj.check_if_audit_event_generated(),
                         msg="Audit event must not be generated")

        self.log.info("Remove whitelabel user")
        self.audit_obj.setWhiteListUsers()
        self.sleep(5, "Removing whitelisted users")

        self.log.info("Update service configuration service parameter: logLevel")
        service_configuration_map = {"logLevel": "TRACE"}
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(
            self.cluster, service_configuration_map, username="cbas_admin")
        self.assertTrue(status,
                        msg="Incorrect status for service configuration PUT request")

        self.sleep(5, "Waiting for audit logs to be generated")
        self.log.info("Verify audit logs are generated as the user is removed from whitelist")
        server_audit_obj = audit(eventID=self.audit_id,
                                 host=self.cluster.cbas_cc_node)
        self.assertTrue(server_audit_obj.check_if_audit_event_generated(),
                        msg="Audit event must be generated")

    def generate_audit_event(self, query_type, username, password, event_type=None):
        expected_audit_log = dict()
        if query_type == "select" or query_type == "ddl":
            self.cbas_util.create_dataset_obj(
                self.cluster, self.bucket_util, dataset_cardinality=3,
                bucket_cardinality=3, enabled_from_KV=False,
                for_all_kv_entities=False, remote_dataset=False, link=None,
                same_dv_for_link_and_dataset=False, name_length=30,
                fixed_length=False, exclude_bucket=[], exclude_scope=[],
                exclude_collection=[], no_of_objs=1)
            dataset = self.cbas_util.list_all_dataset_objs()[0]
            if query_type == "select":
                if not self.cbas_util.create_dataset(
                    self.cluster, dataset.name, dataset.full_kv_entity_name,
                    dataverse_name=dataset.dataverse_name):
                    self.fail("Error while creating dataset")
                query = "select count(*) from {0}".format(dataset.full_name)
                status, metrics, errors, results, handle = self.cbas_util.execute_statement_on_cbas_util(
                    self.cluster, query, username=username, password=password)
                if status == "success" and event_type:
                    self.log.error("Query execution should have failed")
                    return expected_audit_log
                expected_audit_log = {
                    "description":"A N1QL SELECT statement was executed",
                    "id":36867, "isAdHoc":True,
                    "local":{"ip":self.cluster.cbas_cc_node.ip,"port":8095},
                    "name":"SELECT statement"}
                if event_type == "forbidden_access":
                    expected_audit_log["errors"] = [{
                        "code":20001,
                        "msg":"User must have permission (cluster.collection[.:.:.].analytics!select)"}]
                    expected_audit_log["real_userid"] = {"domain":"local","user":username}
                    expected_audit_log["status"] = "errors"
                elif event_type == "unauthorised_access":
                    expected_audit_log["errors"] = [{"code":20000,"msg":"Unauthorized user."}]
                    expected_audit_log["real_userid"] = {"domain":"anonymous","user":""}
                    expected_audit_log["status"] = "errors"
                else:
                    expected_audit_log["real_userid"] = {"domain":"admin","user":"Administrator"}
                    expected_audit_log["status"] = "success"
                return expected_audit_log
            else:
                if not self.cbas_util.create_dataverse(
                    self.cluster, dataset.dataverse_name, if_not_exists=True):
                    self.fail("Error while creating dataverse")
                if self.cbas_util.create_dataset(
                    self.cluster, dataset.full_name, dataset.full_kv_entity_name,
                    username=username, password=password) and event_type:
                    self.log.error("Dataset creation should have failed")
                    return expected_audit_log
                expected_audit_log = {
                    "description":"A N1QL CREATE DATASET statement was executed",
                    "id":36870, "isAdHoc":True,
                    "local":{"ip":self.cluster.cbas_cc_node.ip,"port":8095},
                    "name":"CREATE DATASET statement"}
                if event_type == "forbidden_access":
                    expected_audit_log["errors"] = [{
                        "code":20001,
                        "msg":"User must have permission (cluster.collection[.:.:.].analytics!select)"}]
                    expected_audit_log["real_userid"] = {"domain":"local","user":username}
                    expected_audit_log["status"] = "errors"
                elif event_type == "unauthorised_access":
                    expected_audit_log["errors"] = [{"code":20000,"msg":"Unauthorized user."}]
                    expected_audit_log["real_userid"] = {"domain":"anonymous","user":""}
                    expected_audit_log["status"] = "errors"
                else:
                    expected_audit_log["real_userid"] = {"domain":"admin","user":"Administrator"}
                    expected_audit_log["status"] = "success"
                return expected_audit_log
        elif query_type == "dml":
            if self.cbas_util.disconnect_link(
                self.cluster, "Default.Local", username=username,
                password=password) and event_type:
                self.log.error("Disconnecting link should have failed")
                return expected_audit_log
            expected_audit_log = {
                    "description":"A N1QL DISCONNECT LINK statement was executed",
                    "id":36878, "isAdHoc":True,
                    "local":{"ip":self.cluster.cbas_cc_node.ip,"port":8095},
                    "name":"DISCONNECT LINK statement",
                    "statement":"disconnect link Default.Local;"}
            if event_type == "forbidden_access":
                expected_audit_log["errors"] = [{
                    "code":20001,
                    "msg":"User must have permission (cluster.collection[.:.:.].analytics!select)"}]
                expected_audit_log["real_userid"] = {"domain":"local","user":username}
                expected_audit_log["status"] = "errors"
            elif event_type == "unauthorised_access":
                expected_audit_log["errors"] = [{"code":20000,"msg":"Unauthorized user."}]
                expected_audit_log["real_userid"] = {"domain":"anonymous","user":""}
                expected_audit_log["status"] = "errors"
            else:
                expected_audit_log["real_userid"] = {"domain":"admin","user":"Administrator"}
                expected_audit_log["status"] = "success"
            return expected_audit_log

    def test_audit_of_forbidden_access_denied_events_for_select_statement(self):
        username = "test_user"
        self.rbac_util._create_user_and_grant_role(username, "cluster_admin")
        expected_audit_log = self.generate_audit_event(
            "select", username, self.cluster.password, "forbidden_access")
        if not expected_audit_log:
            self.fail("Audit event was not generated")
        self.sleep(5, "Waiting for audit logs to be generated")
        audit_obj = audit(eventID=expected_audit_log["id"],
                          host=self.cluster.cbas_cc_node)
        data = audit_obj.returnEvent(expected_audit_log["id"])
        if not audit_obj.validateData(data, expected_audit_log):
            self.fail("Audit event generated does not match the expected audit data")

    def test_audit_of_forbidden_access_denied_events_for_ddl_statement(self):
        username = "test_user"
        self.rbac_util._create_user_and_grant_role(username, "cluster_admin")
        expected_audit_log = self.generate_audit_event(
            "ddl", username, self.cluster.password, "forbidden_access")
        if not expected_audit_log:
            self.fail("Audit event was not generated")
        self.sleep(5, "Waiting for audit logs to be generated")
        audit_obj = audit(eventID=expected_audit_log["id"],
                          host=self.cluster.cbas_cc_node)
        data = audit_obj.returnEvent(expected_audit_log["id"])
        if not audit_obj.validateData(data, expected_audit_log):
            self.fail("Audit event generated does not match the expected audit data")

    def test_audit_of_forbidden_access_denied_events_for_dml_statement(self):
        username = "test_user"
        self.rbac_util._create_user_and_grant_role(username, "cluster_admin")
        expected_audit_log = self.generate_audit_event(
            "dml", username, self.cluster.password, "forbidden_access")
        if not expected_audit_log:
            self.fail("Audit event was not generated")
        self.sleep(5, "Waiting for audit logs to be generated")
        audit_obj = audit(eventID=expected_audit_log["id"],
                          host=self.cluster.cbas_cc_node)
        data = audit_obj.returnEvent(expected_audit_log["id"])
        if not audit_obj.validateData(data, expected_audit_log):
            self.fail("Audit event generated does not match the expected audit data")

    def test_audit_of_unauthorised_access_denied_events_for_select_statement(self):
        expected_audit_log = self.generate_audit_event(
            "select", self.cluster.username, "passwor", "unauthorised_access")
        if not expected_audit_log:
            self.fail("Audit event was not generated")
        self.sleep(5, "Waiting for audit logs to be generated")
        audit_obj = audit(eventID=expected_audit_log["id"],
                          host=self.cluster.cbas_cc_node)
        data = audit_obj.returnEvent(expected_audit_log["id"])
        if not audit_obj.validateData(data, expected_audit_log):
            self.fail("Audit event generated does not match the expected audit data")

    def test_audit_of_unauthorised_access_denied_events_for_ddl_statement(self):
        expected_audit_log = self.generate_audit_event(
            "ddl", self.cluster.username, "passwor", "unauthorised_access")
        if not expected_audit_log:
            self.fail("Audit event was not generated")
        self.sleep(5, "Waiting for audit logs to be generated")
        audit_obj = audit(eventID=expected_audit_log["id"],
                          host=self.cluster.cbas_cc_node)
        data = audit_obj.returnEvent(expected_audit_log["id"])
        if not audit_obj.validateData(data, expected_audit_log):
            self.fail("Audit event generated does not match the expected audit data")

    def test_audit_of_unauthorised_access_denied_events_for_dml_statement(self):
        expected_audit_log = self.generate_audit_event(
            "dml", self.cluster.username, "passwor", "unauthorised_access")
        if not expected_audit_log:
            self.fail("Audit event was not generated")
        self.sleep(5, "Waiting for audit logs to be generated")
        audit_obj = audit(eventID=expected_audit_log["id"],
                          host=self.cluster.cbas_cc_node)
        data = audit_obj.returnEvent(expected_audit_log["id"])
        if not audit_obj.validateData(data, expected_audit_log):
            self.fail("Audit event generated does not match the expected audit data")

    def test_audit_of_successful_events_for_select_statement(self):
        expected_audit_log = self.generate_audit_event(
            "select", self.cluster.username, self.cluster.password)
        if not expected_audit_log:
            self.fail("Audit event was not generated")
        self.sleep(5, "Waiting for audit logs to be generated")
        audit_obj = audit(eventID=expected_audit_log["id"],
                          host=self.cluster.cbas_cc_node)
        data = audit_obj.returnEvent(expected_audit_log["id"])
        if not audit_obj.validateData(data, expected_audit_log):
            self.fail("Audit event generated does not match the expected audit data")

    def test_audit_of_successful_events_for_ddl_statement(self):
        expected_audit_log = self.generate_audit_event(
            "ddl", self.cluster.username, self.cluster.password)
        if not expected_audit_log:
            self.fail("Audit event was not generated")
        self.sleep(5, "Waiting for audit logs to be generated")
        audit_obj = audit(eventID=expected_audit_log["id"],
                          host=self.cluster.cbas_cc_node)
        data = audit_obj.returnEvent(expected_audit_log["id"])
        if not audit_obj.validateData(data, expected_audit_log):
            self.fail("Audit event generated does not match the expected audit data")

    def test_audit_of_successful_events_for_dml_statement(self):
        expected_audit_log = self.generate_audit_event(
            "dml", self.cluster.username, self.cluster.password)
        if not expected_audit_log:
            self.fail("Audit event was not generated")
        self.sleep(5, "Waiting for audit logs to be generated")
        audit_obj = audit(eventID=expected_audit_log["id"],
                          host=self.cluster.cbas_cc_node)
        data = audit_obj.returnEvent(expected_audit_log["id"])
        if not audit_obj.validateData(data, expected_audit_log):
            self.fail("Audit event generated does not match the expected audit data")
