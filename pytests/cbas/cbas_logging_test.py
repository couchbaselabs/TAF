'''
Created on 21-Mar-2018

@author: tanzeem
'''
import json
import time

from cbas.cbas_base import CBASBaseTest
from cbas.cbas_utils import cbas_utils
from basetestcase import RemoteMachineShellConnection
from node_utils.node_ready_functions import NodeHelper

class CbasLogging(CBASBaseTest):
    # Dictionary containing the default logging configuration that we set and verify if they are set
    DEFAULT_LOGGER_CONFIG_DICT = {}

    # Converts the logging configuration PUT response to a dictionary, name corresponds to logger and value maps to logging level
    @staticmethod
    def convert_logger_get_result_to_a_dict(response):
        logger_dict = {}
        loaded_json = json.loads(response)
        for _, v in loaded_json.items():
            for value in v:
                logger_dict[list(value.values())[1]] = (list(value.values())[0])
        return logger_dict

    def setUp(self):
        super(CbasLogging, self).setUp()

        # Dictionary containing the default logging configuration that we set and verify if they are set
        CbasLogging.DEFAULT_LOGGER_CONFIG_DICT = {"org.apache.asterix": "INFO",
                                                  "com.couchbase.client.dcp.conductor.DcpChannel": "WARN",
                                                  "com.couchbase.client.core.node": "WARN",
                                                  "com.couchbase.analytics": "DEBUG",
                                                  "org.apache.hyracks": "DEBUG",
                                                  "org.apache.asterix": "DEBUG",
                                                  "org.apache.hyracks.http.server.CLFLogger": "ACCESS",
                                                  "": "ERROR"} # Empty string corresponds to ROOT logger

        # Fetch the NC node ID and add trace logger to default logger config dictionary, trace logger has NodeId so this has to be picked at run time
        _, node_id, _ = self.cbas_util.retrieve_nodes_config()
        CbasLogging.DEFAULT_LOGGER_CONFIG_DICT["org.apache.hyracks.util.trace.Tracer@" + node_id] = "INFO"

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_logging_test.CbasLogging.test_get_cbas_default_logger_levels,default_bucket=False
    GET the default logging configuration that is set on every cbas node during bootstrapping and assert against the DEFAULT_LOGGER_CONFIG_DICT
    Default loggers are not finalized and test will fail on change of logging configuration 
    '''

    def test_get_cbas_default_logger_levels(self):
        self.log.info("Get the default analytics logging configurations")
        status, content, response = self.cbas_util.get_log_level_on_cbas()
        self.assertTrue(status, msg="Response status incorrect for GET request")

        self.log.info("Convert response to a dictionary")
        log_dict = CbasLogging.convert_logger_get_result_to_a_dict(content)
        self.assertEqual(len(log_dict), len(CbasLogging.DEFAULT_LOGGER_CONFIG_DICT), "Logger count incorrect")

        self.log.info("Verify logger configuration and count")
        for key in CbasLogging.DEFAULT_LOGGER_CONFIG_DICT.keys():
            self.assertEqual(log_dict[key], CbasLogging.DEFAULT_LOGGER_CONFIG_DICT[key],
                             "Incorrect value for key : " + key)

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_logging_test.CbasLogging.test_set_cbas_logger_levels,default_bucket=False
    Loggers are still not finalised, so in order to avoid all test failing due to change in logger and logging levels, we will delete loggers using delete all, then set logger and there levels to custom and then verify if they are set
    '''

    def test_set_cbas_logger_levels(self):

        self.log.info("Delete all loggers")
        self.cbas_util.delete_all_loggers_on_cbas()

        self.log.info("Set the logging level using json object from default logger config dictionary")
        status, content, response = self.cbas_util.set_log_level_on_cbas(CbasLogging.DEFAULT_LOGGER_CONFIG_DICT)
        self.assertTrue(status, msg="Response status incorrect for SET request")

        self.log.info("Verify logging configuration that we set above")
        for name, level in CbasLogging.DEFAULT_LOGGER_CONFIG_DICT.items():
            status, content, response = self.cbas_util.get_specific_cbas_log_level(name)
            self.assertTrue(status, msg="Response status incorrect for GET request")
            self.assertEquals(content, level, msg="Logger configuration mismatch for logger " + name)

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_logging_test.CbasLogging.test_set_specific_cbas_logger_level,default_bucket=False,logger_name=org.apache.asterix,logger_level=FATAL
    -i b/resources/4-nodes-template.ini -t cbas.cbas_logging_test.CbasLogging.test_set_specific_cbas_logger_level,default_bucket=False,logger_name=com.couchbase.client.dcp.conductor.DcpChannel,logger_level=ERROR
    -i b/resources/4-nodes-template.ini -t cbas.cbas_logging_test.CbasLogging.test_set_specific_cbas_logger_level,default_bucket=False,logger_name=com.couchbase.client.core.node,logger_level=WARN
    -i b/resources/4-nodes-template.ini -t cbas.cbas_logging_test.CbasLogging.test_set_specific_cbas_logger_level,default_bucket=False,logger_name=com.couchbase.analytics,logger_level=ALL
    -i b/resources/4-nodes-template.ini -t cbas.cbas_logging_test.CbasLogging.test_set_specific_cbas_logger_level,default_bucket=False,logger_name=org.apache.hyracks,logger_level=OFF
    -i b/resources/4-nodes-template.ini -t cbas.cbas_logging_test.CbasLogging.test_set_specific_cbas_logger_level,default_bucket=False,logger_name="",logger_level=INFO
    '''

    def test_set_specific_cbas_logger_level(self):

        self.log.info("Set log level")
        logger_level = self.input.param("logger_level", "FATAL")
        logger_name = self.input.param("logger_name", "org.apache.asterix")
        status, response, content = self.cbas_util.set_specific_log_level_on_cbas(logger_name, logger_level)
        self.assertTrue(status, msg="Status mismatch for SET")

        self.log.info("Get and verify the log level")
        status, content, response = self.cbas_util.get_specific_cbas_log_level(logger_name)
        self.assertTrue(status, msg="Status mismatch for GET")
        self.assertEquals(content, logger_level, msg="Logger configuration mismatch for " + logger_name)

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_logging_test.CbasLogging.test_logging_configurations_are_shared_across_cbas_node,default_bucket=False,add_all_cbas_nodes=True,logger_name=com.couchbase.client.dcp.conductor.DcpChannel,logger_level=ERROR
    '''

    def test_logging_configurations_are_shared_across_cbas_node(self):

        self.log.info("Add a cbas node")
        result = self.add_node(self.cbas_servers[0], services=["cbas"], rebalance=True)
        self.assertTrue(result, msg="Failed to add CBAS node")

        self.log.info("Delete all loggers")
        self.cbas_util.delete_all_loggers_on_cbas()

        self.log.info(
            "Set the logging level using json object from default logger config dictionary on master cbas node")
        status, content, response = self.cbas_util.set_log_level_on_cbas(CbasLogging.DEFAULT_LOGGER_CONFIG_DICT)
        self.assertTrue(status, msg="Response status incorrect for SET request")

        self.log.info("Verify logging configuration that we set on cbas Node")
        for name, level in CbasLogging.DEFAULT_LOGGER_CONFIG_DICT.items():
            status, content, response = self.cbas_util.get_specific_cbas_log_level(name)
            self.assertTrue(status, msg="Response status incorrect for GET request")
            self.assertEquals(content, level, msg="Logger configuration mismatch for logger " + name)

        self.sleep(timeout=10, message="Waiting for logger configuration to be copied across cbas nodes")

        self.log.info("Verify logging configuration on other cbas node")
        for name, level in CbasLogging.DEFAULT_LOGGER_CONFIG_DICT.items():
            status, content, response = cbas_utils(self.master, self.cbas_servers[0]).get_specific_cbas_log_level(name)
            self.assertTrue(status, msg="Response status incorrect for GET request")
            self.assertEquals(content, level, msg="Logger configuration mismatch for logger " + name)

        self.log.info("Update logging configuration on other cbas node")
        logger_level = self.input.param("logger_level", "FATAL")
        logger_name = self.input.param("logger_name", "org.apache.asterix")
        status, content, response = cbas_utils(self.master, self.cbas_servers[0]).set_specific_log_level_on_cbas(
            logger_name, logger_level)
        self.assertTrue(status, msg="Status mismatch for SET")

        self.sleep(timeout=10, message="Waiting for logger configuration to be copied across cbas nodes")

        self.log.info("Assert log level on master cbas node")
        status, content, response = self.cbas_util.get_specific_cbas_log_level(logger_name)
        self.assertTrue(status, msg="Status mismatch for GET")
        self.assertEquals(content, logger_level, msg="Logger configuration mismatch for " + logger_name)

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_logging_test.CbasLogging.test_setting_an_invalid_logging_level,default_bucket=False,logger_name=org.apache.asterix,logger_level=FAT
    '''

    def test_setting_an_invalid_logging_level(self):
        self.log.info("Set log level")
        logger_level = self.input.param("logger_level", "FAT")
        logger_name = self.input.param("logger_name", "org.apache.asterix")
        status, response, content = self.cbas_util.set_specific_log_level_on_cbas(logger_name, logger_level)
        self.assertEquals(content['status'], "400", msg="Incorrect status while setting unsupported logging level")
        self.assertFalse(status, msg="Status mismatch for GET")

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_logging_test.CbasLogging.test_deleting_specific_logger_falls_back_to_its_parent_logging_level,default_bucket=False
    '''

    def test_deleting_specific_logger_falls_back_to_its_parent_logging_level(self):

        self.log.info("Delete all loggers")
        self.cbas_util.delete_all_loggers_on_cbas()

        self.log.info(
            "Set the logging level using json object from default logger config dictionary on master cbas node")
        status, content, response = self.cbas_util.set_log_level_on_cbas(CbasLogging.DEFAULT_LOGGER_CONFIG_DICT)
        self.assertTrue(status, msg="Response status incorrect for SET request")

        self.log.info("Set log level of root to ERROR")
        logger_level = "FATAL"
        logger_name = ""
        status, content, response = self.cbas_util.set_specific_log_level_on_cbas(logger_name, logger_level)
        self.assertTrue(status, msg="Status mismatch for SET")

        self.log.info("Get log level of root")
        status, content, response = self.cbas_util.get_specific_cbas_log_level(logger_name)
        self.assertTrue(status, msg="Status mismatch for GET")
        self.assertEquals(content, logger_level, msg="Logger configuration mismatch for logger " + logger_name)

        self.log.info("Delete specific logger")
        logger_name = "com.couchbase.analytics"
        status, content, response = self.cbas_util.delete_specific_cbas_log_level(logger_name)
        self.assertTrue(status, msg="Status mismatch for DELETE")

        self.log.info("Verify log level is set to ROOT logger")
        status, content, response = self.cbas_util.get_specific_cbas_log_level(logger_name)
        self.assertTrue(status, msg="Status mismatch for GET")
        self.assertEquals(content, logger_level, msg="Logger configuration mismatch for logger " + logger_name)

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_logging_test.CbasLogging.test_logging_configurations_are_restored_post_service_restarts,default_bucket=False,add_all_cbas_nodes=True,
    logger_name_to_delete=com.couchbase.client.core.node,logger_name_to_update=org.apache.hyracks,logger_level_to_update=FATAL,logger_name_to_add=org.apache.hyracks123,logger_level_to_add=ALL,
    process_name=java,service_name=/opt/couchbase/lib/cbas/runtime/bin/java
    '''

    def test_logging_configurations_are_restored_post_service_restarts(self):

        self.log.info("Add a cbas node")
        result = self.add_node(self.cbas_servers[0], services=["cbas"], rebalance=True)
        self.assertTrue(result, msg="Failed to add CBAS node")

        self.log.info("Delete all loggers")
        self.cbas_util.delete_all_loggers_on_cbas()

        self.log.info("Set the logging level using the json object")
        status, content, response = self.cbas_util.set_log_level_on_cbas(CbasLogging.DEFAULT_LOGGER_CONFIG_DICT)
        self.assertTrue(status, msg="Response status incorrect for SET request")

        self.log.info("Delete specific logger")
        logger_name = self.input.param("logger_name_to_delete", "com.couchbase.client.core.node")
        status, content, response = self.cbas_util.delete_specific_cbas_log_level(logger_name)
        self.assertTrue(status, msg="Status mismatch for DELETE")
        del CbasLogging.DEFAULT_LOGGER_CONFIG_DICT[logger_name]

        self.log.info("Update specific logger")
        logger_name = self.input.param("logger_name_to_update", "org.apache.hyracks")
        logger_level_to_update = self.input.param("logger_level_to_update", "FATAL")
        status, response, content = self.cbas_util.set_specific_log_level_on_cbas(logger_name, logger_level_to_update)
        self.assertTrue(status, msg="Status mismatch for SET")
        CbasLogging.DEFAULT_LOGGER_CONFIG_DICT[logger_name] = logger_level_to_update

        self.log.info("Add a new logger")
        logger_name = self.input.param("logger_name_to_add", "org.apache.hyracks123")
        logger_level_to_add = self.input.param("logger_level_to_add", "ALL")
        status, response, content = self.cbas_util.set_specific_log_level_on_cbas(logger_name, logger_level_to_add)
        self.assertTrue(status, msg="Status mismatch for SET")
        CbasLogging.DEFAULT_LOGGER_CONFIG_DICT[logger_name] = logger_level_to_add

        self.log.info("Verify logging configuration that we set on cbas Node")
        for name, level in CbasLogging.DEFAULT_LOGGER_CONFIG_DICT.items():
            status, content, response = self.cbas_util.get_specific_cbas_log_level(name)
            self.assertTrue(status, msg="Response status incorrect for GET request")
            self.assertEquals(content, level, msg="Logger configuration mismatch for logger " + name)

        self.sleep(timeout=10, message="Waiting for logger configuration to be copied across cbas nodes")

        self.log.info("Verify logging configuration on other cbas node")
        for name, level in CbasLogging.DEFAULT_LOGGER_CONFIG_DICT.items():
            status, content, response = cbas_utils(self.master, self.cbas_servers[0]).get_specific_cbas_log_level(name)
            self.assertTrue(status, msg="Response status incorrect for GET request")
            self.assertEquals(content, level, msg="Logger configuration mismatch for logger " + name)

        self.log.info("Read input params")
        process_name = self.input.param('process_name', None)
        service_name = self.input.param('service_name', None)
        restart_couchbase = self.input.param('restart_couchbase', False)
        reboot = self.input.param('reboot', False)
        kill_services = self.input.param('kill_services', False)

        self.log.info("Establish a remote connection")
        shell_cc = RemoteMachineShellConnection(self.cbas_node)
        shell_nc = RemoteMachineShellConnection(self.cbas_servers[0])

        if kill_services:
            self.log.info("Kill the %s service on CC cbas node" % service_name)
            shell_cc.kill_process(process_name, service_name)

            self.log.info("Kill the %s service on other cbas node" % service_name)
            shell_nc.kill_process(process_name, service_name)

        if restart_couchbase:
            self.log.info("Restart couchbase service")
            status, _, _ = self.cbas_util.restart_analytics_cluster_uri()
            self.assertTrue(status, msg="Failed to restart cbas")

        if reboot:
            self.log.info("Reboot couchbase CC node")
            NodeHelper.reboot_server_new(self.cbas_node, self)

            self.log.info("Reboot couchbase NC node")
            NodeHelper.reboot_server_new(self.cbas_servers[0], self)

        self.log.info("Wait for request to complete and cluster to be active: Using private ping() function")
        cluster_recover_start_time = time.time()
        while time.time() < cluster_recover_start_time + 180:
            try:
                status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("set `import-private-functions` `true`;ping()")
                if status == "success":
                    break
            except:
                self.sleep(3, message="Wait for service to up")

        self.log.info("Verify logging configuration post service kill")
        for name, level in CbasLogging.DEFAULT_LOGGER_CONFIG_DICT.items():
            status, content, response = self.cbas_util.get_specific_cbas_log_level(name)
            self.assertTrue(status, msg="Response status incorrect for GET request")
            self.assertEquals(content, level, msg="Logger configuration mismatch for logger " + name)

        self.sleep(timeout=10, message="Waiting for logger configuration to be copied across cbas nodes")

        self.log.info("Verify logging configuration on other cbas node post service kill")
        for name, level in CbasLogging.DEFAULT_LOGGER_CONFIG_DICT.items():
            status, content, response = cbas_utils(self.master, self.cbas_servers[0]).get_specific_cbas_log_level(name)
            self.assertTrue(status, msg="Response status incorrect for GET request")
            self.assertEquals(content, level, msg="Logger configuration mismatch for logger " + name)

    def tearDown(self):
        super(CbasLogging, self).tearDown()
