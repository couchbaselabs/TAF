from datetime import datetime
import json
import os
import re
from pytests.basetestcase import ClusterSetup
from pytests.bucket_collections.collections_base import CollectionBase
from constants.platform_constants import os_constants
from remote.remote_util import RemoteMachineShellConnection
from security_utils.security_utils import SecurityUtils
from membase.api.rest_client import RestConnection
from Cb_constants import CbServer
from couchbase_utils.security_utils.x509_multiple_CA_util import x509main

class InternalUserPassword(ClusterSetup):
    def setUp(self):
        super(InternalUserPassword, self).setUp()
        self.security_util = SecurityUtils(self.log)
        self.spec_name = self.input.param("bucket_spec", "single_bucket.bucket_for_magma_collections")
        self.initial_data_spec = self.input.param("initial_data_spec", "initial_load")
        self.rebalance_type = self.input.param("rebalance_type", "rebalance_in")
        self.force_rotate = self.input.param("force_rotate", True)
        self.num_node_failures = self.input.param("num_node_failures", 1)
        self.servers_to_fail = self.cluster.servers[1:self.num_node_failures
                                                          + 1]
        self.current_fo_strategy = self.input.param("current_fo_strategy",
                                                    "auto")
        self.standard = self.input.param("standard", "pkcs8")
        self.passphrase_type = self.input.param("passphrase_type", "script")
        self.encryption_type = self.input.param("encryption_type", "aes256")
        self.x509 = x509main(host=self.cluster.master, standard=self.standard,
                             encryption_type=self.encryption_type,
                             passphrase_type=self.passphrase_type)
        self.rest = RestConnection(self.cluster.master)

        #Set passwword rotation interval
        if not self.force_rotate:
            result, content = self.security_util.set_internal_creds_rotation_interval(self.cluster, 60000)
            if not result:
                self.fail("Failed to set internal password rotation interval: {}".format(content))

        # Creating buckets from spec file
        CollectionBase.deploy_buckets_from_spec_file(self)

        # Create clients in SDK client pool
        CollectionBase.create_clients_for_sdk_pool(self)

        # Load initial async_write docs into the cluster
        self.log.info("Initial doc generation process starting...")
        CollectionBase.load_data_from_spec_file(self, self.initial_data_spec,
                                                validate_docs=False)

    def failover_task(self):
        rest_nodes = self.rest.get_nodes()
        self.log.info("servers to fail: {0}".format(self.servers_to_fail))
        if self.current_fo_strategy == CbServer.Failover.Type.GRACEFUL:
            self.rest.monitorRebalance()
            for node in self.servers_to_fail:
                node = [t_node for t_node in rest_nodes
                            if t_node.ip == node.ip][0]
                status = self.rest.fail_over(node.id, graceful=True)
                if status is False:
                    self.fail("Graceful failover failed for %s" % node)
                self.sleep(5, "Wait for failover to start")
        elif self.current_fo_strategy == CbServer.Failover.Type.FORCEFUL:
            self.rest.monitorRebalance()
            for node in self.servers_to_fail:
                node = [t_node for t_node in rest_nodes
                        if t_node.ip == node.ip][0]
                status = self.rest.fail_over(node.id, graceful=False)
                if status is False:
                    self.ail("Hard failover failed for %s" % node)
                self.sleep(5, "Wait for failover to start")

    def set_low_rotation_interval(self, interval):

        #enable diag/eval on non local host
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        erlang_cmd = 'ns_config:set(int_creds_protection_sleep, {}).'.format(interval)

        status, content = self.rest.diag_eval(erlang_cmd)

        self.log.info("Status, set low interval: {}".format(status))
        self.log.info("Content, set low interval: {}".format(content))

        if not status:
            self.fail("Failed to set int_creds_protection_sleep: {}".format(content))

        result, content = self.security_util.set_internal_creds_rotation_interval(self.cluster, interval)
        if not result:
            self.fail("Failed to set internal password rotation interval: {}".format(content))

    def validate_password_rotation(self, start_time, end_time):
        lib_cb = os_constants.Linux.COUCHBASE_LIB_PATH
        logs_dir = lib_cb + "logs/"

        def check_logs(grep_output_list):

            for line in grep_output_list:
                # eg: 2021-07-12T04:03:45
                timestamp_regex = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
                match_obj = timestamp_regex.search(line)
                if not match_obj:
                    self.log.critical("%s does not match any timestamp" % line)
                    return True
                timestamp = match_obj.group()
                timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")

                if timestamp >= start_time and timestamp <= end_time:
                    return True

            return False

        shell = RemoteMachineShellConnection(self.cluster.master)
        log_files = shell.execute_command(
                    "ls " + os.path.join(logs_dir, "info.log"))[0]

        log_file = log_files[0]
        log_file = log_file.strip("\n")
        grep_for_str = "Password rotation finished"
        cmd_to_run = "grep -r '%s' %s" \
                                    % (grep_for_str, log_file)
        grep_output = shell.execute_command(cmd_to_run)[0]
        return check_logs(grep_output)

    def get_current_time(self, server):
        shell = RemoteMachineShellConnection(server)
        timestamps = shell.execute_command('date +"%Y-%m-%dT%H:%M:%S"')
        timestamp = timestamps[0][0]
        timestamp = timestamp.strip("\n")
        current_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
        return current_time

    def wait_for_rebalance_to_start(self, rebalance_task):
        while rebalance_task.start_time == None:
            self.sleep(5, "Wait for rebalance to start")

    def test_password_rotation(self):
        start_time = self.get_current_time(self.cluster.master)
        self.log.info("Internal user password test case")
        result, content = self.security_util.rotate_password_for_internal_users(self.cluster)
        if not result:
            self.fail("Failed to rotate password: {}".format(content))
        self.log.info("Successfully rotated password: {}".format(content))
        result, content = self.security_util.set_internal_creds_rotation_interval(self.cluster, 600000)
        if not result:
            self.fail("Failed to set internal password rotation interval: {}".format(content))
        self.log.info("Successfully set internal password rotation interval: {}".format(content))
        result, content = self.security_util.get_internal_creds_rotation_interval(self.cluster)
        if not result:
            self.fail("Failed to get security settings: {}".format(content))
        self.log.info("Cred rotations interval: {}".format(content))
        self.sleep(30, "Wait for password rotation")
        end_time = self.get_current_time(self.cluster.master)
        self.log.info("Start time: {}".format(start_time))
        self.log.info("End time: {}".format(end_time))
        validate_result = self.validate_password_rotation(start_time, end_time)
        self.log.info("Validation result: {}".format(validate_result))
        if not validate_result:
            self.fail("Did not find any password rotation logs in the specified interval")


    def test_password_rotation_rebalance(self):

        self.set_low_rotation_interval(4000)

        rebalance_task = None
        if self.rebalance_type == "rebalance_in":
            nodes_in = self.cluster.servers[
                    self.nodes_init:self.nodes_init + self.nodes_in]
            rebalance_task = self.task.async_rebalance(self.cluster,
                                    nodes_in, [],
                                    retry_get_process_num=2000)

        elif self.rebalance_type == "swap_rebalance":
            nodes_in = self.cluster.servers[
                self.nodes_init:self.nodes_init + self.nodes_in]
            nodes_out = self.cluster.servers[
                1:self.nodes_out + 1]
            rebalance_task = self.task.async_rebalance(self.cluster,
                                    nodes_in, nodes_out,
                                    retry_get_process_num=2000)
        self.wait_for_rebalance_to_start(rebalance_task)
        # start_time = datetime.fromtimestamp(rebalance_task.start_time)
        self.sleep(5, "Wait after rebalance started")

        self.task_manager.get_task_result(rebalance_task)
        if not rebalance_task.result:
            self.fail("Failed to complete rebalance")

        #set the rotation interval back to default
        result, content = self.security_util.set_internal_creds_rotation_interval(self.cluster, 60000)
        if not result:
            self.fail("Failed to set internal password rotation interval: {}".format(content))
        self.sleep(10, "Wait after resetting rotation interval")

        start_time = self.get_current_time(self.cluster.master)
        self.sleep(60, "Wait after rebalance completes")
        if self.force_rotate:
            result, content = self.security_util.rotate_password_for_internal_users(self.cluster)
            if not result:
                self.fail("Failed to rotate password")
        self.sleep(30, "Wait after rotating password")
        end_time = self.get_current_time(self.cluster.master)
        self.log.info("Start time: {}".format(start_time))
        self.log.info("End time: {}".format(end_time))
        validate_result = self.validate_password_rotation(start_time, end_time)
        self.log.info("Validate result: {}".format(validate_result))
        if not validate_result:
            self.fail("Did not find any password rotation logs in the specified interval")

    def test_password_rotation_failover(self):
        failover_error = None
        start_time = self.get_current_time(self.cluster.master)
        try:
            self.failover_task()
        except Exception as e:
            failover_error = str(e)
            if failover_error:
                self.fail(failover_error)
        if self.force_rotate:
            result, content = self.security_util.rotate_password_for_internal_users(self.cluster)
            if not result:
                self.fail("Failed to rotate password: {}".format(content))
            self.log.info("Password rotated succesfully!")

        self.rest.monitorRebalance()
        end_time = self.get_current_time(self.cluster.master)
        validate_result = self.validate_password_rotation(start_time, end_time)
        self.log.info("Validate result: {}".format(validate_result))
        if not validate_result:
            self.fail("Did not find any password rotation logs in the specified interval")

    def test_password_rotation_node_down(self):
        #disable autofailover
        status = self.rest.update_autofailover_settings(
            False, 60)
        self.assertTrue(status, "Auto-failover disable failed")
        start_time = self.get_current_time(self.cluster.master)
        for server_to_fail in self.servers_to_fail:
            self.cluster_util.stop_server(self.cluster, server_to_fail)
        self.sleep(60, "keeping the failure")
        if self.force_rotate:
            result, content = self.security_util.rotate_password_for_internal_users(self.cluster)
            if not result:
                self.fail("Failed to rotate password: {}".format(content))
            self.log.info("Password rotated succesfully!")
        self.sleep(60, "Wait after for password rotation")
        end_time = self.get_current_time(self.cluster.master)
        validate_result = self.validate_password_rotation(start_time, end_time)
        self.log.info("Validate result: {}".format(validate_result))
        if not validate_result:
            self.fail("Did not find any password rotation logs in the specified interval")
        self.log.info("Starting the server")
        for server_to_fail in self.servers_to_fail:
            self.cluster_util.start_server(self.cluster,
                                           server_to_fail)
        self.sleep(60, "Wait after server is back up!")

        if self.force_rotate:
            result, content = self.security_util.rotate_password_for_internal_users(self.cluster)
            if not result:
                self.fail("Failed to rotate password: {}".format(content))
            self.log.info("Password rotated succesfully!")

    def test_password_rotation_certificates(self):
        start_time = self.get_current_time(self.cluster.master)
        self.x509.generate_multiple_x509_certs(servers=self.cluster.servers)
        self.log.info("Manifest #########\n {0}".format(json.dumps(self.x509.manifest, indent=4)))
        for server in self.cluster.servers:
            _ = self.x509.upload_root_certs(server)
        self.x509.upload_node_certs(servers=self.cluster.servers)
        if self.force_rotate:
            result, content = self.security_util.rotate_password_for_internal_users(self.cluster)
            if not result:
                self.fail("Failed to rotate password: {}".format(content))
            self.log.info("Password rotated succesfully!")
        self.sleep(120, "Wait for for password rotation")
        end_time = self.get_current_time(self.cluster.master)
        validate_result = self.validate_password_rotation(start_time, end_time)
        self.log.info("Validate result: {}".format(validate_result))
        if not validate_result:
            self.fail("Did not find any password rotation logs in the specified interval")
