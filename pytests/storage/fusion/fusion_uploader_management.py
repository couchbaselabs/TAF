import ast
from copy import deepcopy
import json
import os
import random
import subprocess
import threading
import time
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from rebalance_utils.rebalance_util import RebalanceUtil
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest
from shell_util.remote_connection import RemoteMachineShellConnection
from cb_constants import CbServer


class FusionUploader(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionUploader, self).setUp()

        self.log.info("FusionUploader setUp Started")

        self.rebalance_master = self.input.param("rebalance_master", False)
        self.nodes_with_blocked_chronicle = list()

        self.upsert_iterations = self.input.param("upsert_iterations", 2)

    def tearDown(self):
        self.cleanup_iptables_rules()
        super(FusionUploader, self).tearDown()


    def test_fusion_term_number_increment(self):

        self.vb_term_number_dict = dict()
        for bucket in self.cluster.buckets:
            self.vb_term_number_dict[bucket.name] = dict()
            for vb_no in range(bucket.numVBuckets):
                self.vb_term_number_dict[bucket.name][vb_no] = 1

        self.log.info(f"VB term number dict = {self.vb_term_number_dict}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Perform a bunch of rebalances
        rebalance_types = ["rebalance_in", "rebalance_in", "swap_rebalance",
                           "rebalance_out", "rebalance_out"]
        rebalance_counter = 1

        for rebalance_type in rebalance_types:

            self.get_fusion_uploader_info()
            self.uploader_map1 = deepcopy(self.fusion_vb_uploader_map)
            self.log.info(f"Uploader map before rebalance: {self.uploader_map1}")

            self.num_nodes_to_rebalance_in = 0
            self.num_nodes_to_rebalance_out = 0
            self.num_nodes_to_swap_rebalance = 0

            if rebalance_type == "rebalance_in":
                self.num_nodes_to_rebalance_in = 1
            elif rebalance_type == "rebalance_out":
                self.num_nodes_to_rebalance_out = 1
            elif rebalance_type == "swap_rebalance":
                self.num_nodes_to_swap_rebalance = 1

            self.log.info(f"Running a Fusion rebalance of type: {rebalance_type}")
            nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                  rebalance_count=rebalance_counter,
                                                  rebalance_master=self.rebalance_master)
            self.sleep(30, "Wait after rebalance")

            self.cluster_util.print_cluster_stats(self.cluster)

            extent_migration_array = list()
            self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
            for node in nodes_to_monitor:
                for bucket in self.cluster.buckets:
                    extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                    extent_th.start()
                    extent_migration_array.append(extent_th)

            for th in extent_migration_array:
                th.join()

            self.log_store_rebalance_cleanup(nodes=nodes_to_monitor)

            self.parse_reb_plan_dict(rebalance_counter)

            self.get_fusion_uploader_info()
            self.uploader_map2 = deepcopy(self.fusion_vb_uploader_map)
            self.log.info(f"Uploader map after rebalance: {self.uploader_map2}")

            self.validate_term_number(self.uploader_map1, self.uploader_map2)

            rebalance_counter += 1


    def test_fusion_deletion_of_stale_logs(self):

        self.rebalance_num_docs = self.input.param("rebalance_num_docs", 2500000)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        rebalance_counter = 1

        # Perform a data workload in parallel when rebalance is taking place
        self.reset_doc_params(doc_ops="create")
        self.create_start = self.num_items
        self.create_end = self.create_start + self.rebalance_num_docs
        self.num_items += self.rebalance_num_docs
        self.log.info("Performing data load during rebalance")
        self.log.info(f"Create start = {self.create_start}, Create End = {self.create_end}")
        doc_loading_tasks, _ = self.java_doc_loader(wait=False,
                                                 skip_default=self.skip_load_to_default_collection,
                                                 ops_rate=10000, doc_ops="create",
                                                 monitor_ops=False)

        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=rebalance_counter,
                                              rebalance_sleep_time=300)
        self.sleep(30, "Wait after rebalance")

        self.cluster_util.print_cluster_stats(self.cluster)

        # Wait for doc load to complete
        for task in doc_loading_tasks:
            self.doc_loading_tm.get_task_result(task)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.log_store_rebalance_cleanup(nodes=nodes_to_monitor)

        self.parse_reb_plan_dict(rebalance_counter)

        self.validate_stale_log_deletion()


    def test_fusion_update_broken_uploaders(self):

        janitor_interval = self.input.param("janitor_interval", 900000)
        stop_uploader_during = self.input.param("stop_uploader_during", "sync") # [sync, rebalance, failover]
        failover_type = self.input.param("failover_type", "graceful") # [graceful, hard]
        post_failover_step = self.input.param("post_failover_step", "recovery") # [recovery, rebalance_out]
        recovery_type = self.input.param("recovery_type", "full") # [full, delta]

        janitor_interval_in_seconds = janitor_interval // 1000

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Get Uploader Map
        self.get_fusion_uploader_info()

        # Set Janitor Interval to a higher value (300 seconds)
        # "'ns_config:set({ns_orchestrator, janitor_interval}, 300000)'"
        for server in self.cluster.nodes_in_cluster:
            status, content = ClusterRestAPI(server).ns_config_set(key="{ns_orchestrator, janitor_interval}", value=janitor_interval)
            self.log.info(f"Server: {server.ip}, Status: {status}, Content: {content}")

        self.sleep(120, "Sleep after updating janitor interval")

        # For every bucket, pick a few VBs from every node to stop uploaders on
        for bucket in self.cluster.buckets:
            vbuckets = list()
            start_vb = 0
            for node in list(self.fusion_uploader_dict[bucket.name].keys()):
                end_vb = start_vb + self.fusion_uploader_dict[bucket.name][node]
                node_vbs = random.sample(range(start_vb, end_vb), (end_vb - start_vb) // 2)
                vbuckets.extend(node_vbs)
                start_vb = end_vb

            vbuckets_str = "[" + ",".join(str(n) for n in vbuckets) + "]"
            self.log.info(f"Bucket: {bucket.name}, Vbuckets to stop uploaders on = {vbuckets_str}")

            # Stop Uploaders on some VBs
            stop_uploader_cmd = f'ns_memcached:maybe_stop_fusion_uploaders("{bucket.name}", {vbuckets_str}).'
            for server in self.cluster.nodes_in_cluster:
                status, content = ClusterRestAPI(server).diag_eval(code=stop_uploader_cmd)
                self.log.info(f"Server: {server.ip}, CMD: {stop_uploader_cmd}, Status: {status}, Content: {content}")

        stop_upload_time = time.time()
        self.sleep(30, "Sleep after stopping uploaders on some VBs")

        # Start DU tracking
        du_th_array = list()
        for bucket in self.cluster.buckets:
            for vb in vbuckets:
                log_store_vb_path = os.path.join(self.nfs_server_path, "kv", bucket.uuid, f"kvstore-{str(vb)}")
                du_th = threading.Thread(target=self.monitor_disk_usage, args=[log_store_vb_path])
                du_th_array.append(du_th)
                du_th.start()

        # Get Uploader Map
        self.get_fusion_uploader_info()

        # Start a data load
        self.perform_workload(start=self.num_items, end=self.num_items * 2, ops_rate=20000)

        if stop_uploader_during == "sync":
            if int(time.time() - stop_upload_time) < janitor_interval_in_seconds:
                self.sleep(janitor_interval_in_seconds - int(time.time() - stop_upload_time) + 60, "Wait for Janitor to start uploaders again")
            self.sleep(300, "Wait for uploaders to catch up")
            # Get Uploader Map after Rebalance
            self.get_fusion_uploader_info()

        self.sleep(60, "Wait before performing rebalance/failover")

        if stop_uploader_during == "failover":

            # Perform failover
            candidates = [server for server in self.cluster.nodes_in_cluster if server.ip != self.cluster.master.ip]
            node_to_failover = candidates[0] if candidates else None
            if not node_to_failover:
                self.fail("No node can be failed over")
            self.log.info(f"Node to failover: {node_to_failover.ip}")

            rest = ClusterRestAPI(self.cluster.master)
            otp_node = self.get_otp_node(self.cluster.master, node_to_failover)

            if failover_type == "graceful":
                self.log.info(f"Gracefully Failing over the node {otp_node.id}")
                success, _ = rest.perform_graceful_failover(otp_node.id)
                self.assertTrue(success, "Failover unsuccessful")
                # Monitor failover rebalance
                rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
                self.assertTrue(rebalance_passed, "Graceful failover rebalance failed")

            elif failover_type == "hard":
                self.log.info(f"Hard Failover of node {otp_node.id}")
                success, _ = rest.perform_hard_failover(otp_node.id)
                self.assertTrue(success, "Failover unsuccessful")

            self.sleep(60, "Wait before recovering/rebalancing-out the node")
            if post_failover_step == "recovery":
                rest.set_failover_recovery_type(otp_node.id, recovery_type)
                self.sleep(15, "Wait after setting failover recovery type")

            elif post_failover_step == "rebalance_out":
                self.num_nodes_to_rebalance_out = 1

        # Perform a Fusion Rebalance
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_sleep_time=60)
        self.sleep(30, "Wait after rebalance")
        self.cluster_util.print_cluster_stats(self.cluster)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.monitor_du = False
        for th in du_th_array:
            th.join()

        # Get Uploader Map after Rebalance
        self.get_fusion_uploader_info()


    def test_fusion_stop_disable_while_uploaders_are_broken(self):

        stop_uploader_post_action = self.input.param("stop_uploader_post_action", "stop") # stop/disable

        janitor_interval = self.input.param("janitor_interval", 900000)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Get Uploader Map
        self.get_fusion_uploader_info()

        # Set Janitor Interval to a higher value (300 seconds)
        # "'ns_config:set({ns_orchestrator, janitor_interval}, 300000)'"
        for server in self.cluster.nodes_in_cluster:
            status, content = ClusterRestAPI(server).ns_config_set(key="{ns_orchestrator, janitor_interval}", value=janitor_interval)
            self.log.info(f"Server: {server.ip}, Status: {status}, Content: {content}")

        self.sleep(120, "Sleep after updating janitor interval")

        # For every bucket, pick a few VBs from every node to stop uploaders on
        for bucket in self.cluster.buckets:
            vbuckets = list()
            start_vb = 0
            for node in list(self.fusion_uploader_dict[bucket.name].keys()):
                end_vb = start_vb + self.fusion_uploader_dict[bucket.name][node]
                node_vbs = random.sample(range(start_vb, end_vb), (end_vb - start_vb) // 2)
                vbuckets.extend(node_vbs)
                start_vb = end_vb

            vbuckets_str = "[" + ",".join(str(n) for n in vbuckets) + "]"
            self.log.info(f"Bucket: {bucket.name}, Vbuckets to stop uploaders on = {vbuckets_str}")

            # Stop Uploaders on some VBs
            stop_uploader_cmd = f'ns_memcached:maybe_stop_fusion_uploaders("{bucket.name}", {vbuckets_str}).'
            for server in self.cluster.nodes_in_cluster:
                status, content = ClusterRestAPI(server).diag_eval(code=stop_uploader_cmd)
                self.log.info(f"Server: {server.ip}, CMD: {stop_uploader_cmd}, Status: {status}, Content: {content}")

        self.sleep(30, "Sleep after stopping uploaders on some VBs")

        # Start DU tracking
        du_th_array = list()
        for bucket in self.cluster.buckets:
            for vb in vbuckets:
                log_store_vb_path = os.path.join(self.nfs_server_path, "kv", bucket.uuid, f"kvstore-{str(vb)}")
                du_th = threading.Thread(target=self.monitor_disk_usage, args=[log_store_vb_path])
                du_th_array.append(du_th)
                du_th.start()

        # Get Uploader Map
        self.get_fusion_uploader_info()

        # Start a data load
        self.perform_workload(start=self.num_items, end=self.num_items * 2, ops_rate=20000)

        self.sleep(60, "Wait before stopping/disabling Fusion")

        if stop_uploader_post_action == "stop":
            self.stop_fusion()
        elif stop_uploader_post_action == "disable":
            self.disable_fusion()

        self.sleep(120, "Wait after stopping/disabling Fusion")

        self.log.info("Enabling Fusion again")
        self.enable_fusion()

        # Get Uploader Map
        self.get_fusion_uploader_info()

        self.sleep(120, "Wait after enabling Fusion")

        # Perform a Fusion Rebalance
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_sleep_time=60)
        self.sleep(30, "Wait after rebalance")
        self.cluster_util.print_cluster_stats(self.cluster)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.monitor_du = False
        for th in du_th_array:
            th.join()

        # Get Uploader Map after Rebalance
        self.get_fusion_uploader_info()


    def test_fusion_uploader_failover(self):

        post_failover_step = self.input.param("post_failover_step", "recovery") # recovery/rebalance-out
        recovery_type = self.input.param("recovery_type", "delta") # full/delta

        # With/without failover data load
        failover_data_load = self.input.param("failover_data_load", False)
        failover_num_docs = self.input.param("failover_num_docs", 2500000)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Get Uploader Map before failover
        self.get_fusion_uploader_info()

        # Perform graceful failover
        node_to_failover = None
        for server in self.cluster.nodes_in_cluster:
            if server.ip != self.cluster.master.ip:
                node_to_failover = server
                break

        rest = ClusterRestAPI(self.cluster.master)
        otp_node = self.get_otp_node(self.cluster.master, node_to_failover)

        self.log.info(f"Failing over the node {otp_node.id}")
        success, _ = rest.perform_graceful_failover(otp_node.id)
        if not success:
            self.fail("Failover unsuccessful")

        # Monitor failover rebalance
        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        if not rebalance_passed:
            self.fail("Graceful failover rebalance failed")

        # Get Uploader Map after failover
        self.get_fusion_uploader_info()

        # Perform data load
        if failover_data_load:
            # Perform a data workload in parallel when rebalance is taking place
            self.reset_doc_params(doc_ops="create")
            self.create_start = self.num_items
            self.create_end = self.create_start + failover_num_docs
            self.num_items += failover_num_docs
            self.log.info("Performing data load aftee failover")
            self.log.info(f"Create start = {self.create_start}, Create End = {self.create_end}")
            self.java_doc_loader(wait=True,
                                skip_default=self.skip_load_to_default_collection,
                                ops_rate=10000, doc_ops="create",
                                monitor_ops=False)

        self.sleep(60, "Wait before recovering/rebalancing-out the node")

        if post_failover_step == "recovery":
            # Perform delta/full recovery
            rest.set_failover_recovery_type(otp_node.id, recovery_type)
            self.sleep(5, "Wait after setting failover recovery type")

            known_nodes=[node.id for node in self.cluster_util.get_nodes(self.cluster.master, inactive_added=True)]
            delta_recovery_buckets = list()
            if recovery_type == "delta":
                delta_recovery_buckets = [bucket.name for bucket in self.cluster.buckets]

            self.log.info(f"Running {recovery_type} recovery rebalance")
            self.log.info("Known nodes: {}".format(known_nodes))
            _, _ = rest.rebalance(known_nodes=known_nodes,
                                delta_recovery_buckets=delta_recovery_buckets)

        elif post_failover_step == "rebalance_out":
            known_nodes=[node.id for node in self.cluster_util.get_nodes(self.cluster.master, inactive_failed=True)]
            self.log.info("Known nodes: {}".format(known_nodes))
            self.log.info("Rebalancing-out the failed over node")
            _, _ = rest.rebalance(known_nodes=known_nodes,
                                  eject_nodes=[otp_node.id])

        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        if not rebalance_passed:
            self.fail("Rebalance operation post failover failed")

        self.log.info("Failover/recovery/rebalance-out process complete")

        self.cluster_util.print_cluster_stats(self.cluster)

        # Get Uploader Map after recovery
        self.get_fusion_uploader_info()


    def test_fusion_uploader_cannot_start_without_chronicle(self):

        self.log.info("Test: Uploader Cannot Start Without Chronicle")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        spare_nodes = self.cluster.servers[len(self.cluster.nodes_in_cluster):]
        if not spare_nodes:
            self.fail("No spare nodes available for test")

        new_node = spare_nodes[0]
        self.log.info(f"Node that will be added: {new_node.ip}")
        self.log.info(f"Master node (Chronicle): {self.cluster.master.ip}")

        self.log.info("Blocking Chronicle communication on new node")
        self.block_chronicle_communication(new_node)
        self.sleep(5, "Wait after blocking Chronicle")

        self.log.info("Running Fusion rebalance (will attempt to start uploader)")
        nodes_to_monitor = self.run_rebalance(
            output_dir=self.fusion_output_dir,
            rebalance_count=1
        )
        self.sleep(30, "Wait after rebalance")

        self.cluster_util.print_cluster_stats(self.cluster)
        self.log.info(f"Nodes to monitor: {[n.ip for n in nodes_to_monitor]}")

        self.log.info("Verifying uploader did not start")
        for node in nodes_to_monitor:
            if node.ip == new_node.ip:
                self.log.info(f"Checking node with blocked Chronicle: {node.ip}")
                self.verify_uploader_not_started(node)

        self.log.info("Cleaning up test")
        self.log_store_rebalance_cleanup(nodes=nodes_to_monitor)

        self.log.info("TEST PASSED: Uploader correctly refused to start without Chronicle access")


    def validate_stale_log_deletion(self):

        self.log.info("Validating stale log file deletion")

        ssh = RemoteMachineShellConnection(self.nfs_server)

        for bucket in self.cluster.buckets:
            for vb_no in list(self.reb_plan_dict[bucket.name]):
                volume_id = self.vb_log_ckpt_dict[bucket.name][vb_no]["volume_id"]
                term_num = int(self.vb_log_ckpt_dict[bucket.name][vb_no]["ckpt"].split("-")[1].split(".")[0])
                ckpt_no = int(self.vb_log_ckpt_dict[bucket.name][vb_no]["ckpt"].split("-")[1].split(".")[1])
                kvstore_path = os.path.join(self.nfs_server_path, volume_id)
                ls_cmd = f"ls -ltr {kvstore_path}"
                o, e = ssh.execute_command(ls_cmd)

                stale_log_files = list()

                for line in o[1:]:
                    line = line.strip()
                    line_arr = line.split(" ")
                    log_name = line_arr[-1]
                    tno = int(log_name.split("-")[1].split(".")[0])
                    cno = int(log_name.split("-")[1].split(".")[1])
                    if tno == term_num and cno > ckpt_no:
                        stale_log_files.append(log_name)
                    elif tno == term_num and cno <= ckpt_no:
                        continue
                    elif tno > term_num:
                        break

                self.log.debug(f"Bucket:{bucket.name}, VB:{vb_no}, Stale log files: {stale_log_files}")

        ssh.disconnect()


    def parse_reb_plan_dict(self, rebalance_counter):

        # Volume_id ex: "kv/1ec40b0ccf8b284bc51d712a85a732ad/kvstore-65"

        self.reb_plan_dict = dict()
        self.vb_log_ckpt_dict = dict()
        self.bucket_uuid_map = dict()

        reb_plan_path = os.path.join(self.fusion_output_dir, "reb_plan{}.json".format(str(rebalance_counter)))
        with open(reb_plan_path, "r") as f:
            data = json.load(f)
        self.involved_nodes = list(data['nodes'].keys())

        for bucket in self.cluster.buckets:
            self.reb_plan_dict[bucket.name] = set()
            self.vb_log_ckpt_dict[bucket.name] = dict()
            bucket_uuid = self.get_bucket_uuid(bucket.name)
            self.bucket_uuid_map[bucket_uuid] = bucket.name

        for node in data['nodes']:
            for kvstore_info in data['nodes'][node]:
                volume_id = kvstore_info['volumeID']
                log_ckpt = kvstore_info["logManifestName"]
                bucket_name = self.bucket_uuid_map[volume_id.split('/')[1]]
                kvstore_num = volume_id.split('/')[2].split('-')[1]
                self.reb_plan_dict[bucket_name].add(int(kvstore_num))
                self.vb_log_ckpt_dict[bucket_name][int(kvstore_num)] = dict()
                self.vb_log_ckpt_dict[bucket_name][int(kvstore_num)]["ckpt"] = log_ckpt
                self.vb_log_ckpt_dict[bucket_name][int(kvstore_num)]["volume_id"] = volume_id

        self.log.info(f"Reb plan dict = {self.reb_plan_dict}")
        self.log.info(f"Log checkpoint dict = {self.vb_log_ckpt_dict}")


    def get_otp_node(self, rest_node, target_node):
        nodes = self.cluster_util.get_nodes(rest_node)
        for node in nodes:
            if node.ip == target_node.ip:
                return node

    def block_chronicle_communication(self, node):
        """
        Block Fusion Chronicle (uploader) ports on localhost so uploads fail,
        but rebalance and node addition still succeed.
        """
        shell = RemoteMachineShellConnection(node)
        self.log.info(f"Blocking Chronicle uploader ports on {node.ip}")

        chronicle_ports = [21200, 21300]

        for port in chronicle_ports:
            # Block only localhost loopback for these ports
            cmds = [
                f"iptables -A OUTPUT -o lo -p tcp -d 127.0.0.1 --dport {port} -j DROP",
                f"iptables -A INPUT -i lo -p tcp -s 127.0.0.1 --sport {port} -j DROP"
            ]
            for cmd in cmds:
                o, e = shell.execute_command(cmd)
                if e:
                    self.log.warning(f"Error executing iptables command on {node.ip}: {e}")
                else:
                    self.log.info(f"Executed iptables command on {node.ip}: {cmd}")

        # Show rules for verification
        o, e = shell.execute_command("iptables -L -n -v | grep 127.0.0.1 | grep DROP || true")
        self.log.info(f"iptables rules on {node.ip} after blocking uploader:\n{o}")

        # Keep track of nodes where we blocked ports
        if node not in self.nodes_with_blocked_chronicle:
            self.nodes_with_blocked_chronicle.append(node)

        shell.disconnect()


    def cleanup_iptables_rules(self):
        """
        Remove Chronicle uploader-blocking iptables rules from all nodes
        where we blocked them.
        """
        chronicle_ports = [21100, 21200, 21300]

        for node in self.nodes_with_blocked_chronicle:
            shell = RemoteMachineShellConnection(node)
            self.log.info(f"Cleaning up Chronicle uploader iptables rules on {node.ip}")

            for port in chronicle_ports:
                cmds = [
                    f"iptables -D OUTPUT -o lo -p tcp -d 127.0.0.1 --dport {port} -j DROP",
                    f"iptables -D INPUT -i lo -p tcp -s 127.0.0.1 --sport {port} -j DROP"
                ]
                for cmd in cmds:
                    o, e = shell.execute_command(cmd)
                    if e:
                        self.log.warning(f"Error removing iptables rule on {node.ip}: {e}")
                    else:
                        self.log.info(f"Removed iptables rule on {node.ip}: {cmd}")

            shell.execute_command("iptables -L -n -v | grep 127.0.0.1 | grep DROP || true")
            shell.disconnect()
            self.log.info(f"Cleanup complete for {node.ip}")

    def verify_uploader_not_started(self, node):
        """
        Check that the uploader did not start (ep_fusion_syncs == 0)
        """
        shell = RemoteMachineShellConnection(node)
        self.log.info(f"Verifying uploader did NOT start on {node.ip}")

        test_passed = True
        for bucket in self.cluster.buckets:
            cmd = f"/opt/couchbase/bin/cbstats localhost:11210 all -b {bucket.name} " \
                  f"-u {self.cluster.master.rest_username} " \
                  f"-p {self.cluster.master.rest_password} | grep 'ep_fusion_syncs:'"
            o, e = shell.execute_command(cmd)

            if o:
                syncs = int(o[0].split(":")[1].strip())
                self.log.info(f"  Bucket {bucket.name}: ep_fusion_syncs = {syncs}")
                if syncs > 0:
                    self.log.error(f"FAIL: Uploader started! Found {syncs} syncs")
                    test_passed = False
            else:
                self.log.info(f"  No fusion stats found for bucket {bucket.name}")

        shell.disconnect()

        if not test_passed:
            self.fail(f"Test FAILED: Uploader started on {node.ip} despite blocked Chronicle")

        self.log.info(f"Verification PASSED: Uploader did NOT start on {node.ip}")
        return True


    def monitor_disk_usage(self, path, interval=10, timeout=1800):

        self.log.info(f"Monitoring DU on path: {path}")

        du_cmd = f"du -sb {path}"
        ssh = RemoteMachineShellConnection(self.nfs_server)

        self.monitor_du = True
        end_time = time.time() + timeout

        while self.monitor_du and time.time() < end_time:

            o, e = ssh.execute_command(du_cmd)
            current_du = int(o[0].split("\t")[0])

            self.log.info(f"Path: {path} DU = {current_du}")

            time.sleep(interval)

        ssh.disconnect()
