import ast
from copy import deepcopy
import json
import os
import random
import subprocess
import threading
import time
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
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

        self.upsert_iterations = self.input.param("upsert_iterations", 2)

        # Override Fusion default settings
        for bucket in self.cluster.buckets:
            self.change_fusion_settings(bucket, upload_interval=self.fusion_upload_interval,
                                        checkpoint_interval=self.fusion_log_checkpoint_interval,
                                        logstore_frag_threshold=self.logstore_frag_threshold)
        # Set Migration Rate Limit
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        for server in self.cluster.servers:
            self.log.info(f"Enabling diag/eval on non local hosts for server: {server.ip}")
            shell = RemoteMachineShellConnection(server)
            o, e = shell.enable_diag_eval_on_non_local_hosts()
            self.log.info(f"Output = {o}, Error = {e}")
            shell.disconnect()

    def tearDown(self):
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

            self.log_store_rebalance_cleanup()

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

        self.log_store_rebalance_cleanup()

        self.parse_reb_plan_dict(rebalance_counter)

        self.validate_stale_log_deletion()


    # TODO
    # StopUploader Fusion API
    # def test_fusion_update_broken_uploaders(self)

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
            self.log.fail("Failover unsuccessful")

        # Monitor failover rebalance
        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        if not rebalance_passed:
            self.log.fail("Graceful failover rebalance failed")

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


    def validate_term_number(self, prev_uploader_map, new_uploader_map):

        for bucket in self.cluster.buckets:

            for vb_no in range(bucket.numVBuckets):

                prev_uploader = prev_uploader_map[bucket.name][vb_no]['node']
                prev_term = prev_uploader_map[bucket.name][vb_no]['term']

                new_uploader = new_uploader_map[bucket.name][vb_no]['node']
                new_term = new_uploader_map[bucket.name][vb_no]['term']

                if prev_uploader != new_uploader:
                    if new_term == prev_term + 1:
                        self.log.info(f"Term number changed for {bucket.name}:vb_{vb_no} as expected")
                    else:
                        self.log.info(f"Expected term number: {prev_term+1}, Actual term number: {new_term}")

                else:
                    if prev_term == new_term:
                        self.log.info(f"Term number not changed for {bucket.name}:vb_{vb_no} as expected")
                    else:
                        self.log.info(f"Expected term number: {prev_term}, Actual term number: {new_term}")


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

        ssh = RemoteMachineShellConnection(self.cluster.master)
        reb_plan_path = os.path.join(self.fusion_scripts_dir, "reb_plan{}.json".format(str(rebalance_counter)))
        cat_cmd = f"cat {reb_plan_path}"
        o, e = ssh.execute_command(cat_cmd)
        json_str = "\n".join(o)
        data = json.loads(json_str)

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

        ssh.disconnect()


    def get_otp_node(self, rest_node, target_node):
        nodes = self.cluster_util.get_nodes(rest_node)
        for node in nodes:
            if node.ip == target_node.ip:
                return node