import json
import os
import random
import threading
import time
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_tools.cbstats import Cbstats
from rebalance_utils.rebalance_util import RebalanceUtil
from storage.fusion.fusion_base import FusionBase
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.magma.magma_base import MagmaBaseTest


class FusionLogCorruption(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionLogCorruption, self).setUp()

        self.log.info("FusionLogCorruption setUp Started")

        self.skip_file_linking = self.input.param("skip_file_linking", True)
        self.corruption_type = self.input.param("corruption_type", "append")
        self.include_log_manifest = self.input.param("include_log_manifest", False)

        self.num_kvstores = self.input.param("num_kvstores", None)
        self.num_guest_volumes = self.input.param("num_guest_volumes", None)
        self.num_log_files = self.input.param("num_log_files", 2)


    def tearDown(self):
        super(FusionLogCorruption, self).tearDown()


    def fetch_log_files_from_manifest(self, rebalance_count, reb_plan_location="slave"):

        self.log.info("Parsing rebalance plan to get a list of log files")

        self.log_manifest_dict = dict()
        self.kvstore_log_dict = dict()

        ssh = RemoteMachineShellConnection(self.cluster.master)

        if reb_plan_location == "slave":
            reb_plan_path = os.path.join(self.fusion_output_dir, "reb_plan{}.json".format(str(rebalance_count)))
            with open(reb_plan_path, "r") as f:
                data = json.load(f)
        elif reb_plan_location == "node":
            reb_plan_path = os.path.join(self.fusion_scripts_dir, "reb_plan{}.json".format(str(rebalance_count)))
            cat_cmd = f"cat {reb_plan_path}"
            o, e = ssh.execute_command(cat_cmd)
            json_str = "\n".join(o)
            data = json.loads(json_str)

        self.involved_nodes = list(data['nodes'].keys())
        self.log.info(f"Involved Nodes = {self.involved_nodes}")

        for node in self.involved_nodes:
            for kvstore_dict in data['nodes'][node]:
                log_manifest = kvstore_dict['logManifestName']
                volumeID = kvstore_dict['volumeID']

                bucket_uuid = volumeID.split("/")[1]
                kvstore = volumeID.split("/")[-1]
                if bucket_uuid not in self.kvstore_log_dict:
                    self.kvstore_log_dict[bucket_uuid] = dict()
                self.kvstore_log_dict[bucket_uuid][kvstore] = list()

                local_nfs_path = self.fusion_log_store_uri.split("//")[-1]

                full_log_mainfest_path = os.path.join(local_nfs_path, volumeID, log_manifest)

                log_dump_cmd = f"""/opt/couchbase/bin/fusion/log_dump -l {full_log_mainfest_path} | grep 'LogID' | awk '{{print "log-" substr($2,6) "." substr($3,7)}}'"""

                o, e = ssh.execute_command(log_dump_cmd)
                for log in o:
                    full_log_file_path = os.path.join(local_nfs_path, volumeID, log.strip())
                    self.kvstore_log_dict[bucket_uuid][kvstore].append(full_log_file_path)

        self.log.info(f"Kvstore log file dict = {self.kvstore_log_dict}")


    def test_fusion_log_corruption_during_acceleration(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        corrupt_th = threading.Thread(target=self.corrupt_log_files_on_log_store,
                                      args=[self.corruption_type, self.num_kvstores, self.num_log_files,
                                            100, 1, self.include_log_manifest])
        corrupt_th.start()

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_sleep_time=300,
                                              skip_file_linking=self.skip_file_linking)

        corrupt_th.join()

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)


    def test_fusion_log_corruption_during_extent_migration(self):

        validate_reads = self.input.param("validate_reads", True)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Perform a subsequent load
        self.perform_workload(self.num_items, self.num_items * 2, "create", ops_rate=30000)
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Set Migration Rate Limit to 0 so that migration doesn't start
        status, content = ClusterRestAPI(self.cluster.master).\
                            manage_global_memcached_setting(fusion_migration_rate_limit=0)
        self.log.info(f"Setting Migration Rate Limit to 0, Status = {status}, Content = {content}")

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_sleep_time=120,
                                              skip_file_linking=self.skip_file_linking)

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Corrupt log files on guest volumes after rebalance
        self.corrupt_log_files_on_guest_volumes(corruption_type=self.corruption_type,
                                                num_kvstores=self.num_kvstores,
                                                num_guest_volumes=self.num_guest_volumes,
                                                include_manifest=self.include_log_manifest)

        # Perform a read workload during extent migration
        read_workload_tasks1 = self.perform_workload(0, self.num_items, "read", wait=False, ops_rate=10000)

        # Updating Migration Rate Limit so that migration starts
        status, content = ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
                                fusion_migration_rate_limit=self.fusion_migration_rate_limit)
        self.log.info(f"Updating Migration Rate Limit, Status = {status}, Content = {content}")

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        for task in read_workload_tasks1:
            self.doc_loading_tm.get_task_result(task)

        if validate_reads:
            self.log_store_rebalance_cleanup(nodes=nodes_to_monitor)
            self.validate_read_failures()


    def test_fusion_log_corruption_during_mounting(self):

        validate_reads = self.input.param("validate_reads", True)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor, current_nodes_str, new_nodes_str = \
                                    self.run_rebalance(
                                        output_dir=self.fusion_output_dir,
                                        rebalance_count=1,
                                        rebalance_sleep_time=120,
                                        skip_file_linking=self.skip_file_linking,
                                        stop_before_rebalance=True)

        # Corrupt log files on guest volumes after rebalance
        self.corrupt_log_files_on_guest_volumes(corruption_type=self.corruption_type,
                                                num_kvstores=self.num_kvstores,
                                                num_guest_volumes=self.num_guest_volumes,
                                                include_manifest=self.include_log_manifest)

        # Trigger Rebalance after corrupting guest volumes
        known_nodes=[node.id for node in self.cluster_util.get_nodes(self.cluster.master, inactive_added=True)]
        current_nodes_list = [node.strip() for node in current_nodes_str.split(',') if node.strip()]
        new_nodes_list = [node.strip() for node in new_nodes_str.split(',') if node.strip()]
        self.log.info(f"Current nodes list = {current_nodes_list}, New nodes list = {new_nodes_list}")
        current_set = set(current_nodes_list)
        new_set = set(new_nodes_list)
        ejected_nodes = list(current_set - new_set)
        ejected_node_ids = [f"ns_1@{ip}" for ip in ejected_nodes]

        self.log.info(f"Known nodes = {known_nodes}")
        self.log.info(f"Ejected nodes = {ejected_node_ids}")

        self.log.info(f"Triggering a Fusion Rebalance with plan UUID: {self.reb_plan_uuids[-1]}")
        rest = ClusterRestAPI(self.cluster.master)
        status, content = rest.rebalance(known_nodes=known_nodes,
                                         eject_nodes=ejected_node_ids,
                                         plan_uuid=self.reb_plan_uuids[-1])
        self.log.info(f"Trigerring Rebalance, Status={status}, Content={content}")

        # Monitor Rebalance
        self.sleep(10, "Wait before checking rebalance progress")
        try:
            rebalance_result = RebalanceUtil(self.cluster).monitor_rebalance(progress_count=500)
            if not rebalance_result:
                self.log.info("Rebalance failed")
        except Exception as ex:
            self.log.error(f"Fusion Rebalance failed: {ex}")

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        if validate_reads:
            self.log_store_rebalance_cleanup(nodes=nodes_to_monitor)
            self.validate_read_failures()


    def corrupt_log_files_on_log_store(self, corruption_type, num_kvstores, num_log_files, sleep_time, rebalance_count, include_manifest=False):

        self.sleep(sleep_time, "Wait before corrupting/deleting log files on log store")

        self.fetch_log_files_from_manifest(rebalance_count=rebalance_count, reb_plan_location="node")

        for bucket in self.cluster.buckets:
            random_kvstores = random.sample(list(self.kvstore_log_dict[bucket.uuid].keys()), num_kvstores)
            self.log.info(f"Random kvstores = {random_kvstores}")

            for kvstore in random_kvstores:
                if include_manifest:
                    random_log_file_list = [self.kvstore_log_dict[bucket.uuid][kvstore[0]]]
                    random_log_file_list += random.sample(self.kvstore_log_dict[bucket.uuid][kvstore][1:], min(num_log_files-1, len(self.kvstore_log_dict[bucket.uuid][kvstore])))
                else:
                    random_log_file_list = random.sample(self.kvstore_log_dict[bucket.uuid][kvstore][1:], min(num_log_files, len(self.kvstore_log_dict[bucket.uuid][kvstore])))

                self.log.info(f"List of log files to be corrupted/deleted in Bucket: {bucket.name}, {kvstore} = {random_log_file_list}")

                self.corrupt_delete_log_files(random_log_file_list, corruption_type)


    def corrupt_log_files_on_guest_volumes(self, corruption_type, num_kvstores=None, num_guest_volumes=None,
                                           num_log_files=5, rebalance_count=1, include_manifest=False):

        self.log.info("Corrupting/deleting log files on guest volumes")

        self.fetch_log_files_from_manifest(rebalance_count=rebalance_count)

        a = self.fusion_log_store_uri.split("///")[-1]
        b = a.split("/")
        self.guest_storage_dir = os.path.join("/" + "/".join(b[:-1]), "guest_storage")

        shell = RemoteMachineShellConnection(self.cluster.master)

        if corruption_type == "delete_all":

            self.log.info("Deleting all log files across guest volumes")

            delete_cmd = f"find {self.guest_storage_dir} -type f -name 'log-*' -delete"
            o, e = shell.execute_command(delete_cmd)
            self.log.info(f"CMD: {delete_cmd}, O = {o}, E = {e}")


        elif num_kvstores is not None:

            for bucket in self.cluster.buckets:
                random_kvstores = random.sample(list(self.kvstore_log_dict[bucket.uuid].keys()), int(num_kvstores))
                self.log.info(f"Random kvstores = {random_kvstores}")

                for kvstore in random_kvstores:

                    log_manifest_file = self.kvstore_log_dict[bucket.uuid][kvstore][0].split("/")[-1]
                    log_files_to_corrupt = list()

                    if include_manifest:
                        log_manifest_find_cmd = f"find {self.guest_storage_dir} -name 'log-*' | grep '{bucket.uuid}' | grep '{kvstore}/' | grep '{log_manifest_file}'"
                        o, e = shell.execute_command(log_manifest_find_cmd)
                        log_files_to_corrupt.append(o[0].strip())

                    find_cmd = f"find {self.guest_storage_dir} -name 'log-*' | grep '{bucket.uuid}' | grep '{kvstore}/' | grep -v '{log_manifest_file}'"
                    o, e = shell.execute_command(find_cmd)
                    self.log.info(f"CMD: {find_cmd}, O = {o}, E = {e}")

                    for log_file in o:
                        log_file = log_file.strip()
                        log_files_to_corrupt.append(log_file)

                    log_files_to_corrupt = log_files_to_corrupt[:num_log_files]

                    self.log.info(f"Bucket: {bucket.name}, {kvstore}, Log Files to corrupt: {log_files_to_corrupt}")

                    self.corrupt_delete_log_files(log_files_to_corrupt, corruption_type)


        elif num_guest_volumes is not None:

            for node in self.involved_nodes:
                node_guest_path = os.path.join(self.guest_storage_dir, node)

                nums = random.sample(range(1, 21), int(num_guest_volumes))
                guest_volumes = [f"guest{n}" for n in nums]

                for guest in guest_volumes:
                    for bucket in self.cluster.buckets:
                        guest_volume_path = os.path.join(node_guest_path, guest)

                        find_cmd = f"find {guest_volume_path} -name 'log-*' | grep '{bucket.uuid}'"
                        self.log.info(f"Executing CMD: {find_cmd}")
                        o, e = shell.execute_command(find_cmd)

                        selected_random_files = random.sample(o, min(num_log_files, len(o)))
                        selected_random_files = [log_file.strip() for log_file in selected_random_files]

                        self.log.info(f"Guest volume: {guest}, Bucket: {bucket.name}, Random log files: {selected_random_files}")

                        self.corrupt_delete_log_files(selected_random_files, corruption_type)

        shell.disconnect()


    def corrupt_delete_log_files(self, log_files, corruption_type):

        shell = RemoteMachineShellConnection(self.cluster.master)

        for log_file in log_files:

            if corruption_type == "append":
                corrupt_cmd = f"dd if=/dev/urandom of='{log_file}' bs=1 count=16 seek=$(stat -c%s '{log_file}') conv=notrunc"

            elif corruption_type == "overwrite":
                corrupt_cmd = (
                    f"size=$(stat -c%s '{log_file}'); "
                    f"mid=$((size / 2)); "
                    f"dd if=/dev/urandom of='{log_file}' bs=1 count=16 seek=$mid conv=notrunc"
                )

            elif corruption_type == "truncate":
                corrupt_cmd = f"truncate -s -16 '{log_file}'"

            elif corruption_type == "delete":
                corrupt_cmd = f"rm {log_file}"

            elif corruption_type == "truncate_full":
                corrupt_cmd = f"truncate -s 0 {log_file}"

            o, e = shell.execute_command(corrupt_cmd)
            self.log.info(f"CMD: {corrupt_cmd}, Output = {o}, Error = {e}")

        shell.disconnect()


    def validate_read_failures(self):

        # Check ep_data_read_failed stat before read workload
        read_failure_dict = dict()
        for server in self.cluster.nodes_in_cluster:
            read_failure_dict[server.ip] = dict()
            for bucket in self.cluster.buckets:
                result = Cbstats(server).all_stats(bucket.name)
                read_failure_dict[server.ip][bucket.name] = result["ep_data_read_failed"]
        self.log.info(f"Read failure dict before workload = {read_failure_dict}")

        # Clear page cache
        for server in self.cluster.nodes_in_cluster:
            ssh = RemoteMachineShellConnection(server)
            o, e = ssh.execute_command(f"sudo sync; sudo sh -c 'echo 1 > /proc/sys/vm/drop_caches'")
            self.log.info(f"Dropping cache on {server.ip}, O = {o}, E = {e}")
            ssh.disconnect()

        self.sleep(30, "Wait after clearing page cache")

        # Perform read workload
        self.log.info("Performing reads after extent migration")
        self.perform_workload(0, self.num_items, doc_op="read")

        # Check ep_data_read_failed stat before read workload
        read_failure_dict2 = dict()
        for server in self.cluster.nodes_in_cluster:
            read_failure_dict2[server.ip] = dict()
            for bucket in self.cluster.buckets:
                result = Cbstats(server).all_stats(bucket.name)
                read_failure_dict2[server.ip][bucket.name] = result["ep_data_read_failed"]
        self.log.info(f"Read failure dict after workload = {read_failure_dict2}")
