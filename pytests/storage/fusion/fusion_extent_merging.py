from collections import defaultdict
import json
import os
import random
import re
import string
import threading
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from py_constants.cb_constants.CBServer import CbServer
from sdk_client3 import SDKClient
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionExtentMerging(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionExtentMerging, self).setUp()

        self.min_extent_size = self.input.param("min_extent_size", 16 * 1024 * 1024) # 16MB

        self.log.info("FusionExtentMerging setUp Started")
        self.rate_limit_toggle_stop = False

    def tearDown(self):
        self.rate_limit_toggle_stop = True
        super(FusionExtentMerging, self).tearDown()


    def test_fusion_extent_merging(self):

        '''
        N node cluster -> 2 VBs, Fusion Enabled
        1 bucket
        upload_interval -> 4 hours (large value)
        minExtentSize set to 16MB
        store keys in a file and read from there into a dict:
            ./cbc-keygen -P password -u Administrator -U couchbase://172.23.217.176/default --keys-per-vbucket 100 > /root/cb_keys.txt
        load few docs of some size per VB
        force sync to fusion, force sync will also call force flush from Magma
        wait for some time

        Ex:
        in a loop: (5 - 6 times)
            load 2 docs per VB (will create 8KB extents)
            force flush
            observe that file size grows and num_extents increase by 1
            average file extent size should keep decreasing
        total extents = 6
        average extent size = (100 + 8 + 8 + 8 + 8 + 8) / 6 = 23.3KB
        observe that extent merging is triggered
        num extents should go below 6
        average extent size should go above 64KB
        '''

        num_sync_intervals = self.input.param("num_sync_intervals", 10)
        slow_write_val_size = self.input.param("slow_write_val_size", 4096) # 4KB
        num_keys_per_sync = self.input.param("num_keys_per_sync", 2)
        crash_during_merge = self.input.param("crash_during_merge", False)
        crash_interval = self.input.param("crash_interval", 30)
        trigger_rebalance = self.input.param("trigger_rebalance", False)
        run_update_workload = self.input.param("run_update_workload", False)

        # Load around 2GB data to the bucket with 2VBs, so 1GB per VB
        # sstable can hold upto 5% of the dataset, so each sstable can be 0.05 * 2GB = 100MB
        self.log.info("Starting initial load")
        self.initial_load()
        self.sleep(60, "Sleep after data loading")

        # Force sync
        status, content = FusionRestAPI(self.cluster.master).sync_log_store()
        self.log.info(f"Force Sync after initial load, Status: {status}, Content: {content}")

        self.sleep(60, "Wait after initial sync")

        num_keys_per_vb = num_sync_intervals * num_keys_per_sync
        self.vb_key_dict = self.generate_vb_key_dict(self.cluster.buckets[0], num_keys_per_vb=num_keys_per_vb)
        self.log.info(f"VB Key dict = {self.vb_key_dict}")

        self.bucket_client_dict = dict()

        for bucket in self.cluster.buckets:
            client = SDKClient(self.cluster, bucket,
                               scope=CbServer.default_scope,
                               collection=CbServer.default_collection)
            self.bucket_client_dict[bucket.name] = client

        vb_key_offset = 0
        val_obj = self.get_json_val_obj(size=slow_write_val_size, mutated=0)

        if crash_during_merge:
            crash_th = threading.Thread(target=self.kill_memcached_on_nodes, args=[crash_interval])
            crash_th.start()

        for i in range(num_sync_intervals):

            self.log.info(f"Slow writes iteration: {i+1}")

            for vb_no in range(self.vbuckets):

                keys = self.vb_key_dict[vb_no][vb_key_offset:vb_key_offset+num_keys_per_sync]

                for bucket in self.cluster.buckets:
                    sdk_client = self.bucket_client_dict[bucket.name]
                    for key in keys:
                        res = sdk_client.insert(key, val_obj, timeout=60)
                        self.log.info(f"Bucket: {bucket.name}, VB: {vb_no}, Key: {key}, Result: {res['status']}")

            vb_key_offset += num_keys_per_sync

            # Force sync to log store
            status, content = FusionRestAPI(self.cluster.master).sync_log_store()
            self.log.info(f"Force Sync after sync interval, Status: {status}, Content: {content}")

            # Fetch Fusion stats
            for bucket in self.cluster.buckets:
                stat_dict = self.get_fusion_kvstore_stats(bucket)
                self.log.info(f"Bucket: {bucket.name}, Fusion Stats: {stat_dict}")

        for bucket in self.cluster.buckets:
            self.bucket_client_dict[bucket.name].close()

        self.crash_loop = False
        if crash_during_merge:
            crash_th.join()

        self.sleep(60, "Wait before validating extent sizes")

        # fetch avg extent size of all sstables from log_dump and validate
        for bucket in self.cluster.buckets:
            violation_dict = self.validate_extent_size(bucket)
            self.log.info(f"Bucket: {bucket.name}, Extent Size Violations: {violation_dict}")

        if run_update_workload:

            # Perform a read workload
            self.log.info("Performing an update workload")
            doc_load_tasks = self.perform_workload(0, self.num_items, "update", False, ops_rate=self.ops_rate, mutate=1)

            self.new_val_obj = self.get_json_val_obj(size=slow_write_val_size, mutated=1)

            self.bucket_client_dict = dict()
            for bucket in self.cluster.buckets:
                client = SDKClient(self.cluster, bucket,
                                scope=CbServer.default_scope,
                                collection=CbServer.default_collection)
                self.bucket_client_dict[bucket.name] = client

            self.log.info("Updating docs in small extents with mutated=1")

            for vb_no in range(self.vbuckets):

                keys = self.vb_key_dict[vb_no]

                for bucket in self.cluster.buckets:
                    sdk_client = self.bucket_client_dict[bucket.name]
                    for key in keys:
                        res = sdk_client.upsert(key, self.new_val_obj, timeout=60)
                        self.log.info(f"Bucket: {bucket.name}, VB: {vb_no}, Key: {key}, Result: {res['status']}")

            for task in doc_load_tasks:
                self.doc_loading_tm.get_task_result(task)

            # Force sync to log store
            status, content = FusionRestAPI(self.cluster.master).sync_log_store()
            self.log.info(f"Force Sync after sync interval, Status: {status}, Content: {content}")

            # Fetch Fusion stats
            for bucket in self.cluster.buckets:
                stat_dict = self.get_fusion_kvstore_stats(bucket)
                self.log.info(f"Bucket: {bucket.name}, Fusion Stats: {stat_dict}")

        if trigger_rebalance:
            self.sleep(60, "Wait before performing a Fusion Rebalance")
            # Perform a Fusion Rebalance
            self.log.info(f"Running Fusion rebalance")
            nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                rebalance_count=1,
                                                skip_file_linking=True,
                                                rebalance_sleep_time=60)

            self.sleep(30, "Wait after Fusion Rebalance Completion")

            # Perform a read workload
            self.log.info("Performing a read workload during extent migration")
            doc_load_tasks = self.perform_workload(0, self.num_items, "read", False, ops_rate=self.read_ops_rate)

            extent_migration_array = list()
            self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
            for node in nodes_to_monitor:
                for bucket in self.cluster.buckets:
                    extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                    extent_th.start()
                    extent_migration_array.append(extent_th)

            for th in extent_migration_array:
                th.join()

            for task in doc_load_tasks:
                self.doc_loading_tm.get_task_result(task)

            self.cluster_util.print_cluster_stats(self.cluster)


        if run_update_workload:

            # Perform a read workload
            self.log.info("Performing an update workload")
            doc_load_tasks = self.perform_workload(0, self.num_items, "update", False, ops_rate=self.ops_rate, mutate=2)

            self.new_val_obj = self.get_json_val_obj(size=slow_write_val_size, mutated=2)

            self.bucket_client_dict = dict()
            for bucket in self.cluster.buckets:
                client = SDKClient(self.cluster, bucket,
                                scope=CbServer.default_scope,
                                collection=CbServer.default_collection)
                self.bucket_client_dict[bucket.name] = client

            self.log.info("Updating docs in small extents with mutated=2")

            for vb_no in range(self.vbuckets):

                keys = self.vb_key_dict[vb_no]

                for bucket in self.cluster.buckets:
                    sdk_client = self.bucket_client_dict[bucket.name]
                    for key in keys:
                        res = sdk_client.upsert(key, self.new_val_obj, timeout=60)
                        self.log.info(f"Bucket: {bucket.name}, VB: {vb_no}, Key: {key}, Result: {res['status']}")

            for task in doc_load_tasks:
                self.doc_loading_tm.get_task_result(task)

            # Force sync to log store
            status, content = FusionRestAPI(self.cluster.master).sync_log_store()
            self.log.info(f"Force Sync after sync interval, Status: {status}, Content: {content}")

            # Fetch Fusion stats
            for bucket in self.cluster.buckets:
                stat_dict = self.get_fusion_kvstore_stats(bucket)
                self.log.info(f"Bucket: {bucket.name}, Fusion Stats: {stat_dict}")


    def generate_vb_key_dict(self, bucket, num_keys_per_vb):

        ssh = RemoteMachineShellConnection(self.cluster.master)
        file_path = "/root/cb_keys.txt"

        # Generate keys using cbc-keygen
        key_gen_cmd = f"/opt/couchbase/bin/cbc-keygen -P password -u Administrator -U couchbase://{self.cluster.master.ip}/{bucket.name} --keys-per-vbucket {num_keys_per_vb} > {file_path}"

        self.log.info(f"Generating keys using cbc-keygen: {key_gen_cmd}")
        output, error = ssh.execute_command(key_gen_cmd)

        self.log.info("Reading keys into a dict")
        output, error = ssh.execute_command(f"cat {file_path}")

        vb_map = defaultdict(list)

        for line in output:
            line = line.strip()
            if not line:
                continue

            key, vb = line.split()
            vb_map[int(vb)].append(key)

        ssh.disconnect()
        return dict(vb_map)


    def get_json_val_obj(self, size, mutated=0):

        val_dict = {
            "name": "pL85wJj73szo6eHfY5H5frK1dpNNbJzxhPgZ4mTcTTRw9c40p8DahqKSgSW27pnRkm9L9VwKFnP1giNx96fY9K8NaV1FqkJkTckK",
            "age": 8,
            "animals": ["Burlywood", "Burnished brown"],
            "attributes": {
                "hair": "Fern green",
                "dimensions": {
                "height": 34,
                "weight": 77
                },
                "hobbies": [{
                "type": "Electronics",
                "name": "Sports",
                "details": {
                    "location": {
                    "lat": 19.63269207579724,
                    "lon": 44.66682144320981
                    }
                }
                }]
            },
            "gender": "T",
            "marital": "W",
            "mutated": mutated,
            "body": ""
        }

        def generate_json_of_size(target_bytes: int):
            """
            Generate a JSON document with the same schema
            and an exact serialized size in bytes.
            """
            doc = dict(val_dict)

            base_size = len(json.dumps(doc, separators=(",", ":")).encode("utf-8"))
            if base_size >= target_bytes:
                return doc

            padding_size = target_bytes - base_size
            doc["body"] = "".join(
                random.choices(string.ascii_letters + string.digits, k=padding_size)
            )

            return doc

        doc = generate_json_of_size(size)
        return doc


    def validate_extent_size(self, bucket):

        extent_size_violations = dict()

        line_re = re.compile(
            r"Path:\s+(?P<path>\S+)\s+\(Extents:\s+(?P<extents>\d+),\s+Size:\s+(?P<size>\d+)"
        )

        kvstore_dirs = self.get_kvstore_directories(bucket)

        for dir in kvstore_dirs:

            kvstore_num = int(dir.split("-")[1])
            extent_size_violations[kvstore_num] = dict()

            i = 0
            tmp_arr = dir.split("/")
            for i in range(len(tmp_arr)):
                if tmp_arr[i] == "buckets":
                    break

            ssh = RemoteMachineShellConnection(self.cluster.master)

            volume_name = "/".join(tmp_arr[i+1:])
            chronicle_cmd = '/opt/couchbase/bin/fusion/metadata_dump --uri "chronicle://localhost:8091" --volume-id "' + volume_name + '"'
            self.log.info(f"Chronicle cmd = {chronicle_cmd}")

            o, e = ssh.execute_command(chronicle_cmd)
            json_str = "\n".join(o)
            stat = json.loads(json_str)
            checkpoint_logid = stat["checkpoint"]["logID"]

            file_name = f"/root/file_map_{bucket.name}_vb_{kvstore_num}.txt"
            file_dump_cmd = f"/opt/couchbase/bin/fusion/log_dump --filemanifest /mnt/nfs/share/buckets/{volume_name}/{checkpoint_logid} > {file_name}"
            self.log.info(f"File Dump CMD: {file_dump_cmd}")
            o, e = ssh.execute_command(file_dump_cmd)

            grep_cmd = f"grep 'seqIndex/sstable' {file_name}"
            self.log.info(f"Grep CMD: {grep_cmd}")
            o, e = ssh.execute_command(grep_cmd)

            for line in o:
                m = line_re.search(line)
                if not m:
                    continue

                sstable = m.group("path")
                extents = int(m.group("extents"))
                size = int(m.group("size"))

                if extents <= 1:
                    continue

                avg_extent_size = size // extents

                self.log.info(f"{sstable}, Avg Extent Size = {avg_extent_size}")

                if avg_extent_size < self.min_extent_size:
                    extent_size_violations[kvstore_num][sstable] = avg_extent_size


        return extent_size_violations
