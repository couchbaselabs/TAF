"""
Magma Disk Usage Validation Test

Test to load 500GB data using Sirius and compare real disk usage (du) with
UI-reported disk usage (cbstats) at regular intervals.

Config:
- Single node, no replica
- 1024 vBuckets
- Load 500M docs
- Capture every 10 seconds:
  1. du (actual disk blocks used)
  2. du --apparent-size (logical file size)
  3. ep_magma_logical_disk_size from cbstats all
"""

import os
import subprocess
import time
import threading
from datetime import datetime

from cb_constants.CBServer import CbServer
from cb_tools.cbstats import Cbstats
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from Jython_tasks.task_manager import TaskManager
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.magma.magma_base import MagmaBaseTest

REMOTE_SCRIPT_PATH = "/tmp/MB-71423_truncate_preallocation.sh"


class MagmaDiskUsageValidation(MagmaBaseTest):
    def setUp(self):
        super(MagmaDiskUsageValidation, self).setUp()
        self.capture_interval = self.input.param("capture_interval", 10)
        self.stop_monitoring = False
        self.disk_usage_stats = []
        self.script_interval_secs = self.input.param("script_interval_secs", 180)
        self.crash_interval_secs = self.input.param("crash_interval_secs", 120)
        self.workload_iterations = self.input.param("workload_iterations", 10000)
        self.run_truncate_script = self.input.param("run_truncate_script", True)
        self.max_age_secs = self.input.param("max_age_secs", 180)
        self.verbose_script = self.input.param("verbose_script", True)
        taf_root = os.path.normpath(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..")
        )
        self.local_script_path = os.path.join(
            taf_root, "scripts", "MB-71423_truncate_preallocation.sh"
        )

    def tearDown(self):
        self.stop_monitoring = True
        super(MagmaDiskUsageValidation, self).tearDown()

    def get_du_stats(self, server, bucket):
        """Get disk usage stats using du command."""
        shell = RemoteMachineShellConnection(server)
        bucket_path = os.path.join(self.data_path, bucket.uuid)

        # du - actual disk blocks used (in MB)
        du_cmd = "du -sm {} | awk '{{print $1}}'".format(bucket_path)
        du_output = shell.execute_command(du_cmd)[0]
        du_mb = int(du_output[0].strip()) if du_output and du_output[0].strip() else 0

        # du --apparent-size - logical file size (in MB)
        du_apparent_cmd = "du --apparent-size -sm {} | awk '{{print $1}}'".format(bucket_path)
        du_apparent_output = shell.execute_command(du_apparent_cmd)[0]
        du_apparent_mb = int(du_apparent_output[0].strip()) if du_apparent_output and du_apparent_output[0].strip() else 0

        shell.disconnect()
        return du_mb, du_apparent_mb

    def capture_disk_usage_loop(self, bucket):
        """Background thread to capture disk usage at intervals."""
        server = self.cluster.master
        self.log.info("Starting disk usage monitoring (interval={}s)".format(
            self.capture_interval))

        while not self.stop_monitoring:
            try:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                du_mb, du_apparent_mb = self.get_du_stats(server, bucket)
                cbstats_bytes = self.magma_utils.get_magma_disk_size(
                    server, bucket.name)
                cbstats_mb = cbstats_bytes // (1024 * 1024)

                stat_entry = {
                    "timestamp": timestamp,
                    "du_mb": du_mb,
                    "du_apparent_mb": du_apparent_mb,
                    "cbstats_disk_usage_mb": cbstats_mb,
                    "du_gb": du_mb / 1024,
                    "du_apparent_gb": du_apparent_mb / 1024,
                    "cbstats_gb": cbstats_mb / 1024
                }
                self.disk_usage_stats.append(stat_entry)

                self.log.info(
                    "DU Stats | Time: {} | du: {}MB ({:.2f}GB) | "
                    "du_apparent: {}MB ({:.2f}GB) | cbstats: {}MB ({:.2f}GB)".format(
                        timestamp, du_mb, du_mb/1024, du_apparent_mb,
                        du_apparent_mb/1024, cbstats_mb, cbstats_mb/1024))

            except Exception as e:
                self.log.error("Error capturing disk usage: {}".format(e))

            time.sleep(self.capture_interval)

    def print_disk_usage_summary(self):
        """Print summary of captured disk usage stats."""
        self.log.info("=" * 80)
        self.log.info("DISK USAGE SUMMARY")
        self.log.info("=" * 80)

        # Print at checkpoints (100GB, 200GB, etc.)
        checkpoints = [100, 200, 300, 400, 500]
        checkpoint_idx = 0

        for stat in self.disk_usage_stats:
            du_gb = stat["du_gb"]
            while checkpoint_idx < len(checkpoints) and du_gb >= checkpoints[checkpoint_idx]:
                self.log.info(
                    "CHECKPOINT ~{}GB | Time: {} | du: {:.2f}GB | "
                    "du_apparent: {:.2f}GB | cbstats: {:.2f}GB | "
                    "diff(du vs cbstats): {:.2f}GB".format(
                        checkpoints[checkpoint_idx],
                        stat["timestamp"],
                        stat["du_gb"],
                        stat["du_apparent_gb"],
                        stat["cbstats_gb"],
                        stat["du_gb"] - stat["cbstats_gb"]))
                checkpoint_idx += 1

        # Final summary
        if self.disk_usage_stats:
            final = self.disk_usage_stats[-1]
            self.log.info("-" * 80)
            self.log.info(
                "FINAL | du: {:.2f}GB | du_apparent: {:.2f}GB | "
                "cbstats: {:.2f}GB".format(
                    final["du_gb"], final["du_apparent_gb"], final["cbstats_gb"]))

    def _build_loader_kwargs_list(self, create_pct, update_pct, read_pct, mutate_val):
        kwargs_list = []
        for bkt in self.cluster.buckets:
            for scope in bkt.scopes.keys():
                if scope == CbServer.system_scope:
                    continue
                for collection in bkt.scopes[scope].collections.keys():
                    if (self.skip_load_to_default_collection
                            and collection == "_default"
                            and scope == "_default"):
                        continue
                    kwargs_list.append(dict(
                        server_ip=self.cluster.master.ip,
                        server_port=self.cluster.master.port,
                        username=self.cluster.master.rest_username,
                        password=self.cluster.master.rest_password,
                        bucket=bkt, scope_name=scope, collection_name=collection,
                        key_prefix=self.key, key_size=self.key_size,
                        doc_size=self.doc_size,
                        key_type=self.key_type,
                        create_percent=create_pct, read_percent=read_pct,
                        update_percent=update_pct, delete_percent=0,
                        expiry_percent=0,
                        create_start_index=self.create_start,
                        create_end_index=self.create_end,
                        read_start_index=0,
                        read_end_index=self.num_items if read_pct > 0 else 0,
                        update_start_index=0, update_end_index=self.num_items,
                        delete_start_index=0, delete_end_index=0,
                        expiry_start_index=0, expiry_end_index=0,
                        exp=0,
                        process_concurrency=self.process_concurrency,
                        validate_docs=False,
                        ops=self.ops_rate,
                        mutate=mutate_val,
                        value_type=self.val_type,
                    ))
        return kwargs_list

    def _run_loader_phase(self, phase_label, doc_loading_tm, loader_kwargs_list):
        self.log.info("=" * 60)
        self.log.info("PHASE: {}".format(phase_label))
        self.log.info("=" * 60)
        tasks = []
        for kwargs in loader_kwargs_list:
            loader = SiriusCouchbaseLoader(**kwargs)
            loader.create_doc_load_task()
            doc_loading_tm.add_new_task(loader)
            tasks.append(loader)
        for task in tasks:
            doc_loading_tm.get_task_result(task)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        total_failures = sum(t.fail_count for t in tasks)
        if total_failures > 0:
            for task in tasks:
                if task.fail_count > 0:
                    self.log.error("Loader failures: {}".format(task.fail))
            self.fail("Phase '{}' reported {} failures".format(phase_label, total_failures))
        self.log.info("Phase '{}' complete.".format(phase_label))

    def _snapshot_disk_usage(self, label, bucket):
        """Log a labelled disk-usage snapshot and return the three values."""
        du_mb, du_apparent_mb = self.get_du_stats(self.cluster.master, bucket)
        cbstats_bytes = self.magma_utils.get_magma_disk_size(
            self.cluster.master, bucket.name)
        cbstats_mb = cbstats_bytes // (1024 * 1024)

        self.log.info("=" * 80)
        self.log.info("DISK USAGE SNAPSHOT — {}".format(label))
        self.log.info("du (blocks):       {} MB ({:.2f} GB)".format(du_mb, du_mb / 1024))
        self.log.info("du --apparent-size: {} MB ({:.2f} GB)".format(
            du_apparent_mb, du_apparent_mb / 1024))
        self.log.info("cbstats disk usage: {} MB ({:.2f} GB)".format(
            cbstats_mb, cbstats_mb / 1024))
        diff_du = abs(du_mb - cbstats_mb)
        diff_ap = abs(du_apparent_mb - cbstats_mb)
        self.log.info("Diff (du vs cbstats):       {} MB ({:.2f}%)".format(
            diff_du, (diff_du / max(du_mb, 1)) * 100))
        self.log.info("Diff (apparent vs cbstats): {} MB ({:.2f}%)".format(
            diff_ap, (diff_ap / max(du_apparent_mb, 1)) * 100))
        self.log.info("=" * 80)
        return du_mb, du_apparent_mb, cbstats_mb

    def test_load_data_and_validate_disk_usage(self):
        """
        Load num_items docs, then overwrite them overwrite_count times (default 2).
        Disk usage (du, du --apparent-size, cbstats) is captured at every
        capture_interval seconds throughout all phases.

        Parameters:
          num_items        – number of docs to create (controls total data size)
          overwrite_count  – how many full-overwrite passes to run after initial load (default 2)
          capture_interval – monitoring interval in seconds (default 10)
        """
        overwrite_count = self.input.param("overwrite_count", 2)
        bucket = self.cluster.buckets[0]
        self.log.info("Starting disk usage validation test")
        self.log.info("Bucket: {}, vBuckets: {}, Replicas: {}".format(
            bucket.name, bucket.numVBuckets, bucket.replicaNumber))
        self.log.info("num_items={}, doc_size={} bytes, overwrite_count={}".format(
            self.num_items, self.doc_size, overwrite_count))

        # Start background monitoring thread
        monitor_thread = threading.Thread(
            target=self.capture_disk_usage_loop,
            args=(bucket,))
        monitor_thread.daemon = True
        monitor_thread.start()

        self.create_start = 0
        self.create_end = self.num_items
        doc_loading_tm = TaskManager(self.process_concurrency)

        # Phase 1: initial load (create)
        self._run_loader_phase(
            "INITIAL LOADING STARTING — {} docs, doc_size={} bytes".format(
                self.num_items, self.doc_size),
            doc_loading_tm,
            self._build_loader_kwargs_list(create_pct=100, update_pct=0, read_pct=0, mutate_val=0))
        self._snapshot_disk_usage("After initial load", bucket)

        # Phase 2+: overwrite passes
        for i in range(1, overwrite_count + 1):
            self._run_loader_phase(
                "UPDATE ITERATION {}".format(i),
                doc_loading_tm,
                self._build_loader_kwargs_list(create_pct=0, update_pct=100, read_pct=0, mutate_val=i))
            self._snapshot_disk_usage(
                "After update iteration {}".format(i), bucket)

        # Stop monitoring and print full timeline summary
        self.stop_monitoring = True
        monitor_thread.join(timeout=30)
        self.print_disk_usage_summary()

    def _push_script_to_nodes(self):
        if not os.path.isfile(self.local_script_path):
            self.fail(
                "Local script not found at {} on runner. "
                "Ensure scripts/MB-71423_truncate_preallocation.sh is "
                "committed and present in the working tree.".format(
                    self.local_script_path))

        for server in self.cluster.servers:
            self.log.info("Pushing truncate script to {} ({} -> {})".format(
                server.ip, self.local_script_path, REMOTE_SCRIPT_PATH))
            scp_cmd = (
                'sshpass -p "{pwd}" scp -o StrictHostKeyChecking=no '
                '-o UserKnownHostsFile=/dev/null '
                '{src} {user}@{host}:{dst}'
            ).format(
                pwd=server.ssh_password,
                src=self.local_script_path,
                user=server.ssh_username,
                host=server.ip,
                dst=REMOTE_SCRIPT_PATH)
            result = subprocess.run(
                scp_cmd, shell=True, executable="/bin/bash",
                capture_output=True, text=True)
            if result.returncode != 0:
                self.fail(
                    "scp failed to {} (rc={}): stdout={!r} stderr={!r}".format(
                        server.ip, result.returncode,
                        result.stdout, result.stderr))

            shell = RemoteMachineShellConnection(server)
            shell.execute_command("chmod +x {}".format(REMOTE_SCRIPT_PATH))
            shell.disconnect()

    def _collect_disk_stats(self, tag):
        self.log.info("=" * 80)
        self.log.info("DISK STATS — {}".format(tag))
        self.log.info("=" * 80)

        cluster_du_total = 0
        cluster_apparent_total = 0

        for server in self.cluster.servers:
            self.log.info("  Node: {}".format(server.ip))
            self.log.info("  " + "-" * 76)

            cbstat = Cbstats(server)
            for bucket in self.cluster.buckets:
                du_mb, du_apparent_mb = self.get_du_stats(server, bucket)
                cluster_du_total += du_mb
                cluster_apparent_total += du_apparent_mb
                prealloc_gap_mb = du_apparent_mb - du_mb

                try:
                    cbstats_bytes = self.magma_utils.get_magma_disk_size(
                        server, bucket.name)
                    cbstats_mb = cbstats_bytes // (1024 * 1024)
                    diff_mb = abs(du_mb - cbstats_mb)
                    diff_pct = (diff_mb / max(du_mb, 1)) * 100
                    cbstats_line = "{:>10} MB  ({:.2f} GB)  [diff vs du: {} MB / {:.1f}%]".format(
                        cbstats_mb, cbstats_mb / 1024, diff_mb, diff_pct)
                except Exception:
                    cbstats_line = "N/A"

                self.log.info("  Bucket: {}".format(bucket.name))
                self.log.info("    du (blocks):          {:>10} MB  ({:.2f} GB)".format(
                    du_mb, du_mb / 1024))
                self.log.info("    du --apparent-size:   {:>10} MB  ({:.2f} GB)".format(
                    du_apparent_mb, du_apparent_mb / 1024))
                self.log.info("    cbstats disk (magma): {}".format(cbstats_line))
                self.log.info("    Preallocated gap:     {:>10} MB  ({:.2f} GB)  [apparent - du]".format(
                    prealloc_gap_mb, prealloc_gap_mb / 1024))

                try:
                    all_stats = cbstat.all_stats(bucket.name)
                    magma_stats = cbstat.magma_stats(bucket.name)

                    disk_keys = {k: v for k, v in all_stats.items()
                                 if any(kw in k for kw in ("disk", "magma", "ep_db", "ep_data"))}
                    if disk_keys:
                        self.log.info("    cbstats all (disk/magma/ep_db/ep_data):")
                        for k, v in disk_keys.items():
                            self.log.info("      {:<55s} {}".format(k, v))

                    if magma_stats:
                        self.log.info("    magma stats:")
                        for k, v in magma_stats.items():
                            self.log.info("      {:<55s} {}".format(k, v))
                except Exception as exc:
                    self.log.warning("    cbstats failed: {}".format(exc))

            cbstat.disconnect()
            self.log.info("")

        self.log.info("  " + "-" * 76)
        self.log.info("  CLUSTER TOTAL (sum across all nodes)")
        self.log.info("    du (blocks):        {:>10} MB  ({:.2f} GB)".format(
            cluster_du_total, cluster_du_total / 1024))
        self.log.info("    du --apparent-size: {:>10} MB  ({:.2f} GB)".format(
            cluster_apparent_total, cluster_apparent_total / 1024))
        total_gap = cluster_apparent_total - cluster_du_total
        self.log.info("    Preallocated gap:   {:>10} MB  ({:.2f} GB)".format(
            total_gap, total_gap / 1024))
        self.log.info("=" * 80)

    def _run_truncate_script_on_all_nodes(self):
        for server in self.cluster.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.cluster.buckets:
                bucket_path = os.path.join(self.data_path, bucket.uuid)
                verbose_flag = "-v " if self.verbose_script else ""
                cmd = "bash {} {}-t {} {}".format(REMOTE_SCRIPT_PATH, verbose_flag, self.max_age_secs, bucket_path)
                self.log.info(
                    "Running truncate script on {} for bucket {} path {}".format(
                        server.ip, bucket.name, bucket_path))
                out, err = shell.execute_command(cmd)
                self.log.info(
                    "Script output [{}]: {}".format(server.ip, "\n".join(out or [])))
                if err:
                    self.log.warning(
                        "Script stderr [{}]: {}".format(server.ip, "\n".join(err)))
            shell.disconnect()

    def _truncate_loop(self, stop_event):
        iteration = 0
        while not stop_event.is_set():
            iteration += 1
            self.log.info("Truncate script loop iteration {}".format(iteration))
            self._run_truncate_script_on_all_nodes()
            stop_event.wait(timeout=self.script_interval_secs)

    def _initial_load_and_push_script(self):
        self.create_start = 0
        self.create_end = self.num_items
        doc_loading_tm = TaskManager(self.process_concurrency)
        self._run_loader_phase(
            "Initial load",
            doc_loading_tm,
            self._build_loader_kwargs_list(
                create_pct=100, update_pct=0, read_pct=0, mutate_val=0))
        self._collect_disk_stats("BEFORE truncate script")
        self._push_script_to_nodes()

    def _crash_memcached_loop(self, stop_event):
        while not stop_event.is_set():
            for server in self.cluster.servers:
                self.log.info("Killing memcached on {}".format(server.ip))
                shell = RemoteMachineShellConnection(server)
                shell.kill_memcached()
                shell.disconnect()
            stop_event.wait(timeout=self.crash_interval_secs)

    def test_validate_disk_reclaim_after_truncate_preallocation(self):
        self.log.info("=== test_validate_disk_reclaim_after_truncate_preallocation ===")
        self._initial_load_and_push_script()
        self._run_truncate_script_on_all_nodes()
        self._collect_disk_stats("AFTER truncate script")

    def test_validate_disk_reclaim_during_concurrent_update_read_workload(self):
        self.log.info("=== test_validate_disk_reclaim_during_concurrent_update_read_workload ===")
        self.log.info("run_truncate_script={}".format(self.run_truncate_script))

        self._initial_load_and_push_script()

        stop_event = threading.Event()
        if self.run_truncate_script:
            truncate_thread = threading.Thread(
                target=self._truncate_loop, args=(stop_event,))
            truncate_thread.daemon = True
            truncate_thread.start()

        workload_tm = TaskManager(self.process_concurrency)
        for i in range(1, self.workload_iterations + 1):
            self.log.info("Starting 50/50 update:read workload iteration {}/{}".format(
                i, self.workload_iterations))
            self._run_loader_phase(
                "50/50 update:read workload iteration {}".format(i),
                workload_tm,
                self._build_loader_kwargs_list(
                    create_pct=0, update_pct=50, read_pct=50, mutate_val=i))

        if self.run_truncate_script:
            stop_event.set()
            truncate_thread.join(timeout=300)
            self._collect_disk_stats("AFTER truncate script + workload")

            self.log.info("Waiting 180 seconds before final truncate script run...")
            time.sleep(180)
            self._run_truncate_script_on_all_nodes()
            self._collect_disk_stats("AFTER final truncate script run")
        else:
            self._collect_disk_stats("AFTER workload (truncate script not run)")

    def test_validate_disk_reclaim_with_memcached_crashes_during_workload(self):
        self.log.info("=== test_validate_disk_reclaim_with_memcached_crashes_during_workload ===")
        self._initial_load_and_push_script()

        stop_event = threading.Event()
        truncate_thread = threading.Thread(
            target=self._truncate_loop, args=(stop_event,))
        truncate_thread.daemon = True
        truncate_thread.start()

        crash_thread = threading.Thread(
            target=self._crash_memcached_loop, args=(stop_event,))
        crash_thread.daemon = True
        crash_thread.start()

        workload_tm = TaskManager(self.process_concurrency)
        for i in range(1, self.workload_iterations + 1):
            self.log.info(
                "Starting 50/50 update:read workload with concurrent crashes "
                "iteration {}/{}".format(i, self.workload_iterations))
            self._run_loader_phase(
                "50/50 update:read workload with memcached crashes iteration {}".format(i),
                workload_tm,
                self._build_loader_kwargs_list(
                    create_pct=0, update_pct=50, read_pct=50, mutate_val=i))
        stop_event.set()
        truncate_thread.join(timeout=300)
        crash_thread.join(timeout=30)
        self._collect_disk_stats("AFTER truncate script + workload + crashes")

        self.log.info("Waiting 180 seconds before final truncate script run...")
        time.sleep(180)
        self._run_truncate_script_on_all_nodes()
        self._collect_disk_stats("AFTER final truncate script run")
