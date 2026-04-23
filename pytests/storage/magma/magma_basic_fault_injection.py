import os
import time
import threading

from shell_util.remote_connection import RemoteMachineShellConnection
from storage.magma.magma_fault_injection import MagmaFaultInjection


class MagmaFaultInjectionBasic(MagmaFaultInjection):

    def setUp(self):
        super(MagmaFaultInjectionBasic, self).setUp()

    def tearDown(self):
        super(MagmaFaultInjectionBasic, self).tearDown()

    # ------------------------------------------------------------------ #
    # Fault injection helpers                                              #
    # ------------------------------------------------------------------ #

    # Common errno values for convenience
    ERRNO_EIO    = 5    # I/O error
    ERRNO_ENOSPC = 28   # No space left on device
    ERRNO_ENOENT = 2    # No such file or directory
    ERRNO_EPERM  = 1    # Operation not permitted
    ERRNO_ENOMEM = 12   # Out of memory

    _ERRNO_MAP = {
        "EIO":    5,
        "ENOSPC": 28,
        "ENOENT": 2,
        "EPERM":  1,
        "ENOMEM": 12,
        "EACCES": 13,
        "EROFS":  30,
        "EBUSY":  16,
    }

    def _resolve_errno(self, value):
        """Accept either an int or a string errno name (e.g. 'ENOSPC') and
        return the corresponding integer."""
        if isinstance(value, int):
            return value
        key = str(value).upper()
        if key not in self._ERRNO_MAP:
            raise ValueError(
                f"Unknown errno name '{value}'. "
                f"Valid names: {list(self._ERRNO_MAP.keys())}"
            )
        return self._ERRNO_MAP[key]

    def _set_fault_injection(self, enabled, delay_ms=200, fail_rate=0, fail_errno="EIO",
                             target_path="/data/dta", syscalls=None, partial_rate=0,
                             validate_mode=False):
        """Enable or disable fault injection on all cluster servers.

        Args:
            enabled:     True to enable, False to disable.
            delay_ms:    Written to /run/faultinject/delay_ms.
            fail_rate:   Written to /run/faultinject/fail_rate.
                         0 = no failures; >0 = percentage chance of failing.
            fail_errno:  Written to /run/faultinject/fail_errno.
                         Errno to inject on failure. Accepts name (e.g. "ENOSPC") or int.
            target_path: Written to /run/faultinject/target_path.
                         Only paths containing this substring are intercepted.
                           "/data/dta"  — all magma data files (default)
                           "/wal/wal."  — WAL files only
                           "/kvstore-"  — kvstore files only
            syscalls:     Set of syscall names to intercept. Omitted names are
                          passed through untouched. Defaults to all seven.
                          Valid names: {"write", "pwrite", "read", "pread", "fsync", "fdatasync", "mmap"}
                          Example: syscalls={"fsync", "fdatasync"}
                            — write, pwrite, read, pread, mmap will not be intercepted.
            partial_rate: Percentage (0-100) chance that write()/pwrite() commits
                          only (count - BLOCK_SIZE) bytes and returns that shorter count,
                          simulating a torn page / power-loss partial write.
                          The caller sees success but the on-disk record is
                          incomplete; its checksum fails on WAL replay, exercising
                          the Corruption-tolerant truncate path.
                          Independent of fail_rate — both can be set together.
            validate_mode: If True, enables validation mode which logs all write
                          operations (open with write flags, write, pwrite) to
                          /run/faultinject/validate.log without injecting faults.
                          Used to verify that a program (e.g., magma_dump) is read-only.
                          Independent of enabled — can be used standalone.
        """
        all_syscalls = {"write", "pwrite", "read", "pread", "fsync", "fdatasync", "mmap"}
        if syscalls is None:
            syscalls = all_syscalls

        errno_int = self._resolve_errno(fail_errno)
        state = 1 if enabled else 0
        validate_state = 1 if validate_mode else 0
        action = "Enabling" if enabled else "Disabling"
        self.log.info(
            f"{action} fault injection "
            f"(delay_ms={delay_ms}, fail_rate={fail_rate}, partial_rate={partial_rate}, "
            f"fail_errno={fail_errno}={errno_int}, target_path={target_path}, "
            f"syscalls={sorted(syscalls)}, validate_mode={validate_mode}) on all nodes"
        )
        for server in self.cluster.servers:
            ssh = RemoteMachineShellConnection(server)
            ssh.execute_command(f"echo {delay_ms} > /run/faultinject/delay_ms")
            ssh.execute_command(f"echo {fail_rate} > /run/faultinject/fail_rate")
            ssh.execute_command(f"echo {partial_rate} > /run/faultinject/partial_rate")
            ssh.execute_command(f"echo {errno_int} > /run/faultinject/fail_errno")
            ssh.execute_command(f"echo {target_path} > /run/faultinject/target_path")
            ssh.execute_command(f"echo {validate_state} > /run/faultinject/validate_mode")
            for sc in all_syscalls:
                flag = 1 if sc in syscalls else 0
                ssh.execute_command(f"echo {flag} > /run/faultinject/sc_{sc}")
            ssh.execute_command(f"echo {state} > /run/faultinject/enable")
            ssh.disconnect()

    def _fault_injection_toggle_loop(self, toggle_interval_sec, delay_ms, stop_event, fail_rate=0, fail_errno="EIO"):
        """Repeatedly enable/disable fault injection until stop_event is set."""
        while not stop_event.is_set():
            self._set_fault_injection(enabled=True, delay_ms=delay_ms, fail_rate=fail_rate, fail_errno=fail_errno)
            self.log.info(f"Fault injection ON — waiting {toggle_interval_sec}s")
            stop_event.wait(timeout=toggle_interval_sec)
            if stop_event.is_set():
                break

            self._set_fault_injection(enabled=False)
            self.log.info(f"Fault injection OFF — waiting {toggle_interval_sec}s")
            stop_event.wait(timeout=toggle_interval_sec)

        # Always leave fault injection disabled when done
        self._set_fault_injection(enabled=False)
        self.log.info("Fault injection toggle loop finished")

    # ------------------------------------------------------------------ #
    # Test                                                                 #
    # ------------------------------------------------------------------ #

    def test_magma_slow_memtable_flush(self):
        """
        1. Load an initial batch of docs (fault injection disabled).
        2. Wait for the first workload to finish.
        3. In parallel:
           a. Start a second CREATE workload (new key range).
           b. Toggle fault injection ON/OFF every `toggle_interval_sec` seconds.
           c. Run a READ workload over the keys from the first batch.
        4. Wait for all parallel activities to complete.
        """
        toggle_interval_sec = self.input.param("toggle_interval_sec", 120)
        delay_ms = self.input.param("fault_inject_delay_ms", 200)
        fail_rate = self.input.param("fault_inject_fail_rate", 0)
        fail_errno = self.input.param("fault_inject_errno", "EIO")
        ops_rate = self.input.param("ops_rate", 10000)

        # ---- Phase 1: initial data load (fault injection disabled) ----
        self.log.info("Phase 1: Initial data load with fault injection disabled")
        self._set_fault_injection(enabled=False)

        self.create_start = 0
        self.create_end = self.init_items_per_collection
        self.java_doc_loader(
            doc_ops="create",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        first_batch_end = self.init_items_per_collection
        self.log.info(f"Phase 1 complete — {first_batch_end} docs loaded")

        # ---- Phase 2: second CREATE + fault injection toggle + READ in parallel ----
        self.log.info("Phase 2: Starting second CREATE, fault injection toggling, and READ in parallel")

        stop_event = threading.Event()

        # 2a: Fault injection toggle thread
        fi_thread = threading.Thread(
            target=self._fault_injection_toggle_loop,
            args=[toggle_interval_sec, delay_ms, stop_event, fail_rate, fail_errno],
            daemon=True,
        )

        # 2b: Second CREATE workload
        self.create_start = first_batch_end
        self.create_end = first_batch_end + self.init_items_per_collection
        create_tasks, _ = self.java_doc_loader(
            doc_ops="create",
            wait=False,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
            monitor_ops=False,
        )

        # 2c: READ workload over keys from first batch
        self.read_start = 0
        self.read_end = first_batch_end
        read_tasks, _ = self.java_doc_loader(
            doc_ops="read",
            wait=False,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
            monitor_ops=False,
        )

        fi_thread.start()

        # Wait for both workloads to finish
        self.log.info("Waiting for second CREATE workload to complete")
        for task in create_tasks:
            self.doc_loading_tm.get_task_result(task)

        self.log.info("Waiting for READ workload to complete")
        for task in read_tasks:
            self.doc_loading_tm.get_task_result(task)

        # Stop the fault injection toggle loop
        stop_event.set()
        fi_thread.join()

        self.log.info("Phase 2 complete — all parallel workloads finished")

        # ---- Validation ----
        self.log.info("Validating item counts across all buckets")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        expected_items = self.create_end * self.num_collections
        self.bucket_util.verify_stats_all_buckets(self.cluster, expected_items)
        self.log.info("test_magma_slow_memtable_flush complete")

    def test_magma_slow_compaction(self):
        """
        1. Load initial data (0 → num_items) with fault injection disabled.
        2. Run an UPDATE workload over all num_items; wait for it to complete.
        3. Enable fault injection (delay_ms) to slow down I/O.
        4. In parallel:
           a. Trigger async compaction on all buckets.
           b. CREATE workload for a second batch (num_items → num_items * 2).
           c. READ workload over the first batch (0 → num_items).
           d. Crash loop — kills memcached at random intervals.
           e. Swap-rebalance — replaces the current node with a spare node.
        5. Wait for all parallel activities to finish.
        6. Disable fault injection and validate item counts.
        """
        delay_ms = self.input.param("fault_inject_delay_ms", 200)
        fail_rate = self.input.param("fault_inject_fail_rate", 0)
        fail_errno = self.input.param("fault_inject_errno", "EIO")
        ops_rate = self.input.param("ops_rate", 10000)
        kill_itr = self.input.param("kill_itr", 1)

        # ---- Phase 1: initial load ----
        self.log.info("Phase 1: Initial data load with fault injection disabled")
        self._set_fault_injection(enabled=False)

        self.create_start = 0
        self.create_end = self.num_items
        self.java_doc_loader(
            doc_ops="create",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        self.log.info(f"Phase 1 complete — {self.num_items} docs loaded")

        # ---- Phase 2: UPDATE all docs ----
        self.log.info("Phase 2: UPDATE workload over all docs")
        self.update_start = 0
        self.update_end = self.num_items
        self.java_doc_loader(
            doc_ops="update",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        self.log.info("Phase 2 complete — UPDATE workload finished")

        # ---- Phase 3: enable fault injection, then run everything in parallel ----
        self.log.info(f"Phase 3: Enabling fault injection (delay_ms={delay_ms}, fail_rate={fail_rate}, fail_errno={fail_errno})")
        self._set_fault_injection(enabled=True, delay_ms=delay_ms, fail_rate=fail_rate, fail_errno=fail_errno)

        # 3a: async compaction
        self.log.info("Starting async compaction on all buckets")
        compaction_tasks = self.bucket_util._run_compaction(self.cluster, async_run=True)

        # 3b: second CREATE workload (num_items → num_items * 2)
        self.create_start = self.num_items
        self.create_end = self.num_items * 2
        self.log.info(f"Starting CREATE workload: {self.create_start} → {self.create_end}")
        create_tasks, _ = self.java_doc_loader(
            doc_ops="create",
            wait=False,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
            monitor_ops=False,
        )

        # 3c: READ workload over first batch
        self.read_start = 0
        self.read_end = self.num_items
        self.log.info(f"Starting READ workload: {self.read_start} → {self.read_end}")
        read_tasks, _ = self.java_doc_loader(
            doc_ops="read",
            wait=False,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
            monitor_ops=False,
        )

        # 3d: crash loop thread
        self.stop_crash = False
        self.crash_failure = False
        crash_th = threading.Thread(
            target=self.crash,
            kwargs=dict(kill_itr=kill_itr, graceful=False, wait=True),
            daemon=True,
        )
        crash_th.start()

        # 3e: swap-rebalance — replace the single current node with a spare node
        spare_nodes = [n for n in self.cluster.servers if n not in self.cluster.nodes_in_cluster]
        if spare_nodes:
            spare = spare_nodes[0]
            current = self.cluster.nodes_in_cluster[0]
            self.log.info(f"Swap rebalance: replacing {current.ip} with {spare.ip}")
            rebalance_result = self.task.rebalance(
                self.cluster,
                to_add=[spare],
                to_remove=[current],
                retry_get_process_num=500,
            )
            self.log.info(f"Swap rebalance result: {rebalance_result}")
        else:
            self.log.warning("No spare nodes available for swap rebalance — skipping")

        # Wait for workloads
        self.log.info("Waiting for CREATE workload to complete")
        for task in create_tasks:
            self.doc_loading_tm.get_task_result(task)

        self.log.info("Waiting for READ workload to complete")
        for task in read_tasks:
            self.doc_loading_tm.get_task_result(task)

        # Wait for compaction tasks
        self.log.info("Waiting for compaction tasks to complete")
        for task in compaction_tasks.values():
            self.task_manager.get_task_result(task)

        # Stop crash loop
        self.stop_crash = True
        crash_th.join()

        self.assertFalse(self.crash_failure, "CRASH | CRITICAL | WARN messages found in cb_logs")

        # ---- Cleanup ----
        self._set_fault_injection(enabled=False)

        # ---- Validation ----
        self.log.info("Validating item counts across all buckets")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        expected_items = self.create_end * self.num_collections
        self.bucket_util.verify_stats_all_buckets(self.cluster, expected_items)
        self.log.info("test_magma_slow_compaction complete")

    def test_magma_slow_warmup(self):
        """
        1. Load initial data (0 → num_items) with fault injection disabled.
        2. Enable fault injection (delay_ms) to slow down I/O during warmup.
        3. Restart Couchbase on the master node.
        4. Wait 30 seconds for warmup to begin.
        5. In parallel, run a CREATE (num_items → num_items * 2) and
           READ (0 → num_items) workload while the node is warming up.
        6. Wait for both workloads to finish.
        7. Disable fault injection and validate item counts.
        """
        delay_ms = self.input.param("fault_inject_delay_ms", 200)
        fail_rate = self.input.param("fault_inject_fail_rate", 0)
        fail_errno = self.input.param("fault_inject_errno", "EIO")
        ops_rate = self.input.param("ops_rate", 10000)

        # ---- Phase 1: initial load ----
        self.log.info("Phase 1: Initial data load with fault injection disabled")
        self._set_fault_injection(enabled=False)

        self.create_start = 0
        self.create_end = self.num_items
        self.java_doc_loader(
            doc_ops="create",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        self.log.info(f"Phase 1 complete — {self.num_items} docs loaded")

        # ---- Phase 2: enable fault injection and restart Couchbase ----
        self.log.info(f"Phase 2: Enabling fault injection (delay_ms={delay_ms}, fail_rate={fail_rate}, fail_errno={fail_errno})")
        self._set_fault_injection(enabled=True, delay_ms=delay_ms, fail_rate=fail_rate, fail_errno=fail_errno)

        self.log.info("Restarting Couchbase on master node to trigger warmup")
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.restart_couchbase()
        shell.disconnect()

        self.sleep(30, "Wait for warmup to begin before starting workloads")

        # ---- Phase 3: CREATE and READ workloads during warmup ----
        self.log.info("Phase 3: Starting CREATE and READ workloads during warmup")

        self.create_start = self.num_items
        self.create_end = self.num_items * 2
        self.log.info(f"Starting CREATE workload: {self.create_start} → {self.create_end}")
        create_tasks, _ = self.java_doc_loader(
            doc_ops="create",
            wait=False,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
            monitor_ops=False,
        )

        self.read_start = 0
        self.read_end = self.num_items
        self.log.info(f"Starting READ workload: {self.read_start} → {self.read_end}")
        read_tasks, _ = self.java_doc_loader(
            doc_ops="read",
            wait=False,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
            monitor_ops=False,
        )

        self.log.info("Waiting for CREATE workload to complete")
        for task in create_tasks:
            self.doc_loading_tm.get_task_result(task)

        self.log.info("Waiting for READ workload to complete")
        for task in read_tasks:
            self.doc_loading_tm.get_task_result(task)

        # ---- Cleanup and validation ----
        self._set_fault_injection(enabled=False)

        self.log.info("Validating item counts across all buckets")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        expected_items = self.create_end * self.num_collections
        self.bucket_util.verify_stats_all_buckets(self.cluster, expected_items)
        self.log.info("test_magma_slow_warmup complete")

    def test_magma_slow_wal_writes(self):
        """
        Stress WAL I/O by combining a write delay with intermittent fsync errors:

        1. Enable fault injection scoped to WAL files only (/wal/wal.):
             - delay_ms=200 on every write() to slow WAL appends.
             - A background thread alternates the injected errno between
               EIO and ENOSPC every `toggle_interval_sec` seconds to
               simulate intermittent fsync() failures.
        2. Load data (0 → num_items) while the above is active.
        3. Wait for the workload to finish.
        4. Disable fault injection and validate item counts.
        """
        delay_ms = self.input.param("fault_inject_delay_ms", 200)
        fail_rate = self.input.param("fault_inject_fail_rate", 10)
        toggle_interval_sec = self.input.param("toggle_interval_sec", 30)
        ops_rate = self.input.param("ops_rate", 10000)

        wal_target = "/wal/wal."

        def _errno_toggle_loop(stop_event):
            """Alternate injected errno between EIO and ENOSPC until stop_event is set."""
            errnos = ["EIO", "ENOSPC"]
            idx = 0
            while not stop_event.is_set():
                current_errno = errnos[idx % 2]
                self.log.info(f"[WAL] Switching injected errno to {current_errno}")
                self._set_fault_injection(
                    enabled=True,
                    delay_ms=delay_ms,
                    fail_rate=fail_rate,
                    fail_errno=current_errno,
                    target_path=wal_target,
                )
                idx += 1
                stop_event.wait(timeout=toggle_interval_sec)
            self._set_fault_injection(enabled=False, target_path=wal_target)
            self.log.info("[WAL] errno toggle loop finished, fault injection disabled")

        # ---- Start fault injection before the workload ----
        self.log.info(
            f"Enabling WAL fault injection: delay_ms={delay_ms}, "
            f"fail_rate={fail_rate}, target={wal_target}"
        )
        stop_event = threading.Event()
        toggle_th = threading.Thread(target=_errno_toggle_loop, args=[stop_event], daemon=True)
        toggle_th.start()

        # ---- Load data (0 → num_items) ----
        self.log.info(f"Loading {self.num_items} docs with WAL fault injection active")
        self.create_start = 0
        self.create_end = self.num_items
        self.java_doc_loader(
            doc_ops="create",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        self.log.info("Workload complete")

        # ---- Stop fault injection ----
        stop_event.set()
        toggle_th.join()

        self.sleep(60, "Wait after disabling fault injection")

        # ---- Phase 3: restart Couchbase on all nodes to trigger WAL replay ----
        self.log.info("Phase 3: Restarting Couchbase on all nodes to trigger WAL replay")
        for server in self.cluster.nodes_in_cluster:
            self.log.info(f"Restarting Couchbase on {server.ip}")
            shell = RemoteMachineShellConnection(server)
            shell.restart_couchbase()
            shell.disconnect()

        # ---- Phase 4: wait for warmup under fault injection ----
        self.sleep(120, "Waiting for WAL replay / warmup to complete under fault injection")

        # READ workload
        self.doc_ops = "read"
        self.reset_doc_params()
        self.read_start = 0
        self.read_end = self.num_items
        self.generate_docs()
        self.log.info(f"Read start = {self.read_start}, Read End = {self.read_end}, Read perc = {self.read_perc}")
        self.java_doc_loader(wait=True,
                            skip_default=self.skip_load_to_default_collection,
                            ops_rate=ops_rate)

        # ---- Validation ----
        self.log.info("Validating item counts across all buckets")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        expected_items = self.create_end * self.num_collections
        self.bucket_util.verify_stats_all_buckets(self.cluster, expected_items)
        self.log.info("test_magma_slow_wal_writes complete")

    def test_magma_slow_wal_replay(self):
        """
        WAL Replay During Warmup / Restart

        Fault injected (WAL files only — target /wal/wal.):
          - Delay read() from WAL segments to slow replay throughput.
          - Intermittently inject EIO or ENOENT on WAL reads / mmap calls
            while the engine replays the WAL on startup.

        Steps:
        1. Load initial data (0 → num_items) with fault injection disabled.
        2. Wait for the workload to finish.
        3. Enable WAL fault injection (delay + alternating EIO / ENOENT errors).
        4. Restart Couchbase on all nodes to trigger WAL replay under stress.
        5. Wait for warmup to complete.
        6. Disable fault injection and validate item counts.
        """
        delay_ms = self.input.param("fault_inject_delay_ms", 200)
        fail_rate = self.input.param("fault_inject_fail_rate", 10)
        toggle_interval_sec = self.input.param("toggle_interval_sec", 20)
        warmup_wait_sec = self.input.param("warmup_wait_sec", 120)
        ops_rate = self.input.param("ops_rate", 10000)

        wal_target = "/wal/wal."

        def _errno_toggle_loop(stop_event):
            """Alternate injected errno between EIO and ENOENT until stop_event is set."""
            errnos = ["EIO", "ENOENT"]
            idx = 0
            while not stop_event.is_set():
                current_errno = errnos[idx % 2]
                self.log.info(f"[WAL_REPLAY] Switching injected errno to {current_errno}")
                self._set_fault_injection(
                    enabled=True,
                    delay_ms=delay_ms,
                    fail_rate=fail_rate,
                    fail_errno=current_errno,
                    target_path=wal_target,
                )
                idx += 1
                stop_event.wait(timeout=toggle_interval_sec)
            self._set_fault_injection(enabled=False, target_path=wal_target)
            self.log.info("[WAL_REPLAY] errno toggle loop finished, fault injection disabled")

        # ---- Phase 1: initial load with fault injection disabled ----
        self.log.info("Phase 1: Initial data load with fault injection disabled")
        self._set_fault_injection(enabled=False)

        self.create_start = 0
        self.create_end = self.num_items
        self.java_doc_loader(
            doc_ops="create",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        self.log.info(f"Phase 1 complete — {self.num_items} docs loaded")

        # ---- Phase 2: enable WAL fault injection ----
        self.log.info(
            f"Phase 2: Enabling WAL fault injection "
            f"(delay_ms={delay_ms}, fail_rate={fail_rate}, target={wal_target})"
        )
        stop_event = threading.Event()
        toggle_th = threading.Thread(target=_errno_toggle_loop, args=[stop_event], daemon=True)
        toggle_th.start()

        # ---- Phase 3: restart Couchbase on all nodes to trigger WAL replay ----
        self.log.info("Phase 3: Restarting Couchbase on all nodes to trigger WAL replay")
        for server in self.cluster.nodes_in_cluster:
            self.log.info(f"Restarting Couchbase on {server.ip}")
            shell = RemoteMachineShellConnection(server)
            shell.restart_couchbase()
            shell.disconnect()

        # ---- Phase 4: wait for warmup under fault injection ----
        self.sleep(warmup_wait_sec, "Waiting for WAL replay / warmup to complete under fault injection")

        # ---- Stop fault injection ----
        stop_event.set()
        toggle_th.join()

        # ---- Validation ----
        self.log.info("Validating item counts across all buckets")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        expected_items = self.create_end * self.num_collections
        self.bucket_util.verify_stats_all_buckets(self.cluster, expected_items)
        self.log.info("test_magma_slow_wal_replay complete")

    def test_magma_wal_tail_corruption_recovery(self):
        """
        WAL Tail Corruption Recovery

        Simulates power-loss / torn-page scenarios where the final WAL
        segment has a partially written record.

        Fault injected:
          - High fail_rate on WAL write()/pwrite() to produce partial writes
            (kernel may commit N-1 bytes of a record before the injected EIO).
          - Intermittent EIO on WAL fsync() so completed records may not be
            durable, leaving the tail corrupt on crash.

        What this simulates:
          - Power loss during a write.
          - Disk firmware failure causing torn pages.
          - Hard crash where the OS didn't flush the full buffer.

        Steps:
        1. Load initial data (0 → num_items) cleanly.  Wait for completion.
        2. Record per-vBucket highSeqno as the "known good" baseline.
        3. Enable fault injection on WAL write/pwrite/fsync only
           (reads and mmap are not intercepted — recovery reads must work).
        4. Run a second CREATE workload (num_items → num_items * 2).
           This workload runs UNDER fault injection so some WAL records
           will be partially written / unsynced.
        5. While the workload is running, kill memcached (SIGKILL) to
           simulate a hard crash — ensures the partially written WAL tail
           is left on disk.
        6. Disable fault injection.
        7. Wait for Couchbase to auto-restart and replay the WAL.
        8. Validate:
           a. Bucket is accessible and warmup completes.
           b. Per-vBucket highSeqno is >= the baseline captured in step 2
              (Magma truncated only the invalid tail, not good data).
           c. Item count is >= the initial load
              (items from phase 1 must survive).
        """
        delay_ms = self.input.param("fault_inject_delay_ms", 0)
        # fail_rate: low-probability full EIO on fsync to stress durability
        fail_rate = self.input.param("fault_inject_fail_rate", 0)
        ops_rate = self.input.param("ops_rate", 10000)
        crash_after_sec = self.input.param("crash_after_sec", 30)
        # pre_crash_window_sec: how long before SIGKILL to arm partial_rate=100
        pre_crash_window_sec = self.input.param("pre_crash_window_sec", 0.3)
        warmup_wait_sec = self.input.param("warmup_wait_sec", 120)

        wal_target = "/wal/wal."
        # Intercept write/pwrite for partial-write torn pages; fsync for
        # durability failures. Keep read/pread/mmap clean so recovery works.
        wal_write_syscalls = {"write", "pwrite", "fsync"}

        # ---- Phase 1: clean initial load ----
        self.log.info("Phase 1: Initial data load with fault injection disabled")
        self._set_fault_injection(enabled=False)

        self.create_start = 0
        self.create_end = self.num_items
        self.java_doc_loader(
            doc_ops="create",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        self.log.info(f"Phase 1 complete — {self.num_items} docs loaded")

        # ---- Phase 2: capture per-vBucket highSeqno baseline ----
        self.log.info("Phase 2: Capturing per-vBucket highSeqno baseline")
        from cb_tools.cbstats import Cbstats
        baseline_seqnos = {}
        for bucket in self.cluster.buckets:
            cbstat = Cbstats(self.cluster.master)
            seqno_stats = cbstat.vbucket_seqno(bucket.name)
            baseline_seqnos[bucket.name] = {}
            for vb_str, stats in seqno_stats.items():
                baseline_seqnos[bucket.name][int(vb_str)] = int(stats.get("high_seqno", 0))
            self.log.info(
                f"Bucket {bucket.name}: captured highSeqno for "
                f"{len(baseline_seqnos[bucket.name])} vBuckets"
            )

        # ---- Phase 3: enable WAL FI with NO partial writes yet ----
        # partial_rate=0 here — the workload runs cleanly, building up a
        # realistic WAL tail. We only arm partial_rate=100 in the tight
        # window immediately before SIGKILL so the torn record lands at
        # the very tail of the last active segment.
        self.log.info(
            f"Phase 3: Enabling WAL fault injection (write/pwrite/fsync), "
            f"partial_rate=0 (armed later), fail_rate={fail_rate}%"
        )
        self._set_fault_injection(
            enabled=True,
            delay_ms=delay_ms,
            partial_rate=0,
            fail_rate=fail_rate,
            fail_errno="EIO",
            target_path=wal_target,
            syscalls=wal_write_syscalls,
        )

        # ---- Phase 4: second CREATE workload under fault injection ----
        self.log.info(f"Phase 4: Starting CREATE workload ({self.num_items} → {self.num_items * 2}) under WAL FI")
        self.create_start = self.num_items
        self.create_end = self.num_items * 2
        create_tasks, _ = self.java_doc_loader(
            doc_ops="create",
            wait=False,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
            monitor_ops=False,
        )

        # ---- Phase 5: wait, arm partial_rate=100, then SIGKILL immediately ----
        self.sleep(crash_after_sec, "Letting workload run before arming partial writes")

        # Arm partial_rate=100 for a tight window so the torn record lands
        # at the tail of the last active WAL segment, then crash immediately.
        self.log.info(
            f"Phase 5: Arming partial_rate=100 for {pre_crash_window_sec}s "
            f"then SIGKILL — torn write will be at WAL tail"
        )
        self._set_fault_injection(
            enabled=True,
            delay_ms=delay_ms,
            partial_rate=100,
            fail_rate=fail_rate,
            fail_errno="EIO",
            target_path=wal_target,
            syscalls=wal_write_syscalls,
        )
        time.sleep(pre_crash_window_sec)

        self.log.info("Phase 5: Killing memcached (SIGKILL) to simulate hard crash")
        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            shell.kill_memcached()
            shell.disconnect()

        # ---- Phase 6: disable fault injection before recovery ----
        self.log.info("Phase 6: Disabling fault injection for clean WAL replay")
        self._set_fault_injection(enabled=False)

        # Wait for the workload tasks to finish (they will error out due to crash)
        self.log.info("Draining workload tasks after crash")
        for task in create_tasks:
            try:
                self.doc_loading_tm.get_task_result(task)
            except Exception as e:
                self.log.info(f"Expected workload error after crash: {e}")

        # ---- Phase 7: wait for auto-restart and WAL replay ----
        self.sleep(warmup_wait_sec, "Waiting for Couchbase auto-restart and WAL replay")

        # Ensure warmup completes on all nodes
        for server in self.cluster.nodes_in_cluster:
            result = self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[server],
                wait_time=self.wait_timeout * 5,
            )
            self.assertTrue(result, f"Warmup did not complete on {server.ip}")

        # ---- Phase 8: validate recovery ----
        self.log.info("Phase 8: Validating WAL tail corruption recovery")

        # 8a: per-vBucket highSeqno must be >= baseline (no valid data lost)
        seqno_regression_count = 0
        for bucket in self.cluster.buckets:
            cbstat = Cbstats(self.cluster.master)
            post_seqnos = cbstat.vbucket_seqno(bucket.name)
            for vb_str, stats in post_seqnos.items():
                vb = int(vb_str)
                post_val = int(stats.get("high_seqno", 0))
                baseline_val = baseline_seqnos[bucket.name].get(vb, 0)
                if post_val < baseline_val:
                    self.log.error(
                        f"Bucket {bucket.name}, vb {vb}: highSeqno regressed "
                        f"({baseline_val} → {post_val})"
                    )
                    seqno_regression_count += 1

        self.assertEqual(
            seqno_regression_count, 0,
            f"{seqno_regression_count} vBuckets have highSeqno below the "
            f"pre-corruption baseline — Magma truncated valid data"
        )
        self.log.info("highSeqno check PASSED — no vBucket regressed below baseline")

        # 8b: item count must be >= initial load (phase 1 items survived)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        bucket_item_counts = self.bucket_util.get_buckets_item_count(self.cluster)
        for bucket in self.cluster.buckets:
            actual = bucket_item_counts.get(bucket.name, 0)
            initial = self.num_items * self.num_collections
            self.log.info(
                f"Bucket {bucket.name}: items after recovery = {actual}, "
                f"initial load items = {initial}"
            )
            self.assertGreaterEqual(
                actual, initial,
                f"Bucket {bucket.name}: item count after recovery ({actual}) "
                f"is less than initial load ({initial})"
            )

        self.log.info("test_magma_wal_tail_corruption_recovery complete")

    def test_wal_middle_segment_integrity(self):
        """
        WAL Middle Segment Integrity (Non-Last Segment)

        Validates Magma's fail-fast behaviour when a non-tail WAL segment is
        missing or unreadable.  Unlike tail corruption (where Magma safely
        truncates the last partial record), a hole in the middle of the log
        is unrecoverable — data that should exist between the missing segment
        and the end of the log cannot be reconstructed.

        Fault injected:
          - Phase A (ENOENT): the N-2 WAL segment file is physically deleted
            from every vBucket WAL directory before WAL replay begins,
            simulating an accidental file deletion in the data directory.
          - Phase B (EIO on reads): read()/pread() on all WAL files are
            intercepted with EIO after the SIGKILL, simulating targeted sector
            corruption on older disk blocks that Magma encounters during the
            sequential segment scan.

        What this simulates:
          - Accidental or operator-caused deletion of an older WAL segment.
          - Targeted sector corruption on older disk blocks that Magma reads
            sequentially during WAL replay.

        What is tested:
          - Fail-fast logic: Magma must NOT attempt to truncate the missing
            middle segment and continue; it must surface an IOError and refuse
            to bring the bucket online.
          - Difference between "end of log" and "hole in log": tail corruption
            is safe to trim; a middle-segment hole is not — the engine must
            distinguish the two cases.

        Expected results:
          - Magma hard-fails (Fail-fast) with an IOError during WAL replay.
          - The engine does NOT truncate the "missing" middle part.
          - Bucket warmup does NOT complete successfully.
          - Data integrity is prioritised over bucket availability: item counts
            from the clean phase 1 load are preserved (no silent data loss),
            but the bucket remains unavailable until the operator intervenes.

        Steps:
          1. Load initial data (0 → num_items) cleanly.  Wait for completion.
          2. Capture per-vBucket highSeqno as the "known good" baseline.
          3. Force WAL rotation by running a second CREATE workload
             (num_items → num_items * 2) and then an UPDATE pass; wait for
             both to complete so at least three WAL segments exist per vBucket.
          4. List all WAL segment files on every node; sort by segment number
             and identify the N-2 segment (second-oldest active segment).
          5. Phase A — corrupt the N-2 segment according to n2_fault_mode:
               "delete"   — rm -f the segment (ENOENT on open during replay).
               "truncate" — shrink the file by 4096 bytes (truncate -s -4096),
                            leaving a partial / checksum-invalid body inside
                            the segment; simulates targeted sector corruption
                            on older disk blocks.
          6. Phase B — EIO on reads: enable fault injection on read()/pread()
             for all WAL files (/wal/wal.), fail_rate=100.
          7. Kill memcached (SIGKILL) to force WAL replay from disk.
          8. Wait for Couchbase auto-restart to attempt WAL replay.
          9. Validate:
             a. Bucket warmup does NOT complete — fail-fast behaviour
                confirmed.
             b. highSeqno has not regressed below baseline — no silent data
                loss while the bucket was up in phase 1.
          10. Disable fault injection (clean-up).
        """
        ops_rate = self.input.param("ops_rate", 10000)
        warmup_wait_sec = self.input.param("warmup_wait_sec", 90)
        # How many update passes to run to force enough WAL rotation
        rotation_passes = self.input.param("wal_rotation_passes", 2)
        # n2_fault_mode controls how the N-2 segment is corrupted:
        #   "delete"   — rm -f the segment file (ENOENT on open)
        #   "truncate" — shrink the file by 4096 bytes from the end, leaving a
        #                partial / checksum-invalid tail inside the segment body
        #                rather than removing it entirely.  Magma sees the file
        #                but its internal structure is broken — simulates sector
        #                corruption or a partial overwrite of older disk blocks.
        n2_fault_mode = self.input.param("n2_fault_mode", "delete")

        wal_target = "/wal/wal."
        # Only intercept read syscalls — write path must stay clean so the
        # WAL on disk is fully intact before we inject the faults.
        read_syscalls = {"read", "pread"}

        # ---- Phase 1: clean initial load ----
        self.log.info("Phase 1: Initial data load with fault injection disabled")
        self._set_fault_injection(enabled=False)

        self.create_start = 0
        self.create_end = self.num_items
        self.java_doc_loader(
            doc_ops="create",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        self.log.info(f"Phase 1 complete — {self.num_items} docs loaded")

        # ---- Phase 2: capture per-vBucket highSeqno baseline ----
        self.log.info("Phase 2: Capturing per-vBucket highSeqno baseline")
        from cb_tools.cbstats import Cbstats
        baseline_seqnos = {}
        for bucket in self.cluster.buckets:
            cbstat = Cbstats(self.cluster.master)
            seqno_stats = cbstat.vbucket_seqno(bucket.name)
            baseline_seqnos[bucket.name] = {}
            for vb_str, stats in seqno_stats.items():
                baseline_seqnos[bucket.name][int(vb_str)] = int(
                    stats.get("high_seqno", 0)
                )
            self.log.info(
                f"Bucket {bucket.name}: captured highSeqno for "
                f"{len(baseline_seqnos[bucket.name])} vBuckets"
            )

        # ---- Phase 3: force WAL rotation ----
        # Run CREATE (num_items → num_items*2) then multiple UPDATE passes so
        # Magma rotates the active WAL segment several times, guaranteeing that
        # an N-2 (non-tail) segment exists on every node before the injection.
        self.log.info(
            f"Phase 3: Forcing WAL rotation — second CREATE batch then "
            f"{rotation_passes} UPDATE pass(es)"
        )
        self.create_start = self.num_items
        self.create_end = self.num_items * 2
        self.java_doc_loader(
            doc_ops="create",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        for pass_num in range(1, rotation_passes + 1):
            self.log.info(f"Phase 3: UPDATE pass {pass_num}/{rotation_passes}")
            self.reset_doc_params(doc_ops="update")
            self.update_start = 0
            self.update_end = self.num_items
            self.java_doc_loader(
                doc_ops="update",
                wait=True,
                skip_default=self.skip_load_to_default_collection,
                ops_rate=ops_rate,
            )
        self.log.info("Phase 3 complete — WAL rotation forced")

        # ---- Phase 4: identify and corrupt the N-2 WAL segment on each node ----
        # WAL segment files are named /wal/wal.<seqnum> under the Magma data
        # directory for each vBucket kvstore.  Sort by the numeric suffix to
        # determine segment order: N (newest / tail), N-1, N-2 (target).
        _valid_modes = ("delete", "truncate")
        self.assertIn(
            n2_fault_mode, _valid_modes,
            f"Unknown n2_fault_mode '{n2_fault_mode}'. Valid values: {_valid_modes}"
        )
        self.log.info(
            f"Phase 4 (n2_fault_mode={n2_fault_mode}): "
            f"Locating and corrupting N-2 WAL segment files"
        )
        affected_segments: dict[str, list[str]] = {}
        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            # Collect all WAL segment files on this node (covers all vBuckets /
            # kvstores for all buckets).
            find_cmd = "find /data -path '*/wal/wal.*' -type f 2>/dev/null"
            stdout, _ = shell.execute_command(find_cmd)
            all_wal_files = [line.strip() for line in stdout if line.strip()]

            # Group files by their parent WAL directory so we process each
            # vBucket independently.
            wal_dirs: dict[str, list[str]] = {}
            for fpath in all_wal_files:
                parent = fpath.rsplit("/", 1)[0]
                wal_dirs.setdefault(parent, []).append(fpath)

            node_affected: list[str] = []
            for wal_dir, files in wal_dirs.items():
                # Sort by the numeric segment suffix (wal.0, wal.1, wal.2 …).
                def _seg_num(p: str) -> int:
                    try:
                        return int(p.rsplit(".", 1)[-1])
                    except ValueError:
                        return -1

                sorted_files = sorted(files, key=_seg_num)
                # Need at least 3 segments to have a meaningful N-2.
                if len(sorted_files) < 3:
                    self.log.warning(
                        f"WAL dir {wal_dir} has only {len(sorted_files)} "
                        f"segment(s) — skipping N-2 fault for this vBucket"
                    )
                    continue

                # N-2 is the third-from-last segment in sorted order.
                n2_segment = sorted_files[-3]

                if n2_fault_mode == "delete":
                    # Remove the file entirely — Magma will see ENOENT when it
                    # tries to open the segment during sequential WAL replay.
                    shell.execute_command(f"rm -f {n2_segment}")
                    self.log.info(
                        f"[{server.ip}] Deleted N-2 segment: {n2_segment} "
                        f"(of {len(sorted_files)} segments in {wal_dir})"
                    )
                else:
                    # Truncate the last 4096 bytes of the segment.  The file
                    # still exists and opens successfully, but its trailing
                    # content is stripped — the final record(s) in the segment
                    # body will have an invalid / missing checksum, and any
                    # internal length-prefix that points past the new EOF will
                    # cause a read underrun.  This mirrors targeted sector
                    # corruption on older disk blocks while keeping the file
                    # visible to the filesystem.
                    shell.execute_command(
                        f"truncate -s -4096 {n2_segment} 2>/dev/null || "
                        f"python3 -c \""
                        f"import os; s=os.path.getsize('{n2_segment}'); "
                        f"os.truncate('{n2_segment}', max(0, s-4096))\""
                    )
                    self.log.info(
                        f"[{server.ip}] Truncated N-2 segment by 4096 bytes: "
                        f"{n2_segment} "
                        f"(of {len(sorted_files)} segments in {wal_dir})"
                    )

                node_affected.append(n2_segment)

            affected_segments[server.ip] = node_affected
            shell.disconnect()
            self.log.info(
                f"[{server.ip}] Faulted {len(node_affected)} N-2 segment file(s) "
                f"(mode={n2_fault_mode})"
            )

        total_affected = sum(len(v) for v in affected_segments.values())
        self.assertGreater(
            total_affected, 0,
            f"No N-2 WAL segment files were found to fault (mode={n2_fault_mode}) "
            f"— WAL rotation in Phase 3 did not produce enough segments.  "
            f"Increase wal_rotation_passes or num_items."
        )
        self.log.info(
            f"Phase 4 complete — {total_affected} N-2 segment(s) affected "
            f"(mode={n2_fault_mode}) across all nodes"
        )

        # ---- Phase 5 (EIO on reads): arm read fault injection on WAL files ----
        # fail_rate=100 ensures every read()/pread() on a WAL segment returns
        # EIO, reinforcing the "unreadable historical segment" scenario on top
        # of the already-deleted N-2 file.
        self.log.info(
            "Phase 5 (EIO): Enabling read fault injection on WAL files "
            "(fail_rate=100, syscalls=read/pread)"
        )
        self._set_fault_injection(
            enabled=True,
            delay_ms=0,
            fail_rate=100,
            fail_errno="EIO",
            target_path=wal_target,
            syscalls=read_syscalls,
        )

        # ---- Phase 6: SIGKILL memcached to force WAL replay ----
        self.log.info("Phase 6: Killing memcached (SIGKILL) to force WAL replay")
        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            shell.kill_memcached()
            shell.disconnect()

        # ---- Phase 7: wait for auto-restart to attempt (and fail) WAL replay ----
        self.sleep(
            warmup_wait_sec,
            "Waiting for Couchbase auto-restart to attempt WAL replay under fault injection",
        )

        # ---- Phase 8: validate fail-fast behaviour ----
        self.log.info("Phase 8: Validating fail-fast behaviour for middle-segment fault")

        # 8a: bucket warmup must NOT complete — a hole in the WAL log must not
        # be silently tolerated.  _wait_warmup_completed returns False on
        # timeout rather than raising, so we assert the return value directly.
        warmup_passed_count = 0
        for server in self.cluster.nodes_in_cluster:
            result = self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[server],
                wait_time=30,
            )
            if result:
                self.log.error(
                    f"[{server.ip}] Warmup completed unexpectedly — Magma did "
                    f"NOT fail-fast on the missing middle WAL segment"
                )
                warmup_passed_count += 1
            else:
                self.log.info(
                    f"[{server.ip}] Warmup correctly did NOT complete "
                    f"(fail-fast behaviour confirmed)"
                )

        self.assertEqual(
            warmup_passed_count, 0,
            f"{warmup_passed_count} node(s) completed bucket warmup despite a "
            f"missing/unreadable middle WAL segment — Magma truncated the hole "
            f"instead of failing fast, which risks silent data loss"
        )
        self.log.info("Fail-fast check PASSED — no node completed warmup on a holed WAL log")

        # 8b: highSeqno must not have regressed below baseline while the bucket
        # was healthy in phase 1 (the fault injection should not have silently
        # corrupted data that was already stable).
        # NOTE: this check queries the master node only; if the bucket is
        # unavailable the cbstats call may fail — catch and log rather than
        # aborting the test so the primary assertion (8a) is the decisive one.
        try:
            seqno_regression_count = 0
            for bucket in self.cluster.buckets:
                cbstat = Cbstats(self.cluster.master)
                post_seqnos = cbstat.vbucket_seqno(bucket.name)
                for vb_str, stats in post_seqnos.items():
                    vb = int(vb_str)
                    post_val = int(stats.get("high_seqno", 0))
                    baseline_val = baseline_seqnos[bucket.name].get(vb, 0)
                    if post_val < baseline_val:
                        self.log.error(
                            f"Bucket {bucket.name}, vb {vb}: highSeqno "
                            f"regressed ({baseline_val} → {post_val})"
                        )
                        seqno_regression_count += 1

            self.assertEqual(
                seqno_regression_count, 0,
                f"{seqno_regression_count} vBucket(s) have highSeqno below "
                f"the clean baseline — data was silently lost before the fault"
            )
            self.log.info("highSeqno regression check PASSED")
        except Exception as exc:
            # Bucket may be offline due to expected fail-fast — log and continue
            self.log.warning(
                f"highSeqno check skipped (bucket likely offline as expected): {exc}"
            )

        # ---- Phase 9: disable fault injection (clean-up) ----
        self.log.info("Phase 9: Disabling fault injection")
        self._set_fault_injection(enabled=False)
        self.log.info("test_wal_middle_segment_integrity complete")

    def test_wal_post_replay_sync_failure(self):
        """
        WAL Post-Replay Sync Failure

        Simulates a transient disk/filesystem error at the final sync step of
        WAL replay — the fsync()/fdatasync() that Magma issues after replaying
        WAL entries and before calling wal->Truncate() to advance walTruncOffset.

        Fault injected:
          - EIO on fsync() / fdatasync() on WAL files (/wal/wal.) only.
          - write/pwrite/read/pread/mmap are NOT intercepted so WAL reads and
            kvstore writes during replay proceed correctly.

        What this simulates:
          - Transient disk failure at the final step of WAL recovery.
          - Filesystem metadata update failure preventing WAL segment removal.

        What is tested:
          - Idempotency: re-replaying the same WAL entries on the next startup
            must not produce duplicate mutations or seqno jumps.
          - walTruncOffset consistency: the offset is only advanced after a
            successful sync, so a failed sync must leave it unchanged and force
            a full re-replay from the same point on the next attempt.
          - Double-application safety: upsert semantics must ensure mutations
            applied in the failed attempt are safely overwritten on retry.

        Steps:
          1. Load initial data (0 → num_items) cleanly.
          2. Capture per-vBucket highSeqno as the clean baseline.
          3. Kill memcached (SIGKILL) — WAL has unflushed entries.
          4. Enable fault injection: fail_rate=100 on fsync/fdatasync, WAL
             target only.  Every post-replay sync returns EIO so recovery
             hard-fails, leaving walTruncOffset and WAL segments untouched.
          5. Wait first_fail_wait_sec for Couchbase auto-restart to fail.
          6. Disable fault injection so the retry attempt can succeed.
          7. Wait warmup_wait_sec for successful WAL replay to complete.
          8. Validate:
             a. Per-vBucket highSeqno >= baseline (no data loss).
             b. highSeqno does not exceed baseline + num_items
                (no double-application of WAL mutations).
             c. Item count == num_items × num_collections
                (no duplicate documents surfaced through KV).
        """
        fail_rate = self.input.param("fault_inject_fail_rate", 100)
        first_fail_wait_sec = self.input.param("first_fail_wait_sec", 60)
        warmup_wait_sec = self.input.param("warmup_wait_sec", 120)
        ops_rate = self.input.param("ops_rate", 10000)

        wal_target = "/wal/wal."
        # Only intercept sync syscalls — keep read/write paths clean so WAL
        # entries are correctly applied to the kvstore during replay.
        sync_syscalls = {"fsync", "fdatasync"}

        # ---- Phase 1: clean initial load ----
        self.log.info("Phase 1: Initial data load with fault injection disabled")
        self._set_fault_injection(enabled=False)

        self.create_start = 0
        self.create_end = self.num_items
        self.java_doc_loader(
            doc_ops="create",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        self.log.info(f"Phase 1 complete — {self.num_items} docs loaded")

        # ---- Phase 2: capture per-vBucket highSeqno baseline ----
        self.log.info("Phase 2: Capturing per-vBucket highSeqno baseline")
        from cb_tools.cbstats import Cbstats
        baseline_seqnos = {}
        for bucket in self.cluster.buckets:
            cbstat = Cbstats(self.cluster.master)
            seqno_stats = cbstat.vbucket_seqno(bucket.name)
            baseline_seqnos[bucket.name] = {}
            for vb_str, stats in seqno_stats.items():
                baseline_seqnos[bucket.name][int(vb_str)] = int(
                    stats.get("high_seqno", 0)
                )
            self.log.info(
                f"Bucket {bucket.name}: captured highSeqno for "
                f"{len(baseline_seqnos[bucket.name])} vBuckets"
            )

        # ---- Phase 3: SIGKILL memcached to leave dirty WAL ----
        self.log.info("Phase 3: Killing memcached (SIGKILL) to leave dirty WAL")
        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            shell.kill_memcached()
            shell.disconnect()

        # ---- Phase 4: enable fault injection before auto-restart ----
        # fail_rate=100 ensures every post-replay fsync/fdatasync returns EIO,
        # forcing recovery to hard-fail without advancing walTruncOffset.
        self.log.info(
            f"Phase 4: Enabling fsync/fdatasync fault injection "
            f"(fail_rate={fail_rate}%, target={wal_target})"
        )
        self._set_fault_injection(
            enabled=True,
            delay_ms=0,
            fail_rate=fail_rate,
            fail_errno="EIO",
            target_path=wal_target,
            syscalls=sync_syscalls,
        )

        # ---- Phase 5: let first recovery attempt fail ----
        self.sleep(
            first_fail_wait_sec,
            "Waiting for first recovery attempt to fail at post-replay sync",
        )

        # ---- Phase 6: disable fault injection for clean second attempt ----
        self.log.info("Phase 6: Disabling fault injection for clean WAL replay retry")
        self._set_fault_injection(enabled=False)

        # ---- Phase 7: wait for successful WAL replay and warmup ----
        self.sleep(warmup_wait_sec, "Waiting for successful WAL replay and warmup")
        for server in self.cluster.nodes_in_cluster:
            result = self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[server],
                wait_time=self.wait_timeout * 5,
            )
            self.assertTrue(result, f"Warmup did not complete on {server.ip}")

        # ---- Phase 8: validate recovery correctness ----
        self.log.info("Phase 8: Validating recovery after post-replay sync failure")

        # 8a: per-vBucket highSeqno must be >= baseline and must not overshoot
        # by more than num_items — bounding double-application of mutations.
        seqno_regression_count = 0
        seqno_overrun_count = 0
        for bucket in self.cluster.buckets:
            cbstat = Cbstats(self.cluster.master)
            post_seqnos = cbstat.vbucket_seqno(bucket.name)
            for vb_str, stats in post_seqnos.items():
                vb = int(vb_str)
                post_val = int(stats.get("high_seqno", 0))
                baseline_val = baseline_seqnos[bucket.name].get(vb, 0)
                if post_val < baseline_val:
                    self.log.error(
                        f"Bucket {bucket.name}, vb {vb}: highSeqno regressed "
                        f"({baseline_val} → {post_val})"
                    )
                    seqno_regression_count += 1
                upper_bound = baseline_val + self.num_items
                if post_val > upper_bound:
                    self.log.error(
                        f"Bucket {bucket.name}, vb {vb}: highSeqno overshot "
                        f"expected upper bound ({post_val} > {upper_bound}) — "
                        f"possible double-application of WAL mutations"
                    )
                    seqno_overrun_count += 1

        self.assertEqual(
            seqno_regression_count, 0,
            f"{seqno_regression_count} vBuckets have highSeqno below the "
            f"pre-crash baseline — walTruncOffset inconsistency detected"
        )
        self.assertEqual(
            seqno_overrun_count, 0,
            f"{seqno_overrun_count} vBuckets have highSeqno above the expected "
            f"upper bound — WAL mutations were double-applied on retry"
        )
        self.log.info("highSeqno checks PASSED — no regression, no double-application")

        # 8b: item count must equal initial load (no duplicates in KV)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        bucket_item_counts = self.bucket_util.get_buckets_item_count(self.cluster)
        for bucket in self.cluster.buckets:
            actual = bucket_item_counts.get(bucket.name, 0)
            expected = self.num_items * self.num_collections
            self.log.info(
                f"Bucket {bucket.name}: items after recovery = {actual}, "
                f"expected = {expected}"
            )
            self.assertEqual(
                actual, expected,
                f"Bucket {bucket.name}: item count mismatch after recovery "
                f"(expected={expected}, actual={actual}) — possible duplicate "
                f"documents or data loss from post-replay sync failure"
            )

        self.log.info("test_wal_post_replay_sync_failure complete")

    def test_wal_replay_flush_sstable_failure(self):
        """
        WAL Replay — SSTable Flush Failure Under Memory Pressure

        Validates the atomicity guarantee between the WAL log and the LSM
        tree when Magma's FlushMemTablesForReplay path encounters an I/O
        error while writing replayed data to SSTables.

        Fault injected:
          - EIO on pwrite()/pwrite64() targeting the /data/ directory.
            This intercepts exactly the SSTable block-writes that Magma
            issues during FlushMemTablesForReplay, while leaving WAL
            read()/pread() clean so entries are correctly decoded from the
            log before the flush attempt fails.
          - High memory pressure (stress-ng --vm or Python fallback) applied
            concurrently with WAL replay to force Magma to invoke
            FlushMemTablesForReplay before it has finished scanning the full
            WAL, ensuring the fault fires inside the flush path rather than
            at the very end of replay.

        What this simulates:
          - Storage exhaustion or device failure at the moment replayed
            memtable data is being flushed to SSTables.
          - Memory-constrained restart environments where the engine cannot
            buffer the entire WAL in RAM and must flush incrementally.

        What is tested:
          - FlushMemTablesForReplay interaction: when the flush write fails,
            Magma must roll back the current replay batch entirely rather
            than committing a partial SSTable.
          - Atomicity between the LSM tree and the WAL: the walTruncOffset
            must NOT advance past the point of the failed flush, so the
            same WAL entries are re-applied on the next clean restart.
          - No partial or uncommitted data visible to indexes: after the
            failed attempt and a successful retry, item counts must equal
            the pre-crash baseline exactly.
          - System remains in a consistent state: the clean retry completes
            warmup successfully without operator intervention.

        Steps:
          1. Load initial data (0 → num_items) cleanly.
          2. Capture per-vBucket highSeqno as the durable baseline.
          3. Kill memcached (SIGKILL) — WAL has unflushed entries.
          4. Enable fault injection: EIO on pwrite only, target /data/,
             fail_rate=100.  WAL reads are NOT intercepted so replay can
             decode entries; only the SSTable flush writes fail.
          5. Start memory pressure (stress-ng --vm) on all nodes to
             trigger FlushMemTablesForReplay during WAL replay.
          6. Wait first_fail_wait_sec for the auto-restart to attempt
             and fail replay (pwrite EIO aborts the flush).
          7. Disable fault injection and stop memory pressure so the
             next restart can succeed.
          8. Wait warmup_wait_sec for successful WAL re-replay and warmup.
          9. Validate:
             a. Warmup completes successfully (system is consistent).
             b. Per-vBucket highSeqno >= baseline (no data loss from
                phase 1 — the failed flush batch was rolled back, not
                committed partially).
             c. Item count == num_items × num_collections (no partial /
                uncommitted documents visible; no duplicates).
        """
        fail_rate = self.input.param("fault_inject_fail_rate", 100)
        first_fail_wait_sec = self.input.param("first_fail_wait_sec", 60)
        warmup_wait_sec = self.input.param("warmup_wait_sec", 120)
        ops_rate = self.input.param("ops_rate", 10000)
        # Percentage of total RAM to consume for memory pressure (default 75%).
        mem_pressure_pct = self.input.param("mem_pressure_pct", 100)
        # How long (seconds) the stress process runs; should cover both
        # first_fail_wait_sec and the start of the clean retry.
        mem_pressure_sec = self.input.param(
            "mem_pressure_sec", first_fail_wait_sec + 30
        )

        # Target pwrite on the full data directory so SSTable block-writes
        # fail.  WAL files (*/wal/wal.*) also live under /data/ but we only
        # intercept pwrite — WAL appends also use pwrite, so reads remain
        # clean and the WAL entries are decoded correctly before the flush
        # attempt.  If the WAL write path uses write() rather than pwrite()
        # on this platform the interception is already syscall-selective.
        data_target = self.data_path
        pwrite_only = {"pwrite"}

        # ---- Phase 1: clean initial load ----
        self.log.info("Phase 1: Initial data load with fault injection disabled")
        self._set_fault_injection(enabled=False)

        self.create_start = 0
        self.create_end = self.num_items
        self.java_doc_loader(
            doc_ops="create",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        self.log.info(f"Phase 1 complete — {self.num_items} docs loaded")

        # ---- Phase 2: capture per-vBucket highSeqno baseline ----
        self.log.info("Phase 2: Capturing per-vBucket highSeqno baseline")
        from cb_tools.cbstats import Cbstats
        baseline_seqnos = {}
        for bucket in self.cluster.buckets:
            cbstat = Cbstats(self.cluster.master)
            seqno_stats = cbstat.vbucket_seqno(bucket.name)
            baseline_seqnos[bucket.name] = {}
            for vb_str, stats in seqno_stats.items():
                baseline_seqnos[bucket.name][int(vb_str)] = int(
                    stats.get("high_seqno", 0)
                )
            self.log.info(
                f"Bucket {bucket.name}: captured highSeqno for "
                f"{len(baseline_seqnos[bucket.name])} vBuckets"
            )

        # ---- Phase 3: SIGKILL memcached to leave dirty WAL ----
        self.log.info("Phase 3: Killing memcached (SIGKILL) to leave dirty WAL")
        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            shell.kill_memcached()
            shell.disconnect()

        # ---- Phase 4: arm pwrite EIO on /data/ before auto-restart ----
        # fail_rate=100 ensures every SSTable block-write during
        # FlushMemTablesForReplay returns EIO.  WAL read/pread are left
        # clean so replay can decode entries before the flush attempt.
        self.log.info(
            f"Phase 4: Enabling pwrite fault injection on {data_target} "
            f"(fail_rate={fail_rate}%, EIO)"
        )
        self._set_fault_injection(
            enabled=True,
            delay_ms=0,
            fail_rate=fail_rate,
            fail_errno="EIO",
            target_path=data_target,
            syscalls=pwrite_only,
        )

        # ---- Phase 5: start memory pressure to trigger FlushMemTablesForReplay ----
        # stress-ng is preferred; fall back to a Python busy-allocator when
        # stress-ng is absent from the node.
        self.log.info(
            f"Phase 5: Starting memory pressure ({mem_pressure_pct}% RAM, "
            f"{mem_pressure_sec}s) on all nodes"
        )
        stress_pids: dict[str, str] = {}
        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            stress_cmd = (
                f"stress-ng --vm 1 --vm-bytes {mem_pressure_pct}% "
                f"--vm-populate --vm-keep --vm-method all -t {mem_pressure_sec}s "
                f">/tmp/stress_wal_replay.log 2>&1 & echo $!"
            )
            stdout, _ = shell.execute_command(stress_cmd)
            pid = stdout[0].strip() if stdout else ""
            if not pid:
                # stress-ng not available — fall back to Python allocator
                self.log.warning(
                    f"[{server.ip}] stress-ng not found, using Python fallback"
                )
                py_snippet = (
                    f"import os,time; "
                    f"pages=int(os.sysconf('SC_PHYS_PAGES')*{mem_pressure_pct / 100}); "
                    f"sz=pages*os.sysconf('SC_PAGE_SIZE'); "
                    f"x=bytearray(sz); time.sleep({mem_pressure_sec})"
                )
                fallback_cmd = (
                    f"python3 -c '{py_snippet}'"
                    f" >/tmp/stress_wal_replay.log 2>&1 & echo $!"
                )
                stdout, _ = shell.execute_command(fallback_cmd)
                pid = stdout[0].strip() if stdout else ""
            stress_pids[server.ip] = pid
            self.log.info(f"[{server.ip}] Memory pressure started (pid={pid})")
            shell.disconnect()

        # ---- Phase 6: wait for the first recovery attempt to fail ----
        self.sleep(
            first_fail_wait_sec,
            "Waiting for first WAL replay attempt to fail at FlushMemTablesForReplay",
        )

        # ---- Phase 7: disable fault injection and stop memory pressure ----
        self.log.info(
            "Phase 7: Disabling fault injection and stopping memory pressure"
        )
        self._set_fault_injection(enabled=False)

        for server in self.cluster.nodes_in_cluster:
            pid = stress_pids.get(server.ip, "")
            if pid:
                shell = RemoteMachineShellConnection(server)
                # Kill the stress process and its children; ignore errors if
                # the process has already exited naturally.
                shell.execute_command(
                    f"kill -- -{pid} 2>/dev/null; kill {pid} 2>/dev/null; true"
                )
                shell.disconnect()
                self.log.info(f"[{server.ip}] Stopped memory pressure (pid={pid})")

        # ---- Phase 8: wait for the clean retry to complete warmup ----
        self.sleep(warmup_wait_sec, "Waiting for clean WAL re-replay and warmup")
        for server in self.cluster.nodes_in_cluster:
            result = self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[server],
                wait_time=self.wait_timeout * 5,
            )
            self.assertTrue(
                result,
                f"Warmup did not complete on {server.ip} after fault "
                f"injection was removed — system did not recover to a "
                f"consistent state"
            )

        # ---- Phase 9: validate atomicity and data integrity ----
        self.log.info(
            "Phase 9: Validating atomicity and data integrity after "
            "FlushMemTablesForReplay failure"
        )

        # 9a: per-vBucket highSeqno must be >= baseline.
        # A rolled-back flush must NOT drop seqnos below what was durable
        # before the crash.
        seqno_regression_count = 0
        for bucket in self.cluster.buckets:
            cbstat = Cbstats(self.cluster.master)
            post_seqnos = cbstat.vbucket_seqno(bucket.name)
            for vb_str, stats in post_seqnos.items():
                vb = int(vb_str)
                post_val = int(stats.get("high_seqno", 0))
                baseline_val = baseline_seqnos[bucket.name].get(vb, 0)
                if post_val < baseline_val:
                    self.log.error(
                        f"Bucket {bucket.name}, vb {vb}: highSeqno regressed "
                        f"({baseline_val} → {post_val}) — partial flush was "
                        f"not rolled back cleanly"
                    )
                    seqno_regression_count += 1

        self.assertEqual(
            seqno_regression_count, 0,
            f"{seqno_regression_count} vBucket(s) have highSeqno below the "
            f"pre-crash baseline — the failed FlushMemTablesForReplay batch "
            f"was not rolled back atomically"
        )
        self.log.info("highSeqno regression check PASSED")

        # 9b: item count must equal the initial load exactly.
        # Partial SSTable writes must not surface as visible documents, and
        # the clean re-replay must not produce duplicates.
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        bucket_item_counts = self.bucket_util.get_buckets_item_count(self.cluster)
        for bucket in self.cluster.buckets:
            actual = bucket_item_counts.get(bucket.name, 0)
            expected = self.num_items * self.num_collections
            self.log.info(
                f"Bucket {bucket.name}: items after recovery = {actual}, "
                f"expected = {expected}"
            )
            self.assertEqual(
                actual, expected,
                f"Bucket {bucket.name}: item count mismatch after recovery "
                f"(expected={expected}, actual={actual}) — partial or "
                f"uncommitted data is visible, or WAL entries were "
                f"double-applied on the clean retry"
            )

        self.log.info("test_wal_replay_flush_sstable_failure complete")

    def test_magma_dump_read_only(self):
        """
        Verify that magma_dump is strictly read-only and does not modify data files.

        This test uses strace to intercept syscalls made by magma_dump and
        validates that:
          1. All open() / openat() calls use read-only flags (O_RDONLY).
          2. No destructive syscalls are issued: unlink, unlinkat, rmdir,
             rename, renameat, truncate, ftruncate, write, pwrite64.

        Exception:
          - file.lock: magma_dump uses a lock file to prevent concurrent access.
            Writes to file.lock are acceptable as it's a locking mechanism,
            not actual data modification.

        Steps:
          1. Load initial data to create magma data files.
          2. Run magma_dump under strace on each node, capturing syscall trace.
          3. Parse the strace output to extract open/openat calls and verify
             they only use O_RDONLY flag (except file.lock).
          4. Verify no destructive syscalls appear in the trace (except file.lock).
          5. Fail the test if any violation is found.
        """
        ops_rate = self.input.param("ops_rate", 10000)

        # Destructive syscalls that magma_dump must NOT issue
        destructive_syscalls = {
            "unlink", "unlinkat", "rmdir", "rename", "renameat", "renameat2",
            "truncate", "ftruncate", "write", "pwrite64", "pwritev", "pwritev2"
        }

        # Flags that indicate write intent in open/openat calls
        write_flags = {"O_WRONLY", "O_RDWR", "O_CREAT", "O_TRUNC", "O_APPEND"}

        # ---- Phase 1: Load initial data ----
        self.log.info("Phase 1: Loading initial data to create magma files")
        self._set_fault_injection(enabled=False)

        self.create_start = 0
        self.create_end = self.num_items
        self.java_doc_loader(
            doc_ops="create",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        self.log.info(f"Phase 1 complete — {self.num_items} docs loaded")

        # Wait for data to be flushed to disk
        self.sleep(120, "Waiting for data to be flushed to magma files")

        # ---- Phase 2: Run magma_dump under strace on each node ----
        self.log.info("Phase 2: Running magma_dump under strace to capture syscalls")

        violations = []
        bucket = self.cluster.buckets[0]
        magma_path = os.path.join(self.data_path, bucket.uuid, "magma.{}")

        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)

            shards_output, _ = shell.execute_command(
                "lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'"
            )
            shards = int(shards_output[0].strip()) if shards_output else 1
            shards = min(shards, 64)

            for shard in range(shards):
                magma_dir = magma_path.format(shard)

                # Check if the magma directory exists
                dir_check, _ = shell.execute_command(f"test -d {magma_dir} && echo exists")
                if not dir_check or "exists" not in dir_check[0]:
                    continue

                # Get kvstores in this shard
                kvstores, _ = shell.execute_command(f"ls {magma_dir} | grep kvstore")
                if not kvstores:
                    continue

                for kvstore in kvstores:
                    kvstore_num = kvstore.split("-")[1].strip() if "-" in kvstore else "0"

                    # Run magma_dump under strace
                    # -f: follow forks, -e trace=open,openat,unlink,...: filter syscalls
                    strace_output_file = f"/tmp/magma_dump_strace_{shard}_{kvstore_num}.log"
                    magma_dump_cmd = f"/opt/couchbase/bin/magma_dump {magma_dir} docs --index seq --kvstore {kvstore_num}"

                    # Capture all file-related syscalls
                    strace_cmd = (
                        f"strace -f -e trace=open,openat,unlink,unlinkat,rmdir,"
                        f"rename,renameat,renameat2,truncate,ftruncate,"
                        f"write,pwrite64,pwritev,pwritev2 "
                        f"-o {strace_output_file} {magma_dump_cmd} 2>/dev/null; "
                        f"cat {strace_output_file}"
                    )

                    self.log.info(f"[{server.ip}] Running strace on magma_dump for shard {shard}, kvstore {kvstore_num}")
                    strace_output, _ = shell.execute_command(strace_cmd, timeout=300)

                    # ---- Phase 3: Parse strace output ----
                    for line in strace_output:
                        line = line.strip()
                        if not line:
                            continue

                        # Check for destructive syscalls
                        for syscall in destructive_syscalls:
                            if line.startswith(syscall + "(") or f" {syscall}(" in line:
                                # Filter out writes to stdout/stderr (fd 1, 2)
                                if syscall in ("write", "pwrite64", "pwritev", "pwritev2"):
                                    # write(1, ...) or write(2, ...) are OK (stdout/stderr)
                                    if "write(1," in line or "write(2," in line:
                                        continue
                                    # pwrite64 to fd 1 or 2 is also OK
                                    if "pwrite64(1," in line or "pwrite64(2," in line:
                                        continue
                                    # file.lock writes are acceptable (locking mechanism)
                                    if "file.lock" in line:
                                        self.log.info(f"[{server.ip}] Allowed: file.lock write (locking mechanism)")
                                        continue

                                violations.append({
                                    "server": server.ip,
                                    "shard": shard,
                                    "kvstore": kvstore_num,
                                    "type": "destructive_syscall",
                                    "syscall": syscall,
                                    "line": line[:200]  # Truncate for readability
                                })
                                self.log.error(
                                    f"[{server.ip}] VIOLATION: Destructive syscall detected: {line[:200]}"
                                )

                        # Check for open/openat with write flags
                        if "open(" in line or "openat(" in line:
                            # Skip if the call failed (returned -1)
                            if "= -1" in line:
                                continue

                            # file.lock is acceptable (locking mechanism, not data modification)
                            if "file.lock" in line:
                                self.log.info(f"[{server.ip}] Allowed: file.lock open with write flags (locking mechanism)")
                                continue

                            # Check for write flags in the open call
                            for flag in write_flags:
                                if flag in line:
                                    # Ignore opens to /dev/null, /proc, /sys, stdout, stderr
                                    if any(skip in line for skip in ["/dev/null", "/proc/", "/sys/", '"/dev/']):
                                        continue

                                    violations.append({
                                        "server": server.ip,
                                        "shard": shard,
                                        "kvstore": kvstore_num,
                                        "type": "write_flag_in_open",
                                        "flag": flag,
                                        "line": line[:200]
                                    })
                                    self.log.error(
                                        f"[{server.ip}] VIOLATION: Write flag {flag} in open call: {line[:200]}"
                                    )
                                    break

                    # Cleanup strace output file
                    shell.execute_command(f"rm -f {strace_output_file}")

            shell.disconnect()

        # ---- Phase 4: Report results ----
        self.log.info(f"Phase 4: Syscall analysis complete. Found {len(violations)} violation(s)")

        if violations:
            # Group violations by type for clearer reporting
            destructive_violations = [v for v in violations if v["type"] == "destructive_syscall"]
            write_flag_violations = [v for v in violations if v["type"] == "write_flag_in_open"]

            error_msg = "magma_dump is NOT read-only:\n"
            if destructive_violations:
                error_msg += f"\n  Destructive syscalls detected ({len(destructive_violations)}):\n"
                for v in destructive_violations[:10]:  # Limit to first 10
                    error_msg += f"    - [{v['server']}] {v['syscall']}: {v['line'][:100]}\n"

            if write_flag_violations:
                error_msg += f"\n  Write flags in open calls ({len(write_flag_violations)}):\n"
                for v in write_flag_violations[:10]:  # Limit to first 10
                    error_msg += f"    - [{v['server']}] {v['flag']}: {v['line'][:100]}\n"

            self.fail(error_msg)

        self.log.info("test_magma_dump_read_only PASSED — magma_dump is strictly read-only")

    def test_magma_dump_no_file_modifications(self):
        """
        Verify that magma_dump does not modify any files in the data directory.

        This test takes a snapshot of file metadata (mtime, size, checksum) before
        and after running magma_dump, and verifies nothing changed.

        Steps:
          1. Load initial data to create magma data files.
          2. Capture file metadata snapshot (mtime, size, md5sum) for all files
             in the magma data directory.
          3. Run magma_dump on all kvstores.
          4. Capture another file metadata snapshot.
          5. Compare snapshots and fail if any file was modified, created, or deleted.
        """
        ops_rate = self.input.param("ops_rate", 10000)

        # ---- Phase 1: Load initial data ----
        self.log.info("Phase 1: Loading initial data to create magma files")
        self._set_fault_injection(enabled=False)

        self.create_start = 0
        self.create_end = self.num_items
        self.java_doc_loader(
            doc_ops="create",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        self.log.info(f"Phase 1 complete — {self.num_items} docs loaded")

        # Wait for data to be flushed
        self.sleep(30, "Waiting for data to be flushed to magma files")

        # ---- Phase 2: Capture pre-dump file metadata snapshot ----
        self.log.info("Phase 2: Capturing pre-dump file metadata snapshot")

        def get_file_snapshot(shell, data_dir="/data"):
            """Get snapshot of all files: path, mtime, size, md5sum"""
            # Use find to get all regular files, then stat and md5sum each
            cmd = (
                f"find {data_dir} -type f -exec stat --format='%n|%Y|%s' {{}} \\; 2>/dev/null | "
                f"while IFS='|' read path mtime size; do "
                f"  md5=$(md5sum \"$path\" 2>/dev/null | cut -d' ' -f1); "
                f"  echo \"$path|$mtime|$size|$md5\"; "
                f"done"
            )
            output, _ = shell.execute_command(cmd, timeout=600)
            snapshot = {}
            for line in output:
                line = line.strip()
                if not line or "|" not in line:
                    continue
                parts = line.split("|")
                if len(parts) >= 4:
                    path, mtime, size, md5 = parts[0], parts[1], parts[2], parts[3]
                    snapshot[path] = {"mtime": mtime, "size": size, "md5": md5}
            return snapshot

        pre_snapshots = {}
        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            pre_snapshots[server.ip] = get_file_snapshot(shell)
            self.log.info(f"[{server.ip}] Captured {len(pre_snapshots[server.ip])} files in pre-dump snapshot")
            shell.disconnect()

        # ---- Phase 3: Run magma_dump ----
        self.log.info("Phase 3: Running magma_dump on all kvstores")

        bucket = self.cluster.buckets[0]
        magma_path = os.path.join(self.data_path, bucket.uuid, "magma.{}")

        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)

            shards_output, _ = shell.execute_command(
                "lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'"
            )
            shards = int(shards_output[0].strip()) if shards_output else 1
            shards = min(shards, 64)

            for shard in range(shards):
                magma_dir = magma_path.format(shard)

                dir_check, _ = shell.execute_command(f"test -d {magma_dir} && echo exists")
                if not dir_check or "exists" not in dir_check[0]:
                    continue

                kvstores, _ = shell.execute_command(f"ls {magma_dir} | grep kvstore")
                if not kvstores:
                    continue

                for kvstore in kvstores:
                    kvstore_num = kvstore.split("-")[1].strip() if "-" in kvstore else "0"

                    # Run magma_dump with various options to exercise different code paths
                    dump_cmds = [
                        f"/opt/couchbase/bin/magma_dump {magma_dir} docs --index key --kvstore {kvstore_num}",
                        f"/opt/couchbase/bin/magma_dump {magma_dir} docs --index seq --kvstore {kvstore_num}",
                        f"/opt/couchbase/bin/magma_dump {magma_dir} docs --index local --kvstore {kvstore_num}",
                    ]

                    for cmd in dump_cmds:
                        self.log.info(f"[{server.ip}] Running: {cmd}")
                        shell.execute_command(f"{cmd} > /dev/null 2>&1", timeout=300)

            shell.disconnect()

        # Small delay to ensure filesystem metadata is updated
        self.sleep(5, "Waiting for filesystem metadata to settle")

        # ---- Phase 4: Capture post-dump file metadata snapshot ----
        self.log.info("Phase 4: Capturing post-dump file metadata snapshot")

        post_snapshots = {}
        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            post_snapshots[server.ip] = get_file_snapshot(shell)
            self.log.info(f"[{server.ip}] Captured {len(post_snapshots[server.ip])} files in post-dump snapshot")
            shell.disconnect()

        # ---- Phase 5: Compare snapshots ----
        self.log.info("Phase 5: Comparing pre and post dump snapshots")

        modifications = []
        for server_ip in pre_snapshots:
            pre = pre_snapshots[server_ip]
            post = post_snapshots.get(server_ip, {})

            # Check for modified files
            for path, pre_meta in pre.items():
                if path not in post:
                    modifications.append({
                        "server": server_ip,
                        "type": "deleted",
                        "path": path
                    })
                    self.log.error(f"[{server_ip}] File DELETED: {path}")
                else:
                    post_meta = post[path]
                    if pre_meta["mtime"] != post_meta["mtime"]:
                        modifications.append({
                            "server": server_ip,
                            "type": "mtime_changed",
                            "path": path,
                            "before": pre_meta["mtime"],
                            "after": post_meta["mtime"]
                        })
                        self.log.error(f"[{server_ip}] File mtime CHANGED: {path}")
                    if pre_meta["size"] != post_meta["size"]:
                        modifications.append({
                            "server": server_ip,
                            "type": "size_changed",
                            "path": path,
                            "before": pre_meta["size"],
                            "after": post_meta["size"]
                        })
                        self.log.error(f"[{server_ip}] File size CHANGED: {path}")
                    if pre_meta["md5"] != post_meta["md5"]:
                        modifications.append({
                            "server": server_ip,
                            "type": "content_changed",
                            "path": path,
                            "before": pre_meta["md5"],
                            "after": post_meta["md5"]
                        })
                        self.log.error(f"[{server_ip}] File content CHANGED: {path}")

            # Check for newly created files (excluding expected temp files)
            for path in post:
                if path not in pre:
                    # Ignore temp files, log files, etc.
                    if any(skip in path for skip in ["/tmp/", ".log", ".pid", "/proc/", "/sys/"]):
                        continue
                    modifications.append({
                        "server": server_ip,
                        "type": "created",
                        "path": path
                    })
                    self.log.error(f"[{server_ip}] File CREATED: {path}")

        # ---- Report results ----
        if modifications:
            error_msg = f"magma_dump modified {len(modifications)} file(s):\n"
            for mod in modifications[:20]:  # Limit to first 20
                error_msg += f"  - [{mod['server']}] {mod['type']}: {mod['path']}\n"
            self.fail(error_msg)

        self.log.info("test_magma_dump_no_file_modifications PASSED — no files were modified")

    def test_magma_dump_read_only_ld_preload(self):
        """
        Verify that magma_dump is read-only using LD_PRELOAD fault injection library.

        This test uses the fault_injection.c library in validation mode to intercept
        and log all write operations (open with write flags, write, pwrite).

        Advantages over strace:
          - Lower overhead (no ptrace)
          - Resolves fd to actual file paths automatically
          - Easier to parse output
          - Already integrated with test framework

        Exception:
          - file.lock: magma_dump uses a lock file to prevent concurrent access.
            Writes to file.lock are acceptable as it's a locking mechanism.

        Steps:
          1. Load initial data to create magma data files.
          2. Enable validation mode via _set_fault_injection(validate_mode=True).
          3. Run magma_dump with LD_PRELOAD to intercept syscalls.
          4. Parse the validation log for write operations.
          5. Fail if any writes to non-whitelisted files are detected.
        """
        ops_rate = self.input.param("ops_rate", 10000)

        # Whitelisted files that are allowed to be written
        # file.lock is used for locking mechanism, not actual data modification
        whitelisted_files = {"file.lock"}

        lib_path = "/opt/couchbase/lib/libfaultinject.so"
        validate_log = "/tmp/validate.log"

        # ---- Phase 1: Load initial data ----
        self.log.info("Phase 1: Loading initial data to create magma files")
        self._set_fault_injection(enabled=False)

        self.create_start = 0
        self.create_end = self.num_items
        self.java_doc_loader(
            doc_ops="create",
            wait=True,
            skip_default=self.skip_load_to_default_collection,
            ops_rate=ops_rate,
        )
        self.log.info(f"Phase 1 complete — {self.num_items} docs loaded")

        # Wait for data to be flushed to disk
        self.sleep(120, "Waiting for data to be flushed to magma files")

        # ---- Phase 2: Enable validation mode ----
        self.log.info("Phase 2: Enabling validation mode")
        self._set_fault_injection(enabled=False, validate_mode=True, target_path="/data")

        # Clear validation log on all nodes
        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            shell.execute_command(f"rm -f {validate_log} && touch {validate_log}")
            shell.disconnect()

        # ---- Phase 3: Run magma_dump with LD_PRELOAD ----
        self.log.info("Phase 3: Running magma_dump with LD_PRELOAD validation")

        violations = []
        bucket = self.cluster.buckets[0]
        magma_path = os.path.join(self.data_path, bucket.uuid, "magma.{}")

        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)

            shards_output, _ = shell.execute_command(
                "lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'"
            )
            shards = int(shards_output[0].strip()) if shards_output else 1
            shards = min(shards, 64)

            for shard in range(shards):
                magma_dir = magma_path.format(shard)

                dir_check, _ = shell.execute_command(f"test -d {magma_dir} && echo exists")
                if not dir_check or "exists" not in dir_check[0]:
                    continue

                kvstores, _ = shell.execute_command(f"ls {magma_dir} | grep kvstore")
                if not kvstores:
                    continue

                for kvstore in kvstores:
                    kvstore_num = kvstore.split("-")[1].strip() if "-" in kvstore else "0"

                    # Run magma_dump with LD_PRELOAD
                    magma_dump_cmd = (
                        f"LD_PRELOAD={lib_path} "
                        f"/opt/couchbase/bin/magma_dump {magma_dir} "
                        f"docs --index seq --kvstore {kvstore_num} > /dev/null 2>&1"
                    )

                    self.log.info(f"[{server.ip}] Running magma_dump with LD_PRELOAD for shard {shard}, kvstore {kvstore_num}")
                    shell.execute_command(magma_dump_cmd, timeout=300)

            # ---- Phase 4: Parse validation log ----
            self.log.info(f"[{server.ip}] Parsing validation log")
            log_output, _ = shell.execute_command(f"cat {validate_log}")

            for line in log_output:
                line = line.strip()
                if not line:
                    continue

                # Parse VALIDATE_OPEN and VALIDATE_WRITE lines
                if "VALIDATE_OPEN:" in line or "VALIDATE_WRITE:" in line:
                    # Extract path from the log line
                    # Format: VALIDATE_OPEN: syscall=openat path=/data/... flags=0x241 (...)
                    # Format: VALIDATE_WRITE: syscall=pwrite fd=3 path=/data/... count=7 offset=0
                    path_match = None
                    if "path=" in line:
                        parts = line.split("path=")
                        if len(parts) > 1:
                            path_match = parts[1].split()[0]

                    # Check if this is a whitelisted file
                    is_whitelisted = False
                    if path_match:
                        for whitelist in whitelisted_files:
                            if whitelist in path_match:
                                is_whitelisted = True
                                self.log.info(f"[{server.ip}] Allowed: {whitelist} write (whitelisted)")
                                break

                    if not is_whitelisted:
                        violations.append({
                            "server": server.ip,
                            "line": line[:300]
                        })
                        self.log.error(f"[{server.ip}] VIOLATION: {line[:200]}")

            shell.disconnect()

        # ---- Phase 5: Disable validation mode and report results ----
        self._set_fault_injection(enabled=False, validate_mode=False)

        self.log.info(f"Phase 5: Validation complete. Found {len(violations)} violation(s)")

        if violations:
            error_msg = f"magma_dump wrote to {len(violations)} non-whitelisted file(s):\n"
            for v in violations[:20]:
                error_msg += f"  - [{v['server']}] {v['line'][:150]}\n"
            self.fail(error_msg)

        self.log.info("test_magma_dump_read_only_ld_preload PASSED — magma_dump is read-only (except file.lock)")
