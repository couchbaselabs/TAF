import importlib
import time

from backup_restore.continuous_backup.continuous_backup_base import ContinuousBackupBase
from StatsLib.StatsOperations import StatsHelper
from pytests.bucket_collections.collections_base import CollectionBase
from shell_util.remote_connection import RemoteMachineShellConnection

class ContinuousBackupRetentionTest(ContinuousBackupBase):
    def setUp(self):
        super(ContinuousBackupRetentionTest, self).setUp()
        self.log.info(f"Loading spec from: {self.spec_name}")
        spec_module = importlib.import_module(f"bucket_collections.bucket_templates.{self.spec_name}")
        self.spec = spec_module.spec

        # Override retention periods via environment variables for testing if needed
        # self.cluster.set_env_variable(self.cluster.master, "CB_CONTBK_RETENTION_PERIOD_UNSAFE", "5m")
        # self.cluster.set_env_variable(self.cluster.master, "CB_CONTBK_RETENTION_CHECK", "5m")

    def tearDown(self):
        super(ContinuousBackupRetentionTest, self).tearDown()
        # onPrem_basetestcase only resets CB_CONTBK_RETENTION_* env vars when
        # cluster.vbuckets != 1024, which is never true for these on-prem
        # clusters -- so the unsafe retention window this class sets on the
        # node's couchbase-server environment would otherwise persist onto
        # every later test that reuses the same nodes without going through
        # initialize_cluster() again (e.g. ContinuousBackupTest, which runs
        # right after this class and defaults skip_cluster_reset=True).
        # Reset unconditionally here so no test outside this class can ever
        # inherit it.
        if self.retention_test:
            for cluster in self.cb_clusters.values():
                self.cluster_util.reset_env_variables(cluster)

    def _load_data_and_get_task(self, data_spec_name):
        self.log.info("Load docs using spec file %s" % data_spec_name)
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package(data_spec_name)
        CollectionBase.over_ride_doc_loading_template_params(
            self, doc_loading_spec)
        CollectionBase.set_retry_exceptions(
            doc_loading_spec, self.durability_level)
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                doc_loading_spec,
                mutation_num=0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                load_using=self.load_docs_using)
        if doc_loading_task.result is False:
            self.fail("Initial doc_loading failed")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        return doc_loading_task

    # 3.1 Configuration & Scheduling Tests
    def test_default_state(self):
        """TC-CONF-01: Default State

        The design doc describes retention as "off by default", but the
        server currently rejects 0 outright (must be 1-1440) -- see
        MB-73171/contbk-retention-default-bug -- so TAF's own
        enable_continuous_backup() defaults continuous_backup_retention_period
        to the max, 1440h, instead of leaving it unset. This test therefore
        checks what TAF actually configures, not the server's own true
        (currently unusable) unset default.
        """
        self.log.info("Testing default state (retention = TAF's default of 1440h)")
        params = self.bucket_util.get_continuous_backup_params(self.cluster, self.bucket.name)

        retention_period = params.get("continuousBackupRetentionPeriod")
        self.log.info(f"Retrieved continuousBackupRetentionPeriod: {retention_period}")

        # Normalise to int to handle both numeric 1440 and string "1440" from the REST layer
        try:
            normalised = int(retention_period) if retention_period is not None else None
        except (ValueError, TypeError):
            normalised = retention_period
        if normalised != 1440:
            self.fail(f"Expected retention period to be TAF's default of 1440, but got {retention_period}")

        self.log.info("Test completed successfully")

    def test_valid_configuration_ranges(self):
        """TC-CONF-02: Valid Configuration Ranges"""
        self.log.info("Testing valid configuration ranges for continuousBackupRetentionPeriod")
        valid_ranges = [1, 24, 1440]

        for period in valid_ranges:
            self.log.info(f"Setting retention period to {period} hours")
            # Assuming update_bucket_property can take this param, if not, direct REST call might be needed
            self.bucket_util.update_bucket_property(self.cluster.master, self.bucket,
                                                    continuous_backup_retention_period=period)
            self.sleep(5, "Wait for setting to apply")

            params = self.bucket_util.get_continuous_backup_params(self.cluster, self.bucket.name)
            raw = params.get("continuousBackupRetentionPeriod")
            try:
                retention_period = int(raw) if raw is not None else None
            except (TypeError, ValueError):
                self.fail(f"Unexpected retention period value: {raw}")
            if retention_period != period:
                self.fail(f"Failed to set retention period to {period}. Found {raw}")

        self.log.info("Test completed successfully")

    def test_invalid_configuration(self):
        """TC-CONF-03: Invalid Configuration"""
        self.log.info("Testing invalid configuration for continuousBackupRetentionPeriod")
        invalid_ranges = [-1, "abc", 2.5, 1441, 0]

        for period in invalid_ranges:
            self.log.info(f"Attempting to set retention period to {period}")
            try:
                # Should throw an exception or return failure
                self.bucket_util.update_bucket_property(self.cluster.master, self.bucket,
                                                        continuous_backup_retention_period=period)
                self.fail(f"Setting invalid retention period {period} succeeded, but should have failed")
            except Exception as e:
                self.log.info(f"Expected failure caught: {e}")

        self.log.info("Test completed successfully")

    def test_execution_scheduling(self):
        """TC-CONF-04: Execution Scheduling Validation

        Retention trigger interval is controlled by CB_CONTBK_RETENTION_CHECK_MINS
        (default 5 mins) set via environment variable.
        """
        self.log.info("Testing execution scheduling for retention")

        # Wait for one full check cycle plus a 30s buffer for execution
        wait_secs = self.retention_check_mins * 60 + 30
        self.sleep(wait_secs, f"Waiting {wait_secs}s for retention check cycle to trigger")

        # 3. Check cont_backup.log on each node for "running retention consolidation"
        log_path = "/opt/couchbase/var/lib/couchbase/logs/cont_backup.log"
        retention_triggered = False
        for server in self.cluster.servers:
            shell = RemoteMachineShellConnection(server)
            output, _ = shell.execute_command(
                f"grep 'running retention consolidation' {log_path}"
            )
            shell.disconnect()
            if output:
                self.log.info(f"Found retention consolidation log entry on {server.ip}: {output[0]}")
                retention_triggered = True
                break

        self.assertTrue(retention_triggered,
                        f"No 'running retention consolidation' entry found in cont_backup.log "
                        f"on any node after {wait_secs}s wait.")
        self.log.info("Test completed successfully")

    def _delete_and_recreate_bucket(self):
        """Delete and recreate self.bucket using its original bucket object."""
        self.log.info(f"Deleting bucket: {self.bucket.name}")
        self.bucket_util.delete_bucket(self.cluster, self.bucket)
        self.log.info(f"Recreating bucket: {self.bucket.name}")
        self.bucket_util.create_bucket(self.cluster, self.bucket)

    def _get_contbk_metrics(self):
        """Fetch contbk_* metric lines from each KV node via StatsHelper.get_all_metrics()."""
        contbk_lines = []
        for server in self.cluster.kv_nodes:
            lines = StatsHelper(server).get_all_metrics()
            for line in lines:
                if line.startswith("contbk_"):
                    contbk_lines.append(line.strip())
        return contbk_lines

    def _get_metric_value(self, lines, metric_name, labels=None):
        """
        Parse Prometheus text-format lines and return the float value for the first
        line matching metric_name and all provided label key=value pairs.
        Returns None if no match is found.
        """
        for line in lines:
            if not line.startswith(metric_name):
                continue
            if labels:
                if not all(f'{k}="{v}"' in line for k, v in labels.items()):
                    continue
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                try:
                    return float(parts[1])
                except ValueError:
                    continue
        return None

    def test_retention_prometheus_metrics(self):
        """TC-MET-01: Validate Prometheus metrics for continuous backup retention.

        Metrics checked:
          contbk_retention_runs              (counter)   bucket, status
          contbk_retention_run_time          (histogram) bucket
          contbk_retention_last_deleted      (gauge)     bucket
          contbk_retention_consolidation_runs (counter)  bucket, status
          contbk_retention_consolidation_run_time (histogram) bucket
        """
        self.log.info("Testing retention Prometheus metrics")
        bucket_name = self.bucket.name

        # Wait for at least one retention cycle plus a 30s buffer
        wait_secs = self.retention_check_mins * 60 + 30
        self.sleep(wait_secs, f"Waiting {wait_secs}s for retention cycle to complete before checking metrics")

        lines = self._get_contbk_metrics()
        self.assertTrue(lines, "No contbk_* metrics found on any KV node")

        # -- contbk_retention_runs (counter): succeeded runs must be > 0 --
        succeeded_runs = self._get_metric_value(
            lines, "contbk_retention_runs",
            labels={"bucket": bucket_name, "status": "succeeded"})
        self.assertIsNotNone(succeeded_runs,
                             f"contbk_retention_runs{{bucket={bucket_name},status=succeeded}} not found")
        self.assertGreater(succeeded_runs, 0,
                           f"Expected contbk_retention_runs succeeded > 0, got {succeeded_runs}")
        self.log.info(f"contbk_retention_runs succeeded={succeeded_runs}")

        # -- contbk_retention_run_time (histogram): _count > 0, _sum >= 0 --
        run_time_count = self._get_metric_value(
            lines, "contbk_retention_run_time_count", labels={"bucket": bucket_name})
        self.assertIsNotNone(run_time_count,
                             f"contbk_retention_run_time_count{{bucket={bucket_name}}} not found")
        self.assertGreater(run_time_count, 0,
                           f"Expected contbk_retention_run_time_count > 0, got {run_time_count}")
        self.log.info(f"contbk_retention_run_time_count={run_time_count}")

        run_time_sum = self._get_metric_value(
            lines, "contbk_retention_run_time_sum", labels={"bucket": bucket_name})
        self.assertIsNotNone(run_time_sum,
                             f"contbk_retention_run_time_sum{{bucket={bucket_name}}} not found")
        self.assertGreaterEqual(run_time_sum, 0,
                                f"Expected contbk_retention_run_time_sum >= 0, got {run_time_sum}")
        self.log.info(f"contbk_retention_run_time_sum={run_time_sum}")

        self.log.info("All retention Prometheus metrics validated successfully")

    def test_retention_pitr_restore(self):
        """Validate PITR restore before and after the retention period.

        Flow:
          1. Capture original doc count and a timestamp (T_before_add).
          2. Load 10000 additional docs.
          3. Wait for continuous backup interval so the new docs are captured.
          4. Restore everything (no writes happen after this point, so
             "everything" == T_after_backup); verify count = original + new docs.
          5. Wait for the retention check cycle so retention runs at least once.
          6. Restore to a new bucket at T_before_add; verify count = original (new docs absent).
        """
        self.log.info("Starting test_retention_pitr_restore")
        cluster_host = f"http://{self.cluster.master.ip}:8091"

        # 1. Original count and timestamp before loading new docs
        original_count = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
        self.log.info(f"Original doc count: {original_count}")

        t_before_add = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"Timestamp before adding docs: {t_before_add}")

        # 2. Load additional docs
        mutation_time = time.time()
        self._load_data_and_get_task(self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_with_new_docs = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
        self.log.info(f"Doc count after loading new docs: {count_with_new_docs}")
        self.assertGreater(count_with_new_docs - original_count, 0,
                           "No new docs were added; check the data spec")

        # 3. Wait for continuous backup to catch up to the new docs. Poll
        # rather than a fixed sleep -- see _wait_for_continuous_backup_catchup
        # for why a fixed interval isn't always enough.
        self._wait_for_continuous_backup_catchup(mutation_time)

        # 4. Delete and recreate bucket, restore everything → expect original + new docs.
        # No further writes happen after count_with_new_docs was captured, so
        # "everything" cbcontbk has is bounded by it -- no need for a specific
        # timestamp here (and "now" risks landing outside the backup's covered
        # range, since cbcontbk now rejects timestamps it doesn't contain).
        self._delete_and_recreate_bucket()
        self.cont_bk_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name,
            cluster_host=cluster_host,
            location=self.continuous_backup_location,
            temp_dir="/data/tmp",
            obj_staging_dir=self.obj_staging_dir_cont_bkp)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_with_new_docs)
        self.log.info(f"Restore of everything verified: {count_with_new_docs} docs")

        # 5. Wait for the retention check cycle to run
        retention_wait_secs = self.retention_check_mins * 60 + 30
        self.sleep(retention_wait_secs,
                   f"Waiting {retention_wait_secs}s for retention check cycle to run")

        # 6. Delete and recreate bucket, restore at T_before_add → expect only original count
        self._delete_and_recreate_bucket()
        self.cont_bk_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name,
            cluster_host=cluster_host,
            location=self.continuous_backup_location,
            temp_dir="/data/tmp",
            timestamp=t_before_add,
            obj_staging_dir=self.obj_staging_dir_cont_bkp)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(original_count)
        self.log.info(f"Restore at T_before_add verified: {original_count} docs (new docs absent)")

    def _snapshot_backup_location(self):
        """Return a {relative_path: (type, size_bytes)} snapshot of every
        directory, sub-directory and file under self.continuous_backup_location.

        Filesystem/NFS-only: the location is a local/NFS path on the node
        (see onPrem_basetestcase.py), listed here via `find` on self.shell
        (the master-node shell opened in ContinuousBackupBase.setUp).
          %y = entry type (d/f/l), %s = size in bytes, %p = full path.
        """
        location = self.continuous_backup_location
        command = ("find %s -mindepth 1 -printf '%%y\\t%%s\\t%%p\\n'"
                   % location)
        output, error = self.shell.execute_command(command)
        if error:
            self.fail(f"Failed to list backup location {location}: {error}")

        snapshot = {}
        for line in output:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) != 3:
                self.log.warning(f"Skipping unparseable find line: {line!r}")
                continue
            entry_type, size, path = parts
            # Key on the path relative to the location root so the snapshot is
            # comparable across calls regardless of the absolute prefix.
            relative_path = path[len(location):].lstrip("/")
            try:
                snapshot[relative_path] = (entry_type, int(size))
            except ValueError:
                snapshot[relative_path] = (entry_type, None)
        self.log.info(f"Backup location snapshot: {len(snapshot)} entries under "
                      f"{location}")
        return snapshot

    def _assert_backup_intact(self, before, after, when):
        """Fail if any entry in `before` is missing from `after` (deleted) or
        any file has shrunk. `when` describes the checkpoint for the message.
        """
        deleted = sorted(path for path in before if path not in after)
        shrunk = []
        for path, (entry_type, before_size) in before.items():
            if path not in after:
                continue
            after_size = after[path][1]
            if entry_type == "f" and before_size is not None \
                    and after_size is not None and after_size < before_size:
                shrunk.append(
                    f"{path} ({before_size} -> {after_size} bytes)")

        if deleted or shrunk:
            self.fail(
                f"Backup data changed {when} (continuous backup disabled).\n "
                f"Deleted entries ({len(deleted)}): {deleted}\n"
                f"Shrunk files ({len(shrunk)}): {shrunk}")

        self.log.info(
            "No entries deleted and no files shrunk %s: all %d backup entries "
            "intact" % (when, len(before)))

    def test_no_deletion_when_cont_bkp_disabled(self):
        """Continuous backup with retention off must never delete backup data.

        Covers MB-70628.

        Flow:
          1. Load data while continuous backup is running.
          2. Snapshot every dir/sub-dir/file (with size) under
             self.continuous_backup_location.
          3. Turn continuous backup off.
          4. Wait 2 minutes and snapshot again (catch any early deletion).
          5. Wait a further 1.5 * continuous backup retention period minutes.
          6. Snapshot again. At neither checkpoint may anything from step 2
             be deleted, and no file may have shrunk.
        """
        # 1. Load data while continuous backup is running.
        self.log.info("Loading data while continuous backup is running")
        self._load_data_and_get_task(self.data_spec_name)
        self.bucket_util.print_bucket_stats(self.cluster)

        # 2. Snapshot the backup location.
        before = self._snapshot_backup_location()
        self.assertTrue(
            before,
            f"No backup files found under {self.continuous_backup_location} "
            f"after loading; nothing to validate. Check that continuous "
            f"backup was enabled and the data spec actually loaded docs.")

        # 3. Turn continuous backup off.
        self.log.info("Disabling continuous backup on bucket %s" % self.bucket.name)
        self.bucket_util.update_bucket_property(
            self.cluster.master, self.bucket, continuous_backup_enabled=False)

        # 4. Wait 2 minutes and snapshot — nothing should have been deleted yet.
        self.sleep(2 * 60,
                   "Waiting 2 minutes after disabling continuous backup before "
                   "the first snapshot")
        after_2min = self._snapshot_backup_location()
        self._assert_backup_intact(before, after_2min,
                                   "2 minutes after disabling continuous backup")

        # 5. Wait a further 1.5 * continuous backup retention period hour(s).
        params = self.bucket_util.get_continuous_backup_params(self.cluster, self.bucket.name)
        retention_period = int(params.get("continuousBackupRetentionPeriod"))
        self.log.info(f"Retrieved continuousBackupRetentionPeriod: {retention_period}")
        self.sleep(int(retention_period * 1.5 * 60 * 60),
                   "Waiting a further 1.5 * continuous backup retention ({} hour(s)) period minutes with continuous backup "
                   "disabled to confirm no backup data is deleted".format(int(retention_period * 1.5)))

        # 6. Snapshot again and diff against the pre-disable snapshot.
        after_retention_time = self._snapshot_backup_location()
        self._assert_backup_intact(before, after_retention_time,
                                   "1.5x continuous backup retention period ({} hour(s)) after " \
                                   "disabling continuous backup".format(int(retention_period * 1.5)))

    def test_restore_fails_after_retention_deletes_data(self):
        """Restore must fail once retention has deleted data with no
        traditional backup to anchor it.

        Covers MB-72528, MB-72706, MB-72488.

        Flow:
          1. Load data repeatedly (load, sleep, repeat) for ~1.5x the continuous backup retention period so
             continuous backup keeps capturing and retention keeps running.
          2. Capture a cluster timestamp after the ~1.5x retention period load.
          3. Create and flush the restore bucket.
          4. Attempt a full restore at that timestamp.
          5. The restore must FAIL with the message:
             "cannot restore as retention has run on the continuous backup
             and deleted data since the last traditional backup".
        """

        def as_text(value):
            """Flatten cbcontbk output/error (list of lines or str) into one str."""
            if value is None:
                return ""
            if isinstance(value, (list, tuple)):
                return "\n".join(str(item) for item in value)
            return str(value)

        # 1. Load in a loop for 1.5 * continuousBackupRetentionPeriod minutes total, sleeping between loads so
        #    continuous backup captures each batch and retention runs.
        params = self.bucket_util.get_continuous_backup_params(self.cluster, self.bucket.name)
        retention_period = int(params.get("continuousBackupRetentionPeriod"))
        self.log.info(f"Retrieved continuousBackupRetentionPeriod: {retention_period}")
        total_load_secs = retention_period * 1.5 * 60 * 60 # 1.5x the retention period in seconds
        deadline = time.time() + total_load_secs
        iteration = 0
        while time.time() < deadline:
            iteration += 1
            self.log.info(f"Load iteration {iteration} "
                          f"({int(deadline - time.time())}s left in load window)")
            self._load_data_and_get_task(self.data_spec_name)
            self.bucket_util.print_bucket_stats(self.cluster)
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            self.sleep(900,
                       f"Sleeping before next load (iteration {iteration})")

        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes after last load")
        self.bucket_util.print_bucket_stats(self.cluster)

        # 2. Timestamp after the ~90-minute load window.
        timestamp = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"Captured timestamp after ~{int(total_load_secs / 60)} min of loading: {timestamp}")

        # 3. Create and flush the restore bucket.
        restore_bucket_name = f"restore_bucket_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)
        self._flush_restore_bucket(restore_bucket_name)

        # 4. Attempt the restore (expected to fail, so skip the success assert).
        output, error = self._restore_entire_bucket(
            timestamp, restore_bucket_name, assert_success=False)

        # 5. Assert the restore failed with the retention-deleted-data message.
        expected_msg = ("cannot restore as retention has run on the continuous "
                        "backup and deleted data since the last traditional "
                        "backup")
        combined = as_text(output) + "\n" + as_text(error)
        self.assertIn(
            expected_msg, combined,
            f"Restore at {timestamp} did not fail with the expected retention "
            f"message.\nExpected substring: {expected_msg!r}\n"
            f"stdout: {output}\nstderr: {error}")
        self.log.info("Restore failed with the expected retention message")


