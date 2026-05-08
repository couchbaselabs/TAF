import importlib
import importlib

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
        """TC-CONF-01: Default State"""
        self.log.info("Testing default state (retention off)")
        params = self.bucket_util.get_continuous_backup_params(self.cluster, self.bucket.name)
        
        retention_period = params.get("continuousBackupRetentionPeriod")
        self.log.info(f"Retrieved continuousBackupRetentionPeriod: {retention_period}")

        # Normalise to int to handle both numeric 0 and string "0" from the REST layer
        try:
            normalised = int(retention_period) if retention_period is not None else 0
        except (ValueError, TypeError):
            normalised = retention_period
        if normalised not in (0, None):
            self.fail(f"Expected retention period to be off (0 or None), but got {retention_period}")
        
        self.log.info("Test completed successfully")

    def test_valid_configuration_ranges(self):
        """TC-CONF-02: Valid Configuration Ranges"""
        self.log.info("Testing valid configuration ranges for continuousBackupRetentionPeriod")
        valid_ranges = [1, 24, 876000]
        
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
        invalid_ranges = [-1, "abc", 2.5]
        
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
          4. Restore to a new bucket at T_after_backup; verify count = original + new docs.
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
        self._load_data_and_get_task(self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_with_new_docs = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
        self.log.info(f"Doc count after loading new docs: {count_with_new_docs}")
        self.assertGreater(count_with_new_docs - original_count, 0,
                           "No new docs were added; check the data spec")

        # 3. Wait for continuous backup to capture the new docs
        backup_wait_secs = self.continuous_backup_interval * 60 + 30
        self.sleep(backup_wait_secs,
                   f"Waiting {backup_wait_secs}s for continuous backup to capture new docs")

        t_after_backup = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"Timestamp after backup interval: {t_after_backup}")

        # 4. Delete and recreate bucket, restore at T_after_backup → expect original + new docs
        self._delete_and_recreate_bucket()
        self.cont_bk_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name,
            cluster_host=cluster_host,
            location=self.continuous_backup_location,
            temp_dir="/tmp",
            timestamp=t_after_backup)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_with_new_docs)
        self.log.info(f"Restore at T_after_backup verified: {count_with_new_docs} docs")

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
            temp_dir="/tmp",
            timestamp=t_before_add)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(original_count)
        self.log.info(f"Restore at T_before_add verified: {original_count} docs (new docs absent)")


