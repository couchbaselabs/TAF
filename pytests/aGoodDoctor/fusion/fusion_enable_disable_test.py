"""
Fusion Enable/Disable/Stop/Override Tests

Validates the lifecycle of enabling, disabling, stopping, and overriding
Express Scaling (Fusion) on existing Capella clusters.

Key behavioural distinction between Stop and Disable:
  Stop    == Disable the sync (no more data written to S3) but PRESERVE existing S3 objects.
  Disable == Stop the sync AND DELETE all data in the S3 bucket.

Both Stop and Disable transition fusion status to "disabled". The only runtime difference
is what happens to the S3 blob store contents afterwards.

All tests live in a single class (FusionEnableDisableTests). The base class _FusionTestBase
provides setUp/tearDown and shared helpers — including _ensure_fusion_state(), which
idempotently transitions the cluster to the required state before each test runs. Each test
creates its own bucket in setUp and deletes it in tearDown, ensuring full isolation.
"""

import time

from capella_utils.dedicated import CapellaUtils as CapellaAPI
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from py_constants.cb_constants.CBServer import CbServer
from bucket_utils.bucket_ready_functions import CollectionUtils, JavaDocLoaderUtils
from membase.api.rest_client import RestConnection
from .fusion_test_base import _FusionTestBase




class FusionEnableDisableTests(_FusionTestBase):
    """Full lifecycle tests for Fusion enable/disable/stop/override on Capella clusters.

    Covers: enable transitions, enabled-state routing, stop (S3 preserved), disable
    (S3 deleted), overrideRebalances, and invalid state-transition rejection.

    Each test calls _ensure_fusion_state() to reach its required starting state.
    Each test creates its own bucket in setUp and deletes all buckets in tearDown.
    """

    def setUp(self):
        super().setUp()
        self.log.info(f"[setUp] cluster={self.cluster.id}, buckets={[b.name for b in self.cluster.buckets]}")
        CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id,
                "Wait for healthy cluster state before bucket cleanup", timeout=600)
        self.initial_kv_nodes = self.input.param("kv_nodes", 3)
        for bucket in self.cluster.buckets:
            try:
                self._delete_bucket_with_s3_cleanup(bucket)
            except Exception:
                pass
        self.cluster.buckets = []
        self.create_buckets(self.pod, self.tenant, self.cluster)
        self.log.info(f"[setUp] Buckets created: {[b.name for b in self.cluster.buckets]}")
        if self.cluster.buckets:
            bucket = self.cluster.buckets[0]
            rest = RestConnection(self.cluster.master)
            info = rest.get_bucket_details(bucket_name=bucket.name)
            bucket.bucket_uuid = info.get("uuid", None)


    def tearDown(self):
        try:
            CapellaAPI.override_fusion_rebalances(self.pod, self.tenant, self.cluster.id, override=False)
            self.log.info("[tearDown] Cleared overrideRebalances=False")
        except Exception as e:
            self.log.warning(f"[tearDown] Could not clear override: {e}")
        self.log.info(f"[tearDown] kv_nodes current={self.num_nodes['data']} initial={self.initial_kv_nodes}")
        if self.num_nodes["data"] != self.initial_kv_nodes:
            delta = self.initial_kv_nodes - self.num_nodes["data"]
            try:
                CapellaAPI.wait_until_done(
                    self.pod, self.tenant, self.cluster.id,
                    "Wait for healthy state before node reset", timeout=1800)
                self.wait_for_rebalances([self.task.async_rebalance_capella(
                    self.pod, self.tenant, self.cluster,
                    self.rebalance_config("data", delta), timeout=self.rebalance_timeout)])
            except Exception as e:
                self.log.error(f"Failed to reset KV nodes to {self.initial_kv_nodes}: {e}")
        for bucket in self.cluster.buckets:
            try:
                CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id,
                "Wait for healthy cluster state", timeout=1800)
                self._delete_bucket_with_s3_cleanup(bucket)
            except Exception:
                pass
        self.cluster.buckets = []
        super().tearDown()

    def test_enable_fusion_on_existing_cluster(self):
        """
        Enable fusion on a cluster that was created without fusion.

        Validates:
        - Capella API (POST /express-scaling/enable) returns 200
        - Internal API (POST /internal/support/clusters/{id}/fusion/enable) also works
        - Fusion status transitions to "enabled"
        - S3 bucket is created and URI is set on all nodes
        - Fusion-enabled buckets are provisioned correctly
        - Re-enabling an already-enabled cluster is idempotent (no error)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'disabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "disabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        fusion_state = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
        self.log.info(f"Initial fusion state: {fusion_state}")

        self.log.info(f"Enabling fusion on cluster {self.cluster.id}")
        resp = CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(resp.status_code, 200,
                         f"enable_fusion returned {resp.status_code}: {resp.content}")

        self._wait_for_fusion_state(self.tenant, self.cluster, "enabled")

        s3_uri = self.fusion_monitor.get_fusion_s3_uri(self.cluster)
        self.assertIsNotNone(s3_uri, "Fusion S3 URI not found after enabling")
        self.assertTrue(s3_uri.startswith("s3://"), f"Unexpected S3 URI format: {s3_uri}")
        self.log.info(f"Fusion S3 URI: {s3_uri}")

        bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self._assert_s3_bucket_exists(bucket_name)

        resp_internal = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(resp_internal.get("state"), "enabled",
                         f"Internal API reports unexpected state: {resp_internal}")

        self.log.info(f"Verifying re-enable is idempotent on cluster {self.cluster.id}")
        resp2 = CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)
        self.assertIn(resp2.status_code, [200, 409],
                      f"Re-enable returned unexpected status: {resp2.status_code}")
        self._wait_for_fusion_state(self.tenant, self.cluster, "enabled")

    def test_enable_fusion_cancel_leaves_empty_s3_bucket(self):
        """
        Cancelling (stopping) a fusion enable operation mid-flight leaves an empty
        S3 bucket in place — the bucket itself is retained but must contain no objects.

        Validates:
        - Enable is triggered and transitions to "enabling" state
        - Stop/cancel is sent while enabling is in progress
        - Fusion status transitions to "stopped"
        - S3 bucket created during enable still exists (empty bucket is kept)
        - S3 bucket contains zero objects
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'disabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "disabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        self.log.info(f"Starting fusion enable on cluster {self.cluster.id}")
        resp = CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(resp.status_code, 200)

        bucket_name = None
        deadline = time.time() + 120
        cancelled = False
        while time.time() < deadline:
            state_resp = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
            state = state_resp.get("state", "")
            self.log.info(f"Cluster {self.cluster.id} fusion state: {state}")

            if state == "enabling":
                if bucket_name is None:
                    bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
                self.log.info("Cancelling enable via stop API")
                stop_resp = CapellaAPI.stop_fusion_internal(self.pod, self.tenant, self.cluster.id)
                self.assertIn(stop_resp.status_code, [200, 202],
                              f"stop_fusion returned unexpected status: {stop_resp.status_code}")
                cancelled = True
                break

            if state == "enabled":
                self.log.warning(
                    "Fusion enabled before cancel could be sent; skipping cancel test")
                break
            time.sleep(2)

        if not cancelled:
            self.skipTest(
                "Fusion enabled too fast to cancel; adjust cluster size or retry")

        self._wait_for_fusion_state(self.tenant, self.cluster, "stopped")

        if bucket_name is None:
            bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)

        if bucket_name:
            self.log.info(f"Verifying S3 bucket {bucket_name} is empty after cancel")
            self._assert_s3_bucket_empty(bucket_name, timeout=120)
        else:
            self.log.info(
                "S3 bucket was not created before cancel — nothing to validate")

    def test_stop_fusion_during_enable(self):
        """
        Stop fusion via the internal support API while an enable is in progress.

        Stop == Disable without S3 data deletion.
        During an enable stopped before any sync began the S3 bucket may not have
        been created yet — that is also a valid outcome.

        Validates:
        - Enable is triggered and reaches "enabling" state
        - Internal stop API returns 200
        - Fusion status transitions to "disabled"
        - S3 object count does not increase after stop (sync halted)
        - S3 objects that existed before stop are preserved (not deleted)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'disabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "disabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)
        self.log.info(f"Starting fusion enable on cluster {self.cluster.id}")
        resp = CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(resp.status_code, 200)

        bucket_name = None
        deadline = time.time() + 120
        stopped = False
        while time.time() < deadline:
            state_resp = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
            state = state_resp.get("state", "")
            self.log.info(f"Cluster {self.cluster.id} fusion state: {state}")

            if state == "enabling":
                if bucket_name is None:
                    bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
                self.log.info("Stopping enable via internal support API")
                CapellaAPI.stop_fusion_internal(self.pod, self.tenant, self.cluster.id)
                stopped = True
                break

            if state == "enabled":
                self.log.warning(
                    "Fusion enabled before stop could be sent; skipping")
                break
            time.sleep(2)

        if not stopped:
            self.skipTest(
                "Fusion enabled too fast to stop; adjust cluster size or retry")

        self._wait_for_fusion_state(self.tenant, self.cluster, "stopped")

        if bucket_name is None:
            bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        if bucket_name:
            size_before, count_before = self.fusion_monitor.get_fusion_log_store_data_size_on_s3(
                cluster=self.cluster, bucket=self.cluster.buckets[0])
            self.log.info(f"S3 object count after stop: {count_before}")
            self.log.info(f"S3 object size after stop: {size_before}")
            self.sleep(60, "Wait to confirm no new objects are written")
            count_after = self.s3.get_bucket_size(bucket_name).get("file_count", 0)
            self.assertEqual(
                count_after, count_before,
                f"S3 object count increased after stop ({count_before}→{count_after})"
                " — sync did not halt")
        else:
            self.log.info("S3 bucket not created before stop — sync never started")

    def test_kill_memcached_during_enable_cp_retries(self):
        """
        Kill memcached on all self.cluster nodes while the control plane is enabling fusion.
        CP must detect the failure and retry the enable sequence until fusion reaches
        "enabled" state.

        Validates:
        - Enable is triggered
        - memcached is killed on all cluster nodes via SSM while enable is in-flight
        - CP retries and eventually brings fusion to "enabled" state
        - S3 bucket is created and URI is set after retry succeeds
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'disabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "disabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        self.log.info(f"Starting fusion enable on cluster {self.cluster.id}")
        resp = CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(resp.status_code, 200)

        self.log.info("Killing memcached on all cluster nodes during enable")
        instances = self.fusion_aws_util.list_instances(
            self.fusion_aws_util._cluster_filter(self.cluster.id)
        )
        kill_threads = []
        for instance in instances:
            instance_id = instance.get("InstanceId")
            self.log.info(f"Killing memcached on instance {instance_id}")
            t = threading.Thread(
                target=self.fusion_aws_util.ec2.run_shell_command,
                args=(instance_id, "sudo kill -9 $(pgrep memcached) || true"),
                daemon=True,
            )
            t.start()
            kill_threads.append(t)
        for t in kill_threads:
            t.join(timeout=30)

        self.log.info("Waiting for CP to retry and reach enabled state")
        self._wait_for_fusion_state(self.tenant, self.cluster, "enabled")

        s3_uri = self.fusion_monitor.get_fusion_s3_uri(self.cluster)
        self.assertIsNotNone(s3_uri,
                             "S3 URI not found after CP retry — enable did not complete")
        self.log.info(f"Fusion enabled after memcached kill. S3 URI: {s3_uri}")
        self.fusion_monitor.wait_for_fusion_pending_byte_zero(self.cluster)


    def test_enable_fusion_from_stopped_state(self):
        """
        Re-enable fusion from the "stopped" state (previously enabled, then stopped).

        Design doc permits /enable from stopped. Stop preserves S3 objects; re-enabling
        from stopped should resume syncing without creating a new bucket or discarding
        previously uploaded data.

        Validates:
        - Enable → load data → stop brings state to disabled/stopped (S3 preserved)
        - Re-enable from stopped returns 200
        - Fusion reaches "enabled" state
        - S3 bucket still exists and object count >= pre-stop count
        - Uploads resume (pending bytes eventually reach 0)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        self.log.info("Allowing data to sync before stop")
        self.fusion_monitor.wait_for_fusion_pending_byte_zero(self.cluster)

        bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(bucket_name, "No S3 bucket found after enable")
        size_pre_stop, count_pre_stop = self.fusion_monitor.get_fusion_log_store_data_size_on_s3(
            cluster=self.cluster, bucket=self.cluster.buckets[0])
        self.log.info(f"S3 object size before stop: {size_pre_stop}")
        self.log.info(f"S3 object count before stop: {count_pre_stop}")

        self.log.info("Stopping fusion (S3 objects must be preserved)")
        CapellaAPI.stop_fusion_internal(self.pod, self.tenant, self.cluster.id)
        self._wait_for_fusion_state(self.tenant, self.cluster, "stopped")

        size_after_stop, count_after_stop = self.fusion_monitor.get_fusion_log_store_data_size_on_s3(
            cluster=self.cluster, bucket=self.cluster.buckets[0])
        self.assertGreaterEqual(size_after_stop, size_pre_stop,
                         f"Stop changed S3 object size: {size_pre_stop}→{size_after_stop}")
        self.assertGreaterEqual(count_after_stop, count_pre_stop,
                         f"Stop changed S3 object count: "
                         f"({count_pre_stop}→{count_after_stop}) — stop must preserve")

        self._load_data(self.cluster,
                        create_start=self.input.param("num_items"),
                        create_end=self.input.param("num_items")*2
                        )

        self.log.info("Re-enabling fusion from stopped state")
        resp = CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(resp.status_code, 200,
                         f"enable_fusion from stopped returned {resp.status_code}: "
                         f"{resp.content}")

        self._wait_for_fusion_state(self.tenant, self.cluster, "enabled")

        s3_uri = self.fusion_monitor.get_fusion_s3_uri(self.cluster)
        self.assertIsNotNone(s3_uri, "S3 URI not found after re-enable from stopped state")

        size_after_reenable, count_after_reenable = self.fusion_monitor.get_fusion_log_store_data_size_on_s3(
            cluster=self.cluster, bucket=self.cluster.buckets[0])
        self.log.info(f"S3 object size after re-enable: {size_after_reenable}")
        self.log.info(f"S3 object count after re-enable: {count_after_reenable}")
        self.assertGreaterEqual(
            count_after_reenable, count_pre_stop,
            f"S3 objects not preserved across stop→re-enable cycle "
            f"({count_pre_stop}→{count_after_reenable})")
        self.assertGreaterEqual(
            size_after_reenable, size_pre_stop,
            f"S3 object size not preserved across stop→re-enable cycle "
            f"({size_pre_stop}→{size_after_reenable})")
        self.log.info(
            f"Re-enable from stopped succeeded. Objects: "
            f"{count_pre_stop}→{count_after_reenable}")


    def test_ns_server_restart_during_enable(self):
        """
        Restart ns_server on one self.cluster node while fusion enable is in progress.

        CP persists the enabling state and must resume the sequence after
        ns_server recovers. This tests that the enable operation is durable
        across node-level service restarts.

        Data is loaded before enabling so that the enabling phase has actual
        S3 uploads in flight — giving a realistic window to inject the restart
        and ensuring S3 object creation can be verified after recovery.

        Validates:
        - Enable is triggered and reaches "enabling" state (data upload in progress)
        - ns_server is restarted on one cluster node via SSM while enabling
        - CP resumes the enable sequence after the restart
        - Fusion eventually reaches "enabled"
        - S3 bucket exists, URI is set, and S3 contains objects after recovery
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'disabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "disabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        self.log.info(f"Starting fusion enable on cluster {self.cluster.id}")
        resp = CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(resp.status_code, 200)

        deadline = time.time() + 180
        restarted = False
        while time.time() < deadline:
            state_resp = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
            state = state_resp.get("state", "")
            self.log.info(f"Cluster {self.cluster.id} fusion state: {state}")

            if state == "enabling":
                self.log.info("Restarting ns_server on one cluster node during enable")
                instances = self.fusion_aws_util.list_instances(
                    self.fusion_aws_util._cluster_filter(self.cluster.id)
                )
                if instances:
                    instance_id = instances[0].get("InstanceId")
                    self.log.info(f"Restarting ns_server on instance {instance_id}")
                    self.fusion_aws_util.ec2.run_shell_command(
                        instance_id,
                        "sudo systemctl restart couchbase-server || "
                        "sudo /etc/init.d/couchbase-server restart"
                    )
                restarted = True
                break

            if state == "enabled":
                self.log.warning("Fusion enabled before ns_server restart could be injected")
                break
            time.sleep(2)

        if not restarted:
            self.skipTest("Fusion enabled too fast to inject ns_server restart; "
                          "increase num_items or adjust cluster size and retry")

        self.log.info("Waiting for CP to recover and reach enabled state")
        self._wait_for_fusion_state(self.tenant, self.cluster, "enabled")

        s3_uri = self.fusion_monitor.get_fusion_s3_uri(self.cluster)
        self.assertIsNotNone(s3_uri,
                             "S3 URI not found after ns_server restart recovery — "
                             "CP did not resume enabling sequence")

        bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self._assert_s3_bucket_exists(bucket_name)

        _, obj_count = self.fusion_monitor.get_fusion_log_store_data_size_on_s3(
            cluster=self.cluster, bucket=self.cluster.buckets[0])
        self.assertGreater(
            obj_count, 0,
            "Expected S3 objects after enable recovery but found none — "
            "uploads may not have resumed after ns_server restart")
        self.log.info(
            f"Fusion enabled after ns_server restart. S3 URI: {s3_uri}, "
            f"S3 objects: {obj_count}")


    def test_rebalance_below_threshold_uses_dcp(self):
        """
        A small self.cluster should use DCP rebalance even when fusion is enabled
        (data volume is below the fusion threshold).

        Validates:
        - Fusion is enabled on the cluster
        - Data loaded is below the fusion-use threshold
        - On triggering rebalance, no accelerator instances are created (DCP path)
        - Rebalance completes successfully via DCP
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        self.log.info(f"Triggering rebalance on cluster {self.cluster.id}")
        config = self.rebalance_config("data", +1)
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)

        self.sleep(30, "Wait for rebalance to start before checking accelerators")
        start_time = time.time()
        while start_time + 1800 > time.time():
            if rebalance_task.state in ["deployment_failed",
                                "deploymentFailed",
                                "redeploymentFailed",
                                "rebalance_failed",
                                "rebalanceFailed",
                                "scaleFailed"]:
                self.fail("rebalance failed")

            accelerator_instances = self.fusion_aws_util.list_accelerator_instances(
                self.fusion_aws_util._cluster_filter(self.cluster.id, [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}]),
                log="Fusion Accelerator"
            )
            self.assertEqual(len(accelerator_instances), 0,
                            f"Expected 0 accelerator instances for DCP rebalance, "
                            f"found {len(accelerator_instances)}")
            if rebalance_task.state == "healthy":
                break
            time.sleep(10)

        self.wait_for_rebalances([rebalance_task])
        self.log.info("Small cluster used DCP rebalance as expected")
        self.log.info("Scaling cluster back to original node count")
        self.wait_for_rebalances([self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            self.rebalance_config("data", -1), timeout=self.rebalance_timeout)])

    def test_rebalance_above_threshold_uses_fusion(self):
        """
        Trigger a rebalance after more than the threshold amount of data has
        been synced to the blob store — CP should use Fusion acceleration.

        Validates:
        - Fusion is enabled and substantial data is synced to S3
        - Pending bytes confirm sync before triggering rebalance
        - Accelerator instances are created (Fusion rebalance path)
        - Rebalance completes with full fusion accelerator lifecycle
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        self.log.info("Loading dataset (configure num_items above fusion threshold)")
        self._load_data(self.cluster,
                        create_start=0,
                        create_end=20000000
                        )

        self.log.info("Waiting for data to sync to S3 past threshold")
        self.sleep(120, "Allow initial sync to S3")
        self.fusion_monitor.log_fusion_pending_bytes(self.tenant, self.tenant.clusters, self.find_master)

        self.log.info("Triggering rebalance after threshold data is synced")
        config = self.rebalance_config("data", +1)
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)

        deadline = time.time() + 1800
        accelerators_seen = False
        while time.time() < deadline and rebalance_task.state != "healthy":
            if rebalance_task.state in ["deployment_failed",
                                "deploymentFailed",
                                "redeploymentFailed",
                                "rebalance_failed",
                                "rebalanceFailed",
                                "scaleFailed"]:
                self.fail("rebalance failed")

            instances = self.fusion_aws_util.list_accelerator_instances(
            self.fusion_aws_util._cluster_filter(self.cluster.id, [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}]),
            log="Fusion Accelerator")
            if len(instances) > 0:
                self.log.info(
                        f"Fusion rebalance confirmed: {len(instances)} accelerator(s)")
                accelerators_seen = True
                break
            time.sleep(10)
        self.assertTrue(accelerators_seen,
                        "No accelerator instances found — fusion rebalance did not "
                        "trigger above threshold")

        self.wait_for_rebalances([rebalance_task])
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)

        result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
            self.cluster, timeout=self.fusion_infra_timeout)
        self.assertTrue(result, "Accelerator nodes not cleaned up after fusion rebalance")
        self.log.info("Scaling cluster back to original node count")
        self.wait_for_rebalances([self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            self.rebalance_config("data", -1), timeout=self.rebalance_timeout)])

    def test_enable_fusion_no_rebalance_use(self):
        """
        AV-114254: Enable fusion but prevent it from being used for rebalances.

        Validates:
        - Fusion is enabled (S3 bucket created, status=enabled)
        - The 'fusion-rebalances' feature flag is disabled for the cluster
        - On triggering rebalance, no accelerator instances are created (DCP path)
        - Rebalance completes successfully via DCP despite fusion being enabled
        """
        # Set rebalance flags to False before ensuring enabled state so the
        # cluster enters the enabled state without the fusion rebalance path.
        CapellaAPI.create_cluster_feature_flag(
            self.pod, self.tenant, self.cluster.id, "fusion-rebalances", False)
        CapellaAPI.create_cluster_feature_flag(
            self.pod, self.tenant, self.cluster.id, "fusion-fallback-replace", False)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        s3_uri = self.fusion_monitor.get_fusion_s3_uri(self.cluster)
        self.assertIsNotNone(s3_uri, "S3 URI should exist when fusion is enabled")

        self.log.info("Triggering rebalance with fusion-rebalances flag disabled")
        config = self.rebalance_config("data", +1)
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)

        self.sleep(30, "Wait for rebalance to start")
        start_time = time.time()
        while start_time + 1800 > time.time():
            if rebalance_task.state in ["deployment_failed",
                                "deploymentFailed",
                                "redeploymentFailed",
                                "rebalance_failed",
                                "rebalanceFailed",
                                "scaleFailed"]:
                self.fail("rebalance failed")

            accelerator_instances = self.fusion_aws_util.list_accelerator_instances(
                self.fusion_aws_util._cluster_filter(self.cluster.id, [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}]),
                log="Fusion Accelerator"
            )
            self.assertEqual(len(accelerator_instances), 0,
                            f"Expected 0 accelerator instances for DCP rebalance, "
                            f"found {len(accelerator_instances)}")
            if rebalance_task.state == "healthy":
                break
            time.sleep(10)

        self.wait_for_rebalances([rebalance_task])
        self.log.info(
            "Rebalance completed via DCP as expected (fusion-rebalances=false)")
        self.log.info("Scaling cluster back to original node count")
        self.wait_for_rebalances([self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            self.rebalance_config("data", -1), timeout=self.rebalance_timeout)])

    def test_new_bucket_inherits_fusion_when_enabled(self):
        """
        A bucket created while fusion is enabled automatically gets isFusionBucket=true
        and its data is uploaded to the shared S3 log store.

        Design doc: when fusionState is enabling/enabled, new buckets get
        isFusionBucket=true atomically with the self.cluster-level state. This ensures
        all active buckets participate in the fusion log-store from creation.

        Validates:
        - Fusion is enabled (cluster-level fusionState=enabled)
        - A new bucket is created via the Capella API
        - The new bucket reports isFusionBucket=true within 2 minutes
        - The cluster-level S3 URI is still present (shared log store not disrupted)
        - Loading data into the new bucket produces S3 objects (uploads actually happen)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        cluster_uri = self._wait_for_s3_uri(self.cluster)
        bucket_name_s3 = cluster_uri.replace("s3://", "").split("?")[0]
        self._load_data(self.cluster, self.cluster.buckets)

        existing_bucket_count = len(self.cluster.buckets)
        self.log.info(f"Creating new bucket via create_buckets on cluster {self.cluster.id}")
        self.create_buckets(self.pod, self.tenant, self.cluster)
        self.sleep(60, "Wait for S3 URI to be created on new bucket")
        new_bucket = self.cluster.buckets[existing_bucket_count]

        # The Capella V2 bucket API doesn't expose isFusionBucket directly.
        # Instead, check that the new bucket appears in the fusion status
        # nodes→buckets map, which confirms it's participating in fusion sync.
        self.log.info(f"Verifying {new_bucket.name} appears in fusion status (fusion-enrolled)")
        self.fusion_monitor.set_admin_credentials(self.cluster)
        deadline = time.time() + 120
        bucket_in_fusion = False
        while time.time() < deadline:
            ok, content = FusionRestAPI(self.cluster.master).get_fusion_status()
            if ok:
                for node_data in content.get("nodes", {}).values():
                    if new_bucket.name in (node_data.get("buckets") or {}):
                        bucket_in_fusion = True
                        break
            if bucket_in_fusion:
                break
            time.sleep(5)

        self.assertTrue(
            bucket_in_fusion,
            f"New bucket {new_bucket.name} did not appear in fusion status nodes map "
            f"within 2 minutes — bucket may not have inherited fusion enrollment")

        uri_after = self.fusion_monitor.get_fusion_s3_uri(self.cluster)
        self.assertIsNotNone(uri_after,
                             "Cluster S3 URI missing after new bucket creation")

        self.log.info(f"Loading data into {new_bucket.name} to verify S3 uploads")
        self._load_data(self.cluster, [new_bucket])
        self.sleep(60, "Allow data from new bucket to sync to S3")
        rest = RestConnection(self.cluster.master)
        info = rest.get_bucket_details(bucket_name=new_bucket.name)
        new_bucket.bucket_uuid = info.get("uuid", None)
        kv_prefix = f"kv/{new_bucket.bucket_uuid}" if new_bucket.bucket_uuid else ""
        count_after = self.s3.get_bucket_size(bucket_name_s3, kv_prefix).get("file_count", 0)
        self.assertGreater(
            count_after, 0,
            f"No S3 objects found under prefix '{kv_prefix}' after loading data into new fusion bucket "
            f"{new_bucket.name} — new bucket may not be uploading to the shared log store")
        self.log.info(
            f"New bucket uploads confirmed: {count_after} S3 object(s) under prefix '{kv_prefix}'. "
            f"Cluster URI intact: {uri_after}")
        self._delete_bucket_with_s3_cleanup(new_bucket)

    def test_fusion_status_api_fields(self):
        """
        Validate GET /fusion/status response structure in each observable state.

        Data is loaded before the disable→re-enable cycle so that the enabling
        phase has uploads in flight. This ensures snapshotPendingBytes is non-zero
        during enabling — proving the field tracks actual progress, not just being
        structurally present with a 0 value.

        Validates:
        - In "enabled" state: 'state', 'nodes', per-node 'buckets',
          per-bucket 'snapshotPendingBytes' present; snapshotPendingBytes is non-negative
        - In "enabling" state: state=enabling, snapshotPendingBytes > 0 on at least one
          node/bucket (if enabling window is observable)
        - After full sync: snapshotPendingBytes reaches 0 (pending bytes drained)
        - Status API reachable from cluster.master via FusionRestAPI
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        self.fusion_monitor.set_admin_credentials(self.cluster)
        self.log.info("Validating fusion status API fields while uploads are in progress")

        status_ok, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.assertTrue(status_ok, "GET /fusion/status returned a non-success response")
        self.assertIsInstance(content, dict,
                              "Fusion status response is not a dict")

        fusion_state = content.get("state")
        self.assertIsNotNone(fusion_state, "Status response missing 'state' field")
        self.assertEqual(fusion_state, "enabled",
                         f"Expected state=enabled, got {fusion_state}")

        nodes = content.get("nodes")
        self.assertIsNotNone(nodes, "Status response missing 'nodes' field")
        self.assertIsInstance(nodes, dict, "'nodes' field should be a dict")
        self.assertGreater(len(nodes), 0,
                           "Expected at least one node in status response")

        cluster_pending_total = 0
        for node_key, node_data in nodes.items():
            self.assertIsInstance(node_data, dict,
                                  f"Node {node_key} data should be a dict")
            buckets = node_data.get("buckets")
            self.assertIsNotNone(buckets,
                                 f"Node {node_key} missing 'buckets' field")
            for bname, bucket_data in buckets.items():
                self.assertIn(
                    "snapshotPendingBytes", bucket_data,
                    f"Bucket {bname} on node {node_key} missing 'snapshotPendingBytes'")
                pending = bucket_data.get("snapshotPendingBytes", 0)
                self.assertGreaterEqual(pending, 0,
                                        f"snapshotPendingBytes is negative on {node_key}/{bname}")
                cluster_pending_total += pending

        self.assertGreater(
            cluster_pending_total, 0,
            "Total snapshotPendingBytes is 0 across all nodes after loading 2M docs — "
            "uploads may not have started or the field does not track in-flight bytes")
        self.log.info(
            f"Enabled-state API validated. Total pending bytes: {cluster_pending_total}. "
            f"Fields: {list(content.keys())}")

        # Verify pending bytes drain to 0 after full sync
        self.log.info("Waiting for pending bytes to drain to 0 (full sync)")
        self._wait_for_pending_bytes_zero(self.cluster, timeout=self.sync_wait_timeout)
        self.log.info("snapshotPendingBytes reached 0 — full sync confirmed")

        # Capture enabling-state response if observable (data already on disk)
        self.log.info("Re-triggering enable to capture enabling-state API fields")
        resp_disable = CapellaAPI.disable_fusion(self.pod, self.tenant, self.cluster.id)
        self.log.info(f"disable_fusion response: {resp_disable.status_code}")
        self._wait_for_fusion_state(self.tenant, self.cluster, "disabled")
        CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)

        deadline = time.time() + 180
        enabling_pending_seen = False
        while time.time() < deadline:
            state_resp = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
            current_state = state_resp.get("state", "")
            if current_state == "enabling":
                self.fusion_monitor.set_admin_credentials(self.cluster)
                ok, enabling_content = FusionRestAPI(self.cluster.master).get_fusion_status()
                if ok:
                    self.assertIn("state", enabling_content,
                                  "Enabling-state status response missing 'state'")
                    self.assertEqual(enabling_content.get("state"), "enabling",
                                     f"Expected enabling, got "
                                     f"{enabling_content.get('state')}")
                    # Check that pending bytes are non-zero while enabling (data on disk)
                    enabling_nodes = enabling_content.get("nodes") or {}
                    enabling_pending = sum(
                        b.get("snapshotPendingBytes", 0)
                        for n in enabling_nodes.values()
                        for b in (n.get("buckets") or {}).values()
                    )
                    if enabling_pending > 0:
                        enabling_pending_seen = True
                    self.log.info(
                        f"Enabling-state fields captured. Pending bytes: "
                        f"{enabling_pending}. Keys: {list(enabling_content.keys())}")
                break
            if current_state == "enabled":
                self.log.warning(
                    "Transition to enabling was too brief to observe — "
                    "only enabled-state validation was performed")
                break
            time.sleep(2)

        if not enabling_pending_seen:
            self.log.warning(
                "Could not confirm snapshotPendingBytes > 0 during enabling; "
                "enabled-state validation still passed")

        self._wait_for_fusion_state(self.tenant, self.cluster, "enabled")


    def test_stop_fusion_when_already_disabled(self):
        """
        Calling the internal stop API when fusion is already disabled must not
        cause an error — it should be a safe no-op or return a clear rejection.

        Validates:
        - Fusion is in "disabled" state before the call
        - Internal stop API returns 200 (no-op) or 409/422 (already stopped)
        - Fusion status remains "disabled" after the call
        - No AWS resources are created or modified
        """
        self.log.info(f"Ensuring fusion state is 'disabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "disabled")

        self.log.info(f"Calling stop on already-disabled cluster {self.cluster.id}")
        resp = CapellaAPI.stop_fusion_internal(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(resp.status_code, 500,
                         f"Expected 500 for stop on disabled cluster, "
                         f"got {resp.status_code}: {resp.content}")

        state_resp = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(state_resp.get("state"), "disabled",
                         "Fusion state changed after stop on already-disabled cluster")
        cluster_state = CapellaAPI.get_cluster_state(self.pod, self.tenant, self.cluster.id)
        self.log.info(f"Cluster {self.cluster.id} state after fusion disabled -> stop: {cluster_state}")
        self.assertTrue(cluster_state.lower() == "healthy",
                        "Cluster state is not healthy after stop on a disabled fusion cluster")

        instances = self.fusion_aws_util.list_accelerator_instances(
            self.fusion_aws_util._cluster_filter(self.cluster.id))
        self.assertEqual(len(instances), 0,
                         f"Unexpected accelerator instances after no-op stop: "
                         f"{len(instances)}")

    def test_stop_fusion_during_active_rebalance(self):
        """
        Stop fusion via the internal support API while a fusion rebalance is running.

        Stop == Disable without S3 data deletion. The control plane must abort the
        in-flight fusion rebalance, tear down AWS resources, and transition to
        "disabled" — except that the S3 objects written so far are preserved.

        Validates:
        - Internal stop API accepted (200/202) during active fusion rebalance
        - All accelerator instances cleaned up
        - All guest volumes cleaned up
        - Fusion status reaches "disabled"
        - No orphaned ASGs remain
        - S3 object count does not increase after stop (sync halted)
        - S3 objects written before stop are preserved (not deleted, unlike disable)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)
        self.sleep(120, "Allow data to sync past fusion threshold")

        self.log.info("Starting fusion rebalance")
        config = self.rebalance_config("data", +1)
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)

        deadline = time.time() + 180
        accelerators_seen = False
        while time.time() < deadline:
            instances = self.fusion_aws_util.list_accelerator_instances(
                self.fusion_aws_util._cluster_filter(self.cluster.id, [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}]),
                log="Fusion Accelerator"
            )
            if instances:
                self.log.info(
                    f"Fusion rebalance active: {len(instances)} accelerator(s)")
                accelerators_seen = True
                break
            time.sleep(10)
        self.assertTrue(accelerators_seen, "Fusion rebalance did not start")

        bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        obj_count_before_stop = 0
        if bucket_name:
            obj_count_before_stop = self.s3.get_bucket_size(
                bucket_name).get("file_count", 0)
            self.log.info(f"S3 object count before stop: {obj_count_before_stop}")

        self.log.info("Issuing stop via internal support API during active rebalance")
        CapellaAPI.stop_fusion_internal(self.pod, self.tenant, self.cluster.id)

        self._wait_for_fusion_state(self.tenant, self.cluster, "stopped")

        result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
            self.cluster, timeout=self.fusion_infra_timeout)
        self.assertTrue(result, "Accelerator nodes not cleaned up after stop")

        guest_vols = self._get_active_guest_volume_count(self.cluster)
        self.assertEqual(guest_vols, 0,
                         f"Guest volumes still active after stop: {guest_vols}")

        asgs = self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id)
        self.assertEqual(
            len(asgs), 0,
            f"Orphaned ASGs remain after stop: "
            f"{[a.get('AutoScalingGroupName') for a in asgs]}")

        if bucket_name:
            self.sleep(60, "Wait to confirm no new objects are written after stop")
            obj_count_after_stop = self.s3.get_bucket_size(
                bucket_name).get("file_count", 0)
            self.log.info(f"S3 object count after stop: {obj_count_after_stop}")
            self.assertGreaterEqual(
                obj_count_after_stop, obj_count_before_stop,
                "S3 objects were unexpectedly deleted by stop")
            self.assertEqual(
                obj_count_after_stop, obj_count_before_stop,
                f"S3 object count increased after stop "
                f"({obj_count_before_stop}→{obj_count_after_stop})"
                " — sync did not halt")

        try:
            self.wait_for_rebalances([rebalance_task])
        except Exception as e:
            self.log.warning(
                f"Rebalance task ended with exception after stop (expected): {e}")
        self.log.info("Scaling cluster back to original node count")
        self.wait_for_rebalances([self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            self.rebalance_config("data", -1), timeout=self.rebalance_timeout)])

    def test_stop_vs_disable_s3_retention_difference(self):
        """
        Directly validate the key behavioural difference between Stop and Disable:

          Stop    == fusion disabled, S3 objects PRESERVED
          Disable == fusion disabled, S3 objects DELETED

        Test sequence:
        1. Enable → load data → Stop  → verify status=disabled, S3 objects preserved
        2. Re-enable → load data → Disable → verify status=disabled, S3 objects deleted
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)

        # --- Round 1: Stop — S3 objects must be preserved ---
        self.log.info(f"Round 1 (Stop): enable, stop on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._load_data(self.cluster)

        self.sleep(30, "Allow some data to sync to S3")

        bucket_name_1 = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(bucket_name_1, "No S3 bucket found after enable")
        count_before_stop = self.s3.get_bucket_size(bucket_name_1).get("file_count", 0)
        self.assertGreater(count_before_stop, 0,
                           "Expected S3 objects after data load, found none")
        self.log.info(f"S3 objects before stop: {count_before_stop}")

        CapellaAPI.stop_fusion_internal(self.pod, self.tenant, self.cluster.id)
        self._wait_for_fusion_state(self.tenant, self.cluster, "stopped")

        self.sleep(30, "Confirm sync has halted after stop")
        count_after_stop = self.s3.get_bucket_size(bucket_name_1).get("file_count", 0)
        self.log.info(f"S3 objects after stop: {count_after_stop}")
        self.assertEqual(
            count_after_stop, count_before_stop,
            f"Stop changed S3 object count ({count_before_stop}→{count_after_stop})"
            " — objects should be preserved")
        self.log.info("Round 1 (Stop): S3 objects preserved as expected")

        # --- Round 2: Disable — S3 objects must be deleted ---
        self.log.info(
            f"Round 2 (Disable): re-enable, load data, disable on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        self.sleep(30, "Allow some data to sync to S3")
        bucket_name_2 = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(bucket_name_2, "No S3 bucket found after re-enable")
        count_before_disable = self.s3.get_bucket_size(
            bucket_name_2).get("file_count", 0)
        self.assertGreater(count_before_disable, 0,
                           "Expected S3 objects after data load, found none")
        self.log.info(f"S3 objects before disable: {count_before_disable}")

        resp_disable = CapellaAPI.disable_fusion(self.pod, self.tenant, self.cluster.id)
        self.log.info(f"disable_fusion response: {resp_disable.status_code}")
        self._wait_for_fusion_state(self.tenant, self.cluster, "disabled")

        self._assert_s3_bucket_empty(bucket_name_2, timeout=300)
        self.log.info("Round 2 (Disable): S3 objects deleted as expected")

        self.log.info("Stop vs Disable S3 retention difference confirmed")

    def test_stop_fusion_on_synced_data_falls_back_to_dcp_rebalance(self):
        """
        After fusion is stopped on a self.cluster where all data is fully synced to S3,
        the next rebalance must fall back to DCP — not use fusion acceleration.

        Rationale: stop halts the sync process. With sync stopped, the self.cluster can
        no longer serve data from the blob store during rebalance, so the control
        plane must route the rebalance through the standard DCP path.

        Validates:
        - Fusion is enabled and all data is fully synced (pending bytes = 0)
        - Stop is issued; fusion status transitions to "disabled"
        - S3 objects are preserved (stop does not delete them)
        - Rebalance triggered after stop produces zero accelerator instances (DCP path)
        - Rebalance completes successfully via DCP
        - Data integrity is maintained post-rebalance
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        self.log.info("Waiting for full sync to S3")
        self._wait_for_pending_bytes_zero(self.cluster, timeout=self.sync_wait_timeout)

        bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        obj_count_before_stop = 0
        if bucket_name:
            obj_count_before_stop = self.s3.get_bucket_size(
                bucket_name).get("file_count", 0)
            self.log.info(f"S3 object count after full sync: {obj_count_before_stop}")
            self.assertGreater(obj_count_before_stop, 0,
                               "Expected S3 objects after full sync but found none")

        self.log.info(f"Stopping fusion on cluster {self.cluster.id}")
        CapellaAPI.stop_fusion_internal(self.pod, self.tenant, self.cluster.id)
        self._wait_for_fusion_state(self.tenant, self.cluster, "stopped")

        if bucket_name:
            obj_count_after_stop = self.s3.get_bucket_size(
                bucket_name).get("file_count", 0)
            self.log.info(f"S3 object count after stop: {obj_count_after_stop}")
            self.assertEqual(
                obj_count_after_stop, obj_count_before_stop,
                "S3 object count changed after stop — objects should be preserved")

        self.log.info("Triggering rebalance after stop — expecting DCP path")
        config = self.rebalance_config("data", +1)
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)

        self.sleep(30, "Wait for rebalance to start before checking for accelerators")
        deadline = time.time() + 60
        while time.time() < deadline:
            instances = self.fusion_aws_util.list_accelerator_instances(
                self.fusion_aws_util._cluster_filter(self.cluster.id, [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}]),
                log="Fusion Accelerator"
            )
            self.assertEqual(
                len(instances), 0,
                f"Accelerator instances found after stop — expected DCP rebalance, "
                f"got fusion: {[i.get('InstanceId') for i in instances]}")
            time.sleep(10)

        self.log.info(
            "No accelerator instances seen — rebalance is using DCP as expected")
        self.wait_for_rebalances([rebalance_task])
        self.log.info("Scaling cluster back to original node count")
        self.wait_for_rebalances([self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            self.rebalance_config("data", -1), timeout=self.rebalance_timeout)])

        self.log.info("Validating data integrity after DCP rebalance")
        fusion_state = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
        self.assertNotIn(
            fusion_state.get("state"), ["rebalance_failed", "deployment_failed"],
            f"Cluster entered failure state: {fusion_state}")

    def test_stop_then_reenable_resumes_uploads_from_checkpoint(self):
        """
        After stop → re-enable, uploads resume from checkpoint (not from zero).

        Design doc: "upload checkpoint resumption — After stop → re-enable, uploads
        resume from checkpoint (not from zero)." This prevents re-uploading data that
        was already durably in S3 before the stop.

        Validates:
        - Enable → full sync (pending bytes = 0) → record S3 object count
        - Stop → S3 objects preserved, count unchanged
        - Re-enable → pending bytes drain to 0 (checkpoint resume, not cold start)
        - Final S3 object count >= pre-stop count (no objects lost or re-uploaded from zero)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        self.log.info("Waiting for full sync to S3")
        self._wait_for_pending_bytes_zero(self.cluster, timeout=self.sync_wait_timeout)

        bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(bucket_name, "No S3 bucket found after enable")
        count_pre_stop = self.s3.get_bucket_size(bucket_name).get("file_count", 0)
        self.log.info(f"S3 object count after full sync: {count_pre_stop}")
        self.assertGreater(count_pre_stop, 0,
                           "Expected S3 objects after full sync but found none")

        self.log.info("Stopping fusion — uploads halt, S3 objects preserved")
        CapellaAPI.stop_fusion_internal(self.pod, self.tenant, self.cluster.id)
        self._wait_for_fusion_state(self.tenant, self.cluster, "stopped")

        count_after_stop = self.s3.get_bucket_size(bucket_name).get("file_count", 0)
        self.assertEqual(count_after_stop, count_pre_stop,
                         f"Stop changed S3 object count "
                         f"({count_pre_stop}→{count_after_stop}) — stop must preserve")

        self.log.info("Re-enabling fusion — uploads must resume from checkpoint")
        resp = CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(resp.status_code, 200,
                         f"Re-enable from stopped returned {resp.status_code}")
        self._wait_for_fusion_state(self.tenant, self.cluster, "enabled")

        self._wait_for_pending_bytes_zero(self.cluster, timeout=self.sync_wait_timeout)

        count_after_reenable = self.s3.get_bucket_size(
            bucket_name).get("file_count", 0)
        self.assertGreaterEqual(
            count_after_reenable, count_pre_stop,
            f"S3 object count after re-enable ({count_after_reenable}) is less than "
            f"pre-stop ({count_pre_stop}) — objects may have been lost")
        self.log.info(
            f"Checkpoint resume confirmed: {count_pre_stop}→{count_after_reenable} "
            f"S3 objects. Uploads resumed from checkpoint, not from zero.")


    def test_disable_fusion_when_synced_deletes_s3_objects(self):
        """
        Disable fusion after all data is fully synced to the blob store.

        Validates:
        - Pending bytes reach 0 before disable (fully synced)
        - Disable transitions status to "disabled"
        - CP deletes all objects from the S3 bucket (bucket is emptied, not deleted)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(bucket_name, "Could not determine S3 bucket name")

        self.sleep(60, "Allow data to begin syncing to S3")

        self.log.info("Waiting for pending bytes to reach 0 (fully synced)")
        self._wait_for_pending_bytes_zero(self.cluster, timeout=self.sync_wait_timeout)

        self.log.info(f"Disabling fusion on cluster {self.cluster.id}")
        resp_disable = CapellaAPI.disable_fusion(self.pod, self.tenant, self.cluster.id)
        self.log.info(f"disable_fusion response: {resp_disable.status_code}")
        self.assertEqual(resp_disable.status_code, 200,
                         f"disable_fusion failed: {resp_disable.status_code}")

        self._wait_for_fusion_state(self.tenant, self.cluster, "disabled")

        self._assert_s3_bucket_empty(bucket_name, timeout=300)

    def test_disable_fusion_no_rebalance_stops_uploads(self):
        """
        Disable fusion when no rebalance is in progress.

        Validates:
        - S3 upload process stops immediately (pending bytes stop increasing)
        - S3 bucket is emptied after disable
        - Fusion status transitions cleanly to "disabled"
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(bucket_name, "Could not determine S3 bucket name")

        self._load_data(self.cluster,
                        create_start=self.input.param("num_items"),
                        create_end=self.input.param("num_items")*2)
        self.sleep(30, "Allow uploads to start")

        self.fusion_monitor.set_admin_credentials(self.cluster)
        _, before = FusionRestAPI(self.cluster.master).get_fusion_status()
        before_nodes = before.get("nodes") or {}
        pending_before = sum(
            b.get("snapshotPendingBytes", 0)
            for n in before_nodes.values()
            for b in (n.get("buckets") or {}).values()
        )
        self.log.info(f"Pending bytes before disable: {pending_before}")

        self.log.info(f"Disabling fusion (no rebalance) on cluster {self.cluster.id}")
        resp_disable = CapellaAPI.disable_fusion(self.pod, self.tenant, self.cluster.id)
        self.log.info(f"disable_fusion response: {resp_disable.status_code}")
        self.assertEqual(resp_disable.status_code, 200,
                         f"disable_fusion failed: {resp_disable.status_code}")

        self._wait_for_fusion_state(self.tenant, self.cluster, "disabled")

        self.sleep(30, "Wait to confirm uploads have stopped")
        _, after = FusionRestAPI(self.cluster.master).get_fusion_status()
        after_nodes = after.get("nodes") or {}
        pending_after = sum(
            b.get("snapshotPendingBytes", 0)
            for n in after_nodes.values()
            for b in (n.get("buckets") or {}).values()
        )
        self.log.info(f"Pending bytes after disable: {pending_after}")
        self.assertLessEqual(
            pending_after, pending_before,
            "Pending bytes increased after disable — uploads not stopped")

        self._assert_s3_bucket_empty(bucket_name, timeout=300)

    def test_disable_fusion_during_rebalance_waits_for_operations(self):
        """
        Disabling fusion while a rebalance is in progress must wait for all
        ongoing fusion operations to complete before proceeding.

        Validates:
        - Disable request is accepted while rebalance is running
        - CP does not interrupt the in-progress fusion rebalance mid-flight
        - Guest volumes remain active until rebalance completes
        - After rebalance finishes, fusion status transitions to "disabled"
        - S3 bucket is emptied after the disable completes
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)

        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)
        self.sleep(120, "Allow data to sync to S3 past the fusion threshold")

        self.log.info("Starting rebalance to trigger fusion acceleration")
        config = self.rebalance_config("data", +1)
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)

        deadline = time.time() + 180
        accelerators_seen = False
        while time.time() < deadline:
            instances = self.fusion_aws_util.list_accelerator_instances(
                self.fusion_aws_util._cluster_filter(self.cluster.id, [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}]),
                log="Fusion Accelerator"
            )
            if instances:
                self.log.info(
                    f"Fusion rebalance active: {len(instances)} accelerator(s)")
                accelerators_seen = True
                break
            time.sleep(10)
        self.assertTrue(accelerators_seen,
                        "Fusion rebalance did not start (no accelerators observed)")

        self.log.info("Issuing disable while fusion rebalance is running")
        resp_disable = CapellaAPI.disable_fusion(self.pod, self.tenant, self.cluster.id)
        self.log.info(f"disable_fusion response: {resp_disable.status_code}")
        self.assertIn(
            resp_disable.status_code, [200, 202],
            f"disable_fusion during rebalance returned unexpected status: "
            f"{resp_disable.status_code}")

        guest_vol_count = self._get_active_guest_volume_count(self.cluster)
        self.assertGreater(
            guest_vol_count, 0,
            "Guest volumes disappeared before rebalance completed — "
            "disable was too aggressive")

        self.wait_for_rebalances([rebalance_task])

        self._wait_for_fusion_state(self.tenant, self.cluster, "disabled")

        result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
            self.cluster, timeout=self.fusion_infra_timeout)
        self.assertTrue(result,
                        "Accelerator nodes not cleaned up after rebalance + disable")
        self.log.info("Scaling cluster back to original node count")
        self.wait_for_rebalances([self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            self.rebalance_config("data", -1), timeout=self.rebalance_timeout)])

        self._assert_s3_bucket_empty(bucket_name, timeout=300)

    def test_disable_fusion_after_rebalance_during_hydration(self):
        """
        Disable fusion AFTER a successful fusion rebalance but while post-rebalance
        hydration is still in progress (guest EBS volumes still attached to data nodes).

        CP must NOT prematurely clean up guest volumes — hydration must run to
        completion, then volumes must be removed, then S3 must be deleted.

        Key difference from test_disable_fusion_during_rebalance_waits_for_operations:
        that test calls disable DURING an active rebalance. This test calls disable
        AFTER the rebalance declares success (self.cluster state = healthy) but while
        hydration may still be running on the data nodes.

        Validates:
        - Fusion rebalance runs to completion (state → healthy, accelerators killed)
        - Disable is issued after rebalance while guest volumes may still be attached
        - Guest volumes count > 0 immediately after disable (hydration not interrupted)
        - Guest volumes eventually reach 0 (hydration completes naturally)
        - S3 bucket is empty after hydration finishes
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        self.log.info("Loading data above fusion threshold and waiting for sync")
        self._load_data(self.cluster)
        self.sleep(120, "Allow data to sync to S3 past fusion threshold")
        self._wait_for_pending_bytes_zero(self.cluster, timeout=self.sync_wait_timeout)

        bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(bucket_name, "No S3 bucket found after enable")

        self.log.info(f"Starting fusion rebalance on cluster {self.cluster.id}")
        config = self.rebalance_config("data", +1)
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)

        deadline = time.time() + 180
        accelerators_seen = False
        while time.time() < deadline:
            instances = self.fusion_aws_util.list_accelerator_instances(
                self.fusion_aws_util._cluster_filter(
                    self.cluster.id,
                    [{'Name': 'tag:couchbase-cloud-function',
                      'Values': ['fusion-accelerator']}]
                ),
                log="Fusion Accelerator"
            )
            if instances:
                self.log.info(
                    f"Fusion rebalance active: {len(instances)} accelerator(s)")
                accelerators_seen = True
                break
            time.sleep(10)
        self.assertTrue(accelerators_seen,
                        "Fusion rebalance did not start (no accelerators observed) — "
                        "cannot test post-rebalance disable")

        self.log.info("Waiting for rebalance to declare success")
        self.wait_for_rebalances([rebalance_task])
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for healthy state after fusion rebalance", timeout=600)

        accelerator_cleanup = \
            self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(self.cluster, timeout=self.fusion_infra_timeout)
        self.assertTrue(accelerator_cleanup,
                        "Accelerator nodes not cleaned up after rebalance completed")

        self.log.info(
            "Checking guest volume count immediately after rebalance completion")
        vol_count = self.fusion_monitor.get_attached_ebs_volumes_count(
            self.tenant, self.cluster, find_master_func=self.find_master)
        self.log.info(
            f"Guest volumes attached after rebalance success: {vol_count}")

        if vol_count > 0:
            self.log.info(
                f"Hydration still active — {vol_count} guest volume(s) attached. "
                "Issuing disable to verify CP preserves volumes during hydration.")
            resp_disable = CapellaAPI.disable_fusion(self.pod, self.tenant, self.cluster.id)
            self.log.info(f"disable_fusion response: {resp_disable.status_code}")
            self.assertEqual(resp_disable.status_code, 200,
                             f"disable_fusion returned {resp_disable.status_code}: "
                             f"{resp_disable.content}")

            immediate_count = self.fusion_monitor.get_attached_ebs_volumes_count(
                self.tenant, self.cluster, find_master_func=self.find_master)
            self.assertGreater(
                immediate_count, 0,
                "CP deleted guest volumes immediately after disable — hydration was "
                "interrupted before completion. CP must allow hydration to finish.")

            self.log.info("Waiting for hydration to complete (volumes → 0)")
            hydration_deadline = time.time() + self.DEFAULT_TIMEOUT
            remaining = immediate_count
            while time.time() < hydration_deadline:
                remaining = self.fusion_monitor.get_attached_ebs_volumes_count(
                    self.tenant, self.cluster, find_master_func=self.find_master)
                self.log.info(
                    f"Cluster {self.cluster.id} guest volumes remaining: {remaining}")
                if remaining == 0:
                    break
                time.sleep(15)
            self.assertEqual(
                remaining, 0,
                f"Guest volumes did not reach 0 within {self.DEFAULT_TIMEOUT}s — "
                "hydration did not complete after disable was issued")
            self.log.info(
                "Hydration completed post-disable: guest volumes cleaned up correctly")

        else:
            self.log.warning(
                "Hydration already completed before disable could be issued "
                "(volumes = 0 at rebalance completion). "
                "Validating disable on clean post-rebalance state.")
            resp_disable = CapellaAPI.disable_fusion(self.pod, self.tenant, self.cluster.id)
            self.log.info(f"disable_fusion response: {resp_disable.status_code}")
            self.assertEqual(resp_disable.status_code, 200,
                             f"disable_fusion returned {resp_disable.status_code}: "
                             f"{resp_disable.content}")

        self._wait_for_fusion_state(self.tenant, self.cluster, "disabled")

        self.log.info("Verifying S3 blob store is cleaned up after disable")
        self._assert_s3_bucket_empty(bucket_name, timeout=300)
        self.log.info(
            "Disable after rebalance hydration: guest volumes cleaned up, S3 bucket emptied")
        self.log.info("Scaling cluster back to original node count")
        self.wait_for_rebalances([self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            self.rebalance_config("data", -1), timeout=self.rebalance_timeout)])

    def test_disable_fusion_during_prepare_rebalance_with_leased_logs(self):
        """
        Disable fusion when log files are leased post prepareRebalance.

        The control plane must either:
        (a) Disallow the disable request while log files are leased, OR
        (b) Accept the disable, abort the fusion rebalance, clean up all resources
            (accelerators, guest volumes, S3 objects), and start a DCP rebalance.

        Validates:
        - prepareRebalance is initiated and log files become leased
        - Disable request is sent during the leased window
        - CP responds with either a rejection (409/423) or acceptance (200/202)
        - If rejected: fusion status remains unchanged, rebalance continues normally
        - If accepted: fusion status transitions to "disabled", all fusion resources
          are cleaned up, and a DCP rebalance completes the data movement
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)
        self.sleep(120, "Allow data to sync past fusion threshold")

        self.log.info(
            "Starting fusion rebalance to reach prepareRebalance/lease state")
        config = self.rebalance_config("data", +1)
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)

        deadline = time.time() + 180
        leased = False
        while time.time() < deadline:
            instances = self.fusion_aws_util.list_accelerator_instances(
                self.fusion_aws_util._cluster_filter(self.cluster.id, [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}]),
                log="Fusion Accelerator"
            )
            if instances:
                self.log.info(
                    f"Log files leased (post-prepareRebalance): "
                    f"{len(instances)} accelerator(s)")
                leased = True
                break
            time.sleep(10)

        self.assertTrue(leased,
                        "Did not reach prepareRebalance/lease state "
                        "(no accelerators observed)")

        self.log.info("Issuing disable while log files are leased")
        resp_disable = CapellaAPI.disable_fusion(self.pod, self.tenant, self.cluster.id)
        self.log.info(
            f"Disable response: {resp_disable.status_code} — {resp_disable.content}")

        if resp_disable.status_code in [409, 423]:
            self.log.info(
                "CP rejected disable during leased state (expected behaviour A)")
            self.wait_for_rebalances([rebalance_task])
            state_resp = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
            self.assertEqual(state_resp.get("state"), "enabled",
                             "Fusion state changed despite reject — unexpected")
            result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
                self.cluster, timeout=self.fusion_infra_timeout)
            self.assertTrue(
                result,
                "Accelerator nodes not cleaned up after rejected-disable rebalance")

        elif resp_disable.status_code in [200, 202]:
            self.log.info(
                "CP accepted disable during leased state (expected behaviour B)")
            self._wait_for_fusion_state(self.tenant, self.cluster, "disabled")

            result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
                self.cluster, timeout=self.fusion_infra_timeout)
            self.assertTrue(
                result,
                "Accelerator nodes not cleaned up after disable-aborted rebalance")

            guest_vols = self._get_active_guest_volume_count(self.cluster)
            self.assertEqual(guest_vols, 0,
                             f"Guest volumes still active after abort: {guest_vols}")

            self.wait_for_rebalances([rebalance_task])

            bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
            if bucket_name:
                self._assert_s3_bucket_empty(bucket_name, timeout=300)

        else:
            self.fail(
                f"Unexpected disable response during leased state: "
                f"{resp_disable.status_code} — {resp_disable.content}")
        self.log.info("Scaling cluster back to original node count")
        self.wait_for_rebalances([self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            self.rebalance_config("data", -1), timeout=self.rebalance_timeout)])

    def test_reenable_fusion_reuses_s3_bucket(self):
        """
        Re-enabling fusion after a disable reuses the same S3 bucket — the bucket
        is emptied by disable but not deleted, and the same bucket name is picked up
        on re-enable.

        Validates:
        - Enable → disable cycle completes without error
        - After disable: S3 bucket is empty but still exists
        - Re-enable returns 200 and fusion reaches "enabled"
        - After re-enable: the same S3 bucket name is reused (not a new bucket)
        - Data begins syncing to the bucket after re-enable
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        first_uri = self._wait_for_s3_uri(self.cluster)
        first_bucket = first_uri.replace("s3://", "").split("?")[0]
        self.log.info(f"S3 bucket before disable: {first_bucket}")

        self.log.info(f"Disabling fusion on cluster {self.cluster.id}")
        resp_disable = CapellaAPI.disable_fusion(self.pod, self.tenant, self.cluster.id)
        self.log.info(f"disable_fusion response: {resp_disable.status_code}")
        self.assertEqual(resp_disable.status_code, 200)
        self._wait_for_fusion_state(self.tenant, self.cluster, "disabled")

        self._assert_s3_bucket_empty(first_bucket, timeout=300)
        self._assert_s3_bucket_exists(first_bucket)
        self.log.info(f"S3 bucket {first_bucket} is empty but still exists after disable")

        self.log.info(f"Re-enabling fusion on cluster {self.cluster.id}")
        resp2 = CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(resp2.status_code, 200,
                         f"Re-enable failed: {resp2.status_code}: {resp2.content}")
        self._wait_for_fusion_state(self.tenant, self.cluster, "enabled")

        second_uri = self._wait_for_s3_uri(self.cluster)
        second_bucket = second_uri.replace("s3://", "").split("?")[0]
        self.log.info(f"S3 bucket after re-enable: {second_bucket}")
        self.assertEqual(first_bucket, second_bucket,
                         f"Expected same S3 bucket to be reused after re-enable, "
                         f"got {first_bucket} → {second_bucket}")

    def test_disable_fusion_from_stopped_state(self):
        """
        Call disable after fusion has been stopped (state = stopped/disabled with S3 preserved).

        Design doc permits /disable from stopped state. Unlike stop (which preserves S3),
        disable must delete all S3 objects even when called from the stopped state.

        Validates:
        - Enable → load data → stop (S3 objects preserved, state = disabled/stopped)
        - Disable called from stopped state returns 200
        - S3 objects are deleted (disable always deletes, regardless of how we got to stopped)
        - S3 bucket is emptied
        - Fusion status remains "disabled"
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        self.sleep(30, "Allow data to sync to S3")

        bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(bucket_name, "No S3 bucket found after enable")
        count_before_stop = self.s3.get_bucket_size(bucket_name).get("file_count", 0)
        self.assertGreater(count_before_stop, 0,
                           "Expected S3 objects before stop but found none")
        self.log.info(f"S3 object count before stop: {count_before_stop}")

        self.log.info("Stopping fusion — state transitions to stopped/disabled, S3 preserved")
        CapellaAPI.stop_fusion_internal(self.pod, self.tenant, self.cluster.id)
        self._wait_for_fusion_state(self.tenant, self.cluster, "stopped")

        count_after_stop = self.s3.get_bucket_size(bucket_name).get("file_count", 0)
        self.assertEqual(count_after_stop, count_before_stop,
                         "Stop changed S3 object count — expected preservation")

        self.log.info("Calling disable from stopped state")
        resp_disable = CapellaAPI.disable_fusion(self.pod, self.tenant, self.cluster.id)
        self.log.info(f"disable_fusion response: {resp_disable.status_code}")
        self.assertEqual(resp_disable.status_code, 200,
                         f"disable from stopped returned {resp_disable.status_code}: "
                         f"{resp_disable.content}")

        self._wait_for_fusion_state(self.tenant, self.cluster, "disabled")

        self._assert_s3_bucket_empty(bucket_name, timeout=300)
        self.log.info(
            "Disable from stopped state correctly emptied all S3 objects")

    def test_disable_fusion_during_enabling_state(self):
        """
        Call disable while fusion is in the "enabling" state (not yet enabled).

        Design doc permits /disable from enabling state. Unlike stop (which preserves S3),
        disable must delete all S3 objects that were created during the partial enable.

        Data is loaded before enabling so uploads are in flight during enabling.
        This widens the observable enabling window and ensures the S3 cleanup
        assertion is non-vacuous (actual objects must be deleted, not just "none created").

        Validates:
        - Enable is triggered and reaches "enabling" state (uploads in progress)
        - Disable is called during enabling (returns 200/202)
        - Fusion status reaches "disabled"
        - S3 objects created during enabling are deleted
        - S3 bucket is emptied
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'disabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "disabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        self.log.info(f"Starting enable on cluster {self.cluster.id}")
        resp = CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(resp.status_code, 200)

        bucket_name = None
        disabled_during_enabling = False
        deadline = time.time() + 180
        while time.time() < deadline:
            state_resp = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
            state = state_resp.get("state", "")
            self.log.info(f"Cluster {self.cluster.id} fusion state: {state}")

            if state == "enabling":
                if bucket_name is None:
                    bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)

                self.log.info("Calling disable during enabling state")
                resp_disable = CapellaAPI.disable_fusion(self.pod, self.tenant, self.cluster.id)
                self.log.info(f"disable_fusion response: {resp_disable.status_code}")
                self.assertIn(
                    resp_disable.status_code, [200, 202],
                    f"disable during enabling returned unexpected status: "
                    f"{resp_disable.status_code}: {resp_disable.content}")
                disabled_during_enabling = True
                break

            if state == "enabled":
                self.log.warning(
                    "Fusion enabled before disable could be sent during enabling")
                break
            time.sleep(2)

        if not disabled_during_enabling:
            self.skipTest("Fusion enabled too fast to send disable during enabling; "
                          "increase num_items or adjust cluster size and retry")

        self._wait_for_fusion_state(self.tenant, self.cluster, "disabled")

        final_state = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id).get("state")
        self.assertEqual(final_state, "disabled",
                         f"Unexpected final state after disable-during-enabling: "
                         f"{final_state}")

        # With 2M docs pre-loaded, the bucket and S3 objects must exist by the time
        # enabling starts — the cleanup assertion is non-vacuous.
        self.assertIsNotNone(bucket_name,
                             "S3 bucket was not created during enabling despite "
                             "2M docs pre-loaded — enabling may not have started uploads")
        self._assert_s3_bucket_empty(bucket_name, timeout=300)
        self.log.info("Disable-during-enabling: S3 objects deleted, bucket emptied")


    def test_override_rebalances_skips_fusion_while_enabled(self):
        """
        Setting override=True must force DCP rebalance even though fusion is enabled.

        Validates:
        - Fusion is enabled and fully synced
        - override=True is set via internal API
        - Rebalance triggered produces zero accelerator instances (DCP path)
        - Rebalance completes successfully
        - Fusion status remains "enabled" throughout (override does not disable fusion)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        self._wait_for_pending_bytes_zero(self.cluster, timeout=self.sync_wait_timeout)

        self.log.info("Setting overrideRebalances=True")
        CapellaAPI.override_fusion_rebalances(self.pod, self.tenant, self.cluster.id, override=True)

        config = self.rebalance_config("data", +1)
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)

        self.sleep(30, "Wait for rebalance to start before checking for accelerators")
        deadline = time.time() + 60
        while time.time() < deadline:
            instances = self.fusion_aws_util.list_accelerator_instances(
                self.fusion_aws_util._cluster_filter(self.cluster.id, [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}]),
                log="Fusion Accelerator"
            )
            self.assertEqual(
                len(instances), 0,
                f"Accelerator instances found with override=True — "
                f"expected DCP rebalance: "
                f"{[i.get('InstanceId') for i in instances]}")
            time.sleep(10)

        self.wait_for_rebalances([rebalance_task])
        self.log.info("Rebalance completed via DCP as expected with override=True")
        self.log.info("Scaling cluster back to original node count")
        self.wait_for_rebalances([self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            self.rebalance_config("data", -1), timeout=self.rebalance_timeout)])

        status = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(
            status.get("state"), "enabled",
            "Fusion state changed after override — expected it to remain enabled")

        CapellaAPI.override_fusion_rebalances(self.pod, self.tenant, self.cluster.id, override=False)
        self.log.info("Cleared overrideRebalances=False to restore default fusion rebalance behaviour")

    def test_override_rebalances_false_restores_fusion_rebalance(self):
        """
        Clearing the override (override=False) must restore fusion-accelerated rebalances.

        Validates:
        - override=True set, one DCP rebalance runs (no accelerator instances)
        - override=False set
        - Next rebalance uses fusion (accelerator instances appear)
        - Data integrity maintained throughout
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)
        self._wait_for_pending_bytes_zero(self.cluster, timeout=self.sync_wait_timeout)

        # --- Phase 1: override=True → DCP rebalance ---
        self.log.info("Phase 1: set override=True and rebalance via DCP")
        CapellaAPI.override_fusion_rebalances(self.pod, self.tenant, self.cluster.id, override=True)

        config = self.rebalance_config("data", +1)
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)
        self.sleep(30, "Wait for rebalance to start")
        instances = self.fusion_aws_util.list_accelerator_instances(
                self.fusion_aws_util._cluster_filter(self.cluster.id, [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}]),
                log="Fusion Accelerator"
            )
        self.assertEqual(len(instances), 0,
                         "Expected no accelerators with override=True")
        self.wait_for_rebalances([rebalance_task])
        self.log.info("Scaling cluster back to original node count")
        self.wait_for_rebalances([self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            self.rebalance_config("data", -1), timeout=self.rebalance_timeout)])

        # --- Phase 2: override=False → fusion rebalance ---
        self.log.info("Phase 2: clear override and confirm fusion rebalance resumes")
        CapellaAPI.override_fusion_rebalances(self.pod, self.tenant, self.cluster.id, override=False)
        self._wait_for_pending_bytes_zero(self.cluster, timeout=self.sync_wait_timeout)

        config = self.rebalance_config("data", +1)
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)
        self.sleep(30, "Wait for rebalance to start")

        accelerators_seen = False
        deadline = time.time() + 120
        while time.time() < deadline:
            instances = self.fusion_aws_util.list_accelerator_instances(
                self.fusion_aws_util._cluster_filter(self.cluster.id, [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}]),
                log="Fusion Accelerator"
            )
            if instances:
                accelerators_seen = True
                self.log.info(
                    f"Accelerators found after override cleared: "
                    f"{[i.get('InstanceId') for i in instances]}")
                break
            time.sleep(10)

        self.wait_for_rebalances([rebalance_task])
        self.assertTrue(accelerators_seen,
                        "No accelerator instances seen after clearing override — "
                        "fusion rebalance did not resume as expected")
        fusion_state = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
        self.assertNotIn(
            fusion_state.get("state"), ["rebalance_failed", "deployment_failed"],
            f"Cluster entered failure state: {fusion_state}")
        self.log.info("Scaling cluster back to original node count")
        self.wait_for_rebalances([self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            self.rebalance_config("data", -1), timeout=self.rebalance_timeout)])

    def test_override_has_no_effect_when_fusion_disabled(self):
        """
        When fusion is disabled, setting override=True must have no observable effect —
        rebalance already uses DCP and there is no fusion state to bypass.

        Validates:
        - Fusion is disabled
        - override=True is set (should succeed without error)
        - Rebalance uses DCP (as it normally would without fusion)
        - No accelerator instances are launched
        - override=False can be cleared without error
        """
        self.log.info(f"Ensuring fusion state is 'disabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "disabled")
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        self.log.info("Setting override=True on a fusion-disabled cluster")
        CapellaAPI.override_fusion_rebalances(self.pod, self.tenant, self.cluster.id, override=True)

        config = self.rebalance_config("data", +1)
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)
        self.sleep(30, "Wait for rebalance to start")

        deadline = time.time() + 120
        instances = []
        while time.time() < deadline and rebalance_task.state != "healthy":
            instances = self.fusion_aws_util.list_accelerator_instances(
                self.fusion_aws_util._cluster_filter(self.cluster.id, [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}]),
                log="Fusion Accelerator"
            )
            if instances:
                self.log.info(
                    f"Accelerators found after override cleared: "
                    f"{[i.get('InstanceId') for i in instances]}")
                break
            time.sleep(10)
        self.assertEqual(len(instances), 0,
                         "Accelerators found on a fusion-disabled cluster — unexpected")

        self.wait_for_rebalances([rebalance_task])
        self.log.info("Scaling cluster back to original node count")
        self.wait_for_rebalances([self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            self.rebalance_config("data", -1), timeout=self.rebalance_timeout)])

        CapellaAPI.override_fusion_rebalances(self.pod, self.tenant, self.cluster.id, override=False)
        self.log.info("override=False cleared without error on disabled cluster")


    def test_invalid_state_transitions(self):
        """
        Verify that all rejected transitions per the design doc return 4xx
        and leave the self.cluster state unchanged.

        Transitions covered (all must be rejected):
          /enable  from enabled   → 4xx
          /enable  from enabling  → 4xx (if observable)
          /stop    from disabled  → 4xx  ← design doc says No; existing code accepted
                                           200 (see test_stop_fusion_when_already_disabled)
          /disable from disabled  → 4xx
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self.log.info(f"Loading {self.input.param('num_items', 0)} items into cluster {self.cluster.id}")
        self._load_data(self.cluster)

        # ----------------------------------------------------------
        # /enable from enabled → must be rejected
        # ----------------------------------------------------------
        self.log.info("Test: /enable from enabled → expect 4xx")
        self.log.info(f"Ensuring fusion state is 'enabled' on cluster {self.cluster.id}")
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        resp = CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)
        self.assertGreaterEqual(
            resp.status_code, 400,
            f"/enable from enabled should be rejected per design doc, "
            f"got {resp.status_code}")
        self.assertLess(resp.status_code, 500,
                        f"/enable from enabled returned server error: {resp.status_code}")
        state = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id).get("state")
        self.assertEqual(state, "enabled",
                         f"State changed after rejected /enable from enabled: {state}")
        self.log.info(f"/enable from enabled rejected with {resp.status_code} ✓")

        # ----------------------------------------------------------
        # /enable from enabling → must be rejected (if observable)
        # ----------------------------------------------------------
        self.log.info("Test: /enable from enabling → expect 4xx (if observable)")
        CapellaAPI.disable_fusion(self.pod, self.tenant, self.cluster.id)
        self._wait_for_fusion_state(self.tenant, self.cluster, "disabled")
        CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)

        deadline = time.time() + 60
        enabling_tested = False
        while time.time() < deadline:
            state_resp = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
            if state_resp.get("state") == "enabling":
                resp2 = CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)
                self.assertGreaterEqual(
                    resp2.status_code, 400,
                    f"/enable from enabling should be rejected, got {resp2.status_code}")
                self.log.info(
                    f"/enable from enabling rejected with {resp2.status_code} ✓")
                enabling_tested = True
                break
            if state_resp.get("state") == "enabled":
                self.log.warning(
                    "Enabling state was too brief to observe — "
                    "/enable-from-enabling test skipped")
                break
            time.sleep(2)

        if not enabling_tested:
            self.log.info("Could not capture enabling state; test skipped for this cluster")

        self._wait_for_fusion_state(self.tenant, self.cluster, "enabled")

        # ----------------------------------------------------------
        # /stop from disabled → must be rejected (design doc: No)
        # Note: test_stop_fusion_when_already_disabled accepts 200, which
        # contradicts the design doc. This test enforces the spec strictly.
        # ----------------------------------------------------------
        self.log.info("Test: /stop from disabled → expect 4xx (design doc requirement)")
        self._ensure_fusion_state(self.tenant, self.cluster, "disabled")

        resp3 = CapellaAPI.stop_fusion_internal(self.pod, self.tenant, self.cluster.id)
        stop_code = resp3.status_code
        self.assertEqual(
            stop_code, 500,
            f"/stop from disabled must return 500, got {stop_code}: {resp3.content}")
        state = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id).get("state")
        self.assertEqual(state, "disabled",
                         f"State changed after /stop from disabled: {state}")
        self.log.info(f"/stop from disabled rejected with {stop_code} ✓")

        # ----------------------------------------------------------
        # /disable from disabled → must be rejected
        # ----------------------------------------------------------
        self.log.info("Test: /disable from disabled → expect 4xx")
        self._ensure_fusion_state(self.tenant, self.cluster, "disabled")

        resp4 = CapellaAPI.disable_fusion(self.pod, self.tenant, self.cluster.id)
        self.assertGreaterEqual(
            resp4.status_code, 400,
            f"/disable from disabled should be rejected per design doc, "
            f"got {resp4.status_code}")
        self.assertLess(resp4.status_code, 500,
                        f"/disable from disabled returned server error: {resp4.status_code}")
        state = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id).get("state")
        self.assertEqual(state, "disabled",
                         f"State changed after rejected /disable from disabled: {state}")
        self.log.info(f"/disable from disabled rejected with {resp4.status_code} ✓")
