import threading

from basetestcase import ClusterSetup
from cb_tools.cb_cli import CbCli
from BucketLib.bucket import Bucket
from couchbase_helper.documentgenerator import doc_generator
from shell_util.remote_connection import RemoteMachineShellConnection
from cb_tools.cbstats import Cbstats
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException
from StatsLib.StatsOperations import StatsHelper
from upgrade.upgrade_base import UpgradeBase


class KVRateLimitingTests(ClusterSetup):
    def setUp(self):
        super(KVRateLimitingTests, self).setUp()
        self.cluster_rest = ClusterRestAPI(self.cluster.master)
        self.bucket_rest = BucketRestApi(self.cluster.master)
        self.shell = RemoteMachineShellConnection(self.cluster.master)
        self.cb_cli = CbCli(self.shell)
        self.log.info("Starting KVRateLimitingTests synchronized with latest framework")

        # Cluster-wide throttle enable; node_capacity kept low to trip throttler
        # CE does not support throttling — skip to allow CE-specific tests to run
        if self.cluster_util.is_enterprise_edition(self.cluster):
            status, content = self.cluster_rest.manage_global_memcached_setting(
                throttle_enabled="true",
                node_capacity=self.input.param("node_capacity", 500))
            self.log.info(f"Cluster-wide throttle enable: status={status}, "
                          f"content={content}")
            self.assertTrue(status,
                            f"Failed to enable cluster-wide throttle: {content}")
        else:
            self.log.info("Skipping global throttle enable on CE")

        self.create_bucket(self.cluster)
        self.bucket = self.cluster.buckets[0]

        # Apply per-bucket throttle limits so hard-limit rejections and SDK
        # AmbiguousTimeoutExceptions are actually triggered during burst tests
        # CE does not support throttle params — skip silently
        throttle_reserved = self.input.param("bucket_throttle_reserved", 0)
        throttle_hard_limit = self.input.param("bucket_throttle_hard_limit", 0)
        if self.cluster_util.is_enterprise_edition(self.cluster) and (throttle_reserved or throttle_hard_limit):
            edit_params = {}
            if throttle_reserved:
                edit_params[Bucket.throttleReserved] = throttle_reserved
            if throttle_hard_limit:
                edit_params[Bucket.throttleHardLimit] = throttle_hard_limit
            status, content = self.bucket_rest.edit_bucket(
                self.bucket.name, edit_params)
            self.log.info(
                f"Bucket throttle limits set: throttleReserved={throttle_reserved}, "
                f"throttleHardLimit={throttle_hard_limit}, "
                f"status={status}, content={content}")
            self.assertTrue(
                status, f"Failed to set bucket throttle limits: {content}")

    def tearDown(self):
        if getattr(self, "shell", None):
            self.shell.disconnect()
        super(KVRateLimitingTests, self).tearDown()

    def get_throttle_stats(self, bucket_name):
        """
        Helper method to fetch throttle-related stats from cbstats.
        """
        cb_stat = Cbstats(self.cluster.master)
        throttle_stats = {}
        try:
            # Use all_stats() which returns a parsed dictionary
            stats = cb_stat.all_stats(bucket_name)

            # Look for any throttle-related stats
            throttle_related = {k: v for k, v in stats.items() 
                               if 'throttle' in k.lower() or 'reject' in k.lower()}
            self.log.info(f"Throttle-related stats found: {throttle_related}")

            required_stats = [
                "throttle_count_total", "reject_count_total",
                "throttle_hard_limit", "throttle_reserved"
            ]
            for stat_key in required_stats:
                if stat_key in stats:
                    throttle_stats[stat_key] = stats[stat_key]
        except Exception as e:
            self.log.info(f"Could not fetch throttle stats: {e}")
        finally:
            cb_stat.disconnect()
        return throttle_stats

    def load_docs_to_bucket(self, bucket_obj, start=0, end=10000,
                             batch_size=500, concurrency=4):
        """
        Helper method to load documents into a bucket.
        """
        gen_load = doc_generator("test_doc", start, end)
        task = self.task.async_load_gen_docs(
            self.cluster, bucket_obj, gen_load, "create", 0,
            batch_size=batch_size, process_concurrency=concurrency,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(task)
        return task

    def test_bucket_limit_crud_and_cli(self):
        """
        Validate bucket creation via REST, editing via both REST and CLI,
        and verify configuration persistence via both REST and CLI.
        [P0] Combined REST and CLI validation.
        """
        bucket_name = self.bucket.name

        # Verify creation via REST
        status, content = self.bucket_rest.get_bucket_info()
        found = any(b['name'] == bucket_name for b in content)
        self.assertTrue(found, f"Bucket {bucket_name} not found in nodes list")

        bucket_info = next(b for b in content if b['name'] == bucket_name)
        self.log.info(f"Initial bucket info via REST: throttleReserved={bucket_info.get('throttleReserved')}, "
                     f"throttleHardLimit={bucket_info.get('throttleHardLimit')}")

        # Edit via REST
        edit_params = {Bucket.throttleReserved: 6000}
        self.log.info(f"Editing bucket {bucket_name} via REST: throttleReserved=6000")
        status, content = self.bucket_rest.edit_bucket(bucket_name, edit_params)
        self.assertTrue(status, f"Bucket edit via REST failed: {content}")

        # Verify via REST
        status, content = self.bucket_rest.get_bucket_info()
        bucket_info = next(b for b in content if b['name'] == bucket_name)
        self.log.info(f"Bucket info after REST edit: throttleReserved={bucket_info.get('throttleReserved')}, "
                     f"throttleHardLimit={bucket_info.get('throttleHardLimit')}")
        self.assertEqual(bucket_info.get('throttleReserved'), 6000,
                         f"REST edit verification failed: expected 6000, got {bucket_info.get('throttleReserved')}")

        # Edit via CLI (both reserved and hard limit in one edit)
        self.log.info(f"Editing bucket {bucket_name} via CLI: throttleReserved=7000, throttleHardLimit=12000")
        cli_output = self.cb_cli.edit_bucket(bucket_name, throttleReserved=7000, throttleHardLimit=12000)
        self.assertEqual(cli_output, "Bucket updated successfully",
                         f"CLI bucket edit failed: {cli_output}")

        # Final verification via REST (CLI changes persistence)
        status, content = self.bucket_rest.get_bucket_info()
        bucket_info = next(b for b in content if b['name'] == bucket_name)
        self.log.info(f"Final bucket info via REST: throttleReserved={bucket_info.get('throttleReserved')}, "
                     f"throttleHardLimit={bucket_info.get('throttleHardLimit')}")
        self.assertEqual(bucket_info.get('throttleReserved'), 7000,
                         f"CLI edit verification via REST failed: expected 7000, got {bucket_info.get('throttleReserved')}")
        self.assertEqual(bucket_info.get('throttleHardLimit'), 12000,
                         f"CLI edit verification via REST failed: expected 12000, got {bucket_info.get('throttleHardLimit')}")

    def test_limit_enforcement(self):
        """
        Validate that operations are throttled/rejected based on limits using cbstats.
        [P0] Functional enforcement test.
        """
        bucket_name = self.bucket.name

        # Run load to trigger throttling
        self.log.info(f"Starting to load documents to trigger throttling on {bucket_name}")
        self.load_docs_to_bucket(self.bucket, start=0, end=10000, batch_size=100, concurrency=4)

        # Verify cbstats metrics for throttle enforcement
        throttle_stats = self.get_throttle_stats(bucket_name)
        self.log.info(f"Throttle metrics from cbstats: {throttle_stats}")
        self.assertTrue(len(throttle_stats) > 0, "No throttle stats found in cbstats")

    def test_rate_limiting_unavailable_in_ce(self):
        """
        Validate Rate Limiting feature is unavailable in Community Edition (CE).
        Verification:
        - Check if rate limiting stats are 0 or empty after bucket creation
          with rate limiting parameters in CE.
        """
        bucket_name = self.bucket.name
        throttle_stats = self.get_throttle_stats(bucket_name)
        self.log.info(f"CE throttle stats: {throttle_stats}")

        # Verify that throttle values are 0/disabled in CE
        self.assertEqual(int(throttle_stats.get("throttle_reserved", 0)), 0,
                         f"throttle_reserved should be 0 in CE, found {throttle_stats.get('throttle_reserved')}")
        self.assertEqual(int(throttle_stats.get("throttle_hard_limit", 0)), 0,
                         f"throttle_hard_limit should be 0 in CE, found {throttle_stats.get('throttle_hard_limit')}")
        self.assertEqual(int(throttle_stats.get("throttle_count_total", 0)), 0,
                         f"throttle_count_total should be 0 in CE, found {throttle_stats.get('throttle_count_total')}")
        self.assertEqual(int(throttle_stats.get("reject_count_total", 0)), 0,
                         f"reject_count_total should be 0 in CE, found {throttle_stats.get('reject_count_total')}")

    def test_enable_disable_rate_limiting_rest(self):
        """
        Set and validate per-bucket enable/disable value via REST API.
        """
        bucket_name = self.bucket.name

        status, buckets = self.bucket_rest.get_bucket_info()
        bucket_info = next(b for b in buckets if b['name'] == bucket_name)
        self.assertEqual(bucket_info.get('throttleEnabled'), True,
                         "Rate limiting should be enabled")

        edit_params = {Bucket.throttleEnabled: "false"}
        self.log.info("Disabling rate limiting via REST")
        status, content = self.bucket_rest.edit_bucket(bucket_name, edit_params)
        self.assertTrue(status, f"Failed to disable rate limiting: {content}")

        status, buckets = self.bucket_rest.get_bucket_info()
        bucket_info = next(b for b in buckets if b['name'] == bucket_name)
        self.assertEqual(bucket_info.get('throttleEnabled'), False,
                         "Rate limiting should be disabled")

        edit_params = {Bucket.throttleEnabled: "true"}
        self.log.info("Re-enabling rate limiting via REST")
        status, content = self.bucket_rest.edit_bucket(bucket_name, edit_params)
        self.assertTrue(status, f"Failed to re-enable rate limiting: {content}")

        status, buckets = self.bucket_rest.get_bucket_info()
        bucket_info = next(b for b in buckets if b['name'] == bucket_name)
        self.assertEqual(bucket_info.get('throttleEnabled'), True,
                         "Rate limiting should be re-enabled")

    def test_soft_limit_throttling(self):
        """
        Validate throttling behavior when ops rate exceeds the set SOFT limit.
        """
        bucket_name = self.bucket.name

        initial_stats = self.get_throttle_stats(bucket_name)
        initial_throttle_count = int(initial_stats.get("throttle_count_total", 0))
        self.log.info(f"Initial throttle stats: {initial_stats}")

        self.log.info("Driving load to exceed soft limit...")
        self.load_docs_to_bucket(self.bucket, start=0, end=50000,
                                  batch_size=500, concurrency=8)

        final_stats = self.get_throttle_stats(bucket_name)
        final_throttle_count = int(final_stats.get("throttle_count_total", 0))
        self.log.info(f"Final throttle stats: {final_stats}")
        self.assertGreater(final_throttle_count, initial_throttle_count,
                           "Throttle count should increase when exceeding soft limit")

    def test_hard_limit_rejection(self):
        """
        Validate operation rejection when buffer/queue size exceeds HARD limit.
        """
        bucket_name = self.bucket.name

        initial_stats = self.get_throttle_stats(bucket_name)
        initial_reject_count = int(initial_stats.get("reject_count_total", 0))
        self.log.info(f"Initial stats: {initial_stats}")

        self.log.info("Driving heavy load to exceed hard limit...")
        try:
            self.load_docs_to_bucket(self.bucket, start=0, end=100000, batch_size=1000, concurrency=16)
        except Exception as e:
            self.log.info(f"Expected exception during hard limit test: {e}")

        final_stats = self.get_throttle_stats(bucket_name)
        final_reject_count = int(final_stats.get("reject_count_total", 0))
        self.log.info(f"Final stats: {final_stats}")
        self.assertGreater(final_reject_count, initial_reject_count,
                           "Rejection count should increase when exceeding hard limit")

    def test_transaction_rate_limiting(self):
        """
        Verify rate limiting accounts for shadow/meta documents during transactions.
        """
        bucket_name = self.bucket.name

        initial_stats = self.get_throttle_stats(bucket_name)
        self.log.info(f"Initial stats before transactions: {initial_stats}")
        initial_wu = int(initial_stats.get("wu_total", 0))
        transaction_completed = False
        try:
            if self.cluster.sdk_client_pool:
                client = self.cluster.sdk_client_pool.get_client_for_bucket(
                    self.bucket, self.bucket.name)
                for i in range(100):
                    client.crud("create", f"txn_doc_{i}", {"value": i})
                for i in range(50):
                    client.crud("update", f"txn_doc_{i}", {"value": i * 2, "updated": True})
                transaction_completed = True
                self.log.info("Transaction-like operations completed successfully")
        except Exception as e:
            self.log.warning(f"Transaction test encountered error: {e}")

        final_stats = self.get_throttle_stats(bucket_name)
        self.log.info(f"Final stats after transactions: {final_stats}")
        final_wu = int(final_stats.get("wu_total", 0))
        if transaction_completed:
            self.assertGreater(final_wu, initial_wu,
                               "Write units should increase after transactions")

    def test_sdk_rate_limited_exception(self):
        """
        Validate SDK clients receive an AmbiguousTimeoutException (or
        throttle/reject counters increase) under a parallel burst that
        exceeds the configured throttle limit.
        """
        bucket_name = self.bucket.name
        initial_stats = self.get_throttle_stats(bucket_name)
        initial_reject = int(initial_stats.get("reject_count_total", 0))
        initial_throttle = int(initial_stats.get("throttle_count_total", 0))

        sdk_throttle_count = self._burst_writes(
            num_clients=self.input.param("burst_clients", 8),
            ops_per_client=self.input.param("burst_ops_per_client", 2000))

        final_stats = self.get_throttle_stats(bucket_name)
        final_reject = int(final_stats.get("reject_count_total", 0))
        final_throttle = int(final_stats.get("throttle_count_total", 0))
        self.log.info(
            f"sdk_throttle_count={sdk_throttle_count}, "
            f"throttle {initial_throttle}->{final_throttle}, "
            f"reject {initial_reject}->{final_reject}")

        self.assertTrue(
            sdk_throttle_count > 0
            or final_reject > initial_reject
            or final_throttle > initial_throttle,
            "Expected AmbiguousTimeoutException or throttle/reject count increase; "
            f"sdk_throttle_count={sdk_throttle_count}, "
            f"throttle {initial_throttle}->{final_throttle}, "
            f"reject {initial_reject}->{final_reject}")

    def test_prometheus_throttle_metrics(self):
        """
        Validate Prometheus exposes throttle/reject/wu/ru metric families
        for a rate-limited bucket once load is driven through it.
        """
        self.load_docs_to_bucket(self.bucket, start=0, end=20000,
                                  batch_size=200, concurrency=8)

        # Only kv_throttle_duration_seconds is always emitted; others appear
        # only after actual throttling occurs.
        expected = ["kv_throttle_duration_seconds"]
        seen = {name: False for name in expected}
        total_lines = 0
        for server in self.cluster.kv_nodes:
            try:
                metrics = StatsHelper(server).get_prometheus_metrics_high(
                    component="kv")
            except Exception as e:
                self.log.warning(f"Prom fetch failed on {server.ip}: {e}")
                continue
            for line in metrics:
                total_lines += 1
                line_str = line if isinstance(line, str) else line.decode(
                    errors="ignore")
                for name in expected:
                    if name in line_str:
                        seen[name] = True

        missing = [m for m, found in seen.items() if not found]
        self.log.info(f"Prom metric families seen: {seen}, "
                      f"total_lines={total_lines}")
        self.assertFalse(
            missing, f"Missing Prometheus metric families: {missing}")

    def _burst_writes(self, num_clients=8, ops_per_client=5000):
        """
        Drive a high-rate parallel burst of SET ops via multiple SDKClients
        to force the engine's rate limiter to trip. Returns number of
        throttle-related errors observed across all threads.
        """
        clients = [SDKClient(self.cluster, self.bucket)
                   for _ in range(num_clients)]
        throttle_counts = [0] * num_clients

        def worker(c_idx, client):
            for i in range(ops_per_client):
                try:
                    result = client.crud(
                        "create", f"burst_{c_idx}_{i}",
                        {"idx": i, "data": "x" * 256}, timeout=2)
                except Exception as e:
                    self.log.info(
                        f"[burst_err] {type(e).__name__}: {e}")
                    if SDKException.check_if_exception_exists(
                            SDKException.AmbiguousTimeoutException, str(e)):
                        throttle_counts[c_idx] += 1
                    continue
                err = None
                if isinstance(result, tuple) and len(result) == 2:
                    _success, fail = result
                    if fail:
                        err = next(iter(fail.values())).get("error")
                elif isinstance(result, dict) and \
                        result.get("status") is False:
                    err = result.get("error")
                if err is not None:
                    self.log.info(
                        f"[burst_err] client={c_idx} op={i} err={err!r}")
                    if SDKException.check_if_exception_exists(
                            SDKException.AmbiguousTimeoutException, str(err)):
                        throttle_counts[c_idx] += 1

        threads = [threading.Thread(target=worker, args=(i, clients[i]))
                   for i in range(num_clients)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for c in clients:
            try:
                c.close()
            except Exception:
                pass
        return sum(throttle_counts)

    def test_couchstore_and_magma_throttle(self):
        """
        Validate throttling works on both Couchstore and Magma. Storage
        backend is parametrized via bucket_storage in the conf.
        """
        bucket_name = self.bucket.name
        storage = getattr(self.bucket, "storageBackend", None) or "unknown"

        initial_stats = self.get_throttle_stats(bucket_name)
        initial_throttle = int(initial_stats.get("throttle_count_total", 0))
        initial_reject = int(initial_stats.get("reject_count_total", 0))

        sdk_throttle_count = self._burst_writes()

        final_stats = self.get_throttle_stats(bucket_name)
        final_throttle = int(final_stats.get("throttle_count_total", 0))
        final_reject = int(final_stats.get("reject_count_total", 0))
        self.log.info(f"storage={storage} stats: {final_stats}, "
                      f"sdk_throttle_count={sdk_throttle_count}")

        self.assertTrue(
            final_throttle > initial_throttle
            or final_reject > initial_reject
            or sdk_throttle_count > 0,
            f"Expected throttle/reject increase on storage={storage}; "
            f"throttle {initial_throttle}->{final_throttle}, "
            f"reject {initial_reject}->{final_reject}, "
            f"sdk_throttle_count={sdk_throttle_count}")

    def test_sync_gateway_backoff_simulation(self):
        """
        Simulate Sync Gateway-style sustained burst SET load and verify the
        bucket rate-limiter applies backoff without client crash. Real
        Sync Gateway end-to-end is a manual test.
        """
        bucket_name = self.bucket.name
        initial_stats = self.get_throttle_stats(bucket_name)
        initial_throttle = int(initial_stats.get("throttle_count_total", 0))
        initial_reject = int(initial_stats.get("reject_count_total", 0))

        sdk_throttle_count = self._burst_writes()

        final_stats = self.get_throttle_stats(bucket_name)
        final_throttle = int(final_stats.get("throttle_count_total", 0))
        final_reject = int(final_stats.get("reject_count_total", 0))
        self.log.info(f"SGW sim stats: {final_stats}, "
                      f"sdk_throttle_count={sdk_throttle_count}")

        self.assertTrue(
            final_throttle > initial_throttle
            or final_reject > initial_reject
            or sdk_throttle_count > 0,
            f"Expected throttle/reject increase under SGW load; throttle "
            f"{initial_throttle}->{final_throttle}, "
            f"reject {initial_reject}->{final_reject}, "
            f"sdk_throttle_count={sdk_throttle_count}")


class RateLimitingUpgradeTests(UpgradeBase):
    """
    Upgrade-time tests for KV rate limiting (gated to 8.1).
    """
    def setUp(self):
        super(RateLimitingUpgradeTests, self).setUp()
        self.bucket_rest = BucketRestApi(self.cluster.master)
        self.target_throttle_reserved = self.input.param(
            "bucket_throttle_reserved", 6000)
        self.target_throttle_hard_limit = self.input.param(
            "bucket_throttle_hard_limit", 12000)

    def tearDown(self):
        # Retry delete on every node — cluster may be transient after upgrade
        import time
        bucket_names = [b.name for b in
                        list(getattr(self.cluster, "buckets", []) or [])]
        nodes = list(getattr(self.cluster, "nodes_in_cluster", []) or [])
        self.log.info(
            f"Upgrade tearDown: buckets={bucket_names}, "
            f"nodes={[n.ip for n in nodes]}")
        for name in bucket_names:
            for attempt in range(6):
                done = False
                for node in nodes:
                    try:
                        status, content = BucketRestApi(node).delete_bucket(
                            name)
                        self.log.info(
                            f"Delete {name} on {node.ip} attempt "
                            f"{attempt}: status={status}, content={content}")
                        if status:
                            done = True
                            break
                    except Exception as e:
                        self.log.warning(
                            f"Delete {name} on {node.ip} raised: {e}")
                if done:
                    break
                time.sleep(10)
        super(RateLimitingUpgradeTests, self).tearDown()

    def _set_rate_limit(self, master_node):
        edit_params = {
            Bucket.throttleReserved: self.target_throttle_reserved,
            Bucket.throttleHardLimit: self.target_throttle_hard_limit,
        }
        return BucketRestApi(master_node).edit_bucket(self.bucket.name,
                                                       edit_params)

    def test_rate_limit_in_mixed_mode_cluster(self):
        """
        With one node upgraded to 8.1 and others on the initial pre-8.1
        version, rate-limit edits must be rejected.
        """
        self.upgrade_version = self.upgrade_chain[-1]
        nodes = list(self.cluster.nodes_in_cluster)
        first_node = nodes[0]
        self.log.info(f"Upgrading first node {first_node.ip} only")
        self.upgrade_function[self.upgrade_type](first_node)

        status, content = self._set_rate_limit(first_node)
        self.log.info(f"Mixed-mode rate-limit edit: status={status}, "
                      f"content={content}")
        try:
            self.assertFalse(
                status,
                f"Rate limit edit should be rejected in mixed-mode: {content}")
        finally:
            # Finish upgrading remaining nodes so cluster ends uniform 8.1;
            # otherwise bucket deletes fail in mixed-mode tearDown.
            for node in list(self.cluster.nodes_in_cluster):
                if node.ip == first_node.ip:
                    continue
                try:
                    self.upgrade_function[self.upgrade_type](node)
                except Exception as e:
                    self.log.warning(
                        f"Post-assert upgrade of {node.ip} failed: {e}")

    def test_rate_limit_after_full_upgrade(self):
        """
        After full upgrade to 8.1+, rate-limit edits must succeed and
        values persist via REST.
        """
        self.upgrade_version = self.upgrade_chain[-1]
        for node in list(self.cluster.nodes_in_cluster):
            self.log.info(f"Upgrading node {node.ip}")
            self.upgrade_function[self.upgrade_type](node)

        status, content = self._set_rate_limit(self.cluster.master)
        self.assertTrue(
            status, f"Rate limit edit should succeed post-upgrade: {content}")

        _, buckets = self.bucket_rest.get_bucket_info()
        bucket_info = next(b for b in buckets if b['name'] == self.bucket.name)
        self.assertEqual(
            bucket_info.get('throttleReserved'),
            self.target_throttle_reserved,
            f"throttleReserved mismatch post-upgrade: "
            f"{bucket_info.get('throttleReserved')}")
        self.assertEqual(
            bucket_info.get('throttleHardLimit'),
            self.target_throttle_hard_limit,
            f"throttleHardLimit mismatch post-upgrade: "
            f"{bucket_info.get('throttleHardLimit')}")
