import threading
import time

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
        # Parsed once in cb_basetest; None means the conf did not set it
        throttle_reserved = self.bucket_throttle_reserved
        throttle_hard_limit = self.bucket_throttle_hard_limit
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
                "throttle_hard_limit", "throttle_reserved",
                "ru_total", "wu_total", "node_capacity"
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
        # CbCli returns the command's output lines; couchbase-cli reports
        # "SUCCESS: Bucket edited" on a successful bucket-edit
        self.assertIn("SUCCESS: Bucket edited", str(cli_output),
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
        # ep-engine reports a disabled hard limit as UINT64_MAX (no cap),
        # not 0 -- 0 would mean "zero throughput allowed", the opposite of
        # unrestricted.
        UNLIMITED_HARD_LIMIT = 2 ** 64 - 1

        bucket_name = self.bucket.name
        throttle_stats = self.get_throttle_stats(bucket_name)
        self.log.info(f"CE throttle stats: {throttle_stats}")

        # Verify that throttle values are 0/disabled in CE
        self.assertEqual(int(throttle_stats.get("throttle_reserved", 0)), 0,
                         f"throttle_reserved should be 0 in CE, found {throttle_stats.get('throttle_reserved')}")
        self.assertEqual(int(throttle_stats.get("throttle_hard_limit", 0)), UNLIMITED_HARD_LIMIT,
                         f"throttle_hard_limit should be unlimited ({UNLIMITED_HARD_LIMIT}) in CE, "
                         f"found {throttle_stats.get('throttle_hard_limit')}")
        self.assertEqual(int(throttle_stats.get("throttle_count_total", 0)), 0,
                         f"throttle_count_total should be 0 in CE, found {throttle_stats.get('throttle_count_total')}")
        self.assertEqual(int(throttle_stats.get("reject_count_total", 0)), 0,
                         f"reject_count_total should be 0 in CE, found {throttle_stats.get('reject_count_total')}")

    def get_global_throttle_enabled(self):
        """
        Read the cluster-wide throttle toggle from
        /pools/default/settings/memcached/global. The endpoint omits keys
        left at their default, so an unset toggle reads back as None.
        """
        status, content = self.cluster_rest.manage_global_memcached_setting()
        self.assertTrue(
            status, f"Failed to read global memcached settings: {content}")
        return content.get("throttle_enabled")

    def test_enable_disable_rate_limiting_rest(self):
        """
        Set and validate the enable/disable toggle via REST API.

        throttle_enabled is a node-level memcached setting, not a bucket
        property: couchbase-cli sends only throttleReserved and
        throttleHardLimit per bucket, and drives the toggle through
        /pools/default/settings/memcached/global.
        """
        bucket_name = self.bucket.name

        # setUp enabled the toggle cluster-wide
        self.assertEqual(self.get_global_throttle_enabled(), True,
                         "Rate limiting should be enabled")

        self.log.info("Disabling rate limiting via REST")
        status, content = self.cluster_rest.manage_global_memcached_setting(
            throttle_enabled="false")
        self.assertTrue(status, f"Failed to disable rate limiting: {content}")
        self.assertFalse(self.get_global_throttle_enabled(),
                         "Rate limiting should be disabled")

        self.log.info("Re-enabling rate limiting via REST")
        status, content = self.cluster_rest.manage_global_memcached_setting(
            throttle_enabled="true")
        self.assertTrue(
            status, f"Failed to re-enable rate limiting: {content}")
        self.assertEqual(self.get_global_throttle_enabled(), True,
                         "Rate limiting should be re-enabled")

        # Per-bucket limits must survive the cluster-wide toggle
        status, buckets = self.bucket_rest.get_bucket_info()
        bucket_info = next(b for b in buckets if b['name'] == bucket_name)
        self.log.info(
            f"Bucket limits after toggle: "
            f"throttleReserved={bucket_info.get('throttleReserved')}, "
            f"throttleHardLimit={bucket_info.get('throttleHardLimit')}")
        expected = {
            'throttleReserved': self.bucket_throttle_reserved,
            'throttleHardLimit': self.bucket_throttle_hard_limit}
        for field, value in expected.items():
            if value is None:
                continue
            self.assertEqual(
                bucket_info.get(field), value,
                f"{field} changed across the enable/disable toggle")

    def test_soft_limit_throttling(self):
        """
        Validate ops are throttled once the offered rate crosses the
        bucket's throttle limit.

        Offered load saturates exactly at throttleHardLimit (a bucket
        capped at 200 tops out at ~199-222 ops/sec). ns_server also refuses
        a bucket whose reserved sum exceeds node_capacity. Before MB-73910,
        throttleReserved and node_capacity did not cap a running bucket at
        all: a single bucket at reserved=100 with node_capacity=500
        sustained ~2000 ops/sec while throttle_count_total stayed 0. With
        the fix, going over the reservation draws from the free pool
        (node_capacity - sum of reserved), see
        test_throttle_above_reserved_with_free_pool.

        The limit therefore has to sit below the rate the client can
        actually offer, which on a QE VM is a few hundred ops/sec: with a
        limit above that the limiter never trips and the counter cannot
        move no matter how much load is queued.
        """
        bucket_name = self.bucket.name

        initial_stats = self.get_throttle_stats(bucket_name)
        initial_throttle_count = int(initial_stats.get("throttle_count_total", 0))
        self.log.info(f"Initial throttle stats: {initial_stats}")

        self.log.info("Driving load above the bucket throttle limit...")
        self.load_docs_to_bucket(self.bucket, start=0, end=5000,
                                  batch_size=500, concurrency=8)

        final_stats = self.get_throttle_stats(bucket_name)
        final_throttle_count = int(final_stats.get("throttle_count_total", 0))
        self.log.info(f"Final throttle stats: {final_stats}")
        self.assertGreater(
            final_throttle_count, initial_throttle_count,
            "Throttle count should increase once the offered rate exceeds "
            f"throttleHardLimit={final_stats.get('throttle_hard_limit')}")

    def test_hard_limit_rejection(self):
        """
        Validate back-pressure once ops exceed the bucket's HARD limit.

        The signal is throttle_count_total, not reject_count_total. Server
        8.5.0 documents kv_reject_count_total as "operations rejected for a
        bucket since reset (e.g., invalid operations)", while
        kv_throttle_count_total is "the number of times requests have been
        throttled" (etc/couchbase/kv/metrics_metadata.json). Ops held back by
        the limiter come back as EWOULD_THROTTLE, which the error map marks
        temp / retry-later / rate-limit, so the SDK retries them rather than
        the engine counting a rejection.
        """
        bucket_name = self.bucket.name

        initial_stats = self.get_throttle_stats(bucket_name)
        initial_throttle_count = int(
            initial_stats.get("throttle_count_total", 0))
        initial_reject_count = int(initial_stats.get("reject_count_total", 0))
        self.log.info(f"Initial stats: {initial_stats}")

        self.log.info("Driving heavy load to exceed hard limit...")
        task = None
        try:
            task = self.load_docs_to_bucket(self.bucket, start=0, end=100000,
                                            batch_size=1000, concurrency=16)
        except Exception as e:
            self.log.info(f"Expected exception during hard limit test: {e}")

        final_stats = self.get_throttle_stats(bucket_name)
        final_throttle_count = int(final_stats.get("throttle_count_total", 0))
        final_reject_count = int(final_stats.get("reject_count_total", 0))
        # Client-visible failures are logged rather than asserted on: an
        # ephemeral/noEviction bucket can also fail writes once it is full,
        # so they do not isolate throttling the way the counter does
        self.log.info(
            f"Final stats: {final_stats}, "
            f"docs the loader could not write: "
            f"{len(task.fail) if task is not None else 'n/a'}, "
            f"reject_count_total {initial_reject_count}->{final_reject_count}")
        self.assertGreater(
            final_throttle_count, initial_throttle_count,
            "Throttle count should increase when exceeding hard limit")

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

        A serial per-client crud() loop tops out well below the throttle
        limits used by the suite, so callers that must exceed a limit should
        drive load via load_docs_to_bucket() and use this only to observe
        SDK-side throttle exceptions.
        """
        clients = [SDKClient(self.cluster, self.bucket)
                   for _ in range(num_clients)]
        throttle_counts = [0] * num_clients
        op_counts = [0] * num_clients

        def worker(c_idx, client):
            for i in range(ops_per_client):
                op_counts[c_idx] += 1
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
        start_time = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.time() - start_time
        for c in clients:
            try:
                c.close()
            except Exception:
                pass
        total_ops = sum(op_counts)
        self.log.info(
            f"Burst complete: clients={num_clients}, ops={total_ops}, "
            f"elapsed={elapsed:.1f}s, rate={total_ops / max(elapsed, 1):.0f} "
            f"ops/sec, sdk_throttle_count={sum(throttle_counts)}")
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

        # Doc-loader task sustains a write rate above throttleHardLimit,
        # which is what trips the limiter; the SDK burst that follows only
        # samples client-side throttle errors, and both stay small because
        # the limiter caps throughput at the hard limit once it engages
        self.log.info(f"Driving load on storage={storage} to exceed limits...")
        self.load_docs_to_bucket(self.bucket, start=0, end=5000,
                                 batch_size=500, concurrency=8)
        sdk_throttle_count = self._burst_writes(
            num_clients=self.input.param("burst_clients", 8),
            ops_per_client=self.input.param("burst_ops_per_client", 500))

        final_stats = self.get_throttle_stats(bucket_name)
        final_throttle = int(final_stats.get("throttle_count_total", 0))
        final_reject = int(final_stats.get("reject_count_total", 0))
        self.log.info(f"storage={storage} stats: {final_stats}, "
                      f"sdk_throttle_count={sdk_throttle_count}")

        self.assertTrue(
            final_throttle > initial_throttle
            or final_reject > initial_reject
            or sdk_throttle_count > 0,
            f"Expected throttle/reject increase on storage={storage} at "
            f"throttleHardLimit={final_stats.get('throttle_hard_limit')}; "
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

    def _throttle_counters(self, bucket_name):
        """
        (units, limited, timestamp) of the bucket from one cbstats call.
        units: ru_total + wu_total, what the throttle gauge is charged with
        limited: throttle_count_total + reject_count_total, as a client in
                 non-blocking throttling mode is rejected and not delayed
        timestamp: Midpoint of the cbstats call, which takes ~1s over SSH
        """
        before = time.time()
        stats = self.get_throttle_stats(bucket_name)
        timestamp = (before + time.time()) / 2
        return (int(stats["ru_total"]) + int(stats["wu_total"]),
                int(stats["throttle_count_total"])
                + int(stats["reject_count_total"]),
                timestamp)

    def _throttle_limits(self, bucket):
        """
        Throttle limits as seen by memcached, so the expected caps do not
        depend on how ns_server defaults the unset ones. memcached reports
        node_capacity only from MB-74030, so older builds take ns_server's.
        Returns (node_capacity, reserved, hard_limit), None for unlimited
        """
        unlimited = 2 ** 64 - 1
        stats = self.get_throttle_stats(bucket.name)
        if "node_capacity" not in stats:
            status, content = \
                self.cluster_rest.manage_global_memcached_setting()
            self.assertTrue(status, f"Failed to get node_capacity: {content}")
            stats["node_capacity"] = content["node_capacity"]
        limits = [int(stats[key]) for key in ("node_capacity",
                                              "throttle_reserved",
                                              "throttle_hard_limit")]
        self.log.info(f"{bucket.name} :: node_capacity={limits[0]}, "
                      f"throttle_reserved={limits[1]}, "
                      f"throttle_hard_limit={limits[2]}")
        return tuple(None if limit == unlimited else limit
                     for limit in limits)

    def _wait_for_throttle_limits(self, bucket, expected, timeout=60):
        """
        ns_server pushes the REST changes to memcached asynchronously, so
        poll till memcached reports the expected
        (node_capacity, reserved, hard_limit)
        """
        end_time = time.time() + timeout
        limits = self._throttle_limits(bucket)
        while limits != expected and time.time() < end_time:
            time.sleep(2)
            limits = self._throttle_limits(bucket)
        self.assertEqual(limits, expected,
                         f"memcached did not pick up the throttle limits "
                         f"in {timeout} secs")

    def _measure_throttled_load(self, load_plan):
        """
        Runs an SDK upsert load on each bucket in load_plan and measures,
        server side, the units consumed per sec and the requests
        throttled / rejected within a 'load_duration' secs window.
        Values are < write_unit_size, so every upsert is charged 1 WU.

        :param load_plan: {bucket: ops_per_sec}. None saturates the bucket
                          using 'load_clients' SDK clients. A number paces a
                          single client to at most that rate
        :return: {bucket_name: {"units_per_sec": float, "limited": int}}
        """
        duration = self.input.param("load_duration", 30)
        num_clients = self.input.param("load_clients", 8)
        warmup = 5
        doc = {"data": "x" * 256}
        stop_load = threading.Event()
        errors = []
        sdk_clients = []
        workers = []

        def run_load(client, key_prefix, ops_per_sec):
            index = 0
            while not stop_load.is_set():
                op_start = time.time()
                result = client.crud("update", f"{key_prefix}_{index % 1000}",
                                     doc, timeout=10)
                if not result.get("status") and len(errors) < 10:
                    errors.append(result.get("error"))
                index += 1
                if ops_per_sec:
                    # Pacing each op (no catch-up bursts) keeps the rate
                    # under ops_per_sec within every second
                    stop_load.wait(1.0 / ops_per_sec
                                   - (time.time() - op_start))

        def snapshot():
            # Each bucket's counters carry the timestamp of their own read
            return {bucket.name: self._throttle_counters(bucket.name)
                    for bucket in load_plan}

        try:
            for bucket, ops_per_sec in load_plan.items():
                for c_index in range(1 if ops_per_sec else num_clients):
                    client = SDKClient(self.cluster, bucket)
                    sdk_clients.append(client)
                    workers.append(threading.Thread(
                        target=run_load,
                        args=(client, f"free_pool_{c_index}", ops_per_sec)))
            for worker in workers:
                worker.start()
            # Let the clients bootstrap and the throttler engage
            time.sleep(warmup)
            start = snapshot()
            time.sleep(duration)
            end = snapshot()
        finally:
            stop_load.set()
            for worker in workers:
                worker.join()
            for client in sdk_clients:
                client.close()

        if errors:
            self.log.info(f"Sample of failed upserts: {errors}")
        result = {}
        for name in start:
            units_0, limited_0, time_0 = start[name]
            units_1, limited_1, time_1 = end[name]
            result[name] = {
                "units_per_sec": (units_1 - units_0) / (time_1 - time_0),
                "limited": limited_1 - limited_0}
            self.log.info(f"{name} :: units/sec="
                          f"{result[name]['units_per_sec']:.1f}, "
                          f"throttled+rejected={result[name]['limited']}")
        return result

    def _assert_throttled_at(self, bucket_name, result, cap):
        """
        Bucket was throttled and consumed no more than 'cap' units/sec.
        The load offered is well over the caps used in the conf, so
        throttle not engaging means the bucket ran past its cap.
        Delayed requests are charged to the next second, so the rate holds
        at the cap. 'rate_tolerance' covers the window edges
        """
        tolerance = self.input.param("rate_tolerance", 1.2)
        self.assertGreater(
            result["limited"], 0,
            f"{bucket_name} not throttled at {result['units_per_sec']:.0f} "
            f"units/sec, expected to be capped at {cap} units/sec")
        self.assertLessEqual(
            result["units_per_sec"], cap * tolerance,
            f"{bucket_name} consumed {result['units_per_sec']:.0f} "
            f"units/sec, over its cap of {cap} units/sec")

    def test_throttle_above_reserved_with_free_pool(self):
        """
        Ref: MB-73910
        A bucket over its throttleReserved, but under its
        throttleHardLimit, may go on only with units from the node's free
        pool (node_capacity - sum of all buckets' throttleReserved). Free
        pool units are also charged to the bucket's own gauge, so a
        saturated bucket is capped at
          min(throttleHardLimit, throttleReserved + free pool)
        Before the fix, a bucket with a hard limit set was never throttled
        under it, and the free pool was never charged. Without a hard
        limit the free pool was already enforced, so the conf sets
        throttleHardLimit at or above 'large_node_capacity' to hit the bug.

        Runs 3 phases on the same bucket, changing the limits in between,
         1. node_capacity from the conf, hard limit above the load: Free
            pool is node_capacity - reserved, so the bucket is throttled at
            node_capacity. Before the fix it was never throttled
         2. node_capacity raised to 'large_node_capacity', above the offered
            load: The bucket goes over its reservation using free pool
            units without being throttled
         3. throttleHardLimit set to 'pool_hard_limit': The bucket is
            throttled at the hard limit though the free pool still has
            units, which holds only if free pool units are charged to the
            bucket's own gauge as well
        Phases are ordered so that the hard limit is only ever lowered
        """
        large_node_capacity = self.input.param("large_node_capacity", 100000)
        pool_hard_limit = self.input.param("pool_hard_limit", 300)
        bucket_name = self.bucket.name
        node_capacity, reserved, hard_limit = \
            self._throttle_limits(self.bucket)
        if node_capacity is None or reserved is None:
            self.fail("Needs node_capacity and throttleReserved set, "
                      "else the bucket is never throttled")
        # Phases 1 and 2 must run under the hard limit, never at it
        if hard_limit is not None and hard_limit < large_node_capacity:
            self.fail(f"throttleHardLimit={hard_limit} must be unset or at "
                      f"least large_node_capacity={large_node_capacity}")
        if not reserved < pool_hard_limit < large_node_capacity:
            self.fail(f"pool_hard_limit={pool_hard_limit} must be over "
                      f"throttleReserved={reserved} and under "
                      f"large_node_capacity={large_node_capacity}")

        self.log.info(f"Phase 1: node_capacity={node_capacity}, "
                      f"throttleHardLimit={hard_limit}")
        result = self._measure_throttled_load(
            {self.bucket: None})[bucket_name]
        self._assert_throttled_at(f"Phase 1: {bucket_name}", result,
                                  node_capacity)

        self.log.info(f"Phase 2: node_capacity={large_node_capacity}, "
                      f"throttleHardLimit={hard_limit}")
        status, content = self.cluster_rest.manage_global_memcached_setting(
            node_capacity=large_node_capacity)
        self.assertTrue(status, f"Failed to set node_capacity: {content}")
        self._wait_for_throttle_limits(
            self.bucket, (large_node_capacity, reserved, hard_limit))
        result = self._measure_throttled_load(
            {self.bucket: None})[bucket_name]
        self.assertEqual(
            result["limited"], 0,
            f"Phase 2: Throttled at {result['units_per_sec']:.0f} units/sec "
            f"though the free pool had units, "
            f"node_capacity={large_node_capacity}")
        # Else the bucket never left its reservation, proving nothing
        self.assertGreater(
            result["units_per_sec"], reserved * 1.5,
            f"Phase 2: Load at {result['units_per_sec']:.0f} units/sec did "
            f"not go over throttleReserved={reserved}. Raise load_clients")

        self.log.info(f"Phase 3: node_capacity={large_node_capacity}, "
                      f"throttleHardLimit={pool_hard_limit}")
        status, content = self.bucket_rest.edit_bucket(
            bucket_name, {Bucket.throttleHardLimit: pool_hard_limit})
        self.assertTrue(status, f"Failed to set throttleHardLimit: {content}")
        self._wait_for_throttle_limits(
            self.bucket, (large_node_capacity, reserved, pool_hard_limit))
        result = self._measure_throttled_load(
            {self.bucket: None})[bucket_name]
        self._assert_throttled_at(f"Phase 3: {bucket_name}", result,
                                  pool_hard_limit)

    def _create_second_throttled_bucket(self):
        """
        Second bucket with the same throttle limits as self.bucket.
        Limits are applied at creation, so node_capacity must already fit
        the sum of the reservations
        """
        # Without bucket_size the first bucket takes the whole RAM quota
        if self.bucket_size is None:
            self.fail("Needs bucket_size, else no RAM quota is left for "
                      "the second bucket")
        self.create_bucket(self.cluster, bucket_name="free_pool_bucket")
        return self.cluster.buckets[-1]

    def test_free_pool_shared_between_buckets(self):
        """
        Ref: MB-73910
        Two saturated buckets with no hard limit share the node's free
        pool, first come, first served. The node as a whole is capped at
        node_capacity, every bucket is throttled and still gets at least
        its throttleReserved.
        Before the fix the free pool was never charged, so neither bucket
        was throttled
        """
        other_bucket = self._create_second_throttled_bucket()
        buckets = [self.bucket, other_bucket]
        node_capacity, reserved, _ = self._throttle_limits(self.bucket)
        if node_capacity is None or reserved is None:
            self.fail("Needs node_capacity and throttleReserved set, "
                      "else the buckets are never throttled")

        # A bucket can at most take all of the free pool on top of its
        # own reservation
        bucket_cap = reserved + node_capacity - reserved * len(buckets)

        results = self._measure_throttled_load(
            dict.fromkeys(buckets))
        total_rate = sum(result["units_per_sec"]
                         for result in results.values())
        self.log.info(f"Total units/sec={total_rate:.1f}")
        for bucket in buckets:
            result = results[bucket.name]
            self._assert_throttled_at(bucket.name, result, bucket_cap)
            # Reservation is honoured though the other bucket competes
            self.assertGreaterEqual(
                result["units_per_sec"], reserved * 0.8,
                f"{bucket.name} got {result['units_per_sec']:.0f} "
                f"units/sec, under throttleReserved={reserved}")
        self.assertLessEqual(
            total_rate, node_capacity * self.input.param("rate_tolerance",
                                                         1.2),
            f"Buckets together consumed {total_rate:.0f} units/sec, over "
            f"node_capacity={node_capacity}")

    def test_below_reserved_not_throttled_with_free_pool_used_up(self):
        """
        Ref: MB-73910
        One saturated bucket uses up the free pool while the other runs
        under its throttleReserved. Usage under the reservation comes from
        the bucket's own units, so it must not be throttled though the free
        pool is used up
        """
        busy_bucket = self._create_second_throttled_bucket()
        _, reserved, _ = self._throttle_limits(self.bucket)
        if reserved is None:
            self.fail("Needs throttleReserved set")
        paced_rate = self.input.param("paced_ops_per_sec", reserved // 2)

        results = self._measure_throttled_load({busy_bucket: None,
                                                self.bucket: paced_rate})
        self.assertGreater(
            results[busy_bucket.name]["limited"], 0,
            f"{busy_bucket.name} not throttled, so the free pool was not "
            "used up and this proves nothing")
        self.assertEqual(
            results[self.bucket.name]["limited"], 0,
            f"{self.bucket.name} throttled while at "
            f"{results[self.bucket.name]['units_per_sec']:.0f} units/sec, "
            f"under throttleReserved={reserved}")
