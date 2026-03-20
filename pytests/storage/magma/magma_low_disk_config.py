'''
Created on 2026-03-18
@author: QE Team
Test case for CBSE-20968: Certify Magma on low disk configuration (throttled IOPS and bandwidth)
Validates that GSI index builds succeed under minimal disk configuration similar to cloud/virtualized environments.
Reference: CBSE-20968 - Amdocs - Index building stuck at 99%
Test environment: Multi-bucket (1024 vb) cluster with GSI indexes
Config 1: 3000 IOPS and 125MB/s bandwidth (AWS baseline)
Goal: Find minimum IOPS/bandwidth requirements for successful index builds
'''
import json
import time
from threading import Thread
from cb_constants import CbServer
from couchbase_helper.tuq_helper import N1QLHelper
from storage.magma.magma_base import MagmaBaseTest
from lib.gsiLib.GsiHelper_Rest import GsiHelper
from shell_util.remote_connection import RemoteMachineShellConnection
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from Jython_tasks.task_manager import TaskManager


class MagmaLowDiskConfigTests(MagmaBaseTest):
    def setUp(self):
        super(MagmaLowDiskConfigTests, self).setUp()
        # Test parameters
        self.target_vbuckets = self.input.param("target_vbuckets", 1024)
        self.iops_limit = self.input.param("iops_limit", 3000)
        self.bandwidth_limit_mbps = self.input.param("bandwidth_limit_mbps", 125)
        self.index_build_timeout = self.input.param("index_build_timeout", 7200)
        self.monitor_interval = self.input.param("monitor_interval", 60)
        self.device_name = self.input.param("device_name",
                                            "/dev/xvdb")  # Block device for throttling (use parent device, not partition)
        # GSI parameters
        self.num_indexes_per_bucket = self.input.param("num_indexes_per_bucket", 3)
        # Default index fields: name (string), age (number), gender (string)
        # These fields exist in the document structure used by Sirius loader
        self.index_fields = self.input.param("index_fields", "name;age;gender").split(";")
        # Update cycle parameters
        self.num_update_cycles = self.input.param("num_update_cycles", 1)
        # Ops rate for Sirius doc loader
        self.ops_rate = self.input.param("ops_rate", 10000)  # For initial load
        self.update_ops_rate = self.input.param("update_ops_rate", 10000)  # For update operations
        # Disk throttling control
        self.apply_cgroup_throttling = self.input.param("apply_cgroup_throttling", True)
        # Initialize GSI helper and N1QL helper
        self.gsi_helper = GsiHelper(self.cluster.master, self.log)
        self.n1ql_node = self.cluster_util.get_nodes_from_services_map(
            cluster=self.cluster, service_type=CbServer.Services.N1QL, get_all_nodes=False)
        self.n1ql_helper = N1QLHelper(
            server=self.n1ql_node,
            use_rest=True,
            log=self.log)
        # Initialize gsi_thread for async index operations
        self.gsi_thread = self.task
        # Store initial disk stats
        self.initial_disk_stats = self._get_disk_stats()
        self.log.info("=== MagmaLowDiskConfigTests setup complete ===")
        self.log.info("Target vbuckets: {}".format(self.target_vbuckets))
        self.log.info("IOPS limit: {}".format(self.iops_limit))
        self.log.info("Bandwidth limit: {} MB/s".format(self.bandwidth_limit_mbps))
        self.log.info("Update cycles: {}".format(self.num_update_cycles))

    def tearDown(self):
        # Remove disk throttling only if it was applied
        if self.apply_cgroup_throttling:
            self._remove_disk_throttling()
        super(MagmaLowDiskConfigTests, self).tearDown()

    def _get_disk_stats(self):
        """Get current disk usage statistics for all nodes"""
        disk_stats = {}
        for node in self.cluster.nodes_in_cluster:
            remote = RemoteMachineShellConnection(node)
            disk_info = remote.get_disk_info(in_MB=True, path=node.data_path)
            disk_stats[node.ip] = {
                'path': node.data_path,
                'info': disk_info
            }
            remote.disconnect()
        return disk_stats

    def _apply_disk_throttling_systemd(self, node):
        """Apply disk throttling using systemd service configuration"""
        remote = RemoteMachineShellConnection(node)
        try:
            systemd_override_dir = "/etc/systemd/system/couchbase-server.service.d"
            override_file = "{}/io_limits.conf".format(systemd_override_dir)
            self.log.info("Applying systemd I/O limits on node {}".format(node.ip))

            # Step 0: Enable IO controller in cgroup hierarchy (CRITICAL!)
            self.log.info("Ensuring IO controller is enabled in cgroup hierarchy...")

            # Enable at root cgroup level
            output, error = remote.execute_command("cat /sys/fs/cgroup/cgroup.subtree_control")
            current_controllers = output[0] if output else ""
            if "io" not in current_controllers:
                self.log.info("Enabling IO controller at root cgroup...")
                remote.execute_command("echo '+io' | sudo tee /sys/fs/cgroup/cgroup.subtree_control")
            else:
                self.log.info("IO controller already enabled at root cgroup")

            # Enable at system.slice level
            output, error = remote.execute_command("cat /sys/fs/cgroup/system.slice/cgroup.subtree_control")
            current_controllers = output[0] if output else ""
            if "io" not in current_controllers:
                self.log.info("Enabling IO controller at system.slice...")
                remote.execute_command("echo '+io' | sudo tee /sys/fs/cgroup/system.slice/cgroup.subtree_control")
            else:
                self.log.info("IO controller already enabled at system.slice")

            # Step 1: Stop couchbase-server service
            self.log.info("Stopping couchbase-server service...")
            remote.execute_command("sudo systemctl stop couchbase-server")

            # Step 1a: Kill any lingering processes to ensure clean start
            self.log.info("Ensuring all Couchbase processes are terminated...")
            remote.execute_command("sudo pkill -9 beam.smp || true")
            remote.execute_command("sudo pkill -9 memcached || true")
            self.sleep(2, "Wait for processes to terminate")

            # Step 2: Create override directory and write config file
            remote.execute_command("sudo mkdir -p {}".format(systemd_override_dir))
            # Calculate bandwidth in bytes (more reliable than using 'M' suffix)
            bandwidth_bytes = self.bandwidth_limit_mbps * 1024 * 1024

            config_content = """[Service]
            IOAccounting=yes
            IOWriteIOPSMax={} {}
            IOWriteBandwidthMax={} {}
            IOReadIOPSMax={} {}
            IOReadBandwidthMax={} {}
            """.format(
                self.device_name, self.iops_limit,
                self.device_name, bandwidth_bytes,
                self.device_name, self.iops_limit,
                self.device_name, bandwidth_bytes
            )

            # Write config using tee (more reliable than heredoc)
            cmd = "echo '{}' | sudo tee {}".format(config_content.replace("'", "'\\''"), override_file)
            output, error = remote.execute_command(cmd)
            if error:
                self.log.warning("Error writing systemd config: {}".format(error))
            # Step 3: Reload systemd daemon after editing config file
            self.log.info("Reloading systemd daemon...")
            remote.execute_command("sudo systemctl daemon-reexec")
            remote.execute_command("sudo systemctl daemon-reload")
            # Step 4: Start couchbase-server service to apply limits
            self.log.info("Starting couchbase-server service...")
            remote.execute_command("sudo systemctl start couchbase-server")
            self.log.info("Applied systemd I/O limits on node {} - IOPS: {}, Bandwidth: {} MB/s, Device: {}".format(
                node.ip, self.iops_limit, self.bandwidth_limit_mbps, self.device_name))
            # Verify the service started successfully
            self.sleep(10, "Wait for service to start")
            output, error = remote.execute_command("sudo systemctl is-active couchbase-server")

            if output and output[0].strip() == "active":
                self.log.info("Couchbase server is active on node {}".format(node.ip))
            else:
                self.log.error("Couchbase server failed to start on node {}".format(node.ip))
                return False

            # Step 5: Wait for bucket warmup to complete after service restart
            self.log.info("Waiting for bucket warmup to complete on node {}".format(node.ip))
            for bucket in self.cluster.buckets:
                warmed_up = self.bucket_util._wait_warmup_completed(
                    bucket, servers=[node])
                if warmed_up:
                    self.log.info("Bucket '{}' warmup completed on node {}".format(
                        bucket.name, node.ip))
                else:
                    self.log.warning("Bucket '{}' warmup incomplete on node {} after timeout".format(
                        bucket.name, node.ip))

            # Step 6: Verify limits are actually applied to cgroup io.max
            self.log.info("Verifying throttling limits in cgroup io.max...")
            output, error = remote.execute_command(
                "sudo cat /sys/fs/cgroup/system.slice/couchbase-server.service/io.max")

            if output:
                self.log.info("Cgroup io.max content:")
                for line in output:
                    if line.strip():
                        self.log.info("  {}".format(line.strip()))

                # Check if any limits are set (look for actual limit parameters)
                has_limits = False
                for line in output:
                    if any(param in line for param in ["rbps=", "wbps=", "riops=", "wiops="]):
                        has_limits = True
                        break

                if has_limits:
                    self.log.info("SUCCESS: Throttling limits verified in cgroup io.max")
                    return True
                else:
                    self.log.warning("WARNING: io.max exists but no limits found (shows 'default')")
                    self.log.warning("Systemd may not have applied limits to cgroup")
                    return False
            else:
                self.log.error("ERROR: Could not read cgroup io.max file")
                return False
        except Exception as e:
            self.log.error("Failed to apply systemd I/O limits: {}".format(str(e)))
            return False
        finally:
            remote.disconnect()

    def _remove_disk_throttling_systemd(self, node):
        """Remove systemd-based disk throttling"""
        remote = RemoteMachineShellConnection(node)
        try:
            override_file = "/etc/systemd/system/couchbase-server.service.d/io_limits.conf"
            self.log.info("Removing systemd I/O limits from node {}".format(node.ip))
            # Step 1: Stop couchbase-server service
            self.log.info("Stopping couchbase-server service...")
            remote.execute_command("sudo systemctl stop couchbase-server")

            # Step 1a: Kill any lingering processes
            remote.execute_command("sudo pkill -9 beam.smp || true")
            remote.execute_command("sudo pkill -9 memcached || true")
            self.sleep(2, "Wait for processes to terminate")

            # Step 2: Remove override file
            remote.execute_command("sudo rm -f {}".format(override_file))
            # Step 3: Reload systemd daemon after removing config file
            self.log.info("Reloading systemd daemon...")
            remote.execute_command("sudo systemctl daemon-reexec")
            remote.execute_command("sudo systemctl daemon-reload")
            # Step 4: Start couchbase-server service
            self.log.info("Starting couchbase-server service...")
            remote.execute_command("sudo systemctl start couchbase-server")
            self.log.info("Removed systemd I/O limits from node {}".format(node.ip))
            # Wait for service to be ready
            self.sleep(10, "Wait for service to start")
        except Exception as e:
            self.log.error("Failed to remove systemd I/O limits: {}".format(str(e)))
        finally:
            remote.disconnect()

    def _apply_disk_throttling(self):
        """Apply disk I/O throttling to all KV nodes using systemd"""
        self.log.info("Applying disk throttling to cluster nodes...")
        throttling_applied = False

        for node in self.cluster.kv_nodes:
            success = self._apply_disk_throttling_systemd(node)
            if success:
                throttling_applied = True
                self.log.info("Throttling successfully applied on node {}".format(node.ip))
            else:
                self.log.error("Throttling FAILED on node {}".format(node.ip))

        if not throttling_applied:
            self.log.error("CRITICAL: Throttling failed on all nodes!")
            self.log.error("Test results will be invalid - no I/O limits are enforced")

        # Wait for cluster to stabilize after service restarts
        self.sleep(30, "Wait for cluster to stabilize after service restarts")

    def _remove_disk_throttling(self):
        """Remove disk throttling from all nodes"""
        self.log.info("Removing disk throttling...")
        for node in self.cluster.nodes_in_cluster:
            self._remove_disk_throttling_systemd(node)
        # Wait for cluster to stabilize
        self.sleep(30, "Wait for cluster to stabilize after removing throttling")

    def _create_gsi_indexes(self, bucket):
        """Create GSI indexes on the bucket using async create operations"""
        self.log.info("Creating {} GSI indexes on bucket {}".format(
            self.num_indexes_per_bucket, bucket.name))
        indexes_created = []
        # Only create indexes for fields that exist in index_fields list
        num_indexes_to_create = min(self.num_indexes_per_bucket, len(self.index_fields))
        for i in range(num_indexes_to_create):
            field = self.index_fields[i]
            index_name = "idx_{}_{}".format(field, bucket.name.replace(".", "_"))
            # Create index with defer build using standard query pattern
            query = "CREATE INDEX `{}` ON `{}`(`{}`) WITH {{'defer_build': true, 'num_replica': {}}}".format(
                index_name, bucket.name, field, self.num_replicas)
            try:
                # Use async_create_index with task management (following base_2i.py pattern)
                create_index_task = self.gsi_thread.async_create_index(
                    server=self.n1ql_node,
                    bucket=bucket,
                    query=query,
                    n1ql_helper=self.n1ql_helper,
                    index_name=index_name,
                    defer_build=True)
                self.task_manager.get_task_result(create_index_task)
                self.assertTrue(create_index_task.result,
                                "Create Index failed for: {}".format(index_name))
                
                # Verify index was created and is in deferred state via direct query
                verify_query = "SELECT state FROM system:indexes WHERE name = '{}' " \
                               "AND keyspace_id = '{}'".format(index_name, bucket.name)
                verify_result = self.n1ql_helper.run_cbq_query(
                    query=verify_query, server=self.n1ql_node)
                index_ready = (len(verify_result.get('results', [])) > 0 and
                               verify_result['results'][0].get('state') == 'deferred')
                self.assertTrue(index_ready,
                                "Index not in deferred state after creation: {}".format(index_name))
                
                indexes_created.append(index_name)
                self.log.info("Created index: {}".format(index_name))
                
                # Add delay between index creations to avoid indexer overload (following base_2i.py pattern)
                self.sleep(1, "Wait between index creation")
            except Exception as e:
                self.log.error("Failed to create index {}: {}".format(index_name, str(e)))
                # Continue with next index instead of failing entire operation
        return indexes_created

    def _build_gsi_indexes(self, bucket, indexes):
        """Build deferred GSI indexes using async build operations"""
        if not indexes:
            return
        self.log.info("Building indexes on bucket {}: {}".format(bucket.name, indexes))
        # Build index query with proper backticks
        index_list_with_backticks = ",".join(["`{}`".format(idx) for idx in indexes])
        query = "BUILD INDEX on `{}`({}) USING GSI".format(bucket.name, index_list_with_backticks)
        try:
            # Use async_build_index with task management (following base_2i.py pattern)
            build_index_task = self.gsi_thread.async_build_index(
                server=self.n1ql_node,
                bucket=bucket,
                query=query,
                n1ql_helper=self.n1ql_helper)
            
            self.task_manager.get_task_result(build_index_task)
            self.assertTrue(build_index_task.result,
                            "Build Index failed for bucket: {}".format(bucket.name))
            
            # Log successful build command
            self.log.info("Build index command successfully issued for bucket {}".format(bucket.name))
            
        except Exception as e:
            self.log.error("Failed to build indexes on bucket {}: {}".format(bucket.name, str(e)))
            raise Exception("Index build failed for bucket {}: {}".format(bucket.name, str(e)))

    def _monitor_index_build_progress(self):
        """Monitor GSI index build progress across all buckets"""
        self.log.info("Monitoring index build progress...")
        start_time = time.time()
        all_indexes_ready = False
        while time.time() - start_time < self.index_build_timeout:
            try:
                index_status = self.gsi_helper.index_status()
                all_ready = True
                for bucket_name, indexes in index_status.items():
                    for index_name, index_info in indexes.items():
                        status = index_info.get('status', 'Unknown')
                        progress = index_info.get('progress', '0')
                        self.log.info("Bucket: {}, Index: {}, Status: {}, Progress: {}%".format(
                            bucket_name, index_name, status, progress))
                        if status != 'Ready':
                            all_ready = False
                if all_ready:
                    all_indexes_ready = True
                    elapsed = time.time() - start_time
                    self.log.info("All indexes are ready! Time taken: {:.2f} seconds".format(elapsed))
                    break
                time.sleep(self.monitor_interval)
            except Exception as e:
                self.log.error("Error monitoring index status: {}".format(str(e)))

                time.sleep(self.monitor_interval)
        if not all_indexes_ready:
            self.log.error("Index build did not complete within timeout ({} seconds)".format(
                self.index_build_timeout))
            return False
        return True

    def _verify_index_functionality(self, bucket):
        """Verify that indexes are functional by running queries"""
        self.log.info("Verifying index functionality on bucket {}".format(bucket.name))
        try:
            # Simple query using the first index
            if self.index_fields:
                field = self.index_fields[0]
                query = "SELECT COUNT(*) FROM `{}` WHERE {} IS NOT NULL".format(
                    bucket.name, field)
                result = self.n1ql_helper.run_cbq_query(query=query)
                self.log.info("Index verification query succeeded: {}".format(result))
                return True
        except Exception as e:
            self.log.error("Index verification query failed: {}".format(str(e)))
            return False

    def test_low_disk_config_with_gsi_indexes(self):
        """
        Main test: Create multi-bucket setup with throttled disk I/O and verify GSI index builds succeed
        Test steps:
        1. Load initial data into all buckets using Sirius
        2. Apply disk I/O throttling (IOPS and bandwidth limits)
        3. Create GSI indexes on all buckets (deferred build)
        4. Build all indexes
        5. Start parallel update operations during index build (runs for N cycles)
        6. Monitor index build progress with detailed logging (during first update cycle)
        7. Wait for N update cycles to complete
        8. Verify all indexes reach "Ready" state
        9. Verify indexes are functional
        """
        self.log.info("=" * 80)
        self.log.info("Starting low disk config test with GSI indexes")
        self.log.info("Configuration: {} IOPS, {} MB/s bandwidth".format(
            self.iops_limit, self.bandwidth_limit_mbps))
        self.log.info("Buckets: {}, Target vbuckets: {}".format(
            len(self.cluster.buckets), self.target_vbuckets))
        self.log.info("Update cycles: {}".format(self.num_update_cycles))
        self.log.info("=" * 80)
        # Step 1: Load initial data using SiriusCouchbaseLoader
        self.log.info("Step 1: Loading initial data using Sirius doc loader")
        self.create_start = 0
        self.create_end = self.num_items
        # Create TaskManager for doc loading
        doc_loading_tm = TaskManager(self.process_concurrency)
        load_tasks = []
        for bucket in self.cluster.buckets:
            for scope in bucket.scopes.keys():
                if scope == CbServer.system_scope:
                    continue
                for collection in bucket.scopes[scope].collections.keys():
                    if self.skip_load_to_default_collection and collection == "_default" and scope == "_default":
                        continue
                    self.log.info("Loading data into {}:{}:{}".format(bucket.name, scope, collection))
                    print(self.ops_rate)
                    loader = SiriusCouchbaseLoader(
                        server_ip=self.cluster.master.ip, server_port=self.cluster.master.port,
                        username=self.cluster.master.rest_username,
                        password=self.cluster.master.rest_password,
                        bucket=bucket, scope_name=scope, collection_name=collection,
                        key_prefix=self.key, key_size=self.key_size,
                        doc_size=self.doc_size,
                        key_type=self.key_type,
                        create_percent=100, read_percent=0,
                        update_percent=0, delete_percent=0,
                        expiry_percent=0,
                        create_start_index=self.create_start, create_end_index=self.create_end,
                        read_start_index=0, read_end_index=0,
                        update_start_index=0, update_end_index=0,
                        delete_start_index=0, delete_end_index=0,
                        expiry_start_index=0, expiry_end_index=0,
                        exp=0,
                        process_concurrency=self.process_concurrency,
                        validate_docs=False,
                        ops=self.ops_rate,
                        mutate=0
                    )
                    loader.create_doc_load_task()
                    doc_loading_tm.add_new_task(loader)
                    load_tasks.append(loader)
        # Wait for initial load to complete
        for task in load_tasks:
            doc_loading_tm.get_task_result(task)
        self.log.info("Step 1: Initial data load complete: {} items per collection".format(self.num_items))
        # Step 2: Apply disk throttling (controlled via parameter)
        if self.apply_cgroup_throttling:
            self.log.info("Step 2: Applying disk I/O throttling")
            self._apply_disk_throttling()
            self.sleep(30, "Wait for disk throttling to take effect")
        else:
            self.log.info("Step 2: Skipping disk I/O throttling (apply_cgroup_throttling=False)")
        # Step 3: Create GSI indexes on all buckets
        self.log.info("Step 3: Creating GSI indexes")
        bucket_indexes = {}
        for bucket in self.cluster.buckets:
            indexes = self._create_gsi_indexes(bucket)
            bucket_indexes[bucket.name] = indexes
        self.sleep(10, "Wait after index creation")
        # Step 4: Build all indexes
        self.log.info("Step 4: Building GSI indexes")
        build_start_time = time.time()
        for bucket in self.cluster.buckets:
            indexes = bucket_indexes.get(bucket.name, [])
            self._build_gsi_indexes(bucket, indexes)
        # Step 5: Start parallel update operations during index build
        self.log.info("Step 5: Starting parallel update operations during index build")
        self.log.info("Update cycles: {}".format(self.num_update_cycles))
        # Run update operations for N cycles
        for cycle in range(self.num_update_cycles):
            self.log.info("Starting update cycle {}/{}".format(cycle + 1, self.num_update_cycles))
            # Create new TaskManager for update operations in this cycle
            update_tm = TaskManager(self.process_concurrency)
            update_tasks = []
            self.update_start = 0
            self.update_end = self.num_items
            for bucket in self.cluster.buckets:
                for scope in bucket.scopes.keys():
                    if scope == CbServer.system_scope:
                        continue
                    for collection in bucket.scopes[scope].collections.keys():
                        if self.skip_load_to_default_collection and collection == "_default" and scope == "_default":
                            continue
                        self.log.info("Starting updates on {}:{}:{} with ops_rate={}".format(
                            bucket.name, scope, collection, self.update_ops_rate))
                        loader = SiriusCouchbaseLoader(
                            server_ip=self.cluster.master.ip, server_port=self.cluster.master.port,
                            username=self.cluster.master.rest_username,
                            password=self.cluster.master.rest_password,
                            bucket=bucket, scope_name=scope, collection_name=collection,
                            key_prefix=self.key, key_size=self.key_size,
                            doc_size=self.doc_size,
                            key_type=self.key_type,
                            create_percent=0, read_percent=20,
                            update_percent=80, delete_percent=0,
                            expiry_percent=0,
                            create_start_index=0, create_end_index=0,
                            read_start_index=self.update_start, read_end_index=self.update_end,
                            update_start_index=self.update_start, update_end_index=self.update_end,
                            delete_start_index=0, delete_end_index=0,
                            expiry_start_index=0, expiry_end_index=0,
                            exp=0,
                            process_concurrency=self.process_concurrency,
                            validate_docs=False,
                            ops=self.update_ops_rate,
                            mutate=1
                        )
                        loader.create_doc_load_task()
                        update_tm.add_new_task(loader)
                        update_tasks.append(loader)
            self.log.info("Update cycle {}/{} started in parallel".format(cycle + 1, self.num_update_cycles))
            # Step 6: Monitor index build progress while updates are running (only in first cycle)
            if cycle == 0:
                self.log.info("Step 6: Monitoring index build progress while updates are running")
                index_build_success = self._monitor_index_build_progress()
            # Step 7: Wait for update operations to complete for this cycle
            self.log.info("Waiting for update cycle {}/{} to complete".format(cycle + 1, self.num_update_cycles))
            for task in update_tasks:
                update_tm.get_task_result(task)
            self.log.info("Update cycle {}/{} completed".format(cycle + 1, self.num_update_cycles))
        build_duration = time.time() - build_start_time
        self.log.info("Total index build and update duration: {:.2f} seconds".format(build_duration))
        # Step 8: Verify results
        self.assertTrue(index_build_success,
                        "Index build failed or timed out under low disk config. IOPS: {}, Bandwidth: {} MB/s".format(
                            self.iops_limit, self.bandwidth_limit_mbps))
        # Step 9: Verify index functionality
        self.log.info("Step 8: Verifying index functionality")
        for bucket in self.cluster.buckets:
            verify_success = self._verify_index_functionality(bucket)
            self.assertTrue(verify_success,
                            "Index verification failed for bucket {}".format(bucket.name))
        # Step 10: Log final stats
        final_disk_stats = self._get_disk_stats()
        total_docs_updated = self.num_items * self.num_update_cycles
        self.log.info("=" * 80)
        self.log.info("Test completed successfully!")
        self.log.info("Configuration: {} IOPS, {} MB/s bandwidth".format(
            self.iops_limit, self.bandwidth_limit_mbps))
        self.log.info("Index build duration with parallel updates: {:.2f} seconds".format(build_duration))
        self.log.info("All {} indexes built and verified successfully".format(
            sum(len(indexes) for indexes in bucket_indexes.values())))
        self.log.info("Initial documents loaded: {} per collection".format(self.num_items))
        self.log.info("Update cycles completed: {}".format(self.num_update_cycles))
        self.log.info("Total documents updated across {} cycles: {} per collection".format(
            self.num_update_cycles, total_docs_updated))
        self.log.info("=" * 80)
