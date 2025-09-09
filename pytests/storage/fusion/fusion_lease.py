import json
import os
import threading
import time
import subprocess
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest

class FusionLease(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionLease, self).setUp()
        self.num_nodes_to_rebalance_in = self.input.param("num_nodes_to_rebalance_in", 0)
        self.num_nodes_to_rebalance_out = self.input.param("num_nodes_to_rebalance_out", 0)
        self.num_nodes_to_swap_rebalance = self.input.param("num_nodes_to_swap_rebalance", 0)
        
        split_path = self.local_test_path.split("/")
        self.fusion_output_dir = "/" + os.path.join("/".join(split_path[1:4]), "fusion_output")
        subprocess.run(f"mkdir -p {self.fusion_output_dir}", shell=True, executable="/bin/bash")

        self.volume_ids = []
    def tearDown(self):
        super().tearDown()

    def _fetch_leases(self, ssh, volume_id):
        try:
            cmd = f'/opt/couchbase/bin/fusion/metadata_dump --uri "chronicle://localhost:8091" --volume-id "{volume_id}"'
            output, error = ssh.execute_command(cmd)
            if error:
                self.log.error(f"metadata_dump failed for {volume_id}: {error}")
                return None
            metadata = json.loads("\n".join(output))
            return metadata.get("leases", [])
        except Exception as e:
            self.log.error(f"Error fetching leases for {volume_id}: {e}")
            return None

    def extract_volume_ids_from_plan(self, ssh, plan_file_path):
        output, error = ssh.execute_command(f'cat "{plan_file_path}"')
        if not output or error:
            raise Exception(f"Failed to read rebalance plan: {error}")
        plan_data = json.loads("\n".join(output))
        volume_ids = []
        for _, manifests in plan_data.get("nodes", {}).items():
            for manifest in manifests:
                vid = manifest.get("volumeID")
                if vid:
                    volume_ids.append(vid)
        return volume_ids

    def wait_for_plan(self, ssh, rebalance_count=1, timeout=300, poll_interval=5):
        plan_file_path = f"/root/fusion/reb_plan{rebalance_count}.json"
        end_time = time.time() + timeout
        while time.time() < end_time:
            out, _ = ssh.execute_command(f'test -f "{plan_file_path}" && echo 1 || echo 0')
            if out and out[0].strip() == "1":
                return plan_file_path
            time.sleep(poll_interval)
        raise TimeoutError(f"Rebalance plan {plan_file_path} not found in {timeout}s")

    def monitor_leases_during_rebalance(self, ssh, monitor_duration=900, poll_interval=1):
        """Poll each volume until it acquires a lease during rebalance, fail immediately if missing."""
        plan_file = self.wait_for_plan(ssh, rebalance_count=1)
        self.volume_ids = self.extract_volume_ids_from_plan(ssh, plan_file)
        lease_acquired = set()
        end_time = time.time() + monitor_duration
        while time.time() < end_time and len(lease_acquired) < len(self.volume_ids):
            for vid in self.volume_ids:
                if vid in lease_acquired:
                    continue
                leases = self._fetch_leases(ssh, vid)
                if not leases:
                    raise Exception(f"Volume {vid} failed to acquire lease during rebalance")
                else:
                    lease_acquired.add(vid)
            time.sleep(poll_interval)
        self.log.info(f"All volumes acquired leases: {len(lease_acquired)}")

    def validate_leases_released(self, ssh):
        """Verify all leases are released after rebalance, debug lease count."""
        lease_release_failures = []
        for vid in self.volume_ids:
            leases = self._fetch_leases(ssh, vid)
            lease_count = len(leases) if leases else 0
            # self.log.info(f"Volume {vid} final lease count: {lease_count}")  # debug print
            if leases:
                lease_release_failures.append(f"Volume {vid} still has leases after rebalance")
        if lease_release_failures:
            for f in lease_release_failures:
                self.log.error(f)
            raise Exception("Lease release check failed")
        self.log.info("All leases released successfully")


    def test_lease_acquire_and_release(self):
        self.initial_load()
        self.sleep(30, "Wait after initial load")
        ssh = RemoteMachineShellConnection(self.cluster.master)
        try:
            # Start rebalance in a separate thread
            rebalance_thread = threading.Thread(
                target=self.run_rebalance,
                kwargs={"output_dir": self.fusion_output_dir, "rebalance_count": 1},
            )
            rebalance_thread.start()
            # Start lease monitoring in parallel
            lease_thread = threading.Thread(target=self.monitor_leases_during_rebalance, args=(ssh,))
            lease_thread.start()
            # Wait for both threads to finish
            rebalance_thread.join()
            lease_thread.join()
            # Validate leases released after rebalance
            self.validate_leases_released(ssh)
        finally:
            ssh.disconnect()

    def monitor_rebalance_status(self, cluster, timeout=120):
        start_time = time.time()
        # Wait until rebalance status becomes 'running', then abort immediately
        while time.time() - start_time < timeout:
            try:
                status, progress = self.cluster_util.get_rebalance_status_and_progress(self.cluster)
                self.log.info(f"Rebalance status: {status}, progress: {progress}")
                if status == 'running':
                    # Abort rebalance as soon as it is detected running
                    stopped = self.cluster_util.stop_rebalance(self.cluster.master, wait_timeout=60,
                                                               include_failover=False)
                    if not stopped:
                        raise Exception("Failed to stop rebalance")
                    self.log.info("Rebalance aborted successfully")
                    return
            except Exception as e:
                self.log.info(f"Waiting for rebalance to start. Error: {e}")
            time.sleep(1)
        raise TimeoutError("Timed out waiting for rebalance to enter running state")

    def test_abort_rebalance_lease_handling(self):
        self.initial_load()
        self.sleep(30, "Wait after initial load")

        self.induce_rebalance_test_condition(self.servers, delay_time=5 * 60 * 1000,test_failure_condition="delay_rebalance_start")
        self.sleep(5, "Wait for rebalance_start delay to take effect")

        ssh = RemoteMachineShellConnection(self.cluster.master)
        try:
            rebalance_thread = threading.Thread(
                target=self.run_rebalance,
                kwargs={"output_dir": self.fusion_output_dir, "rebalance_count": 1},
            )
            rebalance_thread.start()
            # Ensure we know the volumes to check before aborting
            plan_file = self.wait_for_plan(ssh, rebalance_count=1, timeout=300, poll_interval=2)
            self.volume_ids = self.extract_volume_ids_from_plan(ssh, plan_file)
            monitor_rebalance_status_thread = threading.Thread(
                target=self.monitor_rebalance_status,
                args=(self.cluster, 600)
            )
            monitor_rebalance_status_thread.start()
            monitor_rebalance_status_thread.join()
            # Wait for leases to be released after abort (poll with timeout)
            end_time = time.time() + 180
            while True:
                try:
                    self.validate_leases_released(ssh)
                    break
                except Exception:
                    if time.time() > end_time:
                        # One last validate to raise detailed error
                        self.validate_leases_released(ssh)
                    time.sleep(2)
            rebalance_thread.join()
        finally:
            ssh.disconnect()
