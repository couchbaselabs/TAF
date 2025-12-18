
import threading
from cb_tools.cbstats import Cbstats
import os
import subprocess
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest
from shell_util.remote_connection import RemoteMachineShellConnection

class FusionAcceleratorCLI(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionAcceleratorCLI, self).setUp()
        self.log.info("Setting up Fusion Accelerator CLI test")
        self.manifest_parts = self.input.param("manifest_parts", 20)
        self.min_storage_size = self.input.param("min_storage_size", 50 * 1024 * 1024 * 1024)  # 1GB default
        # Ensure rebalance mode is defined, else run_rebalance() will break
        self.num_nodes_to_rebalance_in = self.input.param("num_nodes_to_rebalance_in", 1)
        self.num_nodes_to_rebalance_out = self.input.param("num_nodes_to_rebalance_out", 0)
        self.num_nodes_to_swap_rebalance = self.input.param("num_nodes_to_swap_rebalance", 0)
        split_path = self.local_test_path.split("/")
        self.fusion_output_dir = "/" + os.path.join("/".join(split_path[1:4]), "fusion_output")
        self.log.info(f"Fusion output dir = {self.fusion_output_dir}")
        subprocess.run(f"mkdir -p {self.fusion_output_dir}", shell=True, executable="/bin/bash")
        # Override Fusion default settings
        for bucket in self.cluster.buckets:
            self.change_fusion_settings(bucket, upload_interval=self.fusion_upload_interval,
                                        checkpoint_interval=self.fusion_log_checkpoint_interval)

    def tearDown(self):
        super(FusionAcceleratorCLI, self).tearDown()

    def validate_manifest_parts(self):
        ssh = RemoteMachineShellConnection(self.nfs_server)
        try:
            max_parts = int(self.manifest_parts)
            min_size_gb = self.min_storage_size / (1024 * 1024 * 1024)
            
            # Construct guest_storage path on NFS server
            guest_storage_base = os.path.join(os.path.dirname(self.nfs_server_path), "guest_storage")
            
            # List all node directories in guest_storage
            list_nodes_cmd = f"ls -1 {guest_storage_base} 2>/dev/null"
            output, error = ssh.execute_command(list_nodes_cmd)
            
            if not output or not output[0].strip():
                self.fail(f"No node directories found in {guest_storage_base}")
            
            node_id = output[0].strip()
            self.log.info(f"Validating guest volumes for node {node_id}")
            
            # Find rebalance directory
            find_cmd = f"ls -d {guest_storage_base}/{node_id}/reb1 2>/dev/null"
            output, error = ssh.execute_command(find_cmd)
            
            if not output or not output[0].strip():
                self.fail(f"No guest directory found at {guest_storage_base}/{node_id}/reb1")
            
            node_guest_dir = output[0].strip()
            self.log.info(f"Guest directory = {node_guest_dir}")
            
            # Wait for guest volumes to be created
            attempts = 180
            count = 0
            while attempts > 0:
                list_cmd = f"ls -1 {node_guest_dir} 2>/dev/null | grep '^guest' | wc -l"
                o, _ = ssh.execute_command(list_cmd)
                count = int(o[0].strip()) if o else 0
                if count > 0:
                    break
                self.sleep(10, "Waiting for guest volumes...")
                attempts -= 1
            
            self.assertTrue(count > 0, f"No guest volumes found in {node_guest_dir}")
            self.sleep(5, "Wait to allow all guest volumes to be created")
            
            # Get final count
            list_cmd = f"ls -1 {node_guest_dir} | grep '^guest' | sort -V"
            output, error = ssh.execute_command(list_cmd)
            guest_volumes = [vol.strip() for vol in output if vol.strip()]
            count = len(guest_volumes)
            
            self.log.info(f"Guest volumes created = {count}, manifest_parts (max) = {max_parts}, min_storage_size = {min_size_gb:.2f} GB")
            
            # Rule 1: Guest volume count should NOT exceed manifest_parts
            self.assertTrue(count <= max_parts, 
                          f"Guest volumes ({count}) exceed manifest_parts limit ({max_parts})")
            
            # Rule 2: Each guest volume (except last) should be >= min_storage_size
            for i, guest_vol in enumerate(guest_volumes, 1):
                guest_vol_path = os.path.join(node_guest_dir, guest_vol)
                du_cmd = f"du -sb {guest_vol_path}"
                o, _ = ssh.execute_command(du_cmd)
                size_bytes = int(o[0].split()[0]) if o else 0
                size_gb = size_bytes / (1024 * 1024 * 1024)
                
                is_last = (i == count)
                self.log.info(f"Guest volume {guest_vol}: {size_gb:.2f} GB")
                
                if not is_last:
                    self.assertTrue(size_bytes >= self.min_storage_size, 
                                  f"Guest volume {guest_vol} size ({size_gb:.2f} GB) is less than min_storage_size ({min_size_gb:.2f} GB)")
            
            self.log.info("Validation passed")
            
        finally:
            ssh.disconnect()

    def test_split_manifest_multiple_parts(self):

        self.log.info("Testing split manifest with multiple parts")
        self.initial_load()
        self.sleep(30, "Sleep after data loading")
        
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(
            output_dir=self.fusion_output_dir,
            manifest_parts=self.manifest_parts,
            min_storage_size=self.min_storage_size
        )
        
        # Validate the manifest parts
        self.validate_manifest_parts()
        
        self.log.info("Test completed successfully")
