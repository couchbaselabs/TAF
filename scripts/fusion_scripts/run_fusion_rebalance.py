#!/usr/bin/env python3

import requests
import json
import subprocess
import time
import argparse
import sys
import os
from typing import List, Set
import paramiko
import socket
import concurrent.futures
import threading
import shutil

# Constants that don't vary
REBALANCE_PLAN_FILE = "/root/fusion/reb_plan.json"
MANIFEST_OUTPUT_DIR = "/root/fusion/out"
NODE_PORT = 18091
DATA_PATH = "/data"

def load_config(config_file: str, env: str) -> dict:
    if not config_file:
        raise ValueError("Config file path is required")
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file not found: {config_file}")

    with open(config_file, 'r') as f:
        config = json.load(f)
        if env not in config:
            raise ValueError(f"Environment '{env}' not found in config file")
        return config[env]

# API Endpoints
API_NODE_INIT = "/nodeInit"
API_ADD_NODE = "/controller/addNode"
API_REMOVE_NODE = "/controller/ejectNode"
API_PREPARE_REBALANCE = "/controller/fusion/prepareRebalance"
API_UPLOAD_VOLUMES = "/controller/fusion/uploadMountedVolumes"
API_START_REBALANCE = "/controller/rebalance"

class RebalanceAutomation:
    def __init__(self, base_url: str, username: str, password: str, current_nodes: List[str],
                 new_nodes: List[str], config: dict, dry_run: bool = False, replica_update: bool = False,
                 skip_file_linking: bool = False):
        self.base_url = base_url.rstrip('/')
        self.auth = (username, password)
        self.current_nodes = current_nodes
        self.new_nodes = new_nodes
        self.dry_run = dry_run
        self.config = config
        self.replica_update = replica_update
        self.skip_file_linking = skip_file_linking

        if os.path.exists(REBALANCE_PLAN_FILE): os.remove(REBALANCE_PLAN_FILE)
        if os.path.exists(MANIFEST_OUTPUT_DIR): shutil.rmtree(MANIFEST_OUTPUT_DIR)

        # Create output directory if it doesn't exist
        if not self.dry_run and not os.path.exists(MANIFEST_OUTPUT_DIR):
            os.makedirs(MANIFEST_OUTPUT_DIR)

    def _execute_command(self, cmd: str, explanation: str) -> subprocess.CompletedProcess:
        """Execute a shell command with dry run support."""
        print(f"Executing: {explanation}")
        print(f"Command: {cmd}")

        try:
            result = subprocess.run(
                cmd,
                shell=True,
                check=True,
                capture_output=True,
                text=True
            )
            if result.stdout:
                print("Command output:", result.stdout)
            return result
        except subprocess.CalledProcessError as e:
            print(f"Command failed with exit code {e.returncode}")
            print("Error output:", e.stderr, e.stdout)
            raise

    def get_ejected_nodes(self) -> Set[str]:
        """Calculate nodes that are being removed."""
        current_set = set(self.current_nodes)
        new_set = set(self.new_nodes)
        return current_set - new_set

    def prepare_rebalance(self) -> tuple[str, List[str]]:
        """Prepare the rebalance plan and return the UUID and involved nodes."""
        keep_nodes = ','.join(f'ns_1@{node}' for node in self.new_nodes)
        url = f"{self.base_url}{API_PREPARE_REBALANCE}"

        try:
            response = requests.post(url, auth=self.auth, data={'keepNodes': keep_nodes})
            response.raise_for_status()
            plan_data = response.json()

            # Save the rebalance plan
            with open(REBALANCE_PLAN_FILE, 'w') as f:
                json.dump(plan_data, f, indent=2)

            # Extract involved nodes from the plan, keeping the "ns_1@" prefix
            involved_nodes = list(plan_data['nodes'].keys())
            return plan_data['planUUID'], involved_nodes
        except requests.exceptions.RequestException as e:
            print(f"Failed to prepare rebalance: {str(e)}")
            raise

    def split_manifest(self):
        """Split the rebalance manifest."""
        if not os.path.exists(REBALANCE_PLAN_FILE) and not self.dry_run:
            raise FileNotFoundError(f"Rebalance plan file {REBALANCE_PLAN_FILE} not found")

        cmd = f"{self.config['accelerator_cli']} split-manifest -manifest {REBALANCE_PLAN_FILE} -output-dir {MANIFEST_OUTPUT_DIR} -parts {self.config['manifest_parts']} -base-uri {self.config['base_uri']}"
        self._execute_command(cmd, "Splitting rebalance manifest")

    def sync_manifests(self):
        """Sync manifest parts to destination path."""
        if not self.dry_run and not os.path.exists(MANIFEST_OUTPUT_DIR):
            raise FileNotFoundError(f"Manifest output directory {MANIFEST_OUTPUT_DIR} not found")

        # Determine if source or destination is S3 path
        is_s3_path = self.config['manifest_dest_path'].startswith('s3://')

        if is_s3_path:
            cmd = f"aws s3 sync {MANIFEST_OUTPUT_DIR}/ {self.config['manifest_dest_path']}"
        else:
            # Create destination directory if it doesn't exist
            if not self.dry_run and not os.path.exists(self.config['manifest_dest_path']):
                os.makedirs(self.config['manifest_dest_path'])
            cmd = f"cp -rp {MANIFEST_OUTPUT_DIR}/* {self.config['manifest_dest_path']}"

        self._execute_command(cmd, "Syncing manifest parts to destination")

    def upload_mounted_volumes(self, plan_uuid: str, involved_nodes: List[str]):
        """Upload the mounted volumes configuration."""
        if not plan_uuid:
            raise ValueError("Plan UUID is required")

        url = f"{self.base_url}{API_UPLOAD_VOLUMES}"

        # Prepare the guest volume paths
        guest_paths = [f"/guest{i}" for i in range(1, self.config['manifest_parts'] + 1)]

        # Prepare the nodes configuration
        nodes_config = {
            "nodes": [
                {
                    "name": node,
                    "guestVolumePaths": guest_paths
                }
                for node in involved_nodes
            ]
        }

        try:
            response = requests.post(
                f"{url}?planUUID={plan_uuid}",
                auth=self.auth,
                json=nodes_config
            )
            response.raise_for_status()
            return response.status_code
        except requests.exceptions.RequestException as e:
            print(f"Failed to upload mounted volumes: {str(e)}")
            raise

    def _setup_single_node(self, node: str) -> None:
        """Setup accelerator on a single node."""
        print(f"Setting up node: {node}")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            ssh.connect(node, username=self.config["ssh_username"], password=self.config["ssh_password"], timeout=30)
            cmd = f"{self.config['accelerator_binary']} ns_1@{node}"
            if self.skip_file_linking:
                cmd += " --skip-file-linking"
            print(cmd)

            # Start command
            stdin, stdout, stderr = ssh.exec_command(cmd, timeout=30)
            stdin.close()  # We don't need stdin

            # Use thread-safe printing
            print_lock = threading.Lock()

            # Stream both stdout and stderr in real-time
            while not stdout.channel.exit_status_ready():
                if stdout.channel.recv_ready():
                    stdout_line = stdout.channel.recv(1024).decode('utf-8', errors='ignore')
                    if stdout_line:
                        with print_lock:
                            print(f"[{node}] {stdout_line}", end='')
                if stderr.channel.recv_stderr_ready():
                    stderr_line = stderr.channel.recv_stderr(1024).decode('utf-8', errors='ignore')
                    if stderr_line:
                        with print_lock:
                            print(f"[{node}] {stderr_line}", end='', file=sys.stderr)

            # Get exit status
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise Exception(f"Command failed on node {node} with exit status {exit_status}")

        except Exception as e:
            print(f"Error setting up node {node}: {str(e)}")
            raise
        finally:
            ssh.close()

    def setup_new_nodes(self, nodes_involved):
        """Setup accelerator on new nodes that aren't in current nodes in parallel."""

        nodes_to_setup = nodes_involved

        if not nodes_to_setup:
            print("No new nodes to setup")
            return

        # Run setups in parallel using a thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes_to_setup)) as executor:
            future_to_node = {executor.submit(self._setup_single_node, node): node for node in nodes_to_setup}

            for future in concurrent.futures.as_completed(future_to_node):
                node = future_to_node[future]
                try:
                    future.result()
                    print(f"Successfully set up node: {node}")
                except Exception as e:
                    print(f"Failed to set up node {node}: {str(e)}")
                    raise

    def start_rebalance(self, plan_uuid: str):
        """Start the rebalance process."""
        if not plan_uuid:
            raise ValueError("Plan UUID is required")

        url = f"{self.base_url}{API_START_REBALANCE}"
        # Include all nodes (both current and new) in knownNodes
        all_nodes = set(self.current_nodes) | set(self.new_nodes)
        known_nodes = ','.join(f'ns_1@{node}' for node in all_nodes)
        ejected_nodes = ','.join(f'ns_1@{node}' for node in self.get_ejected_nodes())

        data = {
            'knownNodes': known_nodes,
            'ejectedNodes': ejected_nodes,
            'planUUID': plan_uuid
        }

        try:
            response = requests.post(url, auth=self.auth, data=data)
            response.raise_for_status()
            return response.status_code
        except requests.exceptions.RequestException as e:
            print(f"Failed to start rebalance: {str(e)}")
            raise

    def _configure_node_data_path(self, node: str) -> None:
        """Initialize node with the data path."""
        url = f"http://{node}:8091{API_NODE_INIT}"

        try:
            print(f"Initializing node {node}...")
            response = requests.post(
                url,
                auth=self.auth,
                data={'dataPath': DATA_PATH}
            )
            response.raise_for_status()
            print(f"Successfully initialized node {node}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to initialize node {node}: {str(e)}")
            raise

    def add_nodes(self):
        """Add new nodes to the cluster."""
        new_nodes_set = set(self.new_nodes)
        current_nodes_set = set(self.current_nodes)
        nodes_to_add = new_nodes_set - current_nodes_set

        if not nodes_to_add:
            print("No new nodes to add")
            return

        add_url = f"{self.base_url}{API_ADD_NODE}"

        for node in nodes_to_add:
            try:
                # Then add the node to the cluster
                print(f"Adding node {node} to cluster...")
                data = {
                    'hostname': f'https://{node}:{NODE_PORT}',
                    'user': self.auth[0],
                    'password': self.auth[1]
                }
                response = requests.post(add_url, auth=self.auth, data=data)
                response.raise_for_status()
                print(f"Successfully added node: {node}")
            except Exception as e:
                print(f"Failed to add node {node}: {str(e)}")
                raise


def parse_node_list(nodes_str: str) -> List[str]:
    """Parse comma-separated node list."""
    if not nodes_str:
        raise ValueError("Node list cannot be empty")
    return [node.strip() for node in nodes_str.split(',') if node.strip()]

def validate_nodes(current_nodes: List[str], new_nodes: List[str]):
    """Validate node lists."""
    if not current_nodes:
        raise ValueError("Current nodes list cannot be empty")
    if not new_nodes:
        raise ValueError("New nodes list cannot be empty")

def main():
    parser = argparse.ArgumentParser(description='Automate cluster rebalancing process')
    parser.add_argument('--current-nodes', required=True, help='Comma-separated list of current node IPs')
    parser.add_argument('--new-nodes', required=True, help='Comma-separated list of new node IPs')
    parser.add_argument('--base-url', default='http://localhost:8091', help='Base URL for the cluster')
    parser.add_argument('--username', default='Administrator', help='Cluster admin username')
    parser.add_argument('--password', default='password', help='Cluster admin password')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without making actual changes')
    parser.add_argument('--config', default='config.json', help='Path to JSON configuration file')
    parser.add_argument('--env', choices=['aws', 'local'], required=True, help='Environment to run in (aws or local)')
    parser.add_argument('--sleep-time', type=int, default=0, help='Time to sleep in seconds (default: 0)')
    parser.add_argument('--reb-count', type=int, default=1, help='Rebalance count')
    parser.add_argument('--replica-update', action='store_true', help='Replica Update Rebalance')
    parser.add_argument('--skip-file-linking', action='store_true', help='Skip File Linking')

    args = parser.parse_args()

    try:
        config = load_config(args.config, args.env)
        current_nodes = parse_node_list(args.current_nodes)
        new_nodes = parse_node_list(args.new_nodes)
        validate_nodes(current_nodes, new_nodes)
        sleep_time = args.sleep_time
        reb_count = args.reb_count
        replica_update = args.replica_update
        skip_file_linking = args.skip_file_linking

        global REBALANCE_PLAN_FILE
        REBALANCE_PLAN_FILE = "/root/fusion/reb_plan{}.json".format(reb_count)

        rebalancer = RebalanceAutomation(
            base_url=args.base_url,
            username=args.username,
            password=args.password,
            current_nodes=current_nodes,
            new_nodes=new_nodes,
            config=config,
            dry_run=args.dry_run,
            replica_update=replica_update,
            skip_file_linking=skip_file_linking
        )

        # Step 1: Add/Remove nodes
        print("Adding new nodes to cluster...")
        rebalancer.add_nodes()

        # Step 2: Prepare rebalance and get UUID
        print("Preparing rebalance plan...")
        plan_uuid, involved_nodes = rebalancer.prepare_rebalance()
        print(f"Rebalance plan UUID: {plan_uuid}")

        # Step 3: Split manifest
        print("Splitting manifest...")
        rebalancer.split_manifest()

        # Step 4: Sync manifests
        print("Syncing manifests...")
        rebalancer.sync_manifests()

        # Step 5: Upload mounted volumes
        print("Uploading mounted volumes configuration...")
        rebalancer.upload_mounted_volumes(plan_uuid, involved_nodes)

        print(f"Sleeping for {sleep_time} seconds after PrepareRebalance")
        time.sleep(sleep_time)

        involved_nodes_list = [node.split("@", 1)[1] for node in involved_nodes]
        print("Involved nodes list =", involved_nodes_list)

        # Step 6: Setup new nodes
        print("Setting up new nodes...")
        rebalancer.setup_new_nodes(nodes_involved=involved_nodes_list)

        # Step 7: Start rebalance
        print("Starting rebalance process...")
        rebalancer.start_rebalance(plan_uuid)

        print("Rebalance automation completed successfully!")

    except Exception as e:
        print(f"Error during rebalance automation: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
