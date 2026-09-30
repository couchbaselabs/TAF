import json
import re
import time
from copy import deepcopy

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from cb_tools.cbepctl import Cbepctl
from rebalance_utils.rebalance_util import RebalanceUtil
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionJanitorTermChange(MagmaBaseTest, FusionBase):
    def setUp(self):
        super().setUp()
        self.lease_lifetime_sec = self.input.param("lease_lifetime_sec", 240)
        self.janitor_windows = self.input.param("janitor_windows", 3)
        self.bucket = self.cluster.buckets[0]
        self.node_a = self.cluster.master
        self.node_b = next(node for node in self.cluster.nodes_in_cluster if node.ip != self.node_a.ip)
        self.spare = next(node for node in self.cluster.servers if node not in self.cluster.nodes_in_cluster)
        self.log_store_path = self.fusion_log_store_uri.split("://")[-1]
        self.volume_prefix = f"kv/{self.get_bucket_uuid(self.bucket.name)}"
        self.memcached_logs = "/opt/couchbase/var/lib/couchbase/logs/memcached.log*"

    def run_on_node(self, node, cmd):
        shell = RemoteMachineShellConnection(node)
        output, _ = shell.execute_command(cmd)
        shell.disconnect()
        return "\n".join(output)

    def set_fusion_params(self, node):
        shell = RemoteMachineShellConnection(node)
        cbepctl = Cbepctl(shell)
        params = {
            "magma_fusion_upload_interval": self.fusion_upload_interval,
            "magma_fusion_logstore_fragmentation_threshold": self.logstore_frag_threshold,
            "magma_fusion_max_log_size": self.fusion_max_log_size,
        }
        for key, value in params.items():
            cbepctl.set(self.bucket.name, "flush_param", key, value)
        shell.disconnect()

    def get_otp_id(self, node):
        nodes = self.cluster_util.get_nodes(self.node_a, inactive_added=True, inactive_failed=True)
        return next(otp.id for otp in nodes if otp.ip == node.ip)

    def add_spare(self):
        self.cluster_util.add_node(self.cluster, self.spare, services=["kv"], rebalance=False)

    def rebalance(self, eject_nodes=None):
        nodes = self.cluster_util.get_nodes(self.node_a, inactive_added=True, inactive_failed=True)
        status, _ = ClusterRestAPI(self.node_a).rebalance(
            known_nodes=[otp.id for otp in nodes], eject_nodes=eject_nodes
        )
        if not status or not RebalanceUtil(self.cluster).monitor_rebalance():
            self.fail("Rebalance failed")
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)

    def get_uploaders(self):
        self.get_fusion_uploader_info([self.bucket])
        return deepcopy(self.fusion_vb_uploader_map[self.bucket.name])

    def get_volume_state(self, vbs):
        """
        Returns {vb: {"leases": [manifest log IDs], "logs": [logs on the log store]}}
        """
        cmd = "".join(
            f"echo VB={vb}; "
            f"/opt/couchbase/bin/fusion/metadata_dump "
            f"--uri chronicle://{self.node_a.ip}:8091 "
            f"--volume-id {self.volume_prefix}/kvstore-{vb}; echo; "
            f"ls {self.log_store_path}/{self.volume_prefix}/kvstore-{vb}; "
            for vb in vbs
        )
        state = {}
        vb = None
        for line in self.run_on_node(self.node_a, cmd).splitlines():
            line = line.strip()
            if line.startswith("VB="):
                vb = int(line[3:])
                state[vb] = {"leases": [], "logs": []}
            elif line.startswith("{"):
                state[vb]["leases"] = [lease["checkpoint"]["logID"] for lease in json.loads(line)["leases"]]
            elif line.startswith("log-"):
                state[vb]["logs"].append(line)
        return state

    def grep_node_a_log(self, pattern, since):
        cmd = f"cat {self.memcached_logs} | awk '$1>=\"{since}\"' | grep '({self.bucket.name})' | grep '{pattern}'"
        return set(self.run_on_node(self.node_a, cmd).splitlines())

    def get_failure_count(self):
        stats = self.get_cbstats_all_stats(self.node_a, self.bucket.name)
        return int(stats["ep_fusion_checkpoint_failures"])

    def test_janitor_stale_lease_after_term_change(self):
        """
        MB-74290: a node that regains the Fusion uploader role must not carry
        a lease from its earlier term into the new one

        A records a lease in term 1 and steps down when C takes over. C deletes
        the lease and the manifest it names. When C and then B leave the cluster,
        A becomes the uploader again in the same process. The lease pass must
        not fail on the deleted manifest, and the log store janitor must keep
        running
        """
        bucket_name = self.bucket.name
        fusion_rest = FusionRestAPI(self.node_a)

        for node in self.cluster.nodes_in_cluster:
            self.set_fusion_params(node)
        self.initial_load()
        fusion_rest.sync_log_store()
        self.sleep(40, "Wait for the initial sync")

        initial_failures = self.get_failure_count()
        if initial_failures != 0:
            self.fail(
                f"{initial_failures} checkpoint failures on {self.node_a.ip} "
                f"before any fault was injected, nothing below is meaningful"
            )

        since = self.run_on_node(self.node_a, "date +%Y-%m-%dT%H:%M:%S")
        uploaders_start = self.get_uploaders()
        a_vbs = [vb for vb, info in uploaders_start.items() if info["node"] == self.node_a.ip]

        self.add_spare()
        keep_nodes = [self.get_otp_id(node) for node in self.cluster.nodes_in_cluster]
        status, _ = fusion_rest.prepare_rebalance(keep_nodes, self.lease_lifetime_sec)
        if not status:
            self.fail("prepareRebalance did not take a snapshot lease")
        self.perform_workload(0, 20000, "update")
        fusion_rest.sync_log_store()
        self.sleep(25, "Wait for the lease pass to record the lease")

        tracked = {
            int(vb)
            for line in self.grep_node_a_log("Incrementing lease ref count", since)
            for vb in re.findall(r"kvstore-(\d+)/", line)
        } & set(a_vbs)
        if not tracked:
            self.fail(
                f"{self.node_a.ip} did not record a lease for any vbucket it "
                f"owns, the stale state this test needs cannot exist"
            )

        leased = {
            vb: state["leases"][0] for vb, state in self.get_volume_state(sorted(tracked)).items() if state["leases"]
        }
        if not leased:
            self.fail(f"No lease in the metadata store for the vbuckets {self.node_a.ip} tracked: {sorted(tracked)}")

        self.rebalance()
        self.set_fusion_params(self.spare)
        uploaders_on_spare = self.get_uploaders()
        manifests = {vb: name for vb, name in leased.items() if uploaders_on_spare[vb]["node"] == self.spare.ip}
        if not manifests:
            self.fail(
                f"No vbucket moved from {self.node_a.ip} to {self.spare.ip} "
                f"after the rebalance, leased vbuckets: {sorted(leased)}"
            )

        end_time = time.time() + self.lease_lifetime_sec + 120
        while True:
            self.perform_workload(0, self.num_items, "update")
            fusion_rest.sync_log_store()
            self.sleep(30, "Wait for the new uploader to clean the log store")
            state = self.get_volume_state(list(manifests))
            if all(not state[vb]["leases"] and name not in state[vb]["logs"] for vb, name in manifests.items()):
                break
            if time.time() > end_time:
                self.fail(f"The new uploader did not delete the leased manifests: {manifests}")

        failures_before_return = self.get_failure_count()
        self.rebalance(eject_nodes=[self.get_otp_id(self.spare)])
        self.rebalance(eject_nodes=[self.get_otp_id(self.node_b)])

        uploaders_end = self.get_uploaders()
        returned = [
            vb
            for vb in manifests
            if uploaders_end[vb]["node"] == self.node_a.ip and uploaders_end[vb]["term"] > uploaders_start[vb]["term"]
        ]
        if not returned:
            self.fail(
                f"No vbucket returned to {self.node_a.ip} at a higher term, "
                f"vbuckets that moved to {self.spare.ip}: {sorted(manifests)}"
            )

        for vb in returned:
            revisions = self.run_on_node(
                self.node_a,
                f"cat {self.memcached_logs} | awk '$1>=\"{since}\"' | grep '({bucket_name})' "
                f"| grep -o 'kvstore-{vb}/rev-[0-9]*' | sort -u",
            )
            if len(revisions.split()) != 1:
                self.fail(
                    f"kvstore-{vb} on {self.node_a.ip} was recreated "
                    f"({revisions.split()}), so the stale state no longer exists"
                )

        self.perform_workload(0, 50000, "update")
        for window in range(self.janitor_windows):
            fusion_rest.sync_log_store()
            self.sleep(20, f"Janitor window {window + 1}/{self.janitor_windows}")

        failed = [
            line
            for line in self.grep_node_a_log("LogStore cleanup failed", since)
            if any(f"kvstore-{vb}/" in line for vb in returned)
        ]
        failures = self.get_failure_count() - failures_before_return
        stats = self.get_cbstats_all_stats(self.node_a, bucket_name)
        self.log.info(
            f"vbuckets back on {self.node_a.ip}: {sorted(returned)}, "
            f"checkpoint failures: {failures}, pending delete size: "
            f"{stats['ep_fusion_log_store_pending_delete_size']}"
        )
        if failed or failures:
            self.fail(
                f"MB-74290: {failures} checkpoint failures after {self.node_a.ip} "
                f"became the uploader again. Last failure: "
                f"{sorted(failed)[-1] if failed else None}"
            )
