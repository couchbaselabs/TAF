'''
Created on 30-Aug-2021

@author: riteshagarwal
'''
import subprocess
import json
from BucketLib.bucket import Bucket
import os
import random
import uuid
from BucketLib.BucketOperations import BucketHelper
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from Jython_tasks.task_manager import TaskManager
from bucket_utils.bucket_ready_functions import JavaDocLoaderUtils
from sdk_client3 import SDKClient
from shell_util.remote_connection import RemoteMachineShellConnection
from table_view import TableView
from membase.api.rest_client import RestConnection
from cb_tools.cbstats import Cbstats
from cb_tools.cbepctl import Cbepctl
import time
from custom_exceptions.exception import RebalanceFailedException,\
    ServerUnavailableException
from py_constants.cb_constants.CBServer import CbServer
from threading import Thread
import threading
from capella_utils.dedicated import CapellaUtils as DedicatedUtils
from couchbase_utils.backup_utils.backup_utils import BackupLocation, \
    BackupMgrUtil, CLOUD_PROVIDER_CLASSES, ContinuousBackupUtil
from couchbase_utils.nfs_utils.nfs_utils import NfsUtil
from couchbase_utils.security_utils.credential_store_utils import \
    CredentialStoreUtils
from TestInput import TestInputServer, TestInputSingleton
from gsiLib.gsiHelper import GsiHelper
from cb_server_rest_util.query.query_api import QueryRestAPI
from collections import defaultdict


class OPD:
    # Object-store destinations for cbbackup_test / cont_bkp_test.
    #
    # Unlike the functional suites, a volume run owns its cluster for the whole
    # campaign, so the destinations are fixed rather than uuid-suffixed: a
    # rerun reuses (and re-cleans) the same location instead of leaving a trail
    # of prefixes behind in the shared bucket.
    #
    # localstack is deliberately absent: its cleanup_for_bkrs() creates and
    # drops whole buckets against a local endpoint, which cannot hold the
    # multi-TB datasets these tests generate. Asking for it fails in
    # BackupLocation.build() rather than silently degrading.
    BACKUP_URL_TEMPLATES = {
        "AWS": "s3://test-backup-volume/backups",
        "Azure": "az://test-backup-volume/backups",
        "GCP": "gs://test-backup-volume/backups",
    }
    CONT_BKP_URL_TEMPLATES = {
        "AWS": "s3://test-backup-volume/cont_bkp",
        "Azure": "az://test-backup-volume/cont_bkp",
        "GCP": "gs://test-backup-volume/cont_bkp",
    }
    NFS_BACKUP_ARCHIVE = f"{NfsUtil.MOUNT_POINT}/test_volume_backups"
    NFS_CONT_BKP_LOCATION = f"{NfsUtil.MOUNT_POINT}/test_volume"

    # Plan stability modes, in the order the rotation thread walks them.
    # The run starts on prepared_only and wraps around from the last entry.
    PLAN_STABILITY_MODES = ["prepared_only", "ad_hoc", "ad_hoc_read_only",
                            "off"]
    # Defaults live on the class because OPD is a mixin whose __init__ is a
    # no-op -- the sub-class' setUp never initialises these.
    plan_stability_thread = None
    stop_plan_stability = False

    # AUS (Automatic Update Statistics) runs on a fixed daily window rather
    # than a computed one, so a rerun schedules the same window every time.
    AUS_START_TIME = "18:00"
    AUS_END_TIME = "21:00"
    AUS_TIMEZONE = "Asia/Calcutta"
    AUS_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
                "Saturday", "Sunday"]
    AWR_INTERVAL = "60m"
    AWR_THRESHOLD = "30s"

    def __init__(self):
        pass

    def threads_calculation(self):
        self.process_concurrency = self.input.param("pc", self.process_concurrency)
        self.doc_loading_tm = TaskManager(self.process_concurrency)

    def get_memory_footprint(self):
        out = subprocess.Popen(['ps', 'v', '-p', str(os.getpid())],stdout=subprocess.PIPE).communicate()[0].split(b'\n')
        vsz_index = out[0].split().index(b'RSS')
        mem = float(out[1].split()[vsz_index]) / 1024
        self.PrintStep("RAM FootPrint: %s" % str(mem))
        return mem

    def create_encryption_at_rest_key(self, cluster):
        """Create a cluster-managed AES-256 key for bucket encryption at rest.

        Called before the buckets exist, so the key is registered against the
        wildcard `bucket-encryption-*` usage rather than a named bucket --
        every bucket the run creates has to be able to use it.

        The id is stashed on the cluster object rather than on self because
        each cluster has its own key namespace (the XDCR remote needs a key of
        its own); create_required_buckets() reads it back per cluster.
        """
        if not self.enable_encryption_at_rest:
            return None

        # An id passed on the command line refers to a key that already exists
        # on the cluster, so there is nothing to create.
        if self.encryption_at_rest_id:
            cluster.encryption_at_rest_id = self.encryption_at_rest_id
            self.log.info("Using pre-existing encryption at rest key ID=%s"
                          % self.encryption_at_rest_id)
            return self.encryption_at_rest_id

        rest = RestConnection(cluster.master)
        bucket_helper = BucketHelper(cluster.master)
        params = bucket_helper.create_secret_params(
            secret_type="cb-server-managed-aes-key-256",
            name="VolumeTestBucketEncryptionKey",
            usage=["bucket-encryption"],
            autoRotation=True,
            rotationIntervalInSeconds=self.rotationIntervalInSeconds,
            port=None)
        self.log.info("Sending request to create secret with params: %s" % params)
        status, response = rest.create_secret(params)
        self.log.info("Secret creation status=%s, response=%s"
                      % (status, response))
        if not status:
            self.fail("Encryption at rest key creation failed on %s: %s"
                      % (cluster.master.ip, response))

        secret_id = json.loads(response).get("id")
        if secret_id is None:
            self.fail("Encryption at rest key created on %s but no id was "
                      "returned: %s" % (cluster.master.ip, response))
        cluster.encryption_at_rest_id = secret_id
        # Only keys this run created are torn down, so record them as they are
        # made (see _delete_bucket_encryption_secrets).
        if getattr(self, "created_encryption_keys", None) is None:
            self.created_encryption_keys = list()
        self.created_encryption_keys.append((cluster, secret_id))
        self.log.info("Created secret ID=%s on %s"
                      % (secret_id, cluster.master.ip))
        return secret_id

    def create_required_buckets(self, cluster, sdk_init=True):
        self.log.info("Get the available memory quota")
        rest = RestConnection(cluster.master)
        self.info = rest.get_nodes_self()

        # threshold_memory_vagrant = 100
        kv_memory = self.info.memoryQuota - 100

        # Creating buckets for data loading purpose
        self.log.info("Create CB buckets")
        self.bucket_expiry = self.input.param("bucket_expiry", 0)
        ramQuota = self.input.param("ramQuota", kv_memory)
        buckets = ["GleamBookUsers"]*self.num_buckets
        # The server takes "membase" as an alias of "couchbase", but
        # BucketHelper.create_bucket() sends the magma-only params (history
        # retention, fragmentation, block sizes) only for Bucket.Type.MEMBASE
        # ("couchbase"), silently dropping them for "membase"
        bucket_type = [Bucket.Type.MEMBASE if b_type == "membase" else b_type
                       for b_type in self.bucket_type.split(';')] \
            * self.num_buckets
        compression_mode = self.compression_mode.split(';')*self.num_buckets
        self.bucket_eviction_policy = self.bucket_eviction_policy

        # Encryption at rest: the key is created on this cluster just before
        # this call (Hospital.create_encryption_at_rest_key), so every bucket
        # created below references the same key.
        encryption_key_id = getattr(cluster, "encryption_at_rest_id", None)

        for i in range(self.num_buckets):
            num_vb = CbServer.total_vbuckets
            if bucket_type[i] in ("membase", "couchbase") \
                    and self.bucket_storage == "magma":
                num_vb = CbServer.magma_default_vbuckets
            bucket_params = {
                Bucket.name: buckets[i] + str(i),
                Bucket.ramQuotaMB: ramQuota//self.num_buckets,
                Bucket.numVBuckets: num_vb,
                Bucket.maxTTL: self.bucket_expiry,
                Bucket.replicaNumber: self.num_replicas,
                Bucket.storageBackend: self.bucket_storage,
                Bucket.evictionPolicy: self.bucket_eviction_policy,
                Bucket.bucketType: bucket_type[i],
                Bucket.flushEnabled: Bucket.FlushBucket.ENABLED,
                Bucket.compressionMode: compression_mode[i],
                Bucket.fragmentationPercentage: self.fragmentation,
                Bucket.width: self.bucket_width,
                Bucket.weight: self.bucket_weight,
                Bucket.historyRetentionBytes: self.bucket_history_retention_bytes,
                Bucket.historyRetentionSeconds: self.bucket_history_retention_seconds}
            # Ephemeral buckets have nothing on disk to encrypt, so the server
            # rejects the key params for them.
            if encryption_key_id and bucket_type[i] != Bucket.Type.EPHEMERAL:
                bucket_params.update({
                    Bucket.encryptionAtRestKeyId: encryption_key_id,
                    Bucket.encryptionAtRestDekRotationInterval:
                        self.encryptionAtRestDekRotationInterval,
                    Bucket.encryptionAtRestDekLifetime:
                        self.encryption_at_rest_dek_lifetime})
            bucket = Bucket(bucket_params)
            bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
            if bucket.loadDefn.get("name"):
                bucket.name = bucket.loadDefn.get("name")
            self.bucket_util.create_bucket(cluster, bucket)

        # if sdk_init:
        #     # cluster.sdk_client_pool = SDKClientPool()
            num_clients = self.input.param("clients_per_db",
                                           min(5, bucket.loadDefn.get("collections")))
            SiriusCouchbaseLoader.create_clients_in_pool(
                        cluster.master, cluster.master.rest_username,
                        cluster.master.rest_password,
                        bucket.name, req_clients=num_clients)
        self.create_required_collections(cluster)
        self.create_sdk_client_pool(cluster, cluster.buckets, 1)

    def create_required_collections(self, cluster, buckets=None):
        buckets = buckets or cluster.buckets
        self.scope_name = self.input.param("scope_name", "_default")

        def create_collections(bucket):
            node = cluster.master
            for scope in bucket.scopes.keys():
                if scope == CbServer.system_scope:
                    continue
                if bucket.loadDefn.get("collections") > 0:
                    self.collection_prefix = self.input.param("collection_prefix",
                                                              "VolumeCollection")

                    for i in range(bucket.loadDefn.get("collections")):
                        collection_name = self.collection_prefix + str(i)
                        self.bucket_util.create_collection(node,
                                                           bucket,
                                                           scope,
                                                           {"name": collection_name})
                        self.sleep(0.1)

                collections = bucket.scopes[scope].collections.keys()
                self.log.debug("Collections list == {}".format(collections))

        for bucket in buckets:
            if bucket.loadDefn.get("scopes") > 1:
                self.scope_prefix = self.input.param("scope_prefix",
                                                     "VolumeScope")
                node = cluster.master
                for i in range(bucket.loadDefn.get("scopes")):
                    scope_name = self.scope_prefix + str(i)
                    self.log.info("Creating scope: %s"
                                  % (scope_name))
                    self.bucket_util.create_scope(node,
                                                  bucket,
                                                  {"name": scope_name})
                    self.sleep(0.1)
        threads = []
        for bucket in buckets:
            th = Thread(
                    target=create_collections,
                    name="{}_create_collection".format(bucket.name),
                    args=(bucket,))
            threads.append(th)
            th.start()
        for thread in threads:
            thread.join()

    def create_sdk_client_pool(self, cluster, buckets, req_clients_per_bucket=1):
        for bucket in buckets:
            # self.log.info("Using SDK endpoint %s" % cluster.master.ip)
            # SDKClientPool.create_clients_in_pool(
            #     self.cluster.master, self.cluster.master.rest_username,
            #     self.cluster.master.rest_password, buckets, req_clients_per_bucket)
            # cluster.sdk_client_pool.create_clients(
            #     bucket.name, server, req_clients_per_bucket)
            # bucket.clients = cluster.sdk_client_pool.clients.get(bucket.name).get("idle_clients")
            bucket.clients = [SDKClient(cluster, bucket) for _ in range(req_clients_per_bucket)]
        self.sleep(1, "Wait for SDK client pool to warmup")

    def stop_purger(self, tombstone_purge_age=60):
        """
        1. Disable ts purger
        2. Create fts indexes (to create metakv, ns_config entries)
        3. Delete fts indexes
        4. Grep ns_config for '_deleted' to get total deleted keys count
        5. enable ts purger and age = 1 mins
        6. Sleep for 2 minutes
        7. Grep for debug.log and check for latest tombstones purged count
        8. Validate step4 count matches step 7 count for all nodes
        """
        self.rest.update_tombstone_purge_age_for_removal(tombstone_purge_age)
        self.rest.disable_tombstone_purger()

    def get_bucket_dgm(self, cluster, bucket):
        self.rest_client = BucketHelper(cluster.master)
        dgm = self.rest_client.fetch_bucket_stats(
            bucket.name)["op"]["samples"]["vb_active_resident_items_ratio"][-1]
        self.log.info("Active Resident Threshold of {0} is {1}".format(
            bucket.name, dgm))
        return dgm

    def _induce_error(self, error_condition, nodes=[]):
        nodes = nodes or [self.cluster.master]
        for node in nodes:
            if error_condition == "stop_server":
                self.cluster_util.stop_server(node)
            elif error_condition == "enable_firewall":
                self.cluster_util.start_firewall_on_node(node)
            elif error_condition == "kill_memcached":
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()
                shell.disconnect()
            elif error_condition == "kill_indexer":
                shell = RemoteMachineShellConnection(node)
                shell.kill_process("indexer", "indexer")
                shell.disconnect()
            elif error_condition == "reboot_server":
                shell = RemoteMachineShellConnection(node)
                shell.reboot_node()
            elif error_condition == "kill_erlang":
                shell = RemoteMachineShellConnection(node)
                shell.kill_erlang()
                shell.disconnect()
            else:
                self.fail("Invalid error induce option")

    def _recover_from_error(self, error_condition):
        for node in self.cluster.nodes_in_cluster:
            if error_condition == "stop_server" or error_condition == "kill_erlang":
                self.cluster_util.start_server(node)
            elif error_condition == "enable_firewall":
                self.cluster_util.stop_firewall_on_node(node)
        if error_condition == "kill_memcached":
            for node in self.cluster.kv_nodes + [self.cluster.master]:
                self.check_warmup_complete(node)
                result = self.cluster_util.wait_for_ns_servers_or_assert([node],
                                                                         wait_time=1200)
                self.assertTrue(result, "Server warmup failed")
        elif error_condition == "kill_indexer":
            self.recover_indexer()

    def rebalance(self, nodes_in=0, nodes_out=0, services=[],
                  retry_get_process_num=3000):
        self.servs_in = list()
        self.nodes_cluster = self.cluster.nodes_in_cluster[:]
        self.nodes_cluster.remove(self.cluster.master)
        self.servs_out = list()
        services = services or ["kv"]
        print("KV nodes in cluster: %s" % [server.ip for server in self.cluster.kv_nodes])
        print("CBAS nodes in cluster: %s" % [server.ip for server in self.cluster.cbas_nodes])
        print("INDEX nodes in cluster: %s" % [server.ip for server in self.cluster.index_nodes])
        print("FTS nodes in cluster: %s" % [server.ip for server in self.cluster.fts_nodes])
        print("QUERY nodes in cluster: %s" % [server.ip for server in self.cluster.query_nodes])
        print("EVENTING nodes in cluster: %s" % [server.ip for server in self.cluster.eventing_nodes])
        print("AVAILABLE nodes for cluster: %s" % [server.ip for server in self.available_servers])
        if nodes_out:
            if "cbas" in services:
                servers = random.sample(self.cluster.cbas_nodes, nodes_out)
                self.servs_out.extend(servers)
            if "index" in services:
                servers = random.sample(self.cluster.index_nodes, nodes_out)
                self.servs_out.extend(servers)
            if "fts" in services:
                servers = random.sample(self.cluster.fts_nodes, nodes_out)
                self.servs_out.extend(servers)
            if "query" in services:
                servers = random.sample(self.cluster.query_nodes, nodes_out)
                self.servs_out.extend(servers)
            if "eventing" in services:
                servers = random.sample(self.cluster.eventing_nodes, nodes_out)
                self.servs_out.extend(servers)
            if "kv" in services:
                nodes = [node for node in self.cluster.kv_nodes if node.ip != self.cluster.master.ip]
                servers = random.sample(nodes, nodes_out)
                self.servs_out.extend(servers)

        if nodes_in:
            candidates = [s for s in self.available_servers
                          if s.ip != self.cluster.master.ip]
            if "cbas" in services:
                servers = random.sample(candidates, nodes_in)
                self.servs_in.extend(servers)
                self.available_servers = [servs for servs in self.available_servers
                                          if servs not in servers]
                candidates = [s for s in self.available_servers
                              if s.ip != self.cluster.master.ip]
            if "index" in services:
                servers = random.sample(candidates, nodes_in)
                self.servs_in.extend(servers)
                self.available_servers = [servs for servs in self.available_servers
                                          if servs not in servers]
                candidates = [s for s in self.available_servers
                              if s.ip != self.cluster.master.ip]
            if "fts" in services:
                servers = random.sample(candidates, nodes_in)
                self.servs_in.extend(servers)
                self.available_servers = [servs for servs in self.available_servers
                                          if servs not in servers]
                candidates = [s for s in self.available_servers
                              if s.ip != self.cluster.master.ip]
            if "query" in services:
                servers = random.sample(candidates, nodes_in)
                self.servs_in.extend(servers)
                self.available_servers = [servs for servs in self.available_servers
                                          if servs not in servers]
                candidates = [s for s in self.available_servers
                              if s.ip != self.cluster.master.ip]
            if "eventing" in services:
                servers = random.sample(candidates, nodes_in)
                self.servs_in.extend(servers)
                self.available_servers = [servs for servs in self.available_servers
                                          if servs not in servers]
                candidates = [s for s in self.available_servers
                              if s.ip != self.cluster.master.ip]
            if "kv" in services:
                servers = random.sample(candidates, nodes_in)
                self.servs_in.extend(servers)
                self.available_servers = [servs for servs in self.available_servers
                                          if servs not in servers]
        if "index" in services:
            services=["index,n1ql"] * len(services)

        print("Servers coming in : %s with services: %s" % ([server.ip for server in self.servs_in], services))
        print("Servers going out : %s" % ([server.ip for server in self.servs_out]))
        self.available_servers.extend(self.servs_out)
        print("NEW AVAILABLE nodes for cluster: %s" % ([server.ip for server in self.available_servers]))
        if nodes_in == nodes_out:
            self.vbucket_check = False

        rebalance_task = self.task.async_rebalance(
            self.cluster, self.servs_in, self.servs_out,
            services=services,
            check_vbucket_shuffling=self.vbucket_check,
            retry_get_process_num=retry_get_process_num,
            validate_bucket_ranking=False)

        return rebalance_task

    def get_gdb(self):
        for node in self.cluster.kv_nodes:
            gdb_shell = RemoteMachineShellConnection(node)
            gdb_out = gdb_shell.execute_command('gdb -p `(pidof memcached)` -ex "thread apply all bt" -ex detach -ex quit')[0]
            print (node.ip)
            print (gdb_out)
            gdb_shell.disconnect()

    def data_validation(self, cluster, skip_default=True):
        pc = min(self.process_concurrency, 20)
        if self._data_validation:
            self.log.info("Validating Active/Replica Docs")
            cmd = dict()
            self.ops_rate = self.input.param("ops_rate", 2000)
            self.loader_map = dict()
            for bucket in cluster.buckets:
                for scope in bucket.scopes.keys():
                    if scope == CbServer.system_scope:
                            continue
                    for i, collection in enumerate(bucket.scopes[scope].collections.keys()):
                        workloads = bucket.loadDefn.get("collections_defn", [bucket.loadDefn])
                        valType = workloads[i % len(workloads)]["valType"]
                        if collection == "_default" and scope == "_default" and skip_default:
                            continue
                        for op_type in bucket.loadDefn.get("load_type"):
                            cmd.update({"deleted": False})
                            hm = dict()
                            if op_type == "create":
                                hm.putAll({"read_s": bucket.create_start,
                                           "read_e": bucket.create_end})
                            elif op_type == "update":
                                hm.putAll({"read_s": bucket.update_start,
                                           "read_e": bucket.update_end})
                            elif op_type == "delete":
                                hm.putAll({"read_s": bucket.delete_start,
                                           "read_e": bucket.delete_end})
                                cmd.update({"deleted": True})
                            elif op_type == "expiry":
                                hm.putAll({"read_s": bucket.expire_start,
                                           "read_e": bucket.expire_end})
                                cmd.update({"deleted": True})
                            else:
                                continue
                            ws = dict({"keyPrefix": self.key,
                                                  "keySize": self.key_size,
                                                  "docSize": bucket.loadDefn.get("doc_size"),
                                                  "cr": 0,
                                                  "rd": 100,
                                                  "up": 0,
                                                  "dl": 0,
                                                  "ex": 0,
                                                  "workers": pc,
                                                  "ops": bucket.original_ops,
                                                  "loadType": None,
                                                  "keyType": self.key_type,
                                                  "valueType": valType,
                                                  "validate": True,
                                                  "gtm": False,
                                                  "deleted": False,
                                                  "mutated": 0})
                            ws.update(hm)
                            self.loader_map.update({bucket.name+scope+collection+op_type: ws})

            tasks = list()
            i = pc
            while i > 0:
                for bucket in cluster.buckets:
                    for scope in bucket.scopes.keys():
                        if scope == CbServer.system_scope:
                            continue
                        for collection in bucket.scopes[scope].collections.keys():
                            if collection == "_default" and scope == "_default" and skip_default:
                                continue
                            for op_type in bucket.loadDefn.get("load_type"):
                                if op_type not in ["create", "update", "delete", "expiry"]:
                                    continue
                                # # client = NewSDKClient(master, bucket.name, scope, collection)
                                # # client.initialiseSDK()
                                # # self.sleep(1)
                                # taskName = "Validate_%s_%s_%s_%s_%s_%s" % (bucket.name, scope, collection, op_type, str(i), time.time())
                                # task = WorkLoadGenerate(taskName, self.loader_map[bucket.name+scope+collection+op_type],
                                #                         cluster.sdk_client_pool, "NONE",
                                #                         self.maxttl, self.time_unit,
                                #                         self.track_failures, 0)
                                # task.set_collection_for_load(bucket.name, scope, collection)
                                # tasks.append(task)
                                # self.doc_loading_tm.submit(task)
                                i -= 1
            return tasks

    def print_crud_stats(self, buckets):
        self.table = TableView(self.log.info)
        self.table.set_headers(["Initial Items",
                                "Current Items",
                                "Items Updated",
                                "Items Created",
                                "Items Deleted",
                                "Items Expired"])
        for bucket in buckets:
            self.table.add_row([
                str(bucket.initial_items),
                str(bucket.final_items),
                str(abs(bucket.update_start)) + "-" + str(abs(bucket.update_end)),
                str(abs(bucket.create_start)) + "-" + str(abs(bucket.create_end)),
                str(abs(bucket.delete_start)) + "-" + str(abs(bucket.delete_end)),
                str(abs(bucket.expire_start)) + "-" + str(abs(bucket.expire_end))
                ])
        self.table.display("Docs statistics")

    def get_magma_disk_usage(self, bucket=None):
        if bucket is None:
            bucket = self.bucket
        servers = self.cluster.nodes_in_cluster
        kvstore = 0
        wal = 0
        keyTree = 0
        seqTree = 0
        data_files = 0

        for server in servers:
            shell = RemoteMachineShellConnection(server)
            bucket_path = os.path.join(RestConnection(server).get_data_path(),bucket.uuid)
            kvstore += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(bucket_path, "magma.*/kv*"))[0][0].split('\n')[0])
            wal += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(bucket_path, "magma.*/wal"))[0][0].split('\n')[0])
            keyTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(bucket_path, "magma.*/kv*/rev*/key*"))[0][0].split('\n')[0])
            seqTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(bucket_path, "magma.*/kv*/rev*/seq*"))[0][0].split('\n')[0])

            cmd = 'find ' + bucket_path + '/magma*/ -maxdepth 1 -type d \
            -print0 | while read -d "" -r dir; do files=("$dir"/*/*/*); \
            printf "%d,%s\n" "${#files[@]}" "$dir"; done'
            data_files = shell.execute_command(cmd)[0]
            for files in data_files:
                if "kvstore" in files and int(files.split(",")[0]) >= 300:
                    self.log.warn("Number of files in {}--{} is {}".format(
                        server.ip, files.split(",")[1].rstrip(), files.split(",")[0]))
            shell.disconnect()
        self.log.debug("Total Disk usage for kvstore is {}MB".format(kvstore))
        self.log.debug("Total Disk usage for wal is {}MB".format(wal))
        self.log.debug("Total Disk usage for keyTree is {}MB".format(keyTree))
        self.log.debug("Total Disk usage for seqTree is {}MB".format(seqTree))
        return kvstore, wal, keyTree, seqTree

    def print_stats_loop(self, cluster, step=300):
        while not self.stop_run:
            try:
                self.print_stats(cluster)
            except:
                pass
            time.sleep(step)

    def print_stats(self, cluster):
        self.bucket_util.print_bucket_stats(cluster)
        self.cluster_util.print_cluster_stats(cluster)
        # File-driven vector loaders (siftBigANN / MSMARCO) don't go through
        # generate_docs, so the crud-stat attrs (initial_items, *_start/_end)
        # aren't populated at initial_setup -> skip the crud stats table.
        if self.val_type not in ("siftBigANN", "MSMARCOEmbeddingProduct",
                                  "MSMARCOSiftEmbeddingProduct"):
            self.print_crud_stats(cluster.buckets)
        for bucket in cluster.buckets:
            self.get_bucket_dgm(cluster, bucket)
            if bucket.storageBackend == Bucket.StorageBackend.magma and \
                cluster.type == "default":
                    self.get_magma_disk_usage(bucket)
                    # self.check_fragmentation_using_magma_stats(bucket)
                    self.check_fragmentation_using_kv_stats(bucket)

    def PrintStep(self, msg=None):
        print("\n")
        print("\t" + "#" * 60)
        print("\t" + "#")
        print("\t" + "#  %s" % msg)
        print("\t" + "#")
        print("\t" + "#" * 60)
        print("\n")

    def check_fragmentation_using_kv_stats(self, bucket, servers=None):
        result = dict()
        if servers is None:
            servers = self.cluster.kv_nodes + [self.cluster.master]
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            frag_val = self.bucket_util.get_fragmentation_kv(
                self.cluster, bucket, server)
            self.log.debug("Current Fragmentation for node {} is {} \
            ".format(server.ip, frag_val))
            result.update({server.ip: frag_val})
        self.log.info("KV stats fragmentation values {}".format(result))

    def dump_magma_stats(self, server, bucket, shard, kvstore):
        if bucket.storageBackend != Bucket.StorageBackend.magma \
            or self.cluster.type != "default":
            return
        shell = RemoteMachineShellConnection(server)
        data_path = RestConnection(server).get_data_path()
        while not self.stop_stats:
            for bucket in self.cluster.buckets:
                self.log.info(self.get_magma_stats(bucket, server)[server.ip]["rw_0:magma"])
                self.dump_seq_index(shell, data_path, bucket.uuid, shard, kvstore)
            self.sleep(600)
        shell.disconnect()

    def dump_seq_index(self, shell, data_path, bucket, shard, kvstore):
        magma_path = os.path.join(data_path, bucket, "magma.{}")
        magma = magma_path.format(shard)
        cmd = '/opt/couchbase/bin/magma_dump {}'.format(magma)
        cmd += ' --kvstore {} --tree seq'.format(kvstore)
        result = shell.execute_command(cmd)[0]
        self.log.info("Seq Tree for {}:{}:{}:{}: \n{}".format(shell.ip, bucket, shard, kvstore, result))

    def check_fragmentation_using_magma_stats(self, bucket, servers=None):
        result = dict()
        stats = list()
        if servers is None:
            servers = self.cluster.kv_nodes + [self.cluster.master]
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            fragmentation_values = list()
            shell = RemoteMachineShellConnection(server)
            output = shell.execute_command(
                    "lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'"
                    )[0][0].split('\n')[0]
            shell.disconnect()
            self.log.debug("machine: {} - core(s): {}".format(server.ip,
                                                              output))
            for i in range(min(int(output), 64)):
                grep_field = "rw_{}:magma".format(i)
                _res = self.get_magma_stats(bucket, server)
                fragmentation_values.append(json.loads(_res[server.ip][grep_field])["Fragmentation"])
                stats.append(_res)
            result.update({server.ip: fragmentation_values})
        res = list()
        for value in result.values():
            res.append(max(value))
        if max(res) < float(self.fragmentation)/100:
            self.log.info("magma stats fragmentation result {} \
            ".format(result))
            return True
        self.log.info("magma stats fragmentation result {} \
        ".format(result))
        return False

    def get_magma_stats(self, bucket, server=None):
        magma_stats_for_all_servers = dict()
        cbstat_obj = Cbstats(server)
        result = cbstat_obj.magma_stats(bucket.name)
        cbstat_obj.disconnect()
        magma_stats_for_all_servers[server.ip] = result
        return magma_stats_for_all_servers

    def pause_rebalance(self):
        rest = RestConnection(self.cluster.master)
        i = 1
        self.sleep(10, "Let the rebalance begin!")
        expected_progress = 20
        while expected_progress < 100:
            expected_progress = 20 * i
            reached = self.cluster_util.rebalance_reached(
                self.cluster, expected_progress)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%"
                            .format(expected_progress))
            if not self.cluster_util.is_cluster_rebalanced(rest):
                self.log.info("Stop the rebalance")
                stopped = rest.stop_rebalance(wait_timeout=self.wait_timeout / 3)
                self.assertTrue(stopped, msg="Unable to stop rebalance")
                rebalance_task = self.task.async_rebalance(self.cluster,[], [],
                                                           retry_get_process_num=3000)
                self.sleep(10, "Rebalance % ={}. Let the rebalance begin!".
                           format(expected_progress))
            i += 1
        return rebalance_task

    def abort_rebalance(self, rebalance, error_type="kill_memcached", nodes=None):
        self.sleep(30, "Let the rebalance begin!")
        nodes = nodes or self.cluster.nodes_in_cluster
        rest = RestConnection(self.cluster.master)
        i = 1
        expected_progress = 20
        rebalance_task = rebalance
        while expected_progress < 80:
            expected_progress = 20 * i
            reached = self.cluster_util.rebalance_reached(
                self.cluster, expected_progress, wait_step=10, num_retry=3600)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%"
                            .format(expected_progress))

            if not self.cluster_util.is_cluster_rebalanced(rest):
                self.log.info("Abort rebalance")
                self._induce_error(error_type, nodes)
                result = self.check_coredump_exist(nodes)
                if result:
                    self.task_manager.abort_all_tasks()
                    self.doc_loading_tm.abortAllTasks()
                    self.assertFalse(
                        result,
                        "CRASH | CRITICAL | WARN messages found in cb_logs")
                self.sleep(60, "Sleep after error introduction")
                self._recover_from_error(error_type)
                result = self.check_coredump_exist(nodes)
                if result:
                    self.task_manager.abort_all_tasks()
                    self.doc_loading_tm.abortAllTasks()
                    self.assertFalse(
                        result,
                        "CRASH | CRITICAL | WARN messages found in cb_logs")
                try:
                    self.task_manager.get_task_result(rebalance_task)
                except RebalanceFailedException:
                    pass
                if rebalance_task.result:
                    self.log.error("Rebalance passed/finished which is not expected")
                    self.log.info("Rebalance % expected but rebalance finished = {}".
                                  format(expected_progress))
                    return None
                else:
                    self.log.info("Restarting Rebalance after killing at {}".
                                  format(expected_progress))
                    rebalance_task = self.task.async_rebalance(
                        self.cluster, [], self.servs_out,
                        retry_get_process_num=3000)
                    self.sleep(120, "Let the rebalance begin after abort")
                    self.log.info("Rebalance % = {}".
                                  format(rest._rebalance_progress()))
            i += 1
        return rebalance_task

    def crash_indexer(self, nodes=None, num_kills=1, graceful=False):
        self.crash_count = 0
        if not nodes:
            nodes = self.cluster.index_nodes

        while self.crash_count < self.crashes:
            sleep = random.randint(60, 120)
            self.sleep(sleep,
                       "Iteration:{} waiting to kill memc on all nodes".
                       format(self.crash_count))
            self.kill_services(num_kills=num_kills,
                               graceful=graceful, wait=True,
                               services=["kv"])
            self.check_index_pending_mutations(self.cluster)
            self.kill_services(nodes, num_kills=num_kills,
                               graceful=graceful, wait=True,
                               services=["indexer"])
            self.recover_indexer()
            self.check_index_pending_mutations(self.cluster)
            self.crash_count += 1
        self.sleep(300)

    def recover_indexer(self):
        for bucket in self.cluster.buckets:
            d = defaultdict(list)
            for key, val in bucket.indexes.items():
                _, _, _, _, c = val
                d[c].append(key)
            rest = GsiHelper(self.cluster.master, self.log)
            status = False
            for collection in sorted(d.keys()):
                for index_name in sorted(d.get(collection)):
                    status = rest.polling_create_index_status(
                        bucket, index_name, 1200, fail_on_warmup=False)
                    print("index: {}, status: {}".format(index_name, status))
                    if status is True:
                        self.log.info("2i index is ready: {}".format(index_name))

    def crash_memcached(self, nodes=None, num_kills=1, graceful=False):
        self.stop_crash = False
        self.crash_count = 0
        if not nodes:
            nodes = self.cluster.kv_nodes

        while not self.stop_crash:
            sleep = random.randint(60, 120)
            self.sleep(sleep,
                       "Iteration:{} waiting to kill memc on all nodes".
                       format(self.crash_count))
            self.kill_services(nodes, num_kills=num_kills,
                               graceful=graceful, wait=True)
            self.crash_count += 1
            if self.crash_count > self.crashes:
                self.stop_crash = True
        self.sleep(300)

    def kill_services(self, servers=None, num_kills=1,
                      graceful=False, wait=True, services=["kv"]):
        """
        Kill the given `services` on each of `servers`.

        Despite the historical name, this is not memcached-only: "kv" kills
        memcached and "indexer" kills the indexer process, and `graceful`
        restarts couchbase-server outright.
        """
        if not servers:
            servers = self.cluster.kv_nodes

        for server in servers:
            for _ in range(num_kills):
                if num_kills > 1:
                    self.sleep(2, "Sleep for 2 seconds b/w cont memc kill on same node.")
                shell = RemoteMachineShellConnection(server)
                if graceful:
                    shell.restart_couchbase()
                if "kv" in services:
                    self.log.info("Killing memcached on %s" % server.ip)
                    shell.kill_memcached()
                if "indexer" in services:
                    self.log.info("Killing indexer on %s" % server.ip)
                    shell.kill_process("indexer", "indexer")
                shell.disconnect()
            self.sleep(5, "Sleep for 5 seconds before killing memc on next node.")

        result = self.check_coredump_exist(servers)
        if result:
            self.stop_crash = True
            self.task_manager.abort_all_tasks()
            self.doc_loading_tm.abortAllTasks()
            self.assertFalse(
                result,
                "CRASH | CRITICAL | WARN messages found in cb_logs")

        if wait and "kv" in services:
            for server in servers:
                self.check_warmup_complete(server)

    def check_warmup_complete(self, server):
        for bucket in self.cluster.buckets:
            start_time = time.time()
            result = self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[server],
                wait_time=self.wait_timeout * 20)
            if not result:
                self.stop_crash = True
                self.task_manager.abort_all_tasks()
                self.doc_loading_tm.abortAllTasks()
                self.assertTrue(result, "Warm-up failed in %s seconds"
                                % (self.wait_timeout * 20))
            else:
                self.log.info("Bucket:%s warm-up completed in %s." %
                              (bucket.name, str(time.time() - start_time)))

    def restart_query_load(self, cluster, num=10):
        self.log.info("Changing query load by: {}".format(num))
        for ql in self.ql:
            ql.stop_query_load()
        for ql in self.ftsQL:
            ql.stop_query_load()
        for ql in self.cbasQL:
            ql.stop_query_load()
        self.sleep(10)
        for bucket in cluster.buckets:
            services = self.input.param("services", "data")
            if (cluster.index_nodes or "index" in services) and bucket.loadDefn.get("2iQPS", 0) > 0:
                bucket.loadDefn["2iQPS"] = bucket.loadDefn["2iQPS"] + num
                ql = [ql for ql in self.ql if ql.bucket == bucket][0]
                ql.start_query_load()
            if (cluster.fts_nodes or "search" in services) and bucket.loadDefn.get("ftsQPS", 0) > 0:
                bucket.loadDefn["ftsQPS"] = bucket.loadDefn["ftsQPS"] + num
                ql = [ql for ql in self.ftsQL if ql.bucket == bucket][0]
                ql.start_query_load()
            if (cluster.cbas_nodes or "analytics" in services) and bucket.loadDefn.get("cbasQPS", 0) > 0:
                bucket.loadDefn["cbasQPS"] = bucket.loadDefn["cbasQPS"] + num
                ql = [ql for ql in self.cbasQL if ql.bucket == bucket][0]
                ql.start_query_load()

    # ------------------------------------------------------------------
    # Workload pause / resume
    #
    # A few steps -- flush + restore above all -- need the dataset to hold
    # still: an item count taken while the mutation thread is writing is a
    # moving target, and every query issued against a bucket that has just
    # been emptied is a spurious failure that buries the real ones.
    #
    # These helpers *park* both workloads rather than tearing them down, so
    # the run resumes with the same loader ranges, the same query-load
    # objects and the same accumulated stats. The data side is a two-event
    # handshake with one owner each: the controller owns
    # `_workload_may_run` (cleared to pause, set to resume) and clears
    # `_data_workload_parked` before each pause; the mutation thread only
    # ever sets the ack. That split means neither side can observe a stale
    # ack from a previous pause.
    #
    # `_workload_may_run` is deliberately phrased as "may run" rather than
    # "is paused" so the parked thread can block on Event.wait() and be
    # released the instant resume is called, instead of sleeping out a poll
    # interval first.
    # ------------------------------------------------------------------

    # Upper bound on how long the parked thread waits before re-checking
    # `self.mutations`; resume itself is immediate via the event. Keeps
    # tearDown from waiting on a thread parked at the barrier.
    WORKLOAD_BARRIER_POLL = 1
    # How often the controller re-signals in-flight loader tasks while it
    # waits for the ack. Larger, because each sweep is a REST round-trip
    # per task_id.
    WORKLOAD_PAUSE_POLL = 10
    # Query worker threads are named by the loaders in n1ql/cbas/fts.py.
    QUERY_THREAD_PREFIX = "query_thread_"

    def init_workload_controls(self):
        """
        Create the pause/resume handshake state. Call once from setUp, before
        any workload thread can exist.
        """
        self._workload_may_run = threading.Event()
        self._workload_may_run.set()
        self._data_workload_parked = threading.Event()
        # Query loads that were serving traffic when we paused, so resume
        # restarts exactly those and not, say, one that a previous step had
        # deliberately left stopped.
        self._resumable_query_loads = list()

    def wait_if_workload_paused(self):
        """
        Barrier for the data-workload thread. Call at the top of every
        mutation iteration, before any range bookkeeping is advanced.

        Parking here rather than between generate_docs() and perform_load()
        is deliberate: the doc ranges are whole-iteration state, so pausing
        at the loop boundary leaves them consistent and the next wave picks
        up exactly where the previous one stopped.

        Re-sets the ack on every wake so that a pause issued immediately
        after a resume -- before this thread has left the barrier -- is still
        acknowledged.
        """
        if self._workload_may_run.is_set():
            return
        self.log.info("Data workload: parking at pause barrier")
        while not self._workload_may_run.is_set() and self.mutations:
            self._data_workload_parked.set()
            # Returns as soon as resume sets the event; the timeout only
            # bounds how often `self.mutations` is re-read.
            self._workload_may_run.wait(timeout=self.WORKLOAD_BARRIER_POLL)
        self.log.info("Data workload: leaving pause barrier")

    def _stop_loader_tasks(self, tasks):
        """
        Ask Sirius to stop the given doc-loading tasks; returns the number
        signalled.

        Calls SiriusCouchbaseLoader.end_task() directly rather than going
        through TaskManager.stop_task(): a loader whose create_doc_load_task()
        failed carries an empty task_ids list, for which end_task() returns a
        None response that TaskManager.stop_task() then subscripts.

        /stop_task is idempotent on the Java side (an already-completed task
        reports status=true), so this is safe to call repeatedly on the same
        task while waiting for the mutation thread to notice the pause.
        """
        stopped = 0
        for task in tasks:
            task_ids = getattr(task, "task_ids", None)
            if not task_ids:
                continue
            try:
                okay, response = task.end_task()
                if okay:
                    stopped += 1
                else:
                    self.log.warning("stop_task failed for %s: %s"
                                     % (task_ids, response))
            except Exception as e:
                self.log.warning("Exception while stopping loader task %s: %s"
                                 % (task_ids, e))
        return stopped

    def pause_data_workload(self, timeout=1800):
        """
        Park the mutation thread and stop whatever it currently has in flight.

        Returns True once the thread has acknowledged (or if no mutation
        thread is running), False if it was still busy when `timeout` expired
        -- the caller decides whether that is fatal.

        The in-flight tasks are signalled repeatedly rather than once: the
        thread may have been a statement past the barrier when the request
        landed, in which case it submits one more full wave that also has to
        be cut short. Stopped tasks make the thread's get_task_result() return
        promptly, at the cost of a benign "Failure during get_task_result"
        line in the log.
        """
        self._data_workload_parked.clear()
        self._workload_may_run.clear()

        mutation_th = getattr(self, "mutation_th", None)
        if mutation_th is None or not mutation_th.is_alive() \
                or not self.mutations:
            self.log.info("Data workload: no mutation thread running, "
                          "nothing to pause")
            return True

        self.log.info("Data workload: pause requested, waiting for the "
                      "current wave to wind down")
        deadline = time.time() + timeout
        parked = False
        while not parked and time.time() < deadline:
            # `or []` because perform_load() returns False, not a list, when
            # a loader task could not be created.
            self._stop_loader_tasks(list(self.loader_tasks or []))
            parked = self._data_workload_parked.wait(
                timeout=self.WORKLOAD_PAUSE_POLL)

        if not parked:
            self.log.critical(
                "Data workload did not park within %ss; continuing with it "
                "still running" % timeout)
            return False

        # Sweep once more for anything submitted between the last sweep and
        # the thread reaching the barrier.
        self._stop_loader_tasks(list(self.loader_tasks or []))
        self.log.info("Data workload: paused")
        return True

    def resume_data_workload(self):
        """
        Release the barrier. The mutation thread resumes at the top of its
        loop, so the next wave continues from the ranges the paused iteration
        left behind -- no range is replayed or skipped.
        """
        self._workload_may_run.set()
        self.log.info("Data workload: resume signalled")

    def _live_query_threads(self):
        return [th for th in threading.enumerate()
                if th.is_alive() and th.name.startswith(
                    self.QUERY_THREAD_PREFIX)]

    def _wait_for_query_threads_to_exit(self, timeout=600):
        """
        Wait for the query worker threads to actually die after stop_run was
        set.

        This matters for correctness, not tidiness: start_query_load() spawns
        a fresh set of workers, so resuming while stragglers are still looping
        on `while not self.stop_run` would permanently inflate the query
        concurrency once stop_run flips back to False.

        The loaders keep no handle on their threads, so identify them by the
        name every loader gives its workers.
        """
        deadline = time.time() + timeout
        live = self._live_query_threads()
        while live and time.time() < deadline:
            self.log.debug("Waiting for %s query thread(s) to exit"
                           % len(live))
            time.sleep(5)
            live = self._live_query_threads()
        if live:
            self.log.critical(
                "%s query thread(s) still alive after %ss: %s. Resuming "
                "anyway; query concurrency may exceed the configured QPS."
                % (len(live), timeout, [th.name for th in live]))
            return False
        return True

    def pause_query_workload(self, timeout=600):
        """
        Stop every query load that is currently serving traffic and wait for
        its worker threads to exit. Records which ones were running so
        resume_query_workload() restarts exactly that set.
        """
        self._resumable_query_loads = [
            ql for ql in (self.ql + self.ftsQL + self.cbasQL)
            if not ql.stop_run]
        if not self._resumable_query_loads:
            self.log.info("Query workload: nothing running, nothing to pause")
            return True

        self.log.info("Query workload: stopping %s query load(s)"
                      % len(self._resumable_query_loads))
        for ql in self._resumable_query_loads:
            ql.stop_query_load()
        drained = self._wait_for_query_threads_to_exit(timeout=timeout)
        self.log.info("Query workload: paused")
        return drained

    def resume_query_workload(self):
        """
        Restart the query loads recorded by pause_query_workload().

        QueryLoad.start_query_load() re-initialises query_stats, which would
        reset the per-query latency/count tables printed by
        monitor_query_status() every time a restore runs. Carry the previous
        dict over so those tables stay cumulative for the whole run. The
        other two loaders do not touch their stats on start, so re-assigning
        the same object back is a no-op for them.
        """
        for ql in self._resumable_query_loads:
            query_stats = getattr(ql, "query_stats", None)
            ql.start_query_load()
            if query_stats is not None:
                ql.query_stats = query_stats
        self.log.info("Query workload: resumed %s query load(s)"
                      % len(self._resumable_query_loads))
        self._resumable_query_loads = list()

    def pause_workloads(self, data_timeout=1800, query_timeout=600):
        """
        Quiesce the cluster: no doc loading, no queries.

        Queries stop first so the KV/GSI capacity they were consuming is
        available to the doc-load drain and the index catch-up that
        pause_data_workload() then waits on.
        """
        self.PrintStep("Pausing data and query workloads")
        query_ok = self.pause_query_workload(timeout=query_timeout)
        data_ok = self.pause_data_workload(timeout=data_timeout)
        return data_ok and query_ok

    def resume_workloads(self):
        """
        Bring both workloads back, in the reverse order of pause_workloads():
        doc loading first, so the queries restarted after it find an index
        that is already being fed again.
        """
        self.PrintStep("Resuming data and query workloads")
        self.resume_data_workload()
        self.resume_query_workload()

    # ------------------------------------------------------------------
    # Backup / restore driven from inside a running volume test
    # ------------------------------------------------------------------

    def run_backup_restore(self):
        """
        Flush the buckets and restore them, using whichever backup mechanism
        the run was configured with. A no-op when neither backup param is set.

        Named `run_backup_restore` rather than `restore_from_backup` because
        hostedHospital.Murphy defines a same-named method with different
        semantics; keeping them distinct avoids an inherited/overridden trap
        for anything that ends up mixing the two hierarchies.

        Expects the workloads to already be quiesced -- see
        restore_with_workloads_paused(), which is what callers should use.

        cbbackupmgr (cont_bkp_test is None):
            backup -> merge -> capture item count -> flush -> restore ->
            monitor
        continuous backup / PITR:
            disable cont-backup -> flush -> restore every recorded timestamp
            in order -> monitor -> re-enable cont-backup
        """
        if self.drBackup is None:
            self.log.info("No backup configured for this run; skipping restore")
            return

        if self.cont_bkp_test is None:
            self._restore_using_cbbackupmgr()
        else:
            self._restore_using_continuous_backup()

    def _restore_using_cbbackupmgr(self):
        """
        Backup, merge and restore are per-cluster operations -- the repo is
        configured with empty include/exclude lists, so one pass of each
        covers every bucket. Only the pre-flush item-count capture and the
        post-restore monitoring are per-bucket.
        """
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.sleep(10, "Waiting for item count to stabilize after ep_queue drain")
        expected_items = {
            bucket.name: self.bucket_util.get_buckets_item_count(
                self.cluster, bucket.name)
            for bucket in self.cluster.buckets}

        output, error = self.drBackup.backup(
            self.backup_archive, self.backup_repo,
            cluster_host=self.backup_cluster_host)
        self.log.info(f"Backup Output: {output}\n\n Backup Error: {error}")

        # merge_all_backups() raises for an object-store archive, which
        # cbbackupmgr cannot merge. Skipping the merge still leaves a
        # restorable repo, so degrade rather than fail the run here.
        if self.drBackup.cloud_provider is None:
            output, error = self.drBackup.merge_all_backups(
                archive=self.backup_archive, repo=self.backup_repo)
            self.log.info(f"Merge Output: {output}\n\n Merge Error: {error}")
        else:
            self.log.info("Skipping merge: cbbackupmgr cannot merge an "
                          "object-store archive (%s)"
                          % type(self.drBackup.cloud_provider).__name__)

        # Print stats before restore
        self.print_stats(self.cluster)

        self.bucket_util.flush_all_buckets(self.cluster)

        output, error = self.drBackup.restore(
            self.backup_archive, self.backup_repo,
            cluster_host=self.backup_cluster_host, threads=60)
        self.log.info(f"Restore Output: {output}\n\n Restore Error: {error}")

        # The single restore above repopulates every bucket concurrently, so
        # the buckets share one deadline rather than getting restore_timeout
        # each -- otherwise a hung restore would burn N * restore_timeout.
        # The 60s floor keeps an already-restored bucket from being reported
        # as failed without a single poll once that deadline has passed.
        deadline = time.time() + self.restore_timeout
        failed = list()
        for bucket_name, items in expected_items.items():
            result = self.drBackup.monitor_restore(
                self.bucket_util, self.cluster, bucket_name, items,
                timeout=max(deadline - time.time(), 60))
            if not result:
                failed.append("%s did not reach %s items" % (bucket_name, items))
        self.assertFalse(failed,
                         "Restore failed within %ss: %s"
                         % (self.restore_timeout, ", ".join(failed)))

        # Print stats after restore
        self.print_stats(self.cluster)

    def _restore_using_continuous_backup(self):
        if self.drContBackup is None:
            self.log.critical("cont_bkp_test is set but drContBackup was "
                              "never built; skipping PITR restore")
            return

        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.sleep(10, "Waiting for item count to stabilize after ep_queue drain")

        # Every recorded timestamp, every time: the flush below empties the
        # buckets, so each restore replays from a clean slate and there is
        # nothing to be gained by skipping the earlier ones.
        #
        # Each entry is [timestamp, {bucket_name: item_count}]. The timestamps
        # are fixed-width ISO-8601 UTC, so ordering them lexicographically on
        # the first element is chronological; the per-bucket counts in the
        # second element are never compared.
        timestamps = sorted(self.continuous_backup_timestamps,
                            key=lambda entry: entry[0])
        if not timestamps:
            self.log.warning("No continuous-backup timestamps recorded; "
                             "skipping PITR restore")
            return

        # Disable continuous backup on every bucket before the flush, so
        # neither the deletes the flush generates nor the writes the restores
        # replay end up in the backup location.
        for bucket in self.cluster.buckets:
            self.toggle_continuous_backup(bucket, enable=False)

        # Print stats before restore
        self.print_stats(self.cluster)

        self.bucket_util.flush_all_buckets(self.cluster)
        self.sleep(30, "Sleep after flushing buckets, before triggering restore")

        buckets_by_name = {bucket.name: bucket for bucket in self.cluster.buckets}
        try:
            for timestamp, items_per_bucket in timestamps:
                self.log.info("Restoring backup with timestamp: %s "
                              "(expected items: %s)"
                              % (timestamp, items_per_bucket))
                # One restore per timestamp: cbcontbk replays every bucket in
                # the repo, so this is per-cluster and not per-bucket.
                output, error = self.drContBackup.trigger_restore(
                    cluster=self.cluster,
                    archive=self.backup_archive,
                    repo=self.backup_repo,
                    cont_backup_location=self.continuous_backup_location,
                    staging_dir="/data/tmp",
                    timestamp=timestamp,
                    threads=60)
                self.log.info(f"PITR Output: {output}\n\n PITR Error: {error}")

                # Monitoring is per-bucket and sequential. The buckets share
                # one deadline because the restore above repopulates them
                # concurrently -- giving each its own restore_timeout would
                # let a hung restore burn N * restore_timeout. The 60s floor
                # keeps a bucket that is already at its count from being
                # reported as failed without a single poll.
                deadline = time.time() + self.restore_timeout
                failed = list()
                for bucket_name, items in items_per_bucket.items():
                    bucket = buckets_by_name.get(bucket_name)
                    if bucket is None:
                        failed.append("%s no longer exists on the cluster"
                                      % bucket_name)
                        continue
                    result = self.drContBackup.monitor_restore(
                        self.bucket_util, cluster=self.cluster, bucket=bucket,
                        items=items,
                        timeout=max(deadline - time.time(), 60))
                    if not result:
                        failed.append("%s did not reach %s items"
                                      % (bucket_name, items))
                self.assertFalse(
                    failed,
                    "Restore failed for backup with timestamp %s: %s"
                    % (timestamp, ", ".join(failed)))
        finally:
            # Leave continuous backup on even if a restore failed, so the
            # buckets are not silently left unprotected for the rest of the run.
            for bucket in self.cluster.buckets:
                self.toggle_continuous_backup(bucket, enable=True)

            # Print stats after restore
            self.print_stats(self.cluster)

    def restore_with_workloads_paused(self):
        """
        Quiesce the workloads, run a full backup/restore verification against
        the live cluster, then bring the workloads back exactly as they were.

        The resume is in a finally block on purpose: a failed restore raises
        out of assertTrue, and leaving the mutation thread parked would strand
        it for whatever the enclosing test does next.
        """
        if self.drBackup is None:
            self.log.info("No backup configured for this run; "
                          "skipping restore step")
            return

        if not self.pause_workloads():
            self.log.critical(
                "Workloads did not fully quiesce; the restore below may see "
                "concurrent traffic and its item counts may be unstable")
        try:
            self.run_backup_restore()
        finally:
            self.resume_workloads()

    def monitor_query_status(self, print_duration=120):
        self.query_result = True

        def check_query_stats():
            st_time = time.time()
            while not self.stop_run:
                if st_time + print_duration < time.time():
                    self.query_table = TableView(self.log.info)
                    self.table = TableView(self.log.info)
                    self.table.set_headers(["Bucket",
                                            "Total Queries",
                                            "Failed Queries",
                                            "Success Queries",
                                            "Rejected Queries",
                                            "Cancelled Queries",
                                            "Timeout Queries",
                                            "Errored Queries"])
                    for ql in self.ql:
                        self.query_table.set_headers(["Bucket",
                                                      "Query",
                                                      "Count",
                                                      "nProbe",
                                                      "Filtering(M)",
                                                      "Avg Execution Time(ms)",
                                                      "Latest 1K - Avg Execution Time(ms)",
                                                      "Avg Accuracy",
                                                      "Avg Recall"
                                                      ])
                        if ql.esClient is not None:
                            self.query_table.headers.extend(["ES Avg Execution Time(ms)",
                                                             "ES Avg Recall"])
                        try:
                            for query, _ in sorted(ql.bucket.query_map.items(), key=lambda x: x[1]["identifier"]):
                                with ql.query_stats[query][5]:
                                    if ql.query_stats[query][1] > 0:
                                        self.query_table.add_row(
                                            [str(ql.bucket.name),
                                             ql.bucket.query_map[query]["identifier"],
                                             ql.query_stats[query][1],
                                             ql.bucket.query_map[query]["vector_defn"].get("nProbe"),
                                             ql.query_stats[query][4],
                                             round(ql.query_stats[query][0]/ql.query_stats[query][1], 2),
                                             round(sum(ql.query_stats[query][8][-1000:])/min(1000,len(ql.query_stats[query][8][-1000:])), 2),
                                             round(ql.query_stats[query][2]/ql.query_stats[query][1], 2),
                                             round(ql.query_stats[query][3]/ql.query_stats[query][1], 2)
                                             ]
                                            )
                                        if ql.esClient is not None:
                                            self.query_table.rows[-1].extend(
                                                [str(round(ql.query_stats[query][6]/ql.query_stats[query][1], 2)),
                                                 str(round(ql.query_stats[query][7]/ql.query_stats[query][1], 2))
                                                 ]
                                                )
                        except Exception as e:
                            print(e)
                        self.table.add_row([
                            str(ql.bucket.name),
                            str(ql.total_query_count),
                            str(ql.failed_count),
                            str(ql.success_count),
                            str(ql.rejected_count),
                            str(ql.cancel_count),
                            str(ql.timeout_count),
                            str(ql.error_count),
                            ])
                        if ql.failures > 0:
                            self.query_result = False
                    self.table.display("N1QL Results Stats")
                    self.query_table.display("N1QL Performance Stats")

                    self.FTStable = TableView(self.log.info)
                    self.FTStable.set_headers(["Bucket",
                                               "Total Queries",
                                               "Failed Queries",
                                               "Success Queries",
                                               "Rejected Queries",
                                               "Cancelled Queries",
                                               "Timeout Queries",
                                               "Errored Queries"])
                    for ql in self.ftsQL:
                        self.FTStable.add_row([
                            str(ql.bucket.name),
                            str(ql.total_query_count),
                            str(ql.failed_count),
                            str(ql.success_count),
                            str(ql.rejected_count),
                            str(ql.cancel_count),
                            str(ql.timeout_count),
                            str(ql.error_count),
                            ])
                        if ql.failures > 0:
                            self.query_result = False
                    self.FTStable.display("FTS Query Result Stats")

                    self.CBAStable = TableView(self.log.info)
                    self.CBAStable.set_headers(["Bucket",
                                                "Total Queries",
                                                "Failed Queries",
                                                "Success Queries",
                                                "Rejected Queries",
                                                "Cancelled Queries",
                                                "Timeout Queries",
                                                "Errored Queries"])
                    self.cbas_query_perf = TableView(self.log.info)
                    self.cbas_query_perf.set_headers(["Bucket",
                                                      "Query",
                                                      "Count",
                                                      "Avg Execution Time(ms)"])
                    for ql in self.cbasQL:
                        self.CBAStable.add_row([
                            str(ql.bucket.name),
                            str(ql.total_query_count),
                            str(ql.failed_count),
                            str(ql.success_count),
                            str(ql.rejected_count),
                            str(ql.cancel_count),
                            str(ql.timeout_count),
                            str(ql.error_count),
                            ])
                        try:
                            for query in sorted(ql.query_stats.keys()):
                                if ql.query_stats[query][1] > 0:
                                    self.cbas_query_perf.add_row([str(ql.bucket.name),
                                                                  ql.bucket.query_map[query][0],
                                                                  ql.query_stats[query][1],
                                                                  ql.query_stats[query][0]/ql.query_stats[query][1]])
                        except Exception as e:
                            print(e)
                        if ql.failures > 0:
                            self.query_result = False
                    self.CBAStable.display("CBAS Query Result Stats")
                    self.cbas_query_perf.display("CBAS Query Performance")

                    st_time = time.time()
                    time.sleep(10)

        query_monitor = threading.Thread(target=check_query_stats)
        query_monitor.start()

    def _execute_query_rest(self, query, timeout=300):
        """Run a N1QL statement against a query node over REST

        The SDK-based query path in n1ql.py is per-bucket and tuned for the
        workload queries; cluster-wide statements (system:settings and the
        like) go directly to the query service instead.

        Returns: (status, results, response)
        """
        if not self.cluster.query_nodes:
            raise Exception("No query nodes in the cluster to run: %s" % query)
        node = random.choice(self.cluster.query_nodes)
        client = QueryRestAPI(node)
        status, response = client.run_query(params={"statement": query},
                                            timeout=timeout)
        results = None
        if isinstance(response, dict):
            results = response.get("results")
        return status, results, response

    def set_plan_stability(self, mode="prepared_only"):
        """Configure plan stability mode for the cluster

        Args:
            mode: Plan stability mode - "prepared_only", "ad_hoc",
                  "ad_hoc_read_only", or "off"
        """
        if mode not in self.PLAN_STABILITY_MODES:
            raise ValueError("Invalid plan stability mode: %s. Must be one of %s"
                             % (mode, self.PLAN_STABILITY_MODES))

        query = 'UPDATE system:settings SET plan_stability.mode = "%s"' % mode
        self.log.info("Running plan stability query: %s" % query)
        status, results, response = self._execute_query_rest(query)
        if not status:
            raise Exception("Failed to set plan stability: %s" % response)

        # Verify plan stability settings
        verify_query = "SELECT plan_stability FROM system:settings"
        self.log.info("Verifying plan stability settings")
        status, results, response = self._execute_query_rest(verify_query)
        self.log.info("Current plan stability settings: %s" % results)

    def start_plan_stability_rotation(self, interval=None):
        """Rotate the cluster's plan stability mode in a background thread

        Starts at "prepared_only" and moves to the next mode in
        PLAN_STABILITY_MODES every `interval` seconds (2h by default), wrapping
        around forever so a long volume run exercises every mode repeatedly.

        The thread is a daemon and also watches self.stop_run, so tearDown
        (which flips stop_run) does not have to wait out a full interval.
        """
        if interval is None:
            interval = self.input.param("plan_stability_interval", 7200)
        if self.plan_stability_thread and self.plan_stability_thread.is_alive():
            self.log.info("Plan stability rotation thread is already running")
            return self.plan_stability_thread

        self.stop_plan_stability = False

        def rotate():
            index = 0
            while not self.stop_run and not self.stop_plan_stability:
                mode = self.PLAN_STABILITY_MODES[
                    index % len(self.PLAN_STABILITY_MODES)]
                try:
                    self.set_plan_stability(mode)
                except Exception as e:
                    # A rotation failure must not kill the volume run; log it
                    # and retry with the next mode after the usual interval.
                    self.log.critical(
                        "Failed to set plan stability mode %s: %s" % (mode, e))
                index += 1
                # Sleep in slices so stop_run/stop_plan_stability is honoured
                # within seconds instead of after the full interval.
                slept = 0
                while slept < interval and not self.stop_run \
                        and not self.stop_plan_stability:
                    time.sleep(min(10, interval - slept))
                    slept += 10
            self.log.info("Plan stability rotation thread exiting")

        self.plan_stability_thread = threading.Thread(
            target=rotate, name="plan_stability_rotator")
        self.plan_stability_thread.daemon = True
        self.plan_stability_thread.start()
        self.log.info("Started plan stability rotation: %s every %ss"
                      % (self.PLAN_STABILITY_MODES, interval))
        return self.plan_stability_thread

    def stop_plan_stability_rotation(self):
        """Signal the plan stability rotation thread to exit"""
        self.stop_plan_stability = True

    def set_awr_aus(self):
        """Configure AWR and AUS settings for the cluster with fixed scheduling

        AUS runs on a fixed 6 PM - 9 PM IST window every day, so a volume run
        long enough to cross that window exercises the update-statistics path.
        AWR reports into the first bucket of the run.
        """
        start_time_str = self.AUS_START_TIME
        end_time_str = self.AUS_END_TIME
        self.log.info("AUS start time: %s, AUS end time: %s"
                      % (start_time_str, end_time_str))

        # Configure AUS (Automatic Update Statistics)
        aus_query = (
            'UPDATE system:aus SET enable = true, change_percentage = 20, '
            'all_buckets = true, schedule = {"start_time": "%s", '
            '"end_time": "%s", "timezone": "%s", "days": %s}'
            % (start_time_str, end_time_str, self.AUS_TIMEZONE,
               json.dumps(self.AUS_DAYS)))

        # Configure AWR location and interval (single quotes for Query
        # Workbench compatibility)
        bucket_name = self.cluster.buckets[0].name
        awr_config_query = (
            "UPDATE system:awr SET location = '%s', interval = '%s', "
            "threshold = '%s'"
            % (bucket_name, self.AWR_INTERVAL, self.AWR_THRESHOLD))

        # Enable AWR
        awr_enable_query = "UPDATE system:awr SET enabled = true"

        try:
            self.log.info("Running AUS query: %s" % aus_query)
            self._execute_query_rest(aus_query)
            self.sleep(30, "Wait for AUS settings to take effect")

            # Verify AUS settings
            _, aus_settings, _ = self._execute_query_rest(
                "SELECT * FROM system:aus")
            self.log.info("Current AUS settings: %s" % aus_settings)

            self.log.info("Running AWR config query: %s" % awr_config_query)
            self._execute_query_rest(awr_config_query)
            self.sleep(30, "Wait for AWR config to take effect")

            self.log.info("Running AWR enable query: %s" % awr_enable_query)
            self._execute_query_rest(awr_enable_query)
            self.sleep(30, "Wait for AWR to be enabled")

            # Verify AWR settings
            _, awr_settings, _ = self._execute_query_rest(
                "SELECT * FROM system:awr")
            self.log.info("Current AWR settings: %s" % awr_settings)
        except Exception as e:
            self.log.error("Error configuring AWR/AUS settings: %s" % str(e))
            raise

    def refresh_cluster(self, tenant, cluster, type="dedicated"):
        while True:
            if cluster.nodes_in_cluster:
                try:
                    cluster.refresh_object(self.cluster_util.get_nodes(
                        random.choice(cluster.nodes_in_cluster)))
                    break
                except ServerUnavailableException:
                    pass
                except IndexError:
                    pass
            else:
                self.log.critical("Cluster object: Nodes in cluster are reset by rebalance task.")
                self.sleep(30)
                self.servers = DedicatedUtils.get_nodes(
                    self.pod, tenant, cluster.id)
                nodes = list()
                for server in self.servers:
                    temp_server = TestInputServer()
                    temp_server.ip = server.get("hostname")
                    temp_server.hostname = server.get("hostname")
                    temp_server.services = server.get("services")
                    temp_server.port = "18091"
                    temp_server.rest_username = cluster.username
                    temp_server.rest_password = cluster.password
                    temp_server.hosted_on_cloud = True
                    temp_server.memcached_port = "11207"
                    temp_server.type = type
                    if type == "columnar":
                        temp_server.cbas_port = 18095
                    nodes.append(temp_server)
                cluster.refresh_object(nodes)

    def trigger_rollback(self):
        mem_only_items = 1000
        rollbacks = 0
        while rollbacks < 20:
            self.PrintStep("Running Rollback: %s" % rollbacks)
            for i, node in enumerate(self.cluster.kv_nodes):
                # Stopping persistence on NodeA
                for bucket in self.cluster.buckets:
                    shell = RemoteMachineShellConnection(node)
                    Cbepctl(shell).persistence(bucket.name, "stop")
            for bucket in self.cluster.buckets:
                bucket.loadDefn["key_prefix"] = "rollback_docs_%s-" % (rollbacks)
            if self.val_type == "siftBigANN":
                JavaDocLoaderUtils.load_sift_data(cluster=self.cluster,
                                buckets=self.cluster.buckets,
                                overRidePattern={"create": 100, "read": 0, "update": 0, "delete": 0, "expiry": 0},
                                validate_data=False,
                                wait_for_stats=False,
                                override_num_items=mem_only_items)
            elif self.val_type in ("MSMARCOEmbeddingProduct",
                                   "MSMARCOSiftEmbeddingProduct"):
                JavaDocLoaderUtils.load_msmarco_data(cluster=self.cluster,
                                buckets=self.cluster.buckets,
                                overRidePattern={"create": 100, "read": 0, "update": 0, "delete": 0, "expiry": 0},
                                validate_data=False,
                                wait_for_stats=False,
                                override_num_items=mem_only_items)
            else:
                JavaDocLoaderUtils.load_data(self.cluster, self.cluster.buckets,
                                             overRidePattern={"create": 100, "read": 0, "update": 0, "delete": 0, "expiry": 0},
                                             suppress_error_table=True,
                                             track_failures=False,
                                             wait_for_stats=False,
                                             override_num_items=mem_only_items)

            self.check_index_pending_mutations(self.cluster)

            # Kill memcached on Nodes to trigger rollback on other Nodes
            for i, node in enumerate(self.cluster.kv_nodes):
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[node],
                wait_time=self.wait_timeout * 10))
            self.sleep(10, "Not Required, but waiting for 10s after warm up")
            self.check_index_pending_mutations(self.cluster)
            result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
            if result:
                self.stop_crash = True
                self.task_manager.abort_all_tasks()
                self.doc_loading_tm.abortAllTasks()
                self.assertFalse(
                    result,
                    "CRASH | CRITICAL | WARN messages found in cb_logs")
            rollbacks += 1
        for i, node in enumerate(self.cluster.kv_nodes):
            # Stopping persistence on NodeA
            for bucket in self.cluster.buckets:
                Cbepctl(shell).persistence(bucket.name, "start")

        for bucket in self.cluster.buckets:
            bucket.loadDefn["key_prefix"] = self.input.param("key", "test_docs-")

    def check_index_pending_mutations(self, cluster):
        while not self.stop_run:
            check = False
            for node in cluster.index_nodes:
                try:
                    stats = GsiHelper(node, self.log).get_bucket_index_stats()
                    for bucket in cluster.buckets:
                        bucket = bucket.name
                        if bucket == "MAINT_STREAM":
                            continue
                        for scope in stats[bucket]:
                            for collection in stats[bucket][scope]:
                                for idx in stats[bucket][scope][collection]:
                                    self.log.info(":".join([
                                        bucket, scope, collection,
                                        idx, "num_docs_pending",
                                        str(stats[bucket][scope][collection][idx]["num_docs_pending"])]))
                                    if stats[bucket][scope][collection][idx]["num_docs_pending"] > 0:
                                        check = True
                except Exception as e:
                    self.log.critical(e)
            if check:
                self.sleep(30, "Wait for index mutations pending")
            else:
                break

    def setup_backup_locations(self):
        """
        Resolve `cbbackup_test` / `cont_bkp_test` into the cbbackupmgr archive
        and the continuous-backup location, and make the latter usable.

        Runs before the buckets exist, so the continuous-backup location is
        already writable -- and its object-store credentials already known to
        the cluster -- by the time continuous backup is switched on. The
        archive needs no separate provisioning step: cbbackupmgr's own
        configure_backup() wipes and recreates it in setup_backup_tools().
        """
        if "NFS" in (self.cbbackup_test, self.cont_bkp_test):
            self.nfs_util = NfsUtil.for_pitr_tests(self.input)
            self.nfs_server = self.nfs_util.nfs_server_node
            self.nfs_util.ensure_nfs_clients(self.servers)

        self.backup_repo = self.input.param("repo_name", "magma")
        self.restore_timeout = self.input.param("restore_timeout", 12 * 60 * 60)

        # An archive on NFS has a fixed path; anywhere else local it lives on
        # the backup node's data path, which setup_backup_tools() resolves.
        self.backup_archive = None
        self.backup_location = BackupLocation.build(
            self.cbbackup_test, "backup archive",
            local_path=self.NFS_BACKUP_ARCHIVE
            if self.cbbackup_test == "NFS" else None,
            url_templates=self.BACKUP_URL_TEMPLATES,
            obj_staging_dir=self.obj_staging_dir_cbbackup, log=self.log)

        self.cont_bkp_location = BackupLocation.build(
            self.cont_bkp_test, "continuous backup location",
            local_path=self.NFS_CONT_BKP_LOCATION,
            url_templates=self.CONT_BKP_URL_TEMPLATES,
            obj_staging_dir=self.obj_staging_dir_cont_bkp, log=self.log)
        self.continuous_backup_location = self.cont_bkp_location.path

        shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            self.cont_bkp_location.provision(shell)
        finally:
            shell.disconnect()

        # The backup service reads the object store itself, so it needs the
        # credentials in the cluster's Credential Store rather than on the CLI.
        self.cont_bkp_credential_store_id = \
            self.cont_bkp_location.create_credential_store(
                RestConnection(self.cluster.master), cred_id="cont_bkp_cred",
                description="Credential store for continuous backup "
                            "volume tests")

    def setup_backup_encryption(self):
        """
        Opt in to encryption-at-rest for the backup tools, per `ear_bk`
        (cbbackupmgr archive) and `ear_contbk` (cbcontbk), both default False.

        Provisions an external KMS key and uploads its credentials to the
        Credential Store, so the backup service can reach the key. The
        provider itself is attached to the CLI wrappers later, in
        setup_backup_tools(), which is where they are built.

        Runs after setup_backup_locations() -- it grants the backup service
        the continuous-backup credential alongside the KMS one -- and before
        create_encryption_at_rest_key(), because bucket-level EaR is a
        prerequisite for continuous-backup EaR and the buckets are created
        straight after. teardown_backup_encryption() reverses all of it.
        """
        self.ear_bk = False
        self.ear_contbk = False
        self.kms_provider = None
        self.km_cred_store_id = None

        if not self.cont_bkp_test:
            return

        self.ear_bk = self.input.param("ear_bk", False)
        self.ear_contbk = self.input.param("ear_contbk", False)
        if not (self.ear_bk or self.ear_contbk):
            return

        # Bucket-level EaR is a PREREQUISITE for continuous-backup EaR. Unlike
        # collections_base, which silently turns it on, a volume run has to be
        # told explicitly -- flipping it here would create buckets that differ
        # from what the run was asked for.
        if self.ear_contbk and self.enable_encryption_at_rest is not True:
            self.fail("ear_contbk=True needs bucket-level encryption at rest; "
                      "pass enable_encryption_at_rest=True as well")

        km_provider_name = self.input.param("km_provider", "AWS")
        if km_provider_name not in CLOUD_PROVIDER_CLASSES:
            self.fail(f"Invalid km_provider {km_provider_name!r}. Expected "
                      f"one of {list(CLOUD_PROVIDER_CLASSES)}.")
        self.kms_provider = CLOUD_PROVIDER_CLASSES[km_provider_name](
            log=self.log)
        self.kms_provider.create_kms_key(
            alias=self.input.param("km_key_alias", None))
        self.log.info(f"KMS key provisioned for EaR: "
                      f"{self.kms_provider.km_key_url}")

        rest = RestConnection(self.cluster.master)
        self.km_cred_store_id = f"km_cred_{uuid.uuid4()}"
        self.kms_provider.create_kms_credential_store(
            rest, cred_id=self.km_cred_store_id,
            description="KMS credential for volume test EaR")
        self.log.info(f"Created credential store for KMS with ID: "
                      f"{self.km_cred_store_id}")

        roles = [f"credential_consumer[{self.km_cred_store_id}]"]
        if self.cont_bkp_credential_store_id:
            roles.append(
                f"credential_consumer[{self.cont_bkp_credential_store_id}]")
        CredentialStoreUtils().put_service_roles(
            rest, service_name="backup", roles=roles)

    def attach_kms_to_backup_tools(self):
        """
        Hand the KMS provider to whichever CLI wrapper asked for it.

        Separate from setup_backup_encryption() because the wrappers are built
        in setup_backup_tools(), which runs well after the key is provisioned
        (the backup node has to join the cluster first).

        BackupMgrUtil is a CbBackupMgr, so it takes the provider directly;
        ContinuousBackupUtil only holds them, so its cbcontbk and cbbackupmgr
        wrappers are set individually -- cbcontbk reads the archive too.
        """
        if getattr(self, "kms_provider", None) is None:
            return
        if self.ear_bk and self.drBackup is not None:
            self.drBackup.kms_provider = self.kms_provider
        if self.ear_contbk and self.drContBackup is not None:
            self.drContBackup.cont_bk_mgr.kms_provider = self.kms_provider
            self.drContBackup.backup_mgr.kms_provider = self.kms_provider
        self.log.info("Attached KMS provider to backup tools: "
                      "ear_bk=%s, ear_contbk=%s"
                      % (self.ear_bk, self.ear_contbk))

    def teardown_backup_encryption(self):
        """
        Reverse setup_backup_encryption(): drop the KMS key, its Credential
        Store entry, and the bucket-encryption secrets.

        On a passing run everything is cleaned up -- cloud KMS keys carry
        ongoing cost and must not be left orphaned. On a failed run nothing is
        deleted: the key, the Credential Store entry and the bucket-encryption
        secrets are left exactly as they are so the encrypted backup can still
        be read during post-mortem. Everything a human needs to find the key in
        the provider's console is logged before returning.

        Every step swallows its own exceptions so one failure cannot strand
        the resources after it.
        """
        if not (getattr(self, "ear_bk", False)
                or getattr(self, "ear_contbk", False)):
            if self.is_test_failed():
                # Same reasoning as the backup-EaR path below: keep the
                # bucket-encryption secrets around for the post-mortem.
                self.log.warning("Test failed. Leaving the bucket encryption "
                                 "secrets in the same state for post-mortem")
                return
            self._delete_bucket_encryption_secrets()
            return

        if self.is_test_failed() and self.kms_provider is not None:
            key_details = {
                "provider": type(self.kms_provider).__name__,
                "km_key_url": self.kms_provider.km_key_url,
                # AWS uses (_km_key_id, _km_alias_name); GCP/Azure use
                # _km_key_name. Read via getattr so the log line is
                # provider-agnostic.
                "km_key_id": getattr(self.kms_provider, "_km_key_id", None),
                "km_alias_name": getattr(
                    self.kms_provider, "_km_alias_name", None),
                "km_key_name": getattr(
                    self.kms_provider, "_km_key_name", None),
                "km_created_by_this_run": getattr(
                    self.kms_provider, "_km_created_by_us", None),
                "km_cred_store_id": self.km_cred_store_id,
            }
            self.log.warning(f"EaR test failed. Leaving the KMS keys in the "
                             f"same state — Details for post-mortem "
                             f"lookup: {key_details}")

        if self.is_test_failed():
            return

        if self.kms_provider is not None:
            try:
                self.kms_provider.delete_kms_key()
            except Exception as e:
                self.log.warning(
                    f"Failed to delete KMS key during teardown: {e}")

        if self.km_cred_store_id is not None:
            try:
                CredentialStoreUtils().delete_credential(
                    RestConnection(self.cluster.master),
                    self.km_cred_store_id)
            except Exception as e:
                self.log.error(f"Exception while deleting KMS credential "
                               f"store entry: {e}")

        self._delete_bucket_encryption_secrets()

    def _delete_bucket_encryption_secrets(self):
        """
        Delete the server-managed ns_server secrets created by
        create_encryption_at_rest_key().

        Only keys this run created are dropped -- an id passed in through
        encryption_at_rest_id belongs to whoever created it.

        ns_server rejects delete_secret while any bucket still references the
        secret via encryptionAtRestKeyId, and BaseTestCase.tearDown() -- which
        drops the buckets -- runs after this. So unbind every bucket first by
        setting encryptionAtRestKeyId=-1.
        """
        for cluster, secret_id in getattr(self, "created_encryption_keys",
                                          None) or []:
            bucket_helper = BucketHelper(cluster.master)
            for bucket in cluster.buckets:
                try:
                    bucket_helper.change_bucket_props(
                        bucket, encryptionAtRestKeyId=-1)
                except Exception as e:
                    self.log.warning(
                        f"Failed to unbind bucket '{bucket.name}' from EaR "
                        f"secret id={secret_id} before delete: {e}")

            try:
                status, response = RestConnection(
                    cluster.master).delete_secret(secret_id)
                if not status:
                    self.log.warning(f"Failed to delete EaR bucket secret "
                                     f"id={secret_id}: {response}")
            except Exception as e:
                self.log.warning(f"Exception deleting EaR bucket secret "
                                 f"id={secret_id}: {e}")

    def setup_backup_tools(self, backup_cluster_host):
        """
        Build the cbbackupmgr/cbcontbk wrappers -- which run on the backup
        node -- and create the repository the volume test backs up into.
        """
        if not (self.cbbackup_test or self.cont_bkp_test
                or self.cluster.backup_nodes):
            return
        if not self.cluster.backup_nodes:
            self.fail("cbbackup_test=%s / cont_bkp_test=%s need a backup "
                      "node; pass backup_nodes or add 'backup' to "
                      "services_init" % (self.cbbackup_test,
                                         self.cont_bkp_test))

        backup_node = self.cluster.backup_nodes[0]
        self.backup_cluster_host = backup_cluster_host
        if self.backup_location.path is None:
            self.backup_location.path = "/data/bkrs"
        self.backup_archive = self.backup_location.path

        self.drBackup = BackupMgrUtil(
            backup_node, cloud_provider=self.backup_location.cloud_provider,
            obj_staging_dir=self.backup_location.obj_staging_dir)
        if self.cont_bkp_test:
            self.drContBackup = ContinuousBackupUtil(
                shell_conn=RemoteMachineShellConnection(backup_node),
                username=self.cluster.master.rest_username,
                password=self.cluster.master.rest_password,
                log=self.log,
                backupmgr_cloud_provider=self.backup_location.cloud_provider,
                contbk_cloud_provider=self.cont_bkp_location.cloud_provider,
                backupmgr_obj_staging_dir=self.backup_location.obj_staging_dir,
                contbk_obj_staging_dir=self.cont_bkp_location.obj_staging_dir)

        # The wrappers only exist now, so this is the first point at which the
        # KMS key provisioned in setup_backup_encryption() can be handed over.
        self.attach_kms_to_backup_tools()

        # configure_backup() wipes the archive before creating the repo, so it
        # subsumes provisioning for every backing store.
        self.drBackup.cleanup_archive(self.backup_archive)
        self.drBackup.configure_backup(self.backup_archive, self.backup_repo,
                                       [], [],
                                       encrypted=self.ear_bk)

    def enable_continuous_backup_on_buckets(self):
        """
        Point every bucket at the continuous-backup location resolved in
        setup_backup_locations(). Called once the buckets exist, since the
        create-bucket REST API does not accept the continuousBackup* settings.
        """
        if not self.cont_bkp_test:
            return
        ContinuousBackupUtil.enable_continuous_backup(
            self.bucket_util, self.cluster, self.cluster.buckets,
            continuous_backup_location=self.continuous_backup_location,
            continuous_backup_interval=self.continuous_backup_interval,
            continuous_backup_cloud_storage_cred_id=self.cont_bkp_credential_store_id,
            # Encryption at rest for the continuous backup itself: without
            # these the KMS key from setup_backup_encryption() would only
            # reach the CLI, and the buckets would keep streaming plaintext.
            continuous_backup_km_key_url=(
                self.kms_provider.km_key_url if self.ear_contbk else None),
            continuous_backup_km_cred_id=(
                self.km_cred_store_id if self.ear_contbk else None),
            default_history_retention_seconds=self.input.param(
                "history_retention_seconds", 86400),
            default_history_retention_bytes=self.input.param(
                "history_retention_bytes", 0),
            continuous_backup_retention_period=self.input.param(
                "continuous_backup_retention_period", 72),
            log=self.log)

    def teardown_backup_locations(self):
        """
        Collect cbbackupmgr/cbcontbk logs when the test failed -- leaving the
        backup data in place for investigation -- and otherwise remove
        everything the run wrote.
        """
        if self.drBackup is None and self.drContBackup is None:
            return

        if self.is_test_failed():
            self.log.warning(
                "Test failed, skipping cleanup of backup data to preserve it "
                "for investigation: archive=%s, continuous backup=%s"
                % (self.backup_archive, self.continuous_backup_location))
            if self.get_cbcollect_info:
                log_path = TestInputSingleton.input.param("logs_folder", "/tmp")
                if self.drBackup is not None:
                    self.drBackup.collect_backup_logs_on_failure(
                        self.backup_archive, log_path=log_path)
                if self.drContBackup is not None:
                    self.drContBackup.collect_continuous_backup_logs_on_failure(
                        self.continuous_backup_location)
            return

        if self.drBackup is not None:
            self.drBackup.cleanup_archive(self.backup_archive)
        if self.drContBackup is not None:
            self.drContBackup.cleanup_continuous_backup(
                self.continuous_backup_location)

    def toggle_continuous_backup(self, bucket, enable):
        """
        Toggle continuous backup on or off for a bucket.

        Args:
            bucket: Bucket object to toggle continuous backup for
            enable: Boolean - True to enable, False to disable

        Returns:
            Boolean indicating success/failure
        """
        rest = BucketHelper(self.cluster.master)
        self.log.info("%s continuous backup for bucket: %s"
                      % ("Enabling" if enable else "Disabling", bucket.name))
        if enable:
            status = rest.change_bucket_props(
                bucket, continuous_backup_enabled=enable,
                continuous_backup_retention_period=self.input.param(
                    "continuous_backup_retention_period", 1440))
        else:
            status = rest.change_bucket_props(bucket, continuous_backup_enabled=enable)

        if status:
            bucket.continuousBackupEnabled = enable
            self.log.info("Continuous backup %s for bucket: %s"
                          % ("enabled" if enable else "disabled", bucket.name))
        else:
            self.log.error("Failed to %s continuous backup for bucket: %s"
                           % ("enable" if enable else "disable", bucket.name))
        return status

    def record_timestamp_for_continuous_backup(self, cluster, bucket_util, continuous_backup_timestamps):
        """
        Record a PITR restore point as [timestamp, {bucket_name: item_count}].

        The item count is captured for every bucket, since one cbcontbk
        restore replays the whole cluster and each bucket has to be verified
        against its own count at that timestamp.
        """
        # Get timestamp of backup for continuous backup test
        shell = RemoteMachineShellConnection(cluster.backup_nodes[0])
        output, _ = shell.execute_command("date -u +'%Y-%m-%dT%H:%M:%SZ'")
        cont_bkp_timestamp = output[0].strip() if output else None
        self.log.info("Continuous backup timestamp: {}".format(cont_bkp_timestamp))
        shell.disconnect()

        if cont_bkp_timestamp is None:
            # Recording a None would both lose the restore point and break the
            # ordering of the list it is appended to.
            self.log.error("Could not read the timestamp off %s; this restore "
                           "point will not be recorded"
                           % cluster.backup_nodes[0].ip)
            return

        items = dict()
        for bucket in cluster.buckets:
            items[bucket.name] = bucket_util.get_buckets_item_count(
                cluster, bucket.name)
        self.log.info("Item count at %s: %s" % (cont_bkp_timestamp, items))

        continuous_backup_timestamps.append([cont_bkp_timestamp, items])

    def traditional_backup_and_record_timestamp_for_continuous_backup(self, order=["record_timestamp", "traditional_backup"]):
        if self.drBackup is None:
            return
        for step in order:
            if step == "traditional_backup":
                output, error = self.drBackup.backup(self.backup_archive, self.backup_repo,
                                                    cluster_host=self.backup_cluster_host)
                self.log.info(f"Backup Output: {output}\n\n Backup Error: {error}")
            elif step == "record_timestamp" and self.cont_bkp_test:
                self.sleep(60 * self.continuous_backup_interval * 2, "Sleep for a while before recording timestamp for continuous backup")
                self.record_timestamp_for_continuous_backup(self.cluster,
                                                            self.bucket_util,
                                                            self.continuous_backup_timestamps)
                self.sleep(60, "Sleep for a while after recording timestamp")
