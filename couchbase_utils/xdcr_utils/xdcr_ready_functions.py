import re

from common_lib import sleep
from global_vars import logger
from membase.api.exception import XDCRException
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import doc_generator
from bucket_utils.bucket_ready_functions import BucketUtils
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster

from TestInput import TestInputSingleton


class TOPOLOGY:
    CHAIN = "chain"
    STAR = "star"
    RING = "ring"
    HYBRID = "hybrid"


class REPLICATION_DIRECTION:
    UNIDIRECTION = "unidirection"
    BIDIRECTION = "bidirection"


class REPLICATION_TYPE:
    CONTINUOUS = "continuous"


class REPLICATION_PROTOCOL:
    CAPI = "capi"
    XMEM = "xmem"


class INPUT:
    REPLICATION_DIRECTION = "rdirection"
    CLUSTER_TOPOLOGY = "ctopology"
    SEED_DATA = "sdata"
    SEED_DATA_MODE = "sdata_mode"
    SEED_DATA_OPERATION = "sdata_op"
    POLL_INTERVAL = "poll_interval"  # in seconds
    POLL_TIMEOUT = "poll_timeout"  # in seconds
    SEED_DATA_MODE_SYNC = "sync"


class OPS:
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    APPEND = "append"


class EVICTION_POLICY:
    VALUE_ONLY = "valueOnly"
    FULL_EVICTION = "fullEviction"
    NO_EVICTION = "noEviction"
    NRU_EVICTION = "nruEviction"
    CB = [VALUE_ONLY, FULL_EVICTION]
    EPH = [NO_EVICTION, NRU_EVICTION]


class BUCKET_PRIORITY:
    HIGH = "high"


class BUCKET_NAME:
    DEFAULT = "default"


class OS:
    WINDOWS = "windows"
    LINUX = "linux"
    OSX = "osx"


class COMMAND:
    SHUTDOWN = "shutdown"
    REBOOT = "reboot"


class STATE:
    RUNNING = "running"


class REPL_PARAM:
    FAILURE_RESTART = "failureRestartInterval"
    CHECKPOINT_INTERVAL = "checkpointInterval"
    OPTIMISTIC_THRESHOLD = "optimisticReplicationThreshold"
    FILTER_EXP = "filterExpression"
    FILTER_SKIP_RESTREAM = "filterSkipRestream"
    SOURCE_NOZZLES = "sourceNozzlePerNode"
    TARGET_NOZZLES = "targetNozzlePerNode"
    BATCH_COUNT = "workerBatchSize"
    BATCH_SIZE = "docBatchSizeKb"
    LOG_LEVEL = "logLevel"
    MAX_REPLICATION_LAG = "maxExpectedReplicationLag"
    TIMEOUT_PERC = "timeoutPercentageCap"
    PAUSE_REQUESTED = "pauseRequested"
    PRIORITY = "priority"
    DESIRED_LATENCY = "desiredLatency"


class TEST_XDCR_PARAM:
    FAILURE_RESTART = "failure_restart_interval"
    CHECKPOINT_INTERVAL = "checkpoint_interval"
    OPTIMISTIC_THRESHOLD = "optimistic_threshold"
    FILTER_EXP = "filter_expression"
    FILTER_SKIP_RESTREAM = "filter_skip_restream"
    SOURCE_NOZZLES = "source_nozzles"
    TARGET_NOZZLES = "target_nozzles"
    BATCH_COUNT = "batch_count"
    BATCH_SIZE = "batch_size"
    LOG_LEVEL = "log_level"
    MAX_REPLICATION_LAG = "max_replication_lag"
    TIMEOUT_PERC = "timeout_percentage"
    PRIORITY = "priority"
    DESIRED_LATENCY = "desired_latency"

    @staticmethod
    def get_test_to_create_repl_param_map():
        return {
            TEST_XDCR_PARAM.FAILURE_RESTART: REPL_PARAM.FAILURE_RESTART,
            TEST_XDCR_PARAM.CHECKPOINT_INTERVAL: REPL_PARAM.CHECKPOINT_INTERVAL,
            TEST_XDCR_PARAM.OPTIMISTIC_THRESHOLD: REPL_PARAM.OPTIMISTIC_THRESHOLD,
            TEST_XDCR_PARAM.FILTER_EXP: REPL_PARAM.FILTER_EXP,
            TEST_XDCR_PARAM.FILTER_SKIP_RESTREAM: REPL_PARAM.FILTER_SKIP_RESTREAM,
            TEST_XDCR_PARAM.SOURCE_NOZZLES: REPL_PARAM.SOURCE_NOZZLES,
            TEST_XDCR_PARAM.TARGET_NOZZLES: REPL_PARAM.TARGET_NOZZLES,
            TEST_XDCR_PARAM.BATCH_COUNT: REPL_PARAM.BATCH_COUNT,
            TEST_XDCR_PARAM.BATCH_SIZE: REPL_PARAM.BATCH_SIZE,
            TEST_XDCR_PARAM.MAX_REPLICATION_LAG: REPL_PARAM.MAX_REPLICATION_LAG,
            TEST_XDCR_PARAM.TIMEOUT_PERC: REPL_PARAM.TIMEOUT_PERC,
            TEST_XDCR_PARAM.LOG_LEVEL: REPL_PARAM.LOG_LEVEL,
            TEST_XDCR_PARAM.PRIORITY: REPL_PARAM.PRIORITY,
            TEST_XDCR_PARAM.DESIRED_LATENCY: REPL_PARAM.DESIRED_LATENCY

        }


class XDCR_PARAM:
    # Per-replication params (input)
    XDCR_FAILURE_RESTART = "xdcrFailureRestartInterval"
    XDCR_CHECKPOINT_INTERVAL = "xdcrCheckpointInterval"
    XDCR_OPTIMISTIC_THRESHOLD = "xdcrOptimisticReplicationThreshold"
    XDCR_FILTER_EXP = "xdcrFilterExpression"
    XDCR_FILTER_SKIP_RESTREAM = "xdcrfilterSkipRestream"
    XDCR_SOURCE_NOZZLES = "xdcrSourceNozzlePerNode"
    XDCR_TARGET_NOZZLES = "xdcrTargetNozzlePerNode"
    XDCR_BATCH_COUNT = "xdcrWorkerBatchSize"
    XDCR_BATCH_SIZE = "xdcrDocBatchSizeKb"
    XDCR_LOG_LEVEL = "xdcrLogLevel"
    XDCR_MAX_REPLICATION_LAG = "xdcrMaxExpectedReplicationLag"
    XDCR_TIMEOUT_PERC = "xdcrTimeoutPercentageCap"
    XDCR_PRIORITY = "xdcrPriority"
    XDCR_DESIRED_LATENCY = "xdcrDesiredLatency"


class CHECK_AUDIT_EVENT:
    CHECK = False


# Event Definition:
# https://github.com/couchbase/goxdcr/blob/master/etc/audit_descriptor.json

class GO_XDCR_AUDIT_EVENT_ID:
    CREATE_CLUSTER = 16384
    MOD_CLUSTER = 16385
    RM_CLUSTER = 16386
    CREATE_REPL = 16387
    PAUSE_REPL = 16388
    RESUME_REPL = 16389
    CAN_REPL = 16390
    DEFAULT_SETT = 16391
    IND_SETT = 16392


class XDCRUtils:

    def __init__(self, cb_clusters, task, taskmgr):
        self.__cb_clusters = cb_clusters
        self.task = task
        self.task_manager = taskmgr
        for cluster in self.__cb_clusters:
            cluster.cluster_util = ClusterUtils(cluster, self.task_manager)
            cluster.bucket_util = BucketUtils(cluster, cluster.cluster_util,
                                              self.task)
        self.input = TestInputSingleton.input
        self.init_parameters()
        self.create_buckets()
        self.log = logger.get("test")

    def __is_test_failed(self):
        return (hasattr(self, '_resultForDoCleanups')
                and len(self._resultForDoCleanups.failures
                        or self._resultForDoCleanups.errors)) \
               or (hasattr(self, '_exc_info')
                   and self._exc_info()[1] is not None)

    def __is_cleanup_needed(self):
        return self.__is_test_failed() and (str(self.__class__).find(
            'upgradeXDCR') != -1 or self._input.param("stop-on-failure", False)
                                            )

    def __is_cluster_run(self):
        return len(set([server.ip for server in self._input.servers])) == 1

    def tearDown(self):
        """Cleanup cluster.
              1. Remove all remote cluster references.
              2. Remove all replications.
        """
        self.log.info("Removing xdcr/nodes settings")
        for cb_cluster in self.__cb_clusters:
            rest = RestConnection(cb_cluster.master)
            rest.remove_all_replications()
            rest.remove_all_remote_clusters()
            rest.remove_all_recoveries()

    def init_parameters(self):
        self.__topology = self.input.param("ctopology", TOPOLOGY.CHAIN)
        # complex topology tests (> 2 clusters must specify chain_length >2)
        self.__chain_length = self.input.param("chain_length", 2)
        self.__num_items = self.input.param("num_items", 100000)
        self.__rep_type = self.input.param("replication_type", REPLICATION_PROTOCOL.XMEM)
        self.__num_sasl_buckets = self.input.param("sasl_buckets", 0)
        self.__num_stand_buckets = self.input.param("standard_buckets", 0)
        self.__eviction_policy = self.input.param("eviction_policy", 'valueOnly')
        self.__mixed_priority = self.input.param("mixed_priority", None)
        self.__lww = self.input.param("lww", 0)
        self.__fail_on_errors = self.input.param("fail_on_errors", True)
        # simply append to this list, any error from log we want to fail test on
        self.__report_error_list = []
        if self.__fail_on_errors:
            self.__report_error_list = ["panic:",
                                        "non-recoverable error from xmem client. response status=KEY_ENOENT"]

        # for format {ip1: {"panic": 2, "KEY_ENOENT":3}}
        self.__error_count_dict = {}
        if len(self.__report_error_list) > 0:
            self.__initialize_error_count_dict()

        self._repl_restart_count_dict = {}
        self.__initialize_repl_restart_count_dict()

        # Public init parameters - Used in other tests too.
        # Move above private to this section if needed in future, but
        # Ensure to change other tests too.
        self._demand_encryption = self.input.param(
            "demand_encryption",
            False)
        self._num_replicas = self.input.param("replicas", 1)
        self.default_bucket = self.input.param("default_bucket", True)
        self._rdirection = self.input.param("rdirection",
                                            REPLICATION_DIRECTION.UNIDIRECTION)
        self._value_size = self.input.param("value_size", 512)
        self._poll_timeout = self.input.param("poll_timeout", 120)
        self._perc_upd = self.input.param("upd", 30)
        self._perc_del = self.input.param("del", 30)
        self._upd_clusters = self.input.param("update", [])
        if self._upd_clusters:
            self._upd_clusters = self._upd_clusters.split("-")
        self._del_clusters = self.input.param("delete", [])
        if self._del_clusters:
            self._del_clusters = self._del_clusters.split('-')
        self._expires = self.input.param("expires", 0)
        self._wait_for_expiration = self.input.param("wait_for_expiration", True)
        self._warmup = self.input.param("warm", "").split('-')
        self._rebalance = self.input.param("rebalance", "").split('-')
        self._failover = self.input.param("failover", "").split('-')
        self._wait_timeout = self.input.param("timeout", 60)
        self._disable_compaction = self.input.param("disable_compaction", "").split('-')
        self._item_count_timeout = self.input.param("item_count_timeout", 300)
        self._checkpoint_interval = self.input.param("checkpoint_interval", 60)
        self._optimistic_threshold = self.input.param("optimistic_threshold", 256)
        self._dgm_run = self.input.param("dgm_run", False)
        self._active_resident_threshold = \
            self.input.param("active_resident_threshold", 100)
        CHECK_AUDIT_EVENT.CHECK = self.input.param("verify_audit", 0)
        self._max_verify = self.input.param("max_verify", 100000)
        self._sdk_compression = self.input.param("sdk_compression", True)
        self._evict_with_compactor = self.input.param("evict_with_compactor", False)
        self._replicator_role = self.input.param("replicator_role", False)
        self._replicator_all_buckets = self.input.param("replicator_all_buckets", False)
        self.gen_create = doc_generator('test_docs', 0, self.__num_items)
        self.bucket_size = self.input.param("bucket_size", None)
        self.compression_mode = self.input.param("compression_mode", 'passive')

    def create_buckets(self):
        for cluster in self.__cb_clusters:
            cluster.bucket_util.create_multiple_buckets(cluster.master, self._num_replicas)

    def get_goxdcr_log_dir(self, node):
        """Gets couchbase log directory, even for cluster_run
        """
        _, dir = RestConnection(node).diag_eval(
            'filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
        return str(dir)

    def check_goxdcr_log(self, server, str, goxdcr_log=None, print_matches=None):
        """ Checks if a string 'str' is present in goxdcr.log on server
            and returns the number of occurances
            @param goxdcr_log: goxdcr log location on the server
        """
        if not goxdcr_log:
            goxdcr_log = self.get_goxdcr_log_dir(server) \
                         + '/goxdcr.log*'

        shell = RemoteMachineShellConnection(server)
        info = shell.extract_remote_info().type.lower()
        if info == "windows":
            matches = []
            if print_matches:
                matches, err = shell.execute_command("grep \"{0}\" {1}".
                                                     format(str, goxdcr_log))
                if matches:
                    self.log.debug(matches)

            count, err = shell.execute_command("grep \"{0}\" {1} | wc -l".
                                               format(str, goxdcr_log))
        else:
            matches = []
            if print_matches:
                matches, err = shell.execute_command("zgrep \"{0}\" {1}".
                                                     format(str, goxdcr_log))
                if matches:
                    self.log.debug(matches)

            count, err = shell.execute_command("zgrep \"{0}\" {1} | wc -l".
                                               format(str, goxdcr_log))
        if isinstance(count, list):
            count = int(count[0])
        else:
            count = int(count)
        print(count)
        shell.disconnect()
        if print_matches:
            return matches, count
        return count

    def __is_cluster_run(self):
        return len(set([server.ip for server in self.input.servers])) == 1

    def __initialize_repl_restart_count_dict(self):
        """
            initializes self.__error_count_dict with ip, repl restart count
            like {{ip1: 3}, {ip2: 4}}
        """
        if not self.__is_cluster_run():
            goxdcr_log = self.get_goxdcr_log_dir(self.input.servers[0]) \
                         + '/goxdcr.log*'
        for node in self.input.servers:
            if self.__is_cluster_run():
                goxdcr_log = self.get_goxdcr_log_dir(node) \
                             + '/goxdcr.log*'
                self._repl_restart_count_dict[node.ip] = \
                    self.check_goxdcr_log(node,
                                          "Try to fix Pipeline",
                                          goxdcr_log)
        print(self._repl_restart_count_dict)

    def __initialize_error_count_dict(self):
        """
            initializes self.__error_count_dict with ip, error and err count
            like {ip1: {"panic": 2, "KEY_ENOENT":3}}
        """
        if not self.__is_cluster_run():
            goxdcr_log = self.get_goxdcr_log_dir(self.input.servers[0]) \
                         + '/goxdcr.log*'
        for node in self.input.servers:
            if self.__is_cluster_run():
                goxdcr_log = self.get_goxdcr_log_dir(node) \
                             + '/goxdcr.log*'
            self.__error_count_dict[node.ip] = {}
            for error in self.__report_error_list:
                self.__error_count_dict[node.ip][error] = \
                    self.check_goxdcr_log(node, error, goxdcr_log)
        print(self.__error_count_dict)

    def get_cb_cluster_by_name(self, name):
        """Return couchbase cluster object for given name.
        @return: CBCluster object
        """
        for cb_cluster in self.__cb_clusters:
            if cb_cluster.name == name:
                return cb_cluster
        raise Exception("Couchbase Cluster with name: %s not exist" % name)

    def get_rc_name(self, src_cluster_name, dest_cluster_name):
        return "remote_cluster_" + src_cluster_name + "-" + dest_cluster_name

    def add_remote_cluster(self, src_cluster, dest_cluster, name, encryption=False, replicator_target_role=False):
        """Create remote cluster reference or add remote cluster for xdcr.
        @param dest_cluster: Destination cb cluster object.
        @param name: name of remote cluster reference
        @param encryption: True if encryption for xdcr else False
        """
        remote_cluster = XDCRRemoteClusterRef(
            src_cluster,
            dest_cluster,
            name,
            encryption,
            replicator_target_role
        )
        remote_cluster.add()
        src_cluster.xdcr_remote_clusters.append(remote_cluster)

    def __set_topology_chain(self):
        """Will Setup Remote Cluster Chain Topology i.e. A -> B -> C
        """
        for i, cb_cluster in enumerate(self.__cb_clusters):
            if i >= len(self.__cb_clusters) - 1:
                break
            self.add_remote_cluster(
                cb_cluster,
                self.__cb_clusters[i + 1],
                self.get_rc_name(
                    cb_cluster.name,
                    self.__cb_clusters[i + 1].name),
                self._demand_encryption,
                self._replicator_role
            )
            if self._rdirection == REPLICATION_DIRECTION.BIDIRECTION:
                self.add_remote_cluster(
                    self.__cb_clusters[i + 1],
                    cb_cluster,
                    self.get_rc_name(
                        self.__cb_clusters[i + 1].name,
                        cb_cluster.name()),
                    self._demand_encryption,
                    self._replicator_role
                )

    def __set_topology_star(self):
        """Will Setup Remote Cluster Star Topology i.e. A-> B, A-> C, A-> D
        """
        hub = self.__cb_clusters[0]
        for cb_cluster in self.__cb_clusters[1:]:
            self.add_remote_cluster(
                hub,
                cb_cluster,
                self.get_rc_name(hub.name, cb_cluster.name),
                self._demand_encryption,
                self._replicator_role
            )
            if self._rdirection == REPLICATION_DIRECTION.BIDIRECTION:
                self.add_remote_cluster(
                    cb_cluster,
                    hub,
                    self.get_rc_name(cb_cluster.name, hub.name),
                    self._demand_encryption,
                    self._replicator_role
                )

    def __set_topology_ring(self):
        """
        Will Setup Remote Cluster Ring Topology i.e. A -> B -> C -> A
        """
        self.__set_topology_chain()
        self.add_remote_cluster(
            self.__cb_clusters[-1],
            self.__cb_clusters[0],
            self.get_rc_name(
                self.__cb_clusters[-1].name,
                self.__cb_clusters[0].name),
            self._demand_encryption,
            self._replicator_role
        )
        if self._rdirection == REPLICATION_DIRECTION.BIDIRECTION:
            self.add_remote_cluster(
                self.__cb_clusters[0],
                self.__cb_clusters[-1],
                self.get_rc_name(
                    self.__cb_clusters[0].name,
                    self.__cb_clusters[-1].name),
                self._demand_encryption,
                self._replicator_role
            )

    def set_xdcr_topology(self):
        """Setup xdcr topology as per ctopology test parameter.
        """
        if self.__topology == TOPOLOGY.CHAIN:
            self.__set_topology_chain()
        elif self.__topology == TOPOLOGY.STAR:
            self.__set_topology_star()
        elif self.__topology == TOPOLOGY.RING:
            self.__set_topology_ring()
        elif self._input.param(TOPOLOGY.HYBRID, 0):
            self.set_hybrid_topology()
        else:
            raise XDCRException(
                'Unknown topology set: {0}'.format(
                    self.__topology))

    def __parse_topology_param(self):
        tokens = re.split(r'(>|<>|<|\s)', self.__topology)
        return tokens

    def set_hybrid_topology(self):
        """Set user defined topology
        Hybrid Topology Notations:
        '> or <' for Unidirection replication between clusters
        '<>' for Bi-direction replication between clusters
        Test Input:  ctopology="C1>C2<>C3>C4<>C1"
        """
        tokens = self.__parse_topology_param()
        counter = 0
        while counter < len(tokens) - 1:
            src_cluster = self.get_cb_cluster_by_name(tokens[counter])
            dest_cluster = self.get_cb_cluster_by_name(tokens[counter + 2])
            if ">" in tokens[counter + 1]:
                src_cluster.add_remote_cluster(
                    dest_cluster,
                    self.get_rc_name(
                        src_cluster.name,
                        dest_cluster.name),
                    self._demand_encryption
                )
            if "<" in tokens[counter + 1]:
                dest_cluster.add_remote_cluster(
                    src_cluster,
                    self.get_rc_name(
                        dest_cluster.name, src_cluster.name),
                    self._demand_encryption
                )
            counter += 2

    def load_data_topology(self):
        for i, cluster in enumerate(self.__cb_clusters):
            if self._rdirection == REPLICATION_DIRECTION.BIDIRECTION:
                if i > len(self.__cb_clusters) - 1:
                    break
            else:
                if i >= len(self.__cb_clusters) - 1:
                    break
            cluster.bucket_util.sync_load_all_buckets(cluster, self.gen_create, "create", 0)

    def setup_all_replications(self):
        """Setup replication between buckets on remote clusters
        based on the xdcr topology created.
        """
        for cb_cluster in self.__cb_clusters:
            for remote_cluster in cb_cluster.xdcr_remote_clusters:
                buckets = remote_cluster.get_src_cluster().bucket_util.get_all_buckets()
                for src_bucket in buckets:
                    remote_cluster.create_replication(
                        src_bucket,
                        rep_type=self.__rep_type,
                        toBucket=remote_cluster.get_dest_cluster().bucket_util.get_bucket_obj(
                            buckets, src_bucket.name))
                remote_cluster.start_all_replications()

    def _resetup_replication_for_recreate_buckets(self, cluster_name):
        for cb_cluster in self.__cb_clusters:
            for remote_cluster_ref in cb_cluster.xdcr_remote_clusters:
                if remote_cluster_ref.get_src_cluster().name != cluster_name and remote_cluster_ref.get_dest_cluster().name != cluster_name:
                    continue
                remote_cluster_ref.clear_all_replications()
                buckets = remote_cluster_ref.get_src_cluster().bucket_util.get_all_buckets()
                for src_bucket in remote_cluster_ref.get_src_cluster().get_buckets():
                    remote_cluster_ref.create_replication(
                        src_bucket,
                        rep_type=self.__rep_type,
                        toBucket=remote_cluster_ref.get_dest_cluster().bucket_util.get_bucket_obj(
                            buckets, src_bucket.name))

    def setup_xdcr(self):
        self.set_xdcr_topology()
        self.setup_all_replications()

    def setup_xdcr_and_load(self):
        self.setup_xdcr()
        self.load_data_topology()

    def setup_xdcr_async_load(self):
        self.setup_xdcr()
        return self.async_load_data_topology()

    def load_and_setup_xdcr(self):
        """Initial xdcr
        first load then create xdcr
        """
        self.load_data_topology()
        self.setup_xdcr()

    def merge_all_buckets(self):
        """Merge bucket data between source and destination bucket
        for data verification. This method should be called after replication started.
        """
        # TODO need to be tested for Hybrid Topology
        for cb_cluster in self.__cb_clusters:
            for remote_cluster_ref in cb_cluster.xdcr_remote_clusters:
                for repl in remote_cluster_ref.get_replications():
                    self.log.debug("Merging keys for replication {0}"
                                   .format(repl))
                    self.__merge_keys(
                        repl.get_src_bucket().get_stats(),
                        repl.get_dest_bucket().get_stats(),
                        kvs_num=1,
                        filter_exp=repl.get_filter_exp())

    def __merge_keys(
            self, kv_src_bucket, kv_dest_bucket, kvs_num=1, filter_exp=None):
        """ Will merge kv_src_bucket keys that match the filter_expression
            if any into kv_dest_bucket.
        """
        valid_keys_src, deleted_keys_src = kv_src_bucket[
            kvs_num].key_set()
        valid_keys_dest, deleted_keys_dest = kv_dest_bucket[
            kvs_num].key_set()

        self.log.debug("src_kvstore has %s valid and %s deleted keys"
                       % (len(valid_keys_src), len(deleted_keys_src)))
        self.log.debug("dest kvstore has %s valid and %s deleted keys"
                       % (len(valid_keys_dest), len(deleted_keys_dest)))

        if filter_exp:
            # If key based adv filter
            if "META().id" in filter_exp:
                filter_exp = filter_exp.split('\'')[1]

            filtered_src_keys = filter(
                lambda key: re.search(str(filter_exp), key) is not None,
                valid_keys_src
            )
            valid_keys_src = filtered_src_keys
            self.log.debug(
                "{0} keys matched the filter expression {1}".format(
                    len(valid_keys_src),
                    filter_exp))

        for key in valid_keys_src:
            # replace/add the values for each key in src kvs
            if key not in deleted_keys_dest:
                partition1 = kv_src_bucket[kvs_num].acquire_partition(key)
                partition2 = kv_dest_bucket[kvs_num].acquire_partition(key)
                # In case of lww, if source's key timestamp is lower than
                # destination than no need to set.
                if self.__lww and partition1.get_timestamp(
                        key) < partition2.get_timestamp(key):
                    continue
                key_add = partition1.get_key(key)
                partition2.set(
                    key,
                    key_add["value"],
                    key_add["expires"],
                    key_add["flag"])
                kv_src_bucket[kvs_num].release_partition(key)
                kv_dest_bucket[kvs_num].release_partition(key)

        for key in deleted_keys_src:
            if key not in deleted_keys_dest:
                partition1 = kv_src_bucket[kvs_num].acquire_partition(key)
                partition2 = kv_dest_bucket[kvs_num].acquire_partition(key)
                # In case of lww, if source's key timestamp is lower than
                # destination than no need to delete.
                if self.__lww and partition1.get_timestamp(
                        key) < partition2.get_timestamp(key):
                    continue
                partition2.delete(key)
                kv_src_bucket[kvs_num].release_partition(key)
                kv_dest_bucket[kvs_num].release_partition(key)

        valid_keys_dest, deleted_keys_dest = kv_dest_bucket[
            kvs_num].key_set()
        self.log.debug("After merging: destination bucket's kv_store now has "
                       "{0} valid keys and {1} deleted keys"
                       .format(len(valid_keys_dest), len(deleted_keys_dest)))

    def __execute_query(self, server, query):
        try:
            res = RestConnection(server).query_tool(query)
            if "COUNT" in query:
                return (int(res["results"][0]['$1']))
            else:
                return 0
        except Exception as e:
            self.fail(
                "Errors encountered while executing query {0} on {1} : {2}".format(query, server, e.message))

    def _create_index(self, server, bucket):
        query_check_index_exists = "SELECT COUNT(*) FROM system:indexes " \
                                   "WHERE name=`" + bucket + "_index`"
        if not self.__execute_query(server, query_check_index_exists):
            self.__execute_query(server, "CREATE PRIMARY INDEX `" + bucket + "_index` "
                                 + "ON `" + bucket + '`')

    def _get_doc_count(self, server, bucket, exp):
            doc_count = self.__execute_query(server, "SELECT COUNT(*) FROM `"
                                             + bucket +
                                             "` WHERE " + exp)
            return doc_count if doc_count else 0

    def verify_filtered_items(self, src_master, dest_master, replications, filter_exp=None, skip_index=False):
        dest_count = 0
        for repl in replications:
            bucket = repl['source']
            if not skip_index:
                self._create_index(src_master, bucket)
                self._create_index(dest_master, bucket)
            for exp in filter_exp:
                dest_count += self._get_doc_count(dest_master, bucket, exp)
        return dest_count

class ValidateAuditEvent:
    @staticmethod
    def validate_audit_event(event_id, master_node, expected_results):
        if CHECK_AUDIT_EVENT.CHECK:
            audit_obj = audit(event_id, master_node)
            field_verified, value_verified = audit_obj.validateEvents(
                expected_results)
            raise_if(
                not field_verified,
                XDCRException("One of the fields is not matching"))
            raise_if(
                not value_verified,
                XDCRException("Values for one of the fields is not matching"))


class XDCRRemoteClusterRef:
    """Class keep the information related to Remote Cluster References.
    """

    def __init__(self, src_cluster, dest_cluster, name, encryption=False, replicator_target_role=False):
        """
        @param src_cluster: source couchbase cluster object.
        @param dest_cluster: destination couchbase cluster object:
        @param name: remote cluster reference name.
        @param encryption: True to enable SSL encryption for replication else
                        False
        """
        self.__src_cluster = src_cluster
        self.__dest_cluster = dest_cluster
        self.name = name
        self.__encryption = encryption
        self.__rest_info = {}
        self.__replicator_target_role = replicator_target_role
        self.__use_scramsha = TestInputSingleton.input.param("use_scramsha", False)

        # List of XDCReplication objects
        self.__replications = []

    def __str__(self):
        return "{0} -> {1}, Name: {2}".format(
            self.__src_cluster.name, self.__dest_cluster.name,
            self.name)

    def get_cb_clusters(self):
        return self.__cb_clusters

    def get_src_cluster(self):
        return self.__src_cluster

    def get_dest_cluster(self):
        return self.__dest_cluster

    def get_name(self):
        return self.name

    def get_replications(self):
        return self.__replications

    def get_rest_info(self):
        return self.__rest_info

    def get_replication_for_bucket(self, bucket):
        for replication in self.__replications:
            if replication.get_src_bucket().name == bucket.name:
                return replication
        return None

    def __get_event_expected_results(self):
        expected_results = {
            "real_userid:source": "ns_server",
            "real_userid:user": self.__src_cluster.master.rest_username,
            "cluster_name": self.name,
            "cluster_hostname": "%s:%s" % (
                self.__dest_cluster.master.ip, self.__dest_cluster.master.port),
            "is_encrypted": self.__encryption,
            "encryption_type": ""}

        return expected_results

    def __validate_create_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.CREATE_CLUSTER,
            self.__src_cluster.master,
            self.__get_event_expected_results())

    def add(self):
        """create cluster reference- add remote cluster
        """
        rest_conn_src = RestConnection(self.__src_cluster.master)
        certificate = ""
        dest_master = self.__dest_cluster.master
        if self.__encryption:
            rest_conn_dest = RestConnection(dest_master)
            certificate = rest_conn_dest.get_cluster_ceritificate()

        if self.__replicator_target_role:
            self.dest_user = "replicator_user"
            self.dest_pass = "password"
        else:
            self.dest_user = dest_master.rest_username
            self.dest_pass = dest_master.rest_password

        if not self.__use_scramsha:
            self.__rest_info = rest_conn_src.add_remote_cluster(
                dest_master.ip, dest_master.port,
                self.dest_user,
                self.dest_pass, self.name,
                demandEncryption=self.__encryption,
                certificate=certificate)
        else:
            print("Using scram-sha authentication")
            self.__rest_info = rest_conn_src.add_remote_cluster(
                dest_master.ip, dest_master.port,
                self.dest_user,
                self.dest_pass, self.name,
                demandEncryption=self.__encryption,
                encryptionType="half"
            )

        self.__validate_create_event()

    def __validate_modify_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.MOD_CLUSTER,
            self.__src_cluster.master,
            self.__get_event_expected_results())

    def use_scram_sha_auth(self):
        self.__use_scramsha = True
        self.__encryption = True
        self.modify()

    def modify(self, encryption=True):
        """Modify cluster reference to enable SSL encryption
        """
        dest_master = self.__dest_cluster.master
        rest_conn_src = RestConnection(self.__src_cluster.master)
        certificate = ""
        if encryption:
            rest_conn_dest = RestConnection(dest_master)
            if not self.__use_scramsha:
                certificate = rest_conn_dest.get_cluster_ceritificate()
                self.__rest_info = rest_conn_src.modify_remote_cluster(
                    dest_master.ip, dest_master.port,
                    self.dest_user,
                    self.dest_pass, self.name,
                    demandEncryption=encryption,
                    certificate=certificate)
            else:
                print("Using scram-sha authentication")
                self.__rest_info = rest_conn_src.modify_remote_cluster(
                    dest_master.ip, dest_master.port,
                    self.dest_user,
                    self.dest_pass, self.name,
                    demandEncryption=encryption,
                    encryptionType="half")
        self.__encryption = encryption
        self.__validate_modify_event()

    def __validate_remove_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.RM_CLUSTER,
            self.__src_cluster.master,
            self.__get_event_expected_results())

    def remove(self):
        RestConnection(
            self.__src_cluster.master).remove_remote_cluster(
            self.name)
        self.__validate_remove_event()

    def create_replication(
            self, fromBucket,
            rep_type=REPLICATION_PROTOCOL.XMEM,
            toBucket=None):
        """Create replication objects, but replication will not get
        started here.
        """
        self.__replications.append(
            XDCReplication(
                self,
                fromBucket,
                rep_type,
                toBucket))

    def clear_all_replications(self):
        self.__replications = []

    def start_all_replications(self):
        """Start all created replication
        """
        [repl.start() for repl in self.__replications]

    def pause_all_replications(self, verify=False):
        """Pause all created replication
        """
        [repl.pause(verify=verify) for repl in self.__replications]

    def pause_all_replications_by_id(self, verify=False):
        [repl.pause(repl_id=repl.get_repl_id(), verify=verify) for repl in self.__replications]

    def resume_all_replications(self, verify=False):
        """Resume all created replication
        """
        [repl.resume(verify=verify) for repl in self.__replications]

    def resume_all_replications_by_id(self, verify=False):
        [repl.resume(repl_id=repl.get_repl_id(), verify=verify) for repl in self.__replications]

    def stop_all_replications(self):
        rest = RestConnection(self.__src_cluster.master)
        rest_all_repls = rest.get_replications()
        for repl in self.__replications:
            for rest_all_repl in rest_all_repls:
                if repl.get_repl_id() == rest_all_repl['id']:
                    repl.cancel(rest, rest_all_repl)
        self.clear_all_replications()


class XDCReplication:

    def __init__(self, remote_cluster_ref, from_bucket, rep_type, to_bucket):
        """
        @param remote_cluster_ref: XDCRRemoteClusterRef object
        @param from_bucket: Source bucket (Bucket object)
        @param rep_type: replication protocol REPLICATION_PROTOCOL.CAPI/XMEM
        @param to_bucket: Destination bucket (Bucket object)
        """
        self.__input = TestInputSingleton.input
        self.__remote_cluster_ref = remote_cluster_ref
        self.__from_bucket = from_bucket
        self.__to_bucket = to_bucket or from_bucket
        self.__src_cluster = self.__remote_cluster_ref.get_src_cluster()
        self.__dest_cluster = self.__remote_cluster_ref.get_dest_cluster()
        self.__src_cluster_name = self.__src_cluster.name
        self.__rep_type = rep_type
        self.__test_xdcr_params = {}
        self.__updated_params = {}

        self.__parse_test_xdcr_params()
        self.log = logger.get("test")

        # Response from REST API
        self.__rep_id = None

    def __str__(self):
        return "Replication {0}:{1} -> {2}:{3}".format(
            self.__src_cluster.name,
            self.__from_bucket.name, self.__dest_cluster.name,
            self.__to_bucket.name)

    # get per replication params specified as from_bucket@cluster_name=<setting>:<value>
    # eg. default@C1=filter_expression:loadOne,failure_restart_interval:20
    def __parse_test_xdcr_params(self):
        param_str = self.__input.param(
            "%s@%s" %
            (self.__from_bucket, self.__src_cluster_name), None)
        if param_str:
            argument_split = re.split('[:,]', param_str)
            self.__test_xdcr_params.update(
                dict(zip(argument_split[::2], argument_split[1::2]))
            )
        if 'filter_expression' in self.__test_xdcr_params:
            if len(self.__test_xdcr_params['filter_expression']) > 0:
                ex = self.__test_xdcr_params['filter_expression']
                if ex.startswith("random"):
                    ex = self.__get_random_filter(ex)
                masked_input = {"comma": ',', "star": '*', "dot": '.', "equals": '=', "{": '', "}": '', "colon": ':'}
                for _ in masked_input:
                    ex = ex.replace(_, masked_input[_])
                self.__test_xdcr_params['filter_expression'] = ex

    def __convert_test_to_xdcr_params(self):
        xdcr_params = {}
        xdcr_param_map = TEST_XDCR_PARAM.get_test_to_create_repl_param_map()
        for test_param, value in self.__test_xdcr_params.iteritems():
            xdcr_params[xdcr_param_map[test_param]] = value
        return xdcr_params

    def get_filter_exp(self):
        if TEST_XDCR_PARAM.FILTER_EXP in self.__test_xdcr_params:
            return self.__test_xdcr_params[TEST_XDCR_PARAM.FILTER_EXP]
        return None

    def get_src_bucket(self):
        return self.__from_bucket

    def get_dest_bucket(self):
        return self.__to_bucket

    def get_src_cluster(self):
        return self.__src_cluster

    def get_dest_cluster(self):
        return self.__dest_cluster

    def get_repl_id(self):
        return self.__rep_id

    def __get_event_expected_results(self):
        expected_results = {
            "real_userid:source": "ns_server",
            "real_userid:user": self.__src_cluster.master.rest_username,
            "local_cluster_name": "%s:%s" % (
                self.__src_cluster.master.ip, self.__src_cluster.master.port),
            "source_bucket_name": self.__from_bucket.name,
            "remote_cluster_name": self.__remote_cluster_ref.name,
            "target_bucket_name": self.__to_bucket.name
        }
        # optional audit param
        if self.get_filter_exp():
            expected_results["filter_expression"] = self.get_filter_exp()
        return expected_results

    def __validate_update_repl_event(self):
        expected_results = {
            "settings": {
                "continuous": 'true',
                "target": self.__to_bucket.name,
                "source": self.__from_bucket.name,
                "type": "xdc-%s" % self.__rep_type
            },
            "id": self.__rep_id,
            "real_userid:source": "ns_server",
            "real_userid:user": self.__src_cluster.master.rest_username,
        }
        expected_results["settings"].update(self.__updated_params)

    def __validate_set_param_event(self):
        expected_results = self.__get_event_expected_results()
        expected_results["updated_settings"] = self.__updated_params
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.IND_SETT,
            self.get_src_cluster().master, expected_results)

    def get_xdcr_setting(self, param):
        """Get a replication setting value
        """
        src_master = self.__src_cluster.master
        return RestConnection(src_master).get_xdcr_param(
            self.__from_bucket.name,
            self.__to_bucket.name,
            param)

    def set_xdcr_param(self, param, value, verify_event=True):
        """Set a replication setting to a value
        """
        src_master = self.__src_cluster.master
        RestConnection(src_master).set_xdcr_param(
            self.__from_bucket.name,
            self.__to_bucket.name,
            param,
            value)
        print("Updated {0}={1} on bucket'{2}' on {3}".format(param, value, self.__from_bucket.name,
                                                                     self.__src_cluster.master.ip))
        self.__updated_params[param] = value
        if verify_event:
            self.__validate_set_param_event()

    def __validate_start_audit_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.CREATE_REPL,
            self.get_src_cluster().master,
            self.__get_event_expected_results())

    def start(self):
        """Start replication"""
        src_master = self.__src_cluster.master
        rest_conn_src = RestConnection(src_master)
        self.__rep_id = rest_conn_src.start_replication(
            REPLICATION_TYPE.CONTINUOUS,
            self.__from_bucket,
            self.__remote_cluster_ref.name,
            rep_type=self.__rep_type,
            toBucket=self.__to_bucket,
            xdcr_params=self.__convert_test_to_xdcr_params())
        self.__validate_start_audit_event()
        # if within this 10s for pipeline updater if we try to create another replication, it doesn't work until the previous pipeline is updated.
        # but better to have this 10s sleep between replications.
        sleep(10, "Wait between replications")

    def __verify_pause(self):
        """Verify if replication is paused"""
        src_master = self.__src_cluster.master
        # Is bucket replication paused?
        if not RestConnection(src_master).is_replication_paused(
                self.__from_bucket.name,
                self.__to_bucket.name):
            raise XDCRException(
                "XDCR is not paused for SrcBucket: {0}, Target Bucket: {1}".
                    format(self.__from_bucket.name,
                           self.__to_bucket.name))

    def __validate_pause_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.PAUSE_REPL,
            self.get_src_cluster().master,
            self.__get_event_expected_results())

    def pause(self, repl_id=None, verify=False):
        """Pause replication"""
        src_master = self.__src_cluster.master
        if repl_id:
            if not RestConnection(src_master).is_replication_paused_by_id(repl_id):
                RestConnection(src_master).pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')
        else:
            if not RestConnection(src_master).is_replication_paused(
                    self.__from_bucket.name, self.__to_bucket.name):
                self.set_xdcr_param(
                    REPL_PARAM.PAUSE_REQUESTED,
                    'true',
                    verify_event=False)

        self.__validate_pause_event()

        if verify:
            self.__verify_pause()

    def _is_cluster_replicating(self):
        count = 0
        src_master = self.__src_cluster.master
        while count < 3:
            outbound_mutations = self.__src_cluster.get_xdcr_stat(
                self.__from_bucket.name,
                'replication_changes_left')
            if outbound_mutations == 0:
                print(
                    "Outbound mutations on {0} is {1}".format(
                        src_master.ip,
                        outbound_mutations))
                count += 1
                continue
            else:
                print(
                    "Outbound mutations on {0} is {1}".format(
                        src_master.ip,
                        outbound_mutations))
                print("Node {0} is replicating".format(src_master.ip))
                break
        else:
            print(
                "Outbound mutations on {0} is {1}".format(
                    src_master.ip,
                    outbound_mutations))
            print(
                "Cluster with node {0} is not replicating".format(
                    src_master.ip))
            return False
        return True

    def __verify_resume(self):
        """Verify if replication is resumed"""
        src_master = self.__src_cluster.master
        # Is bucket replication paused?
        if RestConnection(src_master).is_replication_paused(self.__from_bucket.name,
                                                            self.__to_bucket.name):
            raise XDCRException(
                "Replication is not resumed for SrcBucket: {0}, \
                Target Bucket: {1}".format(self.__from_bucket, self.__to_bucket))

        if not self._is_cluster_replicating():
            self.log.debug("XDCR completed on {0}".format(src_master.ip))

    def __validate_resume_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.RESUME_REPL,
            self.get_src_cluster().master,
            self.__get_event_expected_results())

    def resume(self, repl_id=None, verify=False):
        """Resume replication if paused"""
        src_master = self.__src_cluster.master
        if repl_id:
            if RestConnection(src_master).is_replication_paused_by_id(repl_id):
                RestConnection(src_master).pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')
        else:
            if RestConnection(src_master).is_replication_paused(
                    self.__from_bucket.name, self.__to_bucket.name):
                self.set_xdcr_param(
                    REPL_PARAM.PAUSE_REQUESTED,
                    'false',
                    verify_event=False)

        self.__validate_resume_event()

        if verify:
            self.__verify_resume()

    def __validate_cancel_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.CAN_REPL,
            self.get_src_cluster().master,
            self.__get_event_expected_results())

    def cancel(self, rest, rest_all_repl):
        rest.stop_replication(rest_all_repl["cancelURI"])
        self.__validate_cancel_event()
