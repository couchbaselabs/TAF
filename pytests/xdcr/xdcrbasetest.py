from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


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
    NO_EVICTION = "noEviction"


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
    SOURCE_NOZZLES = "sourceNozzlePerNode"
    TARGET_NOZZLES = "targetNozzlePerNode"
    BATCH_COUNT = "workerBatchSize"
    BATCH_SIZE = "docBatchSizeKb"
    LOG_LEVEL = "logLevel"
    MAX_REPLICATION_LAG = "maxExpectedReplicationLag"
    TIMEOUT_PERC = "timeoutPercentageCap"
    PAUSE_REQUESTED = "pauseRequested"


class TEST_XDCR_PARAM:
    FAILURE_RESTART = "failure_restart_interval"
    CHECKPOINT_INTERVAL = "checkpoint_interval"
    OPTIMISTIC_THRESHOLD = "optimistic_threshold"
    FILTER_EXP = "filter_expression"
    SOURCE_NOZZLES = "source_nozzles"
    TARGET_NOZZLES = "target_nozzles"
    BATCH_COUNT = "batch_count"
    BATCH_SIZE = "batch_size"
    LOG_LEVEL = "log_level"
    MAX_REPLICATION_LAG = "max_replication_lag"
    TIMEOUT_PERC = "timeout_percentage"

    @staticmethod
    def get_test_to_create_repl_param_map():
        return {
            TEST_XDCR_PARAM.FAILURE_RESTART: REPL_PARAM.FAILURE_RESTART,
            TEST_XDCR_PARAM.CHECKPOINT_INTERVAL: REPL_PARAM.CHECKPOINT_INTERVAL,
            TEST_XDCR_PARAM.OPTIMISTIC_THRESHOLD: REPL_PARAM.OPTIMISTIC_THRESHOLD,
            TEST_XDCR_PARAM.FILTER_EXP: REPL_PARAM.FILTER_EXP,
            TEST_XDCR_PARAM.SOURCE_NOZZLES: REPL_PARAM.SOURCE_NOZZLES,
            TEST_XDCR_PARAM.TARGET_NOZZLES: REPL_PARAM.TARGET_NOZZLES,
            TEST_XDCR_PARAM.BATCH_COUNT: REPL_PARAM.BATCH_COUNT,
            TEST_XDCR_PARAM.BATCH_SIZE: REPL_PARAM.BATCH_SIZE,
            TEST_XDCR_PARAM.MAX_REPLICATION_LAG: REPL_PARAM.MAX_REPLICATION_LAG,
            TEST_XDCR_PARAM.TIMEOUT_PERC: REPL_PARAM.TIMEOUT_PERC,
            TEST_XDCR_PARAM.LOG_LEVEL: REPL_PARAM.LOG_LEVEL
        }


class XDCR_PARAM:
    # Per-replication params (input)
    XDCR_FAILURE_RESTART = "xdcrFailureRestartInterval"
    XDCR_CHECKPOINT_INTERVAL = "xdcrCheckpointInterval"
    XDCR_OPTIMISTIC_THRESHOLD = "xdcrOptimisticReplicationThreshold"
    XDCR_FILTER_EXP = "xdcrFilterExpression"
    XDCR_SOURCE_NOZZLES = "xdcrSourceNozzlePerNode"
    XDCR_TARGET_NOZZLES = "xdcrTargetNozzlePerNode"
    XDCR_BATCH_COUNT = "xdcrWorkerBatchSize"
    XDCR_BATCH_SIZE = "xdcrDocBatchSizeKb"
    XDCR_LOG_LEVEL = "xdcrLogLevel"
    XDCR_MAX_REPLICATION_LAG = "xdcrMaxExpectedReplicationLag"
    XDCR_TIMEOUT_PERC = "xdcrTimeoutPercentageCap"


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

class XDCRNewBaseTest(BaseTestCase):


    def __init_parameters(self):
        self.__case_number = self.input.param("case_number", 0)
        self.__topology = self.input.param("ctopology", TOPOLOGY.CHAIN)
        # complex topology tests (> 2 clusters must specify chain_length >2)
        self.__chain_length = self.input.param("chain_length", 2)
        self.__rep_type = self.input.param("replication_type",REPLICATION_PROTOCOL.XMEM)
        self.__num_sasl_buckets = self.input.param("sasl_buckets", 0)
        self.__num_stand_buckets = self.input.param("standard_buckets", 0)

        self.__eviction_policy = self.input.param("eviction_policy",'valueOnly')
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
        self._create_default_bucket = self.input.param("default_bucket",True)
        self._rdirection = self.input.param("rdirection",
                            REPLICATION_DIRECTION.UNIDIRECTION)
        self._num_items = self.input.param("items", 1000)
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
        self._wait_for_expiration = self.input.param("wait_for_expiration",True)
        self._warmup = self.input.param("warm", "").split('-')
        self._rebalance = self.input.param("rebalance", "").split('-')
        self._failover = self.input.param("failover", "").split('-')
        self._wait_timeout = self.input.param("timeout", 60)
        self._disable_compaction = self.input.param("disable_compaction","").split('-')
        self._item_count_timeout = self.input.param("item_count_timeout", 300)
        self._checkpoint_interval = self.input.param("checkpoint_interval",60)
        self._optimistic_threshold = self.input.param("optimistic_threshold", 256)
        self._dgm_run = self.input.param("dgm_run", False)
        self._active_resident_threshold = \
            self.input.param("active_resident_threshold", 100)
        CHECK_AUDIT_EVENT.CHECK = self.input.param("verify_audit", 0)
        self._max_verify = self.input.param("max_verify", 100000)
        self._sdk_compression = self.input.param("sdk_compression", True)
        self._evict_with_compactor = self.input.param("evict_with_compactor", False)
        self._replicator_role = self.input.param("replicator_role",False)
        self._replicator_all_buckets = self.input.param("replicator_all_buckets",False)

    def get_goxdcr_log_dir(self, node):
        """Gets couchbase log directory, even for cluster_run
        """
        _, dir = RestConnection(node).diag_eval('filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
        return str(dir)

    def check_goxdcr_log(self, server, str, goxdcr_log=None, print_matches=None):
        """ Checks if a string 'str' is present in goxdcr.log on server
            and returns the number of occurances
            @param goxdcr_log: goxdcr log location on the server
        """
        if not goxdcr_log:
            goxdcr_log = self.get_goxdcr_log_dir(server)\
                     + '/goxdcr.log*'

        shell = RemoteMachineShellConnection(server)
        info = shell.extract_remote_info().type.lower()
        if info == "windows":
            matches = []
            if print_matches:
                matches, err = shell.execute_command("grep \"{0}\" {1}".
                                            format(str, goxdcr_log))
                if matches:
                    self.log.info(matches)

            count, err = shell.execute_command("grep \"{0}\" {1} | wc -l".
                                            format(str, goxdcr_log))
        else:
            matches = []
            if print_matches:
                matches, err = shell.execute_command("zgrep \"{0}\" {1}".
                                                     format(str, goxdcr_log))
                if matches:
                    self.log.info(matches)

            count, err = shell.execute_command("zgrep \"{0}\" {1} | wc -l".
                                               format(str, goxdcr_log))
        if isinstance(count, list):
            count = int(count[0])
        else:
            count = int(count)
        self.log.info(count)
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
            goxdcr_log = self.get_goxdcr_log_dir(self.input.servers[0])\
                     + '/goxdcr.log*'
        for node in self.input.servers:
            if self.__is_cluster_run():
                goxdcr_log = self.get_goxdcr_log_dir(node)\
                     + '/goxdcr.log*'
                self._repl_restart_count_dict[node.ip] = \
                    self.check_goxdcr_log(node,
                                                "Try to fix Pipeline",
                                                goxdcr_log)
        self.log.info(self._repl_restart_count_dict)

    def __initialize_error_count_dict(self):
        """
            initializes self.__error_count_dict with ip, error and err count
            like {ip1: {"panic": 2, "KEY_ENOENT":3}}
        """
        if not self.__is_cluster_run():
            goxdcr_log = self.get_goxdcr_log_dir(self.input.servers[0])\
                     + '/goxdcr.log*'
        for node in self.input.servers:
            if self.__is_cluster_run():
                goxdcr_log = self.get_goxdcr_log_dir(node)\
                     + '/goxdcr.log*'
            self.__error_count_dict[node.ip] = {}
            for error in self.__report_error_list:
                self.__error_count_dict[node.ip][error] = \
                    self.check_goxdcr_log(node, error, goxdcr_log)
        self.log.info(self.__error_count_dict)

