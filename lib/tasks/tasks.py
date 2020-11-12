import copy
import time
import traceback

from BucketLib.BucketOperations import BucketHelper
from BucketLib.MemcachedOperations import MemcachedHelper
from common_lib import sleep
from global_vars import logger
from membase.api.exception import FailoverFailedException, \
    ServerUnavailableException, BucketFlushFailed
from membase.api.rest_client import RestConnection
from testconstants import MIN_KV_QUOTA, INDEX_QUOTA, FTS_QUOTA, CBAS_QUOTA
from java.util.concurrent import Callable, TimeUnit, CancellationException, \
    TimeoutException


CHECK_FLAG = False

PENDING = 'PENDING'
EXECUTING = 'EXECUTING'
CHECKING = 'CHECKING'
FINISHED = 'FINISHED'
CANCELLED = 'CANCELLED'


class Task(Callable):
    def __init__(self, name, task_manager):
        self.name = name
        self.thread_name = name
        self.task_manager = task_manager
        self.state = EXECUTING
        self.result = None
        self.future = None
        self.log = logger.get("infra")
        self.test_log = logger.get("test")
        self.log.debug("*** TASK {0} Scheduled...".format(self.name))

    def call(self):
        self.log.debug("*** TASK  {0} in progress...".format(self.name))
        if self.state == PENDING:
            self.state = EXECUTING
            self.future = self.task_manager.schedule(self)
        if self.state == EXECUTING:
            self.execute()
        elif self.state == CHECKING:
            self.check()
        elif self.state != FINISHED:
            raise Exception("Bad State in {0}: {1}".format(self.name,
                                                           self.state))

    def execute(self):
        raise NotImplementedError

    def check(self):
        raise NotImplementedError

    def set_result(self, result):
        self.result = result

    def get_result(self, timeout=None):
        try:
            if timeout:
                self.future.get(timeout, TimeUnit.SECONDS)
            else:
                self.future.get()
            if self.state == FINISHED:
                self.log.debug("*** TASK  {0} Finished...".format(self.name))
                return self.result
        except CancellationException as ex:
            self.state = CANCELLED
            self.result = False
        except TimeoutException as ex:
            self.log.error("Task Timed Out")

    def cancel(self, interrupt_if_running=True):
        check = self.future.cancel(interrupt_if_running)
        if check:
            self.state = CANCELLED
            raise CancellationException
        elif self.future.isDone():
            self.state = FINISHED

    def cancelled(self):
        return self.future.isCancelled()

    def set_unexpected_exception(self, e, suffix=""):
        self.log.error("Unexpected exception [{0}] caught".format(e) + suffix)
        self.log.error(''.join(traceback.format_stack()))

    @staticmethod
    def wait_until(value_getter, condition, timeout_secs=-1):
        """
        Repeatedly calls value_getter returning the value when it
        satisfies condition. Calls to value getter back off exponentially.
        Useful if you simply want to synchronously wait for a condition to be
        satisfied.

        :param value_getter: no-arg function that gets a value
        :param condition: single-arg function that tests the value
        :param timeout_secs: number of seconds after which to timeout; if negative
                             waits forever; default is to wait forever
        :return: the value returned by value_getter
        :raises: TimeoutError if the operation times out before
                 getting a value that satisfies condition
        """
        start_time = time.time()
        stop_time = start_time + max(timeout_secs, 0)
        interval = 0.01
        attempt = 0
        value = value_getter()
        while not condition(value):
            now = time.time()
            if timeout_secs < 0 or now < stop_time:
                sleep(2**attempt * interval, log_type="infra")
                attempt += 1
                value = value_getter()
            else:
                raise TimeoutException(
                    'Timed out after {0} seconds and {1} attempts' .format(
                        now - start_time, attempt))
        return value


class FailoverTask(Task):
    def __init__(
            self,
            servers,
            task_manager,
            to_failover=[],
            wait_for_pending=0,
            graceful=False,
            use_hostnames=False):
        Task.__init__(self, "failover_task", task_manager)
        self.servers = servers
        self.to_failover = to_failover
        self.graceful = graceful
        self.wait_for_pending = wait_for_pending
        self.use_hostnames = use_hostnames

    def execute(self):
        try:
            self._failover_nodes(self.task_manager)
            self.test_log.debug("{0} seconds sleep after failover for nodes to go pending...."
                                .format(self.wait_for_pending))
            sleep(self.wait_for_pending)
            self.state = FINISHED
            self.set_result(True)

        except FailoverFailedException as e:
            self.state = FINISHED
            self. set_unexpected_exception(e)

        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def _failover_nodes(self, task_manager):
        rest = RestConnection(self.servers[0])
        # call REST fail_over for the nodes to be failed over
        for server in self.to_failover:
            for node in rest.node_statuses():
                if (server.hostname if self.use_hostnames else server.ip) == node.ip and int(
                        server.port) == int(node.port):
                    self.test_log.debug(
                        "Failing over {0}:{1} with graceful={2}"
                        .format(node.ip, node.port, self.graceful))
                    rest.fail_over(node.id, self.graceful)


class BucketFlushTask(Task):
    def __init__(self, server, task_manager, bucket="default"):
        Task.__init__(self, "bucket_flush_task", task_manager)
        self.server = server
        self.bucket = bucket
        if isinstance(bucket, Bucket):
            self.bucket = bucket.name

    def execute(self):
        try:
            rest = BucketHelper(self.server)
            if rest.flush_bucket(self.bucket):
                self.state = CHECKING
                self.task_manager.schedule(self)
            else:
                self.state = FINISHED
                self.set_result(False)

        except BucketFlushFailed as e:
            self.state = FINISHED
            self. set_unexpected_exception(e)

        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self):
        try:
            # check if after flush the vbuckets are ready
            if MemcachedHelper.wait_for_vbuckets_ready_state(
                    self.server, self.bucket):
                self.set_result(True)
            else:
                self.test_log.error(
                    "Unable to reach bucket {0} on server {1} after flush"
                    .format(self.bucket, self.server))
                self.set_result(False)
            self.state = FINISHED
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)
