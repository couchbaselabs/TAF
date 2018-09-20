import logging
import sys
from java.util.concurrent import Executors, TimeUnit
from tasks import Task

log = logging.getLogger(__name__)

MAX_CONCURRENT = 5

class TaskManager(object):
    def __init__(self, thread_name):
        self.thread_pool_name = thread_name
        self.thread_pool = Executors.newScheduledThreadPool(MAX_CONCURRENT)
        self.futures = []

    def schedule(self, task, sleep_time=0):
        if not isinstance(task, Task):
            raise TypeError("Tried to schedule something that's not a task")
        future = self.thread_pool.schedule(task, sleep_time, TimeUnit.SECONDS)
        task.future = future
        self.futures.append(future)
        return future

    def shutdown(self, force=False, timeout=None):
        self.thread_pool.shutdown()
        if force:
            self.thread_pool.setExecuteExistingDelayedTasksAfterShutdownPolicy(False)
        try:
            if not self.thread_pool.awaitTermination(timeout, TimeUnit.SECONDS):
                self.thread_pool.shutdownNow()
                if not self.thread_pool.awaitTermination(timeout, TimeUnit.SECONDS):
                    log.error("{0}: Pool did not terminate".format(sys.stderr))
        except Exception, ex:
            # (Re-)Cancel if current thread also interrupted
            self.thread_pool.shutdownNow()