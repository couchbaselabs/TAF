from java.util.concurrent import Executors, TimeUnit
from threading import InterruptedException
import sys
import threading


class TaskManager():

    def __init__(self, number_of_threads=10):
        self.number_of_threads = number_of_threads
        self.pool = Executors.newFixedThreadPool(self.number_of_threads)
        self.futures = {}
        self.tasks = []

    def add_new_task(self, task):
        future = self.pool.submit(task)
        self.futures[task.thread_name] = future
        self.tasks.append(task)

    def get_all_result(self):
        return self.pool.invokeAll(self.tasks)

    def get_task_result(self, task):
        future = self.futures[task.thread_name]
        return future.get()

    def schedule(self, task, sleep_time=0):
        self.add_new_task(task)

    def stop_task(self, task):
        future = self.futures[task.thread_name]
        if not future.isDone():
            future.cancel(True)

    def shutdown_task_manager(self, timeout=5):
        self.shutdown(timeout)

    def shutdown(self, timeout):
        self.pool.shutdown()
        try:
            if not self.pool.awaitTermination(timeout, TimeUnit.SECONDS):
                self.pool.shutdownNow()
                if (not self.pool.awaitTermination(timeout, TimeUnit.SECONDS)):
                    print >> sys.stderr, "Pool did not terminate"
        except InterruptedException, ex:
            # (Re-)Cancel if current thread also interrupted
            self.pool.shutdownNow()
            # Preserve interrupt status
            threading.currentThread().interrupt()

    def print_tasks_in_pool(self):
        for task_name, future in self.futures.items():
            if not future.isDone():
                print "Task {} not completed".format(task_name)

# MAX_CONCURRENT = 5
# 
# 
# class TaskManager_temp(object):
#     def __init__(self, thread_name):
#         self.thread_pool_name = thread_name
#         self.thread_pool = Executors.newScheduledThreadPool(MAX_CONCURRENT)
#         self.futures = []
# 
#     def schedule(self, task, sleep_time=0):
#         if not isinstance(task, Task):
#             raise TypeError("Tried to schedule something that's not a task")
#         future = self.thread_pool.schedule(task, sleep_time, TimeUnit.SECONDS)
#         task.future = future
#         self.futures.append(future)
#         return future
# 
#     def shutdown(self, force=False, timeout=None):
#         self.thread_pool.shutdown()
#         if force:
#             self.thread_pool.setExecuteExistingDelayedTasksAfterShutdownPolicy(False)
#         try:
#             if not self.thread_pool.awaitTermination(timeout, TimeUnit.SECONDS):
#                 self.thread_pool.shutdownNow()
#                 if not self.thread_pool.awaitTermination(timeout, TimeUnit.SECONDS):
#                     log.error("{0}: Pool did not terminate".format(sys.stderr))
#         except Exception, ex:
#             # (Re-)Cancel if current thread also interrupted
#             self.thread_pool.shutdownNow()
