import logging
import threading

from java.util.concurrent import Executors, TimeUnit
from threading import InterruptedException
from time import sleep


class TaskManager:
    def __init__(self, number_of_threads=10):
        self.log = logging.getLogger("infra")
        self.log.debug("Initiating TaskManager with {0} threads"
                       .format(number_of_threads))
        self.number_of_threads = number_of_threads
        self.pool = Executors.newFixedThreadPool(self.number_of_threads)
        self.futures = {}
        self.tasks = []

    def add_new_task(self, task):
        future = self.pool.submit(task)
        self.futures[task.thread_name] = future
        self.tasks.append(task)
        self.log.debug("Added new task: {0}".format(task.thread_name))

    def get_all_result(self):
        return self.pool.invokeAll(self.tasks)

    def get_task_result(self, task):
        self.log.debug("Getting task result for {0}".format(task.thread_name))
        future = self.futures[task.thread_name]
        return future.get()

    def schedule(self, task, sleep_time=0):
        self.add_new_task(task)

    def stop_task(self, task):
        future = self.futures[task.thread_name]
        i = 0
        while not future.isDone() and i < 30:
            self.log.debug("task {0}{1}".format(task.thread_name, future.isDone()))
            sleep(1)
            i += 1
        else:
            self.log.debug("Task {0} in already finished. No need to stop task".format(task.thread_name))
        if not future.isDone():
            self.log.debug("Stopping task {0}".format(task.thread_name))
            future.cancel(True)

    def shutdown_task_manager(self, timeout=5):
        self.shutdown(timeout)

    def shutdown(self, timeout):
        self.log.debug("Running TaskManager shutdown")
        self.pool.shutdown()
        try:
            if not self.pool.awaitTermination(timeout, TimeUnit.SECONDS):
                self.pool.shutdownNow()
                if not self.pool.awaitTermination(timeout, TimeUnit.SECONDS):
                    self.log.error("Pool did not terminate")
        except InterruptedException, ex:
            self.log.error(ex)
            # (Re-)Cancel if current thread also interrupted
            self.pool.shutdownNow()
            # Preserve interrupt status
            threading.currentThread().interrupt()

    def print_tasks_in_pool(self):
        for task_name, future in self.futures.items():
            if not future.isDone():
                self.log.warning("Task '{0}' not completed".format(task_name))
