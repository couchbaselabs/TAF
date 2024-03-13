import time
from concurrent.futures import ThreadPoolExecutor

from common_lib import sleep
from global_vars import logger


class TaskManager(object):
    def __init__(self, max_workers=100):
        self.log = logger.get("infra")
        self.log.info(f"Initiating TaskManager with {max_workers} threads")
        self.number_of_threads = max_workers
        self.pool = ThreadPoolExecutor(self.number_of_threads,
                                       thread_name_prefix="ThreadPool")
        self.futures = dict()

    def add_new_task(self, task):
        future = self.pool.submit(task.call)
        self.futures[task.thread_name] = future
        self.log.info("Added new task: %s" % task.thread_name)

    def get_task_result(self, task):
        self.log.debug("Getting task result for %s" % task.thread_name)
        future = self.futures[task.thread_name]
        result = False
        try:
            result = future.result()
            exception = future.exception()
            if exception:
                self.log.critical(f"Exception in {task.thread_name}: {exception}")
        except Exception:
            self.log.warning("%s is already cancelled" % task.thread_name)

        self.futures.pop(task.thread_name)
        return result

    def schedule(self, task, sleep_time=0):
        if sleep_time > 0:
            sleep(sleep_time,
                  "Wait before scheduling task %s" % task.thread_name,
                  log_type="infra")
        self.add_new_task(task)

    def stop_task(self, task):
        if task.thread_name not in self.futures.keys():
            return
        future = self.futures[task.thread_name]
        i = 0
        while not future.isDone() and i < 30:
            sleep(1,
                  "Wait for %s to complete. Current status: %s"
                  % (task.thread_name, future.isDone()),
                  log_type="infra")
            i += 1
        else:
            self.log.debug("Task %s in already finished. No need to stop task"
                           % task.thread_name)
        if not future.isDone():
            self.log.debug("Stopping task %s" % task.thread_name)
            future.cancel(True)

    def shutdown(self, timeout=5):
        self.log.info("Running TaskManager shutdown")
        self.pool.shutdown()
        time.sleep(5)
        self.pool.shutdown(wait=True)

    def print_tasks_in_pool(self):
        for task_name, future in self.futures.items():
            if not future.isDone():
                self.log.warning("Task '%s' not completed" % task_name)

    def abort_all_tasks(self):
        for task_name, future in self.futures.items():
            self.log.info("Stopping task {0}".format(task_name))
            result = future.cancel(True)
            if result:
                self.log.debug("Stopped task {0}".format(task_name))
            else:
                self.log.debug("Task {0} could not be cancelled or already finished."
                               .format(task_name))
