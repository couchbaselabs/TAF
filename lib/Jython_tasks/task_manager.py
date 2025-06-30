import itertools
import time
from concurrent.futures import Future, ThreadPoolExecutor

from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader, SiriusJavaMongoLoader
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
        if isinstance(task, SiriusCouchbaseLoader) or isinstance(task, SiriusJavaMongoLoader):
            if not task.start_task():
                raise Exception(f"Failed to add new task {task.thread_name}")
            return
        # Regular framework Task object handling
        future = self.pool.submit(task.call)
        self.futures[task.thread_name] = future
        self.log.info("Added new task: %s" % task.thread_name)

    def get_task_result(self, task):
        self.log.debug("Getting task result for %s" % task.thread_name)
        if isinstance(task, SiriusCouchbaseLoader) or isinstance(task, SiriusJavaMongoLoader):
            okay = task.get_task_result()
            if not okay:
                self.log.critical("Failure during get_task_result of "
                                  f"{task.thread_name}")
            return okay
        # Regular framework Task object handling
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
        if isinstance(task, SiriusCouchbaseLoader) or isinstance(task, SiriusJavaMongoLoader):
            okay, json_response = task.end_task()
            if not okay or not json_response["status"]:
                self.log.critical(f"Some error during stop task of "
                                  f"{task.thread_name}: {json_response}")
            return okay

        # Regular framework Task Object
        if task.thread_name not in self.futures.keys():
            return
        future = self.futures[task.thread_name]
        i = 0
        while not future.done() and i < 30:
            sleep(1,
                  "Wait for %s to complete. Current status: %s"
                  % (task.thread_name, future.done()),
                  log_type="infra")
            i += 1
        else:
            self.log.debug("Task %s in already finished. No need to stop task"
                           % task.thread_name)
        if not future.done():
            self.log.debug("Stopping task %s" % task.thread_name)
            future.cancel()

    def shutdown(self, timeout=5):
        self.log.info("Running TaskManager shutdown")
        self.pool.shutdown()
        time.sleep(5)
        self.pool.shutdown(wait=True)

    def print_tasks_in_pool(self):
        for task_name, future in self.futures.items():
            if not future.done():
                self.log.warning("Task '%s' not completed" % task_name)

    def abort_all_tasks(self):
        for task_name, future in self.futures.items():
            self.log.info("Stopping task {0}".format(task_name))
            result = future.cancel()
            if result:
                self.log.debug("Stopped task {0}".format(task_name))
            else:
                self.log.debug("Task {0} could not be cancelled or already finished."
                               .format(task_name))


class Task(Future):
    serial_number = itertools.count()

    def __init__(self, thread_name):
        self.thread_name = "{}_{}".format(thread_name, next(Task.serial_number))
        self.exception = None
        self.completed = False
        self.started = False
        self.start_time = None
        self.end_time = None
        self.log = logger.get("infra")
        self.test_log = logger.get("test")
        self.result = False
        self.sleep = sleep
        self.state = None

    def __str__(self):
        if self.exception:
            raise self.exception
        elif self.completed:
            self.log.info("Task %s completed on: %s"
                          % (self.thread_name,
                             str(time.strftime("%H:%M:%S",
                                               time.gmtime(self.end_time)))))
            return "%s task completed in %.2fs" % \
                (self.thread_name, self.completed - self.started,)
        elif self.started:
            return "Thread %s at %s" % \
                (self.thread_name,
                 str(time.strftime("%H:%M:%S",
                                   time.gmtime(self.start_time))))
        else:
            return "[%s] not yet scheduled" % self.thread_name

    def start_task(self):
        self.started = True
        self.start_time = time.time()
        self.log.info("Thread '%s' started" % self.thread_name)

    def set_exception(self, exception):
        self.exception = exception
        self.complete_task()
        raise Exception(self.exception)

    def set_warn(self, exception):
        self.exception = exception
        self.complete_task()
        self.log.warn("Warning from '%s': %s" % (self.thread_name, exception))

    def complete_task(self):
        self.completed = True
        self.end_time = time.time()
        self.log.info("Thread '%s' completed" % self.thread_name)

    def set_result(self, result):
        self.result = result

    def call(self):
        raise NotImplementedError

    @staticmethod
    def wait_until(value_getter, condition, timeout_secs=300):
        """
        Repeatedly calls value_getter returning the value when it
        satisfies condition. Calls to value getter back off exponentially.
        Useful if you simply want to synchronously wait for a condition to be
        satisfied.

        :param value_getter: no-arg function that gets a value
        :param condition: single-arg function that tests the value
        :param timeout_secs: number of seconds after which to timeout
                             default=300 seconds (5 mins.)
        :return: the value returned by value_getter
        :raises: Exception if the operation times out before
                 getting a value that satisfies condition
        """
        start_time = time.time()
        stop_time = start_time + timeout_secs
        interval = 0.01
        attempt = 0
        value = value_getter()
        logger.get("infra").debug(
            "Wait for expected condition to get satisfied")
        while not condition(value):
            now = time.time()
            if timeout_secs < 0 or now < stop_time:
                sleep(2 ** attempt * interval)
                attempt += 1
                value = value_getter()
            else:
                raise Exception('Timeout after {0} seconds and {1} attempts'
                                .format(now - start_time, attempt))
        return value
