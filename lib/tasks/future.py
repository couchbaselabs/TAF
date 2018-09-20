import logging
import threading
import traceback
import time
from java.util.concurrent import Future

class Future(Future):
    def __init__(self):
        pass

    def set_result(self, result):
        self.result = result

    def result(self, timeout=None):
        try:
            self.future.get(timeout, TimeUnit.SECONDS)
            if self.state == FINISHED:
                return self.result
        except CancellationException, ex:
            self.state = CANCELLED
            self.result = False
        except TimeoutException, ex:
            log.error("Task Timed Out")

    def cancel(self, interrupt_if_running=True):
        check = self.future.cancel(interrupt_if_running)
        if check:
            self.state = CANCELLED
            raise CancellationException
        elif self.future.isDone():
            self.state = FINISHED

    def cancelled(self):
        return self.future.isCancelled()

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
                time.sleep(2**attempt * interval)
                attempt += 1
                value = value_getter()
            else:
                raise TimeoutException('Timed out after {0} seconds and {1} attempts'
                                   .format(now - start_time, attempt))
        return value

