import time


def retry_with_timeout(timeout, f):
    """ Retries until the condition is met and returns True, otherwise returns
    False. """
    endtime = time.time() + timeout

    while time.time() < endtime:
        if f():
            return True

    return False
