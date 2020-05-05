import inspect
import time

from global_vars import logger


def sleep(seconds, message=None, log_type="test"):
    """
    :param seconds: Time to sleep in seconds
    :param message: Reason for sleep from the caller
    :param log_type: Log handle to use. Example: test / infra
    :return None:
    """
    log = logger.get(log_type)
    sleep_msg = "Sleep %s seconds. Reason: %s" % (seconds, message)

    log.debug("Sleep is called from %s -> %s():L%s"
              % (inspect.stack()[1][1],
                 inspect.stack()[1][3],
                 inspect.stack()[1][2]))
    if message:
        log.info(sleep_msg)
    else:
        log.debug(sleep_msg)

    time.sleep(seconds)
