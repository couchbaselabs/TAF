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


def humanbytes(B):
    """Return the given bytes as a human friendly
    KB, MB, GB, or TB string"""

    B = float(B)
    KB = float(1024)
    MB = float(KB ** 2)  # 1,048,576
    GB = float(KB ** 3)  # 1,073,741,824
    TB = float(KB ** 4)  # 1,099,511,627,776

    if B < KB:
        return '{0} {1}'.format(B, 'Bytes' if 0 == B > 1 else 'Byte')
    elif KB <= B < MB:
        return '{0:.2f} KiB'.format(B / KB)
    elif MB <= B < GB:
        return '{0:.2f} MiB'.format(B / MB)
    elif GB <= B < TB:
        return '{0:.2f} GiB'.format(B / GB)
    elif TB <= B:
        return '{0:.2f} TiB'.format(B / TB)
