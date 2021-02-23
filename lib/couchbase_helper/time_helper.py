import re
import datetime


class TimeUtil:
    """ A class for time utility functions
    """

    @staticmethod
    def rfc3339nano_to_datetime(my_time):
        """ Converts a cbbackupmgr RFC3399Nano timestamp to a datetime object ignoring the nanoseconds and timezone
        """
        my_time = my_time.replace('_', ':')  # Replace undercores with colons
        my_time = re.sub(r"\.\d*", "", my_time)  # Strip nanoseconds
        my_time = re.sub(r"([zZ]|(-|\+)\d{2}:\d{2})$", "", my_time)  # Strip timezone information
        return datetime.datetime.strptime(my_time, "%Y-%m-%dT%H:%M:%S")  # Parse string to datetime
