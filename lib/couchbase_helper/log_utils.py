"""
Log utility functions for timestamp parsing and log analysis.
These utilities are designed to be testable without requiring external cluster connections.
"""

import re
from datetime import datetime


def check_logs_for_timestamp(grep_output_list, start_timestamp):
    """
    Check grep output for timestamps and compare with test start time.
    Returns True only if a timestamp is found AND it's after test start time.
    Returns False if no timestamp is found or all timestamps are before test start.

    This function is designed to filter out errors from previous test runs by
    comparing log timestamps against the test's start timestamp.

    Args:
        grep_output_list: List of strings from grep output
        start_timestamp: datetime object representing test start time

    Returns:
        bool: True if error should be flagged (timestamp after test start),
              False if logs are from previous runs (timestamp before test start)

    Note:
        This method works only if slave's time(timezone) matches
        that of VM's. Else it won't be possible to compare timestamps.
    """
    # eg: 2021-07-12T04:03:45
    timestamp_regex = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    # Scan backwards from end to find the latest timestamp efficiently
    # Log files are chronological, so last timestamp found = latest
    for line in reversed(grep_output_list):
        match_obj = timestamp_regex.search(line)
        if match_obj:
            timestamp_str = match_obj.group()
            try:
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
                if timestamp > start_timestamp:
                    return True
                else:
                    return False
            except ValueError:
                continue

    # No timestamp found in any line
    return False


def parse_time_based_pattern(line, pattern, time_threshold):
    """
    Parse log line for time-based patterns like 'Slow operation' or 'Slow runtime'
    Returns True if the pattern is found and the time exceeds threshold

    Args:
        line: String line from log
        pattern: Pattern string to search for (e.g., "Slow operation")
        time_threshold: Time threshold in seconds

    Returns:
        bool: True if pattern found and time exceeds threshold
    """
    if pattern in line:
        # Extract time value from the line
        time_regex = re.compile(r"(\d+(?:\.\d+)?)\s*(?:ms|s|seconds?)")
        time_match = time_regex.search(line)
        if time_match:
            time_value = float(time_match.group(1))
            # Convert to seconds if in milliseconds
            if "ms" in line:
                time_value = time_value / 1000
            return time_value > time_threshold
    return False


def check_error_patterns(grep_output, pattern):
    """
    Check if grep output matches the error pattern.
    Returns tuple of (bool, int) where:
    - bool indicates if pattern was found
    - int is the index where pattern was found, or -1 if not found

    Args:
        grep_output: List of strings from grep output
        pattern: Error pattern (string or dict with time-based pattern)

    Returns:
        tuple: (found: bool, index: int)
    """
    if isinstance(pattern, dict):
        # Handle time-based pattern
        if 'string' in pattern and 'time_to_consider_in_seconds' in pattern:
            for i, line in enumerate(grep_output):
                if parse_time_based_pattern(
                        line, pattern['string'],
                        pattern['time_to_consider_in_seconds']):
                    return True, i
    else:
        for i, line in enumerate(grep_output):
            if pattern in line:
                return True, i
    return False, -1


def extract_timestamps_from_logs(log_lines):
    """
    Extract all timestamps from a list of log lines.

    Args:
        log_lines: List of log line strings

    Returns:
        list: List of datetime objects extracted from logs
    """
    timestamp_regex = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
    timestamps = []

    for line in log_lines:
        match_obj = timestamp_regex.search(line)
        if match_obj:
            timestamp_str = match_obj.group()
            try:
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
                timestamps.append(timestamp)
            except ValueError:
                continue

    return timestamps
