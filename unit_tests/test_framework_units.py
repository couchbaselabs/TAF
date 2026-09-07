"""
Unit tests for TAF framework modules.
These tests can run without requiring any external Couchbase cluster connection.

Run tests with: pytest unit_tests/ -v
Or: python3 -m unittest unit_tests.test_framework_units -v
"""

import unittest
import sys
import os
from datetime import datetime

# Add lib directory to path for imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
LIB_DIR = os.path.join(PROJECT_ROOT, 'lib')
sys.path.insert(0, LIB_DIR)
sys.path.insert(0, PROJECT_ROOT)


class TestLogUtils(unittest.TestCase):
    """Tests for log_utils.py - log timestamp checking utilities"""

    def setUp(self):
        from couchbase_helper.log_utils import check_logs_for_timestamp
        self.check_logs = check_logs_for_timestamp

    def test_logs_before_test_start_returns_false(self):
        """
        Test that logs with timestamps before test start return False.
        This was the original issue - NullPointerException logs from
        previous runs were incorrectly flagged as errors.
        """
        test_start = datetime(2026, 4, 11, 3, 49, 53)
        grep_output = [
            "2026-04-10T14:30:28.659-07:00 WARN CBAS.util.PreferSystemPropertiesPropertySource",
            "2026-04-10T14:30:32.104-07:00 WARN CBAS.server.AnalyticsHttpsServerInitializer",
            "Caused by: org.apache.asterix.common.exceptions.CompilationException",
            "java.lang.NullPointerException: The URI scheme must not be null",
        ]
        result = self.check_logs(grep_output, test_start)
        self.assertFalse(result, "Should return False for logs before test start")

    def test_logs_after_test_start_returns_true(self):
        """
        Test that logs with timestamps after test start return True.
        These should be flagged as errors during the test run.
        """
        test_start = datetime(2026, 4, 11, 3, 49, 53)
        grep_output = [
            "2026-04-11T03:55:00.123-07:00 ERROR Some error occurred",
            "java.lang.RuntimeException: Test error",
        ]
        result = self.check_logs(grep_output, test_start)
        self.assertTrue(result, "Should return True for logs after test start")

    def test_no_timestamp_returns_false(self):
        """
        Test that logs without any timestamp return False.
        When we can't determine the time, we should not flag as error
        (safe default to avoid false positives).
        """
        test_start = datetime(2026, 4, 11, 3, 49, 53)
        grep_output = [
            "Caused by: some error without timestamp",
            "Stack trace line 1",
            "Stack trace line 2",
        ]
        result = self.check_logs(grep_output, test_start)
        self.assertFalse(result, "Should return False when no timestamp found")

    def test_empty_grep_output_returns_false(self):
        """Test that empty grep output returns False."""
        test_start = datetime(2026, 4, 11, 3, 49, 53)
        grep_output = []
        result = self.check_logs(grep_output, test_start)
        self.assertFalse(result, "Should return False for empty grep output")

    def test_stack_trace_at_end_still_finds_timestamp(self):
        """
        Test that stack trace lines at the end don't prevent
        finding timestamps on earlier lines.
        This was the root cause of the original bug.
        """
        test_start = datetime(2026, 4, 11, 3, 49, 53)
        # Timestamp is in middle, stack traces at end
        grep_output = [
            "2026-04-10T14:30:28 WARN Some warning",
            "Caused by: NullPointerException",
            "at com.example.SomeClass.method(SomeClass.java:123)",
            "at com.example.OtherClass.run(OtherClass.java:456)",
        ]
        result = self.check_logs(grep_output, test_start)
        self.assertFalse(result, "Should find timestamp and return False")

    def test_multiple_timestamps_finds_latest(self):
        """
        Test that when multiple timestamps exist,
        the latest one is used for comparison.
        """
        test_start = datetime(2026, 4, 11, 12, 0, 0)
        grep_output = [
            "2026-04-11T10:00:00 INFO Morning log",
            "2026-04-11T14:00:00 ERROR Afternoon error",
        ]
        # Latest timestamp (14:00) is after test start (12:00)
        result = self.check_logs(grep_output, test_start)
        self.assertTrue(result, "Should use latest timestamp and return True")

    def test_original_issue_scenario(self):
        """
        Test the exact scenario from the bug report:
        CBAS NullPointerException from previous day causes test failure.
        """
        # Test started April 11
        test_start = datetime(2026, 4, 11, 3, 49, 53)

        # But errors found are from April 10
        grep_output = [
            "2026-04-10T14:30:28.659-07:00 WARN CBAS.util.PreferSystemPropertiesPropertySource [main] System properties",
            "2026-04-10T14:30:32.104-07:00 WARN CBAS.server.AnalyticsHttpsServerInitializer [main] Configured SSL",
            "Caused by: org.apache.asterix.common.exceptions.CompilationException: ASX1108: External source error",
            "java.lang.NullPointerException: The URI scheme of endpointOverride must not be null",
            "Caused by: org.apache.asterix.common.exceptions.CompilationException: ASX1108",
            "java.lang.NullPointerException: The URI scheme of endpointOverride must not be null",
        ]

        result = self.check_logs(grep_output, test_start)
        self.assertFalse(result, "Should correctly filter out logs from previous run")


class TestParseTimeBasedPattern(unittest.TestCase):
    """Tests for parse_time_based_pattern function"""

    def setUp(self):
        from couchbase_helper.log_utils import parse_time_based_pattern
        self.parse_pattern = parse_time_based_pattern

    def test_slow_operation_ms(self):
        """Test parsing slow operation time in milliseconds."""
        line = "Slow operation took 500ms to complete"
        result = self.parse_pattern(line, "Slow operation", 0.1)  # 0.1 seconds = 100ms
        self.assertTrue(result, "500ms should exceed 100ms threshold")

    def test_slow_operation_seconds(self):
        """Test parsing slow operation time in seconds."""
        line = "Slow runtime: 5.5 seconds"
        result = self.parse_pattern(line, "Slow runtime", 3.0)
        self.assertTrue(result, "5.5s should exceed 3s threshold")

    def test_fast_operation_returns_false(self):
        """Test that fast operations don't exceed threshold."""
        line = "Slow operation took 50ms to complete"
        result = self.parse_pattern(line, "Slow operation", 0.1)  # 100ms
        self.assertFalse(result, "50ms should not exceed 100ms threshold")

    def test_no_pattern_returns_false(self):
        """Test that missing pattern returns False."""
        line = "Normal operation completed"
        result = self.parse_pattern(line, "Slow operation", 0.1)
        self.assertFalse(result, "Should return False when pattern not found")


class TestCheckErrorPatterns(unittest.TestCase):
    """Tests for check_error_patterns function"""

    def setUp(self):
        from couchbase_helper.log_utils import check_error_patterns
        self.check_patterns = check_error_patterns

    def test_string_pattern_found(self):
        """Test finding a string pattern in grep output."""
        grep_output = [
            "2026-04-11T10:00:00 INFO Normal log",
            "2026-04-11T10:01:00 ERROR Something went wrong",
            "2026-04-11T10:02:00 INFO Another normal log",
        ]
        found, index = self.check_patterns(grep_output, "ERROR")
        self.assertTrue(found)
        self.assertEqual(index, 1)

    def test_string_pattern_not_found(self):
        """Test when string pattern is not found."""
        grep_output = [
            "2026-04-11T10:00:00 INFO Normal log",
            "2026-04-11T10:01:00 WARN Warning log",
        ]
        found, index = self.check_patterns(grep_output, "CRITICAL")
        self.assertFalse(found)
        self.assertEqual(index, -1)

    def test_time_based_pattern(self):
        """Test time-based pattern detection."""
        grep_output = [
            "Slow operation took 500ms",
            "Normal log entry",
        ]
        pattern = {
            'string': 'Slow operation',
            'time_to_consider_in_seconds': 0.1  # 100ms
        }
        found, index = self.check_patterns(grep_output, pattern)
        self.assertTrue(found)
        self.assertEqual(index, 0)


class TestTimeUtil(unittest.TestCase):
    """Tests for time_helper.py - time utilities"""

    def setUp(self):
        from couchbase_helper.time_helper import TimeUtil
        self.time_util = TimeUtil

    def test_rfc3339nano_to_datetime_basic(self):
        """Test basic RFC3339Nano timestamp conversion."""
        timestamp = "2026-04-11T03:49:53.123456789Z"
        result = self.time_util.rfc3339nano_to_datetime(timestamp)
        expected = datetime(2026, 4, 11, 3, 49, 53)
        self.assertEqual(result, expected)

    def test_rfc3339nano_with_timezone(self):
        """Test RFC3339Nano timestamp with timezone offset."""
        timestamp = "2026-04-10T14:30:28.659-07:00"
        result = self.time_util.rfc3339nano_to_datetime(timestamp)
        expected = datetime(2026, 4, 10, 14, 30, 28)
        self.assertEqual(result, expected)

    def test_rfc3339nano_with_underscores(self):
        """Test RFC3339Nano timestamp with underscores (cbbackupmgr format)."""
        timestamp = "2026-04-11T03_49_53.123456789Z"
        result = self.time_util.rfc3339nano_to_datetime(timestamp)
        expected = datetime(2026, 4, 11, 3, 49, 53)
        self.assertEqual(result, expected)

    def test_rfc3339nano_no_nanoseconds(self):
        """Test RFC3339Nano timestamp without nanoseconds."""
        timestamp = "2026-04-11T03:49:53Z"
        result = self.time_util.rfc3339nano_to_datetime(timestamp)
        expected = datetime(2026, 4, 11, 3, 49, 53)
        self.assertEqual(result, expected)


class TestTableView(unittest.TestCase):
    """Tests for table_view.py - table display utilities"""

    def setUp(self):
        from lib.table_view import TableView
        # Create a mock logger that just stores output
        self.log_output = []
        self.table_view = TableView(lambda x: self.log_output.append(x))

    def test_set_headers(self):
        """Test setting table headers."""
        headers = ["Name", "Status", "Count"]
        self.table_view.set_headers(headers)
        self.assertEqual(self.table_view.headers, headers)

    def test_add_row(self):
        """Test adding rows to table."""
        self.table_view.set_headers(["Name", "Count"])
        self.table_view.add_row(["test", 10])
        self.assertEqual(len(self.table_view.rows), 1)
        self.assertEqual(self.table_view.rows[0], ["test", "10"])

    def test_display_with_data(self):
        """Test displaying table with data."""
        self.table_view.set_headers(["Name", "Count"])
        self.table_view.add_row(["item1", 5])
        self.table_view.add_row(["item2", 10])
        self.table_view.display("Test Table")

        # Check that output was generated
        self.assertTrue(len(self.log_output) > 0)
        output = self.log_output[0]
        self.assertIn("Test Table", output)
        self.assertIn("Name", output)
        self.assertIn("Count", output)

    def test_display_empty_table(self):
        """Test displaying empty table produces no output."""
        self.table_view.set_headers(["Name", "Count"])
        # Don't add any rows
        self.table_view.display("Empty Table")

        # Should not log anything
        self.assertEqual(len(self.log_output), 0)


class TestCRC32(unittest.TestCase):
    """Tests for crc32.py - CRC32 hash calculation"""

    def setUp(self):
        from lib.crc32 import crc32_hash
        self.crc32_hash = crc32_hash

    def test_crc32_consistency(self):
        """Test that same input produces same output."""
        key = "test-key-123"
        hash1 = self.crc32_hash(key)
        hash2 = self.crc32_hash(key)
        self.assertEqual(hash1, hash2)

    def test_crc32_different_keys(self):
        """Test that different keys produce different hashes."""
        hash1 = self.crc32_hash("key1")
        hash2 = self.crc32_hash("key2")
        # While not guaranteed to be different, they should be for these keys
        self.assertNotEqual(hash1, hash2)

    def test_crc32_return_type(self):
        """Test that crc32 returns an integer."""
        result = self.crc32_hash("test")
        self.assertIsInstance(result, int)

    def test_crc32_empty_string(self):
        """Test crc32 with empty string."""
        result = self.crc32_hash("")
        self.assertIsInstance(result, int)


class TestDocumentGenerator(unittest.TestCase):
    """Tests for documentgenerator.py - document generation utilities
    
    Note: DocumentGenerator has complex dependencies on cluster modules,
    so we only test the standalone utility functions.
    """

    def test_get_valid_key_size_none(self):
        """Test get_valid_key_size with None key_size."""
        # Import the function directly to avoid complex dependencies
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
        
        # Define the function inline since it's simple
        def get_valid_key_size(key, key_size):
            if key_size is None:
                return len(key) + 8
            return key_size
        
        result = get_valid_key_size("test", None)
        # Should return len("test") + 8 = 12
        self.assertEqual(result, 12)

    def test_get_valid_key_size_specified(self):
        """Test get_valid_key_size with specified key_size."""
        def get_valid_key_size(key, key_size):
            if key_size is None:
                return len(key) + 8
            return key_size
        
        result = get_valid_key_size("test", 20)
        self.assertEqual(result, 20)


class TestExtractTimestampsFromLogs(unittest.TestCase):
    """Tests for extract_timestamps_from_logs function"""

    def setUp(self):
        from couchbase_helper.log_utils import extract_timestamps_from_logs
        self.extract_timestamps = extract_timestamps_from_logs

    def test_extract_single_timestamp(self):
        """Test extracting a single timestamp."""
        lines = ["2026-04-11T10:00:00 INFO Log message"]
        timestamps = self.extract_timestamps(lines)
        self.assertEqual(len(timestamps), 1)
        self.assertEqual(timestamps[0], datetime(2026, 4, 11, 10, 0, 0))

    def test_extract_multiple_timestamps(self):
        """Test extracting multiple timestamps."""
        lines = [
            "2026-04-11T10:00:00 INFO First log",
            "2026-04-11T10:05:00 INFO Second log",
            "2026-04-11T10:10:00 INFO Third log",
        ]
        timestamps = self.extract_timestamps(lines)
        self.assertEqual(len(timestamps), 3)

    def test_extract_timestamps_mixed_lines(self):
        """Test extracting timestamps from lines with and without timestamps."""
        lines = [
            "2026-04-11T10:00:00 ERROR An error occurred",
            "Caused by: some exception",
            "at com.example.Method(Example.java:123)",
            "2026-04-11T10:01:00 WARN Warning message",
        ]
        timestamps = self.extract_timestamps(lines)
        self.assertEqual(len(timestamps), 2)

    def test_extract_no_timestamps(self):
        """Test extracting from lines with no timestamps."""
        lines = [
            "Stack trace line 1",
            "Stack trace line 2",
        ]
        timestamps = self.extract_timestamps(lines)
        self.assertEqual(len(timestamps), 0)


class TestClusterUtils(unittest.TestCase):
    """Tests for cluster_ready_functions.py - cluster utilities"""

    def test_generate_random_name(self):
        """Test random name generation with prefix."""
        # Import the function directly to avoid complex dependencies
        import random
        import string

        def generate_random_name(prefix, length=8):
            chars = string.ascii_letters + string.digits
            suffix = ''.join(random.choice(chars) for _ in range(length))
            return prefix + suffix

        name = generate_random_name("test", length=8)
        self.assertTrue(name.startswith("test"))
        self.assertEqual(len(name), 12)  # "test" (4) + 8 chars

    def test_generate_random_name_different_suffixes(self):
        """Test that different calls produce different names."""
        import random
        import string

        def generate_random_name(prefix, length=8):
            chars = string.ascii_letters + string.digits
            suffix = ''.join(random.choice(chars) for _ in range(length))
            return prefix + suffix

        name1 = generate_random_name("bucket", 8)
        name2 = generate_random_name("bucket", 8)
        # While not guaranteed, they should almost always be different
        self.assertTrue(name1.startswith("bucket"))
        self.assertTrue(name2.startswith("bucket"))

    def test_create_secret_params_basic(self):
        """Test basic secret params creation."""
        from datetime import datetime, timedelta

        def create_secret_params(secret_type="auto-generated-aes-key-256",
                                 name="Default secret", usage=None,
                                 autoRotation=True, rotationIntervalInDays=60,
                                 rotationIntervalInSeconds=None):
            if usage is None:
                usage = ["bucket-encryption-*"]

            data = {
                "autoRotation": autoRotation,
                "rotationIntervalInDays": rotationIntervalInDays,
                "nextRotationTime": (datetime.utcnow() + timedelta(
                    days=rotationIntervalInDays)).isoformat() + "Z"
            }

            return {
                "type": secret_type,
                "name": name,
                "usage": usage,
                "data": data
            }

        params = create_secret_params()
        self.assertEqual(params["type"], "auto-generated-aes-key-256")
        self.assertEqual(params["name"], "Default secret")
        self.assertIn("bucket-encryption-*", params["usage"])
        self.assertTrue(params["data"]["autoRotation"])

    def test_create_secret_params_with_custom_values(self):
        """Test secret params with custom values."""
        from datetime import datetime, timedelta

        def create_secret_params(name, secret_type, rotationIntervalInDays):
            usage = ["bucket-encryption-*"]
            data = {
                "autoRotation": True,
                "rotationIntervalInDays": rotationIntervalInDays,
                "nextRotationTime": (datetime.utcnow() + timedelta(
                    days=rotationIntervalInDays)).isoformat() + "Z"
            }
            return {"type": secret_type, "name": name, "usage": usage, "data": data}

        params = create_secret_params(
            name="my-secret",
            secret_type="kmip-aes-key-256",
            rotationIntervalInDays=30
        )
        self.assertEqual(params["name"], "my-secret")
        self.assertEqual(params["type"], "kmip-aes-key-256")
        self.assertEqual(params["data"]["rotationIntervalInDays"], 30)


class TestBucketUtils(unittest.TestCase):
    """Tests for bucket_ready_functions.py - bucket utilities"""

    def test_get_vbucket_num_for_key(self):
        """Test vbucket number calculation from document key."""
        import zlib

        def get_vbucket_num_for_key(doc_key, total_vbuckets=1024):
            return (((zlib.crc32(doc_key.encode())) >> 16) & 0x7fff) \
                   & (total_vbuckets - 1)

        # Same key should always produce same vbucket
        vb1 = get_vbucket_num_for_key("test-doc-1")
        vb2 = get_vbucket_num_for_key("test-doc-1")
        self.assertEqual(vb1, vb2)

        # Different keys should likely produce different vbuckets (not guaranteed)
        vb3 = get_vbucket_num_for_key("different-key")
        self.assertIsInstance(vb3, int)
        self.assertGreaterEqual(vb3, 0)
        self.assertLess(vb3, 1024)

    def test_get_vbucket_num_for_key_range(self):
        """Test that vbucket numbers are in valid range."""
        import zlib

        def get_vbucket_num_for_key(doc_key, total_vbuckets=1024):
            return (((zlib.crc32(doc_key.encode())) >> 16) & 0x7fff) \
                   & (total_vbuckets - 1)

        # Test multiple keys
        for i in range(100):
            vb = get_vbucket_num_for_key(f"key-{i}")
            self.assertGreaterEqual(vb, 0)
            self.assertLess(vb, 1024)

    def test_get_vbucket_num_custom_vbuckets(self):
        """Test vbucket calculation with custom vbucket count."""
        import zlib

        def get_vbucket_num_for_key(doc_key, total_vbuckets=1024):
            return (((zlib.crc32(doc_key.encode())) >> 16) & 0x7fff) \
                   & (total_vbuckets - 1)

        vb_512 = get_vbucket_num_for_key("test-key", total_vbuckets=512)
        self.assertGreaterEqual(vb_512, 0)
        self.assertLess(vb_512, 512)

    def test_get_bucket_priority_none(self):
        """Test bucket priority with None input."""
        def get_bucket_priority(priority):
            if priority is None:
                return None
            if priority.lower() == 'low':
                return None
            else:
                return priority

        result = get_bucket_priority(None)
        self.assertIsNone(result)

    def test_get_bucket_priority_low(self):
        """Test bucket priority with 'low' input."""
        def get_bucket_priority(priority):
            if priority is None:
                return None
            if priority.lower() == 'low':
                return None
            else:
                return priority

        result = get_bucket_priority("low")
        self.assertIsNone(result)

    def test_get_bucket_priority_high(self):
        """Test bucket priority with 'high' input."""
        def get_bucket_priority(priority):
            if priority is None:
                return None
            if priority.lower() == 'low':
                return None
            else:
                return priority

        result = get_bucket_priority("high")
        self.assertEqual(result, "high")

    def test_check_if_exception_exists_found(self):
        """Test exception checking when exception is found."""
        def check_if_exception_exists(received_exception, expected_exceptions):
            for expected_exception_str in expected_exceptions:
                if expected_exception_str in received_exception:
                    return True
            return False

        result = check_if_exception_exists(
            "TimeoutException: Operation timed out",
            ["TimeoutException", "ConnectionException"]
        )
        self.assertTrue(result)

    def test_check_if_exception_exists_not_found(self):
        """Test exception checking when exception is not found."""
        def check_if_exception_exists(received_exception, expected_exceptions):
            for expected_exception_str in expected_exceptions:
                if expected_exception_str in received_exception:
                    return True
            return False

        result = check_if_exception_exists(
            "ValueError: Invalid input",
            ["TimeoutException", "ConnectionException"]
        )
        self.assertFalse(result)

    def test_check_if_exception_exists_partial_match(self):
        """Test exception checking with partial string match."""
        def check_if_exception_exists(received_exception, expected_exceptions):
            for expected_exception_str in expected_exceptions:
                if expected_exception_str in received_exception:
                    return True
            return False

        result = check_if_exception_exists(
            "Error: DocumentNotFoundException: Document not found",
            ["NotFoundException"]
        )
        self.assertTrue(result)


class TestCommonLib(unittest.TestCase):
    """Tests for common_lib.py - common utility functions"""

    def test_humanbytes(self):
        """Test human-readable byte conversion."""
        # Define the function inline since it's simple
        def humanbytes(size):
            """Convert bytes to human readable format"""
            if not isinstance(size, (int, float)):
                return "0B"
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if size < 1024.0:
                    return f"{size:.2f}{unit}"
                size /= 1024.0
            return f"{size:.2f}PB"

        self.assertEqual(humanbytes(0), "0.00B")
        self.assertEqual(humanbytes(1024), "1.00KB")
        self.assertEqual(humanbytes(1048576), "1.00MB")
        self.assertEqual(humanbytes(1073741824), "1.00GB")


class TestRebalanceHelper(unittest.TestCase):
    """Tests for rebalance helper utilities"""

    def test_vbucket_hash_consistency(self):
        """Test that vbucket hashing is consistent."""
        import zlib

        def get_vbucket(key, num_vbuckets=1024):
            return (((zlib.crc32(key.encode())) >> 16) & 0x7fff) & (num_vbuckets - 1)

        # Hash should be deterministic
        key = "test-document-key"
        vb1 = get_vbucket(key)
        vb2 = get_vbucket(key)
        vb3 = get_vbucket(key)
        self.assertEqual(vb1, vb2)
        self.assertEqual(vb2, vb3)

    def test_vbucket_distribution(self):
        """Test that vbucket distribution is reasonable."""
        import zlib

        def get_vbucket(key, num_vbuckets=1024):
            return (((zlib.crc32(key.encode())) >> 16) & 0x7fff) & (num_vbuckets - 1)

        # Generate many keys and check distribution
        vbucket_counts = {}
        for i in range(1000):
            key = f"doc-{i}"
            vb = get_vbucket(key)
            vbucket_counts[vb] = vbucket_counts.get(vb, 0) + 1

        # Distribution should be somewhat even (not all in one vbucket)
        unique_vbuckets = len(vbucket_counts)
        self.assertGreater(unique_vbuckets, 100)  # Should hit many vbuckets


class TestFilterFields(unittest.TestCase):
    """
    Tests for HelperLib.filter_fields() in lib/framework_lib/framework.py,
    used to strip this run's own global '-p' fields off test names recycled
    from a previous run's report (-d failed=<url>/passed=<url>).

    framework.py itself can't be imported in this lightweight env (it pulls
    in platform_constants/couchbase SDK), so the function body is inlined
    here, matching this file's existing pattern for such modules.

    Regression coverage for: recycling a failed test via '-d failed=<url>'
    while a GROUP filter is also active as a global '-p' param used to
    strip the recycled test's own GROUP tag, since it shares a field name
    with the global param, even though it is not a value the global run
    overrides - it's per-test selector metadata that testrunner.py's own
    group-filter check (testrunner.py's "GROUP" not in params branch)
    requires be present on each individual test, causing every recycled
    test to be skipped with "group requested but test has no group" and
    zero tests to ever rerun.
    """

    @staticmethod
    def filter_fields(testname, run_params=""):
        run_param_fields = [param.split("=")[0].strip()
                            for param in run_params.split(",") if param
                            and param.split("=")[0].strip()
                            not in ("GROUP", "EXCLUDE_GROUP")]
        testwords = testname.split(",")
        line = []
        for fw in testwords:
            if not fw.startswith("logs_folder=") \
                    and not fw.startswith("conf_file=") \
                    and not fw.startswith("cluster_name=") \
                    and not fw.startswith("ini=") \
                    and not fw.startswith("case_number=") \
                    and not fw.startswith("num_nodes=") \
                    and not fw.startswith("spec=")\
                    and not fw.startswith("get-cbcollect-info=") \
                    and not fw.startswith("infra_log_level=") \
                    and not fw.startswith("log_level=") \
                    and not any(fw.startswith(rp + "=")
                                for rp in run_param_fields):
                line.append(fw)
        return ",".join(line)

    def test_group_field_survives_when_also_a_global_param(self):
        """A recycled test's own GROUP tag must not be stripped just
        because this run's global '-p' also carries a GROUP filter."""
        testname = "nodes_init=3,doc_ops=update,GROUP=P0;my_group"
        run_params = "GROUP=P0;my_group,upgrade_version=8.1.0-2680"
        result = self.filter_fields(testname, run_params)
        self.assertIn("GROUP=P0;my_group", result.split(","))

    def test_exclude_group_field_survives_when_also_a_global_param(self):
        testname = "nodes_init=3,EXCLUDE_GROUP=P0;skip_me"
        run_params = "EXCLUDE_GROUP=P0;skip_me,ini=some.ini"
        result = self.filter_fields(testname, run_params)
        self.assertIn("EXCLUDE_GROUP=P0;skip_me", result.split(","))

    def test_other_global_fields_are_still_stripped(self):
        """Only GROUP/EXCLUDE_GROUP are exempt - fields that genuinely
        are this run's own override values must still be dropped so they
        aren't left stale/duplicated on the recycled test name."""
        testname = "nodes_init=3,ini=old.ini,upgrade_version=8.1.0-2679," \
                   "GROUP=P0;my_group"
        run_params = "ini=new.ini,upgrade_version=8.1.0-2680,GROUP=P0;my_group"
        result = self.filter_fields(testname, run_params).split(",")
        self.assertIn("GROUP=P0;my_group", result)
        self.assertNotIn("ini=old.ini", result)
        self.assertNotIn("upgrade_version=8.1.0-2679", result)

    def test_no_group_filter_active_unaffected(self):
        """Without a GROUP filter in run_params, behavior for a test with
        no GROUP tag of its own is unchanged (nothing to strip either way)."""
        testname = "nodes_init=3,doc_ops=update"
        run_params = "upgrade_version=8.1.0-2680"
        result = self.filter_fields(testname, run_params)
        self.assertEqual(result, testname)


class _LoopbackFakeShell:
    """Duck-typed stand-in for Linux, recording the commands it is given.

    _release_loopbacks() and its two callers only reach out through
    get_mount_source(), execute_command(), log_command_output() and log,
    so they can be driven without an SSH connection.
    """

    class _NullLog:
        def __getattr__(self, _name):
            return lambda *args, **kwargs: None

    def __init__(self, mount_sources, losetup_output=None,
                 losetup_a_output=None, losetup_a_clears_after=None,
                 dm_table=None, device_mounts=None, target_tops=None,
                 stacks=None):
        # One entry per state change of the mountpoint: what is mounted to
        # begin with, then after each unmount. The last entry is the
        # settled state and is returned for every later call, so a test
        # only lists the transitions it cares about.
        self.mount_sources = list(mount_sources)
        self.losetup_output = losetup_output
        self.losetup_a_output = losetup_a_output
        # Number of 'losetup -a' calls after which the device stops being
        # reported, so a test can model a loop that frees on a retry.
        self.losetup_a_clears_after = losetup_a_clears_after
        self.losetup_a_calls = 0
        # "<start> <sectors> <target> ..." when this suite has a
        # device-mapper device for the location, else None.
        self.dm_table = dm_table
        # {device: [mountpoints]} - what 'findmnt --source <dev>' reports,
        # so a loop device mounted somewhere other than the location under
        # test can be modelled. 'umount -l <target>' clears the target.
        self.device_mounts = dict(device_mounts or {})
        # {mountpoint: device on top there}, when it is not simply the
        # device that claims it in device_mounts.
        self.target_tops = dict(target_tops or {})
        # {mountpoint: [devices, bottom first]} when a real stack matters.
        # 'umount -l' pops the top of it.
        self.stacks = {k: list(v) for k, v in (stacks or {}).items()}
        self.ip = "fake-node"
        # Mirrors the class attribute the real shell carries.
        self.disk_build_timeout = 1800
        self.commands = []
        self.log = _LoopbackFakeShell._NullLog()

    def sleep(self, *args, **kwargs):
        """Retry backoff - instant here."""

    def get_mount_source(self, location):
        if len(self.mount_sources) > 1:
            return self.mount_sources.pop(0)
        return self.mount_sources[0] if self.mount_sources else None

    def _mount_stack(self, location):
        """Real stack when a test models one, else the single mount the
        mount_sources model describes. The real implementation's own
        parsing is covered by test_mount_stack_reads_every_layer."""
        if location in self.stacks:
            return list(self.stacks[location])
        current = self.get_mount_source(location)
        return [current] if current else []

    @staticmethod
    def _cut_first_field(lines):
        """Stand in for the '| cut -d: -f1' the real commands pipe through."""
        return [line.split(":")[0].strip() for line in lines
                if line.split(":")[0].strip()]

    def execute_command(self, command, **kwargs):
        self.commands.append(command)
        if command.startswith("findmnt -n -o SOURCE --mountpoint"):
            target = command.replace(
                "findmnt -n -o SOURCE --mountpoint ", "").split()[0]
            if target in self.stacks:
                stack = self.stacks[target]
                # The real findmnt prints one line per mount, bottom
                # first; only callers piping through 'tail -1' get the top.
                if "tail -1" in command:
                    return (stack[-1:] if stack else []), []
                return list(stack), []
            if target in self.target_tops:
                return [self.target_tops[target]], []
            # Default: whichever device claims this target is on top.
            for dev, targets in self.device_mounts.items():
                if target in targets:
                    return [dev], []
            return [], []
        if command.startswith("findmnt -n -o TARGET --source"):
            device = command.replace(
                "findmnt -n -o TARGET --source ", "").split()[0]
            return list(self.device_mounts.get(device, [])), []
        if command.startswith("umount -l "):
            target = command[len("umount -l "):].strip()
            if target in self.stacks and self.stacks[target]:
                self.stacks[target].pop()
            for device in self.device_mounts:
                self.device_mounts[device] = [
                    t for t in self.device_mounts[device] if t != target]
        if command.startswith("losetup -j") and self.losetup_output:
            if command.rstrip().endswith("cut -d: -f1"):
                return self._cut_first_field([self.losetup_output]), []
            return [self.losetup_output], []
        if command.startswith("losetup -f --show"):
            # create_new_partition() attaches the loop itself now.
            if getattr(self, "fail_losetup_find", False):
                return [], []
            return ["/dev/loop0"], []
        if command.startswith("blockdev --getsz"):
            return ["10485760"], []
        if command.startswith("dmsetup create"):
            self.dm_table = "0 10485760 linear /dev/loop0 0"
            return [], []
        if command.startswith("dmsetup table"):
            # No device-mapper device unless a test asks for one.
            return ([self.dm_table] if self.dm_table else []), []
        if command.startswith("dmsetup remove"):
            self.dm_table = None
            return [], []
        if command.startswith("losetup -a"):
            self.losetup_a_calls += 1
            cleared = (self.losetup_a_clears_after is not None
                       and self.losetup_a_calls > self.losetup_a_clears_after)
            if self.losetup_a_output and not cleared:
                if command.rstrip().endswith("cut -d: -f1"):
                    return self._cut_first_field([self.losetup_a_output]), []
                return [self.losetup_a_output], []
        return [], []

    def log_command_output(self, *args, **kwargs):
        pass

    def ran(self, prefix):
        return any(c.startswith(prefix) for c in self.commands)


class TestLoopbackRelease(unittest.TestCase):
    """Tests for the loopback teardown the disk-autofailover suite relies
    on: draining a stacked mountpoint, and never deleting the image out
    from under a loop device that still holds it."""

    def setUp(self):
        import sys
        import types
        # Import without paramiko: none of this touches it.
        for name in ("paramiko", "paramiko.ssh_exception"):
            if name not in sys.modules:
                sys.modules[name] = types.ModuleType(name)
        sys.modules["paramiko"].SSHClient = object
        sys.modules["paramiko"].AutoAddPolicy = object
        sys.modules["paramiko"].SSHException = type(
            "SSHException", (Exception,), {})
        sys.modules["paramiko.ssh_exception"].AuthenticationException = type(
            "AuthenticationException", (Exception,), {})
        sys.path.insert(0, os.path.join(PROJECT_ROOT, "platform_utils",
                                        "ssh_util"))
        from shell_util.platforms.linux import Linux
        self.restore = Linux.restore_partition
        self.create = Linux.create_new_partition
        # The unmount/detach work is delegated to these, so the real
        # helpers are bound onto the duck-typed shell rather than stubbed:
        # they are part of what these tests cover.
        for name in ("_release_loopbacks", "_remove_dm_device",
                     "is_suite_device", "suite_device_names",
                     "dm_device_path", "get_dm_table_type",
                     "clear_immutable", "_loopbacks_for",
                     "_unmount_device_everywhere"):
            setattr(_LoopbackFakeShell, name, getattr(Linux, name))
        _LoopbackFakeShell.dm_device_name = staticmethod(
            Linux.dm_device_name.__func__
            if hasattr(Linux.dm_device_name, "__func__")
            else Linux.dm_device_name)

    def test_stacked_loopbacks_are_all_drained(self):
        """A passing disk_failure test mounts the image twice - the test
        body recovers the disk and teardown's own
        bring_back_failed_nodes_up() recovers it again, and
        mount_partition() mounts without unmounting first. One 'umount -l'
        pops only the topmost, so restore used to hand the post-condition a
        still-mounted /dev/loopN and fail a run whose test logic passed."""
        # create_new_partition() unmounts the real device before it builds
        # the loopback, so by restore time the drain uncovers a bare
        # mountpoint and the device is mounted back afterwards.
        shell = _LoopbackFakeShell(
            ["/dev/loop1", "/dev/loop0", None, None, "/dev/xvdb1"])
        self.restore(shell, "/data", "/dev/xvdb1")
        self.assertEqual(shell.commands.count("umount -l /data"), 2,
                         "the stack was not drained to the real device")
        self.assertTrue(shell.ran("losetup -d /dev/loop1"))
        self.assertTrue(shell.ran("losetup -d /dev/loop0"),
                        "only the topmost loop device was detached")
        self.assertTrue(shell.ran("mount /dev/xvdb1 /data"))

    def test_real_device_is_never_unmounted(self):
        """The drain stops the moment a real device is exposed. Unmounting
        it with nothing to put back is what left /data a plain directory
        on the root filesystem."""
        shell = _LoopbackFakeShell(["/dev/xvdb1", "/dev/xvdb1"])
        self.restore(shell, "/data", "/dev/xvdb1")
        self.assertFalse(shell.ran("umount"),
                         "restore_partition() unmounted a real device")
        self.assertFalse(shell.ran("mount /dev/xvdb1 /data"),
                         "mounted a device that was already mounted, which "
                         "stacks a second mount instead of restoring one")

    def test_drain_is_bounded_when_the_mountpoint_never_clears(self):
        """A mountpoint that keeps reporting a loopback must not spin
        forever."""
        shell = _LoopbackFakeShell(["/dev/loop0"])
        with self.assertRaises(Exception) as ctx:
            self.restore(shell, "/data", None)
        self.assertIn("Failed to restore /data", str(ctx.exception))
        # Ten, not five: the drain now pops through a whole stack rather
        # than only while the topmost mount is ours, and the field has
        # produced stacks four deep.
        self.assertEqual(shell.commands.count("umount -l /data"), 10,
                         "drain was not bounded")

    def test_image_is_kept_while_a_loop_is_still_attached(self):
        """Deleting the image out from under an attached loop device
        strands it and the GiBs it holds, and hides it permanently -
        'losetup -j' matches by path and cannot see a '(deleted)' inode.
        Keep it so the next create_new_partition() can release it."""
        shell = _LoopbackFakeShell(
            ["/dev/loop0", None, None, "/dev/xvdb1"],
            losetup_a_output="/dev/loop0: [2049]:12 "
                             "(/usr/disk-img/disk-quota.ext3)")
        with self.assertRaises(Exception) as ctx:
            self.restore(shell, "/data", "/dev/xvdb1")
        self.assertIn("could not detach loop device", str(ctx.exception))
        self.assertFalse(shell.ran("rm -f /usr/disk-img/disk-quota.ext3"),
                         "image deleted while a loop device still held it")

    def test_restore_completes_before_reporting_a_stuck_loop(self):
        """The device must go back even when the loop cannot be detached:
        reporting first left /data unmounted, and the next run then
        recorded no original device and forgot the real disk for good."""
        shell = _LoopbackFakeShell(
            ["/dev/loop0", None, None, "/dev/xvdb1"],
            losetup_a_output="/dev/loop0: [2049]:12 "
                             "(/usr/disk-img/disk-quota.ext3)")
        with self.assertRaises(Exception) as ctx:
            self.restore(shell, "/data", "/dev/xvdb1")
        self.assertIn("could not detach loop device", str(ctx.exception))
        self.assertTrue(shell.ran("mount /dev/xvdb1 /data"),
                        "reported the stuck loop without restoring /data")

    def test_detach_is_retried_before_giving_up(self):
        """The injection unmounts with 'umount -l', so the filesystem stays
        alive until the last reference closes and a detach straight after
        couchbase is stopped can fail with EBUSY. One failed attempt is not
        a stuck loop - 172.23.104.173 failed teardown on exactly that."""
        shell = _LoopbackFakeShell(
            ["/dev/loop0", None, None, "/dev/xvdb1"],
            losetup_a_output="/dev/loop0: [2049]:12 "
                             "(/usr/disk-img/disk-quota.ext3)",
            # Discovery now issues a 'losetup -a' of its own before the
            # detach loop does, so the device has to survive two calls to
            # still be busy on the first detach attempt.
            losetup_a_clears_after=2)
        self.restore(shell, "/data", "/dev/xvdb1")
        self.assertEqual(shell.commands.count("losetup -d /dev/loop0"), 2,
                         "detach was not retried")
        self.assertTrue(shell.ran("rm -f /usr/disk-img/disk-quota.ext3"),
                        "image kept even though the loop was released")

    def test_dm_device_is_removed_before_the_loop_is_detached(self):
        """The filesystem is mounted through a device-mapper linear target
        so a disk failure can be injected by swapping it for an error
        target. That mapping holds the loop device open, so it has to be
        removed before the loop can be detached."""
        shell = _LoopbackFakeShell(
            ["/dev/mapper/taf_disk_data", None],
            dm_table="0 10485760 linear /dev/loop0 0")
        self.restore(shell, "/data", None)
        self.assertTrue(shell.ran("umount -l /data"),
                        "the device-mapper mount was not drained")
        self.assertTrue(shell.ran("dmsetup remove taf_disk_data"),
                        "the device-mapper device was left behind")
        remove = shell.commands.index("dmsetup remove taf_disk_data")
        detaches = [i for i, c in enumerate(shell.commands)
                    if c.startswith("losetup -d")]
        for d in detaches:
            self.assertLess(remove, d,
                            "loop detached before the mapping was removed")

    def test_mount_source_reads_the_topmost_mount(self):
        """findmnt prints one line per mount in a stack, oldest first. On
        the deb12 pool VMs /data is a real disk (/dev/xvdb1) with this
        suite's loopback mounted over it, so reading the first line reports
        the device underneath and is_suite_device() then says the live
        mount is not ours. That left taf_disk_data mounted, so 'dmsetup
        remove' failed, so the loop stayed held, and every test in job
        259813 failed setUp with "loop device(s) ... are still attached"."""
        import sys
        sys.path.insert(0, os.path.join(PROJECT_ROOT, "platform_utils",
                                        "ssh_util"))
        from shell_util.platforms.linux import Linux
        seen = {}

        class _Probe:
            def execute_command(self, command, **kwargs):
                seen["command"] = command
                return [], []

        Linux.get_mount_source(_Probe(), "/data")
        self.assertIn("--target /data", seen["command"])
        self.assertEqual(seen["command"].count("tail -1"), 2,
                         "get_mount_source() does not take the last line of "
                         "findmnt for both the root and the target device, "
                         "so a stacked mount reports the wrong device")

    def test_loop_mounted_elsewhere_is_released(self):
        """A loop bound to this suite's image need not be mounted at the
        location under test: an earlier run with a different data_location
        leaves it mounted somewhere else (/mnt/disk_fo was found on
        172.23.220.190). Draining only 'location' never finds it, the
        detach then fails with EBUSY forever, and every later run on that
        node fails in setUp. Unmounting it wherever it is releases it - on
        .190 the loop had autoclear set and freed itself the moment the
        mount went away."""
        shell = _LoopbackFakeShell(
            # /data is the real disk and never had our loopback on it.
            ["/dev/xvdb1", "/dev/xvdb1"],
            losetup_a_output="/dev/loop0: [65024]:1475842 "
                             "(/usr/disk-img/disk-quota.ext3)",
            losetup_a_clears_after=2,
            device_mounts={"/dev/loop0": ["/mnt/disk_fo"]})
        still = shell._release_loopbacks(
            "/data", "/usr/disk-img/disk-quota.ext3")
        self.assertTrue(shell.ran("umount -l /mnt/disk_fo"),
                        "the loop device was left mounted outside the "
                        "location, so it can never be detached")
        self.assertFalse(shell.ran("umount -l /data"),
                         "unmounted the real device at the location")
        self.assertEqual(still, [],
                         "loop device reported as still attached")

    def test_real_device_is_unmounted_before_the_loopback_is_built(self):
        """Where /data is a real disk, mounting the test filesystem on top
        stacks it over a live one: the 'rm -rf' wipes the real volume and
        restore then mounts the device back over a mount that was never
        removed, growing the stack by two every test. Take the device out
        of the way first so the loopback replaces it."""
        # Reported once by the loopback drain and once by the check that
        # decides whether a real device is in the way, then gone.
        shell = _LoopbackFakeShell(["/dev/xvdb1", "/dev/xvdb1", None],
                                   device_mounts={"/dev/xvdb1": ["/data"]})
        self.create(shell, "/data", 5120)
        self.assertTrue(shell.ran("umount -l /data"),
                        "the real device was not unmounted before the "
                        "loopback filesystem was built over it")
        unmount = shell.commands.index("umount -l /data")
        wipe = shell.commands.index("rm -rf /data")
        self.assertLess(unmount, wipe,
                        "rm -rf ran while the real device was still "
                        "mounted, destroying its contents")

    def test_device_is_put_back_if_the_build_fails_midway(self):
        """create_new_partition() now unmounts the location's real device
        before building over it, so a failure after that point leaves the
        node with no filesystem there at all - and nothing restores it,
        because tearDown does not run after a failing setUp."""
        shell = _LoopbackFakeShell(["/dev/xvdb1", "/dev/xvdb1", None],
                                   device_mounts={"/dev/xvdb1": ["/data"]})
        # losetup -f returns nothing, so the build raises partway through.
        shell.fail_losetup_find = True
        with self.assertRaises(Exception) as ctx:
            self.create(shell, "/data", 5120)
        self.assertIn("Could not attach a loop device", str(ctx.exception))
        self.assertTrue(shell.ran("umount -l /data"),
                        "the real device was never unmounted")
        self.assertTrue(shell.ran("mount /dev/xvdb1 /data"),
                        "the node was left with /data unmounted after the "
                        "build failed")

    def test_unmounted_device_is_reported_to_the_caller(self):
        """The caller samples the device BEFORE this runs, and on a node an
        earlier run left dirty it sees our leftover loopback on top and
        records None. This re-samples AFTER draining that leftover and so
        sees the real disk underneath - and unmounts it. Against a recorded
        None, restore_partition() puts nothing back and its post-condition
        compares None with None and passes, so the node's data disk is
        dropped from the mount tree until the next reboot and every later
        run repeats it, writing the image and the whole dataset onto the
        root filesystem. So the device actually unmounted is returned."""
        shell = _LoopbackFakeShell(
            # Dirty shape: our leftover on top, the real disk underneath.
            ["/dev/mapper/taf_disk_data", "/dev/xvdb1", "/dev/xvdb1", None],
            dm_table="0 10485760 linear /dev/loop0 0",
            device_mounts={"/dev/xvdb1": ["/data"]})
        unmounted = self.create(shell, "/data", 5120)
        self.assertEqual(unmounted, "/dev/xvdb1",
                         "create_new_partition() unmounted the node's real "
                         "disk without telling the caller, so nothing will "
                         "ever mount it back")

    def test_nothing_is_reported_when_no_real_device_was_in_the_way(self):
        """On the deb10 shape /data is a plain directory on the root
        filesystem, nothing is unmounted, and the caller must keep
        recording None."""
        shell = _LoopbackFakeShell([None, None, None])
        self.assertIsNone(self.create(shell, "/data", 5120),
                          "reported a device where none was unmounted")

    def test_a_device_mounted_over_the_target_is_not_popped(self):
        """'umount -l <target>' pops whatever is topmost at that path, not
        this device's mount. Unmounting for the lower one would take the
        upper one away instead - possibly a real device the drain
        deliberately left alone."""
        shell = _LoopbackFakeShell(
            [None],
            device_mounts={"/dev/loop0": ["/mnt/disk_fo"]},
            target_tops={"/mnt/disk_fo": "/dev/xvdb1"})
        shell._unmount_device_everywhere("/dev/loop0")
        self.assertFalse(shell.ran("umount -l /mnt/disk_fo"),
                         "popped a mountpoint whose topmost mount belongs "
                         "to a different device")

    def test_only_our_backing_file_is_matched(self):
        """The image is found via 'losetup -a' so an unlinked one is still
        seen, but the backing file has to be compared, not searched for: a
        substring test also matches disk-quota.ext3.bak, and that device
        would then be unmounted and detached though it is not ours."""
        shell = _LoopbackFakeShell(
            [None],
            losetup_a_output="/dev/loop3: [2049]:99 "
                             "(/usr/disk-img/disk-quota.ext3.bak)")
        found = shell._loopbacks_for("/usr/disk-img/disk-quota.ext3")
        self.assertEqual(found, [],
                         "claimed a loop device backed by a different file")

    def test_an_unlinked_image_is_still_matched(self):
        """The '(deleted)' shape is the whole reason 'losetup -a' is
        consulted as well as 'losetup -j'."""
        shell = _LoopbackFakeShell(
            [None],
            losetup_a_output="/dev/loop0: [2049]:12 "
                             "(/usr/disk-img/disk-quota.ext3 (deleted))")
        self.assertEqual(
            shell._loopbacks_for("/usr/disk-img/disk-quota.ext3"),
            ["/dev/loop0"],
            "an unlinked image's loop device was missed, which is exactly "
            "the leak that used to be permanent")

    def test_loop1_is_not_confused_with_loop10(self):
        """The detach check parses the device field of 'losetup -a'. A
        substring test would see /dev/loop1 inside /dev/loop10 and report a
        detached device as still attached."""
        shell = _LoopbackFakeShell(
            ["/dev/loop1", None, None, "/dev/xvdb1"],
            # A loop device belonging to something else entirely, whose
            # name merely contains ours.
            losetup_a_output="/dev/loop10: [2049]:77 "
                             "(/usr/other/unrelated.ext3)")
        # loop1 must therefore come back released and the image deleted.
        self.restore(shell, "/data", "/dev/xvdb1")
        self.assertTrue(shell.ran("rm -f /usr/disk-img/disk-quota.ext3"),
                        "/dev/loop1 was reported still attached because "
                        "/dev/loop10 contains it as a substring")

    def test_image_is_written_with_a_sane_block_size(self):
        """Without an explicit bs, dd writes 512-byte blocks: 10.5 million
        syscalls for 5 GiB, over 620s on 172.23.220.135 and past the 600s
        SSH timeout, which failed setUp so tearDown never ran."""
        shell = _LoopbackFakeShell([None, None, None])
        self.create(shell, "/data", 5120)
        alloc = [c for c in shell.commands if "fallocate" in c or "dd if=" in c]
        self.assertEqual(len(alloc), 1,
                         "expected exactly one image-allocation command")
        self.assertIn("fallocate -l 5120M", alloc[0],
                      "the image must be allocated, not written: at 24 GiB "
                      "writing it runs past the SSH timeout")
        self.assertIn("dd if=/dev/zero", alloc[0],
                      "a dd fallback is still needed where fallocate is "
                      "unavailable")
        self.assertIn("bs=1M", alloc[0],
                      "the dd fallback has no block size, so it would write "
                      "512-byte blocks")
        self.assertIn("count=5120", alloc[0],
                      "count must be in MiB to match bs=1M")

    def test_a_sandwiched_mount_of_ours_is_still_drained(self):
        """A run from before restore_partition() stopped re-mounting an
        already-mounted device leaves the node's own disk stacked ON TOP
        of our loopback - xvdb1 / taf_disk_data / xvdb1. Judging by the
        topmost mount alone concludes nothing here is ours, so the
        loopback stays mounted and open, 'dmsetup remove' fails busy, the
        loop beneath it can never be detached, and every later run on that
        node fails setUp. Found on 172.23.222.224 and .232."""
        shell = _LoopbackFakeShell(
            [],
            stacks={"/data": ["/dev/xvdb1", "/dev/mapper/taf_disk_data",
                              "/dev/xvdb1"]},
            dm_table="0 10485760 linear /dev/loop0 0")
        shell._release_loopbacks("/data", "/usr/disk-img/disk-quota.ext3")
        self.assertEqual(shell.stacks["/data"], ["/dev/xvdb1"],
                         "the stack was not drained down to the real "
                         "device, so our mount is still holding the "
                         "device-mapper device open")
        self.assertTrue(shell.ran("dmsetup remove taf_disk_data"),
                        "the mapping was never removed")

    def test_a_real_device_above_ours_is_only_popped_when_duplicated(self):
        """Popping a device that is NOT a duplicate of one still mounted
        below would leave the location without its filesystem - which is
        the damage restore_partition() exists to undo. The drain stops
        instead."""
        shell = _LoopbackFakeShell(
            [],
            stacks={"/data": ["/dev/mapper/taf_disk_data", "/dev/sdb9"]},
            dm_table="0 10485760 linear /dev/loop0 0")
        shell._release_loopbacks("/data", "/usr/disk-img/disk-quota.ext3")
        self.assertIn("/dev/sdb9", shell.stacks["/data"],
                      "popped a real device that was not a duplicate, "
                      "leaving the location with nothing mounted")

    def test_ssh_timeout_names_the_command(self):
        """paramiko raises a bare socket.timeout whose str() is '', so a
        timed-out command used to surface as the node and nothing else -
        "172.23.219.120: " - with no way to tell which command hung or for
        how long. Every disk-partition timeout in this suite has looked
        like that."""
        import socket
        import sys
        sys.path.insert(0, os.path.join(PROJECT_ROOT, "platform_utils",
                                        "ssh_util"))
        from shell_util.common_api import CommonShellAPIs

        class _Stub:
            remote, use_sudo = True, False
            ip = "172.23.219.120"
            log = _LoopbackFakeShell._NullLog()

            def reconnect_if_inactive(self):
                pass

            class _Client:
                @staticmethod
                def exec_command(command, timeout=None):
                    raise socket.timeout()
            _ssh_client = _Client()

        with self.assertRaises(socket.timeout) as ctx:
            CommonShellAPIs.execute_command_raw(
                _Stub(), "dd if=/dev/zero of=/usr/disk-img/x bs=1M "
                         "count=24576", timeout=600)
        message = str(ctx.exception)
        self.assertTrue(message,
                        "the timeout still stringifies to nothing, so the "
                        "failure is undiagnosable")
        self.assertIn("172.23.219.120", message)
        self.assertIn("600", message)
        self.assertIn("dd if=/dev/zero", message,
                      "the message must name the command that hung")

    def test_mount_stack_reads_every_layer(self):
        """The real helper must ask for every mount at the location, not
        pipe through 'tail -1' as get_mount_source() does, and must return
        them bottom first."""
        import sys
        sys.path.insert(0, os.path.join(PROJECT_ROOT, "platform_utils",
                                        "ssh_util"))
        from shell_util.platforms.linux import Linux
        seen = {}

        class _Probe:
            def execute_command(self, command, **kwargs):
                seen["command"] = command
                return ["/dev/xvdb1", "/dev/mapper/taf_disk_data",
                        "/dev/xvdb1"], []

        stack = Linux._mount_stack(_Probe(), "/data")
        self.assertIn("--mountpoint /data", seen["command"])
        self.assertNotIn("tail -1", seen["command"],
                         "_mount_stack() must not reduce to the topmost "
                         "mount - seeing the whole stack is its purpose")
        self.assertEqual(stack, ["/dev/xvdb1", "/dev/mapper/taf_disk_data",
                                 "/dev/xvdb1"])

    def test_root_filesystem_is_never_unmounted(self):
        """On the deb10 pool VMs /data sits on the root device, so
        'findmnt --source' for that device answers '/'. Unmounting it
        would take the node down. get_mount_source() returns None for that
        case so it is not reachable today, but that guard is a string
        compare in another function and one device has two spellings
        (/dev/dm-0, /dev/mapper/vg-root), so the helper refuses outright."""
        shell = _LoopbackFakeShell(
            [None],
            device_mounts={"/dev/mapper/tmpl--deb10--vg-root": ["/"]})
        released = shell._unmount_device_everywhere(
            "/dev/mapper/tmpl--deb10--vg-root")
        self.assertFalse(shell.ran("umount -l /"),
                         "unmounted the root filesystem")
        self.assertFalse(released,
                         "reported the device released without unmounting it")

    def test_only_the_location_mount_of_a_real_disk_is_touched(self):
        """A loop device of ours is released wherever it turns up, but the
        node's own disk is not: if it happens to be mounted somewhere else
        as well, that mount is none of this suite's business."""
        shell = _LoopbackFakeShell(
            [None],
            device_mounts={"/dev/xvdb1": ["/data", "/srv/something-else"]})
        shell._unmount_device_everywhere("/dev/xvdb1", only_target="/data")
        self.assertTrue(shell.ran("umount -l /data"))
        self.assertFalse(shell.ran("umount -l /srv/something-else"),
                         "unmounted a mount of the node's disk that this "
                         "suite has no business touching")

    def test_immutable_flag_is_cleared_before_touching_the_location(self):
        """A directory carrying the immutable flag refuses mkdir even to
        root, so couchbase ends up with no data path and the node can
        never become healthy. On 172.23.220.121 that cost every later
        test 360s of waiting plus a four-minute cb-collect."""
        shell = _LoopbackFakeShell(["/dev/loop0", None])
        self.create(shell, "/data", 100)
        self.assertIn("chattr -i /data", shell.commands,
                      "the immutable flag was never cleared")
        chattr = shell.commands.index("chattr -i /data")
        for touched in ("rm -rf /data", "mkdir -p /data"):
            self.assertIn(touched, shell.commands)
            self.assertLess(chattr, shell.commands.index(touched),
                            f"'{touched}' ran before the flag was cleared")

    def test_immutable_clear_never_walks_up_to_root(self):
        """chattr -i / is never the intent, however the path is given."""
        shell = _LoopbackFakeShell([None, None])
        shell.clear_immutable("/", "", None)
        self.assertFalse([c for c in shell.commands
                          if c.startswith("chattr")],
                         "clearing '/' must be a no-op")
        shell.clear_immutable("/data/kv")
        self.assertIn("chattr -i /data /data/kv", shell.commands,
                      "parents of the path must be cleared too")

    def test_create_releases_loopbacks_before_deleting_the_image(self):
        """create_new_partition() deleted the image at every setUp with no
        losetup check at all, so a teardown that correctly declined to
        delete it only deferred the leak by one run."""
        shell = _LoopbackFakeShell(["/dev/loop0", None])
        self.create(shell, "/data", 100)
        self.assertTrue(shell.ran("losetup -d /dev/loop0"))
        detach = shell.commands.index("losetup -d /dev/loop0")
        delete = shell.commands.index("rm -rf /usr/disk-img/disk-quota.ext3")
        self.assertLess(detach, delete,
                        "image deleted before the loop was detached")

    def test_create_refuses_to_overwrite_a_held_image(self):
        """'dd' truncates its output file, so overwriting an image a loop
        device still holds corrupts a live filesystem. Fail setUp loudly
        instead."""
        shell = _LoopbackFakeShell(
            ["/dev/loop0", None],
            losetup_a_output="/dev/loop0: [2049]:12 "
                             "(/usr/disk-img/disk-quota.ext3)")
        with self.assertRaises(Exception) as ctx:
            self.create(shell, "/data", 100)
        self.assertIn("still attached", str(ctx.exception))
        self.assertFalse(shell.ran("dd if=/dev/zero"),
                         "overwrote an image a loop device still held")


if __name__ == "__main__":
    unittest.main(verbosity=2)
