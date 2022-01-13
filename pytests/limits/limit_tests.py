import re
import json

from BucketLib.BucketOperations import BucketHelper
from basetestcase import ClusterSetup
from limits.resource_producer import UserResourceProducer, LimitConfig
from limits.util import retry_with_timeout, copy_node_for_user, apply_rebalance
from membase.api.rest_client import RestConnection


class Scope():
    """ Represents a shared scope resource and how many throughput producers own it. """

    def __init__(self, user, name):
        # The user that owns this scope
        self.user = user
        # The name of the scope
        self.name = name
        # The scope resource tasks which hold this object
        self.held = []

    def acquire(self, scope_resource_task):
        self.held.append(scope_resource_task)

    def no_of_holders(self):
        return len(self.held)

    def is_last_holder(self, scope_resource_task):
        return scope_resource_task == self.held[-1]

    def get_throughput_success(self):
        raise NotImplementedError("Please implement this method")


class User:
    """ Represents a Couchbase local account """

    def __init__(self, username, password="password", rbacrole="admin"):
        """ Creates a Couchbase local account """
        self.rbacrole = rbacrole
        self.username = username
        self.password = password

        # The scope that is owned by this resource
        self.scope = Scope(self, self.username)

    def __hash__(self):
        return hash(self.username)

    def __eq__(self, other):
        return self.username == other.username

    def params(self, limits={}):
        """ The params for creating this user. """
        return "name={}&roles={}&password={}&limits={}".format(
    self.username, self.rbacrole, self.password, json.dumps(limits))


class LimitTest(ClusterSetup):

    def setUp(self):
        super(LimitTest, self).setUp()
        self.no_of_users = self.input.param("users", 3)

        # The resource we're testing
        self.resource_name = self.input.param("resource_name", "ns_server_num_concurrent_requests")

        # The threshold at which the limit is configured at
        self.resource_limit = self.input.param("resource_limit", 100)

        # Extra resources - a list of key:value pairs separated by dashes
        self.extra_resources = self.input.param("extra_resources", "")
        self.extra_resources = dict(item.split(":") for item in self.extra_resources.split("-") if item)

        # The absolute error that is allowed in validation
        self.error = self.input.param("error", 5)

        # The difference between throughput changes
        self.throughput_difference = self.input.param("throughput_difference", 50)

        # The retry timeout
        self.retry_timeout = self.input.param("retry_timeout", 5)

        # A multiplier for the throughput producer (e.g. use 1 << 20 for
        # mebibytes)
        self.units = self.input.param("units", 1 << 20)

        # A rest connection
        self.rest = RestConnection(self.cluster.master)

        # The name of the bucket
        self.bucket_name = "default"

        # Create bucket
        self.create_bucket(self.cluster, bucket_name=self.bucket_name)

        # Create users
        self.users = self.create_users()

        # Some hints for controlling the user resource tasks
        self.hints = {}

        # A streaming uri for http long polling
        self.hints["streaminguri"] = RestConnection(self.cluster.master).get_path("pools/default/buckets/default", "streamingUri")

        # A factory-esque object that produces objects for the given resource
        self.resource_producer = UserResourceProducer(self.resource_name, self.hints)

        # Create throughput tasks from test params
        self.tasks = self.create_tasks()

    def create_users(self):
        """ Creates users """
        users = [User("user{}".format(i)) for i in range(self.no_of_users)]
        for user in users:
            self.rest.add_set_builtin_user(user.username, user.params())
        return users

    def create_tasks(self):
        """ Creates resource tasks """
        tasks = []
        for user in self.users:
            for node in self.cluster.servers[:self.nodes_init]:
                tasks.append(self.resource_producer.get_resource_task(user, copy_node_for_user(user, node)))
        return tasks

    def remove_user_and_task(self, user):
        """ Removes a user and their associated task """
        self.users.remove(user)
        self.tasks = [task for task in self.tasks if task.user != user]
        self.rest.delete_builtin_user(user.username)

    def insert_user_and_task(self, user):
        """ Inserts a user and their associated task """
        self.users.append(user)
        self.rest.add_set_builtin_user(user.username, user.params())

        for node in self.cluster.servers[:self.nodes_init]:
            self.tasks.append(self.resource_producer.get_resource_task(user, copy_node_for_user(user, node)))

    def set_throughput_to_zero(self):
        """ Set the throughput for all tasks to 0 """
        for task in self.tasks:
            task.set_throughput(0)

    def tearDown(self):
        super(LimitTest, self).tearDown()

    def enforce(self):
        """ Make effects take place """
        self.rest.enforce_limits()

    def set_limits_for_all_users(self, resource_name, resource_limit):
        """ Sets limits for all users and makes them take effect """
        # Configure limits from the test params
        limit_config = LimitConfig()
        limit_config.set_limit(resource_name, resource_limit)

        # Configure extra limits
        for resource_name, resource_limit in self.extra_resources.items():
            limit_config.set_limit(resource_name, int(resource_limit))

        # Set user limits
        for user in self.users:
            self.rest.add_set_builtin_user(user.username, user.params(limit_config.get_user_config()))

        # A pattern for matching UUID-4
        pattern = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")

        # Ensure the user limits were set
        if limit_config.get_user_config():
            for user in self.users:
                content = self.rest.get_builtin_user(user.username)
                # Ensure the user's uuid is a UUID-4
                self.assertTrue(pattern.match(content['uuid']), "The user's uuid is does match a UUID-4")
                self.assertEqual(content['limits'], limit_config.get_user_config())

        # Set scope limits
        for user in self.users:
            self.rest.set_scope_limit(self.bucket_name, user.scope.name, limit_config.get_scope_config())

        # Ensure the scope limits were set
        if limit_config.get_scope_config():
            for user in self.users:
                _, content = BucketHelper(self.cluster.master).list_collections(self.bucket_name)
                scope = next(scope for scope in json.loads(content)['scopes'] if scope['name'] == user.scope.name)
                self.assertEqual(scope['limits'], limit_config.get_scope_config())

        self.enforce()

    def test_below_threshold(self):
        """ A test in which the throughput is below the threshold and operations succeed. """
        throughput_difference = self.throughput_difference

        self.set_limits_for_all_users(self.resource_name, self.resource_limit)

        # Check that a throughput below the limit succeeds
        for task in self.tasks:
            task.set_throughput(self.resource_limit * self.units - throughput_difference)

        for task in self.tasks:
            self.assertTrue(retry_with_timeout(self.retry_timeout, lambda: self.check(task.get_throughput_success(), task.get_throughput(), self.error)))

        self.set_throughput_to_zero()

    def check(self, lhs, rhs, error):
        self.log.info("Expected:{} Actual:{} Actual Difference:{} Expected Difference:{}".format(lhs, rhs, abs(lhs-rhs), error))
        return abs(lhs-rhs) <= error

    def check_error(self, actual_error, expected_error):
        self.log.info("Expected: {} Actual: {}".format(expected_error, actual_error[:20]))
        return actual_error == expected_error

    def test_above_threshold(self):
        """ A test in which the throughput is above the threshold and
        operations exceeding the threshold fail. """
        throughput_difference = self.throughput_difference

        self.set_limits_for_all_users(self.resource_name, self.resource_limit)

        # Check that a throughput above the threshold is constrained by the
        # resource limit
        for task in self.tasks:
            task.set_throughput(self.resource_limit * self.units + throughput_difference)

        for task in self.tasks:
            self.assertTrue(retry_with_timeout(self.retry_timeout, lambda: self.check(task.get_throughput_success(), self.resource_limit * self.units, self.error)))

        # Once above threshold, ensure the expected error message is thrown
        if self.tasks and self.tasks[0].expected_error():
            self.assertTrue(retry_with_timeout(self.retry_timeout, lambda: self.check_error(self.tasks[0].error(), self.tasks[0].expected_error())))

        self.set_throughput_to_zero()

    def test_above_to_below_threshold(self):
        """ A test in which the throughput is initially above the threshold and
        operations fail, the throughput is decreased to below the threshold and
        operations succeed. """
        throughput_difference = self.throughput_difference

        self.set_limits_for_all_users(self.resource_name, self.resource_limit)

        # Check that a throughput above the threshold is constrained by the
        # resource limit
        for task in self.tasks:
            task.set_throughput(self.resource_limit * self.units + throughput_difference)

        for task in self.tasks:
            self.assertTrue(retry_with_timeout(self.retry_timeout, lambda: self.check(task.get_throughput_success(), self.resource_limit * self.units, self.error)))

        self.sleep(5)

        # Check that a throughput below the limit succeeds
        for task in self.tasks:
            task.set_throughput(self.resource_limit * self.units - throughput_difference)

        for task in self.tasks:
            self.assertTrue(retry_with_timeout(self.retry_timeout, lambda: self.check(task.get_throughput_success(), task.get_throughput(), self.error)))

        self.set_throughput_to_zero()

    def test_below_to_above_threshold(self):
        """ A test in which the throughput is initially below the threshold and
        operations succeed, the throughput is increased to above the threshold and
        operations fail. """
        throughput_difference = self.throughput_difference

        self.set_limits_for_all_users(self.resource_name, self.resource_limit)

        # Check that a throughput below the limit succeeds
        for task in self.tasks:
            task.set_throughput(self.resource_limit * self.units - throughput_difference)

        for task in self.tasks:
            self.assertTrue(retry_with_timeout(self.retry_timeout, lambda: self.check(task.get_throughput_success(), task.get_throughput(), self.error)))

        self.sleep(5)

        # Check that a throughput above the threshold is constrained by the
        # resource limit
        for task in self.tasks:
            task.set_throughput(self.resource_limit * self.units + throughput_difference)

        for task in self.tasks:
            self.assertTrue(retry_with_timeout(self.retry_timeout, lambda: self.check(task.get_throughput_success(), self.resource_limit * self.units, self.error)))

        self.set_throughput_to_zero()

    def test_user_churn(self):
        """ A test in which initial users are deleted to make space for new
        users. Limits are applied to new users. A throughput of above the
        threshold is applied and it is expected for the throughput to remain at
        or below the limit threshold. """
        for i in range(self.input.param("iterations", 5)):
            # Test that throughput remains at or below the threshold
            self.test_above_threshold()
            # Remove the user from the system
            self.remove_user_and_task(self.users[-1])
            # Add a new user to the system
            self.insert_user_and_task(User("user{}".format(len(self.users) + i)))

    def test_cluster_ops(self):
        """ A test in which the limits are configured, a cluser operation
        happens and throughput is produced above the threshold and operations
        exceeding the threshold fail.
        """
        # The number of times the cluster operation happens
        cycles = self.input.param("cycles", 3)
        # The type of cluster operation
        strategy = self.input.param("strategy", "rebalance")

        self.assertGreaterEqual(self.nodes_init, 3, "Requires at least 3 nodes")

        if strategy not in {'rebalance', 'graceful-failover', 'hard-failover'}:
            self.fail("Strategy {} is not an allowed strategy".format(strategy))

        # Apply cluster operation
        apply_rebalance(self.task, self.cluster.servers, cycles=cycles, strategy=strategy)
        # Ensure limits still apply after cluster operation
        self.test_above_threshold()
