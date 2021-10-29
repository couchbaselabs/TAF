import itertools
import json

from basetestcase import ClusterSetup
from copy import deepcopy
from membase.api.rest_client import RestConnection
from limits.resource_producer import UserResourceProducer
from limits.util import retry_with_timeout


class User:
    """ Represents a Couchbase local account """

    def __init__(self, username, password="password", rbacrole="admin"):
        """ Creates a Couchbase local account """
        self.rbacrole = rbacrole
        self.username = username
        self.password = password

    def __hash__(self):
        return hash(self.username)

    def __eq__(self, other):
        return self.username == other.username

    def params(self, limits={}):
        """ The params for creating this user. """
        return "name={}&roles={}&password={}&limits={}".format(
    self.username, self.rbacrole, self.password, json.dumps(limits))


class UserLimitTest(ClusterSetup):

    def setUp(self):
        super(UserLimitTest, self).setUp()
        self.no_of_users = self.input.param("users", 3)

        # The resource we're testing
        self.resource_name = self.input.param("resource_name", "ns_server_num_concurrent_requests")

        # The threshold at which the limit is configured at
        self.resource_limit = self.input.param("resource_limit", 100)

        # A rest connection
        self.rest = RestConnection(self.cluster.master)

        # Create default bucket
        self.create_bucket(self.cluster)

        # Create users
        self.users = self.create_users()

        # Some hints for controlling the user resource tasks
        self.hints = {}

        # A streaming uri for http long polling
        self.hints["streaminguri"] = RestConnection(self.cluster.master).get_path("pools/default/buckets/default", "streamingUri")

        # A factory-esque object that produces objects for the given resource
        self.resource_producer = UserResourceProducer(self.resource_name, self.hints)

        # Create resources from test params
        self.tasks = self.create_tasks()

    def create_users(self):
        """ Creates users """
        users = [User("user{}".format(i)) for i in range(self.no_of_users)]
        for user in users:
            self.rest.add_set_builtin_user(user.username, user.params())
        return users

    @staticmethod
    def copy_node_for_user(user, node):
        """ Copy the ServerInfo object and change it's rest credentials to
        those of the use object. """
        nodecopy = deepcopy(node)
        nodecopy.rest_username = user.username
        nodecopy.rest_password = user.password
        return nodecopy

    def create_tasks(self):
        """ Creates resource tasks """
        tasks = []
        for user in self.users:
            for node in self.cluster.servers[:self.nodes_init]:
                tasks.append(self.resource_producer.get_resource_task(user, UserLimitTest.copy_node_for_user(user, node)))
        return tasks

    def set_throughput_to_zero(self):
        """ Set the throughput for all tasks to 0 """
        for task in self.tasks:
            task.set_throughput(0)

    def tearDown(self):
        super(UserLimitTest, self).tearDown()

    def enforce(self):
        """ Make effects take place """
        self.rest.enforce_limits()

    def set_limits_for_all_users(self, limits):
        """ Sets limits for all users and makes them take effect """
        for user in self.users:
            self.rest.add_set_builtin_user(user.username, user.params(limits))

        self.enforce()

    def test_below_threshold(self):
        """ A test in which the throughput is below the threshold and operations succeed. """
        throughput_difference = 50

        # TODO: Limits can only be configured for the num_concurrent_requests
        self.set_limits_for_all_users(
            {"clusterManager": {"num_concurrent_requests": self.resource_limit}})

        # Check that a throughput below the limit succeeds
        for task in self.tasks:
            task.set_throughput(self.resource_limit - throughput_difference)
            self.assertTrue(retry_with_timeout(5, lambda: abs(task.get_throughput_success() - task.throughput) < 5))

        self.set_throughput_to_zero()

    def test_above_threshold(self):
        """ A test in which the throughput is above the threshold and
        operations exceeding the threshold fail. """
        throughput_difference = 50

        # TODO: Limits can only be configured for the num_concurrent_requests
        self.set_limits_for_all_users(
            {"clusterManager": {"num_concurrent_requests": self.resource_limit}})

        # Check that a throughput above the threshold is constrained by the
        # resource limit
        for task in self.tasks:
            task.set_throughput(self.resource_limit + throughput_difference)
            self.assertTrue(retry_with_timeout(5, lambda: abs(task.get_throughput_success() - self.resource_limit) < 5))

        self.set_throughput_to_zero()

    def test_above_to_below_threshold(self):
        """ A test in which the throughput is initially above the threshold and
        operations fail, the throughput is decreased to below the threshold and
        operations succeed. """
        throughput_difference = 50

        # TODO: Limits can only be configured for the num_concurrent_requests
        self.set_limits_for_all_users(
            {"clusterManager": {"num_concurrent_requests": self.resource_limit}})

        # Check that a throughput above the threshold is constrained by the
        # resource limit
        for task in self.tasks:
            task.set_throughput(self.resource_limit + throughput_difference)
            self.assertTrue(retry_with_timeout(5, lambda: abs(task.get_throughput_success() - self.resource_limit) < 5))

        self.sleep(5)

        # Check that a throughput below the limit succeeds
        for task in self.tasks:
            task.set_throughput(self.resource_limit - throughput_difference)
            self.assertTrue(retry_with_timeout(5, lambda: abs(task.get_throughput_success() + task.throughput) < 5))

        self.set_throughput_to_zero()

    def test_below_to_above_threshold(self):
        """ A test in which the throughput is initially below the threshold and
        operations succeed, the throughput is increased to above the threshold and
        operations fail. """
        throughput_difference = 50

        # TODO: Limits can only be configured for the num_concurrent_requests
        self.set_limits_for_all_users(
            {"clusterManager": {"num_concurrent_requests": self.resource_limit}})

        # Check that a throughput below the limit succeeds
        for task in self.tasks:
            task.set_throughput(self.resource_limit - throughput_difference)
            self.assertTrue(retry_with_timeout(5, lambda: abs(task.get_throughput_success() + task.throughput) < 5))

        self.sleep(5)

        # Check that a throughput above the threshold is constrained by the
        # resource limit
        for task in self.tasks:
            task.set_throughput(self.resource_limit + throughput_difference)
            self.assertTrue(retry_with_timeout(5, lambda: abs( task.get_throughput_success() - self.resource_limit) < 5))

        self.set_throughput_to_zero()
