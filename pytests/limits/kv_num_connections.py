from BucketLib.bucket import Bucket
from global_vars import logger
from limits.abstract_resource_tasks import UserResourceTask
from memcached.helper.data_helper import MemcachedClientHelper

log = logger.get("test")


class KvNumConnections(UserResourceTask):

    def __init__(self, user, node):
        super(KvNumConnections, self).__init__(user, node)
        self.clients = []
        self.throughput_success = 0

    def make_client(self):
        try:
            return MemcachedClientHelper.direct_client(self.node, Bucket(
                {'name': "default"}), admin_user=self.node.rest_username, admin_pass=self.node.rest_password)
        except Exception:
            return None

    def on_throughput_increase(self, throughput):
        log.debug(
            "Increasing throughput by {}".format(
                throughput -
                self.throughput))

        for _ in range(self.throughput, throughput):
            self.clients.append(self.make_client())
            if self.clients[-1]:
                self.throughput_success += 1

    def on_throughput_decrease(self, throughput):
        log.debug(
            "Decreasing throughput by {}".format(
                self.throughput -
                throughput))

        for i in range(throughput, self.throughput):
            if self.clients[i]:
                self.clients[i].close()
                self.throughput_success -= 1

        del self.clients[throughput: self.throughput]

    def get_throughput_success(self):
        return self.throughput_success

    def error(self):
        pass

    def expected_error(self):
        pass
