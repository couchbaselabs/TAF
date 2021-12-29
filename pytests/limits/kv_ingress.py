from BucketLib.bucket import Bucket
from limits.abstract_resource_tasks import UserResourceTask
from limits.abstract_timed_throughput import AbstractTimedThroughputWorker
from limits.common import random_string
from limits.util import key_for_node
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper


class IngressThroughputWorker(AbstractTimedThroughputWorker):

    def __init__(self, node):
        super(IngressThroughputWorker, self).__init__(
            period=60, chunks=30, throughput=0)

        self.key = key_for_node(node, "default")
        self.node = node
        self.reset_client()

    def action(self, throughput):
        """ Sets a document to produce throughput """
        try:
            self.client.set(self.key, 0, 0, random_string(throughput))
            return True
        except Exception:
            return False

    def reset_client(self):
        self.client = MemcachedClientHelper.direct_client(self.node, Bucket(
            {'name': "default"}), admin_user=self.node.rest_username, admin_pass=self.node.rest_password)


class KvIngress(UserResourceTask):
    """ Produces throughput for egress_mib_per_min """

    def __init__(self, user, node):
        super(KvIngress, self).__init__(user, node)
        self.rest = RestConnection(node)
        self.workers = []
        self.threads = 10

        for _ in range(self.threads):
            self.workers.append(
                IngressThroughputWorker(self.node))

        for worker in self.workers:
            worker.start()

    def on_throughput_update(self, throughput):
        """ Updates document size """
        for worker in self.workers:
            try:
                worker.reset_client()
                worker.throughput.set(throughput / self.threads)
            except Exception:
                pass

        if throughput == 0:
            for worker in self.workers:
                worker.stop()

    on_throughput_increase = on_throughput_update
    on_throughput_decrease = on_throughput_update

    def get_throughput_success(self):
        return sum(worker.throughput_success.get() for worker in self.workers)

    def error(self):
        pass

    def expected_error(self):
        pass
