from BucketLib.bucket import Bucket
from limits.abstract_resource_tasks import UserResourceTask
from limits.abstract_timed_throughput import AbstractTimedThroughputWorker
from limits.common import create_document_of_size
from limits.util import key_for_node
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper


class EgressThroughputWorker(AbstractTimedThroughputWorker):

    def __init__(self, node, key):
        super(EgressThroughputWorker, self).__init__(
            period=60, chunks=60, throughput=0)
        self.key = key
        self.node = node
        self.reset_client()

    def action(self, throughput):
        """ Retrieves a document to produce throughput """
        try:
            self.client.get(self.key)
            return True
        except Exception:
            return False

    def reset_client(self):
        """ Reset a client, in particular after a limit update as limits do not
        apply to pre-existing connections. """
        self.client = MemcachedClientHelper.direct_client(self.node, Bucket(
            {'name': "default"}), admin_user=self.node.rest_username, admin_pass=self.node.rest_password)


class KvEgress(UserResourceTask):
    """ Produces throughput for egress_mib_per_min """

    def __init__(self, user, node):
        super(KvEgress, self).__init__(user, node)
        self.rest = RestConnection(node)
        self.workers = []
        self.threads = 10

        self.key = key_for_node(node, "default")

        for _ in range(self.threads):
            self.workers.append(EgressThroughputWorker(self.node, self.key))

        for worker in self.workers:
            worker.start()

    def on_throughput_update(self, throughput):
        """ Updates document size """
        document_size = throughput / (self.threads * self.workers[0].chunks)

        self.rest.set_document("default", self.key,
                               create_document_of_size(document_size))

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
        # self.client = SDKClient([self.node], Bucket({'name': "default"}), username=self.node.rest_username, password=self.node.rest_password)
        pass

    def expected_error(self):
        pass
