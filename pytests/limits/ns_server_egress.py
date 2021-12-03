from limits.abstract_resource_tasks import UserResourceTask
from limits.common import create_document_of_size
from limits.abstract_timed_throughput import AbstractTimedThroughputWorker
from membase.api.rest_client import RestConnection


class EgressThroughputWorker(AbstractTimedThroughputWorker):

    def __init__(self, node):
        super(EgressThroughputWorker, self).__init__(
            period=60, chunks=30, throughput=0)
        self.rest = RestConnection(node)

    def action(self, throughput):
        """ Retrieves a document to produce throughput """
        status, content = self.rest.get_document("default", "doc24601")
        # print("Content:{}, Throughput:{}".format(throughput, len(content)))
        return status


class NsServerEgress(UserResourceTask):
    """ Produces throughput for egress_mib_per_min """

    def __init__(self, user, node):
        super(NsServerEgress, self).__init__(user, node)
        self.rest = RestConnection(node)
        self.workers = []
        self.threads = 10

        for _ in range(self.threads):
            self.workers.append(EgressThroughputWorker(self.node))

        for worker in self.workers:
            worker.start()

    def on_throughput_update(self, throughput):
        """ Updates document size """
        document_size = throughput / (self.threads * self.workers[0].chunks)
        self.rest.set_document("default", "doc24601", create_document_of_size(document_size - 127))

        for worker in self.workers:
            worker.throughput.set(throughput / self.threads)

        if throughput == 0:
            for worker in self.workers:
                worker.stop()

    on_throughput_increase = on_throughput_update
    on_throughput_decrease = on_throughput_update

    def get_throughput_success(self):
        return sum(worker.throughput_success.get() for worker in self.workers)

    def error(self):
        return self.rest._http_request(self.rest.baseUrl + "/pools/default")[1]

    def expected_error(self):
        return 'Limit(s) exceeded [egress]'
