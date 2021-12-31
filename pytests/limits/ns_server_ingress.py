import json
import time

from limits.abstract_resource_tasks import UserResourceTask
from limits.abstract_timed_throughput import AbstractTimedThroughputWorker
from membase.api.rest_client import RestConnection


class IngressThroughputWorker(AbstractTimedThroughputWorker):

    def __init__(self, node):
        super(IngressThroughputWorker, self).__init__(
            period=60, chunks=100, throughput=0)
        self.rest = RestConnection(node)
        self.rest.log_errors = False

    def action(self, throughput):
        """ Updates a document to produce throughput """
        difference = len(json.dumps({"key": ""}))
        as_to_send = max(0, throughput-difference)
        return self.rest.set_document("default", "doc24601", {'key': 'a' * as_to_send})


class NsServerIngress(UserResourceTask):
    """ Produces throughput for ingress_mib_per_min """

    def __init__(self, user, node):
        super(NsServerIngress, self).__init__(user, node)
        self.rest = RestConnection(node)
        self.workers = []
        self.threads = 10

        for _ in range(self.threads):
            self.workers.append(IngressThroughputWorker(self.node))

        for worker in self.workers:
            worker.start()

    def on_throughput_update(self, throughput):
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
        return 'Limit(s) exceeded [ingress]'
