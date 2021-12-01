from sdk_client3 import SDKClient
from BucketLib.bucket import Bucket
from BucketLib.BucketOperations import BucketHelper
from limits.common import random_string
from limits.abstract_resource_tasks import ScopeResourceTask
from limits.util import vbuckets_on_node


class KvDataSize(ScopeResourceTask):
    """ Targets throughput for num collections """

    def __init__(self, bucket, scope, user, node, collections=2):
        super(KvDataSize, self).__init__(bucket, scope, user, node)
        # The target bucket
        self.bucket = bucket
        # The vbuckets this task writes to
        self.vbuckets = vbuckets_on_node(node, bucket)
        # The collections this task will operate on
        self.collections = [self.collection_name(i) for i in range(collections)]

        # Count of documents which exist
        self.throughput_success = 0

        # The clients must be intiialised once on a throughput update, this is
        # because the scopes are created after this particular constructor is
        # called.
        self.initialised = False

        # The prefix of the key depends on the node that's being targetted
        self.key_prefix = str(abs(hash(self.node.ip)))

        # The size of each document
        self.item_size = 1024

    def items_per_collection(self, throughput):
        return throughput // (self.item_size * len(self.collections))

    def initialise(self):
        # Create the collections for this task
        for collection in self.collections:
            BucketHelper(self.node).create_collection(
                Bucket({'name': self.bucket}), self.scope.name, {"name": collection})

        # A client for each collection
        self.clients = {}
        for collection in self.collections:
            self.clients[collection] = \
                SDKClient([self.node], Bucket(
                    {'name': self.bucket}), scope=self.scope.name, collection=collection)

    def collection_name(self, collection_no):
        """ The collection name is specified by the hash of the node's ip """
        return "{}-{}".format(abs(hash(self.node.ip)), collection_no)

    def on_throughput_increase(self, throughput):
        if not self.initialised:
            self.initialise()
            self.initialised = True

        start = self.items_per_collection(self.throughput)
        end = self.items_per_collection(throughput)

        # Updates
        for c in self.collections:
            for i in range(start, end):
                dvalue = {'k': random_string(self.item_size)}
                result = self.clients[c].upsert(self.key_prefix + str(i), dvalue)
                if not result['error']:
                    self.throughput_success += 1

    def on_throughput_decrease(self, throughput):
        if not self.initialised:
            self.initialise()
            self.initialised = True

        start = self.items_per_collection(throughput)
        end = self.items_per_collection(self.throughput)

        # Deletes
        for c in self.collections:
            for i in range(start, end):
                if not self.clients[c].delete(self.key_prefix + str(i))['error']:
                    self.throughput_success -= 1

    def get_throughput_success(self):
        return self.throughput_success * self.item_size

    def error(self):
        pass

    def expected_error(self):
        return "kava_data_size"
