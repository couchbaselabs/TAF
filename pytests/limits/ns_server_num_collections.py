import json

from limits.abstract_resource_tasks import ScopeResourceTask
from BucketLib.bucket import Bucket
from BucketLib.BucketOperations import BucketHelper


class NsServerNumCollections(ScopeResourceTask):
    """ Targets throughput for num collections """

    def __init__(self, bucket, scope, user, node):
        super(NsServerNumCollections, self).__init__(bucket, scope, user, node)
        self.bucket_obj = Bucket({'name': bucket})
        self.bucket_hlp = BucketHelper(self.node)

    def collection_name(self, collection_no):
        return "{}-{}".format(abs(hash(self.node.ip)), collection_no)

    def on_throughput_increase(self, throughput):
        for i in range(self.throughput, throughput):
            collection = {"name": self.collection_name(i)}
            self.bucket_hlp.create_collection(
                self.bucket_obj, self.scope.name, collection)

    def on_throughput_decrease(self, throughput):
        for i in range(throughput, self.throughput):
            self.bucket_hlp.delete_collection(
                self.bucket_obj, self.scope.name, self.collection_name(i))

    def get_throughput_success(self):
        _, content = self.bucket_hlp.list_collections(self.bucket)

        for scope in json.loads(content)['scopes']:
            if scope['name'] == self.scope.name:
                return len(scope['collections'])

        return 0

    def error(self):
        content = self.bucket_hlp.create_collection(
            self.bucket_obj, self.scope.name, {"name": "justacollection"})[1]
        content = json.loads(content)
        return content['errors']['limits']

    def expected_error(self):
        return 'Maximum number of collections has been reached for scope "{}"'.format(self.scope.name)
