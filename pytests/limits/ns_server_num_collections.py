from limits.abstract_resource_tasks import ScopeResourceTask


class NsServerNumCollections(ScopeResourceTask):
    """ Targets throughput for num collections """

    def __init__(self, bucket, scope, user, node):
        super(NsServerNumCollections, self).__init__(bucket, scope, user, node)
