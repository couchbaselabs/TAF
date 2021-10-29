from limits.abstract_resource_tasks import UserResourceTask


class NsServerIngress(UserResourceTask):
    """ Produces throughput for ingress """

    def __init__(self, user, node):
        super(NsServerIngress, self).__init__(user, node)
