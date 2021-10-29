from limits.abstract_resource_tasks import UserResourceTask


class NsServerEgress(UserResourceTask):
    """ Produces throughput for egress """

    def __init__(self, user, node):
        super(NsServerEgress, self).__init__(user, node)
