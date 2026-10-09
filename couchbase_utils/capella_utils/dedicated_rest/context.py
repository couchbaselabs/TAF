# POC-NOTE (remove later)
# WHY: Capella API calls need pod + tenant + cluster, but REST objects are
#      created from just one node (eg BucketHelper(cluster.master)).
#      So every Capella node carries this "ID card"
# CAME FROM: dedicatedbasetestcase.__populate_cluster_info -> attach()
#      and task.py RebalanceTaskCapella / UpgradeProvisionedCluster
#      (copied onto new node objects after scale / upgrade)
# DOES: holds pod / tenant / cluster; read back with CapellaContext.of(node)
class CapellaContext(object):
    """
    Control-plane handles a Capella dedicated node needs to reach the
    Capella API: pod, tenant and the cluster object it belongs to.

    Attached to every TestInputServer of a dedicated cluster as
    `server.capella`, so REST variants built from just a node
    (eg: BucketHelper(cluster.master)) can call CapellaUtils.

    NOTE: tenant carries API keys / password. Never log this object's
    attributes; __repr__ intentionally prints only the cluster id.
    """

    def __init__(self, pod, tenant, cluster):
        self.pod = pod
        self.tenant = tenant
        self.cluster = cluster

    @property
    def cluster_id(self):
        return self.cluster.id

    def __repr__(self):
        return "CapellaContext(cluster_id=%s)" % self.cluster_id

    @staticmethod
    def attach(nodes, pod, tenant, cluster):
        context = CapellaContext(pod, tenant, cluster)
        for node in nodes:
            node.capella = context
        return context

    @staticmethod
    def of(server):
        """
        :return: CapellaContext attached to the given server
        :raises CapellaContextMissing: if the node was built without one
        """
        context = getattr(server, "capella", None)
        if context is None:
            raise CapellaContextMissing(server)
        return context


class CapellaContextMissing(Exception):
    def __init__(self, server):
        super(CapellaContextMissing, self).__init__(
            "Node %s has type=dedicated but no Capella context attached. "
            "Nodes of a dedicated cluster must be built with "
            "CapellaContext.attach()" % getattr(server, "ip", server))
