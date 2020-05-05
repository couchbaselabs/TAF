from basetestcase import BaseTestCase
from cluster_utils.cluster_ready_functions import ClusterUtils


class XDCRNewBaseTest(BaseTestCase):
    def setUp(self):
        super(XDCRNewBaseTest, self).setUp()
        self.clusters = self.get_clusters()
        self.task = self.get_task()
        self.taskmgr = self.get_task_mgr()
        for cluster in self.clusters:
            cluster_util = ClusterUtils(cluster, self.taskmgr)
            cluster_util.add_all_nodes_then_rebalance(cluster.servers[1:])

    def tearDown(self):
        super(XDCRNewBaseTest, self).tearDown()
