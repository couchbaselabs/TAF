"""
Created on January 9, 2026

@author: Thuan Nguyen
"""
import time
from pytests.Capella.RestAPIv4.ExpressScaling.\
    enable_cluster_express_scaling import EnableClusterExpressScaling


class DisableClusterExpressScaling(EnableClusterExpressScaling):

    def setUp(self, nomenclature="DisableExpressScaling_PUT"):
        EnableClusterExpressScaling.setUp(self, nomenclature)

    def tearDown(self):
        super(DisableClusterExpressScaling, self).tearDown()
 

    def disable_express_scaling(self):
        self.wait_for_cluster_healthy()
        result = self.capellaAPI.cluster_ops_apis.disable_express_scaling(
            self.organisation_id, self.project_id, self.cluster_id)
        if result.status_code != 204:
            self.fail("Failed to disable express scaling. Reason: {}".format(result.content))
        time.sleep(15)
        cluster_express_scaling_status = self.get_cluster_express_scaling_status()
        print("************* cluster_express_scaling_status after disabling: {} *************".format(cluster_express_scaling_status))
        if cluster_express_scaling_status=="disabled":
            self.wait_for_cluster_healthy()
