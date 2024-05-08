import time

from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI

class PauseResume(ColumnarBaseTest):
    def setUp(self):
        super(PauseResume, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]
        self.columnarAPI = ColumnarAPI(self.pod.url_public, '', '', self.tenant.user,
                                       self.tenant.pwd, '')
        self.doc_size = self.input.param("doc_size", 1000)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "regressions.copy_to_s3"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        """
        Delete all the analytics link and columnar instance
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def test_manual_pause_resume(self):
        resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning off instance")
        else:
            self.fail("Failed to turn off instance")
        status = None
        start_time = time.time()
        while (status == 'turning_off' or not status) and time.time() < start_time + 900:
            resp = self.columnar_utils.get_instance_info(self.pod.url_public, self.tenant, self.tenant.project_id,
                                                         self.cluster.instance_id)
            status = resp["data"]["state"]
        if status == "turned_off":
            self.log.info("Instance pause successful")
        else:
            self.fail("Failed to pause instance")

        # resume the instance
        resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning on instance")
        else:
            self.fail("Failed to turn on instance")
        status = None
        start_time = time.time()
        while (status == 'turning_on' or not status) and time.time() < start_time + 900:
            resp = self.columnar_utils.get_instance_info(self.pod.url_public, self.tenant, self.tenant.project_id,
                                                         self.cluster.instance_id)
            status = resp["data"]["state"]
        if status == "healthy":
            self.log.info("Instance resume successful")
        else:
            self.fail("Failed to resume instance")