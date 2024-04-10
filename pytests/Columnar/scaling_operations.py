from Columnar.columnar_base import ColumnarBaseTest


class ColumnarScalingOperations(ColumnarBaseTest):

    def setUp(self):
        super(ColumnarScalingOperations, self).setUp()
        self.instance = self.project.instances[0]

    def tearDown(self):
        super(ColumnarScalingOperations, self).tearDown()

    def test_scale_in_and_scale_out_columnar_instances(self):
        # increase compute units from 2 to 5
        self.columnar_utils.scale_instance(self.instance, 5)
        self.columnar_utils.wait_for_instance_scaling_operation(
            self.instance)
        # decrease compute units from 5 to 2
        self.columnar_utils.scale_instance(self.instance, 2)
        self.columnar_utils.wait_for_instance_scaling_operation(
            self.instance)
