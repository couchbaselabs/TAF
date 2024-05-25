"""
Created on 4-April-2024

@author: umang.agrawal@couchbase.com
"""
from cbas.cbas_base import CBASBaseTest
from cbas_utils.cbas_utils_columnar import CbasUtil as columnarCBASUtil


class ColumnarOnPremBase(CBASBaseTest):

    def setUp(self):
        super(ColumnarOnPremBase, self).setUp()
        self.use_sdk_for_cbas = self.input.param("use_sdk_for_cbas", False)
        self.columnar_cbas_utils = columnarCBASUtil(
            self.task, self.use_sdk_for_cbas)

        self.aws_region = self.input.param("aws_region", "ap-south-1")
        self.s3_source_bucket = self.input.param("s3_source_bucket", None)

    def tearDown(self):
        super(ColumnarOnPremBase, self).tearDown()
