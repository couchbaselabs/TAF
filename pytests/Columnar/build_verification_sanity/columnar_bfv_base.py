"""
Created on 4-April-2024

@author: umang.agrawal@couchbase.com
"""
from cbas.cbas_base import CBASBaseTest
from cbas_utils.cbas_utils_columnar import CbasUtil as columnarCBASUtil


class BFVBase(CBASBaseTest):

    def setUp(self):
        super(BFVBase, self).setUp()
        self.use_sdk_for_cbas = self.input.param("use_sdk_for_cbas", False)
        self.columnar_cbas_utils = columnarCBASUtil(
            self.task, self.use_sdk_for_cbas)

    def tearDown(self):
        super(BFVBase, self).tearDown()
