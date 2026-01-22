"""
Created on January 20, 2026

@author: Thuan Nguyen
"""

import copy
from pytests.Capella.RestAPIv4.AppEndpoints.get_app_endpoints \
    import GetAppEndpoints


class PostAppEndpointsLogStreaming(GetAppEndpoints):

    def setUp(self, nomenclature="AppEndpointsLogStreaming_POST"):
        GetAppEndpoints.setUp(self, nomenclature)

    def tearDown(self):
        super(PostAppEndpoints, self).tearDown()

