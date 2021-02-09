'''
Created on Mar 8, 2018

@author: riteshagarwal
'''

'''
Created on Jan 4, 2018

@author: riteshagarwal
'''

import json
import os
import time

from cbas_base import CBASBaseTest, TestInputSingleton
from lib.memcached.helper.data_helper import MemcachedClientHelper
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class SQLPP_Composability_CBAS(CBASBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})
        
        super(SQLPP_Composability_CBAS, self).setUp()
            
        if "add_all_cbas_nodes" in self.input.test_params and self.input.test_params["add_all_cbas_nodes"] and len(self.cluster.cbas_nodes) > 0:
            self.otpNodes.append(self.add_all_nodes_then_rebalance(self.cluster.cbas_nodes))
        self.shell = RemoteMachineShellConnection(self.cbas_node)
        
    def tearDown(self):
        super(SQLPP_Composability_CBAS, self).tearDown()

    def test_composability(self):
        bucket_username = "cbadminbucket"
        bucket_password = "password"
        url = 'http://{0}:8095/analytics/service'.format(self.cbas_node.ip)
        files_dict={'union':['non_unary_subplan_01_1_ddl.sqlpp',
                             'non_unary_subplan_01.2.update.sqlpp',
                             'non_unary_subplan_01.3.query.sqlpp',
                             'non_unary_subplan_01.4.query.sqlpp',
                             'non_unary_subplan_01.5.query.sqlpp',
                             'non_unary_subplan_01.6.query.sqlpp'],
                             
                    'inner-join':['non_unary_subplan_02.1.ddl.sqlpp',
                                  'non_unary_subplan_02.2.update.sqlpp',
                                  'non_unary_subplan_02.3.query.sqlpp',
                                  'non_unary_subplan_02.4.query.sqlpp',
                                  'non_unary_subplan_02.5.query.sqlpp',
                                  'non_unary_subplan_02.6.query.sqlpp'],
                    
                    'outer-join':[
                        ]
                }
        
        for key in files_dict.keys():
            for query_file in files_dict[key]:
                cmd = 'curl -s --data pretty=true --data-urlencode "statement@'+os.getcwd()+'/b/resources/non_unary_subplan_01/%s" '%query_file + url + " -u " + bucket_username + ":" + bucket_password
                output, error = self.shell.execute_command(cmd)
                response = ""
                for line in output:
                    response = response + line
                response = json.loads(response)
                self.log.info(response)
         
                if "errors" in response:
                    errors = response["errors"]
                else:
                    errors = None
         
                if "results" in response:
                    results = response["results"]
                else:
                    results = None
         
                if "handle" in response:
                    handle = response["handle"]
                else:
                    handle = None
            
                self.assertTrue(response["status"] == "success")

        self.shell.disconnect()