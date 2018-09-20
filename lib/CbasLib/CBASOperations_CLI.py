'''
Created on Nov 14, 2017

@author: riteshagarwal
'''

import logger
from ClusterLib.ClusterOperations_Rest import ClusterHelper as cluster_helper_rest
log = logger.Logger.get_logger()
from remote.remote_util import RemoteMachineShellConnection

class CBASHelper(cluster_helper_rest):

    def __init__(self, server, username, password, cb_version=None):
        self.server = server
        self.hostname = "%s:%s" % (server.ip, server.port)
        self.username = username
        self.password = password
        self.cb_version = cb_version
        super(CBASHelper, self).__init__(server)
        
    def execute_statement_on_cbas(self, statement, server, mode=None):
        """
        Executes a statement on CBAS using the REST API through curl command
        """
        shell = RemoteMachineShellConnection(server)
        if mode:
            output, error = shell.execute_command(
                """curl -s --data pretty=true --data mode={2} --data-urlencode 'statement={1}' http://{0}:8095/analytics/service -v -u {3}:{4}""".format(
                    self.cbas_node.ip, statement, mode, self.cbas_node.rest_username, self.cbas_node.rest_password))
        else:
            output, error = shell.execute_command(
            """curl -s --data pretty=true --data-urlencode 'statement={1}' http://{0}:8095/analytics/service -v -u {2}:{3}""".format(
                self.cbas_node.ip, statement, self.cbas_node.rest_username, self.cbas_node.rest_password))
        response = ""
        for line in output:
            response = response + line
        response = json.loads(response)
        self.log.info(response)
        shell.disconnect()
 
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
 
        return response["status"], response["metrics"], errors, results, handle