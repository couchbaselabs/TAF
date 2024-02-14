from cb_constants.CBServer import CbServer
from cb_constants.ClusterRun import ClusterRun

constants = CbServer
try:
    from TestInput import TestInputSingleton
    if TestInputSingleton.input:
        global_port = TestInputSingleton.input.param("port", None)
        for server in TestInputSingleton.input.servers:
            if (global_port == ClusterRun.port) \
                    or (int(server.port) == ClusterRun.port):
                constants = ClusterRun
                break
except:
    pass
