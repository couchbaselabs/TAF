from cb_constants.CBServer import CbServer


class ClusterRun(CbServer):
    is_enabled = False

    port = 9000
    memcached_port = 12000

    ssl_port = 19000
    ssl_memcached_port = 11999
