from cb_constants import constants


class CouchbaseCluster:
    state = None
    def __init__(self, name="default", username="Administrator",
                 password="password", paths=None, servers=None, vbuckets=1024):
        self.name = name
        self.username = username
        self.password = password
        self.paths = paths
        self.srv = None
        self.master = servers[0]
        self.ram_settings = dict()
        self.servers = servers
        self.kv_nodes = list()
        self.fts_nodes = list()
        self.cbas_nodes = list()
        self.index_nodes = list()
        self.query_nodes = list()
        self.eventing_nodes = list()
        self.backup_nodes = list()
        self.serviceless_nodes = list()
        self.nodes_in_cluster = list()
        self.xdcr_remote_clusters = list()
        self.buckets = list()
        self.vbuckets = vbuckets
        # version = 9.9.9-9999
        # edition = community / enterprise
        # type = default / serverless / dedicated
        self.version = None
        self.edition = None
        self.type = "default"

        # SDK related objects
        self.sdk_client_pool = None
        # Note: Referenced only for sdk_client3.py SDKClient
        self.sdk_auth = None
        self.sdk_cluster_options = None

        # Capella specific params
        self.pod = None
        self.tenant = None
        self.cluster_config = None

        # Bucket Serverless Topology
        self.bucketDNNodes = dict()
        self.bucketNodes = dict()


class OtpNode(object):
    def __init__(self, id='', status=''):
        self.id = id
        self.ip = ''
        self.replication = ''
        self.port = constants.port
        self.gracefulFailoverPossible = 'true'
        # extract ns ip from the otpNode string
        # its normally ns_1@10.20.30.40
        if id.find('@') >= 0:
            self.ip = id[id.index('@') + 1:]
            if self.ip.count(':') > 0:
                # raw ipv6? enclose in square brackets
                self.ip = '[' + self.ip + ']'
        self.status = status


class Node(object):
    def __init__(self):
        self.uptime = 0
        self.memoryTotal = 0
        self.memoryFree = 0
        self.mcdMemoryReserved = 0
        self.mcdMemoryAllocated = 0
        self.status = ""
        self.hostname = ""
        self.clusterCompatibility = ""
        self.clusterMembership = ""
        self.version = ""
        self.os = ""
        self.ports = []
        self.availableStorage = []
        self.storage = []
        self.memoryQuota = 0
        self.memcached = constants.memcached_port
        self.id = ""
        self.ip = ""
        self.rest_username = ""
        self.rest_password = ""
        self.port = constants.port
        self.services = []
        self.storageTotalRam = 0
        self.server_group = ""
        self.limits = None
        self.utilization = None
        self.cpuCount = 0
        self.gracefulFailoverPossible = 'true'

    def __str__(self):
        ip_str = "ip:{0} port:{1}".format(self.ip, self.port)
        return ip_str

    def __repr__(self):
        ip_str = "ip:{0} port:{1}".format(self.ip, self.port)
        return ip_str


class NodeDataStorage(object):
    def __init__(self):
        self.type = ''  # hdd or ssd
        self.path = ''
        self.index_path = ''
        self.cbas_path = ''
        self.data_path = ''
        self.quotaMb = ''
        self.state = ''  # ok

    def __str__(self):
        return '{0}'.format({'type': self.type,
                             'path': self.path,
                             'index_path': self.index_path,
                             'quotaMb': self.quotaMb,
                             'state': self.state})

    def get_data_path(self):
        return self.path

    def get_index_path(self):
        return self.index_path


class NodeDiskStorage(object):
    def __init__(self):
        self.type = 0
        self.path = ''
        self.sizeKBytes = 0
        self.usagePercent = 0
