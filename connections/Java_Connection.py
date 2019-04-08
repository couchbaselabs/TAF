'''
Created on Nov 6, 2017
Java based SDK client interface

@author: riteshagarwal

'''
from com.couchbase.client.java import Cluster
from com.couchbase.client.core.error import CouchbaseException
from java.util.logging import Logger, Level, ConsoleHandler
from com.couchbase.client.java.env import ClusterEnvironment
from com.couchbase.client.core.service import KeyValueServiceConfig
from java.util.concurrent import TimeUnit
from java.lang import System
from com.couchbase.client.core.env import TimeoutConfig
from java.time import Duration


class SDKClient(object):
    """Java SDK Client Implementation for testrunner - master branch Implementation"""

    def __init__(self, server):
        self.server = server
        self.username = self.server.rest_username
        self.password = self.server.rest_password
        self.cluster = None
        self.clusterManager = None

    def __del__(self):
        self.disconnectCluster()

    def connectCluster(self, username=None, password=None):
        if username:
            self.username = username
        if password:
            self.password = password
        try:
            System.setProperty("com.couchbase.forceIPv4", "false")
            logger = Logger.getLogger("com.couchbase.client")
            logger.setLevel(Level.SEVERE)
            for h in logger.getParent().getHandlers():
                if isinstance(h, ConsoleHandler):
                    h.setLevel(Level.SEVERE)
            self.cluster = Cluster.connect(
                ClusterEnvironment.builder(",".join(self.server.ip),
                                           self.username, self.password)
                .timeoutConfig(TimeoutConfig.builder().kvTimeout(Duration.ofSeconds(10)))
                .build())
        except CouchbaseException:
            print "cannot login from user: %s/%s"%(self.username, self.password)
            raise

    def reconnectCluster(self):
        self.disconnectCluster()
        self.connectCluster()

    def disconnectCluster(self):
        self.cluster.disconnect()