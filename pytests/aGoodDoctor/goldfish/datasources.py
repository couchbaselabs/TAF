'''
Created on Nov 3, 2023

@author: ritesh.agarwal
'''
# from mongo.loader import MongoSDKClient
import time

from com.couchbase.test.sdk import Server
from com.couchbase.test.taskmanager import TaskManager
from com.mongo.loader import MongoSDKClient, WorkLoadGenerate, DocumentGenerator
from com.couchbase.test.docgen import DocRange
from com.couchbase.test.docgen import DRConstants
from com.couchbase.test.docgen import WorkLoadSettings
from java.util import HashMap
from TestInput import TestInputSingleton


class MongoDB(object):

    def __init__(self, hostname, username, password, atlas=False):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = 27017
        self.atlas = atlas
        self.connString = "mongodb"
        self.type = "onPrem"
        self.collections = list()

        if self.atlas:
            self.connString = self.connString + "+srv";
            self.type = "onCloud"
        self.connString = self.connString + "://" + self.username + ":" + self.password + "@" + self.hostname + ":27017/"

    def set_mongo_database(self, name):
        self.database = name

    def set_mongo_collections(self):
        for i in range(self.loadDefn.get("collections")):
            self.collections.append("volCollection" + str(i))

    def create_link(self, link_name):
        link_cmd = 'CREATE LINK Default.' + link_name + " TYPE KAFKA WITH { 'sourceDetails': {'source': 'MONGODB', 'connectionFields':{ 'connectionUri': '" + self.connString + '\' }}};'
        return link_cmd


class MongoWorkload():

    def _loader_dict(self, databases, overRidePattern=None, workers=10, cmd={}):
        self.workers = workers
        self.loader_map = dict()
        self.default_pattern = [100, 0, 0, 0, 0]
        for database in databases:
            pattern = overRidePattern or database.loadDefn.get("pattern", self.default_pattern)
            for i, collection in enumerate(database.collections):
                workloads = database.loadDefn.get("collections_defn", [database.loadDefn])
                valType = workloads[i % len(workloads)]["valType"]
                ws = WorkLoadSettings(cmd.get("keyPrefix", database.key),
                                      cmd.get("keySize", database.key_size),
                                      cmd.get("docSize", database.loadDefn.get("doc_size")),
                                      cmd.get("cr", pattern[0]),
                                      cmd.get("rd", pattern[1]),
                                      cmd.get("up", pattern[2]),
                                      cmd.get("dl", pattern[3]),
                                      cmd.get("ex", pattern[4]),
                                      cmd.get("workers", workers),
                                      cmd.get("ops", database.loadDefn.get("ops")),
                                      cmd.get("loadType", None),
                                      cmd.get("keyType", database.key_type),
                                      cmd.get("valueType", valType),
                                      cmd.get("validate", False),
                                      cmd.get("gtm", False),
                                      cmd.get("deleted", False),
                                      cmd.get("mutated", 0)
                                      )
                hm = HashMap()
                hm.putAll({DRConstants.create_s: database.create_start,
                           DRConstants.create_e: database.create_end,
                           DRConstants.update_s: database.update_start,
                           DRConstants.update_e: database.update_end,
                           DRConstants.expiry_s: database.expire_start,
                           DRConstants.expiry_e: database.expire_end,
                           DRConstants.delete_s: database.delete_start,
                           DRConstants.delete_e: database.delete_end,
                           DRConstants.read_s: database.read_start,
                           DRConstants.read_e: database.read_end})
                dr = DocRange(hm)
                ws.dr = dr
                dg = DocumentGenerator(ws, database.key_type, valType)
                self.loader_map.update({database.name+collection: dg})

    def perform_load(self, databases=None, wait_for_load=True, overRidePattern=None, tm=None):
        self.doc_loading_tm = tm
        self._loader_dict(databases, overRidePattern)
        self.tasks = list()
        i = self.workers
        while i > 0:
            for database in databases:
                master = Server(database.hostname, database.port,
                                database.username, database.password,
                                "")
                for collection in database.collections:
                    client = MongoSDKClient(master, database.name, collection, database.atlas)
                    client.connectCluster()
                    time.sleep(1)
                    taskName = "Loader_%s_%s_%s" % (database.name, collection, time.time())
                    task = WorkLoadGenerate(taskName, self.loader_map[database.name+collection], client)
                    self.tasks.append(task)
                    self.doc_loading_tm.submit(task)
                    i -= 1

        if wait_for_load:
            self.wait_for_doc_load_completion()
        else:
            return self.tasks

    def wait_for_doc_load_completion(self):
        self.doc_loading_tm.getAllTaskResult()

