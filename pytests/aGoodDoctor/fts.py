'''
Created on May 31, 2022

@author: ritesh.agarwal
'''

from FtsLib.FtsOperations import FtsHelper
from global_vars import logger
from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection
import time
import json
from Cb_constants.CBServer import CbServer


class DoctorFTS:

    def __init__(self, cluster, bucket_util, num_indexes):
        self.cluster = cluster
        self.bucket_util = bucket_util
        self.input = TestInputSingleton.input
        self.fts_index_partitions = self.input.param("fts_index_partition", 6)
        self.log = logger.get("test")
        self.fts_helper = FtsHelper(self.cluster.fts_nodes[0])
        self.indexes = dict()
        self.stop_run = False
        i = 0
        while i < num_indexes:
            for b in self.cluster.buckets:
                for s in self.bucket_util.get_active_scopes(b, only_names=True):
                    for c in sorted(self.bucket_util.get_active_collections(b, s, only_names=True)):
                        if c == CbServer.default_collection:
                            continue
                        fts_param_template = self.get_fts_idx_template()
                        fts_param_template.update({
                            "name": "fts_idx_{}".format(i), "sourceName": b.name})
                        fts_param_template["planParams"].update({
                            "indexPartitions": self.fts_index_partitions})
                        fts_param_template["params"]["mapping"]["types"].update({
                            "%s.%s" % (s, c): {
                                "dynamic": True, "enabled": True}
                            }
                        )
                        fts_param_template = str(fts_param_template).replace("True", "true")
                        fts_param_template = str(fts_param_template).replace("False", "false")
                        fts_param_template = str(fts_param_template).replace("'", "\"")
                        self.indexes.update({"fts_idx_"+str(i): fts_param_template})
                        i += 1
                        if i >= num_indexes:
                            break
                    if i >= num_indexes:
                        break
                if i >= num_indexes:
                    break

    def discharge_FTS(self):
        self.stop_run = True

    def get_fts_idx_template(self):
        fts_idx_template = {
            "type": "fulltext-index",
            "name": "fts-index",
            "sourceType": "gocbcore",
            "sourceName": "default",
            "planParams": {
                "maxPartitionsPerPIndex": 1024,
                "indexPartitions": 1
             },
            "params": {
                "doc_config": {
                    "docid_prefix_delim": "",
                    "docid_regexp": "",
                    "mode": "scope.collection.type_field",
                    "type_field": "type"
                    },
                "mapping": {
                    "analysis": {},
                    "default_analyzer": "standard",
                    "default_datetime_parser": "dateTimeOptional",
                    "default_field": "_all",
                    "default_mapping": {
                        "dynamic": True,
                        "enabled": False
                        },
                    "default_type": "_default",
                    "docvalues_dynamic": False,
                    "index_dynamic": True,
                    "store_dynamic": False,
                    "type_field": "_type",
                    "types": {}
                    },
                "store": {
                    "indexType": "scorch",
                    "segmentVersion": 15
                    }
                },
            "sourceParams": {}
           }
        return fts_idx_template

    def create_fts_indexes(self):
        status = False
        for name, index in self.indexes.items():
            self.log.debug("Creating fts index: {}".format(name))
            status, _ = self.fts_helper.create_fts_index_from_json(
                name, str(index))
        return status

    def wait_for_fts_index_online(self, item_count, timeout=86400):
        status = False
        for index_name, _ in self.indexes.items():
            status = False
            stop_time = time.time() + timeout
            while time.time() < stop_time:
                status, content = self.fts_helper.fts_index_item_count(index_name)
                self.log.debug("index: {}, status: {}, count: {}"
                               .format(index_name, status,
                                       json.loads(content)["count"]))
                if json.loads(content)["count"] == item_count:
                    self.log.info("FTS index is ready: {}".format(index_name))
                    status = True
                    break
                time.sleep(5)
            if status is False:
                return status
        return status

    def drop_fts_indexes(self, idx_name):
        """
        Drop count number of fts indexes using fts name
        from fts_dict
        """
        self.log.debug("Dropping fts index: {}".format(idx_name))
        status, _ = self.fts_helper.delete_fts_index(idx_name)
        return status
