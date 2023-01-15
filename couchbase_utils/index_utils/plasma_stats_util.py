'''
Created on 22-Aug-2021

@author: sanjit chauhan
'''
import json

from Cb_constants import constants
from global_vars import logger
from index_utils.index_ready_functions import IndexUtils
from membase.api.rest_client import RestConnection


class PlasmaStatsUtil(IndexUtils):

    def __init__(self, index_node, server_task=None, cluster=None, storage_stat=None, index_stat=None):
        self.task = server_task
        self.task_manager = self.task.jython_task_manager
        self.log = logger.get("test")
        self.cluster =cluster
        self.index_node = index_node
        self.storage_stat = storage_stat
        self.index_stat = index_stat


    def get_index_baseURL(self, index_node=None):
        rest_client = RestConnection(self.index_node)
        api = rest_client.indexUrl
        return api

    def get_index_storage_stats(self, index_node=None, timeout=120):
        rest_client = RestConnection(self.index_node)
        if index_node is None:
            index_node = self.index_node
        api = rest_client.indexUrl + 'stats/storage'
        self.log.info("api is:"+str(api))
        content = None
        counter = 0
        while content is None:
            rest_client = RestConnection(index_node)
            status, content, header = rest_client._http_request(api, timeout=timeout)
            if not status:
                self.log.debug("Status not true {}".format(status))
                counter += 1
                raise Exception(content)
            elif content.find("Indexer In Warmup") != -1:
                content = None
                continue
            else:
                break
        json_parsed = json.loads(content)
        index_storage_stats = {}
        for index_stats in json_parsed:
            bucket = index_stats["Index"].split(":")[0]
            index_name = index_stats["Index"].split(":")[-1]
            if bucket not in list(index_storage_stats.keys()):
                index_storage_stats[bucket] = {}
            if index_name is not '#primary':
                index_storage_stats[bucket][index_name] = index_stats["Stats"]
        return index_storage_stats

    def get_index_stats(self, index_node=None, timeout=120):
        if index_node is None:
            index_node = self.index_node
        rest_client = RestConnection(index_node)
        api = self.get_index_baseURL() + 'stats'
        status, content, header = rest_client._http_request(api, timeout=timeout)
        return status, content, header

    def get_all_index_stat_map(self, index_node=None, timeout=120):
        status, content, header = self.get_index_stats(index_node,timeout)
        if status:
            json_parsed = json.loads(content)
            index_map = self.get_bucket_index_stats(json_parsed)
            index_stat_map = self.get_indexer_stats(json_parsed)
            index_stat_map['bucket_index_map'] = index_map
            return index_stat_map

    def get_indexer_stats(self, json_parsed):
        index_stats_map = dict()
        for key in list(json_parsed.keys()):
            tokens = key.split(":")
            val = json_parsed[key]
            if len(tokens) == 1:
                field = tokens[0]
                index_stats_map[field] = val
        return index_stats_map

    def get_bucket_index_stats(self, parsed):
        index_map = {}
        for key in list(parsed.keys()):
            tokens = key.split(":")
            val = parsed[key]
            if len(tokens) == 3:
                bucket = tokens[0]
                index_name = tokens[1]
                stats_name = tokens[2]
                if bucket not in list(index_map.keys()):
                    index_map[bucket] = {}
                if index_name not in list(index_map[bucket].keys()):
                    index_map[bucket][index_name] = {}
                index_map[bucket][index_name][stats_name] = val
            elif len(tokens) == 5:
                bucket = tokens[0]
                scope_name = tokens[1]
                collection_name = tokens[2]
                index_name = tokens[3]
                stats_name = tokens[4]
                if bucket not in index_map:
                    index_map[bucket] = dict()
                if scope_name not in index_map[bucket]:
                    index_map[bucket][scope_name] = dict()
                if collection_name not in index_map[bucket][scope_name]:
                    index_map[bucket][scope_name][collection_name] = dict()
                if index_name not in index_map[bucket][scope_name][collection_name]:
                    index_map[bucket][scope_name][collection_name][index_name] = dict()
                index_map[bucket][scope_name][collection_name][index_name][stats_name] = val
        return index_map
