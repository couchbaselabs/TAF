import json
import urllib

import requests
from copy import deepcopy
from random import choice

from cb_constants import CbServer
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI


class SystemEventRestHelper:
    def __init__(self, server_list):
        """
        :param server_list: Valid list of servers through which event
                            specific APIs can be accessed"""
        self.servers = server_list

    def get_rest_object(self, rest, server, username, password):
        def update_auth(r_obj, u_name, p_word):
            if u_name is not None:
                r_obj = deepcopy(r_obj)
                r_obj.username = u_name
                r_obj.password = p_word
            return r_obj
        if rest is not None:
            return update_auth(rest, username, password)

        if server:
            rest = ClusterRestAPI(server)
        else:
            rest = ClusterRestAPI(choice(self.servers))
        rest = update_auth(rest, username, password)
        return rest

    def create_event(self, event_dict, rest=None, server=None,
                     username=None, password=None):
        """
        Create event in the cluster as per event_dict
        :param event_dict: Dictionary containing parameters for an event
        :param rest: RestConnection object to send requests
        :param server: Target server to create RestConnection
        :param username: Username auth to use during API operations
        :param password: Password auth to use during API operations
        """
        rest = self.get_rest_object(rest, server, username, password)
        return rest.create_system_event(event_dict)

    def create_event_stream(self, rest=None, server=None,
                            username=None, password=None):
        """
        Creates an event stream object to the specific cluster node
        :param rest: RestConnection object to send requests
        :param server: Target server to create RestConnection
        :param username: Username auth to use during API operations
        :param password: Password auth to use during API operations
        """
        rest = self.get_rest_object(rest, server, username, password)
        status, content, _ = rest.get_system_event_streaming()
        return content

    def get_events(self, rest=None, server=None, username=None, password=None,
                   since_time=None, events_count=None):
        """
        Fetches events from the cluster_node with respect to
        optional since_time and event_count
        :param rest: RestConnection object to send requests
        :param server: Target server to create RestConnection
        :param username: Username auth to use during API operations
        :param password: Password auth to use during API operations
        :param since_time: Time from which the events needs to be fetched
        :param events_count: Number of events to fetch from the specific
                           'since_time' value"""
        rest = self.get_rest_object(rest, server, username, password)
        status, content = rest.get_system_event_logs(since_time, events_count)
        return content

    def update_max_events(self,
                          max_event_count=CbServer.sys_event_def_logs,
                          rest=None, server=None,
                          username=None, password=None):
        """
        Update the maximum number of events that will be stored in the cluster
        :param max_event_count: New event count to be updated in the cluster
        :param rest: RestConnection object to send requests
        :param server: Target server to create RestConnection
        :param username: Username auth to use during API operations
        :param password: Password auth to use during API operations
        :return: status, content
        """
        rest = self.get_rest_object(rest, server, username, password)
        return rest.set_internal_settings("eventLogsLimit", max_event_count)
