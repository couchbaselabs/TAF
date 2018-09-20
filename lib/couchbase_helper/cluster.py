#from tasks.future import Future
import logging
from tasks.taskmanager import TaskManager
import tasks.tasks as conc
import types


"""An API for scheduling tasks that run against Couchbase Server

This module is contains the top-level API's for scheduling and executing tasks. The
API provides a way to run task do syncronously and asynchronously.
"""

log = logging.getLogger(__name__)

class ServerTasks(object):
    """A Task API for performing various operations synchronously or asynchronously on Couchbase cluster."""

    def __init__(self):
        self.task_manager = TaskManager("Cluster_Thread")

    def async_create_bucket(self, server, bucket):
        """Asynchronously creates the default bucket

        Parameters:
            bucket_params - a dictionary containing bucket creation parameters. (Dict)
        Returns:
            BucketCreateTask - A task future that is a handle to the scheduled task."""
#         bucket_params['bucket_name'] = 'default'
        _task = conc.BucketCreateTask(server, bucket, task_manager=self.task_manager)
        self.task_manager.schedule(_task)
        return _task

    def sync_create_bucket(self, server, bucket):
        """Synchronously creates the default bucket

        Parameters:
            bucket_params - a dictionary containing bucket creation parameters. (Dict)
        Returns:
            BucketCreateTask - A task future that is a handle to the scheduled task."""
#         bucket_params['bucket_name'] = 'default'
        _task = conc.BucketCreateTask(server, bucket, task_manager=self.task_manager)
        self.task_manager.schedule(_task)
        return _task.get_result()
    
    def async_bucket_delete(self, server, bucket='default'):
        """Asynchronously deletes a bucket

        Parameters:
            server - The server to delete the bucket on. (TestInputServer)
            bucket - The name of the bucket to be deleted. (String)

        Returns:
            BucketDeleteTask - A task future that is a handle to the scheduled task."""
        _task = conc.BucketDeleteTask(server, self.task_manager, bucket)
        self.task_manager.schedule(_task)
        return _task

    def async_failover(self, servers=[], failover_nodes=[], graceful=False,
                       use_hostnames=False, wait_for_pending=0):
        """Asynchronously failover a set of nodes

        Parameters:
            servers - servers used for connection. (TestInputServer)
            failover_nodes - The set of servers that will under go failover .(TestInputServer)
            graceful = True/False. True - graceful, False - hard. (Boolean)

        Returns:
            FailOverTask - A task future that is a handle to the scheduled task."""
        _task = conc.FailoverTask(servers, task_manager=self.task_manager,
                             to_failover=failover_nodes,
                             graceful=graceful, use_hostnames=use_hostnames,
                             wait_for_pending=wait_for_pending)
        self.task_manager.schedule(_task)
        return _task

    def async_init_node(self, server, disabled_consistent_view=None,
                        rebalanceIndexWaitingDisabled=None, rebalanceIndexPausingDisabled=None,
                        maxParallelIndexers=None, maxParallelReplicaIndexers=None, port=None,
                        quota_percent=None, services=None, index_quota_percent=None, gsi_type='forestdb'):
        """Asynchronously initializes a node

        The task scheduled will initialize a nodes username and password and will establish
        the nodes memory quota to be 2/3 of the available system memory.

        Parameters:
            server - The server to initialize. (TestInputServer)
            disabled_consistent_view - disable consistent view
            rebalanceIndexWaitingDisabled - index waiting during rebalance(Boolean)
            rebalanceIndexPausingDisabled - index pausing during rebalance(Boolean)
            maxParallelIndexers - max parallel indexers threads(Int)
            index_quota_percent - index quote used by GSI service (added due to sherlock)
            maxParallelReplicaIndexers - max parallel replica indexers threads(int)
            port - port to initialize cluster
            quota_percent - percent of memory to initialize
            services - can be kv, n1ql, index
            gsi_type - Indexer Storage Mode
        Returns:
            NodeInitTask - A task future that is a handle to the scheduled task."""
        _task = conc.NodeInitializeTask(server, self.task_manager, disabled_consistent_view,
                                        rebalanceIndexWaitingDisabled, rebalanceIndexPausingDisabled,
                                        maxParallelIndexers, maxParallelReplicaIndexers,
                                        port, quota_percent, services=services,
                                        index_quota_percent=index_quota_percent,
                                        gsi_type=gsi_type)

        self.task_manager.schedule(_task)
        return _task

    def async_load_gen_docs(self, server, bucket, generator, kv_store, op_type, exp=0, flag=0, only_store_hash=True,
                            batch_size=1, pause_secs=1, timeout_secs=5, proxy_client=None, compression=True):
        log.info("BATCH SIZE for documents load: %s" % batch_size)
        if isinstance(generator, list):
                _task = conc.LoadDocumentsGeneratorsTask(server, self.task_manager, bucket, generator, kv_store, op_type, exp, flag, only_store_hash, batch_size, compression=compression)
        else:
                _task = conc.LoadDocumentsGeneratorsTask(server, self.task_manager, bucket, [generator], kv_store, op_type, exp, flag, only_store_hash, batch_size, compression=compression)

        self.task_manager.schedule(_task)
        return _task
    
#     def async_load_gen_docs_java(self, server, bucket, start_from, num_items=10000):
#         def read_json_tempelate(path):
#             import json
#             istream = open(path);
#             with istream as data_file:    
#                 data = json.load(data_file)
#             return data["key"], data["value"]
#         
#         path = "b/testdata.json"
#         k,v = read_json_tempelate(path)
#         
#         _task = conc.LoadDocumentsTask_java(self.task_manager, server, bucket, num_items, start_from, k, v)
# 
#         self.task_manager.schedule(_task)
#         return _task

    def async_rebalance(self, servers, to_add, to_remove, use_hostnames=False, services = None, check_vbucket_shuffling=True):
        """Asyncronously rebalances a cluster

        Parameters:
            servers - All servers participating in the rebalance ([TestInputServers])
            to_add - All servers being added to the cluster ([TestInputServers])
            to_remove - All servers being removed from the cluster ([TestInputServers])
            use_hostnames - True if nodes should be added using hostnames (Boolean)

        Returns:
            RebalanceTask - A task future that is a handle to the scheduled task"""
        _task = conc.RebalanceTask(servers, self.task_manager, to_add, to_remove, use_hostnames=use_hostnames, services = services, check_vbucket_shuffling=check_vbucket_shuffling)
        self.task_manager.schedule(_task)
        return _task

    def async_wait_for_stats(self, servers, bucket, param, stat, comparison, value):
        """Asynchronously wait for stats

        Waits for stats to match the criteria passed by the stats variable. See
        couchbase.stats_tool.StatsCommon.build_stat_check(...) for a description of
        the stats structure and how it can be built.

        Parameters:
            servers - The servers to get stats from. Specifying multiple servers will
                cause the result from each server to be added together before
                comparing. ([TestInputServer])
            bucket - The name of the bucket (String)
            param - The stats parameter to use. (String)
            stat - The stat that we want to get the value from. (String)
            comparison - How to compare the stat result to the value specified.
            value - The value to compare to.

        Returns:
            RebalanceTask - A task future that is a handle to the scheduled task"""
        _task = conc.StatsWaitTask(servers, bucket, param, stat, comparison, value, task_manager=self.task_manager)
        self.task_manager.schedule(_task)
        return _task

    def create_default_bucket(self, bucket_params, timeout=600):
        """Synchronously creates the default bucket

        Parameters:
            bucket_params - A dictionary containing a list of bucket creation parameters. (Dict)

        Returns:
            boolean - Whether or not the bucket was created."""

        _task = self.async_create_default_bucket(bucket_params)
        return _task.get_result(timeout)

    def create_sasl_bucket(self, name, password,bucket_params, timeout=None):
        """Synchronously creates a sasl bucket

        Parameters:
            bucket_params - A dictionary containing a list of bucket creation parameters. (Dict)

        Returns:
            boolean - Whether or not the bucket was created."""

        _task = self.async_create_sasl_bucket(name, password, bucket_params)
        self.task_manager.schedule(_task)
        return _task.get_result(timeout)

    def create_standard_bucket(self, name, port, bucket_params, timeout=None):
        """Synchronously creates a standard bucket
        Parameters:
            bucket_params - A dictionary containing a list of bucket creation parameters. (Dict)
        Returns:
            boolean - Whether or not the bucket was created."""
        _task = self.async_create_standard_bucket(name, port, bucket_params)
        return _task.get_result(timeout)

    def bucket_delete(self, server, bucket='default', timeout=None):
        """Synchronously deletes a bucket

        Parameters:
            server - The server to delete the bucket on. (TestInputServer)
            bucket - The name of the bucket to be deleted. (String)

        Returns:
            boolean - Whether or not the bucket was deleted."""
        _task = self.async_bucket_delete(server, bucket)
        return _task.get_result(timeout)

    def init_node(self, server, async_init_node=True, disabled_consistent_view=None, services = None, index_quota_percent = None):
        """Synchronously initializes a node

        The task scheduled will initialize a nodes username and password and will establish
        the nodes memory quota to be 2/3 of the available system memory.

        Parameters:
            server - The server to initialize. (TestInputServer)
            index_quota_percent - index quota percentage
            disabled_consistent_view - disable consistent view

        Returns:
            boolean - Whether or not the node was properly initialized."""
        _task = self.async_init_node(server, async_init_node, disabled_consistent_view, services = services, index_quota_percent= index_quota_percent)
        return _task.result()

    def rebalance(self, servers, to_add, to_remove, timeout=None, use_hostnames=False, services = None):
        """Syncronously rebalances a cluster

        Parameters:
            servers - All servers participating in the rebalance ([TestInputServers])
            to_add - All servers being added to the cluster ([TestInputServers])
            to_remove - All servers being removed from the cluster ([TestInputServers])
            use_hostnames - True if nodes should be added using their hostnames (Boolean)
            services - Services definition per Node, default is None (this is since Sherlock release)
        Returns:
            boolean - Whether or not the rebalance was successful"""
        _task = self.async_rebalance(servers, to_add, to_remove, use_hostnames, services = services)
        return _task.get_result(timeout)

    def load_gen_docs(self, server, bucket, generator, kv_store, op_type, exp=0, timeout=None,
                      flag=0, only_store_hash=True, batch_size=1, proxy_client=None, compression=True):
        _task = self.async_load_gen_docs(server, bucket, generator, kv_store, op_type, exp, flag,
                                         only_store_hash=only_store_hash, batch_size=batch_size, 
                                         proxy_client=proxy_client, compression=compression)
        return _task.get_result(timeout)

    def verify_data(self, server, bucket, kv_store, timeout=None, compression=True):
        _task = self.async_verify_data(server, bucket, kv_store, compression=compression)
        return _task.result(timeout)

    def async_verify_data(self, server, bucket, kv_store, max_verify=None,
                          only_store_hash=True, batch_size=1, replica_to_read=None, timeout_sec=5, compression=True):
        if batch_size > 1:
            _task = conc.BatchedValidateDataTask(server, bucket, kv_store, max_verify, only_store_hash, batch_size, 
                                                 timeout_sec, self.task_manager, compression=compression)
        else:
            _task = conc.ValidateDataTask(server, bucket, kv_store, max_verify, only_store_hash, replica_to_read, 
                                          self.task_manager, compression=compression)
        self.task_manager.schedule(_task)
        return _task
    
    def wait_for_stats(self, servers, bucket, param, stat, comparison, value, timeout=None):
        """Synchronously wait for stats

        Waits for stats to match the criteria passed by the stats variable. See
        couchbase.stats_tool.StatsCommon.build_stat_check(...) for a description of
        the stats structure and how it can be built.

        Parameters:
            servers - The servers to get stats from. Specifying multiple servers will
                cause the result from each server to be added together before
                comparing. ([TestInputServer])
            bucket - The name of the bucket (String)
            param - The stats parameter to use. (String)
            stat - The stat that we want to get the value from. (String)
            comparison - How to compare the stat result to the value specified.
            value - The value to compare to.

        Returns:
            boolean - Whether or not the correct stats state was seen"""
        _task = self.async_wait_for_stats(servers, bucket, param, stat, comparison, value)
        return _task.result(timeout)

    def shutdown(self, force=False):
        self.task_manager.shutdown(force)
        if force:
            log.info("Cluster instance shutdown with force")

    def async_n1ql_query_verification(self, server, bucket, query, n1ql_helper=None,
                                      expected_result=None, is_explain_query=False,
                                      index_name=None, verify_results=True, retry_time=2,
                                      scan_consistency=None, scan_vector=None):
        """Asynchronously runs n1ql querya and verifies result if required

        Parameters:
            server - The server to handle query verification task. (TestInputServer)
            query - Query params being used with the query. (dict)
            expected_result - expected result after querying
            is_explain_query - is query explain query
            index_name - index related to query
            bucket - The name of the bucket containing items for this view. (String)
            verify_results -  Verify results after query runs successfully
            retry_time - The time in seconds to wait before retrying failed queries (int)
            n1ql_helper - n1ql helper object
            scan_consistency - consistency value for querying
            scan_vector - scan vector used for consistency
        Returns:
            N1QLQueryTask - A task future that is a handle to the scheduled task."""
        _task = conc.N1QLQueryTask(n1ql_helper = n1ql_helper,
                 server = server, bucket = bucket,
                 query = query, expected_result=expected_result,
                 verify_results = verify_results,
                 is_explain_query = is_explain_query,
                 index_name = index_name,
                 retry_time= retry_time,
                 scan_consistency = scan_consistency,
                 scan_vector = scan_vector, task_manager=self.task_manager)
        self.task_manager.schedule(_task)
        return _task

    def n1ql_query_verification(self, server, bucket, query, n1ql_helper = None,
                                expected_result=None, is_explain_query = False,
                                index_name = None, verify_results = True,
                                scan_consistency = None, scan_vector = None,
                                retry_time=2, timeout = 60):
        """Synchronously runs n1ql querya and verifies result if required

        Parameters:
            server - The server to handle query verification task. (TestInputServer)
            query - Query params being used with the query. (dict)
            expected_result - expected result after querying
            is_explain_query - is query explain query
            index_name - index related to query
            bucket - The name of the bucket containing items for this view. (String)
            verify_results -  Verify results after query runs successfully
            retry_time - The time in seconds to wait before retrying failed queries (int)
            n1ql_helper - n1ql helper object
            scan_consistency - consistency used during querying
            scan_vector - vector used during querying
            timeout - timeout for task
        Returns:
            N1QLQueryTask - A task future that is a handle to the scheduled task."""
        _task = self.async_n1ql_query_verification(n1ql_helper = n1ql_helper,
                 server = server, bucket = bucket,
                 query = query, expected_result=expected_result,
                 is_explain_query = is_explain_query,
                 index_name = index_name,
                 verify_results = verify_results,
                 retry_time= retry_time,
                 scan_consistency = scan_consistency,
                 scan_vector = scan_vector)
        return _task.result(timeout)

    def async_create_index(self, server, bucket, query, n1ql_helper = None,
                           index_name = None, defer_build = False, retry_time=2,
                           timeout = 240):
        """Asynchronously runs create index task

        Parameters:
            server - The server to handle query verification task. (TestInputServer)
            query - Query params being used with the query.
            bucket - The name of the bucket containing items for this view. (String)
            index_name - Name of the index to be created
            defer_build - build is defered
            retry_time - The time in seconds to wait before retrying failed queries (int)
            n1ql_helper - n1ql helper object
            timeout - timeout for index to come online
        Returns:
            CreateIndexTask - A task future that is a handle to the scheduled task."""
        _task = conc.CreateIndexTask(n1ql_helper = n1ql_helper,
                 server = server, bucket = bucket,
                 defer_build = defer_build,
                 index_name = index_name,
                 query = query,
                 retry_time= retry_time,
                 timeout = timeout, task_manager=self.task_manager)
        self.task_manager.schedule(_task)
        return _task

    def async_monitor_index(self, server, bucket, n1ql_helper = None,
                            index_name = None, retry_time=2, timeout = 240):
        """Asynchronously runs create index task

        Parameters:
            server - The server to handle query verification task. (TestInputServer)
            query - Query params being used with the query.
            bucket - The name of the bucket containing items for this view. (String)
            index_name - Name of the index to be created
            retry_time - The time in seconds to wait before retrying failed queries (int)
            timeout - timeout for index to come online
            n1ql_helper - n1ql helper object
        Returns:
            MonitorIndexTask - A task future that is a handle to the scheduled task."""
        _task = conc.MonitorIndexTask(n1ql_helper = n1ql_helper,
                 server = server, bucket = bucket,
                 index_name = index_name,
                 retry_time= retry_time,
                 timeout = timeout, task_manager=self.task_manager)
        self.task_manager.schedule(_task)
        return _task

    def async_build_index(self, server, bucket, query, n1ql_helper = None, retry_time=2):
        """Asynchronously runs create index task

        Parameters:
            server - The server to handle query verification task. (TestInputServer)
            query - Query params being used with the query.
            bucket - The name of the bucket containing items for this view. (String)
            retry_time - The time in seconds to wait before retrying failed queries (int)
            n1ql_helper - n1ql helper object
        Returns:
            BuildIndexTask - A task future that is a handle to the scheduled task."""
        _task = conc.BuildIndexTask(n1ql_helper = n1ql_helper,
                 server = server, bucket = bucket,
                 query = query,
                 retry_time= retry_time, task_manager=self.task_manager)
        self.task_manager.schedule(_task)
        return _task

    def create_index(self, server, bucket, query, n1ql_helper = None, index_name = None,
                     defer_build = False, retry_time=2, timeout= 60):
        """Asynchronously runs drop index task

        Parameters:
            server - The server to handle query verification task. (TestInputServer)
            query - Query params being used with the query.
            bucket - The name of the bucket containing items for this view. (String)
            index_name - Name of the index to be created
            retry_time - The time in seconds to wait before retrying failed queries (int)
            n1ql_helper - n1ql helper object
            defer_build - defer the build
            timeout - timeout for the task
        Returns:
            N1QLQueryTask - A task future that is a handle to the scheduled task."""
        _task = self.async_create_index(n1ql_helper = n1ql_helper,
                 server = server, bucket = bucket,
                 query = query,
                 index_name = index_name,
                 defer_build = defer_build,
                 retry_time= retry_time)
        return _task.result(timeout)

    def async_drop_index(self, server = None, bucket = "default", query = None,
                         n1ql_helper = None, index_name = None, retry_time=2):
        """Synchronously runs drop index task

        Parameters:
            server - The server to handle query verification task. (TestInputServer)
            query - Query params being used with the query.
            bucket - The name of the bucket containing items for this view. (String)
            index_name - Name of the index to be dropped
            retry_time - The time in seconds to wait before retrying failed queries (int)
            n1ql_helper - n1ql helper object
        Returns:
            DropIndexTask - A task future that is a handle to the scheduled task."""
        _task = conc.DropIndexTask(n1ql_helper = n1ql_helper,
                 server = server, bucket = bucket,
                 query = query,
                 index_name = index_name,
                 retry_time= retry_time, task_manager=self.task_manager)
        self.task_manager.schedule(_task)
        return _task

    def drop_index(self, server, bucket, query, n1ql_helper = None,
                   index_name = None, retry_time=2, timeout = 60):
        """Synchronously runs drop index task

        Parameters:
            server - The server to handle query verification task. (TestInputServer)
            query - Query params being used with the query. (dict)
            bucket - The name of the bucket containing items for this view. (String)
            index_name - Name of the index to be created
            retry_time - The time in seconds to wait before retrying failed queries (int)
            n1ql_helper - n1ql helper object
            timeout - timeout for the task
        Returns:
            N1QLQueryTask - A task future that is a handle to the scheduled task."""
        _task = self.async_drop_index(n1ql_helper = n1ql_helper,
                 server = server, bucket = bucket,
                 query = query,
                 index_name = index_name,
                 retry_time= retry_time)
        return _task.result(timeout)

    def failover(self, servers=[], failover_nodes=[], graceful=False, use_hostnames=False,timeout=None):
        """Synchronously flushes a bucket

        Parameters:
            servers - node used for connection (TestInputServer)
            failover_nodes - servers to be failovered, i.e. removed from the cluster. (TestInputServer)
            bucket - The name of the bucket to be flushed. (String)

        Returns:
            boolean - Whether or not the bucket was flushed."""
        _task = self.async_failover(servers, failover_nodes, graceful, use_hostnames)
        return _task.result(timeout)

    def async_bucket_flush(self, server, bucket='default'):
        """Asynchronously flushes a bucket

        Parameters:
            server - The server to flush the bucket on. (TestInputServer)
            bucket - The name of the bucket to be flushed. (String)

        Returns:
            BucketFlushTask - A task future that is a handle to the scheduled task."""
        _task = conc.BucketFlushTask(server,self.task_manager,bucket)
        self.task_manager.schedule(_task)
        return _task

    def bucket_flush(self, server, bucket='default', timeout=None):
        """Synchronously flushes a bucket

        Parameters:
            server - The server to flush the bucket on. (TestInputServer)
            bucket - The name of the bucket to be flushed. (String)

        Returns:
            boolean - Whether or not the bucket was flushed."""
        _task = self.async_bucket_flush(server, bucket)
        return _task.get_result(timeout)

    def async_compact_bucket(self, server, bucket="default"):
        """Asynchronously starts bucket compaction

        Parameters:
            server - source couchbase server
            bucket - bucket to compact

        Returns:
            boolean - Whether or not the compaction started successfully"""
        _task = conc.CompactBucketTask(server, self.task_manager, bucket)
        self.task_manager.schedule(_task)
        return _task

    def compact_bucket(self, server, bucket="default"):
        """Synchronously runs bucket compaction and monitors progress

        Parameters:
            server - source couchbase server
            bucket - bucket to compact

        Returns:
            boolean - Whether or not the cbrecovery completed successfully"""
        _task = self.async_compact_bucket(server, bucket)
        status = _task.get_result()
        return status

    def async_cbas_query_execute(self, master, cbas_server, cbas_endpoint, statement, bucket='default', mode=None, pretty=True):
        """
        Asynchronously execute a CBAS query
        :param master: Master server
        :param cbas_server: CBAS server
        :param cbas_endpoint: CBAS Endpoint URL (/analytics/service)
        :param statement: Query to be executed
        :param bucket: bucket to connect
        :param mode: Query Execution mode
        :param pretty: Pretty formatting
        :return: task with the output or error message
        """
        _task = conc.CBASQueryExecuteTask(master, cbas_server, self.task_manager, cbas_endpoint, statement, bucket,
                                          mode, pretty)
        self.task_manager.schedule(_task)
        return _task
