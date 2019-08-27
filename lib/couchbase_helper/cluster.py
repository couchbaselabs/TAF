import logging
import tasks.tasks as conc
import Jython_tasks.task as jython_tasks

from couchbase_helper.documentgenerator import doc_generator, \
    SubdocDocumentGenerator
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient as VBucketAwareMemcached
from BucketLib.BucketOperations import BucketHelper


"""An API for scheduling tasks that run against Couchbase Server

This module is contains the top-level API's for scheduling and executing tasks.
The API provides a way to run task do syncronously and asynchronously.
"""


class ServerTasks(object):
    """
    A Task API for performing various operations synchronously or
    asynchronously on Couchbase cluster
    """

    def __init__(self, task_manager):
        self.jython_task_manager = task_manager
        self.log = logging.getLogger("infra")
        self.test_log = logging.getLogger("test")
        self.log.debug("Initiating ServerTasks")

    def async_create_bucket(self, server, bucket):
        """
        Asynchronously creates the default bucket

        Parameters:
          bucket_params - a dictionary containing bucket creation parameters.
        Returns:
          BucketCreateTask - Task future that is a handle to the scheduled task
        """
#         bucket_params['bucket_name'] = 'default'
        _task = conc.BucketCreateTask(server, bucket,
                                      task_manager=self.jython_task_manager)
        self.jython_task_manager.schedule(_task)
        return _task

    def sync_create_bucket(self, server, bucket):
        """
        Synchronously creates the default bucket

        Parameters:
          bucket_params - a dictionary containing bucket creation parameters.
        Returns:
          BucketCreateTask - Task future that is a handle to the scheduled task
        """
#         bucket_params['bucket_name'] = 'default'
        _task = conc.BucketCreateTask(server, bucket,
                                      task_manager=self.jython_task_manager)
        self.jython_task_manager.schedule(_task)
        return _task.get_result()

    def async_failover(self, servers=[], failover_nodes=[], graceful=False,
                       use_hostnames=False, wait_for_pending=0):
        """
        Asynchronously failover a set of nodes

        Parameters:
          servers - servers used for connection. (TestInputServer)
          failover_nodes - Servers that will be failed over (TestInputServer)
          graceful = True/False. True - graceful, False - hard. (Boolean)

        Returns:
          FailOverTask - A task future that is a handle to the scheduled task
        """
        _task = conc.FailoverTask(
            servers, task_manager=self.jython_task_manager,
            to_failover=failover_nodes, graceful=graceful,
            use_hostnames=use_hostnames, wait_for_pending=wait_for_pending)
        self.jython_task_manager.schedule(_task)
        return _task

    def async_init_node(self, server, disabled_consistent_view=None,
                        rebalanceIndexWaitingDisabled=None,
                        rebalanceIndexPausingDisabled=None,
                        maxParallelIndexers=None,
                        maxParallelReplicaIndexers=None, port=None,
                        quota_percent=None, services=None,
                        index_quota_percent=None, gsi_type='forestdb'):
        """
        Asynchronously initializes a node

        The task scheduled will initialize a nodes username and password and
        will establish the nodes memory quota to be 2/3 of the available
        system memory.

        Parameters:
          server - The server to initialize. (TestInputServer)
          disabled_consistent_view - disable consistent view
          rebalanceIndexWaitingDisabled - index waiting during rebalance(Bool)
          rebalanceIndexPausingDisabled - index pausing during rebalance(Bool)
          maxParallelIndexers - max parallel indexers threads(int)
          index_quota_percent - index quote used by GSI service
                                (added due to sherlock)
          maxParallelReplicaIndexers - max replica indexers threads (int)
          port - port to initialize cluster
          quota_percent - percent of memory to initialize
          services - can be kv, n1ql, index
          gsi_type - Indexer Storage Mode
        Returns:
          NodeInitTask - A task future that is a handle to the scheduled task
        """
        _task = conc.NodeInitializeTask(
            server, self.jython_task_manager, disabled_consistent_view,
            rebalanceIndexWaitingDisabled, rebalanceIndexPausingDisabled,
            maxParallelIndexers, maxParallelReplicaIndexers,
            port, quota_percent, services=services,
            index_quota_percent=index_quota_percent, gsi_type=gsi_type)

        self.jython_task_manager.schedule(_task)
        return _task

    def async_load_gen_docs(self, cluster, bucket, generator, op_type, exp=0,
                            flag=0, persist_to=0, replicate_to=0,
                            only_store_hash=True, batch_size=1, pause_secs=1,
                            timeout_secs=5, compression=True,
                            process_concurrency=8, retries=5,
                            active_resident_threshold=100,
                            durability="", print_ops_rate=True,
                            task_identifier="",
                            skip_read_on_error=False,
                            ryow=False, check_persistence=False,
                            start_task=True):

        clients = list()
        if active_resident_threshold == 100:
            self.log.debug("Loading documents to {}".format(bucket.name))
            if not task_identifier:
                task_identifier = bucket.name
            gen_start = int(generator.start)
            gen_end = max(int(generator.end), 1)
            gen_range = max(int((generator.end-generator.start) / process_concurrency), 1)
            for _ in range(gen_start, gen_end, gen_range):
                client = VBucketAwareMemcached(RestConnection(cluster.master),
                                               bucket)
                clients.append(client)
            if not ryow:
                _task = jython_tasks.LoadDocumentsGeneratorsTask(
                    cluster, self.jython_task_manager, bucket, clients,
                    [generator], op_type, exp, exp_unit="seconds", flag=flag,
                    persist_to=persist_to, replicate_to=replicate_to,
                    only_store_hash=only_store_hash,
                    batch_size=batch_size, pause_secs=pause_secs,
                    timeout_secs=timeout_secs, compression=compression,
                    process_concurrency=process_concurrency,
                    print_ops_rate=print_ops_rate, retries=retries,
                    durability=durability, task_identifier=task_identifier,
                    skip_read_on_error=skip_read_on_error)
            else:
                majority_value = (bucket.replicaNumber + 1)/2 + 1

                if durability.lower() == "none":
                    check_persistence = False
                    majority_value = 1
                elif durability.lower() == "majority":
                    check_persistence = False

                _task = jython_tasks.Durability(
                    cluster, self.jython_task_manager, bucket, clients,
                    generator, op_type, exp,
                    flag=flag, persist_to=persist_to,
                    replicate_to=replicate_to, only_store_hash=only_store_hash,
                    batch_size=batch_size, pause_secs=pause_secs,
                    timeout_secs=timeout_secs, compression=compression,
                    process_concurrency=process_concurrency, retries=retries,
                    durability=durability, majority_value=majority_value,
                    check_persistence=check_persistence)
        else:
            rest_client = RestConnection(cluster.master)
            for _ in range(process_concurrency):
                client = VBucketAwareMemcached(rest_client, bucket)
                clients.append(client)
            _task = jython_tasks.LoadDocumentsForDgmTask(
                cluster, self.jython_task_manager, bucket, clients,
                generator.name, 0,
                doc_index=generator.start,
                batch_size=batch_size,
                persist_to=persist_to,
                replicate_to=replicate_to,
                durability=durability,
                timeout_secs=timeout_secs,
                process_concurrency=process_concurrency,
                active_resident_threshold=active_resident_threshold)
        if start_task:
            self.jython_task_manager.add_new_task(_task)
        return _task

    def async_load_gen_sub_docs(self, cluster, bucket, generator,
                                op_type, exp=0, path_create=False,
                                xattr=False,
                                flag=0, persist_to=0, replicate_to=0,
                                only_store_hash=True, batch_size=1,
                                pause_secs=1,
                                timeout_secs=5, compression=True,
                                process_concurrency=8, print_ops_rate=True,
                                durability="",
                                start_task=True,
                                task_identifier=""):
        self.log.debug("Loading sub documents to {}".format(bucket.name))
        if not isinstance(generator, SubdocDocumentGenerator):
            raise Exception("Document generator needs to be of "
                            "type SubdocDocumentGenerator")
        clients = []
        gen_start = int(generator.start)
        gen_end = max(int(generator.end), 1)
        gen_range = max(int(
            (generator.end - generator.start) / process_concurrency), 1)
        for _ in range(gen_start, gen_end, gen_range):
            client = VBucketAwareMemcached(
                RestConnection(cluster.master),
                bucket)
            clients.append(client)
        _task = jython_tasks.LoadSubDocumentsGeneratorsTask(
            cluster,
            self.jython_task_manager,
            bucket,
            clients,
            [generator],
            op_type,
            exp,
            create_paths=path_create,
            xattr=xattr,
            exp_unit="seconds",
            flag=flag,
            persist_to=persist_to,
            replicate_to=replicate_to,
            only_store_hash=only_store_hash,
            batch_size=batch_size,
            pause_secs=pause_secs,
            timeout_secs=timeout_secs,
            compression=compression,
            process_concurrency=process_concurrency,
            print_ops_rate=print_ops_rate,
            durability=durability,
            task_identifier=task_identifier)
        if start_task:
            self.jython_task_manager.add_new_task(_task)
        return _task

    def async_continuous_update_docs(self, cluster, bucket, generator, exp=0,
                                     flag=0, persist_to=0, replicate_to=0,
                                     durability="",
                                     only_store_hash=True, batch_size=1,
                                     pause_secs=1, timeout_secs=5,
                                     compression=True,
                                     process_concurrency=8, retries=5):
        self.log.debug("Mutating documents to {}".format(bucket.name))
        client = VBucketAwareMemcached(RestConnection(cluster.master), bucket)
        _task = jython_tasks.ContinuousDocUpdateTask(
            cluster, self.jython_task_manager, bucket, client, [generator],
            "update", exp, flag=flag, persist_to=persist_to,
            replicate_to=replicate_to, durability=durability,
            batch_size=batch_size, pause_secs=pause_secs,
            timeout_secs=timeout_secs, compression=compression,
            process_concurrency=process_concurrency, retries=retries)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def async_load_gen_docs_atomicity(self, cluster, buckets, generator,
                                      op_type, exp=0, flag=0,
                                      persist_to=0, replicate_to=0,
                                      only_store_hash=True, batch_size=1,
                                      pause_secs=1, timeout_secs=5,
                                      compression=True,
                                      process_concurrency=8, retries=5,
                                      update_count=1, transaction_timeout=5,
                                      commit=True, durability=0, sync=True,num_threads=50):

        self.log.info("Loading documents ")
        bucket_list = list()
        client_list = list()
        gen_start = int(generator.start)
        gen_end = max(int(generator.end), 1)
        gen_range = max(int((generator.end-generator.start) / process_concurrency), 1)
        for _ in range(gen_start, gen_end, gen_range):
            temp_bucket_list = []
            temp_client_list = []
            for bucket in buckets:
                client = VBucketAwareMemcached(RestConnection(cluster.master),
                                               bucket)
                temp_client_list.append(client)
                temp_bucket_list.append(client.collection)
            bucket_list.append(temp_bucket_list)
            client_list.append(temp_client_list)

        _task = jython_tasks.Atomicity(
            cluster, self.jython_task_manager, bucket_list,
            client, client_list, [generator], op_type, exp,
            flag=flag, persist_to=persist_to,
            replicate_to=replicate_to, only_store_hash=only_store_hash,
            batch_size=batch_size,
            pause_secs=pause_secs, timeout_secs=timeout_secs,
            compression=compression,
            process_concurrency=process_concurrency, retries=retries,
            update_count=update_count,
            transaction_timeout=transaction_timeout, commit=commit,
            durability=durability,
            sync=sync,num_threads=num_threads)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def load_bucket_into_dgm(self, cluster, bucket, key, num_items,
                             active_resident_threshold, load_batch_size=100000,
                             batch_size=10, process_concurrency=4,
                             persist_to=None, replicate_to=None,
                             durability="", sdk_timeout=5,
                             doc_type="json"):
        rest = BucketHelper(cluster.master)
        bucket_stat = rest.get_bucket_stats_for_node(bucket.name,
                                                     cluster.master)
        while bucket_stat["vb_active_resident_items_ratio"] > \
                active_resident_threshold:
            self.test_log.info(
                "Resident_ratio for '%s': %s"
                % (bucket.name,
                   bucket_stat["vb_active_resident_items_ratio"]))
            gen_load = doc_generator(key, num_items,
                                     num_items+load_batch_size,
                                     doc_type=doc_type)
            num_items += load_batch_size
            task = self.async_load_gen_docs(
                cluster, bucket, gen_load, "create", 0,
                batch_size=batch_size, process_concurrency=process_concurrency,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout_secs=sdk_timeout,
                task_identifier=bucket.name,
                print_ops_rate=False)
            self.jython_task_manager.get_task_result(task)
            bucket_stat = rest.get_bucket_stats_for_node(bucket.name,
                                                         cluster.master)
        return num_items

    def async_validate_docs(self, cluster, bucket, generator, opt_type, exp=0,
                            flag=0, only_store_hash=True, batch_size=1,
                            pause_secs=1, timeout_secs=5, compression=True,
                            process_concurrency=4):
        self.log.debug("Validating documents")
        client = VBucketAwareMemcached(RestConnection(cluster.master), bucket)
        _task = jython_tasks.DocumentsValidatorTask(
            cluster, self.jython_task_manager, bucket, client, [generator],
            opt_type, exp, flag=flag, only_store_hash=only_store_hash,
            batch_size=batch_size, pause_secs=pause_secs,
            timeout_secs=timeout_secs, compression=compression,
            process_concurrency=process_concurrency)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def async_monitor_view_fragmentation(self, server,
                                         design_doc_name,
                                         fragmentation_value,
                                         bucket="default"):
        """Asynchronously monitor view fragmentation.
           When <fragmentation_value> is reached on the
           index file for <design_doc_name> this method
           will return.
        Parameters:
            server - The server to handle fragmentation config task. (TestInputServer)
            design_doc_name - design doc with views represented in index file. (String)
            fragmentation_value - target amount of fragmentation within index file to detect. (String)
            bucket - The name of the bucket design_doc belongs to. (String)
        Returns:
            MonitorViewFragmentationTask - A task future that is a handle to the scheduled task."""

        _task = jython_tasks.MonitorViewFragmentationTask(server, design_doc_name,
                                             fragmentation_value, bucket)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def async_compact_view(self, server, design_doc_name, bucket="default", with_rebalance=False):
        """Asynchronously run view compaction.
        Compacts index file represented by views within the specified <design_doc_name>
        Parameters:
            server - The server to handle fragmentation config task. (TestInputServer)
            design_doc_name - design doc with views represented in index file. (String)
            bucket - The name of the bucket design_doc belongs to. (String)
            with_rebalance - there are two cases that process this parameter:
                "Error occured reading set_view _info" will be ignored if True
                (This applies to rebalance in case),
                and with concurrent updates(for instance, with rebalance)
                it's possible that compaction value has not changed significantly
        Returns:
            ViewCompactionTask - A task future that is a handle to the scheduled task."""

        _task = jython_tasks.ViewCompactionTask(server, design_doc_name, bucket, with_rebalance)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def async_rebalance(self, servers, to_add, to_remove, use_hostnames=False,
                        services=None, check_vbucket_shuffling=True):
        """
        Asynchronously rebalances a cluster

        Parameters:
          servers - Servers participating in the rebalance ([TestServers])
          to_add - Servers being added to the cluster ([TestServers])
          to_remove - Servers being removed from the cluster ([TestServers])
          use_hostnames - True if nodes should be added using hostnames (Bool)

        Returns:
          RebalanceTask - A task future that is a handle to the scheduled task
        """
        _task = jython_tasks.RebalanceTask(
            servers, to_add, to_remove, use_hostnames=use_hostnames,
            services=services, check_vbucket_shuffling=check_vbucket_shuffling)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def async_wait_for_stats(self, shell_conn_list, bucket, stat_cmd, stat,
                             comparison, value, timeout=60):
        """
        Asynchronously wait for stats

        Waits for stats to match the criteria passed by the stats variable.
        See couchbase.stats_tool.StatsCommon.build_stat_check(...) for a
        description of the stats structure and how it can be built.

        Parameters:
          shell_conn_list - Objects of type 'RemoteMachineShellConnection'.
                            Uses this object to execute cbstats binary in the
                            cluster nodes
          bucket     - The name of the bucket (String)
          stat_cmd   - The stats name to fetch using cbstats. (String)
          stat       - The stat that we want to get the value from. (String)
          comparison - How to compare the stat result to the value specified.
          value      - The value to compare to.
          timeout    - Timeout for stat verification task

        Returns:
          RebalanceTask - Task future that is a handle to the scheduled task
        """
        self.log.debug("Starting StatsWaitTask for %s on bucket %s" % (stat, bucket.name))
        _task = jython_tasks.StatsWaitTask(shell_conn_list, bucket, stat_cmd,
                                           stat, comparison, value,
                                           timeout=timeout)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def async_monitor_db_fragmentation(self, server, bucket, fragmentation,
                                       get_view_frag=False):
        """
        Asyncronously monitor db fragmentation
        Parameters:
            servers - server to check(TestInputServers)
            bucket  - bucket to check
            fragmentation - fragmentation to reach
            get_view_frag - Monitor view fragmentation.
                            In case enabled when <fragmentation_value> is
                            reached this method will return (boolean)
        Returns:
            MonitorDBFragmentationTask - A task future that is a handle to the
                                         scheduled task
        """
        _task = jython_tasks.MonitorDBFragmentationTask(server, fragmentation,
                                                        bucket, get_view_frag)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def create_sasl_bucket(self, name, password, bucket_params, timeout=None):
        """Synchronously creates a sasl bucket

        Parameters:
            bucket_params - A dictionary containing a list of bucket creation
                            parameters. (Dict)

        Returns:
            boolean - Whether or not the bucket was created."""

        _task = self.async_create_sasl_bucket(name, password, bucket_params)
        self.jython_task_manager.schedule(_task)
        return _task.get_result(timeout)

    def create_standard_bucket(self, name, port, bucket_params, timeout=None):
        """Synchronously creates a standard bucket
        Parameters:
            bucket_params - A dictionary containing a list of bucket creation
                            parameters. (Dict)
        Returns:
            boolean - Whether or not the bucket was created."""
        _task = self.async_create_standard_bucket(name, port, bucket_params)
        return _task.get_result(timeout)

    def init_node(self, server, async_init_node=True,
                  disabled_consistent_view=None, services=None,
                  index_quota_percent=None):
        """Synchronously initializes a node

        The task scheduled will initialize a nodes username and password and
        will establish the nodes memory quota to be 2/3 of the available
        system memory.

        Parameters:
            server - The server to initialize. (TestInputServer)
            index_quota_percent - index quota percentage
            disabled_consistent_view - disable consistent view

        Returns:
            boolean - Whether or not the node was properly initialized."""
        _task = self.async_init_node(
            server, async_init_node, disabled_consistent_view,
            services=services, index_quota_percent=index_quota_percent)
        return _task.result()

    def rebalance(self, servers, to_add, to_remove, timeout=None,
                  use_hostnames=False, services=None):
        """
        Synchronously rebalances a cluster

        Parameters:
          servers - Servers participating in the rebalance ([TestServers])
          to_add - Servers being added to the cluster ([TestServers])
          to_remove - Servers being removed from the cluster ([TestServers])
          use_hostnames - True if nodes should be added using hostnames (Bool)
          services - Services definition per Node, default is None
                     (since Sherlock release)
        Returns:
          boolean - Whether or not the rebalance was successful
        """
        _task = self.async_rebalance(servers, to_add, to_remove, use_hostnames=use_hostnames,
                                     services=services)
        self.jython_task_manager.get_task_result(_task)
        return _task.result

    def load_gen_docs(self, cluster, bucket, generator, op_type, exp=0, flag=0,
                      persist_to=0, replicate_to=0, only_store_hash=True,
                      batch_size=1, compression=True, process_concurrency=8,
                      retries=5):
        _task = self.async_load_gen_docs(
            cluster, bucket, generator, op_type, exp, flag,
            persist_to=persist_to, replicate_to=replicate_to,
            only_store_hash=only_store_hash, batch_size=batch_size,
            compression=compression, process_concurrency=process_concurrency,
            retries=retries)
        return self.jython_task_manager.get_task_result(_task)

    def verify_data(self, server, bucket, kv_store, timeout=None,
                    compression=True):
        _task = self.async_verify_data(server, bucket, kv_store,
                                       compression=compression)
        return _task.result(timeout)

    def async_verify_data(self, server, bucket, kv_store, max_verify=None,
                          only_store_hash=True, batch_size=1,
                          replica_to_read=None, timeout_sec=5,
                          compression=True):
        if batch_size > 1:
            _task = conc.BatchedValidateDataTask(
                server, bucket, kv_store, max_verify, only_store_hash,
                batch_size, timeout_sec, self.jython_task_manager,
                compression=compression)
        else:
            _task = conc.ValidateDataTask(
                server, bucket, kv_store, max_verify, only_store_hash,
                replica_to_read, self.jython_task_manager, compression=compression)
        self.jython_task_manager.schedule(_task)
        return _task

    def wait_for_stats(self, cluster, bucket, param, stat, comparison, value,
                       timeout=None):
        """Synchronously wait for stats

        Waits for stats to match the criteria passed by the stats variable. See
        couchbase.stats_tool.StatsCommon.build_stat_check(...) for a
        description of the stats structure and how it can be built.

        Parameters:
            servers - The servers to get stats from. Specifying multiple
                      servers will cause the result from each server to be
                      added together before comparing. ([TestInputServer])
            bucket - The name of the bucket (String)
            param - The stats parameter to use. (String)
            stat - The stat that we want to get the value from. (String)
            comparison - How to compare the stat result to the value specified.
            value - The value to compare to.

        Returns:
            boolean - Whether or not the correct stats state was seen"""
        _task = self.async_wait_for_stats(cluster, bucket, param, stat,
                                          comparison, value)
        return self.jython_task_manager.get_task_result(_task)

    def shutdown(self, force=False):
        self.jython_task_manager.shutdown(force)
        if force:
            self.log.warning("Cluster instance shutdown with force")

    def async_n1ql_query_verification(self, server, bucket, query,
                                      n1ql_helper=None,
                                      expected_result=None,
                                      is_explain_query=False,
                                      index_name=None, verify_results=True,
                                      retry_time=2, scan_consistency=None,
                                      scan_vector=None):
        """Asynchronously runs n1ql querya and verifies result if required

        Parameters:
          server - Server to handle query verification task (TestInputServer)
          query - Query params being used with the query. (dict)
          expected_result - expected result after querying
          is_explain_query - is query explain query
          index_name - index related to query
          bucket - Name of the bucket containing items for this view (String)
          verify_results -  Verify results after query runs successfully
          retry_time - Seconds to wait before retrying failed queries (int)
          n1ql_helper - n1ql helper object
          scan_consistency - consistency value for querying
          scan_vector - scan vector used for consistency
        Returns:
          N1QLQueryTask - A task future that is a handle to the scheduled task
        """
        _task = jython_tasks.N1QLQueryTask(
            n1ql_helper=n1ql_helper, server=server, bucket=bucket,
            query=query, expected_result=expected_result,
            verify_results=verify_results, is_explain_query=is_explain_query,
            index_name=index_name, retry_time=retry_time,
            scan_consistency=scan_consistency, scan_vector=scan_vector)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def n1ql_query_verification(self, server, bucket, query, n1ql_helper=None,
                                expected_result=None, is_explain_query=False,
                                index_name=None, verify_results=True,
                                scan_consistency=None, scan_vector=None,
                                retry_time=2, timeout=60):
        """
        Synchronously runs n1ql querya and verifies result if required

        Parameters:
          server - Server to handle query verification task (TestInputServer)
          query - Query params being used with the query. (dict)
          expected_result - expected result after querying
          is_explain_query - is query explain query
          index_name - index related to query
          bucket - Name of the bucket containing items for this view (String)
          verify_results -  Verify results after query runs successfully
          retry_time - Seconds to wait before retrying failed queries (int)
          n1ql_helper - n1ql helper object
          scan_consistency - consistency used during querying
          scan_vector - vector used during querying
          timeout - timeout for task
        Returns:
          N1QLQueryTask - A task future that is a handle to the scheduled task
        """
        _task = self.async_n1ql_query_verification(
            n1ql_helper=n1ql_helper, server=server, bucket=bucket, query=query,
            expected_result=expected_result, is_explain_query=is_explain_query,
            index_name=index_name, verify_results=verify_results,
            retry_time=retry_time, scan_consistency=scan_consistency,
            scan_vector=scan_vector)
        return self.jython_task_manager.get_task_result(_task)

    def async_create_index(self, server, bucket, query, n1ql_helper=None,
                           index_name=None, defer_build=False, retry_time=2,
                           timeout=240):
        """
        Asynchronously runs create index task

        Parameters:
          server - Server to handle query verification task (TestInputServer)
          query - Query params being used with the query.
          bucket - Name of the bucket containing items for this view (String)
          index_name - Name of the index to be created
          defer_build - build is defered
          retry_time - Seconds to wait before retrying failed queries (int)
          n1ql_helper - n1ql helper object
          timeout - timeout for index to come online
        Returns:
          CreateIndexTask - A task future that is a handle for scheduled task
        """
        _task = jython_tasks.CreateIndexTask(
            n1ql_helper=n1ql_helper, server=server, bucket=bucket,
            defer_build=defer_build, index_name=index_name, query=query,
            retry_time=retry_time, timeout=timeout)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def async_monitor_index(self, server, bucket, n1ql_helper=None,
                            index_name=None, retry_time=2, timeout=240):
        """
        Asynchronously runs create index task

        Parameters:
          server - Server to handle query verification task (TestInputServer)
          query - Query params being used with the query.
          bucket - Name of the bucket containing items for this view (String)
          index_name - Name of the index to be created
          retry_time - Seconds to wait before retrying failed queries (int)
          timeout - timeout for index to come online
          n1ql_helper - n1ql helper object
        Returns:
          MonitorIndexTask - A task future that is a handle for scheduled task
        """
        _task = jython_tasks.MonitorIndexTask(
            n1ql_helper=n1ql_helper, server=server, bucket=bucket,
            index_name=index_name, retry_time=retry_time, timeout=timeout)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def async_build_index(self, server, bucket, query, n1ql_helper=None,
                          retry_time=2):
        """
        Asynchronously runs create index task

        Parameters:
          server - Server to handle query verification task (TestInputServer)
          query - Query params being used with the query.
          bucket - Name of the bucket containing items for this view (String)
          retry_time - Seconds to wait before retrying failed queries (int)
          n1ql_helper - n1ql helper object
        Returns:
          BuildIndexTask - A task future that is a handle to the scheduled task
        """
        _task = jython_tasks.BuildIndexTask(
            n1ql_helper=n1ql_helper, server=server, bucket=bucket, query=query,
            retry_time=retry_time)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def create_index(self, server, bucket, query, n1ql_helper=None,
                     index_name=None, defer_build=False, retry_time=2,
                     timeout=60):
        """
        Asynchronously runs drop index task

        Parameters:
          server - Server to handle query verification task. (TestInputServer)
          query - Query params being used with the query.
          bucket - Name of the bucket containing items for this view (String)
          index_name - Name of the index to be created
          retry_time - Seconds to wait before retrying failed queries (int)
          n1ql_helper - n1ql helper object
          defer_build - defer the build
          timeout - timeout for the task
        Returns:
          N1QLQueryTask - A task future that is a handle to the scheduled task
        """
        _task = self.async_create_index(
            n1ql_helper=n1ql_helper, server=server, bucket=bucket, query=query,
            index_name=index_name, defer_build=defer_build,
            retry_time=retry_time)
        return self.jython_task_manager.get_task_result(_task)

    def async_drop_index(self, server=None, bucket="default", query=None,
                         n1ql_helper=None, index_name=None, retry_time=2):
        """
        Synchronously runs drop index task

        Parameters:
          server - Server to handle query verification task (TestInputServer)
          query - Query params being used with the query.
          bucket - Name of the bucket containing items for this view (String)
          index_name - Name of the index to be dropped
          retry_time - Seconds to wait before retrying failed queries (int)
          n1ql_helper - n1ql helper object
        Returns:
          DropIndexTask - A task future that is a handle to the scheduled task
        """
        _task = jython_tasks.DropIndexTask(
            n1ql_helper=n1ql_helper, server=server, bucket=bucket, query=query,
            index_name=index_name, retry_time=retry_time)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def drop_index(self, server, bucket, query, n1ql_helper=None,
                   index_name=None, retry_time=2, timeout=60):
        """
        Synchronously runs drop index task

        Parameters:
          server - Server to handle query verification task (TestInputServer)
          query - Query params being used with the query. (dict)
          bucket - Name of the bucket containing items for this view. (String)
          index_name - Name of the index to be created
          retry_time - Seconds to wait before retrying failed queries (int)
          n1ql_helper - n1ql helper object
          timeout - timeout for the task
        Returns:
          N1QLQueryTask - A task future that is a handle to the scheduled task
        """
        _task = self.async_drop_index(
            n1ql_helper=n1ql_helper, server=server, bucket=bucket, query=query,
            index_name=index_name, retry_time=retry_time)
        return self.jython_task_manager.get_task_result(_task)

    def failover(self, servers=[], failover_nodes=[], graceful=False,
                 use_hostnames=False, timeout=None):
        """Synchronously flushes a bucket

        Parameters:
            servers - node used for connection (TestInputServer)
            failover_nodes - Servers to be failed over (TestInputServer)
            bucket - The name of the bucket to be flushed. (String)

        Returns:
            boolean - Whether or not the bucket was flushed."""
        _task = self.async_failover(servers, failover_nodes, graceful,
                                    use_hostnames)
        return _task.result(timeout)

    def async_bucket_flush(self, server, bucket='default'):
        """
        Asynchronously flushes a bucket

        Parameters:
          server - The server to flush the bucket on. (TestInputServer)
          bucket - The name of the bucket to be flushed. (String)

        Returns:
          BucketFlushTask - A task future that is a handle for scheduled task
        """
        _task = conc.BucketFlushTask(server, self.jython_task_manager, bucket)
        self.jython_task_manager.schedule(_task)
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
        _task = conc.CompactBucketTask(server, self.jython_task_manager, bucket)
        self.jython_task_manager.schedule(_task)
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

    def async_cbas_query_execute(self, master, cbas_server, cbas_endpoint,
                                 statement, bucket='default', mode=None,
                                 pretty=True):
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
        _task = conc.CBASQueryExecuteTask(
            master, cbas_server, self.jython_task_manager, cbas_endpoint, statement,
            bucket, mode, pretty)
        self.jython_task_manager.schedule(_task)
        return _task
