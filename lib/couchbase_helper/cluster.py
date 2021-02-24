import Jython_tasks.task as jython_tasks
from BucketLib.bucket import Bucket
from Cb_constants import CbServer
from Jython_tasks.task import MutateDocsFromSpecTask

from couchbase_helper.documentgenerator import doc_generator, \
    SubdocDocumentGenerator
from global_vars import logger
from sdk_client3 import SDKClient
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
        self.log = logger.get("infra")
        self.test_log = logger.get("test")
        self.log.debug("Initiating ServerTasks")

    def async_failover(self, servers=[], failover_nodes=[], graceful=False,
                       use_hostnames=False, wait_for_pending=0, allow_unsafe=False,
                       all_at_once=False):
        """
        Asynchronously failover a set of nodes

        Parameters:
          servers - servers used for connection. (TestInputServer)
          failover_nodes - Servers that will be failed over (TestInputServer)
          graceful = True/False. True - graceful, False - hard. (Boolean)
          all_at_once = whether to failover all of failover_nodes at once in case of
                        multiple failover nodes

        Returns:
          FailOverTask - A task future that is a handle to the scheduled task
        """
        _task = jython_tasks.FailoverTask(servers,
                                          to_failover=failover_nodes,
                                          graceful=graceful,
                                          use_hostnames=use_hostnames,
                                          wait_for_pending=wait_for_pending,
                                          allow_unsafe=allow_unsafe,
                                          all_at_once=all_at_once)
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
        _task = jython_tasks.NodeInitializeTask(
            server, self.jython_task_manager, disabled_consistent_view,
            rebalanceIndexWaitingDisabled, rebalanceIndexPausingDisabled,
            maxParallelIndexers, maxParallelReplicaIndexers,
            port, quota_percent, services=services,
            index_quota_percent=index_quota_percent, gsi_type=gsi_type)

        self.jython_task_manager.schedule(_task)
        return _task

    def async_load_gen_docs_from_spec(self, cluster, task_manager, loader_spec,
                                      sdk_client_pool,
                                      batch_size=200,
                                      process_concurrency=1,
                                      print_ops_rate=True,
                                      start_task=True,
                                      track_failures=True):
        self.log.debug("Initializing mutation task for given spec")
        task_manager = task_manager or self.jython_task_manager
        task = MutateDocsFromSpecTask(
            cluster, task_manager, loader_spec,
            sdk_client_pool,
            batch_size=batch_size,
            process_concurrency=process_concurrency,
            print_ops_rate=print_ops_rate,
            track_failures=track_failures)
        if start_task:
            task_manager.add_new_task(task)
        return task

    def async_load_gen_docs(self, cluster, bucket, generator, op_type,
                            exp=0, random_exp=False,
                            flag=0, persist_to=0, replicate_to=0,
                            only_store_hash=True, batch_size=1, pause_secs=1,
                            timeout_secs=5, compression=None,
                            process_concurrency=8, retries=5,
                            active_resident_threshold=100,
                            durability="", print_ops_rate=True,
                            task_identifier="",
                            skip_read_on_error=False,
                            ryow=False, check_persistence=False,
                            start_task=True,
                            suppress_error_table=False,
                            dgm_batch=5000,
                            scope=CbServer.default_scope,
                            collection=CbServer.default_collection,
                            sdk_client_pool=None,
                            monitor_stats=["doc_ops"],
                            track_failures=True):
        clients = list()
        if active_resident_threshold == 100:
            if not task_identifier:
                task_identifier = "%s_%s_%s" % (op_type,
                                                generator.start,
                                                generator.end)
                if exp:
                    task_identifier += "_ttl=%s" % str(exp)
            gen_start = int(generator.start)
            gen_end = int(generator.end)
            gen_range = max(int((generator.end - generator.start)
                                / process_concurrency), 1)
            for _ in range(gen_start, gen_end, gen_range):
                client = None
                if sdk_client_pool is None:
                    client = SDKClient([cluster.master], bucket,
                                       scope, collection)
                clients.append(client)
            if not ryow:
                _task = jython_tasks.LoadDocumentsGeneratorsTask(
                    cluster, self.jython_task_manager, bucket, clients,
                    [generator], op_type,
                    exp, random_exp=random_exp, exp_unit="seconds", flag=flag,
                    persist_to=persist_to, replicate_to=replicate_to,
                    only_store_hash=only_store_hash,
                    batch_size=batch_size, pause_secs=pause_secs,
                    timeout_secs=timeout_secs, compression=compression,
                    process_concurrency=process_concurrency,
                    print_ops_rate=print_ops_rate, retries=retries,
                    durability=durability, task_identifier=task_identifier,
                    skip_read_on_error=skip_read_on_error,
                    suppress_error_table=suppress_error_table,
                    sdk_client_pool=sdk_client_pool,
                    scope=scope, collection=collection,
                    monitor_stats=monitor_stats,
                    track_failures=track_failures)
            else:
                majority_value = (bucket.replicaNumber + 1) / 2 + 1

                if durability.lower() == "none":
                    check_persistence = False
                    majority_value = 1
                elif durability.upper() == Bucket.DurabilityLevel.MAJORITY:
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
            for _ in range(process_concurrency):
                client = None
                if sdk_client_pool is None:
                    client = SDKClient([cluster.master], bucket,
                                       scope, collection)
                clients.append(client)
            _task = jython_tasks.LoadDocumentsForDgmTask(
                cluster, self.jython_task_manager, bucket, clients,
                key=generator.name, exp=0,
                doc_key_size=generator.key_size,
                doc_size=generator.doc_size,
                doc_index=generator.start,
                randomize_doc_size=generator.randomize_doc_size,
                randomize_value=generator.randomize_value,
                randomize=generator.randomize,
                mix_key_size=generator.mix_key_size,
                deep_copy=generator.deep_copy,
                batch_size=batch_size,
                persist_to=persist_to,
                replicate_to=replicate_to,
                durability=durability,
                timeout_secs=timeout_secs,
                process_concurrency=process_concurrency,
                active_resident_threshold=active_resident_threshold,
                dgm_batch=dgm_batch,
                sdk_client_pool=sdk_client_pool)
        if start_task:
            self.jython_task_manager.add_new_task(_task)
        return _task

    def async_load_gen_sub_docs(self, cluster, bucket, generator,
                                op_type, exp=0, path_create=False,
                                xattr=False,
                                flag=0, persist_to=0, replicate_to=0,
                                only_store_hash=True, batch_size=1,
                                pause_secs=1,
                                timeout_secs=5, compression=None,
                                process_concurrency=8, print_ops_rate=True,
                                durability="",
                                start_task=True,
                                task_identifier="",
                                sdk_client_pool=None,
                                scope=CbServer.default_scope,
                                collection=CbServer.default_collection):
        self.log.debug("Loading sub documents to {}".format(bucket.name))
        if not isinstance(generator, SubdocDocumentGenerator):
            raise Exception("Document generator needs to be of "
                            "type SubdocDocumentGenerator")
        clients = []
        gen_start = int(generator.start)
        gen_end = int(generator.end)
        gen_range = max(int(
            (generator.end - generator.start) / process_concurrency), 1)
        for _ in range(gen_start, gen_end, gen_range):
            client = None
            if sdk_client_pool is None:
                client = SDKClient([cluster.master], bucket,
                                   scope, collection)
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
            sdk_client_pool=sdk_client_pool,
            scope=scope, collection=collection,
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

    def async_continuous_doc_ops(self, cluster, bucket, generator,
                                 op_type="update", exp=0,
                                 persist_to=0, replicate_to=0,
                                 durability="",
                                 batch_size=200,
                                 timeout_secs=5,
                                 process_concurrency=4,
                                 scope=CbServer.default_scope,
                                 collection=CbServer.default_collection,
                                 sdk_client_pool=None):
        clients = list()
        for _ in range(process_concurrency):
            if sdk_client_pool is None:
                clients.append(SDKClient([cluster.master], bucket))
            else:
                clients.append(None)
        _task = jython_tasks.ContinuousDocOpsTask(
            cluster, self.jython_task_manager, bucket, clients, generator,
            scope=scope,
            collection=collection,
            op_type=op_type, exp=exp,
            persist_to=persist_to, replicate_to=replicate_to,
            durability=durability,
            batch_size=batch_size,
            timeout_secs=timeout_secs,
            process_concurrency=process_concurrency,
            sdk_client_pool=sdk_client_pool)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def async_load_gen_docs_atomicity(self, cluster, buckets, generator,
                                      op_type, exp=0, flag=0,
                                      persist_to=0, replicate_to=0,
                                      only_store_hash=True, batch_size=1,
                                      pause_secs=1, timeout_secs=5,
                                      compression=None,
                                      process_concurrency=8, retries=5,
                                      update_count=1, transaction_timeout=5,
                                      commit=True, durability=0, sync=True,
                                      num_threads=5, record_fail=False,
                                      defer=False,
                                      scope=CbServer.default_scope,
                                      collection=CbServer.default_collection,
                                      start_task=True):

        bucket_list = list()
        client_list = list()
        gen_start = int(generator.start)
        gen_end = int(generator.end)
        gen_range = max(int((generator.end - generator.start)
                            / process_concurrency), 1)
        for _ in range(gen_start, gen_end, gen_range):
            temp_bucket_list = list()
            temp_client_list = list()
            for bucket in buckets:
                client = SDKClient([cluster.master], bucket,
                                   scope, collection)
                temp_client_list.append(client)
                temp_bucket_list.append(client.collection)
            bucket_list.append(temp_bucket_list)
            client_list.append(temp_client_list)

        _task = jython_tasks.Atomicity(
            cluster, self.jython_task_manager, bucket_list,
            client_list, [generator], op_type, exp,
            flag=flag, persist_to=persist_to,
            replicate_to=replicate_to, only_store_hash=only_store_hash,
            batch_size=batch_size,
            pause_secs=pause_secs, timeout_secs=timeout_secs,
            compression=compression,
            process_concurrency=process_concurrency, retries=retries,
            update_count=update_count,
            transaction_timeout=transaction_timeout, commit=commit,
            durability=durability, sync=sync, num_threads=num_threads,
            record_fail=record_fail, defer=defer)
        if start_task:
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
                                     num_items + load_batch_size,
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

    def async_validate_docs_using_spec(self, cluster, task_manager, loader_spec,
                                       check_replica,
                                       sdk_client_pool,
                                       batch_size=200,
                                       process_concurrency=1,
                                       pause_secs=1):
        task_manager = task_manager or self.jython_task_manager
        _task = jython_tasks.ValidateDocsFromSpecTask(
            cluster, task_manager, loader_spec,
            sdk_client_pool=sdk_client_pool,
            check_replica=check_replica,
            batch_size=batch_size,
            process_concurrency=process_concurrency,
            pause_secs=pause_secs)
        task_manager.add_new_task(_task)
        return _task

    def async_validate_docs(self, cluster, bucket, generator, opt_type, exp=0,
                            flag=0, only_store_hash=True, batch_size=1,
                            pause_secs=1, timeout_secs=5, compression=None,
                            process_concurrency=4, check_replica=False,
                            scope=CbServer.default_scope,
                            collection=CbServer.default_collection,
                            sdk_client_pool=None, is_sub_doc=False,
                            suppress_error_table=False):
        clients = list()
        gen_start = int(generator.start)
        gen_end = int(generator.end)
        gen_range = max(int((generator.end - generator.start)
                            / process_concurrency), 1)
        for _ in range(gen_start, gen_end, gen_range):
            client = None
            if sdk_client_pool is None:
                client = SDKClient([cluster.master], bucket,
                                   scope, collection)
            clients.append(client)
        _task = jython_tasks.DocumentsValidatorTask(
            cluster, self.jython_task_manager, bucket, clients, [generator],
            opt_type, exp, flag=flag, only_store_hash=only_store_hash,
            batch_size=batch_size, pause_secs=pause_secs,
            timeout_secs=timeout_secs, compression=compression,
            process_concurrency=process_concurrency,
            check_replica=check_replica,
            scope=scope, collection=collection,
            sdk_client_pool=sdk_client_pool,
            is_sub_doc=is_sub_doc,
            suppress_error_table=suppress_error_table)
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
            server - TestInputServer to handle fragmentation config task.
            design_doc_name - design doc with views represented in index file.
            fragmentation_value - target amount of fragmentation within index
            file to detect. (String)
            bucket - The name of the bucket design_doc belongs to. (String)
        Returns:
            MonitorViewFragmentationTask - A task future handle to the task.
        """

        _task = jython_tasks.MonitorViewFragmentationTask(
            server,
            design_doc_name,
            fragmentation_value,
            bucket)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def async_compact_view(self, server, design_doc_name, bucket="default",
                           with_rebalance=False):
        """Asynchronously run view compaction.
        Compacts index file represented by views within the specified
        <design_doc_name>

        Parameters:
            server - TestInputServer to handle fragmentation config task.
            design_doc_name - design doc with views represented in index file.
            bucket - The name of the bucket design_doc belongs to. (String)
            with_rebalance - there are two cases that process this parameter:
                "Error occured reading set_view _info" will be ignored if True
                (This applies to rebalance in case),
                and with concurrent updates(for instance, with rebalance)
                it's possible that compaction value has not changed
                significantly

        Returns:
            ViewCompactionTask:
                A task future that is a handle to the scheduled task."""

        _task = jython_tasks.ViewCompactionTask(server,
                                                design_doc_name,
                                                bucket,
                                                with_rebalance)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def async_rebalance(self, servers, to_add=[], to_remove=[],
                        use_hostnames=False, services=None,
                        check_vbucket_shuffling=True,
                        sleep_before_rebalance=0, retry_get_process_num=25):
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
            services=services, check_vbucket_shuffling=check_vbucket_shuffling,
            sleep_before_rebalance=sleep_before_rebalance,
            retry_get_process_num=retry_get_process_num)
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
        self.log.debug("Starting StatsWaitTask for %s on bucket %s"
                       % (stat, bucket.name))
        _task = jython_tasks.StatsWaitTask(shell_conn_list, bucket, stat_cmd,
                                           stat, comparison, value,
                                           timeout=timeout)
        self.jython_task_manager.add_new_task(_task)
        return _task

    def async_monitor_db_fragmentation(self, server, bucket_name,
                                       fragmentation,
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
        _task = jython_tasks.MonitorDBFragmentationTask(server,
                                                        fragmentation,
                                                        bucket_name,
                                                        get_view_frag)
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

    def rebalance(self, servers, to_add, to_remove,
                  use_hostnames=False, services=None,
                  check_vbucket_shuffling=True):
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
        _task = self.async_rebalance(
            servers, to_add, to_remove,
            use_hostnames=use_hostnames,
            services=services,
            check_vbucket_shuffling=check_vbucket_shuffling)
        self.jython_task_manager.get_task_result(_task)
        return _task.result

    def load_gen_docs(self, cluster, bucket, generator, op_type, exp=0,
                      flag=0, persist_to=0, replicate_to=0,
                      only_store_hash=True, batch_size=1, pause_secs=1,
                      timeout_secs=5, compression=None,
                      process_concurrency=8, retries=5,
                      active_resident_threshold=100,
                      durability="", print_ops_rate=True,
                      task_identifier="",
                      skip_read_on_error=False,
                      ryow=False, check_persistence=False,
                      start_task=True,
                      suppress_error_table=False,
                      dgm_batch=5000,
                      scope=CbServer.default_scope,
                      collection=CbServer.default_collection):
        _task = self.async_load_gen_docs(
            cluster, bucket, generator, op_type, exp=exp,
            flag=flag, persist_to=persist_to, replicate_to=replicate_to,
            only_store_hash=only_store_hash, batch_size=batch_size,
            pause_secs=pause_secs,
            timeout_secs=timeout_secs, compression=compression,
            process_concurrency=process_concurrency, retries=retries,
            active_resident_threshold=active_resident_threshold,
            durability=durability, print_ops_rate=print_ops_rate,
            task_identifier=task_identifier,
            skip_read_on_error=skip_read_on_error,
            ryow=ryow, check_persistence=check_persistence,
            start_task=start_task,
            suppress_error_table=suppress_error_table,
            dgm_batch=dgm_batch,
            scope=scope,
            collection=collection)
        self.jython_task_manager.get_task_result(_task)
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
                                          comparison, value, timeout)
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
                                      scan_vector=None, timeout=900):
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
            scan_consistency=scan_consistency, scan_vector=scan_vector,
            timeout=timeout)
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
            scan_vector=scan_vector, timeout=timeout)
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

    def async_n1qlTxn_query(self, stmts, n1ql_helper,
                            commit=True, scan_consistency='REQUEST_PLUS'):
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
        _task = jython_tasks.N1QLTxnQueryTask(
            stmts=stmts, n1ql_helper=n1ql_helper,
            commit=commit, scan_consistency=scan_consistency)
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
            retry_time=retry_time, timeout=timeout)
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
                   index_name=None, retry_time=2):
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
                 use_hostnames=False, wait_for_pending=0, allow_unsafe=False,
                 all_at_once=False):
        """Synchronously flushes a bucket

        Parameters:
            servers - node used for connection (TestInputServer)
            failover_nodes - Servers to be failed over (TestInputServer)
            bucket - The name of the bucket to be flushed. (String)
            all_at_once = whether to failover all of failover_nodes at once in case of
                        multiple failover nodes

        Returns:
            boolean - Whether or not the bucket was flushed."""
        _task = self.async_failover(servers, failover_nodes, graceful,
                                    use_hostnames, wait_for_pending, allow_unsafe,
                                    all_at_once=all_at_once)
        self.jython_task_manager.get_task_result(_task)
        return _task.result

    def async_bucket_flush(self, server, bucket='default',
                           timeout=300):
        """
        Asynchronously flushes a bucket

        Parameters:
          server - The server to flush the bucket on. (TestInputServer)
          bucket - The name of the bucket to be flushed. (String)

        Returns:
          BucketFlushTask - A task future that is a handle for scheduled task
        """
        _task = jython_tasks.BucketFlushTask(server, self.jython_task_manager,
                                             bucket, timeout=timeout)
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

    def async_compact_bucket(self, server, bucket):
        """Asynchronously starts bucket compaction

        Parameters:
            server - source couchbase server
            bucket - bucket to compact

        Returns:
            boolean - Whether or not the compaction started successfully"""
        _task = jython_tasks.CompactBucketTask(server, bucket)
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
        status = self.jython_task_manager.get_task_result(_task)
        return status

    def async_monitor_compaction(self, cluster, bucket):
        """
        :param cluster: Couchbase cluster object
        :param bucket: Bucket object to monitor compaction
        :return task: MonitorBucketCompaction object
        """
        task = jython_tasks.MonitorBucketCompaction(cluster, bucket)
        self.jython_task_manager.add_new_task(task)
        return task

    def async_cbas_query_execute(self, master, cbas_util, cbas_endpoint,
                                 statement, bucket_name='default'):
        """
        Asynchronously execute a CBAS query
        :param master: Master server
        :param cbas_util: CbasUtil object from testcase
        :param cbas_endpoint: CBAS Endpoint URL (/analytics/service)
        :param statement: Query to be executed
        :param bucket_name: Name of the bucket
        :return: task with the output or error message
        """
        task = jython_tasks.CBASQueryExecuteTask(
            master, cbas_util, cbas_endpoint, statement, bucket_name)
        self.jython_task_manager.add_new_task(task)
        return task
