"""
Created on Sep 26, 2017

@author: riteshagarwal
"""

import copy
import logging

import crc32
import exceptions
import json
import random
import string
import time
import uuid
import zlib
from subprocess import call
from collections import defaultdict

import mc_bin_client
import memcacheConstants
from BucketLib.BucketOperations import BucketHelper
from Jython_tasks.task import ViewCreateTask, ViewDeleteTask, ViewQueryTask, \
    BucketCreateTask, StatsWaitTask, PrintOpsRate, \
    MonitorDBFragmentationTask
from SecurityLib.rbac import RbacUtil
from TestInput import TestInputSingleton
from bucket_utils.Bucket import Bucket
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from couchbase_helper.data_analysis_helper import DataCollector, DataAnalyzer,\
                                                  DataAnalysisResultAnalyzer
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from membase.api.exception import StatsUnavailableException
from membase.api.rest_client import Node, RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper, \
                                         VBucketAwareMemcached
from remote.remote_util import RemoteMachineShellConnection
from table_view import TableView
from testconstants import MAX_COMPACTION_THRESHOLD, \
                          MIN_COMPACTION_THRESHOLD
from sdk_client3 import SDKClient
# from couchbase_helper.stats_tools import StatsCommon

"""
Create a set of bucket_parameters to be sent to all bucket_creation methods

Parameters:
    size - The size of the bucket to be created. (int)
    enable_replica_index - can be 0 or 1,
                           1 enables indexing of replica bucket data (int)
    replicas - The number of replicas for this bucket. (int)
    eviction_policy - The eviction policy for the bucket (String). Can be
        ephemeral bucket: noEviction or nruEviction
        non-ephemeral bucket: valueOnly or fullEviction.
    bucket_priority - Priority of the bucket:either none/low/high. (String)
    bucket_type - The type of bucket. (String)
    flushEnabled - Enable/Disable the flush functionality of the bucket. (int)
    lww = determine the conflict resolution type of the bucket. (Boolean)
"""


class BucketUtils:
    def __init__(self, cluster, cluster_util, server_task):
        self.cluster = cluster
        self.task = server_task
        self.task_manager = self.task.jython_task_manager
        self.cluster_util = cluster_util
        self.buckets = list()
        self.input = TestInputSingleton.input
        self.enable_time_sync = self.input.param("enable_time_sync", False)
        self.sdk_compression = self.input.param("sdk_compression", True)
        self.data_collector = DataCollector()
        self.data_analyzer = DataAnalyzer()
        self.result_analyzer = DataAnalysisResultAnalyzer()
        self.log = logging.getLogger("test")

    # Supporting APIs
    def sleep(self, timeout=15, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def assertTrue(self, expr, msg=None):
        if msg:
            msg = "{0} is not true : {1}".format(expr, msg)
        else:
            msg = "{0} is not true".format(expr)
        if not expr:
            raise(Exception(msg))

    # Fetch/Create/Delete buckets
    def create_bucket(self, bucket, wait_for_warmup=True):
        if not isinstance(bucket, Bucket):
            raise Exception("Create bucket needs Bucket object as parameter")
        self.log.info("Creating bucket: %s" % bucket.name)
        _task = BucketCreateTask(self.cluster.master, bucket)
        self.task_manager.add_new_task(_task)
        result = self.task_manager.get_task_result(_task)
        if wait_for_warmup:
            self.sleep(2)
            warmed_up = self._wait_warmup_completed(
                self.cluster_util.get_kv_nodes(), bucket, wait_time=60)
            if not warmed_up:
                raise("Bucket %s not warmed up" % bucket.name)
        if result:
            self.buckets.append(bucket)
        else:
            raise Exception("Could not create bucket {}".format(bucket.name))

    def delete_bucket(self, serverInfo, bucket, wait_for_bucket_deletion=True):
        self.log.info('Deleting existing bucket {0} on {1}'
                      .format(bucket, serverInfo))

        bucket_conn = BucketHelper(serverInfo)
        if self.bucket_exists(bucket):
            status = bucket_conn.delete_bucket(bucket.name)
            if not status:
                try:
                    self.print_dataStorage_content([serverInfo])
                    self.log.info(StatsCommon.get_stats([serverInfo], bucket,
                                                        "timings"))
                except Exception as ex:
                    self.log.error("Unable to get timings for bucket: {0}"
                                   .format(ex))
            self.log.info('Deleted bucket: {0} from {1}'
                          .format(bucket, serverInfo.ip))
        msg = 'Bucket "{0}" not deleted even after waiting for two minutes' \
              .format(bucket)
        if wait_for_bucket_deletion:
            if not self.wait_for_bucket_deletion(bucket, bucket_conn, 200):
                try:
                    self.print_dataStorage_content([serverInfo])
                    self.log.info(StatsCommon.get_stats([serverInfo], bucket,
                                                        "timings"))
                except Exception as ex:
                    self.log.error("Unable to get timings for bucket: {0}"
                                   .format(ex))
                self.log.info(msg)
                return False
            else:
                return True

    def wait_for_bucket_deletion(self, bucket, bucket_conn,
                                 timeout_in_seconds=120):
        self.log.info("Waiting for bucket %s deletion to finish" % bucket.name)
        start = time.time()
        while (time.time() - start) <= timeout_in_seconds:
            if not self.bucket_exists(bucket):
                return True
            else:
                self.sleep(2)
        return False

    def wait_for_bucket_creation(self, bucket, bucket_conn,
                                 timeout_in_seconds=120):
        self.log.info('Waiting for bucket creation to complete....')
        start = time.time()
        while (time.time() - start) <= timeout_in_seconds:
            if self.bucket_exists(bucket):
                return True
            else:
                time.sleep(2)
        return False

    def bucket_exists(self, bucket):
        try:
            buckets = self.get_all_buckets(self.cluster.master)
            for item in buckets:
                if item.name == bucket.name:
                    return True
            return False
        except Exception:
            return False

    def delete_all_buckets(self, servers):
        for serverInfo in servers:
            try:
                buckets = self.get_all_buckets(serverInfo)
            except Exception as e:
                self.log.error(e)
                self.log.error('15 secs sleep before get_all_buckets() call')
                time.sleep(15)
                buckets = self.get_all_buckets(serverInfo)
            self.log.debug('Deleting existing buckets {0} on {1}'
                           .format([b.name for b in buckets], serverInfo.ip))
            for bucket in buckets:
                self.log.debug("Remove bucket {0} ...".format(bucket.name))
                try:
                    status = self.delete_bucket(serverInfo, bucket)
                except Exception as e:
                    self.log.error(e)
                    raise e
                if not status:
                    raise Exception("Bucket {0} could not be deleted"
                                    .format(bucket.name))

    def create_default_bucket(self, bucket_type=Bucket.bucket_type.MEMBASE,
                              ram_quota=None, replica=1, maxTTL=0,
                              compression_mode="off", wait_for_warmup=True):
        node_info = RestConnection(self.cluster.master).get_nodes_self()
        if ram_quota:
            ramQuotaMB = ram_quota
        elif node_info.memoryQuota and int(node_info.memoryQuota) > 0:
            ram_available = node_info.memoryQuota
            ramQuotaMB = ram_available - 1
        else:
            # By default set 100Mb if unable to fetch proper value
            ramQuotaMB = 100

        default_bucket = Bucket({Bucket.bucketType: bucket_type,
                                 Bucket.ramQuotaMB: ramQuotaMB,
                                 Bucket.replicaNumber: replica,
                                 Bucket.compressionMode: compression_mode,
                                 Bucket.maxTTL: maxTTL})
        self.create_bucket(default_bucket, wait_for_warmup)
        if self.enable_time_sync:
            self._set_time_sync_on_buckets([default_bucket.name])

    def get_bucket_object_from_name(self, bucket="", num_attempt=1, timeout=1):
        bucketInfo = None
        bucket_helper = BucketHelper(self.cluster.master)
        num = 0
        while num_attempt > num:
            try:
                content = bucket_helper.get_bucket_json(bucket)
                bucketInfo = self.parse_get_bucket_json(content)
                break
            except Exception:
                time.sleep(timeout)
                num += 1
        return bucketInfo

    def get_vbuckets(self, bucket='default'):
        b = self.get_bucket_object_from_name(bucket)
        return None if not b else b.vbuckets

    def print_bucket_stats(self):
        table = TableView(self.log.info)
        table.set_headers(["Bucket", "Type", "Replicas",
                           "TTL", "Items", "RAM Quota",
                           "RAM Used", "Disk Used"])
        self.get_all_buckets()
        if len(self.buckets) == 0:
            table.add_row(["No buckets", "", "", "", "", "", "", ""])
        else:
            for bucket in self.buckets:
                table.add_row(
                    [bucket.name, bucket.bucketType,
                     str(bucket.replicaNumber),
                     str(bucket.maxTTL),
                     str(bucket.stats.itemCount),
                     str(bucket.stats.ram),
                     str(bucket.stats.memUsed),
                     str(bucket.stats.diskUsed)])
        table.display("Bucket statistics")

    def get_vbucket_num_for_key(self, doc_key, total_vbuckets=1024):
        """
        Calculates vbucket number based on the document's key

        Argument:
        :doc_key        - Document's key
        :total_vbuckets - Total vbuckets present in the bucket

        Returns:
        :vbucket_number calculated based on the 'doc_key'
        """
        return (((zlib.crc32(doc_key)) >> 16) & 0x7fff) & (total_vbuckets-1)

    def is_lww_enabled(self, bucket='default'):
        bucket_helper = BucketHelper(self.cluster.master)
        bucket_info = bucket_helper.get_bucket_json(bucket=bucket)
        try:
            if bucket_info['conflictResolutionType'] == 'lww':
                return True
        except KeyError:
            return False

    def change_max_buckets(self, total_buckets):
        command = "curl -X POST -u {0}:{1} -d maxBucketCount={2} http://{3}:{4}/internalSettings" \
                  .format(self.cluster.master.rest_username,
                          self.cluster.master.rest_password, total_buckets,
                          self.cluster.master.ip, self.cluster.master.port)
        shell = RemoteMachineShellConnection(self.cluster.master)
        output, error = shell.execute_command_raw(command)
        shell.log_command_output(output, error)
        shell.disconnect()

    def _get_bucket_size(self, mem_quota, num_buckets):
        # min size is 100MB now
        return max(100, int(float(mem_quota) / float(num_buckets)))

    def _set_time_sync_on_buckets(self, buckets):

        # get the credentials beforehand
        memcache_credentials = dict()
        for s in self.cluster.nodes_in_cluster:
            memcache_admin, memcache_admin_password = \
                RestConnection(s).get_admin_credentials()
            memcache_credentials[s.ip] = {'id': memcache_admin,
                                          'password': memcache_admin_password}

        for b in buckets:
            client1 = VBucketAwareMemcached(
                RestConnection(self.cluster.master), b)

            for j in range(b.vbuckets):
                    active_vbucket = client1.memcached_for_vbucket(j)
                    active_vbucket.sasl_auth_plain(
                        memcache_credentials[active_vbucket.host]['id'],
                        memcache_credentials[active_vbucket.host]['password'])
                    active_vbucket.bucket_select(b)
                    _ = active_vbucket.set_time_sync_state(j, 1)

    def get_bucket_compressionMode(self, bucket='default'):
        bucket_helper = BucketHelper(self.cluster.master)
        bucket_info = bucket_helper.get_bucket_json(bucket=bucket)
        return bucket_info['compressionMode']

    def create_multiple_buckets(self, server, replica,
                                bucket_ram_ratio=(2.0 / 3.0),
                                bucket_count=3, bucket_type='membase',
                                eviction_policy='valueOnly', maxttl=0,
                                compression_mode="active"):
        success = True
        rest = RestConnection(server)
        info = rest.get_nodes_self()
        if info.memoryQuota < 450.0:
            self.log.error("At least need 450MB memoryQuota")
            success = False
        else:
            available_ram = info.memoryQuota * bucket_ram_ratio
            if available_ram / bucket_count > 100:
                bucket_ram = int(available_ram / bucket_count)
            else:
                bucket_ram = 100
                # choose a port that is not taken by this ns server
            for i in range(0, bucket_count):
                name = "bucket-{0}".format(i)
                bucket = Bucket({Bucket.name: name,
                                 Bucket.ramQuotaMB: bucket_ram,
                                 Bucket.replicas: replica,
                                 Bucket.bucketType: bucket_type,
                                 Bucket.evictionPolicy: eviction_policy,
                                 Bucket.maxTTL: maxttl,
                                 Bucket.compressionMode: compression_mode})
                self.create_bucket(bucket)
                time.sleep(10)
        return success

    def create_standard_buckets(self, server, num_buckets, bucket_size=None,
                                bucket_priorities=None):
        if bucket_priorities is None:
            bucket_priorities = []
        if not num_buckets:
            return
        if bucket_size is None:
            bucket_size = 100
        for i in range(num_buckets):
            name = 'standard_bucket' + str(i)
            bucket_priority = None
            if len(bucket_priorities) != 0:
                tem_prioroty = bucket_priorities[i]
                bucket_priority = self.get_bucket_priority(tem_prioroty)

            bucket = Bucket({Bucket.name: name,
                             Bucket.ramQuotaMB: bucket_size,
                             Bucket.priority: bucket_priority})
            self.create_bucket(bucket)

        if self.enable_time_sync:
            self._set_time_sync_on_buckets(['standard_bucket' + str(i)
                                            for i in range(num_buckets)])

    def flush_all_buckets(self):
        flush_tasks = []
        for bucket in self.buckets:
            flush_tasks.append(self.task.async_bucket_flush(
                self.cluster.master, bucket.name))

        for task in flush_tasks:
            task.result()

    def verify_stats_for_bucket(self, bucket, items, timeout=60):
        self.log.debug("Verifying stats for bucket {0}".format(bucket.name))
        stats_tasks = []
        servers = self.cluster.nodes_in_cluster
        if bucket.bucketType == Bucket.bucket_type.MEMCACHED:
            items_actual = 0
            for server in servers:
                client = MemcachedClientHelper.direct_client(server, bucket)
                items_actual += int(client.stats()["curr_items"])
            if items != items_actual:
                raise Exception("Items are not correct")

        # TODO: Need to fix the config files to always satisfy the
        #       replica number based on the available number_of_servers
        available_replicas = bucket.replicaNumber
        if len(servers) == bucket.replicaNumber:
            available_replicas = len(servers) - 1
        elif len(servers) <= bucket.replicaNumber:
            available_replicas = len(servers) - 1

        # Create connection to master node for verifying cbstats
        stat_cmd = "all"
        shell_conn_list = list()
        for cluster_node in self.cluster.nodes_in_cluster:
            remote_conn = RemoteMachineShellConnection(cluster_node)
            shell_conn_list.append(remote_conn)

        # Create Tasks to verify total items/replica count in the bucket
        stats_tasks.append(self.task.async_wait_for_stats(
            shell_conn_list, bucket, stat_cmd,
            'vb_replica_curr_items', '==', items * available_replicas,
            timeout=timeout))
        self.sleep(5)
        stats_tasks.append(self.task.async_wait_for_stats(
            shell_conn_list, bucket, stat_cmd,
            'curr_items_tot', '==', items * (available_replicas + 1),
            timeout=timeout))
        try:
            for task in stats_tasks:
                self.task_manager.get_task_result(task)
        except Exception as e:
            self.log.error("{0}".format(e))
            for task in stats_tasks:
                self.task_manager.stop_task(task)
            self.log.error("Unable to get expected stats from the "
                           "selected node")

        for remote_conn in shell_conn_list:
            remote_conn.disconnect()

    def update_all_bucket_maxTTL(self, maxttl=0):
        for bucket in self.buckets:
            self.log.info("Updating maxTTL for bucket {0} to {1}s"
                          .format(bucket.name, maxttl))
            BucketHelper(self.cluster.master).change_bucket_props(
                bucket, maxTTL=maxttl)

    def update_all_bucket_replicas(self, replicas=1):
        for bucket in self.buckets:
            self.log.info("Updating replica for bucket {0} to {1}s"
                          .format(bucket.name, replicas))
            BucketHelper(self.cluster.master).change_bucket_props(
                bucket, replicaNumber=replicas)

    def verify_cluster_stats(self, items, master=None,
                             timeout=None, check_items=True,
                             check_bucket_stats=True,
                             check_ep_items_remaining=False,
                             verify_total_items=True):
        if master is None:
            master = self.cluster.master
        self._wait_for_stats_all_buckets(
            timeout=(timeout or 120),
            check_ep_items_remaining=check_ep_items_remaining)
        if check_items:
            if check_bucket_stats:
                self.verify_stats_all_buckets(items=items,
                                              timeout=(timeout or 120))
            if verify_total_items:
                verified = True
                for bucket in self.buckets:
                    verified &= self.wait_till_total_numbers_match(
                        master, bucket, timeout_in_seconds=(timeout or 500))
                if not verified:
                    msg = "Lost items!!! Replication was completed " \
                          "but sum (curr_items) don't match the " \
                          "curr_items_total"
                    self.log.error(msg)
                    raise Exception(msg)

    def verify_stats_all_buckets(self, items, timeout=60):
        for bucket in self.buckets:
            self.verify_stats_for_bucket(bucket, items, timeout=timeout)

    # Bucket doc_ops support APIs
    @staticmethod
    def key_generator(size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def get_doc_op_info_dict(self, bucket, op_type, exp=0, replicate_to=0,
                             persist_to=0, durability="",
                             timeout=5, time_unit="seconds",
                             ignore_exceptions=[], retry_exceptions=[]):
        info_dict = dict()
        info_dict["ops_failed"] = False
        info_dict["bucket"] = bucket
        info_dict["op_type"] = op_type
        info_dict["exp"] = exp
        info_dict["replicate_to"] = replicate_to
        info_dict["persist_to"] = persist_to
        info_dict["durability"] = durability
        info_dict["timeout"] = timeout
        info_dict["time_unit"] = time_unit
        info_dict["ignore_exceptions"] = ignore_exceptions
        info_dict["retry_exceptions"] = retry_exceptions
        info_dict["retried"] = {"success": dict(), "fail": dict()}
        info_dict["unwanted"] = {"success": dict(), "fail": dict()}
        info_dict["ignored"] = dict()

        return info_dict

    def doc_ops_tasks_status(self, tasks_info):
        """
        :param tasks_info: dict with "ops_failed" Bool value updated
        :return: Aggregated success status for all tasks. (Boolean)
        """
        for task, _ in tasks_info.items():
            if tasks_info[task]["ops_failed"]:
                return False
        return True

    def log_doc_ops_task_failures(self, tasks_info):
        """
        Validated all exceptions and retires the doc_ops if required
        from each task within the tasks_info dict().

        If doc failures are seen, task["ops_failed"] will be marked as True

        :param tasks_info: dictionary updated with retried/unwanted docs
        """
        for _, task_info in tasks_info.items():
            op_type = task_info["op_type"]
            ignored_keys = task_info["ignored"].keys()
            retried_success_keys = task_info["retried"]["success"].keys()
            retried_failed_keys = task_info["retried"]["fail"].keys()
            unwanted_success_keys = task_info["unwanted"]["success"].keys()
            unwanted_failed_keys = task_info["unwanted"]["fail"].keys()

            # Success cases
            if len(ignored_keys) > 0:
                self.log.info("Ignored exceptions for '{0}': ({1}): {2}"
                              .format(op_type, len(ignored_keys),
                                      ignored_keys))

            if len(retried_success_keys) > 0:
                self.log.info("Docs succeeded for expected retries "
                              "for '{0}' ({1}): {2}"
                              .format(op_type, len(retried_success_keys),
                                      retried_success_keys))

            # Failure cases
            if len(retried_failed_keys) > 0:
                task_info["ops_failed"] = True
                self.log.error("Docs failed after expected retry "
                               "for '{0}' ({1}): {2}"
                               .format(op_type, len(retried_failed_keys),
                                       retried_failed_keys))
                self.log.error("Exceptions for failure on retried docs: {0}"
                               .format(task_info["retried"]["fail"]))

            if len(unwanted_success_keys) > 0:
                task_info["ops_failed"] = True
                self.log.error("Unexpected exceptions, succeeded "
                               "after retry for '{0}' ({1}): {2}"
                               .format(op_type, len(unwanted_success_keys),
                                       unwanted_success_keys))

            if len(unwanted_failed_keys) > 0:
                task_info["ops_failed"] = True
                self.log.error("Unexpected exceptions, failed even "
                               "after retry for '{0}' ({1}): {2}"
                               .format(op_type, len(unwanted_failed_keys),
                                       unwanted_failed_keys))
                self.log.error("Exceptions for unwanted doc failures "
                               "after retry: {0}"
                               .format(task_info["unwanted"]["fail"]))

    def verify_doc_op_task_exceptions(self, tasks_info, cluster):
        """
        :param tasks_info:  dict() of dict() of form,
                            tasks_info[task_obj] = get_doc_op_info_dict()
        :param cluster:     Cluster object
        :return: tasks_info dictionary updated with retried/unwanted docs
        """
        for task, task_info in tasks_info.items():
            self.task_manager.get_task_result(task)

            client = SDKClient(RestConnection(cluster.master),
                               task_info["bucket"])
            for key, failed_doc in task.fail.items():
                found = False
                exception = failed_doc["error"]
                key_value = {key: failed_doc}

                for ex in task_info["ignore_exceptions"]:
                    if str(exception).find(ex) != -1:
                        tasks_info[task]["ignored"].update(key_value)
                        found = True
                        break
                if found:
                    continue

                result = client.crud(
                    task_info["op_type"], key, failed_doc["value"],
                    exp=task_info["exp"],
                    replicate_to=task_info["replicate_to"],
                    persist_to=task_info["persist_to"],
                    durability=task_info["durability"],
                    timeout=task_info["timeout"],
                    time_unit=task_info["time_unit"])

                dict_key = "unwanted"
                for ex in task_info["retry_exceptions"]:
                    if str(exception).find(ex) != -1:
                        dict_key = "retried"
                        found = True
                        break

                if result["status"]:
                    tasks_info[task][dict_key]["success"].update(key_value)
                else:
                    tasks_info[task][dict_key]["fail"].update(key_value)
            # Close client for this task
            client.close()

        return tasks_info

    def async_load_bucket(self, cluster, bucket, generator, op_type, exp=0,
                          flag=0, persist_to=0, replicate_to=0,
                          durability="", sdk_timeout=5,
                          only_store_hash=True, batch_size=10, pause_secs=1,
                          compression=True, process_concurrency=8, retries=5):
        return self.task.async_load_gen_docs(
            cluster, bucket, generator, op_type, exp=exp, flag=flag,
            persist_to=persist_to, replicate_to=replicate_to,
            durability=durability, timeout_secs=sdk_timeout,
            only_store_hash=only_store_hash, batch_size=batch_size,
            pause_secs=pause_secs, compression=compression,
            process_concurrency=process_concurrency, retries=retries)

    def _async_load_all_buckets(self, cluster, kv_gen, op_type, exp, flag=0,
                                persist_to=0, replicate_to=0,
                                only_store_hash=True, batch_size=1,
                                pause_secs=1, timeout_secs=30,
                                sdk_compression=True, process_concurrency=8,
                                retries=5, durability="",
                                ignore_exceptions=[], retry_exceptions=[]):

        """
        Asynchronously apply load generation to all buckets in the
        cluster.bucket.name, gen, op_type, exp
        Args:
            server - A server in the cluster. (TestInputServer)
            kv_gen - The generator to use to generate load. (DocumentGenerator)
            op_type - "create", "read", "update", or "delete" (String)
            exp - The expiration for the items if updated or created (int)

        Returns:
            task_info - dict of dict populated using get_doc_op_info_dict()
        """
        tasks_info = dict()
        self.buckets = self.get_all_buckets(cluster.master)
        for bucket in self.buckets:
            gen = copy.deepcopy(kv_gen)
            if bucket.bucketType != 'memcached':
                self.log.info("BATCH SIZE for documents load: %s" % batch_size)
                task = self.async_load_bucket(
                    cluster, bucket, gen, op_type, exp, flag, persist_to,
                    replicate_to, durability, timeout_secs,
                    only_store_hash, batch_size, pause_secs,
                    sdk_compression, process_concurrency, retries)
                tasks_info[task] = self.get_doc_op_info_dict(
                    bucket, op_type, exp, replicate_to=replicate_to,
                    persist_to=persist_to, durability=durability,
                    timeout=timeout_secs, time_unit="seconds",
                    ignore_exceptions=ignore_exceptions,
                    retry_exceptions=retry_exceptions)
            else:
                self._load_memcached_bucket(cluster.master, gen, bucket.name)
        return tasks_info

    def sync_load_all_buckets(self, cluster, kv_gen, op_type, exp, flag=0,
                              persist_to=0, replicate_to=0,
                              only_store_hash=True, batch_size=1,
                              pause_secs=1, timeout_secs=30,
                              sdk_compression=True, process_concurrency=8,
                              retries=5, durability="",
                              ignore_exceptions=[], retry_exceptions=[]):

        """
        Asynchronously apply load generation to all buckets in the
        cluster.bucket.name, gen, op_type, exp
        Then wait for all doc_loading tasks to complete and verify the
        task's results and retry failed_docs if required.
        Args:
            server - A server in the cluster. (TestInputServer)
            kv_gen - The generator to use to generate load. (DocumentGenerator)
            op_type - "create", "read", "update", or "delete" (String)
            exp - The expiration for the items if updated or created (int)

        Returns:
            task_info - dict of dict populated using get_doc_op_info_dict()
        """
        # Start doc_loading in all buckets in async manner
        tasks_info = self._async_load_all_buckets(
            cluster, kv_gen, op_type, exp, flag,
            persist_to, replicate_to,
            only_store_hash, batch_size, pause_secs, timeout_secs,
            sdk_compression, process_concurrency, retries, durability,
            ignore_exceptions, retry_exceptions)

        # Wait for all doc_loading tasks to complete and populate failures
        self.verify_doc_op_task_exceptions(tasks_info, cluster)
        self.log_doc_ops_task_failures(tasks_info)

        return tasks_info

    def _load_all_ephemeral_buckets_until_no_more_memory(
            self, server, kv_gen, op_type, exp, increment, kv_store=1, flag=0,
            only_store_hash=True, batch_size=1000, pause_secs=1,
            timeout_secs=30, proxy_client=None, percentage=0.90):

        stats_all_buckets = dict()
        for bucket in self.buckets:
            stats_all_buckets[bucket.name] = StatsCommon()

        for bucket in self.buckets:
            memory_is_full = False
            while not memory_is_full:
                memory_used = \
                    stats_all_buckets[bucket.name].get_stats(
                        [self.cluster.master], bucket, '', 'mem_used')[server]
                # memory is considered full if mem_used is at say 90% of the available memory
                if int(memory_used) < percentage * self.bucket_size * 1000000:
                    self.log.info(
                        "Still have memory. %s used is less than %s MB quota for %s in bucket %s. Continue loading to the cluster" %
                        (memory_used, self.bucket_size, self.cluster.master.ip, bucket.name))

                    self._load_bucket(
                        bucket, self.cluster.master, kv_gen, "create", exp=0,
                        kv_store=1, flag=0, only_store_hash=True,
                        batch_size=batch_size, pause_secs=5, timeout_secs=60)
                    kv_gen.start = kv_gen.start + increment
                    kv_gen.end = kv_gen.end + increment
                    kv_gen = BlobGenerator('key-root', 'param2', self.value_size, start=kv_gen.start, end=kv_gen.end)
                else:
                    memory_is_full = True
                    self.log.info("Memory is full, %s bytes in use for %s and bucket %s!" %
                                  (memory_used, self.cluster.master.ip, bucket.name))

    def _wait_for_stats_all_buckets(self, ep_queue_size=0,
                                    ep_queue_size_cond='==',
                                    check_ep_items_remaining=False,
                                    timeout=60):
        """
        Waits for queues to drain on all servers and buckets in a cluster.

        A utility function that waits for all of the items loaded to be
        persisted and replicated.

        Args:
          servers - List of all servers in the cluster ([TestInputServer])
          ep_queue_size - expected ep_queue_size (int)
          ep_queue_size_cond - condition for comparing (str)
          check_ep_dcp_items_remaining - to check if replication is complete
          timeout - Waiting the end of the thread. (str)
        """
        tasks = []
        stat_cmd = "all"
        for server in self.cluster.nodes_in_cluster:
            shell_conn = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                if bucket.bucketType == 'memcached':
                    continue
                tasks.append(self.task.async_wait_for_stats(
                    [shell_conn], bucket, stat_cmd,
                    'ep_queue_size', ep_queue_size_cond, ep_queue_size,
                    timeout=timeout))
                if check_ep_items_remaining:
                    stat_cmd = 'dcp'
                    ep_items_remaining = 'ep_{0}_items_remaining' \
                                         .format(stat_cmd)
                    tasks.append(self.task.async_wait_for_stats(
                        [shell_conn], bucket, stat_cmd,
                        ep_items_remaining, "==", 0,
                        timeout=timeout))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
            for shell in task.shellConnList:
                shell.disconnect()

    def verify_unacked_bytes_all_buckets(self, filter_list=[], sleep_time=5):
        """
        Waits for max_unacked_bytes = 0 on all servers and buckets in a cluster
        A utility function that waits upr flow with unacked_bytes = 0
        """
        self.sleep(sleep_time)
        servers = self.cluster_util.get_kv_nodes()
        dcp_stat_map = self.data_collector.collect_compare_dcp_stats(
            self.buckets, servers, filter_list=filter_list)
        for bucket in dcp_stat_map.keys():
            if not dcp_stat_map[bucket]:
                raise Exception("the bucket {0} has unacked bytes != 0"
                                .format(bucket))

    def _verify_all_buckets(self, server, kv_store=1, timeout=180,
                            max_verify=None, only_store_hash=True,
                            batch_size=1000, replica_to_read=None):
        """
        Verifies data on all of the nodes in a cluster.

        Verifies all of the data in a specific kv_store index for all buckets
        in the cluster.

        Args:
            server - A server in the cluster. (TestInputServer)
            kv_store - The kv store index to check. (int)
            timeout - Waiting the end of the thread. (str)
        """
        tasks = []
        if len(self.buckets) > 1:
            batch_size = 1
        for bucket in self.buckets:
            if bucket.params.type == 'memcached':
                continue
            tasks.append(self.task.async_verify_data(
                server, bucket, bucket.kvs[kv_store], max_verify,
                only_store_hash, batch_size, replica_to_read,
                compression=self.sdk_compression))
        for task in tasks:
            task.get_result(timeout)

    def disable_compaction(self, server=None, bucket="default"):
        server = server or self.cluster.servers[0]
        new_config = {"viewFragmntThresholdPercentage": None,
                      "dbFragmentThresholdPercentage": None,
                      "dbFragmentThreshold": None,
                      "viewFragmntThreshold": None}
        self.modify_fragmentation_config(new_config, bucket)

    def modify_fragmentation_config(self, config, bucket="default"):
        bucket_op = BucketHelper(self.cluster.master)
        _config = {"parallelDBAndVC": "false",
                   "dbFragmentThreshold": None,
                   "viewFragmntThreshold": None,
                   "dbFragmentThresholdPercentage": 100,
                   "viewFragmntThresholdPercentage": 100,
                   "allowedTimePeriodFromHour": None,
                   "allowedTimePeriodFromMin": None,
                   "allowedTimePeriodToHour": None,
                   "allowedTimePeriodToMin": None,
                   "allowedTimePeriodAbort": None,
                   "autoCompactionDefined": "true"}
        _config.update(config)
        bucket_op.set_auto_compaction(
            parallelDBAndVC=_config["parallelDBAndVC"],
            dbFragmentThreshold=_config["dbFragmentThreshold"],
            viewFragmntThreshold=_config["viewFragmntThreshold"],
            dbFragmentThresholdPercentage=_config["dbFragmentThresholdPercentage"],
            viewFragmntThresholdPercentage=_config["viewFragmntThresholdPercentage"],
            allowedTimePeriodFromHour=_config["allowedTimePeriodFromHour"],
            allowedTimePeriodFromMin=_config["allowedTimePeriodFromMin"],
            allowedTimePeriodToHour=_config["allowedTimePeriodToHour"],
            allowedTimePeriodToMin=_config["allowedTimePeriodToMin"],
            allowedTimePeriodAbort=_config["allowedTimePeriodAbort"],
            bucket=bucket)
        time.sleep(5)

    def get_vbucket_seqnos(self, servers, buckets, skip_consistency=False,
                           per_node=True):
        """
        Method to get vbucket information from a cluster using cbstats
        """
        new_vbucket_stats = self.data_collector.collect_vbucket_stats(
            buckets, servers, collect_vbucket=False,
            collect_vbucket_seqno=True, collect_vbucket_details=False,
            perNode=per_node)
        if not skip_consistency:
            new_vbucket_stats = self.compare_per_node_for_vbucket_consistency(
                new_vbucket_stats)
        return new_vbucket_stats

    def get_vbucket_seqnos_per_Node_Only(self, servers, buckets):
        """
        Method to get vbucket information from a cluster using cbstats
        """
        servers = self.cluster_util.get_kv_nodes(servers)
        new_vbucket_stats = self.data_collector.collect_vbucket_stats(
            buckets, servers, collect_vbucket=False,
            collect_vbucket_seqno=True, collect_vbucket_details=False,
            perNode=True)
        self.compare_per_node_for_vbucket_consistency(new_vbucket_stats)
        return new_vbucket_stats

    def compare_vbucket_seqnos(self, prev_vbucket_stats, servers, buckets,
                               perNode=False):
        """
            Method to compare vbucket information to a previously stored value
        """
        compare = "=="
        # if self.withMutationOps:
        #     compare = "<="
        comp_map = dict()
        comp_map["uuid"] = {'type': "string", 'operation': "=="}
        comp_map["abs_high_seqno"] = {'type': "long", 'operation': compare}
        comp_map["purge_seqno"] = {'type': "string", 'operation': compare}

        new_vbucket_stats = dict()
        self.log.info("Begin Verification for vbucket seq_nos comparison")
        if perNode:
            new_vbucket_stats = self.get_vbucket_seqnos_per_Node_Only(servers,
                                                                      buckets)
        else:
            new_vbucket_stats = self.get_vbucket_seqnos(servers, buckets)
        isNotSame = True
        summary = ""
        if not perNode:
            compare_vbucket_seqnos_result = self.data_analyzer.compare_stats_dataset(
                prev_vbucket_stats, new_vbucket_stats, "vbucket_id",
                comparisonMap=comp_map)
            isNotSame, summary, _ = self.result_analyzer.analyze_all_result(
                compare_vbucket_seqnos_result, addedItems=False,
                deletedItems=False, updatedItems=False)
        else:
            compare_vbucket_seqnos_result = self.data_analyzer.compare_per_node_stats_dataset(
                prev_vbucket_stats, new_vbucket_stats, "vbucket_id",
                comparisonMap=comp_map)
            isNotSame, summary, _ = self.result_analyzer.analyze_per_node_result(
                compare_vbucket_seqnos_result, addedItems=False,
                deletedItems=False, updatedItems=False)
        if not isNotSame:
            raise Exception(summary)
        self.log.info("End Verification for vbucket seq_nos comparison")
        return new_vbucket_stats

    def compare_per_node_for_vbucket_consistency(self, map1, check_abs_high_seqno=False, check_purge_seqno=False):
        """
            Method to check uuid is consistent on active and replica new_vbucket_stats
        """
        bucketMap = dict()
        logic = True
        for bucket in map1.keys():
            map = dict()
            nodeMap = dict()
            output = ""
            for node in map1[bucket].keys():
                for vbucket in map1[bucket][node].keys():
                    uuid = map1[bucket][node][vbucket]['uuid']
                    abs_high_seqno = map1[bucket][node][vbucket]['abs_high_seqno']
                    purge_seqno = map1[bucket][node][vbucket]['purge_seqno']
                    if vbucket in map.keys():
                        common_str = "\n bucket {0}, vbucket {1} :: " \
                                     "Original in node {2}. UUID" \
                                     .format(bucket, vbucket, nodeMap[vbucket])
                        if map[vbucket]['uuid'] != uuid:
                            logic = False
                            output += "{0} {1}, Change in node {2}. UUID {3}" \
                                      .format(common_str, map[vbucket]['uuid'],
                                              node, uuid)
                        if check_abs_high_seqno and int(map[vbucket]['abs_high_seqno']) != int(abs_high_seqno):
                            logic = False
                            output += "{0} {1}, Change in node {2}. UUID {3}" \
                                      .format(common_str,
                                              map[vbucket]['abs_high_seqno'],
                                              node, abs_high_seqno)
                        if check_purge_seqno and int(map[vbucket]['purge_seqno']) != int(purge_seqno):
                            logic = False
                            output += "{0} {1}, Change in node {2}. UUID {3}" \
                                      .format(common_str,
                                              map[vbucket]['abs_high_seqno'],
                                              node, abs_high_seqno)
                    else:
                        map[vbucket] = dict()
                        map[vbucket]['uuid'] = uuid
                        map[vbucket]['abs_high_seqno'] = abs_high_seqno
                        map[vbucket]['purge_seqno'] = purge_seqno
                        nodeMap[vbucket] = node
            bucketMap[bucket] = map
        if not logic:
            raise Exception(output)
        return bucketMap

    def compare_vbucketseq_failoverlogs(self, vbucketseq={}, failoverlog={}):
        """
            Method to compare failoverlog and vbucket-seq for uuid and  seq no
        """
        isTrue = True
        output = ""
        for bucket in vbucketseq.keys():
            for vbucket in vbucketseq[bucket].keys():
                seq = vbucketseq[bucket][vbucket]['abs_high_seqno']
                uuid = vbucketseq[bucket][vbucket]['uuid']
                fseq = failoverlog[bucket][vbucket]['seq']
                fuuid = failoverlog[bucket][vbucket]['id']
                common_str = "\n Error Condition in bucket {0} vbucket {1}::" \
                             .format(bucket, vbucket)
                if seq < fseq:
                    output += "{0} seq:vbucket-seq {1} != failoverlog-seq {2}" \
                              .format(common_str, seq, fseq)
                    isTrue = False
                if uuid != fuuid:
                    output += "{0} uuid : vbucket-seq {1} != failoverlog-seq {2}" \
                              .format(common_str, uuid, fuuid)
                    isTrue = False
        if not isTrue:
            raise Exception(output)

    def print_results_per_node(self, node_map):
        """ Method to print map results - Used only for debugging purpose """
        for bucket in node_map.keys():
            print("----- Bucket {0} -----".format(bucket))
            for node in node_map[bucket].keys():
                print("-------------Node {0}------------".format(node))
                for vbucket in node_map[bucket][node].keys():
                    print("   for vbucket {0}".format(vbucket))
                    for key in node_map[bucket][node][vbucket].keys():
                        print("            :: for key {0} = {1}"
                              .format(key,
                                      node_map[bucket][node][vbucket][key]))

    def get_meta_data_set_all(self, dest_server, kv_store=1):
        """
        Method to get all meta data set for buckets and from the servers
        """
        data_map = dict()
        for bucket in self.buckets:
            self.log.info(" Collect data for bucket {0}".format(bucket.name))
            task = self.task.async_get_meta_data(
                dest_server, bucket, bucket.kvs[kv_store],
                compression=self.sdk_compression)
            task.result()
            data_map[bucket.name] = task.get_meta_data_store()
        return data_map

    def vb_distribution_analysis(self, servers=[], buckets=[], num_replicas=0,
                                 total_vbuckets=0, std=1.0, type="rebalance",
                                 graceful=True):
        """
        Method to check vbucket distribution analysis after rebalance
        """
        self.log.info("Begin Verification for vb_distribution_analysis")
        servers = self.cluster_util.get_kv_nodes(servers)
        active, replica = self.get_vb_distribution_active_replica(
            servers=servers, buckets=buckets)
        for bucket in active.keys():
            self.log.info("Begin Verification for Bucket {0}".format(bucket))
            active_result = active[bucket]
            replica_result = replica[bucket]
            if graceful or type == "rebalance":
                self.assertTrue(active_result["total"] == total_vbuckets,
                                "total vbuckets do not match for active data set (= criteria), actual {0} expectecd {1}"
                                .format(active_result["total"], total_vbuckets))
            else:
                self.assertTrue(active_result["total"] <= total_vbuckets,
                                "total vbuckets do not match for active data set  (<= criteria), actual {0} expectecd {1}"
                                .format(active_result["total"], total_vbuckets))
            if type == "rebalance":
                rest = RestConnection(self.cluster.master)
                nodes = rest.node_statuses()
                if (len(nodes) - num_replicas) >= 1:
                    self.assertTrue(replica_result["total"] == num_replicas * total_vbuckets,
                                    "total vbuckets do not match for replica data set (= criteria), actual {0} expected {1}"
                                    .format(replica_result["total"], num_replicas ** total_vbuckets))
                else:
                    self.assertTrue(replica_result["total"] < num_replicas * total_vbuckets,
                                    "total vbuckets do not match for replica data set (<= criteria), actual {0} expected {1}"
                                    .format(replica_result["total"], num_replicas ** total_vbuckets))
            else:
                self.assertTrue(replica_result["total"] <= num_replicas * total_vbuckets,
                                "total vbuckets do not match for replica data set (<= criteria), actual {0} expected {1}"
                                .format(replica_result["total"], num_replicas ** total_vbuckets))
            self.assertTrue(active_result["std"] >= 0.0 and active_result["std"] <= std,
                            "std test failed for active vbuckets")
            self.assertTrue(replica_result["std"] >= 0.0 and replica_result["std"] <= std,
                            "std test failed for replica vbuckets")
        self.log.info("End Verification for vb_distribution_analysis")

    def data_analysis_active_replica_all(self, prev_data_set_active,
                                         prev_data_set_replica, servers,
                                         buckets, path=None,
                                         mode="disk"):
        """
            Method to do data analysis using cb transfer
            This works at cluster level
            1) Get Active and Replica data_path
            2) Compare Previous Active and Replica data
            3) Compare Current Active and Replica data
        """
        self.log.info(" Begin Verification for data comparison ")
        info, curr_data_set_replica = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, getReplica=True,
            mode=mode)
        info, curr_data_set_active = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, getReplica=False,
            mode=mode)
        self.log.info(" Comparing :: Prev vs Current :: Active and Replica ")
        comparison_result_replica = self.data_analyzer.compare_all_dataset(
            info, prev_data_set_replica, curr_data_set_replica)
        comparison_result_active = self.data_analyzer.compare_all_dataset(
            info, prev_data_set_active, curr_data_set_active)
        logic_replica, summary_replica, output_replica = self.result_analyzer.analyze_all_result(
            comparison_result_replica, deletedItems=False, addedItems=False,
            updatedItems=False)
        logic_active, summary_active, output_active = \
            self.result_analyzer.analyze_all_result(
                comparison_result_active, deletedItems=False, addedItems=False,
                updatedItems=False)
        if not logic_replica:
            self.log.error(output_replica)
            raise Exception(output_replica)
        if not logic_active:
            self.log.error(output_active)
            raise Exception(output_active)
        self.log.info("Comparing :: Current :: Active and Replica")
        comparison_result = self.data_analyzer.compare_all_dataset(
            info, curr_data_set_active, curr_data_set_replica)
        logic, summary, output = self.result_analyzer.analyze_all_result(
            comparison_result, deletedItems=False, addedItems=False,
            updatedItems=False)
        self.log.info("End Verification for data comparison")

    def data_analysis_all(self, prev_data_set, servers, buckets, path=None,
                          mode="disk", deletedItems=False, addedItems=False,
                          updatedItems=False):
        """
        Method to do data analysis using cb transfer.
        This works at cluster level
        """
        self.log.info("Begin Verification for data comparison")
        servers = self.cluster_util.get_kv_nodes(servers)
        info, curr_data_set = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, mode=mode)
        comparison_result = self.data_analyzer.compare_all_dataset(
            info, prev_data_set, curr_data_set)
        logic, summary, output = self.result_analyzer.analyze_all_result(
            comparison_result, deletedItems=deletedItems,
            addedItems=addedItems, updatedItems=updatedItems)
        if not logic:
            raise Exception(summary)
        self.log.info("End Verification for data comparison")

    def get_data_set_all(self, servers, buckets, path=None, mode="disk"):
        """ Method to get all data set for buckets and from the servers """
        servers = self.cluster_util.get_kv_nodes(servers)
        _, dataset = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, mode=mode)
        return dataset

    def get_data_set_with_data_distribution_all(self, servers, buckets,
                                                path=None, mode="disk"):
        """ Method to get all data set for buckets and from the servers """
        servers = self.cluster_util.get_kv_nodes(servers)
        _, dataset = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, mode=mode)
        distribution = self.data_analyzer.analyze_data_distribution(dataset)
        return dataset, distribution

    def get_vb_distribution_active_replica(self, servers=[], buckets=[]):
        """
        Method to distribution analysis for active and replica vbuckets
        """
        servers = self.cluster_util.get_kv_nodes(servers)
        active, replica = self.data_collector.collect_vbucket_num_stats(
            servers, buckets)
        active_result, replica_result = \
            self.data_analyzer.compare_analyze_active_replica_vb_nums(active,
                                                                      replica)
        return active_result, replica_result

    def get_and_compare_active_replica_data_set_all(self, servers, buckets,
                                                    path=None, mode="disk"):
        """
        Method to get all data set for buckets and from the servers
        1) Get active and replica data in the cluster
        2) Compare active and replica data in the cluster
        3) Return active and replica data
        """
        servers = self.cluster_util.get_kv_nodes(servers)
        _, disk_replica_dataset = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, getReplica=True,
            mode=mode)
        info, disk_active_dataset = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, getReplica=False,
            mode=mode)
        self.log.info("Begin Verification for Active Vs Replica")
        comparison_result = self.data_analyzer.compare_all_dataset(
            info, disk_replica_dataset, disk_active_dataset)
        logic, summary, _ = self.result_analyzer.analyze_all_result(
            comparison_result, deletedItems=False, addedItems=False,
            updatedItems=False)
        if not logic:
            self.log.error(summary)
            raise Exception(summary)
        self.log.info("End Verification for Active Vs Replica")
        return disk_replica_dataset, disk_active_dataset

    def data_active_and_replica_analysis(self, server, max_verify=None,
                                         only_store_hash=True, kv_store=1):
        for bucket in self.buckets:
            task = self.task.async_verify_active_replica_data(
                server, bucket, bucket.kvs[kv_store], max_verify,
                self.sdk_compression)
            task.result()

    def data_meta_data_analysis(self, dest_server, meta_data_store,
                                kv_store=1):
        for bucket in self.buckets:
            task = self.task.async_verify_meta_data(
                dest_server, bucket, bucket.kvs[kv_store],
                meta_data_store[bucket.name])
            task.result()

    def compare_per_node_for_failovers_consistency(self, map1, vbucketMap):
        """
        Method to check uuid is consistent on active and
        replica new_vbucket_stats
        """
        bucketMap = dict()
        logic = True
        for bucket in map1.keys():
            map = dict()
            tempMap = dict()
            output = ""
            for node in map1[bucket].keys():
                for vbucket in map1[bucket][node].keys():
                    id = map1[bucket][node][vbucket]['id']
                    seq = map1[bucket][node][vbucket]['seq']
                    num_entries = map1[bucket][node][vbucket]['num_entries']
                    state = vbucketMap[bucket][node][vbucket]['state']
                    if vbucket in map.keys():
                        if map[vbucket]['id'] != id:
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2} {3} :: UUID {4}, Change node {5} {6} UUID {7}".format(
                                bucket, vbucket, tempMap[vbucket]['node'], tempMap[vbucket]['state'],
                                map[vbucket]['id'], node, state, id)
                        if int(map[vbucket]['seq']) != int(seq):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2} {3} :: seq {4}, Change node {5} {6}  :: seq {7}".format(
                                bucket, vbucket, tempMap[vbucket]['node'], tempMap[vbucket]['state'],
                                map[vbucket]['seq'], node, state, seq)
                        if int(map[vbucket]['num_entries']) != int(num_entries):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2} {3} :: num_entries {4}, Change node {5} {6} :: num_entries {7}".format(
                                bucket, vbucket, tempMap[vbucket]['node'], tempMap[vbucket]['state'],
                                map[vbucket]['num_entries'], node, state, num_entries)
                    else:
                        map[vbucket] = dict()
                        tempMap[vbucket] = dict()
                        map[vbucket]['id'] = id
                        map[vbucket]['seq'] = seq
                        map[vbucket]['num_entries'] = num_entries
                        tempMap[vbucket]['node'] = node
                        tempMap[vbucket]['state'] = state
            bucketMap[bucket] = map
        if not logic:
            raise Exception(output)
        return bucketMap

    def get_failovers_logs(self, servers, buckets):
        """
        Method to get failovers logs from a cluster using cbstats
        """
        vbucketMap = self.data_collector.collect_vbucket_stats(
            buckets, servers, collect_vbucket=True,
            collect_vbucket_seqno=False, collect_vbucket_details=False,
            perNode=True)
        new_failovers_stats = self.data_collector.collect_failovers_stats(
            buckets, servers, perNode=True)
        new_failovers_stats = self.compare_per_node_for_failovers_consistency(
            new_failovers_stats, vbucketMap)
        return new_failovers_stats

    def compare_failovers_logs(self, prev_failovers_stats, servers, buckets,
                               perNode=False, comp_map=None):
        """
        Method to compare failover log information to a previously stored value
        """
        comp_map = dict()
        comp_map["id"] = {'type': "string", 'operation': "=="}
        comp_map["seq"] = {'type': "long", 'operation': "<="}
        comp_map["num_entries"] = {'type': "string", 'operation': "<="}

        self.log.info(" Begin Verification for failovers logs comparison ")
        servers = self.cluster_util.get_kv_nodes(servers)
        new_failovers_stats = self.get_failovers_logs(servers, buckets)
        compare_failovers_result = self.data_analyzer.compare_stats_dataset(
            prev_failovers_stats, new_failovers_stats, "vbucket_id", comp_map)
        isNotSame, summary, result = self.result_analyzer.analyze_all_result(
            compare_failovers_result, addedItems=False, deletedItems=False,
            updatedItems=False)
        if not isNotSame:
            raise Exception(summary)
        self.log.info("End Verification for failovers logs comparison")
        return new_failovers_stats

    def sync_ops_all_buckets(self, docs_gen_map={}, batch_size=10,
                             verify_data=True):
        for key in docs_gen_map.keys():
            if key != "remaining":
                op_type = key
                if key == "expiry":
                    op_type = "update"
                    verify_data = False
                    self.expiry = 3
                self.load(docs_gen_map[key], op_type=op_type, exp=self.expiry,
                          verify_data=verify_data, batch_size=batch_size)
        if "expiry" in docs_gen_map.keys():
            self._expiry_pager()

    def async_ops_all_buckets(self, docs_gen_map={}, batch_size=10):
        tasks = []
        if "expiry" in docs_gen_map.keys():
            self._expiry_pager()
        for key in docs_gen_map.keys():
            if key != "remaining":
                op_type = key
                if key == "expiry":
                    op_type = "update"
                    self.expiry = 3
                tasks += self.async_load(docs_gen_map[key], op_type=op_type,
                                         exp=self.expiry,
                                         batch_size=batch_size)
        return tasks

    def async_monitor_db_fragmentation(self, server, fragmentation, bucket,
                                       get_view_frag=False):
        _task = MonitorDBFragmentationTask(server, fragmentation, bucket.name,
                                           get_view_frag)
        self.task_manager.add_new_task(_task)
        return _task

    def _expiry_pager(self, val=10):
        for node in self.cluster_util.get_kv_nodes():
            shell_conn = RemoteMachineShellConnection(node)
            cbepctl_obj = Cbepctl(shell_conn)
            for bucket in self.buckets:
                cbepctl_obj.set(bucket.name,
                                "flush_param",
                                "exp_pager_stime",
                                val)
            shell_conn.disconnect()

    def _run_compaction(self, number_of_times=100):
        try:
            for _ in range(1, number_of_times):
                for bucket in self.buckets:
                    BucketHelper(self.cluster.master).compact_bucket(
                        bucket.name)
        except Exception, ex:
            self.log.info(ex)

    def _load_data_in_buckets_using_mc_bin_client(self, bucket, data_set,
                                                  max_expiry_range=None):
        client = VBucketAwareMemcached(RestConnection(self.cluster.master),
                                       bucket)
        try:
            for key in data_set.keys():
                expiry = 0
                if max_expiry_range is not None:
                    expiry = random.randint(1, max_expiry_range)
                o, c, d = client.set(key, expiry, 0, json.dumps(data_set[key]))
        except Exception as ex:
            print('Exception: {0}'.format(ex))

    def run_mc_bin_client(self, number_of_times=500000, max_expiry_range=30):
        data_map = dict()
        for i in range(number_of_times):
            name = "key_" + str(i) + str((random.randint(1, 10000))) + \
                    str((random.randint(1, 10000)))
            data_map[name] = {"name": "none_the_less"}
        for bucket in self.buckets:
            try:
                self._load_data_in_buckets_using_mc_bin_client(
                    bucket, data_map, max_expiry_range)
            except Exception, ex:
                self.log.info(ex)

    def get_item_count(self, server, bucket):
        client = MemcachedClientHelper.direct_client(server, bucket)
        return int(client.stats()["curr_items"])

    def get_buckets_current_items_count(self, cluster):
        rest = RestConnection(cluster.master)
        bucket_map = rest.get_buckets_itemCount()
        return bucket_map

    def get_bucket_current_item_count(self, cluster, bucket):
        bucket_map = self.get_buckets_current_items_count(cluster)
        return bucket_map[bucket.name]

    def get_buckets_itemCount(self):
        server = self.cluster_util.get_nodes_from_services_map(
            service_type="kv")
        return BucketHelper(server).get_buckets_itemCount()

    def expire_pager(self, servers, val=10):
        for bucket in self.buckets:
            for server in servers:
                ClusterOperationHelper.flushctl_set(server, "exp_pager_stime",
                                                    val, bucket)
        self.sleep(val, "wait for expiry pager to run on all these nodes")

    def set_auto_compaction(
            self, rest, parallelDBAndVC="false", dbFragmentThreshold=None,
            viewFragmntThreshold=None, dbFragmentThresholdPercentage=None,
            viewFragmntThresholdPercentage=None,
            allowedTimePeriodFromHour=None, allowedTimePeriodFromMin=None,
            allowedTimePeriodToHour=None, allowedTimePeriodToMin=None,
            allowedTimePeriodAbort=None, bucket=None):
        output, rq_content, _ = rest.set_auto_compaction(
            parallelDBAndVC, dbFragmentThreshold, viewFragmntThreshold,
            dbFragmentThresholdPercentage, viewFragmntThresholdPercentage,
            allowedTimePeriodFromHour, allowedTimePeriodFromMin,
            allowedTimePeriodToHour, allowedTimePeriodToMin,
            allowedTimePeriodAbort, bucket)

        if not output and (dbFragmentThresholdPercentage, dbFragmentThreshold,
                           viewFragmntThresholdPercentage,
                           viewFragmntThreshold <= MIN_COMPACTION_THRESHOLD
                           or dbFragmentThresholdPercentage,
                           viewFragmntThresholdPercentage >= MAX_COMPACTION_THRESHOLD):
            self.assertFalse(output,
                             "Should be impossible to set compaction val {0}%"
                             .format(viewFragmntThresholdPercentage))
            self.assertTrue("errors" in json.loads(rq_content),
                            "Error is not present in response")
            self.assertTrue(str(json.loads(rq_content)["errors"])
                            .find("Allowed range is 2 - 100") > -1,
                            "Error 'Allowed range is 2 - 100', but was '{0}'"
                            .format(str(json.loads(rq_content)["errors"])))
            self.log.info("Response contains error = '%(errors)s' as expected"
                          % json.loads(rq_content))

    def get_bucket_priority(self, priority):
        if priority is None:
            return None
        if priority.lower() == 'low':
            return None
        else:
            return priority

    def _load_memcached_bucket(self, server, gen_load, bucket_name):
        num_tries = 0
        while num_tries < 6:
            try:
                num_tries += 1
                client = MemcachedClientHelper.direct_client(server,
                                                             bucket_name)
                break
            except Exception as ex:
                if num_tries < 5:
                    self.log.info("Retry..Unable to create memcached client: {0}"
                                  .format(ex))
                else:
                    self.log.error("Unable to create memcached client: {0}"
                                   .format(ex))
        while gen_load.has_next():
            key, value = gen_load.next()
            for v in xrange(1024):
                try:
                    client.set(key, 0, 0, value, v)
                    break
                except Exception:
                    pass
        client.close()

    def load_sample_buckets(self, servers=None, bucketName=None,
                            total_items=None):
        """ Load the specified sample bucket in Couchbase """
        self.assertTrue(
            BucketHelper(self.cluster.master).load_sample(bucketName),
            "Failure while loading sample bucket: %s" % bucketName)

        """ check for load data into travel-sample bucket """
        if total_items:
            end_time = time.time() + 600
            while time.time() < end_time:
                self.sleep(10)
                num_actual = 0
                if not servers:
                    num_actual = self.get_item_count(self.cluster.master,
                                                     bucketName)
                else:
                    for server in servers:
                        if "kv" in server.services:
                            num_actual += self.get_item_count(server,
                                                              bucketName)
                if int(num_actual) == total_items:
                    self.log.info("%s items are loaded in the %s bucket"
                                  % (num_actual, bucketName))
                    break
                self.log.info("%s items are loaded in the %s bucket"
                              % (num_actual, bucketName))
            if int(num_actual) != total_items:
                return False
        else:
            self.sleep(120)

        return True

    def create_default_buckets(self, servers, number_of_replicas=1,
                               assert_on_test=None):
        for serverInfo in servers:
            ip_rest = BucketHelper(serverInfo)
            ip_rest.create_bucket(bucket='default', ramQuotaMB=256,
                                  replicaNumber=number_of_replicas,
                                  proxyPort=11220, maxTTL=self.maxttl,
                                  compressionMode=self.compression_mode)
            msg = 'create_bucket succeeded but bucket "default" does not exist'
            removed_all_buckets = self.wait_for_bucket_creation('default',
                                                                ip_rest)
            if not removed_all_buckets:
                self.log.error(msg)
                if assert_on_test:
                    assert_on_test.fail(msg=msg)

    def wait_for_vbuckets_ready_state(self, node, bucket,
                                      timeout_in_seconds=300, log_msg='',
                                      admin_user='cbadminbucket',
                                      admin_pass='password'):
        start_time = time.time()
        end_time = start_time + timeout_in_seconds
        ready_vbuckets = dict()
        rest = RestConnection(node)
        bucket_conn = BucketHelper(node)
        bucket_conn.vbucket_map_ready(bucket, 60)
        vbucket_count = len(bucket.vbuckets)
        vbuckets = bucket.vbuckets
        obj = VBucketAwareMemcached(rest, bucket, info=node)
        _, vbucket_map, _ = obj.request_map(rest, bucket)
        # Create dictionary with key:"ip:port" and value: a list of vbuckets
        server_dict = defaultdict(list)
        for everyID in range(0, vbucket_count):
            memcached_ip_port = str(vbucket_map[everyID])
            server_dict[memcached_ip_port].append(everyID)
        while time.time() < end_time and len(ready_vbuckets) < vbucket_count:
            for every_ip_port in server_dict:
                # Retrieve memcached ip and port
                ip, port = every_ip_port.split(":")
                client = mc_bin_client.MemcachedClient(ip, int(port),
                                                       timeout=30)
                client.vbucket_count = len(vbuckets)
                # versions = rest.get_nodes_versions(logging=False)
                client.sasl_auth_plain(admin_user, admin_pass)
                bucket_name = bucket.name.encode('ascii')
                client.bucket_select(bucket_name)
                for i in server_dict[every_ip_port]:
                    try:
                        (_, _, c) = client.get_vbucket_state(i)
                    except mc_bin_client.MemcachedError as e:
                        ex_msg = str(e)
                        if "Not my vbucket" in log_msg:
                            log_msg = log_msg[:log_msg.find("vBucketMap") + 12] + "..."
                        if e.status == memcacheConstants.ERR_NOT_MY_VBUCKET:
                            # May receive while waiting for vbuckets, retry
                            continue
                        self.log.error("%s: %s" % (log_msg, ex_msg))
                        continue
                    except exceptions.EOFError:
                        # The client was disconnected for some reason. This can
                        # happen just after the bucket REST API is returned
                        # (before the buckets are created in each of the
                        # memcached processes)
                        # See here for some details:
                        # http://review.couchbase.org/#/c/49781/
                        # Longer term when we don't disconnect clients in this
                        # state we should probably remove this code.
                        self.log.error("Reconnecting to the server")
                        client.reconnect()
                        client.sasl_auth_plain(
                            bucket.name.encode('ascii'),
                            bucket.saslPassword.encode('ascii'))
                        continue

                    if c.find("\x01") > 0 or c.find("\x02") > 0:
                        ready_vbuckets[i] = True
                    elif i in ready_vbuckets:
                        self.log.warning("vbucket state changed from active "
                                         "to {0}".format(c))
                        del ready_vbuckets[i]
                client.close()
        return len(ready_vbuckets) == vbucket_count

    # try to insert key in all vbuckets before returning from this function
    # bucket { 'name' : 90,'password':,'port':1211'}
    def wait_for_memcached(self, node, bucket, timeout_in_seconds=300,
                           log_msg=''):
        msg = "waiting for memcached bucket : {0} in {1} to accept set ops"
        self.log.info(msg.format(bucket, node.ip))
        all_vbuckets_ready = self.wait_for_vbuckets_ready_state(
            node, bucket, timeout_in_seconds, log_msg)
        # return (counter == vbucket_count) and all_vbuckets_ready
        return all_vbuckets_ready

    def print_dataStorage_content(self, servers):
        """"printout content of data and index path folders"""
        # Determine whether its a cluster_run/not
        cluster_run = True

        firstIp = servers[0].ip
        if len(servers) == 1 and servers[0].port == '8091':
            cluster_run = False
        else:
            for node in servers:
                if node.ip != firstIp:
                    cluster_run = False
                    break

        for serverInfo in servers:
            node = RestConnection(serverInfo).get_nodes_self()
            paths = set([node.storage[0].path, node.storage[0].index_path])
            for path in paths:
                if "c:/Program Files" in path:
                    path = path.replace("c:/Program Files",
                                        "/cygdrive/c/Program Files")

                if cluster_run:
                    call(["ls", "-lR", path])
                else:
                    self.log.info(
                        "Total number of files. No need to printout all "
                        "that flood the test log.")
                    shell = RemoteMachineShellConnection(serverInfo)
                    # o, r = shell.execute_command("ls -LR '{0}'".format(path))
                    o, r = shell.execute_command("wc -l '{0}'".format(path))
                    shell.log_command_output(o, r)
                    shell.disconnect()

    def load_some_data(self, serverInfo, fill_ram_percentage=10.0,
                       bucket_name='default'):
        if fill_ram_percentage <= 0.0:
            fill_ram_percentage = 5.0
        client = MemcachedClientHelper.direct_client(serverInfo, bucket_name)
        # populate key
        bucket_conn = BucketHelper(serverInfo)
        bucket_conn.vbucket_map_ready(bucket_name, 60)
        vbucket_count = len(bucket_conn.get_vbuckets(bucket_name))
        testuuid = uuid.uuid4()
        info = bucket_conn.get_bucket(bucket_name)
        emptySpace = info.stats.ram - info.stats.memUsed
        self.log.info('emptySpace: {0} fill_ram_percentage: {1}'
                      .format(emptySpace, fill_ram_percentage))
        fill_space = (emptySpace * fill_ram_percentage) / 100.0
        self.log.info("fill_space {0}".format(fill_space))
        # each packet can be 10 KB
        packetSize = int(10 * 1024)
        number_of_buckets = int(fill_space) / packetSize
        self.log.info('packetSize: {0}'.format(packetSize))
        self.log.info('memory usage before key insertion: {0}'
                      .format(info.stats.memUsed))
        self.log.info('inserting {0} new keys to memcached @ {0}'
                 .format(number_of_buckets, serverInfo.ip))
        keys = ["key_%s_%d" % (testuuid, i) for i in range(number_of_buckets)]
        inserted_keys = []
        for key in keys:
            vbucketId = crc32.crc32_hash(key) & (vbucket_count - 1)
            client.vbucketId = vbucketId
            try:
                client.set(key, 0, 0, key)
                inserted_keys.append(key)
            except mc_bin_client.MemcachedError as error:
                self.log.error(error)
                client.close()
                msg = "unable to push key: {0} to vbucket: {1}" \
                      .format(key, client.vbucketId)
                self.log.error(msg)
                self.fail(msg)

        client.close()
        return inserted_keys

    def perform_doc_ops_in_all_cb_buckets(self, num_items, operation,
                                          start_key=0, end_key=1000,
                                          batch_size=5000, exp=0,
                                          _async=False):
        """
        Create/Update/Delete docs in all cb buckets
        :param num_items: No. of items to be created/deleted/updated
        :param operation: String - "create","update","delete"
        :param start_key: Doc Key to start the operation with
        :param end_key: Doc Key to end the operation with
        :return:
        """
        age = range(70)
        first = ['james', 'sharon', 'dave', 'bill', 'mike', 'steve']
        profession = ['doctor', 'lawyer']
        template = '{{"number": {0}, "first_name": "{1}", ' \
                   + '"profession": "{2}", "mutated": 0}}'
        gen_load = DocumentGenerator('test_docs', template, age, first,
                                     profession, start=start_key, end=end_key)
        self.log.info("%s %s documents..." % (operation, num_items))
        try:
            if not _async:
                self.log.info("BATCH SIZE for documents load: %s" % batch_size)
                self._load_all_buckets(
                    self.cluster.master, gen_load, operation, exp,
                    batch_size=batch_size)
                self._verify_stats_all_buckets(self.input.servers)
            else:
                tasks = self._async_load_all_buckets(
                    self.cluster.master, gen_load, operation, exp,
                    batch_size=batch_size)
                return tasks
        except Exception as e:
            self.log.info(e.message)

    def fetch_available_memory_for_kv_on_a_node(self):
        """
        Calculates the Memory that can be allocated for KV service on a node
        :return: Memory that can be used for KV service.
        """
        info = RestConnection(self.cluster.master).get_nodes_self()
        free_memory_in_mb = info.memoryFree // 1024 ** 2
        total_available_memory_in_mb = 0.8 * free_memory_in_mb

        active_service = info.services
        if "index" in active_service:
            total_available_memory_in_mb -= info.indexMemoryQuota
        if "fts" in active_service:
            total_available_memory_in_mb -= info.ftsMemoryQuota
        if "cbas" in active_service:
            total_available_memory_in_mb -= info.cbasMemoryQuota
        if "eventing" in active_service:
            total_available_memory_in_mb -= info.eventingMemoryQuota

        return total_available_memory_in_mb

    def load_buckets_with_high_ops(self, server, bucket, items, batch=2000,
                                   threads=5, start_document=0, instances=1,
                                   ttl=0):
        import subprocess
        cmd_format = "python utils/bucket_utils/thanosied.py " \
                     "--spec couchbase://{0} --bucket {1} "\
                     "--user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} " \
                     "--start_document {7} --cb_version {8} --workers {9} " \
                     "--ttl {10} --rate_limit {11} " \
                     "--passes 1"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if self.num_replicas > 0 and self.use_replica_to:
            cmd_format = "{} --replicate_to 1".format(cmd_format)
        cmd = cmd_format.format(
            server.ip, bucket.name, server.rest_username,
            server.rest_password, items, batch, threads, start_document,
            cb_version, instances, ttl, self.rate_limit)
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        if error:
            # self.log.error(error)
            if "Authentication failed" in error:
                cmd = cmd_format.format(
                    server.ip, bucket.name, server.rest_username,
                    server.rest_password, items, batch, threads,
                    start_document, "4.0", instances, ttl, self.rate_limit)
                self.log.info("Running {}".format(cmd))
                result = subprocess.Popen(cmd, shell=True,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE)
                output = result.stdout.read()
                error = result.stderr.read()
                if error:
                    self.log.error(error)
                    self.fail("Failed to run the loadgen.")
        if output:
            loaded = output.split('\n')[:-1]
            total_loaded = 0
            for load in loaded:
                total_loaded += int(load.split(':')[1].strip())
            self.assertEqual(total_loaded, items,
                             "Failed to load {} items. Loaded only {} items"
                             .format(items, total_loaded))

    def delete_buckets_with_high_ops(self, server, bucket, items, ops,
                                     batch=20000, threads=5,
                                     start_document=0, instances=1):
            import subprocess
            cmd_format = "python utils/bucket_utils/thanosied.py " \
                         "--spec couchbase://{0} --bucket {1} " \
                         "--user {2} --password {3} " \
                         "--count {4} --batch_size {5} --threads {6} " \
                         "--start_document {7} --cb_version {8} " \
                         "--workers {9} --rate_limit {10} " \
                         "--passes 1  --delete --num_delete {4}"
            cb_version = RestConnection(server).get_nodes_version()[:3]
            if self.num_replicas > 0 and self.use_replica_to:
                cmd_format = "{} --replicate_to 1".format(cmd_format)
            cmd = cmd_format.format(
                server.ip, bucket.name, server.rest_username,
                server.rest_password, items, batch, threads, start_document,
                cb_version, instances, ops)
            self.log.info("Running {}".format(cmd))
            result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
            output = result.stdout.read()
            error = result.stderr.read()
            if error:
                self.log.error(error)
                self.fail("Failed to run the loadgen.")
            if output:
                loaded = output.split('\n')[:-1]
                total_loaded = 0
                for load in loaded:
                    total_loaded += int(load.split(':')[1].strip())
                self.assertEqual(total_loaded, ops,
                                 "Failed to update {} items. Loaded {} items"
                                 .format(ops, total_loaded))

    def check_dataloss_for_high_ops_loader(self, server, bucket, items,
                                           batch=2000, threads=5,
                                           start_document=0,
                                           updated=False, ops=0, ttl=0,
                                           deleted=False, deleted_items=0):
        import subprocess
        cmd_format = "python utils/bucket_utils/thanosied.py " \
                     "--spec couchbase://{0} --bucket {1} " \
                     "--user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} " \
                     "--start_document {7} --cb_version {8} --validation 1 " \
                     "--rate_limit {9} --passes 1"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if updated:
            cmd_format = "{} --updated --ops {}".format(cmd_format, ops)
        if deleted:
            cmd_format = "{} --deleted --deleted_items {}" \
                         .format(cmd_format, deleted_items)
        if ttl > 0:
            cmd_format = "{} --ttl {}".format(cmd_format, ttl)
        cmd = cmd_format.format(server.ip, bucket.name, server.rest_username,
                                server.rest_password, int(items), batch,
                                threads, start_document, cb_version,
                                self.rate_limit)
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        _ = result.stdout.read()
        errors = result.stderr.read()
        return errors

    def get_all_buckets(self, server=None):
        if server is None:
            server = self.cluster.master
        self.buckets = []
        rest = BucketHelper(server)
        json_parsed = rest.get_buckets_json()
        for item in json_parsed:
            bucket_obj = self.parse_get_bucket_json(item)
            self.buckets.append(bucket_obj)
        return self.buckets

    def parse_get_bucket_json(self, parsed):
        bucket = Bucket()
        bucket.name = parsed['name']
        bucket.uuid = parsed['uuid']
        bucket.bucketType = parsed['bucketType']
        bucket.authType = parsed["authType"]
        bucket.saslPassword = parsed["saslPassword"]
        bucket.nodes = list()
        bucket.maxTTL = parsed["maxTTL"]
        if 'vBucketServerMap' in parsed:
            vBucketServerMap = parsed['vBucketServerMap']
            serverList = vBucketServerMap['serverList']
            bucket.servers.extend(serverList)
            if "numReplicas" in vBucketServerMap:
                bucket.replicaNumber = vBucketServerMap["numReplicas"]
            if "serverList" in vBucketServerMap:
                bucket.replicaServers = vBucketServerMap["serverList"]
            # vBucketMapForward
            if 'vBucketMapForward' in vBucketServerMap:
                # let's gather the forward map
                vBucketMapForward = vBucketServerMap['vBucketMapForward']
                counter = 0
                for vbucket in vBucketMapForward:
                    # there will be n number of replicas
                    vbucketInfo = Bucket.vBucket()
                    vbucketInfo.master = serverList[vbucket[0]]
                    if vbucket:
                        for i in range(1, len(vbucket)):
                            if vbucket[i] != -1:
                                vbucketInfo.replica.append(
                                    serverList[vbucket[i]])
                    vbucketInfo.id = counter
                    counter += 1
                    bucket.forward_map.append(vbucketInfo)
            vBucketMap = vBucketServerMap['vBucketMap']
            counter = 0
            for vbucket in vBucketMap:
                # there will be n number of replicas
                vbucketInfo = Bucket.vBucket()
                vbucketInfo.master = serverList[vbucket[0]]
                if vbucket:
                    for i in range(1, len(vbucket)):
                        if vbucket[i] != -1:
                            vbucketInfo.replica.append(serverList[vbucket[i]])
                vbucketInfo.id = counter
                counter += 1
                bucket.vbuckets.append(vbucketInfo)
                # now go through each vbucket and populate the info
            # who is master , who is replica
        # get the 'storageTotals'
        self.log.debug('read {0} vbuckets'.format(len(bucket.vbuckets)))
        stats = parsed['basicStats']
        # vBucketServerMap
        bucketStats = Bucket.BucketStats()
        self.log.debug('Stats: {0}'.format(stats))
        bucketStats.opsPerSec = stats['opsPerSec']
        bucketStats.itemCount = stats['itemCount']
        if bucket.bucketType != "memcached":
            bucketStats.diskUsed = stats['diskUsed']
        bucketStats.memUsed = stats['memUsed']
        quota = parsed['quota']
        bucketStats.ram = quota['ram']
        bucket.stats = bucketStats
        nodes = parsed['nodes']
        for nodeDictionary in nodes:
            node = Node()
            node.uptime = nodeDictionary['uptime']
            node.memoryFree = nodeDictionary['memoryFree']
            node.memoryTotal = nodeDictionary['memoryTotal']
            node.mcdMemoryAllocated = nodeDictionary['mcdMemoryAllocated']
            node.mcdMemoryReserved = nodeDictionary['mcdMemoryReserved']
            node.status = nodeDictionary['status']
            node.hostname = nodeDictionary['hostname']
            if 'clusterCompatibility' in nodeDictionary:
                node.clusterCompatibility = nodeDictionary['clusterCompatibility']
            if 'clusterMembership' in nodeDictionary:
                node.clusterCompatibility = nodeDictionary['clusterMembership']
            node.version = nodeDictionary['version']
            node.os = nodeDictionary['os']
            if "ports" in nodeDictionary:
                ports = nodeDictionary["ports"]
                if "proxy" in ports:
                    node.moxi = ports["proxy"]
                if "direct" in ports:
                    node.memcached = ports["direct"]
            if "hostname" in nodeDictionary:
                value = str(nodeDictionary["hostname"])
                node.ip = value[:value.rfind(":")]
                node.port = int(value[value.rfind(":") + 1:])
            if "otpNode" in nodeDictionary:
                node.id = nodeDictionary["otpNode"]
            bucket.nodes.append(node)
        return bucket

    def _stats_befor_warmup(self, bucket_name):
        self.pre_warmup_stats[bucket_name] = dict()
        self.stats_monitor = self.input.param("stats_monitor", "")
        self.warmup_stats_monitor = self.input.param("warmup_stats_monitor", "")
        if self.stats_monitor is not '':
            self.stats_monitor = self.stats_monitor.split(";")
        if self.warmup_stats_monitor is not '':
            self.warmup_stats_monitor = self.warmup_stats_monitor.split(";")
        for server in self.servers:
            mc_conn = MemcachedClientHelper.direct_client(server, bucket_name,
                                                          self.timeout)
            node_key = "%s:%s" % (server.ip, server.port)
            self.pre_warmup_stats[bucket_name][node_key] = dict()
            self.pre_warmup_stats[bucket_name][node_key]["uptime"] = \
                mc_conn.stats("")["uptime"]
            self.pre_warmup_stats[bucket_name][node_key]["curr_items_tot"] = \
                mc_conn.stats("")["curr_items_tot"]
            self.pre_warmup_stats[bucket_name][node_key]["curr_items"] = \
                mc_conn.stats("")["curr_items"]
            for stat_to_monitor in self.stats_monitor:
                self.pre_warmup_stats[bucket_name][node_key][stat_to_monitor] = \
                    mc_conn.stats('')[stat_to_monitor]
            if self.without_access_log:
                for stat_to_monitor in self.warmup_stats_monitor:
                    self.pre_warmup_stats[bucket_name][node_key][stat_to_monitor] = \
                        mc_conn.stats('warmup')[stat_to_monitor]
            mc_conn.close()

    def wait_till_total_numbers_match(self, master, bucket,
                                      timeout_in_seconds=120):
        self.log.info('waiting for sum_of_curr_items == total_items....')
        start = time.time()
        verified = False
        while (time.time() - start) <= timeout_in_seconds:
            try:
                if self.verify_items_count(master, bucket):
                    verified = True
                    break
                else:
                    time.sleep(2)
            except StatsUnavailableException:
                self.log.error("Unable to retrieve stats for any node!")
                break
        if not verified:
            self.log.info("Print taps for all nodes..")
            rest = RestConnection(master)
            RebalanceHelper.print_taps_from_all_nodes(rest, bucket)
        return verified

    def verify_items_count(self, master, bucket, num_attempt=3, timeout=2):
        # get the #of buckets from rest
        rest = RestConnection(master)
        replica_factor = bucket.replicaNumber
        vbucket_active_sum = 0
        vbucket_replica_sum = 0
        vbucket_pending_sum = 0
        all_server_stats = []
        stats_received = True
        nodes = rest.get_nodes()
        for server in nodes:
            # get the stats
            server_stats = BucketHelper(master).get_bucket_stats_for_node(
                bucket, server)
            if not server_stats:
                self.log.info("Unable to get stats from {0}: {1}"
                              .format(server.ip, server.port))
                stats_received = False
            all_server_stats.append((server, server_stats))
        if not stats_received:
            raise StatsUnavailableException()
        sum = 0
        for server, single_stats in all_server_stats:
            if not single_stats or "curr_items" not in single_stats:
                continue
            sum += single_stats["curr_items"]
            self.log.info("curr_items from {0}:{1} - {2}"
                          .format(server.ip, server.port,
                                  single_stats["curr_items"]))
            if 'vb_pending_num' in single_stats:
                vbucket_pending_sum += single_stats['vb_pending_num']
                self.log.info("vb_pending_num from {0}:{1} - {2}"
                              .format(server.ip, server.port,
                                      single_stats["vb_pending_num"]))
            if 'vb_active_num' in single_stats:
                vbucket_active_sum += single_stats['vb_active_num']
                self.log.info("vb_active_num from {0}:{1} - {2}"
                              .format(server.ip, server.port,
                                      single_stats["vb_active_num"]))
            if 'vb_replica_num' in single_stats:
                vbucket_replica_sum += single_stats['vb_replica_num']
                self.log.info("vb_replica_num from {0}:{1} - {2}"
                              .format(server.ip, server.port,
                                      single_stats["vb_replica_num"]))

        msg = "sum of vb_active_num {0}, vb_pending_num {1}, vb_replica_num {2}"
        self.log.info(msg.format(vbucket_active_sum, vbucket_pending_sum,
                                 vbucket_replica_sum))
        msg = 'sum: {0} and sum * (replica_factor + 1) ({1}) : {2}'
        self.log.info(msg.format(sum, replica_factor+1,
                                 (sum * (replica_factor+1))))
        master_stats = BucketHelper(master).get_bucket_stats(bucket)
        if "curr_items_tot" in master_stats:
            self.log.info('curr_items_tot from master: {0}'
                          .format(master_stats["curr_items_tot"]))
        else:
            raise Exception("Bucket {0} stats doesnt contain 'curr_items_tot':"
                            .format(bucket))
        if replica_factor >= len(nodes):
            self.log.warn("Number of nodes is less than replica requires")
            delta = sum * (len(nodes)) - master_stats["curr_items_tot"]
        else:
            delta = sum * (replica_factor + 1) - master_stats["curr_items_tot"]
        delta = abs(delta)

        if delta > 0:
            if sum == 0:
                missing_percentage = 0
            else:
                missing_percentage = delta * 1.0 / (sum * (replica_factor + 1))
            self.log.info("Nodes stats are: {0}"
                          .format([node.ip for node in nodes]))
        else:
            missing_percentage = 1
        self.log.info("Delta: {0} missing_percentage: {1} replica_factor: {2}"
                      .format(delta, missing_percentage, replica_factor))
        # If no items missing then, return True
        if not delta:
            return True
        return False

    def _wait_warmup_completed(self, servers, bucket, wait_time=300):
        # Return True, if bucket_type is not equal to MEMBASE
        if bucket.bucketType != Bucket.bucket_type.MEMBASE:
            return True

        self.log.info("Waiting for bucket %s to complete warmup" % bucket.name)
        warmed_up = False
        start = time.time()
        for server in servers:
            # Cbstats implementation to wait for bucket warmup
            warmed_up = False
            shell = RemoteMachineShellConnection(server)
            cbstat_obj = Cbstats(shell)
            while time.time() - start < wait_time:
                result = cbstat_obj.all_stats(bucket.name, "ep_warmup_thread")
                if result is not None and result == "complete":
                    warmed_up = True
                    break
                self.sleep(2, "Warmup not complete for %s on %s"
                           % (bucket.name, server.ip))
            shell.disconnect()

            """
            # Try to get the stats for 5 minutes, else hit out.
            while time.time() - start < wait_time:
                # Get the warm-up time for each server
                try:
                    mc = MemcachedClientHelper.direct_client(server,
                                                             bucket)
                    stats = mc.stats()
                    if stats is not None and 'ep_warmup_thread' in stats \
                            and stats['ep_warmup_thread'] == 'complete':
                        break
                    else:
                        self.log.info("Failed to get the server stats, retrying..")
                        time.sleep(2)
                except Exception as e:
                    self.log.error("Failed to get ep_warmup_time stats "
                                   "from %s:%s, exception %s"
                                   % (server.ip, server.port, e))
            else:
                self.fail("Failed to get ep_warmup_time stats "
                          "from %s:%s after %s secs"
                          % (server.ip, server.port, wait_time))

            # Waiting for warm-up
            start = time.time()
            warmed_up = False
            while time.time() - start < wait_time and not warmed_up:
                if mc.stats()["ep_warmup_thread"] == "complete":
                    self.log.info("Warmup completed, Warmed up. %s items"
                                  % (mc.stats()["curr_items_tot"]))
                    warmed_up = True
                    continue
                elif mc.stats()["ep_warmup_thread"] == "running":
                    self.log.info("still warming up .... curr_items_tot : %s"
                                  % (mc.stats()["curr_items_tot"]))
                else:
                    self.fail("Value of ep_warmup thread does not exist, exiting from this server")
                time.sleep(5)
            mc.close()
            """
        return warmed_up

    def add_rbac_user(self, testuser=None, rolelist=None, node=None):
        """
           From spock, couchbase server is built with some users that handles
           some specific task such as:
               cbadminbucket
           Default added user is cbadminbucket with admin role
        """
        if node is None:
            node = self.cluster.master
        # rest = BucketHelper(node)
        # cluster_compatibility = rest.check_cluster_compatibility("5.0")
        # if cluster_compatibility is None:
        #     pre_spock = True
        # else:
        #     pre_spock = not cluster_compatibility
        # if pre_spock:
        #     self.log.info("Atleast one of the nodes in the cluster is "
        #                   "pre 5.0 version. Hence not creating rbac user "
        #                   "for the cluster. RBAC is a 5.0 feature.")
        #     return
        if testuser is None:
            testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                         'password': 'password'}]
        if rolelist is None:
            rolelist = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                         'roles': 'admin'}]

        self.log.info("**** add built-in '%s' user to node %s ****"
                      % (testuser[0]["name"], node.ip))
        RbacUtil().create_user_source(testuser, 'builtin', node)

        self.log.info("**** add '%s' role to '%s' user ****"
                      % (rolelist[0]["roles"], testuser[0]["name"]))
        status = RbacUtil().add_user_role(rolelist, RestConnection(node),
                                          'builtin')
        return status

    def async_create_views(self, server, design_doc_name, views,
                           bucket="default", with_query=True,
                           check_replication=False):
        tasks = []
        if len(views):
            for view in views:
                t_ = self.async_create_view(
                    server, design_doc_name, view, bucket, with_query,
                    check_replication=check_replication)
                tasks.append(t_)
                time.sleep(5)
        else:
            t_ = self.async_create_view(
                server, design_doc_name, None, bucket, with_query,
                check_replication=check_replication)
            tasks.append(t_)
            time.sleep(5)
        return tasks

    def create_views(self, server, design_doc_name, views, bucket="default",
                     timeout=None, check_replication=False):
        if len(views):
            for view in views:
                self.create_view(
                    server, design_doc_name, view, bucket, timeout,
                    check_replication=check_replication)
        else:
            self.create_view(
                server, design_doc_name, None, bucket, timeout,
                check_replication=check_replication)

    def async_create_view(self, server, design_doc_name, view,
                          bucket="default", with_query=True,
                          check_replication=False, ddoc_options=None):
        """
        Asynchronously creates a views in a design doc

        Parameters:
          server - The server to handle create view task. (TestInputServer)
          design_doc_name - Design doc to be created or updated with view(s)
                            being created (String)
          view - The view being created (document.View)
          bucket - Name of the bucket containing items for this view. (String)
          with_query - Wait indexing to get view query results after creation
          check_replication - Should the test check replication or not (Bool)
          ddoc_options - DDoc options to define automatic index building
                         (minUpdateChanges, updateInterval ...) (Dict)
        Returns:
          ViewCreateTask - A task future that is a handle to the scheduled task
        """
        _task = ViewCreateTask(
            server, design_doc_name, view, bucket, with_query,
            check_replication, ddoc_options)
        self.task_manager.add_new_task(_task)
        return _task

    def create_view(self, server, design_doc_name, view, bucket="default",
                    timeout=None, with_query=True, check_replication=False):
        """
        Synchronously creates a views in a design doc

        Parameters:
          server - The server to handle create view task. (TestInputServer)
          design_doc_name - Design doc to be created or updated with view(s)
                            being created (String)
          view - The view being created (document.View)
          bucket - Name of the bucket containing items for this view. (String)
          with_query - Wait indexing to get view query results after creation

        Returns:
          string - revision number of design doc
        """
        _task = self.async_create_view(server, design_doc_name, view, bucket,
                                       with_query, check_replication)
        return self.task_manager.get_task_result(_task)

    def async_delete_view(self, server, design_doc_name, view,
                          bucket="default"):
        """
        Asynchronously deletes a views in a design doc

        Parameters:
          server - The server to handle delete view task. (TestInputServer)
          design_doc_name - Design doc to be deleted or updated with view(s)
                            being deleted (String)
          view - The view being deleted (document.View)
          bucket - Name of the bucket containing items for this view. (String)

        Returns:
          ViewDeleteTask - A task future that is a handle to the scheduled task
        """
        _task = ViewDeleteTask(server, design_doc_name, view, bucket)
        self.task_manager.add_new_task(_task)
        return _task

    def delete_view(self, server, design_doc_name, view, bucket="default",
                    timeout=None):
        """
        Synchronously deletes a views in a design doc

        Parameters:
          server - The server to handle delete view task. (TestInputServer)
          design_doc_name - Design doc to be deleted or updated with view(s)
                            being deleted (String)
          view - The view being deleted (document.View)
          bucket - Name of the bucket containing items for this view. (String)

        Returns:
          boolean - Whether or not delete view was successful
        """
        _task = self.async_delete_view(server, design_doc_name, view, bucket)
        return self.task_manager.get_task_result(_task)

    def async_query_view(self, server, design_doc_name, view_name, query,
                         expected_rows=None, bucket="default", retry_time=2):
        """
        Asynchronously query a views in a design doc

        Parameters:
          server - The server to handle query view task. (TestInputServer)
          design_doc_name - Design doc with view(s) being queried(String)
          view_name - The view being queried (String)
          expected_rows - The number of rows expected to be returned from
                          the query (int)
          bucket - Name of the bucket containing items for this view. (String)
          retry_time - Time in secs to wait before retrying
                       failed queries (int)

        Returns:
          ViewQueryTask - A task future that is a handle to the scheduled task
        """
        _task = ViewQueryTask(server, design_doc_name, view_name, query,
                              expected_rows, bucket, retry_time)
        self.task_manager.add_new_task(_task)
        return _task

    def query_view(self, server, design_doc_name, view_name, query,
                   expected_rows=None, bucket="default", retry_time=2,
                   timeout=None):
        """
        Synchronously query a views in a design doc

        Parameters:
          server - The server to handle query view task. (TestInputServer)
          design_doc_name - Design doc with view(s) being queried(String)
          view_name - The view being queried (String)
          expected_rows - The number of rows expected to be returned from
                          the query (int)
          bucket - Name of the bucket containing items for this view (String)
          retry_time - Time in seconds to wait before retrying
                       failed queries (int)

        Returns:
          ViewQueryTask - Task future that is a handle to the scheduled task
        """
        _task = self.async_query_view(server, design_doc_name, view_name,
                                      query, expected_rows, bucket, retry_time)
        return self.task_manager.get_task_result(_task)

    def perform_verify_queries(self, num_views, prefix, ddoc_name, view_name,
                               query, wait_time=120, bucket="default",
                               expected_rows=None, retry_time=2, server=None):
        tasks = []
        result = True
        if server is None:
            server = self.cluster.master
        if expected_rows is None:
            expected_rows = self.num_items
        for i in xrange(num_views):
            tasks.append(self.async_query_view(
                server, prefix + ddoc_name, view_name + str(i), query,
                expected_rows, bucket, retry_time))
        try:
            for task in tasks:
                self.task_manager.get_task_result(task)
                if not task.result:
                    self.log.error("Task {} is failed".format(task.thread_name))
                result = result and task.result
        except Exception as e:
            print e
            for task in tasks:
                task.cancel()
            raise Exception("Failed to get expected results for view query after {0} sec"
                            .format(wait_time))
        return result

    def make_default_views(self, default_view, prefix, count,
                           is_dev_ddoc=False, different_map=False):
        ref_view = default_view
        ref_view.name = (prefix, ref_view.name)[prefix is None]
        if different_map:
            views = []
            for i in xrange(count):
                views.append(View(ref_view.name + str(i),
                                  'function (doc, meta) {'
                                  'emit(meta.id, "emitted_value%s");}'
                                  % str(i), None, is_dev_ddoc))
            return views
        else:
            return [
                View("{0}{1}".format(ref_view.name, i),
                     ref_view.map_func, None, is_dev_ddoc)
                for i in xrange(count)
                ]

    def async_print_bucket_ops(self, bucket, sleep=1):
        task = PrintOpsRate(self.cluster, bucket, sleep)
        self.task_manager.add_new_task(task)
        return task

    def base_bucket_ratio(self, servers):
        ratio = 1.0
        # check if ip is same for all servers
        ip = servers[0].ip
        dev_environment = True
        for server in servers:
            if server.ip != ip:
                dev_environment = False
                break
        if dev_environment:
            ratio = 2.0 / 3.0 * 1 / len(servers)
        else:
            ratio = 2.0 / 3.0
        return ratio

    def start_parallel_cruds_for_all_buckets(
            self, cluster, doc_ops, gen_create, gen_update, gen_delete,
            exp=0, persist_to=0, replicate_to=0, durability="", sdk_timeout=10,
            pause_secs=5, batch_size=20, process_concurrency=8,
            retry_exceptions=[], ignore_exceptions=[],
            task_verification=False):
        tasks_info = dict()
        if "update" in doc_ops:
            tem_tasks_info = self._async_load_all_buckets(
                cluster, gen_update, "update", exp, batch_size=batch_size,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability, pause_secs=pause_secs,
                timeout_secs=sdk_timeout,
                process_concurrency=process_concurrency,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)
            tasks_info.update(tem_tasks_info.items())
        if "create" in doc_ops:
            tem_tasks_info = self._async_load_all_buckets(
                cluster, gen_create, "create", exp, batch_size=batch_size,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability, pause_secs=pause_secs,
                timeout_secs=sdk_timeout,
                process_concurrency=process_concurrency,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)
            tasks_info.update(tem_tasks_info.items())
        if "delete" in doc_ops:
            tem_tasks_info = self._async_load_all_buckets(
                cluster, gen_delete, "delete", exp, batch_size=batch_size,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability, pause_secs=pause_secs,
                timeout_secs=sdk_timeout,
                process_concurrency=process_concurrency,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)
            tasks_info.update(tem_tasks_info.items())

        if task_verification:
            self.verify_doc_op_task_exceptions(tasks_info, cluster)
            self.log_doc_ops_task_failures(tasks_info)

        return tasks_info
