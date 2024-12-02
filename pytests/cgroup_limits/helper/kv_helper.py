import math
import time
from BucketLib.bucket import Bucket
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from cb_constants import CbServer
from cgroup_limits.cgroup_base import CGgroupConstants
from shell_util.remote_connection import RemoteMachineShellConnection


class CGroupDataHelper:

    def __init__(self, cluster, host, task_manager, task, logger, bucket_util):
        self.cluster = cluster
        self.task_manager = task_manager
        self.task = task
        self.host = host
        self.log = logger
        self.bucket_util = bucket_util

    def create_buckets(self, server, num_buckets, ram_quota, replicas, bucket_storage, bucket_prefix):

        self.log.info(f"Creating {num_buckets} buckets")

        for i in range(num_buckets):
            bucket_name = bucket_prefix + str(i)
            cmd = "/opt/couchbase/bin/couchbase-cli bucket-create -c 127.0.0.1" \
                    " --username Administrator --password password" \
                    " --bucket {} --bucket-type couchbase".format(bucket_name) + \
                    " --bucket-ramsize {} --bucket-replica {}".format(
                        ram_quota, replicas) + \
                    " --storage-backend {}".format(bucket_storage)
            self.log.info("Bucket create cmd: {}".format(cmd))
            if CGgroupConstants.base_infra == "docker":
                o, e = self.execute_command_docker(cmd, server)
            else:
                o, e = RemoteMachineShellConnection(server).execute_command(cmd)
            self.log.info("Create bucket:{} output = {}".format(bucket_name, o))

            bucket_obj = Bucket(
                            {
                                "name": bucket_name,
                                "replicaNumber": replicas,
                                "ramQuotaMB": ram_quota,
                                "storageBackend": bucket_storage
                            }
                        )

            self.cluster.buckets.append(bucket_obj)
            self.log.info(f"Bucket obj: {bucket_obj.__dict__}")
            time.sleep(5)

        # Calculating clients_per_bucket
        max_clients = min(self.task_manager.number_of_threads, 20)
        if num_buckets > 20:
            max_clients = num_buckets
        clients_per_bucket = int(math.ceil(max_clients / num_buckets))

        # Create clients for buckets
        for bucket in self.cluster.buckets:
            self.log.info(f"Bucket: {bucket.name}, bucket_obj: {bucket.__dict__}")
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.host,
                self.host.rest_username,
                self.host.rest_password,
                bucket.name,
                clients_per_bucket)

    def create_scopes_collections_for_bucket(self, num_scopes, num_collections):

        scope_prefix = "Scope"
        for bucket in self.cluster.buckets:
            for i in range(1, num_scopes):
                scope_name = scope_prefix + str(i)
                self.log.info("Creating bucket::scope {} {}\
                ".format(bucket.name, scope_name))
                self.bucket_util.create_scope(self.cluster.master,
                                              bucket,
                                              {"name": scope_name})
                time.sleep(2)
        self.scopes = self.cluster.buckets[0].scopes.keys()
        self.log.info("Scopes list is {}".format(self.scopes))

        collection_prefix = "FunctionCollection"
        for bucket in self.cluster.buckets:
            for scope_name in self.scopes:
                if scope_name == CbServer.system_scope:
                    continue
                for i in range(len(bucket.scopes[scope_name].collections), num_collections):
                    collection_name = collection_prefix + str(i)
                    self.log.info("Creating scope::collection {} {}\
                    ".format(scope_name, collection_name))
                    self.bucket_util.create_collection(
                        self.cluster.master, bucket,
                        scope_name, {"name": collection_name})
                    time.sleep(2)
        self.collections = self.cluster.buckets[0].scopes[CbServer.default_scope].collections.keys()
        self.log.info("Collections list == {}".format(self.collections))

    def load_data_to_bucket(self, start, end, doc_op="create", ops_rate=50000):

        create_start = 0
        create_end = 0
        create_percent = 0
        update_start = 0
        update_end = 0
        update_percent = 0

        if doc_op == "create":
            create_start = start
            create_end = end
            create_percent = 100

        elif doc_op == "update":
            update_start = start
            update_end = end
            update_percent = 100

        doc_loading_tasks = list()

        for bucket in self.cluster.buckets:
            self.log.info(f"Loading data to bucket: {bucket.name}")
            scopes_keys = bucket.scopes.keys()
            for scope in scopes_keys:
                if scope == CbServer.system_scope:
                    continue
                collections_keys = bucket.scopes[scope].collections.keys()
                for collection in collections_keys:
                    self.log.info(f"Starting {doc_op} workload on {scope}:{collection}")
                    _task = SiriusCouchbaseLoader(
                                self.cluster.master.unique_ip,
                                self.cluster.master.port,
                                None, doc_op,
                                self.host.rest_username,
                                self.host.rest_password,
                                bucket, scope, collection,
                                doc_size=1024,
                                key_type="RandomKey",
                                create_percent=create_percent,
                                create_start_index=create_start,
                                create_end_index=create_end,
                                update_start_index=update_start,
                                update_end_index=update_end,
                                update_percent=update_percent,
                                process_concurrency=2,
                                validate_docs=False,
                                ops=ops_rate)
                    status, info = _task.create_doc_load_task()
                    self.log.info(f"Status = {status}, Info = {info}")
                    self.task_manager.add_new_task(_task)
                    doc_loading_tasks.append(_task)

        for task in doc_loading_tasks:
            self.task_manager.get_task_result(task)

    def execute_command_docker(self, cmd, server):

        cmd = f"docker exec {server.container_name} {cmd}"
        o, e = RemoteMachineShellConnection(self.host).execute_command(cmd)
        return o, e