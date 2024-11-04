from random import choice

from BucketLib.bucket import Bucket
from cb_constants import DocLoading
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from sdk_constants.java_client import SDKConstants


class NsServerFeaturePlugins(object):
    @staticmethod
    def test_durability_impossible_fallback(test_obj, server_for_rest_conn):
        def set_fallback_value(value):
            bucket_rest = BucketRestApi(server_for_rest_conn)
            bucket_param = {Bucket.durabilityImpossibleFallback: value}
            test_obj.log.info(f"Updating {bucket_param}")
            for t_bucket in test_obj.cluster.buckets:
                bucket_rest.edit_bucket(t_bucket.name, bucket_param)
            # As per the ns_server code, we update the new value every 10secs
            test_obj.sleep(12, "Wait for new settings to take effect")

        def load_and_validate(load_will_fail):
            durability = choice([
                SDKConstants.DurabilityLevel.MAJORITY,
                SDKConstants.DurabilityLevel.PERSIST_TO_MAJORITY,
                SDKConstants.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE])
            tasks = list()
            for t_bucket in test_obj.cluster.buckets:
                tasks.append(test_obj.task.async_load_gen_docs(
                    test_obj.cluster, t_bucket, doc_gen[t_bucket.name],
                    DocLoading.Bucket.DocOps.UPDATE,
                    batch_size=200,
                    durability=durability,
                    process_concurrency=2,
                    load_using=test_obj.load_docs_using))

            if load_will_fail:
                for task in tasks:
                    test_obj.task_manager.get_task_result(task)
                    test_obj.assertEqual(len(task.fail), num_items,
                                         "Load failed with fallback enabled")
            else:
                for task in tasks:
                    test_obj.task_manager.get_task_result(task)
                    test_obj.assertTrue(len(task.fail) == 0,
                                        "Failures seen in update docs task")

            # If doc_load is expected to succeed, then delete the docs
            tasks = list()
            for t_bucket in test_obj.cluster.buckets:
                tasks.append(test_obj.task.async_load_gen_docs(
                    test_obj.cluster, t_bucket, doc_gen[t_bucket.name],
                    DocLoading.Bucket.DocOps.DELETE,
                    durability=durability,
                    process_concurrency=2,
                    load_using=test_obj.load_docs_using))

            for task in tasks:
                test_obj.task_manager.get_task_result(task)
                if not load_will_fail:
                    test_obj.assertTrue(len(task.fail) == 0,
                                        "Failures seen in delete docs task")

        def get_committed_not_durable_count_for_buckets():
            active_commited_not_durable_count = dict()
            replica_commited_not_durable_count = dict()
            stat_name_pattern = "vb_%s_sync_write_committed_not_durable_count"
            for cbstat_obj in cbstats_obj:
                for t_bucket in test_obj.cluster.buckets:
                    if t_bucket.name not in active_commited_not_durable_count:
                        active_commited_not_durable_count[t_bucket.name] = 0
                        replica_commited_not_durable_count[t_bucket.name] = 0
                    active_commited_not_durable_count[t_bucket.name] += \
                        int(cbstat_obj.all_stats(t_bucket.name)[
                                stat_name_pattern % "active"])
                    replica_commited_not_durable_count[t_bucket.name] += \
                        int(cbstat_obj.all_stats(t_bucket.name)[
                                stat_name_pattern % "replica"])
            return (active_commited_not_durable_count,
                    replica_commited_not_durable_count)

        nodes = test_obj.cluster_util.get_nodes(server_for_rest_conn)
        servers_to_create_cbstat_obj = list()
        # Filtering 'active' nodes since during failovers,
        # we should not create cbstat objs for nodes bucket is not hosted
        for node in nodes:
            for cluster_node in test_obj.cluster.kv_nodes:
                if node.ip == cluster_node.ip:
                    servers_to_create_cbstat_obj.append(cluster_node)
                    break
        cbstats_obj = [Cbstats(server) for server in
                       servers_to_create_cbstat_obj]

        doc_gen = dict()
        num_items = 2000
        for bucket in test_obj.cluster.buckets:
            target_vbs = list(range(bucket.numVBuckets))
            available_replicas = list()
            for cbstat_obj in cbstats_obj:
                available_replicas.extend(cbstat_obj.vbucket_list(
                    bucket.name, vbucket_type="replica"))

            target_vbs = list(set(target_vbs).difference(
                set(available_replicas)))
            doc_gen[bucket.name] = doc_generator(
                "d_fb_keys", 0, num_items, key_size=20,
                vbuckets=bucket.numVBuckets, target_vbucket=target_vbs)

        init_active_non_durable_cnt, init_replica_non_durable_cnt = \
            get_committed_not_durable_count_for_buckets()

        test_obj.log.info("Load with default durability_fallback (disabled)")
        load_and_validate(load_will_fail=True)
        disabled_active_non_durable_cnt, disabled_replica_non_durable_cnt = \
            get_committed_not_durable_count_for_buckets()
        for bucket in test_obj.cluster.buckets:
            test_obj.assertEqual(
                disabled_active_non_durable_cnt[bucket.name],
                init_active_non_durable_cnt[bucket.name],
                f"Active stats mismatch for '{bucket.name}': "
                f"{disabled_active_non_durable_cnt}")
            test_obj.assertEqual(
                disabled_replica_non_durable_cnt[bucket.name],
                init_active_non_durable_cnt[bucket.name],
                f"Replica stats mismatch for '{bucket.name}': "
                f"{disabled_active_non_durable_cnt}")

        set_fallback_value("fallbackToActiveAck")
        test_obj.log.info("Loading docs with durability fallback enabled")
        load_and_validate(load_will_fail=False)

        fallback_active_non_durable_cnt, fallback_replica_non_durable_cnt = \
            get_committed_not_durable_count_for_buckets()
        for bucket in test_obj.cluster.buckets:
            # 'num_items*2' since we do update + delete for fallback case
            test_obj.assertEqual(
                fallback_active_non_durable_cnt[bucket.name],
                disabled_active_non_durable_cnt[bucket.name]+(num_items*2),
                f"Active stats mismatch for '{bucket.name}': "
                f"{fallback_active_non_durable_cnt}")
            test_obj.assertEqual(
                fallback_replica_non_durable_cnt[bucket.name],
                disabled_active_non_durable_cnt[bucket.name],
                f"Replica stats mismatch for '{bucket.name}': "
                f"{fallback_replica_non_durable_cnt}")

        set_fallback_value("disabled")
        test_obj.log.info("Loading docs with durability fallback disabled")
        load_and_validate(load_will_fail=True)

        disabled_active_non_durable_cnt, disabled_replica_non_durable_cnt = \
            get_committed_not_durable_count_for_buckets()
        for bucket in test_obj.cluster.buckets:
            test_obj.assertEqual(
                disabled_active_non_durable_cnt[bucket.name],
                fallback_active_non_durable_cnt[bucket.name],
                f"Active stats mismatch for '{bucket.name}': "
                f"{disabled_active_non_durable_cnt}")
            test_obj.assertEqual(
                disabled_replica_non_durable_cnt[bucket.name],
                fallback_replica_non_durable_cnt[bucket.name],
                f"Replica stats mismatch for '{bucket.name}': "
                f"{disabled_active_non_durable_cnt}")
        [cbstat_obj.disconnect() for cbstat_obj in cbstats_obj]
