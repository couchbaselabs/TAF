import threading
import time

from Cb_constants import DocLoading
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from gsiLib.gsiHelper import GsiHelper

from storage.plasma.plasma_base import PlasmaBaseTest


class PlasmaStatsTest(PlasmaBaseTest):
    def setUp(self):
        super(PlasmaStatsTest, self).setUp()
        self.timer = self.input.param("timer", 600)
        self.items_add = self.input.param("items_add", 1000000)
        self.resident_ratio = \
            float(self.input.param("resident_ratio", .9))

    def tearDown(self):
        super(PlasmaStatsTest, self).tearDown()

    def test_item_count_stats(self):
        query_nodes_list = self.cluster_util.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        self.log.info("Starting test")
        i = 0
        indexDict = dict()
        self.index_count = self.input.param("index_count", 11)
        for i in range(self.index_count):
            indexName = "Index0" + str(i)
            index_query = "CREATE INDEX `%s` ON `%s`(`body`)" % (indexName,
                                                                 self.buckets[0].name)
            self.query_client = RestConnection(query_nodes_list[0])
            indexDict[indexName] = index_query
            result = self.query_client.query_tool(index_query)
            self.assertTrue(result["status"] == "success", "Index query failed!")

        indexer_rest = GsiHelper(self.cluster.master, self.log)
        indexStatMap = dict()

        counter = 0
        for server in self.cluster.servers:
            server.services = self.services[counter]
            counter = counter + 1
        total_count = 0
        ipIndexDict = dict()
        for server in self.cluster.servers:
            if server.services.find("index") != -1:
                generic_url = "http://%s:%s/"
                ip = server.ip
                port = "9102"
                baseURL = generic_url % (ip, port)
                self.log.info("Try to get index from URL as {}".format(baseURL))
                indexStatMap = indexer_rest.get_index_stats(URL=baseURL)

                self.log.info(
                    "Memory quota is {} and Memory used is {} for baseURL".format(str(indexStatMap['memory_quota']),
                                                                                  str(indexStatMap['memory_used']),
                                                                                  str(baseURL)))
                total_count = 0
                if indexStatMap.has_key(self.buckets[0].name):
                    for key in indexStatMap[self.buckets[0].name]:
                        ipIndexDict[key] = ip + ":" + self.query_client.port
                        self.assertEqual(indexStatMap[self.buckets[0].name][key]['num_docs_indexed'], self.num_items,
                                         "Expected no of items are not coming")
                        self.log.info("Data size for index {} is:{}".format(key,
                                                                            str(indexStatMap[self.buckets[0].name][key][
                                                                                    'data_size'])))
                        self.log.info("Disk size for index:{} is:{}".format(key,
                                                                            str(indexStatMap[self.buckets[0].name][key][
                                                                                    'disk_size'])))

                        self.log.info("Fragmentation for index {} is {}".format(key, str(
                            indexStatMap[self.buckets[0].name][key]['frag_percent'])))

                        for dist in dict(indexStatMap[self.buckets[0].name][key]['key_size_distribution']).items():
                            total_count = total_count + dist[1]

                        self.assertEqual(total_count, self.num_items,
                                         "Expected total no of items are {} in key size distribution list But actual is {}".format(
                                             str(total_count), str(self.num_items)))
                        total_count = 0
        for x in range(5):
            result = indexer_rest.index_status()
            if result[self.buckets[0].name].has_key(indexDict.items()[0][0]):
                break
            self.sleep(1)

        # Validating the Definition associated with the indexes
        self.log.info("Comparing the actual index list with expected one")
        for key in indexDict:
            self.assertEqual(result[self.buckets[0].name][key]['definition'], indexDict[key],
                             "Index queries is:{} and definition is :{}".format(
                                 result[self.buckets[0].name][key]['definition'], indexDict[key]))
            self.assertEqual(result[self.buckets[0].name][key]['hosts'],
                             (ipIndexDict[key]), "Actual hosts values is: {} and expected host value is: {}".format(
                    result[self.buckets[0].name][key]['hosts'], ipIndexDict[key]))

    # Test case details
    # 1. Create a cluster with 2 kv and 2 index, query node
    # 2. Load 1M documents in the bucket
    # 3. Create 5 indexes with index replica present
    # 4. Run 5 queries in parallel. == Comment
    # 5. Start a upsert load which will upsert the 1M documents in async continuously until we stop it.
    # 6. Add 2 more index nodes to the cluster.
    # 7. Create 5 more indexes and 10 queries in parallel.
    # 8. Run 20 queries in parallel
    # 9. Run 40 queries in parallel
    # 10. Check indexer fragmentation is maintained throughout the test.
    # 11. Assert indexer disk / data size for any one index against expected value manually calculated based on indexed data.
    # 12. Validate the memory
    # 12. Drop all index
    # 14. Compare the memory as good as Step 1

    def test_index_upsert_ops(self):
        indexDict = dict()
        indexTask_list = list()
        count = 0

        self.index_replicas = self.input.param("index_replicas", 1)
        self.log.info("Starting upsert test")
        i = 0
        query_nodes_list = self.cluster_util.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)

        indexer_rest = GsiHelper(self.cluster.master, self.log)
        self.index_count = self.input.param("index_count", 1)
        query_len = len(query_nodes_list)

        for i in range(self.index_count):
            indexName = "Index" + str(i)
            index_query = "CREATE INDEX `%s` ON `%s`(`body`) with {\"num_replica\":%s}" % (indexName,
                                                                                           self.buckets[0].name,
                                                                                           self.index_replicas)
            instance = i % query_len
            self.query_client = RestConnection(query_nodes_list[instance])
            indexDict[indexName] = index_query
            self.query_client.query_tool(index_query)
            result = indexer_rest.polling_create_index_status(bucket=self.buckets[0], index=indexName)
            # self.assertTrue(result, "Index query failed!")
            self.log.info("Status is:" + str(result))

        # Run 20 queries
        tasks_info = list()
        queryList = list()
        contentType = 'application/x-www-form-urlencoded'
        connection = 'keep-alive'
        count = self.index_count
        for x in range(20):
            index_instance = x % count
            queryString = self.randStr(Num=4)
            query = "select * from `%s` data USE INDEX (%s USING GSI) where body like '%%%s%%' limit 10" % (
                self.buckets[0].name, indexDict.keys()[index_instance], queryString)
            task = self.task.aysnc_execute_query(query_nodes_list[0], query, contentType, connection)
            tasks_info.append(task)

        for taskInstance in tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)

        self.items_add = self.input.param("items_add", 1000000)
        start = self.num_items
        end = self.num_items + self.items_add
        initial_load = doc_generator(self.key, start, end,
                                     doc_size=self.doc_size)
        insertTask = self.task.async_load_gen_docs(
            self.cluster, self.cluster.buckets[0], initial_load,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=100, process_concurrency=8,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool)

        # Add 2 more index nodes to the cluster
        self.nodes_in = self.input.param("nodes_in", 2)
        count = len(self.dcp_services) + self.nodes_init
        nodes_in = self.cluster.servers[count:count + self.nodes_in]
        services = ["index", "n1ql"]
        result = self.task.rebalance([self.cluster.master],
                                     nodes_in,
                                     [],
                                     services=services)

        # Create 5 index in parallel
        count = count + 5
        counter = self.index_count
        self.iterations = self.input.param("iterations", 20)
        for counter in range(self.index_count, count):
            indexName = "Index" + str(counter)
            query = "CREATE INDEX `%s` ON `%s`(`body`) with {\"num_replica\":%s}" % (indexName,
                                                                                     self.buckets[0].name,
                                                                                     self.index_replicas)
            task = self.task.async_execute_query(server=query_nodes_list[0], query=query, bucket=self.buckets[0],
                                                 indexName=indexName, isIndexerQuery=True)
            indexTask_list.append(task)
            indexDict[indexName] = query
            counter += 1
        for taskInstance in indexTask_list:
            self.task.jython_task_manager.get_task_result(taskInstance)

        # Run 5  queries
        tasks_info = list()
        for x in range(5):
            query_node_index = x % len(query_nodes_list)
            index_instance = x % count
            self.log.info("Index for query node is:" + str(query_node_index))
            queryString = self.randStr(Num=8)
            query = "select * from `%s` data USE INDEX (%s USING GSI) where body like '%%%s%%' limit 10" % (
                self.buckets[0].name, indexDict.keys()[index_instance], queryString)
            task = self.task.async_execute_query(query_nodes_list[query_node_index], query, contentType, connection)
            tasks_info.append(task)

        for taskInstance in tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)

        new_task_info = list()
        self.log.info("Starting executing 10 queries")
        # Run 20 queries
        for x in range(20):
            query_node_index = x % len(query_nodes_list)
            index_instance = x % count
            queryString = self.randStr(Num=8)
            query = "select * from `%s` data USE INDEX (%s USING GSI) where body like '%%%s%%' limit 10" % (
                self.buckets[0].name, indexDict.keys()[index_instance], queryString)
            task = self.task.aysnc_execute_query(query_nodes_list[query_node_index], query, contentType, connection)
            new_task_info.append(task)

        for taskInstance in new_task_info:
            self.task.jython_task_manager.get_task_result(taskInstance)
        task_list = list()
        # Run 40 queries
        for x in range(40):
            query_node_index = x % len(query_nodes_list)
            queryString = self.randStr(Num=8)
            index_instance = x % count
            query = "select * from `%s` data USE INDEX (%s USING GSI) where body like '%%%s%%' limit 10" % (
                self.buckets[0].name, indexDict.keys()[index_instance], queryString)
            task = self.task.aysnc_execute_query(query_nodes_list[query_node_index], query, contentType, connection)
            task_list.append(task)

        for taskInstance in task_list:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.task_manager.stop_task(insertTask)
        self.cluster_util.print_cluster_stats()

    # Test case details
    # 1. Create a cluster with 2 kv and 2 index, query node
    # 2. Load 1M documents in the bucket
    # 3. Create 5 indexes with index replica present
    # 4. Start a upsert load which will upsert the 1M documents in async continuously until we stop it.
    # 5.  Check indexer fragmentation is maintained throughout the test.
    # 6. Assert indexer disk / data size for any one index against expected value manually calculated based on indexed data.
    # 7. Validate the memory
    # 8. Drop all index
    # 9. Compare the memory as good as Step 1

    def test_plasma_stats(self):
        indexDict = dict()

        self.cluster_util.print_cluster_stats()
        self.index_replicas = self.input.param("index_replicas", 1)
        self.log.info("Starting upsert test")
        i = 0
        query_nodes_list = self.cluster_util.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        indexer_nodes_list = self.cluster_util.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        indexer_rest = GsiHelper(self.cluster.master, self.log)

        rest = RestConnection(self.cluster.master)
        cluster_stat = rest.get_cluster_stats()

        index_Mem_Map = dict()
        for indexNode in indexer_nodes_list:
            index_Mem_Map[indexNode.ip + ":8091"] = cluster_stat[indexNode.ip + ":8091"]['mem_free']

        initialPlasmaStats = indexer_rest.get_plasma_stats(nodes_list=indexer_nodes_list)

        bucket_Index_key = self.buckets[0].name + ":" + "initial_idx"
        self.assertEqual(initialPlasmaStats[bucket_Index_key + '_items_count'], self.num_items, "Count is expected")
        self.assertEqual(initialPlasmaStats[bucket_Index_key + '_inserts'], self.num_items,
                         "insert count is not expected")

        self.print_plasma_stats(plasmaDict=initialPlasmaStats, bucket=self.buckets[0], indexname="initial_idx")

        self.index_count = self.input.param("index_count", 5)

        for i in range(self.index_count):
            indexName = "Index" + str(i)
            index_query = "CREATE INDEX `%s` ON `%s`(`body`) with {\"num_replica\":%s}" % (indexName,
                                                                                           self.buckets[0].name,
                                                                                           self.index_replicas)
            self.query_client = RestConnection(query_nodes_list[0])
            indexDict[indexName] = index_query
            self.query_client.query_tool(index_query)
            result = indexer_rest.polling_create_index_status(bucket=self.buckets[0], index=indexName)
            self.log.info("Status is:" + str(result))

        interMediatePlasmaStats = indexer_rest.get_plasma_stats(nodes_list=indexer_nodes_list)

        for indexName in indexDict.items():
            bucket_Index_key = self.buckets[0].name + ":" + indexName[0]
            self.assertEqual(interMediatePlasmaStats[bucket_Index_key + '_items_count'], self.num_items,
                             "Count is expected")
            self.assertEqual(interMediatePlasmaStats[bucket_Index_key + '_inserts'], self.num_items,
                             "insert count is not expected")
            self.print_plasma_stats(plasmaDict=interMediatePlasmaStats, bucket=self.buckets[0], indexname=indexName[0])

        self.items_add = self.input.param("items_add", 1000000)
        start = self.num_items
        end = self.num_items + self.items_add
        initial_load = doc_generator(self.key, start, end,
                                     doc_size=self.doc_size)
        insertTask = self.task.async_load_gen_docs(
            self.cluster, self.cluster.buckets[0], initial_load,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=100, process_concurrency=8,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool)

        for indexName in indexDict.items():
            drop_query = "Drop INDEX `%s`.`%s` Using GSI" % (self.buckets[0].name, indexName[0])
            self.query_client = RestConnection(query_nodes_list[0])
            self.query_client.query_tool(drop_query)
            indexer_rest.polling_delete_index(bucket=self.buckets[0], index=indexName)

        p_stats_with_Five_index = indexer_rest.get_plasma_stats(nodes_list=indexer_nodes_list)

        for indexName in indexDict.items():
            bucket_Index_key = self.buckets[0].name + ":" + indexName[0]
            self.assertTrue(p_stats_with_Five_index.get(bucket_Index_key + "_memory_size") is None,
                            "Dropped index are still present in the plams stats")

        indexName = "initial_idx"
        self.print_plasma_stats(plasmaDict=p_stats_with_Five_index, bucket=self.buckets[0], indexname=indexName)
        bucket_Index_key = self.buckets[0].name + ":" + indexName
        self.log.info("item count is {0} insert count is {1} expected item count {2}".format(
            str(p_stats_with_Five_index[bucket_Index_key + '_items_count']),
            str(p_stats_with_Five_index[bucket_Index_key + '_inserts']), str(self.num_items)))
        self.assertTrue(int(p_stats_with_Five_index[bucket_Index_key + '_items_count']) >= int(self.num_items),
                        "Expected item count is {0} but actual value is {1}".format(
                            str(p_stats_with_Five_index[bucket_Index_key + '_items_count']), str(self.num_items)))
        self.assertTrue(int(p_stats_with_Five_index[bucket_Index_key + '_inserts']) >= int(self.num_items),
                        "Expected insert count is {0} but actual count is {1}".format(
                            str(p_stats_with_Five_index[bucket_Index_key + '_inserts']), str(self.num_items)))
        self.cluster_util.print_cluster_stats()
        self.task_manager.stop_task(insertTask)
        self.time_counter = self.input.param("time_counter", 300)

        timer = 0
        flag = False
        for item, itemValue in index_Mem_Map.items():
            flag = False
            while (True):
                cluster_stat = rest.get_cluster_stats()
                expectedValue = int(cluster_stat[item]['mem_free'])
                inputValue = int(itemValue)
                if (.9 * inputValue <= expectedValue):
                    flag = True
                    break
                timer += 1
                self.sleep(1)
                # self.log.info("Initial memory is {0} recent memory is {1}".format(str(inputValue), str(expectedValue)))
                if timer > self.time_counter:
                    break

        self.log.info("Timer value is:" + str(timer))
        self.assertTrue(flag, "Memory not recored")

    # Refer JIRA for more details: https://issues.couchbase.com/browse/MB-46206
    def test_memory_usage_without_index_nodes(self):
        indexDict = dict()

        self.cluster_util.print_cluster_stats()
        self.index_replicas = self.input.param("index_replicas", 0)
        self.log.info("Starting upsert test")
        i = 0
        query_nodes_list = self.cluster_util.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        indexer_nodes_list = self.cluster_util.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        indexer_rest = GsiHelper(self.cluster.master, self.log)

        rest = RestConnection(self.cluster.master)
        cluster_stat = rest.get_cluster_stats()

        index_Mem_Map = dict()
        for indexNode in indexer_nodes_list:
            index_Mem_Map[indexNode.ip + ":8091"] = cluster_stat[indexNode.ip + ":8091"]['mem_free']

        self.cluster_util.print_cluster_stats()

        self.index_count = self.input.param("index_count", 2)
        index_list = list()
        listCounter = 0
        for bucket in self.cluster.buckets:
            for scope_name in bucket.scopes.keys():
                for collection in bucket.scopes[scope_name].collections.keys():
                    index_list_instance = list()
                    for i in range(self.index_count):
                        indexName = "Index" + str(listCounter) + str(i)
                        index_query = "CREATE INDEX `%s` ON `%s`.`%s`.`%s`(`body`) Using GSI with {\"defer_build\":True}" % (
                            indexName,
                            bucket.name, scope_name, collection)
                        self.query_client = RestConnection(query_nodes_list[0])
                        indexDict[indexName] = index_query
                        self.query_client.query_tool(index_query)
                        index_list_instance.append(indexName)
                    index_list.append(index_list_instance)
                    listCounter += 1

        i = 0

        for bucket in self.cluster.buckets:
            for scope_name in bucket.scopes.keys():
                for collection in bucket.scopes[scope_name].collections.keys():
                    index_query = "Build INDEX  ON `%s`.`%s`.`%s` (%s) USING GSI" % (
                        bucket.name, scope_name, collection, index_list[i])
                    self.query_client = RestConnection(query_nodes_list[0])
                    self.query_client.query_tool(index_query)
                    i = i + 1

        for index_list_instance in index_list:
            for indexName in index_list_instance:
                indexer_rest.polling_create_index_status(bucket=self.buckets[0], index=indexName)
        self.bucket_util.delete_all_buckets(self.cluster)
        nodes_out = indexer_nodes_list[1:]
        result = self.task.rebalance([self.cluster.master], to_add=[],
                                     to_remove=nodes_out)

        self.cluster_util.print_cluster_stats()
        self.time_counter = self.input.param("time_counter", 300)
        timer = 0
        flag = False
        for item, itemValue in index_Mem_Map.items():
            status = self.isServerListContainsNode(serverList=nodes_out, ip=item)
            while (status == False):
                flag = False
                cluster_stat = rest.get_cluster_stats()
                expectedValue = int(cluster_stat[item]['mem_free'])
                inputValue = int(itemValue)
                if (.9 * inputValue <= expectedValue):
                    flag = True
                    break
                timer += 1
                self.sleep(1)
                # self.log.info("Initial memory is {0} recent memory is {1}".format(str(inputValue), str(expectedValue)))
                if timer > self.time_counter:
                    break

        self.log.info("Timer value is:" + str(timer))
        self.assertTrue(flag, "Memory not recored")
        print ("stop here")

    def test_system_stability(self):
        self.log.info("Cluster ops test")

        # Set indexer storage mode
        for index_node in self.cluster.index_nodes:
            self.indexer_rest = GsiHelper(index_node, self.log)
            doc = {"indexer.plasma.backIndex.enablePageBloomFilter": True,
                   "indexer.settings.enable_corrupt_index_backup": True,
                   "indexer.settings.rebalance.redistribute_indexes": True}
            self.indexer_rest.set_index_settings_internal(doc)

        # Perform CRUD operations
        self.create_start = self.init_items_per_collection
        self.create_end = (.2 * self.init_items_per_collection) + self.init_items_per_collection
        self.delete_start = 0
        self.delete_end = (.2 * self.init_items_per_collection)
        self.update_start = (.2 * self.init_items_per_collection)
        self.update_end = (.4 * self.init_items_per_collection)

        end_refer = self.create_end

        self.generate_docs(doc_ops="create:update:delete")

        self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
        task = self.data_load()
        self.wait_for_doc_load_completion(task)

        indexDict = dict()
        self.index_count = self.input.param("index_count", 2)

        indexes_to_build, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                        replica=self.index_replicas, defer=True,
                                                                        number_of_indexes_per_coll=self.index_count,
                                                                        count=1,
                                                                        field='body', sync=False)

        self.indexUtil.build_deferred_indexes(self.cluster, indexes_to_build)
        self.assertTrue(self.polling_for_All_Indexer_to_Ready(indexes_to_build),
                        "polling for deferred indexes failed")
        th = list()
        for node in self.cluster.index_nodes:
            thread_name = node.ip + "_thread"
            t = threading.Thread(target=self.kill_indexer, name=thread_name,
                                 kwargs=dict(server=node,
                                             timeout=5))
            t.start()
            th.append(t)
        start = self.create_end
        self.items_add = self.input.param("items_add", 1000000)
        end = self.create_end + self.items_add
        self.monitor_stats = ["doc_ops", "ep_queue_size"]
        # load initial items in the bucket

        self.generate_docs(doc_ops="create", create_start=start, create_end=end)
        tasks_info = dict()
        self.time_out = self.input.param("time_out", 300)

        self.create_start = start
        self.create_end = end

        self.gen_delete = None
        self.gen_update = None
        self.generate_docs(doc_ops="create")

        self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
        data_load_task = self.data_load()

        for t in th:
            self.stop_killIndexer = True
            self.log.info("Stopping thread {}".format(t.name))
            t.join()
        # TO-DO
        # Remove hardcoded wait to polling wait
        self.log.info("Wait for indexer service to up")
        self.sleep(self.wait_timeout, "Waiting for indexer service to up")
        self.time_out = self.input.param("time_out", 10)

        self.wait_for_indexer_service_to_Active(self.indexer_rest, self.cluster.index_nodes, time_out=self.time_out)

        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexes_to_build, key='body')
        alter_index_task_info = list()
        count = len(indexDict)
        x = 0
        query_len = len(self.cluster.query_nodes)
        for bucket in self.cluster.buckets[-2:]:
            for scope_name in bucket.scopes.keys():
                for collection in bucket.scopes[scope_name].collections.keys():
                    gsi_index_names = indexes_to_build[bucket.name][scope_name][collection]
                    for gsi_index_name in gsi_index_names:
                        full_keyspace_name = "default:`" + bucket.name + "`.`" + scope_name + "`.`" + \
                                             collection + "`.`" + gsi_index_name + "`"
                        query_node_index = x % query_len
                        query = "ALTER INDEX %s WITH {\"action\": \"replica_count\", \"num_replica\": %s}" % (
                        full_keyspace_name, self.index_replicas + 1)
                        task = self.task.async_execute_query(self.cluster.query_nodes[query_node_index], query,
                                                             isIndexerQuery=False)
                        alter_index_task_info.append(task)
                        x += 1

        for taskInstance in alter_index_task_info:
            self.task.jython_task_manager.get_task_result(taskInstance)
        x = 0
        self.log.info("Creating index from last 2 buckets started")
        new_bucket_list = self.cluster.buckets[-2:]

        newlyAddedIndexes, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                              new_bucket_list,
                                                                                              replica=self.index_replicas,
                                                                                              defer=False,
                                                                                              number_of_indexes_per_coll=self.index_count,
                                                                                              count=self.index_count + 1,
                                                                                              field='body', sync=True)

        dropIndexTaskList, indexDict = self.indexUtil.async_drop_indexes(self.cluster, indexes_to_build,
                                                                         buckets=new_bucket_list)

        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)

        self.log.info("Starting rebalance in and rebalance out task")
        self.nodes_in = self.input.param("nodes_in", 2)
        count = len(self.dcp_services) + self.nodes_init
        nodes_in = self.cluster.servers[count:count + self.nodes_in]
        services = ["index", "n1ql"]

        self.retry_get_process_num = self.input.param("retry_get_process_num", 40)

        rebalance_in_task_result = self.task.rebalance([self.cluster.master],
                                                       nodes_in,
                                                       [],
                                                       services=services)
        indexer_nodes_list = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                           get_all_nodes=True)

        self.assertTrue(rebalance_in_task_result, "Rebalance in task failed")
        self.log.info("Rebalance in task completed, starting Rebalance-out task")
        self.num_failed_nodes = self.input.param("num_failed_nodes", 1)
        self.nodes_out = indexer_nodes_list[:self.num_failed_nodes]
        rebalance_out_task_result = self.task.rebalance([self.cluster.master],
                                                        [],
                                                        to_remove=self.nodes_out)
        self.assertTrue(rebalance_out_task_result, "Rebalance out task failed")

        self.cluster.query_nodes.extend(nodes_in)
        for node in self.nodes_out:
            if node in self.cluster.query_nodes:
                self.cluster.query_nodes.remove(node)

        query_nodes_list = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="n1ql",
                                                                         get_all_nodes=True)
        self.indexUtil.set_query_nodes_list(query_nodes_list)
        for taskInstance in query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)

        for taskInstance in dropIndexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)

        dropAllIndexTaskListOne, indexDict = self.indexUtil.async_drop_indexes(self.cluster, indexes_to_build)
        dropAllIndexTaskListTwo, indexDict = self.indexUtil.async_drop_indexes(self.cluster, newlyAddedIndexes)
        self.wait_for_doc_load_completion(data_load_task)

        for taskInstance in dropAllIndexTaskListOne:
            self.task.jython_task_manager.get_task_result(taskInstance)
        for taskInstance in dropAllIndexTaskListTwo:
            self.task.jython_task_manager.get_task_result(taskInstance)

        # Delete all buckets
        self.bucket_util.delete_all_buckets(self.cluster)

    def test_system_stability_with_indexer(self):
        self.log.info("Cluster ops test")
        stat_obj_list = self.create_Stats_Obj_list()
        self.index_count = self.input.param("index_count", 1)
        # Set indexer storage mode
        for index_node in self.cluster.index_nodes:
            self.indexer_rest = GsiHelper(index_node, self.log)
            doc = {"indexer.plasma.backIndex.enablePageBloomFilter": True,
                   "indexer.settings.enable_corrupt_index_backup": True,
                   "indexer.settings.rebalance.redistribute_indexes": True,
                   "indexer.plasma.backIndex.enableInMemoryCompression": True,
                   "indexer.plasma.mainIndex.enableInMemoryCompression": True}
            self.indexer_rest.set_index_settings_internal(doc)

        self.resident_ratio = \
            float(self.input.param("resident_ratio", .9))

        while not self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", self.resident_ratio, ops='lesser'):
            self.create_start = self.create_end
            self.create_end = self.create_start + self.items_add
            self.generate_docs(doc_ops="create")
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
            self.log.debug("Added items from {} to {}".format(self.create_start, self.create_end))
        self.init_items_per_collection = self.create_end
        total_items = self.create_end
        delete_start = 0
        delete_end = 0
        # Perform CRUD operations
        start = time.time()
        self.gen_create = None

        th = list()
        for node in self.cluster.index_nodes:
            thread_name = node.ip + "_thread"
            t = threading.Thread(target=self.kill_indexer, name=thread_name,
                                 kwargs=dict(server=node,
                                             timeout=500, kill_sleep_time=10))
            t.start()
            th.append(t)
        first_indexes_map, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                              replica=self.index_replicas,
                                                                                              defer=True,
                                                                                              number_of_indexes_per_coll=self.index_count,
                                                                                              field='body', sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)


        self.indexUtil.build_deferred_indexes(self.cluster, first_indexes_map)
        self.assertTrue(self.polling_for_All_Indexer_to_Ready(first_indexes_map),
                        "polling for deferred indexes failed")

        for t in th:
            self.stop_killIndexer = True
            self.log.info("Stopping thread {}".format(t.name))
            t.join()

        while time.time() - start < self.timer:
            self.create_start = total_items
            self.create_end = int((.2 * total_items) + total_items)
            total_items = self.create_end
            self.delete_start = delete_end
            self.delete_end = int(delete_end + (.2 * total_items))
            delete_end = self.delete_end
            self.update_start = int(delete_end + (.2 * total_items))
            self.update_end = int(delete_end + (.4 * total_items))
            self.log.debug(
                "self.create is {} self.create_end is {} self.delete_start is {} self.delete_end is {} self.update_start is {} self.update_end is {}".format(
                    self.create_start, self.create_end, self.delete_start, self.delete_end, self.update_start, self.update_end))
            end_refer = self.create_end
            self.generate_docs(doc_ops="create:update:delete")
            self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
            task = self.data_load()
            self.wait_for_doc_load_completion(task)
        total_items = int(self.create_end - self.delete_end)
        self.log.debug("Total items are {}".format(total_items))
        second_indexes_map, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                               replica=self.index_replicas,
                                                                                               defer=True,
                                                                                               number_of_indexes_per_coll=2 * self.index_count,
                                                                                               count=self.index_count,
                                                                                               field='body', sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.indexUtil.build_deferred_indexes(self.cluster, second_indexes_map)
        self.assertTrue(self.polling_for_All_Indexer_to_Ready(second_indexes_map, timeout=1800),
                        "polling for deferred indexes failed")

        self.log.info("Creating index from last 2 buckets started")
        new_bucket_list = self.cluster.buckets[-2:]

        third_indexes_map, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                              new_bucket_list,
                                                                                              replica=self.index_replicas,
                                                                                              defer=False,
                                                                                              number_of_indexes_per_coll=3 * self.index_count,
                                                                                              count=2 * self.index_count,
                                                                                              field='body', sync=True)

        dropIndexTaskList, indexDict = self.indexUtil.async_drop_indexes(self.cluster, first_indexes_map,
                                                                         buckets=new_bucket_list)

        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)

        self.log.info("Starting rebalance in and rebalance out task")
        self.nodes_in = self.input.param("nodes_in", 2)
        count = len(self.dcp_services) + self.nodes_init
        nodes_in = self.cluster.servers[count:count + self.nodes_in]
        services = ["index", "index"]

        self.retry_get_process_num = self.input.param("retry_get_process_num", 40)

        rebalance_in_task_result = self.task.rebalance([self.cluster.master],
                                                       nodes_in,
                                                       [],
                                                       services=services)
        indexer_nodes_list = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                           get_all_nodes=True)

        self.assertTrue(rebalance_in_task_result, "Rebalance in task failed")
        self.log.info("Rebalance in task completed, starting Rebalance-out task")
        self.num_failed_nodes = self.input.param("num_failed_nodes", 1)
        self.nodes_out = indexer_nodes_list[:self.num_failed_nodes]
        rebalance_out_task_result = self.task.rebalance([self.cluster.master],
                                                        [],
                                                        to_remove=self.nodes_out)
        self.assertTrue(rebalance_out_task_result, "Rebalance out task failed")

        for taskInstance in dropIndexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)

        self.cluster.index_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                         get_all_nodes=True)
        self.cluster.query_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="n1ql",
                                                                         get_all_nodes=True)
        first_query_tasks_info = self.indexUtil.run_full_scan(self.cluster, second_indexes_map, key='body',
                                                        totalCount=80000, limit=self.query_limit)
        second_query_tasks_info = self.indexUtil.run_full_scan(self.cluster, third_indexes_map, key='body',
                                                        totalCount=80000, limit=self.query_limit)
        alter_indexes_task_list = self.indexUtil.alter_indexes(self.cluster, second_indexes_map)
        for taskInstance in alter_indexes_task_list:
            self.task.jython_task_manager.get_task_result(taskInstance)

        for taskInstance in first_query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)
        for taskInstance in second_query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)


        second_dropAllIndexTaskList, indexDict = self.indexUtil.async_drop_indexes(self.cluster, second_indexes_map)
        third_dropAllIndexTaskList, indexDict = self.indexUtil.async_drop_indexes(self.cluster, third_indexes_map)

        for taskInstance in second_dropAllIndexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)

        for taskInstance in third_dropAllIndexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)

        # Delete all buckets
        self.bucket_util.delete_all_buckets(self.cluster)

    def test_system_stability_with_different_indexer(self):
        self.log.info("Cluster ops test")
        stat_obj_list = self.create_Stats_Obj_list()
        self.index_count = self.input.param("index_count", 1)
        # Set indexer storage mode
        for index_node in self.cluster.index_nodes:
            self.indexer_rest = GsiHelper(index_node, self.log)
            doc = {"indexer.plasma.backIndex.enablePageBloomFilter": True,
                   "indexer.settings.enable_corrupt_index_backup": True,
                   "indexer.settings.rebalance.redistribute_indexes": True,
                   "indexer.plasma.backIndex.enableInMemoryCompression": True,
                   "indexer.plasma.mainIndex.enableInMemoryCompression": True}
            self.indexer_rest.set_index_settings_internal(doc)
        self.resident_ratio = \
            float(self.input.param("resident_ratio", .9))
        first_indexes_map, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                              replica=self.index_replicas,
                                                                                              defer=True,
                                                                                              number_of_indexes_per_coll=self.index_count,
                                                                                              field='body', sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.indexUtil.build_deferred_indexes(self.cluster, first_indexes_map)
        self.assertTrue(self.polling_for_All_Indexer_to_Ready(first_indexes_map),
                        "polling for deferred indexes failed")
        self.isAvg = self.input.param("isAvg", False)
        total_items_added = self.load_item_till_dgm_reached(stat_obj_list, self.resident_ratio, self.create_end,
                                                            self.items_add, self.isAvg)
        self.init_items_per_collection = self.init_items_per_collection + total_items_added
        self.log.debug("Added item count is {} and total item count is {}".format(total_items_added,
                                                                                  self.init_items_per_collection))
        # Modifying half of the documents
        self.gen_create = None
        self.upsert_start = 0
        self.upsert_end = self.init_items_per_collection / 2
        self.generate_subDocs(sub_doc_ops="upsert")
        self.different_field = True
        data_load_task = self.data_load()
        self.log.debug("update from {} to {}".format(self.upsert_start, self.upsert_end))
        self.wait_for_doc_load_completion(data_load_task)
        second_indexes_map, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                               replica=self.index_replicas,
                                                                                               defer=True,
                                                                                               number_of_indexes_per_coll=2 * self.index_count,
                                                                                               count=self.index_count,
                                                                                               field='mod_body',
                                                                                               sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.indexUtil.build_deferred_indexes(self.cluster, second_indexes_map)
        self.assertTrue(self.polling_for_All_Indexer_to_Ready(second_indexes_map, timeout=1800),
                        "polling for deferred indexes failed")
        mem_comp_compare_task = self.validate_index_data(second_indexes_map, self.upsert_end, 'mod_body',
                                                         limit=self.query_limit)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.kill_sleep_time = self.input.param("kill_sleep_time", 100)
        th = list()
        for node in self.cluster.index_nodes:
            thread_name = node.ip + "_thread"
            t = threading.Thread(target=self.kill_indexer, name=thread_name,
                                 kwargs=dict(server=node,
                                             timeout=500, kill_sleep_time=100))
            t.start()
            th.append(t)
        # Perform CRUD operations
        start = time.time()
        self.gen_create = None
        self.create_end = self.init_items_per_collection
        start = time.time()
        self.gen_create = None
        delete_end = 0
        while time.time() - start < self.timer:
            self.create_start = self.init_items_per_collection
            self.create_end = int((.2 * self.init_items_per_collection) + self.init_items_per_collection)
            total_items = self.create_end
            self.delete_start = delete_end
            self.delete_end = int(delete_end + (.2 * self.init_items_per_collection))
            delete_end = self.delete_end
            self.update_start = int(delete_end + (.2 * self.init_items_per_collection))
            self.update_end = int(delete_end + (.4 * self.init_items_per_collection))
            self.log.debug(
                "self.create is {} self.create_end is {} self.delete_start is {} self.delete_end is {} "
                "self.update_start is {} self.update_end is {}".format(
                    self.create_start, self.create_end, self.delete_start, self.delete_end, self.update_start,
                    self.update_end))
            end_refer = self.create_end
            self.generate_docs(doc_ops="create:update:delete")
            task = self.data_load()
            self.wait_for_doc_load_completion(task)
        total_items = int(self.create_end - self.delete_end)
        self.check_negative_plasma_stats(stat_obj_list)
        self.log.debug("Total items are {}".format(total_items))
        for t in th:
            self.stop_killIndexer = True
            self.log.info("Stopping thread {}".format(t.name))
            t.join()
        second_indexes_map, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                               replica=self.index_replicas,
                                                                                               defer=True,
                                                                                               number_of_indexes_per_coll=2 * self.index_count,
                                                                                               count=self.index_count,
                                                                                               field='body', sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.indexUtil.build_deferred_indexes(self.cluster, second_indexes_map)
        self.assertTrue(self.polling_for_All_Indexer_to_Ready(second_indexes_map, timeout=1800),
                        "polling for deferred indexes failed")
        self.log.info("Creating index from last 2 buckets started")
        new_bucket_list = self.cluster.buckets[-2:]
        third_indexes_map, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                              new_bucket_list,
                                                                                              replica=self.index_replicas,
                                                                                              defer=False,
                                                                                              number_of_indexes_per_coll=3 * self.index_count,
                                                                                              count=2 * self.index_count,
                                                                                              field='body', sync=True)
        dropIndexTaskList, indexDict = self.indexUtil.async_drop_indexes(self.cluster, first_indexes_map,
                                                                         buckets=new_bucket_list)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        th = list()
        counter = 0
        for node in self.cluster.index_nodes:
            thread_name = node.ip + "_Rebalance_thread_" + str(counter)
            t = threading.Thread(target=self.kill_indexer, name=thread_name,
                                 kwargs=dict(server=node,
                                             timeout=500, kill_sleep_time=20))
            t.start()
            th.append(t)
            counter += 1
        self.log.info("Starting rebalance in and rebalance out task")
        self.nodes_in = self.input.param("nodes_in", 2)
        count = len(self.dcp_services) + self.nodes_init
        nodes_in = self.cluster.servers[count:count + self.nodes_in]
        services = ["index", "index"]
        self.retry_get_process_num = self.input.param("retry_get_process_num", 40)
        rebalance_in_task_result = self.task.rebalance([self.cluster.master],
                                                       nodes_in,
                                                       [],
                                                       services=services)
        indexer_nodes_list = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                           get_all_nodes=True)
        self.assertTrue(rebalance_in_task_result, "Rebalance in task failed")
        self.log.info("Rebalance in task completed, starting Rebalance-out task")
        self.num_failed_nodes = self.input.param("num_failed_nodes", 1)
        self.nodes_out = indexer_nodes_list[:self.num_failed_nodes]
        rebalance_out_task_result = self.task.rebalance([self.cluster.master],
                                                        [],
                                                        to_remove=self.nodes_out)
        self.assertTrue(rebalance_out_task_result, "Rebalance out task failed")
        self.check_negative_plasma_stats(stat_obj_list)
        for t in th:
            self.stop_killIndexer = True
            self.log.info("Stopping thread {}".format(t.name))
            t.join()
        for taskInstance in dropIndexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.cluster.index_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                                 get_all_nodes=True)
        self.cluster.query_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="n1ql",
                                                                                 get_all_nodes=True)
        first_query_tasks_info = self.indexUtil.run_full_scan(self.cluster, second_indexes_map, key='body',
                                                              totalCount=80000, limit=self.query_limit)
        second_query_tasks_info = self.indexUtil.run_full_scan(self.cluster, third_indexes_map, key='body',
                                                               totalCount=80000, limit=self.query_limit)
        alter_indexes_task_list = self.indexUtil.alter_indexes(self.cluster, second_indexes_map)
        for taskInstance in alter_indexes_task_list:
            self.task.jython_task_manager.get_task_result(taskInstance)
        for taskInstance in first_query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)
        for taskInstance in second_query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)
        second_dropAllIndexTaskList, indexDict = self.indexUtil.async_drop_indexes(self.cluster, second_indexes_map)
        third_dropAllIndexTaskList, indexDict = self.indexUtil.async_drop_indexes(self.cluster, third_indexes_map)
        for taskInstance in second_dropAllIndexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)
        for taskInstance in third_dropAllIndexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)
        # Delete all buckets
        self.bucket_util.delete_all_buckets(self.cluster)

    def test_system_stability_set_two(self):
        self.log.info("Cluster ops test")
        stat_obj_list = self.create_Stats_Obj_list()
        self.index_count = self.input.param("index_count", 1)
        # Set indexer storage mode
        for index_node in self.cluster.index_nodes:
            self.indexer_rest = GsiHelper(index_node, self.log)
            doc = {"indexer.plasma.backIndex.enablePageBloomFilter": True,
                   "indexer.settings.enable_corrupt_index_backup": True,
                   "indexer.settings.rebalance.redistribute_indexes": True,
                   "indexer.plasma.backIndex.enableInMemoryCompression": True,
                   "indexer.plasma.mainIndex.enableInMemoryCompression": True}
            self.indexer_rest.set_index_settings_internal(doc)

        self.resident_ratio = \
            float(self.input.param("resident_ratio", .9))

        first_indexes_map, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                              replica=self.index_replicas,
                                                                                              defer=True,
                                                                                              number_of_indexes_per_coll=self.index_count,
                                                                                              field='body', sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)

        self.indexUtil.build_deferred_indexes(self.cluster, first_indexes_map)
        self.assertTrue(self.polling_for_All_Indexer_to_Ready(first_indexes_map),
                        "polling for deferred indexes failed")
        th = list()
        while not self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", self.resident_ratio, ops='lesser'):
            self.create_start = self.create_end
            self.create_end = self.create_start + self.items_add
            self.generate_docs(doc_ops="create")
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
            self.log.debug("Added items from {} to {}".format(self.create_start, self.create_end))
        self.init_items_per_collection = self.create_end
        total_items = self.create_end
        delete_start = 0
        delete_end = 0
        # Perform CRUD operations
        start = time.time()
        self.gen_create = None

        for node in self.cluster.index_nodes:
            thread_name = node.ip + "_thread"
            t = threading.Thread(target=self.kill_indexer, name=thread_name,
                                 kwargs=dict(server=node,
                                             timeout=500))
            t.start()
            th.append(t)

        while time.time() - start < self.timer:
            self.create_start = total_items
            self.create_end = int((.2 * total_items) + total_items)
            total_items = self.create_end
            self.delete_start = delete_end
            self.delete_end = int(delete_end + (.2 * total_items))
            delete_end = self.delete_end
            self.update_start = int(delete_end + (.2 * total_items))
            self.update_end = int(delete_end + (.4 * total_items))
            self.log.debug(
                "self.create is {} self.create_end is {} self.delete_start is {} self.delete_end is {} self.update_start is {} self.update_end is {}".format(
                    self.create_start, self.create_end, self.delete_start, self.delete_end, self.update_start,
                    self.update_end))
            end_refer = self.create_end
            self.generate_docs(doc_ops="create:update:delete")
            self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
            task = self.data_load()
            self.wait_for_doc_load_completion(task)
        total_items = int(self.create_end - self.delete_end)
        self.check_negative_plasma_stats(stat_obj_list)
        self.log.debug("Total items are {}".format(total_items))
        second_indexes_map, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                               replica=self.index_replicas,
                                                                                               defer=True,
                                                                                               number_of_indexes_per_coll=2 * self.index_count,
                                                                                               count=self.index_count,
                                                                                               field='body', sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.indexUtil.build_deferred_indexes(self.cluster, second_indexes_map)
        self.assertTrue(self.polling_for_All_Indexer_to_Ready(second_indexes_map, timeout=600, sleep_time=30),
                        "polling for deferred indexes failed")

        self.log.info("Creating index from last 2 buckets started")
        new_bucket_list = self.cluster.buckets[-2:]

        third_indexes_map, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                              new_bucket_list,
                                                                                              replica=self.index_replicas,
                                                                                              defer=False,
                                                                                              number_of_indexes_per_coll=3 * self.index_count,
                                                                                              count=2 * self.index_count,
                                                                                              field='body', sync=True)

        dropIndexTaskList, indexDict = self.indexUtil.async_drop_indexes(self.cluster, first_indexes_map,
                                                                         buckets=new_bucket_list)

        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.log.info("Starting rebalance in and rebalance out task")
        self.nodes_in = self.input.param("nodes_in", 2)
        count = len(self.dcp_services) + self.nodes_init
        nodes_in = self.cluster.servers[count:count + self.nodes_in]
        services = ["index", "index"]

        self.retry_get_process_num = self.input.param("retry_get_process_num", 40)

        rebalance_in_task_result = self.task.rebalance([self.cluster.master],
                                                       nodes_in,
                                                       [],
                                                       services=services)
        indexer_nodes_list = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                           get_all_nodes=True)

        self.assertTrue(rebalance_in_task_result, "Rebalance in task failed")
        self.log.info("Rebalance in task completed, starting Rebalance-out task")
        self.num_failed_nodes = self.input.param("num_failed_nodes", 1)
        self.nodes_out = indexer_nodes_list[:self.num_failed_nodes]
        rebalance_out_task_result = self.task.rebalance([self.cluster.master],
                                                        [],
                                                        to_remove=self.nodes_out)
        self.assertTrue(rebalance_out_task_result, "Rebalance out task failed")

        for taskInstance in dropIndexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.check_negative_plasma_stats(stat_obj_list)
        for t in th:
            self.stop_killIndexer = True
            self.log.info("Stopping thread {}".format(t.name))
            t.join()

        self.cluster.index_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                         get_all_nodes=True)
        self.cluster.query_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="n1ql",
                                                                         get_all_nodes=True)
        first_query_tasks_info = self.indexUtil.run_full_scan(self.cluster, second_indexes_map, key='body',
                                                        totalCount=80000, limit=self.query_limit)
        second_query_tasks_info = self.indexUtil.run_full_scan(self.cluster, third_indexes_map, key='body',
                                                        totalCount=80000, limit=self.query_limit)
        alter_indexes_task_list = self.indexUtil.alter_indexes(self.cluster, second_indexes_map)
        for taskInstance in alter_indexes_task_list:
            self.task.jython_task_manager.get_task_result(taskInstance)

        for taskInstance in first_query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)
        for taskInstance in second_query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)


        second_dropAllIndexTaskList, indexDict = self.indexUtil.async_drop_indexes(self.cluster, second_indexes_map)
        third_dropAllIndexTaskList, indexDict = self.indexUtil.async_drop_indexes(self.cluster, third_indexes_map)

        for taskInstance in second_dropAllIndexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)

        for taskInstance in third_dropAllIndexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)

        # Delete all buckets
        self.bucket_util.delete_all_buckets(self.cluster)

    def test_system_stability_set_three(self):
        self.log.info("Cluster ops system test")
        stat_obj_list = self.create_Stats_Obj_list()
        self.index_count = self.input.param("index_count", 1)
        # Set indexer storage mode
        for index_node in self.cluster.index_nodes:
            self.indexer_rest = GsiHelper(index_node, self.log)
            doc = {"indexer.plasma.backIndex.enablePageBloomFilter": True,
                   "indexer.settings.enable_corrupt_index_backup": True,
                   "indexer.settings.rebalance.redistribute_indexes": True,
                   "indexer.plasma.backIndex.enableInMemoryCompression": True,
                   "indexer.plasma.mainIndex.enableInMemoryCompression": True}
            self.indexer_rest.set_index_settings_internal(doc)

        self.resident_ratio = \
            float(self.input.param("resident_ratio", .9))

        first_indexes_map, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                              replica=self.index_replicas,
                                                                                              defer=True,
                                                                                              number_of_indexes_per_coll=self.index_count,
                                                                                              field='body', sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.indexUtil.build_deferred_indexes(self.cluster, first_indexes_map)
        self.assertTrue(self.polling_for_All_Indexer_to_Ready(first_indexes_map),
                        "polling for deferred indexes failed")
        self.isAvg = self.input.param("isAvg", False)
        total_items_added = self.load_item_till_dgm_reached(stat_obj_list, self.resident_ratio, self.create_end,
                                                            self.items_add, self.isAvg)
        self.init_items_per_collection = self.init_items_per_collection + total_items_added
        self.log.debug("Added item count is {} and total item count is {}".format(total_items_added,
                                                                                  self.init_items_per_collection))
        th = list()
        self.kill_count = self.input.param("kill_count", 200)
        for node in self.cluster.index_nodes:
            thread_name = node.ip + "_thread"
            t = threading.Thread(target=self.kill_indexer, name=thread_name,
                                 kwargs=dict(server=node,
                                             timeout=self.kill_count))
            t.start()
            th.append(t)

        start = time.time()
        self.gen_create = None
        delete_end = 0
        while time.time() - start < self.timer:
            self.create_start = self.init_items_per_collection
            self.create_end = int((.2 * self.init_items_per_collection) + self.init_items_per_collection)
            total_items = self.create_end
            self.delete_start = delete_end
            self.delete_end = int(delete_end + (.2 * self.init_items_per_collection))
            delete_end = self.delete_end
            self.update_start = int(delete_end + (.2 * self.init_items_per_collection))
            self.update_end = int(delete_end + (.4 * self.init_items_per_collection))
            self.log.debug(
                "self.create is {} self.create_end is {} self.delete_start is {} self.delete_end is {} "
                "self.update_start is {} self.update_end is {}".format(
                    self.create_start, self.create_end, self.delete_start, self.delete_end, self.update_start,
                    self.update_end))
            end_refer = self.create_end
            self.generate_docs(doc_ops="create:update:delete")
            task = self.data_load()
            self.wait_for_doc_load_completion(task)

        for t in th:
            self.stop_killIndexer = True
            self.log.info("Stopping thread {}".format(t.name))
            t.join()
        total_items = int(self.create_end - self.delete_end)
        self.recreate_count = self.input.param("recreate_count", 100)
        for x in range(self.recreate_count):
            first_dropAllIndexTaskList, first_indexes_map = self.indexUtil.async_drop_indexes(self.cluster,
                                                                                              first_indexes_map)
            for taskInstance in first_dropAllIndexTaskList:
                self.task.jython_task_manager.get_task_result(taskInstance)
            self.indexUtil.recreate_dropped_indexes(self.cluster, first_indexes_map, field='body', defer=False)
            self.assertTrue(
                self.verify_bucket_count_with_index_count(first_indexes_map, totalCount=total_items, field='body'))

        mem_comp_compare_task = self.validate_index_data(first_indexes_map, self.init_items_per_collection, 'body',
                                                         limit=self.query_limit)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

    def test_system_stability_with_ttl(self):
        self.log.info("Cluster ops system test")
        stat_obj_list = self.create_Stats_Obj_list()
        self.index_count = self.input.param("index_count", 1)
        # Set indexer storage mode
        for index_node in self.cluster.index_nodes:
            self.indexer_rest = GsiHelper(index_node, self.log)
            doc = {"indexer.plasma.backIndex.enablePageBloomFilter": True,
                   "indexer.settings.enable_corrupt_index_backup": True,
                   "indexer.settings.rebalance.redistribute_indexes": True,
                   "indexer.plasma.backIndex.enableInMemoryCompression": True,
                   "indexer.plasma.mainIndex.enableInMemoryCompression": True}
            self.indexer_rest.set_index_settings_internal(doc)

        self.resident_ratio = \
            float(self.input.param("resident_ratio", .9))

        field = 'META().expiration'
        stat_obj_list = self.create_Stats_Obj_list()
        self.timer = self.input.param("timer", 600)
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False,
                                                                                     timeout=self.wait_timeout)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        start = time.time()
        self.gen_create = None
        delete_end = 0
        self.expiry_end = self.create_end
        initial_count = self.create_end - self.create_start
        while (time.time() - start) < self.timer:
            self.log.debug("Adding items")
            self.gen_expiry = None
            self.create_start = self.expiry_end
            self.create_end = self.create_start + self.items_add
            self.generate_docs(doc_ops="create")
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)

            self.log.debug("Marking items as expired")
            self.maxttl = self.input.param("maxttl", 1000)
            self.gen_create = None
            self.expiry_start = self.create_start
            self.maxttl = 1000
            self.expiry_end = self.create_end
            self.generate_docs(doc_ops="expiry")
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
            self.log.info("TTL experiment done")
            self.wait_for_mutuations_to_settle_down(stat_obj_list)
            data_dict = self.get_plasma_index_stat_value("lss_data_size", stat_obj_list)
            disk_dict = self.get_plasma_index_stat_value("lss_disk_size", stat_obj_list)
            used_space_dict = self.get_plasma_index_stat_value("lss_used_space", stat_obj_list)
            self.sleep(1000, "Waiting for items to delete")
            timeout = 1000
            start_time = time.time()
            stop_time = start_time + timeout

            size_counter = 0
            while not self.verify_bucket_count_with_index_count(indexMap, initial_count, field) and size_counter < 100:
                self.sleep(100, "wait for items to get delete")
                size_counter += 1
            query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexMap, key=field,
                                                            totalCount=self.expiry_end,
                                                            limit=self.query_limit)
            for taskInstance in query_tasks_info:
                self.task.jython_task_manager.get_task_result(taskInstance)
            self.assertTrue(self.compare_plasma_stat_field_value(stat_obj_list, "lss_data_size", data_dict,
                                                                 ops='lesser',timeout=120), "data size is not going down")
            self.assertTrue(self.compare_plasma_stat_field_value(stat_obj_list, "lss_disk_size", disk_dict,
                                                                 ops='lesser',timeout=120), "disk size is not coming down")
            self.assertTrue(self.compare_plasma_stat_field_value(stat_obj_list, "lss_used_space", used_space_dict,
                                                                 ops='lesser',timeout=120), "used space is not coming down")

    def test_system_stability_with_ttl_dgm(self):
        self.log.info("Cluster ops system test")
        stat_obj_list = self.create_Stats_Obj_list()
        self.index_count = self.input.param("index_count", 1)
        # Set indexer storage mode
        for index_node in self.cluster.index_nodes:
            self.indexer_rest = GsiHelper(index_node, self.log)
            doc = {"indexer.plasma.backIndex.enablePageBloomFilter": True,
                   "indexer.settings.enable_corrupt_index_backup": True,
                   "indexer.settings.rebalance.redistribute_indexes": True,
                   "indexer.plasma.backIndex.enableInMemoryCompression": True,
                   "indexer.plasma.mainIndex.enableInMemoryCompression": True}
            self.indexer_rest.set_index_settings_internal(doc)

        self.resident_ratio = \
            float(self.input.param("resident_ratio", .9))

        field = 'body'
        stat_obj_list = self.create_Stats_Obj_list()
        self.timer = self.input.param("timer", 600)
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False,
                                                                                     timeout=self.wait_timeout)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        start = time.time()
        self.gen_create = None
        delete_end = 0
        self.expiry_end = self.create_end

        initial_count = self.create_end - self.create_start
        self.gen_create = None
        self.maxttl = self.input.param("maxttl", 1000)
        while (time.time() - start) < self.timer:
            while not self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", self.resident_ratio,
                                                            ops='equalOrLessThan', check_single_collection=True):
                self.expiry_start = self.expiry_end
                self.expiry_end = self.expiry_start + self.items_add
                self.generate_docs(doc_ops="expiry")
                data_load_task = self.data_load()
                self.wait_for_doc_load_completion(data_load_task)
                self.log.debug("Added items from {} to {}".format(self.expiry_start, self.expiry_end))
            self.log.info("TTL experiment done")
            self.wait_for_mutuations_to_settle_down(stat_obj_list)
            data_dict = self.get_plasma_index_stat_value("lss_data_size", stat_obj_list)
            disk_dict = self.get_plasma_index_stat_value("lss_disk_size", stat_obj_list)
            used_space_dict = self.get_plasma_index_stat_value("lss_used_space", stat_obj_list)
            self.sleep(self.maxttl, "Waiting for items to delete")
            size_counter = 0
            while not self.verify_bucket_count_with_index_count(indexMap, initial_count, field) and size_counter < 100:
                self.sleep(100, "wait for items to get delete")
                size_counter += 1
            query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexMap, key='body',
                                                            totalCount=self.expiry_end,
                                                            limit=self.query_limit)
            for taskInstance in query_tasks_info:
                self.task.jython_task_manager.get_task_result(taskInstance)
            self.compare_plasma_stat_field_value(stat_obj_list, "lss_data_size", data_dict,
                                                 ops='lesser', timeout=120)
            self.compare_plasma_stat_field_value(stat_obj_list, "lss_disk_size", disk_dict,
                                                 ops='lesser', timeout=120)
            self.compare_plasma_stat_field_value(stat_obj_list, "lss_used_space", used_space_dict,
                                                 ops='lesser', timeout=120)
