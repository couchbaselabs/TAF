from Cb_constants import DocLoading
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from gsiLib.gsiHelper import GsiHelper

from storage.plasma.plasma_base import PlasmaBaseTest
import array as arr


class PlasmaStatsTest(PlasmaBaseTest):
    def setUp(self):
        super(PlasmaStatsTest, self).setUp()

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

        self.num_replicas = self.input.param("num_replicas", 1)
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
                                                                                           self.num_replicas)
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
                                                                                     self.num_replicas)
            task = self.task.aysnc_execute_query(server=query_nodes_list[0], query=query, bucket=self.buckets[0],
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
            self.log.info("Index for query node is:"+str(query_node_index))
            queryString = self.randStr(Num=8)
            query = "select * from `%s` data USE INDEX (%s USING GSI) where body like '%%%s%%' limit 10" % (
                self.buckets[0].name, indexDict.keys()[index_instance], queryString)
            task = self.task.aysnc_execute_query(query_nodes_list[query_node_index], query, contentType, connection)
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
        self.num_replicas = self.input.param("num_replicas", 1)
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
                                                                                           self.num_replicas)
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
        self.num_replicas = self.input.param("num_replicas", 1)
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
