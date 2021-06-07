from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from gsiLib.gsiHelper import GsiHelper
from plasma.plasma_base import PlasmaBaseTest


class PlasmaStatsTest(PlasmaBaseTest):
    def setUp(self):
        super(PlasmaStatsTest, self).setUp()

    def tearDown(self):
        super(PlasmaStatsTest, self).tearDown()

    def test_item_count_stats(self):

        self.log.info("Starting test")
        i = 0
        indexDict = dict()
        self.index_count = self.input.param("index_count", 11)
        for i in range(self.index_count):
            indexName = "Index0" + str(i)
            index_query = "CREATE INDEX `%s` ON `%s`(`body`)" % (indexName,
                                                                 self.buckets[0].name)
            self.query_client = RestConnection(self.cluster.master)
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

        result = indexer_rest.index_status()

        # Validating the Definition associated with the indexes
        self.log.info("Comparing the actual index list with expected one")
        for key in indexDict:
            self.assertEqual(result[self.buckets[0].name][key]['definition'], indexDict[key],
                             "Index queries is:{} and definition is :{}".format(
                                 result[self.buckets[0].name][key]['definition'], indexDict[key]))
            self.assertEqual(result[self.buckets[0].name][key]['hosts'],
                             (ipIndexDict[key]), "Actual hosts values is: {} and expected host value is: {}".format(
                    result[self.buckets[0].name][key]['hosts'], ipIndexDict[key]))
