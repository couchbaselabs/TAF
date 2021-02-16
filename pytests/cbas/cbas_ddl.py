import json
import time

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import TravelSample, BeerSample
from cbas_base import CBASBaseTest
from TestInput import TestInputSingleton


class CBASDDLTests(CBASBaseTest):
    def setUp(self, add_default_cbas_node=True):
        self.input = TestInputSingleton.input

        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket": False})
        
        if "set_cbas_memory_from_available_free_memory" not in \
                self.input.test_params:
            self.input.test_params.update(
                {"set_cbas_memory_from_available_free_memory": True})
        
        super(CBASDDLTests, self).setUp(add_default_cbas_node)

        self.validate_error = False
        if self.expected_error:
            self.validate_error = True

        ''' Considering all the scenarios where:
        1. There can be 1 KV and multiple cbas nodes
           (and tests wants to add all cbas into cluster.)
        2. There can be 1 KV and multiple cbas nodes
           (and tests wants only 1 cbas node)
        3. There can be only 1 node running KV, CBAS service.
        NOTE: Cases pending where there are nodes which are running only cbas.
              For that service check on nodes is needed.
        '''
        self.sample_bucket = TravelSample()
        result = self.bucket_util.load_sample_bucket(self.sample_bucket)
        self.assertTrue(result, msg="Load sample bucket failed")

    def test_create_link_Local(self):
        code = self.input.param('error_code', None)
        result = self.cbas_util.create_link_on_cbas(
            link_name="Local",
            validate_error_msg=self.validate_error,
            expected_error=self.expected_error,
            expected_error_code=code)
        self.assertTrue(result, "another local link creation succeeded.")

    def test_drop_link_Local(self):
        code = self.input.param('error_code', None)
        result = self.cbas_util.drop_link_on_cbas(
            link_name="Local",
            validate_error_msg=self.validate_error,
            expected_error=self.expected_error,
            expected_error_code=code)
        self.assertTrue(result, "another local link creation succeeded.")

    def test_create_dataverse_Default(self):
        code = self.input.param('error_code', None)
        result = self.cbas_util.create_dataverse_on_cbas(
            dataverse_name="Default",
            validate_error_msg=self.validate_error,
            expected_error=self.expected_error,
            expected_error_code=code)
        self.assertTrue(result, "another local link creation succeeded.")

    def test_drop_dataverse_Default(self):
        code = self.input.param('error_code', None)
        result = self.cbas_util.drop_dataverse_on_cbas(
            dataverse_name="Default",
            validate_error_msg=self.validate_error,
            expected_error=self.expected_error,
            expected_error_code=code)
        self.assertTrue(result, "another local link creation succeeded.")

    def test_create_multiple_dataverse(self):
        # Create dataset on the CBAS bucket
        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)
        self.assertTrue(result, "another local link creation succeeded.")

        number_of_dataverse = self.input.param("number_of_dataverse", 2)

        for num in xrange(1, number_of_dataverse + 1):
            dataverse_name = "Default" + str(num)
            cbas_dataset_name = self.cbas_dataset_name+str(num)

            result = self.cbas_util.create_dataverse_on_cbas(dataverse_name)
            self.assertTrue(result, "Dataverse Creation Failed.")

            cmd_use_dataset = "use %s;" % dataverse_name
            status, metrics, errors, results, _ = \
                self.cbas_util.execute_statement_on_cbas_util(cmd_use_dataset)

            result = self.cbas_util.create_dataset_on_bucket(
                cbas_bucket_name=self.cb_bucket_name,
                cbas_dataset_name=cbas_dataset_name)
            self.assertTrue(result, "Data_set creation failed")

        self.cbas_util.connect_link()

        for num in xrange(1, number_of_dataverse + 1):
            dataverse_name = "Default" + str(num)
            cbas_dataset_name = self.cbas_dataset_name+str(num)

            cmd_use_dataset = "use %s;" % dataverse_name
            status, metrics, errors, results, _ = \
                self.cbas_util.execute_statement_on_cbas_util(cmd_use_dataset)
            self.assertTrue(
                self.cbas_util.validate_cbas_dataset_items_count(
                    cbas_dataset_name,
                    self.sample_bucket.scopes["_default"].collections["_default"].num_items),
                "Data loss in CBAS.")

    def test_connect_link_dataverse_Local(self):
        # Create dataset on the CBAS bucket
        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)
        self.assertTrue(result, "another local link creation succeeded.")

        number_of_dataverse = self.input.param("number_of_dataverse", 2)

        for num in xrange(1, number_of_dataverse + 1):
            dataverse_name = "Default" + str(num)
            cbas_dataset_name = self.cbas_dataset_name+str(num)

            result = self.cbas_util.create_dataverse_on_cbas(dataverse_name)
            self.assertTrue(result, "Dataverse Creation Failed.")

            cmd_use_dataset = "use %s;" % dataverse_name
            cmd_create_dataset = 'create dataset %s on `%s`;' \
                                 % (cbas_dataset_name,
                                    self.sample_bucket.name)
            cmd = cmd_use_dataset+cmd_create_dataset
            status, metrics, errors, results, _ = \
                self.cbas_util.execute_statement_on_cbas_util(cmd)

            self.cbas_util.connect_link(link_name=dataverse_name+".Local")
            self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(
                                dataverse_name+"." + cbas_dataset_name,
                                self.sample_bucket.scopes["_default"].collections["_default"].num_items),
                            "Data loss in CBAS.")

    def test_connect_link_delete_bucket(self):
        # Create dataset on the CBAS bucket
        number_of_datasets = self.input.param("number_of_datasets", 2)

        for num in xrange(number_of_datasets):
            cbas_dataset_name = self.cbas_dataset_name+str(num)

            result = self.cbas_util.create_dataset_on_bucket(
                cbas_bucket_name=self.cb_bucket_name,
                cbas_dataset_name=cbas_dataset_name)
            self.assertTrue(result, "Dataset Creation Failed.")

        self.cbas_util.connect_link()
        self.sleep(30, "Wait for 30 secs after connect link.")

        deleted = BucketHelper(self.cluster.master).delete_bucket(
            self.sample_bucket.name)
        self.assertTrue(deleted, "Failed to delete KV bucket")
        self.sleep(5, "Wait for 5 secs after deleting bucket.")

        self.log.info("Request sent will now either succeed or fail, "
                      "or its connection will be abruptly closed. "
                      "Verify the state")
        start_time = time.time()

        while start_time+120 > time.time():
            status, content, _ = self.cbas_util.fetch_bucket_state_on_cbas()
            self.assertTrue(status, msg="Fetch bucket state failed")
            content = json.loads(content)
            self.log.info(content)
            if content['buckets'][0]['state'] == "disconnected":
                break

        for num in xrange(number_of_datasets):
            cbas_dataset_name = self.cbas_dataset_name+str(num)
            self.assertTrue(self.cbas_util.drop_dataset(cbas_dataset_name),
                            "Drop dataset after deleting KV bucket failed "
                            "without disconnecting link.")

    def test_drop_one_bucket(self):
        beer_sample = BeerSample()
        result = self.bucket_util.load_sample_bucket(beer_sample)
        self.assertTrue(result, "Bucket Creation Failed.")

        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.sample_bucket.name,
            cbas_dataset_name="ds1")
        self.assertTrue(result, "Dataset Creation Failed.")

        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=beer_sample.name,
            cbas_dataset_name="ds2")
        self.assertTrue(result, "Dataset Creation Failed.")

        self.cbas_util.connect_link()
        deleted = BucketHelper(self.cluster.master).delete_bucket(
            beer_sample.name)
        self.assertTrue(deleted, "Failed to delete KV bucket")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(
                            "ds1",
                            self.sample_bucket.scopes["_default"].collections["_default"].num_items),
                        "Data loss in CBAS.")

    def test_create_dataset_on_connected_link(self):
        beer_sample = BeerSample()
        result = self.bucket_util.load_sample_bucket(beer_sample)
        self.assertTrue(result, "Bucket Creation Failed.")

        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.sample_bucket.name,
            cbas_dataset_name="ds2")
        self.assertTrue(result, "Dataset Creation Failed.")

        self.cbas_util.connect_link()

        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=beer_sample.name,
            cbas_dataset_name="ds1")
        self.assertTrue(result, "Dataset Creation Failed.")

        self.cbas_util.connect_link()
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(
                            "ds1",
                            beer_sample.stats.expected_item_count),
                        "Data loss in CBAS.")
