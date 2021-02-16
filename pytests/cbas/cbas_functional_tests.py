from BucketLib.bucket import TravelSample
from cbas_base import CBASBaseTest
from TestInput import TestInputSingleton


class CBASFunctionalTests(CBASBaseTest):
    def setUp(self, add_default_cbas_node=True):
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket": False})
            
        if "set_cbas_memory_from_available_free_memory" not in \
                self.input.test_params:
            self.input.test_params.update(
                {"set_cbas_memory_from_available_free_memory": True})
        super(CBASFunctionalTests, self).setUp(add_default_cbas_node)
        sample_bucket = TravelSample()

        self.validate_error = False
        if self.expected_error:
            self.validate_error = True

        ''' Considering all the scenarios where:
        1. There can be 1 KV and multiple cbas nodes
           (and tests wants to add all cbas into cluster.)
        2. There can be 1 KV and multiple cbas nodes
           (and tests wants only 1 cbas node)
        3. There can be only 1 node running KV,CBAS service.
        NOTE: Cases pending where there are nodes which are running only cbas.
              For that service check on nodes is needed.
        '''
        result = self.bucket_util.load_sample_bucket(sample_bucket)
        self.assertTrue(result, msg="Failed to load '%s'" % sample_bucket)

    def test_create_dataset_on_bucket(self):
        # Create dataset on the CBAS bucket
        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name,
            validate_error_msg=self.validate_error,
            expected_error=self.expected_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_create_another_dataset_on_bucket(self):
        # Create first dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)

        # Create another dataset on the CBAS bucket
        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset2_name,
            validate_error_msg=self.validate_error,
            expected_error=self.expected_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_connect_bucket(self):
        if self.cb_bucket_password:
            self.cb_server_ip = self.cluster.master.ip

        # Create dataset on the CBAS bucket
        if not self.skip_create_dataset:
            self.cbas_util.create_dataset_on_bucket(
                cbas_bucket_name=self.cb_bucket_name,
                cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        result = self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name_invalid,
            cb_bucket_password=self.cb_bucket_password,
            validate_error_msg=self.validate_error,
            expected_error=self.expected_error)
        if self.otpNodes:
            self.remove_node(otpnode=self.otpNodes)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_connect_bucket_on_a_connected_bucket(self):
        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password)

        # Create another connection to bucket
        result = self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password,
            validate_error_msg=self.validate_error,
            expected_error=self.expected_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_disconnect_bucket(self):
        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password)

        # Disconnect from bucket
        result = self.cbas_util.disconnect_from_bucket(
            cbas_bucket_name=self.cbas_bucket_name_invalid,
            disconnect_if_connected=self.disconnect_if_connected,
            validate_error_msg=self.validate_error,
            expected_error=self.expected_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_disconnect_bucket_already_disconnected(self):
        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password)

        # Disconnect from Bucket
        self.cbas_util.disconnect_from_bucket(
            cbas_bucket_name=self.cbas_bucket_name)

        # Disconnect again from the same bucket
        result = self.cbas_util.disconnect_from_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            disconnect_if_connected=self.disconnect_if_connected,
            validate_error_msg=self.validate_error,
            expected_error=self.expected_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_drop_dataset_on_bucket(self):
        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password)

        # Drop Connection
        if not self.skip_drop_connection:
            self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        # Drop dataset
        result = self.cbas_util.drop_dataset(
            cbas_dataset_name=self.cbas_dataset_name_invalid,
            validate_error_msg=self.validate_error,
            expected_error=self.expected_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_drop_cbas_bucket(self):
        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password)

        # Drop connection and dataset
        if not self.skip_drop_connection:
            self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        if not self.skip_drop_dataset:
            self.cbas_util.drop_dataset(self.cbas_dataset_name)

        result = self.cbas_util.drop_cbas_bucket(
            cbas_bucket_name=self.cbas_bucket_name_invalid,
            validate_error_msg=self.validate_error,
            expected_error=self.expected_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_or_predicate_evaluation(self):
        ds_name = "filtered_ds"
        predicates = self.input.param("predicates", None) \
            .replace("&eq", "=").replace("&qt", "\"")
        self.log.info("predicates = %s", predicates)

        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)

        # Create a dataset on the CBAS bucket with filters
        statement_create_dataset = "create dataset {0} on `{1}`" \
                                   .format(ds_name, self.cb_bucket_name)
        if predicates:
            statement_create_dataset += " where {0}" \
                                        .format(predicates) + ";"
        else:
            statement_create_dataset += ";"

        status, metrics, errors, results, _ = \
            self.cbas_util.execute_statement_on_cbas_util(
                statement_create_dataset)

        self.log.info("Executing statement on CBAS: %s",
                      statement_create_dataset)

        if errors:
            self.log.info("Statement = %s", statement_create_dataset)
            self.log.info(errors)
            self.fail("Error while creating Dataset with OR predicates")

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password)

        # Allow ingestion to complete
        self.sleep(60)

        stmt_count_records_on_full_ds = "select count(*) from {0}" \
                                        .format(self.cbas_dataset_name)
        if predicates:
            stmt_count_records_on_full_ds += " where {0}" \
                                             .format(predicates) + ";"
        else:
            stmt_count_records_on_full_ds += ";"

        stmt_count_records_on_filtered_ds = "select count(*) from {0};" \
                                            .format(ds_name)

        # Run query on full dataset with filters applied in the query
        status, metrics, errors, results, _ = \
            self.cbas_util.execute_statement_on_cbas_util(
                stmt_count_records_on_full_ds)
        count_full_ds = results[0]["$1"]
        self.log.info("*** Count of records in full dataset = %s",
                      count_full_ds)

        # Run query on the filtered dataset to retrieve all records
        status, metrics, errors, results, _ = \
            self.cbas_util.execute_statement_on_cbas_util(
                stmt_count_records_on_filtered_ds)
        count_filtered_ds = results[0]["$1"]
        self.log.info("*** Count of records in filtered dataset ds1 = %s",
                      count_filtered_ds)
        # Validate if the count of records in both cases is the same
        if count_filtered_ds != count_full_ds:
            self.fail("Count not matching for full dataset with filters in "
                      "query vs. filtered dataset")
