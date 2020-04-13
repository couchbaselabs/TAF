import json

from cbas.cbas_base import CBASBaseTest
from sdk_client3 import SDKClient
import uuid


class CbasStats(CBASBaseTest):
    """
    Class contains test cases for CBAS stats. More stats to be added in Alice scope.
    https://docs.google.com/document/d/1AFlgeSdDvMYj5jK0cgN83vPtmSlVSbbIgNQrGwYtkVg/edit#heading=h.c6qof4s8tomn
    """

    def setUp(self):
        super(CbasStats, self).setUp()

        self.log.info("Add Json documents to default bucket")
        self.perform_doc_ops_in_all_cb_buckets("create", 0, self.num_items)

        self.log.info("Create reference to SDK client")
        client = SDKClient(hosts=[self.master.ip], bucket=self.cb_bucket_name,
                           password=self.master.rest_password)

        self.log.info("Insert binary data into default bucket")
        keys = ["%s" % (uuid.uuid4()) for i in range(0, self.num_items)]
        client.insert_binary_document(keys)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Create dataset on the CBAS bucket")
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cb_bucket_name,
                                                cbas_dataset_name=self.cbas_dataset_name)

        self.log.info("Connect to Bucket")
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                         cb_bucket_password=self.cb_bucket_password)
        
        statement = "select count(*) from {0} where mutated=0;".format(self.cbas_dataset_name)
        status, metrics, errors, results, response_handle = self.cbas_util.execute_statement_on_cbas_util(
            statement, mode=self.mode, timeout=75, analytics_timeout=120)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    def assert_cbas_stats(self, stats_json, failed_count, incoming_count):
        self.assertTrue(stats_json['gc_count'] > 0, msg='gc_count value must be greater than 0')
        self.assertTrue(stats_json['gc_time'] > 0, msg='gc_time value must be greater than 0')
        self.assertTrue(stats_json['thread_count'] > 0, msg='thread_count value must be greater than 0')
        self.assertTrue(stats_json['heap_used'] < (1024 * (10 ** 6)),
                        msg='heap_used value must be less than configured memory value for CBAS')

        self.assertEqual(stats_json[self.cb_bucket_name + ':all:failed_at_parser_records_count'], failed_count,
                         msg="Incorrect failed at parser record count")
        self.assertEqual(stats_json[self.cb_bucket_name + ':all:failed_at_parser_records_count_total'], failed_count,
                         msg="Incorrect failed at parser records count total")
        self.assertEqual(stats_json[self.cb_bucket_name + ':all:incoming_records_count'], incoming_count,
                         msg="Incorrect incoming record count")
        self.assertEqual(stats_json[self.cb_bucket_name + ':all:incoming_records_count_total'], incoming_count,
                         msg="Incorrect incoming record count total")

    '''
    cbas.cbas_stats.CbasStats.fetch_stats_on_cbas_node_before_and_after_flush,cbas_bucket_name=cbas,cbas_dataset_name=ds,items=10,default_bucket=True,cb_bucket_name=default
    '''

    def fetch_stats_on_cbas_node_before_and_after_flush(self):

        """
        1. Add a default CB bucket
        2. Add N Json documents and N Non-Json documents
        3. Create CBAS bucket and Dataset
        4. Verify the count on CBAS and assert on stats - Incoming records = Failed + Mutated(Insert/Update/Deleted)
        5. Flush the CB bucket and verify the count for Incoming and Failed count is reset to zero
        """
        self.log.info("Fetch cbas stats before flush")
        status, content, response = self.cbas_util.fetch_cbas_stats()
        self.assertTrue(status, "Error invoking fetch cbas stats api")

        self.log.info("Assert Analytics stats before flush")
        stats_json = json.loads(content)
        self.log.info(stats_json)
        self.assert_cbas_stats(stats_json, self.num_items, self.num_items * 2)

        self.log.info("Flush the CB bucket")
        self.cluster.bucket_flush(server=self.master, bucket=self.cb_bucket_name)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

        self.log.info("Fetch cbas stats")
        status, content, response = self.cbas_util.fetch_cbas_stats()
        self.assertTrue(status, "Error invoking fetch cbas stats api")
        stats_json = json.loads(content)
        self.log.info(stats_json)

        self.log.info("Assert Analytics stats after flush")
        self.assert_cbas_stats(stats_json, 0, 0)
    
    '''
    cbas.cbas_stats.CbasStats.fetch_stats_on_cbas_before_and_after_cbas_bucket_disconnect,cbas_bucket_name=cbas,cbas_dataset_name=ds,items=10,default_bucket=True,cb_bucket_name=default
    '''
        
    def fetch_stats_on_cbas_before_and_after_cbas_bucket_disconnect(self):

        """
        1. Add a default CB bucket
        2. Add N Json documents and N Non-Json documents
        3. Create CBAS bucket and Dataset
        4. Verify the count on CBAS and assert on stats - Incoming records = Failed + Mutated(Insert/Update/Deleted)
        5. Disconnect the CB bucket and verify the count for Incoming and Failed count is not displayed
        6. Connect back the bucket and verify the count is reset to zero
        """
        self.log.info("Fetch cbas stats before flush")
        status, content, response = self.cbas_util.fetch_cbas_stats()
        self.assertTrue(status, "Error invoking fetch cbas stats api")

        self.log.info("Assert Analytics stats before bucket disconnect")
        stats_json = json.loads(content)
        self.log.info(stats_json)
        self.assert_cbas_stats(stats_json, self.num_items, self.num_items * 2)

        self.log.info("Disconnect the CBAS bucket")
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

        self.log.info("Fetch cbas stats after bucket disconnect")
        status, content, response = self.cbas_util.fetch_cbas_stats()
        self.assertTrue(status, "Error invoking fetch cbas stats api")
        stats_json = json.loads(content)
        self.log.info(stats_json)
        self.assertTrue(self.cb_bucket_name + ':all:failed_at_parser_records_count' not in stats_json, msg="Json must not have bucket related field")
        self.assertTrue(self.cb_bucket_name + ':all:failed_at_parser_records_count_total' not in stats_json, msg="Json must not have bucket related field")
        self.assertTrue(self.cb_bucket_name + ':all:incoming_records_count' not in stats_json, msg="Json must not have bucket related field")
        self.assertTrue(self.cb_bucket_name + ':all:incoming_records_count_total' not in stats_json, msg="Json must not have bucket related field")
        
        self.log.info("Connect back CBAS bucket")
        self.cbas_util.connect_to_bucket(self.cbas_bucket_name)

        self.log.info("Assert Analytics stats after bucket re-connect")
        status, content, response = self.cbas_util.fetch_cbas_stats()
        self.assertTrue(status, "Error invoking fetch cbas stats api")
        stats_json = json.loads(content)
        self.log.info(stats_json)
        self.assert_cbas_stats(stats_json, 0, 0)
