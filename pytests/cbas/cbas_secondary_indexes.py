from cbas_base import *
from CbasLib.CBASOperations import CBASHelper
from BucketLib.bucket import BeerSample


class CBASSecondaryIndexes(CBASBaseTest):
    def setUp(self):
        super(CBASSecondaryIndexes, self).setUp()
        exclude_bucket = []
        for bucket in self.cluster.buckets:
            if bucket.name != 'lineitem':
                exclude_bucket.append(bucket.name)

        self.dataset = self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util,
            dataset_cardinality=self.input.param('cardinality', 1),
            bucket_cardinality=self.input.param('bucket_cardinality', 3),
            no_of_objs=1, exclude_bucket=exclude_bucket, exclude_scope=[
                "_system"])[0]

        if not self.cbas_util.create_dataset(
                self.cluster, self.dataset.name, self.dataset.get_fully_qualified_kv_entity_name(
                    self.input.param('bucket_cardinality', 3)),
                self.dataset.dataverse_name):
            self.fail("Error while creating dataset")

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(CBASSecondaryIndexes, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def get_appropriate_where_predicate(self, index_fields=None):
        if index_fields:
            fields = index_fields
        else:
            fields = self.input.param('index_fields').split('-')
        predicate = ''
        while fields:
            field = fields.pop()
            if field.split(':')[1] == "string":
                predicate = predicate + field.split(':')[0] + ' > ""'
            elif field.split(':')[1] == "bigint":
                predicate = predicate + field.split(':')[0] + ' > 0'
            elif field.split(':')[1] == "double":
                predicate = predicate + field.split(':')[0] + ' > 0.0'
            if fields:
                predicate += ' and '
        return predicate

    def test_create_secondary_index_on_dataset(self):
        """
        Steps :
        1. Create bucket in CBAS, create dataset
        2. Create index on various fields as passed in the parameters
        3. Validate if the index is created and the index definition has the expected fields

        Author : Vipul Bhardwaj
        Created date : 15/5/2023
        """
        if not self.cbas_util.create_cbas_index(self.cluster, "idx",
                                                self.input.param('index_fields').split('-'),
                                                self.dataset.full_name,
                                                self.input.param('analytics_index', False)):
            self.fail("Error while creating Analytics Index")

        if not self.cbas_util.verify_index_created(self.cluster, "idx", self.dataset.name,
                                                   self.input.param('index_fields', '').split('-'))[0]:
            self.fail("Index Creation not verified.")

        # handling where predicate based on the index_fields from conf
        if not self.cbas_util.verify_index_used(self.cluster,
                                                "select * from `{0}` where {1} limit 10".format(
                                                    CBASHelper.unformat_name(self.dataset.full_name),
                                                    self.get_appropriate_where_predicate()),
                                                True, "idx"):
            self.fail("Index Usage not verified.")

    def test_create_index_without_if_not_exists(self):
        """
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Create index
            3. Again create an index with the same name without using IF_NOT_EXISTS clause
            3. Validate if the error msg is as expected

            Author : Vipul Bhardwaj
            Created date : 19/5/2023
        """
        # Create Index
        if not self.cbas_util.create_cbas_index(self.cluster, "idx",
                                                self.input.param('index_fields').split('-'),
                                                self.dataset.full_name,
                                                self.input.param('analytics_index', False)):
            self.fail("Error while creating Analytics Index")

        if not self.cbas_util.verify_index_created(self.cluster, "idx", self.dataset.name,
                                                   self.input.param('index_fields').split('-'))[0]:
            self.fail("Index Creation not verified.")

        # Create another index with same name
        if not self.cbas_util.create_cbas_index(self.cluster, "idx",
                                                self.input.param('index_fields').split('-'),
                                                self.dataset.full_name,
                                                self.input.param('analytics_index', False),
                                                True, self.input.param("error")):
            self.fail("Error msg not matching expected error msg")

    def test_create_index_with_if_not_exists(self):
        """
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Create index
            3. Again create an index with the same name using IF_NOT_EXISTS clause
            3. Validate if that there  is no error

            Author : Vipul Bhardwaj
            Created date : 21/5/2023
        """
        # Create Index
        if not self.cbas_util.create_cbas_index(self.cluster, "idx",
                                                self.input.param('index_fields').split('-'),
                                                self.dataset.full_name,
                                                self.input.param('analytics_index', False),
                                                if_not_exists=True):
            self.fail("Error while creating Analytics Index")

        if not self.cbas_util.verify_index_created(self.cluster, "idx", self.dataset.name,
                                                   self.input.param('index_fields').split('-'))[0]:
            self.fail("Index Creation not verified.")

        # Create another index with same name
        if not self.cbas_util.create_cbas_index(self.cluster, "idx",
                                                self.input.param('index_fields').split('-'),
                                                self.dataset.full_name,
                                                self.input.param('analytics_index', False),
                                                if_not_exists=True):
            self.fail("Error while creating Analytics Index")

        if not self.cbas_util.verify_index_created(self.cluster, "idx", self.dataset.name,
                                                   self.input.param('index_fields').split('-'))[0]:
            self.fail("Index Creation not verified.")

    def test_create_index_with_if_not_exists_different_fields(self):
        """
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Create index
            3. Again create an index with the same name but with different fields using IF_NOT_EXISTS clause
            4. Validate there is no error
            5. The index definition of should not change.

            Author : Vipul Bhardwaj
            Created date : 22/5/2023
        """
        idx_fields = self.input.param('index_fields').split('-')

        if not self.cbas_util.create_cbas_index(self.cluster, "idx", [idx_fields[0]],
                                                self.dataset.full_name,
                                                self.input.param('analytics_index', False),
                                                if_not_exists=True):
            self.fail("Error while creating Analytics Index")

        if not self.cbas_util.verify_index_created(self.cluster, "idx", self.dataset.name,
                                                   [idx_fields[0]])[0]:
            self.fail("Index Creation not verified.")

        # Create another index with same name
        if not self.cbas_util.create_cbas_index(self.cluster, "idx", [idx_fields[1]],
                                                self.dataset.full_name,
                                                self.input.param('analytics_index', False),
                                                if_not_exists=True):
            self.fail("Error while creating Analytics Index")

        if not self.cbas_util.verify_index_created(self.cluster, "idx", self.dataset.name,
                                                   [idx_fields[0]])[0]:
            self.fail("Index Creation not verified.")

    def test_drop_index(self):
        """
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Create index
            3. Validate the index is created correctly
            4. Drop index
            5. Validate that the index is dropped

            Author : Vipul Bhardwaj
            Created date : 22/5/2023
        """
        # Create Index
        if not self.cbas_util.create_cbas_index(self.cluster, "idx",
                                                self.input.param('index_fields').split('-'),
                                                self.dataset.full_name,
                                                self.input.param('analytics_index', False)):
            self.fail("Error while creating Analytics Index")

        if not self.cbas_util.verify_index_created(self.cluster, "idx", self.dataset.name,
                                                   self.input.param('index_fields').split('-'))[0]:
            self.fail("Index Creation not verified.")

        if not self.cbas_util.drop_cbas_index(self.cluster, "idx", self.dataset.full_name,
                                              self.input.param('analytics_index', False)):
            self.fail("Error while dropping Analytics Index")

        if self.cbas_util.verify_index_created(self.cluster, "idx", self.dataset.name,
                                               self.input.param('index_fields').split('-'))[0]:
            self.fail("Index Existence verified, even though it should have been dropped.")

    def test_drop_non_existing_index(self):
        """
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Drop a non-existing index without using IF_EXISTS clause
            3. Validate that the error msg is as expected
            4. Drop a non-existing index using IF_EXISTS clause
            5. Validate there is no error

            Author : Vipul Bhardwaj
            Created date : 22/5/2023
        """
        if not self.cbas_util.drop_cbas_index(self.cluster, "idx", self.dataset.full_name,
                                              self.input.param('analytics_index', False),
                                              True, self.input.param("error")):
            self.fail("Error while validating error for dropping Analytics Index")

        # Drop non-existing index with IF EXISTS
        if not self.cbas_util.drop_cbas_index(self.cluster, "idx", self.dataset.full_name,
                                              self.input.param('analytics_index', False),
                                              if_exists=True):
            self.fail("Drop non existent index with IF EXISTS failed")

    def test_drop_dataset_drops_index(self):
        """
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Create index
            3. Validate the index is created correctly
            4. Drop dataset
            5. Validate that the index is also dropped

            Author : Vipul Bhardwaj
            Created date : 22/5/2023
        """
        # Create Index
        if not self.cbas_util.create_cbas_index(self.cluster, "idx",
                                                self.input.param('index_fields').split('-'),
                                                self.dataset.full_name,
                                                self.input.param('analytics_index', False)):
            self.fail("Error while creating Analytics Index")

        if not self.cbas_util.verify_index_created(self.cluster, "idx", self.dataset.name,
                                                   self.input.param('index_fields').split('-'))[0]:
            self.fail("Index Creation not verified.")

        # Drop dataset
        self.cbas_util.drop_dataset(self.cluster, self.dataset.full_name)

        # Check that the index no longer exists
        if self.cbas_util.verify_index_created(self.cluster, "idx", self.dataset.name,
                                               self.input.param('index_fields').split('-'))[0]:
            self.fail("Index still exists.")

    def test_multiple_composite_index_with_overlapping_fields(self):
        """
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Create index
            3. Again create a composite index
            4. Now create another composite index with some overlapping fields
            5. Both the indexes should get created successfully

            Author : Vipul Bhardwaj
            Created date : 22/5/2023
        """
        if not self.cbas_util.create_cbas_index(self.cluster, "idx1",
                                                self.input.param('index_fields').split('-')[:2],
                                                self.dataset.full_name,
                                                self.input.param('analytics_index', False)):
            self.fail("Error while creating Analytics Index")

        if not self.cbas_util.verify_index_created(self.cluster, "idx1", self.dataset.name,
                                                   self.input.param('index_fields').split('-')[:2])[0]:
            self.fail("Index Creation not verified.")

        # Create another composite index with overlapping fields
        if not self.cbas_util.create_cbas_index(self.cluster, "idx2",
                                                self.input.param('index_fields').split('-')[1:],
                                                self.dataset.full_name,
                                                self.input.param('analytics_index', False)):
            self.fail("Error while creating Analytics Index")

        if not self.cbas_util.verify_index_created(self.cluster, "idx2", self.dataset.name,
                                                   self.input.param('index_fields').split('-')[1:])[0]:
            self.fail("Index Creation not verified.")

        # Verify any index's usage
        if not self.cbas_util.verify_index_used(self.cluster,
                                                "select * from `{0}` where {1} limit 10".format(
                                                    CBASHelper.unformat_name(self.dataset.full_name),
                                                    self.get_appropriate_where_predicate()),
                                                True, "idx1"):
            self.fail("Index Usage not verified.")

    def test_index_on_nested_fields_same_object(self):
        index_fields = ["geo.lon:double", "geo.lat:double"]

        if not self.cbas_util.create_cbas_index(self.cluster, "idx", index_fields,
                                                self.dataset.full_name):
            self.fail("Error while creating Analytics Index")

        if not self.cbas_util.verify_index_created(self.cluster, "idx",
                                                   self.dataset.name, index_fields)[0]:
            self.fail("Index Creation not verified.")

        if not self.cbas_util.verify_index_used(self.cluster,
                                                "select * from `{0}` where {1} limit 10".format(
                                                    CBASHelper.unformat_name(self.dataset.full_name),
                                                    self.get_appropriate_where_predicate(index_fields)),
                                                True, "idx"):
            self.fail("Index Usage not verified.")
