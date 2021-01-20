import copy
import json
import sys

from BucketLib.bucket import Bucket
from Cb_constants import CbServer, DocLoading
from basetestcase import BaseTestCase
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import \
    doc_generator, \
    sub_doc_generator
from error_simulation.cb_error import CouchbaseError
from membase.api.exception import DesignDocCreationException
from couchbase_helper.document import View
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException


class SubdocBaseTest(BaseTestCase):
    def setUp(self):
        super(SubdocBaseTest, self).setUp()

        # Initialize cluster using given nodes
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)

        # Create default bucket and add rbac user
        self.bucket_util.create_default_bucket(
            replica=self.num_replicas, compression_mode=self.compression_mode,
            bucket_type=self.bucket_type, storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy)
        self.bucket_util.add_rbac_user()

        # Create required scope/collection for testing
        if self.collection_name != CbServer.default_collection:
            self.collection_name = self.bucket_util.get_random_name()
            if self.scope_name != CbServer.default_scope:
                self.scope_name = self.bucket_util.get_random_name()
                self.bucket_util.create_scope(self.cluster.master,
                                              self.bucket_util.buckets[0],
                                              {"name": self.scope_name})
            self.bucket_util.create_collection(
                self.cluster.master,
                self.bucket_util.buckets[0],
                scope_name=self.scope_name,
                collection_spec={"name": self.collection_name})

        for bucket in self.bucket_util.buckets:
            testuser = [{'id': bucket.name,
                         'name': bucket.name,
                         'password': 'password'}]
            rolelist = [{'id': bucket.name,
                         'name': bucket.name,
                         'roles': 'admin'}]
            self.bucket_util.add_rbac_user(testuser=testuser,
                                           rolelist=rolelist)
        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()

    def tearDown(self):
        super(SubdocBaseTest, self).tearDown()

    def generate_json_for_nesting(self):
        return {
            "i_0": 0,
            "i_b": 1038383839293939383938393,
            "d_z": 0.0,
            "i_p": 1,
            "i_n": -1,
            "d_p": 1.1,
            "d_n": -1.1,
            "f": 2.99792458e8,
            "f_n": -2.99792458e8,
            "a_i": [1, 2, 3, 4, 5],
            "a_d": [1.1, 2.2, 3.3, 4.4, 5.5],
            "a_f": [2.99792458e8, 2.99792458e8, 2.99792458e8],
            "a_m": [0, 2.99792458e8, 1.1],
            "a_a": [[2.99792458e8, 2.99792458e8, 2.99792458e8], [0, 2.99792458e8, 1.1], [], [0, 0, 0]],
            "l_c": "abcdefghijklmnoprestuvxyz",
            "u_c": "ABCDEFGHIJKLMNOPQRSTUVWXZYZ",
            "s_e": "",
            "d_t": "2012-10-03 15:35:46.461491",
            "s_c": "_-+!#@$%&*(){}\][;.,<>?/",
            "js": {"not_to_bes_tested_string_field1": "not_to_bes_tested_string"}
        }

    def generate_simple_data_null(self):
        return {
            "null": None,
            "n_a": [None, None]
        }

    def generate_simple_data_boolean(self):
        return {
            "1": True,
            "2": False,
            "3": [True, False, True, False]
        }

    def generate_nested_json(self):
        json_data = self.generate_json_for_nesting()
        json = {
            "json_1": {"json_2": {"json_3": json_data}}
        }
        return json

    def generate_simple_data_numbers(self):
        return {
            "1": 0,
            "2": 1038383839293939383938393,
            "3": 0.0,
            "4": 1,
            "5": -1,
            "6": 1.1,
            "7": -1.1,
            "8": 2.99792458e8,
            "9": -2.99792458e8,
        }

    def generate_simple_data_numbers_boundary(self):
        return {
            "int_max": sys.maxint,
            "int_min": sys.minint
        }

    def generate_simple_data_array_of_numbers(self):
        return {
            "ai": [1, 2, 3, 4, 5],
            "ad": [1.1, 2.2, 3.3, 4.4, 5.5],
            "af": [2.99792458e8, 2.99792458e8, 2.99792458e8],
            "am": [0, 2.99792458e8, 1.1],
            "aa": [[2.99792458e8, 2.99792458e8, 2.99792458e8], [0, 2.99792458e8, 1.1], [], [0, 0, 0]]
        }

    def generate_simple_data_strings(self):
        return {
            "lc": "abcdefghijklmnoprestuvxyz",
            "uc": "ABCDEFGHIJKLMNOPQRSTUVWXZYZ",
            "se": "",
            "dt": "2012-10-03 15:35:46.461491",
            "sc": "_-+!#@$%&*(){}\][;.,<>?/"
        }

    def generate_simple_data_array_strings(self):
        return {
            "ac": ['a', 'b', ''],
            "as": ['aa', '11', '&#^#', ''],
            "aas": [['aa', '11', '&#^#', ''], ['a', 'b', '']]
        }

    def generate_simple_data_mix_arrays(self):
        return {
            "am": ["abcdefghijklmnoprestuvxyz", 1, 1.1, ""],
            "aai": [[1, 2, 3], [4, 5, 6]],
            "aas": [["abcdef", "ghijklmo", "prririr"], ["xcvf", "ffjfjf", "pointer"]]
        }

    def generate_simple_arrays(self):
        return {
            "1_d_a": ["abcdefghijklmnoprestuvxyz", 1, 1.1, ""],
            "2_d_a": [[1, 2, 3], ["", -1, 1, 1.1, -1.1]]
        }

    def generate_path(self, level, key):
        path = key
        t_list = range(level)
        t_list.reverse()
        for i in t_list:
            path = "level_"+str(i)+"."+path
        return path

    def generate_nested(self, base_nested_level, nested_data, level_counter):
        json_data = copy.deepcopy(base_nested_level)
        original_json = json_data
        for i in range(level_counter):
            level = "level_"+str(i)
            json_data[level] = copy.deepcopy(base_nested_level)
            json_data = json_data[level]
        json_data.update(nested_data)
        return original_json


class SubdocXattrSdkTest(SubdocBaseTest):
    VALUES = {
        "int_zero": 0,
        "int_big": 1038383839293939383938393,
        "double_z": 0.0,
        "int_posit": 1,
        "int_neg": -1,
        "double_s": 1.1,
        "double_n": -1.1,
        "float": 2.99792458e8,
        "float_neg": -2.99792458e8,
        "arr_ints": [1, 2, 3, 4, 5],
        "a_doubles": [1.1, 2.2, 3.3, 4.4, 5.5],
        "arr_floa": [2.99792458e8, 2.99792458e8, 2.99792458e8],
        "arr_mixed": [0, 2.99792458e8, 1.1],
        "arr_arrs": [[2.99792458e8, 2.99792458e8, 2.99792458e8], [0, 2.99792458e8, 1.1], [], [0, 0, 0]],
        "low_case": "abcdefghijklmnoprestuvxyz",
        "u_c": "ABCDEFGHIJKLMNOPQRSTUVWXZYZ",
        "str_empty": "",
        "d_time": "2012-10-03 15:35:46.461491",
        "spec_chrs": "_-+!#@$%&*(){}\][;.,<>?/",
        "json": {"not_to_bes_tested_string_field1": "not_to_bes_tested_string"}
    }

    EXPECTED_VALUE = {u'u_c': u'ABCDEFGHIJKLMNOPQRSTUVWXZYZ', u'low_case': u'abcdefghijklmnoprestuvxyz',
                      u'int_big': 1.0383838392939393e+24, u'double_z': 0, u'arr_ints': [1, 2, 3, 4, 5], u'int_posit': 1,
                      u'int_zero': 0, u'arr_floa': [299792458, 299792458, 299792458], u'float': 299792458,
                      u'float_neg': -299792458, u'double_s': 1.1, u'arr_mixed': [0, 299792458, 1.1], u'double_n': -1.1,
                      u'str_empty': u'', u'a_doubles': [1.1, 2.2, 3.3, 4.4, 5.5],
                      u'd_time': u'2012-10-03 15:35:46.461491',
                      u'arr_arrs': [[299792458, 299792458, 299792458], [0, 299792458, 1.1], [], [0, 0, 0]],
                      u'int_neg': -1, u'spec_chrs': u'_-+!#@$%&*(){}\\][;.,<>?/',
                      u'json': {u'not_to_bes_tested_string_field1': u'not_to_bes_tested_string'}}

    def setUp(self):
        super(SubdocXattrSdkTest, self).setUp()
        self.xattr = self.input.param("xattr", True)
        self.doc_id = 'xattrs'
        self.client = SDKClient([self.cluster.master],
                                self.bucket_util.buckets[0])

    def tearDown(self):
        # Delete the inserted doc
        self.client.crud("remove", self.doc_id)

        # Close the SDK connection
        self.client.close()
        super(SubdocXattrSdkTest, self).tearDown()

    def __upsert_document_and_validate(self, op_type, value):
        result = self.client.crud(op_type, self.doc_id, value=value)
        if result["status"] is False:
            self.fail("Initial doc create failed")

    def __insert_sub_doc_and_validate(self, op_type, key, value):
        _, failed_items = self.client.crud(
            op_type,
            self.doc_id,
            [key, value],
            durability=self.durability_level,
            timeout=self.sdk_timeout,
            time_unit="seconds",
            create_path=True,
            xattr=self.xattr)
        self.assertFalse(failed_items, "Subdoc Xattr insert failed")

    def __read_doc_and_validate(self, expected_val, subdoc_key=None):
        if subdoc_key:
            success, failed_items = self.client.crud("subdoc_read",
                                                     self.doc_id,
                                                     subdoc_key,
                                                     xattr=self.xattr)
            self.assertFalse(failed_items, "Xattr read failed")
            self.assertEqual(expected_val,
                             str(success[self.doc_id]["value"][0]),
                             "Sub_doc value mismatch: %s != %s"
                             % (success[self.doc_id]["value"][0],
                                expected_val))
        else:
            result = self.client.crud("read", self.doc_id)
            self.assertEqual(result["value"], expected_val,
                             "Document value mismatch: %s != %s"
                             % (result["value"], expected_val))

    def test_basic_functionality(self):
        self.__upsert_document_and_validate("create", {})

        # Try to upsert a single xattr
        self.__insert_sub_doc_and_validate("subdoc_insert",
                                           "my.attr", "value")

        # Read full doc and validate
        self.__read_doc_and_validate("{}")

        # Using lookup_in
        result, _ = self.client.crud("subdoc_read",
                                     self.doc_id,
                                     "my.attr")
        self.assertEqual(
            result["xattrs"]["value"][0],
            "PATH_NOT_FOUND",
            "Invalid SDK return value: %s" % result["xattrs"]["value"])

        # Finally, use lookup_in with 'xattrs' attribute enabled
        self.__read_doc_and_validate("value", "my.attr")

    def test_multiple_attrs(self):
        self.__upsert_document_and_validate("update", {})

        xattrs_to_insert = [["my.attr", "value"],
                            ["new_my.attr", "new_value"]]

        # Try to upsert multiple xattr
        for key, val in xattrs_to_insert:
            self.__insert_sub_doc_and_validate("subdoc_insert",
                                               key, val)

        # Read full doc and validate
        self.__read_doc_and_validate("{}")

        # Use lookup_in with 'xattrs' attribute enabled to validate the values
        for key, val in xattrs_to_insert:
            self.__read_doc_and_validate(val, key)

    def test_xattr_big_value(self):
        sub_doc_key = "my.attr"
        value = {"v": "v" * 500000}
        self.__upsert_document_and_validate("update", value)

        self.__insert_sub_doc_and_validate("subdoc_insert",
                                           sub_doc_key, value)

        # Read full doc and validate
        result = self.client.crud("read", self.doc_id)
        result = json.loads(result["value"])
        self.assertEqual(result, value,
                         "Document value mismatch: %s != %s" % (result, value))

        # Read sub_doc for validating the value
        success, failed_items = self.client.crud("subdoc_read",
                                                 self.doc_id,
                                                 sub_doc_key,
                                                 xattr=self.xattr)
        self.assertFalse(failed_items, "Xattr read failed")
        result = json.loads(str(success[self.doc_id]["value"][0]))
        self.assertEqual(result, value,
                         "Sub_doc value mismatch: %s != %s" % (result, value))

    def test_add_to_parent(self):
        self.__upsert_document_and_validate("update", {})

        # Read and record CAS
        result = self.client.crud("read", self.doc_id)
        self.assertTrue(result["status"], "Read failed")
        initial_cas = result["cas"]

        self.__insert_sub_doc_and_validate("subdoc_insert",
                                           "my", {'value': 1})

        # Read and record CAS
        result = self.client.crud("read", self.doc_id)
        self.assertTrue(result["status"], "Read failed")
        updated_cas_1 = result["cas"]

        self.__insert_sub_doc_and_validate("subdoc_insert",
                                           "my.inner", {'value_inner': 2})

        # Read and record CAS
        result = self.client.crud("read", self.doc_id)
        self.assertTrue(result["status"], "Read failed")
        updated_cas_2 = result["cas"]

        self.__read_doc_and_validate("{}")

        result, _ = self.client.crud("subdoc_read",
                                     self.doc_id,
                                     "my.attr")
        self.assertEqual(
            result["xattrs"]["value"][0],
            "PATH_NOT_FOUND",
            "Invalid SDK return value: %s" % result["xattrs"]["value"])

        self.__read_doc_and_validate("{\"value_inner\":2}", "my.inner")
        self.__read_doc_and_validate("{\"value\":1,\"inner\":{\"value_inner\":2}}",
                                     "my")
        self.assertTrue(initial_cas != updated_cas_1, "CAS not updated")
        self.assertTrue(updated_cas_1 != updated_cas_2, "CAS not updated")
        self.assertTrue(initial_cas != updated_cas_2, "CAS not updated")

    # https://issues.couchbase.com/browse/PYCBC-378
    def test_key_length_big(self):
        self.__upsert_document_and_validate("update", {})
        self.__insert_sub_doc_and_validate("subdoc_insert",
                                           "g" * 15, 1)

        _, failed_items = self.client.crud(
            "subdoc_insert",
            self.doc_id,
            ["f" * 16, 2],
            durability=self.durability_level,
            timeout=self.sdk_timeout,
            time_unit="seconds",
            create_path=True,
            xattr=True)
        self.assertTrue(failed_items, "Subdoc Xattr insert with 16 chars")

        self.assertTrue(SDKException.DecodingFailedException
                        in failed_items[self.doc_id]["error"],
                        "Invalid exception: %s" % failed_items[self.doc_id])

    # https://issues.couchbase.com/browse/MB-23108
    def test_key_underscore(self):
        self.doc_id = 'mobile_doc'
        mobile_value = {'name': 'Peter', 'task': 'performance',
                        'ids': [1, 2, 3, 4]}

        mob_metadata = {
            'rev': '10-cafebabefweqfa',
            'deleted': False,
            'sequence': 1234,
            'history': ['8-cafasdfgabqfa', '9-cafebadfasdfa'],
            'channels': ['users', 'customers', 'admins'],
            'access': {'users': 'read', 'customers': 'read', 'admins': 'write'}
        }
        new_metadata = {'secondary': ['new', 'new2']}

        self.client.set(k, mobile_value)
        self.client.mutate_in(k, SD.upsert("_sync", mob_metadata, xattr=True))
        self.client.mutate_in(k, SD.upsert("_data", new_metadata, xattr=True))

        rv = self.client.lookup_in(k, SD.get("_sync", xattr=True))
        self.assertTrue(rv.exists('_sync'))
        rv = self.client.lookup_in(k, SD.get("_data", xattr=True))
        self.assertTrue(rv.exists('_data'))

    def test_key_start_characters(self):
        self.__upsert_document_and_validate("update", {})

        for ch in "!\"#%&'()*+,-./:;<=>?@[\]^`{|}~":
            try:
                key = ch + 'test'
                self.log.info("test '%s' key" % key)
                self.client.mutate_in(k, SD.upsert(key, 1, xattr=True))
                rv = self.client.lookup_in(k, SD.get(key, xattr=True))
                self.log.error("xattr %s exists? %s" % (key, rv.exists(key)))
                self.log.error("xattr %s value: %s" % (key, rv[key]))
                self.fail("key shouldn't start from " + ch)
            except Exception as e:
                self.assertEquals("Operational Error", e.message)

    def test_key_inside_characters_negative(self):
        self.__upsert_document_and_validate("update", {})

        for ch in "\".:;[]`":
            try:
                key = 'test' + ch + 'test'
                self.log.info("test '%s' key" % key)
                self.client.mutate_in(k, SD.upsert(key, 1, xattr=True))
                rv = self.client.lookup_in(k, SD.get(key, xattr=True))
                self.log.error("xattr %s exists? %s" % (key, rv.exists(key)))
                self.log.error("xattr %s value: %s" % (key, rv[key]))
                self.fail("key must not contain a character: " + ch)
            except Exception as e:
                print e.message
                self.assertTrue(e.message in ['Subcommand failure',
                                              'key must not contain a character: ;'])

    def test_key_inside_characters_positive(self):
        self.__upsert_document_and_validate("update", {})

        for ch in "#!#$%&'()*+,-/;<=>?@\^_{|}~":
            key = 'test' + ch + 'test'
            self.log.info("test '%s' key" % key)
            self.client.mutate_in(k, SD.upsert(key, 1, xattr=True))
            rv = self.client.lookup_in(k, SD.get(key, xattr=True))
            self.log.info("xattr %s exists? %s" % (key, rv.exists(key)))
            self.log.info("xattr %s value: %s" % (key, rv[key]))

    def test_key_special_characters(self):
        self.__upsert_document_and_validate("update", {})

        for key in ["a#!#$%&'()*+,-a", "b/<=>?@\\b^_{|}~"]:
            self.log.info("test '%s' key" % key)
            self.client.mutate_in(k, SD.upsert(key, key, xattr=True))
            rv = self.client.lookup_in(k, SD.get(key, xattr=True))
            self.assertTrue(rv.exists(key))
            self.assertEquals(key, rv[key])

    def test_deep_nested(self):
        self.__upsert_document_and_validate("update", {})

        key = "a!._b!._c!._d!._e!"
        self.log.info("test '%s' key" % key)
        self.client.mutate_in(k, SD.upsert(key, key, xattr=True, create_parents=True))
        rv = self.client.lookup_in(k, SD.get(key, xattr=True))
        self.log.info("xattr %s exists? %s" % (key, rv.exists(key)))
        self.log.info("xattr %s value: %s" % (key, rv[key]))
        self.assertEquals(key, rv[key])

    def test_delete_doc_with_xattr(self):
        self.__upsert_document_and_validate("update", {})

        self.__insert_sub_doc_and_validate("subdoc_insert",
                                           "my_attr", "value")
        self.__read_doc_and_validate("value", "my_attr")

        # trying get before delete
        result = self.client.crud("read", self.doc_id)
        self.assertTrue(result["cas"] != 0, "CAS is zero!")
        self.assertEqual(result["value"], "{}", "Value mismatch")

        # Delete the full document
        self.client.crud("delete", self.doc_id)

        # Try reading the document
        result = self.client.crud("read", self.doc_id)
        self.assertTrue(result["cas"] == 0, "CAS is non-zero")

        # Try reading the sub_doc and xattr to validate
        for is_xattr in [False, True]:
            _, failed_items = self.client.crud("subdoc_read",
                                               self.doc_id,
                                               "my_attr",
                                               xattr=is_xattr)
            self.assertEqual(failed_items[self.doc_id]["cas"], 0,
                             "CAS is non-zero")
            self.assertTrue(SDKException.DocumentNotFoundException
                            in str(failed_items[self.doc_id]["error"]),
                            "Invalid exception")

    # https://issues.couchbase.com/browse/MB-24104
    def test_delete_doc_with_xattr_access_deleted(self):
        k = 'xattrs'

        self.client.upsert(k, {"a": 1})

        # Try to upsert a single xattr with _access_deleted
        try:
            rv = self.client.mutate_in(k, SD.upsert('my_attr', 'value',
                                                    xattr=True,
                                                    create_parents=True), _access_deleted=True)
        except Exception as e:
            self.assertEquals("couldn't parse arguments", e.message)

    def test_delete_doc_without_xattr(self):
        k = 'xattrs'

        self.client.upsert(k, {})

        # Try to upsert a single xattr
        rv = self.client.mutate_in(k, SD.upsert('my_attr', 'value'))
        self.assertTrue(rv.success)

        rv = self.client.lookup_in(k, SD.get('my_attr'))
        self.assertTrue(rv.exists('my_attr'))

        # trying get before delete
        rv = self.client.get(k)
        self.assertTrue(rv.success)
        self.assertEquals({u'my_attr': u'value'}, rv.value)
        self.assertEquals(0, rv.rc)
        self.assertTrue(rv.cas != 0)
        self.assertTrue(rv.flags != 0)

        # delete
        body = self.client.delete(k)
        self.assertEquals(None, body.value)

        # trying get after delete
        try:
            self.client.get(k)
            self.fail("get should throw NotFoundError when doc deleted")
        except NotFoundError:
            pass

        try:
            self.client.retrieve_in(k, 'my_attr')
            self.fail("retrieve_in should throw NotFoundError when doc deleted")
        except NotFoundError:
            pass

        try:
            self.client.lookup_in(k, SD.get('my_attr'))
            self.fail("lookup_in should throw NotFoundError when doc deleted")
        except NotFoundError:
            pass

    def test_delete_xattr(self):
        self.__upsert_document_and_validate("update", {})

        # Trying getting non-existing xattr
        result, _ = self.client.crud("subdoc_read",
                                     self.doc_id,
                                     "my_attr",
                                     xattr=True)
        self.assertEqual(
            result["xattrs"]["value"][0],
            "PATH_NOT_FOUND",
            "Invalid SDK return value: %s" % result["xattrs"]["value"])

        # Try to upsert a single xattr
        self.__insert_sub_doc_and_validate("subdoc_insert",
                                           "my_attr", "value")
        self.__read_doc_and_validate("value", "my_attr")

        result = self.client.crud("read", self.doc_id)
        self.assertEqual(result["value"], "{}",
                         "Document value mismatch: %s != %s"
                         % (result["value"], "{}"))

        cas_before = result["cas"]

        # Delete xattr
        success, failed_items = self.client.crud("subdoc_delete",
                                                 self.doc_id,
                                                 "my_attr",
                                                 xattr=True)
        self.assertFalse(failed_items, "Subdoc delete failed")

        # Trying get doc after xattr deleted
        result = self.client.crud("read", self.doc_id)
        self.assertTrue(result["status"], "Read status is 'False'")
        self.assertTrue(result["cas"] != 0, "Document CAS is Zero")
        self.assertTrue(result["cas"] != cas_before, "CAS not updated after "
                                                     "subdoc delete")
        self.assertEqual(result["value"], "{}",
                         "Document value mismatch: %s != %s"
                         % (result["value"], "{}"))

        # Read deleted xattr to verify
        result, _ = self.client.crud("subdoc_read",
                                     self.doc_id,
                                     "my_attr",
                                     xattr=True)
        self.assertEqual(
            result["xattrs"]["value"][0],
            "PATH_NOT_FOUND",
            "Invalid SDK return value: %s" % result["xattrs"]["value"])

    def test_cas_changed_upsert(self):
        if self.xattr is False:
            self.doc_id = 'non_xattrs'

        self.__upsert_document_and_validate("update", {})

        # Read and record CAS
        result = self.client.crud("read", self.doc_id)
        self.assertTrue(result["status"], "Read failed")
        initial_cas = result["cas"]

        self.__insert_sub_doc_and_validate("subdoc_insert",
                                           "my", {'value': 1})

        # Read and record CAS
        result = self.client.crud("read", self.doc_id)
        self.assertTrue(result["status"], "Read failed")
        updated_cas_1 = result["cas"]

        self.__insert_sub_doc_and_validate("subdoc_insert",
                                           "my.inner", {'value_inner': 2})

        # Read and record CAS
        result = self.client.crud("read", self.doc_id)
        self.assertTrue(result["status"], "Read failed")
        updated_cas_2 = result["cas"]

        if self.xattr:
            self.__read_doc_and_validate("{}")

        result, _ = self.client.crud("subdoc_read",
                                     self.doc_id,
                                     "my.attr")
        if self.xattr:
            result = result["xattrs"]
        else:
            result = result["non_xattrs"]

        self.assertEqual(
            result["value"][0],
            "PATH_NOT_FOUND",
            "Invalid SDK return value: %s" % result["value"])

        self.__read_doc_and_validate("{\"value_inner\":2}", "my.inner")
        self.__read_doc_and_validate("{\"value\":1,\"inner\":{\"value_inner\":2}}",
                                     "my")
        self.assertTrue(initial_cas != updated_cas_1, "CAS not updated")
        self.assertTrue(updated_cas_1 != updated_cas_2, "CAS not updated")
        self.assertTrue(initial_cas != updated_cas_2, "CAS not updated")

    def test_use_cas_changed_upsert(self):
        self.__upsert_document_and_validate("update", {})

        # Read and record CAS
        result = self.client.crud("read", self.doc_id)
        self.assertTrue(result["status"], "Read failed")
        initial_cas = result["cas"]

        self.__insert_sub_doc_and_validate("subdoc_insert",
                                           "my", {'value': 1})

        # Read and record CAS
        result = self.client.crud("read", self.doc_id)
        self.assertTrue(result["status"], "Read failed")
        updated_cas_1 = result["cas"]

        _, failed_items = self.client.crud(
            "subdoc_insert",
            self.doc_id,
            ["my.inner", {'value_inner': 2}],
            durability=self.durability_level,
            timeout=self.sdk_timeout,
            time_unit="seconds",
            create_path=True,
            xattr=self.xattr,
            cas=initial_cas)
        self.assertTrue(failed_items, "Subdoc Xattr insert failed")

        success, failed_items = self.client.crud(
            "subdoc_insert",
            self.doc_id,
            ["my.inner", {'value_inner': 2}],
            durability=self.durability_level,
            timeout=self.sdk_timeout,
            time_unit="seconds",
            create_path=True,
            xattr=self.xattr,
            cas=updated_cas_1)
        self.assertFalse(failed_items, "Subdoc Xattr insert failed")

        # Read and record CAS
        result = self.client.crud("read", self.doc_id)
        self.assertTrue(result["status"], "Read failed")
        updated_cas_2 = result["cas"]

        self.assertTrue(initial_cas != updated_cas_1, "CAS not updated")
        self.assertTrue(updated_cas_1 != updated_cas_2, "CAS not updated")
        self.assertTrue(initial_cas != updated_cas_2, "CAS not updated")

    def test_recreate_xattr(self):
        self.__upsert_document_and_validate("update", {})
        for i in xrange(5):
            self.log.info("Create iteration: %d" % (i+1))
            # Try to upsert a single xattr
            self.__insert_sub_doc_and_validate("subdoc_insert",
                                               "my_attr", "value")

            # Get and validate
            success, failed_item = self.client.crud("subdoc_read",
                                                    self.doc_id,
                                                    "my_attr",
                                                    xattr=self.xattr)
            self.assertFalse(failed_item, "Subdoc read failed")
            self.assertTrue(success[self.doc_id]["cas"] != 0, "CAS is zero")

            # Delete sub_doc
            success, failed_item = self.client.crud("subdoc_delete",
                                                    self.doc_id,
                                                    "my_attr",
                                                    xattr=self.xattr)
            self.assertFalse(failed_item, "Subdoc delete failed")
            self.assertTrue(success[self.doc_id]["cas"] != 0, "CAS is zero")

            # Get and validate
            success, _ = self.client.crud("subdoc_read",
                                          self.doc_id,
                                          "my_attr",
                                          xattr=self.xattr)
            self.assertEqual(
                success["xattrs"]["value"][0],
                "PATH_NOT_FOUND",
                "Invalid SDK return value: %s" % success["xattrs"]["value"])

    def test_update_xattr(self):
        self.__upsert_document_and_validate("update", {})
        # use xattr like a counters
        for i in xrange(5):
            self.log.info("Update iteration: %d" % (i+1))
            self.__insert_sub_doc_and_validate("subdoc_upsert",
                                               "my_attr", i)

            success, _ = self.client.crud("subdoc_read",
                                          self.doc_id,
                                          "my_attr",
                                          xattr=self.xattr)
            self.assertTrue(success, "Subdoc read failed")
            self.assertEqual(success[self.doc_id]["value"][0], i,
                             "Mismatch in value")

    def test_delete_child_xattr(self):
        k = 'xattrs'

        self.client.upsert(k, {})

        rv = self.client.mutate_in(k, SD.upsert('my.attr', 'value',
                                                xattr=True,
                                                create_parents=True))
        self.assertTrue(rv.success)

        rv = self.client.mutate_in(k, SD.remove('my.attr', xattr=True))
        self.assertTrue(rv.success)
        rv = self.client.lookup_in(k, SD.get('my.attr', xattr=True))
        self.assertFalse(rv.exists('my.attr'))

        rv = self.client.lookup_in(k, SD.get('my', xattr=True))
        self.assertTrue(rv.exists('my'))
        self.assertEquals({}, rv['my'])

    def test_delete_xattr_key_from_parent(self):
        k = 'xattrs'

        self.client.upsert(k, {})

        self.client.mutate_in(k, SD.upsert('my', {'value': 1},
                                           xattr=True))
        rv = self.client.mutate_in(k, SD.upsert('my.inner', {'value_inner': 2},
                                                xattr=True))
        self.assertTrue(rv.success)

        rv = self.client.lookup_in(k, SD.get('my', xattr=True))
        self.assertTrue(rv.exists('my'))
        self.assertEqual({u'inner': {u'value_inner': 2}, u'value': 1}, rv['my'])

        rv = self.client.mutate_in(k, SD.remove('my.inner', xattr=True))
        self.assertTrue(rv.success)

        rv = self.client.lookup_in(k, SD.get('my.inner', xattr=True))
        self.assertFalse(rv.exists('my.inner'))

        rv = self.client.lookup_in(k, SD.get('my', xattr=True))
        self.assertTrue(rv.exists('my'))
        self.assertEqual({u'value': 1}, rv['my'])

    def test_delete_xattr_parent(self):
        k = 'xattrs'

        self.client.upsert(k, {})

        self.client.mutate_in(k, SD.upsert('my', {'value': 1},
                                           xattr=True))
        rv = self.client.mutate_in(k, SD.upsert('my.inner', {'value_inner': 2},
                                                xattr=True))
        self.assertTrue(rv.success)

        rv = self.client.lookup_in(k, SD.get('my', xattr=True))
        self.assertTrue(rv.exists('my'))
        self.assertEqual({u'inner': {u'value_inner': 2}, u'value': 1}, rv['my'])

        rv = self.client.mutate_in(k, SD.remove('my', xattr=True))
        self.assertTrue(rv.success)

        rv = self.client.lookup_in(k, SD.get('my', xattr=True))
        self.assertFalse(rv.exists('my'))

        rv = self.client.lookup_in(k, SD.get('my.inner', xattr=True))
        self.assertFalse(rv.exists('my.inner'))

    def test_xattr_value_none(self):
        k = 'xattrs'

        self.client.upsert(k, None)

        rv = self.client.mutate_in(k, SD.upsert('my_attr', None,
                                                xattr=True,
                                                create_parents=True))
        self.assertTrue(rv.success)

        body = self.client.get(k)
        self.assertEquals(None, body.value)

        rv = self.client.lookup_in(k, SD.get('my_attr', xattr=True))
        self.assertTrue(rv.exists('my_attr'))
        self.assertEqual(None, rv['my_attr'])

    def test_xattr_delete_not_existing(self):
        k = 'xattrs'

        self.client.upsert(k, {})

        self.client.mutate_in(k, SD.upsert('my', 1,
                                           xattr=True))
        try:
            self.client.mutate_in(k, SD.remove('not_my', xattr=True))
            self.fail("operation to delete non existing key should be failed")
        except SubdocPathNotFoundError:
            pass

    def test_insert_list(self):
        k = 'xattrs'

        self.client.upsert(k, {})

        # Try to upsert a single xattr
        rv = self.client.mutate_in(k, SD.upsert('my_attr', [1, 2, 3],
                                                xattr=True))
        self.assertTrue(rv.success)

        # trying get
        body = self.client.get(k)
        self.assertTrue(body.value == {})

        # Using lookup_in
        rv = self.client.retrieve_in(k, 'my_attr')
        self.assertFalse(rv.success)
        self.assertFalse(rv.exists('my_attr'))

        # Finally, use lookup_in with 'xattrs' attribute enabled
        rv = self.client.lookup_in(k, SD.get('my_attr', xattr=True))
        self.assertTrue(rv.exists('my_attr'))
        self.assertEqual([1, 2, 3], rv['my_attr'])

    # https://issues.couchbase.com/browse/PYCBC-381
    def test_insert_integer_as_key(self):
        k = 'xattr'

        self.client.upsert(k, {})

        rv = self.client.mutate_in(k, SD.upsert('integer_extra', 1,
                                                xattr=True))
        self.assertTrue(rv.success)

        rv = self.client.mutate_in(k, SD.upsert('integer', 2,
                                                xattr=True))
        self.assertTrue(rv.success)

        body = self.client.get(k)
        self.assertTrue(body.value == {})

        rv = self.client.retrieve_in(k, 'integer')
        self.assertFalse(rv.success)
        self.assertFalse(rv.exists('integer'))

        rv = self.client.lookup_in(k, SD.get('integer', xattr=True))
        self.assertTrue(rv.exists('integer'))
        self.assertEqual(2, rv['integer'])

    # https://issues.couchbase.com/browse/PYCBC-381
    def test_insert_double_as_key(self):
        k = 'xattr'

        self.client.upsert(k, {})

        rv = self.client.mutate_in(k, SD.upsert('double_extra', 1.0,
                                                xattr=True))
        self.assertTrue(rv.success)

        rv = self.client.mutate_in(k, SD.upsert('double', 2.0,
                                                xattr=True))
        self.assertTrue(rv.success)

        body = self.client.get(k)
        self.assertTrue(body.value == {})

        rv = self.client.retrieve_in(k, 'double')
        self.assertFalse(rv.success)
        self.assertFalse(rv.exists('double'))

        rv = self.client.lookup_in(k, SD.get('double', xattr=True))
        self.assertTrue(rv.exists('double'))
        self.assertEqual(2, rv['double'])

    # https://issues.couchbase.com/browse/MB-22691
    def test_multiple_xattrs(self):
        key = 'xattr'

        self.client.upsert(key, {})

        values = {
            'array_mixed': [0, 299792458.0, 1.1],
            'integer_negat': -1,
            'date_time': '2012-10-03 15:35:46.461491',
            'float': 299792458.0,
            'arr_ints': [1, 2, 3, 4, 5],
            'integer_pos': 1,
            'array_arrays': [[299792458.0, 299792458.0, 299792458.0], [0, 299792458.0, 1.1], [], [0, 0, 0]],
            'add_integer': 0,
            'json': {'not_to_bes_tested_string_field1': 'not_to_bes_tested_string'},
            'string_empty': '',
            'simple_up_c': "ABCDEFGHIJKLMNOPQRSTUVWXZYZ",
            'a_add_int': [0, 1],
            'array_floats': [299792458.0, 299792458.0, 299792458.0],
            'integer_big': 1038383839293939383938393,
            'a_sub_int': [0, 1],
            'double_s': 1.1,
            'simple_low_c': "abcdefghijklmnoprestuvxyz",
            'special_chrs': "_-+!#@$%&*(){}\][;.,<>?/",
            'array_double': [1.1, 2.2, 3.3, 4.4, 5.5],
            'sub_integer': 1,
            'double_z': 0.0,
            'add_int': 0,
        }

        size = 0
        for k, v in values.iteritems():
            self.log.info("adding xattr '%s': %s" % (k, v))
            rv = self.client.mutate_in(key, SD.upsert(k, v,
                                                      xattr=True))
            self.log.info("xattr '%s' added successfully?: %s" % (k, rv.success))
            self.assertTrue(rv.success)

            rv = self.client.lookup_in(key, SD.exists(k, xattr=True))
            self.log.info("xattr '%s' exists?: %s" % (k, rv.success))
            self.assertTrue(rv.success)

            size += sys.getsizeof(k) + sys.getsizeof(v)

            rv = self.client.lookup_in(key, SD.get(k, xattr=True))
            self.assertTrue(rv.exists(k))
            self.assertEqual(v, rv[k])
            self.log.info("~ Total size of xattrs: %s" % size)

    def test_multiple_xattrs2(self):
        key = 'xattr'

        self.client.upsert(key, {})

        size = 0
        for k, v in SubdocXattrSdkTest.VALUES.iteritems():
            self.log.info("adding xattr '%s': %s" % (k, v))
            rv = self.client.mutate_in(key, SD.upsert(k, v,
                                                      xattr=True))
            self.log.info("xattr '%s' added successfully?: %s" % (k, rv.success))
            self.assertTrue(rv.success)

            rv = self.client.lookup_in(key, SD.exists(k, xattr=True))
            self.log.info("xattr '%s' exists?: %s" % (k, rv.success))
            self.assertTrue(rv.success)

            size += sys.getsizeof(k) + sys.getsizeof(v)

            rv = self.client.lookup_in(key, SD.get(k, xattr=True))
            self.assertTrue(rv.exists(k))
            self.assertEqual(v, rv[k])
            self.log.info("~ Total size of xattrs: %s" % size)

    # https://issues.couchbase.com/browse/MB-22691
    def test_check_spec_words(self):
        k = 'xattr'

        self.client.upsert(k, {})
        ok = True

        for key in ('start', 'integer', "in", "int", "double",
                    "for", "try", "as", "while", "else", "end"):
            try:
                self.log.info("using key %s" % key)
                rv = self.client.mutate_in(k, SD.upsert(key, 1,
                                                        xattr=True))
                print rv
                self.assertTrue(rv.success)
                rv = self.client.lookup_in(k, SD.get(key, xattr=True))
                print rv
                self.assertTrue(rv.exists(key))
                self.assertEqual(1, rv[key])
                self.log.info("successfully set xattr with key %s" % key)
            except Exception as e:
                ok = False
                self.log.info("unable to set xattr with key %s" % key)
                print e
        self.assertTrue(ok, "unable to set xattr with some name. See logs above")

    def test_upsert_nums(self):
        k = 'xattr'
        self.client.upsert(k, {})
        for i in xrange(100):
            rv = self.client.mutate_in(k, SD.upsert('n' + str(i), i, xattr=True))
            self.assertTrue(rv.success)
        for i in xrange(100):
            rv = self.client.lookup_in(k, SD.get('n' + str(i), xattr=True))
            self.assertTrue(rv.exists('n' + str(i)))
            self.assertEqual(i, rv['n' + str(i)])

    def test_upsert_order(self):
        k = 'xattr'

        self.client.upsert(k, {})
        rv = self.client.mutate_in(k, SD.upsert('integer', 2, xattr=True))
        self.assertTrue(rv.success)

        self.client.delete(k)
        self.client.upsert(k, {})
        rv = self.client.mutate_in(k, SD.upsert('start_end_extra', 1, xattr=True))
        self.assertTrue(rv.success)
        rv = self.client.mutate_in(k, SD.upsert('integer', 2, xattr=True))
        self.assertTrue(rv.success)

        self.client.delete(k)
        self.client.upsert(k, {})
        rv = self.client.mutate_in(k, SD.upsert('integer_extra', 1, xattr=True))
        self.assertTrue(rv.success)
        rv = self.client.mutate_in(k, SD.upsert('integer', 2, xattr=True))
        self.assertTrue(rv.success)

    def test_xattr_expand_macros_true(self):
        k = 'xattrs'

        self.client.upsert(k, 1)

        rv = self.client.get(k)
        self.assertTrue(rv.success)
        cas_before = rv.cas

        self.client.mutate_in(k, SD.upsert('my', {'value': 1},
                                           xattr=True))
        rv = self.client.get(k)
        self.assertTrue(rv.success)
        cas_after = rv.cas

        self.client.mutate_in(k, SD.upsert('my', '${Mutation.CAS}', _expand_macros=True))

        rv1 = self.client.get(k)
        self.assertTrue(rv1.success)
        cas_after2 = rv1.cas

        self.assertTrue(cas_before != cas_after)
        self.assertTrue(cas_after != cas_after2)

    def test_xattr_expand_macros_false(self):
        k = 'xattrs'

        self.client.upsert(k, 1)

        rv = self.client.get(k)
        self.assertTrue(rv.success)
        cas_before = rv.cas

        self.client.mutate_in(k, SD.upsert('my', {'value': 1},
                                           xattr=True))
        rv = self.client.get(k)
        self.assertTrue(rv.success)
        cas_after = rv.cas

        try:
            self.client.mutate_in(k, SD.upsert('my', '${Mutation.CAS}', _expand_macros=False))
        except Exception as e:
            self.assertEquals(e.all_results['xattrs'].errstr,
                              'Could not execute one or more multi lookups or mutations')
            self.assertEquals(e.rc, 64)

        rv1 = self.client.get(k)
        self.assertTrue(rv1.success)
        cas_after2 = rv1.cas

        self.assertTrue(cas_before != cas_after)
        self.assertTrue(cas_after == cas_after2)

    def test_virt_non_xattr_document_exists(self):
        k = 'xattrs'

        self.client.upsert(k, 1)

        rv = self.client.get(k)
        self.assertTrue(rv.success)
        try:
            self.client.lookup_in(k, SD.exists('$document', xattr=False))
        except Exception as e:
            self.assertEquals(e.all_results['xattrs'].errstr,
                              'Could not execute one or more multi lookups or mutations')
            self.assertEquals(e.rc, 64)
        else:
            self.fail("was able to lookup_in $document with xattr=False")

    def test_virt_xattr_document_exists(self):
        k = 'xattrs'

        self.client.upsert(k, 1)

        rv = self.client.get(k)
        self.assertTrue(rv.success)

        rv = self.client.lookup_in(k, SD.exists('$document', xattr=True))

        self.assertTrue(rv.exists('$document'))
        self.assertEqual(None, rv['$document'])

    def test_virt_xattr_not_exists(self):
        k = 'xattrs'

        self.client.upsert(k, 1)

        rv = self.client.get(k)
        self.assertTrue(rv.success)
        for vxattr in ['$xattr', '$document1', '$', '$1']:
            try:
                self.client.lookup_in(k, SD.exists(vxattr, xattr=True))
            except Exception as e:
                self.assertEqual(e.message, 'Operational Error')
                self.assertEqual(e.result.errstr,
                                 'The server replied with an unrecognized status code. '
                                 'A newer version of this library may be able to decode it')
            else:
                self.fail("was able to get invalid vxattr?")

    def test_virt_xattr_document_modify(self):
        k = 'xattrs'

        self.client.upsert(k, 1)

        rv = self.client.get(k)
        self.assertTrue(rv.success)
        try:
            self.client.mutate_in(k, SD.upsert('$document', {'value': 1}, xattr=True))
        except Exception as e:
            self.assertEqual(e.message, 'Subcommand failure')
            # self.assertEqual(e.result.errstr,
            #                  'The server replied with an unrecognized status code. '
            #                  'A newer version of this library may be able to decode it')
        else:
            self.fail("was able to modify $document vxattr?")

    def test_virt_xattr_document_remove(self):
        k = 'xattrs'

        self.client.upsert(k, 1)

        rv = self.client.get(k)
        self.assertTrue(rv.success)
        try:
            self.client.lookup_in(k, SD.remove('$document', xattr=True))
        except Exception as e:
            self.assertEqual(e.message, 'Subcommand failure')
            # self.assertEqual(e.result.errstr,
            #                  'The server replied with an unrecognized status code. '
            #                  'A newer version of this library may be able to decode it')
        else:
            self.fail("was able to delete $document vxattr?")

    # https://issues.couchbase.com/browse/MB-23085
    def test_default_view_mixed_docs_meta_first(self):
        k = 'xattr'

        self.client.upsert(k, {"xattr": True})
        self.client.mutate_in(k, SD.upsert('integer', 2, xattr=True))

        k = 'not_xattr'
        self.client.upsert(k, {"xattr": False})

        default_map_func = "function (doc, meta) {emit(meta.id, null);}"
        default_view_name = ("xattr", "default_view")[False]
        view = View(default_view_name, default_map_func, None, False)

        ddoc_name = "ddoc1"
        tasks = self.async_create_views(self.master, ddoc_name, [view], self.buckets[0].name)
        for task in tasks:
            task.result()
        rest = RestConnection(self.master)
        query = {"stale": "false", "full_set": "true", "connection_timeout": 60000}

        result = rest.query_view(ddoc_name, view.name, self.buckets[0].name, query)
        self.assertEqual(result['total_rows'], 2, "2 document should be returned")
        self.assertEqual(result['rows'][0], {u'value': None, u'id': u'not_xattr', u'key': u'not_xattr'})
        self.assertEqual(result['rows'][1], {u'value': None, u'id': u'xattr', u'key': u'xattr'})

    # https://issues.couchbase.com/browse/MB-23085
    def test_default_view_mixed_docs(self):
        k = 'xattr'

        self.client.upsert(k, {"xattr": True})
        self.client.mutate_in(k, SD.upsert('integer', 2, xattr=True))

        k = 'not_xattr'
        self.client.upsert(k, {"xattr": False})

        default_map_func = "function (doc, meta) {emit(doc, meta.id );}"
        default_view_name = ("xattr", "default_view")[False]
        view = View(default_view_name, default_map_func, None, False)

        ddoc_name = "ddoc1"
        tasks = self.async_create_views(self.master, ddoc_name, [view], self.buckets[0].name)
        for task in tasks:
            task.result()
        rest = RestConnection(self.master)
        query = {"stale": "false", "full_set": "true", "connection_timeout": 60000}

        result = rest.query_view(ddoc_name, view.name, self.buckets[0].name, query)
        self.assertEqual(result['total_rows'], 2, "2 document should be returned")
        self.assertEqual(result['rows'][0], {u'value': u'not_xattr', u'id': u'not_xattr', u'key': {u'xattr': False}})
        self.assertEqual(result['rows'][1], {u'value': u'xattr', u'id': u'xattr', u'key': {u'xattr': True}})

    def test_view_one_xattr(self):
        k = 'xattr'

        self.client.upsert(k, {"xattr": True})
        self.client.mutate_in(k, SD.upsert('integer', 2, xattr=True))

        default_map_func = "function (doc, meta) {emit(doc, meta.xattrs.integer);}"
        default_view_name = ("xattr", "default_view")[False]
        view = View(default_view_name, default_map_func, None, False)

        ddoc_name = "ddoc1"
        tasks = self.async_create_views(self.master, ddoc_name, [view], self.buckets[0].name)
        for task in tasks:
            task.result()
        rest = RestConnection(self.master)
        query = {"stale": "false", "full_set": "true", "connection_timeout": 60000}

        result = rest.query_view(ddoc_name, view.name, self.buckets[0].name, query)
        self.assertEqual(result['total_rows'], 1, "1 document should be returned")
        self.assertEqual(result['rows'][0], {u'value': 2, u'id': u'xattr', u'key': {u'xattr': True}})

    def test_view_one_xattr_index_xattr_on_deleted_docs(self):
        k = 'xattr'

        self.client.upsert(k, {"xattr": True})
        self.client.mutate_in(k, SD.upsert('integer', 2, xattr=True))

        shell = RemoteMachineShellConnection(self.master)
        shell.execute_command("""echo '{
    "views" : {
        "view1": {
             "map" : "function(doc, meta){emit(meta.id, null);}"
        }
    },
    "index_xattr_on_deleted_docs" : true
    }' > /tmp/views_def.json""")
        o, e = shell.execute_command(
            "curl -X PUT -H 'Content-Type: application/json' http://Administrator:password@127.0.0.1:8092/default/_design/ddoc1 -d @/tmp/views_def.json")
        self.log.info(o)
        rest = RestConnection(self.master)
        query = {"stale": "false", "full_set": "true", "connection_timeout": 60000}

        result = rest.query_view('ddoc1', 'view1', self.buckets[0].name, query)
        self.assertEqual(result['total_rows'], 1, "1 document should be returned")
        self.assertEqual(result['rows'][0], {u'value': 2, u'id': u'xattr', u'key': {u'xattr': True}})

    def test_view_all_xattrs(self):
        k = 'xattr'

        self.client.upsert(k, {"xattr": True})
        self.client.mutate_in(k, SD.upsert('integer', 2, xattr=True))

        default_map_func = "function (doc, meta) {emit(doc, meta.xattrs);}"
        default_view_name = ("xattr", "default_view")[False]
        view = View(default_view_name, default_map_func, None, False)

        ddoc_name = "ddoc1"
        tasks = self.async_create_views(self.master, ddoc_name, [view], self.buckets[0].name)
        for task in tasks:
            task.result()
        rest = RestConnection(self.master)
        query = {"stale": "false", "full_set": "true", "connection_timeout": 60000}

        result = rest.query_view(ddoc_name, view.name, self.buckets[0].name, query)
        self.assertEqual(result['total_rows'], 1, "1 document should be returned")
        self.assertEqual(result['rows'][0], {u'value': {u'integer': 2}, u'id': u'xattr', u'key': {u'xattr': True}})

    def test_view_all_docs_only_meta(self):
        k = 'xattr'

        self.client.upsert(k, {"xattr": True})

        default_map_func = "function (doc, meta) {emit(meta.xattrs);}"
        default_view_name = ("xattr", "default_view")[False]
        view = View(default_view_name, default_map_func, None, False)

        ddoc_name = "ddoc1"
        tasks = self.async_create_views(self.master, ddoc_name, [view], self.buckets[0].name)
        for task in tasks:
            task.result()
        rest = RestConnection(self.master)
        query = {"stale": "false", "full_set": "true", "connection_timeout": 60000}

        result = rest.query_view(ddoc_name, view.name, self.buckets[0].name, query)
        self.assertEqual(result['total_rows'], 1, "1 document should be returned")
        self.assertEqual(result['rows'][0], {u'value': None, u'id': u'xattr', u'key': {}})

    def test_view_all_docs_without_xattrs(self):
        k = 'xattr'

        self.client.upsert(k, {"xattr": True})

        default_map_func = "function (doc, meta) {emit(doc, meta.xattrs);}"
        default_view_name = ("xattr", "default_view")[False]
        view = View(default_view_name, default_map_func, None, False)

        ddoc_name = "ddoc1"
        tasks = self.async_create_views(self.master, ddoc_name, [view], self.buckets[0].name)
        for task in tasks:
            task.result()
        rest = RestConnection(self.master)
        query = {"stale": "false", "full_set": "true", "connection_timeout": 60000}

        result = rest.query_view(ddoc_name, view.name, self.buckets[0].name, query)
        self.assertEqual(result['total_rows'], 1, "1 document should be returned")
        self.assertEqual(result['rows'][0], {u'value': {}, u'id': u'xattr', u'key': {u'xattr': True}})

    def test_view_all_docs_without_xattrs_only_meta(self):
        k = 'xattr'

        self.client.upsert(k, {"xattr": True})

        default_map_func = "function (doc, meta) {emit(doc, meta.xattrs);}"
        default_view_name = ("xattr", "default_view")[False]
        view = View(default_view_name, default_map_func, None, False)

        ddoc_name = "ddoc1"
        tasks = self.async_create_views(self.master, ddoc_name, [view], self.buckets[0].name)
        for task in tasks:
            task.result()
        rest = RestConnection(self.master)
        query = {"stale": "false", "full_set": "true", "connection_timeout": 60000}

        result = rest.query_view(ddoc_name, view.name, self.buckets[0].name, query)
        self.assertEqual(result['total_rows'], 1, "1 document should be returned")
        self.assertEqual(result['rows'][0], {u'value': {}, u'id': u'xattr', u'key': {u'xattr': True}})

    def test_view_xattr_not_exist(self):
        k = 'xattr'

        self.client.upsert(k, {"xattr": True})
        self.client.mutate_in(k, SD.upsert('integer', 2, xattr=True))

        default_map_func = "function (doc, meta) {emit(doc, meta.xattrs.fakeee);}"
        default_view_name = ("xattr", "default_view")[False]
        view = View(default_view_name, default_map_func, None, False)

        ddoc_name = "ddoc1"
        tasks = self.async_create_views(self.master, ddoc_name, [view], self.buckets[0].name)
        for task in tasks:
            task.result()
        rest = RestConnection(self.master)
        query = {"stale": "false", "full_set": "true", "connection_timeout": 60000}

        result = rest.query_view(ddoc_name, view.name, self.buckets[0].name, query)
        self.assertEqual(result['total_rows'], 1, "1 document should be returned")
        self.assertEqual(result['rows'][0], {u'value': None, u'id': u'xattr', u'key': {u'xattr': True}})

    def test_view_all_xattrs_inner_json(self):
        k = 'xattr'

        self.client.upsert(k, {"xattr": True})
        self.client.mutate_in(k, SD.upsert('big', SubdocXattrSdkTest.VALUES, xattr=True))

        default_map_func = "function (doc, meta) {emit(doc, meta.xattrs);}"
        default_view_name = ("xattr", "default_view")[False]
        view = View(default_view_name, default_map_func, None, False)

        ddoc_name = "ddoc1"
        tasks = self.async_create_views(self.master, ddoc_name, [view], self.buckets[0].name)
        for task in tasks:
            task.result()
        rest = RestConnection(self.master)
        query = {"stale": "false", "full_set": "true", "connection_timeout": 60000}

        result = rest.query_view(ddoc_name, view.name, self.buckets[0].name, query)
        self.assertEqual(result['total_rows'], 1, "1 document should be returned")
        self.assertEqual(result['rows'][0],
                         {u'value': {
                             u'big': {u'u_c': u'ABCDEFGHIJKLMNOPQRSTUVWXZYZ', u'low_case': u'abcdefghijklmnoprestuvxyz',
                                      u'int_big': 1.0383838392939393e+24, u'double_z': 0, u'arr_ints': [1, 2, 3, 4, 5],
                                      u'int_posit': 1, u'int_zero': 0, u'arr_floa': [299792458, 299792458, 299792458],
                                      u'float': 299792458, u'float_neg': -299792458, u'double_s': 1.1,
                                      u'arr_mixed': [0, 299792458, 1.1], u'double_n': -1.1, u'str_empty': u'',
                                      u'a_doubles': [1.1, 2.2, 3.3, 4.4, 5.5], u'd_time': u'2012-10-03 15:35:46.461491',
                                      u'arr_arrs': [[299792458, 299792458, 299792458], [0, 299792458, 1.1], [],
                                                    [0, 0, 0]], u'int_neg': -1,
                                      u'spec_chrs': u'_-+!#@$%&*(){}\\][;.,<>?/',
                                      u'json': {u'not_to_bes_tested_string_field1': u'not_to_bes_tested_string'}}},
                             u'id': u'xattr', u'key': {u'xattr': True}})

    def test_view_all_xattrs_many_items(self):
        key = 'xattr'

        self.client.upsert(key, {"xattr": True})
        for k, v in SubdocXattrSdkTest.VALUES.iteritems():
            self.client.mutate_in(key, SD.upsert(k, v, xattr=True))

        default_map_func = "function (doc, meta) {emit(doc, meta.xattrs);}"
        default_view_name = ("xattr", "default_view")[False]
        view = View(default_view_name, default_map_func, None, False)

        ddoc_name = "ddoc1"
        tasks = self.async_create_views(self.master, ddoc_name, [view], self.buckets[0].name)
        for task in tasks:
            try:
                task.result()
            except DesignDocCreationException:
                if self.bucket_type == Bucket.Type.EPHEMERAL:
                    return True
                else:
                    raise

        rest = RestConnection(self.master)
        query = {"stale": "false", "full_set": "true", "connection_timeout": 60000}

        result = rest.query_view(ddoc_name, view.name, self.buckets[0].name, query)
        self.assertEqual(result['total_rows'], 1, "1 document should be returned")
        self.assertEqual(result['rows'][0], {u'value': {u'u_c': u'ABCDEFGHIJKLMNOPQRSTUVWXZYZ',
                                                        u'low_case': u'abcdefghijklmnoprestuvxyz',
                                                        u'int_big': 1.0383838392939393e+24, u'double_z': 0,
                                                        u'arr_ints': [1, 2, 3, 4, 5], u'int_posit': 1,
                                                        u'int_zero': 0, u'arr_floa': [299792458, 299792458, 299792458],
                                                        u'float': 299792458, u'float_neg': -299792458, u'double_s': 1.1,
                                                        u'arr_mixed': [0, 299792458, 1.1], u'double_n': -1.1,
                                                        u'str_empty': u'', u'a_doubles': [1.1, 2.2, 3.3, 4.4, 5.5],
                                                        u'd_time': u'2012-10-03 15:35:46.461491',
                                                        u'arr_arrs': [[299792458, 299792458, 299792458],
                                                                      [0, 299792458, 1.1], [], [0, 0, 0]],
                                                        u'int_neg': -1, u'spec_chrs': u'_-+!#@$%&*(){}\\][;.,<>?/',
                                                        u'json': {u'not_to_bes_tested_string_field1':
                                                                      u'not_to_bes_tested_string'}},
                                             u'id': u'xattr', u'key': {u'xattr': True}})

    def test_view_all_xattrs_many_items_index_xattr_on_deleted_docs(self):
        key = 'xattr'

        self.client.upsert(key, {"xattr": True})
        for k, v in SubdocXattrSdkTest.VALUES.iteritems():
            self.client.mutate_in(key, SD.upsert(k, v, xattr=True))

        shell = RemoteMachineShellConnection(self.master)
        shell.execute_command("""echo '{
        "views" : {
            "view1": {
                 "map" : "function(doc, meta){emit(doc, meta.xattrs);}"
            }
        },
        "index_xattr_on_deleted_docs" : true
        }' > /tmp/views_def.json""")
        o, _ = shell.execute_command(
                "curl -X PUT -H 'Content-Type: application/json' http://Administrator:password@127.0.0.1:8092/default/_design/ddoc1 -d @/tmp/views_def.json")
        self.log.info(o)

        ddoc_name = "ddoc1"
        rest = RestConnection(self.master)
        query = {"stale": "false", "full_set": "true", "connection_timeout": 60000}

        result = rest.query_view(ddoc_name, "view1", self.buckets[0].name, query)
        self.assertEqual(result['total_rows'], 1, "1 document should be returned")
        self.assertEqual(result['rows'][0], {u'value': {u'u_c': u'ABCDEFGHIJKLMNOPQRSTUVWXZYZ',
                                                        u'low_case': u'abcdefghijklmnoprestuvxyz',
                                                        u'int_big': 1.0383838392939393e+24, u'double_z': 0,
                                                        u'arr_ints': [1, 2, 3, 4, 5], u'int_posit': 1,
                                                        u'int_zero': 0, u'arr_floa': [299792458, 299792458, 299792458],
                                                        u'float': 299792458, u'float_neg': -299792458, u'double_s': 1.1,
                                                        u'arr_mixed': [0, 299792458, 1.1], u'double_n': -1.1,
                                                        u'str_empty': u'', u'a_doubles': [1.1, 2.2, 3.3, 4.4, 5.5],
                                                        u'd_time': u'2012-10-03 15:35:46.461491',
                                                        u'arr_arrs': [[299792458, 299792458, 299792458],
                                                                      [0, 299792458, 1.1], [], [0, 0, 0]],
                                                        u'int_neg': -1, u'spec_chrs': u'_-+!#@$%&*(){}\\][;.,<>?/',
                                                        u'json': {u'not_to_bes_tested_string_field1':
                                                                      u'not_to_bes_tested_string'}},
                                             u'id': u'xattr', u'key': {u'xattr': True}})

    def test_reboot_node(self):
        key = 'xattr'

        self.client.upsert(key, {})

        for k, v in SubdocXattrSdkTest.VALUES.iteritems():
            self.log.info("adding xattr '%s': %s" % (k, v))
            rv = self.client.mutate_in(key, SD.upsert(k, v,
                                                      xattr=True))
            self.log.info("xattr '%s' added successfully?: %s" % (k, rv.success))
            self.assertTrue(rv.success)

            rv = self.client.lookup_in(key, SD.exists(k, xattr=True))
            self.log.info("xattr '%s' exists?: %s" % (k, rv.success))
            self.assertTrue(rv.success)

        shell = RemoteMachineShellConnection(self.master)
        shell.stop_couchbase()
        self.sleep(2)
        shell.start_couchbase()
        self.sleep(20)

        if self.bucket_type == Bucket.Type.EPHEMERAL:
            try:
                self.assertFalse(self.client.get(key).success)
                self.fail("get should throw NotFoundError when doc deleted")
            except NotFoundError:
                pass
        else:
            for k, v in SubdocXattrSdkTest.VALUES.iteritems():
                rv = self.client.lookup_in(key, SD.get(k, xattr=True))
                self.assertTrue(rv.exists(k))
                self.assertEqual(v, rv[k])

    def test_use_persistence(self):
        k = 'xattrs'

        self.client.upsert(k, 1)

        rv = self.client.get(k)
        self.assertTrue(rv.success)
        cas_before = rv.cas

        try:
            self.client.mutate_in(k, SD.upsert('my', {'value': 1},
                                               xattr=True), persist_to=1)
        except:
            if self.bucket_type == Bucket.Type.EPHEMERAL:
                return
            else:
                raise
        rv = self.client.get(k)
        self.assertTrue(rv.success)
        cas_after = rv.cas

        try:
            self.client.mutate_in(k, SD.upsert('my.inner', {'value_inner': 2},
                                               xattr=True), cas=cas_before, persist_to=1)
            self.fail("upsert with wrong cas!")
        except KeyExistsError:
            pass

        self.client.mutate_in(k, SD.upsert('my.inner', {'value_inner': 2},
                                           xattr=True), cas=cas_after, persist_to=1)
        rv = self.client.get(k)
        self.assertTrue(rv.success)
        cas_after2 = rv.cas

        self.assertTrue(cas_before != cas_after)
        self.assertTrue(cas_after != cas_after2)


class SubdocXattrDurabilityTest(SubdocBaseTest):
    def setUp(self):
        super(SubdocXattrDurabilityTest, self).setUp()
        self.xattr = self.input.param("xattr", True)
        self.doc_id = 'xattrs'
        self.client = SDKClient([self.cluster.master],
                                self.bucket_util.buckets[0],
                                scope=self.scope_name,
                                collection=self.collection_name,
                                compression_settings=self.sdk_compression)

    def tearDown(self):
        # Close the SDK connections
        self.client.close()
        super(SubdocXattrDurabilityTest, self).tearDown()

    def test_durability_impossible(self):
        # Create document without durability
        result = self.client.crud(DocLoading.Bucket.DocOps.CREATE,
                                  self.doc_id, {"test": "val"},
                                  timeout=10)
        self.assertTrue(result["status"], "Doc create failed")

        # Trying creating a subdoc without enough kv nodes
        success, failed_items = self.client.crud(
            "subdoc_insert",
            self.doc_id,
            ["my_attr", "value"],
            xattr=self.xattr,
            durability=self.durability_level)
        sdk_error = str(failed_items[self.doc_id]["error"])
        self.assertTrue(failed_items, "Subdoc CRUD succeeded: %s" % success)
        self.assertTrue(SDKException.DurabilityImpossibleException
                        in sdk_error, "Invalid exception: %s" % sdk_error)

    def test_doc_sync_write_in_progress(self):
        shell = None
        doc_tasks = [DocLoading.Bucket.DocOps.CREATE,
                     DocLoading.Bucket.DocOps.UPDATE,
                     DocLoading.Bucket.DocOps.REPLACE,
                     DocLoading.Bucket.DocOps.DELETE]
        basic_ops = [DocLoading.Bucket.DocOps.CREATE,
                     DocLoading.Bucket.DocOps.UPDATE,
                     "subdoc_insert", "subdoc_upsert",
                     "subdoc_replace", "subdoc_delete",
                     DocLoading.Bucket.DocOps.DELETE]
        doc_gen = dict()
        doc_gen["doc_crud"] = doc_generator(self.doc_id, 0, 1)

        doc_key = doc_gen["doc_crud"].next()[0]
        target_vb = self.bucket_util.get_vbucket_num_for_key(doc_key)

        # Reset it back to start index
        doc_gen["doc_crud"].reset()

        for node in self.cluster_util.get_kv_nodes():
            shell = RemoteMachineShellConnection(node)
            cbstat_obj = Cbstats(shell)
            replica_vbs = cbstat_obj.vbucket_list(
                self.bucket_util.buckets[0],
                "replica")
            if target_vb in replica_vbs:
                break

            shell.disconnect()

        error_sim = CouchbaseError(self.log, shell)

        for op_type in [DocLoading.Bucket.DocOps.CREATE,
                        DocLoading.Bucket.DocOps.UPDATE,
                        DocLoading.Bucket.DocOps.DELETE]:
            sync_write_task = self.task.async_load_gen_docs(
                self.cluster, self.bucket_util.buckets[0],
                doc_gen["doc_crud"], op_type,
                scope=self.scope_name,
                collection=self.collection_name,
                batch_size=1,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                print_ops_rate=False,
                task_identifier="sw_docTask",
                start_task=False)

            doc_cas = self.client.crud(DocLoading.Bucket.DocOps.READ,
                                       doc_key)["cas"]

            error_sim.create(CouchbaseError.STOP_MEMCACHED)
            self.task_manager.add_new_task(sync_write_task)
            self.sleep(5, "Wait for doc_op task to start")

            for sw_test_op in basic_ops + [DocLoading.Bucket.DocOps.REPLACE]:
                self.log.info("Testing %s over %s" % (sw_test_op, op_type))
                value = "test_val"
                if sw_test_op not in doc_tasks:
                    value = ["exists_path", "0"]
                    if sw_test_op in ["subdoc_insert"]:
                        value = ["non_exists_path", "val"]
                    if sw_test_op in ["subdoc_delete"]:
                        value = "exists_path"

                result = self.client.crud(sw_test_op, doc_key, value,
                                          durability=self.durability_level,
                                          timeout=3, time_unit="seconds",
                                          create_path=True,
                                          xattr=self.xattr,
                                          fail_fast=True)
                if sw_test_op not in doc_tasks:
                    result = result[1][doc_key]

                sdk_exception = str(result["error"])
                expected_exception = \
                    SDKException.RequestCanceledException
                retry_reason = SDKException.RetryReason \
                    .KV_SYNC_WRITE_IN_PROGRESS_NO_MORE_RETRIES
                if op_type == DocLoading.Bucket.DocOps.CREATE:
                    if sw_test_op in [DocLoading.Bucket.DocOps.DELETE,
                                      DocLoading.Bucket.DocOps.REPLACE] \
                            or (sw_test_op not in doc_tasks):
                        expected_exception = \
                            SDKException.DocumentNotFoundException
                        retry_reason = None
                elif sw_test_op not in doc_tasks:
                    expected_exception = \
                        SDKException.AmbiguousTimeoutException
                    retry_reason = \
                        SDKException.RetryReason.KV_SYNC_WRITE_IN_PROGRESS
                if expected_exception not in sdk_exception:
                    self.log_failure("Invalid exception: %s" % result)
                elif retry_reason is not None \
                        and retry_reason not in sdk_exception:
                    self.log_failure("Retry reason missing: %s" % result)

                # Validate CAS doesn't change after sync_write failure
                curr_cas = self.client.crud(DocLoading.Bucket.DocOps.READ,
                                            doc_key)["cas"]
                if curr_cas != doc_cas:
                    self.log_failure("CAS mismatch. %s != %s"
                                     % (curr_cas, doc_cas))
            error_sim.revert(CouchbaseError.STOP_MEMCACHED)
            self.task_manager.get_task_result(sync_write_task)
            if op_type != DocLoading.Bucket.DocOps.DELETE:
                self.client.crud("subdoc_insert",
                                 doc_key, ["exists_path", 1],
                                 durability=self.durability_level,
                                 timeout=3, time_unit="seconds",
                                 create_path=True,
                                 xattr=self.xattr)

        # Closing the shell connection
        shell.disconnect()
        self.validate_test_failure()

    def test_subdoc_sync_write_in_progress(self):
        shell = None
        doc_gen = dict()
        doc_key = doc_generator(self.doc_id, 0, 1).next()[0]
        target_vb = self.bucket_util.get_vbucket_num_for_key(doc_key)

        for node in self.cluster_util.get_kv_nodes():
            shell = RemoteMachineShellConnection(node)
            cbstat_obj = Cbstats(shell)
            replica_vbs = cbstat_obj.vbucket_list(
                self.bucket_util.buckets[0],
                "replica")
            if target_vb in replica_vbs:
                break

            shell.disconnect()

        error_sim = CouchbaseError(self.log, shell)

        self.client.crud(DocLoading.Bucket.DocOps.CREATE, doc_key, "{}",
                         timeout=3, time_unit="seconds")
        self.client.crud("subdoc_insert",
                         doc_key, ["exists_path", 1],
                         durability=self.durability_level,
                         timeout=3, time_unit="seconds",
                         create_path=True,
                         xattr=self.xattr)

        sub_doc_op_dict = dict()
        sub_doc_op_dict["insert"] = "subdoc_insert"
        sub_doc_op_dict["upsert"] = "subdoc_upsert"
        sub_doc_op_dict["replace"] = "subdoc_replace"
        sub_doc_op_dict["remove"] = "subdoc_delete"

        for op_type in sub_doc_op_dict.keys():
            doc_gen[op_type] = sub_doc_generator(self.doc_id, 0, 1,
                                                 key_size=self.key_size)
            doc_gen[op_type].template = '{{ "new_value": "value" }}'

        for op_type in sub_doc_op_dict.keys():
            self.log.info("Testing SyncWriteInProgress with %s" % op_type)
            value = ["new_path", "new_value"]
            if op_type != DocLoading.Bucket.SubDocOps.INSERT:
                doc_gen[op_type].template = '{{ "exists_path": 0 }}'
                if op_type == DocLoading.Bucket.SubDocOps.REMOVE:
                    value = "exists_path"
                else:
                    value = ["exists_path", [0, 1]]
            sync_write_task = self.task.async_load_gen_sub_docs(
                self.cluster, self.bucket_util.buckets[0],
                doc_gen[op_type], op_type,
                scope=self.scope_name,
                collection=self.collection_name,
                path_create=True,
                xattr=self.xattr,
                batch_size=1,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                print_ops_rate=False,
                task_identifier="sw_subdocTask",
                start_task=False)

            doc_cas = self.client.crud(DocLoading.Bucket.DocOps.READ,
                                       doc_key)["cas"]

            error_sim.create(CouchbaseError.STOP_MEMCACHED)
            self.task_manager.add_new_task(sync_write_task)
            self.sleep(5, "Wait for doc_op task to start")

            _, failed_item = self.client.crud(sub_doc_op_dict[op_type],
                                              doc_key, value,
                                              durability=self.durability_level,
                                              timeout=3, time_unit="seconds",
                                              create_path=True,
                                              xattr=self.xattr)
            sdk_exception = str(failed_item[doc_key]["error"])
            if SDKException.AmbiguousTimeoutException not in sdk_exception:
                self.log_failure("Invalid exception: %s" % failed_item)
            if SDKException.RetryReason.KV_SYNC_WRITE_IN_PROGRESS \
                    not in sdk_exception:
                self.log_failure("Retry reason missing: %s" % failed_item)

            # Validate CAS doesn't change after sync_write failure
            curr_cas = self.client.crud(DocLoading.Bucket.DocOps.READ,
                                        doc_key)["cas"]
            if curr_cas != doc_cas:
                self.log_failure("CAS mismatch. %s != %s"
                                 % (curr_cas, doc_cas))
            error_sim.revert(CouchbaseError.STOP_MEMCACHED)
            self.task_manager.get_task_result(sync_write_task)

        # Closing the shell connection
        shell.disconnect()
        self.validate_test_failure()
