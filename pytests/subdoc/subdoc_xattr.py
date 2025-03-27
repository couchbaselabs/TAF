import copy
import functools
import time
import json
import sys
import zlib
from random import choice, shuffle

from BucketLib.bucket import Bucket
from Cb_constants import CbServer, DocLoading
from basetestcase import ClusterSetup
from cb_tools.cbstats import Cbstats
from constants.sdk_constants.java_client import SDKConstants
from couchbase_helper.documentgenerator import \
    doc_generator, \
    sub_doc_generator, \
    SubdocDocumentGenerator, \
    DocumentGenerator
from error_simulation.cb_error import CouchbaseError
from custom_exceptions.exception import DesignDocCreationException
from com.couchbase.client.java.kv import StoreSemantics
from couchbase_helper.document import View
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException
from storage_utils.magma_utils import MagmaUtils
from Jython_tasks.task import FunctionCallTask


class SubdocBaseTest(ClusterSetup):
    def setUp(self):
        super(SubdocBaseTest, self).setUp()

        # Create default bucket
        self.create_bucket(self.cluster)

        # Create required scope/collection for testing
        if self.collection_name != CbServer.default_collection:
            self.collection_name = self.bucket_util.get_random_name()
            if self.scope_name != CbServer.default_scope:
                self.scope_name = self.bucket_util.get_random_name()
                self.bucket_util.create_scope(self.cluster.master,
                                              self.cluster.buckets[0],
                                              {"name": self.scope_name})
            self.bucket_util.create_collection(
                self.cluster.master,
                self.cluster.buckets[0],
                scope_name=self.scope_name,
                collection_spec={"name": self.collection_name})

        for bucket in self.cluster.buckets:
            testuser = [{'id': bucket.name,
                         'name': bucket.name,
                         'password': 'password'}]
            rolelist = [{'id': bucket.name,
                         'name': bucket.name,
                         'roles': 'admin'}]
            self.bucket_util.add_rbac_user(self.cluster.master,
                                           testuser=testuser,
                                           rolelist=rolelist)
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

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
                                self.cluster.buckets[0])

    def tearDown(self):
        # Delete the inserted doc
        self.client.crud("delete", self.doc_id)

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
            time_unit=SDKConstants.TimeUnit.SECONDS,
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
                             type(expected_val)(success[self.doc_id]["value"][0]),
                             "Sub_doc value mismatch: %s != %s"
                             % (success[self.doc_id]["value"][0],
                                expected_val))
        else:
            result = self.client.crud("read", self.doc_id)
            self.assertEqual(type(expected_val)(result["value"]), expected_val,
                             "Document value mismatch: %s != %s"
                             % (result["value"], expected_val))

    def test_basic_functionality(self):
        self.__upsert_document_and_validate("create", {})

        # Try to upsert a single xattr
        self.__insert_sub_doc_and_validate("subdoc_insert",
                                           "my.attr", "value")

        # Read full doc and validate
        self.__read_doc_and_validate('{}')

        # Using lookup_in
        _, failure = self.client.crud("subdoc_read", self.doc_id, "my.attr")
        self.assertTrue(failure)

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
        self.__read_doc_and_validate('{}')

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

        self.__read_doc_and_validate('{}')

        _, failure = self.client.crud("subdoc_read", self.doc_id, "my.attr")
        self.assertTrue(failure)

        self.__read_doc_and_validate({"value_inner":2}, "my.inner")
        self.__read_doc_and_validate({"value":1,"inner":{"value_inner":2}},
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
            time_unit=SDKConstants.TimeUnit.SECONDS,
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
                print(e.message)
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
        _, failure = self.client.crud("subdoc_read", self.doc_id, "my_attr", xattr=True)
        self.assertTrue(failure)

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
        _, failure = self.client.crud("subdoc_read", self.doc_id, "my_attr", xattr=True)
        self.assertTrue(failure)

    def test_cas_changed_upsert(self):
        """ Test the CAS field changes following subdoc insert operations.
        """
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
            self.__read_doc_and_validate('{}')

        # Ensure we cannot read a non-existent xattribute
        _, failure = self.client.crud("subdoc_read", self.doc_id, "my.attr")
        self.assertTrue(failure)

        self.__read_doc_and_validate({"value_inner":2}, "my.inner")
        self.__read_doc_and_validate({"value":1,"inner":{"value_inner":2}},
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
            time_unit=SDKConstants.TimeUnit.SECONDS,
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
            time_unit=SDKConstants.TimeUnit.SECONDS,
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
            _, failure = self.client.crud("subdoc_read", self.doc_id, "my_attr", xattr=self.xattr)
            self.assertTrue(failure)

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
                self.assertTrue(rv.success)
                rv = self.client.lookup_in(k, SD.get(key, xattr=True))
                self.assertTrue(rv.exists(key))
                self.assertEqual(1, rv[key])
                self.log.info("successfully set xattr with key %s" % key)
            except Exception as e:
                ok = False
                self.log.info("unable to set xattr with key %s" % key)
                self.log.error(e)
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
                                self.cluster.buckets[0],
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

        for node in self.cluster_util.get_kv_nodes(self.cluster):
            shell = RemoteMachineShellConnection(node)
            cbstat_obj = Cbstats(shell)
            replica_vbs = cbstat_obj.vbucket_list(
                self.cluster.buckets[0],
                "replica")
            if target_vb in replica_vbs:
                break

            shell.disconnect()

        error_sim = CouchbaseError(self.log, shell)

        for op_type in [DocLoading.Bucket.DocOps.CREATE,
                        DocLoading.Bucket.DocOps.UPDATE,
                        DocLoading.Bucket.DocOps.DELETE]:
            sync_write_task = self.task.async_load_gen_docs(
                self.cluster, self.cluster.buckets[0],
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
                sdk_retry_strategy = choice(
                    [SDKConstants.RetryStrategy.FAIL_FAST,
                     SDKConstants.RetryStrategy.BEST_EFFORT])
                self.log.info("Testing %s over %s, sdk_retry_strategy=%s"
                              % (sw_test_op, op_type, sdk_retry_strategy))
                value = "test_val"
                if sw_test_op not in doc_tasks:
                    value = ["exists_path", "0"]
                    if sw_test_op in ["subdoc_insert"]:
                        value = ["non_exists_path", "val"]
                    if sw_test_op in ["subdoc_delete"]:
                        value = "exists_path"

                result = self.client.crud(
                    sw_test_op, doc_key, value,
                    durability=self.durability_level,
                    timeout=3, time_unit=SDKConstants.TimeUnit.SECONDS,
                    create_path=True,
                    xattr=self.xattr,
                    sdk_retry_strategy=sdk_retry_strategy)
                if sw_test_op not in doc_tasks:
                    result = result[1][doc_key]

                sdk_exception = str(result["error"])
                expected_exception = \
                    SDKException.AmbiguousTimeoutException
                retry_reason = \
                    SDKException.RetryReason.KV_SYNC_WRITE_IN_PROGRESS
                if sdk_retry_strategy == SDKConstants.RetryStrategy.FAIL_FAST:
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
                self.client.crud(
                    "subdoc_insert", doc_key, ["exists_path", 1],
                    durability=self.durability_level,
                    timeout=3, time_unit=SDKConstants.TimeUnit.SECONDS,
                    create_path=True, xattr=self.xattr)

        # Closing the shell connection
        shell.disconnect()
        self.validate_test_failure()

    def test_subdoc_sync_write_in_progress(self):
        shell = None
        doc_gen = dict()
        doc_key = doc_generator(self.doc_id, 0, 1).next()[0]
        target_vb = self.bucket_util.get_vbucket_num_for_key(doc_key)

        for node in self.cluster_util.get_kv_nodes(self.cluster):
            shell = RemoteMachineShellConnection(node)
            cbstat_obj = Cbstats(shell)
            replica_vbs = cbstat_obj.vbucket_list(
                self.cluster.buckets[0],
                "replica")
            if target_vb in replica_vbs:
                break

            shell.disconnect()

        error_sim = CouchbaseError(self.log, shell)

        self.client.crud(DocLoading.Bucket.DocOps.CREATE, doc_key, "{}",
                         timeout=3, time_unit=SDKConstants.TimeUnit.SECONDS)
        self.client.crud("subdoc_insert",
                         doc_key, ["exists_path", 1],
                         durability=self.durability_level,
                         timeout=3, time_unit=SDKConstants.TimeUnit.SECONDS,
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
                self.cluster, self.cluster.buckets[0],
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

            _, failed_item = self.client.crud(
                sub_doc_op_dict[op_type], doc_key, value,
                durability=self.durability_level,
                timeout=3, time_unit=SDKConstants.TimeUnit.SECONDS,
                create_path=True, xattr=self.xattr)
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


class XattrTests(SubdocBaseTest):
    """ Xattributes testing in the context of KV featuring storage, tombstones
    and lifetimes."""

    def setUp(self):
        """ Sets the cluster up
        """
        super(XattrTests, self).setUp()

        # The name of the bucket
        self.bucket = self.cluster.buckets[0]

        # A client for reading xattributes
        self.client = SDKClient([self.cluster.master], self.bucket)

        # Parallelism for verifying xattributes
        self.parallelism = self.input.param("parallelism", 5)

        # Common document prefix
        self.doc_prefix = self.input.param("doc_prefix", "doc")

        # The number of user and system attributes per document
        self.no_of_usr_attributes = self.input.param("no_of_usr_xattr", 1)
        self.no_of_sys_attributes = self.input.param("no_of_sys_xattr", 1)

        # The size of each document body and xattribute value
        self.doc_size = self.input.param("doc_size", 1024)
        self.xattr_size = self.input.param("xattr_size", 1024)

        # A list of fault to introduce
        self.faults = self.input.param("faults", "")
        self.faults = self.faults.split(";") if self.faults else []

        # A list of xattributes to include in documents
        self.paths = self.create_paths()

        # Common async_workload kwargs
        self.async_gen_common = \
            {'scope': CbServer.default_scope,
             'collection': CbServer.default_collection,
             'durability': self.durability_level,
             'process_concurrency': 8,
             'batch_size': 50,
             'print_ops_rate': True}

        # Some workloads can be ran cyclically
        self.cycles = self.input.param("cycles", 1)

        # Number of keys in a cycle
        self.cycle_size = self.input.param("cycle_size", 100000)

        # The current cycle
        self.cycle = 1

        # If the test cannot be ran cyclically, we can define a sensible key_min and key_max
        self.key_min = 0
        self.key_max = self.input.param("key_max", self.cycle_size)

        # Bloat storage with regular documents before test
        self.preload_storage = self.input.param("preload_storage", False)

        # Enable steady state load while tests are running
        self.steady_state_load = self.input.param("steady_state_load", False)

        # Bloat storage with tombstones
        self.preload_tombstones = self.input.param("preload_tombstones", False)

        # A list of tasks that will be stopped at the end of the test
        self.tasks = []

        # An alias which returns the vbucket of a given key
        self.to_vbucket = self.bucket_util.get_vbucket_num_for_key

        # A magma util object
        self.magma_utils = MagmaUtils()

        self.preamble()

        self.txn = TxnTransition(self)

        # Please configure the bucket settings
        # E.g. compression, eviction type and storage backend

    def preamble(self):
        """ Apply operations before a test begins """
        if self.preload_storage:
            self.apply_preload_storage(key_max=self.input.param("preload_size", 100))

        if self.preload_tombstones:
            self.apply_preload_tombstones()

        if self.steady_state_load:
            self.async_apply_steady_load()

    def tearDown(self):
        """ Tears down the cluster
        """
        for task in self.tasks:
            self.task.jython_task_manager.stop_task(task)

        super(XattrTests, self).tearDown()

    def async_apply_steady_load(self):
        """ Loads documents asynchronously
        TODO add different operations
        """
        # A generator for regular documents
        doc_gen = doc_generator(
            "steady_state", 0, 100000000, doc_type=self.doc_type, doc_size=self.doc_size)

        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, doc_gen, "create", **self.async_gen_common)

        self.tasks.append(task)

    def apply_preload_storage(self, key_min=0, key_max=100, doc_prefix=None):
        """ Preload a certain quantity of documents before a test. """
        if doc_prefix is None:
            doc_prefix = "preload"

        # A generator for regular documents
        doc_gen = doc_generator(
            doc_prefix, key_min, key_max, doc_type=self.doc_type, doc_size=1000)

        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, doc_gen, "create", **self.async_gen_common)

        self.task.jython_task_manager.get_task_result(task)

    def apply_faults(self):
        """ Applies the list of faults the user has provided. """
        fault_functions = {"dgm": self.apply_dgm,
                           "rebalance": lambda: self.apply_rebalance(strategy="rebalance"),
                           "hard-failover": lambda: self.apply_rebalance(strategy="hard-failover"),
                           "graceful-failover": lambda: self.apply_rebalance(strategy="graceful-failover"),
                           "lose_last_node": self.apply_lose_last_node,
                           "node_restart": self.apply_node_restart,
                           "stop_persistence": self.apply_stop_persistence,
                           "apply_preload_tombstones": self.apply_preload_tombstones}

        for fault in self.faults:
            fault_functions[fault]()

    def apply_kill_memcached(self, delay=0):
        """ Kill memcached on a random node """
        self.sleep(delay, "Sleeping before killing memcached")

        node = choice(self.cluster.servers)
        shell = RemoteMachineShellConnection(node)
        shell.kill_memcached()
        self.assertTrue(self.bucket_util._wait_warmup_completed([node], self.bucket, wait_time=600))
        shell.disconnect()

    def apply_start_compaction(self, delay=0):
        """ Trigger manual compaction """
        self.sleep(delay, "Sleeping before starting compaction")

        # Trigger manual compaction
        self.bucket_util._run_compaction(self.cluster, number_of_times=1)

    def apply_preload_tombstones(self, key_min=0, key_max=1000000):
        """ Disable autocompaction and create deleted documents with xattributes
        """
        doc_prefix = "tombstone"

        self.create_workload(key_min, key_max, doc_prefix=doc_prefix)
        self.xattrs_workload(key_min, key_max, doc_prefix=doc_prefix)

        # Delete keys to create tombstones
        self.delete_workload(key_min, key_max, doc_prefix=doc_prefix)

    def apply_dgm(self, percentage=50):
        """ Places the cluster in dgm at the given percentage. """
        dgm_gen = doc_generator("dgm", 0, 1000000, doc_size=self.doc_size)

        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, dgm_gen, "create", exp=0, active_resident_threshold=percentage, **self.async_gen_common)

        self.task.jython_task_manager.get_task_result(task)

    def apply_rebalance(self, cycles=3, strategy="rebalance"):
        """ Shuffles servers in and out via a swap-rebalance or failover.
        Requires a minimum of 3 servers. """
        servers = copy.copy(self.cluster.servers)

        # Remove last server
        self.task.rebalance(servers, [], servers[-1:])

        # Swap rebalance a single node for several cycles.
        for i in range(cycles):
            # Add last server and remove second-to-last server
            to_add, to_remove = servers[-1:], servers[-2:-1]

            if strategy == "rebalance":
                # Perform a swap rebalance
                self.task.rebalance(servers, to_add, to_remove)

            if strategy == "graceful-failover" or strategy == "hard-failover":
                # Perform a graceful-failover followed
                self.task.failover(
                    servers=servers, failover_nodes=to_remove, graceful=strategy == "graceful-failover")
                self.task.rebalance(servers, to_add, [])

            # Swap last two elements
            servers[-1], servers[-2] = servers[-2], servers[-1]
            # Shuffle elements between index 1 and index n - 2 inclusive
            shuffled = servers[1:-1]
            shuffle(shuffled)
            servers[1:-1] = shuffled

    def apply_stop_persistence(self):
        """ Stop persistence  """
        # Stopping persistence on main node
        mem_client = MemcachedClientHelper.direct_client(
            self.cluster.master, self.bucket)
        mem_client.stop_persistence()

    def apply_lose_last_node(self):
        """ Loses the last node """
        # Lose a single node by performing a graceful-failover if followed
        self.task.failover(servers=self.cluster.servers,
                           failover_nodes=self.cluster.servers[-1:], graceful=False)

    def apply_node_restart(self):
        """ Restarts a the last node """
        shell = RemoteMachineShellConnection(self.cluster.servers[-1])
        shell.restart_couchbase()
        shell.disconnect()

    def format_doc_key(self, key_number):
        """ Returns a document key given a document number. """
        return "{}-{:04}".format(self.doc_prefix, key_number)

    def get_subdoc_val(self):
        """ Given a document key and sub-doc path returns a pure value."""
        return 'a' * self.xattr_size

    def create_paths(self):
        """ Returns a list with user and system attributes. """
        paths = []

        for i in range(self.no_of_sys_attributes):
            paths.append("_sys{}".format(i))

        for i in range(self.no_of_usr_attributes):
            paths.append("usr{}".format(i))

        return paths

    def xattr_type(self, xattribute):
        """ Returns the type of an xattribute. """
        if not xattribute:
            raise ValueError('The xattribute is empty.')
        first_character = xattribute[0]

        if first_character == '_':
            return 'SYS_ATTR'

        if first_character == '$':
            return 'VIR_ATTR'

        return 'USR_ATTR'

    def create_workload(self, key_min, key_max, exp=0, doc_prefix=None):
        """ Produces documents

        Creates documents between keys key_min and key_max.

        Args:
            key_min (int): The first key in the interval.
            key_max (int): The final key in the interval.
        """
        if doc_prefix is None:
            doc_prefix = self.doc_prefix

        # A generator for regular documents
        doc_gen = doc_generator(
            doc_prefix, key_min, key_max, doc_type=self.doc_type,
            doc_size=self.doc_size)

        # Create docs between keys_min and keys_max. This is required because
        # documents must previously exist before xattrs can be added to them.
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, doc_gen, "create", exp=exp,
            **self.async_gen_common)

        self.task.jython_task_manager.get_task_result(task)

    def xattrs_workload(self, key_min, key_max, exp=0, doc_prefix=None):
        """ A faster version of the xattrs workload method to produce
        xattributes between key_min and key_max. Expects the documents to
        pre-exist.
        """
        tasks = []

        if doc_prefix is None:
            doc_prefix = self.doc_prefix

        for path in self.paths:
            xattribute_template = '{{ "' + path + '": "{0}" }}'
            template_value = [self.get_subdoc_val()]
            sub_doc_gen = SubdocDocumentGenerator(
                doc_prefix, xattribute_template, template_value, start=key_min, end=key_max)
            task = self.task.async_load_gen_sub_docs(
                self.cluster, self.bucket, sub_doc_gen,
                DocLoading.Bucket.SubDocOps.UPSERT, exp=exp, xattr=True,
                path_create=True, store_semantics=StoreSemantics.UPSERT,
                **self.async_gen_common)
            tasks.append(task)

        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

    def xattrs_workload_slow(self, key_min, key_max, vbucket_filter=None):
        """ Updates xattributes between key_min and key_max.

        Args:
            vbucket_filter (set(int)): A set of vbuckets that the key must be in.
        """
        # Create xattrs for docs between keys_min and keys_max.
        for path in self.paths:
            for key_number in range(key_min, key_max):
                doc_key = self.format_doc_key(key_number)
                if vbucket_filter and self.to_vbucket(doc_key) not in vbucket_filter:
                    continue
                sub_val = self.get_subdoc_val()
                sub_doc = [path, sub_val]
                success, failed = self.client.crud(
                    "subdoc_upsert", doc_key, sub_doc, durability=self.durability_level, create_path=True, xattr=True)

    def delete_workload(self, key_min, key_max, doc_prefix=None):
        """ Deletes documents in the range key_min, key_max.
        """
        if doc_prefix is None:
            doc_prefix = self.doc_prefix

        # A generator for regular documents
        doc_gen = doc_generator(doc_prefix, key_min, key_max)

        # Delete documents between keys_min and keys_max.
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, doc_gen,
            DocLoading.Bucket.DocOps.DELETE, **self.async_gen_common)
        self.task.jython_task_manager.get_task_result(task)

    def get_xattribute(self, doc_key, path, access_deleted=True):
        """ Returns a tuple where the first element indicates the tuple was
        accessible and the second element contains the value. """
        success, failed = self.client.crud(
            "subdoc_read", doc_key, path, xattr=True,
            access_deleted=access_deleted)

        accessible = success and (
            success[doc_key]['value'][0] != "PATH_NOT_FOUND")

        if accessible:
            xattrvalue = success[doc_key]['value'][0]
        else:
            xattrvalue = None

        return accessible, xattrvalue

    def verify_purged_tombstones(self, key_min, key_max):
        """ Validate documents xattrs once they have been deleted and purged.

        Check that there is at most 1 tombstones with system xattribute per
        vbucket once tombstones have been purged.

        Context: Each document has a sequence number, assuming all documents
        have been deleted the tombstone with the highest sequence number is
        preserved to remember the high sequence number for that vbucket.
        """
        seen_vbuckets = set()

        for doc_key in map(self.format_doc_key, range(key_min, key_max)):
            for xattr in self.paths:
                if self.get_xattribute(doc_key, xattr, access_deleted=True)[0]:
                    if self.to_vbucket(doc_key) in seen_vbuckets:
                        self.fail("Found multiple tombstones in the same "
                                  "vbucket post purging.")
                    else:
                        seen_vbuckets.add(self.to_vbucket(doc_key))
                        break

    def verify_workload(self, key_min, key_max, is_deleted=False):
        """ Ensures document xattrs have the correct values between the
        half-open interval key_min and key_max.

        Args:
            key_min (int): The first key in the interval.
            key_max (int): The final key in the interval.
            is_deleted (bool): Indicates the documents have been deleted.
        """
        self.log.info(
            "Verifying workload between {} and {}".format(key_min, key_max))
        for doc_key in map(self.format_doc_key, range(key_min, key_max)):
            for path in self.paths:
                accessible, xattrvalue = self.get_xattribute(
                    doc_key, path, access_deleted=True)

                # User attributes are discarded upon deletion.
                # System attributes are only discarded upon purging.
                # System attributes are not accessible once expired.
                if is_deleted and self.xattr_type(path) == 'USR_ATTR':
                    self.assertFalse(accessible)
                else:
                    self.assertEqual(xattrvalue, self.get_subdoc_val(),
                                     "for key %s" % doc_key)

    def parallel(self, function, key_min, key_max, **kwargs):
        """ Execute the workload in parallel by batching keys between key_max
        and key_min into groups and executing the given function across each
        group in parallel.
        """
        tasks = []
        minimum_size = 10000
        batch_size = max((key_max - key_min) // self.parallelism, minimum_size)

        for lower_bound in range(key_min, key_max, batch_size):
            upper_bound = min(lower_bound + batch_size, key_max)
            tasks.append(FunctionCallTask(function,
                                          args=[lower_bound, upper_bound],
                                          kwds=kwargs))
            self.task_manager.add_new_task(tasks[-1])

        for task in tasks:
            self.task_manager.get_task_result(task)

    def cyclic(f):
        """ A decorator to run a test cyclically.

        Params:
            cycles: The number of cycles.
            cycle_size: The number of documents in a cycle.
        """
        @functools.wraps(f)
        def run_cycles(self):
            for i in range(self.cycles):
                self.key_min = self.cycle_size * i
                self.key_max = self.cycle_size * (i + 1)
                self.cycle = i + 1
                f(self)

        return run_cycles

    @cyclic
    def test_xattributes(self):
        """ Create some documents with xattributes.

        Create some documents with xattributes.

        Expect both user and system xattributes to be accessible.
        """
        key_min = self.key_min
        key_max = self.key_max

        self.create_workload(key_min, key_max)
        self.xattrs_workload(key_min, key_max)
        self.apply_faults()
        self.parallel(self.verify_workload, key_min, key_max)

    @cyclic
    def test_xattribute_compaction(self):
        """ Manually run compaction.

        Disable auto-compaction, bloat storage, create documents with
        xattributes and manually run compaction.

        Expect both user attributes and system xattributes to be accessible
        following compaction.
        """
        key_min = self.key_min
        key_max = self.key_max

        # Disable auto-compaction
        self.bucket_util.disable_compaction(self.cluster)

        # Run workload until threshold is reached
        self.create_workload(key_min, key_max)
        self.xattrs_workload(key_min, key_max)

        self.apply_faults()

        # Trigger manual compaction
        self.bucket_util._run_compaction(self.cluster, number_of_times=1)

        # Validate the keys
        self.parallel(self.verify_workload, key_min, key_max)

    @cyclic
    def test_xattribute_deletion(self):
        """ Test xattribute deletion

        Create documents with xattributes and delete them.

        Expect the user attributes to be discarded and the system xattributes
        to be accessible following deletion.
        """
        key_min = self.key_min
        key_max = self.key_max

        # Run workload until threshold is reached
        self.create_workload(key_min, key_max)
        self.xattrs_workload(key_min, key_max)

        # Delete keys between a certain range
        self.delete_workload(key_min, key_max)

        # Check system keys exist and user attributes no longer exist
        self.parallel(self.verify_workload, key_min, key_max, is_deleted=True)

    def verify_expired_workload(self, key_min, key_max):
        """ Ensures doc's xattrs are no longer accessible following expiration.

        Args:
            key_min (int): The first key in the interval.
            key_max (int): The final key in the interval.
        """
        self.log.info(
            "Verifying workload between {} and {}".format(key_min, key_max))
        for doc_key in map(self.format_doc_key, range(key_min, key_max)):
            for path in self.paths:
                accessible, xattrvalue = self.get_xattribute(
                    doc_key, path, access_deleted=False)

                if accessible:
                    self.log.info("%s %s is True, xattrvalue: %s"
                                  % (doc_key, path, xattrvalue))
                self.assertFalse(accessible)

    def wait_for_expiration(self, ttl, expiry_pager_time):
        """ Waits for the expiry pager to delete documents.
        """
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, [self.bucket], timeout=1200)

        # Wait for documents to expire
        self.sleep(ttl, "Waiting for documents to expire.")
        # Set expiry pager interval
        self.bucket_util._expiry_pager(self.cluster, expiry_pager_time)
        # Wait for expiry pager to expire documents.
        self.sleep(expiry_pager_time*3, "Wait for expiry pager to complete.")

        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, [self.bucket])
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, [self.bucket], cbstat_cmd="all",
            stat_name="vb_replica_queue_size")

    @cyclic
    def test_xattribute_expiry(self):
        """ Test xattribute expiry

        Create documents with xattributes, configure expiry by setting a
        time-to-live for each document and wait for the documents to expire.

        Expect both user and system attributes to be inaccessible following
        expiration.
        """
        key_min = self.key_min
        key_max = self.key_max
        ttl = self.input.param("ttl", 15)
        expiry_pager_time = self.input.param("expiry_pager_time", 10)

        # Run workload until threshold is reached
        self.create_workload(key_min, key_max)
        self.xattrs_workload(key_min, key_max, exp=ttl)

        self.wait_for_expiration(ttl, expiry_pager_time)
        self.apply_faults()

        # Check both user and system attributes are no longer accessible
        # following expiration.
        self.parallel(self.verify_expired_workload, 20000, 50000)

    @cyclic
    def test_xattribute_metadata_purge(self):
        """ Test xattributes are discarded during metadata purging.

        Delete documents with xattributes and run compaction once the metadata
        purge interval has been exceeded such that the tombstones are
        sufficiently old to be discarded.

        Expect both user and system attributes to be inaccesible following the
        compaction.
        """
        key_min = self.key_min
        key_max = self.key_max

        self.create_workload(key_min, key_max)
        self.xattrs_workload(key_min, key_max)

        # Delete keys between a certain range
        self.delete_workload(key_min, key_max)

        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     [self.bucket])

        # Set the metadata purge interval to 120 seconds.
        # The autoCompactionDefined field must be set to true, otherwise the
        # metadata purge interval will reset back to 3 days.
        self.bucket_util.modify_fragmentation_config(self.cluster, {})
        self.bucket_util.set_metadata_purge_interval(self.cluster,
            0.0014, [self.bucket], self.cluster.master)

        self.sleep(120, "Waiting for the metadata purge interval to pass.")

        # Trigger manual compaction
        self.bucket_util._run_compaction(self.cluster, number_of_times=1)

        # Check at most 1 tombstone exists per vbucket
        self.parallel(self.verify_purged_tombstones, key_min, key_max)

    def vbuckets_on_node(self, server, vbucket_type='active'):
        """ Returns vbuckets for a specific node """
        shell = RemoteMachineShellConnection(server)
        vbuckets = set(Cbstats(shell).vbucket_list(
            self.bucket.name, vbucket_type))
        shell.disconnect()
        return vbuckets

    def verify_rollback(self, key_min, key_max, vbuckets=None):
        """ Validates rollback by expecting the xattributes belonging to
        vbuckets not to exist. """
        for doc_key in map(self.format_doc_key, range(key_min, key_max)):
            for path in self.paths:
                accessible, xattrvalue = self.get_xattribute(
                    doc_key, path, access_deleted=True)
                # If vb belongs to a node 1, then xattr should not exist
                if self.to_vbucket(doc_key) in vbuckets:
                    self.assertFalse(accessible)
                else:
                    self.assertEqual(
                        xattrvalue, self.get_subdoc_val())

    @cyclic
    def test_xattributes_with_rollback(self):
        """ Test xattributes with rollback.

        After stopping persistence, create documents with xattributes. Kill
        memcached and wait for it to restart. At this point the replicas for
        each active vbucket on 1 are further ahead. Similarly, the replica
        vbuckets on node 1 are behind their active vbuckets. Consequently, the
        actives on node 1 cause their replicas to rollback and the replicas
        present on node 1 accept the new mutations from their actives.

        Expect xattributes belonging to active vbucket's on node 1 to not
        exist. Expect xattributes belonging to non-active vbuckets.
        """
        key_min = self.key_min
        key_max = self.key_max

        node1 = self.cluster.servers[0]
        shell = RemoteMachineShellConnection(node1)
        mem_client = MemcachedClientHelper.direct_client(node1, self.bucket)
        active_vbuckets = self.vbuckets_on_node(node1, vbucket_type='active')

        # Load some data
        self.create_workload(key_min, (key_max + key_min) // 2)
        self.xattrs_workload(key_min, (key_max + key_min) // 2)

        self.apply_faults()

        # Stop persistence so replicas of node 1 move ahead
        mem_client.stop_persistence()

        # Create documents with xattributes
        self.create_workload((key_max + key_min) // 2, key_max)
        self.xattrs_workload((key_max + key_min) // 2, key_max)

        # Kill memcached and wait for it to restart
        shell.kill_memcached()

        self.assertTrue(self.bucket_util._wait_warmup_completed(
            [node1], self.bucket, wait_time=300))

        self.parallel(self.verify_rollback, (key_max + key_min) // 2,
                      key_max, vbuckets=active_vbuckets)

        shell.disconnect()

    def verify_stopped_replicas(self, key_min, key_max, vbuckets=None):
        """ Perform validation """
        for doc_key in map(self.format_doc_key, range(key_min, key_max)):
            for path in self.paths:
                # Fetch xattribute
                accessible, xattrvalue = self.get_xattribute(
                    doc_key, path, access_deleted=True)
                # If vb belongs to a node 1, then that xattr should be accessible
                if self.to_vbucket(doc_key) in vbuckets:
                    self.assertEqual(
                        xattrvalue, self.get_subdoc_val())

    @cyclic
    def test_xattributes_with_stopped_replicas(self):
        """ Test xattributes with no replica vbuckets.

        Stop replica nodes and generate documents with xattributes that exist
        in the active vbuckets of the non-stopped node.

        Expect the xattributes belonging to the active documents to be
        accessible.
        """
        key_min = self.key_min
        key_max = self.key_max
        active_vbuckets = self.vbuckets_on_node(self.cluster.master)

        # Create documents
        self.create_workload(key_min, key_max)

        self.apply_faults()

        # Stop replica nodes (Sigstop nodes numbered 2 and above)
        remote_connections = [RemoteMachineShellConnection(
            server) for server in self.cluster.servers[1:]]
        for connection in remote_connections:
            connection.pause_memcached()

        # Create xattributes
        self.parallel(self.xattrs_workload_slow, key_min,
                      key_max, vbucket_filter=active_vbuckets)

        # Resume the replica nodes
        for connection in remote_connections:
            connection.unpause_memcached()

        self.parallel(self.verify_stopped_replicas, key_min,
                      key_max, vbuckets=active_vbuckets)

        for connection in remote_connections:
            connection.disconnect()

    def test_xattributes_tolerate_1_failure(self):
        """ Test xattributes with sync replication tolerate 1 failure.

        Lose a single node in a cluster with nodes >= 3 with items which have
        been created using synchronous replication by performing a hard
        failover without a rebalance-out.

        Expect a replica to be promoted on the healthy side and no xattributes
        to be lost.
        """
        key_min = self.key_min
        key_max = self.key_max

        # Load data
        self.create_workload(key_min, key_max)
        self.xattrs_workload(key_min, key_max)

        self.apply_faults()

        # Lose a single node by performing a hard-failover
        self.task.failover(servers=self.cluster.servers,
                           failover_nodes=self.cluster.servers[-1:],
                           graceful=False)

        # Perform validation
        self.parallel(self.verify_workload, key_min, key_max)

    def average_kvstore_usage(self):
        """ Returns the average kvstore usage. """
        kvstore, wal, key_tree, seq_tree = \
        self.magma_utils.get_disk_usage(self.cluster, self.bucket,
                "/opt/couchbase/var/lib/couchbase/data/", servers=self.servers)
        return kvstore

    def test_fragmentation(self):
        """ Fragmentation test

        Load an initial set of documents with xattributes. Repeat the initial
        workload and such that fragmentation reaches 50%. Auto-compaction
        should kick in and ensure the disk usage remains below a sensible
        threshold.

        Expect the cost of repeating the workload result be at most 2.5x the
        initial load.
        """
        key_min = self.key_min
        key_max = self.key_max

        props = "magma;magma_max_checkpoints={}".format(0)
        self.bucket_util.update_bucket_props("backend", props, self.cluster,
                                             self.cluster.buckets)

        # Load initial set of xattribuites xattributes
        self.xattrs_workload(key_min, key_max)
        # The cost of storing the initial documents
        initial_cost = self.average_kvstore_usage()

        # Upsert xattributes
        self.xattrs_workload(key_min, key_max)

        timeout = time.time() + 30
        while time.time() < timeout:
            # The cost of storing the additional documents
            update_cost = self.average_kvstore_usage()
            self.log.info("Initial cost: {} Update cost: {}"
                          .format(initial_cost, update_cost))
            if update_cost <= initial_cost * 2.5:
                return

        self.fail("The update cost exceeded the initial cost.")

    @cyclic
    def test_crashing_processes(self):
        """
        Asynchronously kill memcached while data is loading and optionally
        while compaction is in progress.
        """
        key_min = self.key_min
        key_max = self.key_max
        tasks = []
        start_compaction = self.input.param("start_compaction", True)

        # Load initial data
        self.create_workload(key_min, key_max)

        # Start compaction after a delay of 8 seconds
        if start_compaction:
            tasks.append(FunctionCallTask(self.apply_start_compaction,
                                          kwds={'delay': 8}))
            self.task_manager.add_new_task(tasks[-1])

        # Kill memcached after a delay of 15 seconds
        tasks.append(FunctionCallTask(self.apply_kill_memcached,
                                      kwds={'delay': 15}))
        self.task_manager.add_new_task(tasks[-1])

        # Load xattributes data
        self.xattrs_workload(key_min, key_max)

        # Wait for task
        for task in tasks:
            self.task_manager.get_task_result(task)

    def test_transitions(self):
        """ Test document transitions

        Params:
            workload (str): A name of a workload (e.g. workload_a).
            type (str): The type of a workload (e.g. shadow, regular).
        """
        loadtype = self.input.param("loadtype", "shadow")
        workload = self.input.param("workload", "workload_a")

        workloads = ["shadow", "regular"]

        if loadtype not in workloads:
            self.fail("Loadtype '{}' is not in {}"
                      .format(workloads, workloads))

        if loadtype == "shadow":
            workloads = self.txn.get_shadow_workloads()
        else:
            workloads = self.txn.get_workloads()

        if workload not in workloads:
            self.fail("Workload does not exist")

        self.log.info("Running workload {0} of type {1}"
                      .format(workload, loadtype))

        self.txn.apply_transitions(workloads[workload])

    def test_shadow_fragmentation(self):
        """ Test disk usage does not exceed sensible amounts at 50%
        fragmentation """
        # Creates shadow documents
        self.txn.staged_insert()

        # The cost of storing the initial documents
        initial_cost = self.average_kvstore_usage()

        # Perform a staged replace to bring fragmentation to 50%
        self.txn.staged_replace()

        timeout = time.time() + 30
        while time.time() < timeout:
            # The cost of storing the additional documents
            update_cost = self.average_kvstore_usage()
            self.log.info("Initial cost: {} Update cost: {}"
                          .format(initial_cost, update_cost))
            if update_cost <= initial_cost * 2.5:
                return

        self.fail("The update cost exceeded the initial cost.")

    def test_shadow_metadata_purge(self):
        """ Tests metadata purging results in the disk space being released """
        # Creates shadow documents
        self.txn.staged_insert()

        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     [self.bucket])

        # Set the metadata purge interval to 120 seconds.
        # The autoCompactionDefined field must be set to true, otherwise the
        # metadata purge interval will reset back to 3 days.
        self.bucket_util.modify_fragmentation_config(self.cluster, {})
        self.bucket_util.set_metadata_purge_interval(
            self.cluster, 0.0014, [self.bucket], self.cluster.master)

        self.sleep(120, "Waiting for the metadata purge interval to pass.")

        # Trigger manual compaction
        self.bucket_util._run_compaction(self.cluster, number_of_times=1)

        update_cost = self.average_kvstore_usage()

        # Check disk usage is 0 at the magma level
        self.assertEqual(update_cost, 0)


class TxnTransition:
    """ Represents the state transitions for documents. """

    # Note: Any sequence that starts with a non-existant document
    # and ends with a remove does not require an unstage

    def __init__(self, base):
        """ Constructor """
        # References to the base class
        self.base = base

        # Logging
        self.log = base.log

        # Reference to the task object
        self.task = base.task

        # Document loading methods
        self.load_docs = functools.partial(
            self.task.async_load_gen_docs, base.cluster, base.bucket)
        self.load_sub_docs = functools.partial(
            self.task.async_load_gen_sub_docs, base.cluster, base.bucket)

        # The xattribute to use
        self.path = 'just_a_path'

        # The document padding size
        self.doc_size = base.doc_size

        # The document key prefix
        self.doc_prefix = base.doc_prefix

        # The documents ranges to mutate
        self.key_min = 0
        self.key_max = base.key_max

        # Increment this after every transition
        self.token = 0

    def apply_transitions(self, transitions):
        """ Applies transitions in sequence. """
        for transition in transitions:
            self.log.info("Applying a transition")
            transition()
            self.increment_token()

        self.reset_token()

    def reset_token(self):
        """ Reset the token back to 0 """
        self.token = 0

    def increment_token(self):
        """ Increments a counter so mutations have different values """
        self.token += 1

    def get_body(self):
        """ The document body """
        return {'key': 'value' * (self.token + 1),
                'padding': 'a' * self.doc_size}

    def get_value(self, op_type='unknown', body=None):
        """ The value of the xattribute """
        if body is None:
            body = self.get_body()
        return json.dumps({'op_type': op_type,
                           'key2': 'value2',
                           'key3': 'value3',
                           'body': body})

    def get_template(self, path=None, binary=False):
        """ Returns a template

        Returns a format string which resembles a Json Object consisting of the
        'path' as the key and a format string placeholder as the value.

        Args:
            path (str): The path supplied to a MutateInSpec.
            binary (bool): Surrounds the template value with quotes.
        """
        if path is None:
            path = self.path

        return '{{ "' + path + '": "{0}" }}' \
            if binary else '{{ "' + path + '": {0} }}'

    def get_generator(self, template=None, value=None):
        """ Returns a SubdocDocumentGenerator

        Args:
        template (str): A format string resembling a Json Object.
        value (str): A value supplied to template.format().
        """
        if value is None:
            value = self.get_value()
        if template is None:
            template = self.get_template()
        return SubdocDocumentGenerator(self.doc_prefix, template, [value],
                                       start=self.key_min, end=self.key_max)

    def merge_dicts(self, a, b):
        """ Merges two dictionaries """
        m = dict(a)
        m.update(b)
        return m

    def kv_delete(self):
        """ Deletes documents between a given range """
        # Note the template and value doesn't really matter here as we are
        # performing deletes
        task = self.load_docs(
            DocumentGenerator(self.doc_prefix, '{{ "key": "{0}" }}', [''],
                              start=self.key_min, end=self.key_max),
            "delete",
            **self.base.async_gen_common)
        self.task.jython_task_manager.get_task_result(task)
        return task

    def subdoc_task(self, sub_doc_gen, op_type, kwds):
        """ Performs a sub-document task """
        task = self.load_sub_docs(
            sub_doc_gen, op_type,
            **self.merge_dicts(kwds, self.base.async_gen_common))
        self.task.jython_task_manager.get_task_result(task)
        return task

    def check(self, gen, xattr=True, access_deleted=True, exists=True):
        """ Perform validation

        Args:
            gen (SubdocDocumentGenerator): Generates documents consisting of
            the path and the expected value of a document.
            xattr (bool): Set if the path points to an xattribute.
            access_deleted (bool): Set if the document is in tombstone form.
            exists (bool): Set False if the document is expected to exist.
        """
        # Fetch documents
        kwds = {'xattr': xattr, 'access_deleted': access_deleted}
        task = self.subdoc_task(gen, "read", kwds)

        # Grab the expected value of the document from generator
        _, path_value_list = next(gen)
        expected_value = path_value_list[0][1]

        if not exists:
            # Expect the path not to exist
            self.base.assertTrue(task.fail)
            self.base.assertFalse(task.success)
        else:
            # Expect the value of actual the document to match the expected
            # value
            for value in task.success.values():
                actual_value = value['value'][0].toMap()
                actual_value.pop('mutated', None)
                self.base.assertEqual(actual_value, expected_value)

    def create_documents(self):
        """ Creates regular documents """
        self.base.apply_preload_storage(key_min=0, key_max=self.key_max,
                                        doc_prefix=self.doc_prefix)

    def staged_insert(self):
        """ Document does not exist -> Create an xattribute with the given
        value.

        Creates Shadow Documents. """
        kwds = {'xattr': True, 'path_create': True,
                'store_semantics': StoreSemantics.INSERT,
                'access_deleted': True, 'create_as_deleted': True}
        _ = self.subdoc_task(self.get_generator(), "insert", kwds)

        self.check(self.get_generator(), xattr=True, access_deleted=True)

    def staged_insert_from_staged(self):
        """ Previous operation was a remove -> Create an xattribute with the
        given value. """
        kwds = {'xattr': True, 'path_create': True,
                'store_semantics': StoreSemantics.REPLACE,
                'access_deleted': True}
        _ = self.subdoc_task(self.get_generator(), "insert", kwds)

        self.check(self.get_generator(), xattr=True, access_deleted=True)

    def staged_replace(self):
        """ Stage a replace.  """
        kwds = {'xattr': True, 'path_create': True,
                'store_semantics': StoreSemantics.REPLACE,
                'access_deleted': True}
        _ = self.subdoc_task(self.get_generator(), "upsert", kwds)

        self.check(self.get_generator(), xattr=True, access_deleted=True)

    def staged_remove_from_staged(self):
        """ Document didn't previously exists and is staged -> Delete
        xattribute (No Unstage or Rollback should follow this). """
        kwds = {'xattr': True, 'path_create': True,
                'store_semantics': StoreSemantics.REPLACE,
                'access_deleted': True}
        _ = self.subdoc_task(self.get_generator(), "remove", kwds)

        self.check(self.get_generator(), xattr=True, access_deleted=True,
                   exists=False)

    def staged_remove_from_document(self):
        """ Document exists -> Mark xattribute as a remove operation. """
        kwds = {'xattr': True, 'path_create': True,
                'store_semantics': StoreSemantics.REPLACE,
                'access_deleted': True}
        _ = self.subdoc_task(
            self.get_generator(value=self.get_value(op_type='remove')),
            "upsert", kwds)

        self.check(
            self.get_generator(value=self.get_value(op_type='remove')),
            xattr=True, access_deleted=False)

    def upsert_and_remove(self, access_deleted=False):
        """ Clear and remove xattribute. """
        # Clear xattribute by setting it to an empty value
        kwds = {'xattr': True, 'access_deleted': access_deleted}
        _ = self.subdoc_task(
            self.get_generator(template=self.get_template(binary=True),
                               value=''),
            "upsert", kwds)

        # Remove xattribute
        _ = self.subdoc_task(self.get_generator(), "remove", kwds)

        # check xattribute does not exist
        self.check(self.get_generator(), xattr=True,
                   access_deleted=access_deleted, exists=False)

    def unstage_insert_or_replace(self):
        """ Read staged document -> Insert/Replace document """
        # Read staged document
        kwds = {'xattr': True, 'access_deleted': True}
        task = self.subdoc_task(self.get_generator(), "read", kwds)

        # Grab a document body
        body = task.success.values()[0]['value'][0].get('body').toString()

        # upsert-and-remove the xattribute
        self.upsert_and_remove()

        # Write body to document using a CMD_SET
        kwds = {'store_semantics': StoreSemantics.UPSERT}
        _ = self.subdoc_task(
            self.get_generator(template=self.get_template(path=''),
                               value=body),
            "replace", kwds)

        # Check body contains the expected value
        self.check(
            self.get_generator(template=self.get_template(path=''),
                               value=body),
            xattr=False, access_deleted=False)

    def unstage_remove(self):
        """ Perform a KV remove on the document """
        self.kv_delete()

        # Check the document does not exist
        self.check(
            self.get_generator(template=self.get_template(path='')),
            xattr=False, exists=False)

    def rollback_insert_or_replace(self):
        """ Remove Staged document """
        self.upsert_and_remove(access_deleted=True)

    def rollback_remove(self):
        """ Document exists -> Removed Staged document """
        self.upsert_and_remove(access_deleted=False)

    def repeat_transition(self, n, transition):
        """ Repeat a transition n times. """
        def repeated_transition():
            for i in range(n):
                transition()
        return repeated_transition

    def compose_transition(self, *transitions):
        """ Compose a transitions """
        def composed_transition():
            for transition in transitions:
                transition()
        return composed_transition

    def rep_staged_replace(self):
        """ Repeat staged replace """
        self.repeat_transition(5, self.staged_replace)

    def insert_replace_and_unstage(self):
        """ Insert, replace and unstage a document """
        return self.compose_transition(self.staged_insert, self.staged_replace,
                                       self.unstage_insert_or_replace)

    def get_shadow_workloads(self):
        """ Returns workloads in which the document does not initially exist
        and work with shadow documents. """

        _ = self.compose_transition(self.staged_remove_from_staged,
                                    self.staged_replace)

        workloads = \
            {"workload_a": [self.staged_insert, self.staged_replace,
                            self.unstage_insert_or_replace],
             "workload_b": [self.staged_insert, self.staged_replace,
                            self.staged_remove_from_staged,
                            self.staged_replace,
                            self.unstage_insert_or_replace],
             "workload_c": [self.staged_insert, self.rep_staged_replace,
                            self.staged_remove_from_staged,
                            self.staged_replace,
                            self.unstage_insert_or_replace],
             "workload_d": [self.staged_insert, self.rep_staged_replace,
                            self.staged_remove_from_staged],
             "workload_e": [self.insert_replace_and_unstage(),
                            self.staged_remove_from_document,
                            self.unstage_remove],
             "workload_f": [self.insert_replace_and_unstage(),
                            self.staged_replace,
                            self.unstage_insert_or_replace],
             "workload_g": [self.staged_insert, self.staged_replace,
                            self.rollback_insert_or_replace],
             "workload_h": [self.staged_insert, self.staged_remove_from_staged,
                            self.staged_insert,
                            self.rollback_insert_or_replace],
             "workload_i": [self.insert_replace_and_unstage(),
                            self.staged_remove_from_document,
                            self.rollback_remove],
             "workload_j": [self.insert_replace_and_unstage(),
                            self.rep_staged_replace,
                            self.rollback_insert_or_replace]}

        return workloads

    def get_workloads(self):
        """ Returns workloads in which a document initially exists.
        """
        workloads = \
            {"workload_a": [self.create_documents,
                            self.staged_remove_from_document,
                            self.unstage_remove],
             "workload_b": [self.create_documents,
                            self.staged_replace,
                            self.unstage_insert_or_replace],
             "workload_c": [self.create_documents,
                            self.staged_remove_from_document,
                            self.staged_replace,
                            self.unstage_insert_or_replace],
             "workload_d": [self.create_documents,
                            self.staged_replace,
                            self.unstage_insert_or_replace,
                            self.staged_remove_from_document,
                            self.unstage_remove],
             "workload_e": [self.create_documents,
                            self.staged_remove_from_document,
                            self.unstage_remove,
                            self.staged_insert,
                            self.unstage_insert_or_replace],
             "workload_f": [self.create_documents,
                            self.staged_remove_from_document,
                            self.staged_replace,
                            self.staged_remove_from_staged,
                            self.unstage_remove],
             "workload_g": [self.create_documents,
                            self.staged_replace,
                            self.rollback_insert_or_replace],
             "workload_h": [self.create_documents,
                            self.staged_remove_from_document,
                            self.rollback_remove],
             "workload_i": [self.create_documents,
                            self.rep_staged_replace,
                            self.rollback_insert_or_replace],
             "workload_j": [self.create_documents,
                            self.staged_replace,
                            self.rollback_insert_or_replace,
                            self.staged_remove_from_document,
                            self.rollback_remove]}
        return workloads
