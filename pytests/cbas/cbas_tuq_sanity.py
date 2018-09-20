# -*- coding: utf-8 -*-
import json
import math

from cbas.cbas_base import CBASBaseTest
from sdk_client import SDKClient

'''
Created on Mar 16, 2018

@author: riteshagarwal
'''
from tuqquery.tuq_sanity import QuerySanityTests
from membase.api.rest_client import RestConnection
from cbas.cbas_base import CBASBaseTest
import re
import time
import datetime
from pytests.tuqquery.date_time_functions import *


class cbas_object_tests(CBASBaseTest):
    def setup_cbas_bucket_dataset_connect(self):
        # Create bucket on CBAS
        self.query = 'insert into %s (KEY, VALUE) VALUES ("test",{"type":"testType","indexMap":{"key1":"val1", "key2":"val2"},"data":{"foo":"bar"}})'%(self.default_bucket_name)
        result = RestConnection(self.master).query_tool(self.query)
        self.assertTrue(result['status'] == "success")
        
        self.cbas_util.createConn(self.default_bucket_name)
        self.assertTrue(self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                       cb_bucket_name=self.default_bucket_name),"bucket creation failed on cbas")
        
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.default_bucket_name,
                          cbas_dataset_name=self.cbas_dataset_name), "dataset creation failed on cbas")
        
        self.assertTrue(self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name),"Connecting cbas bucket to cb bucket failed")
        
        self.assertTrue(self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name], 1),"Data ingestion to cbas couldn't complete in 300 seconds.")

    def test_object_add(self):
        self.setup_cbas_bucket_dataset_connect()
        self.query = "SELECT object_add(indexMap,'key3','value3') from %s;"%self.cbas_dataset_name
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")
        expected_result = {
                            "key1": "val1",
                            "key2": "val2",
                            "key3": "value3"
                            }
        self.assertTrue(status=="success")
        self.assertTrue(result[0]['$1']==expected_result)
        
    def test_object_put(self):
        self.setup_cbas_bucket_dataset_connect()
        self.query = "SELECT object_put(indexMap,'key3','value3') from %s;"%self.cbas_dataset_name
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")
        expected_result = {
                            "key1": "val1",
                            "key2": "val2",
                            "key3": "value3"
                            }
        
        self.assertTrue(status=="success")
        self.assertTrue(result[0]['$1']==expected_result)
        
        self.query = "SELECT object_put(indexMap,'key2','new_value') from %s;"%self.cbas_dataset_name
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")
        expected_result = {
                            "key1": "val1",
                            "key2": "new_value",
                            }
        
        self.assertTrue(status=="success")
        self.assertTrue(result[0]['$1']==expected_result)

    def test_object_rename(self):
        self.setup_cbas_bucket_dataset_connect()
        self.query = "SELECT object_rename(%s, 'type', 'new_type') from %s;"%(self.cbas_dataset_name,self.cbas_dataset_name)
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")
        self.assertTrue(status=="success")
        self.assertTrue(result[0]['$1']['new_type'])

    def test_object_remove(self):
        self.setup_cbas_bucket_dataset_connect()
        self.query = "SELECT object_remove(%s, 'indexMap') from %s;"%(self.cbas_dataset_name,self.cbas_dataset_name)
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")
        self.assertTrue(status=="success")
        self.assertFalse(result[0]['$1'].get('indexMap',False))

    def test_object_replace(self):
        self.setup_cbas_bucket_dataset_connect()
        self.query = "SELECT object_replace(%s, 'testType', 'devType') from %s;"%(self.cbas_dataset_name,self.cbas_dataset_name)
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")
        self.assertTrue(status=="success")
        self.assertTrue(result[0]['$1']['type']=='devType')

    def test_object_unwrap(self):
        self.setup_cbas_bucket_dataset_connect()
        self.query = "SELECT OBJECT_UNWRAP(data) from %s;"%(self.cbas_dataset_name)
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")
        self.assertTrue(status=="success")
        self.assertTrue(result[0]['$1']=='bar')

    def test_object_values(self):
        self.setup_cbas_bucket_dataset_connect()
        self.query = "SELECT OBJECT_VALUES(indexMap) from %s;"%self.cbas_dataset_name
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")
        expected_result = [
                            "val1",
                            "val2"
                            ]
        self.assertTrue(status=="success")
        self.assertTrue(result[0]['$1']==expected_result)
        
        self.query = "SELECT count(*) from %s where OBJECT_VALUES( indexMap )[0] = 'val1';"%(self.cbas_dataset_name)
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")
        self.assertTrue(status=="success")
        self.assertTrue(result[0]['$1']==1)
        
    def test_object_pairs(self):
        self.setup_cbas_bucket_dataset_connect()
        self.query = "SELECT object_pairs(indexMap) from %s;"%self.cbas_dataset_name
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")
        expected_result = [{
                        "name": "key1",
                        "value": "val1"
                      },
                      {
                        "name": "key2",
                        "value": "val2"
                      }]
        self.assertTrue(status=="success")
        self.assertTrue(result[0]['$1']==expected_result)

    def test_object_length(self):
        self.setup_cbas_bucket_dataset_connect()
        self.query = "SELECT object_length(indexMap) from %s;"%self.cbas_dataset_name
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")
        self.assertTrue(status=="success")
        self.assertTrue(result[0]['$1']==2)
        
    def test_object_names(self):
        self.setup_cbas_bucket_dataset_connect()
        self.query = "SELECT object_names(indexMap) from %s;"%self.cbas_dataset_name
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")

        expected_result = [{"$1": ["key1","key2"]}]
        
        self.assertTrue(status=="success")
        self.assertTrue(result==expected_result)
        
class CBASTuqSanity(QuerySanityTests):
    FORMATS = ["2006-01-02T15:04:05.999+07:00",
           "2006-01-02T15:04:05.999",
           "2006-01-02T15:04:05+07:00",
           "2006-01-02T15:04:05",
           "2006-01-02 15:04:05.999+07:00",
           "2006-01-02 15:04:05.999",
           "2006-01-02 15:04:05+07:00",
           "2006-01-02 15:04:05",
           "2006-01-02",
           "15:04:05.999+07:00",
           "15:04:05.999",
           "15:04:05+07:00",
           "15:04:05"]

#     def test_regex_replace(self):
#         for bucket in self.buckets:
#             self.query = "select name, REGEXP_REPLACE(email, '-mail', 'domain') as mail from %s" % (bucket.name)
# 
#             actual_list = self.run_cbq_query()
#             actual_result = sorted(actual_list['results'])
#             expected_result = [{"name" : doc["name"],
#                                 "mail" : doc["email"].replace('-mail', 'domain')}
#                                for doc in self.full_list]
#             expected_result = sorted(expected_result)
#             self._verify_results(actual_result, expected_result)
            
    def test_to_str(self):
        for bucket in self.buckets:
            self.query = "SELECT TOSTR(join_mo) month FROM %s" % bucket.name
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])

            expected_result = [{"month" : str(doc['join_mo'])} for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_substr(self):
        indices_to_test = [0, 1, 2, 100]
        for index in indices_to_test:
            for bucket in self.buckets:
                self.query = "select name, SUBSTR(email, %s) as DOMAIN from %s" % (
                str(index), bucket.name)
                query_result = self.run_cbq_query()
                query_docs = query_result['results']
                sorted_query_docs = sorted(query_docs,
                                           key=lambda doc: (doc['name'], doc['DOMAIN']))

                expected_result = [{"name": doc["name"],
                                    "DOMAIN": self.expected_substr(doc['email'], 0, index)}
                                   for doc in self.full_list]
                sorted_expected_result = sorted(expected_result, key=lambda doc: (
                doc['name'], doc['DOMAIN']))

                self._verify_results(sorted_query_docs, sorted_expected_result)
                
    def test_let_string(self):
        for bucket in self.buckets:
            self.query = "select name, join_date date from %s let join_date = tostr(join_yr) || '-' || tostr(join_mo)" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])

            expected_result = [{"name" : doc["name"],
                                "date" : '%s-%s' % (doc['join_yr'], doc['join_mo'])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)
            
    def test_with_clause(self):
        for bucket in self.buckets:
            self.query = "WITH avghike AS (SELECT VALUE AVG(hikes[0]) FROM %s AS user)[0] \
            SELECT count(*), avghike \
            FROM %s user \
            WHERE user.hikes[0] > ceil(avghike);" % (bucket.name,bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])[0]

            expected_result = len([doc['hikes'][0] for doc in self.full_list if doc['hikes'][0] > actual_result['avghike']])
            self.assertTrue(actual_result["$1"]==expected_result, "With clause failed.")
            
    def test_array_length(self):
        for bucket in self.buckets:
            self.query = "SELECT _id, array_length(hikes) as hike_count" +\
            " FROM %s " % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['_id']))

            tmp_ids = set([doc['_id'] for doc in self.full_list])
            expected_result = [{"_id" : id,
                                "hike_count" : len([x["hikes"] for x in self.full_list
                                           if x["_id"] == id][0])}
                               for id in tmp_ids]
            expected_result = sorted(expected_result, key=lambda doc: (doc['_id']))
            self._verify_results(actual_result, expected_result)
        
    def test_floor(self):
        for bucket in self.buckets:
            self.query = "select name, floor(test_rate) as rate from %s"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['rate']))

            expected_result = [{"name" : doc['name'], "rate" : math.floor(doc['test_rate'])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where floor(test_rate) > 5"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['name']))
            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if math.floor(doc['test_rate']) > 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_array_avg(self):
        for bucket in self.buckets:
            self.query = "SELECT _id, array_avg(hikes)" +\
            " as avg_hike FROM %s " % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['_id']))
            for doc in actual_result:
                doc['avg_hike'] = round(doc['avg_hike'])
            tmp_ids = set([doc['_id'] for doc in self.full_list])
            expected_result = [{"_id" : id,
                                "avg_hike" : round(sum([x["hikes"] for x in self.full_list
                                           if x["_id"] == id][0]) / float(len([x["hikes"]
                                                                                     for x in self.full_list
                                           if x["_id"] == id][0])))}
                               for id in tmp_ids]
            expected_result = sorted(expected_result, key=lambda doc: (doc['_id']))
            self._verify_results(actual_result, expected_result)

    def test_array_count(self):
        for bucket in self.buckets:
            self.query = "SELECT _id, array_count(hikes) as hike_count" +\
            " FROM %s " % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['_id']))

            tmp_ids = set([doc['_id'] for doc in self.full_list])
            expected_result = [{"_id" : id,
                                "hike_count" : len([x["hikes"] for x in self.full_list
                                           if x["_id"] == id][0])}
                               for id in tmp_ids]
            expected_result = sorted(expected_result, key=lambda doc: (doc['_id']))
            self._verify_results(actual_result, expected_result)

    def test_array_max(self):
        for bucket in self.buckets:
            self.query = "SELECT _id, array_max(hikes) as max_hike" +\
            " FROM %s " % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['_id']))

            tmp_ids = set([doc['_id'] for doc in self.full_list])
            expected_result = [{"_id" : id,
                                "max_hike" : max([x["hikes"] for x in self.full_list
                                           if x["_id"] == id][0])}
                               for id in tmp_ids]
            expected_result = sorted(expected_result, key=lambda doc: (doc['_id']))
            self._verify_results(actual_result, expected_result)
            
    def test_array_min(self):
        for bucket in self.buckets:
            self.query = "SELECT _id, array_min(hikes) as min_hike" +\
            " FROM %s " % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['_id']))

            tmp_ids = set([doc['_id'] for doc in self.full_list])
            expected_result = [{"_id" : id,
                                "min_hike" : min([x["hikes"] for x in self.full_list
                                           if x["_id"] == id][0])}
                               for id in tmp_ids]
            expected_result = sorted(expected_result, key=lambda doc: (doc['_id']))
            self._verify_results(actual_result, expected_result)

    def test_array_sum(self):
        for bucket in self.buckets:
            self.query = "SELECT _id, round(array_sum(hikes)) as total_hike" +\
            " FROM %s" % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['_id']))
            
            tmp_ids = set([doc['_id'] for doc in self.full_list])
            expected_result = [{"_id" : id,
                                "total_hike" : round(sum([x["hikes"] for x in self.full_list
                                           if x["_id"] == id][0]))}
                               for id in tmp_ids]
            expected_result = sorted(expected_result, key=lambda doc: (doc['_id']))
            self._verify_results(actual_result, expected_result)
            
    def test_array_contains(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_contains((select value %s.name from g), 'employee-1')"% (bucket.name)  +\
            " as emp_job FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "emp_job" : 'employee-1' in [x["name"] for x in self.full_list
                                                           if x["job_title"] == group] }
                               for group in tmp_groups]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_array_append(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title," +\
                         " array_append((select DISTINCT value %s.name from g), 'new_name') as names"% (bucket.name) +\
                         " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "names" : sorted(set([x["name"] for x in self.full_list
                                               if x["job_title"] == group] + ['new_name']))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT job_title," +\
                         " array_append((select DISTINCT value %s.name from g), 'new_name','123') as names"% (bucket.name) +\
                         " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))
            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "names" : sorted(set([x["name"] for x in self.full_list
                                               if x["job_title"] == group] + ['new_name'] + ['123']))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_remove(self):
        value = 'employee-1'
        for bucket in self.buckets:
            self.query = "SELECT job_title," +\
                         " array_remove((select DISTINCT value %s.name from g), '%s') as names" % (bucket.name, value) +\
                         " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "names" : sorted(set([x["name"] for x in self.full_list
                                               if x["job_title"] == group and x["name"]!= value]))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

            value1 = 'employee-2'
            value2 = 'emp-2'
            value3 = 'employee-1'
            self.query = "SELECT job_title," +\
                         " array_remove((select DISTINCT value %s.name from g), '%s','%s','%s') as names" % (bucket.name,value1,value2,value3) +\
                         " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))
            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "names" : sorted(set([x["name"] for x in self.full_list
                                               if x["job_title"] == group and x["name"]!= value1 and x["name"]!=value3]))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_prepend(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title," +\
                         " array_prepend(1.2, (select value %s.test_rate from g)) as rates"% (bucket.name) +\
                         " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))
            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "rates" : sorted([x["test_rate"] for x in self.full_list
                                                  if x["job_title"] == group] + [1.2])}

                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)
            self.query = "SELECT job_title," +\
                         " array_prepend(1.2,2.4, (select value %s.test_rate from g)) as rates"% (bucket.name) +\
                         " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "rates" : sorted([x["test_rate"] for x in self.full_list
                                                  if x["job_title"] == group] + [1.2]+[2.4])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT job_title," +\
                         " array_prepend(['skill5', 'skill8'], (select value %s.skills from g)) as skills_new"% (bucket.name) +\
                         " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "skills_new" : sorted([x["skills"] for x in self.full_list
                                                  if x["job_title"] == group] + \
                                                  [['skill5', 'skill8']])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT job_title," +\
                         " array_prepend(['skill5', 'skill8'],['skill9','skill10'], (select value %s.skills from g)) as skills_new"% (bucket.name) +\
                         " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "skills_new" : sorted([x["skills"] for x in self.full_list
                                                  if x["job_title"] == group] + \
                                                  [['skill5', 'skill8']]+ [['skill9','skill10']])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))

            self._verify_results(actual_result, expected_result)

    def test_array_put(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_put((select distinct value %s.name from g), 'employee-1') as emp_job"% (bucket.name) +\
            " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result,
                                   key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "emp_job" : sorted(set([x["name"] for x in self.full_list
                                           if x["job_title"] == group]))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT job_title, array_put((select distinct value %s.name from g), 'employee-50','employee-51') as emp_job"% (bucket.name) +\
            " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result,
                                   key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "emp_job" : sorted(set([x["name"] for x in self.full_list
                                           if x["job_title"] == group]  + ['employee-50'] + ['employee-51']))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT job_title, array_put((select distinct value %s.name from g), 'employee-47') as emp_job"% (bucket.name) +\
            " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result,
                                   key=lambda doc: (doc['job_title']))

            expected_result = [{"job_title" : group,
                                "emp_job" : sorted(set([x["name"] for x in self.full_list
                                           if x["job_title"] == group] + ['employee-47']))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_concat(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title," +\
                         " array_concat((select value %s.name from g), (select value %s.email from g)) as names"% (bucket.name,bucket.name) +\
                         " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result1 = sorted(actual_result, key=lambda doc: (doc['job_title']))
            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "names" : sorted([x["name"] for x in self.full_list
                                                  if x["job_title"] == group] + \
                                                 [x["email"] for x in self.full_list
                                                  if x["job_title"] == group])}
                               for group in tmp_groups]
            expected_result1 = sorted(expected_result, key=lambda doc: (doc['job_title']))

            self._verify_results(actual_result1, expected_result1)

            self.query = "SELECT job_title," +\
                         " array_concat((select value %s.name from g), (select value %s.email from g), (select value %s.join_day from g)) as names"%(bucket.name,bucket.name,bucket.name) +\
                         " FROM %s GROUP BY job_title GROUP AS g limit 10" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result2 = sorted(actual_result, key=lambda doc: (doc['job_title']))


            expected_result = [{"job_title" : group,
                                "names" : sorted([x["name"] for x in self.full_list
                                                  if x["job_title"] == group] + \
                                                 [x["email"] for x in self.full_list
                                                  if x["job_title"] == group] + \
                                                 [x["join_day"] for x in self.full_list
                                                  if x["job_title"] == group])}
                               for group in tmp_groups][0:10]
            expected_result2 = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self.assertTrue(actual_result2==expected_result2)

    def test_array_distinct(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_distinct((select value %s.name from g)) as names"%(bucket.name) +\
            " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "names" : sorted(set([x["name"] for x in self.full_list
                                               if x["job_title"] == group]))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_replace(self):
        for bucket in self.buckets:

            self.query = "SELECT job_title, array_replace((select value %s.name from g), 'employee-1', 'employee-47') as emp_job"%(bucket.name) +\
            " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result,
                                   key=lambda doc: (doc['job_title']))
            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "emp_job" : sorted(["employee-47" if x["name"] == 'employee-1' else x["name"]
                                             for x in self.full_list
                                             if x["job_title"] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_union_symdiff(self):
        for bucket in self.buckets:
            self.query = 'select ARRAY_SORT(ARRAY_UNION(["skill1","skill2","skill2010","skill2011"],skills)) as skills_union from {0} order by meta().id limit 5'.format(bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['results']==([{u'skills_union': [u'skill1', u'skill2', u'skill2010', u'skill2011']}, {u'skills_union': [u'skill1', u'skill2', u'skill2010', u'skill2011']},
                            {u'skills_union': [u'skill1', u'skill2', u'skill2010', u'skill2011']}, {u'skills_union': [u'skill1', u'skill2', u'skill2010', u'skill2011']},
                            {u'skills_union': [u'skill1', u'skill2', u'skill2010', u'skill2011']}]))

            self.query = 'select ARRAY_SORT(ARRAY_SYMDIFF(["skill1","skill2","skill2010","skill2011"],skills)) as skills_diff1 from {0} order by meta().id limit 5'.format(bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['results']==[{u'skills_diff1': [u'skill1', u'skill2']}, {u'skills_diff1': [u'skill1', u'skill2']}, {u'skills_diff1': [u'skill1', u'skill2']}, {u'skills_diff1': [u'skill1', u'skill2']}, {u'skills_diff1': [u'skill1', u'skill2']}])

            self.query = 'select ARRAY_SORT(ARRAY_SYMDIFFN(skills,["skill2010","skill2011","skill2012"],["skills2010","skill2017"])) as skills_diff3 from {0} order by meta().id limit 5'.format(bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'] == [{u'skills_diff3': [u'skill2012', u'skill2017', u'skills2010']}, {u'skills_diff3': [u'skill2012', u'skill2017', u'skills2010']}, {u'skills_diff3': [u'skill2012', u'skill2017', u'skills2010']}, {u'skills_diff3': [u'skill2012', u'skill2017', u'skills2010']}, {u'skills_diff3': [u'skill2012', u'skill2017', u'skills2010']}])

    def test_array_intersect(self):
        self.query = 'select ARRAY_INTERSECT([2011,2012,2016,"test"], [2011,2016], [2012,2016]) as test'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'][0]["test"] == [2016]) 

    def test_array_star(self):
        for bucket in self.buckets:
            self.query = 'SELECT ARRAY_STAR(ARRAY_FLATTEN((select value VMs from %s  where %s.VMs is not missing limit 1),1)) as test'%(bucket.name,bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(len(actual_result['results'][0]["test"]["RAM"]) == 2)
            self.assertTrue(len(actual_result['results'][0]["test"]["memory"]) == 2)
            self.assertTrue(len(actual_result['results'][0]["test"]["name"]) == 2)
            self.assertTrue(len(actual_result['results'][0]["test"]["os"]) == 2)

    def test_array_sort(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_sort((select distinct value %s.test_rate from g)) as emp_job"%(bucket.name) +\
            " FROM %s GROUP BY job_title GROUP AS g" % (bucket.name)
            
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['job_title']))
            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "emp_job" : sorted(set([x["test_rate"] for x in self.full_list
                                             if x["job_title"] == group]))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)
            
    def test_encode_json(self):
        self.query = 'select ENCODE_JSON({"key":"value"})'
        
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['status'] == "success")

    def test_decode_json(self):
        self.query = '''select DECODE_JSON('{"key":"value"}')'''
        
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['status'] == "success")
        self.assertTrue(actual_result['results'][0]['$1']["key"] == "value")
        
    def test_pairs(self):
        for bucket in self.buckets:
            self.query = 'select VMs as orig_t, PAIRS(VMs) AS pairs_t from %s  where %s.VMs is not missing limit 1'%(bucket.name,bucket.name)
            actual_result = self.run_cbq_query()
            pairs_t = actual_result['results'][0]["pairs_t"]
            orig_t = actual_result['results'][0]["orig_t"]
            
            for item in orig_t:
                for key in item.keys():
                    self.assertTrue([key,item[key]] in pairs_t)
                   
    def test_array_flatten(self):
        # Create bucket on CBAS
        self.query = 'INSERT INTO %s (KEY, value) VALUES ("na", {"a":2, "b":[1,2,[31,32,33],4,[[511, 512], 52]]});'%(self.default_bucket_name)
        result = RestConnection(self.master).query_tool(self.query)
        self.assertTrue(result['status'] == "success")
        self.query = 'SELECT ARRAY_FLATTEN(b,1) AS flatten_by_1level FROM default where meta().id = "na";'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'][0]["flatten_by_1level"] == [1,2,31,32,33,4,[511,512],52]) 
             
    def test_check_types(self):
        types_list = [("name", "ISSTR", True), ("skills[0]", "ISSTR", True),
                      ("test_rate", "ISSTR", False), ("VMs", "ISSTR", False),
                      ("false", "ISBOOL", True), ("join_day", "ISBOOL", False),
                      ("VMs", "ISARRAY", True), ("VMs[0]", "ISARRAY", False),
                      ("VMs", "ISATOM", False), ("hikes[0]", "ISATOM", True),("name", "ISATOM", True),
                      ("hikes[0]", "ISNUMBER", True), ("hikes", "ISNUMBER", False),
                      ("skills[0]", "ISARRAY", False), ("skills", "ISARRAY", True)]
        for bucket in self.buckets:
            for name_item, fn, expected_result in types_list:
                self.query = 'SELECT %s(%s) as type_output FROM %s' % (
                                                        fn, name_item, bucket.name)
                actual_result = self.run_cbq_query()
                for doc in actual_result['results']:
                    self.assertTrue(doc["type_output"] == expected_result,
                                    "Expected output for fn %s( %s) : %s. Actual: %s" %(
                                                fn, name_item, expected_result, doc["type_output"]))
                self.log.info("Fn %s(%s) is checked. (%s)" % (fn, name_item, expected_result))
                
    def test_to_string(self):
        for bucket in self.buckets:
            self.query = "SELECT TOSTRING(join_mo) month FROM %s" % bucket.name
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{"month" : str(doc['join_mo'])} for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_regex_contains(self):
        for bucket in self.buckets:
            self.query = "select email from %s where REGEXP_CONTAINS(email, '-m..l')" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"email" : doc["email"]}
                               for doc in self.full_list
                               if len(re.compile('-m..l').findall(doc['email'])) > 0]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)
            
    def test_title(self):
        for bucket in self.buckets:
            self.query = "select TITLE(VMs[0].os) as OS from %s" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])

            expected_result = [{"OS" : (doc["VMs"][0]["os"][0].upper() + doc["VMs"][0]["os"][1:])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_path_expression(self):
        for bucket in self.buckets:
            self.query = "SELECT name as name, tasks_points.task1 as task1 , hikes[2] as hike from %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['name'],doc['hike']))
            expected_result = [{"name": doc['name'],
                                "task1" : doc['tasks_points']['task1'],
                                "hike" : doc['hikes'][2]}
                               for doc in self.full_list]
            
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],doc['hike']))
            
            self._verify_results(actual_result, expected_result)

    def test_like_aliases(self):
        for bucket in self.buckets:
            self.query = "select name AS NAME from %s " % (bucket.name) +\
            "AS EMPLOYEE where EMPLOYEE.name LIKE '_mpl%' ORDER BY name"
            actual_result = self.run_cbq_query()
            expected_result = [{"NAME" : doc['name']} for doc in self.full_list
                               if doc["name"].find('mpl') == 1]
            expected_result = sorted(expected_result, key=lambda doc: (doc['NAME']))
            self._verify_results(actual_result['results'], expected_result)

    def test_like_wildcards(self):
        for bucket in self.buckets:
            self.query = "SELECT email FROM %s WHERE email " % (bucket.name) +\
                         "LIKE '%@%.%' ORDER BY email"
            actual_result = self.run_cbq_query()

            expected_result = [{"email" : doc['email']} for doc in self.full_list
                               if re.match(r'.*@.*\..*', doc['email'])]
            expected_result = sorted(expected_result, key=lambda doc: (doc['email']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT email FROM %s WHERE email" % (bucket.name) +\
                         " LIKE '%@%.h' ORDER BY email"
            actual_result = self.run_cbq_query()
            expected_result = []
            self._verify_results(actual_result['results'], expected_result)
            
    def test_like(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM {0} WHERE job_title LIKE 'S%' ORDER BY name".format(bucket.name)
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if doc["job_title"].startswith('S')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE job_title LIKE '%u%' ORDER BY name".format(
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if doc["job_title"].find('u') != -1]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE job_title NOT LIKE 'S%' ORDER BY name".format(
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if not doc["job_title"].startswith('S')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE job_title NOT LIKE '_ales' ORDER BY name".format(
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if not (doc["job_title"].endswith('ales') and\
                               len(doc["job_title"]) == 5)]

            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)
            
    def test_case_and_like(self):
        for bucket in self.buckets:
            self.query = "SELECT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE (CASE WHEN" % (bucket.name) +\
            " join_mo < 3 OR join_mo > 11 THEN 'winter' ELSE 'other' END) LIKE 'win%'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                doc['name'],doc['period']))
            expected_result = [{"name" : doc['name'],
                                "period" : ('other','winter')
                                            [doc['join_mo'] in [12,1,2]]}
                               for doc in self.full_list
                               if ('other','winter')[doc['join_mo'] in [12,1,2]].startswith(
                                                                                'win')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['period']))
            self._verify_results(actual_result, expected_result)

    def test_case_and_logic_exp(self):
        for bucket in self.buckets:
            self.query = "SELECT DISTINCT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE (CASE WHEN join_mo < 3" %(bucket.name) +\
            " OR join_mo > 11 THEN 1 ELSE 0 END) > 0 AND job_title='Sales'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name'],doc['period']))
            expected_result = [{"name" : doc['name'],
                                "period" : ('other','winter')
                                            [doc['join_mo'] in [12,1,2]]}
                               for doc in self.full_list
                               if (0, 1)[doc['join_mo'] in [12,1,2]] > 0 and\
                                  doc['job_title'] == 'Sales']
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['period']))
            self._verify_results(actual_result, expected_result)

    def test_case_and_comparision_exp(self):
        for bucket in self.buckets:
            self.query = "SELECT DISTINCT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE (CASE WHEN join_mo < 3" %(bucket.name) +\
            " OR join_mo > 11 THEN 1 END) = 1 AND job_title='Sales'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name'],doc['period']))
            expected_result = [{"name" : doc['name'],
                                "period" : ('other','winter')
                                            [doc['join_mo'] in [12,1,2]]}
                               for doc in self.full_list
                               if (doc['join_mo'], 1)[doc['join_mo'] in [12,1,2]] == 1 and\
                                  doc['job_title'] == 'Sales']
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['period']))
            self._verify_results(actual_result, expected_result)

    def test_contains(self):
        for bucket in self.buckets:
            self.query = "select name from %s where contains(job_title, 'Sale')" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])

            expected_result = [{"name" : doc["name"]}
                               for doc in self.full_list
                               if doc['job_title'].find('Sale') != -1]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)
            
    def test_between_bigint(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM {0} WHERE join_mo BETWEEN -9223372036854775808 AND 9223372036854775807 ORDER BY name".format(bucket.name)
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if doc["join_mo"] >= -9223372036854775807 and doc["join_mo"] <= 9223372036854775807]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE join_mo NOT BETWEEN -9223372036854775808 AND 9223372036854775807 ORDER BY name".format(bucket.name)
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if not(doc["join_mo"] >= -9223372036854775807 and doc["join_mo"] <= 9223372036854775807)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)
            
    def test_between_double(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM {0} WHERE join_mo BETWEEN -1.79769313486231570E308  AND 1.79769313486231570E308 ORDER BY name".format(bucket.name)
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if doc["join_mo"] >= -1.79769313486231570E308 and doc["join_mo"] <= 1.79769313486231570E308]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE join_mo NOT BETWEEN -1.79769313486231570E308  AND 1.79769313486231570E308 ORDER BY name".format(bucket.name)
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if not(doc["join_mo"] >= -1.79769313486231570E308 and doc["join_mo"] <= 1.79769313486231570E308)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)
            
    def test_concatenation_where(self):
        for bucket in self.buckets:
            self.query = 'SELECT name, skills' +\
            ' FROM %s WHERE skills[0]=("skill" || "2010")' % (bucket.name)

            actual_list = self.run_cbq_query()

            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"], "skills" : doc["skills"]}
                               for doc in self.full_list
                               if doc["skills"][0] == 'skill2010']
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_meta(self):
        for bucket in self.buckets:
            expected_result = [{"name" : doc['name']} for doc in self.full_list]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))

            self.query = "SELECT distinct name FROM %s WHERE META(%s).id IS NOT NULL"  % (
                                                                                   bucket.name, bucket.name)
            actual_result = self.run_cbq_query()

            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_tan(self):
        self.query = "select tan(radians(45))"
        actual_list = self.run_cbq_query()
        expected_result = 1
        actual_result = int(math.ceil(actual_list['results'][0]['$1']*1000000000000000)/1000000000000000)
        self.assertTrue(actual_result==expected_result, "The result of the query is: %s"%actual_list)
        
    def test_asin(self):
        self.query = "select degrees(asin(0.5))"
        actual_list = self.run_cbq_query()
        actual_result = int(math.ceil(actual_list['results'][0]['$1']*1000000000000000)/1000000000000000)
        expected_result = 30
        self.assertTrue(actual_result==expected_result, "The result of the query is: %s"%actual_list)
        
    def test_clock_formats(self):
        self.query = 'SELECT NOW_LOCAL("2006-01-02") as NOW_LOCAL, \
        NOW_STR("2006-01-02") as NOW_STR, \
        CLOCK_UTC("2006-01-02") as CLOCK_UTC, \
        CLOCK_STR("2006-01-02") as CLOCK_STR, \
        STR_TO_UTC(CLOCK_STR("2006-01-02")) as STR_TO_UTC, \
        CLOCK_LOCAL("2006-01-02") as CLOCK_LOCAL'
        res = self.run_cbq_query()
        now = datetime.datetime.now()
        
        expected = "%s-%02d-%02d" % (now.year, now.month, now.day)
        expected_utc = "%s-%02d-%02d" % (now.year, now.month, now.day+1)
        result = True
        
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)
        
        if not res['results'][0]["NOW_LOCAL"]==expected:
            self.log.info("NOW_LOCAL(2006-01-02) failed.")
            result = False
        if not res['results'][0]["NOW_STR"]==expected:
            self.log.info("NOW_STR(2006-01-02) failed.")
            result = False
        if not (res['results'][0]["CLOCK_UTC"]==expected or res['results'][0]["CLOCK_UTC"]==expected_utc):
            self.log.info("CLOCK_UTC(2006-01-02) failed.")
            result = False
        if not res['results'][0]["CLOCK_STR"]==expected:
            self.log.info("CLOCK_STR(2006-01-02) failed.")
            result = False
        if not res['results'][0]["STR_TO_UTC"]==expected:
            self.log.info("STR_TO_UTC(CLOCK_STR(2006-01-02)) failed.")
            result = False
        if not res['results'][0]["CLOCK_LOCAL"]==expected:
            self.log.info("CLOCK_LOCAL(2006-01-02) failed.")
            result = False
            
        self.assertTrue(result, "Query %s failed."%self.query)
        
    def test_clock_millis(self):
        self.query = "select clock_millis() as now"
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)
        
    def test_clock_local(self):
        self.query = "select clock_local() as now"
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)
        now = datetime.datetime.now()
        expected = "%s-%02d-%02dT" % (now.year, now.month, now.day)
        self.assertTrue(res["results"][0]["now"].startswith(expected),
                        "Result expected: %s. Actual %s" % (expected, res["results"]))
    
    def test_millis_to_local(self):
        now_millis = time.time()
        now_time = datetime.datetime.fromtimestamp(now_millis)
        expected = "%s-%02d-%02dT%02d:%02d" % (now_time.year, now_time.month, now_time.day,
                                         now_time.hour, now_time.minute)
        self.query = "select millis_to_local(%s) as now" % (now_millis * 1000)
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["now"].startswith(expected),
                        "Result expected: %s. Actual %s" % (expected, res["results"]))
        
        self.query = "SELECT MILLIS_TO_STR(1463284740000) as full_date,\
        MILLIS_TO_STR(1463284740000, 'invalid format') as invalid_format,\
        MILLIS_TO_STR(1463284740000, '1111-11-11') as short_date;"
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [{
            "full_date": "2016-05-14T20:59:00-07:00",
            "invalid_format": "2016-05-14T20:59:00-07:00",
            "short_date": "2016-05-14"
            }]
        
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)
          
    def test_now_local(self):
        self.query = "select now_local() as now"
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)
        now = datetime.datetime.now()
        expected = "%s-%02d-%02dT" % (now.year, now.month, now.day)
        self.assertTrue(res["results"][0]["now"].startswith(expected),
                        "Result expected: %s. Actual %s" % (expected, res["results"]))
        
    def test_clock_utc(self):
        self.query = "select clock_utc() as now"
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)        

    def test_DATE_TRUNC_MILLIS(self):
        self.query = "SELECT DATE_TRUNC_MILLIS(1463284740000, 'day') as day,\
       DATE_TRUNC_MILLIS(1463284740000, 'month') as month,\
       DATE_TRUNC_MILLIS(1463284740000, 'year') as year;"
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                      {
                        "day": 1463270400000,
                        "month": 1462060800000,
                        "year": 1451606400000
                      }
                    ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)   
        
    def test_DATE_TRUNC_STR(self):
        self.query = "SELECT DATE_TRUNC_STR('2016-05-18T03:59:00Z', 'day') as day,\
        DATE_TRUNC_STR('2016-05-18T03:59:00Z', 'month') as month,\
        DATE_TRUNC_STR('2016-05-18T03:59:00Z', 'year') as year;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                      {
                        "day": "2016-05-18T00:00:00Z",
                        "month": "2016-05-01T00:00:00Z",
                        "year": "2016-01-01T00:00:00Z"
                      }
                    ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)
    
    def test_DURATION_TO_STR(self):
        self.query = "SELECT DURATION_TO_STR(2000) as microsecs,\
        DURATION_TO_STR(2000000) as millisecs,\
        DURATION_TO_STR(2000000000) as secs;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                      {
                        "microsecs": u"2µs",
                        "millisecs": "2ms",
                        "secs": "2s"
                      }
                    ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)

    def test_STR_TO_TZ(self):
        self.query = "SELECT STR_TO_TZ('1111-11-11T00:00:00+08:00', 'America/New_York') as est,\
        STR_TO_TZ('1111-11-11T00:00:00+08:00', 'UTC') as utc,\
        STR_TO_TZ('1111-11-11', 'UTC') as utc_short;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                      {
                        "est": "1111-11-10T11:00:00-05:00",
                        "utc": "1111-11-10T16:00:00Z",
                        "utc_short": "1111-11-11"
                      }
                    ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)
        
    def test_MILLIS_TO_TZ(self):
        self.query = "SELECT MILLIS_TO_TZ(1463284740000, 'America/New_York') as est,\
        MILLIS_TO_TZ(1463284740000, 'Asia/Kolkata') as ist,\
        MILLIS_TO_TZ(1463284740000, 'UTC') as utc;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                      {
                        "est": "2016-05-14T23:59:00-04:00",
                        "ist": "2016-05-15T09:29:00+05:30",
                        "utc": "2016-05-15T03:59:00Z"
                      }
                    ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)
        
        self.query = "SELECT MILLIS_TO_ZONE_NAME(1463284740000, 'America/New_York') as est,\
        MILLIS_TO_ZONE_NAME(1463284740000, 'Asia/Kolkata') as ist,\
        MILLIS_TO_ZONE_NAME(1463284740000, 'UTC') as utc;"
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)
        
    def test_MILLIS_TO_UTC(self):
        self.query = "SELECT MILLIS_TO_UTC(1463284740000) as full_date,\
        MILLIS_TO_UTC(1463284740000, 'invalid format') as invalid_format,\
        MILLIS_TO_UTC(1463284740000, '1111-11-11') as short_date;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                    {
                        "full_date": "2016-05-15T03:59:00Z",
                        "invalid_format": "2016-05-15T03:59:00Z",
                        "short_date": "2016-05-15"
                    }
                    ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)

    def test_STR_TO_DURATION(self):
        
        self.query = "SELECT STR_TO_DURATION('1h') as hour,\
        STR_TO_DURATION('1us') as microsecond,\
        STR_TO_DURATION('1ms') as millisecond,\
        STR_TO_DURATION('1m') as minute,\
        STR_TO_DURATION('1ns') as nanosecond,\
        STR_TO_DURATION('1s') as second;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                      {
                        "hour": 3600000000000L,
                        "microsecond": 1000,
                        "millisecond": 1000000,
                        "minute": 60000000000L,
                        "nanosecond": 1,
                        "second": 1000000000
                      }
                    ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)

    def test_WEEKDAY_MILLIS(self):
        self.query = "SELECT WEEKDAY_MILLIS(1486237655742) as Day;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                    {
                        "Day": "Saturday"
                    }
                   ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)

    def test_WEEKDAY_STR(self):
        self.query = "SELECT WEEKDAY_STR('2017-02-05') as Day;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                    {
                        "Day": "Sunday"
                    }
                   ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)

    def test_MILLIS(self):
        self.query = 'SELECT MILLIS("2016-05-15T03:59:00Z") as DateStringInMilliseconds;'
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                    {
                        "DateStringInMilliseconds": 1463284740000
                    }
                   ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)
    
class DateTimeFunctionClass_cbas(DateTimeFunctionClass):
    
    def test_date_part_millis(self):
        for count in range(5):
            if count == 0:
                milliseconds = 0
            else:
                milliseconds = random.randint(658979899785, 876578987695)
            for part in PARTS:
                expected_local_query = 'SELECT DATE_PART_STR(MILLIS_TO_STR({0}), "{1}")'.format(milliseconds, part)
                expected_local_result = self.run_cbq_query(expected_local_query)
                actual_local_query = self._generate_date_part_millis_query(milliseconds, part)
                self.log.info(actual_local_query)
                actual_local_result = self.run_cbq_query(actual_local_query)
                self.assertEqual(actual_local_result["results"][0]["$1"], expected_local_result["results"][0]["$1"],
                                 "Actual result {0} and expected result {1} don't match for {2} milliseconds and \
                                 {3} parts".format(actual_local_result["results"][0], expected_local_result["results"][0]["$1"],
                                                   milliseconds, part))


class CBASBacklogQueries(CBASBaseTest):
    """
    cbas.cbas_tuq_sanity.CBASBacklogQueries.test_correlated_aggregation_columns,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds
    """
    def test_correlated_aggregation_columns(self):
        self.log.info("Create a reference to SDK client")
        client = SDKClient(hosts=[self.master.ip], bucket=self.cb_bucket_name, password=self.master.rest_password)

        self.log.info("Insert documents in KV bucket")
        documents = [
            '{"name":"tony","salary":100,"dept":"engineering"}',
            '{"name":"alex","salary":50,"dept":"engineering"}',
            '{"name":"bob","salary":75,"dept":"IT"}',
            '{"name":"charles","salary":175,"dept":"IT"}'
        ]
        client.insert_json_documents("id-", documents)

        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Connect to Local link")
        self.cbas_util.connect_link()

        self.log.info("Validate count on CBAS")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, len(documents)), msg="Count mismatch on CBAS")

        self.log.info("Assert correlated aggregate columns")
        correlated_query = """from ds as e 
                           group by dept group as g
                           let highest_salary = max(e.salary),
                           best_paid = (from g
                           where g.e.salary = highest_salary
                           select g.e.name)
                           select dept, highest_salary, best_paid;"""
        status, result, errors, a, b = self.cbas_util.execute_statement_on_cbas_util(correlated_query)
        self.assertEquals(status, "success", msg="correlated aggregation query failed.")

    """
    cbas.cbas_tuq_sanity.CBASBacklogQueries.test_mod_operator,default_bucket=False
    """
    def test_mod_operator(self):
        query = "select 5 mod 2;"
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(query, "immediate")
        expected_result = {
            "$1": 1
        }
        self.assertTrue(status == "success")
        self.assertTrue(result[0] == expected_result)

    """
    cbas.cbas_tuq_sanity.CBASBacklogQueries.test_div_operator,default_bucket=False
    """
    def test_div_operator(self):
        query = "select 5 div 2;"
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(query, "immediate")
        expected_result = {
            "$1": 2
        }
        self.assertTrue(status == "success")
        self.assertTrue(result[0] == expected_result)

    """
    cbas.cbas_tuq_sanity.CBASBacklogQueries.test_substring_with_negative_position,default_bucket=False
    """
    def test_substring_with_negative_position(self):
        query = 'select SUBSTR("abcdefg",-2);'
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(query, "immediate")
        expected_result = {
            "$1": "fg"
        }
        self.assertTrue(status == "success")
        self.assertTrue(result[0] == expected_result)

    """
    cbas.cbas_tuq_sanity.CBASBacklogQueries.test_substring_with_negative_position_greater_than_length,default_bucket=False
    """
    def test_substring_with_negative_position_greater_than_length(self):
        query = 'select SUBSTR("abcdefg",-8);'
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(query, "immediate")
        expected_result = {
            "$1": None
        }
        self.assertTrue(status == "success")
        self.assertTrue(result[0] == expected_result)

    """
    cbas.cbas_tuq_sanity.CBASBacklogQueries.test_IFINF,default_bucket=False
    """
    def test_IFINF(self):
        query = 'Select IFINF(2,1/0);'
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(query, "immediate")
        expected_result = {
            "$1": 2
        }
        self.assertTrue(status == "success")
        self.assertTrue(result[0] == expected_result)