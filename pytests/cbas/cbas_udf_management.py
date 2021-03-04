'''
Created on 21-February-2020

@author: umang.agrawal
'''

import random

from CbasLib.CBASOperations import CBASHelper
from CbasLib.cbas_entity import Dataverse, Synonym, CBAS_Index, CBAS_UDF
from cbas.cbas_base import CBASBaseTest
from TestInput import TestInputSingleton


class CBASUDF(CBASBaseTest):

    def setUp(self):
        # Setting default value for common test parameters
        self.input = TestInputSingleton.input
        if "bucket_spec" not in self.input.test_params:
            self.input.test_params.update({"bucket_spec": "analytics.default"})
        if "cbas_spec" not in self.input.test_params:
            self.input.test_params.update({"cbas_spec": "local_datasets"})
        if "set_cbas_memory_from_available_free_memory" not in \
                self.input.test_params:
            self.input.test_params.update(
                {"set_cbas_memory_from_available_free_memory": True})
        if "override_spec_params" not in self.input.test_params:
            self.input.test_params.update(
                {"override_spec_params":
                     "num_buckets;num_scopes;num_collections;num_items"})

        super(CBASUDF, self).setUp()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(CBASUDF, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def setup_for_test(self):
        update_spec = {
            "no_of_dataverses": self.input.param('no_of_dv', 2),
            "no_of_datasets_per_dataverse": self.input.param('ds_per_dv', 5),
            "no_of_synonyms": self.input.param('no_of_synonym', 5),
            "no_of_indexes": self.input.param('no_of_index', 5),
            "max_thread_count": self.input.param('no_of_threads', 10),
            "cardinality": self.input.param('cardinality', 0)
        }

        if self.cbas_spec_name:
            self.cbas_spec = self.cbas_util_v2.get_cbas_spec(
                self.cbas_spec_name)
            if update_spec:
                self.cbas_util_v2.update_cbas_spec(self.cbas_spec,
                                                   update_spec, "dataverse")
            cbas_infra_result = \
                self.cbas_util_v2.create_cbas_infra_from_spec(
                    self.cbas_spec, self.bucket_util,
                    wait_for_ingestion=True)
            if not cbas_infra_result[0]:
                self.fail("Error while creating infra from CBAS spec -- " +
                          cbas_infra_result[1])

    def create_udf_object(self, no_of_parameters=0, body_type="expression",
                          dependent_entity_dv="same", use_full_name=True):
        if no_of_parameters == -1:
            parameters = ["..."]
        else:
            parameters = []
            for i in range(0, no_of_parameters):
                parameters.append("param_{0}".format(i))

        dataverse = random.choice(self.cbas_util_v2.dataverses.values())
        dependent_entity = list()
        body_template = {
            "expression": "",
            "dataset": "select value count(*) from {0}",
            "udf": "{0}({1})"
        }
        body = ""
        if body_type == "expression":
            if no_of_parameters == -1:
                body += "ARRAY_SUM(args)"
            elif no_of_parameters > 0:
                for param in parameters:
                    body += "{0}+".format(param)
                body = body.rstrip("+")
            else:
                body = "{0}".format(1)

        def get_dependent_entity_in_a_dv(list_of_objects, dataverse):
            entity = random.choice(list_of_objects)
            if dependent_entity_dv == "same":
                while entity.dataverse_name != dataverse.name:
                    if len(list_of_objects) == 1:
                        dataverse = random.choice(
                            self.cbas_util_v2.dataverses.values())
                    else:
                        entity = random.choice(list_of_objects)
            elif dependent_entity_dv == "diff":
                while entity.dataverse_name == dataverse.name:
                    if len(list_of_objects) == 1:
                        dataverse = random.choice(
                            self.cbas_util_v2.dataverses.values())
                    else:
                        entity = random.choice(list_of_objects)
            return entity

        if body_type == "dataset" or body_type == "synonym":
            
            if body_type == "dataset":
                entity_list = self.cbas_util_v2.list_all_dataset_objs()
            elif body_type == "synonym":
                entity_list = self.cbas_util_v2.list_all_synonym_objs()
            
            if use_full_name:
                dependent_entity.append(get_dependent_entity_in_a_dv(
                    entity_list, dataverse))
                body += body_template["dataset"].format(
                    dependent_entity[0].full_name)
            else:
                dataverse = self.cbas_util_v2.get_dataverse_obj("Default") 
                dependent_entity.append(get_dependent_entity_in_a_dv(
                    entity_list, dataverse))
                body += body_template["dataset"].format(
                    dependent_entity[0].name)
        elif body_type == "udf":
            dependent_entity.append(get_dependent_entity_in_a_dv(
                self.cbas_util_v2.list_all_udf_objs(), dataverse
            ))
            if use_full_name:
                body += body_template[body_type].format(
                    dependent_entity[0].full_name,
                    ",".join(dependent_entity[0].parameters))
            else:
                body += body_template[body_type].format(
                    dependent_entity[0].name,
                    ",".join(dependent_entity[0].parameters))
            parameters = dependent_entity[0].parameters
        
        obj = CBAS_UDF(
            name=self.cbas_util_v2.generate_name(),
            dataverse_name=dataverse.name, parameters=parameters,
            body=body, referenced_entities=dependent_entity)
        dataverse.udfs[obj.full_name] = obj
        return obj

    def test_create_analytics_udf(self):
        self.log.info("Test started")
        self.setup_for_test()
        udf_obj = self.create_udf_object(
            self.input.param('num_create_params', 0),
            self.input.param('body_type', "expression"),
            self.input.param('dependent_entity_dv', "same"),
            self.input.param('use_full_name', True),
        )

        if self.input.param('func_name', None):
            udf_obj.name = self.input.param('func_name')
            udf_obj.full_name = udf_obj.dataverse_name + "." + udf_obj.name
        if self.input.param('no_dataverse', False):
            udf_obj.dataverse_name = None
        if self.input.param('dataverse_name', None):
            udf_obj.dataverse_name = self.input.param('dataverse_name')
        if self.input.param('num_create_params', 0) == -2:
            udf_obj.parameters = None
        if self.input.param('no_body', False):
            udf_obj.body = None
        if self.input.param('invalid_ds', False):
            udf_obj.body = "select count(*) from invalid"
        if self.input.param('custom_params', None):
            if self.input.param('custom_params') == "empty_string":
                udf_obj.parameters = ["", ""]
            elif self.input.param('custom_params') == "mix_param_1":
                udf_obj.parameters = ["a", "b", "..."]
            elif self.input.param('custom_params') == "mix_param_2":
                udf_obj.parameters = ["...", "a", "b"]
            elif self.input.param('custom_params') == "int_param":
                udf_obj.parameters = ["1", "2"]
            elif self.input.param('custom_params') == "bool_param":
                udf_obj.parameters = ["True", "False"]

        if not self.cbas_util_v2.create_udf(
                name=udf_obj.name, dataverse=udf_obj.dataverse_name,
                or_replace=False, parameters=udf_obj.parameters,
                body=udf_obj.body, if_not_exists=False,
                query_context=self.input.param('query_context', False),
                use_statement=self.input.param('use_statement', False),
                validate_error_msg=self.input.param('validate_error', False),
                expected_error=self.input.param('expected_error', None),
                timeout=300, analytics_timeout=300):
            self.fail("Error while creating Analytics UDF")
        if not self.input.param('validate_error', False):
            if self.input.param('no_dataverse', False):
                udf_obj.dataverse_name = "Default"
                udf_obj.reset_full_name()
            if not self.cbas_util_v2.validate_udf_in_metadata(
                    udf_name=udf_obj.name,
                    udf_dataverse_name=udf_obj.dataverse_name,
                    parameters=udf_obj.parameters,
                    body=udf_obj.body,
                    dataset_dependencies=udf_obj.dataset_dependencies,
                    udf_dependencies=udf_obj.udf_dependencies):
                self.fail("Error while validating Function in Metadata")

            if self.input.param('num_execute_params', -1) == -1:
                num_execute_params = len(udf_obj.parameters)
            else:
                num_execute_params = self.input.param('num_execute_params')

            execute_params = [i for i in range(1, num_execute_params + 1)]
            if not execute_params:
                if udf_obj.dataset_dependencies:
                    expected_result = 0
                    for dependency in udf_obj.dataset_dependencies:
                        obj = self.cbas_util_v2.get_dataset_obj(
                            CBASHelper.format_name(dependency[1]), 
                            CBASHelper.format_name(dependency[0]))
                        expected_result += obj.num_of_items
                else:
                    expected_result = 1
            else:
                expected_result=sum(execute_params)
            if not self.cbas_util_v2.verify_function_execution_result(
                    func_name=udf_obj.full_name,
                    func_parameters=execute_params,
                    expected_result=expected_result,
                    validate_error_msg=self.input.param(
                        'validate_execute_error', False),
                    expected_error=self.input.param('expected_error', None)):
                self.fail("Failed while verifying function execution result")
        self.log.info("Test Finished")

    def test_create_multiple_analytics_udfs(self):
        self.log.info("Test started")
        self.setup_for_test()
        udf_objs = list()

        for i in range(0, self.input.param('num_init_udf', 1)):
            udf_obj = self.create_udf_object(
                self.input.param('num_create_params', 0), "expression",
                "same", True)

            if not self.cbas_util_v2.create_udf(
                    name=udf_obj.name, dataverse=udf_obj.dataverse_name,
                    or_replace=False, parameters=udf_obj.parameters,
                    body=udf_obj.body, if_not_exists=False,
                    query_context=False, use_statement=False,
                    validate_error_msg=False, expected_error=None,
                    timeout=300, analytics_timeout=300):
                self.fail("Error while creating Analytics UDF")
            udf_objs.append(udf_obj)

        # Create UDF test to test
        test_udf_obj = self.create_udf_object(
            self.input.param('num_test_udf_params', 0),
            self.input.param('body_type', "expression"),
            self.input.param('dependent_entity_dv', "same"),
            self.input.param('use_full_name', True))

        if self.input.param('test_udf_name', "diff") == "same":
            test_udf_obj.name = udf_objs[0].name
            test_udf_obj.reset_full_name()
        if self.input.param('test_udf_dv', "diff") == "same":
            test_udf_obj.dataverse_name = udf_objs[0].dataverse_name
            test_udf_obj.reset_full_name()
        else:
            while test_udf_obj.dataverse_name == udf_objs[0].dataverse_name:
                test_udf_obj.dataverse_name = random.choice(
                    self.cbas_util_v2.dataverses.values())
        if self.input.param('test_udf_param_name', "diff") == "same":
            test_udf_obj.parameters = udf_objs[0].parameters

        if not self.cbas_util_v2.create_udf(
                name=test_udf_obj.name, dataverse=test_udf_obj.dataverse_name,
                or_replace=self.input.param('or_replace', False),
                parameters=test_udf_obj.parameters,
                body=test_udf_obj.body,
                if_not_exists=self.input.param('if_not_exists', False),
                query_context=False, use_statement=False,
                validate_error_msg=self.input.param('validate_error', False),
                expected_error=self.input.param('expected_error', "").format(
                    CBASHelper.unformat_name(test_udf_obj.full_name)),
                timeout=300, analytics_timeout=300):
            self.fail("Error while creating Analytics UDF")

        if not self.input.param('validate_error', False):
            if self.input.param('if_not_exists', False):
                object_to_validate = udf_objs[0]
            else:
                object_to_validate = test_udf_obj
            if not self.cbas_util_v2.validate_udf_in_metadata(
                    udf_name=object_to_validate.name,
                    udf_dataverse_name=object_to_validate.dataverse_name,
                    parameters=object_to_validate.parameters,
                    body=object_to_validate.body,
                    dataset_dependencies=object_to_validate.dataset_dependencies,
                    udf_dependencies=object_to_validate.udf_dependencies):
                self.fail("Error while validating Function in Metadata")

            if self.input.param('num_execute_params', -1) == -1:
                num_execute_params = len(test_udf_obj.parameters)
            else:
                num_execute_params = self.input.param('num_execute_params')

            execute_params = [i for i in range(1, num_execute_params + 1)]
            if not self.cbas_util_v2.verify_function_execution_result(
                    test_udf_obj.full_name, execute_params,
                    sum(execute_params)):
                self.fail("Failed while verifying function execution result")
        self.log.info("Test Finished")

    def test_drop_analytics_udf(self):
        self.log.info("Test started")
        self.setup_for_test()

        udf_obj = self.create_udf_object(
            self.input.param('num_create_params', 0),
            self.input.param('body_type', "expression"), "same", True)

        if not self.cbas_util_v2.create_udf(
                name=udf_obj.name, dataverse=udf_obj.dataverse_name,
                or_replace=False, parameters=udf_obj.parameters,
                body=udf_obj.body, if_not_exists=False,
                query_context=False, use_statement=False,
                validate_error_msg=False, expected_error=None,
                timeout=300, analytics_timeout=300):
            self.fail("Error while creating Analytics UDF")

        if self.input.param('second_udf', False):
            # Create UDF using another UDF
            udf_obj_2 = self.create_udf_object(
                self.input.param('num_test_udf_params', 0),
                "udf", self.input.param('dependent_entity_dv', "same"),
                self.input.param('use_full_name', True))

            if not self.cbas_util_v2.create_udf(
                    name=udf_obj_2.name, dataverse=udf_obj_2.dataverse_name,
                    or_replace=False, parameters=udf_obj_2.parameters,
                    body=udf_obj_2.body, if_not_exists=False,
                    query_context=False, use_statement=False,
                    validate_error_msg=False, expected_error=None,
                    timeout=300, analytics_timeout=300):
                self.fail("Error while creating Analytics UDF")

        if self.input.param('no_params', False):
            udf_obj.parameters = None
        if self.input.param('invalid_name', False):
            udf_obj.name = "invalid"
        if self.input.param('invalid_dataverse', False):
            udf_obj.dataverse_name = "invalid"
        if isinstance(self.input.param('change_params', None), int):
            if self.input.param('change_params', None) == -1:
                udf_obj.parameters = ["..."]
            else:
                udf_obj.parameters = []
                for i in range(0, self.input.param('change_params', None)):
                    udf_obj.parameters.append(
                        CBASHelper.format_name(self.cbas_util_v2.generate_name()))

        if not self.cbas_util_v2.drop_udf(
                name=udf_obj.name, dataverse=udf_obj.dataverse_name,
                parameters=udf_obj.parameters,
                if_exists=self.input.param('if_exists', False),
                use_statement=self.input.param('use_statement', False),
                query_context=self.input.param('query_context', False),
                validate_error_msg=self.input.param('validate_error', False),
                expected_error=self.input.param('expected_error', None),
                timeout=300, analytics_timeout=300):
            self.fail("Failed to drop Analytics UDF")

        if not (self.input.param('validate_error', False) or
                self.input.param('if_exists', False)):
            if self.cbas_util_v2.validate_udf_in_metadata(
                    udf_name=udf_obj.name,
                    udf_dataverse_name=udf_obj.dataverse_name,
                    parameters=udf_obj.parameters,
                    body=udf_obj.body,
                    dataset_dependencies=udf_obj.dataset_dependencies,
                    udf_dependencies=udf_obj.udf_dependencies):
                self.fail("Metadata entry for UDF is still present even "
                          "after dropping the UDF")
        self.log.info("Test Finished")

    def test_create_dataset_with_udf_in_where_clause(self):
        self.log.info("Test started")
        self.setup_for_test()

        udf_obj = self.create_udf_object(2, "expression", "same", True)

        if not self.cbas_util_v2.create_udf(
                name=udf_obj.name, dataverse=udf_obj.dataverse_name,
                or_replace=False, parameters=udf_obj.parameters,
                body=udf_obj.body, if_not_exists=False,
                query_context=False, use_statement=False,
                validate_error_msg=False, expected_error=None,
                timeout=300, analytics_timeout=300):
            self.fail("Error while creating Analytics UDF")
            
        if not self.cbas_util_v2.create_dataset(
                dataset_name=self.cbas_util_v2.generate_name(),
                kv_entity=(
                    self.cbas_util_v2.list_all_dataset_objs()[0]
                    ).full_kv_entity_name,
                dataverse_name=udf_obj.dataverse_name,
                where_clause="age > {0}({1})".format(
                    udf_obj.full_name, ",".join(udf_obj.parameters)),
                validate_error_msg=True,
                expected_error="Illegal use of user-defined function {"
                               "0}".format(
                                   CBASHelper.format_name(udf_obj.full_name)),
                timeout=300, analytics_timeout=300,
                analytics_collection=False):
            self.fail("Dataset creation was successfull while using user "
                      "defined function in where clause of the DDL")
        self.log.info("Test Finished")

    def test_drop_dataset_while_it_is_being_used_by_UDF(self):
        self.log.info("Test started")
        self.setup_for_test()

        udf_obj = self.create_udf_object(
            2, "dataset", self.input.param('dependent_entity_dv', "same"), True)

        if not self.cbas_util_v2.create_udf(
                name=udf_obj.name, dataverse=udf_obj.dataverse_name,
                or_replace=False, parameters=udf_obj.parameters,
                body=udf_obj.body, if_not_exists=False,
                query_context=False, use_statement=False,
                validate_error_msg=False, expected_error=None,
                timeout=300, analytics_timeout=300):
            self.fail("Error while creating Analytics UDF")
        
        dataset_name=CBASHelper.format_name(*udf_obj.dataset_dependencies[0])
        if not self.cbas_util_v2.drop_dataset(
                dataset_name=dataset_name,
                validate_error_msg=True,
                expected_error="Cannot drop dataset", 
                expected_error_code=24142,
                timeout=300, analytics_timeout=300):
            self.fail("Successfully dropped dataset being used by a UDF")
        self.log.info("Test Finished")

    def test_drop_dataverse_with_udf_and_dependent_entities(self):
        self.log.info("Test started")
        self.setup_for_test()

        udf_obj = self.create_udf_object(
            2, "dataset", self.input.param('dependent_entity_dv', "same"),
            True)

        if not self.cbas_util_v2.create_udf(
                name=udf_obj.name, dataverse=udf_obj.dataverse_name,
                or_replace=False, parameters=udf_obj.parameters,
                body=udf_obj.body, if_not_exists=False,
                query_context=False, use_statement=False,
                validate_error_msg=False, expected_error=None,
                timeout=300, analytics_timeout=300):
            self.fail("Error while creating Analytics UDF")

        if not self.cbas_util_v2.drop_dataverse(
                dataverse_name=CBASHelper.format_name(
                    udf_obj.dataset_dependencies[0][0]),
                validate_error_msg=self.input.param('validate_error', False),
                expected_error=self.input.param('expected_error', None),
                timeout=300, analytics_timeout=300, delete_dataverse_obj=True,
                disconnect_local_link=True):
            self.fail("Successfully dropped dataverse being used by a UDF")
        self.log.info("Test Finished")