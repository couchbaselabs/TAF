import copy
import random
import time

from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase
from cbas_utils.cbas_utils_on_prem import CBASRebalanceUtil

# External Database loader related imports
from Jython_tasks.sirius_task import MongoUtil, CouchbaseUtil
from sirius_client_framework.sirius_constants import SiriusCodes

# Import kafka utils
from couchbase_utils.kafka_util.msk_utils import MSKUtils


class SelectOrder(ColumnarOnPremBase):

    def setUp(self):
        super(SelectOrder, self).setUp()

        # Columnar entities creation specifications
        self.columnar_spec_name = self.input.param(
            "columnar_spec_name", "full_template")

        for cluster_name, cluster in self.cb_clusters.items():
            if hasattr(cluster, "cbas_cc_node"):
                self.analytics_cluster = cluster
            else:
                self.remote_cluster = cluster

        # Remove this once the feature is released.
        self.log.info("Setting compilerOrderfields to True")
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(
            self.analytics_cluster, {"compilerOrderfields": True})
        if not status:
            self.fail("Unable to set compilerOrderfields to True")
        status, _, _ = self.cbas_util.restart_analytics_cluster_uri(
            self.analytics_cluster)
        if not status:
            self.fail("Unable to restart the cluster")

        self.sdk_clients_per_user = self.input.param("sdk_clients_per_user", 1)

        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task, False,
            self.cbas_util)

    def tearDown(self):
        self.cbas_util.cleanup_cbas(self.analytics_cluster)

        if not self.msk_util.kafka_cluster_util.delete_topic_by_topic_prefix(
            self.kafka_topic_prefix):
            self.fail(f"Unable to either cleanup AWS Kafka resources or "
                      f"Confluent Kafka resources")
        super(SelectOrder, self).tearDown()

    def setup_infra(self):
        # Initialize variables for Kafka
        self.kafka_topic_prefix = f"select_order_{int(time.time())}"

        # Initializing AWS_MSK util and AWS_MSK cluster object.
        self.msk_util = MSKUtils(
            access_key=self.aws_access_key, secret_key=self.aws_secret_key,
            region=self.input.param("msk_region", "us-east-1"))
        self.msk_cluster_obj = self.msk_util.generate_msk_cluster_object(
            msk_cluster_name=self.input.param("msk_cluster_name"),
            topic_prefix=self.kafka_topic_prefix,
            sasl_username=self.input.param("msk_username"),
            sasl_password=self.input.param("msk_password"))
        if not self.msk_cluster_obj:
            self.fail("Unable to initialize AWS Kafka cluster object")

        self.kafka_topics = {
            "aws_kafka": {
                "MONGODB": [
                    {
                        "topic_name": "do-not-delete-mongo-cdc.Product_Template.10GB",
                        "key_serialization_type": "json",
                        "value_serialization_type": "json",
                        "cdc_enabled": True,
                        "source_connector": "DEBEZIUM",
                        "num_items": 10000000
                    }
                ],
                "POSTGRESQL": [],
                "MYSQLDB": []
            }
        }

        self.log.info("Creating Buckets, Scopes and Collection on Remote "
                      "cluster.")
        self.collectionSetUp(cluster=self.remote_cluster, load_data=False,
                             create_sdk_clients=False)

        self.couchbase_doc_loader = CouchbaseUtil(
            task_manager=self.task_manager,
            hostname=self.remote_cluster.master.ip,
            username=self.remote_cluster.master.rest_username,
            password=self.remote_cluster.master.rest_password,
        )

        for remote_bucket in self.remote_cluster.buckets:
            for scope_name, scope in remote_bucket.scopes.items():
                if scope_name != "_system":
                    for collection_name, collection in (
                            scope.collections.items()):
                        self.log.info(
                            f"Loading docs in {remote_bucket.name}."
                            f"{scope_name}.{collection_name}")
                        cb_doc_loading_task = self.couchbase_doc_loader.load_docs_in_couchbase_collection(
                            bucket=remote_bucket.name, scope=scope_name,
                            collection=collection_name, start=0,
                            end=self.num_items,
                            doc_template=SiriusCodes.Templates.PRODUCT,
                            doc_size=self.doc_size, sdk_batch_size=1000
                        )
                        if not cb_doc_loading_task.result:
                            self.fail(
                                f"Failed to load docs in couchbase "
                                f"collection {remote_bucket.name}."
                                f"{scope_name}.{collection_name}")
                        else:
                            collection.num_items = cb_doc_loading_task.success_count

        aws_kafka_cluster_details = [
            self.msk_util.generate_aws_kafka_cluster_detail(
                brokers_url=self.msk_cluster_obj.bootstrap_brokers[
                    "PublicSaslScram"],
                auth_type="SCRAM_SHA_512", encryption_type="TLS",
                username=self.msk_cluster_obj.sasl_username,
                password=self.msk_cluster_obj.sasl_password)]

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"],
            path_on_external_container="level_1_folder_1/level_2_folder_1"
                                       "/level_3_folder_1/",
            aws_kafka_cluster_details=aws_kafka_cluster_details,
            external_dbs=["MONGODB"],
            kafka_topics=self.kafka_topics)

        self.columnar_spec["kafka_dataset"]["primary_key"] = [
            {"_id": "string"}]
        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"id": "string"}]
        self.columnar_spec["index"]["indexed_fields"] = ["price:double"]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.analytics_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])

        if not result:
            self.fail(msg)

        self.cbas_util.refresh_remote_dataset_item_count(
            self.bucket_util)

        self.standalone_collections_kafka = list()
        self.standalone_collections = list()
        for standalone_collection in self.cbas_util.get_all_dataset_objs(
                "standalone"):
            if not standalone_collection.data_source:
                self.standalone_collections.append(standalone_collection)
            elif standalone_collection.data_source in [
                "MONGODB", "MYSQLDB", "POSTGRESQL"]:
                self.standalone_collections_kafka.append(standalone_collection)

        # Wait for kafka collection to ingest around 1000 docs.
        for collection in self.standalone_collections_kafka:
            while (self.cbas_util.get_num_items_in_cbas_dataset(
                    self.analytics_cluster, collection.full_name) <
                   self.num_items):
                continue
            if not self.cbas_util.disconnect_link(
                    self.analytics_cluster, collection.link_name):
                self.fail(f"Unable to disconnect {collection.link_name}")

        self.log.info("Copy from S3 into standalone collection")
        for standalone_collection in self.standalone_collections:
            if not (
                    self.cbas_util.copy_from_external_resource_into_standalone_collection(
                        cluster=self.analytics_cluster,
                        collection_name=standalone_collection.name,
                        aws_bucket_name=self.s3_source_bucket,
                        external_link_name=self.cbas_util.get_all_link_objs(
                            "s3")[0].full_name,
                        dataverse_name=standalone_collection.dataverse_name,
                        database_name=standalone_collection.database_name,
                        path_on_aws_bucket="level_1_folder_1/level_2_folder_1"
                                           "/level_3_folder_1/",
                        files_to_include=["*.json"]
                    )):
                self.fail(f"Failed to copy data from s3 into standalone "
                          f"collection {standalone_collection.full_name}")

    def fetch_doc_keys(self, dataset_name):
        query = f"select value c from {dataset_name} as c limit 1;"
        status, _, errors, results, _, _ = (
            self.cbas_util.execute_statement_on_cbas_util(
                cluster=self.analytics_cluster, statement=query
            ))
        if status:
            sample_doc = results[0]
            return list(sample_doc.keys())
        else:
            self.fail(f"Error while running query {query} : {errors}")

    def test_simple_select_queries(self):
        self.setup_infra()
        for collection in self.cbas_util.get_all_dataset_objs():
            list_of_keys = self.fetch_doc_keys(collection.full_name)
            list_of_keys.append("10*2 as mul")
            random.shuffle(list_of_keys)
            if "_id" in list_of_keys:
                order_by = "`_id`"
                list_of_keys[list_of_keys.index("_id")] = order_by
            else:
                order_by = "`id`"
                list_of_keys[list_of_keys.index("id")] = order_by
            with_select_order_query = (
                f"select {','.join(list_of_keys)} from {collection.full_name} "
                f"order by {order_by} limit 100;")
            without_select_order_query = (
                f"Set `compiler.orderfields` \"false\"; select "
                f"{','.join(list_of_keys)} from {collection.full_name} order "
                f"by {order_by} limit 100;")

            list_of_keys[list_of_keys.index("10*2 as mul")] = "mul"

            self.log.info("Running query with select order as True")
            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster,
                    statement=with_select_order_query
                ))
            if status:
                for doc in results:
                    actual_list_of_keys, expected_list_of_keys = self.get_common_keys(
                        list(doc.keys()), list_of_keys, order_by
                    )
                    if actual_list_of_keys.index("mul") != expected_list_of_keys.index("mul"):
                        self.fail(f"Select order was not honoured. Expected "
                                  f"{expected_list_of_keys} Actual "
                                  f"{actual_list_of_keys}")
            else:
                self.fail(f"Error while running query "
                          f"{with_select_order_query} : {errors}")

            self.log.info("Running query with select order as False")
            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster,
                    statement=without_select_order_query
                ))
            if status:
                for doc in results:
                    if not (list(doc.keys())[1] == "mul" or list(
                            doc.keys())[0] == "mul"):
                        self.fail(f"Select order was honoured. Expected "
                                  f"{list_of_keys} Actual {doc.keys()}")
            else:
                self.fail(f"Error while running query "
                          f"{without_select_order_query} : {errors}")

    def test_select_queries_with_subqueries(self):
        self.setup_infra()
        for collection in self.cbas_util.get_all_dataset_objs():
            list_of_keys = self.fetch_doc_keys(collection.full_name)
            if "_id" in list_of_keys:
                order_by = "`_id`"
                list_of_keys[list_of_keys.index("_id")] = order_by
            else:
                order_by = "`id`"
                list_of_keys[list_of_keys.index("id")] = order_by

            if order_by in list_of_keys[:len(list_of_keys)//2]:
                list_of_keys_with_id = list_of_keys[:len(list_of_keys)//2]
                list_of_keys_with_custom_field = list_of_keys[
                                                 len(list_of_keys) // 2:]
            else:
                list_of_keys_with_custom_field = list_of_keys[
                                                 :len(list_of_keys) // 2]
                list_of_keys_with_id = list_of_keys[len(list_of_keys) // 2:]

            list_of_keys_with_custom_field.append("10*2 as mul")
            random.shuffle(list_of_keys_with_custom_field)
            random.shuffle(list_of_keys_with_id)

            with_select_order_query = (
                f"select (select {','.join(list_of_keys_with_id)} from"
                f" {collection.full_name} order by {order_by} limit 100) "
                f"as list_of_keys_with_id, (select "
                f"{','.join(list_of_keys_with_custom_field)} from"
                f" {collection.full_name} order by {order_by} limit 100) "
                f"as list_of_keys_with_custom_field;")

            without_select_order_query = (
                f"Set `compiler.orderfields` \"false\"; "
                f"select (select {','.join(list_of_keys_with_id)} from"
                f" {collection.full_name} order by {order_by} limit 100) "
                f"as list_of_keys_with_id, (select "
                f"{','.join(list_of_keys_with_custom_field)} from"
                f" {collection.full_name} order by {order_by} limit 100) "
                f"as list_of_keys_with_custom_field;")

            list_of_keys_with_custom_field[
                list_of_keys_with_custom_field.index("10*2 as mul")] = "mul"

            self.log.info("Running query with select order as True")
            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster,
                    statement=with_select_order_query
                ))
            if status:
                doc = results[0]
                for list_name, docs in doc.items():
                    for item in docs:
                        if list_name == "list_of_keys_with_id":
                            actual_list_of_keys, expected_list_of_keys = self.get_common_keys(
                                list(item.keys()), list_of_keys_with_id,
                                order_by)
                            if (actual_list_of_keys.index(order_by.strip("`")) !=
                                    expected_list_of_keys.index(order_by)):
                                self.fail(
                                    f"Select order was not honoured. Expected "
                                    f"{expected_list_of_keys} "
                                    f"Actual {actual_list_of_keys}")
                        else:
                            actual_list_of_keys, expected_list_of_keys = self.get_common_keys(
                                list(item.keys()),
                                list_of_keys_with_custom_field, order_by)
                            if (actual_list_of_keys.index("mul") !=
                                    expected_list_of_keys.index("mul")):
                                self.fail(
                                    f"Select order was not honoured. Expected "
                                    f"{expected_list_of_keys} "
                                    f"Actual {actual_list_of_keys}")
            else:
                self.fail(f"Error while running query "
                          f"{with_select_order_query} : {errors}")

            self.log.info("Running query with select order as False")
            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster,
                    statement=without_select_order_query
                ))
            if status:
                doc = results[0]
                for list_name, docs in doc.items():
                    if list_name != "list_of_keys_with_id":
                        for item in docs:
                            if list(item.keys())[0] != "mul":
                                self.fail(
                                    f"Select order was not honoured. Expected "
                                    f"{list_of_keys_with_custom_field} "
                                    f"Actual {item.keys()}")
            else:
                self.fail(f"Error while running query "
                          f"{without_select_order_query} : {errors}")

    def test_select_query_in_udf(self):
        self.setup_infra()
        for collection in self.cbas_util.get_all_dataset_objs():
            list_of_keys = self.fetch_doc_keys(collection.full_name)
            list_of_keys.append("10*2 as mul")
            random.shuffle(list_of_keys)
            if "_id" in list_of_keys:
                order_by = "`_id`"
                list_of_keys[list_of_keys.index("_id")] = order_by
            else:
                order_by = "`id`"
                list_of_keys[list_of_keys.index("id")] = order_by

            select_query = f"select {','.join(list_of_keys)} from " \
                           f"{collection.full_name} order by {order_by} " \
                           f"limit 100"

            udf_name = self.cbas_util.generate_name()
            if not self.cbas_util.create_udf(
                    cluster=self.analytics_cluster, name=udf_name,
                    body=select_query):
                self.fail("Error while creating UDF")

            list_of_keys[list_of_keys.index("10*2 as mul")] = "mul"

            self.log.info("Running query with select order as True")
            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster,
                    statement=f"{udf_name}();"
                ))
            if status:
                for doc in results:
                    actual_list_of_keys, expected_list_of_keys = self.get_common_keys(
                        list(doc.keys()), list_of_keys, order_by)
                    if actual_list_of_keys.index("mul") != expected_list_of_keys.index(
                            "mul"):
                        self.fail(f"Select order was not honoured. Expected "
                                  f"{expected_list_of_keys} Actual {actual_list_of_keys}")
            else:
                self.fail(f"Error while running query {udf_name}() : {errors}")

            self.log.info("Running query with select order as False")
            without_select_order_query = (
                f"Set `compiler.orderfields` \"false\"; {udf_name}();")
            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster,
                    statement=without_select_order_query
                ))
            if status:
                for doc in results:
                    if not (list(doc.keys())[1] == "mul" or list(
                            doc.keys())[0] == "mul"):
                        self.fail(f"Select order was honoured. Expected "
                                  f"{list_of_keys} Actual {doc.keys()}")
            else:
                self.fail(f"Error while running query "
                          f"{without_select_order_query} : {errors}")

    def test_udf_in_select_query(self):
        self.setup_infra()
        udf_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_udf(
            cluster=self.analytics_cluster, name=udf_name, body="10*2"):
            self.fail(f"Error while creating UDF {udf_name}")
        for collection in self.cbas_util.get_all_dataset_objs():
            list_of_keys = self.fetch_doc_keys(collection.full_name)
            list_of_keys.append(f"{udf_name}() as udf_result")
            random.shuffle(list_of_keys)
            if "_id" in list_of_keys:
                order_by = "`_id`"
                list_of_keys[list_of_keys.index("_id")] = order_by
            else:
                order_by = "`id`"
                list_of_keys[list_of_keys.index("id")] = order_by
            with_select_order_query = (
                f"select {','.join(list_of_keys)} from {collection.full_name} "
                f"order by {order_by} limit 100;")
            without_select_order_query = (
                f"Set `compiler.orderfields` \"false\"; select "
                f"{','.join(list_of_keys)} from {collection.full_name} order "
                f"by {order_by} limit 100;")

            list_of_keys[list_of_keys.index(
                f"{udf_name}() as udf_result")] = "udf_result"

            self.log.info("Running query with select order as True")
            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster,
                    statement=with_select_order_query
                ))
            if status:
                for doc in results:
                    actual_list_of_keys, expected_list_of_keys = self.get_common_keys(
                        list(doc.keys()), list_of_keys, order_by)
                    if actual_list_of_keys.index(
                            "udf_result") != expected_list_of_keys.index("udf_result"):
                        self.fail(f"Select order was not honoured. Expected "
                                  f"{expected_list_of_keys} Actual {actual_list_of_keys}")
            else:
                self.fail(f"Error while running query "
                          f"{with_select_order_query} : {errors}")

            self.log.info("Running query with select order as False")
            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster,
                    statement=without_select_order_query
                ))
            if status:
                for doc in results:
                    if not (list(doc.keys())[1] == "udf_result" or list(
                            doc.keys())[0] == "udf_result"):
                        self.fail(f"Select order was honoured. Expected "
                                  f"{list_of_keys} Actual {doc.keys()}")
            else:
                self.fail(f"Error while running query "
                          f"{without_select_order_query} : {errors}")

    def test_select_order_in_view(self):
        self.setup_infra()
        for collection in self.cbas_util.get_all_dataset_objs():
            list_of_keys = self.fetch_doc_keys(collection.full_name)
            list_of_keys.append("10*2 as mul")
            random.shuffle(list_of_keys)
            if "_id" in list_of_keys:
                order_by = "`_id`"
                list_of_keys[list_of_keys.index("_id")] = order_by
            else:
                order_by = "`id`"
                list_of_keys[list_of_keys.index("id")] = order_by

            view_name = collection.name + "_view"
            view_def = f"select {','.join(list_of_keys)} from" \
                       f" {collection.full_name} order by {order_by} limit 100"
            if not self.cbas_util.create_analytics_view(
                cluster=self.analytics_cluster, view_full_name=view_name,
                view_defn=view_def
            ):
                self.fail(f"Unable to create analytics view on "
                          f"{collection.full_name}")

            with_select_order_query = f"select value c from {view_name} as c;"
            without_select_order_query = (
                f"Set `compiler.orderfields` \"false\"; "
                f"select value c from {view_name} as c;")

            list_of_keys[list_of_keys.index("10*2 as mul")] = "mul"

            self.log.info("Running query with select order as True")
            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster,
                    statement=with_select_order_query
                ))
            if status:
                for doc in results:
                    actual_list_of_keys, expected_list_of_keys = self.get_common_keys(
                        list(doc.keys()), list_of_keys, order_by)
                    if actual_list_of_keys.index("mul") != expected_list_of_keys.index(
                            "mul"):
                        self.fail(f"Select order was not honoured. Expected "
                                  f"{expected_list_of_keys} Actual {actual_list_of_keys}")
            else:
                self.fail(f"Error while running query "
                          f"{with_select_order_query} : {errors}")

            self.log.info("Running query with select order as False")
            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster,
                    statement=without_select_order_query
                ))
            if status:
                for doc in results:
                    if not (list(doc.keys())[1] == "mul" or list(
                            doc.keys())[0] == "mul"):
                        self.fail(f"Select order was honoured. Expected "
                                  f"{list_of_keys} Actual {doc.keys()}")
            else:
                self.fail(f"Error while running query "
                          f"{without_select_order_query} : {errors}")

    def test_select_order_in_tabular_view(self):
        self.setup_infra()
        for collection in self.cbas_util.get_all_dataset_objs():
            query = f"select value c from {collection.full_name} as c limit 1;"
            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster, statement=query
                ))
            if status:
                sample_doc = results[0]
                key_type_info = dict()
                for key, value in sample_doc.items():
                    if type(value).__name__ == "float":
                        key_type_info[key] = "DOUBLE"
                    elif type(value).__name__ == "str":
                        key_type_info[key] = "STRING"
                    elif type(value).__name__ == "int":
                        key_type_info[key] = "BIGINT"
                    elif type(value).__name__ == "bool":
                        key_type_info[key] = "BOOLEAN"
                key_type_info["mul"] = "BIGINT"
            else:
                self.fail(f"Error while running query {query} : {errors}")

            if "_id" in key_type_info:
                order_by = "`_id`"
            else:
                order_by = "`id`"

            tabular_view_name = self.cbas_util.generate_name()
            tabular_view_query = f"CREATE OR REPLACE VIEW {tabular_view_name}"
            view_def = ""
            select_fields = ""
            list_of_keys = list(key_type_info.keys())
            random.shuffle(list_of_keys)
            for key in list_of_keys:
                view_def += f"{key} {key_type_info[key]}, "
                if key == "mul":
                    select_fields += f"10*2 as mul, "
                else:
                    if key == order_by.strip("`"):
                        select_fields += f"`{key}`, "
                    else:
                        select_fields += f"{key}, "
            view_def = view_def.rstrip(", ")
            select_fields = select_fields.rstrip(", ")

            tabular_view_query += (
                f"({view_def}) DEFAULT NULL PRIMARY KEY ({order_by}) NOT "
                f"ENFORCED AS SELECT {select_fields} FROM "
                f"{collection.full_name};")

            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster,
                    statement=tabular_view_query))
            if not status:
                self.fail(f"Error while creating Tabular view : {errors}")

            with_select_order_query = f"select value c from {tabular_view_name} as c;"
            without_select_order_query = (
                f"Set `compiler.orderfields` \"false\"; "
                f"select value c from {tabular_view_name} as c;")

            self.log.info("Running query with select order as True")
            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster,
                    statement=with_select_order_query
                ))
            if status:
                for doc in results:
                    if list(doc.keys()).index("mul") != list_of_keys.index(
                            "mul"):
                        self.fail(f"Select order was not honoured. Expected "
                                  f"{list_of_keys} Actual {doc.keys()}")
            else:
                self.fail(f"Error while running query "
                          f"{with_select_order_query} : {errors}")

            self.log.info("Running query with select order as False")
            status, _, errors, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    cluster=self.analytics_cluster,
                    statement=without_select_order_query
                ))
            if status:
                for doc in results:
                    if list(doc.keys()).index("mul") != list_of_keys.index(
                            "mul"):
                        self.fail(f"Select order was not honoured. Expected "
                                  f"{list_of_keys} Actual {doc.keys()}")
            else:
                self.fail(f"Error while running query "
                          f"{without_select_order_query} : {errors}")

    def get_common_keys(self, actual_list_of_keys, expected_list_of_keys,
                        order_by):
        if len(actual_list_of_keys) == len(expected_list_of_keys):
            return actual_list_of_keys, expected_list_of_keys
        else:
            actual_list_of_keys = copy.deepcopy(actual_list_of_keys)
            expected_list_of_keys = copy.deepcopy(expected_list_of_keys)
            if order_by in expected_list_of_keys:
                expected_list_of_keys[expected_list_of_keys.index(order_by)]\
                    = order_by.strip("`")
            if len(actual_list_of_keys) > len(expected_list_of_keys):
                missing_keys = set(actual_list_of_keys) - set(
                    expected_list_of_keys)
                for missing_key in missing_keys:
                    actual_list_of_keys.pop(actual_list_of_keys.index(missing_key))
            else:
                missing_keys = set(expected_list_of_keys) - set(actual_list_of_keys)
                for missing_key in missing_keys:
                    expected_list_of_keys.pop(expected_list_of_keys.index(missing_key))
            return actual_list_of_keys, expected_list_of_keys
