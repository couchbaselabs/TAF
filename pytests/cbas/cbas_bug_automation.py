'''
Created on 6-October-2021
@author: umang.agrawal
'''

import random
from cbas.cbas_base import CBASBaseTest
from security_utils.security_utils import SecurityUtils
from cbas_utils.cbas_utils import CBASRebalanceUtil
from couchbase_utils.security_utils.x509_multiple_CA_util import x509main


class CBASBugAutomation(CBASBaseTest):

    def setUp(self):

        super(CBASBugAutomation, self).setUp()

        self.num_dataverses = int(self.input.param("no_of_dv", 1))
        self.ds_per_dv = int(self.input.param("ds_per_dv", 1))
        self.security_util = SecurityUtils(self.log)

        self.do_rebalance = self.input.param("do_rebalance", False)
        if self.do_rebalance:
            self.rebalance_util = CBASRebalanceUtil(
                self.cluster_util, self.bucket_util, self.task, True,
                self.cbas_util)

        self.log.info("Disabling Auto-Failover")
        if not self.cluster.rest.update_autofailover_settings(
                False, 120, False):
            self.fail("Disabling Auto-Failover failed")

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        x509 = x509main(
            host=self.cluster.master, standard=self.standard,
            encryption_type=self.encryption_type,
            passphrase_type=self.passphrase_type)
        x509.teardown_certs(self.servers)
        super(CBASBugAutomation, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def load_data_into_bucket(self):
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package("initial_load")
        doc_loading_spec["doc_crud"]["create_percentage_per_collection"] = 25
        self.load_data_into_buckets(
            self.cluster, doc_loading_spec=doc_loading_spec,
            async_load=False, validate_task=True, mutation_num=0)

    def create_dataset(self):
        dataset_obj = self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, dataset_cardinality=3,
            bucket_cardinality=3)[0]
        if not self.cbas_util.create_dataset(
                self.cluster, dataset_obj.name, dataset_obj.full_kv_entity_name,
                dataverse_name=dataset_obj.dataverse_name):
            self.fail("Error while creating dataset {0}".format(
                dataset_obj.full_name))
        self.cbas_util.dataverses[dataset_obj.dataverse_name].datasets[
            dataset_obj.name] = dataset_obj

    def test_cbas_with_n2n_encryption_and_client_cert_auth(self):
        step_count = 1

        self.log.info("Step {0}: Initial Data loading in KV bucket is "
                      "Complete".format(step_count))
        step_count += 1

        self.log.info("Step {0}: Creating CBAS infra".format(step_count))
        step_count += 1
        update_spec = {
            "no_of_dataverses": self.num_dataverses,
            "no_of_datasets_per_dataverse": self.ds_per_dv,
            "no_of_synonyms": 0, "no_of_indexes": 0, "max_thread_count": 1,
            "dataset": {"creation_methods": ["cbas_collection",
                                             "cbas_dataset"]}}
        if self.cbas_spec_name:
            self.cbas_spec = self.cbas_util.get_cbas_spec(self.cbas_spec_name)
            self.cbas_util.update_cbas_spec(self.cbas_spec, update_spec)
            cbas_infra_result = self.cbas_util.create_cbas_infra_from_spec(
                self.cluster, self.cbas_spec, self.bucket_util,
                wait_for_ingestion=True)
            if not cbas_infra_result[0]:
                self.fail("Error while creating infra from CBAS spec -- " +
                          cbas_infra_result[1])

        x509 = x509main(
            host=self.cluster.master, standard=self.standard,
            encryption_type=self.encryption_type,
            passphrase_type=self.passphrase_type)
        self.log.info("Step {0}: Setting up certificates".format(step_count))
        step_count += 1
        self.generate_and_upload_cert(
            self.servers, x509, upload_root_certs=True,
            upload_node_certs=True, upload_client_certs=True)

        if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
            self.fail("Analytics service failed to come up after enabling "
                      "Multple CA certificate")

        node_versions = self.cluster.rest.get_nodes_versions()
        if len(set(node_versions)) > 1:
            self.fail("Cluster is in mixed mode.")
        cluster_version = node_versions[0].split("-")[0]

        if cluster_version in ["7.0.0", "7.0.1"]:
            strict_mode_supported = False
        else:
            strict_mode_supported = True

        if strict_mode_supported:
            self.log.info("Step {0}: Setting node to node encryption level to "
                          "strict".format(step_count))
            step_count += 1
            self.security_util.set_n2n_encryption_level_on_nodes(
                self.cluster.nodes_in_cluster, level="strict")
        else:
            self.log.info("Step {0}: Setting node to node encryption level to "
                          "all".format(step_count))
            step_count += 1
            self.security_util.set_n2n_encryption_level_on_nodes(
                self.cluster.nodes_in_cluster, level="all")
        if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
            self.fail("Analytics service Failed to recover")

        self.log.info("Step {0}: Loading more docs".format(step_count))
        step_count += 1
        self.load_data_into_bucket()

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        self.create_dataset()

        if self.do_rebalance:
            self.log.info("Step {0}: Rebalancing IN KV and CBAS nodes".format(
                step_count))
            step_count += 1
            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=1, kv_nodes_out=0, cbas_nodes_in=1,
                cbas_nodes_out=0, available_servers=self.available_servers,
                exclude_nodes=[])
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalancing IN KV and CBAS nodes Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(self.cluster,
                                                            self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Setting node to node encryption level to "
                      "control".format(step_count))
        step_count += 1
        self.security_util.set_n2n_encryption_level_on_nodes(
            self.cluster.nodes_in_cluster, level="control")
        if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
            self.fail("Analytics service Failed to recover")

        self.log.info("Step {0}: Loading more docs".format(step_count))
        step_count += 1
        self.load_data_into_bucket()

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        self.create_dataset()

        if self.do_rebalance:
            self.log.info("Step {0}: Rebalancing OUT KV and CBAS nodes".format(
                step_count))
            step_count += 1
            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=0, kv_nodes_out=1, cbas_nodes_in=0,
                cbas_nodes_out=1, available_servers=self.available_servers,
                exclude_nodes=[self.cluster.cbas_cc_node, self.cluster.master])
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalancing OUT KV and CBAS nodes Failed")

            self.generate_and_upload_cert(
                self.available_servers, x509, generate_certs=False,
                delete_inbox_folder=False, upload_root_certs=True,
                upload_node_certs=True, delete_out_of_the_box_CAs=False,
                upload_client_certs=False)

        if not self.cbas_util.validate_docs_in_all_datasets(self.cluster,
                                                            self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Setting node to node encryption level to "
                      "all".format(step_count))
        step_count += 1
        self.security_util.set_n2n_encryption_level_on_nodes(
            self.cluster.nodes_in_cluster, level="all")
        if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
            self.fail("Analytics service Failed to recover")

        self.log.info("Step {0}: Loading more docs".format(step_count))
        step_count += 1
        self.load_data_into_bucket()

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        self.create_dataset()

        if self.do_rebalance:
            self.log.info("Step {0}: Rebalancing IN KV and CBAS nodes".format(
                step_count))
            step_count += 1
            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=1, kv_nodes_out=0, cbas_nodes_in=1,
                cbas_nodes_out=0, available_servers=self.available_servers,
                exclude_nodes=[])
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalancing IN KV and CBAS nodes Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(self.cluster,
                                                            self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        if strict_mode_supported:
            self.log.info("Step {0}: Setting node to node encryption level to "
                          "strict".format(step_count))
            step_count += 1
            self.security_util.set_n2n_encryption_level_on_nodes(
                self.cluster.nodes_in_cluster, level="strict")
            if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
                self.fail("Analytics service Failed to recover")

            self.log.info("Step {0}: Dropping Dataset".format(step_count))
            step_count += 1
            dataset_to_be_dropped = random.choice(
                self.cbas_util.list_all_dataset_objs())
            if not self.cbas_util.drop_dataset(
                    self.cluster, dataset_to_be_dropped.full_name):
                self.fail("Error while dropping dataset")
            del self.cbas_util.dataverses[
                dataset_to_be_dropped.dataverse_name].datasets[
                dataset_to_be_dropped.name]

            self.log.info("Step {0}: Loading more docs".format(step_count))
            step_count += 1
            self.load_data_into_bucket()

            if self.do_rebalance:
                self.log.info(
                    "Step {0}: Rebalancing OUT KV and CBAS nodes".format(
                        step_count))
                step_count += 1
                rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                    self.cluster, kv_nodes_in=0, kv_nodes_out=1,
                    cbas_nodes_in=0,
                    cbas_nodes_out=1, available_servers=self.available_servers,
                    exclude_nodes=[self.cluster.cbas_cc_node, self.cluster.master])
                if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                        rebalance_task, self.cluster):
                    self.fail("Rebalancing OUT KV and CBAS nodes Failed")

                self.generate_and_upload_cert(
                    self.available_servers, x509, generate_certs=False,
                    delete_inbox_folder=False, upload_root_certs=True,
                    upload_node_certs=True, delete_out_of_the_box_CAs=False,
                    upload_client_certs=False)

            self.log.info("Step {0}: Setting node to node encryption level to "
                          "all".format(step_count))
            step_count += 1
            self.security_util.set_n2n_encryption_level_on_nodes(
                self.cluster.nodes_in_cluster, level="all")
            if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
                self.fail("Analytics service Failed to recover")

            self.log.info("Step {0}: Dropping Dataset".format(step_count))
            step_count += 1
            dataset_to_be_dropped = random.choice(
                self.cbas_util.list_all_dataset_objs())
            if not self.cbas_util.drop_dataset(
                    self.cluster, dataset_to_be_dropped.full_name):
                self.fail("Error while dropping dataset")
            del self.cbas_util.dataverses[
                dataset_to_be_dropped.dataverse_name].datasets[
                dataset_to_be_dropped.name]

            self.log.info("Step {0}: Loading more docs".format(step_count))
            step_count += 1
            self.load_data_into_bucket()

            if self.do_rebalance:
                self.log.info(
                    "Step {0}: Rebalancing IN KV and CBAS nodes".format(
                        step_count))
                step_count += 1
                rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                    self.cluster, kv_nodes_in=1, kv_nodes_out=0,
                    cbas_nodes_in=1,
                    cbas_nodes_out=0, available_servers=self.available_servers,
                    exclude_nodes=[])
                if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                        rebalance_task, self.cluster):
                    self.fail("Rebalancing IN KV and CBAS nodes Failed")

            if not self.cbas_util.validate_docs_in_all_datasets(self.cluster,
                                                                self.bucket_util):
                self.fail("Data ingestion into datasets after data reloading "
                          "failed")

            self.log.info("Step {0}: Setting node to node encryption level to "
                          "strict".format(step_count))
            step_count += 1
            self.security_util.set_n2n_encryption_level_on_nodes(
                self.cluster.nodes_in_cluster, level="strict")
            if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
                self.fail("Analytics service Failed to recover")

            self.log.info("Step {0}: Loading more docs".format(step_count))
            step_count += 1
            self.load_data_into_bucket()

            self.log.info("Step {0}: Creating dataset".format(step_count))
            step_count += 1
            self.create_dataset()

            if self.do_rebalance:
                self.log.info(
                    "Step {0}: Rebalancing OUT KV and CBAS nodes".format(
                        step_count))
                step_count += 1
                rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                    self.cluster, kv_nodes_in=0, kv_nodes_out=1,
                    cbas_nodes_in=0,
                    cbas_nodes_out=1, available_servers=self.available_servers,
                    exclude_nodes=[self.cluster.cbas_cc_node, self.cluster.master])
                if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                        rebalance_task, self.cluster):
                    self.fail("Rebalancing OUT KV and CBAS nodes Failed")

                self.generate_and_upload_cert(
                    self.available_servers, x509, generate_certs=False,
                    delete_inbox_folder=False, upload_root_certs=True,
                    upload_node_certs=True, delete_out_of_the_box_CAs=False,
                    upload_client_certs=False)

            if not self.cbas_util.validate_docs_in_all_datasets(self.cluster,
                                                                self.bucket_util):
                self.fail("Data ingestion into datasets after data reloading "
                          "failed")
        else:
            self.log.info("Step {0}: Setting node to node encryption level to "
                          "control".format(step_count))
            step_count += 1
            self.security_util.set_n2n_encryption_level_on_nodes(
                self.cluster.nodes_in_cluster, level="control")
            if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
                self.fail("Analytics service Failed to recover")

            self.log.info("Step {0}: Dropping Dataset".format(step_count))
            step_count += 1
            dataset_to_be_dropped = random.choice(
                self.cbas_util.list_all_dataset_objs())
            if not self.cbas_util.drop_dataset(
                    self.cluster, dataset_to_be_dropped.full_name):
                self.fail("Error while dropping dataset")
            del self.cbas_util.dataverses[
                dataset_to_be_dropped.dataverse_name].datasets[
                dataset_to_be_dropped.name]

            self.log.info("Step {0}: Loading more docs".format(step_count))
            step_count += 1
            self.load_data_into_bucket()

            if self.do_rebalance:
                self.log.info(
                    "Step {0}: Rebalancing OUT KV and CBAS nodes".format(
                        step_count))
                step_count += 1
                rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                    self.cluster, kv_nodes_in=0, kv_nodes_out=1,
                    cbas_nodes_in=0,
                    cbas_nodes_out=1, available_servers=self.available_servers,
                    exclude_nodes=[self.cluster.cbas_cc_node, self.cluster.master])
                if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                        rebalance_task, self.cluster):
                    self.fail("Rebalancing OUT KV and CBAS nodes Failed")

                self.generate_and_upload_cert(
                    self.available_servers, x509, generate_certs=False,
                    delete_inbox_folder=False, upload_root_certs=True,
                    upload_node_certs=True, delete_out_of_the_box_CAs=False,
                    upload_client_certs=False)

            if not self.cbas_util.validate_docs_in_all_datasets(self.cluster,
                                                                self.bucket_util):
                self.fail("Data ingestion into datasets after data reloading "
                          "failed")

        self.log.info("Step {0}: Disabling node-to-node encryption and "
                      "client cert auth".format(step_count))
        step_count += 1
        self.security_util.disable_n2n_encryption_cli_on_nodes(self.servers)
        if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
            self.fail("Analytics service Failed to recover")

        self.log.info("Step {0}: Tearing down Certs".format(step_count))
        step_count += 1
        x509.teardown_certs(self.servers)

        self.log.info("Step {0}: Loading more docs".format(step_count))
        step_count += 1
        self.load_data_into_bucket()

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        self.create_dataset()

        if self.do_rebalance:
            self.log.info("Step {0}: Rebalancing IN KV and CBAS nodes".format(
                step_count))
            step_count += 1
            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=1, kv_nodes_out=0, cbas_nodes_in=1,
                cbas_nodes_out=0, available_servers=self.available_servers,
                exclude_nodes=[])
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalancing IN KV and CBAS nodes Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(self.cluster,
                                                            self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        if strict_mode_supported:
            self.log.info("Step {0}: Setting node to node encryption level to "
                          "strict".format(step_count))
            step_count += 1
            self.security_util.set_n2n_encryption_level_on_nodes(
                self.cluster.nodes_in_cluster, level="strict")
        else:
            self.log.info("Step {0}: Setting node to node encryption level to "
                          "all".format(step_count))
            step_count += 1
            self.security_util.set_n2n_encryption_level_on_nodes(
                self.cluster.nodes_in_cluster, level="all")
        if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
            self.fail("Analytics service Failed to recover")

        self.log.info("Step {0}: Loading more docs".format(step_count))
        step_count += 1
        self.load_data_into_bucket()

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        self.create_dataset()

        if self.do_rebalance:
            self.log.info("Step {0}: Rebalancing OUT KV and CBAS nodes".format(
                step_count))
            step_count += 1
            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=0, kv_nodes_out=1, cbas_nodes_in=0,
                cbas_nodes_out=1, available_servers=self.available_servers,
                exclude_nodes=[self.cluster.cbas_cc_node, self.cluster.master])
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalancing OUT KV and CBAS nodes Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(self.cluster,
                                                            self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Setting up certificates".format(step_count))
        step_count += 1
        x509 = x509main(
            host=self.cluster.master, standard=self.standard,
            encryption_type=self.encryption_type,
            passphrase_type=self.passphrase_type)
        self.generate_and_upload_cert(
            self.servers, x509, upload_root_certs=True,
            upload_node_certs=True, upload_client_certs=True)

        if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
            self.fail("Analytics service failed to come up after enabling "
                      "Multple CA certificate")

        self.log.info("Step {0}: Loading more docs".format(step_count))
        step_count += 1
        self.load_data_into_bucket()

        self.log.info("Step {0}: Dropping Dataset".format(step_count))
        step_count += 1
        dataset_to_be_dropped = random.choice(
            self.cbas_util.list_all_dataset_objs())
        if not self.cbas_util.drop_dataset(
                self.cluster, dataset_to_be_dropped.full_name):
            self.fail("Error while dropping dataset")
        del self.cbas_util.dataverses[
            dataset_to_be_dropped.dataverse_name].datasets[
            dataset_to_be_dropped.name]

        if self.do_rebalance:
            self.log.info("Step {0}: Rebalancing IN KV and CBAS nodes".format(
                step_count))
            step_count += 1
            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=1, kv_nodes_out=0, cbas_nodes_in=1,
                cbas_nodes_out=0, available_servers=self.available_servers,
                exclude_nodes=[])
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalancing IN KV and CBAS nodes Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(self.cluster,
                                                            self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")

        self.log.info("Step {0}: Setting node to node encryption level to "
                      "control".format(step_count))
        step_count += 1
        self.security_util.set_n2n_encryption_level_on_nodes(
            self.cluster.nodes_in_cluster, level="control")
        if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
            self.fail("Analytics service Failed to recover")

        self.log.info("Step {0}: Loading more docs".format(step_count))
        step_count += 1
        self.load_data_into_bucket()

        self.log.info("Step {0}: Creating dataset".format(step_count))
        step_count += 1
        self.create_dataset()

        if self.do_rebalance:
            self.log.info("Step {0}: Rebalancing OUT KV and CBAS nodes".format(
                step_count))
            step_count += 1
            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=0, kv_nodes_out=1, cbas_nodes_in=0,
                cbas_nodes_out=1, available_servers=self.available_servers,
                exclude_nodes=[self.cluster.cbas_cc_node, self.cluster.master])
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalancing OUT KV and CBAS nodes Failed")

        if not self.cbas_util.validate_docs_in_all_datasets(self.cluster,
                                                            self.bucket_util):
            self.fail("Data ingestion into datasets after data reloading "
                      "failed")
