import random

from cbas.cbas_base import CBASBaseTest
from Jython_tasks.task import CreateDatasetsTask, DropDatasetsTask, \
    CreateSynonymsTask, DropSynonymsTask, DropDataversesTask, \
    CreateCBASIndexesTask, DropCBASIndexesTask, CreateUDFTask, DropUDFTask
from cbas_utils.cbas_utils import BackupUtils
import urllib


class BackupRestoreTest(CBASBaseTest):

    def setUp(self):

        super(BackupRestoreTest, self).setUp()
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.setUp.__name__)

        self.num_dataverses = int(self.input.param("no_of_dv", 1))
        self.ds_per_dv = int(self.input.param("ds_per_dv", 1))
        self.kv_name_cardinality = self.input.param("kv_name_cardinality", 3)
        self.cbas_name_cardinality = self.input.param(
            "cbas_name_cardinality", 2)
        self.ds_per_collection = self.input.param("ds_per_collection", 1)

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]

        self.synonyms_per_ds = int(self.input.param("synonyms_per_ds", 1))
        self.overlap_path = self.input.param("overlap_path", False)
        self.drop_datasets = self.input.param("drop_datasets", True)
        self.drop_synonyms = self.input.param("drop_synonyms", True)
        self.drop_indexes = self.input.param("drop_synonyms", True)
        self.drop_dataverses = self.input.param("drop_dataverses", True)
        self.drop_udfs = self.input.param("drop_udfs", True)
        self.remap_bucket = self.input.param("remap_bucket", False)
        self.ds_fields = ['DatasetName', 'DataverseName', 'BucketName', 'ScopeName',
            'CollectionName']

        self.backup_util = BackupUtils()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(BackupRestoreTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def validate_restore(self, dv_before_backup, ds_before_backup,
                         syn_before_backup, idx_before_backup, include, exclude,
                         original_bucket, original_scope, original_collection,
                         remap_bucket, remap_scope, remap_collection,
                         level="cluster"):
        dv_after_restore = self.cbas_util.get_dataverses(self.cluster)
        ds_after_restore = self.cbas_util.get_datasets(
            self.cluster, retries=1, fields=self.ds_fields)
        syn_after_restore = self.cbas_util.get_synonyms(self.cluster)
        idx_after_restore = self.cbas_util.get_indexes(self.cluster)
        if include:
            if not isinstance(include, list):
                include = urllib.quote(include).split(",")
            self.assertEquals(len(ds_after_restore), len(include))
            self.assertEquals(len(idx_after_restore), len(include) * 2)
        elif exclude:
            if not isinstance(exclude, list):
                exclude = urllib.quote(exclude).split(",")
            self.assertEquals(len(ds_after_restore),
                              len(ds_before_backup) - len(exclude))
            self.assertEquals(len(idx_after_restore),
                              len(idx_before_backup) - (len(exclude) * 2))
        else:
            self.assertEquals(len(ds_after_restore), len(ds_before_backup))
            self.assertEquals(len(idx_after_restore), len(idx_before_backup))
        if level == "cluster":
            self.assertEquals(len(dv_after_restore), len(dv_before_backup))
            self.assertEquals(len(syn_after_restore), len(syn_before_backup))
        if remap_bucket or remap_scope or remap_collection:
            original_ds = list(
                filter(
                    lambda ds:
                    ds['BucketName'] == original_bucket.name
                    and
                    ds['ScopeName'] == original_scope.name
                    and
                    ds['CollectionName'] == original_collection.name,
                    ds_before_backup))
            for ds_restored in ds_after_restore:
                backed_up_ds = list(
                    filter(
                        lambda ds: ds['DatasetName'] == \
                                   ds_restored['DatasetName'] and \
                                   ds['DataverseName'] == \
                                   ds_restored['DataverseName'], original_ds))
                if backed_up_ds:
                    self.assertEquals(ds_restored['BucketName'],
                                      remap_bucket.name)
                    self.assertEquals(ds_restored['ScopeName'],
                                      remap_scope.name)
                    self.assertEquals(ds_restored['CollectionName'],
                                      remap_collection.name)

    def create_datasets(self,
                        creation_methods=["cbas_collection", "cbas_dataset"]):
        create_datasets_task = CreateDatasetsTask(
            self.cluster,
            bucket_util=self.bucket_util,
            cbas_name_cardinality=self.cbas_name_cardinality,
            cbas_util=self.cbas_util,
            kv_name_cardinality=self.kv_name_cardinality,
            ds_per_dv=self.ds_per_dv, ds_per_collection=self.ds_per_collection,
            creation_methods=creation_methods)
        self.task_manager.add_new_task(create_datasets_task)
        self.task_manager.get_task_result(create_datasets_task)
        return create_datasets_task.result

    def create_synonyms(self, cbas_entities=[], synonym_on_synonym=False):
        if not cbas_entities:
            cbas_entities = self.cbas_util.list_all_dataset_objs()
        results = []
        for ds in cbas_entities:
            synonyms_task = CreateSynonymsTask(
                cluster=self.cluster, cbas_util=self.cbas_util, cbas_entity=ds,
                dataverse=self.cbas_util.dataverses[ds.dataverse_name],
                synonyms_per_entity=self.synonyms_per_ds,
                synonym_on_synonym=synonym_on_synonym)
            self.task_manager.add_new_task(synonyms_task)
            self.task_manager.get_task_result(synonyms_task)
            results.append(synonyms_task.result)
        return all(results)

    def create_indexes(self, datasets=[], prefix=""):
        results = []
        if not datasets:
            datasets = self.cbas_util.list_all_dataset_objs()
        for ds in datasets:
            create_index_task = CreateCBASIndexesTask(
                self.cluster, self.cbas_util, ds)
            create_index_task.call()
            results.append(create_index_task.result)
        return all(results)

    def create_udfs(self):
        if self.input.param("udfs_on_datasets", True):
            for num, ds in enumerate(self.cbas_util.list_all_dataset_objs()):
                name = "func_ds_" + str(num)
                dv = self.cbas_util.dataverses[ds.dataverse_name]
                ds_name = ds.full_name
                body = "select count(*) from {0}".format(ds_name)
                create_udf_task = CreateUDFTask(
                    self.cluster, self.cbas_util, name, dv, body,
                    parameters=[], referenced_entities=[dv.datasets[ds_name]])
                self.task_manager.add_new_task(create_udf_task)
                self.task_manager.get_task_result(create_udf_task)
        if self.input.param("udfs_on_synonyms", False):
            for num, syn in enumerate(self.cbas_util.get_synonyms(self.cluster)):
                name = "func_syn_" + str(num)
                dv, syn_name = syn.split(".")
                body = "SELECT COUNT(*) FROM {0}".format(syn)
                create_udf_task = CreateUDFTask(
                    self.cluster, self.cbas_util, name,
                    self.cbas_util.dataverses[dv], body, parameters=[],
                    referenced_entities=[syn])
                self.task_manager.add_new_task(create_udf_task)
                self.task_manager.get_task_result(create_udf_task)

    def drop_all_udfs(self):
        for dv in self.cbas_util.dataverses.values():
            drop_udf_task = DropUDFTask(self.cluster, self.cbas_util, dv)
            self.task_manager.add_new_task(drop_udf_task)
            self.task_manager.get_task_result(drop_udf_task)

    def drop_all_indexes(self, datasets=[]):
        results = []
        if not datasets:
            datasets = self.cbas_util.list_all_dataset_objs()
        for ds in datasets:
            drop_indexes_task = DropCBASIndexesTask(self.cluster, self.cbas_util, ds)
            drop_indexes_task.call()
            results.append(drop_indexes_task.result)
        return all(results)

    def drop_all_datasets(self):
        drop_datasets_task = DropDatasetsTask(
            self.cluster, self.cbas_util, self.kv_name_cardinality)
        self.task_manager.add_new_task(drop_datasets_task)
        self.task_manager.get_task_result(drop_datasets_task)
        return drop_datasets_task.result

    def drop_all_synonyms(self):
        drop_synonyms_task = DropSynonymsTask(self.cluster, self.cbas_util)
        self.task_manager.add_new_task(drop_synonyms_task)
        self.task_manager.get_task_result(drop_synonyms_task)
        return drop_synonyms_task.result

    def drop_all_dataverses(self):
        drop_dataverses_task = DropDataversesTask(self.cluster, self.cbas_util)
        self.task_manager.add_new_task(drop_dataverses_task)
        self.task_manager.get_task_result(drop_dataverses_task)
        return drop_dataverses_task.result

    def test_cluster_level_backup_restore(self):
        """
        cluster level api is used to backup entities which are not related to \
        buckets
        1. Create KV infra
        2. Create CBAS infra with synonyms
        3. Take backup using cluster level API "/api/v1/backup"
        4. Validate backed up metadata
        5. Drop CBAS infra
        6. Restore using backed up metadata
        7. Validate CBAS infra.(Dataverses, Synonyms, Datasets and \
        Ingestion)
        """
        self.log.debug("test_cluster_level_backup started")
        self.create_datasets()
        self.create_synonyms()
        self.create_indexes()
        self.create_udfs()
        syn_before_backup = self.cbas_util.get_synonyms(self.cluster)
        dv_before_backup = self.cbas_util.get_dataverses(self.cluster)
        status, backup, response = self.backup_util.rest_backup_cbas(
            self.cluster, level="cluster")
        self.assertTrue(status)
        if self.drop_synonyms:
            self.drop_all_synonyms()
        if self.drop_udfs:
            self.drop_all_udfs()
        if self.drop_datasets:
            self.drop_all_datasets()
        if self.drop_dataverses:
            self.drop_all_dataverses()
        status, restore, response = self.backup_util.rest_restore_cbas(
            self.cluster, level="cluster", backup=backup)
        self.assertTrue(status)
        syn_after_restore = self.cbas_util.get_synonyms(self.cluster)
        dv_after_restore = self.cbas_util.get_dataverses(self.cluster)
        ds_after_restore = self.cbas_util.get_datasets(self.cluster)
        idx_after_restore = self.cbas_util.get_indexes(self.cluster)
        self.assertEquals(len(syn_before_backup), len(syn_after_restore))
        self.assertEquals(len(dv_before_backup), len(dv_after_restore))
        if self.drop_datasets:
            self.assertEquals(len(ds_after_restore), 0)
        if self.drop_indexes:
            self.assertEquals(len(idx_after_restore), 0)
        self.log.debug("test_cluster_level_backup finished")

    def test_bucket_level_backup(self):
        """
        1. Create KV infra
        2. Create CBAS infra
        3. Take backup using bucket level API "/api/v1/bucket/[BUCKET]/backup"\
         with args include or exclude
        4. Validate backed up metadata
        5. Drop CBAS infra
        6. Restore using backed up metadata
        7. Validate CBAS infra.(Dataverses, Links, Datasets and Ingestion)
        """
        self.log.debug("test_cluster_level_backup started")
        include = self.input.param("include", True)
        exclude = self.input.param("exclude", False)
        self.create_datasets()
        self.create_synonyms()
        self.create_indexes()
        self.create_udfs()
        dv_before_backup = self.cbas_util.get_dataverses(self.cluster)
        ds_before_backup = self.cbas_util.get_datasets(
            self.cluster, fields=self.ds_fields)
        syn_before_backup = self.cbas_util.get_synonyms(self.cluster)
        idx_before_backup = self.cbas_util.get_indexes(self.cluster)
        bucket = random.choice(self.cluster.buckets)
        scope = random.choice(self.bucket_util.get_active_scopes(bucket))
        collection = random.choice(self.bucket_util.get_active_collections(
            bucket, scope.name))
        path = scope.name + "." + collection.name
        if self.overlap_path:
            path = scope.name + "," + path
        path = urllib.quote(path)
        if include:
            include = path
        else:
            include = ""
        if exclude:
            exclude = path
        else:
            exclude = ""
        status, backup, response = self.backup_util.rest_backup_cbas(
            self.cluster, level="bucket", bucket=bucket.name, include=include,
            exclude=exclude)
        if (include and exclude) or self.overlap_path:
            self.assertFalse(status)
        else:
            self.assertTrue(status)
            if self.drop_indexes:
                self.drop_all_indexes()
            if self.drop_synonyms:
                self.drop_all_synonyms()
            if self.drop_udfs:
                self.drop_all_udfs()
            if self.drop_datasets:
                self.drop_all_datasets()
            if self.drop_dataverses:
                self.drop_all_dataverses()
            status, restore, response = self.backup_util.rest_restore_cbas(
                self.cluster, level="bucket", bucket=bucket.name, backup=backup)
            self.assertTrue(status)
            self.validate_restore(
                dv_before_backup, ds_before_backup, syn_before_backup,
                idx_before_backup, include, exclude, bucket, scope, collection,
                None, None, None, level="bucket")
        # validate metadata
        self.log.debug("test_cluster_level_backup finished")

    def test_bucket_level_restore(self):
        """
        1. Create KV infra
        2. Create CBAS infra
        3. Take backup using cluster level API "/api/v1/bucket/[BUCKET]/backup"\
         with args include or exclude
        4. Validate backed up metadata
        5. Drop CBAS infra
        6. Restore using backed up metadata along with remap arg
        7. Validate CBAS infra.(Dataverses, Links, Datasets and Ingestion)
        """
        include = self.input.param("include", True)
        exclude = self.input.param("exclude", False)
        self.log.debug("test_cluster_level_backup started")
        self.create_datasets()
        self.create_synonyms()
        self.create_indexes()
        self.create_udfs()
        dv_before_backup = self.cbas_util.get_dataverses(self.cluster)
        ds_before_backup = self.cbas_util.get_datasets(self.cluster, fields=self.ds_fields)
        syn_before_backup = self.cbas_util.get_synonyms(self.cluster)
        idx_before_backup = self.cbas_util.get_indexes(self.cluster)
        original_bucket = random.choice(self.cluster.buckets)
        # remap collections
        original_scope = random.choice(
            self.bucket_util.get_active_scopes(original_bucket))
        original_collection = random.choice(
            self.bucket_util.get_active_collections(
                original_bucket, original_scope.name))
        if self.remap_bucket:
            remap_bucket = random.choice(
                list(
                    filter(
                        lambda b:
                        b.name != original_bucket.name,
                        self.cluster.buckets)))
        else:
            remap_bucket = original_bucket
        remap_scope = random.choice(
            list(filter(lambda scope: scope.name != original_scope.name,
                        self.bucket_util.get_active_scopes(
                            remap_bucket))))
        remap_collection = random.choice(
            self.bucket_util.get_active_collections(remap_bucket,
                                                    remap_scope.name))
        remap = urllib.quote("{0}.{1}:{2}.{3}".format(original_scope.name,
                                         original_collection.name,
                                         remap_scope.name,
                                         remap_collection.name))
        if self.overlap_path:
            remap += urllib.quote(",{0}:{1}".format(
                original_scope.name, remap_scope.name))
        if include:
            include = urllib.quote("{0}.{1}".format(
                original_scope.name, original_collection.name))
        else:
            include = ""
        if exclude:
            exclude = urllib.quote("{0}.{1}".format(
                original_scope.name, original_collection.name))
        else:
            exclude = ""
        status, backup, response = self.backup_util.rest_backup_cbas(
            self.cluster, level="bucket", bucket=original_bucket.name)

        self.assertTrue(status)

        if self.drop_indexes:
            self.drop_all_indexes()
        if self.drop_synonyms:
            self.drop_all_synonyms()
        if self.drop_udfs:
            self.drop_all_udfs()
        if self.drop_datasets:
            self.drop_all_datasets()
        if self.drop_dataverses:
            self.drop_all_dataverses()

        status, restore, response = self.backup_util.rest_restore_cbas(
            self.cluster, level="bucket", bucket=remap_bucket.name,
            backup=backup, include=include, exclude=exclude, remap=remap)
        if (include and exclude) or self.overlap_path:
            self.assertFalse(status)
        else:
            self.assertTrue(status)
            self.validate_restore(
                dv_before_backup, ds_before_backup, syn_before_backup,
                idx_before_backup, include, exclude, original_bucket,
                original_scope, original_collection, remap_bucket, remap_scope,
                remap_collection, level="bucket")

    def test_backup_with_cbbackupmgr(self):
        self.log.debug("test_cluster_level_backup started")
        include = self.input.param("include", True)
        exclude = self.input.param("exclude", False)
        self.create_datasets()
        self.create_synonyms()
        self.create_indexes()
        self.create_udfs()
        dv_before_backup = self.cbas_util.get_dataverses(self.cluster)
        ds_before_backup = self.cbas_util.get_datasets(self.cluster)
        syn_before_backup = self.cbas_util.get_synonyms(self.cluster)
        idx_before_backup = self.cbas_util.get_indexes(self.cluster)
        bucket = random.choice(self.cluster.buckets)
        scope = random.choice(self.bucket_util.get_active_scopes(bucket))
        collection = random.choice(self.bucket_util.get_active_collections(
            bucket, scope.name))
        paths = [(bucket.name + "." + scope.name + "." + collection.name)]
        if self.overlap_path:
            paths.append(bucket.name + "." + scope.name)
        if include:
            include = paths
        else:
            include = []
        if exclude:
            exclude = paths
        else:
            exclude = []
        o = self.backup_util.cbbackupmgr_backup_cbas(
            self.cluster, include=include, exclude=exclude)
        if (include and exclude) or self.overlap_path:
            self.assertFalse('Backup completed successfully' in ''.join(o),
                            msg='Backup was successful')
        else:
            self.assertTrue('Backup completed successfully' in ''.join(o),
                            msg='Backup was unsuccessful')
            if self.drop_indexes:
                self.drop_all_indexes()
            if self.drop_synonyms:
                self.drop_all_synonyms()
            if self.drop_udfs:
                self.drop_all_udfs()
            if self.drop_datasets:
                self.drop_all_datasets()
            if self.drop_dataverses:
                self.drop_all_dataverses()
            o = self.backup_util.cbbackupmgr_restore_cbas(self.cluster)
            self.assertTrue('Restore completed successfully' in ''.join(o),
                            msg='Restore was unsuccessful')
            self.validate_restore(
                dv_before_backup, ds_before_backup, syn_before_backup,
                idx_before_backup, include, exclude, bucket, scope, collection,
                None, None, None)
        # validate metadata
        self.log.debug("test_cluster_level_backup finished")

    def test_restore_with_cbbackupmgr(self):
        """
        1. Create KV infra
        2. Create CBAS infra
        3. Take backup using cluster level API "/api/v1/bucket/[BUCKET]/backup"\
         with args include or exclude
        4. Validate backed up metadata
        5. Drop CBAS infra
        6. Restore using backed up metadata along with remap arg
        7. Validate CBAS infra.(Dataverses, Links, Datasets and Ingestion)
        """
        self.log.debug("test_cluster_level_backup started")
        include = self.input.param("include", True)
        exclude = self.input.param("exclude", False)
        self.create_datasets()
        self.create_synonyms()
        self.create_indexes()
        self.create_udfs()
        dv_before_backup = self.cbas_util.get_dataverses(self.cluster)
        ds_before_backup = self.cbas_util.get_datasets(self.cluster ,fields=self.ds_fields)
        syn_before_backup = self.cbas_util.get_synonyms(self.cluster)
        idx_before_backup = self.cbas_util.get_indexes(self.cluster)
        original_bucket = random.choice(self.cluster.buckets)
        # remap collections
        original_scope = random.choice(
            self.bucket_util.get_active_scopes(original_bucket))
        original_collection = random.choice(
            self.bucket_util.get_active_collections(
                original_bucket, original_scope.name))
        if self.remap_bucket:
            remap_bucket = random.choice(
                list(
                    filter(
                        lambda b:
                        b.name != original_bucket.name,
                        self.cluster.buckets)))
        else:
            remap_bucket = original_bucket
        remap_scope = random.choice(
            list(filter(lambda scope: scope.name != original_scope.name,
                        self.bucket_util.get_active_scopes(remap_bucket))))
        remap_collection = random.choice(
            self.bucket_util.get_active_collections(remap_bucket, remap_scope.name))
        mappings = ["{0}.{1}.{2}={3}.{4}.{5}".format(
            original_bucket.name, original_scope.name, original_collection.name,
            remap_bucket.name, remap_scope.name, remap_collection.name)]
        if self.overlap_path:
            mappings.append("{0}.{1}={2}.{3}".format(
                original_bucket.name, original_scope.name, remap_bucket.name,
                remap_scope.name))
        if include:
            include = ["{0}.{1}.{2}".format(
                original_bucket.name, original_scope.name, original_collection.name)]
        else:
            include = []
        if exclude:
            exclude = ["{0}.{1}.{2}".format(
                original_bucket.name, original_scope.name, original_collection.name)]
        else:
            exclude = []
        o = self.backup_util.cbbackupmgr_backup_cbas(self.cluster)
        self.assertTrue('Backup completed successfully' in ''.join(o),
                        msg='Backup was unsuccessful')

        if self.drop_indexes:
            self.drop_all_indexes()
        if self.drop_synonyms:
            self.drop_all_synonyms()
        if self.drop_udfs:
            self.drop_all_udfs()
        if self.drop_datasets:
            self.drop_all_datasets()
        if self.drop_dataverses:
            self.drop_all_dataverses()

        o = self.backup_util.cbbackupmgr_restore_cbas(
            self.cluster, include=include, exclude=exclude, mappings=mappings)
        if (include and exclude) or self.overlap_path:
            self.assertFalse('Restore completed successfully' in ''.join(o),
                             msg='Restore was successful')
        else:
            self.assertTrue('Restore completed successfully' in ''.join(o),
                            msg='Restore was unsuccessful')
            self.validate_restore(
                dv_before_backup, ds_before_backup, syn_before_backup,
                idx_before_backup,include, exclude, original_bucket,
                original_scope, original_collection, remap_bucket, remap_scope,
                remap_collection)

