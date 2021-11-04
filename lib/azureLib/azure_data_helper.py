import os
import subprocess
import json
import random
import copy
from com.couchbase.client.java.json import JsonObject
from couchbase_helper.documentgenerator import DocumentGenerator
from threading import Thread
from com.azure.storage.blob import BlobServiceClientBuilder,BlobClient,BlobContainerClient,BlobServiceClient
from com.azure.storage.blob.models import BlobAccessPolicy,BlobSignedIdentifier,PublicAccessType
from java.time import OffsetDateTime
from java.util import Collections
from TestInput import TestInputSingleton


class AzureDataHelper():
    """
    This class is used to generate files that are to be uploaded on AWS S3.
    """

    def __init__(self, connection_string,
                 cluster, bucket_util, task, log, n1ql_helper,
                ):
        self.connection_string = connection_string
        self.cluster = cluster
        self.bucket_util = bucket_util
        self.task = task
        self.log = log
        self.n1ql_helper = n1ql_helper
        self.blob_container_client = BlobContainerClient
        self.max_thread_count = 5
        self.failed_uploads = []
        self.n1ql_helper = n1ql_helper

    def azure_create_containers(self,azure_container_name):
        """
        This function creates azure container.
        """
        try:
            blob_service_client = BlobServiceClientBuilder().connectionString(self.connection_string).buildClient()
            self.blob_container_client = blob_service_client.createBlobContainer(azure_container_name)
            return self.blob_container_client
        except Exception as err:
            self.log.error(str(err))
        finally:
            return self.blob_container_client
    def set_azure_container_permission(self,container_public_access=False):
            if container_public_access:
                identifier = BlobSignedIdentifier().setId(self.blob_container_client.getBlobContainerName()).setAccessPolicy(BlobAccessPolicy().setStartsOn(OffsetDateTime.now())
                         .setExpiresOn(OffsetDateTime.now().plusMinutes(1))
                         .setPermissions("racwdl"))
                self.blob_container_client.setAccessPolicy(PublicAccessType.CONTAINER, Collections.singletonList(identifier))
            else:
                self.blob_container_client.setAccessPolicy(PublicAccessType.CONTAINER,None)

    def delete_azure_blob_container(self):
        self.blob_container_client.delete()

    def azure_upload_files(self,blob_container_client,no_of_files = 5, file_size_in_KB=10,
            record_type="json", upload_to_azure=True, file_extension=None):
        file_record_count = dict()

        if not file_extension:
            file_extension = record_type

        sample_data = {
            "key1": "sample1",
            "key2": "sample2",
            "key3": "sample3",
            "key4": "sample4"
        }

        if record_type == "json":
            data_to_write = json.dumps(sample_data)
        elif record_type == "csv":
            data_to_write = ",".join(str(x) for x in sample_data.values())
        elif record_type == "tsv":
            data_to_write = "\t".join(str(x) for x in sample_data.values())

        for i in range(0, no_of_files):
            filename = "file{0}_{1}KB.{2}".format(str(i), file_size_in_KB,
                                                  file_extension)
            self.log.info("Creating file {0} and uploading to azure container".format(
                filename))
            cur_dir = os.path.dirname(__file__)
            filepath = os.path.join(cur_dir, filename)

            no_of_records = 0
            with open(filepath, "a+") as fh:
                while os.stat(filepath).st_size <= (file_size_in_KB * 1024):
                    # This for loop is to prevent checking file size after each record insertion.
                    for i in range(0, 1000):
                        fh.write(data_to_write + "\n")
                    no_of_records += 1000

            file_record_count[filename] = no_of_records
            blobClient = blob_container_client.getBlobClient(filename)

            if upload_to_azure:
                blobClient.uploadFromFile(filepath)
                self.sleep(10)
        return file_record_count

    @staticmethod
    def generate_folder(no_of_folder, max_depth, root_path=""):
        """
        Generates a list of folders.
        :param no_of_folder: no of folder paths to be generated.
        :param max_depth: max depth of folder path, if max depth is 0,
        then all the folder path is root for all the folders. Root path is denoted by empty string.
        :param root_path: path of the root folder.
        :return: list of folder paths
        """
        folder_paths = [root_path]

        if not no_of_folder or not max_depth:
            return folder_paths
        else:
            for i in xrange(0, no_of_folder):
                depth = random.randint(1, max_depth)
                path = copy.deepcopy(root_path)
                for j in xrange(0, depth):
                    path += "folder{0}/".format(str(random.randint(0, no_of_folder)))
                folder_paths.append(path)
            return folder_paths

    @staticmethod
    def generate_filenames(no_of_files, formats=["json", "csv", "tsv"]):
        """
        Generates a list of files of specified formats.
        :param no_of_files: no of file names to be generated.
        :param formats: list, formats of files to be generated.
        :return: list of file names.
        """
        filenames = list()
        for i in xrange(0, no_of_files ):
            #filenames.append("file_{0}.{1}".format(str(i), random.choice(formats)))
            filenames.append("file_{0}.{1}".format(str(i), random.choice(formats)))
        return filenames

    def load_data_in_bucket(self, folders, filenames, missing_field,
                            operation, bucket, start_key=0, end_key=1000,
                            batch_size=10, exp=0, durability="",
                            mutation_num=0, key=None):
        """
        Loads data in CB bucket.
        :param folders: list, folder paths in aws bucket
        :param filenames: list, file names in the bucket
        :param missing_field: list of booleans, if missing_field is True,
        then this fields value with be omitted while creating a S3 file.
        :param operation: create/update/delete
        :param bucket: name of the bucket on CB server
        :param start_key:
        :param end_key:
        :param batch_size:
        :param exp:
        :param durability:
        :param mutation_num:
        :param key: doc key
        :return:
        """
        self.log.info("Loading data into bucket")
        folder = folders
        filename = filenames
        missing_field = missing_field
        template_obj = JsonObject.create()
        template_obj.put("filename", "")
        template_obj.put("folder", "")
        template_obj.put("mutated", mutation_num)
        template_obj.put("null_key", None)
        template_obj.put("missing_field", "")

        if not key:
            key = "test_docs"

        doc_gen = DocumentGenerator(
            key, template_obj, start=start_key, end=end_key, randomize=True,
            filename=filename, folder=folder, missing_field=missing_field)
        return self.bucket_util.async_load_bucket(
            self.cluster, bucket, doc_gen, operation, exp,
            durability=durability, batch_size=batch_size,
            suppress_error_table=True)

    def generate_data_for_azure_and_upload(
            self, azure_container_name, key, no_of_files, file_formats,
            no_of_folders, max_folder_depth, header, null_key, operation,
            bucket, no_of_docs, batch_size=10, exp=0, durability="",
            mutation_num=0, randomize_header=False, large_file=False,
            missing_field=[False]):
        """
        Uploads  files azure  based on data in CB bucket.
        :param key: string, doc key
        :param no_of_files: int, no of files to be uploaded to S3
        :param file_formats: list, file formats to be generated and uploaded,
        support values json, csv, tsv
        :param no_of_folders: int, no of folders to be uploaded to S3
        :param max_folder_depth: int, max folder depth, if 0, all files will
        be created in root folder.
        :param header: boolean, True/False, if True
        :param null_key: string, a string that represents the NULL value.
        :param operation: create/update/delete
        :param bucket: bucket_obj
        :param no_of_docs: int,
        :param batch_size: int,
        :param exp: int,
        :param durability: string,
        :param mutation_num: int,
        :param randomize_header: boolean, choose a random value for header.
        :param large_file: boolean, if large files are to be uploaded on S3.
        :param missing_field: list of booleans, if missing_field is True,
        then this fields value with be omitted while creating a S3 file.
        :return:
        """
        self.bucket = bucket
        self.filenames = AzureDataHelper.generate_filenames(
            no_of_files, formats=file_formats)
        self.folders = AzureDataHelper.generate_folder(no_of_folders,
                                                    max_folder_depth)
        self.n1ql_helper.create_primary_index()

        tasks = self.load_data_in_bucket(
            folders=self.folders, filenames=self.filenames,
            missing_field=missing_field, operation=operation,
            bucket=self.bucket, end_key=no_of_docs, batch_size=batch_size,
            exp=exp, durability=durability, mutation_num=mutation_num, key=key)
        result = self.task.jython_task_manager.get_task_result(tasks)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        item_list = [(folder, filename)
                     for folder in self.folders for filename in self.filenames]

        threads = list()
        files_per_thread = abs(len(item_list) / self.max_thread_count) + 1
        start = 0
        count = 0
        for i in xrange(0, len(item_list)):
            if count > self.max_thread_count or start >= len(item_list):
                break
            else:
                if start + files_per_thread >= len(item_list):
                    temp_list = item_list[start:]
                else:
                    temp_list = item_list[start:start + files_per_thread]
                threads.append(Thread(
                    target=self.thread_helper,
                    name="azure_thread_{0}".format(i),
                    args=(self.bucket, temp_list, header, null_key,
                          randomize_header, large_file,)
                ))
                start += files_per_thread
                count += 1
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return self.failed_uploads

    def thread_helper(self, bucket_name, item_list, header, null_key,
                      randomize_header, large_file):
        for item in item_list:
            self.create_and_upload_files_in_azure(
                bucket_name, item[0], item[1], header, null_key,
                randomize_header, large_file)

    def create_and_upload_files_in_azure(self, bucket_name, folder, filename,
                                      header, null_key, randomize_header=False,
                                      large_file=False):
        """
        Creates a file with data as uploaded in CB bucket and uploads the
        file to AWS S3 bucket.
        :param folder: string, path of the folder relative to S3.
        :param filename: string, name of the file to be uploaded on S3
        :param header: boolean, valid for csv or tsv files only, if True,
        first line of csv or tsv
        contains header specified by [RFC4180]
        :param null_key: string, a string that represents the NULL value.
        :param randomize_header: boolean, choose a random value for header.
        :param large_file: boolean, if large files are to be uploaded on S3.
        :return:
        """

        self.log.info("Creating file {0} and uploading to Azure".format(filename))
        query = "select * from `{0}` where folder='{1}' and filename='{2}';".format(
            bucket_name, folder, filename)
        retry = 0
        while retry < 5:
            try:
                n1ql_result = self.n1ql_helper.run_cbq_query(query)["results"]
                break
            except Exception:
                retry += 1
        list_of_json_obj = list()
        cur_dir = os.path.dirname(__file__)
        filepath = os.path.join(cur_dir, "-".join(folder.split("/")) + filename)
        with open(filepath, "a+") as fh:
            if randomize_header:
                header = random.choice(["True", "False"])
            if ("csv" in filename) and header:
                fh.write(",".join(str(x) for x in n1ql_result[0][
                    self.bucket.name].keys()))
                fh.write("\n")
            elif ("tsv" in filename) and header:
                fh.write("\t".join(str(x) for x in n1ql_result[0][
                    self.bucket.name].keys()))
                fh.write("\n")
            for result in n1ql_result:
                result = result[self.bucket.name]
                if null_key:
                    result["null_key"] = null_key
                record = [result["filename"], result["folder"],
                          result["mutated"], result["null_key"],
                          result.get("missing_field")]
                if result["missing_field"]:
                    del (result["missing_field"])
                    record = [result["filename"], result["folder"],
                              result["mutated"], result["null_key"]]
                if "json" in filename:
                    sub_type = random.choice(["json", "list_of_json"])
                    if sub_type == "json":
                        fh.write(json.dumps(result))
                        fh.write("\n")
                    else:
                        list_of_json_obj.append(result)
                elif "csv" in filename:
                    fh.write(",".join(str(x) for x in record))
                    fh.write("\n")
                elif "tsv" in filename:
                    fh.write("\t".join(str(x) for x in record))
                    fh.write("\n")
                elif "txt" in filename:
                    fh.write("".join(str(x) for x in record))
                    fh.write("\n")
            if len(list_of_json_obj) > 0:
                fh.write(json.dumps(list_of_json_obj))
        try:
            if large_file:
                blobClient = self.blob_container_client.getBlobClient(filename)
            else:
                blobClient = self.blob_container_client.getBlobClient(filename)
                blobClient.uploadFromFile(filepath)
            if blobClient:
                upload_success = True
            else:
                upload_success = False
        except Exception as err:
            self.log.error("Error while uploading file - {0} to Azure".format(
                filepath))
            self.log.error("Error -- {0}".format(str(err)))
            upload_success = False
        finally:
            os.remove(filepath)
            if not upload_success:
                self.log.error("Error while uploading file - {0} to Azure".format(
                    filepath))
                self.failed_uploads.append(filepath)
            return upload_success

    def generate_mix_data_file(self, bucket_name, file_format="json",
                               upload_to_azure=True):
        """
        Generate a single file of format specified, but file contains data of
        type json, csv and tsv, and uploads file to AWS S3 bucket.
        :param file_format: string, json, csv, tsv
        :param upload_to_s3: boolean
        """
        filename = "mix_data_file.{0}".format(file_format)
        self.log.info("Creating file {0} and uploading to Azure".format(filename))
        cur_dir = os.path.dirname(__file__)
        filepath = os.path.join(cur_dir, filename)
        sample_data = {
            "key1": "sample1",
            "key2": "sample2",
            "key3": "sample3",
            "key4": "sample4"
        }
        with open(filepath, "a+") as fh:
            fh.write(json.dumps(sample_data))
            fh.write("\n")
            fh.write(",".join(str(x) for x in sample_data.values()))
            fh.write("\n")
            fh.write("\t".join(str(x) for x in sample_data.values()))
            fh.write("\n")
        if upload_to_azure:
            try:
                blobClient = self.blob_container_client.getBlobClient(filename)
                blobClient.uploadFromFile(filepath)
                if blobClient:
                    upload_success = True
                else:
                    upload_success = False
            except Exception as err:
                self.log.error("Error while uploading file - {0} to Azure".format(
                    filepath))
                self.log.error("Error -- {0}".format(str(err)))
                upload_success = False
            finally:
                os.remove(filepath)
                if not upload_success:
                    self.log.error("Error while uploading file - {0} to Azure".format(
                        filepath))
                return upload_success
        else:
            return filepath

    def generate_file_with_record_of_size_and_upload(
            self, bucket_name, filename, record_size=32000000,
            file_format="json", upload_to_azure=True, file_extension=None):
        """
        Generate a single file of format specified, but file contains a single
        record of data, of type json, csv or tsv,
        and uploads file to AWS S3 bucket.
        :param record_size: int, size in Bytes
        :param file_format: string, json, csv, tsv
        :param upload_to_s3: boolean
        """
        if file_extension is None:
            file_extension = file_format
        if file_extension != "":
            filename = "{0}.{1}".format(filename, file_extension)
        self.log.info("Creating file {0} and uploading to Azure".format(filename))
        cur_dir = os.path.dirname(__file__)
        filepath = os.path.join(cur_dir, filename)
        sample_data = {
            "key1": "sample1",
            "key2": "sample2",
            "key3": "sample3",
            "key4": "sample4"
        }
        sample_data["key5"] = [''.rjust(record_size, 'a')][0]

        with open(filepath, "a+") as fh:
            if file_format == "json":
                fh.write(json.dumps(sample_data))
            elif file_format == "csv":
                fh.write(",".join(str(x) for x in sample_data.values()))
            elif file_format == "tsv":
                fh.write("\t".join(str(x) for x in sample_data.values()))
        if upload_to_azure:
            try:
                blobClient = self.blob_container_client.getBlobClient(filename)
                blobClient.uploadFromFile(filepath)
                if blobClient:
                    upload_success = True
                else:
                    upload_success = False
            except Exception as err:
                self.log.error("Error while uploading file - {0} to Azure".format(
                    filepath))
                self.log.error("Error -- {0}".format(str(err)))
                upload_success = False
            finally:
                os.remove(filepath)
                if not upload_success:
                    self.log.error("Error while uploading file - {0} to Azure".format(
                        filepath))
                return upload_success
        else:
            return filename
