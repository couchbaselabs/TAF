import os as OS
from com.couchbase.client.core.error import DocumentNotFoundException
from com.couchbase.client.java import Cluster
from com.couchbase.client.java.json import JsonObject, JsonArray
from com.couchbase.client.java.kv import UpsertOptions
from java.time import Duration
import argparse
import json
import time
import get_jenkins_params as jenkins_api

host = '172.23.121.84'
bucket_name = 'rerun_jobs'


def get_run_results():
    run_results = {}
    return run_results


def parse_args():
    """
    Parse command line arguments into a dictionary
    :return: Dictionary of parsed command line arguments
    :rtype: dict
    """
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("build_version", type=str,
                                 help="Couchbase build version of the "
                                      "job")
    argument_parser.add_argument("--executor_jenkins_job",
                                 action='store_true',
                                 help="Run with current executor job")
    argument_parser.add_argument("--jenkins_job", action="store_true",
                                 help="Run with current jenkins job")
    argument_parser.add_argument("--store_data", action="store_true",
                                 help="Store the test_run details. To "
                                      "be used only after testrunner "
                                      "is run")
    argument_parser.add_argument("--install_failure",
                                 action='store_true',
                                 help="Was there install failure in "
                                      "the run?")
    args = vars(argument_parser.parse_args())
    return args


def build_args(build_version, executor_jenkins_job=False,
               jenkins_job=False, store_data=False,
               install_failure=False):
    """
    Build a dictionary of arguments needed for the program
    :param build_version: Couchbase build version of the job
    :type build_version: str
    :param executor_jenkins_job: Run with current Executor job
    :type executor_jenkins_job: bool
    :param jenkins_job: Run with current jenkins job
    :type jenkins_job: bool
    :param store_data: "Store the test_run details. To be used only
    after testrunner is run"
    :type store_data: bool
    :param install_failure: Was there install failure in the run?
    :type install_failure: bool
    :return: Dictionary of parameters
    :rtype: dict
    """
    return locals()


def get_json_object(document):
    """
    Function to convert python dictionary object into Couchbase
    JsonObject. This function will recursively convert the dictionary
    into JsonObject. Also will add JsonArray as required
    :param document: Dictionary to convert to json_object
    :type document: dict
    :return: Couchbase Jsonobject of the dictionary
    :rtype: JsonObject
    """
    json_object = JsonObject.create()
    for field, val in document.items():
        value = None
        if isinstance(val, dict):
            value = get_json_object(val)
        elif isinstance(val, list):
            value = get_json_array(val)
        else:
            value = val
        json_object.put(field, value)
    return json_object


def get_json_array(array):
    """
    Function to convert python list object into Couchbase
    JsonArray. This function will recursively convert the list
    into JsonArray. Also will add JsonObject as required
    :param array: python list to be converted to JsonArray
    :type array: list
    :return: JsonArray of the list
    :rtype: JsonArray
    """
    json_array = JsonArray.create()
    for item in array:
        value = None
        if isinstance(item, dict):
            value = get_json_object(item)
        elif isinstance(item, list):
            value = get_json_array(item)
        else:
            value = item
        json_array.add(value)
    return json_array


def get_document(collection, doc_id):
    """
    Function to get document from server using java sdk
    :param collection: Collection object to the default collection of
    the bucket
    :type collection: Collection
    :param doc_id: Document id to get
    :type doc_id: str
    :return: Boolean of success of getting the document and the
    document in dict
    :rtype: (bool, dict)
    """
    try:
        print ("reading from %s" % doc_id)
        document = collection.get(doc_id)
        content = document.contentAsObject()
        doc = json.loads(str(content))
        return True, doc
    except DocumentNotFoundException as e:
        print(e)
        return False, None
    except Exception as e:
        print(e)
        return False, None


def upsert_document(collection, doc_id, document):
    """
    Function to upsert document into couchbase server using Java sdk
    :param collection: Collection object to the default collection of
    the bucket
    :type collection: Collection
    :param doc_id: Document id to upsert
    :type doc_id: str
    :param document: Document to insert
    :type document: dict
    :return: Nothing
    :rtype: None
    """
    doc_to_insert = get_json_object(document)
    duration = Duration.ofDays(7)
    upsert_option = UpsertOptions.upsertOptions().expiry(duration)
    try:
        collection.upsert(doc_id, doc_to_insert, upsert_option)
        print('upserted %s' % doc_id)
    except Exception as e:
        print(e)


def find_rerun_job(args):
    """
    Find if the job was run previously
    :param args: Dictionary of arguments. Run build_args() if calling
    this from python script or parse_args() if running from shell
    :type args: dict
    :return: If the job was run previously. If yes the re_run
    document from the server
    :rtype: (bool, dict)
    """
    name = None
    store_data = args['store_data']
    install_failure = args['install_failure']
    if args['executor_jenkins_job']:
        os = OS.getenv('os')
        component = OS.getenv('component')
        sub_component = OS.getenv('subcomponent')
        version_build = OS.getenv('version_number')
        name = "{}_{}_{}".format(os, component, sub_component)
    elif args['jenkins_job']:
        name = OS.getenv('JOB_NAME')
        version_build = args['build_version']
    else:
        os = args['os']
        component = args['component']
        sub_component = args['sub_component']
        if os and component and sub_component:
            name = "{}_{}_{}".format(os, component, sub_component)
        elif args['name']:
            name = args['name']
        version_build = args['build_version']
    if not name or not version_build:
        return False, {}
    cluster = Cluster.connect(host, 'Administrator', 'password')
    rerun_jobs = cluster.bucket(bucket_name)
    collection = rerun_jobs.defaultCollection()
    time.sleep(10)
    rerun = False
    doc_id = "{}_{}".format(name, version_build)
    try:
        success, run_document = get_document(collection, doc_id)
        if not store_data:
            if not success:
                return False, {}
            else:
                return True, run_document
        parameters = jenkins_api.get_params(OS.getenv('BUILD_URL'))
        run_results = get_run_results()
        job_to_store = {
            "job_url": OS.getenv('BUILD_URL'),
            "build_id": OS.getenv('BUILD_ID'),
            "run_params": parameters,
            "run_results": run_results,
            "install_failure": install_failure}
        if success:
            rerun = True
        else:
            run_document = {
                "build": version_build,
                "num_runs": 0,
                "jobs": []}
        run_document['num_runs'] += 1
        run_document['jobs'].append(job_to_store)
        upsert_document(collection, doc_id, run_document)
        return rerun, run_document
    except Exception as e:
        print(e)
        return False, {}


if __name__ == "__main__":
    args = parse_args()
    rerun, document = find_rerun_job(args)
    print(rerun.__str__())
