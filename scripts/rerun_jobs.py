import argparse
import json
import os as OS
import requests
import xml.dom.minidom

import find_rerun_job
import get_jenkins_params
import merge_reports

host = 'greenboard.sc.couchbase.com'
bucket_name = 'rerun_jobs'
AWS_LINK = 'http://cb-logs-qe.s3-website-us-west-2.amazonaws.com'
TIMEOUT = 60


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
    argument_parser.add_argument("--install_failure",
                                 action='store_true',
                                 help="Was there install failure in "
                                      "the run?")
    argument_parser.add_argument("--manual_run", action="store_true",
                                 help="Is this a manual rerun of the "
                                      "job")
    argument_parser.add_argument("--run_params", type=str, default="",
                                 help="Testrunner extra params for "
                                      "the job")
    args = vars(argument_parser.parse_args())
    return args


def build_args(build_version, executor_jenkins_job=False,
               jenkins_job=False, install_failure=False):
    """
    Build a dictionary of arguments needed for the program
    :param build_version: Couchbase build version of the job
    :type build_version: str
    :param executor_jenkins_job: Run with current Executor job
    :type executor_jenkins_job: bool
    :param jenkins_job: Run with current jenkins job
    :type jenkins_job: bool
    :param install_failure: Was there install failure in the run?
    :type install_failure: bool
    :return: Dictionary of parameters
    :rtype: dict
    """
    return locals()


def merge_xmls(rerun_document, run_params=""):
    """
    Merge the xml of the runs into a single xml for the jenkins job
    to consume to show the test results
    :param rerun_document: The rerun document containing the details
    of previous runs.
    :type rerun_document: dict
    :return: The merged testsuites from runs
    :rtype: dict
    """
    if not rerun_document:
        testsuites = merge_reports.merge_reports("logs/**/*.xml",
                                                 run_params)
        return testsuites
    print("Merging xmls")
    num_runs = rerun_document['num_runs'] - 1
    valid_run = False
    job = None
    while not valid_run and num_runs > 0:
        job = rerun_document['jobs'][num_runs - 1]
        if job['install_failure']:
            num_runs -= 1
        else:
            valid_run = True
    if not job:
        print("no valid jobs found with run results")
        testsuites = merge_reports.merge_reports("logs/**/*.xml",
                                                 run_params)
        return testsuites
    job_url = job['job_url']
    artifacts = get_jenkins_params.get_js(job_url, "tree=artifacts[*]")
    if not artifacts or len(artifacts['artifacts']) == 0:
        # The build's archived artifacts (raw report xml files) are
        # pruned on their own, often stricter, retention policy and
        # can disappear well before the build record itself does.
        # The build's testReport is kept alongside the build record,
        # so try that live Jenkins source before falling back to AWS.
        print("Could not find the job. Job might be deleted")
        print("Trying to get the test report directly from Jenkins")
        logs = get_from_testreport(job_url)
        if not logs:
            print("Trying to get the job logs from AWS")
            logs = get_from_aws(rerun_document, job_url)
    else:
        relative_paths = []
        for artifact in artifacts["artifacts"]:
            if artifact["relativePath"].startswith("logs/") and \
                    artifact["relativePath"].endswith(".xml"):
                relative_paths.append(artifact["relativePath"])
        logs = []
        for rel_path in relative_paths:
            got_data = False
            retries = 5
            xml_data = None
            while not got_data and retries > 0:
                xml_data = get_jenkins_params.download_url_data(
                    "{0}artifact/"
                    "{1}".format(
                        job_url, rel_path))
                if xml_data:
                    got_data = True
                else:
                    retries -= 1
            if not xml_data:
                print("Could not reach the URL. Skipping for now. "
                      "Reconcile with data from %s" % (job_url))
                continue
            try:
                file_name = rel_path.split('/')[-1]
                file_name = "Old_Report_{0}".format(file_name)
                with open(file_name, "w") as f:
                    if isinstance(xml_data, str):
                        f.writelines(xml_data)
                    elif isinstance(xml_data, bytes):
                        f.writelines(xml_data.decode('utf-8'))
                logs.append(file_name)
            except Exception as e:
                print(e)
        if not logs:
            print("Could not download the artifacts")
            print("Trying to download from AWS")
            logs = get_from_aws(rerun_document, job_url)
    if logs is None or not logs:
        print("Could not download any previous logs")
        logs = []
    logs.append("logs/**/*.xml")
    testsuites = merge_reports.merge_reports(logs, run_params)
    try:
        # Remove old logs from the machine
        try:
            logs.remove("logs/**/*.xml")
        except ValueError:
            pass
        for path in logs:
            OS.remove(path)
    except:
        pass
    print("merged xmls")
    return testsuites


def _node_text(node):
    """
    Get the concatenated text content of an xml element
    :param node: xml element whose text content is needed
    :type node: xml.dom.minidom.Element
    :return: Text content of the node
    :rtype: str
    """
    return "".join(child.data for child in node.childNodes
                   if child.nodeType == child.TEXT_NODE)


def _junit_result_from_jenkins_status(status):
    """
    Map a Jenkins testReport case status to the pass/fail/skip result
    values used in TAF's own JUnit reports
    :param status: Jenkins case status (e.g. PASSED, FAILED, SKIPPED)
    :type status: str
    :return: One of "pass", "fail" or "skip"
    :rtype: str
    """
    status = (status or "").upper()
    if status in ("FAILED", "REGRESSION"):
        return "fail"
    if status == "SKIPPED":
        return "skip"
    return "pass"


def convert_testreport_to_junit(xml_data):
    """
    Convert Jenkins' native testReport xml (hudson.tasks.junit.TestResult
    schema: <testResult><suite><case><className/><name/><status/></case>
    ...) into the plain JUnit <testsuite tests= failures=><testcase
    result=> format that merge_reports.merge_reports() understands. This
    schema is what Jenkins keeps attached to the build record itself
    (and what get AWS-saved copy mirrors), independent of whether the
    build's archived artifact files are still around.
    :param xml_data: Raw testReport xml content
    :type xml_data: str or bytes
    :return: Equivalent JUnit xml string, or None if it couldn't be
    parsed or had no test cases
    :rtype: str
    """
    try:
        doc = xml.dom.minidom.parseString(xml_data)
    except Exception as e:
        print(e)
        return None
    testresult_elems = doc.getElementsByTagName("testResult")
    if not testresult_elems:
        return None

    suites = {}
    for suite_elem in testresult_elems[0].getElementsByTagName("suite"):
        for case_elem in suite_elem.getElementsByTagName("case"):
            name_elem = case_elem.getElementsByTagName("name")
            status_elem = case_elem.getElementsByTagName("status")
            if not name_elem or not status_elem:
                continue
            class_name_elem = case_elem.getElementsByTagName("className")
            duration_elem = case_elem.getElementsByTagName("duration")
            class_name = _node_text(class_name_elem[0]) if \
                class_name_elem else _node_text(name_elem[0])
            tc_name = _node_text(name_elem[0])
            tc_time = _node_text(duration_elem[0]) if duration_elem \
                else "0"
            result = _junit_result_from_jenkins_status(
                _node_text(status_elem[0]))

            error_text = ""
            if result == "fail":
                for tag in ("errorStackTrace", "errorDetails"):
                    detail_elem = case_elem.getElementsByTagName(tag)
                    if detail_elem and detail_elem[0].childNodes:
                        error_text = _node_text(detail_elem[0])
                        break

            suites.setdefault(class_name, []).append(
                (tc_name, tc_time, result, error_text))

    if not suites:
        return None

    out_doc = xml.dom.minidom.Document()
    root = out_doc.createElement("testsuites")
    out_doc.appendChild(root)
    for class_name, cases in suites.items():
        failures = sum(1 for case in cases if case[2] == "fail")
        testsuite = out_doc.createElement("testsuite")
        testsuite.setAttribute("name", class_name)
        testsuite.setAttribute("tests", str(len(cases)))
        testsuite.setAttribute("failures", str(failures))
        testsuite.setAttribute("errors", str(failures))
        testsuite.setAttribute("skips", "0")
        testsuite.setAttribute("time", "0")
        for tc_name, tc_time, result, error_text in cases:
            testcase = out_doc.createElement("testcase")
            testcase.setAttribute("name", tc_name)
            testcase.setAttribute("time", tc_time)
            testcase.setAttribute("result", result)
            if result == "fail":
                error_elem = out_doc.createElement("error")
                error_elem.appendChild(
                    out_doc.createTextNode(error_text or "Failed"))
                testcase.appendChild(error_elem)
            testsuite.appendChild(testcase)
        root.appendChild(testsuite)
    return root.toxml()


def _write_junit_report(junit_xml, file_name):
    """
    Write a converted JUnit xml string to disk for merge_reports to
    later glob and parse
    :param junit_xml: JUnit-format xml content
    :type junit_xml: str
    :param file_name: Name of the file to write to
    :type file_name: str
    :return: List with the written file name, or None on failure
    :rtype: list
    """
    try:
        with open(file_name, "w") as f:
            f.write(junit_xml)
        return [file_name]
    except Exception as e:
        print(e)
        return None


def get_from_testreport(job_url):
    """
    Get the previous job's results directly from Jenkins' testReport,
    which Jenkins keeps attached to the build record for as long as the
    build itself exists - typically much longer than the build's
    archived artifact files, which can be pruned on their own, more
    aggressive retention policy.
    :param job_url: Job url of the job whose test report is needed
    :type job_url: str
    :return: List of converted report files if successful, else None
    :rtype: list
    """
    url = "{0}/testReport/api/xml?pretty=true".format(
        job_url.rstrip('/'))
    xml_data = get_jenkins_params.download_url_data(url)
    if not xml_data:
        print("Could not get the test report from jenkins. ")
        return None
    junit_xml = convert_testreport_to_junit(xml_data)
    if not junit_xml:
        print("Could not parse the test report from jenkins. ")
        return None
    job_build_number = job_url.rstrip('/').split('/')[-1]
    return _write_junit_report(
        junit_xml, "Old_Report_testreport_{0}.xml".format(
            job_build_number))


def get_from_aws(rerun_document, job_url):
    """
    Get the previous job's results from aws. The jenkins_logs archive
    for a build only contains individual saved files (testresult.xml,
    consoleText.txt, etc.), not a zipped bundle of the raw report xmls,
    so this reads the same Jenkins-native testResult xml format used
    by get_from_testreport().
    :param rerun_document: The rerun document containing reerun details
    :type rerun_document: dict
    :param job_url: Job url of the job whose logs have to be downloaded
    :type job_url: str
    :return: List of downloaded files if successful, else None
    :rtype: list
    """
    build = rerun_document['build']
    job_name = job_url.rstrip('/').split('/')[-2]
    job_build_number = job_url.rstrip('/').split('/')[-1]
    aws_link = '{0}/{1}/jenkins_logs/{2}/{3}' \
               '/testresult.xml'.format(AWS_LINK, build, job_name,
                                        job_build_number)
    xml_data = get_jenkins_params.download_url_data(aws_link)
    if not xml_data:
        print('Could not get the test result from aws. ')
        return None
    junit_xml = convert_testreport_to_junit(xml_data)
    if not junit_xml:
        print('Could not parse the test result from aws. ')
        return None
    return _write_junit_report(
        junit_xml, 'Old_Report_{0}.xml'.format(job_build_number))


def should_rerun_tests(testsuites=None, install_failure=False,
                       retries=0):
    """
    Finds out if the job has to be rerun again based on number of
    failure in the current job, if number of retries has been exceeded
    :param testsuites: The testsuite containing the merged results
    from current and previous runs.
    :type testsuites: dict
    :param install_failure: Was there an install failure in this job
    :type install_failure: bool
    :param retries: Number of times to retry
    :type retries: int
    :return: Boolean telling whether to rerun the job or not
    :rtype: bool
    """
    if install_failure and retries > 0:
        return True
    if retries < 1:
        return False
    for tskey in testsuites.keys():
        tests = testsuites[tskey]['tests']
        for testname in tests.keys():
            testcase = tests[testname]
            if testcase['error'] or testcase["result"] == "not_run":
                return True
    return False


def get_rerun_parameters(rerun_document=None, is_rerun=False):
    """
    Get the rerun parameters for the rerun of the job
    :param rerun_document: Document containing the run history of the
    job
    :type rerun_document: dict
    :param is_rerun: Was this job a rerun
    :type is_rerun: bool
    :return: Re-run parameters to be used in the next job or current job
    :rtype: str
    """
    rerun_params = None
    if not is_rerun and not rerun_document and (rerun_document and
                                                rerun_document[
                                                    'num_runs'] == 1):
        current_job_url = OS.getenv("BUILD_URL")
        rerun_params = "-d failed={}".format(current_job_url)
    num_runs = rerun_document['num_runs']
    valid_run = False
    valid_job = None
    while not valid_run and num_runs > 0:
        job = rerun_document['jobs'][num_runs - 1]
        if job['install_failure']:
            num_runs -= 1
        else:
            job_url = job['job_url']
            artifacts = get_jenkins_params.get_js(job_url, "tree=artifacts[*]")
            if not artifacts or len(artifacts['artifacts']) == 0:
                print("Could not find the job. Job might be deleted. Seeing if the job was saved on s3")
                build = rerun_document['build']
                job_name = job_url.rstrip('/').split('/')[-2]
                job_build_number = job_url.rstrip('/').split('/')[-1]
                aws_link = '{0}/{1}/jenkins_logs/{2}/{3}' \
                           '/testresult.xml'.format(AWS_LINK, build, job_name,
                                                     job_build_number)
                test_result_xml = get_jenkins_params.download_url_data(aws_link)
                if not test_result_xml:
                    print("Job wasn't saved. Trying with older build.")
                    num_runs -= 1
                    continue
            valid_run = True
            valid_job = job
    if valid_run and valid_job:
        job_url = valid_job['job_url']
        rerun_params = "-d failed={}".format(job_url)
    return rerun_params


def run_jenkins_job(url, params):
    """
    Trigger a jenkins job with the url provided and the params to the
    job
    :param url: Jenkins job url
    :type url: str
    :param params: Parameters to be passed to the job
    :type params: dict
    :return: Content of the call
    :rtype: str
    """
    url = "{0}&{1}".format(url, requests.quote(params))
    print(url)
    try:
        return requests.get(url).text
    except Exception as e:
        print(e)
        return None


def rerun_job(args):
    """
    Rerun a job based on the arguments to the program. Determine if a
    rerun has to occur or not
    :param args: Dictionary of arguments to the program
    :type args: dict
    :return: Nothing
    :rtype: None
    """
    build_version = args['build_version']
    executor_jenkins_job = args['executor_jenkins_job']
    jenkins_job = args['jenkins_job']
    install_failure = args['install_failure']
    fresh_run = OS.getenv('fresh_run', False)
    run_params = args['run_params']
    is_rerun_args = find_rerun_job.build_args(build_version,
                                              executor_jenkins_job=executor_jenkins_job,
                                              jenkins_job=jenkins_job,
                                              store_data=True,
                                              install_failure=install_failure)
    is_rerun, rerun_document = find_rerun_job.find_rerun_job(is_rerun_args)
    if is_rerun and not install_failure and (fresh_run != 'true' or
                                             fresh_run is False):
        test_suites = merge_xmls(rerun_document, run_params)
    else:
        test_suites = merge_xmls({}, run_params)
    retry_count = OS.getenv("retries")
    if not retry_count:
        if "retries" in args:
            retry_count = args['retries']
        else:
            retry_count = 0
    if isinstance(retry_count, str):
        retry_count = int(retry_count)
    should_rerun = should_rerun_tests(test_suites, install_failure,
                                      retry_count)
    if not should_rerun:
        print("No more failed tests. Stopping reruns")
        return
    rerun_params = get_rerun_parameters(rerun_document, is_rerun)
    if not rerun_params:
        if install_failure:
            rerun_params = ''
        else:
            return
    if jenkins_job:
        current_job_url = OS.getenv('BUILD_URL')
        current_job_params = get_jenkins_params.get_params(
            current_job_url)
        current_job_params['rerun_params'] = rerun_params
        current_job_params['retries'] = retry_count - 1
        job_url = OS.getenv("JOB_URL")
        job_token = args['token']
        job_url = "{0}buildWithParameters?token={1}".format(job_url,
                                                            job_token)
        _ = run_jenkins_job(job_url, current_job_params)
        return
    dispatcher_params = OS.getenv('dispatcher_params').lstrip(
        "parameters=")
    dispatcher_params = json.loads(dispatcher_params)
    dispatcher_params['rerun_params'] = rerun_params
    dispatcher_params['retries'] = retry_count - 1
    dispatcher_params['component'] = OS.getenv('component')
    dispatcher_params['subcomponent'] = OS.getenv('subcomponent')
    dispatcher_params['fresh_run'] = "false"
    job_url = dispatcher_params.pop('dispatcher_url')
    job_url = "{0}buildWithParameters?token=extended_sanity".format(
        job_url)
    _ = run_jenkins_job(job_url, dispatcher_params)


def manual_rerun(args):
    """
    Get the rrerun parameters for manual rerun of the job. Puts the
    parameter into a file to be consumed by  jenkins job
    :param args: Dictionary of arguments to the program
    :type args: dict
    :return: Nothing
    :rtype: None
    """
    build_version = args['build_version']
    executor_jenkins_job = args['executor_jenkins_job']
    jenkins_job = args['jenkins_job']
    is_rerun_args = find_rerun_job.build_args(build_version,
                                              executor_jenkins_job=executor_jenkins_job,
                                              jenkins_job=jenkins_job,
                                              store_data=False,
                                              install_failure=False)
    is_rerun, rerun_document = find_rerun_job.find_rerun_job(
        is_rerun_args)
    if not is_rerun:
        print("This is the first run for this build.")
        return
    rerun_param = get_rerun_parameters(rerun_document, is_rerun)
    if not rerun_param:
        print("Could not find a valid previous build to run with")
        return
    with open("rerun_props_file", 'w') as f:
        f.write("{}".format(rerun_param))


if __name__ == '__main__':
    args = parse_args()
    if args['manual_run']:
        manual_rerun(args)
    else:
        rerun_job(args)
