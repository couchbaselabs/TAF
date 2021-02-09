#!/usr/bin/env python
import base64
import gzip
import glob
import logging
import logging.config
import os
import re
import shutil
import sys
import threading
import time
import unittest
import urllib2
import xml.dom.minidom

from httplib import BadStatusLine
from optparse import OptionParser, OptionGroup
from os.path import basename, splitext
from pprint import pprint
from threading import Thread, Event

from java.lang import System

sys.path = [".", "lib", "pytests", "pysystests", "couchbase_utils",
            "platform_utils", "connections"] + sys.path
from sdk_client3 import SDKClient
from remote.remote_util import RemoteMachineShellConnection
from TestInput import TestInputParser, TestInputSingleton
from scripts.collect_server_info import cbcollectRunner, couch_dbinfo_Runner
from scripts.getcoredumps import Getcoredumps, Clearcoredumps
from xunit import XUnitTestResult

if sys.hexversion < 0x02060000:
    print("Testrunner requires version 2.6+ of python")
    sys.exit()


def usage(err=None):
    print("""\
Syntax: testrunner [options]

Examples:
  ./testrunner -i tmp/local.ini -t performance.perf.DiskDrainRate
  ./testrunner -i tmp/local.ini -t performance.perf.DiskDrainRate.test_9M
""")
    sys.exit(0)


def parse_args(argv):

    parser = OptionParser()

    parser.add_option("-q", action="store_false", dest="verbose")

    tgroup = OptionGroup(parser, "TestCase/Runlist Options")
    tgroup.add_option("-i", "--ini",
                      dest="ini",
                      help="Path to .ini file containing server "
                           "information,e.g -i tmp/local.ini")
    tgroup.add_option("-c", "--config", dest="conf",
                      help="Config file name (located in the conf "
                           "subdirectory), e.g -c py-view.conf")
    tgroup.add_option("-t", "--test",
                      dest="testcase",
                      help="Test name (multiple -t options add more "
                           "tests) e.g -t "
                           "performance.perf.DiskDrainRate")
    tgroup.add_option("-m", "--mode",
                      dest="mode",
                      help="Use java for Java SDK, rest for rest APIs.")
    tgroup.add_option("-d", "--include_tests", dest="include_tests",
                      help="Value can be 'failed' (or) 'passed' (or) "
                           "'failed=<junit_xml_path (or) "
                           "jenkins_build_url>' (or) "
                           "'passed=<junit_xml_path or "
                           "jenkins_build_url>' (or) "
                           "'file=<filename>' (or) '<regular "
                           "expression>' to include tests in the run. "
                           "Use -g option to search "
                           "entire conf files. e.g. -d 'failed' or -d "
                           "'failed=report.xml' or -d "
                           "'^2i.*nodes_init=2.*'")
    tgroup.add_option("-e", "--exclude_tests", dest="exclude_tests",
                      help="Value can be 'failed' (or) 'passed' (or) "
                           "'failed=<junit_xml_path (or) "
                           "jenkins_build_url>' (or) "
                           "'passed=<junit_xml_path (or) "
                           "jenkins_build_url>' or 'file=<filename>' "
                           "(or) '<regular expression>' "
                           "to exclude tests in the run. Use -g "
                           "option to search entire conf "
                           "files. e.g. -e 'passed'")
    tgroup.add_option("-r", "--rerun", dest="rerun",
                      help="Rerun fail or pass tests with given "
                           "=count number of times maximum. "
                           "\ne.g. -r 'fail=3'")
    tgroup.add_option("-g", "--globalsearch", dest="globalsearch",
                      help="Option to get tests from given conf file "
                           "path pattern, "
                           "like conf/**/*.conf. Useful for include "
                           "or exclude conf files to "
                           "filter tests. e.g. -g 'conf/**/.conf'",
                      default="")
    parser.add_option_group(tgroup)

    parser.add_option("-p", "--params",
                      dest="params",
                      help="Optional key=value parameters, "
                           "comma-separated -p k=v,k2=v2,...",
                      default="")
    parser.add_option("-n", "--noop", action="store_true",
                      help="NO-OP - emit test names, but don't "
                           "actually run them e.g -n true")
    parser.add_option("-l", "--log-level",
                      dest="loglevel", default="INFO",
                      help="e.g -l debug,info,warning")
    options, args = parser.parse_args()

    tests = []
    test_params = {}

    if not options.ini:
        parser.error("please specify an .ini file (-i)")
        parser.print_help()
    else:
        test_params['ini'] = options.ini
        if not os.path.exists(options.ini):
            sys.exit("ini file {0} was not found".format(options.ini))

    test_params['cluster_name'] = \
    splitext(os.path.basename(options.ini))[0]

    if not options.testcase and not options.conf and not \
            options.globalsearch and not options.include_tests and \
            not options.exclude_tests:
        parser.error(
            "Please specify a configuration file (-c) or a test case "
            "(-t) or a globalsearch (-g) option.")
        parser.print_help()
    if options.conf and not options.globalsearch:
        parse_conf_file(options.conf, tests, test_params)
    if options.globalsearch:
        parse_global_conf_file(options.globalsearch, tests, test_params)
    if options.include_tests:
        tests = process_include_or_filter_exclude_tests("include",
                                                        options.include_tests,
                                                        tests,
                                                        options)
    if options.exclude_tests:
        tests = process_include_or_filter_exclude_tests("exclude",
                                                        options.exclude_tests,
                                                        tests, options)

    if options.testcase:
        tests.append(options.testcase)
    if options.noop:
        print(("---\n" + "\n".join(tests) + "\n---\nTotal=" + str(
            len(tests))))
        sys.exit(0)

    return tests, test_params, options.ini, options.params, options


def create_log_file(log_config_file_name, log_file_name, level):
    tmpl_log_file = open("jython.logging.conf")
    log_file = open(log_config_file_name, "w")
    log_file.truncate()
    for line in tmpl_log_file:
        line = line.replace("@@FILENAME@@", log_file_name.replace('\\', '/'))
        log_file.write(line)
    log_file.close()
    tmpl_log_file.close()


def append_test(tests, name):
    prefix = ".".join(name.split(".")[0:-1])
    """
        Some tests carry special chars, need to skip it
    """
    if "test_restore_with_filter_regex" not in name and \
        "test_restore_with_rbac" not in name and \
        "test_backup_with_rbac" not in name and \
         name.find('*') > 0:
        for t in unittest.TestLoader().loadTestsFromName(name.rstrip('.*')):
            tests.append(prefix + '.' + t._testMethodName)
    else:
        tests.append(name)


def locate_conf_file(filename):
    print("Filename: %s" % filename)
    if filename:
        if os.path.exists(filename):
            return file(filename)
        if os.path.exists("conf{0}{1}".format(os.sep, filename)):
            return file("conf{0}{1}".format(os.sep, filename))
        return None


def parse_conf_file(filename, tests, params):
    """Parse a configuration file.
    Configuration files contain information and parameters about test execution.
    Should follow the following order:

    Part1: Tests to execute.
    Part2: Parameters to override the defaults.

    @e.x:
        TestModuleName1:
            TestName1
            TestName2
            ....
        TestModuleName2.TestName3
        TestModuleName2.TestName4
        ...

        params:
            items=4000000
            num_creates=400000
            ....
    """
    f = locate_conf_file(filename)
    if not f:
        usage("unable to locate configuration file: " + filename)
    prefix = None
    for line in f:
        stripped = line.strip()
        if stripped.startswith("#") or len(stripped) <= 0:
            continue
        if stripped.endswith(":"):
            prefix = stripped.split(":")[0]
            print("Prefix: %s" % prefix)
            continue
        name = stripped
        if prefix and prefix.lower() == "params":
            args = stripped.split("=", 1)
            if len(args) == 2:
                params[args[0]] = args[1]
            continue
        elif line.startswith(" ") and prefix:
            name = prefix + "." + name
        prefix = ".".join(name.split(",")[0].split('.')[0:-1])
        append_test(tests, name)

    # If spec parameter isn't defined, testrunner uses the *.conf
    # filename for
    # the spec value
    if 'spec' not in params:
        params['spec'] = splitext(basename(filename))[0]

    params['conf_file'] = filename


def parse_global_conf_file(dirpath, tests, params):
    print("dirpath=" + dirpath)
    if os.path.isdir(dirpath):
        dirpath = dirpath + os.sep + "**" + os.sep + "*.conf"
        print("Global filespath=" + dirpath)

    conf_files = glob.glob(dirpath)
    for file in conf_files:
        parse_conf_file(file, tests, params)


def process_include_or_filter_exclude_tests(filtertype, option, tests,
                                            options):
    if filtertype == 'include' or filtertype == 'exclude':

        if option.startswith('failed') or option.startswith(
                'passed') or option.startswith(
                "http://") or option.startswith("https://"):
            passfail = option.split("=")
            tests_list = []
            if len(passfail) == 2:
                if passfail[1].startswith("http://") or passfail[
                    1].startswith("https://"):
                    tp, tf = parse_testreport_result_xml(passfail[1])
                else:
                    tp, tf = parse_junit_result_xml(passfail[1])
            elif option.startswith("http://") or option.startswith(
                    "https://"):
                tp, tf = parse_testreport_result_xml(option)
                tests_list = tp + tf
            else:
                tp, tf = parse_junit_result_xml()
            if tp is None and tf is None:
                return tests
            if option.startswith('failed') and tf:
                tests_list = tf
            elif option.startswith('passed') and tp:
                tests_list = tp
            if filtertype == 'include':
                tests = tests_list
            else:
                for line in tests_list:
                    isexisted, t = check_if_exists_with_params(tests,
                                                               line,
                                                               options.params)
                    if isexisted:
                        tests.remove(t)
        elif option.startswith("file="):
            filterfile = locate_conf_file(option.split("=")[1])
            if filtertype == 'include':
                tests_list = []
                if filterfile:
                    for line in filterfile:
                        tests_list.append(line.strip())
                tests = tests_list
            else:
                for line in filterfile:
                    isexisted, t = check_if_exists_with_params(tests,
                                                               line.strip(),
                                                               options.params)
                    if isexisted:
                        tests.remove(t)
        else:  # pattern
            if filtertype == 'include':
                tests = [i for i in tests if re.search(option, i)]
            else:
                tests = [i for i in tests if not re.search(option, i)]

    else:
        print(
            "Warning: unknown filtertype given (only include/exclude "
            "supported)!")

    return tests


def parse_testreport_result_xml(filepath=""):
    if filepath.startswith("http://") or filepath.startswith(
            "https://"):
        url_path = filepath + "/testReport/api/xml?pretty=true"
        jobnamebuild = filepath.split('/')
        if not os.path.exists('logs'):
            os.mkdir('logs')
        newfilepath = 'logs' + ''.join(os.sep) + '_'.join(
            jobnamebuild[-3:]) + "_testresult.xml"
        print("Downloading " + url_path + " to " + newfilepath)
        try:
            req = urllib2.Request(url_path)
            filedata = urllib2.urlopen(req)
            datatowrite = filedata.read()
            filepath = newfilepath
            with open(filepath, 'wb') as f:
                f.write(datatowrite)
        except Exception as ex:
            print("Error:: " + str(
                ex) + "! Please check if " + url_path + " URL is "
                                                        "accessible!!")
            print("Running all the tests instead for now.")
            return None, None
    if filepath == "":
        filepath = "logs/**/*.xml"
    print("Loading result data from " + filepath)
    xml_files = glob.glob(filepath)
    passed_tests = []
    failed_tests = []
    for xml_file in xml_files:
        print("-- " + xml_file + " --")
        doc = xml.dom.minidom.parse(xml_file)
        testresultelem = doc.getElementsByTagName("testResult")
        testsuitelem = testresultelem[0].getElementsByTagName("suite")
        for ts in testsuitelem:
            testcaseelem = ts.getElementsByTagName("case")
            for tc in testcaseelem:
                tcname = getNodeText(
                    (tc.getElementsByTagName("name")[0]).childNodes)
                tcstatus = getNodeText(
                    (tc.getElementsByTagName("status")[0]).childNodes)
                if tcstatus == 'PASSED':
                    failed = False
                    passed_tests.append(tcname)
                else:
                    failed = True
                    failed_tests.append(tcname)

    if failed_tests:
        failed_tests = transform_and_write_to_file(failed_tests,
                                                   "failed_tests.conf")

    if passed_tests:
        passed_tests = transform_and_write_to_file(passed_tests,
                                                   "passed_tests.conf")

    return passed_tests, failed_tests


def getNodeText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)


def parse_junit_result_xml(filepath=""):
    if filepath.startswith("http://") or filepath.startswith(
            "https://"):
        return parse_testreport_result_xml(filepath)
    if filepath == "":
        filepath = "logs/**/*.xml"
    print("Loading result data from " + filepath)
    xml_files = glob.glob(filepath)
    passed_tests = []
    failed_tests = []
    for xml_file in xml_files:
        print("-- " + xml_file + " --")
        doc = xml.dom.minidom.parse(xml_file)
        testsuitelem = doc.getElementsByTagName("testsuite")
        for ts in testsuitelem:
            tsname = ts.getAttribute("name")
            testcaseelem = ts.getElementsByTagName("testcase")
            failed = False
            for tc in testcaseelem:
                tcname = tc.getAttribute("name")
                tcerror = tc.getElementsByTagName("error")
                for tce in tcerror:
                    failed_tests.append(tcname)
                    failed = True
                if not failed:
                    passed_tests.append(tcname)

    if failed_tests:
        failed_tests = transform_and_write_to_file(failed_tests,
                                                   "failed_tests.conf")

    if passed_tests:
        passed_tests = transform_and_write_to_file(passed_tests,
                                                   "passed_tests.conf")
    return passed_tests, failed_tests


def transform_and_write_to_file(tests_list, filename):
    new_test_list = []
    for test in tests_list:
        line = filter_fields(test)
        line = line.rstrip(",")
        isexisted, _ = check_if_exists(new_test_list, line)
        if not isexisted:
            new_test_list.append(line)

    file = open(filename, "w+")
    for line in new_test_list:
        file.writelines((line) + "\n")
    file.close()
    return new_test_list


def filter_fields(testname):
    testwords = testname.split(",")
    line = ""
    for fw in testwords:
        if not fw.startswith("logs_folder") and not fw.startswith(
                "conf_file") \
                and not fw.startswith("cluster_name:") \
                and not fw.startswith("ini:") \
                and not fw.startswith("case_number:") \
                and not fw.startswith("num_nodes:") \
                and not fw.startswith("spec:"):
            line = line + fw.replace(":", "=", 1)
            if fw != testwords[-1]:
                line = line + ","
    return line


def check_if_exists(test_list, test_line):
    new_test_line = ''.join(sorted(test_line))
    for t in test_list:
        t1 = ''.join(sorted(t))
        if t1 == new_test_line:
            return True, t
    return False, ""


def check_if_exists_with_params(test_list, test_line, test_params):
    new_test_line = ''.join(sorted(test_line))
    for t in test_list:
        if test_params:
            t1 = ''.join(sorted(t + "," + test_params.strip()))
        else:
            t1 = ''.join(sorted(t))

        if t1 == new_test_line:
            return True, t
    return False, ""


def create_headers(username, password):
    authorization = base64.encodestring('%s:%s' % (username, password))
    return {'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic %s' % authorization,
            'Accept': '*/*'}


def get_server_logs(input_obj, path):
    for server in input_obj.servers:
        diag_url = "http://%s:%s/diag" % (server.ip, server.port)
        print("Grabbing diags from server %s: %s" % (server.ip, diag_url))

        try:
            req = urllib2.Request(diag_url)
            req.headers = create_headers(input.membase_settings.rest_username,
                                         input.membase_settings.rest_password)
            filename = "{0}/{1}-diag.txt".format(path, server.ip)
            page = urllib2.urlopen(req)
            with open(filename, 'wb') as output:
                sys.stdout.write("downloading {0} ...".format(server.ip))
                while True:
                    buffer = page.read(65536)
                    if not buffer:
                        break
                    output.write(buffer)
                    sys.stdout.write(".")
            file_input = open('{0}'.format(filename), 'rb')
            zipped = gzip.open("{0}.gz".format(filename), 'wb')
            zipped.writelines(file_input)
            file_input.close()
            zipped.close()

            os.remove(filename)
            print("Downloaded and zipped diags @: %s.gz" % filename)
        except urllib2.URLError:
            print("Unable to obtain diags from %s" % diag_url)
        except BadStatusLine:
            print("Unable to obtain diags from %s" % diag_url)
        except Exception as e:
            print("Unable to obtain diags from %s: %s" % (diag_url, e))


def get_logs_cluster_run(path, ns_server_path):
    print("Grabbing logs (cluster-run)")
    path = path or "."
    logs_path = ns_server_path + os.sep + "logs"
    try:
        shutil.make_archive(path + os.sep + "logs", 'zip', logs_path)
    except Exception as e:
        print("Failed to grab logs (CLUSTER_RUN): %s" % str(e))


def get_cbcollect_info(input_obj, path):
    for server in input_obj.servers:
        print("Grabbing cbcollect from %s" % server.ip)
        path = path or "."
        try:
            cbcollectRunner(server, path).run()
        except Exception as e:
            print("Failed to grab CBCOLLECT from %s: %s" % (server.ip, e))


def get_couch_dbinfo(input_obj, path):
    for server in input_obj.servers:
        print("Grabbing dbinfo from %s" % server.ip)
        path = path or "."
        try:
            couch_dbinfo_Runner(server, path).run()
        except Exception as e:
            print("Failed to grab dbinfo from %s: %s" % (server.ip, e))


def clear_old_core_dumps(_input, path):
    for server in _input.servers:
        path = path or "."
        try:
            Clearcoredumps(server, path).run()
        except Exception as e:
            print("Unable to clear core dumps on %s: %s" % (server.ip, e))


def get_core_dumps(_input, path):
    ret = False
    for server in _input.servers:
        print("Grabbing core dumps files from %s" % server.ip)
        path = path or "."
        try:
            if Getcoredumps(server, path).run():
                ret = True
        except Exception as e:
            print("Failed to grab CORE DUMPS from %s: %s"
                  % (server.ip, e))
    return ret


class StoppableThreadWithResult(Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(StoppableThreadWithResult, self).__init__(
            group=group, target=target,
            name=name, args=args, kwargs=kwargs)
        self._stop = Event()

    def stop(self):
        self._stop.set()
        self._Thread__stop()

    def stopped(self):
        return self._stop.isSet()

    def run(self):
        if self._Thread__target is not None:
            self._return = self._Thread__target(*self._Thread__args,
                                                **self._Thread__kwargs)

    def join(self, timeout=None):
        Thread.join(self, timeout=None)
        return self._return


def main():
    names, runtime_test_params, arg_i, arg_p, options = parse_args(sys.argv)
    # get params from command line
    TestInputSingleton.input = TestInputParser.get_test_input(sys.argv)
    # ensure command line params get higher priority
    runtime_test_params.update(TestInputSingleton.input.test_params)
    TestInputSingleton.input.test_params = runtime_test_params

    print("Global Test input params:")
    pprint(TestInputSingleton.input.test_params)
    import mode
    if options.mode == "rest":
        mode.rest = True
    elif options.mode == "cli":
        mode.cli = True
    else:
        mode.java = True
    xunit = XUnitTestResult()
    # Create root logs directory
    abs_path = os.path.dirname(os.path.abspath(sys.argv[0]))
    # Create testrunner logs subdirectory
    str_time = time.strftime("%y-%b-%d_%H-%M-%S", time.localtime())
    root_log_dir = os.path.join(abs_path,
                                "logs%stestrunner-%s" % (os.sep, str_time))
    if not os.path.exists(root_log_dir):
        os.makedirs(root_log_dir)

    results = []
    case_number = 1
    if "GROUP" in runtime_test_params:
        print("Only cases in GROUPs '%s' will be executed"
              % runtime_test_params["GROUP"])
    if "EXCLUDE_GROUP" in runtime_test_params:
        print("Cases from GROUPs '%s' will be excluded"
              % runtime_test_params["EXCLUDE_GROUP"])

    for name in names:
        start_time = time.time()

        # Reset SDK/Shell connection counters
        RemoteMachineShellConnection.connections = 0
        RemoteMachineShellConnection.disconnections = 0
        SDKClient.sdk_connections = 0
        SDKClient.sdk_disconnections = 0

        argument_split = [a.strip()
                          for a in re.split("[,]?([^,=]+)=", name)[1:]]
        params = dict(zip(argument_split[::2], argument_split[1::2]))

        # Note that if ALL is specified at runtime then tests
        # which have no groups are still run - just being explicit on this

        if "GROUP" in runtime_test_params \
                and "ALL" not in runtime_test_params["GROUP"].split(";"):
            # Params is the .conf file parameters.
            if 'GROUP' not in params:
                # this test is not in any groups so we do not run it
                print("Test '%s' skipped, group requested but test has no group"
                      % name)
                continue
            else:
                skip_test = False
                tc_groups = params["GROUP"].split(";")
                for run_group in runtime_test_params["GROUP"].split(";"):
                    if run_group not in tc_groups:
                        skip_test = True
                        break
                if skip_test:
                    print("Test '{0}' skipped, GROUP not satisfied"
                          .format(name))
                    continue
        if "EXCLUDE_GROUP" in runtime_test_params:
            if 'GROUP' in params and \
                    len(set(runtime_test_params["EXCLUDE_GROUP"].split(";"))
                        & set(params["GROUP"].split(";"))) > 0:
                print("Test '%s' skipped, is in an excluded group" % name)
                continue

        # Create Log Directory
        logs_folder = os.path.join(root_log_dir, "test_%s" % case_number)
        os.mkdir(logs_folder)
        test_log_file = os.path.join(logs_folder, "test.log")
        log_config_filename = r'{0}'.format(os.path.join(logs_folder,
                                                         "test.logging.conf"))
        create_log_file(log_config_filename, test_log_file, options.loglevel)
        logging.config.fileConfig(log_config_filename)
        print("Logs will be stored at %s" % logs_folder)
        print("\nguides/gradlew --refresh-dependencies testrunner -P jython=/opt/jython/bin/jython -P 'args=-i {0} {1} -t {2}'\n"
              .format(arg_i or "", arg_p or "", name))
        name = name.split(",")[0]

        # Update the test params for each test
        TestInputSingleton.input.test_params = params
        TestInputSingleton.input.test_params.update(runtime_test_params)
        TestInputSingleton.input.test_params["case_number"] = case_number
        TestInputSingleton.input.test_params["logs_folder"] = logs_folder
        if "rerun" not in TestInputSingleton.input.test_params:
            TestInputSingleton.input.test_params["rerun"] = False
        print("Test Input params:\n%s"
              % TestInputSingleton.input.test_params)
        if "get-coredumps" in TestInputSingleton.input.test_params:
            if TestInputSingleton.input.param("get-coredumps", True):
                clear_old_core_dumps(TestInputSingleton.input, logs_folder)
        try:
            suite = unittest.TestLoader().loadTestsFromName(name)
        except AttributeError, e:
            print("Test %s was not found: %s" % (name, e))
            result = unittest.TextTestRunner(verbosity=2)._makeResult()
            result.errors = [(name, e.message)]
        except SyntaxError, e:
            print("SyntaxError in %s: %s" % (name, e))
            result = unittest.TextTestRunner(verbosity=2)._makeResult()
            result.errors = [(name, e.message)]
        else:
            result = unittest.TextTestRunner(verbosity=2).run(suite)
            if TestInputSingleton.input.param("rerun") \
                    and (result.failures or result.errors):
                print("#"*60, "\n",
                      "## \tTest Failed: Rerunning it one more time",
                      "\n", "#"*60)
                print("####### Running test with trace logs enabled #######")
                TestInputSingleton.input.test_params["log_level"] = "debug"
                result = unittest.TextTestRunner(verbosity=2).run(suite)
            # test_timeout = TestInputSingleton.input.param("test_timeout",
            #                                               None)
            # t = StoppableThreadWithResult(
            #    target=unittest.TextTestRunner(verbosity=2).run,
            #    name="test_thread",
            #    args=(suite))
            # t.start()
            # result = t.join(timeout=test_timeout)
            if "get-coredumps" in TestInputSingleton.input.test_params:
                if TestInputSingleton.input.param("get-coredumps", True):
                    if get_core_dumps(TestInputSingleton.input, logs_folder):
                        result = unittest.TextTestRunner(verbosity=2)._makeResult()
                        result.errors = [(name,
                                          "Failing test: new core dump(s) "
                                          "were found and collected."
                                          " Check testrunner logs folder.")]
                        print("FAIL: New core dump(s) was found and collected")
            if not result:
                for t in threading.enumerate():
                    if t != threading.current_thread():
                        t._Thread__stop()
                result = unittest.TextTestRunner(verbosity=2)._makeResult()
                case_number += 1000
                print("========TEST WAS STOPPED DUE TO  TIMEOUT=========")
                result.errors = [(name, "Test was stopped due to timeout")]
        time_taken = time.time() - start_time
        connection_status_msg = \
            "During the test,\n" \
            "Remote Connections: %s, Disconnections: %s\n" \
            "SDK Connections: %s, Disconnections: %s" \
            % (RemoteMachineShellConnection.connections,
               RemoteMachineShellConnection.disconnections,
               SDKClient.sdk_connections, SDKClient.sdk_disconnections)

        if RemoteMachineShellConnection.connections \
                != RemoteMachineShellConnection.disconnections:
            connection_status_msg += \
                "\n!!!!!! CRITICAL :: Shell disconnection mismatch !!!!!"
        if SDKClient.sdk_connections != SDKClient.sdk_disconnections:
            connection_status_msg += \
                "\n!!!!!! CRITICAL :: SDK disconnection mismatch !!!!!"
        print(connection_status_msg)
        # Concat params to test name
        # To make tests more readable
        params = ''
        if TestInputSingleton.input.test_params:
            for key, value in TestInputSingleton.input.test_params.items():
                if key and value:
                    params += "," + str(key) + ":" + str(value)

        if result.failures or result.errors:
            errors = []
            for failure in result.failures:
                test_case, failure_string = failure
                errors.append(failure_string)
                break
            for error in result.errors:
                test_case, error_string = error
                errors.append(error_string)
                break
            xunit.add_test(name=name, status='fail', time=time_taken,
                           errorType='membase.error', errorMessage=str(errors),
                           params=params)
            results.append({"result": "fail", "name": name})
        else:
            xunit.add_test(name=name, time=time_taken, params=params)
            results.append({"result": "pass",
                            "name": name,
                            "time": time_taken})
        xunit.write("%s%sreport-%s"
                    % (os.path.dirname(logs_folder), os.sep, str_time))
        xunit.print_summary()
        print("testrunner logs, diags and results are available under %s"
              % logs_folder)
        case_number += 1
        if (result.failures or result.errors) and \
                TestInputSingleton.input.param("stop-on-failure", False):
            print("Test fails, all of the following tests will be skipped!!!")
            break

    if "makefile" in TestInputSingleton.input.test_params:
        # Print fail for those tests which failed and do sys.exit() error code
        fail_count = 0
        for result in results:
            if result["result"] == "fail":
                test_run_result = result["name"] + " fail"
                fail_count += 1
            else:
                test_run_result = result["name"] + " pass"
            print(test_run_result)
        if fail_count > 0:
            System.exit(1)
    System.exit(0)


if __name__ == "__main__":
    main()
