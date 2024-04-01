import os
import re
from signal import signal, SIGINT, SIGTERM

import requests
import sys
from argparse import ArgumentParser
from os.path import basename, exists, isdir, sep, splitext
from glob import glob
from unittest import TestLoader
from xml.dom.minidom import parse as xml_parse

from doc_loader.sirius import SiriusClient
from platform_constants import taf


class HelperLib(object):
    def __init__(self):
        pass

    @staticmethod
    def print_and_exit(msg=None, exit_code=0):
        if msg is not None:
            print("Error: %s" % msg)
        sys.exit(exit_code)

    @staticmethod
    def handle_kill_signal(signum, frame):
        print(f"Critical:: Abrupt termination due to signal {signum}")
        HelperLib.cleanup()
        sys.exit(signum)

    @staticmethod
    def register_signal_handlers():
        signal(SIGINT, HelperLib.handle_kill_signal)
        signal(SIGTERM, HelperLib.handle_kill_signal)

    @staticmethod
    def cleanup():
        SiriusClient.terminate_sirius()

    @staticmethod
    def validate_python_version(current_version):
        min_sup_maj_ver, min_sup_minor_ver = taf.min_python_version.split(".")
        max_sup_maj_ver, max_sup_minor_ver = taf.max_python_version.split(".")
        min_sup_maj_ver = int(min_sup_maj_ver)
        min_sup_minor_ver = int(min_sup_minor_ver)
        max_sup_maj_ver = int(max_sup_maj_ver)
        max_sup_minor_ver = int(max_sup_minor_ver)
        curr_maj_ver = int(current_version.major)
        curr_minor_ver = int(current_version.minor)
        if curr_maj_ver < min_sup_maj_ver or curr_maj_ver > max_sup_maj_ver:
            return False
        if curr_maj_ver == min_sup_maj_ver \
                and curr_minor_ver < min_sup_minor_ver:
            return False
        return True

    @staticmethod
    def parse_cmd_line_options():
        parser = ArgumentParser(description="TAF - Test Automation Framework")
        parser.add_argument("-q", action="store_false", dest="verbose")

        parser.add_argument("-i", "--ini", dest="ini", required=True,
                            help="Path to .ini file containing server "
                                 "information,e.g -i tmp/local.ini")
        parser.add_argument("-c", "--config", dest="conf",
                            help="Config file name (located in the conf "
                                 "subdirectory), e.g -c py-view.conf")
        parser.add_argument("-t", "--test", dest="testcase",
                            help="Test name (multiple -t options add more "
                                 "tests) e.g -t "
                                 "performance.perf.DiskDrainRate")
        parser.add_argument("-m", "--mode", dest="mode", default="rest",
                            help="Use java for Java SDK, rest for rest APIs.")
        parser.add_argument("-d", "--include_tests", dest="include_tests",
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
        parser.add_argument("-e", "--exclude_tests", dest="exclude_tests",
                            help="Value can be 'failed' (or) 'passed' (or) "
                                 "'failed=<junit_xml_path (or) "
                                 "jenkins_build_url>' (or) "
                                 "'passed=<junit_xml_path (or) "
                                 "jenkins_build_url>' or 'file=<filename>' "
                                 "(or) '<regular expression>' "
                                 "to exclude tests in the run. Use -g "
                                 "option to search entire conf "
                                 "files. e.g. -e 'passed'")
        parser.add_argument("-r", "--rerun", dest="rerun",
                            help="Rerun fail or pass tests with given "
                                 "=count number of times maximum. "
                                 "\ne.g. -r 'fail=3'")
        parser.add_argument("-g", "--globalsearch",
                            dest="globalsearch", default="",
                            help="Option to get tests from given conf file "
                                 "path pattern, "
                                 "like conf/**/*.conf. Useful for include "
                                 "or exclude conf files to "
                                 "filter tests. e.g. -g 'conf/**/.conf'")

        parser.add_argument("-p", "--params", dest="params", default="",
                            help="Optional key=value parameters, "
                                 "comma-separated -p k=v,k2=v2,...")
        parser.add_argument("-n", "--noop", action="store_true",
                            help="NO-OP - emit test names, but don't "
                                 "actually run them e.g -n true")
        parser.add_argument("-l", "--log-level",
                            dest="loglevel", default="INFO",
                            choices=["DEBUG", "INFO", "WARNING", "CRITICAL"])
        parser.add_argument("--launch_sirius_process", action="store_true",
                            dest="launch_sirius_process", default=False,
                            help="If enabled, will start Sirius as subprocess")
        parser.add_argument("--launch_sirius_docker", action="store_true",
                            dest="launch_sirius_docker", default=False,
                            help="If enabled, will start Sirius as subprocess")
        parser.add_argument("--sirius_url", dest="sirius_url",
                            default="http://localhost:4000",
                            help="Target")
        options = parser.parse_args()

        # Validate input options
        if not exists(options.ini):
            sys.exit("ini file {0} not found".format(options.ini))

        if not options.testcase and not options.conf and not \
                options.globalsearch and not options.include_tests and \
                not options.exclude_tests:
            parser.error(
                "Please specify a configuration file (-c) or a test case "
                "(-t) or a globalsearch (-g) option.")
            parser.print_help()

        return options

    def locate_conf_file(self, filename):
        print("Filename: %s" % filename)
        if filename:
            if exists(filename):
                return open(filename)
            if exists("conf{0}{1}".format(sep, filename)):
                return open("conf{0}{1}".format(sep, filename))
            return None

    def append_test(self, tests, name):
        prefix = ".".join(name.split(".")[0:-1])
        """
            Some tests carry special chars, need to skip it
        """
        if "test_restore_with_filter_regex" not in name and \
                "test_restore_with_rbac" not in name and \
                "test_backup_with_rbac" not in name and \
                name.find('*') > 0:
            for t in TestLoader().loadTestsFromName(name.rstrip('.*')):
                tests.append(prefix + '.' + t._testMethodName)
        else:
            tests.append(name)

    def parse_conf_file(self, filename, tests, params):
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
        f = self.locate_conf_file(filename)
        if not f:
            self.print_and_exit("Unable to locate config file: " + filename)
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
            self.append_test(tests, name)

        # If spec parameter isn't defined, testrunner uses the *.conf
        # filename for
        # the spec value
        if 'spec' not in params:
            params['spec'] = splitext(basename(filename))[0]

        params['conf_file'] = filename

    def parse_global_conf_file(self, dirpath, tests, params):
        print("dirpath=" + dirpath)
        if isdir(dirpath):
            dirpath = dirpath + sep + "**" + sep + "*.conf"
            print("Global filespath=" + dirpath)

        for t_file in glob(dirpath):
            self.parse_conf_file(t_file, tests, params)

    def check_if_exists(self, test_list, test_line):
        new_test_line = ''.join(sorted(test_line))
        for t in test_list:
            t1 = ''.join(sorted(t))
            if t1 == new_test_line:
                return True, t
        return False, ""

    def filter_fields(self, testname):
        if "logs_folder:" in testname:
            testwords = testname.split(",")
            line = ""
            for fw in testwords:
                if not fw.startswith("logs_folder") \
                        and not fw.startswith("conf_file") \
                        and not fw.startswith("cluster_name:") \
                        and not fw.startswith("ini:") \
                        and not fw.startswith("case_number:") \
                        and not fw.startswith("num_nodes:") \
                        and not fw.startswith("spec:"):
                    line = line + fw.replace(":", "=", 1)
                    if fw != testwords[-1]:
                        line = line + ","
            return line
        else:
            testwords = testname.split(",")
            line = []
            for fw in testwords:
                if not fw.startswith("logs_folder=") \
                        and not fw.startswith("conf_file=") \
                        and not fw.startswith("cluster_name=") \
                        and not fw.startswith("ini=") \
                        and not fw.startswith("case_number=") \
                        and not fw.startswith("num_nodes=") \
                        and not fw.startswith("spec="):
                    line.append(fw)
            return ",".join(line)

    def transform_and_write_to_file(self, tests_list, filename):
        new_test_list = []
        for test in tests_list:
            line = self.filter_fields(test)
            line = line.rstrip(",")
            isexisted, _ = self.check_if_exists(new_test_list, line)
            if not isexisted:
                new_test_list.append(line)

        with open(filename, "w+") as fp:
            for line in new_test_list:
                fp.writelines(line + "\n")
        return new_test_list

    def parse_junit_result_xml(self, filepath=""):
        if filepath.startswith("http://") or filepath.startswith(
                "https://"):
            return self.parse_testreport_result_xml(filepath)
        if filepath == "":
            filepath = "logs/**/*.xml"
        print("Loading result data from " + filepath)
        xml_files = glob(filepath)
        passed_tests = []
        failed_tests = []
        for xml_file in xml_files:
            print("-- " + xml_file + " --")
            doc = xml_parse(xml_file)
            testsuitelem = doc.getElementsByTagName("testsuite")
            for ts in testsuitelem:
                testcaseelem = ts.getElementsByTagName("testcase")
                failed = False
                for tc in testcaseelem:
                    tcname = tc.getAttribute("name")
                    tcerror = tc.getElementsByTagName("error")
                    for _ in tcerror:
                        failed_tests.append(tcname)
                        failed = True
                    if not failed:
                        passed_tests.append(tcname)

        if failed_tests:
            failed_tests = self.transform_and_write_to_file(
                failed_tests, "failed_tests.conf")

        if passed_tests:
            passed_tests = self.transform_and_write_to_file(
                passed_tests, "passed_tests.conf")
        return passed_tests, failed_tests

    def get_node_text(self, nodes):
        rc = list()
        for node in nodes:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)

    def parse_testreport_result_xml(self, filepath=""):
        if filepath.startswith("http://") or filepath.startswith(
                "https://"):
            url_path = filepath + "/testReport/api/xml?pretty=true"
            jobnamebuild = filepath.split('/')
            if not exists('logs'):
                os.mkdir('logs')
            newfilepath = 'logs' + ''.join(os.sep) + '_'.join(
                jobnamebuild[-3:]) + "_testresult.xml"
            print("Downloading " + url_path + " to " + newfilepath)
            try:
                req = requests.get(url_path, allow_redirects=True)
                filepath = newfilepath
                with open(filepath, 'wb') as fp:
                    fp.write(req.content)
            except Exception as ex:
                print("Error:: " + str(
                    ex) + "! Please check if " + url_path + " URL is "
                                                            "accessible!!")
                print("Running all the tests instead for now.")
                return None, None
        if filepath == "":
            filepath = "logs/**/*.xml"
        print("Loading result data from " + filepath)
        xml_files = glob(filepath)
        passed_tests = list()
        failed_tests = list()
        for xml_file in xml_files:
            print("-- " + xml_file + " --")
            doc = xml_parse(xml_file)
            testresultelem = doc.getElementsByTagName("testResult")
            testsuitelem = testresultelem[0].getElementsByTagName("suite")
            for ts in testsuitelem:
                testcaseelem = ts.getElementsByTagName("case")
                for tc in testcaseelem:
                    tcname = self.get_node_text(
                        (tc.getElementsByTagName("name")[0]).childNodes)
                    tcstatus = self.get_node_text(
                        (tc.getElementsByTagName("status")[0]).childNodes)
                    if tcstatus == 'PASSED':
                        passed_tests.append(tcname)
                    else:
                        failed_tests.append(tcname)

        if failed_tests:
            failed_tests = self.transform_and_write_to_file(
                failed_tests, "failed_tests.conf")

        if passed_tests:
            passed_tests = self.transform_and_write_to_file(
                passed_tests, "passed_tests.conf")

        return passed_tests, failed_tests

    def check_if_exists_with_params(self, test_list, test_line, test_params):
        new_test_line = ''.join(sorted(test_line))
        for t in test_list:
            if test_params:
                t1 = ''.join(sorted(t + "," + test_params.strip()))
            else:
                t1 = ''.join(sorted(t))

            if t1 == new_test_line:
                return True, t
        return False, ""

    def process_include_or_filter_exclude_tests(self, filtertype, option,
                                                tests, options):
        if filtertype == 'include' or filtertype == 'exclude':
            if option.startswith('failed') \
                    or option.startswith('passed') \
                    or option.startswith("http://") \
                    or option.startswith("https://"):
                passfail = option.split("=")
                tests_list = []
                if len(passfail) == 2:
                    if passfail[1].startswith("http://") \
                            or passfail[1].startswith("https://"):
                        tp, tf = self.parse_testreport_result_xml(passfail[1])
                    else:
                        tp, tf = self.parse_junit_result_xml(passfail[1])
                elif option.startswith("http://") \
                        or option.startswith("https://"):
                    tp, tf = self.parse_testreport_result_xml(option)
                    tests_list = tp + tf
                else:
                    tp, tf = self.parse_junit_result_xml()
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
                        isexisted, t = self.check_if_exists_with_params(
                            tests, line, options.params)
                        if isexisted:
                            tests.remove(t)
            elif option.startswith("file="):
                filterfile = self.locate_conf_file(option.split("=")[1])
                if filtertype == 'include':
                    tests_list = []
                    if filterfile:
                        for line in filterfile:
                            tests_list.append(line.strip())
                    tests = tests_list
                else:
                    for line in filterfile:
                        isexisted, t = self.check_if_exists_with_params(
                            tests, line.strip(), options.params)
                        if isexisted:
                            tests.remove(t)
            else:  # pattern
                if filtertype == 'include':
                    tests = [i for i in tests if re.search(option, i)]
                else:
                    tests = [i for i in tests if not re.search(option, i)]
        else:
            print("Warning: unknown filtertype given (only include/exclude "
                  "supported)!")
        return tests

    @staticmethod
    def launch_sirius_client(taf_path, urls):
        """
        urls is expected to be in the format,
            172.23.10.1:4000;172.23.10.2:4000;...
        and we are interested in getting the intended port_number for this run.
        So the trigger can call the script with format,
            localhost:<port_num>
        """
        port = (urls.split(";")[0]).split(':')[-1]
        SiriusClient.start_sirius(taf_path, port=port)
