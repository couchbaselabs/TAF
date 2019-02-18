#!/usr/bin/env python

import base64
import gzip
from httplib import BadStatusLine
import os
import urllib2
import sys
import threading
from os.path import basename, splitext
from pprint import pprint
sys.path = ["lib", "pytests", "pysystests", "couchbase_utils", "platform_utils", "connections"] + sys.path

if sys.hexversion < 0x02060000:
    print "Testrunner requires version 2.6+ of python"
    sys.exit()

import re
import time
import unittest
import logging.config
from threading import Thread, Event
from xunit import XUnitTestResult
from TestInput import TestInputParser, TestInputSingleton
from optparse import OptionParser, OptionGroup
from scripts.collect_server_info import cbcollectRunner, couch_dbinfo_Runner
from scripts.getcoredumps import Getcoredumps, Clearcoredumps
import signal
import shutil

def usage(err=None):
    print """\
Syntax: testrunner [options]

Examples:
  ./testrunner -i tmp/local.ini -t performance.perf.DiskDrainRate
  ./testrunner -i tmp/local.ini -t performance.perf.DiskDrainRate.test_9M
"""
    sys.exit(0)


def parse_args(argv):

    parser = OptionParser()

    parser.add_option("-q", action="store_false", dest="verbose")

    tgroup = OptionGroup(parser, "TestCase/Runlist Options")
    tgroup.add_option("-i", "--ini",
                      dest="ini", help="Path to .ini file containing server information,e.g -i tmp/local.ini")
    tgroup.add_option("-c", "--config", dest="conf",
                      help="Config file name (located in the conf subdirectory), e.g -c py-view.conf")
    tgroup.add_option("-t", "--test",
                      dest="testcase", help="Test name (multiple -t options add more tests) e.g -t performance.perf.DiskDrainRate")
    tgroup.add_option("-m", "--mode",
                      dest="mode", help="Use java for Java SDK, rest for rest APIs.")
    parser.add_option_group(tgroup)

    parser.add_option("-p", "--params",
                      dest="params", help="Optional key=value parameters, comma-separated -p k=v,k2=v2,...",
                      default="")
    parser.add_option("-n", "--noop", action="store_true",
                      help="NO-OP - emit test names, but don't actually run them e.g -n true")
    parser.add_option("-l", "--log-level",
                      dest="loglevel", default="INFO", help="e.g -l debug,info,warning")
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

    test_params['cluster_name'] = splitext(os.path.basename(options.ini))[0]

    if not options.testcase and not options.conf:
        parser.error("please specify a configuration file (-c) or a test case (-t)")
        parser.print_help()
    if options.conf:
        parse_conf_file(options.conf, tests, test_params)
    if options.testcase:
        tests.append(options.testcase)
    if options.noop:
        print("\n".join(tests))
        sys.exit(0)

    return tests, test_params, options.ini, options.params, options


def create_log_file(log_config_file_name, log_file_name, level):
    tmpl_log_file = open("jython.logging.conf")
    log_file = open(log_config_file_name, "w")
    log_file.truncate()
    for line in tmpl_log_file:
        newline = line.replace("@@LEVEL@@", level)
        newline = newline.replace("@@FILENAME@@", log_file_name.replace('\\', '/'))
        log_file.write(newline)
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
    print "filename: %s" % filename
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
            print "prefix: {0}".format(prefix)
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

    # If spec parameter isn't defined, testrunner uses the *.conf filename for
    # the spec value
    if 'spec' not in params:
        params['spec'] = splitext(basename(filename))[0]

    params['conf_file'] = filename


def create_headers(username, password):
    authorization = base64.encodestring('%s:%s' % (username, password))
    return {'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic %s' % authorization,
            'Accept': '*/*'}


def get_server_logs(input, path):
    for server in input.servers:
        print "grabbing diags from ".format(server.ip)
        diag_url = "http://{0}:{1}/diag".format(server.ip, server.port)
        print diag_url

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
            print "downloaded and zipped diags @ : {0}".format("{0}.gz".format(filename))
        except urllib2.URLError:
            print "unable to obtain diags from %s" % diag_url
        except BadStatusLine:
            print "unable to obtain diags from %s" % diag_url
        except Exception as e:
            print "unable to obtain diags from %s %s" % (diag_url, e)

def get_logs_cluster_run(input, path, ns_server_path):
    print "grabbing logs (cluster-run)"
    path = path or "."
    logs_path = ns_server_path + os.sep + "logs"
    try:
        shutil.make_archive(path + os.sep + "logs", 'zip', logs_path)
    except Exception as e:
        print "NOT POSSIBLE TO GRAB LOGS (CLUSTER_RUN)"

def get_cbcollect_info(input, path):
    for server in input.servers:
        print "grabbing cbcollect from {0}".format(server.ip)
        path = path or "."
        try:
            cbcollectRunner(server, path).run()
        except Exception as e:
            print "NOT POSSIBLE TO GRAB CBCOLLECT FROM {0}: {1}".format(server.ip, e)

def get_couch_dbinfo(input, path):
    for server in input.servers:
        print "grabbing dbinfo from {0}".format(server.ip)
        path = path or "."
        try:
            couch_dbinfo_Runner(server, path).run()
        except Exception as e:
            print "NOT POSSIBLE TO GRAB dbinfo FROM {0}: {1}".format(server.ip, e)

def clear_old_core_dumps(_input, path):
    for server in _input.servers:
        path = path or "."
        try:
            Clearcoredumps(server, path).run()
        except Exception as e:
            print "Unable to clear core dumps on {0} : {1}".format(server.ip, e)

def get_core_dumps(_input, path):
    ret = False
    for server in _input.servers:
        print "grabbing core dumps files from {0}".format(server.ip)
        path = path or "."
        try:
            if Getcoredumps(server, path).run():
                ret = True
        except Exception as e:
            print "NOT POSSIBLE TO GRAB CORE DUMPS FROM {0} : {1}".\
                format(server.ip, e)
    return ret


class StoppableThreadWithResult(Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(StoppableThreadWithResult, self).__init__(group=group, target=target,
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

    BEFORE_SUITE = "suite_setUp"
    AFTER_SUITE = "suite_tearDown"
    names, runtime_test_params, arg_i, arg_p, options = parse_args(sys.argv)
    # get params from command line
    TestInputSingleton.input = TestInputParser.get_test_input(sys.argv)
    # ensure command line params get higher priority
    runtime_test_params.update(TestInputSingleton.input.test_params)
    TestInputSingleton.input.test_params = runtime_test_params
    print "Global Test input params:"
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
    root_log_dir = os.path.join(abs_path, "logs{0}testrunner-{1}".format(os.sep, str_time))
    if not os.path.exists(root_log_dir):
        os.makedirs(root_log_dir)

    results = []
    case_number = 1
    if "GROUP" in runtime_test_params:
        print "Only cases in GROUPs '{0}' will be executed".format(runtime_test_params["GROUP"])
    if "EXCLUDE_GROUP" in runtime_test_params:
        print "Cases from GROUPs '{0}' will be excluded".format(runtime_test_params["EXCLUDE_GROUP"])

    for name in names:
        start_time = time.time()
        argument_split = [a.strip() for a in re.split("[,]?([^,=]+)=", name)[1:]]
        params = dict(zip(argument_split[::2], argument_split[1::2]))

        # Note that if ALL is specified at runtime then tests which have no groups are still run - just being
        # explicit on this

        if "GROUP" in runtime_test_params and "ALL" not in runtime_test_params["GROUP"].split(";"):
            if 'GROUP' not in params:         # params is the .conf file parameters.
                # this test is not in any groups so we do not run it
                print "test '{0}' skipped, a group was requested and this is not any groups".format(name)
                continue

            # there is a group for this test case, if that group is not specified at run time then do not run it
            elif len( set(runtime_test_params["GROUP"].split(";")) & set(params["GROUP"].split(";")) ) == 0:
                print "test '{0}' skipped, is not in the requested group".format(name)
                continue
            else:
                pass # the test was in requested group, will run it

        elif "EXCLUDE_GROUP" in runtime_test_params:
            if 'GROUP' in params and \
                len(set(runtime_test_params["EXCLUDE_GROUP"].split(";")) & set(params["GROUP"].split(";"))) > 0:
                    print "test '{0}' skipped, is in an excluded group".format(name)
                    continue



        # Create Log Directory
        logs_folder = os.path.join(root_log_dir, "test_%s" % case_number)
        os.mkdir(logs_folder)
        test_log_file = os.path.join(logs_folder, "test.log")
        log_config_filename = r'{0}'.format(os.path.join(logs_folder, "test.logging.conf"))
        create_log_file(log_config_filename, test_log_file, options.loglevel)
        logging.config.fileConfig(log_config_filename)
        print "Logs will be stored at {0}".format(logs_folder)
        print "\n.{3}testrunner -i {0} {1} -t {2}\n"\
              .format(arg_i or "", arg_p or "", name, os.sep)
        name = name.split(",")[0]

        # Update the test params for each test
        TestInputSingleton.input.test_params = params
        TestInputSingleton.input.test_params.update(runtime_test_params)
        TestInputSingleton.input.test_params["case_number"] = case_number
        TestInputSingleton.input.test_params["logs_folder"] = logs_folder
        print "Test Input params:"
        print(TestInputSingleton.input.test_params)
        if "get-coredumps" in TestInputSingleton.input.test_params:
            if TestInputSingleton.input.param("get-coredumps", True):
                clear_old_core_dumps(TestInputSingleton.input, logs_folder)
        if case_number == 1:
            before_suite_name = "%s.%s" % (name[:name.rfind('.')], BEFORE_SUITE)
            try:
                print "Run before suite setup for %s" % name
                suite = unittest.TestLoader().loadTestsFromName(before_suite_name)
                result = unittest.TextTestRunner(verbosity=2).run(suite)
                if "get-coredumps" in TestInputSingleton.input.test_params:
                    if TestInputSingleton.input.param("get-coredumps", True):
                        if get_core_dumps(TestInputSingleton.input, logs_folder):
                            result = unittest.TextTestRunner(verbosity=2)._makeResult()
                            result.errors = [(name, "Failing test : new core dump(s) "
                                             "were found and collected."
                                             " Check testrunner logs folder.")]
                            print("FAIL: New core dump(s) was found and collected")
            except AttributeError as ex:
                pass
        try:
            suite = unittest.TestLoader().loadTestsFromName(name)
        except AttributeError, e:
            print "Test {0} was not found: {1}".format(name, e)
            result = unittest.TextTestRunner(verbosity=2)._makeResult()
            result.errors = [(name, e.message)]
        except SyntaxError, e:
            print "SyntaxError in {0}: {1}".format(name, e)
            result = unittest.TextTestRunner(verbosity=2)._makeResult()
            result.errors = [(name, e.message)]
        else:
            result = unittest.TextTestRunner(verbosity=2).run(suite)
#             test_timeout = TestInputSingleton.input.param("test_timeout", None)
#             t = StoppableThreadWithResult(target=unittest.TextTestRunner(verbosity=2).run,
#                name="test_thread",
#                args=(suite))
#             t.start()
#             result = t.join(timeout=test_timeout)
            if "get-coredumps" in TestInputSingleton.input.test_params:
                if TestInputSingleton.input.param("get-coredumps", True):
                    if get_core_dumps(TestInputSingleton.input, logs_folder):
                        result = unittest.TextTestRunner(verbosity=2)._makeResult()
                        result.errors = [(name, "Failing test : new core dump(s) "
                                             "were found and collected."
                                             " Check testrunner logs folder.")]
                        print("FAIL: New core dump(s) was found and collected")
            if not result:
                for t in threading.enumerate():
                    if t != threading.current_thread():
                        t._Thread__stop()
                result = unittest.TextTestRunner(verbosity=2)._makeResult()
                case_number += 1000
                print ("========TEST WAS STOPPED DUE TO  TIMEOUT=========")
                result.errors = [(name, "Test was stopped due to timeout")]
        time_taken = time.time() - start_time

        # Concat params to test name
        # To make tests more readable
        params = ''
        if TestInputSingleton.input.test_params:
            for key, value in TestInputSingleton.input.test_params.items():
                if key and value:
                    params += "," + str(key) + ":" + str(value)

        if result.failures or result.errors:
            # Immediately get the server logs, if
            # the test has failed or has errors
            if "get-logs" in TestInputSingleton.input.test_params:
                get_server_logs(TestInputSingleton.input, logs_folder)

            if "get-logs-cluster-run" in TestInputSingleton.input.test_params:
                if TestInputSingleton.input.param("get-logs-cluster-run", True):
                    # Generate path to ns_server directory
                    ns_server_path = os.path.normpath(abs_path + os.sep + os.pardir + os.sep + "ns_server")
                    get_logs_cluster_run(TestInputSingleton.input, logs_folder, ns_server_path)

            if "get-cbcollect-info" in TestInputSingleton.input.test_params:
                if TestInputSingleton.input.param("get-cbcollect-info", True):
                    get_cbcollect_info(TestInputSingleton.input, logs_folder)

            if "get-couch-dbinfo" in TestInputSingleton.input.test_params and \
                TestInputSingleton.input.param("get-couch-dbinfo", True):
                    get_couch_dbinfo(TestInputSingleton.input, logs_folder)

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
            results.append({"result": "pass", "name": name, "time": time_taken})
        xunit.write("{0}{2}report-{1}".format(os.path.dirname(logs_folder), str_time, os.sep))
        xunit.print_summary()
        print "testrunner logs, diags and results are available under {0}".format(logs_folder)
        case_number += 1
        if (result.failures or result.errors) and \
                TestInputSingleton.input.param("stop-on-failure", False):
            print "test fails, all of the following tests will be skipped!!!"
            break

    after_suite_name = "%s.%s" % (name[:name.rfind('.')], AFTER_SUITE)
    try:
        print "Run after suite setup for %s" % name
        suite = unittest.TestLoader().loadTestsFromName(after_suite_name)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    except AttributeError as ex:
        pass
    except Exception as e:
        print e.message
    if "makefile" in TestInputSingleton.input.test_params:
        # print out fail for those tests which failed and do sys.exit() error code
        fail_count = 0
        for result in results:
            if result["result"] == "fail":
                print result["name"], " fail "
                fail_count += 1
            else:
                print result["name"], " pass"
        if fail_count > 0:
            sys.exit(1)

def testfunc():
    pass

if __name__ == "__main__":
    main()
