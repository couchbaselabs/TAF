#!/usr/bin/env python
import logging
import logging.config
import os
import re
import sys
import threading
import time
import unittest

from os.path import splitext
from pprint import pprint

sys.path = [".", "lib", "pytests", "pysystests", "couchbase_utils",
            "platform_utils", "platform_utils/ssh_util",
            "connections", "constants", "py_constants"] + sys.path
from TestInput import TestInputParser, TestInputSingleton
from framework_lib.framework import HelperLib, Parameters
from framework_lib.xunit import XUnitTestResult
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient


def parse_args():
    framework_helper = HelperLib()
    options = framework_helper.parse_cmd_line_options()

    tests = list()
    test_params = {
        "ini": options.ini,
        "cluster_name": splitext(os.path.basename(options.ini))[0]
    }

    if options.conf and not options.globalsearch:
        framework_helper.parse_conf_file(options.conf, tests, test_params)
    if options.globalsearch:
        framework_helper.parse_global_conf_file(options.globalsearch,
                                                tests, test_params)
    try:
        if options.include_tests:
            tests = framework_helper.process_include_or_filter_exclude_tests(
                "include", options.include_tests, tests, options)
        if options.exclude_tests:
            tests = framework_helper.process_include_or_filter_exclude_tests(
                "exclude", options.exclude_tests, tests, options)
    except Exception as e:
        print("Failed to get the test xml to include or exclude the tests. Running all the tests instead.")
    if options.testcase:
        tests.append(options.testcase)
    if options.noop:
        print(("---\n" + "\n".join(tests) + "\n---\nTotal=" + str(
            len(tests))))
        sys.exit(0)

    return tests, test_params, options.ini, options.params, options


def create_log_file(log_config_file_name, log_file_name):
    tmpl_log_file = open("logging.conf")
    log_file = open(log_config_file_name, "w")
    for line in tmpl_log_file:
        line = line.replace("@@FILENAME@@", log_file_name.replace('\\', '/'))
        log_file.write(line)
    log_file.close()
    tmpl_log_file.close()


def main():
    names, runtime_test_params, arg_i, arg_p, options = parse_args()
    # get params from command line
    TestInputSingleton.input = TestInputParser.get_test_input(options)
    # ensure command line params get higher priority
    runtime_test_params.update(TestInputSingleton.input.test_params)
    TestInputSingleton.input.test_params = runtime_test_params

    HelperLib.register_signal_handlers()

    print("Global Test input params:")
    pprint(TestInputSingleton.input.test_params)
    import mode
    if options.mode == "java":
        mode.java = True
    elif options.mode == "cli":
        mode.cli = True
    else:
        mode.rest = True
    xunit = XUnitTestResult()
    # Create root logs directory
    taf_path = os.path.dirname(os.path.abspath(sys.argv[0]))
    # Create testrunner logs subdirectory
    str_time = time.strftime("%y-%b-%d_%H-%M-%S", time.localtime())
    root_log_dir = os.path.join(taf_path,
                                "logs%stestrunner-%s" % (os.sep, str_time))
    if not os.path.exists(root_log_dir):
        os.makedirs(root_log_dir)

    if options.launch_java_doc_loader:
        HelperLib.launch_sirius_client(taf_path, options.sirius_url,
                                       process_type="standalone_Java_loader")
    elif options.launch_sirius_process:
        HelperLib.launch_sirius_client(taf_path, options.sirius_url,
                                       process_type="standalone_GoLang_loader")
    elif options.launch_sirius_docker:
        HelperLib.launch_sirius_client(taf_path, options.sirius_url,
                                       process_type="docker_Golang_loader")

    case_number = 0
    if "GROUP" in runtime_test_params:
        print("Only cases in GROUPs '%s' will be executed"
              % runtime_test_params["GROUP"])
    if "EXCLUDE_GROUP" in runtime_test_params:
        print("Cases from GROUPs '%s' will be excluded"
              % runtime_test_params["EXCLUDE_GROUP"])

    tests_to_be_exec = list()
    for name in names:
        argument_split = [a.strip()
                          for a in re.split("[,]?([^,=]+)=", name)[1:]]
        params = dict(zip(argument_split[::2], argument_split[1::2]))
        # Note that if ALL is specified at runtime then tests
        # which have no groups are still run - just being explicit on this

        if "GROUP" in runtime_test_params \
                and "ALL" not in runtime_test_params["GROUP"].split(";"):
            # Params is the .conf file parameters.
            if 'GROUP' not in params:
                # this test is not in any groups, so we do not run it
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

        # Concat params to test name to make tests more readable in xml
        s_params = ''
        # Handles common params passed via '-p' options
        if TestInputSingleton.input.test_params:
            for key, value in TestInputSingleton.input.test_params.items():
                if key and value:
                    s_params += "," + str(key) + "=" + str(value)
        # Handles params wrt individual test definition
        for key, value in params.items():
            if key and value:
                s_params += "," + str(key) + "=" + str(value)

        # If we reach here, the test has passed all criteria to be executed
        # in this run
        case_number += 1
        logs_folder = os.path.join(root_log_dir, "test_%s" % case_number)
        name = name.split(",")[0]
        xunit_test_suite_ref = xunit.get_unit_test_suite(name)
        xunit_test_ref = xunit_test_suite_ref.add_test(
            name, params=s_params, status="not_run")
        xunit.write("%s%sreport-%s"
                    % (os.path.dirname(logs_folder), os.sep, str_time))
        tests_to_be_exec.append({
            "case_number": case_number,
            "params": params,
            "name": name,
            "logs_folder": logs_folder,
            "status": "not_run",
            "xunit_suite_ref": xunit_test_suite_ref,
            "xunit_test_ref": xunit_test_ref,
        })

    TestInputSingleton.input.test_params["no_of_test_identified"] = len(tests_to_be_exec)

    exit_status = 0
    print(f"Total test to be executed: {case_number}")
    for test_to_exec in tests_to_be_exec:
        if Parameters.ABORTED:
            break
        name = test_to_exec["name"]
        params = test_to_exec["params"]
        case_number = test_to_exec["case_number"]
        xunit_suite_ref = test_to_exec["xunit_suite_ref"]
        xunit_test_ref = test_to_exec["xunit_test_ref"]

        start_time = time.time()

        # Reset SDK/Shell connection counters
        RemoteMachineShellConnection.connections = 0
        RemoteMachineShellConnection.disconnections = 0
        SDKClient.sdk_connections = 0
        SDKClient.sdk_disconnections = 0

        params["sirius_url"] = options.sirius_url

        # Note that if ALL is specified at runtime then tests
        # which have no groups are still run - just being explicit on this

        # Create Log Directory
        logs_folder = os.path.join(root_log_dir, "test_%s" % case_number)
        os.mkdir(logs_folder)
        test_log_file = os.path.join(logs_folder, "test.log")
        log_config_filename = "test.logging.conf"
        create_log_file(log_config_filename, test_log_file)
        logging.config.fileConfig(log_config_filename)
        print("Logs will be stored at %s" % logs_folder)
        print("\npython testrunner -i {0} {1} -t {2}{3}\n"
              .format(arg_i or "", ("-p " + arg_p if arg_p else ""),
                      name, xunit_test_ref.params))
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
        try:
            suite = unittest.TestLoader().loadTestsFromName(name)
        except AttributeError as e:
            print("Test %s was not found: %s" % (name, e))
            result = unittest.TextTestRunner(verbosity=2)._makeResult()
            result.errors = [(name, e.message)]
        except SyntaxError as e:
            print("SyntaxError in %s: %s" % (name, e))
            result = unittest.TextTestRunner(verbosity=2)._makeResult()
            result.errors = [(name, e.message)]
        else:
            result = unittest.TextTestRunner(verbosity=2).run(suite)
            if TestInputSingleton.input.param("rerun") \
                    and (result.failures or result.errors):
                print("#" * 60, "\n",
                      "## \tTest Failed: Rerunning it one more time",
                      "\n", "#" * 60)
                print("####### Running test with trace logs enabled #######")
                TestInputSingleton.input.test_params["log_level"] = "debug"
                result = unittest.TextTestRunner(verbosity=2).run(suite)
            if not result:
                for t in threading.enumerate():
                    if t != threading.current_thread():
                        t._Thread__stop()
                result = unittest.TextTestRunner(verbosity=2)._makeResult()
                case_number += 1000
                print("=== TEST WAS STOPPED DUE TO  TIMEOUT ===")
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
            xunit_test_ref.update_results(xunit_suite_ref,
                                          "fail", time_taken,
                                          error_type="membase.error",
                                          error_message=str(errors))
            test_to_exec["status"] = "fail"
            exit_status = 1
        else:
            test_to_exec["status"] = "pass"
            xunit_test_ref.update_results(xunit_suite_ref, "pass", time_taken)
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

    HelperLib.cleanup()
    sys.exit(exit_status)


if __name__ == "__main__":
    assert HelperLib.validate_python_version(sys.version_info)
    main()
