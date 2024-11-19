import xml.dom.minidom

# a junit compatible xml example
#<?xml version="1.0" encoding="UTF-8"?>
#<testsuite name="nosetests" tests="1" errors="1" failures="0" skip="0">
#    <testcase classname="path_to_test_suite.TestSomething"
#              name="path_to_test_suite.TestSomething.test_it" time="0">
#        <error type="exceptions.TypeError">
#        Traceback (most recent call last):
#        ...
#        TypeError: oops, wrong type
#        </error>
#    </testcase>
#</testsuite>


#
# XUnitTestCase has name , time and error is a XUnitTestCase
class XUnitTestCase(object):
    def __init__(self):
        self.name = ""
        self.time = 0
        self.error = None
        self.params = ''
        self.run_status = "skip"

    def update_results(self, suite, result=None, time_taken=None,
                       error_type=None, error_message=None):
        suite.skips -= 1
        if result is not None:
            self.run_status = result
            if result == "fail":
                self.error = XUnitTestCaseError()
                self.error.type = error_type
                self.error.message = error_message
                suite.errors += 1
                suite.failures += 1
            elif result == "pass":
                suite.passed += 1
        if time_taken is not None:
            self.time = time_taken


#
# XUnitTestCaseError has type and message
#
class XUnitTestCaseError(object):
    def __init__(self):
        self.type = ""
        self.message = ""


#
# XUnitTestSuite has name , time , list of XUnitTestCase objects
# errors : number of errors
# failures : number of failures
# skips : number of skipped tests

#

class XUnitTestResult(object):

    def __init__(self):
        self.suites = []

    def get_unit_test_suite(self, name):
        class_name = name[:name.rfind(".")]
        # If params are passed to the test
        if "," in name:
            class_name = name

        for suite in self.suites:
            if suite.name == class_name:
                return suite

        # No existing suite name matched, so creating one
        suite = XUnitTestSuite()
        suite.name = class_name
        self.suites.append(suite)
        return suite

    def to_xml(self, suite):
        doc = xml.dom.minidom.Document()
        testsuite = doc.createElement('testsuite')
        #<testsuite name="nosetests" tests="1" errors="1" failures="0" skip="0">
        testsuite.setAttribute('name', suite.name)
        testsuite.setAttribute('errors', str(suite.errors))
        testsuite.setAttribute('failures', str(suite.failures))
        testsuite.setAttribute('errors', str(suite.errors))
        testsuite.setAttribute('tests', str(len(suite.tests)))
        testsuite.setAttribute('time', str(suite.time))
        # 'skip' means the test is supposed to run but 'not_run' due to job abort
        testsuite.setAttribute('skip', str(suite.skips))
        for testobject in suite.tests:
            testcase = doc.createElement('testcase')
            full_name = testobject.name+testobject.params
            testcase.setAttribute('name', full_name)
            testcase.setAttribute('time', str(testobject.time))
            if testobject.run_status == "skip":
                skipped_element = doc.createElement("skipped")
                testcase.appendChild(skipped_element)
            else:
                testcase.setAttribute("result", testobject.run_status)
            if testobject.error:
                error = doc.createElement('error')
                error.setAttribute('type', testobject.error.type)
                if testobject.error.message:
                    message = doc.createTextNode(testobject.error.message)
                    error.appendChild(message)
                testcase.appendChild(error)
            testsuite.appendChild(testcase)
        doc.appendChild(testsuite)
        return doc.toprettyxml()

    def write(self, prefix, params=''):
        for suite in self.suites:
            name = suite.name
            # If test_params are passed
            if "," in name:
                name = name[:name.find(",")]

            report_xml_file = open("{0}-{1}.xml".format(prefix, name), 'w')
            report_xml_file.write(self.to_xml(suite))
            report_xml_file.close()

    def print_summary(self):
        print(f"Summary :: ")
        for suite in self.suites:
            num_passed = 0
            errors = []
            num_not_run = 0
            for test in suite.tests:
                if test.error:
                    errors.append(test.name)
                elif test.run_status == "skip":
                    num_not_run += 1
                else:
                    num_passed += 1
            print(f"  - Suite {suite.name}, pass {num_passed}, "
                  f"fail {len(errors)}, scheduled {num_not_run}")
            if errors:
                print("    Failures:")
                for error in errors:
                    print(f"     * {error}")


class XUnitTestSuite(object):
    def __init__(self):
        self.name = ""
        self.time = 0
        self.tests = []
        self.errors = 0
        self.failures = 0
        self.skips = 0
        self.passed = 0

    # create a new XUnitTestCase and update the errors/failures/skips count
    def add_test(self, name, time=0, errorType=None, errorMessage=None, status='pass', params=''):
        # create a test_case and add it to this suite
        # todo: handle 'skip' or 'setup_failure' or other
        # status codes that testrunner might pass to this function
        test = XUnitTestCase()
        test.name = name
        test.time = time
        test.params = params
        test.run_status = status
        self.tests.append(test)
        if status == 'fail':
            error = XUnitTestCaseError()
            error.type = errorType
            error.message = errorMessage
            test.error = error
            # Incr. counters
            self.failures += 1
            self.errors += 1
        elif status == 'skip':
            self.skips += 1
        self.time += time
        return test
