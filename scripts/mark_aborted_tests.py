#!/usr/bin/env python
"""
Rewrite never-run tests in TAF's JUnit reports as errored tests.

testrunner.py writes its report file up-front, with every scheduled test
recorded as <skipped/>, and rewrites the file as each test finishes
(testrunner.py -> xunit.write). A run that dies mid-suite - OOM-killed by
the kernel, SIGKILLed on the slave, or crashed before its first test -
therefore leaves behind a complete-looking report in which the tests that
never ran are indistinguishable from tests that were deliberately skipped
(GROUP not satisfied, stop-on-failure, ...).

Jenkins counts those as skips rather than failures, so the build stays
SUCCESS and greenboard, which derives its pass ratio from pass+fail only,
shows the suite as "PASS 0/0" instead of red - a run that produced no
results at all looks indistinguishable from a healthy one.

This script converts those <skipped/> testcases into errored ones, so the
lost tests stay visible in the report and rerun_jobs.py's
should_rerun_tests() (which triggers on testcase['error']) picks them up
for a retry.

Only call this on a run that is already known to have died - executor
scripts gate it on testrunner.py exiting on a signal, or exiting non-zero
without a single test reaching a verdict. Calling it on a healthy run
would turn legitimately skipped tests into failures.
"""

import argparse
import glob
import os
import xml.dom.minidom

DEFAULT_REASON = "testrunner.py exited before this test ran to completion"
ERROR_TYPE = "testrunner.aborted"


def parse_args():
    """
    Parse command line arguments
    :return: Parsed command line arguments
    :rtype: argparse.Namespace
    """
    argument_parser = argparse.ArgumentParser(
        description="Mark never-run tests in JUnit reports as errored")
    argument_parser.add_argument(
        "patterns", metavar="<xml file or glob>", type=str, nargs="+",
        help="Report xml paths, or glob patterns matching them "
             "(e.g. 'logs/*/*.xml'). Quote the pattern so this script "
             "expands it rather than the shell")
    argument_parser.add_argument(
        "--reason", type=str, default=DEFAULT_REASON,
        help="Error message recorded against each never-run test")
    return argument_parser.parse_args()


def bump_count(element, attribute, delta):
    """
    Add delta to an integer xml attribute, clamped at zero. A missing or
    non-numeric attribute counts as zero
    :param element: Element whose attribute is updated
    :type element: xml.dom.minidom.Element
    :param attribute: Name of the attribute to update
    :type attribute: str
    :param delta: Amount to add (may be negative)
    :type delta: int
    :return: Nothing
    :rtype: None
    """
    try:
        current = int(element.getAttribute(attribute) or 0)
    except ValueError:
        current = 0
    element.setAttribute(attribute, str(max(current + delta, 0)))


def mark_testcase(doc, testcase, reason):
    """
    Turn one <skipped/> testcase into an errored one. The skipped marker
    is dropped, result="fail" is set (what merge_reports.py reads to
    classify the test) and an <error> child carrying the reason is added
    :param doc: Document the testcase belongs to, used to create nodes
    :type doc: xml.dom.minidom.Document
    :param testcase: The testcase element to rewrite
    :type testcase: xml.dom.minidom.Element
    :param reason: Error message to record against the test
    :type reason: str
    :return: Nothing
    :rtype: None
    """
    for skipped in testcase.getElementsByTagName("skipped"):
        testcase.removeChild(skipped)
        skipped.unlink()
    testcase.setAttribute("result", "fail")
    error = doc.createElement("error")
    error.setAttribute("type", ERROR_TYPE)
    error.appendChild(doc.createTextNode(reason))
    testcase.appendChild(error)


def mark_report(xml_file, reason):
    """
    Rewrite every never-run test in one report file, in place. The file
    is left untouched if it holds no never-run tests
    :param xml_file: Path of the report xml to rewrite
    :type xml_file: str
    :param reason: Error message to record against each never-run test
    :type reason: str
    :return: Number of tests marked in this file
    :rtype: int
    """
    doc = xml.dom.minidom.parse(xml_file)
    marked = 0
    for testsuite in doc.getElementsByTagName("testsuite"):
        suite_marked = 0
        for testcase in testsuite.getElementsByTagName("testcase"):
            if not testcase.getElementsByTagName("skipped"):
                continue
            mark_testcase(doc, testcase, reason)
            suite_marked += 1
        if suite_marked:
            bump_count(testsuite, "errors", suite_marked)
            bump_count(testsuite, "failures", suite_marked)
            # 'skip' is what xunit.py writes on the testsuite element
            bump_count(testsuite, "skip", -suite_marked)
            marked += suite_marked
    if marked:
        # Written to a sibling temp file and renamed into place, never
        # straight over the original: open(..., "w") truncates up front,
        # so a write that dies part-way - the disk filling up is a real
        # condition on these slaves, which is why this script reclaims
        # space before every run - would leave an empty report behind
        # and destroy the verdicts of the tests that did finish before
        # the kill. os.replace() is atomic within a directory, so the
        # report is either the old one or the fully rewritten one.
        #
        # Bytes with an explicit utf-8 declaration, not text: writing
        # text would encode using the slave's locale, which is commonly
        # POSIX/ascii and cannot represent error text captured from the
        # cluster, and that failure would land mid-write.
        #
        # Not toprettyxml(): re-prettifying an already pretty-printed
        # document keeps adding blank lines on every pass. Consumers
        # (merge_reports.py, Jenkins' junit publisher) parse the xml,
        # they do not read its layout.
        tmp_file = f"{xml_file}.tmp"
        try:
            with open(tmp_file, "wb") as report_file:
                report_file.write(doc.toxml(encoding="utf-8"))
            os.replace(tmp_file, xml_file)
        except Exception:
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            raise
    return marked


def mark_reports(patterns, reason):
    """
    Rewrite the never-run tests of every report matching the patterns.
    Best-effort by design: this runs on an already-failing path, so a
    report that cannot be parsed is reported and skipped rather than
    replacing the real failure with this script's own
    :param patterns: Report xml paths or glob patterns matching them
    :type patterns: list of str
    :param reason: Error message to record against each never-run test
    :type reason: str
    :return: Total number of tests marked across all reports
    :rtype: int
    """
    xml_files = []
    for pattern in patterns:
        xml_files.extend(sorted(glob.glob(pattern)))
    if not xml_files:
        print(f"No report xml matched {', '.join(patterns)} "
              f"- nothing to mark")
        return 0

    total = 0
    for xml_file in xml_files:
        try:
            marked = mark_report(xml_file, reason)
        except Exception as e:
            print(f"Could not mark {xml_file}: {e}")
            continue
        if marked:
            print(f"Marked {marked} never-run test(s) as errored "
                  f"in {xml_file}")
        total += marked
    print(f"Total never-run tests marked: {total}")
    return total


def main():
    """
    Entry point. Always exits zero - the caller has already decided the
    build is a failure, and this bookkeeping must not change how
    :return: Nothing
    :rtype: None
    """
    args = parse_args()
    mark_reports(args.patterns, args.reason)


if __name__ == "__main__":
    main()
