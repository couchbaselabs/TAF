# -*- coding: utf-8 -*-
"""
UCP Portal Usage Report Tests -- report format validation and download
delivery.

    - case 109: an unsupported format value is rejected (422 validation_error;
                confirmed live the portal accepts only "pdf").
    - case 110: a valid report downloads as a real binary artifact with
                Content-Type: application/pdf and a non-empty body.
    - case 111: that download carries a Content-Disposition attachment header.

Report GENERATION creating an audit event (case 114) is already covered by
audit_tests.AuditTests.test_audit_event_created_on_report_generation and is not
duplicated here -- that test asserts the audit side effect, these assert the
response contract.

NOTE: the portal currently rejects 'from'/'to' as unknown query parameters and
accepts only 'format' (required), so the matrix's date-range report cases (106,
108, 115) cannot be exercised until a time range is supported. See
lib/unified_control_plane/ucp_client.py::generate_usage_report.

Only an authenticated caller may generate a report, so setUp opens an admin
session. Response header/body access lives on UCPResponse (.get_header) --
this class holds test methods only.
"""
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.response import UCPResponse
from lighthouse.ucp_helper_methods import (
    create_session,
    get_session_cookie,
)


class UsageReportTests(LighthouseBase):

    def setUp(self):
        super(UsageReportTests, self).setUp()
        self.expected_validation_status = self.input.param(
            "expected_validation_status", 422)
        self.report_format = self.input.param("report_format", "pdf")
        self.invalid_report_format = self.input.param(
            "invalid_report_format", "xml")
        status, content, _ = create_session(
            self.ucp_client, self.ucp_portal.username,
            self.ucp_portal.password)
        self.assertTrue(status, "Admin login failed: %s" % content)

    def tearDown(self):
        try:
            if get_session_cookie(self.ucp_client):
                self.ucp_client.session_logout()
        except Exception as e:
            self.log.warning("tearDown: logout failed: %s" % e)
        super(UsageReportTests, self).tearDown()

    def test_invalid_report_format_rejected(self):
        """
        Case 109: an unsupported report format is rejected as a validation
        error rather than silently falling back to the default format.
        """
        status, content, header = self.ucp_client.generate_usage_report(
            format_type=self.invalid_report_format)
        response = UCPResponse(status, content, header)
        self.assertFalse(
            status, "format=%s: expected %s but the report was generated"
            % (self.invalid_report_format, self.expected_validation_status))
        self.assertEqual(
            response.status_code, self.expected_validation_status,
            "format=%s: expected HTTP %s, got %s: %s"
            % (self.invalid_report_format, self.expected_validation_status,
               response.status_code, content))
        self.log.info("PASS -- report format=%s rejected with %s"
                      % (self.invalid_report_format, response.status_code))

    def test_report_download_headers(self):
        """
        Cases 110 and 111: a valid report downloads as a binary attachment.

        Both cases are asserted from a single request on purpose -- they are
        two headers on the same response, and generating the PDF twice would
        double the cost for no extra coverage.

        The body is checked for content as well as the headers: correct headers
        on an empty body would still be a broken download.
        """
        status, content, header = self.ucp_client.generate_usage_report(
            format_type=self.report_format)
        response = UCPResponse(status, content, header)
        self.assertTrue(
            status, "Report generation failed (HTTP %s): %s"
            % (response.status_code, content))

        content_type = response.get_header('Content-Type')
        self.assertIsNotNone(
            content_type,
            "Report download carried no Content-Type header. Headers were: %s"
            % response.headers)
        self.assertIn(
            'application/pdf', content_type,
            "Report download Content-Type should be application/pdf, got %r"
            % content_type)

        disposition = response.get_header('Content-Disposition')
        self.assertIsNotNone(
            disposition,
            "Report download carried no Content-Disposition header. Headers "
            "were: %s" % response.headers)
        self.assertIn(
            'attachment', disposition.lower(),
            "Report download Content-Disposition should mark the body as an "
            "attachment, got %r" % disposition)

        self.assertTrue(
            content,
            "Report download returned the correct headers but an empty body")
        self.log.info(
            "PASS -- report downloaded as %s, %s, %d bytes"
            % (content_type, disposition, len(content)))
