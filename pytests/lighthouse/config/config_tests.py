# -*- coding: utf-8 -*-
"""
UCP Portal Config Tests -- the portal's global configuration singleton
(GET/PUT /api/v1/config).

    - case 165 (configurable half): the telemetry retention period can be
      changed, the new value is what a subsequent read returns, the write
      moves the ETag, and the change is recorded in the audit log.

Scope note: case 165 also asks that the change "takes effect", i.e. that
data beyond the new retention window is actually purged. That half is NOT
covered here and cannot be -- it depends on the portal's background purge
job, whose cadence is not controllable or observable from TAF; the purge
cases themselves (162/163) are marked manual for the same reason. This test
covers the configuration contract only, and says so rather than implying
retention behaviour was verified.

PUT /api/v1/config is a full replacement, not a partial update: all five
fields must be present or the portal answers 422 (confirmed live
2026-07-24). setUp therefore snapshots the whole config and every write
resends it in full, with only telemetryRetentionDays changed; tearDown
restores the snapshot. The config is a single global singleton shared with
every other user of this portal, so leaving it modified would corrupt other
runs.
"""
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.response import UCPResponse
from lighthouse.ucp_helper_methods import (
    create_session,
    get_current_iso8601_timestamp,
    get_latest_audit_event,
    get_session_cookie,
)
from unified_control_plane.constants import ACTION_CONFIG_UPDATED


class ConfigTests(LighthouseBase):

    def setUp(self):
        super(ConfigTests, self).setUp()
        self.config_id = self.input.param("config_id", "default")
        self.retention_days_under_test = self.input.param(
            "retention_days_under_test", 400)
        status, content, _ = create_session(
            self.ucp_client, self.ucp_portal.username,
            self.ucp_portal.password)
        self.assertTrue(status, "Admin login failed: %s" % content)

        status, content, header = self.ucp_client.get_config()
        baseline = UCPResponse(status, content, header)
        self.assertTrue(
            status, "Could not fetch baseline config: %s" % content)
        self._original_config = baseline.json or {}
        self._original_etag = baseline.etag
        self.log.info("Baseline config: %s" % self._original_config)

    def tearDown(self):
        try:
            status, content, header = self.ucp_client.get_config()
            current = UCPResponse(status, content, header)
            if status and current.etag and self._original_config:
                self.ucp_client.update_config(
                    etag=current.etag,
                    telemetry_retention_days=self._original_config.get(
                        'telemetryRetentionDays'),
                    session_idle_timeout_minutes=self._original_config.get(
                        'sessionIdleTimeoutMinutes'),
                    session_absolute_timeout_minutes=(
                        self._original_config.get(
                            'sessionAbsoluteTimeoutMinutes')),
                    global_rate_limit_per_sec=self._original_config.get(
                        'globalRateLimitPerSec'),
                    expensive_rate_limit_per_sec=self._original_config.get(
                        'expensiveRateLimitPerSec'))
        except Exception as e:
            self.log.warning("tearDown: failed to restore config: %s" % e)
        try:
            if get_session_cookie(self.ucp_client):
                self.ucp_client.session_logout()
        except Exception as e:
            self.log.warning("tearDown: logout failed: %s" % e)
        super(ConfigTests, self).tearDown()

    def test_retention_period_is_configurable(self):
        """
        Case 165 (configuration contract): changing telemetryRetentionDays
        is accepted, persisted, reflected in the ETag, and audited.

        The target value is derived from the current one so the test is a
        real change on any portal it runs against -- writing back a value
        that happened to already be set would pass without the portal
        storing anything.
        """
        current_days = self._original_config.get('telemetryRetentionDays')
        target_days = self.retention_days_under_test
        if target_days == current_days:
            target_days = current_days + 1
        self.log.info("Changing telemetryRetentionDays %s -> %s"
                      % (current_days, target_days))

        before_write = get_current_iso8601_timestamp()
        status, content, header = self.ucp_client.update_config(
            etag=self._original_etag,
            telemetry_retention_days=target_days,
            session_idle_timeout_minutes=self._original_config.get(
                'sessionIdleTimeoutMinutes'),
            session_absolute_timeout_minutes=self._original_config.get(
                'sessionAbsoluteTimeoutMinutes'),
            global_rate_limit_per_sec=self._original_config.get(
                'globalRateLimitPerSec'),
            expensive_rate_limit_per_sec=self._original_config.get(
                'expensiveRateLimitPerSec'))
        put_response = UCPResponse(status, content, header)
        self.assertTrue(
            status,
            "Updating telemetryRetentionDays to %s failed (HTTP %s): %s"
            % (target_days, put_response.status_code, content))

        status, content, header = self.ucp_client.get_config()
        after = UCPResponse(status, content, header)
        self.assertTrue(
            status, "Could not read config back after the update: %s"
            % content)
        self.assertEqual(
            after.json.get('telemetryRetentionDays'), target_days,
            "Config read back telemetryRetentionDays=%s after writing %s"
            % (after.json.get('telemetryRetentionDays'), target_days))

        # Every other field must be untouched -- a full-replacement PUT that
        # quietly reset a neighbouring field would otherwise go unnoticed.
        for field in ('sessionIdleTimeoutMinutes',
                      'sessionAbsoluteTimeoutMinutes',
                      'globalRateLimitPerSec',
                      'expensiveRateLimitPerSec'):
            self.assertEqual(
                after.json.get(field), self._original_config.get(field),
                "Field '%s' changed from %s to %s while only "
                "telemetryRetentionDays was being updated"
                % (field, self._original_config.get(field),
                   after.json.get(field)))

        self.assertIsNotNone(
            after.etag, "Config returned no ETag after the update")
        self.assertNotEqual(
            after.etag, self._original_etag,
            "Config ETag did not change after a successful update (was %s); "
            "a stale ETag would let a later conditional write clobber this "
            "change" % self._original_etag)

        event, audit_response = get_latest_audit_event(
            self.ucp_client, action=ACTION_CONFIG_UPDATED,
            actor=self.ucp_portal.username, resource_id=self.config_id,
            since=before_write)
        self.assertTrue(
            audit_response.is_success(),
            "GET /audit failed while looking for the config-update event: %s"
            % audit_response.content)
        self.assertIsNotNone(
            event,
            "No '%s' audit event was recorded for the retention change "
            "(actor=%s resource=%s since=%s)"
            % (ACTION_CONFIG_UPDATED, self.ucp_portal.username,
               self.config_id, before_write))
        self.log.info(
            "PASS -- telemetryRetentionDays configurable (%s -> %s), ETag "
            "moved and audit event %s recorded"
            % (current_days, target_days, event.get('auditEventId')))
