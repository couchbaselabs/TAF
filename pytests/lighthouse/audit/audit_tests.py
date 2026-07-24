# -*- coding: utf-8 -*-
"""
UCP Portal Audit Log Tests.

Validates that portal-mutating actions are recorded as audit events, that
the audit log's actor/action/time-range filters work, that the default
(unspecified-sort) order surfaces the newest event first, and that audit
records cannot be mutated or deleted once written (append-only).

Per-action coverage (verified via list_audit_events' own actor/action
query filters). Audit event shape, field names, and action-string enum
were confirmed 2026-07-24 against the live portal (GET /api/v1/audit):
each event is {auditEventId, actor, action, resourceType, resourceId,
timestamp, details}, and the action query param rejects anything outside
"login, logout, password_changed, user_created, user_updated,
user_deleted, entitlement_updated, entitlement_imported,
cluster_metadata_updated, config_updated, usage_report_generated,
activity_log_exported, ldap_config_updated" with a 422 -- all 9 ACTION_*
constants used below matched exactly. Note "entitlement_imported" is a
real action the product already tracks, confirming the second
entitlement-mutation path referenced in the lighthouse-ucp-entitlements
memory as previously unconfirmed.
    - login / logout
    - user create / update / delete
    - role change (roles is the only mutable field on update_user in
      current builds -- 'enabled' is rejected as an unexpected property,
      see LIGHTHOUSE_CONTEXT.md Gotcha #13 -- so this exercises a second,
      independent role transition rather than duplicating the generic
      user-update case)
    - cluster metadata update (requires the cluster to first be registered
      on the portal via a forced collector report, reusing the same
      diag/eval + wait_for_cluster_on_portal flow as collector_tests.py --
      note the reporting endpoint is always the domain
      'couchbase.fleetmanager.internal', never the portal's raw IP)
    - entitlement update
    - config update
    - usage report generation

system_viewer being forbidden from reading the audit log (case 78) is
already covered by user_rbac_tests.SystemViewerRbacTests and is not
duplicated here.

Config and entitlements are single global-profile resources shared by the
whole suite, so (matching entitlement_tests.py's existing convention)
both are snapshotted in setUp and unconditionally restored in tearDown,
regardless of which test ran. Response parsing lives on UCPResponse;
audit lookup/raw-request plumbing lives in ucp_helper_methods -- this
class holds test methods only.

This portal is a shared QE lab instance (other engineers/test runs hit it
concurrently), so every "audit event created on X" test captures a
`since` timestamp immediately before performing its mutation and passes
it plus the mutated resource's own id (userId / cluster UUID / the
literal "default" profileId-or-configId) to get_latest_audit_event --
trusting an actor+action-only "latest" lookup would be a race on a busy
portal (someone else's matching event could land in between and get
mistaken for yours, or push yours off the page). Session/report actions
carry no resourceId (confirmed empty string live), so those rely on the
`since` bound alone.
"""
from datetime import datetime, timedelta

from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.response import UCPResponse
from lighthouse.ucp_helper_methods import (
    create_session,
    get_session_cookie,
    safe_delete_user,
    get_user_with_etag,
    build_subscription_payload,
    get_latest_audit_event,
    raw_request,
    get_current_iso8601_timestamp,
    format_iso8601_timestamp,
    parse_iso8601_timestamp,
)
from lighthouse.collector_helper_methods import (
    get_cb_cluster_uuid,
    wait_for_cluster_on_portal,
    set_lighthouse_ns_config_via_diag_eval,
    set_lighthouse_interval_via_diag_eval,
    LIGHTHOUSE_DEFAULT_PORTAL_PORT,
)
from unified_control_plane.constants import (
    ROLE_SYSTEM_ADMIN,
    ROLE_SYSTEM_VIEWER,
    ACTION_LOGIN,
    ACTION_LOGOUT,
    ACTION_USER_CREATED,
    ACTION_USER_UPDATED,
    ACTION_USER_DELETED,
    ACTION_ENTITLEMENT_UPDATED,
    ACTION_CLUSTER_METADATA_UPDATED,
    ACTION_CONFIG_UPDATED,
    ACTION_USAGE_REPORT_GENERATED,
)


class AuditTests(LighthouseBase):

    def setUp(self):
        super(AuditTests, self).setUp()
        self.audit_user_id = self.input.param(
            "audit_user_id", "lh_audit@example.com")
        self.audit_temp_password = self.input.param(
            "audit_temp_password", "TempAudit#2026xyz")
        self._login_since = get_current_iso8601_timestamp()
        status, content, _ = create_session(
            self.ucp_client, self.ucp_portal.username,
            self.ucp_portal.password)
        self.assertTrue(status, "Admin login failed: %s" % content)
        safe_delete_user(self.ucp_client, self.audit_user_id)

        status, content, header = self.ucp_client.get_config()
        self.assertTrue(status, "Could not fetch baseline config: %s" % content)
        baseline_config = UCPResponse(status, content, header)
        self._original_config = baseline_config.json

        status, content, header = self.ucp_client.get_entitlements()
        self.assertTrue(
            status, "Could not fetch baseline entitlements: %s" % content)
        baseline_entitlements = UCPResponse(status, content, header)
        self._original_subscriptions = (
            baseline_entitlements.json.get('subscriptions', [])
            if baseline_entitlements.json else [])

    def tearDown(self):
        try:
            if not get_session_cookie(self.ucp_client):
                create_session(self.ucp_client, self.ucp_portal.username,
                               self.ucp_portal.password)
            safe_delete_user(self.ucp_client, self.audit_user_id)
        except Exception as e:
            self.log.warning("tearDown: audit user cleanup failed: %s" % e)
        try:
            status, content, header = self.ucp_client.get_config()
            if status:
                current = UCPResponse(status, content, header)
                # PUT /config is a full replacement, not a partial update --
                # every field must be resent, not just the one we changed.
                self.ucp_client.update_config(
                    etag=current.etag,
                    telemetry_retention_days=self._original_config.get(
                        'telemetryRetentionDays'),
                    session_idle_timeout_minutes=self._original_config.get(
                        'sessionIdleTimeoutMinutes'),
                    session_absolute_timeout_minutes=self._original_config.get(
                        'sessionAbsoluteTimeoutMinutes'),
                    global_rate_limit_per_sec=self._original_config.get(
                        'globalRateLimitPerSec'),
                    expensive_rate_limit_per_sec=self._original_config.get(
                        'expensiveRateLimitPerSec'))
        except Exception as e:
            self.log.warning("tearDown: config restore failed: %s" % e)
        try:
            status, content, header = self.ucp_client.get_entitlements()
            if status:
                current = UCPResponse(status, content, header)
                if current.etag:
                    self.ucp_client.update_entitlements(
                        etag=current.etag,
                        subscriptions=self._original_subscriptions)
        except Exception as e:
            self.log.warning("tearDown: entitlements restore failed: %s" % e)
        try:
            if get_session_cookie(self.ucp_client):
                self.ucp_client.session_logout()
        except Exception as e:
            self.log.warning("tearDown: logout failed: %s" % e)
        super(AuditTests, self).tearDown()

    # ==================== Session actions ====================

    def test_audit_event_created_on_login(self):
        """setUp's own admin login is the event under test here."""
        event, response = get_latest_audit_event(
            self.ucp_client, action=ACTION_LOGIN,
            actor=self.ucp_portal.username, since=self._login_since)
        self.assertTrue(
            response.is_success(), "GET /audit failed: %s" % response.content)
        self.assertIsNotNone(
            event, "No audit event found for action=%s actor=%s at/after "
            "%s" % (ACTION_LOGIN, self.ucp_portal.username,
                   self._login_since))
        self.log.info("PASS -- login recorded an audit event: %s" % event)

    def test_audit_event_created_on_logout(self):
        since = get_current_iso8601_timestamp()
        status, content, _ = self.ucp_client.session_logout()
        self.assertTrue(status, "Logout failed: %s" % content)
        # Re-login so we have a session to read the audit log with.
        status, content, _ = create_session(
            self.ucp_client, self.ucp_portal.username,
            self.ucp_portal.password)
        self.assertTrue(status, "Re-login after logout failed: %s" % content)

        event, response = get_latest_audit_event(
            self.ucp_client, action=ACTION_LOGOUT,
            actor=self.ucp_portal.username, since=since)
        self.assertTrue(
            response.is_success(), "GET /audit failed: %s" % response.content)
        self.assertIsNotNone(
            event, "No audit event found for action=%s actor=%s at/after %s"
            % (ACTION_LOGOUT, self.ucp_portal.username, since))
        self.log.info("PASS -- logout recorded an audit event: %s" % event)

    # ==================== User management actions ====================

    def test_audit_event_created_on_user_create(self):
        since = get_current_iso8601_timestamp()
        status, content, _ = self.ucp_client.create_user(
            self.audit_user_id, roles=[ROLE_SYSTEM_VIEWER],
            auth_type='local', password=self.audit_temp_password)
        self.assertTrue(status, "create_user failed: %s" % content)

        event, response = get_latest_audit_event(
            self.ucp_client, action=ACTION_USER_CREATED,
            actor=self.ucp_portal.username, resource_id=self.audit_user_id,
            since=since)
        self.assertTrue(
            response.is_success(), "GET /audit failed: %s" % response.content)
        self.assertIsNotNone(
            event, "No audit event found for action=%s actor=%s "
            "resourceId=%s" % (ACTION_USER_CREATED, self.ucp_portal.username,
                              self.audit_user_id))
        self.log.info("PASS -- user creation recorded an audit event: %s"
                      % event)

    def test_audit_event_created_on_user_update(self):
        status, content, _ = self.ucp_client.create_user(
            self.audit_user_id, roles=[ROLE_SYSTEM_VIEWER],
            auth_type='local', password=self.audit_temp_password)
        self.assertTrue(status, "create_user failed: %s" % content)
        user, etag = get_user_with_etag(self.ucp_client, self.audit_user_id)
        self.assertIsNotNone(
            user, "Could not fetch newly created user '%s'"
            % self.audit_user_id)

        since = get_current_iso8601_timestamp()
        status, content, _ = self.ucp_client.update_user(
            self.audit_user_id, etag, roles=[ROLE_SYSTEM_ADMIN])
        self.assertTrue(status, "update_user failed: %s" % content)

        event, response = get_latest_audit_event(
            self.ucp_client, action=ACTION_USER_UPDATED,
            actor=self.ucp_portal.username, resource_id=self.audit_user_id,
            since=since)
        self.assertTrue(
            response.is_success(), "GET /audit failed: %s" % response.content)
        self.assertIsNotNone(
            event, "No audit event found for action=%s actor=%s "
            "resourceId=%s" % (ACTION_USER_UPDATED, self.ucp_portal.username,
                              self.audit_user_id))
        self.log.info("PASS -- user update recorded an audit event: %s"
                      % event)

    def test_audit_event_created_on_user_delete(self):
        status, content, _ = self.ucp_client.create_user(
            self.audit_user_id, roles=[ROLE_SYSTEM_VIEWER],
            auth_type='local', password=self.audit_temp_password)
        self.assertTrue(status, "create_user failed: %s" % content)

        since = get_current_iso8601_timestamp()
        status, content, _ = self.ucp_client.delete_user(self.audit_user_id)
        self.assertTrue(status, "delete_user failed: %s" % content)

        event, response = get_latest_audit_event(
            self.ucp_client, action=ACTION_USER_DELETED,
            actor=self.ucp_portal.username, resource_id=self.audit_user_id,
            since=since)
        self.assertTrue(
            response.is_success(), "GET /audit failed: %s" % response.content)
        self.assertIsNotNone(
            event, "No audit event found for action=%s actor=%s "
            "resourceId=%s" % (ACTION_USER_DELETED, self.ucp_portal.username,
                              self.audit_user_id))
        self.log.info("PASS -- user deletion recorded an audit event: %s"
                      % event)

    def test_audit_event_created_on_role_change(self):
        """
        A second, independent role transition (admin -> viewer) on a
        freshly created user, kept separate from
        test_audit_event_created_on_user_update so the two do not share
        state -- see the module docstring for why both go through the
        same roles-only mutation.
        """
        status, content, _ = self.ucp_client.create_user(
            self.audit_user_id, roles=[ROLE_SYSTEM_ADMIN],
            auth_type='local', password=self.audit_temp_password)
        self.assertTrue(status, "create_user failed: %s" % content)
        user, etag = get_user_with_etag(self.ucp_client, self.audit_user_id)
        self.assertIsNotNone(
            user, "Could not fetch newly created user '%s'"
            % self.audit_user_id)
        self.assertEqual(user.get('roles'), [ROLE_SYSTEM_ADMIN])

        since = get_current_iso8601_timestamp()
        status, content, _ = self.ucp_client.update_user(
            self.audit_user_id, etag, roles=[ROLE_SYSTEM_VIEWER])
        self.assertTrue(status, "role-change update_user failed: %s" % content)

        event, response = get_latest_audit_event(
            self.ucp_client, action=ACTION_USER_UPDATED,
            actor=self.ucp_portal.username, resource_id=self.audit_user_id,
            since=since)
        self.assertTrue(
            response.is_success(), "GET /audit failed: %s" % response.content)
        self.assertIsNotNone(
            event, "No audit event found for role-change action=%s actor=%s "
            "resourceId=%s" % (ACTION_USER_UPDATED, self.ucp_portal.username,
                              self.audit_user_id))
        self.log.info("PASS -- role change recorded an audit event: %s"
                      % event)

    # ==================== Cluster / entitlement / config actions =======

    def test_audit_event_created_on_cluster_metadata_update(self):
        portal_domain = 'couchbase.fleetmanager.internal'
        cluster_uuid = get_cb_cluster_uuid(self.cluster.master)
        self.assertIsNotNone(
            cluster_uuid, "Could not resolve cluster UUID from /pools")

        diag_status, diag_content = set_lighthouse_ns_config_via_diag_eval(
            self.cluster.master,
            reporting_endpoint=portal_domain,
            reporting_port=LIGHTHOUSE_DEFAULT_PORTAL_PORT,
            reporting_interval_hours=1 / 3600.0)
        self.assertTrue(
            diag_status, "Could not force an immediate report: %s"
            % diag_content)
        self.sleep(10, "waiting for initial report to fire")
        set_lighthouse_interval_via_diag_eval(self.cluster.master, 2)

        appeared = wait_for_cluster_on_portal(
            self.ucp_client, cluster_uuid, timeout=60, poll_interval=5)
        self.assertTrue(
            appeared, "Cluster %s never appeared on the portal" % cluster_uuid)

        status, content, header = self.ucp_client.get_cluster(cluster_uuid)
        self.assertTrue(status, "get_cluster failed: %s" % content)
        current = UCPResponse(status, content, header)
        self.assertIsNotNone(
            current.etag, "get_cluster succeeded but returned no ETag "
            "header -- response headers were: %s" % current.headers)
        original_description = current.json.get('description')

        new_description = 'audit-test-%s' % cluster_uuid[:8]
        try:
            since = get_current_iso8601_timestamp()
            status, content, _ = self.ucp_client.update_cluster(
                cluster_uuid, current.etag, description=new_description)
            self.assertTrue(status, "update_cluster failed: %s" % content)

            event, response = get_latest_audit_event(
                self.ucp_client, action=ACTION_CLUSTER_METADATA_UPDATED,
                actor=self.ucp_portal.username, resource_id=cluster_uuid,
                since=since)
            self.assertTrue(
                response.is_success(),
                "GET /audit failed: %s" % response.content)
            self.assertIsNotNone(
                event, "No audit event found for action=%s actor=%s "
                "resourceId=%s" % (ACTION_CLUSTER_METADATA_UPDATED,
                                  self.ucp_portal.username, cluster_uuid))
            self.log.info(
                "PASS -- cluster metadata update recorded an audit event: %s"
                % event)
        finally:
            status, content, header = self.ucp_client.get_cluster(
                cluster_uuid)
            if status:
                restore_response = UCPResponse(status, content, header)
                try:
                    self.ucp_client.update_cluster(
                        cluster_uuid, restore_response.etag,
                        description=original_description)
                except Exception as e:
                    self.log.warning(
                        "Failed to restore cluster description: %s" % e)

    def test_audit_event_created_on_entitlement_update(self):
        status, content, header = self.ucp_client.get_entitlements()
        self.assertTrue(status, "get_entitlements failed: %s" % content)
        current = UCPResponse(status, content, header)
        self.assertIsNotNone(
            current.etag, "get_entitlements succeeded but returned no ETag "
            "header -- response headers were: %s" % current.headers)

        subscription = build_subscription_payload(
            start_at='2026-01-01T00:00:00Z', end_at='2026-12-31T23:59:59Z',
            nodes=10, logical_cores=32, ram_bytes=68719476736)
        since = get_current_iso8601_timestamp()
        status, content, _ = self.ucp_client.update_entitlements(
            etag=current.etag, subscriptions=[subscription])
        self.assertTrue(status, "update_entitlements failed: %s" % content)

        # The entitlement profile is a single global singleton with a fixed
        # id ("default", confirmed live) -- there is only ever one to match.
        event, response = get_latest_audit_event(
            self.ucp_client, action=ACTION_ENTITLEMENT_UPDATED,
            actor=self.ucp_portal.username, resource_id='default',
            since=since)
        self.assertTrue(
            response.is_success(), "GET /audit failed: %s" % response.content)
        self.assertIsNotNone(
            event, "No audit event found for action=%s actor=%s"
            % (ACTION_ENTITLEMENT_UPDATED, self.ucp_portal.username))
        self.log.info(
            "PASS -- entitlement update recorded an audit event: %s" % event)

    def test_audit_event_created_on_config_update(self):
        # PUT /config is a full replacement, not a partial update --
        # confirmed live 2026-07-24 (omitting any field returns 422 "expected
        # required property ... to be present") -- so every current field
        # must be resent even though only telemetryRetentionDays changes.
        status, content, header = self.ucp_client.get_config()
        self.assertTrue(status, "get_config failed: %s" % content)
        current = UCPResponse(status, content, header)
        self.assertIsNotNone(
            current.etag, "get_config succeeded but returned no ETag "
            "header -- response headers were: %s" % current.headers)
        new_retention_days = current.json.get('telemetryRetentionDays') + 1

        since = get_current_iso8601_timestamp()
        status, content, _ = self.ucp_client.update_config(
            etag=current.etag,
            telemetry_retention_days=new_retention_days,
            session_idle_timeout_minutes=current.json.get(
                'sessionIdleTimeoutMinutes'),
            session_absolute_timeout_minutes=current.json.get(
                'sessionAbsoluteTimeoutMinutes'),
            global_rate_limit_per_sec=current.json.get(
                'globalRateLimitPerSec'),
            expensive_rate_limit_per_sec=current.json.get(
                'expensiveRateLimitPerSec'))
        self.assertTrue(status, "update_config failed: %s" % content)

        # The config resource is a single global singleton with a fixed id
        # ("default", confirmed live) -- there is only ever one to match.
        event, response = get_latest_audit_event(
            self.ucp_client, action=ACTION_CONFIG_UPDATED,
            actor=self.ucp_portal.username, resource_id='default',
            since=since)
        self.assertTrue(
            response.is_success(), "GET /audit failed: %s" % response.content)
        self.assertIsNotNone(
            event, "No audit event found for action=%s actor=%s"
            % (ACTION_CONFIG_UPDATED, self.ucp_portal.username))
        self.log.info(
            "PASS -- config update recorded an audit event: %s" % event)

    def test_audit_event_created_on_report_generation(self):
        # Confirmed live 2026-07-24: the portal currently rejects 'from'/'to'
        # as unknown query parameters and only accepts 'format' (required,
        # 'pdf' only) -- no time range to pass here yet.
        since = get_current_iso8601_timestamp()
        status, content, _ = self.ucp_client.generate_usage_report(
            format_type='pdf')
        self.assertTrue(status, "generate_usage_report failed: %s" % content)

        event, response = get_latest_audit_event(
            self.ucp_client, action=ACTION_USAGE_REPORT_GENERATED,
            actor=self.ucp_portal.username, since=since)
        self.assertTrue(
            response.is_success(), "GET /audit failed: %s" % response.content)
        self.assertIsNotNone(
            event, "No audit event found for action=%s actor=%s"
            % (ACTION_USAGE_REPORT_GENERATED, self.ucp_portal.username))
        self.log.info(
            "PASS -- usage report generation recorded an audit event: %s"
            % event)

    # ==================== Log-level behaviour ====================

    def test_audit_log_filterable_by_actor_action_and_time_range(self):
        """
        Verify the actor, action, and from/to time-range query filters on
        GET /audit each independently narrow the result set.
        """
        status, content, header = self.ucp_client.list_audit_events(
            action=ACTION_LOGIN)
        by_action = UCPResponse(status, content, header)
        self.assertTrue(
            by_action.is_success(),
            "GET /audit?action=login failed: %s" % content)
        self.assertTrue(
            len(by_action.items) >= 1,
            "Expected at least one login audit event")

        status, content, header = self.ucp_client.list_audit_events(
            actor=self.ucp_portal.username)
        by_actor = UCPResponse(status, content, header)
        self.assertTrue(
            by_actor.is_success(),
            "GET /audit?actor=... failed: %s" % content)
        self.assertTrue(
            len(by_actor.items) >= 1,
            "Expected at least one audit event for the admin actor")

        status, content, header = self.ucp_client.list_audit_events(
            actor=self.ucp_portal.username, action=ACTION_LOGIN)
        by_actor_and_action = UCPResponse(status, content, header)
        self.assertTrue(
            by_actor_and_action.is_success(),
            "GET /audit?actor=...&action=login failed: %s" % content)
        self.assertTrue(
            len(by_actor_and_action.items) >= 1,
            "Expected at least one combined actor+action match")
        if by_actor_and_action.total is not None and by_action.total is not None:
            self.assertLessEqual(
                by_actor_and_action.total, by_action.total,
                "actor+action filter returned more events than action alone")

        far_future = format_iso8601_timestamp(
            datetime.utcnow() + timedelta(days=3650))
        far_past = format_iso8601_timestamp(
            datetime.utcnow() - timedelta(days=3650))

        status, content, header = self.ucp_client.list_audit_events(
            from_timestamp=far_future)
        future_only = UCPResponse(status, content, header)
        self.assertTrue(
            future_only.is_success(),
            "GET /audit?from=<10y future> failed: %s" % content)
        self.assertEqual(
            len(future_only.items), 0,
            "from=<10y in the future> should return no events, got %d"
            % len(future_only.items))

        status, content, header = self.ucp_client.list_audit_events(
            from_timestamp=far_past, to_timestamp=far_future)
        wide_range = UCPResponse(status, content, header)
        self.assertTrue(
            wide_range.is_success(),
            "GET /audit with a 10y-wide time range failed: %s" % content)
        self.assertTrue(
            len(wide_range.items) >= 1,
            "Expected at least one event within a 10-year wide time range")
        self.log.info(
            "PASS -- audit log filterable by actor, action, and time range")

    def test_default_sort_is_timestamp_descending(self):
        """
        The default (unspecified-sort) order must be timestamp descending.
        Confirmed live (2026-07-24) that every audit event carries a
        'timestamp' field (RFC3339), so this asserts directly on it rather
        than inferring order indirectly.
        """
        status, content, header = self.ucp_client.list_audit_events(
            actor=self.ucp_portal.username, limit=10)
        response = UCPResponse(status, content, header)
        self.assertTrue(status, "GET /audit failed: %s" % content)
        items = response.items
        self.assertTrue(
            len(items) >= 2,
            "Need at least 2 audit events for actor=%s to verify sort "
            "order, got %d" % (self.ucp_portal.username, len(items)))

        timestamps = [parse_iso8601_timestamp(item['timestamp'])
                     for item in items]
        self.assertEqual(
            timestamps, sorted(timestamps, reverse=True),
            "Default audit log order is not timestamp descending: %s"
            % [item['timestamp'] for item in items])
        self.log.info(
            "PASS -- default audit order is timestamp descending across "
            "%d events" % len(items))

    def test_audit_records_are_append_only(self):
        """
        An existing audit record must reject both DELETE and PUT -- the
        typed client exposes no update/delete audit method by design;
        this confirms the raw HTTP surface agrees, and that the record is
        unchanged afterwards.
        """
        event, response = get_latest_audit_event(self.ucp_client)
        self.assertTrue(
            response.is_success(), "GET /audit failed: %s" % response.content)
        self.assertIsNotNone(event, "No audit event available to probe")
        event_id = event['auditEventId']

        path = 'api/v1/audit/%s' % event_id
        status, content, header = raw_request(self.ucp_client, 'DELETE', path)
        delete_response = UCPResponse(status, content, header)
        self.assertFalse(
            status, "DELETE on an audit event unexpectedly succeeded "
            "(HTTP %s)" % delete_response.status_code)

        status, content, header = raw_request(
            self.ucp_client, 'PUT', path, body={'action': 'tampered'})
        update_response = UCPResponse(status, content, header)
        self.assertFalse(
            status, "PUT on an audit event unexpectedly succeeded "
            "(HTTP %s)" % update_response.status_code)

        status, content, header = self.ucp_client.get_audit_event(event_id)
        after = UCPResponse(status, content, header)
        self.assertTrue(
            after.is_success(),
            "Audit event %s disappeared/broke after mutation attempts: %s"
            % (event_id, content))
        self.assertEqual(
            after.json, event,
            "Audit event content changed despite rejected mutation attempts")
        self.log.info(
            "PASS -- audit event %s rejected DELETE/PUT and is unchanged"
            % event_id)
