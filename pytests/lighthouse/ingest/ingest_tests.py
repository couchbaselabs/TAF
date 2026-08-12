# -*- coding: utf-8 -*-
"""
UCP Portal Ingest Tests -- storage semantics and the security boundary of
the telemetry ingest surface.

    - case 102: the same (clusterUuid, collectedAt) posted twice stores one
                record, not two (upsert, not append).
    - case 103: the same payload posted concurrently still stores exactly
                one record -- the upsert holds under a write-write race,
                which sequential duplicates cannot prove.
    - cases 101/186/188: the ingest identity is write-only. Ingest is
                unauthenticated (confirmed live 2026-08-12: a POST with no
                session cookie reaches payload validation), so the identity
                a collector -- or anyone who can reach the port -- holds is
                "no session at all". That identity must be able to write
                telemetry and nothing else: every user/entitlement/admin
                endpoint must answer 401, and no read of stored telemetry
                may succeed. One test asserts all three rows because they
                are one property of one identity; splitting them would
                re-provision and re-probe the same surface three times.
    - case 182: the portal refuses cleartext HTTP. Asserted on the TLS port
                (a plaintext request there must not be served) and on port
                80 (no cleartext listener should answer at all).

Cases 102/103 target a synthetic cluster UUID rather than a real cluster:
the assertion is about the portal's storage key, and driving a real
collector would make the arriving payload -- and its collectedAt -- outside
the test's control. The UUID is a conf param and is deliberately stable
across runs (the portal exposes no DELETE for a cluster: confirmed live,
DELETE /api/v1/clusters/{uuid} -> 405), so re-runs reuse one synthetic
document instead of leaking a new one each time. Idempotency is therefore
counted per collectedAt instant, not on the history total -- see
ucp_helper_methods.count_history_snapshots_at.

Reads of stored history need an admin session, so setUp opens one; the
ingest posts themselves deliberately go through an unauthenticated client
so that no test accidentally proves ingest works only when an admin
happens to be logged in.
"""
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.response import UCPResponse
from lighthouse.ucp_helper_methods import (
    build_minimal_ingest_payload,
    count_history_snapshots_at,
    create_session,
    get_current_iso8601_timestamp,
    get_session_cookie,
    ingest_concurrently,
    is_tcp_port_open,
    new_anonymous_client,
    plain_http_request,
)


class IngestTests(LighthouseBase):

    def setUp(self):
        super(IngestTests, self).setUp()
        self.synthetic_cluster_uuid = self.input.param(
            "synthetic_cluster_uuid",
            "5f7c1a30-0000-4000-8000-100000000102")
        self.concurrent_ingest_count = self.input.param(
            "concurrent_ingest_count", 5)
        self.expected_unauthorized_status = self.input.param(
            "expected_unauthorized_status", 401)
        self.cleartext_port = self.input.param("cleartext_port", 80)
        # The ingest identity: no session cookie, exactly what a collector
        # (or a rogue client on the network) holds.
        self.anon_client = new_anonymous_client(self.ucp_portal)
        # History/user reads need admin; the ingest posts do not use this.
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
        super(IngestTests, self).tearDown()

    def test_duplicate_ingest_is_idempotent(self):
        """
        Case 102: posting the identical payload twice must upsert, leaving
        exactly one stored snapshot for that (clusterUuid, collectedAt).

        The count is taken before the first post as well, so a pre-existing
        snapshot at the same instant (a re-run within the same second)
        cannot be mistaken for the duplicate this test is looking for.
        """
        collected_at = get_current_iso8601_timestamp()
        payload = build_minimal_ingest_payload(
            self.synthetic_cluster_uuid, collected_at=collected_at)

        before = count_history_snapshots_at(
            self.ucp_client, self.synthetic_cluster_uuid, collected_at)
        self.assertEqual(
            before, 0,
            "Expected no pre-existing snapshot at %s for cluster %s, found "
            "%d -- pick a fresh collectedAt or synthetic_cluster_uuid"
            % (collected_at, self.synthetic_cluster_uuid, before))

        for attempt in (1, 2):
            status, content, header = self.anon_client.ingest_telemetry(
                payload)
            response = UCPResponse(status, content, header)
            self.assertTrue(
                status, "Ingest attempt %d failed (HTTP %s): %s"
                % (attempt, response.status_code, content))

        stored = count_history_snapshots_at(
            self.ucp_client, self.synthetic_cluster_uuid, collected_at)
        self.assertEqual(
            stored, 1,
            "Duplicate ingest of (clusterUuid=%s, collectedAt=%s) stored %d "
            "snapshots; expected exactly 1 (upsert, not append)"
            % (self.synthetic_cluster_uuid, collected_at, stored))
        self.log.info(
            "PASS -- duplicate ingest at %s stored exactly one snapshot"
            % collected_at)

    def test_concurrent_duplicate_ingest_stores_one_record(self):
        """
        Case 103: the same payload posted by several threads at once still
        results in exactly one stored record.

        Every concurrent post must also be accepted -- a portal that
        serialised the race by rejecting the losers would store one record
        for the wrong reason, so the response statuses are asserted too.
        """
        collected_at = get_current_iso8601_timestamp()
        payload = build_minimal_ingest_payload(
            self.synthetic_cluster_uuid, collected_at=collected_at)

        before = count_history_snapshots_at(
            self.ucp_client, self.synthetic_cluster_uuid, collected_at)
        self.assertEqual(
            before, 0,
            "Expected no pre-existing snapshot at %s for cluster %s, found "
            "%d" % (collected_at, self.synthetic_cluster_uuid, before))

        results = ingest_concurrently(
            self.anon_client, payload, self.concurrent_ingest_count)
        failures = [content for status, content in results if not status]
        self.assertFalse(
            failures,
            "%d of %d concurrent ingest posts were rejected: %s"
            % (len(failures), self.concurrent_ingest_count, failures))

        stored = count_history_snapshots_at(
            self.ucp_client, self.synthetic_cluster_uuid, collected_at)
        self.assertEqual(
            stored, 1,
            "%d concurrent identical ingests stored %d snapshots at %s; "
            "expected exactly 1"
            % (self.concurrent_ingest_count, stored, collected_at))
        self.log.info(
            "PASS -- %d concurrent identical ingests stored exactly one "
            "snapshot" % self.concurrent_ingest_count)

    def test_ingest_identity_is_write_only(self):
        """
        Cases 101, 186 and 188: the unauthenticated ingest identity may
        write telemetry and reach nothing else.

        Three assertions, one identity:
          1. it CAN post telemetry (the positive control -- without it, a
             portal that rejected everything would pass vacuously),
          2. every user/entitlement/admin endpoint answers 401,
          3. no read of stored telemetry succeeds (write-only surface).
        """
        payload = build_minimal_ingest_payload(self.synthetic_cluster_uuid)
        status, content, header = self.anon_client.ingest_telemetry(payload)
        response = UCPResponse(status, content, header)
        self.assertTrue(
            status,
            "Positive control failed: the unauthenticated ingest identity "
            "could not post telemetry (HTTP %s): %s"
            % (response.status_code, content))

        # The admin/user/entitlement surface (cases 101, 186) and any read of
        # stored telemetry (case 188) must both be closed to this identity.
        forbidden_reads = [
            ('users', self.anon_client.list_users),
            ('entitlements', self.anon_client.get_entitlements),
            ('entitlement usage', self.anon_client.get_entitlement_usage),
            ('config', self.anon_client.get_config),
            ('audit', self.anon_client.list_audit_events),
            ('clusters', self.anon_client.list_clusters),
        ]
        for label, call in forbidden_reads:
            status, content, header = call()
            response = UCPResponse(status, content, header)
            self.assertFalse(
                status,
                "%s: the ingest identity was served a successful response; "
                "the ingest surface must be write-only" % label)
            self.assertEqual(
                response.status_code, self.expected_unauthorized_status,
                "%s: expected HTTP %s for an unauthenticated caller, got "
                "%s: %s" % (label, self.expected_unauthorized_status,
                            response.status_code, content))

        # A targeted read of the very cluster it just wrote must also fail --
        # write access to a document must not imply read access to it.
        status, content, header = self.anon_client.get_cluster(
            self.synthetic_cluster_uuid)
        response = UCPResponse(status, content, header)
        self.assertFalse(
            status,
            "The ingest identity could read back the cluster it wrote; the "
            "ingest surface must be write-only")
        self.assertEqual(
            response.status_code, self.expected_unauthorized_status,
            "cluster read-back: expected HTTP %s, got %s: %s"
            % (self.expected_unauthorized_status, response.status_code,
               content))
        self.log.info(
            "PASS -- ingest identity can write telemetry and read nothing "
            "(%d endpoints returned %s)"
            % (len(forbidden_reads) + 1, self.expected_unauthorized_status))

    def test_plain_http_ingest_rejected(self):
        """
        Case 182: cleartext ingest is refused.

        Two probes, because a portal can fail this two different ways:
          - a plaintext request to the TLS port must not be served (the TLS
            listener answers with an HTTP error or drops the connection --
            either is a rejection, neither is a 2xx),
          - the cleartext port must have no listener at all.
        """
        payload = build_minimal_ingest_payload(self.synthetic_cluster_uuid)
        status_code, status_line, error = plain_http_request(
            self.ucp_portal.ip, self.ucp_portal.port,
            '/api/v1/ingest/telemetry', method='POST', body=payload)

        if error is not None:
            self.log.info(
                "Cleartext request to the TLS port was dropped at the "
                "transport layer: %s" % error)
        else:
            self.assertIsNotNone(
                status_code,
                "Cleartext request to port %s returned an unparseable "
                "response: %r" % (self.ucp_portal.port, status_line))
            self.assertFalse(
                200 <= status_code < 300,
                "Cleartext ingest on port %s was SERVED: %s"
                % (self.ucp_portal.port, status_line))
            self.log.info(
                "Cleartext request to the TLS port was rejected: %s"
                % status_line)

        self.assertFalse(
            is_tcp_port_open(self.ucp_portal.ip, self.cleartext_port),
            "A cleartext listener answered on port %s; telemetry must only "
            "ever be accepted over TLS" % self.cleartext_port)
        self.log.info(
            "PASS -- cleartext ingest rejected on port %s and no listener "
            "on port %s" % (self.ucp_portal.port, self.cleartext_port))
