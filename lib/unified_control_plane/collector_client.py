# -*- coding: utf-8 -*-
"""
Lighthouse Collector API Client (v1.4)
Raw HTTP client for hitting the ns_server Lighthouse Collector endpoints.

Inherits from TAF's RestConnection to reuse _http_request() with Basic auth.
Targets the Couchbase Server node (default port 8091) — NOT the UCP portal.

Relevant endpoints (all on the CB Server node):
  GET  /internal/settings/lighthouse
  POST /internal/settings/lighthouse
  POST /_lighthouseCollector/ingest?product_name=<name>&instance_id=<id>

Returns raw (status, content, response) tuples — no wrapper logic.

Design reference: Lighthouse Collector Design Revision v1.4 (17 Jun 2026)
"""
import json
import urllib

from connections.Rest_Connection import RestConnection as BaseRestConnection
from global_vars import logger


class LighthouseCollectorClient(BaseRestConnection):
    """
    Raw API client for the ns_server Lighthouse Collector endpoints.

    Inherits from TAF's RestConnection so that _http_request() and
    _create_capi_headers() (Basic auth) are reused automatically.

    The client connects to a standard Couchbase Server node (port 8091)
    and is completely separate from the UCP portal client.
    """

    def __init__(self, server, timeout=300):
        """
        Initialise the collector client.

        Args:
            server: TestInputServer (or any object with .ip, .rest_username,
                    .rest_password, .port) pointing at a CB Server node.
            timeout: Default request timeout in seconds.
        """
        self.log = logger.get("infra")
        self.test_log = logger.get("test")
        self._default_timeout = timeout
        # Delegate full initialisation (baseUrl, auth headers, etc.)
        # to RestConnection.__init__ so we stay consistent with the rest
        # of TAF.
        super(LighthouseCollectorClient, self).__init__(server,
                                                        timeout=timeout)

    # ------------------------------------------------------------------
    # /internal/settings/lighthouse
    # ------------------------------------------------------------------

    def get_lighthouse_settings(self):
        """
        GET /internal/settings/lighthouse

        Returns the current Lighthouse Collector configuration.

        Expected response body (example):
            {
                "enabled": true,
                "endpoint": "lighthouse.couchbase.internal",
                "reportIntervalHours": 2,
                "reportTimeoutSeconds": 1,
                "externalNodesMaxPayloadBytes": 10240,
                "externalNodesMaxCount": 100
            }

        Returns:
            (status, content, header) tuple
        """
        api = self.baseUrl + 'internal/settings/lighthouse'
        status, content, header = self._http_request(
            api, 'GET', headers=self._create_capi_headers(),
            timeout=self._default_timeout)
        return status, content, header

    def update_lighthouse_settings(self, enabled=None, endpoint=None,
                                   report_interval_hours=None,
                                   report_timeout_seconds=None,
                                   external_nodes_max_payload_bytes=None,
                                   external_nodes_max_count=None):
        """
        POST /internal/settings/lighthouse

        Update one or more Lighthouse Collector configuration values.
        Omitted parameters are not sent, leaving existing values unchanged.

        Note: Any update (including a no-op) immediately triggers a report
        attempt to the configured portal endpoint, which can be used to
        manually force / verify connectivity.

        Args:
            enabled (bool):   Enable or disable the collector.
            endpoint (str):   Lighthouse portal hostname/IP.
            report_interval_hours (int):
                              How often to report telemetry (default 2).
            report_timeout_seconds (int):
                              Connection timeout per attempt (default 1).
            external_nodes_max_payload_bytes (int):
                              Max bytes for a single external node payload
                              (default 10240).
            external_nodes_max_count (int):
                              Max number of tracked external nodes
                              (default 100).

        Returns:
            (status, content, header) tuple
        """
        api = self.baseUrl + 'internal/settings/lighthouse'
        body_dict = {}
        if enabled is not None:
            body_dict['enabled'] = enabled
        if endpoint is not None:
            body_dict['endpoint'] = endpoint
        if report_interval_hours is not None:
            body_dict['reportIntervalHours'] = report_interval_hours
        if report_timeout_seconds is not None:
            body_dict['reportTimeoutSeconds'] = report_timeout_seconds
        if external_nodes_max_payload_bytes is not None:
            body_dict['externalNodesMaxPayloadBytes'] = \
                external_nodes_max_payload_bytes
        if external_nodes_max_count is not None:
            body_dict['externalNodesMaxCount'] = external_nodes_max_count
        body = json.dumps(body_dict)
        status, content, header = self._http_request(
            api, 'POST', body, headers=self._create_capi_headers(),
            timeout=self._default_timeout)
        return status, content, header

    # ------------------------------------------------------------------
    # /_lighthouseCollector/ingest
    # ------------------------------------------------------------------

    def ingest_external_telemetry(self, product_name, instance_id, payload):
        """
        POST /_lighthouseCollector/ingest?product_name=<name>&instance_id=<id>

        Submit telemetry from an external component (e.g. Sync Gateway) to
        the ns_server collector.  ns_server stores the latest payload per
        (product_name, instance_id) pair and includes it in the next
        lighthouse report under ``externalNodes.<product_name>``.

        The payload is passed through to the lighthouse without inspection
        (beyond confirming it is valid JSON), so the schema is owned by the
        external component.

        Reference SGW payload shape:
            {
                "instanceId": "",
                "cpuCores": 1,
                "ramBytesTotal": 0,
                "ramBytesUsed": 0,
                "product": {
                    "edition": "",
                    "version": "",
                    "name": ""
                },
                "osVersion": "",
                "hostname": "",
                "uptimeSeconds": 0
            }

        Args:
            product_name (str):
                Opaque product identifier (e.g. "sync_gateway").
                All nodes sharing the same name are grouped together in
                the collector payload.  Should remain stable across
                releases unless intentional re-grouping is desired.
            instance_id (str):
                Opaque, stable, unique identifier for the specific
                external node instance.  Used to deduplicate payloads
                so that only the latest report per node is kept.
            payload (dict):
                Telemetry body to forward.  Must be JSON-serialisable.

        Returns:
            (status, content, header) tuple
        """
        params = urllib.urlencode({
            'product_name': product_name,
            'instance_id': instance_id
        })
        api = self.baseUrl + '_lighthouseCollector/ingest?' + params
        body = json.dumps(payload)
        status, content, header = self._http_request(
            api, 'POST', body, headers=self._create_capi_headers(),
            timeout=self._default_timeout)
        return status, content, header

