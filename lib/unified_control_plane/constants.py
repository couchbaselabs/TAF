
"""
Constants for Unified Control Plane API
"""
API_V1 = 'api/v1'
# Session Endpoints
SESSION_LOGIN = API_V1 + '/session/login'
SESSION_LOGOUT = API_V1 + '/session/logout'
SESSION_ME = API_V1 + '/session/me'
# User Endpoints
USERS = API_V1 + '/users'
# Ingest Endpoints
INGEST_TELEMETRY = API_V1 + '/ingest/telemetry'
INGEST_HEALTH = API_V1 + '/ingest/health'
# Cluster Endpoints
CLUSTERS = API_V1 + '/clusters'
# Entitlement Endpoints
ENTITLEMENTS = API_V1 + '/entitlements'
ENTITLEMENTS_USAGE = API_V1 + '/entitlements/usage'
# Reports Endpoints
REPORTS_USAGE = API_V1 + '/reports/usage'
# Audit Endpoints
AUDIT = API_V1 + '/audit'
# Config Endpoints
CONFIG = API_V1 + '/config'
# Health Endpoints
HEALTH = API_V1 + '/health'

# ---- Lighthouse Collector (ns_server) endpoints ----
# These are on the Couchbase Server node (port 8091), NOT the UCP portal.

# Config endpoint – GET to read, POST to update (and force a report).
# Permission: cluster.admin.settings.lighthouse!read (GET)
#             cluster.admin.settings.lighthouse!write (POST)
#             mobile_sync_gateway role has read access.
COLLECTOR_SETTINGS = 'internal/settings/lighthouse'

# External telemetry ingest endpoint.
# Append ?product_name=<name>&instance_id=<id> as query params.
# Permission: cluster.lighthouse_telemetry!write
#             (granted only to mobile_sync_gateway)
COLLECTOR_INGEST = '_lighthouseCollector/ingest'

# Default collector configuration values (from design doc v1.4)
COLLECTOR_DEFAULT_ENABLED = True
COLLECTOR_DEFAULT_ENDPOINT = 'lighthouse.couchbase.internal'
COLLECTOR_DEFAULT_PORT = 433
COLLECTOR_DEFAULT_REPORT_INTERVAL_HOURS = 2
COLLECTOR_DEFAULT_REPORT_TIMEOUT_SECONDS = 1
COLLECTOR_DEFAULT_EXTERNAL_NODES_MAX_PAYLOAD_BYTES = 10240
COLLECTOR_DEFAULT_EXTERNAL_NODES_MAX_COUNT = 100

# Collector metrics labels
COLLECTOR_METRIC_RESULT_SUCCESS = 'success'
COLLECTOR_METRIC_RESULT_FAILURE = 'failure'
# Cluster Classifications
CLASSIFICATION_PRODUCTION = 'production'
CLASSIFICATION_BACKUP = 'backup'
CLASSIFICATION_DEVELOPMENT = 'development'
CLASSIFICATION_TEST = 'test'
# User Roles
ROLE_SYSTEM_ADMIN = 'system_admin'
ROLE_SYSTEM_VIEWER = 'system_viewer'
# Audit Actions
ACTION_LOGIN = 'login'
ACTION_LOGOUT = 'logout'
ACTION_USER_CREATED = 'user_created'
ACTION_USER_UPDATED = 'user_updated'
ACTION_USER_DELETED = 'user_deleted'
ACTION_ENTITLEMENT_UPDATED = 'entitlement_updated'
ACTION_CLUSTER_METADATA_UPDATED = 'cluster_metadata_updated'
ACTION_CONFIG_UPDATED = 'config_updated'
ACTION_USAGE_REPORT_GENERATED = 'usage_report_generated'
