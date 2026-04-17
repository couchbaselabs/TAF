---
name: cb-rest-cluster-nodes-agent
description: >
  REST API methods for Couchbase cluster and node management — failover,
  rebalance, node add/remove, settings, stats, and events in TAF.
model: inherit
---

# cluster_nodes/

**Composite class:** `ClusterRestAPI` (`cluster_nodes_api.py`)
**Base URL attribute:** `self.base_url`

## Files → Classes

| File | Class | Inherits |
|---|---|---|
| `cluster_nodes_api.py` | `ClusterRestAPI` | All below (composite) |
| `auto_failover.py` | `AutoFailoverAPI` | `CBRestConnection` |
| `auto_reprovision.py` | `AutoReprovisionAPI` | `CBRestConnection` |
| `cluster_init_provision.py` | `ClusterInitializationProvision` | `CBRestConnection` |
| `diag_eval.py` | `DiagEvalAPI` | `CBRestConnection` |
| `cb_logging.py` | `Logging` | `CBRestConnection` |
| `manual_failover.py` | `ManualFailoverAPI` | `CBRestConnection` |
| `node_add_remove.py` | `NodeAdditionRemoval` | `CBRestConnection` |
| `rebalance.py` | `RebalanceRestAPI` | `CBRestConnection` |
| `settings_and_connections.py` | `SettingsAndConnectionsAPI` | `CBRestConnection` |
| `statistics.py` | `StatisticsAPI` | `CBRestConnection` |
| `status_and_events.py` | `StatusAndEventsAPI` | `CBRestConnection` |

## Method Inventory

### auto_failover.py — `AutoFailoverAPI`

| Method | Verb | Path |
|---|---|---|
| `get_auto_failover_settings` | GET | `/settings/autoFailover` |
| `update_auto_failover_settings` | POST | `/settings/autoFailover` |
| `reset_auto_failover_count` | POST | `/settings/autoFailover/resetCount` |
| `set_recovery_type` | POST | `/controller/setRecoveryType` |

### auto_reprovision.py — `AutoReprovisionAPI`

| Method | Verb | Path |
|---|---|---|
| `get_auto_reprovision_settings` | GET | `/settings/autoReprovision` |
| `update_auto_reprovision_settings` | POST | `/settings/autoReprovision` |
| `reset_auto_reprovision` | POST | `/settings/autoReprovision/resetCount` |

### cluster_init_provision.py — `ClusterInitializationProvision`

| Method | Verb | Path |
|---|---|---|
| `initialize_cluster` | POST | `/clusterInit` |
| `initialize_node` | POST | `/nodes/self/controller/settings` |
| `establish_credentials` | POST | `/settings/web` |
| `reset_node` | POST | `/controller/hardResetNode` |
| `rename_node` | POST | `/node/controller/rename` |
| `configure_memory` | POST | `/pools/default` |
| `setup_services` | POST | `/node/controller/setupServices` |
| `naming_the_cluster` | POST | `/node/controller/setupServices` |

### diag_eval.py — `DiagEvalAPI`

| Method | Verb | Path | Notes |
|---|---|---|---|
| `diag_eval` | POST | `/diag/eval` | Executes raw Erlang code |
| `ns_bucket_update_bucket_props` | POST | `/diag/eval` | Wraps diag_eval |
| `ns_config_set` | POST | `/diag/eval` | Wraps diag_eval |
| `testconditions_set` | POST | `/diag/eval` | Wraps diag_eval |
| `testconditions_delete` | POST | `/diag/eval` | Wraps diag_eval |
| `cluster_compat_mode_get_version` | POST | `/diag/eval` | Wraps diag_eval |
| `get_admin_credentials` | POST | `/diag/eval` | Wraps diag_eval |
| `change_memcached_t_option` | POST | `/diag/eval` | Wraps diag_eval |

### cb_logging.py — `Logging`

| Method | Verb | Path |
|---|---|---|
| `start_logs_collection` | POST | `/controller/startLogsCollection` |
| `cancel_logs_collection` | POST | `/controller/cancelLogsCollection` |
| `log_client_error` | POST | `/logClientError` |

### manual_failover.py — `ManualFailoverAPI`

| Method | Verb | Path |
|---|---|---|
| `perform_hard_failover` | POST | `/controller/failOver` |
| `perform_graceful_failover` | POST | `/controller/startGracefulFailover` |
| `set_failover_recovery_type` | POST | `/controller/setRecoveryType` |

### node_add_remove.py — `NodeAdditionRemoval`

| Method | Verb | Path |
|---|---|---|
| `add_node` | POST | `/controller/addNode` |
| `join_node_to_cluster` | POST | `/node/controller/doJoinCluster` |
| `eject_node` | POST | `/controller/ejectNode` |
| `re_add_node` | POST | `/controller/reAddNode` |

### rebalance.py — `RebalanceRestAPI`

| Method | Verb | Path |
|---|---|---|
| `rebalance` | POST | `/controller/rebalance` |
| `stop_rebalance` | POST | `/controller/stopRebalance` |
| `rebalance_progress` | GET | `/pools/default/rebalanceProgress` |
| `retry_rebalance` | GET/POST | `/settings/retryRebalance` |
| `pending_retry_rebalance` | GET | `/pools/default/pendingRetryRebalance` |
| `cancel_retry_rebalance` | POST | `/controller/cancelRebalanceRetry/{rebalance_id}` |
| `rebalance_settings` | GET/POST | `/settings/rebalance` |
| `set_index_aware_rebalance` | POST | `/internalSettings` |

### settings_and_connections.py — `SettingsAndConnectionsAPI`

| Method | Verb | Path |
|---|---|---|
| `set_internal_settings` | GET/POST | `/internalSettings` |
| `create_secret` | POST | `/settings/encryptionKeys` |
| `get_all_secrets` | GET | `/settings/encryptionKeys` |
| `get_specific_secret` | GET | `/secrets/{secret_id}` |
| `download_keks` | GET | `/secrets/{secret_id}/backup` |
| `modify_secret` | PUT | `/settings/encryptionKeys/{secret_id}` |
| `delete_secret` | DELETE | `/settings/encryptionKeys/{secret_id}` |
| `get_encryption_at_rest_config` | GET | `/settings/security/encryptionAtRest` |
| `configure_encryption_at_rest` | POST | `/settings/security/encryptionAtRest` |
| `trigger_data_reencryption` | POST | `/controller/dropEncryptionAtRestKeys/bucket/{bucket}` |
| `trigger_kek_rotation` | POST | `/controller/rotateSecret/{secret_id}` |
| `enable_config_encryption` | POST | `/settings/security/encryptionAtRest` |
| `set_auto_compaction_settings` | POST | `/controller/setAutoCompaction` |
| `manage_internal_settings_max_parallel_indexers` | GET/POST | `/internalSettings/maxParallelIndexers` |
| `manage_global_memcached_setting` | GET/POST | `/pools/default/settings/memcached/global` |
| `manage_alternate_address` | GET/POST/PUT/DELETE | `/node/controller/setupAlternateAddresses/external` |
| `manage_alerts` | GET/POST | `/settings/alerts` |
| `set_cgroup_overrides` | POST | `/settings/cgroups` |
| `get_cgroup_overrides` | GET | `/settings/cgroups` |
| `delete_cgroup_override` | DELETE | `/settings/cgroups/{service}` |

### statistics.py — `StatisticsAPI`

| Method | Verb | Path |
|---|---|---|
| `get_prometheus_sd_config` | GET | `/prometheus_sd_config` |
| `get_stats_for_metric` | GET | `/pools/default/stats/range/{metric_name}[/{function_expression}]` |
| `get_multiple_stats` | POST | `/pools/default/stats/range` |
| `query_prometheus` | GET | `/_prometheus/api/v1/query?query={query}` |

### status_and_events.py — `StatusAndEventsAPI`

| Method | Verb | Path |
|---|---|---|
| `cluster_tasks` | GET | `/pools/default/tasks` |
| `rebalance_report` | GET | `/logs/rebalanceReport?reportID={report_id}` |
| `cluster_info` | GET | `/pools` |
| `get_terse_cluster_info` | GET | `/pools/default/terseClusterInfo` |
| `cluster_details` | GET | `/pools/default` |
| `node_details` | GET | `/nodes/self` |
| `get_node_statuses` | GET | `/nodeStatuses` |
| `ui_logs` | GET | `/logs` |
| `log_client_error` | POST | `/logClientError` |
| `get_system_event_logs` | GET | `/events[?sinceTime={t}&limit={n}]` |
| `get_system_event_streaming` | GET | `/eventsStreaming` |
| `create_system_event` | POST | `/_event` |

## No Business Logic — Pure Wrappers Only

Every method must be a single REST call that returns `(status, content)`.
**Forbidden in this directory:** response parsing (`content["key"]`), validation,
retries, polling loops, helper/utility functions, wrapping functions, `self.fail()`,
exceptions, or logging beyond a single debug line.
Put all such logic in the caller or a utility helper outside `cb_server_rest_util/`.

## HTTP Library

All HTTP calls must use `self.request()` inherited from `CBRestConnection` (which uses `requests` internally).
**Never** import `http.client`, `urllib.request`, `urllib3`, `httplib2`, `aiohttp`, `httpx`, or any other HTTP library.
If you encounter such an import, warn the user and replace it with `self.request()`.

## Notes

- `diag_eval` executes raw Erlang — use only for test-infrastructure operations, never for production-facing test assertions.
- Methods that accept both GET and POST (e.g. `retry_rebalance`, `rebalance_settings`) select the verb based on whether mutation params are provided.
