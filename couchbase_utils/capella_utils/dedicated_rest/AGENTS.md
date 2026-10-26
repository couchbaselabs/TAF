---
name: capella-dedicated-rest-agent
description: >
  Capella dedicated REST variants - on-prem REST classes resolved to Capella
  variants at object creation so on-prem tests run on Capella unchanged.
model: inherit
---

# capella_utils/dedicated_rest

**Goal:** run on-prem tests on Capella dedicated clusters without changing
call sites, while exercising the Capella control plane where it exists.

## How it works

1. `CBRestConnection.__new__` (cb_server_rest_util/connection.py): when
   the node has `type == "dedicated"`, it builds the Capella class from
   the map in `CBRestConnection.capella_variant()` instead:

   | Requested class | server.type | Built object |
   |---|---|---|
   | `BucketHelper` (both import paths) | `dedicated` | `CapellaBucketHelper` |
   | `ClusterRestAPI` | `dedicated` | `CapellaClusterRestAPI` |

   The map is hardcoded for the POC; to be made dynamic later.
2. Any other node type -> the requested class, unchanged.
3. Anything Capella could not do is logged as a warning starting with
   `CAPELLA-GAP` (grep the test log for it).
4. Nodes of a dedicated cluster carry `server.capella` (`CapellaContext`:
   pod, tenant, cluster) so a variant built from just a node can call
   `CapellaUtils`. Attached in `__populate_cluster_info` and carried over
   by `RebalanceTaskCapella` / `UpgradeProvisionedCluster` node refresh.

## Files

| File | Responsibility |
|---|---|
| `context.py` | `CapellaContext` - pod/tenant/cluster on the node. Never log tenant |
| `bucket_param_translator.py` | Pure mapping of on-prem bucket params <-> Capella API fields. No HTTP |
| `bucket_helper.py` | `CapellaBucketHelper` - create / delete / flush via `CapellaUtils`, wait till node reflects it, read back & compare |
| `cluster_rest.py` | `CapellaClusterRestAPI` - honest `cluster_tasks()`, raw `node_cluster_tasks()`, unsupported topology/failover calls |
| `scale_spec.py` | `CapellaScaleSpec` - pure: current specs (GET) -> object-form scale payload (POST /specs), min 3 data nodes |
| `rebalance_watcher.py` | `RebalanceWatcher` / `RebalanceReport` - follows a scale: Capella state + ns_server rebalance attempts and `rebalance_method` (FBR) from node tasks |

## Scaling (rebalance on Capella)

`RebalanceTask.__new__` returns the Capella twin `CapellaRebalanceTask`
for dedicated clusters (lib/Jython_tasks/task.py).

## Rules for adding a variant method

- Variant must **subclass** the on-prem class; override only methods with
  a Capella control-plane equivalent (or that must not be faked).
- Keep the on-prem **name, arguments and return contract**
  (`BucketHelper` -> bool, `ClusterRestAPI` -> `(status, content)`).
- Reuse `CapellaUtils`; do not call `lib/capellaAPI` directly, never edit
  the submodule.
- Anything not done through Capella (unsupported param, mismatch after
  read back, unsupported operation) is logged as a `CAPELLA-GAP` warning.
  Never return success for something that did not happen.
- Async control-plane operations must wait for the effect (node REST view
  or Capella cluster state) before returning.
- Parameter mapping / comparison logic goes in a pure translator class so
  it is unit testable without a cluster.

## Not covered yet

- Legacy `lib/membase/api/rest_client.RestConnection` family (different
  base class, needs its own hook).
- Scopes / collections, XDCR, security, backup variants.
- Shell / CLI access to Capella nodes (SSM).
- Unit tests.
