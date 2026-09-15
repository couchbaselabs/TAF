# CRL Automation — Implementation Log & Flow Reference

Living document tracking **how** the CRL (Certificate Revocation List) TAF automation is built,
and everything an agent needs to debug a failing test or get oriented fast. Read this first if
you're picking up this work cold (human or AI agent) — it explains the flow end-to-end, what
every function does, and the specific gotchas that have already burned this project more than
once, without needing to re-derive any of it from git history or re-read old conversations.

For **what** CRL is and **why** it exists, see the sibling docs:
- [`CRL_PRD.md`](./CRL_PRD.md) — product requirements + ns-server implementation-state audit
- [`CRL_API_Contract.md`](./CRL_API_Contract.md) — exact REST field/type/error reference
- [`CRL_MANUAL_VALIDATIONS.md`](./CRL_MANUAL_VALIDATIONS.md) — manual QA findings, known bugs/gaps
- [`CRL_TEST_PLAN.md`](./CRL_TEST_PLAN.md) — the original P0 test-case list this automation implements
- [`pytests/security/CRL_AGENTS.md`](./pytests/security/CRL_AGENTS.md) — architecture doc for the
  test suite
- `/Users/kushagrakinjawadekar/ns_server/doc/CRL KV + Nserv Test plan` — the fuller 145-row
  KV+ns_server test plan, kept in sync with this suite's `Automated(Y/N)`/status columns as of
  the last audit (see §6)
- `/Users/kushagrakinjawadekar/ns_server/doc/CRL_missing_automation_and_manual.md` — the
  gap tracker (27 entries; only #23 still open). **Its `Status:` lines drift behind the code** —
  see §0 item 16 before planning work off it.

This doc is about **sequencing, current state, and hard-won lessons** — what's implemented, how
it works, what's still open, and every gotcha already found the hard way so nobody has to
rediscover them. Update it whenever a module lands, a bug is found, or a new gotcha surfaces.

---

## 0. If a test case fails — check this first

Before assuming a fresh product bug, rule out these known, already-diagnosed causes. Every one of
these has actually happened in this project, more than once in some cases.

1. **"Freshest CRL wins per issuer" cross-contamination.** If a sub-case inside a multi-part test
   method (e.g. an isolated expired-CRL or untrusted-CA check that runs after other sub-cases in
   the same method) gets an unexpected `valid`/`active` result instead of `undetermined`/
   `expired`/`untrusted`, suspect a **leftover CRL file from an earlier sub-case in the same test,
   for the same CA** — the server only ever considers the single freshest CRL per issuer for some
   purposes, but falls back to an older-but-still-valid file for that same issuer once the
   freshest one degrades (expires/becomes untrusted), rather than treating the issuer as having no
   usable CRL at all. Fix: call `self._cleanup_created_files()` immediately before the isolated
   sub-case, so only its own file exists for that CA. This exact pattern has independently bitten
   `test_crl_bypass_hardening`'s race test, `test_crl_restart_persistence`'s post-restart re-upload,
   `test_crl_health_warnings`' untrusted-CA sub-case, and `test_crl_diagnostics_endpoints`'
   expired-CRL sub-case. If you clean up files mid-test for this reason, double-check nothing
   *later* in the same test still depends on a file you just deleted.

2. **`enable_tls_on_nodes` setup failure (`"Services did not honor enforce tls"`) is not a CRL bug
   — it's node-to-node distribution TLS being broken on the shared test node.** This runs at the
   start of *every* test's `setUp()` (framework-level, not CRL-specific) and actively toggles real
   inter-node encryption via `couchbase-cli enable-n2n-encryption`/`set-n2n-encryption-level`. If it
   fails, check `GET /pools/default/certificates` for a `"Certificate is not signed with cluster
   CA."` warning — that means the node's active leaf cert and its trusted CA have desynced (see §4
   incident write-up). A plain service restart does **not** reliably fix this; `POST
   /controller/regenerateCertificate` sometimes fixes it once, but it has recurred even after that.
   If it keeps recurring, the node likely needs a full reinstall, not another REST-level patch —
   don't spend more than one or two attempts chasing it before flagging it to whoever owns the
   shared infra.

3. **The persistent shell's working directory can silently revert** to
   `/Users/kushagrakinjawadekar/ns_server` between calls instead of staying in
   `/Users/kushagrakinjawadekar/TAF`, producing `python: can't open file '.../testrunner.py'`
   errors that look like a broken environment but are just a stale `cd`. Always prefix test-run
   commands with an explicit `cd /Users/kushagrakinjawadekar/TAF &&` rather than relying on a
   previously-set directory.

4. **Strict mode does not exist.** Dev-confirmed (`Strict` == `Require` was the plan, then `Strict`
   was dropped from spec entirely). `CRLUtils.POLICY_MODES` only has `Disabled`/`Permissive`/
   `Require`. Don't write or expect a test for it; if an old planning doc mentions it, it's stale.

5. **`diagnostics/validate`'s real status enum is 4 values** (`valid`/`revoked`/`undetermined`/
   `failed`), not the 8-value enum (`not_revoked`/`unknown_missing_crl`/`unknown_expired_crl`/
   `unknown_untrusted_issuer`/`invalid_certificate_chain`/`invalid_crl`/`unsupported`) some early
   planning docs expected. Missing/expired/untrusted-issuer all collapse to `undetermined`,
   distinguished only by the free-text `details` field, not a distinct status value.

6. **`diagnostics/validate` bypasses the cluster's real configured policy** — it always evaluates
   against the `policy` request parameter, independent of what `/settings/crl` actually has
   configured. `/_cbauth/crlsValidate` is the opposite: it has no override parameter and always
   honors the real configured policy. A test comparing a diagnostics verdict against a real live
   handshake outcome must explicitly set the cluster's *real* policy to match what's passed to
   `diagnostics/validate`, or the two will legitimately disagree.

7. **A real TLS handshake with a cert from an untrusted (or never-trusted) CA never produces a
   `(CRL)`-tagged runtime log line.** It gets rejected at the generic TLS chain-validation layer,
   before `cb_crl:apply_policy` ever runs. If you need to observe/assert that specific runtime log
   line for an untrusted-issuer scenario, call `diagnostics/validate` instead (with a cert from
   that issuer) — it invokes the exact same `apply_policy` code path directly and does produce the
   log line, matching its own response `details` text.

8. **Untrusting a CA takes a moment to reach `cb_crl_manager`'s cache.** Calling
   `diagnostics/validate` (or attempting a handshake) immediately after
   `CRLUtils.untrust_ca_by_cn()` risks a race — poll `diagnostics/status` for that file's
   `cacheStatus == "untrusted"` first (see `test_crl_diagnostics_endpoints`/
   `test_crl_auditing_logs_and_metrics` for the proven pattern) before asserting on the
   validate/handshake outcome.

9. **`cb_crl_manager`'s push-config `version`** (`CRLUtils.get_push_config_version`) only bumps for
   "hashed" config keys (`policyPerScope`, `checkIntermediateCerts`), CRL file changes, and trusted-CA
   changes. "Operational" keys (`dirPollIntervalMs`, `urlPollIntervalMs`) never affect it, and a
   genuine no-op re-post of an unchanged value correctly does not bump it either — don't mistake
   either for a bug.

10. **A newly-joined-node fail-open race (MB-73216) and a rebalanced-out-node resetting its own CRL
    policy to `Disabled`** were both investigated live this project and **closed as expected
    behavior**, not bugs — see §4. Don't re-open either without new evidence; the mechanisms are
    documented there in detail.

11. **A TLS 1.3 rejection does not raise from `wrap_socket` the way 1.2 does** — it surfaces as
    `session_reused=False` and *no HTTP response at all* (`response_line is None`). If you're
    writing a 1.3 assertion and expecting `ssl.SSLError` with "revoked" in it, you'll get a
    confusing pass-through instead. `test_crl_bypass_hardening` (`crl_test.py:4184-4205`) accepts
    both shapes deliberately — that's a real client-side behavioural difference, not test slop.

12. **`self.rest` on `CRLBase` is a `RestConnection`, not a `ClusterRestAPI`.** Their `add_node`
    signatures differ — `RestConnection.add_node(user, password, remoteIp, port, zone_name,
    services)` takes no `host_name` kwarg. A `TypeError: got an unexpected keyword argument` here
    means you read the wrong class's signature, not that the call is wrong in principle.

13. **For tests that perform many topology changes, use `self.cluster_util.rebalance(...)`, not
    `self.task.rebalance(...)`.** The task version works from a cached cluster object that goes
    stale after several add/remove cycles and fails with an `IndexError` deep inside `task.py`;
    `ClusterUtils.rebalance` re-reads live state via `get_nodes(..., inactive_added=True)`. See
    `pytests/rebalance_new/swaprebalancetests.py` for the established REST-level pattern.

14. **`testrunner.py -t` silently runs only the FIRST test in a comma-separated list.** The banner
    says "Total test to be executed: 1" and everything looks fine. Pass one `-t` per invocation
    (a shell loop) when running several.

15. **`test_crl_rebalance_and_failover_enforcement_continuity` needs a 4th node in the ini pool**
    despite `nodes_init=3` — the unequal add-2/remove-1 and swap sub-cases use `servers[3]` as the
    spare. A 3-node ini fails with an `IndexError` inside the rebalance task, which looks like a
    framework bug rather than a missing fixture. CI runs it via the 4-node template.

16. **Before implementing a "gap" from a tracker or audit, grep the code to confirm it still
    exists.** On 2026-09-14 all four remaining "not automated" entries in the ns_server tracker
    (#10, #11, #12, #13) turned out to be **already implemented** — closed by `d2117067f` and
    `f2109f09b` without the entries being updated. Tracker entries are written at discovery time
    and the fix usually lands folded into some other test, so drift in the *done* direction is the
    norm. Also: make the grep specific and **don't pipe an existence check through `head`** — a
    truncated result reads exactly like an absent one, which is how #12 got mis-reported as open.

---

## 1. Ticket & PR tracker

**Jira:** CBQE-9010 — "CRL automation: foundational REST/util/test-base layer" (early PRs, layer
by layer)
**Jira:** CBQE-9041 — "CRL automation: module-by-module test coverage" (tracks each
`CRL_TEST_PLAN.md`/`CRL KV + Nserv Test plan` module's automation as one ongoing ticket rather than
one ticket per module)

### Foundational layer (CBQE-9010)

| Commit | Contents |
|---|---|
| `e44c43c03` | `CRLUtils` crypto generation slice (CA/leaf/CRL fixtures) |
| `5ff97f84b` | Fixed upload timeout handling; extended crypto for RSA/ECDSA |
| `9a2b671ef` | `CRLUtils` REST orchestration + mTLS handshake helper |
| `791b19934` | `CRLBase` test base class (setUp/tearDown, CA trust, RBAC helpers) |
| `16b000fb9` | Fixed CA-trust install-path bug (see §4); `test_settings_and_file_lifecycle` |

### Module-by-module coverage (CBQE-9041), in landing order

| Commit | Module / test method(s) |
|---|---|
| `43dbfbb60` | `CRL_Core.File_Lifecycle` — `crl_file_lifecycle.py` (6 methods) |
| `e61385e96` | `test_crl_trust_and_signature_boundary`, `test_crl_temporal_validity_lifecycle` |
| `a5fd22364` | `test_crl_url_poll_ingestion`, `test_crl_settings_scope_independence_and_ingestion` |
| `43b4e85d3` | `test_crl_policy_mode_matrix` |
| `1d9f923c7` | `test_crl_tampered_duplicate_and_empty_crl_handling` |
| `a547c04c7` | `test_mtls_client_cert_auth_mode_matrix`, `test_mtls_revocation_ordering_and_cross_ca_isolation` |
| `cb7ca8853` | `test_crl_diagnostics_endpoints` (initial) |
| `cf9c4323a` | `test_crl_auditing_logs_and_metrics` (initial) |
| `251d97ce5` | `test_crl_rbac_permission_boundaries` |
| `a9f1d6510` | `test_crl_cert_chain_usage_encoding` |
| `bb0c14754` | `test_crl_bypass_hardening` |
| `3e73404c9` | `test_crl_restart_persistence` |
| `58608846c` | `test_crl_hot_reload_and_node_scoping`, `test_crl_health_warnings` |
| `6047c6705` | `test_crl_cross_service_kv_vs_ns_server_consistency` |
| `d0043aafe` | `test_crl_performance_upload_timeout_and_handshake_overhead` |
| — | `test_crl_rebalance_and_failover_enforcement_continuity` (rebalance/failover continuity + delete-propagation, folded across a few commits below) |
| `273957eea` | Delete-propagation + un-revoke-via-reissue checks |
| `6188e7555` | Additional audit checks; rebalance/failover test cleanup |
| `4364d931f` | Expired-CRL distinction + live-handshake parity in `test_crl_diagnostics_endpoints` |
| `79b1ca9c3` | `test_crl_cbauth_crls_validate` — `/_cbauth/crlsValidate` contract |
| `65bf52fc5` | (unrelated to CRL) suite-wide version-target rename 8.1 → 8.5 |
| `b9ffd1bcd` | `test_crl_cbauth_push_config` — push-config version/policy semantics |
| `0910d2749` | Directory-path validation (invalid/unreadable) in `test_settings_and_file_lifecycle` |
| `78918f4c2` | Fixed `crl_file_lifecycle.py`'s two schema-guessing tests against the confirmed real API; added `CRLUtils.find_file_entry`/`find_diagnostics_file_entry`, deduplicating 2 local closures in `crl_test.py` |
| `9408b2fe1` | Runtime log-line coverage for an already-loaded CRL whose issuing CA becomes untrusted |
| `5e2ed5333` | `nodeToNode` breadth — server-cert direction, `ccv=false`, Permissive, OOTB, capi listener |
| `f2109f09b` | Status-vocabulary enforcement everywhere; upload request-shape + DELETE-filename validation (tracker #11, #13) |
| `d2117067f` | Remaining **P2** gaps — bad files in poll dir, internal-identity scope, `onlySomeReasons`, **TLS 1.3 + `checkIntermediateCerts` resumption** (#12), **verdict-cache hit path + no-op-reload version invariant** (#10) |
| `28516ba88` | Remaining **P3** gaps |
| `0a18cfa87` | CE-edition rejection of CRL (and JWT) settings (tracker #22) |
| `8a9aabde6` | Coverage-audit remediation, batch 1 — 11 over-claiming assertions tightened |
| `543c7e96e` | Coverage-audit remediation, batch 2 — concurrent CRL writes, swap rebalance, **per-node enforcement** in hot-reload |

**Current state: all P0/P1/P2/P3 work in the plan that's feasible in this environment is
automated**, including a full remediation pass against an independent 145-row coverage audit (see
§7). The only genuinely open work is `Cluster_Ops.Upgrade_Mixed_Version` (needs real multi-version
provisioning TAF can't drive alone) plus items blocked on a server-side fault-injection hook that
does not exist. A full cross-check against the source 145-row plan (both files read in full, every
row checked) found **100 of 145 rows already covered**, with **95 of those** the source plan itself
still listed as "Not Started"/"Blocked" — that plan has since been corrected to match reality
(see §6).

---

## 2. The flow, end to end

The simplest call path a test follows, from "generate a CA" to "assert a revoked cert is
rejected." Most tests build on this same skeleton with extra steps interleaved (policy changes,
diagnostics checks, audit-log assertions, multi-node probing, etc.) — see §3 for what each test
method actually adds on top of it.

```
1. Test setUp (CRLBase) generates a CA in memory
      CRLUtils.generate_ca(cn, key_algorithm="rsa2048"|"ecdsa_p256")
      -> writes the CA cert PEM to the target node's inbox/CA folder over SSH,
         then calls RestConnection.load_trusted_CAs() to make the cluster trust it
         (CRLBase._trust_ca_on_cluster)

2. Test generates a leaf cert signed by that CA
      CRLUtils.generate_leaf_cert(ca_cert, ca_key, cn, key_algorithm=...)
      -> returns (cert, key, serial) — serial is what gets revoked later

3. Test configures the cluster to require CRL checking
      CRLUtils.set_settings(rest, policyPerScope={"clientAuth": "Require"})
      -> CRLAPI.post_crl_settings() -> POST /settings/crl

4. Test uploads a baseline (non-revoking) CRL so the cert can authenticate at all
      CRLUtils.upload_file(rest, filename, pem_bytes)
      -> CRLAPI.upload_crl_file() -> POST /settings/crl/files (multipart)

5. Test performs a real mTLS handshake with the (not-yet-revoked) leaf cert
      CRLUtils.perform_mtls_handshake(host, port, cert_path, key_path)
      -> expect HTTP 200 (or use CRLUtils.probe_mtls_state / tls_handshake_ok
         for a tri-state "connected"/"rejected"/"timeout" read instead)

6. Test revokes the leaf cert's serial and re-uploads the CRL
      CRLUtils.revoke_and_upload(rest, ca_cert, ca_key, serial, filename)
      -> CRLUtils.build_crl(..., revoked_serials=[serial]) then upload_file()
      CRLUtils.reload_crl(rest) -> CRLAPI.reload_crl() -> POST /node/controller/reloadCrl

7. Test retries the same mTLS handshake
      -> expect requests.exceptions.SSLError (TLS-layer rejection, not an HTTP status —
         a revoked cert never gets far enough to receive an HTTP response)

8. Test tearDown (CRLBase) cleans up: deletes uploaded CRL files, resets
   policyPerScope to Disabled, disables clientCertAuth, removes RBAC test users,
   untrusts any extra CAs the test trusted, removes temp PEM files
```

Everything below `CRLUtils`/`CRLAPI` talks to a real Couchbase Server node; everything above
`generate_ca`/`generate_leaf_cert`/`build_crl` is pure in-memory `cryptography` — no network, no SSH.

---

## 3. Function-by-function reference

### Layer 1 — `couchbase_utils/cb_server_rest_util/security/crl.py` :: `CRLAPI(CBRestConnection)`

Transport only — one method per REST endpoint, no crypto, no test logic.

| Function | Does |
|---|---|
| `get_crl_settings()` | `GET /settings/crl` — current cluster-wide CRL config |
| `post_crl_settings(payload)` | `POST /settings/crl` — partial update (SET semantics) |
| `get_crl_files()` | `GET /settings/crl/files` — list uploaded CRL file metadata |
| `upload_crl_file(filename, content_bytes, timeout=300)` | `POST /settings/crl/files`, multipart. Bypasses `CBRestConnection.request()` (no multipart support there) — has its own retry-until-`timeout` loop, so transient connection errors don't crash the test before a real HTTP outcome can be asserted on. `timeout` is caller-configurable because large CRLs (200k+ entries) can take minutes server-side. |
| `delete_crl_file(filename)` | `DELETE /settings/crl/files/:filename` |
| `get_diagnostics_status(nodes=None)` | `GET /settings/crl/diagnostics/status[?nodes=...]` |
| `post_diagnostics_status(nodes=None)` | Same, node list as JSON body (for long lists) |
| `post_diagnostics_validate(policy=None, certs=None)` | `POST /settings/crl/diagnostics/validate` — admin diagnostic endpoint, bypasses the cluster's actually-configured policy in favor of the `policy` param |
| `reload_crl()` | `POST /node/controller/reloadCrl` — force reload on local node only |
| `cbauth_crls_validate(certs, scope)` | `POST /_cbauth/crlsValidate` — internal endpoint cbauth-registered GO services (Query/FTS/Analytics/Indexer/XDCR) actually call; unlike diagnostics/validate, always honors the real configured policy and requires `[admin,internal]` |

### Layer 2 — `couchbase_utils/security_utils/crl_utils.py` :: `CRLUtils` + module-level helpers

**Crypto (in-memory, `cryptography` library — no network/SSH):**

| Function | Does |
|---|---|
| `_generate_private_key(key_algorithm)` | Internal. RSA-2048 or ECDSA-P256, the two algorithms most common in real customer PKI. |
| `generate_ca(cn, key_algorithm="rsa2048", valid_days=3650)` | Self-signed CA cert/key pair, with `key_cert_sign`+`crl_sign` usage. |
| `generate_intermediate_ca(parent_cert, parent_key, cn, key_algorithm=..., valid_days=1825, path_length=0)` | Same CA shape as `generate_ca` but signed by a parent CA — for 3-tier chain tests. Returns `(cert, key, serial)`. **CRL-signing trust does not chain through the certificate hierarchy** — the intermediate must be separately, explicitly trusted via `_trust_ca_on_cluster` even though its own *certificate* validly chains to an already-trusted root. |
| `generate_leaf_cert(ca_cert, ca_key, cn, key_algorithm=..., valid_days=825, extended_key_usage=None, crl_distribution_url=None, dns_names=None, serial=None)` | Leaf cert signed by `ca_cert`/`ca_key`. Returns `(cert, key, serial)`. `serial=` forces a specific serial (for deliberate cross-CA serial collisions). `dns_names` is for node certs (SAN). |
| `build_crl(ca_cert, ca_key, revoked_serials=None, this_update=None, next_update=None, crl_number=None, expired=False)` | Builds and signs a CRL. `expired=True` sets `this_update`/`next_update` safely in the past (60d/30d back respectively, specifically ordered so `this_update` always precedes `next_update` regardless of which one the caller also overrides). Passing mismatched `ca_cert`/`ca_key` deliberately forges the issuer name vs. signing key, for AKI-mismatch tests. |
| `pem_crl_to_der(pem_bytes)` | PEM CRL → DER bytes. |
| `cert_to_pem(cert)` / `cert_to_der(cert)` / `cert_to_der_b64(cert)` / `key_to_pem(key)` | Serialization helpers. `cert_to_der_b64` is the format `/_cbauth/crlsValidate` and `diagnostics/validate`'s base64-DER mode both expect. |

**REST orchestration** (every method takes `rest` first, builds a `CRLAPI` via `_crl_api()`):

| Function | Does |
|---|---|
| `get_settings(rest)` / `set_settings(rest, **fields)` | `(status_bool, parsed_content)` wrappers |
| `list_files(rest)` / `upload_file(rest, filename, pem_bytes, timeout=300)` / `delete_file(rest, filename)` | File lifecycle wrappers |
| `find_file_entry(files, filename)` | Static. Finds `filename`'s entry in a plain `list_files()` response by filename. Returns `None` if absent — caller decides whether that's a failure. **This endpoint's entries have no status field at all** — `cacheStatus` only exists on the diagnostics endpoint below. |
| `find_diagnostics_file_entry(diagnostics_content, node_key, filename)` | Static. Same, but for a `diagnostics_status()` response's `{node_key: {crlFiles: [...]}}` shape — this is where `cacheStatus` (`active`/`expired`/`untrusted`/etc.) actually lives. |
| `diagnostics_status(rest, nodes=None)` / `diagnostics_validate(rest, policy=None, certs=None)` | Diagnostics wrappers. `diagnostics_validate`'s `policy` overrides the cluster's real configured policy — see §0.6. |
| `reload_crl(rest)` / `cbauth_crls_validate(rest, certs, scope)` | Reload + internal-endpoint wrappers |
| `revoke_and_upload(rest, ca_cert, ca_key, serials, filename, timeout=300, **crl_kwargs)` | Convenience: `build_crl()` + `upload_file()` in one call — the single most-used helper across the suite |
| `untrust_ca_by_cn(rest, cn)` | Untrusts every currently-trusted CA whose subject contains `cn`, via a direct `chronicle_kv` edit through `diag_eval` — **there is no REST endpoint for untrusting a single CA**. Returns `(status_bool, content)`. See §0.8 for the propagation-delay gotcha, and §4 for why heavy reliance on this exact pattern across many days of manual testing is implicated in the node cert/CA desync incident. |
| `get_push_config_version(rest)` | Reads `cb_crl_manager:get_push_config/0`'s `version` field via `diag_eval` (no REST endpoint exposes this). See §0.9 for what does/doesn't bump it. |
| `get_push_config_policy_per_scope(rest)` | Same push-config's `policy_per_scope`, translated into the `{"clientAuth": ..., "nodeToNode": ...}` shape `/settings/crl` uses, for direct comparison. |
| `get_all_metrics_text(server)` / `get_metric_value(server, metric_name, labels=None)` | Reads `/metrics` (Prometheus text) as one string, or a specific metric's numeric value (`None` if the metric doesn't exist at all — distinct from "exists but 0"). |
| `get_crl_alert_count(server, alert_type)` / `wait_for_crl_alert_increment(server, alert_type, baseline, ...)` / `get_alert_messages(rest)` | Health-warning helpers — `cm_alerts_triggered_total` counter reads/polling, and the human-readable alert text from `GET /pools/default`. |
| `tls_handshake(host, port, cert_path, key_path, tls_version="1.2"|"1.3", ...)` / `perform_mtls_handshake(...)` / `tls_handshake_ok(...)` / `time_tls_handshake(...)` | Real mTLS via plain `requests`/raw `ssl`, deliberately with `verify=False` — the node's own serving cert lacks `CA:TRUE`, so it can't be a `verify=` trust anchor, and these tests only care about the *client* cert's CRL-driven outcome, not server identity. |
| `probe_mtls_state(ip, port, cert_path, key_path, timeout=5)` | Tri-state single probe: `"connected"` / `"rejected"` / `"timeout"`. Prefer this over hand-rolled probe logic — a hand-rolled raw-socket probe script produced a false "connected" reading once this session, root-caused to a bug in the throwaway script itself, not the product (see §4). |
| `probe_during(trigger, target_ips, port, cert_path, key_path, ...)` | Runs `trigger` (a callable — e.g. a rebalance/failover kickoff) while continuously probing `target_ips` in the background, returning every sampled state across the whole operation. The standard tool for "no enforcement gap during X" tests. |
| `wait_for_failover_count(cluster_util, master, expected_count, timeout, ...)` | Polls until the cluster's failover-event count reaches `expected_count`. |
| `wait_for_crl_log_text(shell_conn, debug_log_path, ip, port, cert_path, key_path, expected_substrings, max_wait=30, interval=3)` | Repeatedly probes `ip:port` (expecting rejection) and greps `debug_log_path`'s `"(CRL)"` lines until every string in `expected_substrings` appears. Needed because the exact wording a live rejection gets can lag behind a per-file status transition visible elsewhere. **Only works when the handshake itself reaches `cb_crl:apply_policy`** — see §0.7 for the untrusted-issuer case where it won't, because the TLS layer rejects first. |
| `get_identity_via_mtls(host, port, client_cert_path, client_key_path, ...)` | Confirms a cert maps to the expected RBAC identity post-handshake (not just that the handshake itself succeeded). |
| `assert_settings_equal(actual, expected_subset)` / `assert_diagnostics_entry(entry, ...)` | Assertion helpers. |
| `setup_url_poll_crl_env(*, crl_utils_obj, cluster_master, rest, ca_cert, ca_key, ...)` / `cleanup_url_poll_crl_env(env)` | Module-level functions (not `CRLUtils` methods — mirror `jwt_utils.setup_jwks_uri_issuer_env`'s shape). Serve a CRL over a throwaway HTTP server on the cluster node and point `urls`/`urlPollIntervalMs` at it. Cleanup kills the server **by port** (`stop_process_on_port`), not by the `$!`-captured PID from start time — see §4 for why. |

**Log/audit helpers (module-level, not `CRLUtils` methods):**

| Function | Does |
|---|---|
| `find_remote_pid(shell_conn, pattern)` | Finds a process's PID by matching `pattern` in `ps`. |
| `tail_remote_log(shell_conn, log_path, lines=200)` | Plain tail — can miss a specific event if enough unrelated log volume happens in between. Prefer `grep_remote_log` when checking for one specific event. |
| `grep_remote_log(shell_conn, log_path, pattern, lines=20)` | Last `lines` lines matching a literal string — the reliable way to find a specific event's log line regardless of surrounding volume. |
| `get_audit_event(shell_conn, log_path, event_id, lines=1000)` | Parses `current-audit.log` directly (one compact JSON object per line) and returns the last entry matching `event_id`. **Does not use TAF's existing `audit_ready_functions.audit`** — that utility reads the node's `audit.json` descriptor, which is encrypted at rest on this server build, breaking it before any event can be read. |
| `audit_keyword_count(shell_conn, log_path, keyword, lines=1000)` | Substring occurrence count in the audit log — used for "did a new event fire at all" checks. |
| `stop_process_on_port(shell_conn, port)` | `lsof -ti:<port> \| xargs kill -9` — kills whatever is actually listening on a port, regardless of how it was started. |

### Layer 3 — `pytests/security/crl_base.py` :: `CRLBase(OnPremBaseTest)`

| Function | Does |
|---|---|
| `setUp()` | Self-heals stuck `clientCertAuth`/stale trusted CAs first (see below), gates on EE, generates+trusts a default `TestCA1`, sets up cleanup-tracking lists |
| `tearDown()` | Deletes uploaded CRL files, resets **every** `/settings/crl` field to default, disables `clientCertAuth`, removes RBAC test users, untrusts extra CAs, removes temp PEM files — each step in its own try/except so one failure doesn't skip the rest |
| `_require_crl_supported()` | Fails immediately if not Enterprise Edition. No compat-version check — this suite assumes it always runs against Totoro+ (8.1+, now 8.5+) clusters. |
| `_self_heal_stuck_client_cert_auth()` | Runs at the very start of `setUp()`, before `super().setUp()`, over plain HTTP (port 8091, no TLS) — if a prior test crashed while `clientCertAuth=mandatory` was active, every subsequent HTTPS request would otherwise get locked out, including the framework's own setup calls. |
| `_self_heal_stuck_trusted_cas()` | Untrusts every CA except the node's own auto-generated one before this test's own CA gets trusted — cleans up leftovers from a previous crashed/interrupted run. Logs a warning naming how many stale CAs it found. |
| `_ca_dir(shell)` / `_trust_ca_on_cluster(ca_cert, server=None)` | Writes a CA PEM to the node's real `inbox/CA` folder over SSH (OS-detected install path, matching `x509main._get_install_path()`), then calls `load_trusted_CAs()`. Each CA gets its own remote filename (derived from CN + serial) so a second call doesn't silently overwrite/un-trust an earlier one. |
| `_ca_remote_filename(ca_cert)` | Static. Builds that unique per-CA filename. |
| `_track_uploaded_file(filename)` / `_cleanup_created_files()` | Cleanup tracking for uploaded CRL files. |
| `_reset_crl_settings()` | Resets **every** `/settings/crl` field to its documented default — not just `policyPerScope`. A test that configures `urls` and doesn't reset it leaves the cluster polling a dead URL indefinitely, generating continuous 404 warnings with no test running to explain them. |
| `_disable_client_cert_auth()` | Resets `clientCertAuth` to `disable` over **plain HTTP**, not HTTPS — `mandatory` mode only enforces on the TLS listener, so going through HTTPS here risks the reset call itself getting locked out by the state it's trying to clear. |
| `_enable_client_cert_auth(state="enable", prefixes=None)` | Counterpart. Defaults to `"enable"`, not `"mandatory"` — mandatory would also lock out the test's own admin REST calls. |
| `_write_temp_pem(pem_bytes, suffix=".pem")` / `_cleanup_temp_pem_files()` | Local temp-file helpers for handshake functions that need filesystem paths, not just bytes. |
| `_cleanup_trusted_cas()` | Untrusts every CA this test explicitly trusted via `_trust_ca_on_cluster`, via the same `chronicle_kv` edit pattern as `untrust_ca_by_cn`. Safe to call more than once (empty-list early return). |
| `_create_rbac_test_user(username, role, password=...)` / `_grant_rbac_role(...)` / `_cleanup_rbac_users()` | RBAC test-user provisioning/live-role-change/cleanup via `RbacUtils`. |

### Layer 4a — `pytests/security/crl_test.py` :: `CRLTest(CRLBase)`

Consolidated home for essentially all CRL enforcement/config/lifecycle test methods — new coverage
lands as additional methods on this one class, "minimal test cases, full coverage": broad,
multi-scenario methods rather than one test per fine-grained case.

| Test method | Covers |
|---|---|
| `test_settings_and_file_lifecycle` | Baseline settings CRUD + file upload/list/delete round-trip + restore defaults + invalid/unreadable `directory` path validation (accepted at POST time regardless of validity; a nonexistent path settles to a silent `"notFound"` with no error surfaced, an existing-but-permission-denied path settles to `"unreadable"` with a populated `errors` list). |
| `test_crl_trust_and_signature_boundary` | A CRL's trust/signature actually applies only to the cert it covers: cross-CA scope isolation (same serial, two different trusted CAs — one revoked, one not, independently), a CRL from a never-trusted CA rejected outright, a forged-issuer-name-but-wrong-key CRL also rejected (signature must validate against the real key, issuer name alone isn't enough). |
| `test_crl_temporal_validity_lifecycle` | `thisUpdate`/`nextUpdate` window enforcement: future-`thisUpdate` rejected at upload; active CRL revokes correctly; once `nextUpdate` passes, `Require` stays fail-closed; already-expired-at-upload is tolerated either way (rejected outright, or accepted with an `expired`-flagged status). |
| `test_crl_url_poll_ingestion` | `urls`/`urlPollIntervalMs` fetches and applies a CRL from a throwaway HTTP endpoint on its own timer, confirmed via server-side `debug.log` (`fetch_one_url` → `200` → `install_url_crl`), not just the client assertion. |
| `test_crl_settings_scope_independence_and_ingestion` | Three checks on one fixture: `nodeToNode`/`clientAuth` policy update independently; a CRL dropped directly into the poll directory over SSH (bypassing the upload endpoint) gets picked up by the background poller; `checkIntermediateCerts` toggle on a 3-tier chain — revoking the *intermediate's own* serial only causes rejection when the toggle is on. |
| `test_crl_policy_mode_matrix` | Walks `Disabled → Permissive → Require`, re-checking a revoked, missing-CRL, and expired-CRL cert at each mode, asserting immediately after each policy-change POST (doubling as the hot-transition check at no extra cost). `Strict` mode struck outright — doesn't exist. |
| `test_crl_tampered_duplicate_and_empty_crl_handling` | Signature-tampered CRL (last DER byte flipped post-signing — still parses, fails signature validation) rejected at upload; validly-signed empty CRL accepted, revokes nothing; duplicate-serial entry accepted, revokes exactly as a single entry would. |
| `test_mtls_client_cert_auth_mode_matrix` | Optional vs. Mandatory `clientCertAuth` × {no cert, valid cert, revoked cert}; Optional+revoked must not silently fall back to password auth even when valid password creds are also presented. |
| `test_mtls_revocation_ordering_and_cross_ca_isolation` | Revocation checked before identity/RBAC evaluation (a revoked cert mapped to a highly-privileged RBAC user still dies at the TLS layer); a valid cert is unaffected by an unrelated CA's loaded CRL; cross-CA isolation under a deliberate serial collision. |
| `test_crl_diagnostics_endpoints` | `diagnostics/status` response shape; concurrent `reloadCrl` idempotency; the real 4-value status enum including untrusted-issuer collapsing to `undetermined`; policy-override behavior (and `policy=Disabled` itself rejected); no-certs cluster-cert mode; 100/101-cert boundary; CA untrusted after its CRL was already loaded (`cacheStatus` flips to `"untrusted"`, and the "freshest CRL wins" gotcha caught live during development — see §0.1/§4); expired-vs-missing-CRL distinction via the `details` text; a parity check between a diagnostics verdict and a real live mTLS handshake for the same certs, with the cluster's real policy explicitly set to match; per-node down-node behavior (explicit `nodes=` surfaces an error; the default call silently omits it — asserted as a known gap). |
| `test_crl_auditing_logs_and_metrics` | All 4 CRL audit events (`8307`–`8310`) with field-level checks (actor, `filename`, the full merged `settings` payload matching the REST response); the missing old-policy-value gap pinned as a known product limitation; revoked-cert rejection and RBAC-denial both audited via **generic** events, not CRL-specific ones; no serial/PEM leakage into either debug.log or the audit log; revoked/missing/expired/already-loaded-then-untrusted-CA CRL all producing distinguishable runtime log text (the last one via `diagnostics/validate`, not a real handshake — see §0.7); confirms no `cm_crl_load` metric exists at all; `cm_crl_status_checks_total` increments correctly for fresh valid/revoked checks. |
| `test_crl_rbac_permission_boundaries` | 9 admin-facing CRL endpoints × 3 roles (full/read-only/zero-permission); unauthenticated → 401 distinct from unauthorized → 403; a live role downgrade takes effect on the very next request, no caching. |
| `test_crl_cbauth_crls_validate` | `/_cbauth/crlsValidate`'s real-policy enforcement (no override param, unlike diagnostics/validate) and per-scope independence; validation edge cases (empty certs, bad scope, bad base64, unsupported field, exactly-100-vs-101 certs); malformed-but-valid-base64 input decode-only-when-enforcing behavior; multi-cert chain response-order preservation; its distinctly narrower `[admin,internal]` RBAC requirement vs. every other CRL endpoint's `[admin,security]`. |
| `test_crl_cbauth_push_config` | `cb_crl_manager`'s push-config `version`: bumps for hashed config keys, CRL file changes, and trusted-CA changes; stays put for a genuine no-op and for operational-only keys (poll intervals); the pushed `policy_per_scope` always matches the real configured policy; `notify_crl_change/0` is callable; per-service `cm_cbauth_crl_cache_*` metrics exist with correct shape (their hit/miss counters need real downstream Go-service activity to actually increment — outside ns_server's control, not asserted). |
| `test_crl_cert_chain_usage_encoding` | Root-direct vs. intermediate-issued revocation; a fully untrusted chain rejected at chain validation (distinct "unknown ca" alert) before revocation is even considered; EKU-agnostic checking (`SERVER_AUTH` and an unrelated `EMAIL_PROTECTION` EKU both check identically — no distinct "unsupported EKU" status); PEM vs. base64-DER encoding evaluated identically; a multi-block concatenated PEM chain parsed as separate results; two distinct malformed-input failure shapes (valid-base64-garbage → per-cert `"failed"`, not-valid-base64-at-all → 400); AKI/SKI disambiguation between two trusted CAs sharing a subject CN. |
| `test_crl_bypass_hardening` | TLS 1.2 session resumption of a since-revoked cert forces a full re-check, not resume-and-skip; DER leading-zero-padded serial still matches; all 4 revocation reason codes reject equally; a CRL with an unrecognized critical extension rejected at upload (error must name "critical extension"); a background prober plus 5 rapid delete/re-upload/reload cycles never shows a transient weaker-enforcement window; `clientAuth`/`nodeToNode` scope independence under enforcement (not just config); revoking a cert doesn't revoke a same-named password-based RBAC credential. |
| `test_crl_restart_persistence` | Config/file persistence and enforcement correctness across three restart mechanisms — graceful full-node restart, killing just the `ns_server` child process, and an unclean `kill -9` of the top-level babysitter — with continuous 1s-interval probing across the *entire* outage+recovery window on all three, zero observed "connected" samples for a revoked cert throughout. |
| `test_crl_hot_reload_and_node_scoping` | Explicit `reloadCrl` applies a newly-revoking CRL on the very next connection, no restart; directory-polling and `reloadCrl` are genuinely per-node (unlike uploaded CRL *content*, which chronicle-replicates cluster-wide regardless); deleting the only revoking file + reload restores access under `Permissive`; re-issuing a newer CRL that omits a previously-revoked serial restores access under `Require` too, with no file deletion needed ("freshest CRL wins per issuer" used constructively here, not as a bug) — confirmed via a genuine identity-mapping check (`whoami`), not just the TLS gate. |
| `test_crl_health_warnings` | `crl_expires_soon` fires proactively inside its own proportional warning window (`min(configured 3-day window, 1/4 of total validity)`, confirmed from source); the same CRL later flips to a distinctly-worded `crl_expired`; the `alerts_triggered` counter increments on every ~60s tick for as long as any CRL remains expired; a CRL whose issuing CA becomes untrusted correctly flips `cacheStatus` to `"untrusted"` but triggers **no health warning of either type** — asserted as a known gap (only `crl_expired`/`crl_expires_soon` alert types exist at all). |
| `test_crl_cross_service_kv_vs_ns_server_consistency` | KV's memcached SSL listener and ns_server's mgmt HTTPS listener reach the same accept/reject outcome for the same revoked/valid certs — accounting for KV's async ~1s-later connection close vs. ns_server's synchronous in-handshake rejection. |
| `test_crl_performance_upload_timeout_and_handshake_overhead` | A 10k-entry-serial CRL upload with too little timeout fails cleanly within 30s with no partial state, and succeeds functionally on retry with an adequate one; handshake latency under `Require`+5k-entry-CRL doesn't measurably multi-fold-regress vs. `Disabled` (a loose sanity bound, not a tight benchmark). KV-side CRUD latency is explicitly out of scope (KV-owned). |
| `test_crl_rebalance_and_failover_enforcement_continuity` | CRL delete propagates to every cluster node, not just uploads; enforcement survives a rebalance-out/rebalance-in cycle and an auto-failover event with zero observed gap on existing/surviving members via continuous probing. Explicitly documents two separate, disputed-then-closed findings as known/expected (not tested further here): the newly-joined-node fail-open race (MB-73216) and a rebalanced-out node resetting its own CRL policy to `Disabled` — both closed as expected per dev confirmation, see §4. |

### Layer 4b — `pytests/security/crl_file_lifecycle.py` :: `CRLFileLifecycle(CRLBase)`

Separate class, same base — steady-state REST coverage for the CRL file upload/list/delete API
specifically, complementary to (not redundant with) `test_settings_and_file_lifecycle`'s
happy-path round-trip. Registered via `conf/security/py-crl_file_lifecycle.conf`, not
`py-crl_test.conf`.

| Test method | Covers |
|---|---|
| `test_crl_upload_valid_der` | DER-encoded CRL accepted identically to PEM. |
| `test_crl_list_metadata_accuracy` | `issuer`/`thisUpdate`/`nextUpdate`/`crlNumber` metadata in the listed file entry's `entries[0]` matches what was signed in. **No revoked-serial-count field exists anywhere in the real API** — `crlNumber` is the closest available "metadata reflects the signed content" signal, used instead. |
| `test_crl_upload_malformed_rejected` | Truncated/random bytes rejected, not listed. |
| `test_crl_upload_invalid_filename_rejected` | Path traversal, disallowed characters, and >255-char filenames all rejected at upload time. |
| `test_crl_upload_oversized_file` | A 50,000-revoked-serial CRL either hits an undocumented size limit or uploads within an extended timeout with no hang — tolerates either outcome, only fails on a hang/exception. |
| `test_crl_file_status_field_accuracy` | Valid/expired/untrusted-issuer CRL status via `cacheStatus` on **`diagnostics/status`**, not the plain files-list endpoint (which has no status field at all — confirmed live; the class previously guessed at this before the real schema was confirmed). Expired/untrusted uploads may themselves be rejected outright at upload time; both that and an accepted-with-status outcome are treated as valid, logged distinctly. |

---

## 4. Design decisions, incidents & lessons learned

Kept in the order they were found. Read this section before assuming a fresh bug — several of
these are recurring gotchas, not one-off surprises.

**Why Gerrit, not GitHub PRs?** This repo's real review system is `review.couchbase.org` — every
commit needs a `Change-Id` trailer (auto-inserted by the installed `commit-msg` hook) and gets
pushed via `git push gerrit HEAD:refs/for/master`. Gerrit rewrites commit SHAs on submit — after
merging, always re-sync local `master` from `gerrit/master`, not just `origin/master` (a GitHub
mirror that can lag), or local history silently diverges from what's actually merged.

**Why one combined `crl_utils.py` instead of separate crypto-util and helper files?** Matches this
repo's existing `jwt_utils.py`/`credential_store_utils.py` convention — both combine crypto/payload
building and REST orchestration into one class.

**Why RSA 2048 + ECDSA P-256, not literal Vault/AWS-PCA/cert-manager integration?** Couchbase's CRL
verification is pure RFC 5280 X.509 logic — it can't distinguish "this CRL came from Vault" vs
"from OpenSSL." Key algorithm and chain depth are what actually vary and matter for correctness,
not vendor/provenance.

**The `_trust_ca_on_cluster` CA-install-path bug (found by running it live):** the first version
guessed at nonexistent `shell.default_install_dir`/`cb_path` attributes, silently writing the CA
into a bogus path that never actually got trusted. Fixed by resolving the install path the same
way `x509main._get_install_path()` already does. **Lesson:** don't guess at attributes on shared
infra objects — grep for how an existing, proven caller in the same codebase does it first.

**The `perform_mtls_handshake` false-negative bug:** the original implementation passed the
target node's own self-signed serving cert as the `verify=` trust anchor, which OpenSSL rejects
outright (`self-signed certificate in certificate chain`) on every single call, client-side,
before the client cert is ever evaluated. This made every "should be rejected" assertion pass for
the wrong reason and every "should connect" assertion permanently fail. Fixed by dropping
server-identity verification entirely (`verify=False`) — these tests only care about the client
cert's CRL-driven outcome. **Lesson:** when a test's "should fail" assertions all pass but its
"should succeed" assertion never does, suspect the success path is unreachable for an unrelated
reason, not that the failure path is "too strict" — reproduce the exact client call standalone
against the live cluster.

**The `$!`-based PID capture is unreliable over a non-interactive SSH exec channel:** backgrounding
a compound shell list (`cd dir && nohup cmd &`) can make `$!` capture a wrapper/subshell PID
instead of the real process, depending on remote shell job-control semantics — killing the
captured PID succeeds while the actual process, now orphaned, keeps running. Fixed by killing
whatever's actually listening on the port instead (`stop_process_on_port`). **Lesson:** identify a
backgrounded remote process by an observable property (port, self-written PID file), never by
trusting `$!` over SSH.

**`checkIntermediateCerts` toggle needed two fixes:** (1) the leaf needs its own applicable CRL
(even empty) from its actual issuer — the intermediate — before the toggle means anything, else it
fails with `no_relevant_crls` regardless of the toggle, looking identical to a real rejection; (2)
CRL-signing trust doesn't chain through the cert hierarchy — the intermediate must be separately,
explicitly trusted even though its own certificate validly chains to an already-trusted root.

**Running against Python 3.13 breaks `common_api.py`'s pre-existing `from typing import re`** —
removed outright in 3.12+. Always run via the `ven-taf` pyenv virtualenv
(`~/.pyenv/versions/3.10.19/envs/ven-taf`), not system `python3`. **Lesson:** an `ImportError`
inside framework code (not test code), before any test log line appears at all, means check the
interpreter/environment first.

**"Freshest CRL wins per issuer" — a recurring cross-contamination gotcha, hit independently at
least 5 times across this project:** the server considers only the single freshest CRL per issuer
for some purposes, but when that freshest file *degrades* (expires, or its issuer becomes
untrusted), the system falls back to an older-but-still-valid file for that same issuer rather
than treating the issuer as having no usable CRL at all. Any test that uploads multiple CRLs for
the same CA across sequential sub-cases within one test method is at risk — an isolated later
sub-case can silently observe stale state from an earlier one. Fix: `self._cleanup_created_files()`
immediately before the isolated sub-case, verified safe by checking no later code in the same test
still depends on the file being removed. Hit in: the `test_crl_bypass_hardening` concurrent race
test, `test_crl_restart_persistence`'s post-restart re-upload, `test_crl_health_warnings`'
untrusted-CA sub-case, and `test_crl_diagnostics_endpoints`'s expired-CRL sub-case (this last one
also then broke a *later* sub-case that assumed the cleaned-up file's revocation still applied —
fixed by re-uploading a fresh revoking CRL under a new filename right before that later check).

**A hand-rolled raw-socket TLS probe script gave a false "connected" reading** during manual
verification of a rebalanced-out-node scenario — root-caused to a bug in the throwaway script
itself, not the product, by switching to the already-proven `CRLUtils.probe_mtls_state` helper.
**Lesson:** always use the established, tested probe helpers, even for "quick" manual/throwaway
verification — don't hand-roll socket/TLS logic.

**MB-73216 — newly-joined-node fail-open race, investigated and closed as expected behavior:** a
~0.5–7s window was originally reported where a node joining a cluster might briefly enforce its
local-default (`Disabled`) CRL policy instead of the cluster's real one. Re-investigated live,
twice — once as a simple 2-node `addNode`+rebalance, once as the actual 4-node
add-2-remove-1/swap-rebalance scenario the original reproductions used — with real internal
timestamp correlation (not just probe hit/miss, which can trivially miss a sub-second window).
Result both times: the policy correction from `disabled` to the real value lands within
milliseconds of "Join succeeded, starting ns_server_cluster back," and the TLS listener
(`ns_ssl_services_sup`) doesn't even *begin* restarting until after that correction has already
happened — there is no point where the listener is both up and enforcing the wrong policy. The one
"connected" window found during re-testing was strictly *before* the node's join had even begun
(matching a live probe hitting a genuinely-standalone node, not a mid-join gap) — the same category
of finding the dev had already identified for the original repro's own evidence. Closed as
expected; not tested further in this suite (`test_crl_rebalance_and_failover_enforcement_continuity`
documents this explicitly rather than re-asserting it).

**Rebalancing a node out resets its own CRL policy to `Disabled` — investigated and closed as
expected behavior, not a bug:** confirmed live that an ejected node's `/settings/crl` and
`clientCertAuth` reset entirely to defaults the moment it leaves the cluster (traced to
`ns_cluster:perform_leave/0`'s `chronicle_local:leave_cluster()` wiping local chronicle state,
which is where CRL config actually lives — not `ns_config`). Dev's position: an ejected node no
longer needs to honor cluster-specific config at all, by design. This is a *different* mechanism
from MB-73216 above (leave-time config wipe, not a join-time race) and was raised as a separate
question before being closed the same way.

**A shared test node's node-to-node distribution TLS silently broke after extensive manual CA
trust/untrust `chronicle_kv` editing across many days of testing** — repeated direct writes to the
`ca_certificates` chronicle key (via `untrust_ca_by_cn`/`_cleanup_trusted_cas`, since no REST
endpoint exists for single-CA untrust) plus multiple `regenerateCertificate` calls at different
points eventually desynced a node's active leaf certificate from its own trusted CA
(`GET /pools/default/certificates` showed `"Certificate is not signed with cluster CA."`). This
manifested as `enable_tls_on_nodes` — the framework's own per-test setup step, unrelated to CRL —
failing every subsequent test run with `"Services did not honor enforce tls"`, and node-to-node
Erlang distribution logs full of repeated `TLS ... Unknown CA` alerts in a continuous failing
retry loop. A plain service restart did not fix it; `regenerateCertificate` fixed it once but it
recurred; a full reinstall of the affected nodes was what finally resolved it cleanly. **Lesson:**
heavy, repeated, direct-datastore manipulation of CA trust state — the kind no real deployment
ever does — can accumulate cert/CA inconsistency in ways the product doesn't self-heal from. If
`enable_tls_on_nodes` starts failing consistently (not flakily) on a shared node, check the
certificate-mismatch warning before assuming it's ordinary infra flakiness, and don't be surprised
if a reinstall is the only durable fix.

**`diagnostics/validate` invokes the exact same runtime `cb_crl:apply_policy` code path as a real
handshake, and can be used to observe/trigger its log line — but a real handshake against an
untrusted-issuer cert cannot.** Confirmed live: calling `diagnostics/validate` with a cert from an
already-untrusted CA produces a fresh `"(CRL) Certificate status undetermined ... rejected CRLs:
... (no_issuer_cert_chain)"` line in `debug.log`, immediately. A real TLS handshake with the same
cert gets rejected too, but produces **no** `(CRL)`-tagged line at all — it's rejected at the
generic TLS chain-validation layer before `apply_policy` ever runs. Useful when a test needs to
assert on the specific runtime enforcement wording for an untrusted-issuer scenario without a real
handshake being reachable for it.

**crl_file_lifecycle.py's two schema-guessing tests were confirmed against the real live API and
rewritten** (see the `78918f4c2` commit): the original author's own docstring had already flagged
both as speculative pending a real run. Confirmed: `GET /settings/crl/files` returns
`{filename, checksum, uploadTimestamp, entries: [{issuer, thisUpdate, nextUpdate, crlNumber}]}` —
**no revoked-count field exists anywhere**, and per-file `cacheStatus` only exists on the separate
`diagnostics/status` endpoint, never on the plain files list.

---

## 5. Environment notes specific to this project's test infrastructure

- **Test node roster has shifted several times** across this project's lifetime — nodes at
  `172.23.220.124`/`.125`/`.126` and occasionally `172.23.106.1`/`172.23.222.151` have all been
  used, sometimes in a genuinely mixed-version cluster (8.1 vs. 8.5) for specific rebalance
  investigations. Don't assume a fixed topology; check `GET /pools/default` fresh before relying
  on any assumption about current cluster membership.
- **A node with lower RAM than the cluster's configured memory quota will fail `addNode`** with a
  quota-mismatch error, not a version-compat error — lower the cluster's `memoryQuota` via
  `POST /pools/default` first if this happens (confirmed this is not a version-compat block; mixed
  8.1/8.5 clusters do join at the REST layer).
- **Always run via the explicit invocation pattern**, prefixed with `cd`, activating the correct
  virtualenv:
  ```
  cd /Users/kushagrakinjawadekar/TAF && \
    source /Users/kushagrakinjawadekar/.pyenv/versions/3.10.19/envs/ven-taf/bin/activate && \
    python testrunner.py -i node.ini \
      -t security.crl_test.CRLTest.<method>,nodes_init=N,services_init=...,GROUP=P0,get-cbcollect-info=False,skip_core_dump_check=True,rerun=False
  ```
- **Both conf files matter for full-suite runs**: `conf/security/py-crl_test.conf` (22 methods) and
  `conf/security/py-crl_file_lifecycle.conf` (6 methods). Direct `-t` invocation of a single method
  doesn't read either file, so a missing/stale conf entry only surfaces on a full-suite run.

---

## 6. External test-plan sync

`/Users/kushagrakinjawadekar/ns_server/doc/CRL KV + Nserv Test plan` (145 rows, tab-separated:
Module/Test_function_name/Description/Steps/Expected/Automated/Bug-Status) is the fuller reference
plan this suite is measured against, including KV-owned and other-team-owned rows out of ns_server's
scope entirely. It was last fully cross-checked against this suite's actual current state (both
test files read in full) and corrected in place — 102 of 147 rows now correctly say `Y`/Done with
the exact covering test method named, and every `N` row's status text was corrected to reflect
reality (genuine gap vs. out-of-scope-for-TAF-and-which-team-owns-it vs. resting on a stale spec
assumption like Strict mode or the old 8-value diagnostics enum). Two rows
(`test_crl_cbauth_crls_validate`, `test_crl_cbauth_push_config`) were added for real ns_server-owned
coverage the plan previously had no row for at all.

**If that plan and this doc's §1/§7 ever disagree** about what's covered, trust a fresh read of the
actual test files over either document — both are reference material, not the source of truth.

---

## 7. What's actually still open

Everything else in the original test plan that's feasible in this environment is done, including
the full remediation of an independent 145-row coverage audit (2026-09-11 → 09-14). What remains,
in priority order:

1. **`Cluster_Ops.Upgrade_Mixed_Version`** — the largest real gap. `test_upgrade_crl_config_survives_online_upgrade`
   needs a genuine online-upgrade harness (TAF alone can't drive a version upgrade); the
   Require-policy-under-mixed-version half of `test_upgrade_mixed_version_blocks_strict_require`
   (the Strict half is moot — dropped from spec) needs real multi-version provisioning.
2. **`test_health_warning_partial_enforcement_mixed_version`** — same multi-version provisioning
   dependency as above. Note the audit's row 95 asked for a related mixed-version divergence check;
   that one is **structurally impossible**, not merely unimplemented — an upgraded node's
   `GET /settings/crl` itself 404s while any pre-CRL node remains, so `policyPerScope` cannot
   diverge across nodes in the first place.
3. **Upload-timeout *retry* path** (audit row 78) — **deliberately skipped.** The existing test's
   `0.01s` deadline structurally guarantees zero retries (the retry loop's own `time.sleep(3)`
   alone exceeds the budget), so it proves nothing about retry. A real version needs a timed
   iptables block/unblock inside the upload window using `remote_util.enable_firewall`. Judged not
   worth the CI flakiness; revisit only if the retry path actually regresses.
4. **`test_system_long_running_mixed_workload_no_fail_open`** — needs a dedicated long-running
   system-test harness outside this suite's scope.
5. Blocked on capabilities that do not exist, not on effort — `test_bypass_fail_closed_on_internal_error`
   and audit rows 103/127/141 (no server-side fault-injection hook to force a genuine internal
   evaluator exception), and `test_bypass_clock_skew_does_not_unrevoke` (no safe clock-manipulation
   harness on shared infra). Tracked as entry #23 in the ns_server tracker, the only entry there
   still open.

**`test_rebalance_swap_equal_nodes` is now covered** (was listed here as low-value/not-done) — the
swap-rebalance case landed in `543c7e96e`, asserting the incoming node enforces CRL immediately
with no unenforced window.

### Audit rows closed as *not* gaps

Worth knowing before someone re-opens them: rows **62, 67, 86, 95, 107** were each investigated
against product source and confirmed to be asking for behaviour the product structurally cannot
produce — e.g. row 67 wants a distinct `(CRL)` log line for an untrusted-CA cert, but such a cert
is rejected by OTP's chain validation before `cb_crl:apply_policy` runs at all (see §0 item 7).
Row **69** was a genuine product gap and became **MB-73929** (no CRL load success/failure metric),
not a test fix.

Everything KV-owned, SDK-owned, CLI/UI-owned, XDCR-internals, backup/restore-internals, and
CAO/K8s-integration is correctly out of ns_server's/this suite's scope — see the external test
plan (§6) for the full accounting of which team owns each.
