# Capella V4 API suites: HTTP resilience and the light executor

## What runs where

| Piece | File | Purpose |
|---|---|---|
| HTTP resilience | `couchbase_utils/capella_utils/capella_http.py` | Timeouts, retries, one 429 policy and one log line per call for every `lib/capellaAPI` client. Installed by `APIBase.setUp` and `SecurityBase.setUp`. |
| Light executor | `scripts/executor_script_v4_api.sh` | Runs a V4-only conf: pinned capellaAPI, ini written directly, gradle deps resolved once then `--offline`, hard suite timeout, timed phases. |
| Report | `scripts/v4_api_report.py` | Pass/fail totals and every failure sorted into ENV_QUOTA, ENV_PROVISIONING, INFRA_NETWORK, INFRA_TIMEOUT, TEST_CODE, ASSERTION or OTHER. |
| Hand-off | top of `scripts/executor_script_cloud.sh` | A conf whose modules are all `Capella.RestAPIv4.*` is handed to the light executor automatically. |

## Jenkins setup

Nothing has to change in Jenkins: once this is on `master_jython`, the
existing `test_suite_executor_cloud-TAF` job hands V4 confs to the light
executor by itself. Two optional job parameters:

| Parameter | Values | Effect |
|---|---|---|
| `V4_LIGHT_EXECUTOR` | `auto` (default), `false`, `true` | `false` = old executor path (kill switch); `true` = force the light one |
| `V4_SUITE_TIMEOUT` | e.g. `6h` (default `8h`) | Hard limit for the test run |
| `CAPELLA_API_REF` | a CapellaRESTAPIs ref | Test a capellaAPI change without bumping the TAF pin |

To try a change before it is merged, use a dedicated job (or a copy of
`test_suite_executor_cloud-TAF`) whose shell step is:

```bash
if [ -n "$cherrypick" ] && [ "$cherrypick" != "None" ]; then sh -c "$cherrypick"; fi
export V4_CHERRYPICK_DONE=1
bash scripts/executor_script_v4_api.sh
```

Agent requirements: `/opt/jython/bin/jython`, Java 8 (picked automatically
when the default Java is 16+), `python3`, GNU `timeout`, `git`.

Stage/prod pods: the executor adds `EXCLUDE_GROUP=SUPPORT` automatically
(the zone-selection support API needs a sandbox-only token).

## Test params (any V4 conf, `-p` or conf line)

| Param | Default | Meaning |
|---|---|---|
| `capella_http_connect_timeout` / `capella_http_read_timeout` | 30 / 180 s | Per request |
| `capella_http_connect_retries` / `read_retries` / `status_retries` | 3 / 2 / 3 | Connection errors (any method); read errors and 502/503/504 (GET/PUT/DELETE only) |
| `capella_http_backoff` | 1.0 | 1s, 2s, 4s... between retries |
| `capella_http_retry_429` | True | Wait `Retry-After` (or the body's "N request(s) per Ns") and retry; owner-key calls switch keys instead |
| `capella_http_max_429_attempts` / `max_429_wait` | 4 / 120 s | A 429 asking for longer than this (an hourly quota) is returned to the test |
| `capella_http_log_lib_bodies` | False | Bring back lib/capellaAPI's full-body INFO logging |
| `capella_http_idempotent_500_retries` | 2 | A GET/PUT answered with 500 is repeated (POST/DELETE never are) |
| `collect_logs` | True (light executor) | Server + dp-agent logs of the run's cluster before it is destroyed, listed in `logs/collected_logs_<suite>.log` |
| `strict_routing_codes` | False | True = malformed-path / non-hex-ID cases must return exactly the listed code (by default any 400/404/405 rejection passes; a 2xx/5xx still fails) |
| `columnar_instance` | auto | Create the columnar instance only for the columnar suites (`always`/`never`) |
| `delete_shared_projects` | False | Delete the shared WRAPPER/Auth_Project at the end (only for a run alone on its org) |

Rate-limit tests (`make_parallel_api_calls`) turn the 429 policy off for
their calls, so they still count every 429.

## Reading a failure

Every Capella call logs one line:

```
[capella-http] GET /v4/organizations/<org>/projects -> 503 (0.41s) attempt=2 request_id=<id> body=...
```

`request_id` is what to search for in the CP logs. A call that never got a
response raises `CapellaRequestError` (previously lib/capellaAPI turned it
into `SystemExit`, which bypassed every `except Exception`).

The end of the console has the run summary from `v4_api_report.py`; the
same data is in `v4_report.json` in the workspace (archive it).

## Unit tests

```bash
jython unit_tests/test_capella_http.py      # Jython 2.7, urllib3 1.25
python3 unit_tests/test_capella_http.py     # CPython 3, current urllib3
```

## Failing fast and cleaning up

If the run-wide setup fails (login, OrgOwner keys, WRAPPER project, the
shared cluster or its deployment), the test that hit it collects the
cluster's logs, deletes what the run created (app service, cluster,
columnar instance, keys) and every later test fails in about a second
as `NOT RUN - shared setup failed earlier in this run: <reason>`. The run
summary lists those under NOT_RUN and the root cause under its own
category. The executor's preflight logs in before gradle starts, so a
wrong pod or bad credentials stop the job in about a minute (exit 3,
"SETUP FAILURE in phase 'preflight'"). The preflight also checks that the
requested server version is one the pod deploys (its cluster
deployment-options) and lists the offered versions when it is not.

Each test appears once in the JUnit report with its final status,
however many HTTP retries it needed; retries are visible as
`attempt=N` on its `[capella-http]` lines.
