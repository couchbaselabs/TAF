#!/bin/bash
#
# Lightweight executor for the Capella V4 REST API suites
# (conf files whose modules are all Capella.RestAPIv4.*).
#
# Same Jenkins parameters as executor_script_cloud.sh, minus everything a
# REST-only suite never uses: no server install, no testrunner/guides
# clones, no populateIni, no pip/maven/Go/DocLoader/sirius, no killall.
# What's left is: pin capellaAPI, write the ini, run the conf, report.
#
#   - lib/capellaAPI is used at the commit TAF pins, never --remote, so a
#     push to CapellaRESTAPIs can't silently change every V4 job.
#   - gradle dependencies are resolved once with retries, then the suite
#     runs --offline, so a Maven/JCenter blip can't kill a run midway.
#   - the whole suite runs under a hard timeout (V4_SUITE_TIMEOUT).
#   - every phase is timed; a setup failure says which phase failed
#     instead of ending as a run with "no tests".
#
# Optional environment / job parameters:
#   V4_SUITE_TIMEOUT   hard limit for the test run (default 8h)
#   GRADLEW            gradle wrapper to use (default ./gradlew)
#   JYTHON             jython binary (default /opt/jython/bin/jython)
#   CAPELLA_API_REF    check out lib/capellaAPI at this ref instead of the
#                      pinned commit (for testing a CapellaRESTAPIs change)
#   V4_CHERRYPICK_DONE set to 1 when the Jenkins shell already applied
#                      $cherrypick before calling this script

set -u
set -o pipefail

WORKSPACE=${WORKSPACE:-$(pwd)}
cd "$WORKSPACE" || exit 2
touch "$WORKSPACE/.executor_lock" 2>/dev/null

JYTHON=${JYTHON:-/opt/jython/bin/jython}
[ -n "${GRADLEW:-}" ] && GRADLEW_EXPLICIT=1
GRADLEW=${GRADLEW:-./gradlew}
V4_SUITE_TIMEOUT=${V4_SUITE_TIMEOUT:-8h}
INI_OUT=/tmp/testexec_v4.$$.ini
RUN_START_MARKER="$WORKSPACE/.v4_run_start.$$"
CURRENT_PHASE="init"

log() { echo "[v4-executor $(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*"; }

phase() {
  CURRENT_PHASE="$1"
  PHASE_START=$(date +%s)
  echo
  log "======== PHASE: $1 ========"
}

phase_done() {
  log "-------- done: $CURRENT_PHASE ($(( $(date +%s) - PHASE_START ))s)"
}

setup_failure() {
  echo
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  echo "  V4 EXECUTOR SETUP FAILURE in phase '$CURRENT_PHASE'"
  echo "  $*"
  echo "  No tests were run. This is an infrastructure failure, not a"
  echo "  test failure."
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  echo "newState=available" > propfile
  exit 3
}

# retry <attempts> <first-delay-seconds> <command...>
retry() {
  local attempts=$1 delay=$2 n=1
  shift 2
  until "$@"; do
    if [ "$n" -ge "$attempts" ]; then
      log "command failed after $n attempts: $*"
      return 1
    fi
    log "attempt $n failed, retrying in ${delay}s: $*"
    sleep "$delay"
    n=$((n + 1))
    delay=$((delay * 2))
  done
}

param() {
  # Value of KEY in the comma-separated $parameters string, if any.
  echo ",${parameters:-}," | grep -o ",$1=[^,]*" | head -1 | cut -d= -f2-
}

# gradle 6.x can't run on Java 16+; use an installed Java 8 when needed.
pin_java8_for_gradle() {
  local java_bin="java" version major home candidate java8_home=""
  [ -n "${JAVA_HOME:-}" ] && java_bin="$JAVA_HOME/bin/java"
  version=$("$java_bin" -version 2>&1 | awk -F'"' '/version/ {print $2; exit}') || true
  case "$version" in
    1.*) major=$(echo "$version" | cut -d. -f2) ;;
    *)   major=$(echo "$version" | cut -d. -f1) ;;
  esac
  case "$major" in ''|*[!0-9]*) return 0 ;; esac
  [ "$major" -lt 16 ] && return 0
  for candidate in /usr/lib/jvm/java-8-openjdk-* /usr/lib/jvm/java-1.8.0-openjdk* /usr/lib/jvm/jre-1.8.0-openjdk*; do
    for home in "$candidate" "$candidate/jre"; do
      if [ -x "$home/bin/java" ]; then
        java8_home="$home"
        break 2
      fi
    done
  done
  if [ -n "$java8_home" ]; then
    export JAVA_HOME="$java8_home"
    log "Default Java is $version; using JAVA_HOME=$JAVA_HOME"
  else
    log "WARNING: default Java is $version and no Java 8 under /usr/lib/jvm"
  fi
}

###########################################################################
phase "validate inputs"
: "${confFile:?confFile is required}"
: "${capella_api_url:?capella_api_url is required}"
: "${tenant_id:?tenant_id is required}"
: "${capella_user:?capella_user is required}"
: "${capella_password:?capella_password is required}"
parameters=${parameters:-}
rerun_params=$(echo "${rerun_params:-}" | xargs)
[ -f "$confFile" ] || setup_failure "conf file '$confFile' not found"
[ -x "$JYTHON" ] || setup_failure "jython not found at $JYTHON"
command -v python3 >/dev/null || setup_failure "python3 not found"
log "conf=$confFile pod=$capella_api_url build_tag=${BUILD_TAG:-none}"
phase_done

###########################################################################
phase "downstream job params"
# Same files executor_script_cloud.sh writes, for the log/cleanup jobs.
{
  echo "descriptor=${descriptor:-}"
  echo "UPSTREAM_BUILD_NUMBER=${BUILD_NUMBER:-}"
  echo "addPoolServers=${addPoolServers:-}"
  echo "version_number=${version_number:-}"
  echo "is_dynamic_vms=false"
} > cleanup_job_params
{
  echo "test_job_url=${JOB_URL:-}"
  echo "test_job_build=${BUILD_NUMBER:-}"
  echo "test_name=${descriptor:-}"
  echo "addPoolServers=${addPoolServers:-}"
  echo "version_number=${version_number:-}"
  echo "is_dynamic_vms=false"
} > savejoblogs_job_params
echo "servers=None" > aws_cleanup_job_params
echo "newState=available" > propfile
phase_done

###########################################################################
phase "source"
if [ "${V4_CHERRYPICK_DONE:-0}" != "1" ] && [ -n "${cherrypick:-}" ] \
    && [ "${cherrypick}" != "None" ]; then
  log "Applying cherrypick: $cherrypick"
  sh -c "$cherrypick" || setup_failure "cherrypick failed: $cherrypick"
fi
# Pinned capellaAPI (NOT --remote). Retried: GitHub hiccups are common.
retry 3 10 git submodule update --init --force lib/capellaAPI \
  || setup_failure "could not check out lib/capellaAPI"
if [ -n "${CAPELLA_API_REF:-}" ]; then
  log "Overriding lib/capellaAPI with ref $CAPELLA_API_REF"
  (cd lib/capellaAPI && retry 3 10 git fetch origin "$CAPELLA_API_REF" \
     && git checkout -q FETCH_HEAD) \
    || setup_failure "could not check out capellaAPI ref $CAPELLA_API_REF"
fi
log "TAF $(git rev-parse --short HEAD), capellaAPI $(git -C lib/capellaAPI rev-parse --short HEAD)"
phase_done

###########################################################################
phase "ini"
TEMPLATE="b/resources/capella/provisioned-template.ini"
if [ -n "${iniFile:-}" ] && [ -f "$WORKSPACE/$iniFile" ]; then
  TEMPLATE="$WORKSPACE/$iniFile"
fi
TEMPLATE="$TEMPLATE" INI_OUT="$INI_OUT" python3 - <<'PYEOF' || setup_failure "could not write ini"
import configparser, os, re
cfg = configparser.RawConfigParser()
cfg.optionxform = str
cfg.read(os.environ["TEMPLATE"])
if not cfg.has_section("capella"):
    cfg.add_section("capella")
pod = os.environ["capella_api_url"].strip()
pod = re.sub(r"^https?://", "", pod).rstrip("/")
# Same rewrite executor_script_cloud.sh does: V4 lives on cloudapi.<pod>.
if pod.startswith("api."):
    pod = "cloud" + pod
values = {
    "pod": pod,
    "tenant_id": os.environ["tenant_id"],
    "capella_user": os.environ["capella_user"],
    "capella_pwd": os.environ["capella_password"],
    "project_id": os.environ.get("project_id", ""),
    "region": os.environ.get("capella_region", ""),
    "server_version": os.environ.get("version_number", ""),
    "dataplane_id": os.environ.get("capella_dataplane_id", ""),
    "cb_image": os.environ.get("cbs_image", ""),
    "access_key": os.environ.get("access_key", ""),
    "secret_key": os.environ.get("secret_key", ""),
    "override_token": os.environ.get("override_token", ""),
    # Same as executor_script_cloud.sh (543492d8c): CMEK tests read it from
    # [capella] via APIBase._get_cmek_arn().
    "cmek_arn": os.environ.get("cmek_arn", ""),
}
for key, value in values.items():
    if value:
        cfg.set("capella", key, value)
    elif cfg.has_option("capella", key):
        # Drop template placeholders (e.g. first.last@...) entirely: an
        # empty "region:" or "server_version:" would hide the tests' own
        # defaults (capella.get(key, default) returns "" for a present key).
        cfg.remove_option("capella", key)
with open(os.environ["INI_OUT"], "w") as f:
    for section in cfg.sections():
        f.write("[%s]\n" % section)
        for key, value in cfg.items(section):
            f.write("%s:%s\n" % (key, value))
        f.write("\n")
print("pod=%s" % pod)
PYEOF
sed -E 's/^(capella_pwd|secret_key|access_key|override_token|password|rest_password):.*/\1:<redacted>/' "$INI_OUT"
phase_done

###########################################################################
phase "preflight"
# Seconds instead of hours: a wrong pod, DNS/VPN problem or bad
# credentials otherwise surfaces as every test failing in setUp.
INI_OUT="$INI_OUT" TR_PARAMS="$parameters" python3 - <<'PYEOF' || setup_failure "preflight failed (see above)"
import base64, configparser, json, os, re, ssl, sys, time
try:
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError, URLError
except ImportError:
    sys.exit(0)
cfg = configparser.RawConfigParser()
cfg.read(os.environ["INI_OUT"])
pod = cfg.get("capella", "pod")
user = cfg.get("capella", "capella_user")
pwd = cfg.get("capella", "capella_pwd")
# lib/capellaAPI logs in on api.<env> (internal_url), V4 is cloudapi.<env>.
internal = "https://" + (pod[len("cloud"):] if pod.startswith("cloudapi.") else pod)
auth = base64.b64encode(("%s:%s" % (user, pwd)).encode()).decode()
ctx = ssl.create_default_context()
last = None
for attempt in range(1, 4):
    try:
        req = Request(internal + "/sessions", method="POST",
                      headers={"Authorization": "Basic " + auth})
        with urlopen(req, timeout=30, context=ctx) as resp:
            jwt = json.loads(resp.read().decode() or "{}").get("jwt")
            if jwt:
                print("preflight: login to %s as %s OK" % (internal, user))
                break
            last = "no jwt in response"
    except HTTPError as e:
        if e.code in (401, 403):
            print("preflight: %s rejected the credentials for %s (HTTP %d)"
                  % (internal, user, e.code))
            sys.exit(1)
        last = "HTTP %d" % e.code
    except (URLError, OSError) as e:
        last = str(e)
    print("preflight: attempt %d: %s" % (attempt, last))
    time.sleep(10 * attempt)
else:
    print("preflight: could not log in to %s: %s" % (internal, last))
    sys.exit(1)

# The requested server version must be one this pod deploys, otherwise the
# shared cluster create fails with 422 (code 4039) for every suite.
params = dict(kv.split("=", 1) for kv in os.environ.get("TR_PARAMS", "").split(",")
              if "=" in kv)
# Same precedence and default as APIBase._get_server_version().
raw = params.get("server_version") or (cfg.get("capella", "server_version")
                                       if cfg.has_option("capella", "server_version") else "") or "7.6"
wanted = ".".join(raw.split("-")[0].split(".")[:2]) if raw else ""
provider = params.get("providerType", "aws")
tenant = cfg.get("capella", "tenant_id")
try:
    req = Request("%s/v2/organizations/%s/clusters/deployment-options/v2?provider=%s"
                  % (internal, tenant, provider),
                  headers={"Authorization": "Bearer " + jwt})
    with urlopen(req, timeout=30, context=ctx) as resp:
        versions = json.loads(resp.read().decode())["serverVersions"]
    offered = [o["key"] for o in versions.get("options", [])]
except Exception as e:
    print("preflight: WARNING: could not read deployment options (%s); "
          "not checking the server version" % e)
    sys.exit(0)
if wanted not in offered:
    print("preflight: server version %s (from %r) is not offered by this pod "
          "for %s; it offers %s (default %s). Set version_number / "
          "server_version accordingly." % (wanted, raw, provider, offered,
                                            versions.get("defaultOptionKey")))
    sys.exit(1)
else:
    print("preflight: server version %s is offered on %s (%s)"
          % (wanted, provider, offered))
PYEOF
phase_done

###########################################################################
phase "parameters"
POD_HOST=$(grep '^pod:' "$INI_OUT" | cut -d: -f2-)
if ! echo "$POD_HOST" | grep -q "sandbox"; then
  if grep -q "SUPPORT" "$confFile"; then
    if [ -z "$(param EXCLUDE_GROUP)" ]; then
      log "Non-sandbox pod: excluding GROUP=SUPPORT (needs internal support token)"
      parameters="${parameters:+$parameters,}EXCLUDE_GROUP=SUPPORT"
    else
      log "WARNING: non-sandbox pod and EXCLUDE_GROUP=$(param EXCLUDE_GROUP) given; SUPPORT tests will fail if not excluded"
    fi
  fi
fi
# Server + dp-agent logs of the run's cluster, collected before it is
# destroyed (listed in logs/collected_logs_<suite>.log). collect_logs=False
# in the job's parameters turns it off.
if [ -z "$(param collect_logs)" ]; then
  parameters="${parameters:+$parameters,}collect_logs=True"
fi
log "parameters=$parameters"
log "rerun_params=$rerun_params"
phase_done

###########################################################################
phase "gradle dependencies"
pin_java8_for_gradle
# ./gradlew downloads Gradle 6.9 on an agent that has never run it (one
# more network dependency per run). Agents already hold the Gradle 6.x the
# old executor's guides/gradlew uses, so prefer a cached 6.x install.
if [ -z "${GRADLEW_EXPLICIT:-}" ] && [ "$GRADLEW" = "./gradlew" ]; then
  cached_gradle=$(ls -d "${GRADLE_USER_HOME:-$HOME/.gradle}"/wrapper/dists/gradle-6.*-bin/*/gradle-6.*/bin/gradle 2>/dev/null \
    | sort -t- -k2 -V 2>/dev/null | tail -1)
  if [ -n "$cached_gradle" ] && [ -x "$cached_gradle" ]; then
    GRADLEW="$cached_gradle"
    log "Using cached Gradle: $GRADLEW"
  fi
fi
[ -x "$GRADLEW" ] || setup_failure "gradle wrapper $GRADLEW not found"
# Configuring the testrunner task (args/jython set) resolves the runtime
# classpath; 'classes' compiles src/. Retried, then the run goes offline.
retry 3 30 "$GRADLEW" --no-daemon -q classes -P jython="$JYTHON" ${sdk_client_params:-} -P args="--resolve-only" \
  || setup_failure "gradle could not resolve dependencies / compile"
phase_done

###########################################################################
if [ "${fresh_run:-true}" == "false" ]; then
  phase "find tests to rerun"
  echo "" > rerun_props_file
  "$GRADLEW" --no-daemon --offline --stacktrace rerun_job -P jython="$JYTHON" \
      -P args="${version_number:-} --executor_jenkins_job --manual_run" \
    || log "WARNING: rerun_job lookup failed; running the full conf"
  if [ -z "$rerun_params" ] && [ -s rerun_props_file ]; then
    rerun_params=$(cat rerun_props_file | xargs)
  fi
  phase_done
fi

###########################################################################
phase "run tests"
mkdir -p logs
touch "$RUN_START_MARKER"
TR_ARGS="-i $INI_OUT -c $confFile"
[ -n "$parameters" ] && TR_ARGS="$TR_ARGS -p $parameters"
TR_ARGS="$TR_ARGS -m rest $rerun_params"
log "testrunner args: $TR_ARGS"
run_suite() {
  timeout --kill-after=300 "$V4_SUITE_TIMEOUT" \
    "$GRADLEW" --no-daemon "$@" testrunner -P jython="$JYTHON" ${sdk_client_params:-} \
      -P args="$TR_ARGS"
}
run_suite --offline
status=$?
if [ "$status" -eq 124 ] || [ "$status" -eq 137 ]; then
  log "WARNING: suite hit V4_SUITE_TIMEOUT=$V4_SUITE_TIMEOUT and was stopped"
elif [ -z "$(find logs -mindepth 1 -maxdepth 1 -type d -newer "$RUN_START_MARKER" 2>/dev/null)" ]; then
  # gradle died before testrunner started (e.g. offline cache incomplete):
  # one online attempt before calling it an infrastructure failure.
  log "testrunner never started (gradle exit $status); retrying online once"
  run_suite
  status=$?
  [ -z "$(find logs -mindepth 1 -maxdepth 1 -type d -newer "$RUN_START_MARKER" 2>/dev/null)" ] \
    && setup_failure "testrunner did not start (gradle exit $status)"
fi
log "testrunner exit status $status"
phase_done

###########################################################################
phase "report"
# Same "<version> - <suite> - <os> (pass/total)" description as the old executor.
desc2=${desc2:-$(echo "${descriptor:-}" | awk '{split($0,r,"-");print r[1],r[2]}')}
python3 scripts/v4_api_report.py --logs logs --since "$RUN_START_MARKER" \
    --json "$WORKSPACE/v4_report.json" \
    --desc-prefix "${version_number:-} - ${desc2:-} - ${os:-}" \
  || log "WARNING: report generation failed (results are still in logs/)"
rm -f "$RUN_START_MARKER"
phase_done

###########################################################################
if [ "${rerun_job:-false}" == "true" ]; then
  phase "record rerun data"
  "$GRADLEW" --no-daemon --offline --stacktrace rerun_job -P jython="$JYTHON" \
      ${sdk_client_params:-} \
      -P args="${version_number:-} --executor_jenkins_job --run_params=${parameters}" \
    || log "WARNING: rerun_job bookkeeping failed"
  phase_done
fi

# Test failures are reported through the JUnit XML (UNSTABLE), not the
# exit code; a non-zero exit here only means the executor itself broke.
exit 0
