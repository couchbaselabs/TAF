#!/bin/bash

# Execution phase of executor_script.sh, split out so the Jenkinsfile can run
# cleanup/install/test-execution as three distinct pipeline stages - see
# cleanup_phase.sh and install_phase.sh for the other two. Only meant to be
# run after install_phase.sh has exited 0 in the SAME workspace (it reuses the
# ini files, rerun_props_file and repo checkouts install_phase.sh left behind).
#
# Uses $BUILD_NUMBER instead of $$ for the shared ini filenames, matching
# install_phase.sh - see the comment there for why.
#
# A few things that live in the middle of the original monolithic script were
# moved here verbatim because they are purely test-execution concerns and only
# depend on Jenkins job parameters (not on anything install_phase.sh computed
# in-process, which wouldn't survive into this separate shell anyway):
#   - Go toolchain setup for the sirius_go_sdk doc loader
#   - the `parameters` mutation (appending upgrade_version=/aws sts creds)
# Both are recomputed here identically to how install_phase.sh's shell would
# have computed them, since they're deterministic functions of job parameters.

cleanup_dir_before_exit() {
  rm -rf .git b build conf pytests DocLoader lib couchbase_utils test_infra_runner
  # Remove any installers downloaded locally
  rm -rf *.deb *.rpm *.msi
}

load_docs_using=$(echo "$parameters" | grep -oP 'load_docs_using=\K[^,]*')
if [[ "$load_docs_using" == "sirius_go_sdk" ]]; then
  # Setup GoLang in local dir
  go_version=1.22.4
  echo "Setting up Golang ${go_version} for sirius"
  wget https://golang.org/dl/go${go_version}.linux-amd64.tar.gz --quiet
  tar -xzf go${go_version}.linux-amd64.tar.gz
  rm -f go${go_version}.linux-amd64.tar.gz
  export GOPATH=`pwd`/go
  export PATH="${GOPATH}/bin:${PATH}"
  export GO111MODULE=on
fi
# Set desired python env (fresh shell - install_phase.sh's exports don't carry over)
export PYENV_VERSION="3.10.14"
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
pyenv local $PYENV_VERSION

# Updating upgrade_version for upgrade jobs
if [ "$parameters" = "" ]; then
    parameters="upgrade_version=${version_number}"
else
    parameters="${parameters},upgrade_version=${version_number}"
fi

# Passing aws_access_key and aws_secret_key for analytics test cases
if [[ "$component" = "analytics" || "$component" = "columnar" ]]; then
  read AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN <<< \
    $(aws sts assume-role \
      --role-arn arn:aws:iam::516524556673:role/jenkins-qe-analytics \
      --role-session-name session \
      --duration-seconds 43200 \
      --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
      --output text)

  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
  parameters="${parameters}"
fi

desc2=`echo $descriptor | awk '{split($0,r,"-");print r[1],r[2]}'`

# Trim whitespaces to detect empty input
rerun_params=$(echo "$rerun_params" | xargs)
if [ "$rerun_params" == "" ]; then
  # Only if user has no input given, get rerun data from
  # the file created by prev. rerun_jobs.py script
  rerun_file_data=$(cat rerun_props_file)
  if [ "$rerun_file_data" != "" ]; then
    rerun_params="$rerun_file_data"
  fi
fi

echo "Building Java doc-loader using mvn"
mkdir -p logs
cd DocLoader
mvn clean compile package > ../logs/sirius_build.log
if [ $? -ne 0 ]; then
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  echo "   Exiting.. Maven build failed"
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  cd ..
  cleanup_dir_before_exit
  exit 1
fi
cd ..

# ---- NFS client pre-cleanup (handles stale mounts from aborted jobs) ----
# Server-side old share pruning is handled by setup_server_new.sh (MAX_SHARES).
if [ "${component}" = "fusion" ]; then
  python scripts/fusion_scripts/nfs_setup.py cleanup_clients \
      --ini "$WORKSPACE/testexec.$BUILD_NUMBER.ini"
fi

# ---- NFS setup for fusion tests (runs once before all tests in confFile) ----
CLIENT_SHARE_DIR=""
NFS_FUSION_SERVER_IP=""
if [ "${component}" = "fusion" ]; then
  echo "NODE_NAME=$NODE_NAME"
  NFS_SETUP_OUTPUT=$(python scripts/fusion_scripts/nfs_setup.py setup --ini "$WORKSPACE/testexec.$BUILD_NUMBER.ini" --node-name "$NODE_NAME" --local-scripts-path "scripts/fusion_scripts")
  echo "$NFS_SETUP_OUTPUT"
  CLIENT_SHARE_DIR=$(echo "$NFS_SETUP_OUTPUT" | grep "^CLIENT_SHARE_DIR=" | cut -d'=' -f2)
  NFS_FUSION_SERVER_IP=$(echo "$NFS_SETUP_OUTPUT" | grep "^NFS_SERVER_IP=" | cut -d'=' -f2)
  parameters="${parameters},skip_fusion_setup=True,client_share_dir=${CLIENT_SHARE_DIR},nfs_server_ip=${NFS_FUSION_SERVER_IP}"
fi

# Find free port on this machine to use for this run
# Use a random starting point in the ephemeral range (49152-65535) to reduce
# collisions with concurrent Jenkins jobs on the same executor.
sirius_port=$(( 49152 + RANDOM % (65535 - 49152) ))
port_iterations=0
while [ "$(ss -tulpn | grep LISTEN | grep ":${sirius_port} " | wc -l)" -ne 0 ]; do
  sirius_port=$((sirius_port+1))
  if [ "$sirius_port" -gt 65535 ]; then
    sirius_port=49152
  fi
  port_iterations=$((port_iterations+1))
  if [ "$port_iterations" -gt $((65535 - 49152)) ]; then
    echo "ERROR: No free ports available in ephemeral range"
    exit 1
  fi
done
echo "Selected Sirius port: ${sirius_port}"
set -x
if [[ "$load_docs_using" == "sirius_go_sdk" ]]; then
  echo "Launching Sirius GO SDK to load documents."
  python testrunner.py -c $confFile -i $WORKSPACE/testexec.$BUILD_NUMBER.ini -p $parameters --launch_sirius_docker --sirius_url http://localhost:$sirius_port ${rerun_params}
else
  echo "Launching java/magma doc loader to load documents."
  python testrunner.py -c $confFile -i $WORKSPACE/testexec.$BUILD_NUMBER.ini -p $parameters --launch_java_doc_loader --sirius_url http://localhost:$sirius_port ${rerun_params}
fi
set +x

# ---- NFS teardown for fusion tests (runs once after all tests complete) ----
if [ "${component}" = "fusion" ] && [ -n "${CLIENT_SHARE_DIR}" ]; then
  CLUSTER_SERVER_IPS=$(grep -oP '^ip:\K.+' "$WORKSPACE/testexec.$BUILD_NUMBER.ini" | paste -sd',' -)
  python scripts/fusion_scripts/nfs_setup.py teardown --nfs-server-ip "$NFS_FUSION_SERVER_IP" --cluster-ips "$CLUSTER_SERVER_IPS" --client-share-dir "$CLIENT_SHARE_DIR"
fi

set -x
awk -F' ' 'BEGIN {failures = 0; total_tests = 0} /<testsuite/ {match($0, /failures="([0-9]+)"/, failures_match); match($0, /tests="([0-9]+)"/, tests_match); if (failures_match[1] > 0) {failures += failures_match[1];} total_tests += tests_match[1]} END {print "Aggregate Failures: " failures ", Aggregate Total Tests: " total_tests;}' $WORKSPACE/logs/*/*.xml
python scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --run_params=${parameters}
status=$?
set +x
if [ $status -ne 0 ]; then
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  echo "Non-zero exit while running rerun_jobs.py"
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  exit $status
fi

# To reduce the disk consumption post run
cleanup_dir_before_exit
