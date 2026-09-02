#!/bin/bash

# Heartbeat marking this workspace as in-use for the disk-cleanup sweep below -
# refreshed at the start of every run, so a workspace this job is (or very
# recently was) actively using never gets reclaimed as "abandoned".
touch "$WORKSPACE/.executor_lock" 2>/dev/null

cleanup_dir_before_exit() {
  # Runs from an EXIT trap, so the cwd is whatever the failing step left
  # behind. Anchor to the workspace first and bail out if that is not
  # possible - never rm -rf whatever directory we happen to have landed in.
  [ -n "$WORKSPACE" ] || return 0
  cd "$WORKSPACE" || return 0
  rm -rf .git b build conf pytests DocLoader lib couchbase_utils test_infra_runner
  # Remove any installers downloaded locally
  rm -rf *.deb *.rpm *.msi
}

# Cleanup on every exit path, including the early `exit`s below. Bash keeps
# the original exit status across an EXIT trap, so this cannot mask a failure.
trap cleanup_dir_before_exit EXIT

setup_test_infra_repo_for_installation() {
  git clone https://${GITHUB_USER}:${GITHUB_TOKEN}@github.com/couchbaselabs/test_infra_runner --depth 1
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to clone test_infra_runner"
    newState=failedInstall
    echo newState=failedInstall>propfile
    exit 1
  fi
  cd test_infra_runner/
  git submodule update --init --force --remote
  pyenv local $PYENV_VERSION
  python -m pip install `cat requirements.txt  | grep -v "#" | grep -v couchbase | xargs`
  cd ..
}

populate_ini() {
  cd test_infra_runner
  set -x
  python scripts/populateIni.py $skip_mem_info \
    -s ${servers} $internal_servers_param \
    -d ${addPoolServerId} \
    -a ${addPoolServers} \
    -i $WORKSPACE/testexec_reformat.$$.ini \
    -p ${os} \
    -o $WORKSPACE/testexec.$$.ini \
    -k '{'${UPDATE_INI_VALUES}'}' \
    --cb_version $version_number \
    --columnar_version "$columnar_version_number" \
    --mixed_build_config "$mixed_build_config" \
    --cluster_profile "$cluster_profile"
  rc=$?
  set +x
  cd ..
  return $rc
}

do_install() {
  echo "Starting server installation"
  cd test_infra_runner
  python scripts/new_install.py -i $WORKSPACE/testexec.$$.ini -p $install_params
  status=$?
  cd ..
}

set +x
echo "Setting ulimit values for this session"
# Set data seg size
ulimit -d unlimited
# Set file size
ulimit -f unlimited
# Set pending signals
ulimit -i 127868
# Set max locked memory
ulimit -l 3075360
# Set max memory size
ulimit -m unlimited
# Set open files
ulimit -n 204200
# Set POSIX message queues
ulimit -q 819200
# Set stack size
ulimit -s 8192
# Set cpu time
ulimit -t unlimited
# Set max user processes
ulimit -u 127868
# Set virtual memory
ulimit -v unlimited
# Set file locks
ulimit -x unlimited

echo "########## ulimit values ###########"
ulimit -a
echo "####################################"

# To clean any available space from docker
docker system prune -f

# To kill Orphan Python / magmaloader.jar
ps -ef | grep 'python testrunner.py' | awk '$3 == 1 {print $2}' | xargs kill -9
ps -ef | grep 'java -' | grep 'magmadocloader' | awk '$3 == 1 {print $2}' | xargs kill -9

# Reclaim disk space from gradle files
for i in `ls /tmp/gradle*.bin`; do
  lsof $i > /dev/null
  if [ $? -eq 1 ]; then
    rm -f $i
  fi
done
for file in `find ~/.gradle/ -name "*.out.log"`; do
    lsof_line_count=`lsof $file | grep -v COMMAND | wc -l`
    if [ $lsof_line_count -eq 0 ]; then
        rm -f $file
    fi
done

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
# Set desired python env
export PYENV_VERSION="3.10.14"
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
pyenv local $PYENV_VERSION

# Find cases for rerun
echo "" > rerun_props_file
if [ ${fresh_run} == false ]; then
 set -x
 python scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --manual_run
 set +x
fi

# Used to pass on to the cleanup job
export is_dynamic_vms=`echo $dispatcher_params | grep -o '"use_dynamic_vms": [^,]*' | cut -d' ' -f2`

echo "###########################################"
echo "  Populating env file for downstream jobs"
echo "1/3 Creating file: cleanup_job_params"
echo "descriptor=$descriptor" > cleanup_job_params
echo "UPSTREAM_BUILD_NUMBER=${BUILD_NUMBER}" >> cleanup_job_params
echo "addPoolServers=$addPoolServers" >> cleanup_job_params
echo "version_number=$version_number" >> cleanup_job_params
echo "is_dynamic_vms=$is_dynamic_vms" >> cleanup_job_params

echo "2/3 Creating file: savejoblogs_job_params"
echo "test_job_url=${JOB_URL}" > savejoblogs_job_params
echo "test_job_build=${BUILD_NUMBER}" >> savejoblogs_job_params
echo "test_name=${descriptor}" >> savejoblogs_job_params
echo "addPoolServers=$addPoolServers" >> savejoblogs_job_params
echo "version_number=$version_number" >> savejoblogs_job_params
echo "is_dynamic_vms=$is_dynamic_vms" >> savejoblogs_job_params

echo "3/3 Creating file: aws_cleanup_job_params"
echo "servers=${servers}" > aws_cleanup_job_params
echo "###########################################"

echo "Set ALLOW_HTP to False so test could run."
sed -i 's/ALLOW_HTP.*/ALLOW_HTP = False/' lib/testconstants.py

set +e
echo newState=available>propfile
newState=available
echo ${servers}

UPDATE_INI_VALUES=""
if [ ! "${username}" = "" ]; then
  UPDATE_INI_VALUES='"username":"'${username}'"'
fi
if [ ! "${password}" = "" ]; then
  if [ "${UPDATE_INI_VALUES}" = "" ]; then
    UPDATE_INI_VALUES='"password":"'${password}'"'
  else
    UPDATE_INI_VALUES=`echo ${UPDATE_INI_VALUES}',"password":"'${password}'"'`
  fi
fi

# Fix for the ini format issue where both : and = used
sed 's/=/:/' ${iniFile} > $WORKSPACE/testexec_reformat.$$.ini
cat $WORKSPACE/testexec_reformat.$$.ini
internal_servers_param=""
if [ ! "${internal_servers}" = "" ]; then
	internal_servers_param="--internal_servers $internal_servers"
fi

skip_mem_info=""
if [ "$server_type" = "CAPELLA_LOCAL" ]; then
  skip_mem_info=" -m "
fi

## cherrypick the gerrit request if it was defined
if [ "$cherrypick" != "None" ] && [ "$cherrypick" != "" ] ; then
   echo "###############################################"
   echo "########### GIT :: Fetching patch #############"
   echo "###############################################"
   echo "$cherrypick"
   sh -c "$cherrypick"
   echo "###############################################"
fi

echo "Running pip install to fix Python packages"
python -m pip install -r requirements.txt

# Derived once and used twice: passed to populateIni.py so the generated .ini
# declares the profile (TAF compares it against the cluster's reported
# /pools configProfile), and turned into the install param further below.
# Empty for every other server_type, which leaves the .ini untouched.
cluster_profile=""
case "$server_type" in
  ELIXIR_ONPREM)       cluster_profile="serverless" ;;
  ON_PREM_PROVISIONED) cluster_profile="provisioned" ;;
esac

setup_test_infra_repo_for_installation
touch $WORKSPACE/testexec.$$.ini
populate_ini
populate_ini_status=$?
if [ $populate_ini_status -ne 0 ] || [ ! -s "$WORKSPACE/testexec.$$.ini" ]; then
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  echo "populateIni.py exited $populate_ini_status and left no usable .ini"
  echo "(commonly: no SSH connectivity to the assigned pool nodes)"
  echo "Skipping install - the run cannot proceed"
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  newState=failedInstall
  echo newState=failedInstall>propfile
  exit 1
fi

parallel=true
if [ "$server_type" = "CAPELLA_LOCAL" ]; then
	installParameters="install_tasks=uninstall-install,h=true"
elif [ -n "$cluster_profile" ]; then
  installParameters="cluster_profile=$cluster_profile"
fi

if [ "$installParameters" = "None" ]; then
   extraInstall=''
else
   extraInstall=,$installParameters
fi
echo extra install is $extraInstall

# Updating upgrade_version for upgrade jobs
if [ "$parameters" = "" ]; then
    parameters="upgrade_version=${version_number}"
else
    parameters="${parameters},upgrade_version=${version_number}"
fi

if [ "${component}" = "fusion" ]; then
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
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

status=0
# Adding this to install libraries
$jython_pip install requests futures

if [ "$server_type" != "CAPELLA_LOCAL" ]; then
  if [ "$os" = "windows" ] ; then
    export install_params="timeout=2000,skip_local_download=False,version=${version_number},product=cb,parallel=${parallel},init_nodes=${initNodes},debug_logs=True,url=${url}${extraInstall}"
    do_install
  else
    # To handle nonroot user
    set -x
    sed 's/nonroot/root/g' $WORKSPACE/testexec.$$.ini > $WORKSPACE/testexec_root.$$.ini
    set +x

    if [ "$os" != "mariner2" ]; then
      set -x
      python scripts/ssh.py -i $WORKSPACE/testexec.$$.ini --command "iptables -F"
      set +x
    fi

    # Doing installation from TESTRUNNER!!!
    skip_local_download_val=False
    if [[ "$os" = windows* ]]; then
      skip_local_download_val=True
    fi
    if [ "$os" = "debian11nonroot" ]; then
      skip_local_download_val=True
    fi

    if [ "$component" = "os_certify" ]; then
      export install_params="timeout=7200,skip_local_download=$skip_local_download_val,get-cbcollect-info=True,version=${version_number},product=cb,ntp=True,debug_logs=True,url=${url},cb_non_package_installer_url=${cb_non_package_installer_url}${extraInstall}"
    else
      export install_params="force_reinstall=True,timeout=2000,skip_local_download=$skip_local_download_val,get-cbcollect-info=True,version=${version_number},product=cb,ntp=True,debug_logs=True,url=${url},cb_non_package_installer_url=${cb_non_package_installer_url}${extraInstall}"
    fi

    # Perform Installation of builds on target servers
    do_install
  fi
fi

if [ $status -eq 0 ]; then
  desc2=`echo $descriptor | awk '{split($0,r,"-");print r[1],r[2]}'`

  ###### Added on 4/April/2018 to fix issues related to disk full on slaves.
  # Clean this build's own old per-run log dirs - each run gets a freshly
  # timestamped logs/testrunner-<ts>/ (testrunner.py os.makedirs), so only
  # genuinely completed runs of THIS job ever match; the live run's own dir
  # always has a fresh ctime.
  find "$WORKSPACE/logs" -mindepth 1 -maxdepth 1 -type d -ctime +7 -exec rm -rf {} \;

  # Reclaim whole workspaces of jobs that haven't run in 7+ days, node-wide.
  # logs/ itself only gets a new entry when testrunner.py actually starts a
  # run, so its own ctime is a reliable "last ran" signal - unlike checking
  # ctime on arbitrary directories, which previously let one job's cleanup
  # delete a different, concurrently running job's live workspace (its cwd
  # would vanish mid-test, ending the build with zero artifacts archived
  # since allowEmptyArchive/allowEmptyResults are both true). Layered guards
  # against catching a workspace still in use: never touch this build's own
  # $WORKSPACE, skip anything with a recent .executor_lock heartbeat
  # (touched at the top of every run), and skip anything with an open file
  # handle.
  for base in /data/workspace /root/workspace /root/jenkins/workspace; do
    [ -d "$base" ] || continue
    find "$base" -mindepth 2 -maxdepth 2 -type d -name logs -ctime +7 2>/dev/null | while IFS= read -r logdir; do
      parent="$(dirname "$logdir")"
      [ "$parent" = "$WORKSPACE" ] && continue
      [ -n "$(find "$parent/.executor_lock" -mtime -1 2>/dev/null)" ] && continue
      lsof +D "$parent" >/dev/null 2>&1 && continue
      rm -rf "$parent"
    done
  done
  ######

  ##Added on August 2nd 2017 to kill all python processes older than 10days, comment if it causes any failures
  killall --older-than 240h python
  killall --older-than 240h python3
  killall --older-than 10h jython

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
    exit 1
  fi
  cd ..

  # ---- NFS client pre-cleanup (handles stale mounts from aborted jobs) ----
  # Server-side old share pruning is handled by setup_server_new.sh (MAX_SHARES).
  if [ "${component}" = "fusion" ]; then
    python scripts/fusion_scripts/nfs_setup.py cleanup_clients \
        --ini "$WORKSPACE/testexec.$$.ini"
  fi

  # ---- NFS setup for fusion tests (runs once before all tests in confFile) ----
  CLIENT_SHARE_DIR=""
  NFS_FUSION_SERVER_IP=""
  if [ "${component}" = "fusion" ]; then
    echo "NODE_NAME=$NODE_NAME"
    NFS_SETUP_OUTPUT=$(python scripts/fusion_scripts/nfs_setup.py setup --ini "$WORKSPACE/testexec.$$.ini" --node-name "$NODE_NAME" --local-scripts-path "scripts/fusion_scripts")
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
    python testrunner.py -c $confFile -i $WORKSPACE/testexec.$$.ini -p $parameters --launch_sirius_docker --sirius_url http://localhost:$sirius_port ${rerun_params}
  else
    echo "Launching java/magma doc loader to load documents."
    python testrunner.py -c $confFile -i $WORKSPACE/testexec.$$.ini -p $parameters --launch_java_doc_loader --sirius_url http://localhost:$sirius_port ${rerun_params}
  fi
  testrunner_status=$?
  set +x
  # Capture this before rerun_jobs.py runs: merge_reports.merge_reports()
  # renames logs/ to job_logs/ and creates a fresh empty logs/ as a side
  # effect, so checking logs/*/*.xml afterwards would always read 0.
  report_count=$(ls $WORKSPACE/logs/*/*.xml 2>/dev/null | wc -l)

  # ---- NFS teardown for fusion tests (runs once after all tests complete) ----
  if [ "${component}" = "fusion" ] && [ -n "${CLIENT_SHARE_DIR}" ]; then
    CLUSTER_SERVER_IPS=$(grep -oP '^ip:\K.+' "$WORKSPACE/testexec.$$.ini" | paste -sd',' -)
    python scripts/fusion_scripts/nfs_setup.py teardown --nfs-server-ip "$NFS_FUSION_SERVER_IP" --cluster-ips "$CLUSTER_SERVER_IPS" --client-share-dir "$CLIENT_SHARE_DIR"
  fi

  set -x
  awk 'BEGIN {failures = 0; total_tests = 0}
    /<testsuite/ {
      if (match($0, /failures="[0-9]+"/)) {
        f = substr($0, RSTART, RLENGTH); gsub(/[^0-9]/, "", f); failures += f;
      }
      if (match($0, /tests="[0-9]+"/)) {
        t = substr($0, RSTART, RLENGTH); gsub(/[^0-9]/, "", t); total_tests += t;
      }
    }
    END {print "Aggregate Failures: " failures ", Aggregate Total Tests: " total_tests;}' $WORKSPACE/logs/*/*.xml
  python scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --run_params=${parameters}
  rerun_status=$?
  set +x
  # testrunner.py can crash before writing any report (e.g. Sirius/doc-loader
  # launch failure) - rerun_jobs.py then finds zero testsuites and happily
  # exits 0 ("no more failed tests"), which would otherwise mask the crash
  # and let Jenkins report the build as SUCCESS with no test results at all.
  # NOTE: this deliberately only fires when there is NO report at all.
  # testrunner.py exits 1 for any failed/errored test case, and those runs do
  # produce reports - they must stay a zero exit here so the JUnit publisher
  # marks the build UNSTABLE rather than FAILURE.
  if [ "$testrunner_status" -ne 0 ] && [ "$report_count" -eq 0 ]; then
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo "testrunner.py exited $testrunner_status without producing any test report"
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    exit $testrunner_status
  fi
  # A rerun_jobs.py crash is bookkeeping only: the run's own results are
  # already in logs/*/*.xml, and the only fallout is that the next build finds
  # no rerun filter and re-runs the whole suite. Warn, but do not turn a
  # passing/unstable run into a FAILURE.
  if [ $rerun_status -ne 0 ]; then
    echo "WARNING: rerun_jobs.py exited $rerun_status - next build will re-run the full suite"
  fi
else
  echo Desc: $desc
  newState=failedInstall
  echo newState=failedInstall>propfile
  set -x
  python scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --install_failure
  set +x
  exit $status
fi
