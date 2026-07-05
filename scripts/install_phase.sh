#!/bin/bash

# Install phase of executor_script.sh, split out so the Jenkinsfile can run
# cleanup/install/test-execution as three distinct pipeline stages - see
# cleanup_phase.sh and execution_phase.sh for the other two.
#
# Uses $BUILD_NUMBER (stable for the whole build) instead of $$ (the old PID-based
# suffix) to name shared workspace files, since install_phase.sh and
# execution_phase.sh run as two separate shell processes and need to agree on
# the same filenames.
#
# On install failure this script itself writes propfile=failedInstall, runs the
# install-failure rerun bookkeeping, cleans up the workspace and exits non-zero
# so the Jenkinsfile can skip the execution stage. On success it exits 0 and
# leaves the workspace (ini files, rerun_props_file, checked-out repos) in
# place for execution_phase.sh.

cleanup_dir_before_exit() {
  rm -rf .git b build conf pytests DocLoader lib couchbase_utils test_infra_runner
}

setup_test_infra_repo_for_installation() {
  git clone https://${GITHUB_USER}:${GITHUB_TOKEN}@github.com/couchbaselabs/test_infra_runner --depth 1
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
    -i $WORKSPACE/testexec_reformat.$BUILD_NUMBER.ini \
    -p ${os} \
    -o $WORKSPACE/testexec.$BUILD_NUMBER.ini \
    -k '{'${UPDATE_INI_VALUES}'}' \
    --cb_version $version_number \
    --columnar_version "$columnar_version_number" \
    --mixed_build_config "$mixed_build_config"
  set +x
  cd ..
}

do_install() {
  echo "Starting server installation"
  cd test_infra_runner
  python scripts/new_install.py -i $WORKSPACE/testexec.$BUILD_NUMBER.ini -p $install_params
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

echo "###########################################"
echo "  Populating env file for downstream jobs"
echo "1/4 Extracting is_dynamic_vms value"
export is_dynamic_vms=`echo $dispatcher_params| sed -n 's/.*"use_dynamic_vms": *\([^,]*\).*/\1/p' | tr -d ' '`
echo "is_dynamic_vms value: $is_dynamic_vms"

echo "2/4 Creating file: cleanup_job_params"
echo "descriptor=$descriptor" > cleanup_job_params
echo "UPSTREAM_BUILD_NUMBER=${BUILD_NUMBER}" >> cleanup_job_params
echo "addPoolServers=$addPoolServers" >> cleanup_job_params
echo "version_number=$version_number" >> cleanup_job_params
echo "is_dynamic_vms=$is_dynamic_vms" >> cleanup_job_params

echo "3/4 Creating file: savejoblogs_job_params"
echo "test_job_url=${JOB_URL}" > savejoblogs_job_params
echo "test_job_build=${BUILD_NUMBER}" >> savejoblogs_job_params
echo "test_name=${descriptor}" >> savejoblogs_job_params
echo "addPoolServers=$addPoolServers" >> savejoblogs_job_params
echo "version_number=$version_number" >> savejoblogs_job_params
echo "is_dynamic_vms=$is_dynamic_vms" >> savejoblogs_job_params

echo "4/4 Creating file: aws_cleanup_job_params"
echo "servers=${servers}" > aws_cleanup_job_params
echo "###########################################"

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
sed 's/=/:/' ${iniFile} > $WORKSPACE/testexec_reformat.$BUILD_NUMBER.ini
cat $WORKSPACE/testexec_reformat.$BUILD_NUMBER.ini
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

setup_test_infra_repo_for_installation
touch $WORKSPACE/testexec.$BUILD_NUMBER.ini
populate_ini

parallel=true
if [ "$server_type" = "CAPELLA_LOCAL" ]; then
	installParameters="install_tasks=uninstall-install,h=true"
else
  if [ "$server_type" = "ELIXIR_ONPREM" ]; then
    installParameters="cluster_profile=serverless"
  fi
fi

if [ "$installParameters" = "None" ]; then
   extraInstall=''
else
   extraInstall=,$installParameters
fi
echo extra install is $extraInstall

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
    sed 's/nonroot/root/g' $WORKSPACE/testexec.$BUILD_NUMBER.ini > $WORKSPACE/testexec_root.$BUILD_NUMBER.ini
    set +x

    if [ "$os" != "mariner2" ]; then
      set -x
      python scripts/ssh.py -i $WORKSPACE/testexec.$BUILD_NUMBER.ini --command "iptables -F"
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

if [ $status -ne 0 ]; then
  echo Desc: $desc
  newState=failedInstall
  echo newState=failedInstall>propfile
  set -x
  python scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --install_failure
  set +x
  cleanup_dir_before_exit
  exit 1
fi

echo "Install phase completed successfully."
