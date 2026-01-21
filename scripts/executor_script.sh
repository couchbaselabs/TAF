#!/bin/bash

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
    -i $WORKSPACE/testexec_reformat.$$.ini \
    -p ${os} \
    -o $WORKSPACE/testexec.$$.ini \
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
ps -ef | grep 'java -jar' | grep 'magmadocloader' | awk '$3 == 1 {print $2}' | xargs kill -9

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

setup_test_infra_repo_for_installation
touch $WORKSPACE/testexec.$$.ini
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

# Updating upgrade_version for upgrade jobs
if [ "$parameters" = "" ]; then
    parameters="upgrade_version=${version_number}"
else
    parameters="${parameters},upgrade_version=${version_number}"
fi

# Passing aws_access_key and aws_secret_key for analytics test cases
if [ "$component" = "analytics" ]; then
	parameters="${parameters},aws_access_key=${aws_access_key},aws_secret_key=${aws_secret_key}"
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
      export install_params="force_reinstall=False,timeout=2000,skip_local_download=$skip_local_download_val,get-cbcollect-info=True,version=${version_number},product=cb,ntp=True,debug_logs=True,url=${url},cb_non_package_installer_url=${cb_non_package_installer_url}${extraInstall}"
    fi

    # Perform Installation of builds on target servers
    do_install
  fi
fi

if [ $status -eq 0 ]; then
  desc2=`echo $descriptor | awk '{split($0,r,"-");print r[1],r[2]}'`

  ###### Added on 4/April/2018 to fix issues related to disk full on slaves.
  find /data/workspace/ -type d -ctime +7 -exec rm -rf {} \;
  find /root/jenkins/workspace/ -type d -ctime +7 -exec rm -rf {} \;
  find /data/workspace/*/logs/* -type d -ctime +7 -delete
  find /data/workspace/*/logs/* -type d -ctime +7 -exec rm -rf {} \;
  find /root/workspace/*/logs/* -type d -ctime +7 -exec rm -rf {} \;
  find /root/workspace/ -type d -ctime +7 -exec rm -rf {} \;
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
    cleanup_dir_before_exit
    exit 1
  fi
  cd ..

  # Find free port on this machine to use for this run
  starting_ports=(49152 49162 49172 49182 49192 49202 49212 49222 49232)
  num_scripts_running=$(ps -ef | grep '/tmp/jenkins' | grep -v 'grep ' | wc -l)
  sirius_port=${starting_ports[$num_scripts_running]} ; while [ "$(ss -tulpn | grep LISTEN | grep $sirius_port | wc -l)" -ne 0 ]; do sirius_port=$((sirius_port+1)) ; done
  set -x
  if [[ "$load_docs_using" == "sirius_go_sdk" ]]; then
    echo "Launching Sirius GO SDK to load documents."
    python testrunner.py -c $confFile -i $WORKSPACE/testexec.$$.ini -p $parameters --launch_sirius_docker --sirius_url http://localhost:$sirius_port ${rerun_params}
  else
    echo "Launching java/magma doc loader to load documents."
    python testrunner.py -c $confFile -i $WORKSPACE/testexec.$$.ini -p $parameters --launch_java_doc_loader --sirius_url http://localhost:$sirius_port ${rerun_params}
  fi
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
else
  echo Desc: $desc
  newState=failedInstall
  echo newState=failedInstall>propfile
  set -x
  python scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --install_failure
  set +x
fi

# To reduce the disk consumption post run
cleanup_dir_before_exit
