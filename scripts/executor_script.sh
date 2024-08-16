#!/bin/bash

check_and_build_testrunner_install_docker() {
  docker_img=testrunner:install
  docker_img_id=$(docker images -q $docker_img)
  if [ "$docker_img_id" == "" ]; then
    echo '
    FROM python:3.8.4
    WORKDIR /
    RUN git clone https://github.com/couchbase/testrunner.git
    WORKDIR /testrunner
    # Install couchbase first to avoid fetching unsupported six package version
    RUN python -m pip install couchbase==3.2.0
    # Now install all other dependencies
    RUN python -m pip install -r requirements.txt
    RUN git submodule init
    RUN git submodule update --init --force --remote
    WORKDIR /
    RUN echo "cd /testrunner" > new_install.sh
    RUN echo "git remote update origin --prune" >> new_install.sh
    RUN echo "git pull -q" >> new_install.sh
    RUN echo "\"\$@\"" >> new_install.sh
    # Set entrypoint for the docker container
    ENTRYPOINT ["sh", "new_install.sh"]' > Dockerfile
    echo "Building docker image $docker_img"
    docker build . --tag $docker_img --quiet
    echo "Docker build '${docker_img}' done"
  else
    echo "Docker image '${docker_img}' exists"
  fi
}

run_populate_ini_script() {
  set -x
  $1 scripts/populateIni.py $skip_mem_info \
  -s ${servers} $internal_servers_param \
  -d ${addPoolServerId} \
  -a ${addPoolServers} \
  -i $WORKSPACE/testexec_reformat.$$.ini \
  -p ${os} \
  -o $WORKSPACE/testexec.$$.ini \
  -k '{'${UPDATE_INI_VALUES}'}'
  set +x
}

check_and_build_testrunner_install_docker() {
  docker_img=testrunner:install
  docker_img_id=$(docker images -q $docker_img)
  if [ "$docker_img_id" == "" ]; then
    echo '
    FROM python:3.8.4
    WORKDIR /
    RUN git clone https://github.com/couchbase/testrunner.git
    WORKDIR /testrunner

    # Install couchbase first to avoid fetching unsupported six package version
    RUN python -m pip install couchbase==3.2.0
    # Now install all other dependencies
    RUN python -m pip install -r requirements.txt

    RUN git submodule init
    RUN git submodule update --init --force --remote
    WORKDIR /

    RUN echo "cd /testrunner" > new_install.sh
    RUN echo "git remote update origin --prune" >> new_install.sh
    RUN echo "git pull -q" >> new_install.sh
    RUN echo "\"\$@\"" >> new_install.sh
    # Set entrypoint for the docker container
    ENTRYPOINT ["sh", "new_install.sh"]' > Dockerfile
    echo "Building docker image $docker_img"
    docker build . --tag $docker_img --quiet
    echo "Docker build '${docker_img}' done"
  else
    echo "Docker image '${docker_img}' exists"
  fi
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

# Clean up the gradle logs folder which bloat up the disk space
for file in `find ~/.gradle/ -name "*.out.log"`
do
    lsof_line_count=`lsof $file | grep -v COMMAND | wc -l`
    if [ $lsof_line_count -eq 0 ]; then
        rm -f $file
    fi
done

# Set desired python env
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
pyenv local 3.10.14

# Find cases for rerun
echo "" > rerun_props_file
if [ ${fresh_run} == false ]; then
 set -x
 python scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --manual_run
 set +x
fi

echo "Set ALLOW_HTP to False so test could run."
sed -i 's/ALLOW_HTP.*/ALLOW_HTP = False/' lib/testconstants.py

echo "###### Checking Docker status ######"
systemctl status docker > /dev/null
docker_status=$?
if [ $docker_status -ne 0 ]; then
  echo "Starting docker service"
  systemctl start docker
else
  echo "Docker up and running"
fi
echo "####################################"

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

check_and_build_testrunner_install_docker
touch $WORKSPACE/testexec.$$.ini
set -x
docker run --rm \
    -v $WORKSPACE/testexec_reformat.$$.ini:/testrunner/testexec_reformat.$$.ini \
    -v $WORKSPACE/testexec.$$.ini:/testrunner/testexec.$$.ini  \
    testrunner:install python3 scripts/populateIni.py $skip_mem_info \
    -s ${servers} $internal_servers_param \
    -d ${addPoolServerId} \
    -a ${addPoolServers} \
    -i testexec_reformat.$$.ini \
    -p ${os} \
    -o testexec.$$.ini \
    -k '{'${UPDATE_INI_VALUES}'}'
# python scripts/populateIni.py $skip_mem_info \
# -s ${servers} $internal_servers_param \
# -d ${addPoolServerId} \
# -a ${addPoolServers} \
# -i $WORKSPACE/testexec_reformat.$$.ini \
# -p ${os} \
# -o $WORKSPACE/testexec.$$.ini \
# -k '{'${UPDATE_INI_VALUES}'}'
set +x

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

# run_populate_ini_script $py_executable
check_and_build_testrunner_install_docker
touch $WORKSPACE/testexec.$$.ini
docker run --rm \
  -v $WORKSPACE/testexec_reformat.$$.ini:/testrunner/testexec_reformat.$$.ini \
  -v $WORKSPACE/testexec.$$.ini:/testrunner/testexec.$$.ini  \
  testrunner:install python3 scripts/populateIni.py $skip_mem_info \
  -s ${servers} $internal_servers_param \
  -d ${addPoolServerId} \
  -a ${addPoolServers} \
  -i testexec_reformat.$$.ini \
  -p ${os} \
  -o testexec.$$.ini \
  -k '{'${UPDATE_INI_VALUES}'}'
if [ "$server_type" != "CAPELLA_LOCAL" ]; then
  if [ "$os" = "windows" ] ; then
    docker run --rm \
      -v $WORKSPACE/testexec.$$.ini:/testrunner/testexec.$$.ini \
      testrunner:install python3 scripts/new_install.py \
      -i testexec.$$.ini \
      -p timeout=2000,skip_local_download=False,version=${version_number},product=cb,parallel=${parallel},init_nodes=${initNodes},debug_logs=True,url=${url}${extraInstall}
    status=$?
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
      new_install_params="timeout=7200,skip_local_download=$skip_local_download_val,get-cbcollect-info=True,version=${version_number},product=cb,ntp=True,debug_logs=True,url=${url},cb_non_package_installer_url=${cb_non_package_installer_url}${extraInstall}"
    else
      new_install_params="force_reinstall=False,timeout=2000,skip_local_download=$skip_local_download_val,get-cbcollect-info=True,version=${version_number},product=cb,ntp=True,debug_logs=True,url=${url},cb_non_package_installer_url=${cb_non_package_installer_url}${extraInstall}"
    fi

    # Perform Installation of builds on target servers
    set -x
    docker run --rm \
      -v $WORKSPACE/testexec.$$.ini:/testrunner/testexec.$$.ini \
      testrunner:install python3 scripts/new_install.py \
      -i testexec.$$.ini \
      -p $new_install_params
    status=$?
    set +x
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

  # Find free port on this machine to use for this run
  sirius_port=49152 ; INCR=1 ; while [ -n "$(ss -tan4H "sport = $sirius_port")" ]; do sirius_port=$((sirius_port+INCR)) ; done
  echo "Will use $sirius_port for starting sirius"
  set -x
  python testrunner.py -c $confFile -i $WORKSPACE/testexec.$$.ini -p $parameters --launch_java_doc_loader --sirius_url http://localhost:$sirius_port ${rerun_params}
  awk -F' ' 'BEGIN {failures = 0; total_tests = 0} /<testsuite/ {match($0, /failures="([0-9]+)"/, failures_match); match($0, /tests="([0-9]+)"/, tests_match); if (failures_match[1] > 0) {failures += failures_match[1];} total_tests += tests_match[1]} END {print "Aggregate Failures: " failures ", Aggregate Total Tests: " total_tests;}' $WORKSPACE/logs/*/*.xml
  python scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --run_params=${parameters}
  status=$?
  set +x
  if [ status -ne 0 ]; then
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo "Non-zero exit while running rerun_jobs.py"
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    exit status
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
rm -rf .git tr_for_install b build conf go pytests
docker system  prune -f
