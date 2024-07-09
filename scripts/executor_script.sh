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

jython_path=/opt/jython/bin/jython
jython_pip=/opt/jython/bin/pip

# Setup GoLang in local dir
go_version=1.22.4
wget https://golang.org/dl/go${go_version}.linux-amd64.tar.gz --quiet
tar -xzf go${go_version}.linux-amd64.tar.gz
rm -f go${go_version}.linux-amd64.tar.gz
export GOPATH=`pwd`/go
export PATH="${GOPATH}/bin:${PATH}"
export GO111MODULE=on

# Set desired python env
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
pyenv local 3.10.14

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

# Fetch all submodules
git submodule init
git submodule update --init --force --remote

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
if [ "${slave}" == "deb12_executor" ]; then
  pyenv local 3.10.14
  py_executable=python

  # Switch to py3 branch that has all dependent modules for execution
  git checkout master_py3_dev
  git submodule init
  git submodule update --init --force --remote
  run_populate_ini_script $py_executable

  if [ "$server_type" != "CAPELLA_LOCAL" ]; then
    cd platform_utils/ssh_util
    export PYTHONPATH="../../couchbase_utils:../../py_constants"
    $py_executable -m install_util.install -i $WORKSPACE/testexec.$$.ini -v ${version_number} --skip_local_download
    status=$?
    # Come back to TAF root dir
    cd ../..
  fi

  # Come back to the requested branch for test execution
  git checkout ${branch}
else
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
      echo sed 's/nonroot/root/g' $WORKSPACE/testexec.$$.ini > $WORKSPACE/testexec_root.$$.ini
      sed 's/nonroot/root/g' $WORKSPACE/testexec.$$.ini > $WORKSPACE/testexec_root.$$.ini

      if [ "$os" != "mariner2" ]; then
      	guides/gradlew --no-daemon --refresh-dependencies iptables -P jython="/opt/jython/bin/jython" -P args="-i $WORKSPACE/testexec_root.$$.ini iptables -F"
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

  echo ${rerun_params_manual}
  echo ${rerun_params}
  if [ -z "${rerun_params_manual}" ] && [ -z "${rerun_params}" ]; then
  	rerun_param=
  elif [ -z "${rerun_params_manual}" ]; then
  	rerun_param=$rerun_params
  else
  	rerun_param=${rerun_params_manual}
  fi

  # Find free port on this machine to use for this run
  sirius_port=49152 ; INCR=1 ; while [ -n "$(ss -tan4H "sport = $sirius_port")" ]; do sirius_port=$((sirius_port+INCR)) ; done
  echo "Will use $sirius_port for starting sirius"
  export PATH=/usr/local/go/bin:$PATH
  set -x
  python testrunner.py -c $confFile -i $WORKSPACE/testexec.$$.ini -p $parameters --launch_sirius_docker --sirius_url http://localhost:$sirius_port
  set +x

  awk -F' ' 'BEGIN {failures = 0; total_tests = 0} /<testsuite/ {match($0, /failures="([0-9]+)"/, failures_match); match($0, /tests="([0-9]+)"/, tests_match); if (failures_match[1] > 0) {failures += failures_match[1];} total_tests += tests_match[1]} END {print "Aggregate Failures: " failures ", Aggregate Total Tests: " total_tests;}' $WORKSPACE/logs/*/*.xml
  set -x
  python scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --run_params=${parameters}
  set +x
else
  echo Desc: $desc
  newState=failedInstall
  echo newState=failedInstall>propfile
  set -x
  python scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --install_failure
  set +x
fi

# To reduce the disk consumption post run
rm -rf .git tr_for_install b build conf guides pytests
docker system  prune -f
