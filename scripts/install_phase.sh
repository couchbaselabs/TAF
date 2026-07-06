#!/bin/bash

# Runs as its own process/stage - see execution_phase.sh and cleanup_phase.sh
# for the other two. Uses $BUILD_NUMBER instead of $$ for shared workspace ini
# filenames since each phase is a separate shell process now. Function defs and
# vars this phase needs (check_and_build_testrunner_install_docker, jython_path/
# jython_pip, ulimits) are duplicated here rather than shared,
# since plain shell state doesn't cross a process boundary.

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

echo "" > rerun_props_file
if [ ${fresh_run} == false ]; then
  set -x
  guides/gradlew --refresh-dependencies --stacktrace rerun_job -P jython="$jython_path" -P args="${version_number} --executor_jenkins_job --manual_run"
  set +x
fi

# Used to pass on to the cleanup job
export is_dynamic_vms=`echo $dispatcher_params | grep -o '"use_dynamic_vms": [^,]*' | cut -d' ' -f2`

set +e
echo newState=available>propfile
newState=available

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
  installParameters="install_tasks=uninstall-install,h=true"
elif [ "$server_type" = "ELIXIR_ONPREM" ]; then
  installParameters="cluster_profile=serverless"
elif [ "$server_type" = "ON_PREM_PROVISIONED" ]; then
  installParameters="cluster_profile=provisioned"
fi

parallel=true
if [ "$os" = "windows" ]; then
   # serial worked even worse but may come back to is
   parallel=true
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

check_and_build_testrunner_install_docker
touch $WORKSPACE/testexec.$BUILD_NUMBER.ini
docker run --rm \
  -v $WORKSPACE/testexec_reformat.$BUILD_NUMBER.ini:/testrunner/testexec_reformat.$BUILD_NUMBER.ini:Z \
  -v $WORKSPACE/testexec.$BUILD_NUMBER.ini:/testrunner/testexec.$BUILD_NUMBER.ini:Z  \
  testrunner:install python3 scripts/populateIni.py $skip_mem_info \
  -s ${servers} $internal_servers_param \
  -d ${addPoolServerId} \
  -a ${addPoolServers} \
  -i testexec_reformat.$BUILD_NUMBER.ini \
  -p ${os} \
  -o testexec.$BUILD_NUMBER.ini \
  -k '{'${UPDATE_INI_VALUES}'}'
if [ "$server_type" != "CAPELLA_LOCAL" ]; then
  if [ "$os" = "windows" ] ; then
    docker run --rm \
      -v $WORKSPACE/testexec.$BUILD_NUMBER.ini:/testrunner/testexec.$BUILD_NUMBER.ini:Z \
      testrunner:install python3 scripts/new_install.py \
      -i testexec.$BUILD_NUMBER.ini \
      -p timeout=2000,skip_local_download=False,version=${version_number},product=cb,parallel=${parallel},init_nodes=${initNodes},debug_logs=True,url=${url}${extraInstall}
    status=$?
  else
    # To handle nonroot user
    echo sed 's/nonroot/root/g' $WORKSPACE/testexec.$BUILD_NUMBER.ini > $WORKSPACE/testexec_root.$BUILD_NUMBER.ini
    sed 's/nonroot/root/g' $WORKSPACE/testexec.$BUILD_NUMBER.ini > $WORKSPACE/testexec_root.$BUILD_NUMBER.ini

    if [ "$os" != "mariner2" ]; then
      guides/gradlew --no-daemon --refresh-dependencies iptables -P jython="/opt/jython/bin/jython" -P args="-i $WORKSPACE/testexec_root.$BUILD_NUMBER.ini iptables -F"
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
      new_install_params="force_reinstall=True,timeout=2000,skip_local_download=$skip_local_download_val,get-cbcollect-info=True,version=${version_number},product=cb,ntp=True,debug_logs=True,url=${url},cb_non_package_installer_url=${cb_non_package_installer_url}${extraInstall}"
    fi

    # Install requirements for this venv
    echo "Starting server installation"
    set -x
    docker run --rm \
      -v $WORKSPACE/testexec.$BUILD_NUMBER.ini:/testrunner/testexec.$BUILD_NUMBER.ini:Z \
      testrunner:install python3 scripts/new_install.py \
      -i testexec.$BUILD_NUMBER.ini \
      -p $new_install_params
    status=$?
    set +x
  fi
fi

if [ $status -ne 0 ]; then
  echo Desc: $desc
  newState=failedInstall
  echo newState=failedInstall>propfile
  guides/gradlew --no-daemon --stacktrace rerun_job -P jython="$jython_path" -P args="${version_number} --executor_jenkins_job --install_failure"
  # To reduce the disk consumption post run
  rm -rf .git b build conf guides pytests
  # To clean any available space from docker
  docker system prune -f
  exit 1
fi
