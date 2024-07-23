#!/bin/bash

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

jython_path=/opt/jython/bin/jython
jython_pip=/opt/jython/bin/pip

# Clean up the gradle logs folder which bloat up the disk space
for file in `find ~/.gradle/ -name "*.out.log"`
do
    lsof_line_count=`lsof $file | grep -v COMMAND | wc -l`
    if [ $lsof_line_count -eq 0 ]; then
        rm -f $file
    fi
done

# Clone the guides repo for Gradle command
git clone https://github.com/couchbaselabs/guides.git

if [ ${fresh_run} == false ]; then
  set -x
  guides/gradlew --refresh-dependencies --stacktrace rerun_job -P jython="$jython_path" -P args="${version_number} --executor_jenkins_job --manual_run"
  set +x
fi

set +e
echo newState=available>propfile
newState=available

majorRelease=`echo ${version_number} | awk '{print substr($0,1,1)}'`
echo the major release is $majorRelease

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
  installParameters="install_tasks=uninstall-install,h=true"
else
    if [ "$server_type" = "ELIXIR_ONPREM" ]; then
        installParameters="cluster_profile=serverless"
    fi
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

if [ "$component" = "transaction" ]; then
    java_client_version=3.4.11
    if [ "$branch" = "neo" ]; then
    	java_client_version=default
    fi
    if [[ "$subcomponent" == *"_defer_"* ]]; then
        transaction_version=1.2.3
    fi
    if [[ "$subcomponent" == *"_metering_"* ]]; then
        transaction_version=1.2.3
        java_client_version=3.4.2
    fi
fi

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

      # Install requirements for this venv
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

# To pass client-versions to use from cmd_line
sdk_client_params="-P transaction_version=$transaction_version -P client_version=$java_client_version"

if [ $status -eq 0 ]; then
  desc2=`echo $descriptor | awk '{split($0,r,"-");print r[1],r[2]}'`

  git checkout ${branch}
  git pull origin ${branch}

  ###### To fix auto merge failures. Please revert this if this does not work.
  git fetch
  git reset --hard origin/${branch}
  ######

  ## cherrypick the gerrit request if it was defined
  if [ "$cherrypick" != "None" ] && [ "$cherrypick" != "" ] ; then
     echo "###############################################"
     echo "########### GIT :: Fetching patch #############"
     echo "###############################################"
     echo "$cherrypick"
     sh -c "$cherrypick"
     echo "###############################################"
  fi

  # Pull all submodules
  git submodule init
  git submodule update --init --force --remote

  # To fix issues related to disk full on slaves.
  find /data/workspace/*/logs/* -type d -ctime +7 -delete 2>/dev/null
  find /data/workspace/ -type d -ctime +7 -exec rm -rf {} \; 2>/dev/null
  find /root/workspace/ -type d -ctime +7 -exec rm -rf {} \; 2>/dev/null
  find /root/jenkins/workspace/ -type d -ctime +7 -exec rm -rf {} \; 2>/dev/null
  find /data/workspace/*/logs/* -type d -ctime +7 -exec rm -rf {} \; 2>/dev/null
  find /root/workspace/*/logs/* -type d -ctime +7 -exec rm -rf {} \; 2>/dev/null

  # To kill all python processes older than 10days
  killall --older-than 240h python
  killall --older-than 240h python3
  killall --older-than 10h jython

  # Trim whitespaces to detect empty input
  rerun_params=$(echo $rerun_params | xargs)
  if [ "$rerun_params" == "" ]; then
    # Only if user has no input given, get rerun data from
    # the file created by prev. rerun_jobs.py script
    rerun_file_data=$(cat rerun_props_file)
    if [ $rerun_file_data != "" ]; then
      rerun_params=$rerun_file_data
    fi
  fi

  # Adding static IP for sirius
  sirius[0]="http://172.23.120.103:4000"

  size=${#sirius[@]}
  index=$(($RANDOM % $size))
  sirius_url=${sirius[$index]}
  parameters=${parameters}",sirius_url="${sirius_url}
  echo ${parameters}
  # End of Sirius dep code

  echo "Timeout: $timeout minutes"
  guides/gradlew --no-daemon --refresh-dependencies testrunner -P jython="$jython_path" $sdk_client_params -P args="-i $WORKSPACE/testexec.$$.ini -c ${confFile} -p ${parameters} -m ${mode} ${rerun_param}"

  status=$?
  echo workspace is $WORKSPACE
  fails=`cat $WORKSPACE/logs/*/*.xml | grep 'testsuite errors' | awk '{split($3,s1,"=");print s1[2]}' | sed s/\"//g | awk '{s+=$1} END {print s}'`
  echo fails is $fails
  total_tests=`cat $WORKSPACE/logs/*/*.xml | grep 'testsuite errors' | awk '{split($6,s1,"=");print s1[2]}' | sed s/\"//g |awk '{s+=$1} END {print s}'`
  echo $total_tests
  echo Desc1: $version_number - $desc2 - $os \($(( $total_tests - $fails ))/$total_tests\)
  guides/gradlew --no-daemon --stacktrace rerun_job -P jython="$jython_path" $sdk_client_params -P args="${version_number} --executor_jenkins_job --run_params=${parameters}"
  # Check if gradle had clean exit. If not, fail the job.
  if [ ! $status = 0 ]; then
    echo "Gradle had non zero exit. Failing the job"
    exit 1
  fi
else
  echo Desc: $desc
  newState=failedInstall
  echo newState=failedInstall>propfile
  guides/gradlew --no-daemon --stacktrace rerun_job -P jython="$jython_path" $sdk_client_params -P args="${version_number} --executor_jenkins_job --install_failure"
fi

# To reduce the disk consumption post run
rm -rf .git b build conf guides pytests
docker system  prune -f
