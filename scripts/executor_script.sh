#!/bin/bash -x

ulimit -a

# Set desired python env
pyenv local 3.10.14

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

# Fetch all submodules
git submodule init
git submodule update --init --force --remote

set -x
python scripts/populateIni.py $skip_mem_info -s ${servers} $internal_servers_param -d ${addPoolServerId} -a ${addPoolServers} -i $WORKSPACE/testexec_reformat.$$.ini -p ${os} -o $WORKSPACE/testexec.$$.ini -k '{'${UPDATE_INI_VALUES}'}'
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

# Perform Installation of builds on target servers
cd platform_utils/ssh_util
python -m install_util.install -i $WORKSPACE/testexec.$$.ini -v ${version_number} --skip_local_download
status=$?
cd ../..

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

  echo "Timeout: $timeout minutes"

  # Find free port on this machine to use for this run
  sirius_port=49152 ; INCR=1 ; while [ -n "$(ss -tan4H "sport = $sirius_port")" ]; do sirius_port=$((sirius_port+INCR)) ; done
  echo "Will use $sirius_port for starting sirius"
  export PATH=/usr/local/go/bin:$PATH
  set -x
  python testrunner.py -c $confFile -i $WORKSPACE/testexec.$$.ini -p $parameters --launch_sirius_docker --sirius_url http://localhost:$sirius_port
  set +x

  awk -F' ' 'BEGIN {failures = 0; total_tests = 0} /<testsuite/ {match($0, /failures="([0-9]+)"/, failures_match); match($0, /tests="([0-9]+)"/, tests_match); if (failures_match[1] > 0) {failures += failures_match[1];} total_tests += tests_match[1]} END {print "Aggregate Failures: " failures ", Aggregate Total Tests: " total_tests;}' $WORKSPACE/logs/*/*.xml
  if [ "${rerun_job}" == true ]; then
  	echo "python tr_for_install/testrunner/scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --run_params=${parameters}"
  	python tr_for_install/testrunner/scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --run_params=${parameters}
  fi
else
  echo Desc: $desc
  newState=failedInstall
  echo newState=failedInstall>propfile
  if [ "${rerun_job}" == true ]; then
  	echo "python tr_for_install/testrunner/scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --install_failure"
    python tr_for_install/testrunner/scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --install_failure
  fi
fi

# Just to reduce the disk consumption
rm -rf tr_for_install
rm -rf .git
