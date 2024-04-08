#!/bin/bash

ulimit -a
pip_path=/opt/jython/bin/pip
jython_path=/opt/jython/bin/jython

support_ver="6.5"
small_ver=${version_number:0:3}
py_executable=python

code=`echo ${version_number} | cut -d "-" -f1 | cut -d "." -f1,2`
if [[ ( $code == 6.6* && "${component}" = "backup_recovery" ) || ( $code == 6.6* && "${subcomponent}" = "import-export-"* ) ]]; then
	py_executable=python3
  branch=master
  testrunner_tag=master
fi

# Py3 since the version_num is > "6.X"
py_executable=python3
# True since the version_num is > "6.5"
rerun_job=true

echo "Set ALLOW_HTP to False so test could run."
sed -i 's/ALLOW_HTP.*/ALLOW_HTP = False/' lib/testconstants.py

set +e
echo newState=available>propfile
newState=available

majorRelease=`echo ${version_number} | awk '{print substr($0,1,1)}'`
echo the major release is $majorRelease
echo ${servers}

${py_executable} -m easy_install httplib2
if [ "$component" = "analytics" ]; then
	${py_executable} -m pip install boto3==1.17.112 pyspark
fi

# Adding this to install libraries
$pip_path install requests futures

# Clone testrunner.git - to utilize latest populateIni changes
mkdir tr_for_install
cd tr_for_install
git clone https://github.com/couchbase/testrunner.git
cd ..

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

cp ${iniFile} tr_for_install/testrunner/nodes.ini
iniFile=nodes.ini

cd tr_for_install/testrunner
echo "#### Testrunner last commit ####"
git log -1
git submodule init
git submodule update --init --force --remote
echo "################################"

# Fix for the ini format issue where both : and = used
sed 's/=/:/' ${iniFile} > $WORKSPACE/testexec_reformat.$$.ini
cat $WORKSPACE/testexec_reformat.$$.ini
internal_servers_param=""
if [ ! "${internal_servers}" = "" ]; then
	internal_servers_param="--internal_servers $internal_servers"
fi

if [ "$server_type" = "CAPELLA_LOCAL" ]; then
	git submodule init
    git submodule update --init --force --remote
    echo python3  scripts/populateIni.py -m -s ${servers} $internal_servers_param -d ${addPoolServerId} -a ${addPoolServers} -i $WORKSPACE/testexec_reformat.$$.ini -p ${os} -o $WORKSPACE/testexec.$$.ini -k '{'${UPDATE_INI_VALUES}'}'
    python3 scripts/populateIni.py -m -s ${servers} $internal_servers_param -d ${addPoolServerId} -a ${addPoolServers} -i $WORKSPACE/testexec_reformat.$$.ini -p ${os} -o $WORKSPACE/testexec.$$.ini -k '{'${UPDATE_INI_VALUES}'}'

else
    echo python3 scripts/populateIni.py -s ${servers} $internal_servers_param -d ${addPoolServerId} -a ${addPoolServers} -i $WORKSPACE/testexec_reformat.$$.ini -p ${os} -o $WORKSPACE/testexec.$$.ini -k '{'${UPDATE_INI_VALUES}'}'
    python3 scripts/populateIni.py -s ${servers} $internal_servers_param -d ${addPoolServerId} -a ${addPoolServers} -i $WORKSPACE/testexec_reformat.$$.ini -p ${os} -o $WORKSPACE/testexec.$$.ini -k '{'${UPDATE_INI_VALUES}'}'
fi

cd ../..

if [ "$os" = "windows" ]; then
   echo "Have Windows,"
   parallel=true   # serial worked even worse but may come back to is
 else
   parallel=true
fi

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

if [ "$server_type" != "CAPELLA_LOCAL" ]; then
  if [ "$os" = "windows" ] || [ $(${py_executable} -c "print($small_ver < $support_ver)") = True ]; then
      cd tr_for_install/testrunner
      python3 scripts/new_install.py -i $WORKSPACE/testexec.$$.ini -p timeout=2000,skip_local_download=False,version=${version_number},product=cb,parallel=${parallel},init_nodes=${initNodes},debug_logs=True,url=${url}${extraInstall}
      status=$?
      cd ../../
  else
      #To handle nonroot user
      echo sed 's/nonroot/root/g' $WORKSPACE/testexec.$$.ini > $WORKSPACE/testexec_root.$$.ini
      sed 's/nonroot/root/g' $WORKSPACE/testexec.$$.ini > $WORKSPACE/testexec_root.$$.ini

      if [ "$os" != "mariner2" ]; then
      	guides/gradlew --refresh-dependencies iptables -P jython="/opt/jython/bin/jython" -P args="-i $WORKSPACE/testexec_root.$$.ini iptables -F"
      fi

      # Doing installation from TESTRUNNER!!!
      skip_local_download_val=False
      if [[ "$os" = windows* ]]; then
        skip_local_download_val=True
      fi
      if [ "$os" = "debian11nonroot" ]; then
      	skip_local_download_val=True
      fi

      cd tr_for_install/testrunner
      git checkout ${branch}
      git pull origin ${branch}

      git submodule init
      git submodule update --init --force --remote
      py_executable=python3
      echo "Server install cmd:"

      if [ "$component" = "os_certify" ]; then
      	echo ${py_executable} scripts/new_install.py -i $WORKSPACE/testexec.$$.ini -p timeout=7200,skip_local_download=$skip_local_download_val,get-cbcollect-info=True,version=${version_number},product=cb,ntp=True,debug_logs=True,url=${url},cb_non_package_installer_url=${cb_non_package_installer_url}${extraInstall}
        ${py_executable} scripts/new_install.py -i $WORKSPACE/testexec.$$.ini -p timeout=7200,skip_local_download=$skip_local_download_val,get-cbcollect-info=True,version=${version_number},product=cb,ntp=True,debug_logs=True,url=${url},cb_non_package_installer_url=${cb_non_package_installer_url}${extraInstall}
        status=$?
      else
        echo ${py_executable} scripts/new_install.py -i $WORKSPACE/testexec.$$.ini -p force_reinstall=False,timeout=2000,skip_local_download=$skip_local_download_val,get-cbcollect-info=True,version=${version_number},product=cb,ntp=True,debug_logs=True,url=${url},cb_non_package_installer_url=${cb_non_package_installer_url}${extraInstall}
        ${py_executable} scripts/new_install.py -i $WORKSPACE/testexec.$$.ini -p force_reinstall=False,timeout=2000,skip_local_download=$skip_local_download_val,get-cbcollect-info=True,version=${version_number},product=cb,ntp=True,debug_logs=True,url=${url},cb_non_package_installer_url=${cb_non_package_installer_url}${extraInstall}
        status=$?
      fi
      cd ../..
  fi
else
  status=0
fi
rm -rf tr_for_install


# To pass client-versions to use from cmd_line
sdk_client_params="-P transaction_version=$transaction_version -P client_version=$java_client_version"

if [ $status -eq 0 ]; then
  desc2=`echo $descriptor | awk '{split($0,r,"-");print r[1],r[2]}'`

  if [ $majorRelease = "3" ]; then
     echo have a 3.x release
     git checkout 0b2bb7a53e350f90737112457877ef2c05ca482a
  elif [[ "$testrunner_tag" != "master" ]]; then
     git checkout $testrunner_tag
  fi
  echo "Need to set ALLOW_HTP back to True to do git pull branch"
  sed -i 's/ALLOW_HTP.*/ALLOW_HTP = True/' lib/testconstants.py
  git checkout ${branch}
  git pull origin ${branch}
  echo "Set ALLOW_HTP to False so test could run."
  sed -i 's/ALLOW_HTP.*/ALLOW_HTP = False/' lib/testconstants.py

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

  # Adding static IP for sirius
  sirius[0]="http://172.23.120.103:4000"

  size=${#sirius[@]}
  index=$(($RANDOM % $size))
  sirius_url=${sirius[$index]}
  parameters=${parameters}",sirius_url="${sirius_url}
  echo ${parameters}
  # End of Sirius dep code

  echo "Timeout: $timeout minutes"
  guides/gradlew --refresh-dependencies testrunner -P jython="$jython_path" $sdk_client_params -P args="-i $WORKSPACE/testexec.$$.ini -c ${confFile} -p ${parameters} -m ${mode} ${rerun_param}"

  status=$?
  echo workspace is $WORKSPACE
  fails=`cat $WORKSPACE/logs/*/*.xml | grep 'testsuite errors' | awk '{split($3,s1,"=");print s1[2]}' | sed s/\"//g | awk '{s+=$1} END {print s}'`
  echo fails is $fails
  total_tests=`cat $WORKSPACE/logs/*/*.xml | grep 'testsuite errors' | awk '{split($6,s1,"=");print s1[2]}' | sed s/\"//g |awk '{s+=$1} END {print s}'`
  echo $total_tests
  echo Desc1: $version_number - $desc2 - $os \($(( $total_tests - $fails ))/$total_tests\)
  if [ ${rerun_job} == true ]; then
  	guides/gradlew --stacktrace rerun_job -P jython="$jython_path" $sdk_client_params -P args="${version_number} --executor_jenkins_job --run_params=${parameters}"
  fi
  # Check if gradle had clean exit. If not, fail the job.
  if [ ! $status = 0 ]; then
  	echo "Gradle had non zero exit. Failing the job"
    exit 1
  fi

else
  echo Desc: $desc
  newState=failedInstall
  echo newState=failedInstall>propfile
  if [ ${rerun_job} == true ]; then
  	guides/gradlew --stacktrace rerun_job -P jython="$jython_path" $sdk_client_params -P args="${version_number} --executor_jenkins_job --install_failure"
  fi
fi

# To reduce the disk consumption post run
rm -rf .git b build conf guides pytests
