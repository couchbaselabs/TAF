#!/bin/bash

support_ver="6.5"
small_ver=${version_number:0:3}
py_executable=python3
jython_path=/opt/jython/bin/jython

git clone https://github.com/couchbaselabs/guides.git

if [ ${fresh_run} == false ]; then
  # Fetch rerun job details to prepare for rerun.
  # This also creates a file 'rerun_props_file' using open(.., "w") API
  guides/gradlew --refresh-dependencies --stacktrace rerun_job -P jython="$jython_path" -P args="${version_number} --executor_jenkins_job --manual_run"
fi

echo "Set ALLOW_HTP to False so test could run."
sed -i 's/ALLOW_HTP.*/ALLOW_HTP = False/' lib/testconstants.py

## cherrypick the gerrit request if it was defined
if [ "$cherrypick" != "None" ]; then
   sh -c "$cherrypick"
fi

set +e
echo newState=available>propfile
newState=available

#majorRelease=${version_number:0:1} - this did not work on
majorRelease=`echo ${version_number} | awk '{print substr($0,1,1)}'`
echo the major release is $majorRelease

echo ${servers}

${py_executable} -m pip install easy_install
${py_executable} -m easy_install httplib2 ground hypothesis_geometry argparse psycopg2 dnspython boto3

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

# Below "if" block added by UMANG to run goldfish tests
if [ "$server_type" = "SERVERLESS_COLUMNAR" ]; then
  cluster_info="{\"pod\": \"$capella_api_url\", \"tenant_id\": \"$tenant_id\", \"capella_user\": \"$capella_user\", \"capella_pwd\": \"$capella_password\", \"region\": \"$capella_region\"}"
  #echo ${py_executable} signup_user.py -e ${capella_email_prefix} -a $capella_api_url -x $capella_signup_token -r $capella_region
  #cluster_info=`${py_executable} signup_user.py -e ${capella_email_prefix} -a $capella_api_url -x $capella_signup_token -r $capella_region`
  echo $cluster_info
  servers="None"
fi

# Below "if" block added by SHAAZIN to run provisioned cloud security tests
if [ "$server_type" = "PROVISIONED_ONCLOUD" ]; then
  #added below 2 lines - by Shaazin
  date
  cluster_info="{\"pod\": \"$capella_api_url\", \"tenant_id\": \"$tenant_id\", \"capella_user\": \"$capella_user\", \"capella_pwd\": \"$capella_password\", \"project_id\": \"$project_id\", \"region\": \"$capella_region\"}"
  #commented below 2 lines by Shaazin
  #echo ${py_executable} signup_user.py -e ${capella_email_prefix} -a $capella_api_url -x $capella_signup_token -r $capella_region
  #cluster_info=`${py_executable} signup_user.py -e ${capella_email_prefix} -a $capella_api_url -x $capella_signup_token -r $capella_region`
  if [ -n "$capella_dataplane_id" ]; then
    cluster_info=`echo $cluster_info | sed s/}/,\ \"dataplane_id\":\ \"$capella_dataplane_id\"}/`
  fi
  if [ -n "$cbs_image" ]; then
    cluster_info=`echo $cluster_info | sed s/}/,\ \"cb_image\":\ \"$cbs_image\"}/`
  fi
  if [ -n "$access_key" ]; then
    cluster_info=`echo $cluster_info | sed s/}/,\ \"access_key\":\ \"$access_key\"}/`
  fi
  if [ -n "$secret_key" ]; then
    cluster_info=`echo $cluster_info | sed s/}/,\ \"secret_key\":\ \"$secret_key\"}/`
  fi
  echo $cluster_info
  servers="None"
fi

# For Capella, signup new user to be use for this test execution
if [ "$server_type" = "SERVERLESS_ONCLOUD" ]; then
#if [ "$server_type" = "SERVERLESS_ONCLOUD" -o "$server_type" = "PROVISIONED_ONCLOUD" ]; then - commented by Shaazin
  echo ${py_executable} signup_user.py -e ${capella_email_prefix} -a $capella_api_url -x $capella_signup_token -r $capella_region
  cluster_info=`${py_executable} signup_user.py -e ${capella_email_prefix} -a $capella_api_url -x $capella_signup_token -r $capella_region`
  if [ -n "$capella_dataplane_id" ]; then
    cluster_info=`echo $cluster_info | sed s/}/,\ \"dataplane_id\":\ \"$capella_dataplane_id\"}/`
  fi
  if [ -n "$cbs_image" ]; then
    cluster_info=`echo $cluster_info | sed s/}/,\ \"cb_image\":\ \"$cbs_image\"}/`
  fi
  if [ -n "$access_key" ]; then
    cluster_info=`echo $cluster_info | sed s/}/,\ \"access_key\":\ \"$access_key\"}/`
  fi
  if [ -n "$secret_key" ]; then
    cluster_info=`echo $cluster_info | sed s/}/,\ \"secret_key\":\ \"$secret_key\"}/`
  fi
  echo $cluster_info
  servers="None"
fi

if [ ${skip_install} == true ]; then
  skipped_install_for_capella=-m
else
  skipped_install_for_capella=
fi

# Run populate ini from testrunner instead of running frm TAF due to changes in TAF to move to python3
mkdir tr_for_install
cd tr_for_install
git clone https://github.com/couchbase/testrunner.git
cd testrunner
git submodule init
git submodule update --init --force --remote

echo ${py_executable}  scripts/populateIni.py -s ${servers} -d ${addPoolServerId} -a ${addPoolServers} -i ${iniFile} -p ${os} -o /tmp/testexec.$$.ini --keyValue "$cluster_info"
${py_executable}  scripts/populateIni.py -s ${servers} -d ${addPoolServerId} -a ${addPoolServers} -i ${iniFile} -p ${os} -o /tmp/testexec.$$.ini --keyValue "$cluster_info"

# Get back to TAF directory
cd ../../
rm -rf tr_for_install

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

timedatectl
if [ ${skip_install} == false ]; then
  if [ "$os" = "windows" ] && ( [ $(${py_executable} -c "print($small_ver < $support_ver)") = True ] || [[ $small_ver != "0.0" ]] ); then
      #echo "Using install.py"
      #first_with_new_installer="4.7"
      #dot_release=${version_number:0:3}
      #echo the dot release is $dot_release
      #if [ $(echo "$first_with_new_installer <= $dot_release" | bc) -eq 0 ]; then
          #echo pre 4.7 release
      ${py_executable} scripts/install.py -i /tmp/testexec.$$.ini -p version=${version_number},product=cb,parallel=${parallel},init_nodes=${initNodes},url=${url}${extraInstall}
      #else
          #echo 4.7 or greater
          # interim nsis install
          #python ./scripts/install.py -i /tmp/testexec.$$.ini -p version=4.7.0-903,product=cb,parallel=true,vbuckets=1024,nsis=True,url=http://172.23.120.24/builds/latestbuilds/couchbase-server/spock/win_installers/nsis/couchbase-server-enterprise_4.7.0-903-windows_amd64.exe
      #fi
  else
      #To handle nonroot user
      echo sed 's/nonroot/root/g' /tmp/testexec.$$.ini > /tmp/testexec_root.$$.ini
      sed 's/nonroot/root/g' /tmp/testexec.$$.ini > /tmp/testexec_root.$$.ini
      echo ${py_executable} scripts/ssh.py -i /tmp/testexec_root.$$.ini "iptables -F"
      ${py_executable} scripts/ssh.py -i /tmp/testexec_root.$$.ini "iptables -F"
      #python scripts/install.py -i /tmp/testexec.$$.ini -p version=${version_number},product=cb,parallel=${parallel},init_nodes=${initNodes}${extraInstall}
      #NOTE: Below 2 lines are for finding the IPs for uninstall during downstream cleanup job.

      if [ "${INSTALL_TIMEOUT}" = "" ]; then
         INSTALL_TIMEOUT="1200"
      fi
      # 6.5.x has install issue. Reverting to older style
      if [ "${SKIP_LOCAL_DOWNLOAD}" = "" ]; then
         SKIP_LOCAL_DOWNLOAD="False"
      fi

      if [ ! "$majorRelease" = "7" ]; then
         SKIP_LOCAL_DOWNLOAD="True"
      fi

      cp /tmp/testexec.$$.ini $WORKSPACE/
      initial_version=$(echo $parameters | grep -P 'initial_version=[0-9]+.[0-9].[0-9]+-[0-9]+' -o | cut -d '=' -f 2)
      #initial_version= $(echo ${parameters} |sed 's/.*,initial_version=\([0-9].[0-9].[0-9]-[0-9]\{4\}\),.*/ \1/')
      echo "Initial version: $initial_version"
      if [ -n "$initial_version" ]; then
          echo ${py_executable} scripts/new_install.py -i /tmp/testexec.$$.ini -p timeout=${INSTALL_TIMEOUT},skip_local_download=${SKIP_LOCAL_DOWNLOAD},get-cbcollect-info=True,version=${initial_version},product=cb,debug_logs=True,ntp=True,url=${url}${extraInstall}
          ${py_executable} scripts/new_install.py -i /tmp/testexec.$$.ini -p timeout=${INSTALL_TIMEOUT},skip_local_download=${SKIP_LOCAL_DOWNLOAD},get-cbcollect-info=True,version=${initial_version},product=cb,debug_logs=True,ntp=True,url=${url}${extraInstall}
      else
          echo ${py_executable} scripts/new_install.py -i /tmp/testexec.$$.ini -p timeout=${INSTALL_TIMEOUT},skip_local_download=${SKIP_LOCAL_DOWNLOAD},get-cbcollect-info=True,version=${version_number},product=cb,debug_logs=True,ntp=True,url=${url}${extraInstall}
          ${py_executable} scripts/new_install.py -i /tmp/testexec.$$.ini -p timeout=${INSTALL_TIMEOUT},skip_local_download=${SKIP_LOCAL_DOWNLOAD},get-cbcollect-info=True,version=${version_number},product=cb,debug_logs=True,ntp=True,url=${url}${extraInstall}
      fi
      #echo "Sleeping now..."
      #sleep 90000
      status=$?
      if [ ! $status = 0 ]; then
          echo exiting
          echo Desc: $desc
          newState=failedInstall
          echo newState=failedInstall>propfile
          if [ ${rerun_job} == true ]; then
              echo "${py_executable} scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --install_failure"
              ${py_executable} scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --install_failure || true
          fi
          exit 1
      else
          echo success
      fi
  fi
fi

if [ "$?" -eq 0 ]; then
  desc2=`echo $descriptor | awk '{split($0,r,"-");print r[1],r[2]}'`
  #export test_params="get-cbcollect-info=True"
  if [ ${skip_install} == false ]; then
    ${py_executable} scripts/ssh.py -i /tmp/testexec.$$.ini "iptables -F"
  fi

  if [ $majorRelease = "3" ]; then
     echo have a 3.x release
     git checkout 0b2bb7a53e350f90737112457877ef2c05ca482a
  elif [[ "$testrunner_tag" != "master" ]]; then
     git checkout $testrunner_tag
  fi
  echo "Need to set ALLOW_HTP back to True to do git pull branch"
  sed -i 's/ALLOW_HTP.*/ALLOW_HTP = True/' lib/testconstants.py
  #git checkout ${branch}
  #git pull origin ${branch}

  ###### Added on 22/March/2018 to fix auto merge failures. Please revert this if this does not work.
  #git fetch
  #git reset --hard origin/${branch}
  ######

  ## cherrypick the gerrit request if it was defined
  if [ "$cherrypick" != "None" ]; then
     sh -c "$cherrypick"
  fi

  if [ "$server_type" = "CAPELLA_LOCAL" ] && [ ${skip_install} == false ]; then
    ############# LOCAL CAPELLA SETUP ####################
    git clone https://github.com/couchbaselabs/productivitynautomation

    export ANSIBLE_CONFIG=$PWD/productivitynautomation/ansible_setup/.ansible.cfg
    export ANSIBLE_HOST_KEY_CHECKING=false
    mkdir -p /root/cloud
    ${py_executable} $PWD/productivitynautomation/ansible_setup/create_hosts.py /tmp/testexec.$$.ini $PWD/productivitynautomation/ansible_setup/hosts_template $PWD/ans_hosts
#    IFS=', ' read -r -a server_array <<< "$servers"
#    for index in "${!server_array[@]}";
#    do
#        serv=$(echo ${server_array[$index]}|tr -d '"');
#        sed -i "s/host$index/$serv/g" $PWD/productivitynautomation/ansible_setup/hosts;
#    done

#    \cp -rf $PWD/productivitynautomation/ansible_setup/hosts /root/cloud/hosts
    #${py_executable} scripts/create_cloud_ansible.py -i /tmp/testexec.$$.ini -o /root/cloud/hosts
    #pushd productivitynautomation/ansible_setup/

    ${py_executable} -m venv $PWD/ansible
    source $PWD/ansible/bin/activate
    ${py_executable} -m pip install ansible paramiko
    yum install sshpass -y

    ansible-playbook -v $PWD/productivitynautomation/ansible_setup/install-firewalld.yml -i $PWD/ans_hosts
    ansible-playbook -v $PWD/productivitynautomation/ansible_setup/12-node-init.yml -i $PWD/ans_hosts
    ansible-playbook -v $PWD/productivitynautomation/ansible_setup/13-cluster-init.yml -i $PWD/ans_hosts
    ansible-playbook -v $PWD/productivitynautomation/ansible_setup/14-certificate.yml -i $PWD/ans_hosts
    ansible-playbook -v $PWD/productivitynautomation/ansible_setup/15-cluster-config.yml --limit 'all:!couchbase_new_nodes' -i $PWD/ans_hosts
    ansible-playbook -v $PWD/productivitynautomation/ansible_setup/20-user.yml -i $PWD/ans_hosts
    if [ "$subcomponent" = "fts" ] || [ "$component" = "fts" ] || [ "$component" = "sanity" ]; then
        ansible-playbook $PWD/productivitynautomation/ansible_setup/21-fts-bucket.yml -i $PWD/ans_hosts
    elif [ "$subcomponent" = "eventing" ] || [ "$component" = "eventing" ] || [ "$component" = "sanity" ]; then
        ansible-playbook $PWD/productivitynautomation/ansible_setup/21-eventing-bucket.yml -i $PWD/ans_hosts
    elif [ "$subcomponent" = "gauntlet-capella-local" ] || [ "$component" = "gauntlet" ]; then
        ansible-playbook $PWD/productivitynautomation/ansible_setup/21-e2eapp-bucket.yml -i $PWD/ans_hosts
    else
        ansible-playbook $PWD/productivitynautomation/ansible_setup/21-bucket.yml -i $PWD/ans_hosts
    fi
    ansible-playbook $PWD/productivitynautomation/ansible_setup/30-firewall.yml -i $PWD/ans_hosts

    deactivate

    if [ "$component" != "backup_recovery" ]; then
      echo "sed -i 's/admin_bucket_username:Administrator/admin_bucket_username:user1/g;s/rest_username:Administrator/rest_username:user1/g' /tmp/testexec.$$.ini"
      sed -i 's/admin_bucket_username:Administrator/admin_bucket_username:user1/g;s/rest_username:Administrator/rest_username:user1/g' /tmp/testexec.$$.ini
    fi

    #popd

    ############# END LOCAL CAPELLA SETUP ####################

  fi

  if [ ${skip_install} == true ]; then
    echo "sed -i 's/admin_bucket_username:Administrator/admin_bucket_username:user1/g;s/rest_username:Administrator/rest_username:user1/g' /tmp/testexec.$$.ini"
    sed -i 's/admin_bucket_username:Administrator/admin_bucket_username:user1/g;s/rest_username:Administrator/rest_username:user1/g' /tmp/testexec.$$.ini

    echo "sed -i 's/admin_bucket_password:password/admin_bucket_password:Passw0rd\$/g;s/rest_password:password/rest_password:Passw0rd\$/g' /tmp/testexec.$$.ini"
    sed -i 's/admin_bucket_password:password/admin_bucket_password:Passw0rd\$/g;s/rest_password:password/rest_password:Passw0rd\$/g' /tmp/testexec.$$.ini
  fi

  ## Setup for java sdk client, e2e-app
  git submodule init
  git submodule update --init --force --remote
  if [ -f /etc/redhat-release ]; then
    yum install -y maven
  fi
  if [ -f /etc/lsb-release ]; then
    apt install maven
  fi

  ###### Added on 4/April/2018 to fix issues related to disk full on slaves.
  find /data/workspace/*/logs/* -type d -ctime +30 -exec rm -rf {} \;
  find /data/workspace/ -type d -ctime +90 -exec rm -rf {} \;
  find /root/workspace/*/logs/* -type d -ctime +30 -exec rm -rf {} \;
  find /root/workspace/ -type d -ctime +90 -exec rm -rf {} \;
  ######

  ##Added on August 2nd 2017 to kill all python processes older than 10days, comment if it causes any failures
  ## Updated on 11/21/19 by Mihir to kill all python processes older than 3 days instead of 10 days.
  killall --older-than 72h ${py_executable}

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

  sed -i 's/pod\:https\:\/\//pod:/g' /tmp/testexec.$$.ini
  sed -i 's/pod\:api/pod:cloudapi/g' /tmp/testexec.$$.ini

  cat /tmp/testexec.$$.ini
  git clone https://github.com/couchbaselabs/guides.git
  echo guides/gradlew --refresh-dependencies testrunner -P jython="$jython_path" $sdk_client_params -P args="-i /tmp/testexec.$$.ini -c ${confFile} -p ${parameters} -m rest ${rerun_param}"
  guides/gradlew --refresh-dependencies testrunner -P jython="$jython_path" $sdk_client_params -P args="-i /tmp/testexec.$$.ini -c ${confFile} -p ${parameters} -m rest ${rerun_param}"


  echo workspace is $WORKSPACE
  fails=`cat $WORKSPACE/logs/*/*.xml | grep 'testsuite errors' | awk '{split($3,s1,"=");print s1[2]}' | sed s/\"//g | awk '{s+=$1} END {print s}'`
  echo fails is $fails
  total_tests=`cat $WORKSPACE/logs/*/*.xml | grep 'testsuite errors' | awk '{split($6,s1,"=");print s1[2]}' | sed s/\"//g |awk '{s+=$1} END {print s}'`
  echo $total_tests
  echo Desc1: $version_number - $desc2 - $os \($(( $total_tests - $fails ))/$total_tests\)
  if [ ${rerun_job} == true ]; then
    guides/gradlew --stacktrace rerun_job -P jython="$jython_path" $sdk_client_params -P args="${version_number} --executor_jenkins_job --run_params=${parameters}"
  fi
else
  echo Desc: $desc
  newState=failedInstall
  echo newState=failedInstall>propfile
fi
