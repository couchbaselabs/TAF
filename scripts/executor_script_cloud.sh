#!/bin/bash

setup_test_infra_repo_for_installation() {
  git clone https://github.com/couchbaselabs/test_infra_runner --depth 1
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
    -i $WORKSPACE/${iniFile} \
    -p ${os} \
    -o $WORKSPACE/testexec.$$.ini \
    -k '{'${UPDATE_INI_VALUES}'}' \
    --keyValue "${cluster_info}"
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

## cherrypick the gerrit request if it was defined
if [ "$cherrypick" != "None" ] && [ "$cherrypick" != "" ] ; then
   echo "###############################################"
   echo "########### GIT :: Fetching patch #############"
   echo "###############################################"
   echo "$cherrypick"
   sh -c "$cherrypick"
   echo "###############################################"
fi

set +e
echo newState=available>propfile
newState=available

#majorRelease=${version_number:0:1} - this did not work on
majorRelease=`echo ${version_number} | awk '{print substr($0,1,1)}'`
echo the major release is $majorRelease

echo ${servers}

# Setup GoLang in local dir
go_version=1.22.4
wget https://golang.org/dl/go${go_version}.linux-amd64.tar.gz --quiet
tar -xzf go${go_version}.linux-amd64.tar.gz
rm -f go${go_version}.linux-amd64.tar.gz
export GOPATH=`pwd`/go
export PATH="${GOPATH}/bin:${PATH}"
export GO111MODULE=on
# Set desired python env
export PYENV_VERSION=3.10.14
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
pyenv local $PYENV_VERSION

# Install requirements for pip
python -m pip install -r requirements.txt

# Export JAVA_HOME for delta_spark dependencies to work
tem_java_home=`update-alternatives --list java | head -1 | sed  's|/bin/java||g'`
export JAVA_HOME=`echo $tem_java_home | sed  's|/jre||g'`
echo "JAVA_HOME is $JAVA_HOME"
# Fetch rerun job to run
echo "" > rerun_props_file
if [ ${fresh_run} == false ]; then
 # Find cases for rerun
 set -x
 python scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --manual_run
 set +x
fi

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

# Below "if" block added by UMANG to run columnar tests
if [ "$server_type" = "SERVERLESS_COLUMNAR" ]; then
  cluster_info="{\"pod\": \"$capella_api_url\", \"tenant_id\": \"$tenant_id\", \"capella_user\": \"$capella_user\", \"capella_pwd\": \"$capella_password\", \"region\": \"$capella_region\", \"project\": \"$project_id\", \"override_token\": \"$override_token\", \"columnar_image\": \"$cbs_image\", \"override_key\": \"$override_key\"}"
  #echo python signup_user.py -e ${capella_email_prefix} -a $capella_api_url -x $capella_signup_token -r $capella_region
  #cluster_info=`python signup_user.py -e ${capella_email_prefix} -a $capella_api_url -x $capella_signup_token -r $capella_region`
  echo $cluster_info
  servers="None"
fi

# Below "if" block added by SHAAZIN to run provisioned cloud security tests
if [ "$server_type" = "PROVISIONED_ONCLOUD" ]; then
  #added below 2 lines - by Shaazin
  date
  cluster_info="{\"pod\": \"$capella_api_url\", \"tenant_id\": \"$tenant_id\", \"capella_user\": \"$capella_user\", \"capella_pwd\": \"$capella_password\", \"project_id\": \"$project_id\", \"region\": \"$capella_region\"}"
  #commented below 2 lines by Shaazin
  #echo python signup_user.py -e ${capella_email_prefix} -a $capella_api_url -x $capella_signup_token -r $capella_region
  #cluster_info=`python signup_user.py -e ${capella_email_prefix} -a $capella_api_url -x $capella_signup_token -r $capella_region`
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
  echo python signup_user.py -e ${capella_email_prefix} -a $capella_api_url -x $capella_signup_token -r $capella_region
  cluster_info=`python signup_user.py -e ${capella_email_prefix} -a $capella_api_url -x $capella_signup_token -r $capella_region`
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

touch $WORKSPACE/testexec.$$.ini
setup_test_infra_repo_for_installation
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

timedatectl
if [ ${skip_install} == false ]; then
  if [ "$os" = "windows" ] ; then
    export install_params="timeout=2000,skip_local_download=False,version=${version_number},product=cb,parallel=${parallel},init_nodes=${initNodes},debug_logs=True,url=${url}${extraInstall}"
    do_install
  else
      # To handle nonroot user
      echo sed 's/nonroot/root/g' $WORKSPACE/testexec.$$.ini > $WORKSPACE/testexec_root.$$.ini
      sed 's/nonroot/root/g' $WORKSPACE/testexec.$$.ini > $WORKSPACE/testexec_root.$$.ini

      if [ "$os" != "mariner2" ]; then
      	python scripts/ssh.py -i $WORKSPACE/testexec.$$.ini "iptables -F"
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
      # Install requirements for this venv
      do_install
  fi
fi

if [ "$?" -eq 0 ]; then
  if [ "$server_type" = "CAPELLA_LOCAL" ] && [ ${skip_install} == false ]; then
    ############# LOCAL CAPELLA SETUP ####################
    git clone https://github.com/couchbaselabs/productivitynautomation

    export ANSIBLE_CONFIG=$PWD/productivitynautomation/ansible_setup/.ansible.cfg
    export ANSIBLE_HOST_KEY_CHECKING=false
    mkdir -p /root/cloud
    python $PWD/productivitynautomation/ansible_setup/create_hosts.py $WORKSPACE/testexec.$$.ini $PWD/productivitynautomation/ansible_setup/hosts_template $PWD/ans_hosts
#    IFS=', ' read -r -a server_array <<< "$servers"
#    for index in "${!server_array[@]}";
#    do
#        serv=$(echo ${server_array[$index]}|tr -d '"');
#        sed -i "s/host$index/$serv/g" $PWD/productivitynautomation/ansible_setup/hosts;
#    done

#    \cp -rf $PWD/productivitynautomation/ansible_setup/hosts /root/cloud/hosts
    #python scripts/create_cloud_ansible.py -i $WORKSPACE/testexec.$$.ini -o /root/cloud/hosts
    #pushd productivitynautomation/ansible_setup/

    python -m venv $PWD/ansible
    source $PWD/ansible/bin/activate
    python -m pip install ansible paramiko
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
    	echo "sed -i 's/admin_bucket_username:Administrator/admin_bucket_username:user1/g;s/rest_username:Administrator/rest_username:user1/g' $WORKSPACE/testexec.$$.ini"
    	sed -i 's/admin_bucket_username:Administrator/admin_bucket_username:user1/g;s/rest_username:Administrator/rest_username:user1/g' $WORKSPACE/testexec.$$.ini
    fi
    #popd
    ############# END LOCAL CAPELLA SETUP ####################
  fi

  if [ ${skip_install} == true ]; then
    	set -x
    	sed -i 's/admin_bucket_username:Administrator/admin_bucket_username:user1/g;s/rest_username:Administrator/rest_username:user1/g' $WORKSPACE/testexec.$$.ini
    	sed -i 's/admin_bucket_password:password/admin_bucket_password:Passw0rd\$/g;s/rest_password:password/rest_password:Passw0rd\$/g' $WORKSPACE/testexec.$$.ini
    	set +x
  fi

  ###### Added on 4/April/2018 to fix issues related to disk full on slaves.
  find /data/workspace/*/logs/* -type d -ctime +30 -exec rm -rf {} \;
  find /data/workspace/ -type d -ctime +90 -exec rm -rf {} \;
  find /root/workspace/*/logs/* -type d -ctime +30 -exec rm -rf {} \;
  find /root/workspace/ -type d -ctime +90 -exec rm -rf {} \;
  ######

  ##Added on August 2nd 2017 to kill all python processes older than 10days, comment if it causes any failures
  ## Updated on 11/21/19 by Mihir to kill all python processes older than 3 days instead of 10 days.
  killall --older-than 72h python

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

  sed -i 's/pod\:https\:\/\//pod:/g' $WORKSPACE/testexec.$$.ini
  sed -i 's/pod\:api/pod:cloudapi/g' $WORKSPACE/testexec.$$.ini

  pyenv local $PYENV_VERSION
  cat 3.10.14/testexec.$$.ini
  # Find free port on this machine to use for this run
  starting_ports=(49152 49162 49172 49182 49192 49202 49212 49222 49232)
  num_scripts_running=$(ps -ef | grep '/tmp/jenkins' | grep -v 'grep ' | wc -l)
  sirius_port=${starting_ports[$num_scripts_running]} ; while [ "$(ss -tulpn | grep LISTEN | grep $sirius_port | wc -l)" -ne 0 ]; do sirius_port=$((sirius_port+1)) ; done
  echo "Will use $sirius_port for starting sirius"
  export PATH=/usr/local/go/bin:$PATH
  set -x
  load_docs_using=$(echo "$parameters" | grep -oP 'load_docs_using=\K[^,]*')
  if [[ "$load_docs_using" == "sirius_java_sdk" ]]; then
    echo "Using Sirius Java SDK to load documents."
    python testrunner.py -c $confFile -i $WORKSPACE/testexec.$$.ini -p "$parameters" --launch_java_doc_loader --sirius_url http://localhost:$sirius_port ${rerun_params}
  else
    echo "Using default doc loader."
    python testrunner.py -c $confFile -i $WORKSPACE/testexec.$$.ini -p "$parameters" --launch_sirius_docker --sirius_url http://localhost:$sirius_port ${rerun_params}
  fi
  python scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --run_params=${parameters}
  status=$?
  set +x

  # fails=`cat $WORKSPACE/logs/*/*.xml | grep 'testsuite errors' | awk '{split($3,s1,"=");print s1[2]}' | sed s/\"//g | awk '{s+=$1} END {print s}'`
  # total_tests=`cat $WORKSPACE/logs/*/*.xml | grep 'testsuite errors' | awk '{split($6,s1,"=");print s1[2]}' | sed s/\"//g |awk '{s+=$1} END {print s}'`
  # echo Desc1: $version_number - $desc2 - $os \($(( $total_tests - $fails ))/$total_tests\)

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
fi
