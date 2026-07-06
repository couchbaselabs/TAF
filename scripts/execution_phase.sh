#!/bin/bash

# Runs as its own process/stage - see install_phase.sh and cleanup_phase.sh
# for the other two. Only meant to run after install_phase.sh has exited 0 in
# the same workspace (Jenkins skips this stage automatically otherwise, since
# a failed "Install TAF" stage aborts the pipeline before "Run TAF Tests").
# That's why the old `if [ $status -eq 0 ] ... else ... fi` wrapper is gone -
# $status was set by install_phase.sh and doesn't survive into this separate
# process, and by the time this script runs, install has already succeeded.
# Uses $BUILD_NUMBER instead of $$ for the shared ini filename, matching
# install_phase.sh.

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

# To pass client-versions to use from cmd_line
sdk_client_params="-P transaction_version=$transaction_version -P client_version=$java_client_version"

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

set -x
guides/gradlew --no-daemon --refresh-dependencies testrunner -P jython="$jython_path" $sdk_client_params -P args="-i $WORKSPACE/testexec.$BUILD_NUMBER.ini -c ${confFile} -p ${parameters} -m ${mode} ${rerun_params}"
status=$?
set +x
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

# To reduce the disk consumption post run
rm -rf .git b build conf guides pytests
# To clean any available space from docker
docker system prune -f
