#!/bin/bash

if [[ -z $2 ]] ; then
    echo "$0 --ini <ini> --test <conf|test_to_run>"
    exit
fi

ini=""
test=""
serverless=""
test_params=" -p infra_log_level=error,log_level=info"
jython_path="/opt/jython"
install_jython=false
workspace_dir=""
quiet=false

# Process cmd line args
while [[ $# -gt 0 ]]; do
    case $1 in
        --ini)
            ini=$2
            shift ; shift
            ;;
        --test)
            test=$2
            shift ; shift
            ;;
        --serverless)
            serverless=" --serverless"
            shift
            ;;
        --jython_path)
            jython_path=$2
            shift ; shift
            ;;
        --install_jython)
            install_jython=true
            shift
            ;;
        --workspace_dir)
            workspace_dir=$2
            shift  ; shift
            ;;
        *)
            echo "ERROR: Invalid argument '$1'"
    esac
done

echo "##################################################"
echo "  ini  - $ini"
echo "  test - $test"
echo "  serverless  - $serverless"
echo "  jython_path - $jython_path"
echo "  install_jython - $install_jython"
echo "  workspace_dir  - $workspace_dir"
echo "##################################################"

# test if a number was passed instead of an ini file
if [ "$ini" -eq "$ini" ] 2>/dev/null; then
    numnodes=$ini
    ini=make_test.ini

    echo "[global]
username:Administrator
password:asdasd
cli:/path/to/build_dir

[membase]
rest_username:Administrator
rest_password:asdasd

[servers]" > ${ini}
    i=1
    while [[ $i -le $numnodes ]] ; do
        echo "$i:127.0.0.1_$i" >> ${ini}
        i=$((i+1))
    done
    i=1
    while [[ $i -le $numnodes ]] ; do
        echo "" >> ${ini}
        echo "[127.0.0.1_$i]" >> ${ini}
        echo "ip:127.0.0.1" >> ${ini}
        echo "port:$((9000+i-1))" >> ${ini}
        i=$((i+1))
    done
fi

if [ -n "$workspace_dir" ]; then
    echo "Updating cli path to '$workspace_dir'"
    sed -i "/^cli:.*/ccli:$workspace_dir/" ${ini}
fi

# test if a conf file or testcase was passed
if [[ -f $test ]] ; then
    conf=" -c $test"
else
    conf=" -t $test"
fi

servers_count=0
while read line ; do
    if $(echo "$line" | grep "^\[.*\]$" &> /dev/null) ; then
        section="$line"
        read line
    fi
    if [[ "$section" == "[servers]" && "$line" != "" ]] ; then
        servers_count=$((servers_count + 1))
    fi
done < $ini

wd=$(pwd)
pushd .
cd ../ns_server
if [ "${quiet}" = "true" ]
then
   make dataclean >> make.out 2>&1
   make >> make.out 2>&1
else
   make dataclean
   make
fi

./cluster_run --nodes=$servers_count $serverless &> $wd/cluster_run.log &
pid=$!
popd

if [ $install_jython == "true" ]; then
  chmod +x scripts/jython_install.sh
  echo "./scripts/jython_install.sh --path '$jython_path'"
  ./scripts/jython_install.sh --path "$jython_path"
fi

# Adding a submodule to the Git repository
git submodule init
git submodule update --init --force --remote

git clone https://github.com/couchbaselabs/guides.git

# Start test execution
echo "guides/gradlew --refresh-dependencies testrunner -P jython='${jython_path}/bin/jython' -P 'args=-i $ini $test_params $conf -m rest' 2>&1  | tee make_test.log"
guides/gradlew --refresh-dependencies testrunner -P jython="${jython_path}/bin/jython" -P "args=-i $ini $test_params $conf -m rest" 2>&1  | tee make_test.log

kill $pid
wait
tail -c 1073741824 $wd/cluster_run.log &> $wd/tmp.log
cp $wd/tmp.log $wd/cluster_run.log
rm $wd/tmp.log
! grep FAILED make_test.log &> /dev/null
