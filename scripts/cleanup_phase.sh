#!/bin/bash

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

# To clean any available space from docker
docker system prune -f

# To kill Orphan Python / magmaloader.jar
ps -ef | grep 'python testrunner.py' | awk '$3 == 1 {print $2}' | xargs kill -9
ps -ef | grep 'java -' | grep 'magmadocloader' | awk '$3 == 1 {print $2}' | xargs kill -9

# Reclaim disk space from gradle files
for i in `ls /tmp/gradle*.bin`; do
  lsof $i > /dev/null
  if [ $? -eq 1 ]; then
    rm -f $i
  fi
done
for file in `find ~/.gradle/ -name "*.out.log"`; do
    lsof_line_count=`lsof $file | grep -v COMMAND | wc -l`
    if [ $lsof_line_count -eq 0 ]; then
        rm -f $file
    fi
done

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
