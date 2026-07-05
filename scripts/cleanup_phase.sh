#!/bin/bash

# To clean any available space from docker
docker system prune -f

# To kill Orphan Python / magmaloader.jar
ps -ef | grep 'python testrunner.py' | awk '$3 == 1 {print $2}' | xargs kill -9
ps -ef | grep 'java -jar' | grep 'magmadocloader' | awk '$3 == 1 {print $2}' | xargs kill -9

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
