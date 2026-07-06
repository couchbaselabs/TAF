#!/bin/bash

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

for file in `find ~/.gradle/ -name "*.out.log"`
do
    lsof_line_count=`lsof $file | grep -v COMMAND | wc -l`
    if [ $lsof_line_count -eq 0 ]; then
        rm -f $file
    fi
done

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

# Clone the guides repo for Gradle command
git clone https://github.com/couchbaselabs/guides.git
