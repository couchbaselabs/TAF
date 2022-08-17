#!/bin/bash

jython_path=/opt/jython
jython_bin=${jython_path}/bin
jython_exe=${jython_bin}/jython

if [ ! -f "$jython_exe" ]; then
    # Jython not present
    echo "Installing Jython"
    rm -rf $jython_path
    mkdir $jython_path
    cd /tmp
    wget https://repo1.maven.org/maven2/org/python/jython-installer/2.7.2/jython-installer-2.7.2.jar
    java -jar jython-installer-2.7.2.jar -d $jython_path -s
    rm -f jython-installer-2.7.2.jar
    cd -

    echo "Installing dependencies..."
    cat requirements.txt | grep -v "#" | xargs | xargs ${jython_bin}/easy_install
    echo "Jython installation done"
fi
