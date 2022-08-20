#!/bin/bash

usage() {
    echo "Usage:"
    echo " -p|--path   Path to install"
    echo " -h|--help   Displays this help message"
    echo " --force     Force install"
    echo ""
}

force_install=false
jython_path=/opt/jython

while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--path)
            jython_path=$2
            shift ; shift
            ;;
        --force)
            force_install=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Error: Unknown option '$1'"
            usage
            exit 1
            ;;
    esac
done

jython_bin=${jython_path}/bin
jython_exe=${jython_bin}/jython

if [ $force_install = "true" -o ! -f "$jython_exe" ]; then
    echo "Installing Jython on '$jython_path'"
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
