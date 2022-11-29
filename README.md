# TAF

## Initial Setup

### On Linux

1. Install Java. sdkman is a very useful tool for this.  JDK 11 is preferred as Jython doesn't seem to like JDK8 on Ubuntu 18+.

2. Install Jython and add submodule dependency. Download Jython Installer from [here](https://repo1.maven.org/maven2/org/python/jython-installer/2.7.2/jython-installer-2.7.2.jar)

```bash
jython_path=/opt/jython
mkdir $jython_path
java -jar jython-installer-2.7.2.jar -d $jython_path -s

# Installing dependency packages
cat requirements.txt | xargs | xargs $jython_path/bin/easy_install

# Adding a submodule to the Git repository
git submodule init
git submodule update --init --force --remote
```

### On Mac

```bash
# Installs Jython 2.7.3
brew install jython

# Installing dependency packages
cat requirements.txt | xargs | xargs jython -m pip install

# Adding a submodule to the Git repository
git submodule init
git submodule update --init --force --remote
```


## Test Environment Requirement

```
- Set of VMs with Couchbase-Server running with correct version. General practice: Keep Couchbase credentials as Administrator:password.
- ssh access for root user on the VMs.
```

One way of setting this up is:
Clone vagrants

```bash 
git clone https://github.com/couchbaselabs/vagrants.git
```

Choose one of those sub-folders, and run `vagrant up` in it.

Get the ip addresses of the node as we need to later in ini file.
	`vagrant status`

Change the password of the nodes for root user to password:

```bash
vagrant ssh node1
sudo passwd
```

Enable root login

```bash
cd /etc/ssh
vi sshd_config
```

Enable root login for the vagrant boxes. To do that, change this setting(It may be different on various linus flavors):

```bash
PermitRootLogin yes
```

Restart sshd:

`sudo systemctl restart sshd` or `sudo service restart sshd`, depending on your vagrant's Linux flavour.

## Running

When running Jython, you have to supply the CLASSPATH.  For a typical Java program this is long, and typically project management tools 
like Gradle and Maven are used to automatically pull in all dependencies, produce the CLASSPATH, and run the Java app.

So a gradle project is included with a task, "testrunner", that runs Jython on the testrunner.py script, sorting out the CLASSPATH
automatically (including downloading all dependencies). A Gradle Wrapper of the correct version is also provided.

Execute:

```bash

./gradlew --refresh-dependencies testrunner -P jython="/path/to/jython" -P args="-i <ini file path> -t <absolute path of test case>"```

Examples:
  ./gradlew --refresh-dependencies testrunner -P jython="/path/to/jython" -P args="-i tmp/local.ini -t rebalance_new.rebalance_in.RebalanceInTests.test_rebalance_in_with_ops,nodes_in=3,GROUP=IN;P0;default -m rest"
```

(Replace `gradlew` with `gradlew.bat` on Windows).

The above command will run the test mentioned using -t option on the cluster defined in local.ini file. 

NOTE: (The `--refresh-dependencies` isn't strictly required every run.  It makes sure that the latest versions of any SNAPSHOT dependencies
have been loaded - e.g. for transactions.)


### Sample ini file for 4 node cluster

```bash
[global]
username:root <ssh user>
password:couchbase <password>
index_port:9102
n1ql_port:8093

[membase]
rest_username:Administrator <Couchbase server console user>
rest_password:password <Couchbase server console password>

[servers]
1:_1
2:_2
3:_3
4:_4

[_1]
ip:<IP>
port:8091
services:kv

[_2]
ip:<IP>
port:8091
services:kv

[_3]
ip:<IP>
port:8091
services:kv

[_4]
ip:<IP>
port:8091
services:kv
```

## Cluster_run support
TAF supports cluster_run with following additional params under the specified sections,
Note: Make sure your rest_password is 'password'

```
[global]
username:Administrator
password:password
# Base directory of the CB Server repo checkout, used to locate cli tools
cli:/path/to/couchbase/server/source/dir

[membase]
rest_username:Administrator
rest_password:password

[servers]
1:_1
2:_2
3:_3

[_1]
ip:127.0.0.1
port:9000
memcached_port:12000

[_2]
ip:127.0.0.1
port:9001
memcached_port:12002

[_3]
ip:127.0.0.1
port:9002
memcached_port:12004

```

## Jython Issues
If Jython reports `Exception in thread "main" java.lang.NoSuchMethodError: java.nio.ByteBuffer.limit(I)Ljava/nio/ByteBuffer;`

Jython appears to be broken on Ubuntu 18+: https://bugs.launchpad.net/ubuntu/+source/jython/+bug/1771476

Resolved by changing to OpenJDK 11.  With sdkman installed:

`sdk use java 11.0.3-zulu`
