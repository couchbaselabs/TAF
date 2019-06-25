# TAF

## Initial Setup

1. Install Java. sdkman is a very useful tool for this.  JDK 11 is preferred as Jython doesn't seem to like JDK8 on Ubuntu 18+.

2. Install Jython. Download Jython Installer from [here](https://www.jython.org/downloads.html)

```bash
java -jar jython-installer-2.7.0.jar Install it wherever you like.
```

3. Install gradle or you the existing gradlew executable in TAF


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
vi sshd-config
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
automatically (including downloading all dependencies).

Execute:

```bash

gradle --refresh-dependencies testrunner -P jython="/path/to/jython" -P args="-i <ini file path> -t <absolute path of test case>"```

Examples:
  gradle --refresh-dependencies testrunner -P jython="/path/to/jython" -P args="-i tmp/local.ini -t rebalance_new.rebalance_in.RebalanceInTests.test_rebalance_in_with_ops,nodes_in=3,GROUP=IN;P0;default -m rest"
```

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

## Jython Issues
If Jython reports `Exception in thread "main" java.lang.NoSuchMethodError: java.nio.ByteBuffer.limit(I)Ljava/nio/ByteBuffer;`

Jython appears to be broken on Ubuntu 18+: https://bugs.launchpad.net/ubuntu/+source/jython/+bug/1771476

Resolved by changing to OpenJDK 11.  With sdkman installed:

`sdk use java 11.0.3-zulu`