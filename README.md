# TAF

## Initial Setup

Install Java.  sdkman is a very useful tool for this.  JDK 11 is preferred as Jython doesn't seem to like JDK8 on Ubuntu 18+.

Download Jython Installer from https://www.jython.org/downloads.html

```java -jar jython-installer-2.7.0.jar```

Install it wherever you like.

## Test Environment
This test-suite requires:
 
- A Couchbase cluster with credentials Administrator:password.
- ssh access.

One way of setting this up is:

`git clone https://github.com/couchbaselabs/vagrants.git`

Choose one of those subfolders, and run `vagrant up` in it.

`vagrant status` --> shows the ip address of the node

Login to those nodes on port 8091, and setup a Couchbase cluster.  You must use credentials Administrator:password.

Change the password of the nodes:
```
vagrant ssh node1
sudo passwd
--change password(enter password twice)

cd /etc/ssh
vi sshd-config
```

Insert this config:

```
AcceptEnv LANG LC_*
ChallengeResponseAuthentication no
GSSAPIAuthentication no
PermitRootLogin yes
PrintMotd no
Subsystem sftp /usr/libexec/openssh/sftp-server
UseDNS no
UsePAM yes
X11Forwarding yes
```

restart sshd:

`sudo systemctl restart sshd` or `sudo service restart sshd`, depending on your vagrant's Linux flavour.

## Running

When running Jython, you have to supply the CLASSPATH.  For a typical Java program this is long, and typically project management tools 
like Gradle and Maven are used to automatically pull in all dependencies, produce the CLASSPATH, and run the Java app.

So a gradle project is included with a task, "testrunner", that runs Jython on the testrunner.py script, sorting out the CLASSPATH
automatically (including downloading all dependencies).

Run like this:

```gradle --refresh-dependencies testrunner -P jython="/path/to/jython" -P args="-i /tmp/atomicity.ini [other testrunner args]"```

(The `--refresh-dependencies` isn't strictly required every run.  It makes sure that the latest versions of any SNAPSHOT dependencies
have been loaded - e.g. for transactions.)

## Jython Issues
If Jython reports `Exception in thread "main" java.lang.NoSuchMethodError: java.nio.ByteBuffer.limit(I)Ljava/nio/ByteBuffer;`

Jython appears to be broken on Ubuntu 18+: https://bugs.launchpad.net/ubuntu/+source/jython/+bug/1771476

Resolved by changing to OpenJDK 11.  With sdkman installed:
 
`sdk use java 11.0.3-zulu`