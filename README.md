# Couchbase Transactions Test Tool

# Initial Setup

Install Java.  sdkman is a very useful tool for this.  JDK 11 is preferred as Jython doesn't seem to like JDK8 on Ubuntu 18+.

Download Jython Installer from https://www.jython.org/downloads.html

```java -jar jython-installer-2.7.0.jar```

Install it wherever you like.

# Running

When running Jython, you have to supply the CLASSPATH.  For a typical Java program this is long, and typically project management tools 
like Gradle and Maven are used to automatically pull in all dependencies, produce the CLASSPATH, and run the Java app.

So a gradle project is included with a task, "testrunner", that runs Jython on the testrunner.py script, sorting out the CLASSPATH
automatically (including downloading all dependencies).

Run like this:

```gradle testrunner -P jython="/path/to/jython" -P args="-i /tmp/atomicity.ini [other testrunner args]"```

# Jython Issues
If Jython reports `Exception in thread "main" java.lang.NoSuchMethodError: java.nio.ByteBuffer.limit(I)Ljava/nio/ByteBuffer;`

Jython appears to be broken on Ubuntu 18+: https://bugs.launchpad.net/ubuntu/+source/jython/+bug/1771476

Resolved by changing to OpenJDK 11.  With sdkman installed:
 
`sdk use java 11.0.3-zulu`