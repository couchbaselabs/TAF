# TAF

## Python Setup

### On Linux

```bash
# Adding a submodule to the Git repository
git submodule init
git submodule update --init --force --remote

# Setup Python 3.10 using Pyenv
py_version="3.10.14"
git clone https://github.com/pyenv/pyenv.git $HOME/.pyenv
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
pyenv install $py_version

# Installing dependency packages
python -m pip install -r requirements.txt
```

### On Mac

```bash
# Installs python 3.10
brew install python@3.10

# Installing dependency packages
python -m pip install -r requirements.txt

# Adding a submodule to the Git repository
git submodule init
git submodule update --init --force --remote
```

## Golang setup (For Sirius loader)

```bash
# Setup Golang in the local dir itself
go_version=1.22.4
wget https://golang.org/dl/go${go_version}.linux-amd64.tar.gz --quiet
tar -xzf go${go_version}.linux-amd64.tar.gz
rm -f go${go_version}.linux-amd64.tar.gz
export GOPATH=`pwd`/go
export PATH="${GOPATH}/bin:${PATH}"
export GO111MODULE=on
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

```bash
# Activate required Python version
pyenv local 3.10.14
# Setup required Golang Paths 
# (This might change as per your Golang install path)
# Below is considered that the path is same as the repo
export GOPATH=`pwd`/go
export PATH="${GOPATH}/bin:${PATH}"
export GO111MODULE=on

# To run fill suite from .conf file
python testrunner.py -i <ini_file> -c <file_with_tests.conf> -p <params_to_all_test=val1,...>

# To run individual test 
python testrunner.py -i <ini_file> -t <modA.modB.test_function,param1=v1,params2=v2,...> -p <params_to_all_test=val1,...>

Examples:
   # To run tests from .conf file
   python testrunner.py -i nodes.ini -c test_suite.conf -p <params_to_all_test=val1,...>
   
   # To run individual test
   python testrunner.py -i nodes.ini -t epengine.basic_ops.basic_ops.test_doc_size,nodes_init=1 -p durability=MAJORITY,get-cbcollect-info=True
```

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
