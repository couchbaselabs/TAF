# ssh_util

## Usage
```
$ python3 -m install_util.install --help
usage: install.py [-h] [--install_tasks INSTALL_TASKS] -i INI -v VERSION [--edition {enterprise,community}] [--url URL] [--storage_mode STORAGE_MODE] [--enable_ipv6]
                  [--install_debug_info] [--skip_local_download] [--timeout TIMEOUT] [--build_download_timeout BUILD_DOWNLOAD_TIMEOUT] [--params PARAMS]
                  [--log_level {info,debug,error,critical}]

Installer for Couchbase-Server

options:
  -h, --help            show this help message and exit
  --install_tasks INSTALL_TASKS
                        List of tasks to run '-' separated
  -i INI, --ini INI     Ini file path
  -v VERSION, --version VERSION
                        Build version to be installed
  --edition {enterprise,community}
                        CB edition
  --url URL             Specific URL to use for build download
  --storage_mode STORAGE_MODE
                        Sets indexer storage mode
  --enable_ipv6         Enable ipv6 mode in ns_server
  --install_debug_info  Flag to install debug package for debugging
  --skip_local_download
                        Download build individually on each node
  --timeout TIMEOUT     End install after timeout seconds
  --build_download_timeout BUILD_DOWNLOAD_TIMEOUT
                        Timeout for build download. Usefull during slower download envs
  --params PARAMS, -p PARAMS
                        Other install params
  --log_level {info,debug,error,critical}
                        Logging level
```
