#!/bin/bash
set -e

staticConfigFile=/opt/couchbase/etc/couchbase/static_config
restPortValue=8091

# Function to override port values
function overridePort() {
    portName=$1
    portNameUpper=$(echo $portName | awk '{print toupper($0)}')
    portValue=${!portNameUpper}

    if [ -n "$portValue" ] && ! grep -Fq "{${portName}," ${staticConfigFile}; then
        echo "Overriding port '$portName' with value '$portValue'"
        echo "{$portName, $portValue}." >> ${staticConfigFile}

        [ "$portName" == "rest_port" ] && restPortValue=${portValue}
    fi
}

# # Start systemd as the container's init system
# echo "Starting systemd..."
# /sbin/init &

# # Wait until systemd is ready
# while ! pidof systemd >/dev/null; do
#     echo "Waiting for systemd to start..."
#     sleep 2
# done

# echo "Systemd started successfully."

# Override ports if needed
overridePort "rest_port"
overridePort "mccouch_port"
overridePort "memcached_port"
overridePort "query_port"
overridePort "ssl_query_port"
overridePort "fts_http_port"
overridePort "moxi_port"
overridePort "ssl_rest_port"
overridePort "ssl_capi_port"
overridePort "ssl_proxy_downstream_port"
overridePort "ssl_proxy_upstream_port"

# Start Couchbase using systemd
if [[ "$1" == "couchbase-server" ]]; then
    if [ "$(whoami)" = "couchbase" ]; then
        if [ ! -w /opt/couchbase/var ] || [ "$(stat -c '%U' /opt/couchbase/var)" != "couchbase" ]; then
            echo "/opt/couchbase/var is not owned and writable by UID 1000"
            echo "Aborting as Couchbase Server will likely not run"
            exit 1
        fi
    fi

    echo "Starting Couchbase Server -- Web UI available at http://<ip>:$restPortValue"
    echo "Logs available in /opt/couchbase/var/lib/couchbase/logs"

    systemctl daemon-reexec
    systemctl enable couchbase-server.service
    exec systemctl start couchbase-server.service
fi

# Run any other command passed to the container
exec "$@"
