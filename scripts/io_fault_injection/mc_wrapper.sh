cat >/opt/couchbase/bin/memcached <<'EOF'
#!/bin/bash

export LD_PRELOAD=/opt/couchbase/lib/libfaultinject.so

# Do NOT put FI_* vars here – runtime config is file-based

exec /opt/couchbase/bin/memcached.real "$@"
EOF

chown couchbase:couchbase /opt/couchbase/bin/memcached
chmod 755 /opt/couchbase/bin/memcached