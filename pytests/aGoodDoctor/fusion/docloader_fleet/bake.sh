#!/bin/bash
# Provision a DocLoader fleet image: JDK 17, DocLoader built from a pinned
# commit, and a systemd template unit docloader@<port> (3 JVMs by default).
#
# Re-bake when the DocLoader submodule moves:
#   1. Launch an Ubuntu 24.04 arm64 c8g instance (SSH from your IP only)
#   2. sudo bash bake.sh $(git -C DocLoader rev-parse HEAD)
#   3. Smoke-load through it, then scrub before imaging: stop the units,
#      rm -rf /var/lib/docloader/*, cloud-init clean --logs --seed,
#      truncate /etc/machine-id, remove ~/.ssh/authorized_keys, shut down
#   4. create-image from the stopped instance and add a new version of the
#      'taf-docloader-fleet' launch template with the new AMI as default
set -euxo pipefail
DOCLOADER_COMMIT=${1:?commit}
export DEBIAN_FRONTEND=noninteractive

apt-get update -y
apt-get install -y openjdk-17-jdk-headless maven git curl jq

useradd --system --home-dir /opt/docloader --shell /usr/sbin/nologin docloader || true
mkdir -p /opt/docloader /etc/docloader
git clone https://github.com/couchbaselabs/DocLoader.git /opt/docloader/src
git -C /opt/docloader/src checkout "$DOCLOADER_COMMIT"
(cd /opt/docloader/src && mvn -q -B -DskipTests package)
cp -r /opt/docloader/src/target/magmadocloader /opt/docloader/magmadocloader
echo "$DOCLOADER_COMMIT" > /opt/docloader/VERSION
rm -rf /root/.m2 /opt/docloader/src/target
chown -R docloader:docloader /opt/docloader

# Overridden per fleet run (user-data rewrites SERVER_NAME and restarts the
# units) so /check-online identifies which run a JVM belongs to.
cat > /etc/docloader/env <<'EOF'
SERVER_NAME=taf_docloader
HEAP_PCT=25
EOF

cat > /etc/systemd/system/docloader@.service <<'EOF'
[Unit]
Description=TAF DocLoader REST server on port %i
After=network-online.target
Wants=network-online.target
[Service]
User=docloader
EnvironmentFile=/etc/docloader/env
StateDirectory=docloader/%i
WorkingDirectory=/var/lib/docloader/%i
ExecStart=/usr/bin/java -XX:MaxRAMPercentage=${HEAP_PCT} -cp /opt/docloader/magmadocloader/magmadocloader.jar RestServer.RestApplication --server.port=%i --server.name=${SERVER_NAME}
LimitNOFILE=1048576
Restart=on-failure
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF

cat > /etc/sysctl.d/90-docloader.conf <<'EOF'
net.ipv4.ip_local_port_range = 1024 65000
net.core.somaxconn = 4096
EOF
sysctl --system >/dev/null

systemctl daemon-reload
for port in 8080 8081 8082; do systemctl enable --now docloader@$port; done
