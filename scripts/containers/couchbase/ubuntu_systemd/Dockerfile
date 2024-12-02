FROM ubuntu:24.04

LABEL maintainer="docker@couchbase.com"

ARG UPDATE_COMMAND="apt-get update -y -q"
ARG CLEANUP_COMMAND="rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*"
ARG BUILD_NO=2440
ARG FLAVOR=morpheus
ARG VERSION=8.0.0
ARG BASE_URL=http://latestbuilds.service.couchbase.com/builds/latestbuilds/couchbase-server/$FLAVOR/$BUILD_NO

# Install dependencies including systemd and dbus
RUN set -x \
    && ${UPDATE_COMMAND} \
    && apt-get install -y -q wget tzdata systemd systemd-sysv dbus \
      lsof lshw sysstat net-tools numactl bzip2 \
    && ${CLEANUP_COMMAND}

# Enable systemd
ENV container docker
STOPSIGNAL SIGRTMIN+3

# Set up directories for systemd
RUN mkdir -p /etc/systemd/system/sockets.target.wants && \
    mkdir -p /etc/systemd/system/dbus.service.d && \
    systemctl mask dev-mqueue.mount \
                    sys-kernel-debug.mount \
                    systemd-remount-fs.service \
                    systemd-logind.service \
                    getty.target \
                    console-getty.service \
                    systemd-udevd.service \
                    systemd-udevd-control.socket \
                    systemd-udevd-kernel.socket

# Ensure dbus starts properly
RUN ln -sf /lib/systemd/system/dbus.service /etc/systemd/system/multi-user.target.wants/dbus.service

ARG CB_RELEASE_URL=http://latestbuilds.service.couchbase.com/builds/latestbuilds/couchbase-server/$FLAVOR/$BUILD_NO
ARG CB_PACKAGE=couchbase-server-enterprise_${VERSION}-${BUILD_NO}-linux_amd64.deb
ARG CB_SKIP_CHECKSUM=true
ENV PATH=$PATH:/opt/couchbase/bin:/opt/couchbase/bin/tools:/opt/couchbase/bin/install

# Create Couchbase user
RUN groupadd -g 1010 couchbase && useradd couchbase -u 1010 -g couchbase -M

# Install Couchbase
RUN set -x \
    && export INSTALL_DONT_START_SERVER=1 \
    && dpkgArch="$(dpkg --print-architecture)" \
    && case "${dpkgArch}" in \
         'arm64') \
           CB_SHA256=f803901b4d4793276df46482ca4a39a7447491b1b266bbee4560e4c8dbd0889c \
           ;; \
         'amd64') \
           CB_SHA256=9401d5ed083af9d6b086df5450df5cd5b83d9a26e37cadc746f4153e2583d140 \
           ;; \
       esac \
    && CB_PACKAGE=$(echo ${CB_PACKAGE} | sed -e "s/@@ARCH@@/${dpkgArch}/") \
    && wget -N --no-verbose $CB_RELEASE_URL/$CB_PACKAGE \
    && { ${CB_SKIP_CHECKSUM} || echo "$CB_SHA256  $CB_PACKAGE" | sha256sum -c - ; } \
    && ${UPDATE_COMMAND} \
    && apt-get install -y ./$CB_PACKAGE \
    && rm -f ./$CB_PACKAGE \
    && ${CLEANUP_COMMAND} \
    && rm -rf /tmp/* /var/tmp/*

RUN mkdir -p /etc/couchbase.d && \
    echo "provisioned" > /etc/couchbase.d/config_profile && \
    chmod ugo+r /etc/couchbase.d/config_profile && \
    chmod 777 /etc/couchbase.d/config_profile

# Prepare systemd service for Couchbase
COPY scripts/run /etc/service/couchbase-server/run
# RUN systemctl enable couchbase-server

# Add dummy script for commands invoked by cbcollect_info that
# make no sense in a Docker container
COPY scripts/dummy.sh /usr/local/bin/
RUN set -x \
    && ln -s dummy.sh /usr/local/bin/iptables-save \
    && ln -s dummy.sh /usr/local/bin/lvdisplay \
    && ln -s dummy.sh /usr/local/bin/vgdisplay \
    && ln -s dummy.sh /usr/local/bin/pvdisplay

# Fix curl RPATH if necessary - if curl.real exists, it's a new
# enough package that we don't need to do anything. If not, it
# may be OK, but just fix it
RUN set -ex \
    &&  if [ ! -e /opt/couchbase/bin/curl.real ]; then \
            ${UPDATE_COMMAND}; \
            apt-get install -y chrpath; \
            chrpath -r '$ORIGIN/../lib' /opt/couchbase/bin/curl; \
            apt-get remove -y chrpath; \
            apt-get autoremove -y; \
            ${CLEANUP_COMMAND}; \
        fi

# Add bootstrap script
COPY scripts/entrypoint.sh /
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/sbin/init"]

# 8091: Cluster administration REST/HTTP traffic, including Couchbase Web Console
# 8092: Views and XDCR access
# 8093: Query service REST/HTTP traffic
# 8094: Search Service REST/HTTP traffic
# 8095: Analytics service REST/HTTP traffic
# 8096: Eventing service REST/HTTP traffic
# 8097: Backup service REST/HTTP traffic
# 9123: Analytics prometheus
# 11207: Data Service (SSL)
# 11210: Data Service
# 11280: Data Service prometheus
# 18091: Cluster administration REST/HTTP traffic, including Couchbase Web Console (SSL)
# 18092: Views and XDCR access (SSL)
# 18093: Query service REST/HTTP traffic (SSL)
# 18094: Search Service REST/HTTP traffic (SSL)
# 18095: Analytics service REST/HTTP traffic (SSL)
# 18096: Eventing service REST/HTTP traffic (SSL)
# 18097: Backup service REST/HTTP traffic (SSL)
EXPOSE 8091 \
       8092 \
       8093 \
       8094 \
       8095 \
       8096 \
       8097 \
       9100 \
       9101 \
       9102 \
       9103 \
       9104 \
       9105 \
       9123 \
       11207 \
       11210 \
       11280 \
       18091 \
       18092 \
       18093 \
       18094 \
       18095 \
       18096 \
       18097 \
       19100 \
       19101 \
       19102 \
       19103 \
       19104 \
       19105


# Bootstrap systemd
VOLUME ["/sys/fs/cgroup", "/opt/couchbase/var"]

