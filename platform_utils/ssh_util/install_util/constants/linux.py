class LinuxConstants(object):
    download_dir = "/tmp"
    default_install_dir = "/opt/couchbase"

    # Non-root params
    nonroot_download_dir = "/home/nonroot"
    nonroot_install_dir = "%s/cb/opt/couchbase" % nonroot_download_dir

    local_download_dir = "/tmp"
    wget_cmd = "cd {} ; wget -Nq {}"

    unmount_nfs_cmd = "umount -a -t nfs,nfs4 -f -l ; "

    # Install steps / commands (Supports .rpm and .deb)
    cmds = {
        "deb": {
            "uninstall":
                "systemctl -q stop couchbase-server;" +
                unmount_nfs_cmd +
                "service ntp restart ; "
                "apt-get purge -y 'couchbase*' > /dev/null; sleep 10;"
                "dpkg --purge $(dpkg -l | grep couchbase | awk '{print $2}'"
                " | xargs echo); sleep 10; "
                "rm /var/lib/dpkg/info/couchbase-server.*; sleep 10;"
                "kill -9 `ps -ef |egrep couchbase|cut -f3 -d' '`;" +
                "rm -rf " + default_install_dir +
                " > /dev/null && echo 1 || echo 0; "
                "dpkg -P couchbase-server; "
                "rm -rf /var/lib/dpkg/info/couchbase-server.*;"
                "dpkg --configure -a; apt-get update; "

                "grep 'kernel.dmesg_restrict=0' /etc/sysctl.conf || "
                "(echo 'kernel.dmesg_restrict=0' >> /etc/sysctl.conf "
                "&& service procps restart) ; "

                "rm -rf" + default_install_dir + " ; "
                " rm -f /etc/couchbase.d/config_profile",
            "pre_install": "mkdir -p /etc/couchbase.d ; "
                           "echo {} > /etc/couchbase.d/config_profile ; "
                           "chmod ugo+r /etc/couchbase.d/",
            "install": "apt-get -y -f install buildpath"
                       " > /dev/null && echo 1 || echo 0",
            "post_install":
                "usermod -aG adm couchbase"
                " && systemctl -q is-active couchbase-server.service"
                " && echo 1 || echo 0",
            "post_install_retry": "systemctl restart couchbase-server.service",
        },
        "rpm": {
            "uninstall": "{} yes | yum remove 'couchbase*' > /dev/null; "
                "rm -rf {} {} > /dev/null && echo 1 || echo 0"
                .format(unmount_nfs_cmd, default_install_dir,
                        nonroot_install_dir) + " ; "
                " rm -f /etc/couchbase.d/config_profile",
            "pre_install": "mkdir -p /etc/couchbase.d ; "
                           "echo {} > /etc/couchbase.d/config_profile ; "
                           "chmod ugo+r /etc/couchbase.d/",
            "install": "yes | yum localinstall -y buildpath"
                       " > /dev/null && echo 1 || echo 0",
            "set_vm_swappiness_and_thp":
                "/sbin/sysctl vm.swappiness=0; " +
                "echo never > /sys/kernel/mm/transparent_hugepage/enabled; " +
                "echo never > /sys/kernel/mm/transparent_hugepage/defrag; ",
            "suse_install":
                "registercloudguest --force-new ;"
                " zypper --no-gpg-checks in -y buildpath"
                " > /dev/null && echo 1 || echo 0",
            "suse_uninstall":
                unmount_nfs_cmd +
                "zypper --ignore-unknown rm -y 'couchbase*' > /dev/null; " +
                "rm -rf /var/cache/zypper/RPMS/couchbase* {} {}"
                " > /dev/null && echo 1 || echo 0"
                .format(default_install_dir, nonroot_install_dir),
            "post_install": "systemctl -q is-active couchbase-server"
                            " && echo 1 || echo 0",
            "post_install_retry": "systemctl daemon-reexec ; "
                                  "systemctl restart couchbase-server",
        }
    }

    non_root_cmds = {
        "deb": {
            "uninstall":
                unmount_nfs_cmd +
                "dpkg --purge $(dpkg -l | grep couchbase | awk '{print $2}'"
                " | xargs echo);"
                " kill -9 `ps -ef |egrep couchbase|cut -f3 -d' '`; " +
                "rm /var/lib/dpkg/info/couchbase-server.* {} {} {}cb"
                " > /dev/null && echo 1 || echo 0"
                .format(default_install_dir, nonroot_install_dir,
                        nonroot_download_dir),
            "pre_install": "",
            "install":
                "mkdir " + nonroot_download_dir + "cb;"
                "cd " +
                nonroot_download_dir + "; "
                "./{} --install --package buildpath --install-location " +
                nonroot_download_dir + "cb/",
            "post_install":
                "cd " + nonroot_download_dir + "cb/opt/couchbase/; "
                "./bin/couchbase-server --start",
            "post_install_retry": "./bin/couchbase-server --start",
        },
        "rpm": {
            "pre_install":
                "ls -l " + nonroot_install_dir + "bin/",
            "uninstall":
                nonroot_download_dir +
                "cb/opt/couchbase/bin/couchbase-server --stop; " +
                unmount_nfs_cmd +
                nonroot_install_dir + "bin/couchbase-server -k ;"
                " kill -9 `ps -ef |egrep couchbase|cut -f3 -d' '`; " +
                "rm -rf " + nonroot_install_dir +
                " > /dev/null && echo 1 || echo 0; " +
                "rm -rf " + nonroot_download_dir + "cb ",
            "install":
                # cb-non-package-installer requires empty dir to extract files
                "mkdir " + nonroot_download_dir + "cb;"
                "cd " + nonroot_download_dir + "; "
                "./cb-non-package-installer --install --package buildpath"
                " --install-location " + nonroot_download_dir + "cb/",
            "suse_install":
                "mkdir " + nonroot_download_dir + "cb;"
                "cd " + nonroot_download_dir + "; "
                "./{} --install --package buildpath --install-location " +
                nonroot_download_dir + "cb/",
            "post_install": nonroot_download_dir
                + "cb/opt/couchbase/bin/couchbase-server --start",
            "post_install_retry": None,
        }
    }
