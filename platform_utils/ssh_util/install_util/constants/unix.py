class UnixConstants(object):
    download_dir = "~/Downloads"
    default_install_dir = "/Applications/Couchbase Server.app"

    # Non-root params
    nonroot_download_dir = download_dir
    nonroot_install_dir = default_install_dir

    # Install steps / commands
    cmds = {
        "uninstall":
            "osascript -e 'quit app \"Couchbase Server\"'; "
            "rm -rf " + default_install_dir + "; "
            "rm -rf ~/Library/Application Support/Couchbase; "
            "rm -rf ~/Library/Application Support/membase; "
            "rm -rf ~/Library/Python/couchbase-py; "
            "launchctl list | grep couchbase-server | xargs -n 3"
            " | cut -f3 -d' ' | xargs -n 1 launchctl stop; "
            "kill -9 `ps -ef |egrep Couchbase | xargs|cut -f1 -d' '`; "
            "umount /Volumes/Couchbase* > /dev/null && echo 1 || echo 0",
        "pre_install": "HDIUTIL_DETACH_ATTACH",
        "install":
            "rm -rf /Applications/Couchbase Server.app; "
            "launchctl list | grep couchbase-server | xargs -n 3"
            " | cut -f3 -d' ' | xargs -n 1 launchctl stop; "
            "kill -9 `ps -ef |egrep Couchbase | xargs|cut -f1 -d' '`; "
            "cp -R mountpoint/Couchbase Server.app"
            " /Applications/Couchbase Server.app; "
            "open /Applications/Couchbase Server.app"
            " > /dev/null && echo 1 || echo 0",
        "post_install": "launchctl list | grep couchbase-server"
                        " > /dev/null && echo 1 || echo 0",
        "post_install_retry": None,
    }

    non_root_cmds = {
        "uninstall":
            "osascript -e 'quit app \"Couchbase Server\"'; "
            "rm -rf " + default_install_dir + "; "
            "rm -rf ~/Library/Application Support/Couchbase; "
            "rm -rf ~/Library/Application Support/membase; "
            "rm -rf ~/Library/Python/couchbase-py; "
            "launchctl list | grep couchbase-server | xargs -n 3"
            " | cut -f3 -d' ' | xargs -n 1 launchctl stop; "
            "umount /Volumes/Couchbase* > /dev/null && echo 1 || echo 0",
        "pre_install": "HDIUTIL_DETACH_ATTACH",
        "install":
            "rm -rf /Applications/Couchbase Server.app; "
            "launchctl list | grep couchbase-server | xargs -n 3"
            " | cut -f3 -d' ' | xargs -n 1 launchctl stop; "
            "cp -R mountpoint/Couchbase Server.app"
            " /Applications/Couchbase Server.app; "
            "open /Applications/Couchbase Server.app"
            " > /dev/null && echo 1 || echo 0",
        "post_install": "launchctl list | grep couchbase-server"
                        " > /dev/null && echo 1 || echo 0",
        "post_install_retry": None,
    }
