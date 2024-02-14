class WindowsConstants(object):
    download_dir = "/cygdrive/c/tmp/"
    default_install_dir = "/cygdrive/c/Program Files/Couchbase/Server"

    # Non-root params
    nonroot_download_dir = download_dir
    nonroot_install_dir = default_install_dir

    # Install steps / commands
    cmds = {
        "uninstall":
            "cd " + download_dir + "; msiexec /x installed-msi /passive",
        "pre_install": None,
        "install":
            "cd " + download_dir + "; "
            "msiexec /i buildbinary /passive /L*V install_status.txt",
        "post_install":
            "cd " + download_dir + "; " +
            "vi +\"set nobomb | set fenc=ascii | x\" install_status.txt; " +
            "grep 'buildversion.*[Configuration|Installation]"
            " completed successfully.' install_status.txt && echo 1 || echo 0",
        "post_install_retry":
            "cd " + download_dir + "; " +
            "msiexec /i buildbinary /passive /L*V install_status.txt",
    }
