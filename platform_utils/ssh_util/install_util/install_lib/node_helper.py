from threading import Thread
from time import sleep

import install_util.constants
import urllib.error
import urllib.parse
import urllib.request
from install_util.constants.build import BuildUrl, \
    LINUX_DISTROS, MACOS_VERSIONS, WINDOWS_SERVER
from install_util.platforms.linux import Linux
from install_util.platforms.unix import Unix
from install_util.platforms.windows import Windows
from shell_util.remote_connection import RemoteMachineShellConnection


class NodeInstallInfo(object):
    def __init__(self, server, server_info, os_type, version, edition, cluster_profile="default"):
        self.server = server
        self.server_info = server_info
        self.os_type = os_type

        self.version = version
        self.edition = edition
        self.cluster_profile = cluster_profile

        self.build_url = None
        self.debug_build_url = None
        self.non_root_package_mgr = None

        self.state = "not_started"


class InstallSteps(object):
    def __init__(self, logger, node_install_info):
        self.log = logger
        self.node_install_info = node_install_info
        self.result = True

    def sleep(self, timeout, msg=None):
        if msg:
            self.log.info(msg)
        sleep(timeout)

    @staticmethod
    def get_node_installer(node_install_info):
        t_class = None
        if node_install_info.os_type in LINUX_DISTROS:
            t_class = Linux
        elif node_install_info.os_type in MACOS_VERSIONS:
            t_class = Unix
        elif node_install_info.os_type in WINDOWS_SERVER:
            t_class = Windows
        return t_class(node_install_info.server)

    @staticmethod
    def get_download_dir(node_installer):
        if node_installer.shell.nonroot:
            return node_installer.nonroot_download_dir
        return node_installer.download_dir

    def __construct_build_url(self, is_debuginfo_build=False):
        file_name = None
        build_version = self.node_install_info.version.split("-")
        os_type = self.node_install_info.os_type
        node_info = RemoteMachineShellConnection.get_info_for_server(
            self.node_install_info.server)
        # Decide between release / regular build URL path
        if len(build_version) == 1:
            # Release build url
            url_path = "http://{}/{}/{}" \
                .format(BuildUrl.CB_DOWNLOAD_SERVER,
                        BuildUrl.CB_RELEASE_URL_PATH,
                        build_version[0])
        else:
            # Build_number specific url
            main_version = ".".join(build_version[0].split(".")[:2])
            # Reference: builds/latestbuilds/couchbase-server/trinity/1000
            url_path = "http://{}/{}/{}/{}" \
                .format(BuildUrl.CB_DOWNLOAD_SERVER,
                        BuildUrl.CB_LATESTBUILDS_URL_PATH,
                        BuildUrl.CB_VERSION_NAME[main_version],
                        build_version[1])

        build_version = "-".join(build_version)

        file_prefix = "{}-{}" \
            .format(BuildUrl.CB_BUILD_FILE_PREFIX,
                    self.node_install_info.edition)

        if os_type in install_util.constants.build.X86:
            # couchbase-server-enterprise-7.1.5-linux.x86_64.rpm
            # couchbase-server-enterprise-debuginfo-7.1.5-linux.x86_64.rpm
            if is_debuginfo_build:
                file_prefix = "{}-{}".format(file_prefix, "debuginfo")

            os_type = "linux"
            if float(build_version[:3]) < 7.1:
                os_type = self.node_install_info.os_type
            file_name = "{}-{}-{}.{}.{}" \
                .format(file_prefix,
                        build_version,
                        os_type,
                        node_info.architecture_type,
                        node_info.deliverable_type)
        elif os_type in install_util.constants.build.LINUX_AMD64:
            # TODO: Check install_utils.py L1127 redundant code presence
            # couchbase-server-enterprise_7.1.5-linux_amd64.deb
            # couchbase-server-enterprise-dbg_7.1.5-linux_amd64.deb
            if is_debuginfo_build:
                file_prefix = "{}-{}".format(file_prefix, "dbg")

            os_type = "linux"
            if float(build_version[:3]) < 7.1:
                os_type = self.node_install_info.os_type
            file_name = "{}_{}-{}_{}.{}" \
                .format(file_prefix,
                        build_version,
                        os_type,
                        "amd64",
                        node_info.deliverable_type)
        elif os_type in install_util.constants.build.WINDOWS_SERVER:
            # couchbase-server-enterprise_6.5.0-4557-windows_amd64.msi
            if "windows" in self.node_install_info.os_type:
                self.node_install_info.deliverable_type = "msi"
            file_name = "{}_{}-{}_{}.{}" \
                .format(file_prefix,
                        build_version,
                        self.node_install_info.os_type,
                        "amd64",
                        node_info.deliverable_type)
        elif os_type in install_util.constants.build.MACOS_VERSIONS:
            # couchbase-server-enterprise_6.5.0-4557-macos_x86_64.dmg
            file_name = "{}_{}-{}_{}-{}.{}" \
                .format(file_prefix,
                        build_version,
                        "macos",
                        node_info.architecture_type,
                        "unnotarized",
                        node_info.deliverable_type)
        else:
            self.result = False
            self.log.critical("Unsupported os_type '{}' for build_url"
                              .format(self.node_install_info.os_type))
        return "{}/{}".format(url_path, file_name)

    def check_url_status(self, url, num_retries=5, timeout=10):
        is_url_okay = False
        self.log.debug("Checking URL status: {0}".format(url))
        if "amazonaws" in url:
            import ssl
            """ no verify cacert when download build from S3 """
            ssl._create_default_https_context = ssl._create_unverified_context
        while num_retries > 0:
            try:
                status = urllib.request.urlopen(url).getcode()
                if status == 200:
                    is_url_okay = True
                    break
            except Exception as e:
                self.sleep(timeout,
                           "Waiting for {0} secs to reach url once again. "
                           "Build server might be too busy.  Error msg: {1}"
                           .format(timeout, str(e)))
                num_retries = num_retries - 1

        if not is_url_okay:
            self.log.critical("{} - URL '{}' failed to connect"
                              .format(self.node_install_info.server.ip, url))
        return is_url_okay

    def populate_build_url(self):
        self.node_install_info.build_url = self.__construct_build_url()
        self.log.info("{} - Build url :: {}"
                      .format(self.node_install_info.server.ip,
                              self.node_install_info.build_url))

    def populate_debug_build_url(self):
        self.node_install_info.debug_build_url = self.__construct_build_url(
            is_debuginfo_build=True)
        self.log.info("{} - Debug build url :: {}"
                      .format(self.node_install_info.server.ip,
                              self.node_install_info.debug_build_url))

    def check_build_url_status(self):
        self.check_url_status(self.node_install_info.build_url)

    def download_build_locally(self, build_url):
        f_path = "{}/{}".format(".", build_url.split('/')[-1])
        f, r = urllib.request.urlretrieve(build_url, f_path)
        return f, r

    def copy_build_to_server(self, node_installer, build_url):
        f_path = "{}/{}".format(".", build_url.split('/')[-1])
        result = node_installer.shell.copy_file_local_to_remote(
            f_path, node_installer.shell.download_dir)
        return result

    def download_build(self, node_installer, build_url,
                       non_root_installer=False):
        download_dir = self.get_download_dir(node_installer)
        f_name = build_url.split("/")[-1]
        # Remove old build (if exists)
        cmd = "rm -f {}/couchbase-server*".format(download_dir)
        node_installer.shell.execute_command(cmd)
        # Download the build
        cmd = node_installer.wget_cmd.format(download_dir, build_url)
        node_installer.shell.execute_command(cmd)
        if non_root_installer:
            node_installer.shell.execute_cmd("chmod a+x {}/{}"
                                             .format(download_dir, f_name))


class NodeInstaller(Thread):
    def __init__(self, logger, node_install_info, steps):
        super(NodeInstaller, self).__init__()
        self.log = logger
        self.steps = steps
        self.node_install_info = node_install_info
        self.result = False

    def run(self):
        installer = InstallSteps(self.log, self.node_install_info)
        node_installer = installer.get_node_installer(
            self.node_install_info)
        for step in self.steps:
            self.log.info("{} - Running '{}'"
                          .format(self.node_install_info.server.ip, step))
            if step == "populate_build_url":
                # To download the main build url
                self.node_install_info.state = "construct_build_url"
                installer.populate_build_url()
            elif step == "populate_debug_build_url":
                # To download the debug_info build url for backtraces
                self.node_install_info.state = "construct_debug_build_url"
                installer.populate_debug_build_url()
            elif step == "check_url_status":
                self.node_install_info.state = "checking_url_status"
                installer.check_url_status(self.node_install_info.build_url)
                if self.node_install_info.debug_build_url:
                    installer.check_url_status(
                        self.node_install_info.debug_build_url)
            elif step == "local_download_build":
                self.node_install_info.state = "downloading_build_on_executor"
                build_urls = [self.node_install_info.build_url]
                if self.node_install_info.debug_build_url:
                    build_urls.append(self.node_install_info.debug_build_url)

                for build_url in build_urls:
                    f_name, res = installer.download_build_locally(build_url)
                    self.log.debug("File saved as '{}'".format(f_name))
                    self.log.debug("File size: {}".format(res["Content-Length"]))
                    self.log.debug("File create date: {}".format(res["Date"]))
            elif step == "copy_local_build_to_server":
                self.node_install_info.state = "copying_build_to_remote_server"
                build_urls = [self.node_install_info.build_url]
                if self.node_install_info.debug_build_url:
                    build_urls.append(self.node_install_info.build_url)
                for build_url in build_urls:
                    installer.result = installer.result and \
                        installer.copy_build_to_server(node_installer,
                                                       build_url)
            elif step == "download_build":
                self.node_install_info.state = "downloading_build"
                installer.download_build(node_installer,
                                         self.node_install_info.build_url)
                if self.node_install_info.debug_build_url:
                    installer.download_build(node_installer,
                                             self.node_install_info.build_url)
            elif step == "uninstall":
                self.node_install_info.state = "uninstalling"
                node_installer.uninstall()
            elif step == "deep_cleanup":
                self.node_install_info.state = "deep_cleaning"
            elif step == "pre_install":
                self.node_install_info.state = "pre_install_procedure"
                node_installer.pre_install(self.node_install_info.cluster_profile)
            elif step == "install":
                self.node_install_info.state = "installing"
                node_installer.install(self.node_install_info.build_url)
                node_installer.post_install()
            elif step == "init_cluster":
                self.node_install_info.state = "init_cluster"
                node_installer.init_cluster(self.node_install_info.server)
            elif step == "post_install":
                self.node_install_info.state = "post_install_procedure"
            elif step == "post_install_cleanup":
                self.node_install_info.state = "post_install_cleanup"
            else:
                self.log.critical("Invalid step '{}'".format(step))
                installer.result = False

            if installer.result is False:
                break

        node_installer.shell.disconnect()
        self.result = installer.result
