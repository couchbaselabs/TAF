import re

from builds.build_query import BuildQuery
from cb_constants import CbServer
from global_vars import cluster_util
from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection
from ssh_util.install_util.constants.build import BuildUrl
from upgrade_lib.couchbase import features
from testconstants import CB_REPO, CB_RELEASE_BUILDS, CB_VERSION_NAME
import global_vars
from ssh_util.install_util.install_lib.node_helper import NodeInstaller, NodeInstallInfo
from ssh_util.install_util.install_lib.helper import InstallHelper
from ssh_util.install_util.install import start_and_wait_for_threads, print_install_status


class CbServerUpgrade(object):
    def __init__(self, logger, product):
        self.log = logger
        self.product = product
        self.helper = InstallHelper(logger)

        self.install_tasks = ["populate_build_url", "check_url_status",
                              "download_build", "uninstall",
                              "pre_install", "install"]

    @staticmethod
    def get_supported_features(cluster_version):
        cluster_version = float(".".join((cluster_version.split('.')[:2])))
        supported_features = list()
        [supported_features.extend(t_features)
         for key, t_features in features.items()
         if float(key) <= cluster_version]
        return supported_features

    def get_build(self, version, remote, wait_timeout=120):
        info = remote.extract_remote_info()
        build_repo = CB_REPO
        if version[:5] in CB_RELEASE_BUILDS.keys():
            if version[:3] in CB_VERSION_NAME:
                build_repo = CB_REPO + CB_VERSION_NAME[version[:3]] + "/"
        builds, changes = BuildQuery().get_all_builds(
            version=version,
            timeout=wait_timeout * 5,
            deliverable_type=info.deliverable_type,
            architecture_type=info.architecture_type,
            edition_type="couchbase-server-enterprise",
            repo=build_repo,
            distribution_version=info.distribution_version.lower())

        if re.match(r'[1-9].[0-9].[0-9]-[0-9]+$', version):
            version = version + "-rel"
        appropriate_build = BuildQuery(). \
            find_build(builds,
                       '%s-enterprise' % self.product,
                       info.deliverable_type,
                       info.architecture_type,
                       version.strip())

        if appropriate_build is None:
            self.log.info("Builds are: %s \n. Remote is %s, %s. Result is: %s"
                          % (builds, remote.ip, remote.username, version))
            raise Exception("Build %s not found" % version)
        return appropriate_build

    def install_version_on_nodes(self, nodes, version,
                                 vbuckets=CbServer.total_vbuckets,
                                 build_type="enterprise",
                                 cluster_profile=None):
        """
        Installs required Couchbase-server version on the target nodes.

        :param nodes: List of nodes to install the cb 'version'
        :param version: Version to install on target 'nodes'
        :param vbuckets: Number of vbuckets with which the server comes up
        :param build_type: Build type. enterprise / community
        :param cluster_profile: serverless / dedicated
        :return:
        """
        self.log.info("Installing {} on nodes {}"
                      .format(version, [node.ip for node in nodes]))

        edition = "community" if build_type == "community" else "enterprise"
        cluster_profile = cluster_profile or "default"

        for server in nodes:
            server.install_status = "not_started"

        self.log.info("Node health check")
        if not self.helper.check_server_state(nodes):
            raise Exception("Node health check failed")

        try:
            self.helper.populate_cb_server_versions()
        except Exception as e:
            self.log.warning("Error while reading couchbase version: {}".format(e))
        if version[:3] not in BuildUrl.CB_VERSION_NAME.keys():
            self.log.critical("Version '{}' not yet supported".format(version[:3]))
            raise Exception("Version '{}' not yet supported".format(version[:3]))

        node_helpers = list()
        for node in nodes:
            server_info = RemoteMachineShellConnection.get_info_for_server(node)
            node_helpers.append(NodeInstallInfo(
                                node, server_info,
                                self.helper.get_os(server_info),
                                version, edition,
                                cluster_profile))

        self.log.info("Starting install tasks")
        install_server_threads = \
                [NodeInstaller(self.log, node_helper, self.install_tasks)
                                for node_helper in node_helpers]
        okay = start_and_wait_for_threads(install_server_threads, 300)
        if not okay:
            self.log.error("Install tasks failed")
        else:
            self.log.info(f"Install tasks: {self.install_tasks} completed")

        print_install_status(install_server_threads, self.log)

    def offline(self, node_to_upgrade, version,
                rebalance_required=True, is_downgrade=False,
                validate_bucket_ranking=True):
        rest = RestConnection(node_to_upgrade)
        shell = RemoteMachineShellConnection(node_to_upgrade)
        appropriate_build = self.get_build(version, shell)
        if appropriate_build.url is False:
            self.log.critical("Unable to find build %s" % version)
            return False

        if shell.download_build(appropriate_build) is False:
            self.log.critical("Failed while downloading the build!")

        self.log.info("Starting node upgrade")
        upgrade_success = shell.couchbase_upgrade(
            appropriate_build, save_upgrade_config=False,
            forcefully=is_downgrade)
        shell.disconnect()
        if not upgrade_success:
            self.log.critical("Upgrade failed")
            return False

        self.log.info("Wait for ns_server to accept connections")
        if not cluster_util.is_ns_server_running(node_to_upgrade,
                                                 timeout_in_seconds=120):
            self.log.critical("Server not started post upgrade")
            return False

        self.log.info("Validate the cluster rebalance status")
        if not rest.cluster_status()["balanced"]:
            if rebalance_required:
                otp_nodes = [node.id for node in rest.node_statuses()]
                rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
                rebalance_passed = rest.monitorRebalance()
                if not rebalance_passed:
                    self.log.critical(
                        "Rebalance failed post node upgrade of {0}"
                        .format(node_to_upgrade))
                    return
                if validate_bucket_ranking:
                    # Validating bucket ranking post rebalance
                    validate_ranking_res = global_vars.cluster_util.validate_bucket_ranking(None, node_to_upgrade)
                    if not validate_ranking_res:
                        self.log.error("The vbucket movement was not according to bucket ranking")
                        return validate_ranking_res
            else:
                self.log.critical(
                    "Cluster reported (/pools/default) balanced=false")
                return

    def new_install_version_on_all_nodes(self, nodes, version,
                                         edition="enterprise",
                                         cluster_profile="default",
                                         install_tasks=None):

        self.log.info("Installing using ssh_util install method")

        if install_tasks is None:
            install_tasks = self.install_tasks

        for server in nodes:
            server.install_status = "not_started"

        self.log.info("Node health check")
        if not self.helper.check_server_state(nodes):
            return 1

        # Populate valid couchbase version and validate the input version
        try:
            self.helper.populate_cb_server_versions()
        except Exception as e:
            self.log.warning("Error while reading couchbase version: {}".format(e))
        if version[:3] not in BuildUrl.CB_VERSION_NAME.keys():
            self.log.critical("Version '{}' not yet supported".format(version[:3]))
            return 1

        node_helpers = list()
        for node in nodes:
            server_info = RemoteMachineShellConnection.get_info_for_server(node)
            node_helpers.append(NodeInstallInfo(
                                node, server_info,
                                self.helper.get_os(server_info),
                                version, edition,
                                cluster_profile))

        self.log.info("Starting install tasks")
        install_server_threads = \
                [NodeInstaller(self.log, node_helper, install_tasks)
                                for node_helper in node_helpers]
        okay = start_and_wait_for_threads(install_server_threads, 300)
        if not okay:
            self.log.info("Install tasks failed")
        else:
            self.log.info(f"Install tasks: {self.install_tasks} completed")

        print_install_status(install_server_threads, self.log)
