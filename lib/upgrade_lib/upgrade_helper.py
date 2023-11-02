import re

from Cb_constants import CbServer
from builds.build_query import BuildQuery
from membase.api.rest_client import RestConnection
from scripts.old_install import InstallerJob
from upgrade_lib.couchbase import features
from remote.remote_util import RemoteMachineShellConnection
from testconstants import CB_REPO, CB_RELEASE_BUILDS, CB_VERSION_NAME


class CbServerUpgrade(object):
    def __init__(self, logger, product):
        self.log = logger
        self.product = product
        self.installer = InstallerJob()

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
        install_params = dict()
        install_params['num_nodes'] = len(nodes)
        install_params['product'] = "cb"
        install_params['version'] = version
        install_params['vbuckets'] = [vbuckets]
        install_params['init_nodes'] = False
        install_params['debug_logs'] = False
        install_params['type'] = build_type
        if cluster_profile == "provisioned" and float(version[:3]) >= 7.6:
            install_params["cluster_profile"] = "provisioned"

        self.installer.parallel_install(nodes, install_params)

    def offline(self, node_to_upgrade, version,
                rebalance_required=True, is_downgrade=False):
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
        if not rest.is_ns_server_running(timeout_in_seconds=120):
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
            else:
                self.log.critical(
                    "Cluster reported (/pools/default) balanced=false")
                return
