from remote.remote_util import RemoteMachineShellConnection
import re
from global_vars import logger
from scripts.old_install import InstallerJob
from builds.build_query import BuildQuery
from testconstants import CB_REPO, COUCHBASE_VERSIONS, CB_VERSION_NAME, \
    CB_RELEASE_BUILDS, COUCHBASE_MP_VERSION, MV_LATESTBUILD_REPO

class InstallUtils:
    def __init__(self):
        self.test_log = logger.get("test")
        self.installer_job = InstallerJob()
        self.product = "couchbase-server"
        self.is_downgrade = False

    def install_version_on_nodes(self, cluster, nodes, version):
        install_params = dict()
        install_params['num_nodes'] = len(nodes)
        install_params['product'] = "cb"
        install_params['version'] = version
        install_params['vbuckets'] = [cluster.vbuckets]
        install_params['init_nodes'] = False
        install_params['debug_logs'] = False
        self.installer_job.parallel_install(nodes, install_params)

    def __get_build(self, version, remote, is_amazon=False, info=None):
        self.wait_timeout = 120
        if info is None:
            info = remote.extract_remote_info()
        build_repo = CB_REPO
        if version[:5] in COUCHBASE_VERSIONS:
            if version[:3] in CB_VERSION_NAME:
                build_repo = CB_REPO + CB_VERSION_NAME[version[:3]] + "/"
            elif version[:5] in COUCHBASE_MP_VERSION:
                build_repo = MV_LATESTBUILD_REPO
        builds, changes = BuildQuery().get_all_builds(
            version=version,
            timeout=self.wait_timeout * 5,
            deliverable_type=info.deliverable_type,
            architecture_type=info.architecture_type,
            edition_type="couchbase-server-enterprise",
            repo=build_repo,
            distribution_version=info.distribution_version.lower())

        if re.match(r'[1-9].[0-9].[0-9]-[0-9]+$', version):
            version = version + "-rel"
        if version[:5] in CB_RELEASE_BUILDS and \
            version[6:].replace("-rel", "") == CB_RELEASE_BUILDS[version[:5]]:
            appropriate_build = BuildQuery(). \
                find_couchbase_release_build(
                '%s-enterprise' % self.product,
                info.deliverable_type,
                info.architecture_type,
                version.strip(),
                is_amazon=is_amazon,
                os_version=info.distribution_version)
        else:
            appropriate_build = BuildQuery(). \
                find_build(builds,
                           '%s-enterprise' % self.product,
                           info.deliverable_type,
                           info.architecture_type,
                           version.strip())

        if appropriate_build is None:
            print("Builds are: {0} \n. Remote is {1}, {2}. Result is: {3}"
                          .format(builds, remote.ip, remote.username, version))
            raise Exception("Build %s not found" % version)
        return appropriate_build

    def install_cb_version_on_node(self, node, upgrade_version):
        shell = RemoteMachineShellConnection(node)
        appropriate_build = self.__get_build(upgrade_version,
                                             shell)
        self.assertTrue(appropriate_build.url,
                        msg="Unable to find build {}".format(
                            upgrade_version))
        self.assertTrue(shell.download_build(appropriate_build),
                        "Failed while downloading the build!")

        self.test_log.info("Starting node upgrade")
        upgrade_success = shell.couchbase_upgrade(appropriate_build,
                                                  save_upgrade_config=False,
                                                  forcefully=self.is_downgrade)
        shell.disconnect()
        return upgrade_success

    def assertTrue(self, expr, msg=None):
        if msg:
            msg = "{0} is not true : {1}".format(expr, msg)
        else:
            msg = "{0} is not true".format(expr)
        if not expr:
            raise (Exception(msg))