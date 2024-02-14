AMAZON = ["amzn2", "al2023"]
CENTOS = ["centos6", "centos7", "centos8"]
DEBIAN = ["debian8", "debian9", "debian10", "debian11", "debian12"]
OEL = ["oel7", "oel8", "oel9"]
RHEL = ["rhel7", "rhel8", "rhel9"]
SUSE = ["suse12", "suse15"]
UBUNTU = ["ubuntu16.04", "ubuntu18.04", "ubuntu20.04", "ubuntu22.04"]
ALMA = ["alma9"]
ROCKY = ['rocky9']
LINUX_DISTROS = AMAZON + CENTOS + DEBIAN + OEL + RHEL + SUSE + UBUNTU \
    + ALMA + ROCKY
MACOS_VERSIONS = ["10.13", "10.14", "10.15", "11.1", "11.2", "11.3", "12.3",
                  "macos"]
WINDOWS_SERVER = ["2016", "2019", "2022", "windows"]
SUPPORTED_OS = LINUX_DISTROS + MACOS_VERSIONS + WINDOWS_SERVER
X86 = CENTOS + SUSE + RHEL + OEL + AMAZON + ALMA + ROCKY
LINUX_AMD64 = DEBIAN + UBUNTU
AMD64 = DEBIAN + UBUNTU + WINDOWS_SERVER
DEBUG_INFO_SUPPORTED = \
    CENTOS + SUSE + RHEL + OEL \
    + DEBIAN + UBUNTU \
    + ALMA + ROCKY \
    + AMAZON


class BuildUrl(object):
    CB_DOWNLOAD_SERVER = "172.23.126.166"
    CB_RELEASE_URL_PATH = "builds/releases"
    CB_LATESTBUILDS_URL_PATH = "builds/latestbuilds/couchbase-server"
    CB_VERSION_NAME = {
        "0.0": "master",
        "5.1": "spock",
        "5.5": "vulcan",
        "6.0": "alice",
        "6.5": "mad-hatter", "6.6": "mad-hatter",
        "7.0": "cheshire-cat",
        "7.1": "neo", "7.2": "neo",
        "7.5": "elixir",
        "7.6": "trinity",
        "8.0": "morpheus"}

    CB_BUILD_FILE_PREFIX = "couchbase-server"
