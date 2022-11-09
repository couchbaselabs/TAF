CB_RELEASE_BUILDS = {"0.0.0": "0000", "2.1.1": "764", "2.2.0": "821",
                     "2.5.0": "1059", "2.5.1": "1083", "2.5.2": "1154",
                     "3.0.3": "1716", "3.1.5": "1859", "3.1.6": "1904",
                     "4.0.0": "4051", "4.1.0": "5005", "4.1.1": "5914",
                     "4.1.2": "6088", "4.5.0": "2601", "4.5.1": "2844",
                     "4.6.0": "3573", "4.6.1": "3652", "4.6.2": "3905",
                     "4.6.3": "4136", "4.6.4": "4590", "4.7.0": "0000",
                     "4.6.5": "4742", "5.0.0": "3519", "5.0.1": "5003",
                     "5.0.2": "5509", "5.1.0": "5552", "5.1.1": "5723",
                     "5.1.2": "6030", "5.1.3": "6212", "5.5.0": "2958",
                     "5.5.1": "3511", "5.5.2": "3733", "5.5.3": "4041",
                     "5.5.4": "4338", "5.5.5": "4523", "5.5.6": "0000",
                     "6.0.0": "1693", "6.0.1": "2037", "6.0.2": "2601",
                     "6.0.3": "2895", "6.0.4": "3090", "6.0.5": "3340",
                     "6.5.0": "4967", "6.5.1": "6296", "6.5.2": "0000",
                     "6.6.0": "7909", "6.6.1": "9213", "6.6.2": "9588",
                     "6.6.3": "0000", "6.6.4": "0000", "6.6.5": "0000",
                     "7.0.0": "0000", "7.0.1": "0000", "7.0.2": "0000",
                     "7.0.3": "0000", "7.0.4": "0000", "7.0.5": "0000",
                     "7.1.0": "0000", "7.1.1": "0000", "7.1.2": "0000",
                     "7.2.0": "0000", "7.5.0": "0000", "8.0.0": "0000"}

CB_VERSION_NAME = {"0.0": "master",
                   "4.0": "sherlock", "4.1": "sherlock",
                   "4.5": "watson", "4.6": "watson", "4.7": "spock",
                   "5.0": "spock", "5.1": "spock", "5.5": "vulcan",
                   "6.0": "alice", "6.5": "mad-hatter", "6.6": "mad-hatter",
                   "7.0": "cheshire-cat", "7.1": "neo", "7.2": "neo",
                   "7.5": "elixir", "8.0": "morpheus"}

SYSTEMD_SERVER = ["centos 8", "centos 7", "suse 12", "suse 15", "ubuntu 16.04",
                  "ubuntu 18.04", "ubuntu 20.04", "debian 8", "debian 9",
                  "debian 10", "debian 11", "rhel 8", "oel 7", "oel 8"]
VERSION_FILE = "VERSION.txt"
MIN_COMPACTION_THRESHOLD = 2
MAX_COMPACTION_THRESHOLD = 100
NR_INSTALL_LOCATION_FILE = "nonroot_install_location.txt"
RPM_DIS_NAME = ["centos", "red hat", "opensuse", "suse", "oracle linux"]
# Allow for easy switch to a local mirror of the download stuff
# (for people outside the mountain view office it's nice to be able to
# be running this locally without being on VPN (which my test machines isn't)
CB_DOWNLOAD_SERVER = "172.23.126.166"
CB_DOWNLOAD_SERVER_FQDN = "latestbuilds.service.couchbase.com"

MV_LATESTBUILD_REPO = "http://latestbuilds.service.couchbase.com/"
COUCHBASE_REPO = "http://{0}/builds/latestbuilds/couchbase-server/".format(CB_DOWNLOAD_SERVER)
CB_LATESTBUILDS_REPO = "http://{0}/builds/latestbuilds/"
CB_REPO = "http://{0}/builds/latestbuilds/couchbase-server/".format(CB_DOWNLOAD_SERVER)
CB_FQDN_REPO = "http://{0}/builds/latestbuilds/couchbase-server/".format(CB_DOWNLOAD_SERVER_FQDN)
CB_RELEASE_REPO = "http://{0}/builds/releases/".format(CB_DOWNLOAD_SERVER)
CB_RELEASE_APT_GET_REPO = "http://latestbuilds.service.couchbase.com/couchbase-release/10/couchbase-release-1.0-0.deb"
CB_RELEASE_YUM_REPO = "http://latestbuilds.service.couchbase.com/couchbase-release/10/couchbase-release-1.0-0.noarch.rpm"
IS_CONTAINER = False
