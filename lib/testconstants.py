CB_RELEASE_BUILDS = {"0.0.0": "0000",
                     "6.0.0": "1693", "6.0.1": "2037", "6.0.2": "2413",
                     "6.0.3": "2895", "6.0.4": "3082", "6.0.5": "3340",
                     "6.5.0": "4960", "6.5.1": "6299", "6.5.2": "6634",
                     "6.6.0": "7909", "6.6.1": "9213", "6.6.2": "9600",
                     "6.6.3": "9808", "6.6.4": "9961", "6.6.5": "10080",
                     "7.0.0": "5302", "7.0.1": "6102", "7.0.2": "6703",
                     "7.0.3": "7031", "7.0.4": "7279",
                     "7.1.0": "2556", "7.1.1": "3175", "7.1.2": "3454",
                     "7.1.3": "3479", "7.1.4": "3601", "7.1.5": "3878",
                     "7.2.0": "5325", "7.2.1": "5934", "7.2.2": "6401",
                     "7.2.3": "0000",
                     "7.5.0": "3000",
                     "7.6.0": "0000",
                     "8.0.0": "0000"}

CB_VERSION_NAME = {"0.0": "master",
                   "4.0": "sherlock", "4.1": "sherlock",
                   "4.5": "watson", "4.6": "watson", "4.7": "spock",
                   "5.0": "spock", "5.1": "spock", "5.5": "vulcan",
                   "6.0": "alice", "6.5": "mad-hatter", "6.6": "mad-hatter",
                   "7.0": "cheshire-cat", "7.1": "neo", "7.2": "neo",
                   "7.5": "elixir", "7.6": "trinity", "8.0": "morpheus"}

SYSTEMD_SERVER = ["centos 8", "centos 7",
                  "suse 12", "suse 15",
                  "ubuntu 16.04", "ubuntu 18.04", "ubuntu 20.04", "ubuntu 22.04",
                  "debian 8", "debian 9", "debian 10", "debian 11", "debian 12",
                  "rhel 8", "rhel 9",
                  "oel 7", "oel 8", "oel 9",
                  "alma 9",
                  "rocky 9",
                  "amazon linux release 2 (karoo)",
                  "amazon linux release 2023 (amazon linux)"]

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
COUCHBASE_DATA_PATH = "/opt/couchbase/var/lib/couchbase/data/"

MV_LATESTBUILD_REPO = "http://latestbuilds.service.couchbase.com/"
COUCHBASE_REPO = "http://{0}/builds/latestbuilds/couchbase-server/".format(CB_DOWNLOAD_SERVER)
CB_LATESTBUILDS_REPO = "http://{0}/builds/latestbuilds/"
CB_REPO = "http://{0}/builds/latestbuilds/couchbase-server/".format(CB_DOWNLOAD_SERVER)
CB_FQDN_REPO = "http://{0}/builds/latestbuilds/couchbase-server/".format(CB_DOWNLOAD_SERVER_FQDN)
CB_RELEASE_REPO = "http://{0}/builds/releases/".format(CB_DOWNLOAD_SERVER)
CB_RELEASE_APT_GET_REPO = "http://latestbuilds.service.couchbase.com/couchbase-release/10/couchbase-release-1.0-0.deb"
CB_RELEASE_YUM_REPO = "http://latestbuilds.service.couchbase.com/couchbase-release/10/couchbase-release-1.0-0.noarch.rpm"
IS_CONTAINER = False
