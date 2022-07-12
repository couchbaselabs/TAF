class Linux(object):
    NAME = "linux"

    DISTRIBUTION_NAME = ["ubuntu", "centos", "red hat", "opensuse",
                         "suse", "oracle linux"]
    LOG_PATH = "/opt"
    ROOT_PATH = "/root/"
    CB_PATH = "/opt/couchbase/"
    COUCHBASE_BIN_PATH = "/opt/couchbase/bin/"
    NONROOT_CB_BIN_PATH = "~/cb/opt/couchbase/bin/"
    GOPATH = "/root/tuq/gocode"
    CAPI_INI = "/opt/couchbase/etc/couchdb/default.d/capi.ini"
    COUCHBASE_PORT_CONFIG_PATH = "/opt/couchbase/etc/couchbase"
    COUCHBASE_OLD_CONFIG_PATH = "/opt/couchbase/var/lib/couchbase/config/"
    STATIC_CONFIG = "/opt/couchbase/etc/couchbase/static_config"
    COUCHBASE_LOGS_PATH = "/opt/couchbase/var/lib/couchbase/logs"
    CONFIG_FILE = "/opt/couchbase/var/lib/couchbase/config/config.dat"
    DIST_CONFIG = "/opt/couchbase/var/lib/couchbase/config/dist_cfg"
    COUCHBASE_DATA_PATH = "/opt/couchbase/var/lib/couchbase/data/"
    COUCHBASE_LIB_PATH = "/opt/couchbase/var/lib/couchbase/"


class Mac(object):
    NAME = "mac"

    CB_PATH = "/Applications/Couchbase Server.app/Contents/Resources/" \
              "couchbase-core/"
    OS_NAME = {"10.10": "Yosemite", "10.11": "El Capitan", "10.12": "Sierra",
               "10.13": "High Sierra", "10.14": "Mojave", "10.15": "Catalina"}

    COUCHBASE_BIN_PATH = "/Applications/Couchbase Server.app/Contents/" \
                         "Resources/couchbase-core/bin/"


class Windows(object):
    NAME = "windows"

    COUCHBASE_DATA_PATH = \
        "/cygdrive/c/Program Files/Couchbase/Server/var/lib/couchbase/data/"
    COUCHBASE_DATA_PATH_RAW = \
        "c:/Program Files/Couchbase/Server/var/lib/couchbase/data/"
    COUCHBASE_CRASH_PATH_RAW = \
        "c:/Program Files/Couchbase/Server/var/lib/couchbase/crash/"
    CB_PATH = "/cygdrive/c/Program Files/Couchbase/Server/"
    CB_PATH_PARA = "/cygdrive/c/Program Files/Couchbase/Server/"
    MB_PATH = "/cygdrive/c/Program Files/Membase/Server/"
    PROCESSES_KILLED = ["msiexec32.exe", "msiexec.exe", "setup.exe",
                        "ISBEW64.*", "iexplore.*", "WerFault.*", "Firefox.*",
                        "bash.exe", "chrome.exe", "cbq-engine.exe"]
    PROCESSES_SPAWNED = ["backup.exe", "cbas.exe", "cbft.exe", "goxdcr.exe",
                         "cbq-engine.exe", "erl.exe", "eventing-producer.exe",
                         "indexer.exe", "java.exe", "memcached.exe",
                         "projector.exe", "prometheus.exe",
                         "saslauthd-port.exe", "eventing-consumer.exe"]
    """
    From spock version, we don't need windows product code above
    This "220": "CC4CF619-03B8-462A-8CCE-7CA1C22B337B" is for build 2.2.0-821 
    and earlier the new build register ID for 2.2.0-837 id is set in 
    create_windows_capture_file in remote_util
    old "211": "7EDC64EF-43AD-48BA-ADB3-3863627881B8"
    old one at 2014.12.03 "211": "6B91FC0F-D98E-469D-8281-345A08D65DAF"
    change one more time; 
    current at 2015.11.10 "211": "4D92395A-BB95-4E46-9D95-B7BFB97F7446" """
    REGISTER_ID = {"1654": "70668C6B-E469-4B72-8FAD-9420736AAF8F",
                   "170": "AF3F80E5-2CA3-409C-B59B-6E0DC805BC3F",
                   "171": "73C5B189-9720-4719-8577-04B72C9DC5A2",
                   "1711": "73C5B189-9720-4719-8577-04B72C9DC5A2",
                   "172": "374CF2EC-1FBE-4BF1-880B-B58A86522BC8",
                   "180": "D21F6541-E7EA-4B0D-B20B-4DDBAF56882B",
                   "181": "A68267DB-875D-43FA-B8AB-423039843F02",
                   "200": "9E3DC4AA-46D9-4B30-9643-2A97169F02A7",
                   "201": "4D3F9646-294F-4167-8240-768C5CE2157A",
                   "202": "7EDC64EF-43AD-48BA-ADB3-3863627881B8",
                   "210": "7EDC64EF-43AD-48BA-ADB3-3863627881B8",
                   "211": "4D92395A-BB95-4E46-9D95-B7BFB97F7446",
                   "220": "CC4CF619-03B8-462A-8CCE-7CA1C22B337B",
                   "221": "3A60B9BB-977B-0424-2955-75346C04C586",
                   "250": "22EF5D40-7518-4248-B932-4536AAB7293E",
                   "251": "AB8A4E81-D502-AE14-6979-68E4C4658CF7",
                   "252": "6E10D93C-76E0-DCA4-2111-73265D001F56",
                   "300": "3D361F67-7170-4CB4-494C-3E4E887BC0B3",
                   "301": "3D361F67-7170-4CB4-494C-3E4E887BC0B3",
                   "302": "DD309984-2414-FDF4-11AA-85A733064291",
                   "303": "0410F3F3-9F5F-5614-51EC-7DC9F7050055",
                   "310": "5E5D7293-AC1D-3424-E583-0644411FDA20",
                   "311": "41276A8D-2A65-88D4-BDCC-8C4FE109F4B8",
                   "312": "F0794D16-BD9D-4638-9EEA-0E591F170BD7",
                   "313": "71C57EAD-8903-0DA4-0919-25A0B17E20F0",
                   "314": "040F761F-28D9-F0F4-99F1-9D572E6EB826",
                   "315": "BC2C3394-12A0-A334-51F0-F1204EC43243",
                   "316": "7E2E785B-3330-1334-01E8-F6E9C27F0B61",
                   "350": "24D9F882-481C-2B04-0572-00B273CE17B3",
                   "400": "24D9F882-481C-2B04-0572-00B273CE17B3",
                   "401": "898C4818-1F6D-C554-1163-6DF5C0F1F7D8",
                   "410": "898C4818-1F6D-C554-1163-6DF5C0F1F7D8",
                   "411": "8A2472CD-C408-66F4-B53F-6797FCBF7F4C",
                   "412": "37571560-C662-0F14-D59B-76867D360689",
                   "450": "A4BB2687-E63E-F424-F9F3-18D739053798",
                   "451": "B457D40B-E596-E1D4-417A-4DD6219B64B0",
                   "460": "C87D9A6D-C189-0C44-F1DF-91ADF99A9CCA",
                   "461": "02225782-B8EE-CC04-4932-28981BC87C72",
                   "462": "C4EC4311-8AE1-28D4-4174-A48CD0291F77",
                   "463": "963479CF-8C24-BB54-398E-0FF6F2A0128C",
                   "464": "83E54668-E2D2-A014-815C-CE8B51BE15CB",
                   "465": "E38D9F16-B7A2-CC64-0D6C-D9D274D44B4F",
                   "470": "5F8BB367-A796-1104-05DE-00BCD7A787A5",
                   "500": "5F8BB367-A796-1104-05DE-00BCD7A787A5"}

    COUCHBASE_BIN_PATH = "/cygdrive/c/Program Files/Couchbase/Server/bin/"
    COUCHBASE_BIN_PATH_RAW = "C:/Program Files/Couchbase/Server/bin/"
    COUCHBASE_PORT_CONFIG_PATH = \
        "/cygdrive/c/Program Files/couchbase/Server/etc/couchbase"
    COUCHBASE_OLD_CONFIG_PATH = \
        "/cygdrive/c/Program Files/couchbase/Server/var/lib/couchbase/config"
    GOPATH = "/cygdrive/c/tuq/gocode"
    CYGWIN_BIN_PATH = "/cygdrive/c/cygwin64/bin/"
    TMP_PATH = "/cygdrive/c/tmp/"
    TMP_PATH_RAW = "C:/tmp/"
    ROOT_PATH = "/home/Administrator/"
    COUCHBASE_LOGS_PATH = \
        "/cygdrive/c/Program Files/Couchbase/Server/var/lib/couchbase/logs/"
    UNZIP = "https://s3-us-west-1.amazonaws.com/" \
            "qebucket/testrunner/win-cmd/unzip.exe"
    PSSUSPEND = "https://s3-us-west-1.amazonaws.com/" \
                "qebucket/testrunner/win-cmd/pssuspend.exe"
