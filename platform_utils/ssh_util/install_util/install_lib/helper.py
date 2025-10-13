import json
import re
from argparse import ArgumentParser
from urllib.request import urlopen

from install_util.constants.build import SUPPORTED_OS, BuildUrl
from shell_util.remote_connection import RemoteMachineShellConnection


class InstallHelper(object):
    def __init__(self, logger):
        self.log = logger

    def check_server_state(self, servers):
        result = True
        reachable = list()
        unreachable = list()
        for server in servers:
            try:
                shell = RemoteMachineShellConnection(server)
                shell.disconnect()
                reachable.append(server.ip)
            except Exception as e:
                self.log.error(e)
                unreachable.append(server.ip)

        if len(unreachable) > 0:
            self.log.info("-" * 100)
            for server in unreachable:
                self.log.error("INSTALL FAILED ON: \t{0}".format(server))
            self.log.info("-" * 100)
            for server in reachable:
                self.log.info("INSTALL COMPLETED ON: \t{0}".format(server))
            self.log.info("-" * 100)
            result = False
        return result

    @staticmethod
    def parse_command_line_args(arguments):
        parser = ArgumentParser(description="Installer for Couchbase-Server")
        parser.add_argument("--install_tasks",
                            help="List of tasks to run '-' separated",
                            default="populate_build_url"
                                    "-check_url_status"
                                    "-download_build"
                                    "-uninstall"
                                    "-pre_install"
                                    "-install"
                                    "-init_cluster",)
        parser.add_argument("-i", "--ini", dest="ini",
                            help="Ini file path",
                            required=True)

        parser.add_argument("-v", "--version", dest="version",
                            help="Build version to be installed",
                            required=True)
        parser.add_argument("--edition", default="enterprise",
                            help="CB edition",
                            choices=["enterprise", "community"])
        parser.add_argument("--cluster_profile", default="default",
                            choices=["default", "serverless", "provisioned", "columnar"])
        parser.add_argument("--url", default="",
                            help="Specific URL to use for build download")
        parser.add_argument("--storage_mode", default="plasma",
                            help="Sets indexer storage mode")
        parser.add_argument("--enable_ipv6", default=False,
                            help="Enable ipv6 mode in ns_server",
                            action="store_true")
        parser.add_argument("--install_debug_info",
                            dest="install_debug_info", default=False,
                            help="Flag to install debug package for debugging",
                            action="store_true")
        parser.add_argument("--skip_local_download",
                            dest="skip_local_download", default=False,
                            help="Download build individually on each node",
                            action="store_true")

        parser.add_argument("--timeout", default=300,
                            help="End install after timeout seconds")
        parser.add_argument("--build_download_timeout", default=300,
                            help="Timeout for build download. "
                                 "Usefull during slower download envs")
        parser.add_argument("--params", "-p", dest="params",
                            help="Other install params")
        parser.add_argument("--log_level", default="info",
                            help="Logging level",
                            choices=["info", "debug", "error", "critical"])

        return parser.parse_args(arguments)

    @staticmethod
    def get_os(info):
        os = info.distribution_version.lower()
        to_be_replaced = ['\n', ' ', 'gnu/linux']
        for _ in to_be_replaced:
            if _ in os:
                os = os.replace(_, '')
        if info.deliverable_type == "dmg":
            major_version = os.split('.')
            os = major_version[0] + '.' + major_version[1]
        if info.distribution_type == "Amazon Linux 2":
            os = "amzn2"
        return os

    def validate_server_status(self, node_helpers):
        result = True
        known_os = set()
        for node_helper in node_helpers:
            if node_helper.os_type not in SUPPORTED_OS:
                self.log.critical(
                    "{} - Unsupported os: {}"
                    .format(node_helper.server.ip, node_helper.os_type))
                result = False
            else:
                known_os.add(node_helper.os_type)

        if len(known_os) != 1:
            self.log.warning("Multiple OS versions found!")
        return result

    def populate_cb_server_versions(self):
        cb_server_manifests_url = "https://github.com/couchbase" \
                                  "/manifest/tree/master/couchbase-server/"
        raw_content_url = "https://raw.githubusercontent.com/couchbase" \
                          "/manifest/master/couchbase-server/"
        version_pattern = r'<annotation name="VERSION" value="([0-9\.]+)"'
        version_pattern = re.compile(version_pattern)
        payload_pattern = r'>({"payload".*})<'
        payload_pattern = re.compile(payload_pattern)
        data = urlopen(cb_server_manifests_url).read()
        data = json.loads(re.findall(payload_pattern, data.decode())[0])
        for item in data["payload"]["tree"]["items"]:
            if item["contentType"] == "file" and item["name"].endswith(".xml"):
                rel_name = item["name"].replace(".xml", "")
                data = urlopen(raw_content_url + item["name"]).read()
                rel_ver = re.findall(version_pattern, data.decode())[0][:3]
                if rel_ver not in BuildUrl.CB_VERSION_NAME:
                    self.log.info("Adding missing version {}={}"
                                  .format(rel_ver, rel_name))
                    BuildUrl.CB_VERSION_NAME[rel_ver] = rel_name
