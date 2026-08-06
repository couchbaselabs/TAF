import logging
import time
import sys

from install_util.constants.build import BuildUrl
from install_util.install_lib.helper import InstallHelper
from install_util.install_lib.node_helper import NodeInstaller, NodeInstallInfo
from install_util.test_input import TestInputParser
from shell_util.remote_connection import RemoteMachineShellConnection


def start_and_wait_for_threads(thread_list, timeout):
    okay = True
    for tem_thread in thread_list:
        tem_thread.start()

    for tem_thread in thread_list:
        tem_thread.join(timeout)
        okay = okay and tem_thread.result
    return okay


def print_install_status(thread_list, logger):
    status_msg = "\n"
    for tem_thread in thread_list:
        node_ip = tem_thread.node_install_info.server.ip
        t_state = tem_thread.node_install_info.state
        if tem_thread.result:
            status_msg += "  {}: Complete".format(node_ip)
        else:
            status_msg += "  {}: Failure during {}".format(node_ip, t_state)
    status_msg += "\n"
    logger.info(status_msg)


def main(logger):
    helper = InstallHelper(logger)
    args = helper.parse_command_line_args(sys.argv[1:])
    logger.setLevel(args.log_level.upper())
    user_input = TestInputParser.get_test_input(args)

    for server in user_input.servers:
        server.install_status = "not_started"

    logger.info("Node health check")
    if not helper.check_server_state(user_input.servers):
        return 1

    # Populate valid couchbase version and validate the input version
    try:
        helper.populate_cb_server_versions()
    except Exception as e:
        logger.warning("Error while reading couchbase version: {}".format(e))
    if args.version[:3] not in BuildUrl.CB_VERSION_NAME.keys():
        log.critical("Version '{}' not yet supported".format(args.version[:3]))
        return 1

    # Objects for each node to track the URLs / state to reuse
    node_helpers = list()
    for server in user_input.servers:
        server_info = RemoteMachineShellConnection.get_info_for_server(server)
        node_helpers.append(
            NodeInstallInfo(server,
                            server_info,
                            helper.get_os(server_info),
                            args.version,
                            args.edition,
                            args.cluster_profile))

    # Validate os_type across servers
    okay = helper.validate_server_status(node_helpers)
    if not okay:
        return 1

    # Populating build url to download
    if args.url:
        for node_helper in node_helpers:
            node_helper.build_url = args.url
    else:
        tasks_to_run = ["populate_build_url"]
        if args.install_debug_info:
            tasks_to_run.append("populate_debug_build_url")

        url_builder_threads = \
            [NodeInstaller(logger, node_helper, tasks_to_run)
             for node_helper in node_helpers]
        okay = start_and_wait_for_threads(url_builder_threads, 60)
        if not okay:
            return 1

    # Checking URL status
    url_builder_threads = \
        [NodeInstaller(logger, node_helper, ["check_url_status"])
         for node_helper in node_helpers]
    okay = start_and_wait_for_threads(url_builder_threads, 60)
    if not okay:
        return 1

    # Downloading build
    if args.skip_local_download:
        # Download on individual nodes
        download_threads = \
            [NodeInstaller(logger, node_helper, ["download_build"])
             for node_helper in node_helpers]
    else:
        # Local file download and scp to all nodes
        download_threads = [
            NodeInstaller(logger, node_helpers[0], ["local_download_build"])]
        okay = start_and_wait_for_threads(download_threads,
                                          args.build_download_timeout)
        if not okay:
            return 1

        download_threads = \
            [NodeInstaller(logger, node_helper, ["copy_local_build_to_server"])
             for node_helper in node_helpers]

    okay = start_and_wait_for_threads(download_threads,
                                      args.build_download_timeout)
    if not okay:
        return 1

    install_tasks = args.install_tasks.split("-")
    logger.info("Starting installation tasks :: {}".format(install_tasks))
    install_threads = [
        NodeInstaller(logger, node_helper, install_tasks)
        for node_helper in node_helpers]
    okay = start_and_wait_for_threads(install_threads, args.timeout)
    print_install_status(install_threads, logger)
    if not okay:
        return 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s: %(message)s',
                        level=logging.ERROR)
    log = logging.getLogger("install_util")

    start_time = time.time()
    exit_status = main(log)
    log.info("TOTAL INSTALL TIME = {0} seconds"
             .format(round(time.time() - start_time)))
    sys.exit(exit_status)
