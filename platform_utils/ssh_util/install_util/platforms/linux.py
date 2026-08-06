from time import sleep

from install_util.constants.linux import LinuxConstants
from shell_util.remote_connection import RemoteMachineShellConnection


class Linux(LinuxConstants):
    def __init__(self, test_server):
        super(Linux, self).__init__()
        self.shell = RemoteMachineShellConnection(test_server)

    def uninstall(self):
        self.shell.stop_couchbase()
        cmd = self.cmds
        if self.shell.nonroot:
            cmd = self.non_root_cmds
        cmd = cmd[self.shell.info.deliverable_type]["uninstall"]
        self.shell.execute_command(cmd)
        return True

    def pre_install(self, cluster_profile):

        cmd = self.cmds
        if self.shell.nonroot and cluster_profile != "default":
            raise Exception("No support for installing {}".format(cluster_profile) +
                            " cluster profile on non root")
        cmd = cmd[self.shell.info.deliverable_type]["pre_install"]
        cmd = cmd.format(cluster_profile)
        self.shell.execute_command(cmd)
        return True

    def install(self, build_url):
        cmd = self.cmds
        if self.shell.nonroot:
            cmd = self.non_root_cmds
        cmd = cmd[self.shell.info.deliverable_type]["install"]
        f_name = build_url.split("/")[-1]
        cmd = cmd.replace("buildpath", "{}/{}"
                          .format(self.download_dir, f_name))
        self.shell.execute_command(cmd)

        output, err = self.shell.execute_command(cmd)
        if output[0] == '1':
            return True
        self.shell.log.critical("Output: {}, Error: {}".format(output, err))
        return False

    def post_install(self):
        cmds = self.cmds
        if self.shell.nonroot:
            cmds = self.non_root_cmds
        cmds = cmds[self.shell.info.deliverable_type]
        cmd = cmds["post_install"]
        retry_cmd = cmds["post_install_retry"]

        if cmd is None:
            return True

        output, err = self.shell.execute_command(cmd)
        if output[0] == '1':
            return True

        self.shell.log.critical("Output: {}, Error: {}".format(output, err))
        if retry_cmd is None:
            return False

        self.shell.log.critical("Retrying post_install steps")
        output, err = self.shell.execute_command(retry_cmd)
        if output[0] == '1':
            return True
        self.shell.log.critical("Output: {}, Error: {}".format(output, err))
        return False

    def init_cluster(self, node):
        from cb_server_rest_util.cluster_nodes.cluster_nodes_api \
            import ClusterRestAPI
        rest = ClusterRestAPI(node)
        for path in set([_f for _f in [node.data_path,
                                       node.index_path,
                                       node.cbas_path] if _f]):
            for cmd in (f"rm -rf {path}/*",
                        f"chown -R couchbase:couchbase {path}"):
                self.shell.execute_command(cmd)

        rest.initialize_node(node.rest_username, node.rest_password,
                             data_path=node.data_path,
                             index_path=node.index_path,
                             cbas_path=node.cbas_path)
        max_retry = 10
        mcd_mem_reservered = 0
        while max_retry > 0 and mcd_mem_reservered == 0:
            status, content = rest.node_details()
            if not status:
                return False
            mcd_mem_reservered = int(content["mcdMemoryReserved"])
            if mcd_mem_reservered != 0:
                break
            sleep(2)
            max_retry -= 1
        else:
            self.shell.log.critical("ERROR: mcdMemoryReserved=0")
            return False

        status, _ = rest.configure_memory(
            {"memoryQuota": mcd_mem_reservered})
        if not status:
            return False
        status, _ = rest.setup_services([node.services or "kv"])
        if not status:
            return False
        status, _ = rest.establish_credentials(node.rest_username,
                                               node.rest_password)
        if not status:
            return False
        return True
