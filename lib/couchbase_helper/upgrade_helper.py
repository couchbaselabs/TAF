from Cb_constants import CbServer
from global_vars import logger
from membase.api.rest_client import RestConnection
from install_utils.install_ready_functions import InstallUtils

class UpgradeHelper:
    def __init__(self, task, cluster, cluster_util):
        self.test_log = logger.get("test")
        self.task = task
        self.installUtil = InstallUtils()
        self.cluster = cluster
        self.cluster_util = cluster_util

    def swap_rebalance_node_upgrade(self, node_to_remove, node_to_add, version):
        rest = self.__get_rest_node(node_to_remove)
        services = rest.get_nodes_services()
        node_to_remove.port = CbServer.port
        services_on_target_node = services[(node_to_remove.ip + ":"
                                            + str(node_to_remove.port))]

        self.installUtil.install_version_on_nodes(self.cluster, [node_to_add], version)

        self.test_log.info("Swap Rebalance starting...")
        reb_task = self.task.async_rebalance(self.cluster_util.get_nodes(self.cluster.master),
                                            to_add=[node_to_add],
                                            to_remove=[node_to_remove],
                                            check_vbucket_shuffling=False,
                                            services=[",".join(services_on_target_node)])
        return reb_task

    def failover_node_upgrade(self, node_to_upgrade, version):
        rest = self.__get_rest_node(node_to_upgrade)
        nodes = rest.node_statuses()
        for node in nodes:
            if node.ip == node_to_upgrade.ip:
                otp_node = node

        # Failing over the node
        failover_task = rest.fail_over(otpNode=otp_node.id, graceful=True)

        rebalance_res = rest.monitorRebalance()

        # Installing the target version on the node
        upgrade_result = self.installUtil.install_cb_version_on_node(node_to_upgrade,
                                                                     version)
        return upgrade_result

    def recover_upgraded_node(self, node_to_recover, recovery_type="full"):
        rest = self.__get_rest_node(node_to_recover)
        nodes = rest.node_statuses()
        for node in nodes:
            if node.ip == node_to_recover.ip:
                otp_node = node
        recovery_status = rest.set_recovery_type(otp_node.id,
                                                 recoveryType=recovery_type)
        if recovery_status:
            self.test_log.info("{0} recovery done".format(recovery_type))

        reb_task = self.task.async_rebalance(self.cluster_util.get_nodes(self.cluster.master),
                                            check_vbucket_shuffling=False)
        return reb_task

    def __get_rest_node(self, node_to_upgrade):
        target_node = None
        for node in self.cluster_util.get_nodes(self.cluster.master):
            if node.ip != node_to_upgrade.ip:
                target_node = node
                break
        return RestConnection(self.__getTestServerObj(target_node))

    def __getTestServerObj(self, node_obj):
        for node in self.cluster.servers:
            if node.ip == node_obj.ip:
                return node