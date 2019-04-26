from Jython_tasks.task import AutoFailoverNodesFailureTask, NodeDownTimerTask


class DiskError:
    FAILOVER_DISK = "failover_disk"
    DISK_FULL = "disk_full"

    def __init__(self, logger, task_manager, orchestrator,
                 server_to_fail, timeout, pause_between_failover_action,
                 failover_expected, timeout_buffer, disk_timeout=120,
                 disk_location="/data", disk_size=5120):
        self.log = logger
        self.task_manager = task_manager
        self.orchestrator = orchestrator
        self.server_to_fail = server_to_fail
        self.timeout = timeout
        self.pause_between_failover_action = pause_between_failover_action
        self.failover_expected = failover_expected
        self.timeout_buffer = timeout_buffer
        self.disk_timeout = disk_timeout
        self.disk_location = disk_location
        self.disk_location_size = disk_size

    def create(self, action=None):
        self.log.info("Simulation disk scenario '{0}'".format(action))

        if action == DiskError.FAILOVER_DISK:
            task = AutoFailoverNodesFailureTask(
                self.task_manager, self.orchestrator, self.server_to_fail,
                "disk_failure", self.timeout,
                self.pause_between_failover_action, self.failover_expected,
                self.timeout_buffer, disk_timeout=self.disk_timeout,
                disk_location=self.disk_location,
                disk_size=self.disk_location_size)
            self.task_manager.add_new_task(task)
            self.task_manager.get_task_result(task)
        elif action == DiskError.DISK_FULL:
            task = AutoFailoverNodesFailureTask(
                self.task_manager, self.orchestrator, self.server_to_fail,
                "disk_full", self.timeout, self.pause_between_failover_action,
                self.failover_expected, self.timeout_buffer,
                disk_timeout=self.disk_timeout,
                disk_location=self.disk_location,
                disk_size=self.disk_location_size)
            self.task_manager.add_new_task(task)
            self.task_manager.get_task_result(task)
        else:
            self.log.warning("Unsupported disk action '{0}'".format(action))

    def revert(self, action=None):
        self.log.info("Reverting disk scenario '{0}'".format(action))

        if action == DiskError.FAILOVER_DISK:
            action = "recover_disk_failure"
        elif action == DiskError.DISK_FULL:
            action = "recover_disk_full_failure"
        else:
            self.log.warning("Unsupported disk action '{0}'".format(action))
            return

        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            action, self.timeout, self.pause_between_failover_action,
            expect_auto_failover=False, timeout_buffer=self.timeout_buffer,
            check_for_failover=False, disk_timeout=self.disk_timeout,
            disk_location=self.disk_location,
            disk_size=self.disk_location_size)
        self.task_manager.add_new_task(task)
        self.task_manager.get_task_result(task)
