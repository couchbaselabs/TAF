import json
from cb_constants import CbServer
from global_vars import logger
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from common_lib import sleep

class RetryRebalanceUtil:

    def __init__(self):
        self.test_log = logger.get("test")

        if CbServer.use_https:
            self.prefix = "-k https"
            self.port_to_use = 18091
        else:
            self.prefix = "http"
            self.port_to_use = 8091


    def induce_rebalance_test_condition(self, servers, test_failure_condition,
                                        bucket_name="default",
                                        vb_num=1,
                                        delay_time=60000):
        if test_failure_condition == "verify_replication":
            condition = 'fail, "{}"'.format(bucket_name)
            set_command = 'testconditions:set(verify_replication, {' \
                          + condition + '})'
        elif test_failure_condition == "backfill_done":
            condition = 'for_vb_move, "{0}", {1}, fail'.format(bucket_name, vb_num)
            set_command = 'testconditions:set(backfill_done, {' \
                          + condition + '})'
        elif test_failure_condition == "delay_rebalance_start":
            condition = 'delay, {}'.format(delay_time)
            set_command = 'testconditions:set(rebalance_start, {' \
                          + condition + '}).'
        elif test_failure_condition == "delay_verify_replication":
            condition = 'delay, "{0}", {1}'.format(bucket_name, delay_time)
            set_command = 'testconditions:set(verify_replication, {' \
                          + condition + '})'
        elif test_failure_condition == "delay_backfill_done":
            condition = 'for_vb_move, "{0}", {1}, '.format(bucket_name, vb_num)
            sub_cond = 'delay, {}'.format(delay_time)
            set_command = 'testconditions:set(backfill_done, {' \
                          + condition + '{' + sub_cond + '}})'
        elif test_failure_condition == 'delay_failover_end':
            condition = 'delay, {}'.format(delay_time)
            set_command = 'testconditions:set(failover_end, {' \
                          + condition + '}).'
        elif test_failure_condition == 'delay_failover_start':
            condition = 'delay, {}'.format(delay_time)
            set_command = 'testconditions:set(failover_start, {' \
                          + condition + '}).'
        else:
            set_command = "testconditions:set({}, fail)" \
                          .format(test_failure_condition)

        if test_failure_condition.startswith("delay_"):
            test_failure_condition = test_failure_condition[6:]
        get_command = "testconditions:get({})".format(test_failure_condition)

        username = servers[0].rest_username
        password = servers[0].rest_password
        diag_eval_command = "curl {0}://{1}:{2}@localhost:{3}/diag/eval -X POST -d".format(
                            self.prefix, username, password, self.port_to_use)

        for server in servers:
            shell = RemoteMachineShellConnection(server)
            command = diag_eval_command + " '{}'".format(set_command)
            output, error = shell.execute_command(command)
            self.test_log.info("Set Command: {0}. Return: {1}".format(command, output))
            command = diag_eval_command + " '{}'".format(get_command)
            output, error = shell.execute_command(command)
            self.test_log.info("Command: {0}, Return: {1}".format(command, output))
            shell.disconnect()

    def delete_rebalance_test_condition(self, servers, test_failure_condition):
        if test_failure_condition.startswith("delay_"):
            test_failure_condition = test_failure_condition[6:]

        delete_command = "testconditions:delete({})".format(test_failure_condition)
        get_command = "testconditions:get({})".format(test_failure_condition)

        username = servers[0].rest_username
        password = servers[0].rest_password
        diag_eval_command = "curl {0}://{1}:{2}@localhost:{3}/diag/eval -X POST -d".format(
                            self.prefix, username, password, self.port_to_use)

        for server in servers:
            shell = RemoteMachineShellConnection(server)
            command = diag_eval_command + " '{}'".format(delete_command)
            output, error = shell.execute_command(command)
            self.test_log.info("Command: {0}, Return: {1}".format(delete_command, output))
            command = diag_eval_command + " '{}'".format(get_command)
            output, error = shell.execute_command(command)
            self.test_log.info("Command: {0}, Return: {1}".format(get_command, output))
            shell.disconnect()

    def check_retry_rebalance_succeeded(self, server):
        rest = RestConnection(server)
        attempts_remaining = retry_rebalance = retry_after_secs = None
        for i in range(10):
            self.test_log.info("Getting stats : try {0}".format(i))
            result = json.loads(rest.get_pending_rebalance_info())
            self.test_log.info(result)
            if "retry_after_secs" in result:
                retry_after_secs = result["retry_after_secs"]
                attempts_remaining = result["attempts_remaining"]
                retry_rebalance = result["retry_rebalance"]
                break
            sleep(5)
        self.test_log.debug("Attempts remaining: {0}, Retry rebalance: {1}"
                       .format(attempts_remaining, retry_rebalance))
        while attempts_remaining:
            # wait for the afterTimePeriod for the failed rebalance to restart
            sleep(retry_after_secs,
                       message="Waiting for the afterTimePeriod to complete")
            try:
                result = rest.monitorRebalance()
                msg = "monitoring rebalance {0}"
                self.test_log.debug(msg.format(result))
            except Exception:
                result = json.loads(rest.get_pending_rebalance_info())
                self.test_log.debug(result)
                try:
                    attempts_remaining = result["attempts_remaining"]
                    retry_rebalance = result["retry_rebalance"]
                    retry_after_secs = result["retry_after_secs"]
                except KeyError:
                    self.test_log.error("Retrying of rebalance still did not help. "
                              "All the retries exhausted...")
                    return False
                self.test_log.info("Attempts remaining: {0}, Retry rebalance: {1}"
                              .format(attempts_remaining, retry_rebalance))
            else:
                self.test_log.info("Retry rebalanced fixed the rebalance failure")
                break
        return True
