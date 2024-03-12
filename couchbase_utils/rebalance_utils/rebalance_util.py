import time

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from common_lib import sleep
from custom_exceptions.exception import ServerUnavailableException, \
    RebalanceFailedException
from global_vars import logger


class RebalanceUtil(object):
    def __init__(self, server):
        self.server = server
        self.cluster_rest = ClusterRestAPI(server)
        self.log = logger.get("test")

    def print_ui_logs(self, last_n=10):
        _, json_parsed = self.cluster_rest.ui_logs()
        logs = json_parsed['list']
        logs.reverse()
        logs = '\n'.join([logs[i] for i in range(min(last_n, len(logs)))])
        self.log.critical(f"Latest logs from UI on {self.server.ip}:\n{logs}")

    def get_rebalance_status(self, include_failover=True):
        status, json_parsed = self.cluster_rest.cluster_tasks()
        # Find the right dict containing the rebalance task
        for i in range(0, len(json_parsed)):
            json_parsed_temp = json_parsed[i]
            if "type" in json_parsed_temp:
                if json_parsed_temp["type"] == "rebalance":
                    json_parsed = json_parsed[i]
                    break
        if status:
            if "status" in json_parsed:
                rebalance_status = json_parsed["status"]
                if rebalance_status == "notRunning":
                    # Rebalance finished/notRunning scenario
                    # Required in AF during rebalance where we confuse rebalance with AF
                    rebalance_status = "none"
                if not include_failover \
                        and json_parsed["subtype"] == "failover":
                    rebalance_status = "none"
                return rebalance_status
        return None

    def stop_rebalance(self, wait_timeout=10):
        status, content = self.cluster_rest.stop_rebalance()
        if status:
            for i in range(wait_timeout):
                if self.get_rebalance_status() == 'running':
                    self.log.warn(f"Rebalance not stopped after {i+1} sec")
                    sleep(1)
                    status = False
                else:
                    self.log.info("Rebalance stopped")
                    status = True
                    break
        else:
            self.log.error(f"Rebalance not stopped due to {content}")
        return status

    def _rebalance_progress_status(self, include_failover=True):
        status, json_parsed = self.cluster_rest.cluster_tasks()
        # Find the right dict containing the rebalance task
        for i in range(0, len(json_parsed)):
            json_parsed_temp = json_parsed[i]
            if "type" in json_parsed_temp:
                if json_parsed_temp["type"] == "rebalance":
                    json_parsed = json_parsed[i]
                    break
        if status:
            if "status" in json_parsed:
                rebalance_status = json_parsed["status"]
                if rebalance_status == "notRunning":
                    # rebalance finished/notRunning scenario
                    rebalance_status = "none"
                # this is required in AF during rebalance where we confuse rebalance with AF
                if not include_failover and json_parsed["subtype"] == "failover":
                    rebalance_status = "none"
                return rebalance_status
        else:
            return None

    def _rebalance_status_and_progress(self, task_status_id=None):
        """
        Returns a 2-tuple capturing the rebalance status and progress, as follows:
            ('running', progress) - if rebalance is running
            ('none', 100)         - if rebalance is not running (i.e. assumed done)
            (None, -100)          - if there's an error getting the rebalance progress
                                    from the server or ServerUnavailable
            (None, -1)            - if the server responds but there's no information on
                                    what the status of rebalance is

        The progress is computed as a average of the progress of each node
        rounded to 2 decimal places.

        Throws RebalanceFailedException if rebalance progress returns an error message
        """
        avg_percentage = -1
        rebalance_status = None
        try:
            status, json_parsed = self.cluster_rest.cluster_tasks()
        except ServerUnavailableException as e:
            self.log.error(e)
            return None, -100
        # Find the right dict containing the rebalance task
        for i in range(0, len(json_parsed)):
            json_parsed_temp = json_parsed[i]
            if "type" in json_parsed_temp:
                if json_parsed_temp["type"] == "rebalance":
                    json_parsed = json_parsed[i]
                    break
        if status:
            if "status" in json_parsed:
                rebalance_status = json_parsed["status"]
                if rebalance_status == "notRunning":
                    # rebalance finished/notRunning scenario
                    rebalance_status = "none"
                if "errorMessage" in json_parsed:
                    msg = '{0} - rebalance failed'.format(json_parsed)
                    self.log.error(msg)
                    self.print_ui_logs()
                    raise RebalanceFailedException(msg)
                elif rebalance_status == "running":
                    if task_status_id is not None \
                            and "statusId" in json_parsed \
                            and json_parsed["statusId"] != task_status_id:
                        # Case where current rebalance is done and
                        # new rebalance / failover task is running in cluster
                        rebalance_status = "none"
                        avg_percentage = 100
                        self.log.warning(
                            "Prev rebalance id '%s' changed to '%s'"
                            % (task_status_id, json_parsed["statusId"]))
                    else:
                        avg_percentage = round(json_parsed["progress"], 2)
                        self.log.debug("Rebalance percentage: {0:.02f} %"
                                       .format(avg_percentage))
                else:
                    # Sleep before printing rebalance failure log
                    sleep(5, log_type="infra")
                    status, json_parsed = self.cluster_rest.cluster_tasks()
                    # Find the right dict containing the rebalance task
                    for i in range(0, len(json_parsed)):
                        json_parsed_temp = json_parsed[i]
                        if "type" in json_parsed_temp:
                            if json_parsed_temp["type"] == "rebalance":
                                json_parsed = json_parsed[i]
                                break
                    if "errorMessage" in json_parsed:
                        msg = '{0} - rebalance failed'.format(json_parsed)
                        self.print_ui_logs()
                        raise RebalanceFailedException(msg)
                    avg_percentage = 100
        else:
            avg_percentage = -100
        return rebalance_status, avg_percentage

    def monitor_rebalance(self, stop_if_loop=True, sleep_step=3,
                          progress_count=100):
        start = time.time()
        progress = 0
        retry = 0
        same_progress_count = 0
        previous_progress = 0
        while progress != -1 \
                and (progress != 100
                     or self._rebalance_progress_status() == 'running') \
                and retry < 20:
            # -1 is error , -100 means could not retrieve progress
            progress = self._rebalance_status_and_progress()[1]
            if progress == -100:
                self.log.error("Unable to retrieve rebalanceProgress. "
                               "Retrying after 1 second")
                retry += 1
            else:
                retry = 0
            if stop_if_loop:
                # reset same_progress_count if get a different result,
                # or progress is still O
                # it may take long time until the results are different from 0
                if previous_progress != progress or progress == 0:
                    previous_progress = progress
                    same_progress_count = 0
                else:
                    same_progress_count += 1
                if same_progress_count > progress_count:
                    self.log.error("Rebalance progress code in "
                                   "infinite loop: %s" % progress)
                    return False
            # Sleep to printout less log
            sleep(sleep_step, log_type="infra")
        if progress < 0:
            self.log.error("Rebalance progress code: %s" % progress)
            return False
        else:
            duration = time.time() - start
            self.log.info(f"Rebalance done. Taken {duration} secs to complete")
            # Sleep required for ns_server to be ready for further actions
            sleep(10, "Wait after rebalance complete")
            return True
