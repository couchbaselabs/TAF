import json
import os
import time

from EventingLib.EventingOperations_Rest import EventingHelper
from table_view import TableView
from global_vars import logger

class DoctorEventing():

    def __init__(self, cluster, bucket_util):
        self.cluster = cluster
        self.bucket_util = bucket_util
        self.log = logger.get("test")
        self.eventing_helper = EventingHelper(self.cluster.eventing_nodes[0])

    def create_eventing_functions(self):
        eventing_functions_dir = os.path.join(os.getcwd(), "pytests/eventing/exported_functions/volume_test")
        for file in os.listdir(eventing_functions_dir):
            fh = open(os.path.join(eventing_functions_dir, file), "r")
            body = fh.read()
            fh.close()
            self.log.debug("Creating Eventing function - {}".format(json.loads(body)["appname"]))
            self.eventing_helper.import_function(body)
        self.log.info("Done creating Eventing functions")

    def lifecycle_operation_for_all_functions(self, operation, state):
        _, response = self.eventing_helper.get_list_of_eventing_functions()
        for function in response["functions"]:
            self.log.info("{0} Eventing function {1}".format(operation, function))
            self.eventing_helper.lifecycle_operation(function, operation)
            self._wait_for_lifecycle_operation_to_complete(function, state)

    def delete_eventing_functions(self):
        status, _ = self.eventing_helper.delete_all_function()
        self.log.info("All Eventing functions deleted")
        return status

    def print_eventing_stats(self):
        self.table = TableView(self.log.info)
        self.table.set_headers(["Eventing Functions", "Mutations", "Success", "Failure", "Timeout"])
        response = self._aggregate_stats_from_all_eventing_nodes()
        for function in response:
            self.table.add_row([
                str(function),
                str(response[function]["execution_stats"]["num_processed_events"]),
                str(response[function]["execution_stats"]["on_update_success"] +
                    response[function]["execution_stats"]["on_delete_success"] +
                    response[function]["execution_stats"]["timer_callback_success"]),
                str(response[function]["execution_stats"]["on_update_failure"] +
                    response[function]["execution_stats"]["on_delete_failure"] +
                    response[function]["failure_stats"]["bucket_op_exception_count"] +
                    response[function]["failure_stats"]["n1ql_op_exception_count"] +
                    response[function]["failure_stats"]["curl_failure_count"] +
                    response[function]["execution_stats"]["timer_create_failure"] +
                    response[function]["execution_stats"]["timer_callback_failure"]),
                str(response[function]["failure_stats"]["timeout_count"])
            ])
        self.table.display("Eventing Statistics")

    def _wait_for_lifecycle_operation_to_complete(self, name, status, timeout=600):
        stop_time = time.time() + timeout
        state = False
        while not state and time.time() < stop_time:
            _, response = self.eventing_helper.get_composite_eventing_status()
            for function in response["apps"]:
                if function["name"] == name:
                    if function["composite_status"] == status:
                        self.log.info("Eventing Function {0} has been {1}".format(name, status))
                        state = True
                        break
                    self.log.debug("Waiting for Eventing function {0} to be {1}, current state is {2}".
                        format(name, status, function["composite_status"]))
                    time.sleep(5)
        return state

    def _aggregate_stats_from_all_eventing_nodes(self):
        eventing_stats = dict()
        for eventing_node in self.cluster.eventing_nodes:
            _, response = EventingHelper(eventing_node).get_all_eventing_stats()
            for function in response:
                if function["function_name"] in eventing_stats:
                    for category in function:
                        if category == "execution_stats" or category == "failure_stats":
                            for stat in function[category]:
                                if isinstance(function[category][stat], int):
                                    eventing_stats[function["function_name"]][category][stat] += function[category][stat]

                else:
                    eventing_stats[function["function_name"]] = {"execution_stats": function["execution_stats"],
                                                                 "failure_stats": function["failure_stats"]}
        return eventing_stats
