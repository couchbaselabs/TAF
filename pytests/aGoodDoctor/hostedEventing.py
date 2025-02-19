import json
import os
import time

from EventingLib.EventingOperations_Rest import EventingHelper
from table_view import TableView
from global_vars import logger

class DoctorEventing():

    def __init__(self, bucket_util):
        self.bucket_util = bucket_util
        self.log = logger.get("test")

    def create_eventing_functions(self, cluster):
        eventing_functions_dir = os.path.join(os.getcwd(), "pytests/eventing/exported_functions/volume_test")
        eventing_helper = EventingHelper(cluster.eventing_nodes[0])
        for file in os.listdir(eventing_functions_dir):
            fh = open(os.path.join(eventing_functions_dir, file), "r")
            body = fh.read()
            body = body.replace("default0", cluster.buckets[0].name)
            fh.close()
            self.log.debug("Creating Eventing function - {}".format(json.loads(body)["appname"]))
            eventing_helper.import_function(body)
        self.log.info("Done creating Eventing functions")

    def create_eventing_function(self, cluster, eventing_helper=None, file=None):
        eventing_helper = eventing_helper or EventingHelper(cluster.eventing_nodes[0])
        fh = open(os.path.join(file), "r")
        body = fh.read()
        body = body.replace("bucketname", cluster.buckets[0].name)
        fh.close()
        self.log.debug("Creating Eventing function - {}".format(json.loads(body)["appname"]))
        eventing_helper.import_function(body)

    def lifecycle_operation_for_all_functions(self, cluster, operation, state):
        eventing_helper = EventingHelper(cluster.eventing_nodes[0])
        _, response = eventing_helper.get_list_of_eventing_functions()
        for function in list(set(response["functions"])):
            self.log.info("{0} Eventing function {1}".format(operation, function))
            eventing_helper.lifecycle_operation(function, operation)
            function_name = function.split("/")[-1]
            self._wait_for_lifecycle_operation_to_complete(cluster, function_name, state)

    def delete_eventing_functions(self, cluster):
        eventing_helper = EventingHelper(cluster.eventing_nodes[0])
        status, _ = eventing_helper.delete_all_function()
        self.log.info("All Eventing functions deleted")
        return status

    def print_eventing_stats(self, cluster):
        self.table = TableView(self.log.info)
        self.table.set_headers(["Eventing Functions", "Mutations", "Success", "Failure", "Timeout"])
        response = self._aggregate_stats_from_all_eventing_nodes(cluster)
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

    def _wait_for_lifecycle_operation_to_complete(self, cluster, name, status, timeout=600):
        stop_time = time.time() + timeout
        state = False
        eventing_helper = EventingHelper(cluster.eventing_nodes[0])
        while not state and time.time() < stop_time:
            _, response = eventing_helper.get_composite_eventing_status()
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

    def _aggregate_stats_from_all_eventing_nodes(self, cluster):
        eventing_stats = dict()
        for eventing_node in cluster.eventing_nodes:
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
