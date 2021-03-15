import traceback
from random import choice
from threading import Thread

from bucket_collections.app.constants.global_vars import sdk_clients
from bucket_collections.app.lib import query_util
from bucket_collections.app.lib.common_util import \
    get_all_scenarios, \
    get_random_scenario
from global_vars import logger

from java.lang import Exception as Java_base_exception


class Guest(Thread):
    scenarios = dict()
    log = logger.get("test")

    def __init__(self, bucket, tenant_scope, op_type, **kwargs):
        super(Guest, self).__init__()
        self.bucket = bucket
        self.tenant_scope = tenant_scope
        self.op_type = op_type
        self.op_count = 1
        self.result = None
        self.exception = None

        if 'op_count' in kwargs:
            self.op_count = kwargs['op_count']

        Guest.scenarios = get_all_scenarios(Guest)

    @staticmethod
    def __get_airline_query_summary(result):
        q_metrics = result["q_result"].metaData().metrics().get()
        return "Total src_airports: %s ,\n" \
               "Total dest_airports: %s (from %s),\n" \
               "Query: %s -> %s on days %s, time <%s>, stops <%s>,\n" \
               "Total hits: %s, \n" \
               "Time: elapsed: %s, execution: %s" \
               % (len(result["src_airports"]), len(result["dest_airports"]),
                  result["dest_airport"], result["src_airport"],
                  result["dest_airport"], result["days"],
                  result["time_clause"], result["stop_clause"],
                  q_metrics.resultCount(), q_metrics.elapsedTime(),
                  q_metrics.executionTime())

    @staticmethod
    def __get_hotel_query_summary(result):
        q_metrics = result["q_result"].metaData().metrics().get()
        return "Total Country: %d, Total Cities in selected country: %d,\n" \
               "Query: Hotels with avg_rating in %s::%s, %s \n" \
               "Total hits: %s, \n" \
               "Time: elapsed: %s, execution: %s" \
               % (len(result["countries"]), len(result["cities"]),
                  result["country"], result["city"], result["with_ratings"],
                  q_metrics.resultCount(), q_metrics.elapsedTime(),
                  q_metrics.executionTime())

    @staticmethod
    def scenario_query_routes_on_days():
        result = query_util.Airline.query_for_routes(sdk_clients["guest"])
        q_summary = "Guest - scenario_query_routes_on_days\n"
        q_summary += Guest.__get_airline_query_summary(result)
        return q_summary

    @staticmethod
    def scenario_query_routes_on_days_time():
        result = query_util.Airline.query_for_routes(sdk_clients["guest"],
                                                     with_time=True)
        q_summary = "Guest - scenario_query_routes_on_days_time\n"
        q_summary += Guest.__get_airline_query_summary(result)
        return q_summary

    @staticmethod
    def scenario_query_fights_with_stop_count():
        result = query_util.Airline.query_for_routes(
            sdk_clients["guest"],
            with_time=choice([True, False]),
            with_stop_count=True)

        q_summary = "Guest - scenario_query_fights_with_stop_count\n"
        q_summary += Guest.__get_airline_query_summary(result)
        return q_summary

    @staticmethod
    def scenario_query_search_available_hotels():
        result = query_util.Hotel.query_for_hotels(sdk_clients["guest"])
        q_summary = "Guest - scenario_search_available_hotels\n"
        q_summary += Guest.__get_hotel_query_summary(result)
        return q_summary

    @staticmethod
    def scenario_query_hotel_based_on_ratings():
        result = query_util.Hotel.query_for_hotels(sdk_clients["guest"],
                                                   with_ratings=True)
        q_summary = "Guest - scenario_query_hotel_based_on_ratings\n"
        q_summary += Guest.__get_hotel_query_summary(result)
        return q_summary

    @staticmethod
    def scenario_read_hotel_reviews():
        result = query_util.Hotel.query_for_hotels(
            sdk_clients["guest"], with_ratings=True, read_reviews=True)
        q_summary = "Guest - scenario_read_hotel_reviews\n"
        q_summary += Guest.__get_hotel_query_summary(result)
        return q_summary

    def run(self):
        while self.op_count > 0:
            try:
                if self.op_type == "random":
                    self.result = Guest.scenarios[
                        get_random_scenario(Guest)]()
                else:
                    self.result = Guest.scenarios[self.op_type]()
                Guest.log.info("%s %s" % (self.tenant_scope, self.result))
            except Exception as e:
                self.exception = e
                traceback.print_exc()
                break
            except Java_base_exception as e:
                self.exception = e
                traceback.print_exc()
                break

            self.op_count -= 1
