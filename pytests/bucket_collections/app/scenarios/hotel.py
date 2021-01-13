import traceback
from threading import Thread

from bucket_collections.app.constants.global_vars import sdk_clients
from bucket_collections.app.lib.common_util import get_all_scenarios, \
    get_random_scenario
from global_vars import logger

from java.lang import Exception as Java_base_exception


class Hotel(Thread):
    scenarios = dict()
    log = logger.get("test")

    def __init__(self, bucket, op_type, **kwargs):
        super(Hotel, self).__init__()
        self.bucket = bucket
        self.op_type = op_type
        self.op_count = 1
        self.result = None
        self.exception = None

        if 'num_items' in kwargs:
            self.num_items = kwargs['num_items']
        if 'op_count' in kwargs:
            self.op_count = kwargs['op_count']

        Hotel.scenarios = get_all_scenarios(Hotel)

    @staticmethod
    def generic_query_run(query):
        client = sdk_clients["cbas_admin"]
        result = client.cluster.analyticsQuery(query)
        return str(result)

    def scenario_get_hotels_with_low_ratings(self):
        query = "SELECT * FROM `hotels`.`hotel_with_low_ratings`"
        return self.generic_query_run(query)

    def scenario_get_hotels_with_good_ratings(self):
        query = "SELECT * FROM `hotels`.`hotel_with_good_ratings`"
        return self.generic_query_run(query)

    def run(self):
        while self.op_count > 0:
            try:
                if self.op_type == "random":
                    rand_scenario = get_random_scenario(Hotel)
                    self.result = Hotel.scenarios[rand_scenario](self)
                else:
                    self.result = Hotel.scenarios[self.op_type](self)
                Hotel.log.info(self.result)
            except Exception as e:
                self.exception = e
                traceback.print_exc()
                break
            except Java_base_exception as e:
                self.exception = e
                traceback.print_exc()
                break

            self.op_count -= 1
