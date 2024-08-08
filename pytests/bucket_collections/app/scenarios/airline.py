import traceback
from threading import Thread

from bucket_collections.app.constants.global_vars import sdk_clients
from bucket_collections.app.lib.common_util import get_all_scenarios, \
    get_random_scenario
from global_vars import logger


class Airline(Thread):
    scenarios = dict()
    log = logger.get("test")

    def __init__(self, bucket, tenant_scope, op_type, **kwargs):
        super(Airline, self).__init__()
        self.bucket = bucket
        self.tenant_scope = tenant_scope
        self.op_type = op_type
        self.op_count = 1
        self.result = None
        self.exception = None
        self.stop_operation = False

        if 'num_items' in kwargs:
            self.num_items = kwargs['num_items']
        if 'op_count' in kwargs:
            self.op_count = kwargs['op_count']

        Airline.scenarios = get_all_scenarios(Airline)

    @staticmethod
    def generic_query_run(query):
        client = sdk_clients["cbas_admin"]
        result = client.cluster.analytics_query(query)
        return result

    def scenario_get_middle_aged_user_profiles(self):
        query = "SELECT * FROM `%s`.`user_middle_age`" % self.tenant_scope
        return self.generic_query_run(query)

    def scenario_get_aged_user_profiles(self):
        query = "SELECT * FROM `%s`.`user_senior`" % self.tenant_scope
        return self.generic_query_run(query)

    def scenario_get_all_user_bookings(self):
        query = "SELECT * FROM `airlines`.`%s_user_bookings`" \
                % self.tenant_scope
        return self.generic_query_run(query)

    def run(self):
        while self.op_count > 0 and not self.stop_operation:
            try:
                if self.op_type == "random":
                    rand_scenario = get_random_scenario(Airline)
                    self.result = Airline.scenarios[rand_scenario](self)
                else:
                    self.result = Airline.scenarios[self.op_type](self)
                Airline.log.debug("%s %s" % (self.tenant_scope, self.result))
            except Exception as e:
                self.exception = e
                traceback.print_exc()
                break

            self.op_count -= 1
