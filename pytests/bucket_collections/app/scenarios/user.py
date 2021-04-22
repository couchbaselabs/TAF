import traceback
from datetime import timedelta
from random import choice, randint, randrange
from threading import Thread

from BucketLib.bucket import Bucket
from Cb_constants import DocLoading
from bucket_collections.app.constants import global_vars
from bucket_collections.app.constants.global_vars import sdk_clients
from bucket_collections.app.lib import query_util
from bucket_collections.app.lib.common_util import \
    get_all_scenarios, \
    get_random_scenario, get_random_reservation_date
from bucket_collections.app.constants.user import \
    COUNTRY, COMMON_KEY, DAYS_BETWEEN_DATES_INT, GENDER, START_DATE, \
    FIRST_NAMES, LAST_NAMES
from global_vars import logger

from com.couchbase.client.java.json import JsonObject
from java.lang import Exception as Java_base_exception

from sdk_exceptions import SDKException


class User(Thread):
    scenarios = dict()
    log = logger.get("test")
    max_retries = 5

    def __init__(self, bucket, scope, op_type, **kwargs):
        super(User, self).__init__()
        self.bucket = bucket
        self.scope = scope
        self.op_type = op_type
        self.op_count = 1
        self.result = None
        self.exception = None

        if 'num_items' in kwargs:
            self.num_items = kwargs['num_items']
        if 'op_count' in kwargs:
            self.op_count = kwargs['op_count']

        User.scenarios = get_all_scenarios(User)

    @staticmethod
    def get_template():
        template = JsonObject.create()
        template.put("mutated", 0)
        template.put("createdDate", "01/31/1970")
        template.put("preferences", JsonObject.create())
        return template

    @staticmethod
    def populate_values(template, u_id, user_name):
        random_seconds = randrange(DAYS_BETWEEN_DATES_INT)
        dob = START_DATE + timedelta(seconds=random_seconds)
        dob = dob.strftime("%Y-%m-%d")

        template.put("id", u_id)
        template.put("user_name", user_name)
        template.put("gender", choice(GENDER))
        template.put("dob", dob)
        template.put("country", choice(COUNTRY))
        template.put("email", user_name + "@travel_app.com")
        template.put("phone", randint(1000000000, 9999999999))

    @staticmethod
    def get_random_user_id(client, scope):
        result = client.cluster.query("SELECT raw id "
                                      "FROM `travel-sample`.`%s`.`profile`"
                                      % scope)
        return choice(result.rowsAs(int))

    def scenario_user_registration(self):
        if 'num_items' in self.__dict__:
            num_items = self.num_items
        else:
            num_items = randint(1, 20)
        collection = "profile"
        client = sdk_clients["user_manager"]
        collection_obj = self.bucket.scopes[self.scope].collections[collection]
        client.select_collection(self.scope, collection)

        template = User.get_template()
        start = collection_obj.doc_index[1]
        end = start + num_items
        if collection_obj.doc_index != (0, 0):
            collection_obj.num_items += (end - start)
        collection_obj.doc_index = (collection_obj.doc_index[0], end)

        while start < end:
            u_id = query_util.CommonUtil.get_next_id(self.scope, collection)
            key = COMMON_KEY + str(u_id)
            User.populate_values(template, u_id, key)
            retry = 1
            while retry <= User.max_retries:
                result = client.crud(
                    DocLoading.Bucket.DocOps.CREATE, key, template,
                    durability=Bucket.DurabilityLevel.MAJORITY,
                    timeout=10)
                if result["status"] is False:
                    if SDKException.DurabilityImpossibleException \
                            in str(result["error"]):
                        User.log.debug("Retrying due to d_impossible")
                    if retry == User.max_retries:
                        raise Exception("User profile creation failed: %s"
                                        % result)
                else:
                    collection_obj.num_items += 1
                    break
                retry += 1
            start += 1
        return "User - registered: %s" % num_items

    @staticmethod
    def book_flight(u_id, tenant_scope, src_airport=None, dest_airport=None):
        summary = dict()
        required_seats = choice(range(1, 7))

        ticket_type = "normal"
        checkout_cart_collection = "checkout_cart"
        d_level = Bucket.DurabilityLevel
        if [src_airport, dest_airport].count(None) == 0:
            ticket_type = "return"

        result = query_util.Airline.query_for_routes(
            sdk_clients["airline_booking"],
            src_airport=src_airport, dest_airport=dest_airport)
        flights = list()
        for row in result["q_result"].rowsAsObject():
            src_airport = row.get("sourceairport")
            dest_airport = row.get("destinationairport")
            for flight in row.get("flights"):
                flights.append(flight)

        summary["src_airport"] = src_airport
        summary["dest_airport"] = dest_airport
        summary["required_seats"] = required_seats
        if not flights:
            summary["status"] = "No flights available"
            return summary

        flight_to_book = choice(flights)
        reservation_date = get_random_reservation_date()

        checkout_doc = JsonObject.create()
        passenger_data = list()
        for _ in range(required_seats):
            gender = choice(["M", "F"])
            first_name = choice(FIRST_NAMES[gender])
            last_name = choice(LAST_NAMES)
            age = randint(3, 90)
            passenger_info = JsonObject.create()
            passenger_info.put("first_name", first_name)
            passenger_info.put("last_name", last_name)
            passenger_info.put("gender", gender)
            passenger_info.put("age", age)
            passenger_data.append(passenger_info)

        client = sdk_clients["airline_booking"]
        client.select_collection(tenant_scope, checkout_cart_collection)
        cart_id = query_util.CommonUtil.get_next_id(tenant_scope,
                                                    checkout_cart_collection)
        cart_key = "cart_%s" % cart_id
        checkout_doc.put("id", cart_id)
        checkout_doc.put("user_id", u_id)
        checkout_doc.put("flight_name", flight_to_book.get("flight"))
        checkout_doc.put("flight_time", flight_to_book.get("utc"))
        checkout_doc.put("travel_date", reservation_date)
        checkout_doc.put("from", src_airport)
        checkout_doc.put("to", dest_airport)
        checkout_doc.put("day_of_week", flight_to_book.get("day"))
        checkout_doc.put("seat_count", required_seats)
        checkout_doc.put("passengers", passenger_data)
        retry = 1
        while retry <= User.max_retries:
            result = client.crud(DocLoading.Bucket.DocOps.CREATE,
                                 cart_key, checkout_doc,
                                 durability=d_level.MAJORITY)
            if result["status"] is False:
                if SDKException.DurabilityImpossibleException \
                        in str(result["error"]):
                    User.log.debug("Retrying due to d_impossible")
                else:
                    raise Exception("Flight cart add failed: %s" % result)
            retry += 1

        if choice([True, False]):
            # Booking confirmed scenario, add ticket under flight booking
            c_name = "booking_data"
            booking_id = query_util.CommonUtil.get_next_id(tenant_scope,
                                                           c_name)
            ticket_key = "ticket_%s" % booking_id
            checkout_doc.put("id", booking_id)
            retry = 1
            while retry <= User.max_retries:
                client.select_collection(tenant_scope, c_name)
                result = client.crud(
                    DocLoading.Bucket.DocOps.CREATE, ticket_key, checkout_doc,
                    durability=d_level.MAJORITY_AND_PERSIST_TO_ACTIVE)
                if result["status"] is False:
                    if SDKException.DurabilityImpossibleException \
                            in str(result["error"]):
                        User.log.debug("Retrying due to d_impossible")
                    else:
                        raise Exception("Ticket booking failed: %s" % result)
                retry += 1

            # Add confirmed ticket under user profile
            f_booking_id = query_util.CommonUtil.get_next_id(tenant_scope,
                                                             "flight_booking")
            f_booking_key = "booking_%s" % f_booking_id
            f_booking_doc = JsonObject.create()
            f_booking_doc.put("id", f_booking_id)
            f_booking_doc.put("user_id", u_id)
            f_booking_doc.put("ticket_id", booking_id)
            f_booking_doc.put("status", "active")
            f_booking_doc.put("booked_on", global_vars.app_current_date)
            f_booking_doc.put("ticket_type", ticket_type)
            client.select_collection(tenant_scope, "flight_booking")
            result = client.crud(DocLoading.Bucket.DocOps.CREATE,
                                 f_booking_key, f_booking_doc)
            if result["status"] is False:
                raise Exception("User flight_booking add failed: %s" % result)

            # Remove booked ticket from cart
            retry = 1
            while retry <= User.max_retries:
                client.select_collection(tenant_scope,
                                         checkout_cart_collection)
                result = client.crud(DocLoading.Bucket.DocOps.DELETE, cart_key,
                                     durability=d_level.MAJORITY)
                if result["status"] is False:
                    if SDKException.DurabilityImpossibleException \
                            in str(result["error"]):
                        User.log.debug("Retrying due to d_impossible")
                else:
                    break
                retry += 1
            summary["status"] = "Booking success"
        else:
            summary["status"] = "cancelled"
        return summary

    def scenario_book_one_way_flight(self):
        summary = "User - scenario_book_one_way_flight\n"
        u_id = self.get_random_user_id(sdk_clients["user_manager"], self.scope)
        b_summary = self.book_flight(u_id, self.scope)
        summary += "From %s -> %s for %s people\n" \
                   % (b_summary["src_airport"], b_summary["dest_airport"],
                      b_summary["required_seats"])
        if b_summary["status"] == "cancelled":
            summary += "Booking not confirmed !"
        else:
            summary += b_summary["status"]
        return summary

    def scenario_book_flight_with_return(self):
        summary = "User - scenario_book_flight_with_return\n"
        u_id = self.get_random_user_id(sdk_clients["user_manager"], self.scope)
        b_summary = self.book_flight(u_id, self.scope)
        summary += "From %s -> %s for %s people\n" \
                   % (b_summary["src_airport"], b_summary["dest_airport"],
                      b_summary["required_seats"])
        if b_summary["status"] != "timeout":
            summary += "%s\n" % b_summary["status"]
            b_summary = self.book_flight(
                u_id, self.scope,
                src_airport=b_summary["dest_airport"],
                dest_airport=b_summary["src_airport"])
            summary += "From %s -> %s for %s people\n" \
                       % (b_summary["src_airport"], b_summary["dest_airport"],
                          b_summary["required_seats"])
            if b_summary["status"] == "timeout":
                summary += "Return booking not confirmed !"
            else:
                summary += "Return %s" % b_summary["status"]
        else:
            summary += "Booking not confirmed !"
        return summary

    # def _scenario_read_flight_booking_history(self):
    #     result = "User - scenario_read_flight_booking_history\n"
    #     return result
    #
    # def _scenario_book_hotel(self):
    #     result = "User - scenario_book_hotel\n"
    #     return result
    #
    # def _scenario_read_hotel_booking_history(self):
    #     result = "User - scenario_read_hotel_booking_history\n"
    #     return result
    #
    # def _scenario_write_hotel_review(self):
    #     result = "User - scenario_write_hotel_review\n"
    #     return result

    def run(self):
        while self.op_count > 0:
            try:
                if self.op_type == "random":
                    rand_scenario = get_random_scenario(User)
                    self.result = User.scenarios[rand_scenario](self)
                else:
                    self.result = User.scenarios[self.op_type](self)
                User.log.info("%s %s" % (self.scope, self.result))
            except Exception as e:
                self.exception = e
                traceback.print_exc()
                break
            except Java_base_exception as e:
                self.exception = e
                traceback.print_exc()
                break

            self.op_count -= 1
