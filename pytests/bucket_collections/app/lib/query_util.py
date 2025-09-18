import time
import threading
from random import sample, choice, randint

from couchbase.options import QueryOptions
from couchbase.subdocument import StoreSemantics

from bucket_collections.app.constants import query, global_vars
from bucket_collections.app.constants.global_vars import sdk_clients
from bucket_collections.app.constants.query import DAYS_IN_WEEK, UTC_FORMAT
from cb_constants import DocLoading


class CommonUtil(object):
    # Class-level lock for thread synchronization
    _lock = threading.Lock()

    @staticmethod
    def get_next_id(scope, collection):
        with CommonUtil._lock:
            doc_key = "%s.%s" % (scope, collection)
            client = sdk_clients["bucket_data_writer"]
            client.select_collection(scope, "meta_data")
            success, fail = client.crud(DocLoading.Bucket.SubDocOps.COUNTER,
                                        doc_key, ["doc_counter", 1],
                                        create_path=True,
                                        store_semantics=StoreSemantics.UPSERT)
            if success:
                success, _ = client.crud(DocLoading.Bucket.SubDocOps.LOOKUP,
                                         doc_key, "doc_counter")
                return int(success[doc_key]["value"]["doc_counter"])
            raise Exception(f"CRUD COUNTER operation failed: {fail}")

    @staticmethod
    def get_current_date(scope_name):
        doc_key = "application"
        client = sdk_clients["bucket_data_writer"]
        client.select_collection(scope_name, "meta_data")
        success, _ = client.crud(DocLoading.Bucket.SubDocOps.LOOKUP,
                                 doc_key, "date")
        return success[doc_key]['value']['date']

    @staticmethod
    def incr_date(tenants):
        doc_key = "application"
        for tenant in tenants:
            tem_date = CommonUtil.get_current_date(tenant)
            client = sdk_clients["bucket_data_writer"]
            q_result = client.cluster.query(
                'SELECT RAW DATE_ADD_STR(STR_TO_UTC("%s"), 1, "day")'
                % tem_date)
            global_vars.app_current_date = q_result.rows[0]
            client.crud(DocLoading.Bucket.SubDocOps.UPSERT,
                        doc_key, ["date", global_vars.app_current_date])
            if tem_date == CommonUtil.get_current_date(tenant):
                raise Exception("Date not incremented")


class Airline(CommonUtil):
    @staticmethod
    def get_all_source_airports(client):
        src_airports = list()
        get_source_airports = client.cluster.query(
            query.Airline.source_airports)
        for row in get_source_airports.rows():
            src_airports.append(row["airport"])
        return src_airports

    @staticmethod
    def get_destination_airport_from_selected_src(client, src_airport):
        dest_airports = list()
        get_dest_airports = client.cluster.query(
            query.Airline.destination_airports % src_airport)
        for row in get_dest_airports.rows():
            dest_airports.append(row["airport"])
        return dest_airports

    @staticmethod
    def query_for_routes(client, src_airport=None, dest_airport=None,
                         with_time=False, with_stop_count=None):
        src_airports = None
        dest_airports = None
        days = sample(DAYS_IN_WEEK, randint(0, 6))

        if src_airport is None:
            # Fetch random source airports
            src_airports = Airline.get_all_source_airports(client)
            src_airport = choice(src_airports)

        if dest_airport is None:
            # Fetch random destination airport from selected src_airport
            dest_airports = Airline.get_destination_airport_from_selected_src(
                    client, src_airport)
            dest_airport = choice(dest_airports)

        time_clause = ""
        stop_clause = ""
        if with_time:
            if choice([True, False]):
                after_hr = randint(0, 22)
                after_min = choice(["00", "30"])
                after_time = UTC_FORMAT % (after_hr, after_min)
                time_clause += ' AND s.utc > "%s"' % after_time

                if choice([True, False]):
                    b4_time = UTC_FORMAT % (randint(after_hr, 23),
                                            choice(["00", "30"]))
                    time_clause += ' AND s.utc <= "%s"' % b4_time
            else:
                b4_time = UTC_FORMAT % (randint(0, 23),
                                        choice(["00", "30"]))
                time_clause += ' AND s.utc <= "%s"' % b4_time
        if with_stop_count:
            stops = list()
            result = client.cluster.query(query.Airline.route_stop_counts
                                          % (src_airport, dest_airport))
            for row in result.rows():
                stops.append(row["stops"])
            stop_clause += ' AND stops in %s' % sample(stops,
                                                       randint(1, len(stops)))

        # Run query to find available flights between src-dest
        result = client.cluster.query(
            query.Airline.routes_on_days % (days, time_clause,
                                            src_airport, dest_airport),
            QueryOptions(metrics=True))

        summary = dict()
        summary["src_airport"] = src_airport
        summary["dest_airport"] = dest_airport
        summary["src_airports"] = src_airports
        summary["dest_airports"] = dest_airports
        summary["days"] = days
        summary["time_clause"] = time_clause
        summary["stop_clause"] = stop_clause
        summary["q_result"] = result
        return summary

    @staticmethod
    def book_ticket(src, dest, seats):
        pass


class Hotel(CommonUtil):
    @staticmethod
    def get_all_countries(client):
        countries = list()
        result = client.cluster.query(query.Hotel.countries)
        for row in result.rows():
            countries.append(row["country"])
        return countries

    @staticmethod
    def get_all_city_from_country(client, country):
        cities = list()
        result = client.cluster.query(query.Hotel.cities % country)
        for row in result.rows():
            cities.append(row["city"])
        return cities

    @staticmethod
    def query_for_hotels(client, with_ratings=False, read_reviews=False):
        countries = Hotel.get_all_countries(client)
        country = choice(countries)

        cities = Hotel.get_all_city_from_country(client, country)
        city = choice(cities)

        if with_ratings:
            with_ratings = " WHERE (s.ratings.Overall) > %d" % randint(1, 5)
        else:
            with_ratings = ""

        result = client.cluster.query(
            query.Hotel.hotels_in_city % (with_ratings, country, city),
            QueryOptions(metrics=True))

        if read_reviews:
            for row in result.rows():
                hotel_clause = ' hotel.name = "%s"' % row["name"]
                _ = client.cluster.query(query.Hotel.hotel_reviews
                                         % hotel_clause)

        summary = dict()
        summary["country"] = country
        summary["city"] = city
        summary["countries"] = countries
        summary["cities"] = cities
        summary["with_ratings"] = with_ratings
        summary["q_result"] = result
        return summary
