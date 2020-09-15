collection_spec = dict()

default_scope = dict()
user_scope = dict()
flight_scope = dict()
hotel_scope = dict()
airline_scope = dict()

# Required scopes
default_scope["name"] = "_default"
user_scope["name"] = "users"
flight_scope["name"] = "flights"
hotel_scope["name"] = "hotels"
airline_scope["name"] = "airlines"

default_scope["collections"] = list()
user_scope["collections"] = list()
flight_scope["collections"] = list()
hotel_scope["collections"] = list()
airline_scope["collections"] = list()

# Required collections
default_scope["collections"].append({"name": "_default"})
default_scope["collections"].append({"name": "meta_data"})
user_scope["collections"].append({"name": "profile"})
user_scope["collections"].append({"name": "search_history"})
user_scope["collections"].append({"name": "flight_booking"})
user_scope["collections"].append({"name": "hotel_booking"})
flight_scope["collections"].append({"name": "flight_schedules"})
flight_scope["collections"].append({"name": "booking_data"})
flight_scope["collections"].append({"name": "checkout_cart",
                                    "maxTTL": 300})
hotel_scope["collections"].append({"name": "hotels"})
hotel_scope["collections"].append({"name": "reviews"})
hotel_scope["collections"].append({"name": "landmark"})
hotel_scope["collections"].append({"name": "booking_data"})
hotel_scope["collections"].append({"name": "checkout_cart",
                                   "maxTTL": 420})
airline_scope["collections"].append({"name": "airport"})
airline_scope["collections"].append({"name": "airline"})
airline_scope["collections"].append({"name": "routes"})

# Final scope_spec
collection_spec["scopes"] = [default_scope, user_scope, flight_scope,
                             hotel_scope, airline_scope]
