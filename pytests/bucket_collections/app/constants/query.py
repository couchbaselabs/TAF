DAYS_IN_WEEK = [0, 1, 2, 3, 4, 5, 6]
UTC_FORMAT = "%s:%s:00"


class Airline(object):
    source_airports = """
        SELECT DISTINCT(sourceairport) as airport 
        FROM `travel-sample`.`inventory`.`route` 
        """
    destination_airports = """
        SELECT DISTINCT(destinationairport) as airport 
        FROM `travel-sample`.`inventory`.`route`
        WHERE sourceairport="%s"
        """
    route_stop_counts = """
        SELECT DISTINCT(stops) as stops 
        FROM `travel-sample`.`inventory`.`route` 
        WHERE sourceairport="%s" AND destinationairport="%s"
        ORDER BY stops
        """
    routes_on_days = """
        SELECT id, sourceairport, destinationairport,
           (SELECT s.*
            FROM t.schedule s
            WHERE s.day in %s %s
            ORDER BY s.utc) as flights
        FROM `travel-sample`.`inventory`.`route` AS t
        WHERE sourceairport = "%s" AND destinationairport="%s"
        """
    # Unused
    flights_from_airline_on_days = """
        SELECT t1.airline, t1.destinationairport, sch AS schedule
        FROM `travel-sample`.`inventory`.`route` AS t1
        LET sch = ARRAY v FOR v IN t1.schedule WHEN v.day in %s END 
        WHERE t1.destinationairport = "%s" AND t1.airline = "%s"
        """


class Hotel(object):
    cities = """
        SELECT DISTINCT(city) as city
        FROM `travel-sample`.`inventory`.`hotel` 
        WHERE country="%s"
        """
    countries = """
        SELECT DISTINCT(country) as country
        FROM `travel-sample`.`inventory`.`hotel`
        """
    hotels_in_city = """
        SELECT name, address, phone, price,
           (SELECT raw avg(s.ratings.Overall) FROM t.reviews as s %s)[0]
           AS avg_rating
        FROM `travel-sample`.`inventory`.`hotel` t
        WHERE country="%s" AND city="%s"
        """

    # TODO: Can accommodate following queries within this
    # WHEN v.ratings.Overall >= 5 END
    hotels_with_review_count = """
        SELECT hotel.name, reviews, ARRAY_LENGTH(reviews) as review_count 
        FROM `travel-sample`.`inventory`.`hotel` AS hotel
        WHERE ARRAY_LENGTH(hotel.reviews) >= %d
        ORDER BY review_count DESC
        """
    hotel_reviews = """
        SELECT hotel.name, review.reviews 
        FROM `travel-sample`.`inventory`.`hotel` AS hotel
        WHERE %s
        """


class User(object):
    pass


class Guest(object):
    pass
