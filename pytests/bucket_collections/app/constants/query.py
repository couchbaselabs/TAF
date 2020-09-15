DAYS_IN_WEEK = [0, 1, 2, 3, 4, 5, 6]
UTC_FORMAT = "%s:%s:00"

CREATE_INDEX_QUERIES = [
    'CREATE PRIMARY INDEX airport_primary ON '
    '`travel-sample`.`airlines`.`airport` WITH { "defer_build": true }',

    'CREATE PRIMARY INDEX routes_primary ON '
    '`travel-sample`.`airlines`.`routes` WITH { "defer_build": true }',

    'CREATE PRIMARY INDEX airline_primary ON '
    '`travel-sample`.`airlines`.`airline` WITH { "defer_build": true }',

    'CREATE PRIMARY INDEX hotel_primary ON '
    '`travel-sample`.`hotels`.`hotels` WITH { "defer_build": true }',

    'CREATE PRIMARY INDEX landmark_primary ON '
    '`travel-sample`.`hotels`.`landmark` WITH { "defer_build": true }',

    'CREATE PRIMARY INDEX reviews_primary ON '
    '`travel-sample`.`hotels`.`reviews` WITH { "defer_build": true }',

    'CREATE PRIMARY INDEX booking_data_primary ON '
    '`travel-sample`.`hotels`.`booking_data` WITH { "defer_build": true }',

    'CREATE PRIMARY INDEX hotel_checkout_cart_primary ON '
    '`travel-sample`.`hotels`.`checkout_cart` WITH { "defer_build": true }',

    'CREATE PRIMARY INDEX user_profile_primary ON '
    '`travel-sample`.`users`.`profile` WITH { "defer_build": true }',

    'CREATE PRIMARY INDEX hotel_booking_primary ON '
    '`travel-sample`.`users`.`hotel_booking` WITH { "defer_build": true }',

    'CREATE PRIMARY INDEX flight_booking_primary ON '
    '`travel-sample`.`users`.`flight_booking` WITH { "defer_build": true }',

    'CREATE PRIMARY INDEX flight_schedules_primary ON '
    '`travel-sample`.`flights`.`flight_schedules` '
    'WITH { "defer_build": true }',

    'CREATE PRIMARY INDEX flight_booking_data_primary ON '
    '`travel-sample`.`flights`.`booking_data` WITH { "defer_build": true }',

    'CREATE PRIMARY INDEX flight_checkout_cart_primary ON '
    '`travel-sample`.`flights`.`checkout_cart` WITH { "defer_build": true }',

    # Specific indexes
    'CREATE INDEX hotel_id ON `travel-sample`.`hotels`.`hotels`(`id`) '
    'WITH { "defer_build": true }',

    'CREATE INDEX review_id ON `travel-sample`.`hotels`.`reviews`(`id`) '
    'WITH { "defer_build": true }',

    # 'CREATE INDEX airport_name ON `travel-sample`.`airlines`.`airport`'
    # '(`airportname`) WITH { "defer_build":true }',
    #
    # 'CREATE INDEX route_src_airport ON `travel-sample`.`airlines`.`routes`'
    # '(`sourceairport`) WITH { "defer_build":true }',
    #
    # 'CREATE INDEX route_schedule_utc ON `travel-sample`.`airlines`.`routes`'
    # '(array (`s`.`utc`) for `s` in `schedule` end) WITH {"defer_build":true}',
    #
    # 'CREATE INDEX route_src_dst_day ON '
    # '`travel-sample`.`airlines`.`routes`(`sourceairport`,`destinationairport`,'
    # '(distinct (array (`v`.`day`) for `v` in `schedule` end))) '
    # 'WHERE (`type` = "route") WITH { "defer_build":true }',
    #
    # 'CREATE INDEX airline_icao ON `travel-sample`.`airlines`.`airline`'
    # '(`icao`) WITH { "defer_build":true }',
    #
    # 'CREATE INDEX airport_icao ON `travel-sample`.`airlines`.`airport`'
    # '(`icao`) WITH { "defer_build":true }',
    #
    # 'CREATE INDEX airport_faa ON `travel-sample`.`airlines`.`airport`'
    # '(`faa`) WITH { "defer_build":true }',
    #
    # 'CREATE INDEX airport_city ON `travel-sample`.`airlines`.`airport`'
    # '(`city`) WITH { "defer_build":true }',
    #
    # 'CREATE INDEX hotel_country ON `travel-sample`.`hotels`.`hotels`'
    # '(`country`) WITH { "defer_build":true }',
    #
    # 'CREATE INDEX hotel_city ON `travel-sample`.`hotels`.`hotels`'
    # '(`city`) WITH { "defer_build":true }',
    #
    # 'CREATE INDEX landmark_city ON `travel-sample`.`hotels`.`landmark`'
    # '(`city`) WITH { "defer_build":true }',

    # 'CREATE INDEX `def_name_type` ON `travel-sample`'
    # '(`name`) WHERE (`_type` = "User") WITH { "defer_build":true }'
]


class Airline(object):
    source_airports = """
        SELECT DISTINCT(sourceairport) as airport 
        FROM `travel-sample`.`airlines`.`routes` 
        """
    destination_airports = """
        SELECT DISTINCT(destinationairport) as airport 
        FROM `travel-sample`.`airlines`.`routes`
        WHERE sourceairport="%s"
        """
    route_stop_counts = """
        SELECT DISTINCT(stops) as stops 
        FROM `travel-sample`.`airlines`.`routes` 
        WHERE sourceairport="%s" AND destinationairport="%s"
        ORDER BY stops
        """
    routes_on_days = """
        SELECT id, sourceairport, destinationairport,
           (SELECT s.*
            FROM t.schedule s
            WHERE s.day in %s %s
            ORDER BY s.utc) as flights
        FROM `travel-sample`.`airlines`.`routes` AS t
        WHERE sourceairport = "%s" AND destinationairport="%s"
        """
    # Unused
    flights_from_airline_on_days = """
        SELECT t1.airline, t1.destinationairport, sch AS schedule
        FROM `travel-sample`.`airlines`.`routes` AS t1
        LET sch = ARRAY v FOR v IN t1.schedule WHEN v.day in %s END 
        WHERE t1.destinationairport = "%s" AND t1.airline = "%s"
        """


class Hotel(object):
    cities = """
        SELECT DISTINCT(city) as city
        FROM `travel-sample`.`hotels`.`hotels` 
        WHERE country="%s"
        """
    countries = """
        SELECT DISTINCT(country) as country
        FROM `travel-sample`.`hotels`.`hotels`
        """
    hotels_in_city = """
        SELECT name, address, phone, price,
           (SELECT raw avg(s.ratings.Overall) FROM t.reviews as s %s)[0]
           AS avg_rating
        FROM `travel-sample`.`hotels`.`hotels` t
        WHERE country="%s" AND city="%s"
        """

    # TODO: Can accommodate following queries within this
    # WHEN v.ratings.Overall >= 5 END
    hotels_with_review_count = """
        SELECT hotel.name, reviews, ARRAY_LENGTH(reviews) as review_count 
        FROM `travel-sample`.`hotels`.`hotels` AS hotel
        LEFT JOIN `travel-sample`.`hotels`.`reviews` as hotel_reviews 
        ON hotel.id=hotel_reviews.id
        LET reviews = Array v FOR v IN hotel_reviews.reviews END
        WHERE ARRAY_LENGTH(reviews) >= %d
        ORDER BY review_count DESC
        """
    hotel_reviews = """
        SELECT hotel.name, review.reviews 
        FROM `travel-sample`.`hotels`.`hotels` AS hotel
        LEFT JOIN `travel-sample`.`hotels`.`reviews` AS review 
        ON hotel.id = review.id
        WHERE %s
        """
    # Unused
    with_min_overall_ratings = """
        SELECT hotel.name, reviews
        FROM `travel-sample`.`hotels`.`hotels` as hotel
        LEFT JOIN `travel-sample`.`hotels`.`reviews` as hotel_reviews 
        ON hotel.id=hotel_reviews.id
        LET reviews = Array v FOR v IN hotel_reviews.reviews 
        WHEN v.ratings.Overall >= %d END
        WHERE reviews != [];
        """


class User(object):
    pass


class Guest(object):
    pass
