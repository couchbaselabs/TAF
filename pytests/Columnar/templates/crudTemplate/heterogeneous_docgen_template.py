"""
Created on 27-May-2025

@author: himanshu.jain@couchbase.com
"""

import json
import random
import string
from datetime import datetime, timedelta



class Person:
    def __init__(self, heterogeneity=1):
        self.heterogeneity = heterogeneity
        self.id = 0

    """
    string (default)- 'asdsadd asdasdas'
    dict - {'firstname': 'asdsadd', 'lastname': 'asdasdas'}
    """
    def __get_name(self):
        def random_name():
            length = random.randint(4, 10)
            first_char = 'a' if random.random() < 0.8 else random.choice(string.ascii_lowercase)
            other_chars = ''.join(random.choices(string.ascii_lowercase, k=length - 1))
            return first_char + other_chars

        use_dict = random.random() < (0.5 * self.heterogeneity)

        first = random_name()
        last = random_name()

        if use_dict:
            return {'firstname': first, 'lastname': last}
        else:
            return f"{first} {last}"

    """
    int (default) or string
    """
    def __get_age(self):
        use_string = random.random() < (0.5 * self.heterogeneity)
        age = random.randint(28, 32)
        if use_string:
            return str(age)
        else:
            return age

    """
    string (default) or null or missing
    """
    def __get_email(self):
        r = random.random()

        if r < (1 / 3 * self.heterogeneity):
            # Missing field
            return ...
        elif r < (2 / 3 * self.heterogeneity):
            # Null
            return None
        else:
            # Valid random email
            def random_part():
                length = random.randint(4, 10)
                first_char = 'a' if random.random() < 0.8 else random.choice(string.ascii_lowercase)
                other_chars = ''.join(random.choices(string.ascii_lowercase, k=length - 1))
                return first_char + other_chars

            return f"{random_part()}@{random_part()}.com"

    """
    list of strings (default) or nested list of strings
    """
    def __get_hobbies(self):
        hobbies_list = [
            "Reading", "Writing", "Drawing", "Painting", "Sculpting", "Photography", "Calligraphy",
            "Pottery", "Origami", "Knitting", "Crocheting", "Embroidery", "Sewing", "Scrapbooking",
            "Card making", "Woodworking", "Metalworking"
        ]

        hobbies = []
        for _ in range(random.randint(3, 4)):
            is_list = random.random() < (0.5 * self.heterogeneity)
            if is_list:
                sublist = [random.choice(hobbies_list) for _ in range(random.randint(1, 3))]
                hobbies.append(sublist)
            else:
                hobbies.append(random.choice(hobbies_list))
        return hobbies

    """
    nested dict (level 0 = default)
    """
    def __get_address(self):
        def random_zip():
            length = random.randint(4, 6)
            zipcode = ''.join(random.choices('0123456789', k=length))
            if random.choice([True, False]):
                return int(zipcode)
            return zipcode

        def get_random_coordinate():
            """
            Return a random float for latitude or longitude OR None.
            50% chance for float, 50% chance for None.
            Latitude range: -90 to 90
            Longitude range: -180 to 180
            """
            if random.choice([True, False]):
                return None
            else:
                # For latitude or longitude, range depends on which is asked,
                # but here we just generate a generic float, so caller decides range.
                return random.uniform(-180, 180)

        def get_random_date():
            random_date_str = (datetime(2015, 1, 1) + timedelta(
                days=random.randint(0, (datetime.now() - datetime(2015, 1, 1)).days))).strftime('%Y-%m-%d')
            return random_date_str

        street_prefixes = ['North', 'East', 'West', 'South', 'Old', 'New', 'Lake', 'Port']
        street_names = ['Oak', 'Maple', 'Pine', 'Cedar', 'Elm', 'Washington', 'Lincoln', 'Adams', 'Jefferson',
                        'Madison']
        street_suffixes = ['St', 'Ave', 'Blvd', 'Rd', 'Ln', 'Dr', 'Ct', 'Pl', 'Terrace', 'Way']

        def get_random_street():
            prefix = random.choice(street_prefixes + [''])  # empty string means no prefix sometimes
            name = random.choice(street_names)
            suffix = random.choice(street_suffixes)

            # Construct street name, strip extra spaces if prefix is empty
            street = f"{prefix} {name} {suffix}".strip()
            return street

        city_list = [
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio",
            "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus",
            "Charlotte", "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington",
            "Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City", "Portland", "Las Vegas",
            "Memphis", "Louisville", "Baltimore", "Milwaukee", "Albuquerque", "Tucson", "Fresno",
            "Sacramento", "Kansas City", "Mesa", "Atlanta", "Omaha", "Colorado Springs", "Raleigh",
            "Miami", "Long Beach", "Virginia Beach", "Oakland", "Minneapolis", "Tulsa", "Arlington",
            "New Orleans", "Wichita", "Cleveland", "Tampa", "Bakersfield", "Aurora", "Honolulu",
            "Anaheim", "Santa Ana", "Corpus Christi", "Riverside", "Lexington", "St. Louis",
            "Stockton", "Pittsburgh", "Saint Paul", "Cincinnati", "Anchorage", "Henderson",
            "Greensboro", "Plano", "Newark", "Lincoln", "Toledo", "Orlando", "Chula Vista",
            "Irvine", "Fort Wayne", "Jersey City", "Durham", "St. Petersburg", "Laredo", "Buffalo",
            "Madison", "Lubbock", "Chandler", "Scottsdale", "Glendale", "Reno", "Norfolk",
            "Winston–Salem", "North Las Vegas", "Irving", "Chesapeake", "Gilbert", "Hialeah",
            "Garland", "Fremont", "Richmond", "Boise", "Baton Rouge"
        ]


        if self.heterogeneity == 0:
            # Simple address
            return {
                'street': '123 Main St',
                'city': 'A'+random.choice(city_list),
                'zip': random_zip()
            }

        elif self.heterogeneity == 1:
            # Complex address with nested structure
            return {
                'primaryAddress': {
                    'street': get_random_street(),
                    'city': 'A'+random.choice(city_list),
                    'zip': random_zip()
                },
                'secondaryAddress': {
                    'street': get_random_street(),
                    'city': 'A'+random.choice(city_list),
                    'zip': random_zip()
                },
                'metadata': {
                    'created_at': get_random_date(),
                    'verified': random.choice([True, False])
                },
                'coordinates': {
                    'primary': {
                        'latitude': get_random_coordinate() if random.choice([True, False]) else None,
                        'longitude': get_random_coordinate() if random.choice([True, False]) else None
                    },
                    'secondary': {
                        'latitude': get_random_coordinate() if random.choice([True, False]) else None,
                        'longitude': get_random_coordinate() if random.choice([True, False]) else None
                    }
                }
            }

        else:
            raise ValueError("heterogeneity must be 0 (simple) or 1 (complex)")

    """
    string
    """
    def __get_description(self):
        # Number of words: 20 to 100
        word_count = random.randint(20, 100)
        words = [
            ''.join(random.choices(string.ascii_lowercase, k=random.randint(3, 10)))
            for _ in range(word_count)
        ]
        return ' '.join(words).capitalize() + '.'


    def __get_spare(self):
        if random.random() > 0.03:  # 97% chance to skip
            return ...  # means missing, don't set field

        int_values = [0, 1, 2, 10, 100]
        string_values = ["active", "pending", "failed", "completed", "unknown"]
        bool_values = [True, False]

        options = [
            lambda: random.choice(int_values),
            lambda: random.choice(string_values),
            lambda: random.choice(bool_values)
        ]

        return random.choice(options)()


    """
    heterogeneity - 0 to 1
    0 = no heterogeneity = homogeneous
    1 = complete heterogeneity (default)
        if a field can have 2 data types, 50% of each data type
    """
    def generate_document(self):
        doc = dict()
        self.id += 1
        doc["id"] = str(self.id)
        doc["name"] = self.__get_name()
        doc["age"] = self.__get_age()

        email = self.__get_email()
        if email is not ...:
            doc["email"] = email

        doc["hobbies"] = self.__get_hobbies()
        doc["address"] = self.__get_address()
        doc["description"] = self.__get_description()

        spare_val = self.__get_spare()
        if spare_val is not ...:
            doc["spare"] = spare_val

        return doc


if __name__ == "__main__":
    person = Person(heterogeneity=1)
    res = []
    for i in range(10):
        doc = None
        try:
            doc = person.generate_document()
        except Exception as err:
            print(str(err))

        json_string = json.dumps(doc)
        res.append(doc)
        # print(json_string)
    print(json.dumps(res))