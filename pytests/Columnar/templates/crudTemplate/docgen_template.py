import json
import random
import string
from datetime import datetime
from faker import Faker


# Initialize pools of unique values (100 each)
_faker = Faker()
# Hardcoded city values for predictable test documents
_hardcoded_cities = ["Trompport"]
_city_pool = list(
    set(_hardcoded_cities + [_faker.city() for _ in range(200)]))[:20]
# Generate phone numbers as long integers (10-15 digits)
_phone_pool = list(set([_faker.random_int(
    min=1000000000, max=999999999999999) for _ in range(200)]))[:100]
_name_pool = list(set([_faker.company() + " Hotel" for _ in range(200)]))[:100]
_email_pool = list(set([_faker.email() for _ in range(200)]))[:100]
_public_likes_pool = list(set([_faker.name() for _ in range(200)]))[:100]
# Hardcoded country values for predictable test documents
_hardcoded_countries = ["Morocco", "Japan", "Dominican Republic"]
_country_pool = list(
    set(_hardcoded_countries + [_faker.country() for _ in range(200)]))[:20]


class Rating:
    """
    Stores rating for class Review
    """

    def __init__(self):
        self.value = None
        self.cleanliness = None
        self.overall = None


class Review:
    """
    Stores review for class Hotel
    """

    def __init__(self):
        self.faker = Faker()
        self.date = str(datetime.now())
        self.author = self.generate_author()
        self.rating = Rating()

    def generate_author(self):
        return self.faker.name()


class Hotel:
    """
    Stores Hotel information to generate document for the doc_loader
    """

    def __init__(self, include_reviews_field=True):
        self.faker = Faker()
        self.characters_with_spaces = string.ascii_letters + string.digits + ' '
        self.characters_without_spaces = string.ascii_letters + string.digits
        self.document_size = None
        self.address = self.faker.street_address()
        self.free_parking = random.choice([True, False])
        # Weighted selection: higher probability for hardcoded cities to ensure they appear
        if _hardcoded_cities and random.random() < 0.05:
            self.city = random.choice(_hardcoded_cities)
        else:
            self.city = random.choice(_city_pool)
        self.type = random.choice(
            ["Inn", "Hostel", "Place", "Center", "Hotel", "Motel", "Suites"])
        self.url = self.faker.url()
        self.phone = random.choice(_phone_pool)
        self.price = self.faker.random_int(min=1000, max=10000)
        self.avg_rating = random.uniform(0, 1)
        self.free_breakfast = random.choice([True, False])
        self.name = random.choice(_name_pool)
        self.email = random.choice(_email_pool)
        self.mutated = 0.0
        self.padding = ""
        self.public_likes = self.build_public_likes(random.randint(0, 10))
        if include_reviews_field:
            self.reviews = self.build_review(random.randint(0, 10))

    def country_selector(self, field_type="string"):
        if field_type == "string":
            # Weighted selection: higher probability for hardcoded countries to ensure they appear
            if _hardcoded_countries and random.random() < 0.05:
                self.country = random.choice(_hardcoded_countries)
            else:
                self.country = random.choice(_country_pool)
        elif field_type == "float":
            self.country = random.uniform(0, 10)
        else:
            self.country = random.randint(1, 100)

    def build_review(self, length):
        reviews = []
        for i in range(length):
            review = Review()
            review.rating.value = random.uniform(0, 10)
            review.rating.cleanliness = random.uniform(0, 10)
            review.rating.overall = random.uniform(0, 10)
            reviews.append(review)
        return reviews

    def build_public_likes(self, length):
        return [random.choice(_public_likes_pool) for _ in range(length)]

    def _review_to_dict(self, review):
        """Convert a Review object to a dictionary."""
        return {
            "date": review.date,
            "author": review.author,
            "rating": {
                "value": review.rating.value,
                "cleanliness": review.rating.cleanliness,
                "overall": review.rating.overall
            }
        }

    def to_dict(self):
        """Convert Hotel object to a dictionary, excluding non-serializable attributes."""
        doc = {
            "characters_with_spaces": self.characters_with_spaces,
            "characters_without_spaces": self.characters_without_spaces,
            "document_size": self.document_size,
            "address": self.address,
            "free_parking": self.free_parking,
            "city": self.city,
            "type": self.type,
            "url": self.url,
            "phone": self.phone,
            "price": self.price,
            "avg_rating": self.avg_rating,
            "free_breakfast": self.free_breakfast,
            "name": self.name,
            "email": self.email,
            "mutated": self.mutated,
            "padding": self.padding,
            "public_likes": self.public_likes
        }

        if hasattr(self, "country"):
            doc["country"] = self.country

        if hasattr(self, "reviews"):
            doc["reviews"] = [self._review_to_dict(
                review) for review in self.reviews]

        return doc

    def generate_document(self, document_size, country_type="string", include_country=None):
        self.document_size = document_size
        if include_country == str('mixed'):
            include = random.choice([True, False])
            if include:
                if country_type == str('mixed'):
                    self.country_selector(random.choice(["string", "int"]))
                else:
                    self.country_selector(country_type)
        elif include_country:
            if country_type == str('mixed'):
                self.country_selector(random.choice(["string", "int"]))
            else:
                self.country_selector(country_type)

        try:
            if hasattr(self, "public_likes"):
                self.public_likes = self.build_public_likes(
                    random.randint(1, 10))
        except Exception as err:
            error = str(err)

        while True:
            document = self.to_dict()
            document_json = json.dumps(document, ensure_ascii=False)
            document_size_bytes = len(document_json.encode("utf-8"))

            new_reviews = self.build_review(random.randint(1, 10))
            new_reviews_dict = [self._review_to_dict(
                review) for review in new_reviews]
            new_reviews_json = json.dumps(new_reviews_dict, ensure_ascii=False)
            new_reviews_size_bytes = len(new_reviews_json.encode("utf-8"))

            if hasattr(self, "reviews") and document_size_bytes + new_reviews_size_bytes <= document_size:
                self.reviews.extend(new_reviews)
            else:
                required_length = document_size - document_size_bytes
                if required_length > 0:
                    self.padding = ''.join(random.choice(
                        string.ascii_letters) for _ in range(required_length))
                break


if __name__ == "__main__":
    res = []
    for i in range(1000):
        hotel = Hotel()
        hotel.generate_document(document_size=1024, include_country=True)
        doc = hotel.to_dict()
        res.append(doc)
    with open("hotel.json", "w") as f:
        json.dump(res, f, indent=4, ensure_ascii=False)
