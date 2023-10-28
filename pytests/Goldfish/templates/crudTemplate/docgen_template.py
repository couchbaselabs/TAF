"""
Document template for the docloader
"""
import json
import random
import string
from datetime import datetime, timedelta


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
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2023, 12, 31)
        self.date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        self.author = ''.join(
            random.choice(string.ascii_letters + string.whitespace) for _ in range(random.randint(10, 20)))
        self.rating = Rating()


class Hotel:
    """
    Stores Hotel information to generate document for the doc_loader
    """

    def __init__(self):
        self.characters_with_spaces = string.ascii_letters + string.digits + string.whitespace
        self.characters_without_spaces = string.ascii_letters + string.digits
        self.document_size = None
        self.country = ''.join(random.choice(self.characters_with_spaces) for _ in range(random.randint(10, 100)))
        self.address = ''.join(random.choice(self.characters_with_spaces) for _ in range(random.randint(30, 100)))
        self.free_parking = int(random.choice([True, False]))
        self.city = ''.join(random.choice(self.characters_with_spaces) for _ in range(random.randint(5, 20)))
        self.type = "Hotel"
        self.url = "www." + ''.join(
            random.choice(self.characters_without_spaces) for _ in range(random.randint(10, 100))) + ".com"
        self.reviews = []
        self.phone = int(''.join([str(random.randint(0, 9)) for _ in range(10)]))
        self.price = random.choice([1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0,
                                    7000.0, 8000.0, 9000.0, 10000.0])
        self.avg_ratings = random.uniform(0, 10)
        self.free_breakfast = int(random.choice([True, False]))
        self.name = ''.join(random.choice(self.characters_with_spaces) for _ in range(random.randint(5, 20)))
        self.public_likes = []
        self.email = ''.join(random.choice(self.characters_without_spaces) for _ in range(random.randint(10, 100)))
        + "@" + ''.join(random.choice(self.characters_without_spaces) for _ in range(random.randint(5, 10))) + ".com"
        self.mutated = 0.0
        self.padding = ""

    def generate_review(self):
        """
        Generated random Review for a hotel
        :return: object of class Review
        """
        review = Review()
        review.rating.value = random.uniform(0, 10)
        review.rating.cleanliness = random.uniform(0, 10)
        review.rating.overall = random.uniform(0, 10)
        return review

    def generate_public_likes(self):
        """
        Generated random value for variable self.PublicLikes
        """
        num_likes = random.randint(1, 10)
        self.public_likes = [''.join(random.choice(self.characters_with_spaces) for _ in range(random.randint(10, 20)))
                             for _ in range(num_likes)]

    def generate_document(self, document_size):
        """
        Generates document os a given size in bytes.
        :param document_size:
        """
        self.reviews = []
        self.document_size = document_size
        self.generate_public_likes()
        while True:
            new_review = self.generate_review()
            document = json.dumps(self.__dict__, default=lambda x: x.__dict__,
                                  ensure_ascii=False)
            new_review_doc = json.dumps(new_review.__dict__, default=lambda x: x.__dict__,
                                        ensure_ascii=False)
            if len(document.encode("utf-8")) + len(new_review_doc.encode("utf-8")) <= document_size:
                self.reviews.append(new_review)
            else:
                required_length = document_size - len(document.encode("utf-8"))
                self.padding = str(''.join(random.choice(string.ascii_letters) for _ in range(required_length)))
                break
