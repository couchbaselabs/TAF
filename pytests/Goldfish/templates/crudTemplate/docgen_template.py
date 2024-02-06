import json
import random
import string
from java.util import Date
from java.lang import StringBuilder


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
        self.date = str(Date())
        self.author = self.generate_author()
        self.rating = Rating()

    def generate_author(self):
        author_builder = StringBuilder()
        author_builder.append(''.join(random.choice(string.ascii_letters + ' ') for _ in range(random.randint(10, 20))))
        return str(author_builder)


class Hotel:
    """
    Stores Hotel information to generate document for the doc_loader
    """

    def __init__(self):
        self.characters_with_spaces = string.ascii_letters + string.digits + ' '
        self.characters_without_spaces = string.ascii_letters + string.digits
        self.document_size = None
        self.address = ''.join(random.choice(self.characters_with_spaces) for _ in range(random.randint(30, 100)))
        self.free_parking = int(random.choice([True, False]))
        self.city = ''.join(random.choice(self.characters_with_spaces) for _ in range(random.randint(5, 20)))
        self.type = "Hotel"
        username = ''.join(random.choice(self.characters_without_spaces) for _ in range(random.randint(10, 100)))
        domain = ''.join(random.choice(self.characters_without_spaces) for _ in range(random.randint(5, 10)))
        self.url = "www.{0}.{1}.com".format(username, domain)
        self.reviews = self.build_review(random.randint(0, 10))
        self.phone = random.randint(1000, 9999999)
        self.price = random.choice([1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0,
                                    7000.0, 8000.0, 9000.0, 10000.0])
        self.avg_rating = random.uniform(0, 1)
        self.free_breakfast = int(random.choice([True, False]))
        self.name = ''.join(random.choice(self.characters_with_spaces) for _ in range(random.randint(5, 20)))
        self.public_likes = self.build_public_likes(random.randint(0, 10))
        self.email = "{0}@{1}.com".format(username, domain)
        self.mutated = 0.0
        self.padding = ""

    def country_selector(self, field_type="string"):
        country_names = ["Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Anguilla", "Antigua &amp; Barbuda",
                         "Argentina",
                         "Armenia", "Aruba", "Australia", "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh",
                         "Barbados",
                         "Belarus", "Belgium", "Belize", "Benin", "Bermuda", "Bhutan", "Bolivia",
                         "Bosnia &amp; Herzegovina",
                         "Botswana", "Brazil", "British Virgin Islands", "Brunei", "Bulgaria", "Burkina Faso",
                         "Burundi", "Cambodia",
                         "Cameroon", "Cape Verde", "Cayman Islands", "Chad", "Chile", "China", "Colombia", "Congo",
                         "Cook Islands",
                         "Costa Rica", "Cote D Ivoire", "Croatia", "Cruise Ship", "Cuba", "Cyprus", "Czech Republic",
                         "Denmark",
                         "Djibouti", "Dominica", "Dominican Republic", "Ecuador", "Egypt", "El Salvador",
                         "Equatorial Guinea",
                         "Estonia", "Ethiopia", "Falkland Islands", "Faroe Islands", "Fiji", "Finland", "France",
                         "French Polynesia",
                         "French West Indies", "Gabon", "Gambia", "Georgia", "Germany", "Ghana", "Gibraltar", "Greece",
                         "Greenland",
                         "Grenada", "Guam", "Guatemala", "Guernsey", "Guinea", "Guinea Bissau", "Guyana", "Haiti",
                         "Honduras",
                         "Hong Kong", "Hungary", "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland",
                         "Isle of Man", "Israel",
                         "Italy", "Jamaica", "Japan", "Jersey", "Jordan", "Kazakhstan", "Kenya", "Kuwait",
                         "Kyrgyz Republic", "Laos",
                         "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein", "Lithuania", "Luxembourg",
                         "Macau",
                         "Macedonia", "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Mauritania",
                         "Mauritius",
                         "Mexico", "Moldova", "Monaco", "Mongolia", "Montenegro", "Montserrat", "Morocco", "Mozambique",
                         "Namibia",
                         "Nepal", "Netherlands", "Netherlands Antilles", "New Caledonia", "New Zealand", "Nicaragua",
                         "Niger",
                         "Nigeria", "Norway", "Oman", "Pakistan", "Palestine", "Panama", "Papua New Guinea", "Paraguay",
                         "Peru",
                         "Philippines", "Poland", "Portugal", "Puerto Rico", "Qatar", "Reunion", "Romania", "Russia",
                         "Rwanda",
                         "Saint Pierre &amp; Miquelon", "Samoa", "San Marino", "Satellite", "Saudi Arabia", "Senegal",
                         "Serbia",
                         "Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia", "South Africa",
                         "South Korea", "Spain",
                         "Sri Lanka", "St Kitts &amp; Nevis", "St Lucia", "St Vincent", "St. Lucia", "Sudan",
                         "Suriname", "Swaziland",
                         "Sweden", "Switzerland", "Syria", "Taiwan", "Tajikistan", "Tanzania", "Thailand",
                         "Timor L'Este", "Togo",
                         "Tonga", "Trinidad &amp; Tobago", "Tunisia", "Turkey", "Turkmenistan", "Turks &amp; Caicos",
                         "Uganda",
                         "Ukraine", "United Arab Emirates", "United Kingdom", "Uruguay", "Uzbekistan", "Venezuela",
                         "Vietnam",
                         "Virgin Islands (US)", "Yemen", "Zambia", "Zimbabwe"]
        if field_type == "string":
            self.country = random.choice(country_names)
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
        return [''.join(random.choice(self.characters_with_spaces) for _ in range(random.randint(10, 20)))
                for _ in range(length)]

    def __to_json(self, obj):
        return json.dumps(obj, default=lambda x: x.__dict__, ensure_ascii=False)

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
            self.build_public_likes(random.randint(1, 10))
        except Exception as err:
            error = str(err)

        while True:
            new_reviews = self.build_review(random.randint(1, 10))
            document = self.__to_json(self.__dict__)
            new_review_doc = self.__to_json([review.__dict__ for review in new_reviews])
            if len(document.encode("utf-8")) + len(new_review_doc.encode("utf-8")) <= document_size:
                self.reviews.extend(new_reviews)
            else:
                required_length = document_size - len(document.encode("utf-8"))
                self.padding = ''.join(random.choice(string.ascii_letters) for _ in range(required_length))
                break
