import datetime

COMMON_KEY = "user_"
GENDER = ["M", "F", "T"]
START_DATE = datetime.date(1940, 1, 1)
END_DATE = datetime.date(2001, 12, 31)
DAYS_BETWEEN_DATES_INT = ((END_DATE - START_DATE).days * 24 * 60 * 60) \
                         + (END_DATE - START_DATE).seconds
COUNTRY = ["US", "CAN", "MEX", "UK", "IRE", "AGN", "IND", "SL", "AUS",
           "PAK", "AFG", "NZ", "SPN", "ITA", "JPN", "CHN"]

FIRST_NAMES = dict()
FIRST_NAMES["M"] = ["James", "John", "Robert", "Michael", "William",
                    "Matt", "Russell", "Vincent", "Dave", "Philip",
                    "David", "Richard", "Joseph", "Thomas", "Charles",
                    "Daniel", "Matthew", "Donald", "Mark", "Paul", "Steven",
                    "Andrew", "Kevin", "Brian", "Gary", "Eric", "Scott"]
FIRST_NAMES["F"] = ["Mary", "Patricia", "Jennifer", "Linda", "Elizabeth",
                    "Barbara", "Susan", "Jessica", "Sarah", "Karen",
                    "Nancy", "Lisa", "Margaret", "Rose", "Sophia",
                    "Natalie", "Diana", "Marie", "Judy", "Julia", "Sara",
                    "Alice", "Kathryn", "Andrea", "Megan", "Kelly", "Julie"]
LAST_NAMES = ["Smith", "Johnson", "Brown", "Taylor", "Anderson", "Cook",
              "Miller", "Garcia", "Moore", "Lee", "Clark", "Walker", "Hall"]
