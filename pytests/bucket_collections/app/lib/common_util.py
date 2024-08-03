from datetime import datetime, timedelta
from random import choice, randrange

from bucket_collections.app.constants import global_vars


def get_all_scenarios(target_class):
    scenarios = dict()
    for scenario_name in [func for func in dir(target_class)
                          if callable(getattr(target_class, func))
                          and str(func).startswith("scenario")]:
        scenarios[scenario_name] = getattr(target_class, scenario_name)
    return scenarios


def get_random_scenario(target_class):
    return choice(list(target_class.scenarios.keys()))


def get_random_reservation_date():
    date_format = "%Y-%m-%d"
    start = datetime.strptime(global_vars.app_current_date, date_format)
    end = start + timedelta(days=180)
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return (start + timedelta(seconds=random_second)).strftime(date_format)
