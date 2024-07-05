import json
import requests


def get_params(url):
    """
    Get parameters from the jenkins job
    :param url: The jenkins job URL
    :type url: str
    :return: Dictionary of job parameters
    :rtype: dict
    """
    res = get_js(url, params="tree=actions[parameters[*]]")
    parameters = {}
    if not res:
        print("Error: could not get parameters")
        return None
    for vals in res['actions']:
        if "parameters" in vals:
            for params in vals['parameters']:
                if "value" in params:
                    parameters[params['name']] = params['value']
            break
    return parameters


def get_js(url, params=None):
    """
    Get the parameters from Jenkins job using Jenkins rest api
    :param url: The jenkins job URL
    :type url: str
    :param params: Parameters to be passed to the json/api
    :type params: str
    :return: Response from the rest api
    :rtype: dict
    """
    try:
        url = url.rstrip("/")
        if params:
            full_url = '{0}/api/json?{1}'.format(url, params)
        else:
            full_url = '{0}/api/json'.format(url)
        return requests.get(full_url).json()
    except:
        print("Error: url unreachable: %s" % url)
    return None


def download_url_data(url, json_api=False, params=None):
    """
    Download the data from the given url and with given parameters
    from the jenkins job
    :param url: Jenkins job url
    :type url: str
    :param params: Parameters to be passed to the api
    :type params: str
    :return: Content of the request to the jenkins api
    :rtype: requests.content
    """
    try:
        url = url.rstrip("/")
        if json_api:
            if params:
                full_url = '{0}/api/json?{1}'.format(url, params)
            else:
                full_url = '{0}/api/json'.format(url)
        else:
            full_url = url
        return requests.get(full_url).text
    except Exception as e:
        print("[Error] url unreachable: %s" % url)
        print(e)
    return None
