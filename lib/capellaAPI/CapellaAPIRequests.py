# -*- coding: utf-8 -*-
# Generic/Built-in
import requests
import logging
import pprint


from .CapellaAPIAuth import CapellaAPIAuth
from .CapellaExceptions import MissingAccessKeyError, \
    MissingSecretKeyError, GenericHTTPError, CbcAPIError


class CapellaAPIRequests(object):

    def __init__(self, url, secret, access):
        # handles http requests - GET , PUT, POST, DELETE
        # to the Couchbase Cloud APIs
        # Read the values from the environmental variables
        self.API_BASE_URL = url
        self.SCERET = secret
        self.ACCESS = access

        self._log = logging.getLogger(__name__)

        # We will re-use the first session we setup to avoid
        # the overhead of creating new sessions for each request
        self.network_session = requests.Session()

    def set_logging_level(self, level):
        self._log.setLevel(level)

    # Methods
    def capella_api_get(self, api_endpoint):
        cbc_api_response = None

        self._log.info(api_endpoint)

        try:
            cbc_api_response = self.network_session.get(
                self.API_BASE_URL + api_endpoint,
                auth=CapellaAPIAuth(self.SCERET, self.ACCESS),
                verify=False)
            self._log.debug(cbc_api_response.content)

        except requests.exceptions.HTTPError:
            error = pprint.pformat(cbc_api_response.json())
            raise GenericHTTPError(error)

        except MissingAccessKeyError:
            self._log.debug("Missing Access Key environment variable")
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            self._log.debug("Missing Access Key environment variable")
            print("Missing Access Key environment variable")

        # Grab any other exception and send to our generic exception
        # handler
        except Exception as e:
            raise CbcAPIError(e)

        return (cbc_api_response)

    def capella_api_post(self, api_endpoint, request_body):
        cbc_api_response = None

        self._log.info(api_endpoint)
        self._log.debug("Request body: " + str(request_body))

        try:
            cbc_api_response = self.network_session.post(
                self.API_BASE_URL + api_endpoint,
                json=request_body,
                auth=CapellaAPIAuth(self.SCERET, self.ACCESS),
                verify=False)
            self._log.debug(cbc_api_response.content)

        except requests.exceptions.HTTPError:
            error = pprint.pformat(cbc_api_response.json())
            raise GenericHTTPError(error)

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        # Grab any other exception and send to our generic exception
        # handler
        except Exception as e:
            raise CbcAPIError(e)

        return (cbc_api_response)

    def capella_api_put(self, api_endpoint, request_body):
        cbc_api_response = None

        self._log.info(api_endpoint)
        self._log.debug("Request body: " + str(request_body))

        try:
            cbc_api_response = self.network_session.put(
                self.API_BASE_URL + api_endpoint,
                json=request_body,
                auth=CapellaAPIAuth(self.SCERET, self.ACCESS),
                verify=False)
            self._log.debug(cbc_api_response.content)

        except requests.exceptions.HTTPError:
            error = pprint.pformat(cbc_api_response.json())
            raise GenericHTTPError(error)

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        return (cbc_api_response)

    def capella_api_del(self, api_endpoint, request_body=None):
        cbc_api_response = None

        self._log.info(api_endpoint)
        self._log.debug("Request body: " + str(request_body))

        try:
            if request_body is None:
                cbc_api_response = self.network_session.delete(
                    self.API_BASE_URL + api_endpoint,
                    auth=CapellaAPIAuth(self.SCERET, self.ACCESS),
                    verify=False)
            else:
                cbc_api_response = self.network_session.delete(
                    self.API_BASE_URL + api_endpoint,
                    json=request_body,
                    auth=CapellaAPIAuth(self.SCERET, self.ACCESS),
                    verify=False)

            self._log.debug(cbc_api_response.content)

        except requests.exceptions.HTTPError:
            error = pprint.pformat(cbc_api_response.json())
            raise GenericHTTPError(error)

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        # Grab any other exception and send to our generic exception
        # handler
        except Exception as e:
            raise CbcAPIError(e)

        return (cbc_api_response)

    def _urllib_request(self, api, method='GET', headers=None,
                        params='', timeout=300, verify=False):
        session = requests.Session()
        try:
            if method == "GET":
                resp = session.get(api, params=params, headers=headers,
                                   timeout=timeout, verify=verify)
            elif method == "POST":
                resp = session.post(api, data=params, headers=headers,
                                    timeout=timeout, verify=verify)
            elif method == "DELETE":
                resp = session.delete(api, data=params, headers=headers,
                                      timeout=timeout, verify=verify)
            elif method == "PUT":
                resp = session.put(api, data=params, headers=headers,
                                   timeout=timeout, verify=verify)
            return resp
        except requests.exceptions.HTTPError as errh:
            self._log.error("HTTP Error {0}".format(errh))
        except requests.exceptions.ConnectionError as errc:
            self._log.error("Error Connecting {0}".format(errc))
        except requests.exceptions.Timeout as errt:
            self._log.error("Timeout Error: {0}".format(errt))
        except requests.exceptions.RequestException as err:
            self._log.error("Something else: {0}".format(err))
