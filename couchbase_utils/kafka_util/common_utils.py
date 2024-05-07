"""
Created on 3-May-2024

@author: Umang Agrawal

This utility is for making rest calls.
"""

import requests
import base64
from requests.auth import AuthBase
import logging
from capellaAPI.capella.lib.CapellaExceptions import (
    CbcAPIError, MissingAccessKeyError, MissingSecretKeyError,
    GenericHTTPError)


class KafkaCluster(object):

    def __init__(self):
        # Topic Details
        self.topic_prefix = None
        self.topics = list()
        self.internal_topics = ["connect-configs", "connect-offsets",
                                "connect-status"]
        # Dead letter queue topic name
        self.dlq_topic = None

        # Connector Info
        self.connectors = {}


class APIAuth(AuthBase):
    # Extends requests AuthBase for API Authentication Handler.

    def __init__(self, access, secret):
        # Create an authentication handler for API calls
        # :param str access_key: access key for API calls
        # :param str secret_key: secret key for API calls

        self.ACCESS_KEY = access
        self.SECRET_KEY = secret

    def __call__(self, r):
        # Values for the header
        base64_encoded_auth = base64.b64encode("{0}:{1}".format(
            self.ACCESS_KEY, self.SECRET_KEY))
        api_request_headers = {
            'Authorization': 'Basic ' + base64_encoded_auth,
            'Content-Type': 'application/json'
        }
        # Add our key:values to the request header
        r.headers.update(api_request_headers)

        # Return the request back
        return r


class APIRequests(object):

    def __init__(self, access=None, secret=None):
        # handles http requests - GET , PUT, POST, DELETE
        self.SECRET = secret
        self.ACCESS = access

        self._log = logging.getLogger(__name__)

        # We will re-use the first session we setup to avoid
        # the overhead of creating new sessions for each request
        self.network_session = requests.Session()

    def api_get(self, url, params=None, headers=None):
        api_response = None
        self._log.info(url)

        try:
            api_response = self.network_session.get(
                url,
                auth=APIAuth(self.ACCESS, self.SECRET),
                params=params, verify=False, headers=headers)
            self._log.info(api_response.content)

        except requests.exceptions.HTTPError:
            raise GenericHTTPError(api_response.json())

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

        return api_response

    def api_post(self, url, request_body, headers=None):
        api_response = None

        self._log.info(url)
        self._log.debug("Request body: " + str(request_body))

        try:
            api_response = self.network_session.post(
                url, json=request_body,
                auth=APIAuth(self.ACCESS, self.SECRET),
                verify=False, headers=headers)
            self._log.debug(api_response.content)

        except requests.exceptions.HTTPError:
            raise GenericHTTPError(api_response.json())

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        # Grab any other exception and send to our generic exception
        # handler
        except Exception as e:
            raise CbcAPIError(e)

        return api_response

    def api_put(self, url, request_body, headers=None):
        api_response = None

        self._log.info(url)
        self._log.debug("Request body: " + str(request_body))

        try:
            api_response = self.network_session.put(
                url, json=request_body,
                auth=APIAuth(self.ACCESS, self.SECRET),
                verify=False, headers=headers)
            self._log.debug(api_response.content)

        except requests.exceptions.HTTPError:
            raise GenericHTTPError(api_response.json())

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        return api_response

    def api_patch(self, url, request_body, headers=None):
        api_response = None

        self._log.info(url)
        self._log.debug("Request body: " + str(request_body))

        try:
            api_response = self.network_session.patch(
                url, json=request_body,
                auth=APIAuth(self.ACCESS, self.SECRET),
                verify=False, headers=headers)
            self._log.debug(api_response.content)

        except requests.exceptions.HTTPError:
            raise GenericHTTPError(api_response.json())

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        return api_response

    def api_del(self, url, request_body=None, params=None, headers=None):
        api_response = None

        self._log.info(url)
        self._log.debug("Request body: " + str(request_body))

        try:
            if request_body:
                api_response = self.network_session.delete(
                    url, json=request_body,
                    auth=APIAuth(self.ACCESS, self.SECRET),
                    verify=False, headers=headers)
            elif params:
                api_response = self.network_session.delete(
                    url, params=params,
                    auth=APIAuth(self.ACCESS, self.SECRET),
                    verify=False, headers=headers)
            else:
                api_response = self.network_session.delete(
                    url,
                    auth=APIAuth(self.ACCESS, self.SECRET),
                    verify=False, headers=headers)

            self._log.debug(api_response.content)

        except requests.exceptions.HTTPError:
            raise GenericHTTPError(api_response.json())

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        # Grab any other exception and send to our generic exception
        # handler
        except Exception as e:
            raise CbcAPIError(e)

        return api_response