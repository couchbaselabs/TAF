# -*- coding: utf-8 -*-
# Generic/Built-in
import base64
import hmac
import hashlib
import datetime
import os
from requests.auth import AuthBase

# Other Libs

# Owned
from .CapellaExceptions import *
import time


class CapellaAPIAuth(AuthBase):
    # Extends requests AuthBase for
    # Couchbase Cloud API Authentication Handler.

    def __init__(self, secret, access):
        # Create an authentication handler for Couchbase Cloud APIs
        # :param str access_key: access key for Couchbase Cloud
        # :param str secret_key: secret key for Couchbase Cloud

        self.ACCESS_KEY = access
        self.SECRET_KEY = secret

    def __call__(self, r):
        # This is the endpoint being called
        # Split out from the entire URL
        endpoint = r.url.split(".com", 1)[-1]

        # The method being used
        method = r.method

        # Epoch time in milliseconds
        cbc_api_now = int(time.time() * 1000)

        # Form the message string for the Hmac hash
        cbc_api_message = method + '\n' + endpoint + '\n' + str(cbc_api_now)

        # Calculate the hmac hash value with secret key and message
        cbc_api_signature = base64.b64encode(
            hmac.new(bytes(self.SECRET_KEY),
                     bytes(cbc_api_message),
                     digestmod=hashlib.sha256).digest())

        # Values for the header
        cbc_api_request_headers = {
           'Authorization': 'Bearer ' + self.ACCESS_KEY + ':' + cbc_api_signature.decode(),
           'Couchbase-Timestamp': str(cbc_api_now),
           'Content-Type': 'application/json'
        }
        # Add our key:values to the request header
        r.headers.update(cbc_api_request_headers)

        # Return the request back
        return r

