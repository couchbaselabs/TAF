# -*- coding: utf-8 -*-
"""
Lighthouse Portal configuration object.
Holds all connection details for the UCP portal instance.
Separate from TestInputServer because the portal is NOT a
Couchbase Server node - it has its own port, credentials,
and service-specific settings.
"""
from testconstants import (
    LIGHTHOUSE_PORTAL_PORT,
    LIGHTHOUSE_PORTAL_USERNAME,
    LIGHTHOUSE_PORTAL_PASSWORD,
)

class LighthousePortal(object):
    """
    Configuration object for a Lighthouse/UCP portal instance.
    Created from ini [LHPortal] server + test params.
    Passed to UnifiedControlPlaneClient for connection.
    """
    def __init__(self, ip, port=LIGHTHOUSE_PORTAL_PORT,
                 username=LIGHTHOUSE_PORTAL_USERNAME,
                 password=LIGHTHOUSE_PORTAL_PASSWORD):
        """
        Args:
            ip: Portal IP address
            port: UCP API port (NOT Couchbase 8091)
            username: Portal admin username
            password: Portal admin password
        """
        self.ip = ip
        self.port = port
        self.username = username
        self.password = password
    @staticmethod
    def from_server_and_params(server, test_input):
        """
        Build a LighthousePortal from a TestInputServer object
        and test params.
        The IP comes from the server (ini [LHPortal] section).
        Port, username, password can be overridden via test params:
            - ucp_port (default: 443)
            - ucp_username (default: testconstants.LIGHTHOUSE_PORTAL_USERNAME)
            - ucp_password (default: testconstants.LIGHTHOUSE_PORTAL_PASSWORD)
        Args:
            server: TestInputServer from input.lh_portal
            test_input: TestInput object for reading params
        Returns:
            LighthousePortal instance
        """
        ip = server.ip
        port = test_input.param("ucp_port", LIGHTHOUSE_PORTAL_PORT)
        username = test_input.param("ucp_username",
                                    LIGHTHOUSE_PORTAL_USERNAME)
        password = test_input.param("ucp_password",
                                    LIGHTHOUSE_PORTAL_PASSWORD)
        return LighthousePortal(ip=ip, port=port,
                                username=username, password=password)
    def __str__(self):
        return "LighthousePortal(ip:%s, port:%s, user:%s)" % (
            self.ip, self.port, self.username)
    def __repr__(self):
        return self.__str__()
