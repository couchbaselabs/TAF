import base64
import json
import os
import re
import requests

import xml.etree.ElementTree as et
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from java.security import KeyPairGenerator, KeyFactory, Signature
from java.security.spec import PKCS8EncodedKeySpec, RSAPublicKeySpec

IDPMetadataTemplate = """
<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" validUntil="2023-08-30T16:40:01Z" cacheDuration="PT1661876151S" entityID="http://capella.test/idp">
    <md:IDPSSODescriptor WantAuthnRequestsSigned="false" protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
        <md:KeyDescriptor use="signing">
            <ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
                <ds:X509Data>
                    <ds:X509Certificate>{}</ds:X509Certificate>
                </ds:X509Data>
            </ds:KeyInfo>
        </md:KeyDescriptor>
        <md:KeyDescriptor use="encryption">
            <ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
                <ds:X509Data>
                    <ds:X509Certificate>{}</ds:X509Certificate>
                </ds:X509Data>
            </ds:KeyInfo>
        </md:KeyDescriptor>
        <md:SingleLogoutService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="http://capella.test/idp/logout"/>
        <md:NameIDFormat>urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress</md:NameIDFormat>
        <md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="http://capella.test/idp"/>
    </md:IDPSSODescriptor>
</md:EntityDescriptor>
"""


class SSOComponents:
    def __init__(self, capella, url):
        # Pre-flight checks
        assert (type(capella) == CapellaAPI)
        assert (type(url) == str)

        # Assignment
        self.capella = capella
        self.internal_url = url.replace("cloud", "", 1)
        self._generate_key_pair()
        self._generate_ssigned_cert()

    def _generate_key_pair(self):
        self.key = """-----BEGIN PRIVATE KEY-----
        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
        WCl1NZaMgGUaWgdmKha5z36DUVDu6Vo=
        -----END PRIVATE KEY-----
        """

        k = self.key.replace(
            "-----BEGIN PRIVATE KEY-----", ""
        ).replace(
            os.linesep, ""
        ).replace(
            "-----END PRIVATE KEY-----", ""
        )

        encoded = base64.b64decode(k)

        kf = KeyFactory.getInstance("RSA")
        ks = PKCS8EncodedKeySpec(encoded)
        self.private = kf.generatePrivate(ks)

        ks = RSAPublicKeySpec(self.private.getModulus(), self.private.getPublicExponent())
        self.public = kf.generatePublic(ks)

    def sign(self, input_text):
        h = base64.b64decode(input_text)
        k = self.key.replace("-----BEGIN PRIVATE KEY-----", "").replace(os.linesep, "").replace(
            "-----END PRIVATE KEY-----", "")
        encoded = base64.b64decode(k)

        kf = KeyFactory.getInstance("RSA")
        ks = PKCS8EncodedKeySpec(encoded)

        s = Signature.getInstance("NONEwithRSA")
        s.initSign(kf.generatePrivate(ks))
        s.update(h)
        return base64.b64encode(s.sign())

    def _generate_ssigned_cert(self):
        self.cert = """MIIFkjCCA3oCCQDb09WUdyEHqDANBgkqhkiG9w0BAQsFADCBijELMAkGA1UEBhMC
VVMxCzAJBgNVBAgMAkNBMRQwEgYDVQQHDAtTYW50YSBDbGFyYTEYMBYGA1UECgwP
Q291Y2hiYXNlLCBJbmMuMRkwFwYDVQQLDBBTZWN1cml0eSBUZXN0aW5nMSMwIQYD
VQQDDBpDb3VjaGJhc2UgU2VjdXJpdHkgVGVzdGluZzAeFw0yMjA4MzAxNjQwMDFa
Fw0yMzA4MzAxNjQwMDFaMIGKMQswCQYDVQQGEwJVUzELMAkGA1UECAwCQ0ExFDAS
BgNVBAcMC1NhbnRhIENsYXJhMRgwFgYDVQQKDA9Db3VjaGJhc2UsIEluYy4xGTAX
BgNVBAsMEFNlY3VyaXR5IFRlc3RpbmcxIzAhBgNVBAMMGkNvdWNoYmFzZSBTZWN1
cml0eSBUZXN0aW5nMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAvkOX
BdeU6RycFvvle4+iNSj13ih7Ch7e7ysDLHTiz9sIDVoQ3hXERnci9ZY6AQyx8g5U
Z7uJb6H1hi9yy3fk7OKGax9Path8xxmxjnHYlb5qUhVAv4Bq/+Krv3rNEuFTStPA
O5ZKr2kzxX1ljxps/e3CSmLK13BpN6b2fFQdJRKrTC1pzc1PwFqy7mPXe5a0tYfw
SMIaskdWHLdn7sPC2B0GC/NP1XQL8r6lh+40IGYF7TuM921PJeULtEBVJcQplWBw
JVjP93WhIwoft3vsvDC6eEZc1UKKAi1sq2mE0XzR5rfeQXBo5VhzGnvOQ16BTokW
LQx9AV/HLynyHoGBCOh8Tv2VkUDMJ32WPlfeTZzNrwfIMlrief+r5/nkP2LYAoEu
xizzEmGm0+Ixccin6f4Aj3qMs9ccz+OItfrMrbusmUqGUbxf/eIIE24eFmQZh3lG
b+p1yWZ6kXgeXxjCayShMYkO21qo4w/eecRRn2+jkmVY/ebkM6CJyYNTUUrhrZpL
RXXNBnJ4yUmkS3mY+/ZOxjvahdXj+0GxQ3ukjjXVEZMAQcmuADQOaVBd2h74xnlP
rtEzQwcKBN0vItEcm771VXRIP74XuVLdFBCW26b4G9wuLkIXMvW7yxhRvtQ8D2jG
EQkI3K7uh5QAQnJbYKKTBK3BsiA1TRUnaXxpvIcCAwEAATANBgkqhkiG9w0BAQsF
AAOCAgEAUTLZzH+BasJO2zJ27rotSlAk9PZCnksJSn1kibz/mNy+8NPCRU/DvMEx
pvkwINAaLLKwcDozlkqRvs+FT2A0ATBcSswEoS7JQ3oVVCVWE+FGslndBwSU1LNW
d5IOoclLG6TmU1j0rRwEdvA7fjmZKL+apaaBbBhvw9Xx2vlVCOLIIa6i7CLwB+Fe
oG5bYlo852Cq9lAkZ4kGnQr8fKd019VX90y2E52D1zZPQFtmiwX2ca76ZK0mD0MK
i82ZZkN3U2GdfU3+r2no9beQhzr4Xwyvs6+XYVsFCFI/xNdp27XvBtoqQTg455sZ
nTQz0sHobOjGiJUWiIuZmg7/Q30dRBSCXQhrppGoazKvlpiYBN9M2YJzXoL8zZZa
qqWgY54nr2c51xDgHeouYepdAiqtY0fQJZ01d6I+ClNNwhIt2oFstqSCPVbZwDuY
t7OZppESXsnWUEIX8NxOf8BIidhayogk15JbBtL/Ixxs4lwVHUlXaQFPAReaIjaC
dSy803llcD39heRATXhhsC57xLiRATQMqToi0O2DWbSf5g+tNEVtgf/4r8F5a0bH
7gGbg6AL4h8RBnFW6KGuNBaNog45FO003l2F0PvK8ZxPFxkxWEsRXg/Y17hTL0PS
tnJTX7zMIfz13aSjcZ3YD7WJsK7rBakRKLXcYz/49i4kN27rID4=
"""

    def get_public(self):
        return base64.b64encode(self.public.getEncoded())

    def get_private(self):
        return base64.b64encode(self.private.getEncoded())

    def get_certificate(self):
        # This certificate is only valid until 08/30/2023
        return self.cert.replace(os.linesep, "").replace(r'\s', "")

    def get_teams(self, tenant_id):
        assert type(tenant_id) == str

        url = "{}/v2/organizations/{}/teams?page=1&perPage=1".format(self.internal_url, tenant_id)

        return self.capella.do_internal_request(url, method="GET")

    def create_realm(self, tenant_id, team_id):
        assert type(tenant_id) == str

        url = "{}/v2/organizations/{}/realms".format(self.internal_url, tenant_id)
        request_body = {
            'connectionOptionsSAML': {
                'metadataXML': IDPMetadataTemplate.format(self.get_certificate(), self.get_certificate())
            },
            'standard': 'SAML 2.0',
            'vendor': 'Okta',
            'defaultTeamId': team_id
        }

        return self.capella.do_internal_request(url, method="POST", params=json.dumps(request_body))

    def delete_realm(self, tenant_id, realm_id):
        assert type(tenant_id) == str

        url = "{}/v2/organizations/{}/realms/{}".format(self.internal_url, tenant_id, realm_id)

        return self.capella.do_internal_request(url, method="DELETE", params="{}")

    def get_realm_by_name(self, tenant_id, realm_name):
        assert type(tenant_id) == str

        url = "{}/v2/organizations/{}/realms".format(self.internal_url, tenant_id)
        realms = self.capella.do_internal_request(url, method="GET")
        if realms.status_code // 100 != 2:
            return None

        realms = json.loads(realms.content)
        if len(realms['data']) == 0:
            return None

        for realm in realms['data']:
            if realm['data']['name'] == realm_name:
                return realm['data']

        return None

    def initiate_idp_login(self, realm_name):
        url = "{}/v2/auth/sso-login/{}".format(self.internal_url, realm_name)

        return self.capella.do_internal_request(url, method="GET")

    @staticmethod
    def get_saml_request(self, login_url):
        # We can just return the output from this.
        return requests.get(login_url)

    @staticmethod
    def parse_saml_request(self, html):
        # Here we're going to try and grab all of the input elements and convert
        # them into a dict so that we can work better with them
        result = re.findall(r"<input\s+type=\"hidden\"\s+name=\"([a-zA-Z]+)\"\s+value=\"([a-zA-Z0-9+=_-]+)\"",
                            html.replace(os.linesep, ""))

        form_inputs = dict()

        for r in result:
            if len(r) == 2:
                form_inputs[r[0]] = r[1]

        return form_inputs

    @staticmethod
    def decode_saml_request(self, saml):
        # We need to grab certain elements to construct a response
        xml = base64.b64decode(saml)
        xdoc = et.fromstring(xml)

        return xdoc.attrib['ID']

    @staticmethod
    def send_saml_response(self, url, response, relay_state, cookies={}):
        result = requests.post(url,
                               data="SAMLResponse={}&RelayState={}".format(response, relay_state),
                               headers={"Content-Type": "application/x-www-form-urlencoded"},
                               cookies=cookies,
                               verify=False,
                               allow_redirects=False)

        return result

    @staticmethod
    def continue_saml_response(url, cookies={}):
        return requests.get(url, verify=False, allow_redirects=False, cookies=cookies)


class SsoUtils:
    def __init__(self, url, secret_key, access_key, user, passwd):
        self.url = url
        self.capella_api = CapellaAPI("https://" + url, secret_key, access_key, user, passwd)

    def check_realm_exists(self, realm_name):
        """
        Check if the realm exists. Initiate IdP login
        """
        url = "{0}/v2/auth/sso-login/{1}".format("https://" + self.url, realm_name)
        resp = self.capella_api.do_internal_request(url, method="GET")
        return resp

    def sso_callback(self, code, state, method="GET"):
        """
        The Callback endpoint is triggered when an SSO user successfully authenticates with an
        identity provider and returns to Capella (via Auth0) with the user’s details.
        """
        url = "{0}/v2/auth/sso-callback?code={1}&state={2}".format("https://" + self.url, code,
                                                                   state)
        resp = self.capella_api.do_internal_request(url, method=method)
        return resp

    def sso_signup(self, marketingOptIn=False):
        """
        The first time a Capella SSO user authenticates via the Callback endpoint, they should
        enter some additional information and accept the terms and conditions.
        """
        url = "{0}/v2/auth/sso-signup".format("https://" + self.url)
        sso_signup_payload = {"marketingOptIn": marketingOptIn}
        resp = self.capella_api.do_internal_request(url, method="POST",
                                                    params=json.dumps(sso_signup_payload))
        return resp

    def create_realm(self, tenant_id, body):
        url = "{0}/v2/organizations/{1}/realms".format("https://" + self.url, tenant_id)
        resp = self.capella_api.do_internal_request(url, method="POST", params=json.dumps(body))
        return resp

    def list_realms(self, tenant_id):
        """
        List all realms in the organization
        """
        url = "{0}/v2/organizations/{1}/realms".format("https://" + self.url, tenant_id)
        resp = self.capella_api.do_internal_request(url, method="GET")
        return resp

    def show_realm(self, tenant_id, realm_id):
        """
        Show details of an individual realm
        """
        url = "{0}/v2/organizations/{1}/realms/{2}".format("https://" + self.url, tenant_id,
                                                           realm_id)
        resp = self.capella_api.do_internal_request(url, method="GET")
        return resp

    def update_realm_default_team(self, tenant_id, realm_id, team_id):
        """
        Updates realm default team
        """
        url = "{0}/v2/organizations/{1}/realms/{2}".format("https://" + self.url, tenant_id,
                                                           realm_id)
        body = {"defaultTeamId": team_id}
        resp = self.capella_api.do_internal_request(url, method="PUT", params=json.dumps(body))
        return resp

    def delete_realm(self, tenant_id, realm_id, keep_users=False):
        """
        Deletes a realm
        """
        url = "{0}/v2/organizations/{1}/realms/{2}".format("https://" + self.url, tenant_id,
                                                           realm_id)
        body = {"keepUsers": keep_users}
        resp = self.capella_api.do_internal_request(url, method="DELETE", params=json.dumps(body))
        return resp

    def list_teams(self, tenant_id):
        """
        List all teams in the organization
        """
        url = "{0}/v2/organizations/{1}/teams?page=1&perPage=10".format("https://" + self.url,
                                                                        tenant_id)
        resp = self.capella_api.do_internal_request(url, method="GET")
        return resp

    def show_team(self, tenant_id, team_id):
        """
        Show details of an individual team
        """
        url = "{0}/v2/organizations/{1}/teams/{2}".format("https://" + self.url, tenant_id,
                                                          team_id)
        resp = self.capella_api.do_internal_request(url, method="GET")
        return resp

    def create_team(self, tenant_id, body):
        """
        Create team in the organization
        """
        url = "{0}/v2/organizations/{1}/teams".format("https://" + self.url, tenant_id)
        resp = self.capella_api.do_internal_request(url, method="POST", params=json.dumps(body))
        return resp

    def update_team(self, tenant_id, body):
        """
        Update team in the organization
        """
        url = "{0}/v2/organizations/{1}/teams".format("https://" + self.url, tenant_id)
        resp = self.capella_api.do_internal_request(url, method="PUT", params=json.dumps(body))
        return resp

    def update_team_org_roles(self, tenant_id, body):
        """
        Update team org roles in the organization
        """
        url = "{0}/v2/organizations/{1}/teams/org-roles".format("https://" + self.url,
                                                                tenant_id)
        resp = self.capella_api.do_internal_request(url, method="PUT", params=json.dumps(body))
        return resp

    def update_team_project_roles(self, tenant_id, body):
        """
        Update team project roles in the organization
        """
        url = "{0}/v2/organizations/{1}/teams/projects".format("https://" + self.url,
                                                               tenant_id)
        resp = self.capella_api.do_internal_request(url, method="PUT", params=json.dumps(body))
        return resp

    def delete_team(self, tenant_id, team_id):
        """
        Delete team
        """
        url = "{0}/v2/organizations/{1}/teams/{2}".format("https://" + self.url, tenant_id, team_id)
        resp = self.capella_api.do_internal_request(url, method="DELETE")
        return resp
