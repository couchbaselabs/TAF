# -*- coding: utf-8 -*-

import os
import re
import json
import base64
import random
import string

from java.security import KeyPairGenerator, KeyFactory
from pytests.security.onCloud.sso_utils import SsoUtils
from pytests.Capella.RestAPIv4.security_base import SecurityBase
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

cert = """
MIIFkjCCA3oCCQDb09WUdyEHqDANBgkqhkiG9w0BAQsFADCBijELMAkGA1UEBhMC
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


def validate_realm_name(realm_name):
    """
    Validates a realm ID according to the following rules:
    - Length must be between 3 and 24 characters.
    - Allowed characters: 0-9, A-Z, a-z, -, _, .
    - Symbols (-, _, .) cannot be repeated consecutively (e.g., __, --, .. are not allowed).

    Returns:
    - True if valid, False otherwise.
    """
    if not (3 <= len(realm_name) <= 24):
        return False

    if re.search(r'[^0-9A-Za-z-_.]', realm_name):
        return False

    if re.search(r'(--|__|\.\.)', realm_name):
        return False
    return True


class SsoTests(SecurityBase):
    def setUp(self):
        SecurityBase.setUp(self)
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.secret_key = self.input.capella.get("secret_key")
        self.access_key = self.input.capella.get("access_key")

        self.sso = SsoUtils(self.url, self.secret_key, self.access_key, self.user, self.passwd)
        self.unauth_z_sso = SsoUtils(self.url, self.secret_key, self.access_key, self.test_users["User3"]["mailid"],
                                     self.test_users["User3"]["password"])

        self._generate_key_pair()
        self._generate_ssigned_cert()
        self.get_team_id()
        self.invalid_id = "00000000-0000-0000-0000-000000000000"

        self.log.info("Deleting any realms that are already present")
        resp = self.sso.list_realms(self.tenant_id)
        if json.loads(resp.content)["data"]:
            self.log.info("Destroying the realm")
            realm_id = json.loads(resp.content)["data"][0]["data"]["id"]
            resp = self.sso.delete_realm(self.tenant_id, realm_id)
            self.assertEqual(resp.status_code // 100, 2)

    def tearDown(self):
        resp = self.sso.list_realms(self.tenant_id)
        if json.loads(resp.content)["data"]:
            self.log.info("Destroying the realm")
            realm_id = json.loads(resp.content)["data"][0]["data"]["id"]
            resp = self.sso.delete_realm(self.tenant_id, realm_id)
            self.validate_response(resp, 2)
        super(SsoTests, self).tearDown()

    def _generate_key_pair(self):
        self.key = """-----BEGIN PRIVATE KEY-----
        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
        WCl1NZaMgGUaWgdmKha5z36DUVDu6Vo=
        -----END PRIVATE KEY-----
        """

        k = self.key.replace("-----BEGIN PRIVATE KEY-----", "").replace(os.linesep, "").replace(
            "-----END PRIVATE KEY-----", "")
        encoded = base64.b64decode(k)

        kf = KeyFactory.getInstance("RSA")
        ks = PKCS8EncodedKeySpec(encoded)
        self.private = kf.generatePrivate(ks)

        ks = RSAPublicKeySpec(self.private.getModulus(), self.private.getPublicExponent())
        self.public = kf.generatePublic(ks)

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
        self.cert_new = """-----BEGIN CERTIFICATE-----
MIIDqDCCApCgAwIBAgIGAYSqj+p4MA0GCSqGSIb3DQEBCwUAMIGUMQswCQYDVQQGEwJVUzETMBEG
A1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzENMAsGA1UECgwET2t0YTEU
MBIGA1UECwwLU1NPUHJvdmlkZXIxFTATBgNVBAMMDGRldi04MjIzNTUxNDEcMBoGCSqGSIb3DQEJ
ARYNaW5mb0Bva3RhLmNvbTAeFw0yMjExMjQxNjUzMjlaFw0zMjExMjQxNjU0MjlaMIGUMQswCQYD
VQQGEwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzENMAsG
A1UECgwET2t0YTEUMBIGA1UECwwLU1NPUHJvdmlkZXIxFTATBgNVBAMMDGRldi04MjIzNTUxNDEc
MBoGCSqGSIb3DQEJARYNaW5mb0Bva3RhLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
ggEBAM+rByaRbnTWXVMx4dI+mjJsquc79ZHXqURzYy4xPojL+49BBGhAjUUswsm2dB5Os4XbW/MW
cwVkSXqPeB/ixYye5ofRokuHsojtcKXau6D4E61qfcwZZkXJnERdWdQ2p1Ur2817/2hAFyQg6WhO
eJDqR4ZFJiANCzcRxSxrSnr6lFtGIq+fMQOEbJ+uiFg10P5lw2JRSHLMsmGMN/Qi8sPZbNmh/hjQ
f2xwa0SJG9TK50mQKEefZykJxXyDvc5oNQVH+hSKg38TQ6gy8eib2s5gyVgdIUNOneHP1tXA1FWh
JUC8+B1Qz/gBxxloFbzEyPGrUgqALn+AtAAPz/bDiyUCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEA
kpGWpt7kRIwN2Sdcp6o77LLYkTllHd7xzcNL3nole1wWqg4QQaz8lC6Q1PSpREVat/ENJK30NAxg
XAl6My1vgZwMFA1y2fMjiuPb1YhceYBEYDp2WBe6TdBdq3qDBC4D/XQWIFsCH8gll3OGrhASYVbE
og/k+oSgeW/KmRIu9+AswdFTg+JIoKOP9TkdnsNkbkvWECDBLDHMxRFsvWJPO/9LKw3HlBwqaT5v
dnJ3l9X1xP327Ujr7xgdUprSFT7DPgBCKpCVmjsxq5vl0kiUykzNbmSrQGowPQi9iVMOxa/H75LP
s0GjYziw9oQWA8BBuEc+tgWntz1vSzDT9ePQ/A==
-----END CERTIFICATE-----
"""
        
    def get_cert(self):
        return self.cert_new

    def get_certificate(self):
        # This certificate is only valid until 08/30/2023
        return self.cert.replace(os.linesep, "").replace('\s', "")

    def validate_response(self, response, expected_response):
        self.assertEqual(response.status_code // 100, expected_response,
                         msg="Expected:{0} :: Resp: {1}, {2}"
                         .format(expected_response, response, response.content))

    def get_team_id(self):
        teams_resp = self.sso.list_teams(self.tenant_id)
        self.assertEqual(teams_resp.status_code // 100, 2, msg="No team present")
        data = json.loads(teams_resp.content)
        team = data['data'][0]['data']
        self.team_id = team['id']

    def create_realm(self, team_id):
        body = {
            'connectionOptionsSAML': {
                'signInEndpoint': "https://dev-82235514.okta.com/app/dev-82235514_cbcdev_2"
                                  "/exk7dwu0sfh6bR27M5d7/sso/saml",
                'signingCertificate': "{0}".format(self.get_cert()),
                "signatureAlgorithm": "rsa-sha256",
                "digestAlgorithm": "sha256",
                "protocolBinding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
            },
            'standard': 'SAML 2.0',
            'disableGroupMapping': False,
            'defaultTeamId': team_id
        }

        self.log.info("Creating realm")
        realm_resp = self.sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 2)

    def test_create_realm_with_diff_payload(self):
        # 1. metadataXML - valid
        body = {
            'connectionOptionsSAML': {
                'signInEndpoint': "https://dev-82235514.okta.com/app/dev-82235514_cbcdev_2"
                                  "/exk7dwu0sfh6bR27M5d7/sso/saml",
                'signingCertificate': "{0}".format(self.get_cert()),
                "signatureAlgorithm": "rsa-sha256",
                "digestAlgorithm": "sha256",
                "protocolBinding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
            },
            'standard': 'SAML 2.0',
            'disableGroupMapping': False,
            'defaultTeamId': self.team_id
        }

        realm_resp = self.sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 2)

        # 2. no payload - invalid
        body = {}

        realm_resp = self.sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 4)

        # 3. no standard - invalid
        body = {
            'connectionOptionsSAML': {
                'metadataXML': IDPMetadataTemplate.format(self.get_certificate(),
                                                          self.get_certificate())
            },
            'vendor': 'Okta',
            'defaultTeamId': self.team_id
        }

        realm_resp = self.sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 4)

        # 4. no vendor - invalid
        body = {
            'connectionOptionsSAML': {
                'metadataXML': IDPMetadataTemplate.format(self.get_certificate(),
                                                          self.get_certificate())
            },
            'standard': 'SAML 2.0',
            'defaultTeamId': self.team_id
        }

        realm_resp = self.sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 4)

        # 5. no profiles - invalid
        body = {
            'connectionOptionsSAML': {
                'metadataXML': IDPMetadataTemplate.format(self.get_certificate(),
                                                          self.get_certificate())
            },
            'standard': 'SAML 2.0',
            'vendor': 'Okta'
        }

        realm_resp = self.sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 4)

    def test_check_realm_exists(self):
        self.create_realm(self.team_id)

        realms_resp = self.sso.list_realms(self.tenant_id)
        data = json.loads(realms_resp.content)
        realm = data['data'][0]['data']
        realm_name = realm['name']
        resp = self.sso.check_realm_exists(realm_name)
        self.validate_response(resp, 2)

        non_exist_realm_name = self.input.param("non_exist_realm_name", "test-realm")
        resp = self.sso.check_realm_exists(non_exist_realm_name)
        self.validate_response(resp, 4)

        resp = self.sso.check_realm_exists("")
        self.validate_response(resp, 4)

    def test_expired_csrf_token(self):
        code = self.input.param("code", "test")
        expired_csrf_token = "eyJjc3JmIjp7ImlkIjoiY3NyZi10b2tlbi0xNjNmZTY3Yy1mOGM5LTExZWMtYjkzOS0wMjQyYWMxMjAwMDIiLCJ0b2tlbiI6IjE2M2ZlNjdjLWY4YzktMTFlYy1iOTM5LTAyNDJhYzEyMDAwMiJ9fQo="
        resp = self.sso.sso_callback(code, expired_csrf_token)
        self.validate_response(resp, 4)

    def test_create_realm_limit(self):
        """
        Currently the limitation of creating a realm is one per tenant
        """
        limit = 1
        realms_resp = self.sso.list_realms(self.tenant_id)
        data = json.loads(realms_resp.content)
        no_realms = len(data['data'])
        body = {
            'connectionOptionsSAML': {
                'metadataXML': IDPMetadataTemplate.format(self.get_certificate(),
                                                          self.get_certificate())
            },
            'standard': 'SAML 2.0',
            'vendor': 'Okta',
            'defaultTeamId': self.team_id
        }
        create_more = limit - no_realms
        while create_more:
            self.create_realm(self.team_id)
            create_more = create_more - 1

        realm_resp = self.sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 4)

    def test_create_realm_unauthz(self):
        body = {
            'connectionOptionsSAML': {
                'metadataXML': IDPMetadataTemplate.format(self.get_certificate(),
                                                          self.get_certificate())
            },
            'standard': 'SAML 2.0',
            'vendor': 'Okta',
            'defaultTeamId': self.team_id
        }

        # user without sufficient permissions
        realm_resp = self.unauth_z_sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 4)

    def test_show_realms(self):
        self.create_realm(self.team_id)

        realms_resp = self.sso.list_realms(self.tenant_id)
        data = json.loads(realms_resp.content)
        realm = data['data'][0]['data']
        realm_id = realm['id']

        resp = self.sso.show_realm(self.tenant_id, realm_id)
        self.validate_response(resp, 2)

        # different tenant id
        resp = self.sso.show_realm(self.invalid_id, realm_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        resp = self.unauth_z_sso.show_realm(self.tenant_id, realm_id)
        self.validate_response(resp, 4)

        # invalid realm id
        resp = self.sso.show_realm(self.tenant_id, self.invalid_id)
        self.validate_response(resp, 4)

    def test_list_realms(self):
        self.create_realm(self.team_id)

        resp = self.sso.list_realms(self.tenant_id)
        self.validate_response(resp, 2)

        # different tenant id
        resp = self.sso.list_realms(self.invalid_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        resp = self.unauth_z_sso.list_realms(self.tenant_id)
        self.validate_response(resp, 4)

    def test_update_realm_default_team(self):
        self.create_realm(self.team_id)

        resp = self.sso.list_realms(self.tenant_id)
        realm_id = json.loads(resp.content)["data"][0]["data"]["id"]

        # User with valid tenantId and realm ID
        self.log.info("Update realm with valid tenantId and realm ID")
        resp = self.sso.update_realm_default_team(self.tenant_id, realm_id, self.team_id)
        self.validate_response(resp, 2)

        # user with insufficient permissions
        self.log.info("Update realm with invalid tenant Id")
        resp = self.sso.update_realm_default_team(self.invalid_id, realm_id, self.team_id)
        self.validate_response(resp, 4)

        # User with invalid realm Id
        self.log.info("Update realm with invalid realm Id")
        resp = self.sso.update_realm_default_team(self.tenant_id, self.invalid_id,
                                                  self.team_id)
        self.validate_response(resp, 4)

        # User with invalid team id
        self.log.info("Update realm with invalid team Id")
        resp = self.sso.update_realm_default_team(self.tenant_id, realm_id, self.invalid_id)
        self.validate_response(resp, 4)

        # User with no realm team id
        self.log.info("Update realm with no team Id")
        resp = self.sso.update_realm_default_team(self.tenant_id, realm_id, "")
        self.validate_response(resp, 4)

        # user without sufficient permissions
        self.log.info("Update realm without sufficient permissions")
        resp = self.unauth_z_sso.update_realm_default_team(self.tenant_id, realm_id, self.team_id)
        self.validate_response(resp, 4)

    def test_delete_realm(self):
        self.create_realm(self.team_id)

        resp = self.sso.list_realms(self.tenant_id)
        realm_id = json.loads(resp.content)["data"][0]["data"]["id"]

        # User with valid tenantId and realm ID
        self.log.info("Delete realm with valid tenantId and realm ID")
        resp = self.sso.delete_realm(self.tenant_id, realm_id)
        self.validate_response(resp, 2)

        self.create_realm(self.team_id)

        resp = self.sso.list_realms(self.tenant_id)
        realm_id = json.loads(resp.content)["data"][0]["data"]["id"]

        # user with insufficient permissions
        self.log.info("Delete realm with invalid tenant Id")
        resp = self.sso.delete_realm(self.invalid_id, realm_id)
        self.validate_response(resp, 4)

        # User with invalid realm Id
        self.log.info("Delete realm with invalid realm Id")
        resp = self.sso.delete_realm(self.tenant_id, self.invalid_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        self.log.info("Delete realm without sufficient permissions")
        resp = self.unauth_z_sso.delete_realm(self.tenant_id, realm_id)
        self.validate_response(resp, 4)

    def test_update_org_roles(self):
        body = {
            "orgRoles": ["organizationOwner"]
        }
        self.log.info("Update organisation role wth valid tenantId")
        resp = self.sso.update_team_org_roles(self.tenant_id, self.team_id, body)
        self.validate_response(resp, 2)

        self.log.info("Update organisation role wth invalid tenantId")
        resp = self.sso.update_team_org_roles(self.invalid_id, self.team_id, body)
        self.validate_response(resp, 4)

        self.log.info("Update organisation role wth missing team_id")
        resp = self.sso.update_team_org_roles(self.invalid_id, "", body)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        self.log.info("Update organisation role without sufficient permissions")
        resp = self.unauth_z_sso.update_team_org_roles(self.tenant_id, self.team_id, body)
        self.validate_response(resp, 4)

    def test_update_project_roles(self):
        body = {
            "projects": [
                {
                    "projectId": self.project_id,
                    "projectName": "SSO_API_TEST",
                    "roles": ["projectOwner"]
                }
            ]
        }
        self.log.info("Update project role with valid tenantId")
        resp = self.sso.update_team_project_roles(self.tenant_id, self.team_id, body)
        self.validate_response(resp, 2)

        self.log.info("Update project role with invalid tenantId")
        resp = self.sso.update_team_project_roles(self.invalid_id, self.team_id, body)
        self.validate_response(resp, 4)

        self.log.info("Update project role wth missing team_id")
        resp = self.sso.update_team_project_roles(self.invalid_id, "", body)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        self.log.info("Update project role without sufficient permissions")
        resp = self.unauth_z_sso.update_team_project_roles(self.tenant_id, self.team_id, body)
        self.validate_response(resp, 4)

    def test_team(self):
        self.log.info("Test to check the functionalities of team")
        self.log.info("Test to create team")
        body = {
            "name": "".join(random.choice(string.ascii_letters + string.digits) for _ in range(10)),
            "orgRoles": ["organizationOwner"],
            "projects": [
                {
                    "projectId": self.project_id,
                    "projectName": "SSO_API_TEST",
                    "roles": ["projectOwner"]
                }
            ],
            "groups": ["Example Okta Group One"]
        }
        # User with invalid tenantId
        self.log.info("Create team with invalid tenantId")
        resp = self.sso.create_team(self.invalid_id, body)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        self.log.info("Create team without sufficient permissions")
        resp = self.unauth_z_sso.create_team(self.tenant_id, body)
        self.validate_response(resp, 4)

        # User with valid tenantID and body
        self.log.info("Create team with valid tenantID")
        resp = self.sso.create_team(self.tenant_id, body)
        self.validate_response(resp, 2)

        resp = self.sso.list_teams(self.tenant_id)
        data = json.loads(resp.content)
        team_id = data['data'][0]['data']['id']
        self.log.info("Test to Update team")
        body = {
            "name": "team_name_changed"
        }
        # User with valid tenantID
        self.log.info("Update team with valid tenantId")
        resp = self.sso.update_team(self.tenant_id, team_id, body)
        self.validate_response(resp, 2)

        # User with invalid tenantId
        self.log.info("Update team with invalid tenantId")
        resp = self.sso.update_team(self.invalid_id, team_id, body)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        self.log.info("Update team without sufficient permissions")
        resp = self.unauth_z_sso.update_team(self.tenant_id, team_id, body)
        self.validate_response(resp, 4)

        self.log.info("Test to list teams")
        self.log.info("List teams with valid tenantId")
        resp = self.sso.list_teams(self.tenant_id)
        self.validate_response(resp, 2)

        # different tenant id
        self.log.info("List teams with invalid tenantId")
        resp = self.sso.list_teams(self.invalid_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        self.log.info("List teams without sufficient permissions")
        resp = self.unauth_z_sso.list_teams(self.tenant_id)
        self.validate_response(resp, 2)

        self.log.info("Test to get team")
        # User with valid tenantId and team ID
        self.log.info("Show team with valid tenantId and team ID")
        resp = self.sso.show_team(self.tenant_id, team_id)
        self.validate_response(resp, 2)

        # User with insufficient permissions
        self.log.info("Show team with invalid tenant Id")
        resp = self.sso.show_team(self.invalid_id, team_id)
        self.validate_response(resp, 4)

        # User with invalid team Id
        self.log.info("Show team with invalid team Id")
        resp = self.sso.show_team(self.tenant_id, self.invalid_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        self.log.info("Show team without sufficient permissions")
        resp = self.unauth_z_sso.show_team(self.tenant_id, team_id)
        self.validate_response(resp, 2)

        resp = self.sso.list_teams(self.tenant_id)
        data = json.loads(resp.content)
        team_id = data['data'][0]['data']['id']
        self.log.info("Test to Delete the team")
        # User with valid tenantId and teamId
        self.log.info("Delete team with valid tenantId and valid teamId")
        resp = self.sso.delete_team(self.tenant_id, team_id)
        self.validate_response(resp, 2)

        # User with invalid tenantId
        self.log.info("Delete team with invalid tenantId")
        resp = self.sso.delete_team(self.invalid_id, team_id)
        self.validate_response(resp, 4)

        # User with invalid teamId
        self.log.info("Delete team with invalid teamId")
        resp = self.sso.delete_team(self.tenant_id, self.invalid_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        self.log.info("Delete team without sufficient permissions")
        resp = self.unauth_z_sso.delete_team(self.tenant_id, team_id)
        self.validate_response(resp, 4)

    def test_update_realm_name(self):
        """
        Tests:
        1) Allow customers to customize realm ids
        2) Validation of Custom Realm IDs: Implement checks to prevent security vulnerabilities due to malicious or
           inappropriate realm names. Support 0-9A-Za-z-_. minimum 3 maximum 24. Symbols cannot be repeated next to each
           other(__ -- are not allowed)
        """
        # Create realm
        self.create_realm(self.team_id)

        resp = self.sso.list_realms(self.tenant_id)
        realm_id = json.loads(resp.content)["data"][0]["data"]["id"]

        new_realm_name = "new_realm_name"

        # User with valid tenantId and realm ID
        self.log.info("Update realm with valid tenantId and realm ID")
        resp = self.sso.update_realm_name(self.tenant_id, realm_id, new_realm_name)
        self.validate_response(resp, 2)

        # user with insufficient permissions
        self.log.info("Update realm with invalid tenant Id")
        resp = self.sso.update_realm_name(self.invalid_id, realm_id, new_realm_name)
        self.validate_response(resp, 4)

        # User with invalid realm id
        self.log.info("Update realm with invalid realm Id")
        resp = self.sso.update_realm_name(self.tenant_id, self.invalid_id, new_realm_name)
        self.validate_response(resp, 4)

        # User with no realm name
        self.log.info("Update realm with no team Id")
        resp = self.sso.update_realm_name(self.tenant_id, realm_id, "")
        self.validate_response(resp, 4)

        # user without sufficient permissions
        self.log.info("Update realm without sufficient permissions")
        resp = self.unauth_z_sso.update_realm_name(self.tenant_id, realm_id, new_realm_name)
        self.validate_response(resp, 4)

        # test_valid_realm_id
        self.assertTrue(validate_realm_name('validRealm_1'))

        # test_realm_id_too_short
        self.assertFalse(validate_realm_name('ab'))

        # test_realm_id_too_long
        self.assertFalse(validate_realm_name('a' * 25))

        # test_realm_id_with_invalid_characters
        self.assertFalse(validate_realm_name('invalid#realm'))
        self.assertFalse(validate_realm_name('invalid@realm'))
        self.assertFalse(validate_realm_name('invalid realm'))

        # test_realm_id_with_consecutive_symbols
        self.assertFalse(validate_realm_name('invalid--realm'))
        self.assertFalse(validate_realm_name('invalid__realm'))
        self.assertFalse(validate_realm_name('invalid..realm'))

        # test_valid_symbols_and_length(self):
        self.assertTrue(validate_realm_name('user_123-abc.xyz'))

        # test_realm_id_edge_cases(self):
        # Edge case: exactly 3 characters
        self.assertTrue(validate_realm_name('abc'))
        # Edge case: exactly 24 characters
        self.assertTrue(validate_realm_name('a' * 24))
        # Edge case: valid realm ID at the edge of the character set
        self.assertTrue(validate_realm_name('realm-_.'))

    def test_sso_users(self):
        """
        Tests:
        1) List SSO users endpoint
        2) Delete SSO users endpoint
        """
        self.log.info("Test to check the functionalities of listing and deleting users")
        self.create_realm(self.team_id)

        realms_resp = self.sso.list_realms(self.tenant_id)
        data = json.loads(realms_resp.content)
        realm = data['data'][0]['data']
        realm_id = realm['id']

        # User with invalid realm id
        self.log.info("List users with invalid realm id")
        resp = self.sso.list_users(self.invalid_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        self.log.info("List users without sufficient permissions")
        resp = self.unauth_z_sso.list_users(realm_id)
        self.validate_response(resp, 2)

        # User with valid realm id
        self.log.info("List users with valid realm id")
        resp = self.sso.list_users(realm_id)
        self.validate_response(resp, 2)

        user_list = []

        # User with invalid realm id
        self.log.info("Delete users with invalid realm id")
        resp = self.sso.delete_users(self.invalid_id, user_list)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        self.log.info("Delete users without sufficient permissions")
        resp = self.unauth_z_sso.delete_users(realm_id, user_list)
        self.validate_response(resp, 4)

        # User with valid realm id and body
        self.log.info("Delete users with valid realm id")
        resp = self.sso.delete_users(realm_id, user_list)
        self.validate_response(resp, 2)
