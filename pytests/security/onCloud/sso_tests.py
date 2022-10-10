# -*- coding: utf-8 -*-

import os
import json
import base64

from pytests.basetestcase import BaseTestCase
from pytests.security.onCloud.sso_utils import SsoUtils
from java.security import KeyPairGenerator, KeyFactory
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


class SsoTests(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.project_id = self.input.capella.get("project_id")
        self.secret_key = self.input.capella.get("secret_key")
        self.access_key = self.input.capella.get("access_key")
        self.sso = SsoUtils(self.url, self.secret_key, self.access_key, self.user, self.passwd)
        self.diff_tenant_id = "00000000-0000-0000-0000-000000000000"
        self.user_unauthz = self.input.capella.get("user_unauthz")
        self.passwd_unauthz = self.input.capella.get("passwd_unauthz")
        self.unauth_z_sso = SsoUtils(self.url, self.secret_key, self.access_key, self.user_unauthz,
                                     self.passwd_unauthz)
        self._generate_key_pair()
        self._generate_ssigned_cert()

    def tearDown(self):
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

    def get_certificate(self):
        # This certificate is only valid until 08/30/2023
        return self.cert.replace(os.linesep, "").replace('\s', "")

    def validate_response(self, response, expected_response):
        self.assertEqual(response.status_code // 100, expected_response,
                         msg="Expected:{0} :: Resp: {1}, {2}"
                         .format(expected_response, response, response.content))

    def test_create_realm_with_diff_payload(self):
        teams_resp = self.sso.list_teams(self.tenant_id)
        data = json.loads(teams_resp.content)
        team = data['data'][0]['data']
        team_id = team['id']

        # 1. metadataXML - valid
        body = {
            'connectionOptionsSAML': {
                'metadataXML': IDPMetadataTemplate.format(self.get_certificate(),
                                                          self.get_certificate())
            },
            'standard': 'SAML 2.0',
            'vendor': 'Okta',
            'defaultTeamId': team_id
        }

        realm_resp = self.sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 2)

        # 2. signingCertificate and signInEndpoint - valid
        body = {
            'connectionOptionsSAML': {
                "signingCertificate": cert,
                "signInEndpoint": "http://capella.test/idp"
            },
            'standard': 'SAML 2.0',
            'vendor': 'Okta',
            'defaultTeamId': team_id
        }

        realm_resp = self.sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 2)

        # 3. no payload - invalid
        body = {}

        realm_resp = self.sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 4)

        # 4. no standard - invalid
        body = {
            'connectionOptionsSAML': {
                'metadataXML': IDPMetadataTemplate.format(self.get_certificate(),
                                                          self.get_certificate())
            },
            'vendor': 'Okta',
            'defaultTeamId': team_id
        }

        realm_resp = self.sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 4)

        # 5. no vendor - invalid
        body = {
            'connectionOptionsSAML': {
                'metadataXML': IDPMetadataTemplate.format(self.get_certificate(),
                                                          self.get_certificate())
            },
            'standard': 'SAML 2.0',
            'defaultTeamId': team_id
        }

        realm_resp = self.sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 4)

        # 6. no profiles - invalid
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
        realms_resp = self.sso.list_realms(self.tenant_id)
        data = json.loads(realms_resp.content)
        realm = data['data'][0]['data']
        realm_name = realm['name']
        non_exist_realm_name = self.input.param("non_exist_realm_name", "test-realm")
        resp = self.sso.check_realm_exists(non_exist_realm_name)
        self.validate_response(resp, 4)
        resp = self.sso.check_realm_exists(realm_name)
        self.validate_response(resp, 2)

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
        teams_resp = self.sso.list_teams(self.tenant_id)
        data = json.loads(teams_resp.content)
        team = data['data'][0]['data']
        team_id = team['id']
        body = {
            'connectionOptionsSAML': {
                'metadataXML': IDPMetadataTemplate.format(self.get_certificate(),
                                                          self.get_certificate())
            },
            'standard': 'SAML 2.0',
            'vendor': 'Okta',
            'defaultTeamId': team_id
        }
        create_more = limit - no_realms
        while create_more:
            realm_resp = self.sso.create_realm(self.tenant_id, body)
            self.validate_response(realm_resp, 2)
            create_more = create_more - 1
        realm_resp = self.sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 4)

    def test_create_realm_unauthz(self):
        teams_resp = self.sso.list_teams(self.tenant_id)
        data = json.loads(teams_resp.content)
        team = data['data'][0]['data']
        team_id = team['id']

        body = {
            'connectionOptionsSAML': {
                'metadataXML': IDPMetadataTemplate.format(self.get_certificate(),
                                                          self.get_certificate())
            },
            'standard': 'SAML 2.0',
            'vendor': 'Okta',
            'defaultTeamId': team_id
        }
        realm_resp = self.sso.create_realm(self.diff_tenant_id, body)
        self.validate_response(realm_resp, 4)

        # user without sufficient permissions
        realm_resp = self.unauth_z_sso.create_realm(self.tenant_id, body)
        self.validate_response(realm_resp, 4)

    def test_show_realms(self):
        realms_resp = self.sso.list_realms(self.tenant_id)
        data = json.loads(realms_resp.content)
        realm = data['data'][0]['data']
        realm_id = realm['id']

        resp = self.sso.show_realm(self.tenant_id, realm_id)
        self.validate_response(resp, 2)

        # different tenant id
        resp = self.sso.show_realm(self.diff_tenant_id, realm_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        resp = self.unauth_z_sso.show_realm(self.tenant_id, realm_id)
        self.validate_response(resp, 4)

        # invalid realm id
        invalid_realm_id = self.input.param("invalid_realm_id",
                                            "00000000-0000-0000-0000-000000000000")
        resp = self.sso.show_realm(self.tenant_id, invalid_realm_id)
        self.validate_response(resp, 4)

    def test_list_realms(self):
        resp = self.sso.list_realms(self.tenant_id)
        self.validate_response(resp, 2)

        # different tenant id
        resp = self.sso.list_realms(self.diff_tenant_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        resp = self.unauth_z_sso.list_realms(self.tenant_id)
        self.validate_response(resp, 4)

    def test_update_realm_default_team(self):
        resp = self.sso.list_realms(self.tenant_id)
        realm_id = json.loads(resp.content)["data"][0]["data"]["id"]
        response = self.sso.show_realm(self.tenant_id, realm_id)
        team_id = json.loads(response.content)["data"]["defaultTeamId"]

        # User with valid tenantId and realm ID
        self.log.info("Update realm with valid tenantId and realm ID")
        resp = self.sso.update_realm_default_team(self.tenant_id, realm_id, team_id)
        self.validate_response(resp, 2)

        # user with insufficient permissions
        self.log.info("Update realm with invalid tenant Id")
        resp = self.sso.update_realm_default_team(self.diff_tenant_id, realm_id, team_id)
        self.validate_response(resp, 4)

        # User with invalid realm Id
        self.log.info("Update realm with invalid realm Id")
        realm_id = self.input.param("realm_id", "00000000-0000-0000-0000-000000000000")
        resp = self.sso.update_realm_default_team(self.tenant_id, realm_id, team_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        resp = self.unauth_z_sso.update_realm_default_team(self.tenant_id, realm_id, team_id)
        self.validate_response(resp, 4)

    def test_delete_realm(self):
        resp = self.sso.list_realms(self.tenant_id)
        realm_id = json.loads(resp.content)["data"][0]["data"]["id"]

        # User with valid tenantId and realm ID
        self.log.info("Delete realm with valid tenantId and realm ID")
        resp = self.sso.delete_realm(self.tenant_id, realm_id)
        self.validate_response(resp, 2)

        # user with insufficient permissions
        self.log.info("Delete realm with invalid tenant Id")
        resp = self.sso.delete_realm(self.diff_tenant_id, realm_id)
        self.validate_response(resp, 4)

        # User with invalid realm Id
        self.log.info("Delete realm with invalid realm Id")
        realm_id = self.input.param("realm_id", "00000000-0000-0000-0000-000000000000")
        resp = self.sso.delete_realm(self.tenant_id, realm_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        resp = self.unauth_z_sso.delete_realm(self.tenant_id, realm_id)
        self.validate_response(resp, 4)

    def test_update_org_roles(self):
        resp = self.sso.list_realms(self.tenant_id)
        team_id = json.loads(resp.content)["data"][0]["data"]["defaultTeamId"]

        body = {
            "orgRoles": ["organizationOwner"]
        }
        self.log.info("Update organisation role wth valid tenantId")
        resp = self.sso.update_team_org_roles(self.tenant_id, body, team_id)
        self.validate_response(resp, 2)

        self.log.info("Update organisation role wth invalid tenantId")
        resp = self.sso.update_team_org_roles(self.diff_tenant_id, body, team_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        resp = self.unauth_z_sso.update_team_org_roles(self.tenant_id, body, team_id)
        self.validate_response(resp, 4)

    def test_update_project_roles(self):
        resp = self.sso.list_realms(self.tenant_id)
        team_id = json.loads(resp.content)["data"][0]["data"]["defaultTeamId"]

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
        resp = self.sso.update_team_project_roles(self.tenant_id, body, team_id)
        self.validate_response(resp, 2)

        self.log.info("Update project role with invalid tenantId")
        resp = self.sso.update_team_project_roles(self.diff_tenant_id, body, team_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        resp = self.unauth_z_sso.update_team_project_roles(self.tenant_id, body, team_id)
        self.validate_response(resp, 4)

    def test_team(self):
        self.log.info("Test to check the functionality of team")
        self.log.info("Test to create team")
        body = {
            "name": "Demo1",
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
        resp = self.sso.create_team(self.diff_tenant_id, body)
        self.validate_response(resp, 4)

        # User with valid tenantID and body
        self.log.info("Create team with valid tenantID")
        resp = self.sso.create_team(self.tenant_id, body)
        self.validate_response(resp, 2)

        # user without sufficient permissions
        resp = self.unauth_z_sso.create_team(self.tenant_id, body)
        self.validate_response(resp, 4)

        resp = self.sso.list_realms(self.tenant_id)
        team_id = json.loads(resp.content)["data"][0]["data"]["defaultTeamId"]
        self.log.info("Test to Update team")
        body = {
            "name": "team_changed"
        }
        # User with valid tenantID
        self.log.info("Update team with valid tenantId")
        resp = self.sso.update_team(self.tenant_id, body, team_id)
        self.validate_response(resp, 2)

        # User with invalid tenantId
        self.log.info("Update team with invalid tenantId")
        resp = self.sso.update_team(self.diff_tenant_id, body, team_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        resp = self.unauth_z_sso.update_team(self.tenant_id, body, team_id)
        self.validate_response(resp, 4)

        self.log.info("Test to list teams")
        resp = self.sso.list_teams(self.tenant_id)
        self.validate_response(resp, 2)

        # different tenant id
        resp = self.sso.list_teams(self.diff_tenant_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        resp = self.unauth_z_sso.list_teams(self.tenant_id)
        self.validate_response(resp, 4)

        self.log.info("Test to get team")
        # User with valid tenantId and team ID
        self.log.info("Show team with valid tenantId and team ID")
        resp = self.sso.show_team(self.tenant_id, team_id)
        self.validate_response(resp, 2)

        # User with insufficient permissions
        self.log.info("Show team with invalid tenant Id")
        resp = self.sso.show_team(self.diff_tenant_id, team_id)
        self.validate_response(resp, 4)

        # User with invalid team Id
        self.log.info("Show team with invalid team Id")
        team_id = self.input.param("team_id", "00000000-0000-0000-0000-000000000000")
        resp = self.sso.show_team(self.tenant_id, team_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        resp = self.unauth_z_sso.show_team(self.tenant_id, team_id)
        self.validate_response(resp, 4)

        self.log.info("Test to Delete the team")
        # User with valid tenantId and teamId
        self.log.info("Delete team with valid tenantId and valid teamId")
        resp = self.sso.delete_team(self.tenant_id, team_id)
        self.validate_response(resp, 2)

        # User with invalid tenantId
        self.log.info("Delete team with invalid tenantId")
        resp = self.sso.delete_team(self.diff_tenant_id, team_id)
        self.validate_response(resp, 4)

        # User with invalid teamId
        self.log.info("Delete team with invalid teamId")
        team_id = self.input.param("team_id", "00000000-0000-0000-0000-000000000000")
        resp = self.sso.delete_team(self.tenant_id, team_id)
        self.validate_response(resp, 4)

        # user without sufficient permissions
        resp = self.unauth_z_sso.delete_team(self.tenant_id, team_id)
        self.validate_response(resp, 4)

    def test_create_realm_only_azure_okta(self):
        tenants = self.sso.list_teams(self.tenant_id)

        # We only care about the most significant digit in this case 2xx being ok
        # anything else being considered an error in the test.
        self.assertEqual(tenants.status_code // 100, 2)

        # Do some decoding
        data = json.loads(tenants.content)
        self.assertEqual(len(data['data']), 1)

        # This is safe due to the above assertion.
        team = data['data'][0]['data']
        self.assertIsNotNone(team['id'])
        self.log.info("Got Team ID: {}".format(team['id']))
        team_id = team['id']

        valid_vendors = ["Okta", "Azure"]
        invalid_vendors = ["azure", "okta", "Ping", "Az1@"]
        error_msg = "Communication vendor is invalid"

        for vendor in valid_vendors:
            body = {
                'connectionOptionsSAML': {
                    'metadataURL': 'example.com'  # invalid URL, used just for verifying the msg
                },
                'standard': 'SAML 2.0',
                'vendor': vendor,
                'defaultTeamId': team_id
            }
            realm = self.sso.create_realm(self.tenant_id, body)
            if error_msg in realm.content:
                self.fail("{0} is a valid vendor".format(vendor))

        for vendor in invalid_vendors:
            body = {
                'connectionOptionsSAML': {
                    'metadataURL': 'example.com'  # invalid URL, used just for verifying the msg
                },
                'standard': 'SAML 2.0',
                'vendor': vendor,
                'defaultTeamId': team_id
            }
            realm = self.sso.create_realm(self.tenant_id, body)
            if error_msg not in realm.content:
                self.fail("{0} is not a valid vendor".format(vendor))
