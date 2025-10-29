# -*- coding: utf-8 -*-

import base64
import json
import os
import re
import requests
import six
import datetime
import binascii
import logging

import xml.etree.ElementTree as et
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from java.security import KeyPairGenerator, KeyFactory, Signature
from java.security.spec import PKCS8EncodedKeySpec, RSAPublicKeySpec


def log_certificate_details(cert_data, component="CERT"):
    """Parse and log detailed certificate information"""
    try:
        from java.security.cert import CertificateFactory, X509Certificate
        from java.io import ByteArrayInputStream

        # Remove any PEM headers and decode base64 if needed
        cert_bytes = cert_data
        if "-----BEGIN CERTIFICATE-----" in cert_data:
            cert_clean = cert_data.replace("-----BEGIN CERTIFICATE-----", "")
            cert_clean = cert_clean.replace("-----END CERTIFICATE-----", "")
            cert_clean = cert_clean.replace("\n", "").replace("\r", "").replace(" ", "").replace("\t", "")
            cert_bytes = base64.b64decode(cert_clean)
        elif isinstance(cert_data, str) and not cert_data.startswith("MII"):
            cert_bytes = base64.b64decode(cert_data)
        elif isinstance(cert_data, str):
            cert_bytes = base64.b64decode(cert_data)

        cf = CertificateFactory.getInstance("X.509")
        cert_stream = ByteArrayInputStream(cert_bytes)
        x509_cert = cf.generateCertificate(cert_stream)

        try:
            x509_cert.checkValidity()
        except Exception as validity_error:
            pass

        public_key = x509_cert.getPublicKey()

    except Exception as java_error:
        # Method 2: Basic ASN.1 analysis (limited info)
        try:
            if isinstance(cert_data, str) and "-----BEGIN CERTIFICATE-----" in cert_data:
                cert_clean = cert_data.replace("-----BEGIN CERTIFICATE-----", "")
                cert_clean = cert_clean.replace("-----END CERTIFICATE-----", "")
                cert_clean = cert_clean.replace("\n", "").replace("\r", "").replace(" ", "").replace("\t", "")
                cert_bytes = base64.b64decode(cert_clean)
            elif isinstance(cert_data, str):
                cert_bytes = base64.b64decode(cert_data)
            else:
                cert_bytes = cert_data

            if len(cert_bytes) > 0 and cert_bytes[0] == 0x30:
                pass  # Valid ASN.1 SEQUENCE detected

        except Exception as fallback_error:
            pass

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
    def __init__(self, capella, url, okta_base_url="https://integrator-2810815.okta.com/"):
        # Pre-flight checks
        assert (type(capella) == CapellaAPI)
        assert (type(url) == str)

        # Assignment
        self.okta_base_url = okta_base_url.rstrip('/')  # Remove trailing slash for consistency
        self.capella = capella
        self.internal_url = url.replace("cloud", "", 1)

        # Setup logging
        self.log = logging.getLogger(self.__class__.__name__)
        if not self.log.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.setLevel(logging.INFO)

        self._generate_key_pair()
        self._generate_ssigned_cert()

    def _generate_key_pair(self):
        self.key = """-----BEGIN PRIVATE KEY-----
        MIIJQwIBADANBgkqhkiG9w0BAQEFAASCCS0wggkpAgEAAoICAQC+Q5cF15TpHJwW
        ++V7j6I1KPXeKHsKHt7vKwMsdOLP2wgNWhDeFcRGdyL1ljoBDLHyDlRnu4lvofWG
        L3LLd+Ts4oZrH09q2HzHGbGOcdiVvmpSFUC/gGr/4qu/es0S4VNK08A7lkqvaTPF
        fWWPGmz97cJKYsrXcGk3pvZ8VB0lEqtMLWnNzU/AWrLuY9d7lrS1h/BIwhqyR1Yc
        t2fuw8LYHQYL80/VdAvyvqWH7jQgZgXtO4z3bU8l5Qu0QFUlxCmVYHAlWM/3daEj
        Ch+3e+y8MLp4RlzVQooCLWyraYTRfNHmt95BcGjlWHMae85DXoFOiRYtDH0BX8cv
        KfIegYEI6HxO/ZWRQMwnfZY+V95NnM2vB8gyWuJ5/6vn+eQ/YtgCgS7GLPMSYabT
        4jFxyKfp/gCPeoyz1xzP44i1+sytu6yZSoZRvF/94ggTbh4WZBmHeUZv6nXJZnqR
        eB5fGMJrJKExiQ7bWqjjD955xFGfb6OSZVj95uQzoInJg1NRSuGtmktFdc0GcnjJ
        SaRLeZj79k7GO9qF1eP7QbFDe6SONdURkwBBya4ANA5pUF3aHvjGeU+u0TNDBwoE
        3S8i0RybvvVVdEg/vhe5Ut0UEJbbpvgb3C4uQhcy9bvLGFG+1DwPaMYRCQjcru6H
        lABCcltgopMErcGyIDVNFSdpfGm8hwIDAQABAoICAAy0RqlFM9Q87M88Ep6lgJc8
        HO6wlUafuRMPNC8LvvlSDr+Ve/rYSTrLSbJMTaM0Tc1z1371rr+deN90251qbAbF
        OWR9urfPrMU+mfSA9Y1KIZ9JfAFNedeMMxud/4N7OCyO5w8TgfkkdSiCrv1XLl/m
        OErvPcwU3XGx3FOA6lFHZdi4T5BGXUTiyX/RetntvjNA2HKDQ3UT/lP8ixwORjxX
        88dof7QpA4M556Oj7CcdqNemWktLKTXGjXmDsivLZVtsDRDxVTGY0SGd03BU/gGy
        30CYsiorPzvV/KOVcCDszLczH8wOzXJC7ayfB67H6KptzOVFKDOnSY2Ep34R96Ob
        AE9YeZog0176eysSFtovGUjCauNAlgwLruiHuKeR8IgGhJlhMSAfqqHVDDyetba+
        ASrb7MVnD8TIzdtZepoR8iHXlRCX5J5r6P84et1Dts4ol2Tn7vWX5UrilBvx9s5Y
        PU+GMxB5zgjW0pB2+uGGP9fCUGUXIgWhPdWEQO5cuST9igk2m0HvyHOwaJVQHELx
        VCJfEi4/9BKkNwqPAJvs73VjnFpWW+RQkcIT/iRTEpsm058trTkXHWDn3Ak4HwoD
        54FZANMh/JrsgJ6TFzC2Ju0NLJusgYvB6SUrov2z2BtlylOGxnX2nw1YjLIUcSbw
        xYnf3qjkp04IxVZVcZ/BAoIBAQD08YM6PyV6CBKkrjQlGiKEBQS7U3LQu7ccjIxe
        UMG+VmaLl0wtOHf24cs7Wvlu+Hync4tPQt/GqJjkRmE36Acp7fLMq23+mTci72mD
        xFEQcvqAIh31Z2k3iOWYlWkh0JXlqdjW90mdhrsPYVTxG3C7N2AhNg5HgEQNGW/t
        K/WOLYZBhvmd1OfZBE+2K7FnawiKMNSYpPHnqtJT789mJvqkM3ThoOPSo9oLheEA
        SWj/KIUd0Kzat247sPEJ/Vyejo7r/YJjNd8tPrLrmNgqQthF4WMq2Xf7ugkj8Uj/
        uwWoS/2gtYkzMQ5xtXI+qk1XSo01HnFsVYcxAGI5Ucer0jhlAoIBAQDG2jhLeOa4
        Z21tI563kV+iLNO8GK/kZpt5LhZsPPNtvlXQWTzRt7fa7iF5jdTX6SQchfIWP+wt
        KvsPBE+m7r08UlGul3x9WsPoUKQ2g44vYD1yf7pcukdyKxLXo0FtMIclAw97LwwS
        ZdzWK0jFcZcBGIogF0VLq0Moh9KtpRlq1NBf2AaKfIiowm3TVpITBf3cBlO7R4TU
        WNUjemNZomCzIT35z88QambzSxGSDlbYRVxrAxo5JrndOMijKyi0LYP+Z174m7q6
        DDfyhoLR3dpZnzEy3zFLrLWNLNft/ZtJjOVxncqyKH5hbyPdaA5PwvBU22HMY7Lu
        QlUXis2lEdR7AoIBAQCd0pt+RT+tgFQa3c/ldBB3RhCFbsBU9A6JC5pKDvi23k7W
        dJb1sCPD5DqhtP1w3hTbhBxn5qvvAaT5uMtdTViIULQX4gFOgYRAM0imSTD+S0dS
        lij9fTMTQFbHWMXTruVWhHieEdnR8QTIZSto6whY6NY8r+rFcUTnt9MhWyiuAnE1
        +XUOmCcG8rs1orN88j8fY1UoQWdIoQ1CXgshQ85NTIZr9tG0YDTXJd0S5ToM5hvX
        WSR3DKevOFQm3qP12d8G+WIw8H4rVfHM0b++gzF83TubWWHwxS6shQRHGkcL6GK0
        USPk4x/ubj39WJfavcN8TXBBoF4qWZfwXNwtvpbtAoIBAQC2vzO/QJQg4oRxKawi
        ncpa24GXOsTjTd0SR+W5gWZh8+sfd/Np9MJVxEUlVdBGwO87+2lZQ1PEytxBl7+v
        9XYZ6rlAj92Lwo+KgBn6M3rHVVYDVRkrQNKKPMWrQVDOjSuWOzLODZW1jsGeXdca
        BWM2UZwXE+vON82XgQOAK9upD32Up46TVRGurkiKUeQa8YW04GexFHAhSqZOXZrS
        dGUUGre/TljIiFCmxNWX+e+cAFqHyikeXtWvVhEly3Mz6ySD9/IS3eOEOdhs3Wt6
        fb2+YYuwaef+2PFcldq30DsXJBJ1EQOIKajD2IUIZCnK6ac/BQLi+LknN/TFlv2Y
        90LjAoIBADivpRrGUmMf8oiXXroWERzsmkCdgyz8/aWrxHHYYoM8042z/vw/QboP
        INM/uHKrA+4QFLNbM3g0hFyWSvaqnVK+6Oxgo4SLAVRM2lWYzMPGeF6/u3wtHz8i
        ZyNaT41/3XvaAiJZ7lhb1T2Qyxj8N3kHNvK0IbKRQQ739zvIh23LS3bPeMWzDPor
        XfJ+s7GxjWyqT6xCCjPc9/BhLv7/j/UjhTZm4rMNsXiixxRsMNMol6t3cn3O3oQO
        kWr3PnrGNTPlwVMozEwUp3HR6jYleBeO37LuGjXFUAt8rs66gOi2AKlwbcF936CC
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
        private_key = kf.generatePrivate(ks)
        s.initSign(private_key)
        s.update(h)
        signature_bytes = s.sign()
        signature_b64 = base64.b64encode(signature_bytes)
        return signature_b64

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
            MIIDtDCCApygAwIBAgIGAZkaUM6HMA0GCSqGSIb3DQEBCwUAMIGaMQswCQYDVQQGEwJVUzETMBEG
            A1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzENMAsGA1UECgwET2t0YTEU
            MBIGA1UECwwLU1NPUHJvdmlkZXIxGzAZBgNVBAMMEmludGVncmF0b3ItMjgxMDgxNTEcMBoGCSqG
            SIb3DQEJARYNaW5mb0Bva3RhLmNvbTAeFw0yNTA5MDUxNDM3NDdaFw0zNTA5MDUxNDM4NDdaMIGa
            MQswCQYDVQQGEwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNj
            bzENMAsGA1UECgwET2t0YTEUMBIGA1UECwwLU1NPUHJvdmlkZXIxGzAZBgNVBAMMEmludGVncmF0
            b3ItMjgxMDgxNTEcMBoGCSqGSIb3DQEJARYNaW5mb0Bva3RhLmNvbTCCASIwDQYJKoZIhvcNAQEB
            BQADggEPADCCAQoCggEBANoJ9nJcx8SSLkbHEtD+ypJvCggVCmZdrjIg58qDKZHw6SWjcHtVtkEq
            bZvhZvjf3CsVp556s8apg86HHvCj2pGGVYey2e5Cf4lCIbgCBnw6bAJhKCTVbZQyQJA2v2tFK9oS
            6dj8giru4ba3DAm3sFH0Ve3MPVCkclD2Xp5JUzsrQ4ZjyPfDa8gfKQPPiZPC14cGgX5D5nhqPOB5
            7eCs3lT9qnwwAn2bqeayp3h2s4dTq2ewYKlUssQtvMQxsS06Sz9ybIaA6xQcxf3CVnd5f/xw/zc4
            J4uPKNGOaHyi9jFyiR7aKzF41x3UFYUkRhGkJJUHWJBoC/04VGD/nSr5bX0CAwEAATANBgkqhkiG
            9w0BAQsFAAOCAQEAx4guTJiG1wA+NAkCorg3EE8NUssOT0D9KdYfab1QuNhlEvDPL75SvJLpefsU
            YbuBFabuPsKAkHAoSpm3Z2r0fcZvvkg8Lw/C+P+oeB/XHqCBmhFWeNsPzO8V6aO9kzkMawvclJ7m
            eBKufGMfQ5ru1jbgmL4gYRZiwRr2/e9flQnMl+Pe4EaGeXfAkuu2cxS85oMtUmLrwc/2WdrsC8Je
            +C2ciHKMKCnEet8eb8eqsUz5z3A6FVEpzv24O2vfxEESwNRcmWiiJ9C3v5aYu0v9DCLD/FpjP60E
            UePycWSlRDiAF7gh8sNnFSOyy1m0leTAnXKVKrfyWV7MxKY8/GeHtA==
            -----END CERTIFICATE-----
            """

    def get_cert(self):
        log_certificate_details(self.cert_new, "SSOComponents")
        return self.cert_new

    def get_public(self):
        return base64.b64encode(self.public.getEncoded())

    def get_private(self):
        return base64.b64encode(self.private.getEncoded())

    def get_certificate(self):
        # Using the newer certificate that's valid longer
        # Strip headers and whitespace to match the format of the original cert
        log_certificate_details(self.cert_new, "SSOComponents-RAW")
        cert_data = self.cert_new.replace("-----BEGIN CERTIFICATE-----", "").replace("-----END CERTIFICATE-----", "")
        processed_cert = cert_data.replace(os.linesep, "").replace(" ", "").replace("\n", "").replace("\r", "").replace("\t", "")
        return processed_cert

    def get_modulus(self):
        """Get current RSA modulus as base64 string"""
        import base64
        try:
            modulus_long = self.private.getModulus()
            # Convert Python long to hex string, then to bytes (Jython 2.7 compatible)
            hex_string = hex(modulus_long)[2:].rstrip('L')  # Remove '0x' prefix and 'L' suffix
            # Ensure even number of hex digits
            if len(hex_string) % 2:
                hex_string = '0' + hex_string
            # Convert hex string to bytes - Python 2/3 compatibility
            try:
                # Python 2
                modulus_bytes = hex_string.decode('hex')
            except AttributeError:
                # Python 3
                modulus_bytes = bytes.fromhex(hex_string)
            encoded_modulus = base64.b64encode(modulus_bytes)
            result = encoded_modulus.decode('utf-8') if isinstance(encoded_modulus, bytes) else encoded_modulus
            return result
        except Exception as e:
            # Fallback to a known working modulus for testing
            self.log.warning("Failed to get dynamic modulus, using fallback: {}".format(e))
            return "AL5DlwXXlOkcnBb75XuPojUo9d4oewoe3u8rAyx04s/bCA1aEN4VxEZ3IvWWOgEMsfIOVGe7iW+h9YYvcst35OzihmsT2rYfMcZsY5x2JW+alIVQL+Aav/iq796zRLhU0rTwDuWSq9pM8V9ZY8abP3twkpiytdwaTem9nxUHSUSq0wtac3NT8Basu5j13uWtLWH8EjCGrJHVhy3Z+7DwtgdBgvzT9V0C/K+pYfuNCBmBe07jPdtTyXlC7RAVSXEKZVgcCVYz/d1oSMKH7d77LwwunhGXNVCigItbKtphNF80ea33kFwaOVYcxp7zkNegU6JFi0MfQFfxy8p8h6BgQjofE79lZFAzCd9lj5X3k2cza8HyDJa4nn/q+f55D9i2AKBLsYs8xJhptPiMXHIp+n+AI96jLPXHM/jiLX6zK27rJlKhlG8X/3iCBNuHhZkGYd5Rm/qdclmepF4Hl8YwmskoTGJDttaqOMP3nnEUZ9vo5JlWP3m5DOgicmDU1FK4a2aS0V1zQZyeMlJpEt5mPv2TsY72oXV4/tBsUN7pI411RGTAEHJrgA0DmlQXdoe+MZ5T67RM0MHCgTdLyLRHJu+9VV0SD++F7lS3RQQltum+BvcLi5CFzL1u8sYUb7UPA9oxhEJCNyu7oeUAEJyW2CikwStwbIgNU0VJ2l8abyH"

    def get_exponent(self):
        """Get current RSA exponent as base64 string"""
        import base64
        try:
            exponent_long = self.private.getPublicExponent()
            # Convert Python long to hex string, then to bytes (Jython 2.7 compatible)
            hex_string = hex(exponent_long)[2:].rstrip('L')  # Remove '0x' prefix and 'L' suffix
            # Ensure even number of hex digits
            if len(hex_string) % 2:
                hex_string = '0' + hex_string
            # Convert hex string to bytes - Python 2/3 compatibility
            try:
                # Python 2
                exponent_bytes = hex_string.decode('hex')
            except AttributeError:
                # Python 3
                exponent_bytes = bytes.fromhex(hex_string)
            encoded_exponent = base64.b64encode(exponent_bytes)
            result = encoded_exponent.decode('utf-8') if isinstance(encoded_exponent, bytes) else encoded_exponent
            return result
        except Exception as e:
            # Fallback to a known working exponent for testing
            self.log.warning("Failed to get dynamic exponent, using fallback: {}".format(e))
            return "EAAB"

    def get_teams(self, tenant_id):
        assert type(tenant_id) == str

        url = "{}/v2/organizations/{}/teams?page=1&perPage=1".format(self.internal_url, tenant_id)

        return self.capella.do_internal_request(url, method="GET")

    def create_realm(self, tenant_id, team_id):
        assert type(tenant_id) == str

        url = "{}/v2/organizations/{}/realms".format(self.internal_url, tenant_id)
        signing_cert = self.get_cert()
        log_certificate_details(signing_cert, "SSOComponents-REALM")

        request_body = {
            "saml": {
                 'signInEndpoint': self.okta_base_url + "/app/integrator-2810815_ssotry_6/exkv90jce03yR8Hfx697/sso/saml",
                'signingCertificate': "{0}".format(signing_cert),
                "signatureAlgorithm": "rsa-sha256",
                "digestAlgorithm": "sha256",
                "protocolBinding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
            },
            'standard': 'SAML 2.0',
            'disableGroupMapping': False,
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

    def get_saml_request(self, login_url):
        # We can just return the output from this.
        response = requests.get(login_url)
        return response

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

    def decode_saml_request(self, saml):
        # We need to grab certain elements to construct a response
        xml = base64.b64decode(saml)
        xdoc = et.fromstring(xml)
        request_id = xdoc.attrib['ID']
        return request_id

    def send_saml_response(self, url, response, relay_state, cookies={}):
        self.log.info("Sending SAML response to URL: {}, response length: {}, relay_state: {}".format(url, len(response), relay_state))

        # Check cookies availability
        if not cookies:
            self.log.warning("No cookies provided for SAML response")

        # Parse URL for request
        from six.moves.urllib.parse import urlparse
        parsed_url = urlparse(url)

        # Test basic connectivity
        try:
            import socket
            sock = socket.create_connection((parsed_url.hostname, parsed_url.port or 443), timeout=10)
            sock.close()
        except Exception as conn_error:
            self.log.error("Basic TCP connection failed: {}".format(conn_error))

        # Test SSL handshake (Jython-compatible)
        try:
            import ssl
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

            sock = socket.create_connection((parsed_url.hostname, parsed_url.port or 443), timeout=10)
            try:
                ssock = context.wrap_socket(sock, server_hostname=parsed_url.hostname)
                ssock.close()
            finally:
                sock.close()
        except Exception as ssl_test_error:
            self.log.error("SSL handshake test failed: {}".format(ssl_test_error))

        # Prepare request data
        request_data = "SAMLResponse={}&RelayState={}".format(response, relay_state)

        # Try the request with detailed error handling
        try:
            # Disable urllib3 SSL warnings since we're using verify=False
            try:
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except:
                pass

            # Create a custom session with SSL adapter
            session = requests.Session()

            result = session.post(url,
                                  data=request_data,
                               headers={"Content-Type": "application/x-www-form-urlencoded"},
                               cookies=cookies,
                               verify=False,
                                  allow_redirects=False,
                                  timeout=30)
            self.log.info("SAML response sent successfully, status: {}, content length: {}".format(result.status_code, len(result.content)))
            return result

        except requests.exceptions.SSLError as ssl_error:
            self.log.error("SSL ERROR: {}".format(str(ssl_error)))

            # Try Java native HTTP connection as final fallback
            self.log.info("Attempting Java native HTTP connection fallback")
            try:
                from java.net import URL, URLConnection, HttpURLConnection
                from java.io import OutputStreamWriter, BufferedReader, InputStreamReader
                from javax.net.ssl import HttpsURLConnection

                # Create URL object
                java_url = URL(url)
                conn = java_url.openConnection()

                # Configure connection
                conn.setRequestMethod("POST")
                conn.setDoOutput(True)
                conn.setDoInput(True)
                conn.setConnectTimeout(30000)
                conn.setReadTimeout(30000)

                # Set headers
                conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
                conn.setRequestProperty("User-Agent", "python-requests/2.28.1")

                # Add cookies if present - handle RequestsCookieJar properly
                if cookies:
                    try:
                        # RequestsCookieJar - iterate over Cookie objects
                        cookie_parts = []
                        for cookie in cookies:
                            cookie_parts.append("{}={}".format(cookie.name, cookie.value))
                        cookie_header = "; ".join(cookie_parts)
                        conn.setRequestProperty("Cookie", cookie_header)
                    except AttributeError:
                        # Fallback for dict-like cookies
                        cookie_header = "; ".join(["{}={}".format(k, v) for k, v in cookies.items()])
                        conn.setRequestProperty("Cookie", cookie_header)
                else:
                    self.log.warning("No cookies provided to Java HTTP connection")

                # Send data
                output_writer = OutputStreamWriter(conn.getOutputStream())
                output_writer.write(request_data)
                output_writer.flush()
                output_writer.close()

                # Get response
                response_code = conn.getResponseCode()

                # Read response body
                if response_code >= 200 and response_code < 400:
                    input_stream = conn.getInputStream()
                else:
                    input_stream = conn.getErrorStream()

                if input_stream:
                    reader = BufferedReader(InputStreamReader(input_stream))
                    response_lines = []
                    line = reader.readLine()
                    while line is not None:
                        response_lines.append(line)
                        line = reader.readLine()
                    reader.close()

                    response_body = "\n".join(response_lines)
                else:
                    response_body = ""

                # Create a mock response object
                class JavaHttpResponse:
                    def __init__(self, status_code, content, headers=None):
                        self.status_code = status_code
                        self.content = content
                        self.headers = headers or {}
                        self.text = content

                java_response = JavaHttpResponse(response_code, response_body)

                if response_code >= 400:
                    self.log.error("Java HTTP fallback failed with status: {}, response: {}".format(response_code, response_body[:1000]))
                else:
                    self.log.info("Java HTTP fallback succeeded, status: {}".format(response_code))

                return java_response

            except Exception as java_fallback_error:
                self.log.error("Java HTTP fallback also failed: {}".format(java_fallback_error))

            raise ssl_error

        except requests.exceptions.ConnectionError as conn_error:
            self.log.error("Connection Error: {}".format(str(conn_error)))
            raise conn_error

        except Exception as general_error:
            self.log.error("General request error: {}".format(str(general_error)))
            raise general_error

    def continue_saml_response(self, url, cookies={}):
        return requests.get(url, verify=False, allow_redirects=False, cookies=cookies)


class SsoUtils:
    def __init__(self, url, secret_key, access_key, user, passwd, okta_base_url="https://integrator-2810815.okta.com/"):
        self.url = url.replace("cloud", "", 1)
        self.okta_base_url = okta_base_url.rstrip('/')  # Remove trailing slash for consistency
        self.capella_api = CapellaAPI("https://" + url, secret_key, access_key, user, passwd)

        # Setup logging
        self.log = logging.getLogger(self.__class__.__name__)
        if not self.log.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.setLevel(logging.INFO)

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
        identity provider and returns to Capella (via Auth0) with the user's details.
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
        header = self.capella_api.get_authorization_internal()
        self.log.info("Calling url, {}, with auth header: {}".format(url, header))
        resp = self.capella_api.do_internal_request(url, method="GET")
        return resp

    def update_realm_default_team(self, tenant_id, realm_id, team_id):
        """
        Updates realm default team
        """
        url = "{0}/v2/organizations/{1}/realms/{2}".format("https://" + self.url, tenant_id,
                                                           realm_id)
        body = {"defaultTeamId": team_id}
        resp = self.capella_api.do_internal_request(url, method="PATCH", params=json.dumps(body))
        return resp

    def update_realm_name(self, tenant_id, realm_id, realm_name):
        """
        Updates realm name
        """
        url = "{0}/v2/organizations/{1}/realms/{2}".format("https://" + self.url, tenant_id,
                                                           realm_id)
        body = {"name": realm_name}
        resp = self.capella_api.do_internal_request(url, method="PATCH", params=json.dumps(body))
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

    def update_team(self, tenant_id, team_id, body):
        """
        Update team in the organization
        """
        url = "{0}/v2/organizations/{1}/teams/{2}".format("https://" + self.url, tenant_id, team_id)
        resp = self.capella_api.do_internal_request(url, method="PUT", params=json.dumps(body))
        return resp

    def update_team_org_roles(self, tenant_id, team_id, body):
        """
        Update team org roles in the organization
        """
        url = "{0}/v2/organizations/{1}/teams/{2}/org-roles".format("https://" + self.url,
                                                                    tenant_id, team_id)
        resp = self.capella_api.do_internal_request(url, method="PUT", params=json.dumps(body))
        return resp

    def update_team_project_roles(self, tenant_id, team_id, body):
        """
        Update team project roles in the organization
        """
        url = "{0}/v2/organizations/{1}/teams/{2}/projects".format("https://" + self.url,
                                                                   tenant_id, team_id)
        resp = self.capella_api.do_internal_request(url, method="PUT", params=json.dumps(body))
        return resp

    def delete_team(self, tenant_id, team_id):
        """
        Delete team
        """
        url = "{0}/v2/organizations/{1}/teams/{2}".format("https://" + self.url, tenant_id, team_id)
        resp = self.capella_api.do_internal_request(url, method="DELETE")
        return resp

    def list_users(self, realm_id):
        """
        List SSO users of the given realm id
        """
        url = "{0}/v2/auth/sso/{1}/users?page=1&perPage=1&sortBy=name&sortDirection=asc".format("https://" + self.url,
                                                                                                realm_id)
        resp = self.capella_api.do_internal_request(url, method="GET")
        return resp

    def delete_users(self, realm_id, user_list):
        """
        Delete users of the given realm id
        """
        url = "{0}/v2/auth/sso/{1}/users".format("https://" + self.url, realm_id)
        body = {
            "userId": user_list
        }
        resp = self.capella_api.do_internal_request(url, method="DELETE", params=json.dumps(body))
        return resp

    def rotate_certificate(self, tenant_id, realm_id, body):
        """
        Rotates certificate
        """
        url = "{0}/v2/organizations/{1}/realms/{2}".format("https://" + self.url, tenant_id, realm_id)
        resp = self.capella_api.do_internal_request(url, method="PATCH", params=json.dumps(body))
        return resp

    def create_okta_application(self, okta_token, okta_account=None):
        if okta_account is None:
            okta_account = self.okta_base_url + "/"
        self.log.info("Creating Okta application at: {}".format(okta_account))
        header = {
            'Content-Type': 'application/json',
            'Authorization': 'SSWS ' + okta_token
        }
        body = {
            "label": "shaazin",
            "accessibility": {
                "selfService": False,
                "errorRedirectUrl": None,
                "loginRedirectUrl": None
            },
            "visibility": {
                "autoSubmitToolbar": False,
                "hide": {
                    "iOS": False,
                    "web": False
                }
            },
            "features": [],
            "signOnMode": "SAML_2_0",
            "credentials": {
                "userNameTemplate": {
                    "template": "${source.login}",
                    "type": "BUILT_IN"
                },
                "signing": {}
            },
            "settings": {
                "app": {},
                "notifications": {
                    "vpn": {
                        "network": {
                            "connection": "DISABLED"
                        },
                        "message": None,
                        "helpUrl": None
                    }
                },
                "manualProvisioning": False,
                "implicitAssignment": False,
                "signOn": {
                    "defaultRelayState": "",
                    "ssoAcsUrl": "https://placeholder",
                    "idpIssuer": "http://www.okta.com/${org.externalKey}",
                    "audience": "uri:placeholder",
                    "recipient": "https://placeholder",
                    "destination": "https://placeholder",
                    "subjectNameIdTemplate": "${user.userName}",
                    "subjectNameIdFormat": "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified",
                    "responseSigned": True,
                    "assertionSigned": True,
                    "signatureAlgorithm": "RSA_SHA256",
                    "digestAlgorithm": "SHA256",
                    "honorForceAuthn": True,
                    "authnContextClassRef": "urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport",
                    "spIssuer": None,
                    "requestCompressed": False,
                    "attributeStatements": [
                        {
                            "type": "EXPRESSION",
                            "name": "email",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "values": ["user.email"]
                        },
                        {
                            "type": "EXPRESSION",
                            "name": "given_name",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "values": ["user.firstName"]
                        },
                        {
                            "type": "EXPRESSION",
                            "name": "family_name",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "values": ["user.lastName"]
                        },
                        {
                            "type": "GROUP",
                            "name": "groups",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "filterType": "REGEX",
                            "filterValue": ".*"
                        }
                    ],
                    "inlineHooks": [],
                    "allowMultipleAcsEndpoints": False,
                    "acsEndpoints": [],
                    "samlSignedRequestEnabled": False,
                    "slo": {
                        "enabled": False
                    }
                }
            }
        }
        resp = requests.post(okta_account + "api/v1/apps",
                             data=json.dumps(body),
                             headers=header,
                             timeout=300, verify=False, allow_redirects=False)
        self.log.info("Okta app creation response status: {}".format(resp.status_code))
        assert resp.status_code == 200

        resp_content = json.loads(resp.content.decode())
        okta_app_id = resp_content["id"]
        self.log.info("Created Okta app with ID: {}".format(okta_app_id))
        idp_metadata_url = resp_content["_links"]["metadata"]["href"]
        self.log.info("Retrieved IDP metadata URL: {}".format(idp_metadata_url))

        resp = requests.get(idp_metadata_url,
                            headers=header,
                            timeout=300, verify=False, allow_redirects=False)
        self.log.info("Metadata fetch response status: {}".format(resp.status_code))
        assert resp.status_code == 200

        idp_metadata = resp.content.decode()
        self.log.info("Retrieved IDP metadata, length: {}".format(len(idp_metadata)))

        return okta_app_id, idp_metadata

    def assign_user(self, okta_token, okta_app_id, okta_account=None):
        if okta_account is None:
            okta_account = self.okta_base_url + "/"
        header = {
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Authorization': 'SSWS ' + okta_token
        }
        body = {"id": "00ucptbpbpCBQfhiS5d7",
                "scope": "USER",
                "credentials":
                    {"userName": "qe.security.testing@couchbase.com"}}
        resp = requests.post(
            okta_account + "api/v1/apps/" + okta_app_id + "/users",
            data=json.dumps(body),
            headers=header,
            timeout=300, verify=False, allow_redirects=False)

    def delete_okta_applications(self, okta_token, okta_account=None):
        if okta_account is None:
            okta_account = self.okta_base_url + "/"
        header = {
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Authorization': 'SSWS ' + okta_token
        }
        resp = requests.get(okta_account + "api/v1/apps",
                            headers=header,
                            timeout=300, verify=False, allow_redirects=False)
        assert resp.status_code == 200
        app_list = json.loads(resp.content.decode())
        for app in app_list:
            app_label = app["label"]
            if app_label == "shaazin":
                resp = requests.post(
                    okta_account + "api/v1/apps/" + app["id"] + "/lifecycle/deactivate",
                    headers=header,
                    timeout=300, verify=False, allow_redirects=False)
                assert resp.status_code == 200
                resp = requests.delete(
                    okta_account + "api/v1/apps/" + app["id"],
                    headers=header,
                    timeout=300, verify=False, allow_redirects=False)
                assert resp.status_code == 204

    def get_okta_app_metadata(self, okta_token, okta_app_id, okta_account=None):
        """
        Get IDP metadata from an existing Okta application.
        """
        if okta_account is None:
            okta_account = self.okta_base_url + "/"

        self.log.info("Getting metadata from existing Okta app: {}".format(okta_app_id))

        header = {
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Authorization': 'SSWS ' + okta_token
        }

        # Get app details
        app_url = "{}api/v1/apps/{}".format(okta_account, okta_app_id)
        self.log.info("Fetching app from: {}".format(app_url))
        resp = requests.get(app_url, headers=header, timeout=300, verify=False, allow_redirects=False)

        if resp.status_code != 200:
            self.log.error("Failed to get app. Status: {}".format(resp.status_code))
            raise Exception("Failed to get Okta app. Status: {}".format(resp.status_code))

        resp_content = json.loads(resp.content.decode())
        idp_metadata_url = resp_content["_links"]["metadata"]["href"]
        self.log.info("Metadata URL: {}".format(idp_metadata_url))

        # Get metadata
        resp = requests.get(idp_metadata_url, headers=header, timeout=300, verify=False, allow_redirects=False)
        if resp.status_code != 200:
            self.log.error("Failed to get metadata. Status: {}".format(resp.status_code))
            raise Exception("Failed to get metadata. Status: {}".format(resp.status_code))

        idp_metadata = resp.content.decode()
        self.log.info("Metadata retrieved (length: {})".format(len(idp_metadata)))
        return idp_metadata

    def update_okta_application(self, okta_token, callbackURL, entityId, okta_app_id,
                                okta_account=None):
        """
        Update only the ACS URL, audience, destination, and recipient in an existing Okta SAML app.
        Similar to update_okta_app_ip, this does a surgical update without changing other settings.
        """
        if okta_account is None:
            okta_account = self.okta_base_url + "/"

        header = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': 'SSWS ' + okta_token
        }

        # Step 1: GET the current app configuration
        self.log.info("Fetching current Okta app configuration for app_id: {}".format(okta_app_id))
        get_resp = requests.get("{0}api/v1/apps/{1}".format(okta_account, okta_app_id),
                                headers=header,
                                timeout=300, verify=False)

        if get_resp.status_code != 200:
            self.log.error("Failed to fetch Okta app: {} - {}".format(get_resp.status_code, get_resp.text))
            raise Exception("Failed to fetch Okta app configuration: {}".format(get_resp.status_code))

        app_config = get_resp.json()
        self.log.info("Successfully fetched Okta app configuration")

        # Step 2: Update only the SAML URLs in the signOn settings
        if 'settings' in app_config and 'signOn' in app_config['settings']:
            sign_on = app_config['settings']['signOn']

            # Update the four SAML URL fields
            old_acs = sign_on.get('ssoAcsUrl', 'N/A')
            old_audience = sign_on.get('audience', 'N/A')

            sign_on['ssoAcsUrl'] = callbackURL
            sign_on['audience'] = entityId
            sign_on['recipient'] = callbackURL
            sign_on['destination'] = callbackURL

            self.log.info("Updated ssoAcsUrl: {} -> {}".format(old_acs, callbackURL))
            self.log.info("Updated audience: {} -> {}".format(old_audience, entityId))
            self.log.info("Updated recipient: {}".format(callbackURL))
            self.log.info("Updated destination: {}".format(callbackURL))
        else:
            raise Exception("Invalid app configuration: missing settings.signOn")

        # Step 3: PUT the modified configuration back
        self.log.info("Updating Okta app with new SAML URLs")
        put_resp = requests.put("{0}api/v1/apps/{1}".format(okta_account, okta_app_id),
                                data=json.dumps(app_config),
                            headers=header,
                            timeout=300, verify=False, allow_redirects=False)

        if put_resp.status_code != 200:
            self.log.error("Failed to update Okta app: {} - {}".format(put_resp.status_code, put_resp.text))
            raise Exception("Failed to update Okta app: {}".format(put_resp.status_code))

        self.log.info("Successfully updated Okta app SAML URLs")
        return put_resp

    def idp_redirect(self, action, SAMLRequest, RelayState):
        """
        Browser-style redirect to IdP with SAML request.
        Extracts state token using robust regex patterns.
        """
        header = {
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        resp = requests.post(action,
                             data="SAMLRequest={0}&RelayState={1}".format(SAMLRequest, RelayState),
                             headers=header,
                             timeout=300, verify=False, allow_redirects=False)

        self.log.info("IdP redirect response status: {}".format(resp.status_code))
        assert resp.status_code == 200, "IdP redirect failed with status: {}".format(resp.status_code)

        # Extract state token more robustly using regex patterns (browser-style)
        content = resp.content.decode()
        state_token = ""

        # Pattern 1: Look for: var stateToken = 'TOKEN';
        pattern = r"var stateToken = '([^']+)'"
        match = re.search(pattern, content)
        if match:
            state_token = match.group(1)
            self.log.info("State token extracted using pattern 1: var stateToken = 'TOKEN'")
        else:
            # Pattern 2: Fallback - look for stateToken: 'TOKEN'
            pattern = r"stateToken:\s*'([^']+)'"
            match = re.search(pattern, content)
            if match:
                state_token = match.group(1)
                self.log.info("State token extracted using pattern 2: stateToken: 'TOKEN'")
            else:
                # Pattern 3: Original fallback - look for stateToken= in the line
                for line in content.split("\n"):
                    if "stateToken=" in line:
                        # Try to extract more carefully
                        token_match = re.search(r'stateToken["\s]*[:=]["\s]*["\']?([^"\';\s]+)', line)
                        if token_match:
                            state_token = token_match.group(1)
                            self.log.info("State token extracted using pattern 3: legacy fallback")
                            break

        if state_token:
            # Decode HTML entities (e.g., &#x2D; -> -)
            original_length = len(state_token)
            state_token = state_token.replace("\\x2D", "-")
            state_token = state_token.replace("\\x2d", "-")
            state_token = state_token.replace("\\x2B", "+")
            state_token = state_token.replace("\\x2b", "+")
            state_token = state_token.replace("\\x2F", "/")
            state_token = state_token.replace("\\x2f", "/")
            state_token = state_token.replace("\\x3D", "=")
            state_token = state_token.replace("\\x3d", "=")

            if len(state_token) != original_length:
                self.log.info("Decoded HTML entities in state token ({} -> {} chars)".format(original_length, len(state_token)))

            self.log.info("Successfully extracted state token (length: {})".format(len(state_token)))
            self.log.info("State token starts with: {}...".format(state_token[:20] if len(state_token) > 20 else state_token))
        else:
            self.log.error("Failed to extract state token from response")
            self.log.error("Searching for stateToken patterns in response...")
            if "var stateToken =" in content:
                self.log.error("Found 'var stateToken =' in response")
            if "stateToken:" in content:
                self.log.error("Found 'stateToken:' in response")
            if "stateToken=" in content:
                self.log.error("Found 'stateToken=' in response")
            # Show first 1000 chars of response for debugging
            self.log.error("Response preview: {}".format(content[:1000]))
            raise Exception("Could not extract state token from Okta response")

        # Extract cookies (browser behavior)
        cookie_dict = {}
        cookie_string = ""
        for i in resp.cookies:
            cookie_dict[i.name] = i.value
            cookie_string = cookie_string + i.name + "=" + i.value + "; "
        cookie_string = cookie_string[:-2]

        j_session_id = cookie_dict.get("JSESSIONID", "")
        if not j_session_id:
            self.log.warning("JSESSIONID not found in cookies")
            self.log.warning("Available cookies: {}".format(list(cookie_dict.keys())))
        else:
            self.log.info("JSESSIONID extracted: {}".format(j_session_id))

        # Extract the login form action URL from HTML
        # For Okta SAML apps, we need to extract the "fromURI" hidden field which contains the SAML app path
        form_action = ""
        from_uri = ""

        # Look for the fromURI hidden field value (this is where we need to redirect after auth)
        from_uri_match = re.search(r'<input[^>]*name="fromURI"[^>]*value="([^"]*)"', content)
        if from_uri_match:
            from_uri = from_uri_match.group(1)
            # Decode HTML entities
            from_uri = from_uri.replace("&#x2f;", "/").replace("&#x2F;", "/")
            self.log.info("Extracted fromURI (SAML app path): {}".format(from_uri))

        # For Okta, we should use the classic auth endpoint with redirect parameter
        # This maintains SAML context by including the app path
        if from_uri:
            form_action = "/api/v1/authn"
            self.log.info("Using Okta authn API with fromURI for SAML context")
        else:
            form_action = "/api/v1/authn"
            self.log.info("Using Okta authn API (no fromURI found)")

        # IMPORTANT: Return RelayState and form_action so we can submit the actual HTML form
        self.log.info("Preserving RelayState for later: {}".format(RelayState if RelayState else "(empty)"))
        return state_token, cookie_string, j_session_id, RelayState, form_action

    def idp_login(self, identifier, passcode, state_token, cookie_string, j_session_id, form_action):
        """
        Authenticate with Okta using the authn API.
        This uses the Okta Classic Authentication API to get a session token.
        """
        # Build the authn API URL
        if form_action.startswith("/"):
            url = self.okta_base_url.rstrip("/") + form_action
        else:
            url = form_action

        # Okta authn API expects JSON with username and password
        auth_data = {
            "username": identifier,
            "password": passcode
        }

        self.log.info("Authenticating with Okta authn API for user: {}".format(identifier))
        self.log.info("Authn API URL: {}".format(url))
        self.log.info("Cookie string: {}".format(cookie_string))

        headers = {
            'Cookie': cookie_string,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }

        self.log.info("Sending authn request")
        resp = requests.post(url, json=auth_data, headers=headers, allow_redirects=False)
        self.log.info("Authn response status code: {}".format(resp.status_code))
        self.log.info("Authn response headers: {}".format(dict(resp.headers)))

        # Log full response body
        response_text = resp.content.decode()
        self.log.info("Authn response body: {}".format(response_text))

        if resp.status_code == 200:
            response_json = json.loads(response_text)
            if response_json.get("status") == "SUCCESS":
                session_token = response_json.get("sessionToken")
                if session_token:
                    self.log.info("Authentication successful, got session token")
                    # Update cookies from response
                    for cookie in resp.cookies:
                        if cookie.name not in cookie_string:
                            cookie_string += "; {}={}".format(cookie.name, cookie.value)
                    self.log.info("Updated cookie string: {}".format(cookie_string))
                    return session_token, cookie_string
                else:
                    raise Exception("Authn succeeded but no sessionToken in response")
            else:
                raise Exception("Authn failed with status: {}".format(response_json.get("status")))
        else:
            self.log.error("Authn failed with status: {}".format(resp.status_code))
            self.log.error("Full error response: {}".format(response_text))
            raise Exception("Okta authn failed with status: {}".format(resp.status_code))

    def get_saml_response(self, session_token, cookie_string, sso_service_url):
        """
        Browser-style: Get SAML Response from IdP using session token.
        After authentication, we use the sessionToken to request the SAML app.
        The sessionToken parameter tells Okta to establish a session and return the SAML response.
        SAML context is maintained - InResponseTo should be present.
        """
        self.log.info("Getting SAML response from SSO service URL: {}".format(sso_service_url))
        self.log.info("Using session token to establish authenticated session")

        # Append sessionToken as query parameter to the SSO service URL
        if "?" in sso_service_url:
            full_url = "{}&sessionToken={}".format(sso_service_url, session_token)
        else:
            full_url = "{}?sessionToken={}".format(sso_service_url, session_token)

        header = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'cookie': cookie_string,
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }

        self.log.info("Requesting SAML app with sessionToken")
        self.log.info("Full request URL: {}...".format(full_url[:100]))  # Truncate token in log
        self.log.info("Full cookie string: {}".format(cookie_string))
        resp = requests.get(full_url, headers=header, allow_redirects=False)
        self.log.info("SAML response page status code: {}".format(resp.status_code))
        self.log.info("SAML response page headers: {}".format(dict(resp.headers)))

        if resp.status_code != 200:
            self.log.error("Failed to get SAML response page. Status: {}".format(resp.status_code))
            raise Exception("Failed to get SAML response page. Status: {}".format(resp.status_code))

        # Parse HTML to extract SAMLResponse and form action URL
        response_content = resp.content.decode()
        self.log.info("Response content length: {}".format(len(response_content)))
        self.log.info("Response content (first 3000 chars): {}".format(response_content[:3000]))

        # Extract form action URL (where to POST the SAMLResponse)
        form_action = ""
        action_match = re.search(r'<form[^>]*action="([^"]*)"', response_content)
        if action_match:
            form_action = action_match.group(1)
            self.log.info("Extracted form action URL (raw): {}".format(form_action))

            # Decode HTML entities in the URL
            form_action = form_action.replace("&#x3a;", ":")
            form_action = form_action.replace("&#x3A;", ":")
            form_action = form_action.replace("&#x2f;", "/")
            form_action = form_action.replace("&#x2F;", "/")
            form_action = form_action.replace("&#x3f;", "?")
            form_action = form_action.replace("&#x3F;", "?")
            form_action = form_action.replace("&#x3d;", "=")
            form_action = form_action.replace("&#x3D;", "=")
            form_action = form_action.replace("&#x2d;", "-")
            form_action = form_action.replace("&#x2D;", "-")

            self.log.info("Decoded form action URL: {}".format(form_action))
        else:
            self.log.warning("Form action URL not found in HTML")

        SAMLResponse = ""
        if 'SAMLResponse' in response_content:
            self.log.info("Found 'SAMLResponse' in content, attempting extraction")

            # Try multiple patterns to extract SAML response
            patterns = [
                r'<input[^>]*name=["\']?SAMLResponse["\']?[^>]*value=["\']([^"\']*)["\'][^>]*>',
                r'<input[^>]*value=["\']([^"\']*)["\'][^>]*name=["\']?SAMLResponse["\']?[^>]*>',
                r'name=["\']SAMLResponse["\'][^>]*value=["\']([^"\']*)["\']',
                r'value=["\']([^"\']*)["\'][^>]*name=["\']SAMLResponse["\']',
                r'"SAMLResponse"[^"]*"([^"]*)"',
                r'SAMLResponse["\'\s]*[=:]["\'\s]*([^"\'\\s><]+)'
            ]

            for i, pattern in enumerate(patterns):
                match = re.search(pattern, response_content, re.IGNORECASE | re.DOTALL)
                if match:
                    SAMLResponse = match.group(1)
                    if SAMLResponse and len(SAMLResponse) > 10:  # Basic validation
                        self.log.info("Extracted SAML response using pattern {}".format(i))
                        break

        if not SAMLResponse:
            self.log.error("SAMLResponse not found in HTML")
            self.log.error("Full HTML response: {}".format(response_content))
            raise Exception("SAMLResponse not found in IdP response HTML")

        # Extract RelayState (also required for Auth0 POST)
        RelayState = ""
        if 'RelayState' in response_content:
            self.log.info("Found 'RelayState' in content, attempting extraction")

            relay_patterns = [
                r'<input[^>]*name=["\']?RelayState["\']?[^>]*value=["\']([^"\']*)["\'][^>]*>',
                r'<input[^>]*value=["\']([^"\']*)["\'][^>]*name=["\']?RelayState["\']?[^>]*>',
                r'name=["\']RelayState["\'][^>]*value=["\']([^"\']*)["\']',
                r'value=["\']([^"\']*)["\'][^>]*name=["\']RelayState["\']',
                # Additional patterns for different HTML formatting
                r'name="RelayState"\s+value="([^"]*)"',
                r'value="([^"]*)"\s+name="RelayState"',
            ]

            for i, pattern in enumerate(relay_patterns):
                match = re.search(pattern, response_content, re.IGNORECASE | re.DOTALL)
                if match:
                    RelayState = match.group(1)
                    if RelayState:
                        self.log.info("Extracted RelayState using pattern {}: {}".format(i, RelayState))
                        break
                else:
                    self.log.info("Pattern {} did not match".format(i))

            # If no match, show a snippet of the RelayState area for debugging
            if not RelayState:
                self.log.warning("RelayState not extracted, searching for context...")
                relay_context_match = re.search(r'.{0,100}RelayState.{0,100}', response_content, re.IGNORECASE | re.DOTALL)
                if relay_context_match:
                    self.log.warning("RelayState context in HTML: {}".format(relay_context_match.group(0)))
        else:
            self.log.warning("'RelayState' keyword not found in response HTML")

        # Decode HTML entities (browser behavior)
        SAMLResponse = SAMLResponse.replace("&#x2b;", "+")
        SAMLResponse = SAMLResponse.replace("&#x3d;", "=")
        SAMLResponse = SAMLResponse.replace("&#x2f;", "/")
        SAMLResponse = SAMLResponse.replace("&#x2B;", "+")
        SAMLResponse = SAMLResponse.replace("&#x3D;", "=")
        SAMLResponse = SAMLResponse.replace("&#x2F;", "/")

        self.log.info("SAML Response extracted successfully (length: {})".format(len(SAMLResponse)))
        self.log.info("SAML Response preview: {}...".format(SAMLResponse[:50]))

        return SAMLResponse, RelayState, form_action

    def saml_consume_url(self, callbackURL, cookie_string, SAMLResponse, RelayState=""):
        """
        Browser-style: POST SAML assertion to Capella callback URL using Java HttpsURLConnection.
        Uses Java's native HTTPS client to properly respect the SSL configuration set at module import.
        """
        self.log.info("Posting SAML response to callback URL: {}".format(callbackURL))
        self.log.info("SAML Response length: {}".format(len(SAMLResponse)))
        self.log.info("RelayState: {}".format(RelayState if RelayState else "(empty)"))

        # Verify SSL configuration
        try:
            import java.lang.System as System
            current_protocols = System.getProperty("https.protocols")
            self.log.info("Current SSL protocols: {}".format(current_protocols))
        except Exception as e:
            self.log.warning("Could not verify SSL properties: {}".format(e))

        # URL encode the SAML Response and RelayState (browser form submission behavior)
        SAMLResponse_encoded = six.moves.urllib.parse.quote(SAMLResponse)
        self.log.info("SAML Response URL encoded (length: {})".format(len(SAMLResponse_encoded)))

        RelayState_encoded = six.moves.urllib.parse.quote(RelayState) if RelayState else ""
        if RelayState:
            self.log.info("RelayState URL encoded (length: {})".format(len(RelayState_encoded)))

        try:
            # Use Java's HttpsURLConnection for proper SSL handling in Jython
            from java.net import URL
            from java.io import OutputStreamWriter, BufferedReader, InputStreamReader
            from javax.net.ssl import HttpsURLConnection

            self.log.info("Using Java HttpsURLConnection for Auth0 POST request")

            # Create URL object
            url = URL(callbackURL)
            connection = url.openConnection()

            # Configure connection as POST
            connection.setRequestMethod("POST")
            connection.setDoOutput(True)
            connection.setDoInput(True)
            connection.setInstanceFollowRedirects(False)  # Don't follow redirects automatically

            # Set headers
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
            connection.setRequestProperty("Cookie", cookie_string)
            connection.setRequestProperty("Accept", "*/*")
            connection.setRequestProperty("Accept-Encoding", "gzip, deflate, br")
            connection.setRequestProperty("Connection", "keep-alive")
            connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")

            # Prepare POST data (browser form submission includes both SAMLResponse and RelayState)
            if RelayState:
                post_data = "SAMLResponse={0}&RelayState={1}".format(SAMLResponse_encoded, RelayState_encoded)
            else:
                post_data = "SAMLResponse={0}".format(SAMLResponse_encoded)
            self.log.info("Sending POST request to Auth0 callback (data length: {})...".format(len(post_data)))

            # Write POST data
            out = OutputStreamWriter(connection.getOutputStream())
            out.write(post_data)
            out.flush()
            out.close()

            # Get response code
            response_code = connection.getResponseCode()
            self.log.info("SAML consume response status: {}".format(response_code))

            # Get response headers
            headers = {}
            i = 0
            while True:
                header_key = connection.getHeaderFieldKey(i)
                header_value = connection.getHeaderField(i)
                if header_key is None and i > 0:
                    break
                if header_key:
                    headers[header_key] = header_value
                i += 1

            self.log.info("Response headers: {}".format(headers))

            # Read response body (if any)
            response_body = ""
            try:
                if response_code < 400:
                    reader = BufferedReader(InputStreamReader(connection.getInputStream()))
                else:
                    reader = BufferedReader(InputStreamReader(connection.getErrorStream()))

                line = reader.readLine()
                while line is not None:
                    response_body += line + "\n"
                    line = reader.readLine()
                reader.close()

                self.log.info("Response body length: {}".format(len(response_body)))
            except Exception as e:
                self.log.info("No response body to read: {}".format(e))

            # Check for redirect (typical for SAML flow)
            if 'Location' in headers:
                self.log.info("SAML consume redirecting to: {}".format(headers['Location']))

            # Check for session cookie (successful authentication)
            if 'Set-Cookie' in headers:
                self.log.info("Session cookie received in response")
            else:
                self.log.warning("No session cookie in response")

            # Check for error responses
            if response_code >= 400:
                self.log.error("SAML consume failed with status: {}".format(response_code))
                self.log.error("Full response body: {}".format(response_body))
            else:
                self.log.info("SAML consume request completed successfully")

            # Create a requests-like response object for compatibility
            class JavaResponse:
                def __init__(self, status_code, headers, content):
                    self.status_code = status_code
                    self.headers = headers
                    self.content = content
                    self.text = content

            connection.disconnect()
            return JavaResponse(response_code, headers, response_body)

        except Exception as e:
            self.log.error("SAML consume request failed: {}".format(e))
            self.log.error("Error type: {}".format(type(e).__name__))
            import traceback
            self.log.error("Full traceback: {}".format(traceback.format_exc()))
            raise
