import base64
import random
import xml.dom.expatbuilder
from datetime import datetime, timedelta
from xml.dom import minidom


class SAMLResponse:
    def __init__(self, acs="", spname="", requestId=""):
        self.doc = minidom.Document()
        self.instant = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        self.expiry = (datetime.utcnow() + timedelta(days=1)).replace(microsecond=0).isoformat() + "Z"
        self.acs = acs
        self.spname = spname
        self.requestId = requestId
        self.modulous = "AL5DlwXXlOkcnBb75XuPojUo9d4oewoe3u8rAyx04s/bCA1aEN4VxEZ3IvWWOgEMsfIOVGe7iW+h9YYvcst35Ozihmsf" \
                        "T2rYfMcZsY5x2JW+alIVQL+Aav/iq796zRLhU0rTwDuWSq9pM8V9ZY8abP3twkpiytdwaTem9nxUHSUSq0wtac3NT8Ba" \
                        "su5j13uWtLWH8EjCGrJHVhy3Z+7DwtgdBgvzT9V0C/K+pYfuNCBmBe07jPdtTyXlC7RAVSXEKZVgcCVYz/d1oSMKH7d7" \
                        "7LwwunhGXNVCigItbKtphNF80ea33kFwaOVYcxp7zkNegU6JFi0MfQFfxy8p8h6BgQjofE79lZFAzCd9lj5X3k2cza8H" \
                        "yDJa4nn/q+f55D9i2AKBLsYs8xJhptPiMXHIp+n+AI96jLPXHM/jiLX6zK27rJlKhlG8X/3iCBNuHhZkGYd5Rm/qdclm" \
                        "epF4Hl8YwmskoTGJDttaqOMP3nnEUZ9vo5JlWP3m5DOgicmDU1FK4a2aS0V1zQZyeMlJpEt5mPv2TsY72oXV4/tBsUN7" \
                        "pI411RGTAEHJrgA0DmlQXdoe+MZ5T67RM0MHCgTdLyLRHJu+9VV0SD++F7lS3RQQltum+BvcLi5CFzL1u8sYUb7UPA9o" \
                        "xhEJCNyu7oeUAEJyW2CikwStwbIgNU0VJ2l8abyH"
        self.exponent = "EAAB"

    def generateRoot(self):
        self.id = random.getrandbits(128)

        root = self.doc.createElement("samlp:Response")
        root.setAttribute("xmlns:samlp", "urn:oasis:names:tc:SAML:2.0:protocol")
        root.setAttribute("xmlns:saml", "urn:oasis:names:tc:SAML:2.0:assertion")
        root.setAttribute("ID", "_%032X" % self.id)
        root.setAttribute("Version", "2.0")
        root.setAttribute("InstantIssue", self.instant)
        root.setAttribute("Destination", self.acs)
        root.setAttribute("InResponseTo", self.requestId)
        self.rootDoc = root
        self._add_issuer(self.rootDoc)
        self._add_status(self.rootDoc)
        self._add_assertion(self.rootDoc)
        self._add_issuer(self.assertions)
        self._add_conditions(self.assertions)
        self._add_authn(self.assertions)
        self._add_attribute_statement(self.assertions)
        self.doc.appendChild(self.rootDoc)

    def _add_issuer(self, parent):
        issuer = self.doc.createElement("saml:Issuer")
        iss = self.doc.createTextNode("http://capella.test/idp")
        issuer.appendChild(iss)
        parent.appendChild(issuer)

    def _add_status(self, parent):
        status = self.doc.createElement("samlp:Status")
        statusCode = self.doc.createElement("samlp:StatusCode")
        statusCode.setAttribute("Value", "urn:oasis:names:tc:SAML:2.0:status:Success")
        status.appendChild(statusCode)
        parent.appendChild(status)

    def _add_assertion(self, parent):
        assertion = self.doc.createElement("saml:Assertion")
        assertion.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        assertion.setAttribute("xmlns:xs", "http://www.w3.org/2001/XMLSchema")
        assertion.setAttribute("ID", "_%032X" % random.getrandbits(128))
        assertion.setAttribute("Version", "2.0")
        assertion.setAttribute("InstantIssue", self.instant)

        self.assertions = assertion
        parent.appendChild(self.assertions)

    def _add_conditions(self, parent):
        conditions = self.doc.createElement("saml:Conditions")
        audienceRestriction = self.doc.createElement("saml:AudienceRestriction")
        audience = self.doc.createElement("saml:Audience")

        conditions.setAttribute("NotBefore", self.instant)
        conditions.setAttribute("NotOnOrAfter", self.expiry)

        audience.appendChild(self.doc.createTextNode(self.spname))

        audienceRestriction.appendChild(audience)
        conditions.appendChild(audienceRestriction)

        parent.appendChild(conditions)

    def _add_authn(self, parent):
        authnStatement = self.doc.createElement("saml:AuthnStatement")
        authnContext = self.doc.createElement("saml:AuthnContext")
        authnContextClass = self.doc.createElement("saml:AuthnContextClassRef")

        authnContextClass.appendChild(self.doc.createTextNode("urn:oasis:names:tc:SAML:2.0:ac:classes:Password"))
        authnContext.appendChild(authnContextClass)

        authnStatement.setAttribute("AuthInstant", self.instant)
        authnStatement.setAttribute("SessionNotOnOrAfter", self.expiry)
        authnStatement.setAttribute("SessionIndex", "_%032X" % random.getrandbits(128))

        authnStatement.appendChild(authnContext)

        parent.appendChild(authnStatement)

    def _add_attribute_statement(self, parent):
        attribute_statement = self.doc.createElement("saml:AttributeStatement")

        self.attribute_statement = attribute_statement

        parent.appendChild(attribute_statement)

    def subject(self, user):
        subject = self.doc.createElement("saml:Subject")
        name = self.doc.createElement("saml:NameID")
        confirmation = self.doc.createElement("saml:SubjectConfirmation")
        confirmationData = self.doc.createElement("saml:SubjectConfirmationData")

        name.setAttribute("SPNameQualifier", self.spname)
        name.setAttribute("Format", "urn:oasis:names:tc:SAML:2.0:nameid-format:transient")
        name.appendChild(self.doc.createTextNode(user))

        confirmationData.setAttribute("NotOnOrAfter", self.expiry)
        confirmationData.setAttribute("Recipient", self.acs)
        confirmationData.setAttribute("InResponseTo", self.requestId)

        confirmation.setAttribute("Method", "urn:oasis:names:tc:SAML:2.0:cm:bearer")

        confirmation.appendChild(confirmationData)
        subject.appendChild(name)
        subject.appendChild(confirmation)
        self.assertions.appendChild(subject)

    def attribute(self, name, values=[]):
        attr = self.doc.createElement("saml:Attribute")

        attr.setAttribute("Name", name)
        attr.setAttribute("NameFormat", "urn:oasis:names:tc:SAML:2.0:attrname-format:basic")

        for val in values:
            attr_val = self.doc.createElement("saml:AttributeValue")

            attr_val.setAttribute("xsi:type", "xs:string")
            attr_val.appendChild(self.doc.createTextNode(val))

            attr.appendChild(attr_val)

        self.attribute_statement.appendChild(attr)

    def add_digest(self, digest, cert):
        self.cert = cert
        attr = self.doc.createElement("ds:Signature")
        si = self.doc.createElement("ds:SignedInfo")
        cm = self.doc.createElement("ds:CanonicalizationMethod")
        sm = self.doc.createElement("ds:SignatureMethod")
        ref = self.doc.createElement("ds:Reference")
        tfm = self.doc.createElement("ds:Transforms")
        tf = self.doc.createElement("ds:Transform")
        tf2 = self.doc.createElement("ds:Transform")
        dm = self.doc.createElement("ds:DigestMethod")
        dv = self.doc.createElement("ds:DigestValue")

        sv = self.doc.createElement("ds:SignatureValue")
        ki = self.doc.createElement("ds:KeyInfo")

        cm.setAttribute("Algorithm", "http://www.w3.org/2006/12/xml-c14n11")
        sm.setAttribute("Algorithm", "http://www.w3.org/2000/09/xmldsig#rsa-sha1")
        ref.setAttribute("URI", "#_%032X" % self.id)

        dm.setAttribute("Algorithm", "http://www.w3.org/2000/09/xmldsig#sha1")
        dv.appendChild(self.doc.createTextNode(digest))

        tf.setAttribute("Algorithm", "http://www.w3.org/2000/09/xmldsig#enveloped-signature")
        tf2.setAttribute("Algorithm", "http://www.w3.org/2006/12/xml-c14n11")
        tfm.appendChild(tf)
        tfm.appendChild(tf2)

        ref.appendChild(tfm)
        ref.appendChild(dm)
        ref.appendChild(dv)

        si.appendChild(cm)
        si.appendChild(sm)
        si.appendChild(ref)

        attr.appendChild(si)
        attr.appendChild(sv)
        attr.appendChild(ki)

        attr.setAttribute("xmlns:ds", "http://www.w3.org/2000/09/xmldsig#")

        self.signed_info = si
        self.signature = attr
        self.key_info = ki
        self.sigval = sv
        self.rootDoc.appendChild(attr)

    def add_signature(self, signature):
        self.sigval.appendChild(self.doc.createTextNode(signature))

        xd = self.doc.createElement("ds:X509Data")
        xc = self.doc.createElement("ds:X509Certificate")
        kv = self.doc.createElement("ds:KeyValue")
        rkv = self.doc.createElement("ds:RSAKeyValue")
        mod = self.doc.createElement("ds:Modulus")
        exp = self.doc.createElement("ds:Exponent")

        mod.appendChild(self.doc.createTextNode(self.modulous))
        exp.appendChild(self.doc.createTextNode(self.exponent))
        rkv.appendChild(mod)
        rkv.appendChild(exp)
        kv.appendChild(rkv)

        xc.appendChild(self.doc.createTextNode(self.cert))
        xd.appendChild(xc)
        # self.key_info.appendChild(kv)
        self.key_info.appendChild(xd)

    def to_string(self):
        return self.node_to_string(self.rootDoc)

    def signed_info_to_string(self):
        return self.node_to_string(self.signed_info, False)

    def node_to_string(self, node, keep_ns=True):
        if node.nodeType == xml.dom.expatbuilder.TEXT_NODE:
            return node.nodeValue
        if keep_ns:
            name = node.nodeName
        else:
            name = node.localName
        strng = ""
        strng += "<" + name
        if node.hasAttributes():
            xmlns = []
            others = []
            for attr in node.attributes.values():
                if attr.name.startswith("xmlns"):
                    xmlns.append((attr.name, attr.value))
                else:
                    others.append((attr.name, attr.value))
            xmlns.sort(key=lambda x: str.lower(x[0]))
            others.sort(key=lambda x: str.lower(x[0]))
            for ns in xmlns:
                strng += " " + ns[0] + "=\"" + ns[1] + "\""
            for att in others:
                strng += " " + att[0] + "=\"" + att[1] + "\""

        strng += ">"
        nodes = []
        for child in node.childNodes:
            if child.nodeType == xml.dom.expatbuilder.TEXT_NODE:
                strng += self.node_to_string(child, keep_ns)
            else:
                nodes.append(child)

        nodes.sort(key=lambda x: str.lower(node.nodeName))
        for child in nodes:
            strng += self.node_to_string(child, keep_ns)
        strng += "</" + name + ">"

        return strng

    def to_base64(self):
        return base64.urlsafe_b64encode(self.to_string())


if __name__ == "__main__":
    s = SAMLResponse(requestId="special_doc", acs="http://acs/", spname="http://spname/")
    s.generateRoot()
    s.subject("user1")
    s.attribute("uid", ["user1"])
    s.attribute("mail", ["user1@capella.test"])
    s.add_digest("test_dgst", "cert_data")
    s.add_signature("test_sig")
    print s.to_string()