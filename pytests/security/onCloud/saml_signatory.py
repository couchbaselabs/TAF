import base64
import hashlib
import saml_response


class SAMLSignatory:
    def __init__(self):
        pass

    def digest(self, input):
        # et = ET.from_string(input)
        # output = StringIO.StringIO()
        # et.write_c14n(output)
        m = hashlib.sha1()
        m.update(input)
        return base64.b64encode(m.digest())


if __name__ == "__main__":
    s = saml_response.SAMLResponse()
    s.generateRoot()
    s.subject("user1")
    ss = SAMLSignatory()
    print ss.digest(s.to_string())