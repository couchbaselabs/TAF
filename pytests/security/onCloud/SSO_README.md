# Using the SSO Testing Utils

## SAMLResponse

### Initialising

```python
from .saml_response import SAMLResponse

def your_test(self, some_argument):
    s = SAMLResponse(requestId=id, spname=self.realm_entity, acs=self.realm_callback)
    s.generateRoot()
    s.subject("test-user1")
    s.attribute("uid", ["test-user1"])
    s.attribute("mail", ["test-user1@capella.test"])
```

Where the requestId has come from parsing the SAML Request, and the other details are known to the user, see [existing
usage](./sso_test.py) for more information