# -*- coding: utf-8 -*-

from pytests.basetestcase import BaseTestCase
from pytests.security.onCloud.sso_utils import SsoUtils


class SsoTests(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.secret_key = self.input.capella.get("secret_key")
        self.access_key = self.input.capella.get("access_key")
        self.sso = SsoUtils(self.url, self.secret_key, self.access_key, self.user, self.passwd)
