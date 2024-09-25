# -*- coding: utf-8 -*-

import json

from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI


class TestFeatureFlagsAPIUtils:
    def __init__(self, url, secret_key, access_key, user, passwd):
        self.url = url.replace("cloud", "", 1)
        self.capella_api = CapellaAPI("https://" + url, secret_key, access_key, user, passwd)

    def list_global_feature_flag(self, flag_name):
        url = "{}/internal/support/features/flags/{}".format("https://" + self.url, flag_name)
        return self.capella_api.do_internal_request(url, method="GET")

    def list_global_feature_flag_specific(self, flag_name):
        url = "{}/internal/support/features/flags/{}".format("https://" + self.url, flag_name)
        return self.capella_api.do_internal_request(url, method="GET")

    def create_global_feature_flag(self, flag_name, payload):
        url = "{}/internal/support/features/flags/{}".format("https://" + self.url, flag_name)
        return self.capella_api.do_internal_request(url, method="POST", params=json.dumps(payload))

    def update_global_feature_flag(self, flag_name, payload):
        url = "{}/internal/support/features/flags/{}".format("https://" + self.url, flag_name)
        return self.capella_api.do_internal_request(url, method="PUT", params=json.dumps(payload))

    def delete_global_feature_flag(self, flag_name):
        url = "{}/internal/support/features/flags/{}".format("https://" + self.url, flag_name)
        return self.capella_api.do_internal_request(url, method="DELETE")

    def list_tenant_feature_flags_internal(self, tenant_id):
        url = "{}/internal/support/features/{}/flags".format("https://" + self.url, tenant_id)
        return self.capella_api.do_internal_request(url, method="GET")

    def list_tenant_feature_flags_internal_specific(self, tenant_id, flag_name):
        url = "{}/internal/support/features/{}/flags?flags={}".format("https://" + self.url, tenant_id, flag_name)
        return self.capella_api.do_internal_request(url, method="GET")

    def create_tenant_feature_flag(self, tenant_id, flag_name, payload):
        url = "{}/internal/support/features/{}/flags/{}".format("https://" + self.url, tenant_id, flag_name)
        return self.capella_api.do_internal_request(url, method="POST", params=json.dumps(payload))

    def update_tenant_feature_flag(self, tenant_id, flag_name, payload):
        url = "{}/internal/support/features/{}/flags/{}".format("https://" + self.url, tenant_id, flag_name)
        return self.capella_api.do_internal_request(url, method="PUT", params=json.dumps(payload))

    def delete_tenant_feature_flag(self, tenant_id, flag_name):
        url = "{}/internal/support/features/{}/flags/{}".format("https://" + self.url, tenant_id, flag_name)
        return self.capella_api.do_internal_request(url, method="DELETE")

    def initialize_feature_flags_from_launchdarkly(self):
        url = "{}/internal/support/features/flags/initialize".format("https://" + self.url)
        return self.capella_api.do_internal_request(url, method="POST")
