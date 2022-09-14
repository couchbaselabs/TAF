# -*- coding: utf-8 -*-

import json
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI


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
    