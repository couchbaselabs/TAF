"""
Created on July 2026

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class DatabaseCredentialBase(GetCluster):

    def setUp(self, nomenclature="DatabaseCredentials_Base"):
        GetCluster.setUp(self, nomenclature)

        self.access = [
            {
                "privileges": ["analyticsAdmin"],
            }
        ]
        self.expected_res = {
            "name": self.prefix + "cred",
            "password": "Mathematics12@",
            "credentialType": "advanced",
            "userRoles": [
                self.prefix + "cred-role"
            ]
        }

        self.log.info("Creating a Database Role for advanced credential tests")
        role_access = [
            {
                "privileges": ["analyticsAdmin"]
            }
        ]
        res = self.capellaAPI.cluster_ops_apis.create_database_role(
            self.organisation_id, self.project_id, self.cluster_id,
            self.prefix + "cred-role", role_access, "Credential test role")
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_database_role(
                self.organisation_id, self.project_id, self.cluster_id,
                self.prefix + "cred-role", role_access, "Credential test role")
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating Database Role for credential tests.")
        self.role_id = res.json()["id"]
        self.role_name = self.prefix + "cred-role"
        self.log.info("Database Role created: {}".format(self.role_id))


    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)

        result = self.capellaAPI.cluster_ops_apis.list_database_users(
            self.organisation_id, self.project_id, self.cluster_id)
        if result.status_code == 429:
            self.handle_rate_limit(int(result.headers["Retry-After"]))
            result = self.capellaAPI.cluster_ops_apis.list_database_users(
                self.organisation_id, self.project_id, self.cluster_id)

        user_ids = []
        if result.status_code == 200:
            data = result.json().get("data", [])
            user_ids = [u["id"] for u in data]

        if hasattr(self, "user_id") and self.user_id and \
                self.user_id not in user_ids:
            user_ids.append(self.user_id)

        for user_id in user_ids:
            self.log.info("Deleting Database Credential: {}".format(user_id))
            res = self.capellaAPI.cluster_ops_apis.delete_database_user(
                self.organisation_id, self.project_id, self.cluster_id,
                user_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.delete_database_user(
                    self.organisation_id, self.project_id, self.cluster_id,
                    user_id)
            if res.status_code not in [200, 202, 204, 404]:
                self.log.error(
                    "Failed to delete Database Credential {}: {}".format(
                        user_id, res.content))

        if hasattr(self, "role_id") and self.role_id:
            self.log.info("Deleting Database Role: {}".format(self.role_id))
            res = self.capellaAPI.cluster_ops_apis.delete_database_role(
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.delete_database_role(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.role_id)
            if res.status_code not in [200, 202, 204, 404]:
                self.log.error(
                    "Failed to delete Database Role {}: {}".format(
                        self.role_id, res.content))

        super(DatabaseCredentialBase, self).tearDown()
