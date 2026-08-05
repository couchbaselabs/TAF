"""
Created on July 2026

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

import random

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class DatabaseRoleBase(GetCluster):

    def setUp(self, nomenclature="DatabaseRoles_Base"):
        GetCluster.setUp(self, nomenclature)

        self.expected_res = {
            "name": self.prefix + "role" + str(random.randint(1, 10000)),
            "description": "",
            "access": [
                    {
                        "privileges": [
                            "analyticsAdmin"
                        ]
                    }
                ]
        }

        self.log.info("Creating Database Role for the test")
        res = self.capellaAPI.cluster_ops_apis.create_database_role(
            self.organisation_id, self.project_id, self.cluster_id,
            self.expected_res["name"], self.expected_res["access"],
            self.expected_res["description"])
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_database_role(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"], self.expected_res["access"],
                self.expected_res["description"])
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating Database Role for the test.")
        self.log.info("Database Role created successfully.")
        self.role_id = res.json()["id"]

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        result = self.capellaAPI.cluster_ops_apis.list_database_roles(
            self.organisation_id, self.project_id, self.cluster_id)
        if result.status_code == 429:
            self.handle_rate_limit(int(result.headers["Retry-After"]))
            result = self.capellaAPI.cluster_ops_apis.list_database_roles(
                self.organisation_id, self.project_id, self.cluster_id)

        role_ids = []
        if result.status_code == 200:
            data = result.json().get("data", [])
            role_ids = [r["id"] for r in data]

        if hasattr(self, "role_id") and self.role_id and \
                self.role_id not in role_ids:
            role_ids.append(self.role_id)

        for role_id in role_ids:
            self.log.info("Deleting Database Role: {}".format(role_id))
            res = self.capellaAPI.cluster_ops_apis.delete_database_role(
                self.organisation_id, self.project_id, self.cluster_id,
                role_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.delete_database_role(
                    self.organisation_id, self.project_id, self.cluster_id,
                    role_id)
            if res.status_code not in [200, 202, 204, 404]:
                self.log.error("Failed to delete Database Role {}: {}".format(
                    role_id, res.content))

        super(DatabaseRoleBase, self).tearDown()
