"""
Created on July 03, 2026

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class AIWorkflowBase(GetCluster):

    def setUp(self, nomenclature="AIWorkflows_Base"):
        GetCluster.setUp(self, nomenclature)

        self.expected_res = {
            "name": self.prefix + "workflow",
            "workflowType": "documentIngestion"
        }

        self.log.info("Creating AI Workflow for the test")
        res = self.capellaAPI.cluster_ops_apis.create_ai_workflow(
            self.organisation_id, self.project_id, self.cluster_id,
            self.expected_res)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_ai_workflow(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res)
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating AI Workflow for the test.")
        self.log.info("AI Workflow created successfully.")
        self.workflow_id = res.json()["id"]

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        result = self.capellaAPI.cluster_ops_apis.list_ai_workflows(
            self.organisation_id, self.project_id, self.cluster_id)
        if result.status_code == 429:
            self.handle_rate_limit(int(result.headers["Retry-After"]))
            result = self.capellaAPI.cluster_ops_apis.list_ai_workflows(
                self.organisation_id, self.project_id, self.cluster_id)

        workflow_ids = []
        if result.status_code == 200:
            data = result.json().get("data", [])
            workflow_ids = [w["id"] for w in data]

        if hasattr(self, "workflow_id") and self.workflow_id and \
                self.workflow_id not in workflow_ids:
            workflow_ids.append(self.workflow_id)

        for workflow_id in workflow_ids:
            self.log.info("Deleting AI Workflow: {}".format(workflow_id))
            res = self.capellaAPI.cluster_ops_apis.delete_ai_workflow(
                self.organisation_id, self.project_id, self.cluster_id,
                workflow_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.delete_ai_workflow(
                    self.organisation_id, self.project_id, self.cluster_id,
                    workflow_id)
            if res.status_code not in [200, 202, 204, 404]:
                self.log.error("Failed to delete AI Workflow {}: {}".format(
                    workflow_id, res.content))

        super(AIWorkflowBase, self).tearDown()
