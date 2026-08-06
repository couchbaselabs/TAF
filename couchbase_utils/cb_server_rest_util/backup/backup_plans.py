from cb_server_rest_util.connection import CBRestConnection


class BackupPlanAPIs(CBRestConnection):
    def __init__(self):
        super(BackupPlanAPIs, self).__init__()

    def create_plan(self, new_plan_id, plan_description):
        """
        POST /plan/<new-plan-id>
        docs.couchbase.com/server/current/rest-api/backup-create-and-edit-plans.html
        """
        api = self.backup_url + "/api/v1/plan/%s" % new_plan_id
        status, content, _ = self.request(api, self.POST,
                                          params=plan_description)
        return status, content

    def edit_plan(self, existing_plan_id, plan_description):
        """
        PUT /plan/<existing-plan-id>
        docs.couchbase.com/server/current/rest-api/backup-create-and-edit-plans.html
        """
        api = self.backup_url + "/api/v1/plan/%s" % existing_plan_id
        status, content, _ = self.request(api, self.PUT,
                                          params=plan_description)
        return status, content

    def get_plan_info(self, plan_id=None):
        """
        GET /plan
        GET /plan/<plan-id>
        docs.couchbase.com/server/current/rest-api/backup-get-plan-info.html
        """
        api = self.backup_url + "/api/v1/plan"
        if plan_id:
            api += "/%s" % plan_id
        status, content, _ = self.request(api, self.GET)
        return status, content

    def delete_plan(self, plan_id):
        """
        DELETE /plan/<plan-id>
        """
        api = self.backup_url + "/api/v1/plan/%s" % plan_id
        status, content, _ = self.request(api, self.DELETE)
        return status, content
