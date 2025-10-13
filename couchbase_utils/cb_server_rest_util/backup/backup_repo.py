from cb_server_rest_util.connection import CBRestConnection


class BackupRepoAPIs(CBRestConnection):
    def __init__(self):
        super(BackupRepoAPIs, self).__init__()

    def create_repository(self, repo_name, repo_definition):
        """
        POST /cluster/self/repository/active/<new-repository-name>
        docs.couchbase.com/server/current/rest-api/backup-create-repository.html
        """
        api = self.backup_url \
            + "/api/v1/cluster/self/repository/active/%s" % repo_name
        params = self.flatten_param_to_str(repo_definition)
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content

    def get_repository_information(self, repo_type, repo_id=None, info=False):
        """
        GET /cluster/self/repository/<"active"|"imported"|"archived">
        GET /cluster/self/repository/<"active"|"imported"|"archived">/<repository-id>
        GET /cluster/self/repository/<"active"|"imported"|"archived">/<repository-id>/info
        docs.couchbase.com/server/current/rest-api/backup-get-repository-info.html
        """

        api = self.backup_url \
            + "/api/v1/cluster/self/repository/%s" % repo_type
        if repo_id:
            api += "/%s" % repo_id
        if info:
            api += "/info"
        status, content, _ = self.request(api, self.GET)
        return status, content

    def archive_repository(self, repo_id):
        """
        POST /repository/active/<repository-id>/archive
        docs.couchbase.com/server/current/rest-api/backup-archive-a-repository.html
        """
        api = self.backup_url \
            + "/api/v1/cluster/self/repository/active/%s/archive" % repo_id
        params = self.flatten_param_to_str({"id": repo_id})
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content

    def import_repository(self, repo_specification):
        """
        POST /cluster/self/repository/import
        docs.couchbase.com/server/current/rest-api/backup-import-repository.html
        """
        api = self.backup_url + "/api/v1/cluster/self/repository/import"
        status, content, _ = self.request(api, self.POST,
                                          params=repo_specification)
        return status, content

    def delete_repository(self, repo_id, remove_repository=False):
        """
        DELETE /cluster/self/repository/archived/<repo_id>
        DELETE /cluster/self/repository/archived/<repo_id>?remove_repository=true
        docs.couchbase.com/server/current/rest-api/backup-delete-repository.html
        """
        api = self.backup_url \
            + "/api/v1/cluster/self/repository/archived/%s" % repo_id
        if remove_repository:
            api += "?remove_repository=true"
        status, content, _ = self.request(api, self.DELETE)
        return status, content
