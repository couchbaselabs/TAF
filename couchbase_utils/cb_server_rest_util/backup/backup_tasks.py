from cb_server_rest_util.connection import CBRestConnection


class BackupTaskAPIs(CBRestConnection):
    def __init__(self):
        super(BackupTaskAPIs, self).__init__()

    def get_task_information(self, repo_type, repo_name=None,
                             task_history=False, task_subset_spec=None):
        """
        GET /cluster/self/repository/<"active"|"imported"|"archived">/<repository-name>/taskHistory
        GET /cluster/self/repository/<"active"|"imported"|"archived">/<repository-name>/taskHistory?<task-subset-specification>
        docs.couchbase.com/server/current/rest-api/backup-get-task-info.html
        """
        api = self.backup_url \
            + "/api/v1/cluster/self/repository/%s" % repo_type
        if repo_name:
            api += "/%s" % repo_name
        if task_history:
            api += "/taskHistory"
        if task_subset_spec:
            api += "?%s" % task_subset_spec
        status, content, _ = self.request(api, self.GET)
        return status, content

    def pause_task(self, repo_id):
        """
        POST /cluster/self/repository/active/<repository-id>/pause
        docs.couchbase.com/server/current/rest-api/backup-pause-and-resume-tasks.html
        """
        api = self.backup_url \
            + "/api/v1/cluster/self/repository/active/%s/pause" % repo_id
        status, content, _ = self.request(api, self.POST)
        return status, content

    def resume_task(self, repo_id):
        """
        POST /cluster/self/repository/active/<repository-id>/resume
        docs.couchbase.com/server/current/rest-api/backup-pause-and-resume-tasks.html
        """
        api = self.backup_url \
              + "/api/v1/cluster/self/repository/active/%s/resume" % repo_id
        status, content, _ = self.request(api, self.POST)
        return status, content

    def examine_backup_data(self, repo_type, repo_id, doc_search_spec):
        """
        POST /cluster/self/repository/<"active"|"imported"|"archived">/<repository-id>/examine
        docs.couchbase.com/server/current/rest-api/backup-examine-data.html
        """
        api = self.backup_url \
            + "/api/v1/cluster/self/repository/%s/%s/examine" \
            % (repo_type, repo_id)
        status, content, _ = self.request(api, self.POST,
                                          params=doc_search_spec)
        return status, content

    def perform_immediate_backup(self, repo_id, full_backup="false"):
        """
        POST /cluster/self/repository/active/<repository-id>/backup
        docs.couchbase.com/server/current/rest-api/backup-trigger-backup.html
        """
        api = self.backup_url \
            + "/api/v1/cluster/self/repository/active/%s/backup" % repo_id
        params = {"full_backup": full_backup}
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content

    def perform_immediate_merge(self, repo_id, merge_period_spec):
        """
        POST /cluster/self/repository/active/<repository-id>/merge
        docs.couchbase.com/server/current/rest-api/backup-trigger-merge.html
        """
        api = self.backup_url \
            + "/api/v1/cluster/self/repository/active/%s/merge" % repo_id
        status, content, _ = self.request(api, self.POST,
                                          params=merge_period_spec)
        return status, content

    def restore_data(self, repo_id, restoration_spec):
        """
        POST /cluster/self/repository/< "active" | "imported" | "archived" >/<repository-id>/restore
        docs.couchbase.com/server/current/rest-api/backup-restore-data.html
        """
        api = self.backup_url \
            + "/api/v1/api/v1/cluster/self/repository/active/%s/restore" \
            % repo_id
        status, content, _ = self.request(api, self.POST,
                                          params=restoration_spec)
        return status, content

    def delete_backup(self, repo_id, backup_id):
        """
        DELETE /cluster/self/repository/active/<repository-id>/backups/<backup-id>
        docs.couchbase.com/server/current/rest-api/backup-delete-backups.html
        """
        api = self.backup_url \
            + "/api/v1/cluster/self/repository/active/%s/backups/%s" \
            % (repo_id, backup_id)
        status, content, _ = self.request(api, self.DELETE)
        return status, content
