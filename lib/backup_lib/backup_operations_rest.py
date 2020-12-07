import urllib

from Rest_Connection import RestConnection


class BackupHelper(RestConnection):
    def __init__(self, server):
        super(BackupHelper, self).__init__(server)

    def create_repo(self, repo_name, params):
        """
        Creates repo for backup with the given name
        :param repo_name: Name of the repo to create
        :param params: Following are the supported param keys,
            plan - Unique plan name used during create_plan (str)
            archive - Location on a file_system / cloud (str)
            bucket_name - Optional. Any existing bucket name (str)
        """
        api = '%s%s' % (self.backup_url,
                        "api/v1/cluster/self/repository/active/%s" % repo_name)
        # params = urllib.urlencode(str(params))
        status, _, _ = self._http_request(
            api, RestConnection.POST, str(params).replace("'", '"'),
            self.get_headers_for_content_type_json())
        return status

    def delete_repo(self, repo_name, delete_archive=False):
        """
        Deleted the archived the repository
        :param repo_name: Target repository to delete
        :param delete_archive: If 'True', deletes the backup from disk as well
        """
        api = "%s%s" % (self.backup_url,
                        "api/v1/cluster/self/repository/archived/%s"
                        % repo_name)
        if delete_archive is True:
            api += "?remove_repository=true"
        status, _, _ = self._http_request(api, RestConnection.DELETE)
        return status

    def create_edit_plan(self, op_type="create", params=dict()):
        """
        Generic API to 'Create' / 'Edit' existing backup plan on the cluster.
        :param op_type: create / edit
        :param params: Following are the supported param keys,
            name - A plan-name that is unique across the cluster (str)
            description - Optional for the plan (str)
            services - Services whose data is to be backed up. (list)
            tasks - Optional or one or more tasks. (list)
        """
        http_method = RestConnection.POST
        if op_type == "edit":
            http_method = RestConnection.PUT
        api = '%s%s' % (self.backup_url, "plan/%s" % params["name"])
        status, _, _ = self._http_request(
            api, http_method,  str(params).replace("'", '"'),
            self.get_headers_for_content_type_json())
        return status

    def delete_plan(self, plan_name):
        """
        Deletes the specified plan
        :param plan_name: Plan name to be deleted
        """
        api = '%s%s' % (self.backup_url, "plan/%s" % plan_name)
        status, _, _ = self._http_request(api, RestConnection.DELETE)
        return status

    def perform_backup(self, repo_name):
        """
        Triggers immediate backup for the given repository
        :param repo_name: Name of the repo to perform backup
        """
        end_point = "api/v1/cluster/self/repository/active/%s/backup" \
                    % repo_name
        api = "%s%s" % (self.backup_url, end_point)
        status, response, _ = self._http_request(api, RestConnection.POST)
        return status, response

    def perform_merge(self, repo_name, offset_start, offset_end):
        """
        Triggers immediate merge on existing backup.
        :param repo_name: Name of the repo to perform merge
        :param offset_start: Integer representing most recent day
        :param offset_end: Integer representing least recent day
        """
        end_point = "api/v1/cluster/self/repository/active/%s/merge" \
                    % repo_name
        params = {"offset_start": offset_start,
                  "offset_end": offset_end}
        api = "%s%s" % (self.backup_url, end_point)
        status, response, _ = self._http_request(
            api, RestConnection.POST,  str(params).replace("'", '"'),
            self.get_headers_for_content_type_json())
        return status, response

    def archive_repo(self, repo_name, archive_id):
        """
        Archives currently active repositories.
        :param repo_name: Active repository id
        :param archive_id: Archived repo id
        """
        end_point = "api/v1/cluster/self/repository/active/%s/archive" \
                    % repo_name
        params = {"id": archive_id}
        api = "%s%s" % (self.backup_url, end_point)
        status, _, _ = self._http_request(
            api, RestConnection.POST,  str(params).replace("'", '"'),
            self.get_headers_for_content_type_json())
        return status

    def restore_data(self, restore_from, repo_name, params):
        """
        Restores data from 'active | imported | archived' repo
        :param restore_from: Any value from 'active / imported / archived'
        :param repo_name: Name of an active, imported, or archived repository
        :param params: Any of the following param keys:
                       "target": <ip-address-or-domain-name>":8091",
                       "user": <username>,
                       "password": <password>",
                       "auto_create_buckets": < true | false >,
                       "auto_remove_collections": < true | false >,
                       "disable_analytics": < true | false >,
                       "disable_data": < true | false >,
                       "disable_eventing": < true | false >,
                       "disable_ft": < true | false >,
                       "disable_gsi_indexes": < true | false >,
                       "disable_views": < true | false >,
                       "end": <earliest-backup-to-include>,
                       "filter_keys": <regular-expression>,
                       "filter_values": <regular-expression>,
                       "force_updates": < true | false >,
                       "map_data": < old-bucket"="new-bucket >,
                       "start": <latest-backup-to-include>,
                       "enable_bucket_config": < true | false >,
                       "replace_ttl": < "all" | "none" | "expired" >,
                       "replace_ttl_with": < RFC3339-time-value | "0"
        """
        end_point = "cluster/self/repository/%s/%s/restore" \
                    % (restore_from, repo_name)
        api = "%s%s" % (self.backup_url, end_point)
        status, response, _ = self._http_request(
            api, RestConnection.POST,  str(params).replace("'", '"'),
            self.get_headers_for_content_type_json())
        return status, response
