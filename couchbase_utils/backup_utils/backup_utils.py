from cb_server_rest_util.backup.backup_api import BackupRestApi
from global_vars import logger


class BackupUtil(object):
    def __init__(self, cluster_node):
        self.server = cluster_node
        self.rest = BackupRestApi(self.server)
        self.log = logger.get("test")

    def reset_cluster_node(self, cluster_node):
        self.server = cluster_node
        self.rest = BackupRestApi(self.server)

    def archive_all_repos(self):
        status, repos = self.rest.get_repository_information("active")
        if status:
            for repo in repos:
                self.log.info("Archiving backup_repo '%s'" % repo["id"])
                status, content = self.rest.archive_repository(repo["id"])
                if not status:
                    self.log.critical("Failed to archive '%s': %s"
                                      % (repo["id"], content))
        return status

    def delete_all_archive_repos(self, remove_repository=False):
        status, repos = self.rest.get_repository_information("archived")
        if status:
            for repo in repos:
                self.log.info("Deleting archive repo '%s'" % repo["id"])
                status, content = self.rest.delete_repository(
                    repo["id"], remove_repository=remove_repository)
                if status:
                    self.log.critical("Failed to delete repo '%s': %s"
                                      % (repo["id"], content))
        return status
