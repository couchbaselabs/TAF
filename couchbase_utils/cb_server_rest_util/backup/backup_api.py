from cb_server_rest_util.backup.backup_plans import BackupPlanAPIs
from cb_server_rest_util.backup.backup_repo import BackupRepoAPIs
from cb_server_rest_util.backup.backup_tasks import BackupTaskAPIs
from cb_server_rest_util.backup.manage_and_config import \
    BackupManageAndConfigAPIs


class BackupRestApi(BackupRepoAPIs, BackupPlanAPIs, BackupTaskAPIs,
                    BackupManageAndConfigAPIs):
    def __init__(self, server):
        super(BackupRestApi, self).__init__()

        self.set_server_values(server)
        self.set_endpoint_urls(server)
