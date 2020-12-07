from backup_lib.backup_operations_rest import BackupHelper as Backup_rest


class BackupHelper(Backup_rest):
    def __init__(self, server):
        self.server = server
        super(BackupHelper, self).__init__(server)
