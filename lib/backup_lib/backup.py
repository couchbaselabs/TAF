import mode

if mode.java:
    from backup_operations_javasdk import BackupHelper as Backup_Lib
elif mode.cli:
    from backup_operations_cli import BackupHelper as Backup_Lib
else:
    from backup_operations_rest import BackupHelper as Backup_Lib


class BackupHelper(Backup_Lib):
    pass
