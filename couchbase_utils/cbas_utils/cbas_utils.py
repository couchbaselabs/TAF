from TestInput import TestInputSingleton

runtype = TestInputSingleton.input.param("runtype", "default").lower()
if runtype in ["columnar", "columnar1"]:
    from cbas_utils.cbas_utils_columnar import (
        CbasUtil as utils,
        FlushToDiskTask as flushtodisktask,
        DisconnectConnectLinksTask as disconnectconnectlinkstask,
        KillProcessesInLoopTask as killprocessinlooptask,
        CBASRebalanceUtil as rebalanceutil,
        BackupUtils as backuputil
    )
else:
    from cbas_utils_on_prem import (
        CbasUtil as utils,
        FlushToDiskTask as flushtodisktask,
        DisconnectConnectLinksTask as disconnectconnectlinkstask,
        KillProcessesInLoopTask as killprocessinlooptask,
        CBASRebalanceUtil as rebalanceutil,
        BackupUtils as backuputil
    )


class CbasUtil(utils):
    pass


class FlushToDiskTask(flushtodisktask):
    pass


class DisconnectConnectLinksTask(disconnectconnectlinkstask):
    pass


class KillProcessesInLoopTask(killprocessinlooptask):
    pass


class CBASRebalanceUtil(rebalanceutil):
    pass


class BackupUtils(backuputil):
    pass
