import mode

if mode.java:
    from StatsLib.StatsOperations_JavaSDK import StatsHelper as StatsLib
elif mode.cli:
    from StatsLib.StatsOperations_CLI import StatsHelper as StatsLib
else:
    from StatsLib.StatsOperations_Rest import StatsHelper as StatsLib


class StatsHelper(StatsLib):
    pass
