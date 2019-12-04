import mode

if mode.java:
    # from GsiHelper_JavaSDK import GsiHelper as GsiLib
    pass
elif mode.cli:
    # from GsiHelper_CLI import GsiHelper as GsiLib
    pass
else:
    from GsiHelper_Rest import GsiHelper as GsiLib


class GsiHelper(GsiLib):
    pass
