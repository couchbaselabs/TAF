from TestInput import TestInputSingleton

if TestInputSingleton.input.param("capella_run", False):
    from onCloud_basetestcase import BaseTestCase as BTC
else:
    from onPrem_basetestcase import BaseTestCase as BTC


class BaseTestCase(BTC):
    pass


class ClusterSetup(BaseTestCase):
    pass
