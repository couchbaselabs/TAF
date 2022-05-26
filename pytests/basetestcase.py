from TestInput import TestInputSingleton

if TestInputSingleton.input.param("capella_run", False):
    from onCloud_basetestcase import OnCloudBaseTest as CbBaseTest
    from onCloud_basetestcase import ClusterSetup as CbClusterSetup
else:
    from onPrem_basetestcase import OnPremBaseTest as CbBaseTest
    from onPrem_basetestcase import ClusterSetup as CbClusterSetup


class BaseTestCase(CbBaseTest):
    pass


class ClusterSetup(CbClusterSetup):
    pass
