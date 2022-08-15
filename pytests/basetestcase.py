from TestInput import TestInputSingleton

runtype = TestInputSingleton.input.param("runtype", "default").lower()
if runtype == "dedicated":
    from dedicatedbasetestcase import OnCloudBaseTest as CbBaseTest
    from dedicatedbasetestcase import ClusterSetup as CbClusterSetup
elif runtype == "serverless":
    from serverlessbasetestcase import OnCloudBaseTest as CbBaseTest
    from serverlessbasetestcase import ClusterSetup as CbClusterSetup
else:
    from onPrem_basetestcase import OnPremBaseTest as CbBaseTest
    from onPrem_basetestcase import ClusterSetup as CbClusterSetup


class BaseTestCase(CbBaseTest):
    pass


class ClusterSetup(CbClusterSetup):
    pass
