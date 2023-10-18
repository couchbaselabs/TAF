from TestInput import TestInputSingleton

runtype = TestInputSingleton.input.param("runtype", "default").lower()
if runtype == "goldfish":
    from cbas_base_goldfish import CBASBaseTest as CBASBase
else:
    from cbas_base_on_prem import CBASBaseTest as CBASBase


class CBASBaseTest(CBASBase):
    pass
