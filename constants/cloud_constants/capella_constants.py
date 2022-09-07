class Cluster(object):
    class Plan(object):
        DEV_PRO = "DeveloperPro"

    class Timezone(object):
        PT = "PT"
        ET = "ET"
        GMT = "GMT"
        IST = "IST"

    class Availability(object):
        SINGLE = "Single"
        MULTIPLE = "Multiple"


class AWS(object):
    __str__ = "aws"

    class Region(object):
        US_WEST_2 = "us-west-2"
        US_EAST_1 = "us-east-1"

    class ComputeNode(object):
        VCPU4_RAM16 = "m5.xlarge"
        VCPU4_RAM32 = "r6g.xlarge"

    class StorageType(object):
        GP3 = "GP3"
        IO2 = "IO2"

    class StorageSize(object):
        MIN = 50
        MAX = 16000

    class StorageIOPS(object):
        MIN = 3000
        MAX = 16000
