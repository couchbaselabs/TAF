class Cluster(object):
    class Plan(object):
        DEV_PRO = "DeveloperPro"
        ENTERPRISE = "Enterprise"

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
    compute = [
        "c5.xlarge",
        "c5.2xlarge",
        "c5.4xlarge",
        "c5.9xlarge",
        "c5.12xlarge",
        "c5.18xlarge",
        "m5.xlarge",
        "m5.2xlarge",
        "m5.4xlarge",
        "m5.8xlarge",
        "m5.12xlarge",
        "m5.16xlarge",
        "r5.xlarge",
        "r5.2xlarge",
        "r5.4xlarge"
        "r5.8xlarge",
        "r5.12xlarge",
        ]

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


class GCP(object):
    __str__ = "gcp"
    compute = ["n2-custom-8-16384",
               "n2-custom-16-32768",
               "n2-custom-36-73728",
               "n2-custom-48-98304",
               "n2-custom-72-147456",
               "n2-highcpu-8",
               "n2-highcpu-16",
               "n2-highcpu-32",
               "n2-highcpu-64",
               "n2-highcpu-80",
               "n2-highmem-4",
               "n2-highmem-8",
               "n2-highmem-16",
               "n2-highmem-32",
               "n2-highmem-64",
               "n2-highmem-96",
               "n2-standard-2",
               "n2-standard-4",
               "n2-standard-8",
               "n2-standard-16",
               "n2-standard-32",
               "n2-standard-64",
               "n2-standard-96"
               ]

    class StorageType(object):
            PD_SSD = "PD-SSD"
