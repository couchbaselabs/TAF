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

    class Region(object):
        US_WEST_2 = "us-west2"
        US_EAST_1 = "us-east1"

    class StorageType(object):
       PD_SSD = "PD-SSD"


class AZURE(object):
    __str__ = "azure"
    compute = [
        "Standard_D4s_v5",
        "Standard_D8s_v5",
        "Standard_D16s_v5",
        "Standard_D32s_v5",
        "Standard_D48s_v5",
        "Standard_D64s_v5",
        "Standard_D96s_v5",
        "Standard_E4s_v5",
        "Standard_E8s_v5",
        "Standard_E16s_v5",
        "Standard_E20s_v5",
        "Standard_E32s_v5",
        "Standard_E48s_v5",
        "Standard_E64s_v5",
        "Standard_E96s_v5",
        "Standard_F8s_v2"
        "Standard_F16s_v2",
        "Standard_F32s_v2",
        "Standard_F48s_v2",
        "Standard_F64s_v2",
        "Standard_F72s_v2"
        ]

    class StorageType(object):
        order = ['P6', 'P10', 'P15', 'P20', 'P30', 'P40', 'P50', 'P60']
        type = {
            "P6": {
                "min": 64,
                "max": 64,
                "iops": {
                    "min": 240,
                    "max": 240
                }
            },
            "P10": {
                "min": 128,
                "max": 128,
                "iops": {
                    "min": 500,
                    "max": 500
                }
            },
            "P15": {
                "min": 256,
                "max": 256,
                "iops": {
                "min": 1100,
                "max": 1100
                }
            },
            "P20": {
                "min": 512,
                "max": 512,
                "iops": {
                "min": 2300,
                "max": 2300
                }
            },
            "P30": {
                "min": 1024,
                "max": 1024,
                "iops": {
                "min": 5000,
                "max": 5000
                }
            },
            "P40": {
                "min": 2048,
                "max": 2048,
                "iops": {
                "min": 7500,
                "max": 7500
                }
            },
            "P50": {
                "min": 4096,
                "max": 4096,
                "iops": {
                "min": 7500,
                "max": 7500
                }
            },
            "P60": {
                "min": 8192,
                "max": 8192,
                "iops": {
                "min": 16000,
                "max": 16000
                }
            }
        }
