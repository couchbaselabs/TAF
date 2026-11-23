"""On-demand list prices.

EC2 prices come from the AWS Pricing API (cached in a JSON file between runs),
with a built-in table as fallback. These are list prices: Apptio's amortized
figures include Savings Plan discounts, so on quiet days the report lands a
little above Apptio.
"""

import json
import os

try:
    from botocore.exceptions import ClientError
except ImportError:          # unit tests run without TAF's requirements
    class ClientError(Exception):
        pass

LOCATIONS = {
    "us-east-1": "US East (N. Virginia)",
    "us-east-2": "US East (Ohio)",
    "us-west-2": "US West (Oregon)",
}

# gp3, USD per month (AWS bills per second against a 730-hour month)
GP3_GB_MONTH = 0.08
GP3_IOPS_MONTH = 0.005       # above 3,000
GP3_MBPS_MONTH = 0.04        # above 125
HOURS_PER_MONTH = 730.0

# Fallback on-demand Linux USD/hour (us-east-1; us-east-2 is the same and
# us-west-2 is within a few percent for these families)
FALLBACK = {
    "c5.2xlarge": 0.34, "c5.large": 0.085, "m5.xlarge": 0.192,
    "m5.2xlarge": 0.384, "m5.4xlarge": 0.768, "m6i.4xlarge": 0.768,
    "m7g.xlarge": 0.1632, "m7g.2xlarge": 0.3264, "m7g.4xlarge": 0.6528,
    "c6g.2xlarge": 0.272, "c6g.4xlarge": 0.544, "c6g.8xlarge": 1.088,
    "c6gn.2xlarge": 0.3456, "c6gn.4xlarge": 0.6912,
    "c6in.2xlarge": 0.4536, "c6in.4xlarge": 0.9072, "c6in.8xlarge": 1.8144,
    "c7g.large": 0.0725, "c7g.xlarge": 0.145, "c7g.2xlarge": 0.289,
    "c7g.4xlarge": 0.578, "c7g.8xlarge": 1.156, "c7g.12xlarge": 1.7344,
    "c7g.16xlarge": 2.3125, "c7gn.4xlarge": 0.9984,
    "c8g.2xlarge": 0.31904, "c8g.4xlarge": 0.63808, "c8g.8xlarge": 1.27616,
    "c8gb.xlarge": 0.237, "c8gb.2xlarge": 0.474, "c8gb.4xlarge": 0.948,
    "c8gn.2xlarge": 0.5485, "c8gn.4xlarge": 1.097,
    "m6in.2xlarge": 0.55692, "m6idn.2xlarge": 0.63648,
    "r6in.2xlarge": 0.69732, "r6idn.2xlarge": 0.78126,
    "m8gb.xlarge": 0.2266, "m8gb.2xlarge": 0.4532,
    "r8gb.xlarge": 0.2954, "r8gb.2xlarge": 0.5908,
    "r5.large": 0.126, "r5.xlarge": 0.252, "r5.2xlarge": 0.504,
}


def gp3_hourly(size_gb, iops=3000, throughput=125):
    """USD/hour for one gp3 volume, including provisioned IOPS/throughput."""
    month = (size_gb * GP3_GB_MONTH
             + max(0, (iops or 3000) - 3000) * GP3_IOPS_MONTH
             + max(0, (throughput or 125) - 125) * GP3_MBPS_MONTH)
    return month / HOURS_PER_MONTH


def default_throughput(iops):
    """Throughput Capella provisions for a requested IOPS (seen in qe-7)."""
    if iops and iops >= 16000:
        return 1000
    if iops and iops > 3000:
        return max(125, int(iops / 16))
    return 125


class PriceBook(object):
    def __init__(self, qe=None, cache_path=None, log=print):
        self.qe = qe
        self.cache_path = cache_path
        self.log = log
        self.cache = {}         # API prices, saved between runs
        self._this_run = {}     # fallback prices, this run only
        self.unpriced = set()
        if cache_path and os.path.exists(cache_path):
            with open(cache_path) as f:
                self.cache = json.load(f)

    def save(self):
        if self.cache_path:
            with open(self.cache_path, "w") as f:
                json.dump(self.cache, f, indent=1, sort_keys=True)

    def _from_api(self, instance_type, region):
        resp = self.qe.client("pricing", "us-east-1").get_products(
            ServiceCode="AmazonEC2", MaxResults=10, Filters=[
                {"Type": "TERM_MATCH", "Field": k, "Value": v} for k, v in (
                    ("instanceType", instance_type),
                    ("location", LOCATIONS.get(region, LOCATIONS["us-east-1"])),
                    ("operatingSystem", "Linux"), ("tenancy", "Shared"),
                    ("preInstalledSw", "NA"), ("capacitystatus", "Used"))])
        for raw in resp.get("PriceList", []):
            product = json.loads(raw)
            for term in product.get("terms", {}).get("OnDemand", {}).values():
                for dim in term.get("priceDimensions", {}).values():
                    usd = float(dim["pricePerUnit"].get("USD", 0))
                    if usd > 0:
                        return usd
        return None

    def hourly(self, instance_type, region="us-east-1"):
        if not instance_type:
            return 0.0
        key = "%s/%s" % (region, instance_type)
        if key in self.cache:
            return self.cache[key]
        if key in self._this_run:
            return self._this_run[key]
        price = None
        if self.qe is not None:
            try:
                price = self._from_api(instance_type, region)
            except ClientError as e:
                self.log("Pricing API failed for %s: %s" % (key, e))
        if price is not None:
            self.cache[key] = price
            return price
        # Not cached: an API failure (or missing pricing:GetProducts) must not
        # pin the approximate price in prices.json; the next run asks again
        price = FALLBACK.get(instance_type)
        if price is None:
            self.unpriced.add(instance_type)
            price = 0.0
        self._this_run[key] = price
        return price
