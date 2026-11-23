"""What is running in qe-7 right now: instances, their data disks, and
volumes nobody is using."""

import datetime as dt

from . import events as ev
from .pricing import gp3_hourly

UNATTACHED_MIN_AGE = dt.timedelta(hours=6)


def _tags(resource):
    return {t["Key"]: t["Value"] for t in resource.get("Tags") or []}


def is_qe7(tags, env="qe-7"):
    return tags.get("stack-env") == env or \
        (tags.get("user-createdby") or "").startswith(env + "-")


def snapshot(qe, regions, env="qe-7"):
    """Return (live_instances, unattached_volumes).

    live_instances: {iid: {region, type, start, tags, disk}} for pending/
    running qe-7 instances; ``disk`` is the largest non-root EBS volume.
    """
    live, unattached = {}, []
    now = dt.datetime.now(dt.timezone.utc)
    for region in regions:
        ec2 = qe.client("ec2", region)
        volumes = {}
        for page in ec2.get_paginator("describe_volumes").paginate():
            for vol in page["Volumes"]:
                volumes[vol["VolumeId"]] = vol
                tags = _tags(vol)
                # Control-plane PVCs (Prometheus/Thanos on qe-7-cp-eks) belong
                # to the sandbox baseline, not to any test
                control_plane = tags.get("stack-service") == "cp" or \
                    "-cp-eks" in tags.get("KubernetesCluster", "")
                # A running fusion rebalance creates volumes that stay
                # unattached for a few minutes; only flag ones left for hours
                recent = vol["CreateTime"] > now - UNATTACHED_MIN_AGE
                if vol["State"] == "available" and is_qe7(tags, env) \
                        and not control_plane and not recent:
                    unattached.append(dict(
                        id=vol["VolumeId"], region=region, size=vol["Size"],
                        iops=vol.get("Iops"), throughput=vol.get("Throughput"),
                        created=vol["CreateTime"].isoformat(),
                        owner=tags.get("user-createdby"),
                        usd_per_day=round(24 * gp3_hourly(
                            vol["Size"], vol.get("Iops"), vol.get("Throughput")), 2)))
        paginator = ec2.get_paginator("describe_instances")
        for page in paginator.paginate(Filters=[
                {"Name": "instance-state-name", "Values": ["pending", "running"]}]):
            for res in page["Reservations"]:
                for inst in res["Instances"]:
                    tags = _tags(inst)
                    if not is_qe7(tags, env):
                        continue
                    root = inst.get("RootDeviceName")
                    disks = [volumes.get(m["Ebs"]["VolumeId"])
                             for m in inst.get("BlockDeviceMappings") or []
                             if m.get("DeviceName") != root and m.get("Ebs")]
                    disks = [d for d in disks if d]
                    disk = None
                    if disks and ev.role(tags) == "node":
                        big = max(disks, key=lambda d: d["Size"])
                        disk = dict(size=big["Size"], iops=big.get("Iops"),
                                    throughput=big.get("Throughput"))
                    live[inst["InstanceId"]] = dict(
                        region=region, type=inst["InstanceType"],
                        start=inst["LaunchTime"], tags=tags, disk=disk)
    return live, unattached
