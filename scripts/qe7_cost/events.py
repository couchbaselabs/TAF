"""Turn raw CloudTrail events into instance launches and terminations.

* RunInstances: instance id, type, launch time and tags (Capella node ASGs,
  doc-loader fleets).
* CreateFleet: instances launched through an EC2 Fleet (the ASG
  mixed-instances policy that fusion accelerators use). The response has ids
  and types but no tags; the tags come from the launch template the request
  names, which CreateLaunchTemplate recorded.
* TerminateInstances: the end time. Accelerators often shut themselves down,
  which records no event; :mod:`model` infers those ends.
"""

import datetime as dt
import glob
import json
import os


def ts(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _as_list(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _events(events_dir, region, event_name):
    pattern = os.path.join(events_dir, "%s_%s_*.jsonl" % (region, event_name))
    for path in sorted(glob.glob(pattern)):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                event = json.loads(line)
                if not event.get("errorCode"):
                    yield event


def launch_template_tags(event):
    """Tags a CreateLaunchTemplate request stamps on the instances it makes."""
    req = (event.get("requestParameters") or {}).get(
        "CreateLaunchTemplateRequest") or {}
    data = req.get("LaunchTemplateData") or {}
    tags = {}
    for spec in _as_list(data.get("TagSpecification")):
        for tag in _as_list((spec or {}).get("Tag")):
            if tag:
                tags[tag.get("Key")] = tag.get("Value")
    return tags


def _run_instance_tags(event, item):
    tag_set = item.get("tagSet")
    tags = {t.get("key"): t.get("value")
            for t in (tag_set.get("items", []) if isinstance(tag_set, dict)
                      else [])}
    if tags:
        return tags
    # Some callers only show tags in the request (tagSet can also be hidden)
    specs = (event.get("requestParameters") or {}).get("tagSpecificationSet")
    for spec in (specs.get("items", []) if isinstance(specs, dict) else []):
        for tag in _as_list(spec.get("tags")):
            tags[tag.get("key")] = tag.get("value")
    return tags


def load(events_dir, region):
    """Return (launches, terminations, stats) for one region.

    launches: {instance_id: {"start", "type", "tags", "region"}}
    terminations: {instance_id: earliest terminate time}
    """
    launches, terms = {}, {}
    stats = {"fleet_instances_without_template": 0}

    for event in _events(events_dir, region, "RunInstances"):
        items = ((event.get("responseElements") or {}).get("instancesSet")
                 or {}).get("items", [])
        for item in items:
            launches[item["instanceId"]] = dict(
                start=ts(event["eventTime"]), type=item.get("instanceType"),
                tags=_run_instance_tags(event, item), region=region)

    template_tags = {}
    for event in _events(events_dir, region, "CreateLaunchTemplate"):
        lt = ((event.get("responseElements") or {}).get(
            "CreateLaunchTemplateResponse") or {}).get("launchTemplate") or {}
        if lt.get("launchTemplateId"):
            template_tags[lt["launchTemplateId"]] = launch_template_tags(event)

    for event in _events(events_dir, region, "CreateFleet"):
        req = (event.get("requestParameters") or {}).get(
            "CreateFleetRequest") or {}
        configs = _as_list(req.get("LaunchTemplateConfigs"))
        lt_id = ((configs[0] if configs else {}).get(
            "LaunchTemplateSpecification") or {}).get("LaunchTemplateId")
        tags = template_tags.get(lt_id)
        resp = (event.get("responseElements") or {}).get(
            "CreateFleetResponse") or {}
        for item in _as_list((resp.get("fleetInstanceSet") or {}).get("item")):
            for iid in _as_list((item.get("instanceIds") or {}).get("item")):
                if tags is None:
                    stats["fleet_instances_without_template"] += 1
                    continue
                launches.setdefault(iid, dict(
                    start=ts(event["eventTime"]), type=item.get("instanceType"),
                    tags=tags, region=region))

    for event in _events(events_dir, region, "TerminateInstances"):
        when = ts(event["eventTime"])
        items = ((event.get("responseElements") or {}).get("instancesSet")
                 or {}).get("items", [])
        for item in items:
            iid = item["instanceId"]
            terms[iid] = min(terms.get(iid, when), when)
    return launches, terms, stats


def role(tags):
    """What an instance is: node, accel(erator), loader, controlplane (the
    sandbox's own EKS nodes, covered by the daily baseline) or other."""
    group = tags.get("aws:autoscaling:groupName") or tags.get("Name") or ""
    if "-cp-eks-" in group:
        return "controlplane"
    function = tags.get("couchbase-cloud-function") or ""
    if function == "fusion-accelerator":
        return "accel"
    if tags.get("taf-docloader-fleet") or function == "docloader":
        return "loader"
    if tags.get("couchbase-cloud-cluster-id"):
        return "node"
    return "other"


def group_key(tags):
    """The cluster or doc-loader fleet an instance belongs to."""
    return (tags.get("couchbase-cloud-cluster-id")
            or tags.get("taf-docloader-fleet")
            or tags.get("aws:autoscaling:groupName")
            or tags.get("Name") or "unknown")
