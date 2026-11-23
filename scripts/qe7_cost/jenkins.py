"""Map qe-7 clusters and doc-loader fleets to the Jenkins builds that made them.

Capella tags each node with its cluster id and tenant owner, but not with the
test. TAF prints "Cluster created with cluster ID: <id>" (or "cluster
provisioning as <id>" for a restore clone), the spec it asked for, and the
``taf-dl-*`` name of any doc-loader fleet, so scanning the console of every
qe-7 build gives cluster -> build, owner and requested disk.

Some jobs (test_suite_executor_cloud-TAF) keep builds for only a few days,
so the mapping is saved in a JSON file carried from run to run rather than
rebuilt from Jenkins.
"""

import datetime as dt
import json
import re
import urllib.parse
import urllib.request

QE7_MARKERS = ("qe-7", "264138468394")
CLUSTER_RE = re.compile(
    r"Cluster created with cluster ID: ([0-9a-f-]{36})"
    r"|cluster provisioning as ([0-9a-f-]{36})")
SPEC_RE = re.compile(r"Specs are (\[.*?\]) \. Provider is \w+\. Region is ([a-z0-9-]+)")
DISK_RE = re.compile(r"'sizeInGb': (\d+)(?:, 'iops': (\d+))?")
FLEET_RE = re.compile(r"\b(taf-dl-[0-9a-f]{10})\b")
KEEP_DAYS = 30     # forget clusters/fleets/builds not seen for this long


def _get_json(url, timeout=120):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.load(resp)


def _q(tree):
    return urllib.parse.quote(tree, safe="=,")


class JenkinsMap(object):
    """cluster/fleet -> {job, build, owner, disk_gb, iops, region, clone}"""

    def __init__(self, url, path=None, job_filter=None, log=print):
        self.url = url.rstrip("/")
        self.path = path
        self.log = log
        self.job_filter = job_filter or re.compile(
            r"fusion|volume|executor_cloud", re.I)
        self.data = {"clusters": {}, "fleets": {}, "builds_scanned": {}}
        self.today = dt.datetime.now(dt.timezone.utc).date().isoformat()
        if path:
            try:
                with open(path) as f:
                    self.data.update(json.load(f))
            except FileNotFoundError:
                pass
        # Older state has no dates: start their clock now
        for name in ("clusters", "fleets"):
            for entry in self.data[name].values():
                entry.setdefault("seen", self.today)
        scanned = self.data["builds_scanned"]
        for key, value in scanned.items():
            if not isinstance(value, dict):
                scanned[key] = dict(found=value, seen=self.today)

    def prune(self, keep_days=KEEP_DAYS):
        cutoff = (dt.datetime.now(dt.timezone.utc)
                  - dt.timedelta(days=keep_days)).date().isoformat()
        for name in ("clusters", "fleets", "builds_scanned"):
            self.data[name] = {k: v for k, v in self.data[name].items()
                               if v.get("seen", self.today) >= cutoff}

    def save(self):
        self.prune()
        if self.path:
            with open(self.path, "w") as f:
                json.dump(self.data, f, indent=1, sort_keys=True)

    def cluster(self, cid):
        return self.data["clusters"].get(cid) or {}

    def fleet(self, name):
        return self.data["fleets"].get(name) or {}

    def _jobs(self):
        d = _get_json(self.url + "/api/json?tree=" + _q("jobs[name,url]"))
        return [j for j in d["jobs"] if self.job_filter.search(j["name"])]

    def _builds(self, job, since):
        tree = ("allBuilds[number,timestamp,duration,building,"
                "actions[parameters[name,value],causes[userId,userName]]]{0,400}")
        d = _get_json("%s/job/%s/api/json?tree=%s"
                      % (self.url, urllib.parse.quote(job), _q(tree)))
        for b in d.get("allBuilds", []):
            start = dt.datetime.fromtimestamp(b["timestamp"] / 1000,
                                              dt.timezone.utc)
            end = start + dt.timedelta(milliseconds=b.get("duration") or 0)
            if b.get("building") or end >= since:
                yield b, start

    @staticmethod
    def _params_and_owner(build):
        params, owner = {}, None
        for action in build.get("actions") or []:
            for p in action.get("parameters") or []:
                params[p["name"]] = str(p.get("value"))
            for c in action.get("causes") or []:
                owner = owner or c.get("userName") or c.get("userId")
        user = params.get("capella_user", "")
        if "@" in user and not user.startswith("qe-7-"):
            owner = user.split("@")[0]
        return params, owner

    def _scan_console(self, job, number, owner, region_default):
        url = "%s/job/%s/%d/consoleText" % (
            self.url, urllib.parse.quote(job), number)
        spec, found = {}, 0
        label = "%s #%d" % (job, number)
        with urllib.request.urlopen(url, timeout=600) as resp:
            for raw in resp:
                line = raw.decode("utf-8", "replace")
                m = SPEC_RE.search(line)
                if m:
                    disk = DISK_RE.search(m.group(1))
                    spec = dict(region=m.group(2))
                    if disk:
                        spec["disk_gb"] = int(disk.group(1))
                        if disk.group(2):
                            spec["iops"] = int(disk.group(2))
                m = CLUSTER_RE.search(line)
                if m:
                    cid = m.group(1) or m.group(2)
                    entry = dict(job=label, owner=owner, clone=bool(m.group(2)),
                                 region=spec.get("region", region_default),
                                 seen=self.today)
                    entry.update({k: v for k, v in spec.items() if k != "region"})
                    self.data["clusters"].setdefault(cid, entry)
                    found += 1
                for name in FLEET_RE.findall(line):
                    if name not in self.data["fleets"]:
                        self.data["fleets"][name] = dict(job=label, owner=owner,
                                                         seen=self.today)
                        found += 1
        return found

    def scan(self, since, rescan_running=True):
        """Scan every qe-7 build that ran since ``since``. Finished builds are
        scanned once; running ones again on each call (they may create more).
        Returns the list of builds still running, with the clusters/fleets
        they mention, for the idle check."""
        running = []
        for job in self._jobs():
            name = job["name"]
            try:
                builds = list(self._builds(name, since))
            except Exception as e:     # one unreadable job must not stop the scan
                self.log("Jenkins: skip %s (%s)" % (name, e))
                continue
            for build, start in builds:
                params, owner = self._params_and_owner(build)
                blob = json.dumps(params)
                if not any(m in blob for m in QE7_MARKERS) and \
                        "fusion" not in params.get("descriptor", "") and \
                        "fusion" not in name:
                    continue
                key = "%s#%d" % (name, build["number"])
                if build.get("building"):
                    running.append(dict(job=name, build=build["number"],
                                        owner=owner, started=start.isoformat()))
                    if not rescan_running and key in self.data["builds_scanned"]:
                        continue
                elif key in self.data["builds_scanned"]:
                    continue
                try:
                    n = self._scan_console(name, build["number"], owner,
                                           params.get("region"))
                except Exception as e:
                    self.log("Jenkins: console %s unreadable (%s)" % (key, e))
                    continue
                if not build.get("building"):
                    self.data["builds_scanned"][key] = dict(found=n,
                                                            seen=self.today)
        return running


def main(argv=None):
    """Scan step, run on an agent that can reach Jenkins (the AWS agents
    that do the rest of the report cannot):
        python3 -m scripts.qe7_cost.jenkins --state-dir state --since-days 3
    Updates <state>/jenkins_map.json and writes <state>/running_builds.json."""
    import argparse
    import os
    p = argparse.ArgumentParser(description="Map qe-7 clusters to Jenkins builds")
    p.add_argument("--state-dir", default="qe7_cost_state")
    p.add_argument("--since-days", type=float, default=3)
    p.add_argument("--jenkins-url", default="http://172.23.121.80")
    args = p.parse_args(argv)
    os.makedirs(args.state_dir, exist_ok=True)
    jmap = JenkinsMap(args.jenkins_url,
                      os.path.join(args.state_dir, "jenkins_map.json"))
    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.since_days)
    running = jmap.scan(since)
    jmap.save()
    with open(os.path.join(args.state_dir, "running_builds.json"), "w") as f:
        json.dump(running, f, indent=1)
    print("Jenkins: %d clusters, %d fleets mapped; %d qe-7 builds running"
          % (len(jmap.data["clusters"]), len(jmap.data["fleets"]), len(running)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
