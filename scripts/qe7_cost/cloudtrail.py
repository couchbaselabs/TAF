"""Download CloudTrail management events for the cost report.

LookupEvents allows about 2 calls/s per account and region, returns at most
50 events per call, and each call takes several seconds, so a busy day in
us-east-1 runs to tens of thousands of events. The window is split into
slices fetched in parallel under a shared rate limit. Each slice is written
to ``<dir>/<region>_<EventName>_<MMDDHH>.jsonl`` (one raw event per line) and
marked ``.done`` when complete, so an interrupted run resumes where it left
off and a backtest can reuse files from earlier runs.

Only a full slice that ended at least DELIVERY_LAG ago is marked ``.done``.
The last slice of a run stops at the current time and CloudTrail delivers
events several minutes late, so that slice is fetched again (and its file
rewritten) by the next run; otherwise its later hours would never be read.
"""

import concurrent.futures as cf
import datetime as dt
import os
import threading
import time

try:
    from botocore.exceptions import ClientError
except ImportError:          # unit tests run without TAF's requirements
    class ClientError(Exception):
        pass

EVENT_NAMES = ("RunInstances", "CreateFleet", "TerminateInstances",
               "CreateLaunchTemplate")
MIN_CALL_INTERVAL = 0.55     # seconds between calls, per region
DELIVERY_LAG = dt.timedelta(minutes=20)   # CloudTrail events can arrive late


class _RateLimiter(object):
    def __init__(self, interval):
        self.interval = interval
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            delay = self._last + self.interval - time.time()
            if delay > 0:
                time.sleep(delay)
            self._last = time.time()


def slice_path(out_dir, region, event_name, start):
    return os.path.join(out_dir, "%s_%s_%s.jsonl"
                        % (region, event_name, start.strftime("%m%d%H")))


def _fetch_slice(qe, region, event_name, start, end, complete, out_dir,
                 limiter, log):
    path = slice_path(out_dir, region, event_name, start)
    if os.path.exists(path + ".done"):
        return 0
    kwargs = dict(StartTime=start, EndTime=end, MaxResults=50,
                  LookupAttributes=[{"AttributeKey": "EventName",
                                     "AttributeValue": event_name}])
    count = 0
    with open(path, "w") as out:
        while True:
            for attempt in range(10):
                limiter.wait()
                try:
                    resp = qe.client("cloudtrail", region).lookup_events(
                        **kwargs)
                    break
                except ClientError as e:
                    code = e.response["Error"]["Code"]
                    if "Throttl" in code or code == "RequestLimitExceeded":
                        time.sleep(2 + attempt)
                        continue
                    raise
            else:
                raise RuntimeError("CloudTrail kept throttling %s %s %s"
                                   % (region, event_name, start))
            for event in resp["Events"]:
                out.write(event["CloudTrailEvent"] + "\n")
                count += 1
            if "NextToken" not in resp:
                break
            kwargs["NextToken"] = resp["NextToken"]
    if complete:
        open(path + ".done", "w").close()
    log("  %s %s %s: %d events%s" % (region, event_name,
                                      start.strftime("%m-%d %H:00"), count,
                                      "" if complete else " (partial)"))
    return count


def download(qe, regions, start, end, out_dir, slice_hours=6, workers=8,
             event_names=EVENT_NAMES, log=print):
    """Fetch every slice of [start, end) for each region and event name.
    Regions run concurrently (each has its own rate limit)."""
    os.makedirs(out_dir, exist_ok=True)
    step = dt.timedelta(hours=slice_hours)
    settled = dt.datetime.now(dt.timezone.utc) - DELIVERY_LAG
    jobs = []
    for region in regions:
        limiter = _RateLimiter(MIN_CALL_INTERVAL)
        t = start
        while t < end:
            e = min(end, t + step)
            complete = e == t + step and e <= settled
            for name in event_names:
                jobs.append((region, name, t, e, complete, limiter))
            t += step
    total = 0
    with cf.ThreadPoolExecutor(max(1, workers * len(regions))) as pool:
        futures = [pool.submit(_fetch_slice, qe, r, n, s, e, c, out_dir, lim,
                               log)
                   for r, n, s, e, c, lim in jobs]
        for future in cf.as_completed(futures):
            total += future.result()
    return total


def prune(out_dir, older_than_days, log=print):
    """Delete downloaded slices older than the lookback window (by mtime)."""
    cutoff = time.time() - older_than_days * 86400
    removed = 0
    for name in os.listdir(out_dir) if os.path.isdir(out_dir) else []:
        path = os.path.join(out_dir, name)
        if os.path.getmtime(path) < cutoff:
            os.remove(path)
            removed += 1
    if removed:
        log("Pruned %d old CloudTrail files" % removed)
