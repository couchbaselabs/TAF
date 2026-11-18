import copy
import re
import time

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster

# Backups successfully requested by this test run (shared across TCs in the
# same testrunner process), as (epoch_time, backup_id, "Class.test"). Used to
# explain per-cluster hourly quota 429s when they happen.
CREATED_BACKUPS = []
QUOTA_WINDOW_SECS = 3600
REQUEST_ID_HEADER = "Vnd-Cloud-Couchbase-Com-Request-Id"


class CloudSnapshotBackupBase(GetCluster):

    def setUp(self, nomenclature="Cloud_Snapshot_Backup_Base"):
        GetCluster.setUp(self, nomenclature)
        self.default_retention = 168
        self.backup_id = None
        self.log.info("[backup-trace] START {} on cluster {}".format(
            self._tc_name(), self.cluster_id))
        self.log_backup_state("setUp")

    def tearDown(self):
        super(CloudSnapshotBackupBase, self).tearDown()

    def _tc_name(self):
        return "{}.{}".format(self.__class__.__name__, self._testMethodName)

    @staticmethod
    def _describe_call(method, result):
        req = getattr(result, "request", None)
        if req is not None and getattr(req, "url", None):
            return "{} {}".format(req.method, req.url)
        return getattr(method, "__name__", str(method))

    def _trace_response(self, method, result, attempt, max_attempts):
        """One line per snapshot API call, with the backend request id so
        failures can be looked up in Capella logs."""
        msg = ("[backup-trace] {} | {} | attempt {}/{} | status={} | "
               "request_id={} | retry_after={} | message={}".format(
                   self._tc_name(), self._describe_call(method, result),
                   attempt, max_attempts, result.status_code,
                   result.headers.get(REQUEST_ID_HEADER),
                   result.headers.get("Retry-After"),
                   self._error_message(result) or "-"))
        if result.status_code >= 400:
            self.log.warning(msg)
        else:
            self.log.debug(msg)

    def _record_created_backup(self, result):
        if result.status_code == 202:
            backup_id = self._extract_backup_id(result)
            CREATED_BACKUPS.append((time.time(), backup_id, self._tc_name()))
            self.log.info("[backup-trace] Backup requested by {}: id={} "
                          "(#{} in this run)".format(
                              self._tc_name(), backup_id,
                              len(CREATED_BACKUPS)))

    def log_quota_usage(self):
        now = time.time()
        recent = [b for b in CREATED_BACKUPS
                  if now - b[0] < QUOTA_WINDOW_SECS]
        self.log.warning(
            "[backup-trace] {} backup(s) requested by this run in the last "
            "{}s (total this run: {}):".format(
                len(recent), QUOTA_WINDOW_SECS, len(CREATED_BACKUPS)))
        for ts, backup_id, tc in recent:
            self.log.warning("[backup-trace]   {:.0f}s ago | id={} | by {}"
                             .format(now - ts, backup_id, tc))
        if recent:
            self.log.warning(
                "[backup-trace] Oldest of these leaves the quota window in "
                "~{:.0f}s".format(QUOTA_WINDOW_SECS - (now - recent[0][0])))

    @staticmethod
    def _summarize_backup(item):
        data = item.get("data", item) if isinstance(item, dict) else {}
        progress = data.get("progress") or {}
        return "id={} status={} type={} method={} created={}".format(
            data.get("id"), progress.get("status"), data.get("type"),
            data.get("method"),
            data.get("createdAt") or progress.get("time") or
            data.get("date"))

    def log_backup_state(self, reason):
        """Log the backups currently on the cluster (as the backend sees
        them) - helps tell quota/ordering problems apart from API bugs."""
        try:
            res = self.capellaAPI.cluster_ops_apis.list_cloud_snapshot_backups(
                self.organisation_id, self.project_id, self.cluster_id)
            if res.status_code != 200:
                self.log.warning(
                    "[backup-trace] ({}) Could not list backups: status={} "
                    "request_id={} message={}".format(
                        reason, res.status_code,
                        res.headers.get(REQUEST_ID_HEADER),
                        self._error_message(res) or res.content))
                return
            items = res.json().get("data", []) or []
            self.log.info("[backup-trace] ({}) {} backup(s) on cluster {}"
                          .format(reason, len(items), self.cluster_id))
            for item in items:
                self.log.info("[backup-trace]   {}".format(
                    self._summarize_backup(item)))
        except Exception as e:
            self.log.warning("[backup-trace] ({}) Error while listing "
                             "backups: {}".format(reason, e))

    @staticmethod
    def _error_message(result):
        try:
            return result.json().get("message", "") or ""
        except Exception:
            return ""

    @classmethod
    def is_backup_quota_exceeded(cls, result):
        # e.g. "Cannot create a snapshot backup because the quota limit has
        # been exceeded. The allowed rate is 5 backup(s) within 1h0m0s."
        return (result.status_code == 429 and
                "quota limit" in cls._error_message(result))

    def api_call_with_retry(self, method, *args, **kwargs):
        """
        Cloud snapshot endpoints have per-cluster limits (e.g. "1 request(s)
        per 20s") on top of the per-key org rate limit. Swapping owner keys
        via handle_rate_limit doesn't help with a per-cluster limit (and does
        nothing when the call carries its own auth header), so wait out the
        window and retry the same call. The hourly backup quota can't be
        waited out within a test, so return that 429 to the caller as-is.
        """
        max_attempts = 4
        result = None
        for attempt in range(max_attempts):
            result = method(*args, **kwargs)
            self._trace_response(method, result, attempt + 1, max_attempts)
            if result.status_code != 429:
                if getattr(method, "__name__", "") == \
                        "create_cloud_snapshot_backup":
                    self._record_created_backup(result)
                return result
            if self.is_backup_quota_exceeded(result):
                self.log.warning("Cloud snapshot backup quota exceeded: {}"
                                 .format(self._error_message(result)))
                self.log_quota_usage()
                self.log_backup_state("quota exceeded")
                return result
            if attempt == max_attempts - 1:
                self.log.error("[backup-trace] Still rate limited after {} "
                               "attempts, giving up".format(max_attempts))
                return result
            header_retry_after = result.headers.get("Retry-After")
            retry_after = int(header_retry_after or 60)
            window = re.search(r"request\(s\) per (\d+)s",
                               self._error_message(result))
            if window:
                # Snapshot endpoints often omit Retry-After; then the window
                # from the message is enough (don't fall back to 60s).
                wait = max(int(window.group(1)),
                           int(header_retry_after or 0)) + 1
                self.log.warning(
                    "Per-cluster rate limit hit ({}), retrying in {}s "
                    "(attempt {}/{})".format(self._error_message(result),
                                             wait, attempt + 1, max_attempts))
                time.sleep(wait)
            else:
                self.handle_rate_limit(retry_after)
        return result

    @staticmethod
    def _extract_backup_id(result):
        try:
            content = result.json()
        except Exception:
            return None

        if isinstance(content, dict):
            if "backupId" in content:
                return content["backupId"]
            if "data" in content and isinstance(content["data"], list):
                for item in content["data"]:
                    if isinstance(item, dict):
                        data = item.get("data", item)
                        if isinstance(data, dict) and data.get("id"):
                            return data["id"]
        return None

    def get_existing_backup_id(self):
        res = self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.list_cloud_snapshot_backups,
            self.organisation_id, self.project_id, self.cluster_id)
        if res.status_code != 200:
            return None
        return self._extract_backup_id(res)

    def ensure_backup(self, wait_for_complete=False):
        res = self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.create_cloud_snapshot_backup,
            self.organisation_id, self.project_id, self.cluster_id,
            retention=self.default_retention)
        if self.is_backup_quota_exceeded(res):
            # Hourly backup quota hit (earlier tests on this cluster already
            # created backups) - reuse an existing backup instead.
            self.log.warning("Backup quota exceeded, reusing an existing "
                             "cloud snapshot backup")
        elif res.status_code not in [202, 409]:
            self.log.error("[backup-trace] ensure_backup failed: status={} "
                           "request_id={} body={}".format(
                               res.status_code,
                               res.headers.get(REQUEST_ID_HEADER),
                               res.content))
            self.log_backup_state("ensure_backup failure")
            self.fail("Unable to create cloud snapshot backup (status {})"
                      .format(res.status_code))

        backup_id = self._extract_backup_id(res)
        source = "create response (status {})".format(res.status_code)
        if not backup_id:
            backup_id = self.get_existing_backup_id()
            source = "existing backup from list (create status {})".format(
                res.status_code)
        if not backup_id:
            self.log_backup_state("no backup id")
            self.fail("Backup ID not found from create/list cloud snapshot "
                      "backup response (create status {})"
                      .format(res.status_code))
        self.log.info("[backup-trace] {} using backup {} from {}".format(
            self._tc_name(), backup_id, source))

        if wait_for_complete:
            if not self.wait_for_backup_complete(backup_id):
                self.log.error("[backup-trace] Backup {} not complete; the "
                               "test will continue but may fail".format(
                                   backup_id))
        self.backup_id = backup_id
        return backup_id

    def wait_for_backup_complete(self, backup_id, timeout=900):
        start_time = time.time()
        end_time = start_time + timeout
        last_status = None
        while time.time() <= end_time:
            res = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.list_cloud_snapshot_backups,
                self.organisation_id, self.project_id, self.cluster_id)
            if res.status_code != 200:
                time.sleep(20)
                continue

            try:
                items = res.json().get("data", [])
            except Exception:
                items = []
            found = False
            for item in items:
                data = item.get("data", item) if isinstance(item, dict) else {}
                if data.get("id") == backup_id:
                    found = True
                    status = data.get("progress", {}).get("status")
                    if status != last_status:
                        self.log.info(
                            "[backup-trace] Backup {} status: {} -> {} "
                            "after {:.0f}s".format(
                                backup_id, last_status, status,
                                time.time() - start_time))
                        last_status = status
                    if status == "complete":
                        return True
            if not found:
                self.log.warning("[backup-trace] Backup {} not in list "
                                 "response ({} item(s))".format(
                                     backup_id, len(items)))
            time.sleep(20)
        self.log.error("[backup-trace] Backup {} did not complete within "
                       "{}s, last status: {}".format(
                           backup_id, timeout, last_status))
        self.log_backup_state("wait_for_backup_complete timeout")
        return False

    def clone_payload(self):
        res = self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.fetch_cluster_info,
            self.organisation_id, self.project_id, self.cluster_id)
        if res.status_code != 200:
            self.log.error(res.content)
            self.fail("Unable to fetch cluster details for clone payload")

        cluster_data = res.json()
        cloud_provider = copy.deepcopy(cluster_data.get("cloudProvider", {}))
        cloud_provider["cidr"] = self.next_cidr(cloud_provider.get("cidr"))

        payload = {
            "name": self.generate_random_string(special_characters=False),
            "description": "Cloud snapshot clone automation",
            "cloudProvider": cloud_provider,
            "availability": copy.deepcopy(cluster_data.get("availability", {"type": "single"})),
            "support": copy.deepcopy(cluster_data.get("support", {"plan": "developer pro", "timezone": "PT"}))
        }

        zones = cluster_data.get("zones")
        if zones:
            payload["zones"] = zones
        return payload

    @staticmethod
    def next_cidr(cidr):
        if not cidr or "/" not in cidr:
            return "10.200.0.0/23"
        ip, mask = cidr.split("/")
        octets = ip.split(".")
        if len(octets) != 4:
            return "10.200.0.0/23"
        try:
            octets[1] = str((int(octets[1]) + 7) % 254 or 1)
            return "{}.{}.{}.{}".format(
                octets[0], octets[1], octets[2], octets[3]) + "/" + mask
        except Exception:
            return "10.200.0.0/23"
