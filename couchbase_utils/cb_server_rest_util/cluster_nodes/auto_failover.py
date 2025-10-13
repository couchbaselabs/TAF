"""
https://docs.couchbase.com/server/current/rest-api/rest-rebalance-overview.html
"""
from cb_server_rest_util.connection import CBRestConnection


class AutoFailoverAPI(CBRestConnection):
    def __init__(self):
        super(AutoFailoverAPI, self).__init__()

    def get_auto_failover_settings(self):
        """
        GET /settings/autoFailover
        docs.couchbase.com/server/current/rest-api/rest-cluster-autofailover-settings.html
        """
        api = self.base_url + "/settings/autoFailover"
        status, content, _ = self.request(api, self.GET)
        return status, content

    def update_auto_failover_settings(
            self, enabled="true", timeout=120, max_count=None,
            fo_on_disk_issue=None, fo_on_disk_timeout=None,
            can_abort_rebalance=None, failover_preserve_durability=None,
            allow_ephemeral_failover_with_no_replicas=None):
        """
        POST /settings/autoFailover
        docs.couchbase.com/server/current/rest-api/rest-cluster-autofailover-enable.html
        """
        api = self.base_url + "/settings/autoFailover"
        params = {"enabled": enabled,
                  "timeout": timeout}

        if max_count is not None:
            params["maxCount"] = max_count
        if fo_on_disk_issue is not None:
            params["failoverOnDataDiskIssues[enabled]"] = fo_on_disk_issue
        if fo_on_disk_timeout is not None:
            params["failoverOnDataDiskIssues[timePeriod]"] = fo_on_disk_timeout
        if can_abort_rebalance is not None:
            params["canAbortRebalance"] = can_abort_rebalance
        if failover_preserve_durability is not None:
            params["failoverPreserveDurabilityMajority"] = \
                failover_preserve_durability
        if allow_ephemeral_failover_with_no_replicas is not None:
            params["allowFailoverEphemeralNoReplicas"] = \
                allow_ephemeral_failover_with_no_replicas
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content

    def reset_auto_failover_count(self):
        """
        POST /settings/autoFailover/resetCount
        docs.couchbase.com/server/current/rest-api/rest-cluster-autofailover-reset.html
        """
        api = self.base_url + "/settings/autoFailover/resetCount"
        status, content, _ = self.request(api, self.POST)
        return status, content

    def set_recovery_type(self, otp_node, recovery_type):
        """
        POST /controller/setRecoveryType
        docs.couchbase.com/server/current/rest-api/rest-node-recovery-incremental.html
        """
        params = {"otpNode": otp_node, "recoveryType": recovery_type}
        api = self.base_url + "/controller/setRecoveryType"
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content
