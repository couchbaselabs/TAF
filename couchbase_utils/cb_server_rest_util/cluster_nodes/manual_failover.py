"""
https://docs.couchbase.com/server/current/rest-api/rest-rebalance-overview.html
"""
from cb_server_rest_util.connection import CBRestConnection


class ManualFailoverAPI(CBRestConnection):
    def __init__(self):
        super(ManualFailoverAPI, self).__init__()

    def perform_hard_failover(self, otp_nodes, allow_unsafe=None, timeout=300):
        """
        POST /controller/failOver
        docs.couchbase.com/server/current/rest-api/rest-node-failover.html
        """
        api = self.base_url + "/controller/failOver"
        params = {"otpNode": otp_nodes}
        if allow_unsafe is not None:
            # Should be a string value true / false
            params["allowUnsafe"] = allow_unsafe
        status, content, _ = self.request(api, self.POST, params=params, timeout=timeout)
        return status, content

    def perform_graceful_failover(self, otp_nodes, timeout=300):
        """
        POST /controller/startGracefulFailover
        docs.couchbase.com/server/current/rest-api/rest-failover-graceful.html
        """
        api = self.base_url + "/controller/startGracefulFailover"
        params = {"otpNode": otp_nodes}
        status, content, _ = self.request(api, self.POST, params=params, timeout=timeout)
        return status, content

    def set_failover_recovery_type(self, otp_node, recovery_type):
        """
        POST /controller/setRecoveryType
        docs.couchbase.com/server/current/rest-api/rest-node-recovery-incremental.html
        """

        api = self.base_url + "/controller/setRecoveryType"
        params = {"otpNode": otp_node,
                  "recoveryType": recovery_type}
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content
