import time

from BucketLib.BucketOperations import BucketHelper
from capella_utils.dedicated import CapellaUtils
from capella_utils.dedicated_rest.bucket_param_translator import \
    BucketParamTranslator
from capella_utils.dedicated_rest.context import CapellaContext
from common_lib import sleep


class CapellaBucketHelper(BucketHelper):
    """
    Capella dedicated variant of BucketHelper.

    Returned automatically by BucketHelper(server) when
    server.type == "dedicated" (see CBRestConnection.capella_variant).
    Bucket lifecycle calls go through the Capella control plane via
    CapellaUtils; every other BucketHelper method is inherited and keeps
    talking to the node's REST endpoint.

    Method names, arguments and return values match BucketHelper.
    """
    visibility_timeout = 300
    poll_interval = 5

    # POC-NOTE (remove later)
    # WHY: on Capella, bucket create must go through the Capella API
    #      (that is what we want to test), not the node's REST endpoint
    # CAME FROM: basic_ops.setUp -> ClusterSetup.create_bucket
    #      -> bucket_util.create_default_bucket -> bucket_util.create_bucket
    #      -> BucketCreateTask.call() -> BucketHelper(server).create_bucket()
    #      BucketHelper(server) became CapellaBucketHelper because
    #      server.type == "dedicated" (see CBRestConnection.__new__)
    # DOES: 1) translate settings, log GAPs for the ones Capella can't take
    #       2) create via CapellaUtils.create_bucket (Capella API)
    #       3) wait till the node shows the bucket
    #       4) read it back, log GAPs for anything not applied
    #       returns True/False exactly like the on-prem BucketHelper
    def create_bucket(self, bucket_params=dict()):
        """
        Same contract as BucketHelper.create_bucket: returns True/False
        """
        bucket_name = bucket_params.get("name")
        context = CapellaContext.of(self.server)
        capella_params, unsupported = \
            BucketParamTranslator.translate(bucket_params)
        for setting, value in unsupported:
            self.log.warning("CAPELLA-GAP create_bucket('%s'): '%s=%s' not "
                             "applied, no Capella API field"
                             % (bucket_name, setting, value))

        self.log.info("Creating bucket '%s' via Capella API on cluster %s"
                      % (bucket_name, context.cluster_id))
        try:
            CapellaUtils.create_bucket(context.pod, context.tenant,
                                       context.cluster, capella_params)
        except Exception as e:
            self.log.critical("Capella create_bucket('%s') failed: %s"
                              % (bucket_name, e))
            return False

        if not self._wait_for_bucket_presence(bucket_name, present=True):
            return False
        self._verify_applied(bucket_name, bucket_params)
        return True

    # POC-NOTE (remove later)
    # WHY: same as create - delete through the Capella API on Capella
    # CAME FROM: any BucketHelper(capella_node).delete_bucket(bucket)
    #      eg bucket_util.delete_bucket during cleanup
    # DOES: CapellaUtils.delete_bucket, then wait till the node no longer
    #      shows it. Returns True/False like on-prem
    def delete_bucket(self, bucket):
        """
        Same contract as BucketHelper.delete_bucket: takes Bucket object,
        returns True/False
        """
        context = CapellaContext.of(self.server)
        self.log.info("Deleting bucket '%s' via Capella API on cluster %s"
                      % (bucket.name, context.cluster_id))
        try:
            CapellaUtils.delete_bucket(context.pod, context.tenant,
                                       context.cluster, bucket.name)
        except Exception as e:
            self.log.critical("Capella delete_bucket('%s') failed: %s"
                              % (bucket.name, e))
            return False
        return self._wait_for_bucket_presence(bucket.name, present=False)

    # POC-NOTE (remove later)
    # WHY: flush through the Capella API on Capella
    # CAME FROM: any BucketHelper(capella_node).flush_bucket(name)
    # DOES: CapellaUtils.flush_bucket. Limitation: CapellaUtils does not
    #      return the API status, so True = "sent without error" only
    def flush_bucket(self, bucket="default"):
        """
        Same contract as BucketHelper.flush_bucket: returns True/False.
        CapellaUtils.flush_bucket does not return the API status, so True
        only means the request was sent without an exception
        """
        bucket_name = getattr(bucket, "name", bucket)
        context = CapellaContext.of(self.server)
        self.log.info("Flushing bucket '%s' via Capella API on cluster %s"
                      % (bucket_name, context.cluster_id))
        try:
            CapellaUtils.flush_bucket(context.pod, context.tenant,
                                      context.cluster, bucket_name)
        except Exception as e:
            self.log.critical("Capella flush_bucket('%s') failed: %s"
                              % (bucket_name, e))
            return False
        return True

    # POC-NOTE (remove later)
    # WHY: Capella replies "accepted" right away and does the work later.
    #      Without this wait the next step would not find the bucket
    # CAME FROM: create_bucket (wait to appear), delete_bucket (to vanish)
    # DOES: asks the node every 5s (inherited bucket_exists -> node REST)
    #      for up to 5 min
    def _wait_for_bucket_presence(self, bucket_name, present):
        """
        Capella applies bucket changes asynchronously. Wait till the node
        (inherited bucket_exists -> node REST) reflects the change
        """
        end_time = time.time() + self.visibility_timeout
        while time.time() < end_time:
            if self.bucket_exists(bucket_name) == present:
                return True
            sleep(self.poll_interval,
                  "Wait for bucket '%s' to be %s on the node"
                  % (bucket_name, "visible" if present else "removed"),
                  log_type="infra")
        self.log.critical("Bucket '%s' not %s on %s after %ss"
                          % (bucket_name,
                             "visible" if present else "removed",
                             self.ip, self.visibility_timeout))
        return False

    # POC-NOTE (remove later)
    # WHY: catch settings that were dropped or not applied by Capella
    #      (this is real control-plane testing)
    # CAME FROM: create_bucket, after the bucket is visible
    # DOES: reads the bucket from the node, compares with what was asked,
    #      logs a GAP per mismatch
    def _verify_applied(self, bucket_name, bucket_params):
        try:
            bucket_json = self.get_buckets_json(bucket_name)
        except Exception as e:
            self.log.warning("CAPELLA-GAP create_bucket('%s'): could not "
                             "read bucket back to verify settings: %s"
                             % (bucket_name, e))
            return
        for setting, requested, applied in \
                BucketParamTranslator.diff_applied(bucket_params,
                                                   bucket_json):
            self.log.warning("CAPELLA-GAP create_bucket('%s'): '%s' "
                             "asked=%s, applied=%s"
                             % (bucket_name, setting, requested, applied))
