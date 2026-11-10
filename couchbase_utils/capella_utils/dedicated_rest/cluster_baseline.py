import time

from BucketLib.BucketOperations import BucketHelper
from capella_utils.dedicated import CapellaUtils
from capella_utils.dedicated_rest.context import CapellaContext
from capella_utils.dedicated_rest.scale_spec import CapellaScaleSpec
from global_vars import logger


class ClusterBaseline(object):
    """
    Reused Capella cluster across tests.
    setUp: scale_to() the node count the test needs (no-op if already
    there), then note the bucket names. tearDown: restore() deletes only
    the buckets the test created. The cluster is not scaled back, so
    the next test scales only if it needs a different size.
    """

    def __init__(self, cluster):
        self.cluster = cluster
        self.context = CapellaContext.of(cluster.master)
        self.log = logger.get("test")
        self.bucket_names = set()

    def record_buckets(self):
        self.bucket_names = {bucket.name for bucket in self.cluster.buckets}

    def restore(self):
        self.delete_new_buckets()

    def delete_new_buckets(self):
        new_buckets = [bucket for bucket in self.cluster.buckets
                       if bucket.name not in self.bucket_names]
        start = time.time()
        for bucket in new_buckets:
            BucketHelper(self.cluster.master).delete_bucket(bucket)
        self.cluster.buckets = [bucket for bucket in self.cluster.buckets
                                if bucket.name in self.bucket_names]
        self.log.info("Capella cleanup: deleted buckets %s in %.1f sec"
                      % ([b.name for b in new_buckets], time.time() - start))

    def scale_to(self, task, node_count):
        """
        :return: True when the cluster has node_count nodes
                 (Capella minimum: CapellaScaleSpec.MIN_DATA_NODES)
        """
        if node_count < CapellaScaleSpec.MIN_DATA_NODES:
            self.log.warning("CAPELLA-GAP nodes_init=%s raised to %s, the "
                             "Capella minimum"
                             % (node_count, CapellaScaleSpec.MIN_DATA_NODES))
            node_count = CapellaScaleSpec.MIN_DATA_NODES
        current = self._capella_node_count()
        delta = node_count - current
        if delta == 0:
            self.log.info("Capella setup: cluster %s already has %s nodes, "
                          "no scale" % (self.cluster.id, current))
            return True
        start = time.time()
        # Capella rebalance: only the number of nodes added / removed counts
        scale = task.async_rebalance(self.cluster,
                                     to_add=[None] * max(delta, 0),
                                     to_remove=[None] * max(-delta, 0))
        result = task.jython_task_manager.get_task_result(scale)
        self.log.info("Capella setup: scaled cluster %s %s -> %s nodes in "
                      "%.1f sec, result=%s"
                      % (self.cluster.id, current, node_count,
                         time.time() - start, result))
        return result

    def _capella_node_count(self):
        specs = CapellaUtils.get_cluster_specs(
            self.context.pod, self.context.tenant, self.cluster.id)
        return sum(spec["count"] for spec in specs)
