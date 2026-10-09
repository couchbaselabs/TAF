"""
REST variants for Capella dedicated clusters.

CBRestConnection.__new__ builds these instead of the on-prem class when
the node has type == "dedicated" (map: CBRestConnection.capella_variant):
    BucketHelper   -> bucket_helper.CapellaBucketHelper
    ClusterRestAPI -> cluster_rest.CapellaClusterRestAPI
Call sites do not change.
"""
