from bucket_utils.bucket_ready_functions import BucketUtils


class CapellaBucketUtils(BucketUtils):
    """
    Capella dedicated variant of BucketUtils, created by the Capella base
    test. The DB user has no RBAC admin rights, so local users are skipped.
    """
    def add_rbac_user(self, cluster_node, testuser=None, rolelist=None):
        self.log.warning("CAPELLA-GAP add_rbac_user skipped: Capella DB "
                         "users are managed by the Capella API")
