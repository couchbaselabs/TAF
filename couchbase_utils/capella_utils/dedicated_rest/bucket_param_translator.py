from BucketLib.bucket import Bucket
from constants.cloud_constants.capella_cluster import CloudCluster


class BucketParamTranslator(object):
    """
    Pure mapping between TAF's on-prem bucket params (Bucket object's
    __dict__, as passed to BucketHelper.create_bucket) and the Capella
    bucket API. No HTTP calls here.

    translate()   : on-prem params -> (capella params, unsupported params)
    diff_applied(): requested params vs bucket json read back from the node
    """

    # On-prem setting -> Capella bucket API field.
    # Same 7 fields the legacy create_default_bucket() path sends today.
    # Extend only after confirming the field against the Capella API.
    FIELD_MAP = {
        Bucket.name: CloudCluster.Bucket.name,
        Bucket.ramQuotaMB: CloudCluster.Bucket.ramQuotaMB,
        Bucket.replicaNumber: CloudCluster.Bucket.replicaNumber,
        Bucket.conflictResolutionType:
            CloudCluster.Bucket.conflictResolutionType,
        Bucket.flushEnabled: CloudCluster.Bucket.flushEnabled,
        Bucket.durabilityMinLevel: CloudCluster.Bucket.durabilityMinLevel,
        Bucket.maxTTL: CloudCluster.Bucket.maxTTL,
    }

    # Bucket.get_params() entries that are framework bookkeeping,
    # not settings a test asks for
    NOT_SETTINGS = {"uuid", "threadsNumber", "bucket_counter",
                    "scope_counter", "collection_counter"}

    # Settings whose value is reported by GET /pools/default/buckets/<name>
    # under the same key and in the same format as the on-prem param.
    # ramQuotaMB is left out: node reports per-node bytes under 'quota'
    VERIFIABLE = (Bucket.replicaNumber, Bucket.conflictResolutionType,
                  Bucket.durabilityMinLevel, Bucket.maxTTL,
                  Bucket.storageBackend, Bucket.evictionPolicy,
                  Bucket.compressionMode)

    _defaults = None

    @classmethod
    def defaults(cls):
        if cls._defaults is None:
            cls._defaults = Bucket().__dict__
        return cls._defaults

    # POC-NOTE (remove later)
    # WHY: on-prem sends ~33 bucket settings, Capella API takes only a few.
    #      This decides what to send and what to report, instead of the
    #      old "pick 7, silently drop the rest"
    # CAME FROM: test -> create_bucket -> BucketCreateTask
    #      -> CapellaBucketHelper.create_bucket -> here
    # DOES: known field -> renamed for Capella and sent
    #      not known + left at default -> ignored
    #      not known + changed by test -> returned as unsupported (GAP)
    @classmethod
    def translate(cls, bucket_params):
        """
        :param bucket_params: dict of on-prem bucket settings
        :return: (capella_params, unsupported)
                 unsupported - list of (setting, value) the caller asked
                 for (differs from Bucket defaults) with no Capella field
        """
        defaults = cls.defaults()
        capella_params = dict()
        unsupported = list()
        for setting in Bucket.get_params():
            if setting in cls.NOT_SETTINGS:
                continue
            value = bucket_params.get(setting)
            if value is None:
                continue
            if setting in cls.FIELD_MAP:
                capella_params[cls.FIELD_MAP[setting]] = \
                    cls._to_capella_value(setting, value)
            elif value != defaults.get(setting):
                unsupported.append((setting, value))
        return capella_params, unsupported

    # POC-NOTE (remove later)
    # WHY: Capella accepting a request does not mean it applied it.
    #      Compare what we asked with what the bucket really has
    # CAME FROM: CapellaBucketHelper._verify_applied (after bucket create)
    # DOES: returns settings where asked != applied
    @classmethod
    def diff_applied(cls, bucket_params, bucket_json):
        """
        :param bucket_params: dict of on-prem bucket settings requested
        :param bucket_json: GET /pools/default/buckets/<name> content
        :return: list of (setting, requested, applied) that differ
        """
        mismatches = list()
        for setting in cls.VERIFIABLE:
            requested = bucket_params.get(setting)
            if requested is None or setting not in bucket_json:
                continue
            applied = bucket_json[setting]
            if str(requested).lower() != str(applied).lower():
                mismatches.append((setting, requested, applied))
        return mismatches

    @staticmethod
    def _to_capella_value(setting, value):
        if setting == Bucket.flushEnabled:
            return value in (1, True, "1", "true", "True")
        if setting == Bucket.maxTTL:
            return {"unit": "seconds", "value": int(value)}
        if setting == Bucket.ramQuotaMB:
            return int(value)
        return value
