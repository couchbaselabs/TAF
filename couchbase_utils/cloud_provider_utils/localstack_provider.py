import logging
import os
from urllib.parse import urlparse

import boto3

from couchbase_utils.cloud_provider_utils.cloud_provider_interface import \
    CloudOperationError, CloudProviderInterface
from couchbase_utils.security_utils.credential_store_utils import \
    CredentialStoreUtils


class LocalstackProvider(CloudProviderInterface):
    def __init__(self, log=None):
        self.log = log if log is not None else logging.getLogger("test")
        self.localstack_region = os.getenv("LOCALSTACK_REGION", "us-east-1")
        self.localstack_access_key_id = os.getenv(
            "LOCALSTACK_ACCESS_KEY_ID")
        self.localstack_secret_access_key = os.getenv(
            "LOCALSTACK_SECRET_ACCESS_KEY")
        self.localstack_endpoint = os.getenv("LOCALSTACK_ENDPOINT")
        # Path -- on the node cbbackupmgr runs on, not on the test runner -- to
        # the CA that signed the endpoint's TLS certificate. Only needed for an
        # https:// endpoint whose CA that node does not already trust, e.g. a
        # MinIO instance with its own private CA. Unset for a plain http://
        # endpoint, which is how this provider was originally used, so existing
        # callers are unaffected.
        #
        # Deliberately --obj-cacert rather than --obj-no-ssl-verify: skipping
        # verification would let a test that means to prove "object-store TLS
        # still works under cluster CRL policy" pass without any certificate
        # being validated at all.
        self.localstack_cacert = os.getenv("LOCALSTACK_CACERT")
        # The same CA is needed in two places on two different filesystems:
        # cbbackupmgr reads it on the backup node (above), while this class's
        # own boto3 calls run on the test runner. A node path is meaningless
        # there, so it gets its own variable -- falling back to
        # localstack_cacert only when that path also exists locally.
        self.localstack_cacert_local = (
            os.getenv("LOCALSTACK_CACERT_LOCAL")
            or (self.localstack_cacert
                if self.localstack_cacert
                and os.path.exists(self.localstack_cacert)
                else None)
        )
        self.validate_credentials()

    def validate_credentials(self):
        if not self.localstack_access_key_id or \
                not self.localstack_secret_access_key or \
                not self.localstack_endpoint:
            raise CloudOperationError("Incomplete Localstack credentials")
        # An https endpoint with no CA to verify against falls back to the
        # node's system trust store, which will not contain a private test CA;
        # cbbackupmgr then fails with "x509: certificate signed by unknown
        # authority" from deep inside an S3 retry loop. Fail here instead,
        # where the cause is obvious.
        if str(self.localstack_endpoint).startswith("https://") \
                and not self.localstack_cacert:
            raise CloudOperationError(
                "Localstack endpoint is https but LOCALSTACK_CACERT is unset: "
                "set it to the CA path on the cbbackupmgr node, or use an "
                "http:// endpoint")

    def _boto3_verify(self):
        """
        `verify` for boto3: a local CA bundle path, or True for system trust.

        Never False -- silently disabling verification here would mask a
        genuinely misconfigured endpoint during cleanup, long after the test
        that cared about TLS has already reported success.
        """
        return self.localstack_cacert_local or True

    def get_cbbackupmgr_flags(self, shell=None):
        flags = (
            "--obj-region {0} --obj-access-key-id {1} "
            "--obj-secret-access-key {2} --obj-endpoint {3} "
            "--s3-force-path-style"
        ).format(self.localstack_region, self.localstack_access_key_id,
                 self.localstack_secret_access_key,
                 self.localstack_endpoint)
        if self.localstack_cacert:
            flags += " --obj-cacert %s" % self.localstack_cacert
        return flags

    def get_cbconbk_flags(self, shell=None):
        return self.get_cbbackupmgr_flags(shell)

    def cleanup_for_bkrs(self, s3_path):
        """
        Leave the bucket present and empty of this archive's objects.

        Not a true "delete this one directory" cleanup - Localstack buckets
        are cheap/local, so every object in the bucket is removed rather than
        just the given prefix. The bucket itself is always created if absent
        and always kept if present.

        It previously DELETED the whole bucket when one already existed, which
        made the outcome depend on prior state: BackupMgrUtil.configure_backup()
        calls this as a pre-step before creating the archive, so a pre-existing
        bucket was destroyed and the archive creation then failed with
        "bucket '<name>' not found", while an absent bucket was created and the
        same flow succeeded. Keeping the bucket makes the call idempotent and
        safe as either a pre-step or a teardown step.

        :param s3_path: e.g. s3://bucket-name/some-dir
        """
        parsed = urlparse(s3_path)
        bucket_name = parsed.netloc
        folder_path = parsed.path.strip("/")

        s3_resource = boto3.resource(
            "s3",
            verify=self._boto3_verify(),
            aws_access_key_id=self.localstack_access_key_id,
            aws_secret_access_key=self.localstack_secret_access_key,
            region_name=self.localstack_region,
            endpoint_url=self.localstack_endpoint)

        existing_buckets = [b.name for b in s3_resource.buckets.all()]
        bucket = s3_resource.Bucket(bucket_name)
        if bucket_name not in existing_buckets:
            create_kwargs = {}
            if self.localstack_region and self.localstack_region != "us-east-1":
                create_kwargs["CreateBucketConfiguration"] = {
                    "LocationConstraint": self.localstack_region}
            bucket.create(**create_kwargs)
        else:
            bucket.objects.all().delete()
        if folder_path:
            bucket.put_object(Key="{0}/".format(folder_path))

    def create_credential_store(self, rest, cred_id, username=None,
                                 password=None, description=None,
                                 allowed_services=None, expires_at_ms=None):
        """
        Build an 'aws' Credential Store payload (Localstack is
        S3-compatible, so it reuses the 'aws' credential type with its
        endpoint set) and create it via POST /settings/credentials/:id.

        Returns:
            tuple: (status_code, content)
        """
        cs_utils = CredentialStoreUtils()
        payload = cs_utils.build_aws_payload(
            self.localstack_access_key_id,
            self.localstack_secret_access_key,
            self.localstack_region,
            endpoint=self.localstack_endpoint,
            description=description, allowed_services=allowed_services,
            expires_at_ms=expires_at_ms)
        return cs_utils.create_credential(
            rest, cred_id, payload, username=username, password=password)

    # Localstack Community lacks a real KMS backend; EaR tests are not
    # supported here. Provide no-op stubs so the ABC is satisfied and any
    # accidental use surfaces a clear error rather than a mysterious import
    # or NotImplementedError from the base class.
    def create_kms_key(self, alias=None):
        raise NotImplementedError(
            "LocalstackProvider does not support KMS operations.")

    def delete_kms_key(self, key_url=None):
        return

    def get_km_flags(self, shell=None):
        raise NotImplementedError(
            "LocalstackProvider does not support KMS operations.")

    def set_km_key(self, key_url):
        raise NotImplementedError(
            "LocalstackProvider does not support KMS operations.")
