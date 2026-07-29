import os
import tempfile

from sdk_client3 import SDKClient

from pytests.security.crl_base import CRLBase


class CRLClusterOpsSteadyState(CRLBase):
    """
    CRL_Core / Cluster_Ops.Steady_State — mTLS client-cert CRUD against the
    default and a named collection, with CRL clientAuth policy=Require
    (TestCases_CRL.csv rows 112-114; TestPlan_CRL.md §2.1's no-cluster-op
    baseline). Rows 115-117 (multi-collection transactions, including the
    mid-transaction-revocation scenario) are deferred — they need a
    multi-collection transaction helper that doesn't exist anywhere in TAF
    today (`TransactionLoader` is single-collection only); building that
    alongside brand-new mTLS SDK plumbing in one pass would stack two
    unverified pieces of infra with no live cluster in this session to
    catch mistakes in either.

    This suite is what motivated adding `CertificateAuthenticator` support
    to `SDKClient` (`lib/sdk_client3.py`) — previously `cert_path` was a
    dead constructor param, and there was no client-cert SDK connection
    anywhere in this repo.

    Two flagged assumptions, unverified against a live server:
    - The `clientCertAuth` prefix rule (`path=subject.cn`, a literal
      prefix, empty delimiter) is built from `CertificateMangementAPI`'s
      own docstring/example, not confirmed end-to-end.
    - `SDKClient.__create_conn`'s connect loop swallows `Cluster.connect()`
      exceptions and retries up to 5 times before falling through; if every
      retry fails, `self.cluster` stays `None` and the *next* line
      (`self.cluster.wait_until_ready(...)`) raises `AttributeError` rather
      than a direct auth/TLS exception. That pre-existing retry-swallowing
      behavior is out of scope to change here — the revoked-cert tests
      below assert broadly (`assertRaises(Exception)`) and log the actual
      exception type/message for the first real run to confirm.
    """

    CERT_CN_PREFIX = "crlsteady-"
    NAMED_SCOPE = "crl_steady_scope"
    NAMED_COLLECTION = "crl_steady_coll"

    def setUp(self):
        super().setUp()
        self._mtls_temp_files = []
        self._mtls_clients = []

        # ClusterSetup creates no bucket, and OnPremBaseTest.setUp deletes
        # any pre-existing one (onPrem_basetestcase.py:571-575);
        # create_bucket() is opt-in per suite (onPrem_basetestcase.py:1971).
        # It returns None -- the bucket is read back off cluster.buckets.
        self.create_bucket(self.cluster)
        self.bucket = self.cluster.buckets[0]

        status, content = self.crl_utils.set_settings(
            self.rest, policyPerScope={"clientAuth": "Require"}
        )
        self.assertTrue(status, f"Failed to set clientAuth=Require: {content}")

        self._enable_client_cert_auth(
            state="enable",
            prefixes=[{
                "path": "subject.cn",
                "prefix": self.CERT_CN_PREFIX,
                "delimiter": "",
            }],
        )

        # Under Require, a cert with no applicable CRL is rejected just as a
        # revoked one is (see crl_test.py's missing-CRL fail-closed assertion).
        # Without this baseline the first, expected-to-succeed connection in
        # every test below would be refused.
        self.baseline_crl = "crl_steady_baseline.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, self.baseline_crl,
            self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1),
        )
        self.assertTrue(status, f"Baseline CRL upload failed: {content}")
        self._track_uploaded_file(self.baseline_crl)
        self.crl_utils.reload_crl(self.rest)

        self.bucket_util.create_scope(
            self.cluster.master, self.bucket, {"name": self.NAMED_SCOPE}
        )
        self.bucket_util.create_collection(
            self.cluster.master, self.bucket, self.NAMED_SCOPE,
            {"name": self.NAMED_COLLECTION}
        )

    def tearDown(self):
        for client in self._mtls_clients:
            try:
                client.close()
            except Exception as exc:
                self.log.warning(f"mTLS SDK client close error: {exc}")
        for path in self._mtls_temp_files:
            try:
                os.remove(path)
            except OSError:
                pass
        super().tearDown()

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _write_temp_pem(self, pem_bytes):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pem",
                                         mode="wb") as f:
            f.write(pem_bytes)
            path = f.name
        self._mtls_temp_files.append(path)
        return path

    def _provision_mtls_identity(self, username, role):
        """Create an RBAC user + a leaf cert whose CN maps to that username
        via the clientCertAuth prefix rule configured in setUp(). Returns
        (leaf_cert, leaf_key, serial, cert_path, key_path)."""
        self._create_rbac_test_user(username, role)
        cn = f"{self.CERT_CN_PREFIX}{username}"
        leaf_cert, leaf_key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, cn
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(leaf_cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(leaf_key))
        return leaf_cert, leaf_key, serial, cert_path, key_path

    def _connect_mtls_sdk(self, cert_path, key_path, scope=None, collection=None):
        kwargs = {}
        if scope is not None:
            kwargs["scope"] = scope
        if collection is not None:
            kwargs["collection"] = collection
        client = SDKClient(self.cluster, self.bucket, cert_path=cert_path,
                           key_path=key_path, **kwargs)
        self._mtls_clients.append(client)
        return client

    def _revoke_and_reload(self, serial, filename):
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename,
            crl_number=2,
        )
        self.assertTrue(status, f"Failed to upload revoking CRL: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)

    def _assert_full_crud(self, client, key):
        client.collection.insert(key, {"v": 1})
        self.assertEqual(client.collection.get(key).content_as[dict], {"v": 1})
        client.collection.replace(key, {"v": 2})
        self.assertEqual(client.collection.get(key).content_as[dict], {"v": 2})
        client.collection.remove(key)

    # ── Tests ────────────────────────────────────────────────────────────────

    def test_steady_crud_default_collection_revoked_rejected(self):
        role = f"data_reader[{self.bucket.name}]:data_writer[{self.bucket.name}]"
        _, _, serial, cert_path, key_path = self._provision_mtls_identity(
            "crl_steady_user1", role
        )

        client = self._connect_mtls_sdk(cert_path, key_path)
        self._assert_full_crud(client, "crl_steady_default_doc")

        self._revoke_and_reload(serial, "crl_steady_default_revoke.pem")

        with self.assertRaises(Exception) as ctx:
            rejected_client = self._connect_mtls_sdk(cert_path, key_path)
            rejected_client.collection.insert("crl_steady_default_doc_2", {"v": 1})
        self.log.info(
            f"Revoked cert correctly rejected: "
            f"{type(ctx.exception).__name__}: {ctx.exception}"
        )

    def test_steady_crud_named_collection_revoked_rejected(self):
        role = f"data_reader[{self.bucket.name}]:data_writer[{self.bucket.name}]"
        _, _, serial, cert_path, key_path = self._provision_mtls_identity(
            "crl_steady_user2", role
        )

        client = self._connect_mtls_sdk(
            cert_path, key_path, scope=self.NAMED_SCOPE,
            collection=self.NAMED_COLLECTION
        )
        self._assert_full_crud(client, "crl_steady_named_doc")

        self._revoke_and_reload(serial, "crl_steady_named_revoke.pem")

        with self.assertRaises(Exception) as ctx:
            rejected_client = self._connect_mtls_sdk(
                cert_path, key_path, scope=self.NAMED_SCOPE,
                collection=self.NAMED_COLLECTION
            )
            rejected_client.collection.insert("crl_steady_named_doc_2", {"v": 1})
        self.log.info(
            f"Revoked cert correctly rejected in named collection: "
            f"{type(ctx.exception).__name__}: {ctx.exception}"
        )

    def test_steady_crud_default_and_named_collection_valid_cert(self):
        role = f"data_reader[{self.bucket.name}]:data_writer[{self.bucket.name}]"
        _, _, _, cert_path, key_path = self._provision_mtls_identity(
            "crl_steady_user3", role
        )

        client = self._connect_mtls_sdk(cert_path, key_path)
        self._assert_full_crud(client, "crl_steady_valid_default_doc")

        client.select_collection(self.NAMED_SCOPE, self.NAMED_COLLECTION)
        self._assert_full_crud(client, "crl_steady_valid_named_doc")
