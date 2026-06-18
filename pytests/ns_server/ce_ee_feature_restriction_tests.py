"""
CE EE Feature Restriction Tests

Verifies that Enterprise Edition-only features are blocked on CE clusters.
Ported from testrunner community-edition-only-1.conf and
community-edition-only-2.conf (REST-only variants; no node-reset required).

Testrunner mapping
------------------
test_disabled_zone
    -> test_disabled_zone
check_audit_available + check_settings_audit
    -> test_audit_not_available
check_ldap_available
    -> test_ldap_not_available
test_ldap_groups (REST)
    -> test_ldap_groups_blocked
test_ldap_cert (REST)
    -> test_ldap_cert_blocked
check_x509_cert
    -> test_x509_cert_blocked
check_roles_base_access,user_add=test22,user_role=admin
    -> test_rbac_admin_user_blocked
check_root_certificate
    -> test_root_certificate_blocked
test_max_ttl_bucket (REST)
    -> test_max_ttl_bucket_blocked
test_setting_audit (REST)
    -> test_setting_audit_blocked
test_setting_autofailover_enterprise_only (REST, disk-failover variants)
    -> test_autofailover_disk_failover_blocked
test_setting_autofailover_enterprise_only (REST, server-group variant)
    -> test_autofailover_server_group_failover_blocked
test_set_bucket_compression,compression_mode=off/passive/active (REST)
    -> test_bucket_compression_blocked
test_log_redaction (REST)
    -> test_log_redaction_blocked
test_network_encryption
    -> test_network_encryption_blocked
test_n2n_encryption
    -> test_n2n_encryption_blocked

CLI variants (CBQE-8979)
------------------------
test_setting_autofailover_enterprise_only,cli_test=True (×4)
    -> test_autofailover_ee_settings_blocked_cli
test_set_bucket_compression,cli_test=True (×3)
    -> test_bucket_compression_blocked_cli
test_max_ttl_bucket,cli_test=True
    -> test_max_ttl_bucket_blocked_cli
test_setting_audit,cli_test=True
    -> test_setting_audit_blocked_cli
test_ldap_groups,cli_test=True
    -> test_ldap_groups_blocked_cli
test_log_redaction,cli_test=True
    -> test_log_redaction_blocked_cli
test_ee_only_features,examine=True
test_ee_only_features,merge=True
test_ee_only_features,s3=True
test_ee_only_features,consistency_check=True
test_ee_only_features,coll_restore=True
    -> test_backup_ee_features_blocked_cli
test_lww
    -> test_lww_conflict_resolution_blocked
check_ent_backup
    -> test_cbbackupmgr_binary_present_on_ce
check_memory_optimized_storage_mode + check_plasma_storage_mode
    -> test_indexer_storage_mode_blocked
check_full_backup_only (diff, accu)
    -> test_cbbackup_full_only_on_ce
"""

from basetestcase import ClusterSetup
from BucketLib.bucket import Bucket
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_tools.cb_cli import CbCli
from cb_tools.cbbackupmgr import CbBackupMgr
from shell_util.remote_connection import RemoteMachineShellConnection


class CeEeFeatureRestrictionTests(ClusterSetup):
    """EE feature blocks on CE — REST-only, no node reset required."""

    def setUp(self):
        super().setUp()
        self.rest = ClusterRestAPI(self.cluster.master)

        if self.cluster_util.is_enterprise_edition(self.cluster):
            self.fail("Tests require Community Edition cluster. "
                      "Install with edition=community parameter.")

        self.log.info("CE cluster confirmed. Master: %s", self.cluster.master.ip)

    def tearDown(self):
        super().tearDown()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _make_request(self, path, method="GET", params=""):
        api = self.rest.base_url + path
        status, content, _ = self.rest.request(api, method, params)
        content_str = (content.decode() if isinstance(content, bytes)
                       else str(content))
        return status, content_str

    def _assert_blocked(self, status, content_str, feature):
        self.assertFalse(
            status,
            "CE must block '%s' but request succeeded. Response: %s"
            % (feature, content_str[:300]))
        self.log.info("CE correctly blocked '%s': %s", feature,
                      content_str[:200])

    # ------------------------------------------------------------------
    # Server groups / zones
    # testrunner: test_disabled_zone
    # ------------------------------------------------------------------

    def test_disabled_zone(self):
        """Server groups (rack-zone awareness) must be blocked on CE."""
        status, content = self._make_request(
            "/pools/default/serverGroups", "POST", {"name": "group1"})
        self._assert_blocked(status, content, "server groups / zones")

    # ------------------------------------------------------------------
    # Audit
    # testrunner: check_audit_available + check_settings_audit
    # ------------------------------------------------------------------

    def test_audit_not_available(self):
        """Audit endpoint must be inaccessible on CE."""
        status, content = self._make_request("/settings/audit")
        self._assert_blocked(status, content, "audit settings")

    def test_setting_audit_blocked(self):
        """Enabling audit via REST must be blocked on CE."""
        status, content = self._make_request(
            "/settings/audit", "POST", {"auditdEnabled": "true"})
        self._assert_blocked(status, content, "enable audit")

    # ------------------------------------------------------------------
    # LDAP
    # testrunner: check_ldap_available, test_ldap_groups, test_ldap_cert
    # ------------------------------------------------------------------

    def test_ldap_not_available(self):
        """LDAP authentication settings must be blocked on CE."""
        status, content = self._make_request(
            "/settings/ldap", "POST", {"authenticationEnabled": "false"})
        self._assert_blocked(status, content, "LDAP settings")

    def test_ldap_groups_blocked(self):
        """LDAP group management must be blocked on CE."""
        status, content = self._make_request(
            "/settings/rbac/groups/test_admins", "POST",
            {"roles": "admin",
             "description": "Test group",
             "ldap_group_ref":
                 "uid=cbadmins,ou=groups,dc=example,dc=com"})
        self._assert_blocked(status, content, "LDAP groups")

    def test_ldap_cert_blocked(self):
        """LDAP TLS cert configuration must be blocked on CE."""
        status, content = self._make_request(
            "/settings/ldap", "POST",
            {"authenticationEnabled": "true",
             "hosts": self.cluster.master.ip,
             "port": 389,
             "encryption": "StartTLSExtension",
             "serverCertValidation": "true"})
        self._assert_blocked(status, content, "LDAP cert settings")

    # ------------------------------------------------------------------
    # X.509 / certificates
    # testrunner: check_x509_cert, check_root_certificate
    # ------------------------------------------------------------------

    def test_x509_cert_blocked(self):
        """Extended X.509 certificate endpoint must return EE error on CE."""
        status, content = self._make_request(
            "/pools/default/certificate?extended=true")
        self._assert_blocked(status, content, "X.509 extended certificate")

    def test_root_certificate_blocked(self):
        """Root certificate endpoint must be blocked on CE."""
        status, content = self._make_request("/pools/default/certificate")
        self._assert_blocked(status, content, "root certificate")

    # ------------------------------------------------------------------
    # RBAC
    # testrunner: check_roles_base_access,user_add=test22,user_role=admin
    # ------------------------------------------------------------------

    def test_rbac_admin_user_blocked(self):
        """Creating an admin RBAC user must be blocked on CE."""
        user = self.input.param("user_add", "test22")
        role = self.input.param("user_role", "admin")
        status, content = self._make_request(
            "/settings/rbac/users/local/%s" % user, "PUT",
            "name=%s&roles=%s" % (user, role))
        self._assert_blocked(status, content, "RBAC admin user creation")

    # ------------------------------------------------------------------
    # Bucket-level EE features
    # testrunner: test_max_ttl_bucket, test_set_bucket_compression
    # ------------------------------------------------------------------

    def test_max_ttl_bucket_blocked(self):
        """Creating a bucket with maxTTL must be blocked on CE."""
        status, content = self._make_request(
            "/pools/default/buckets", "POST",
            {"name": "ttl_test_bucket", "maxTTL": 100, "ramQuotaMB": 256})
        self._assert_blocked(status, content, "maxTTL bucket")
        self.assertIn("enterprise edition", content.lower(),
                      "Expected enterprise edition message, got: %s" % content)

    def test_bucket_compression_blocked(self):
        """Setting bucket compressionMode must be blocked on CE for all modes.

        Iterates off, passive, and active in one pass.
        """
        for mode in ("off", "passive", "active"):
            status, content = self._make_request(
                "/pools/default/buckets", "POST",
                {"name": "comp_test_bucket",
                 "compressionMode": mode,
                 "ramQuotaMB": 256})
            self._assert_blocked(status, content,
                                 "bucket compression mode=%s" % mode)
            self.assertIn("enterprise edition", content.lower(),
                          "Expected enterprise edition message, got: %s"
                          % content)

    # ------------------------------------------------------------------
    # Log redaction
    # testrunner: test_log_redaction (REST)
    # ------------------------------------------------------------------

    def test_log_redaction_blocked(self):
        """Log redaction must be blocked on CE."""
        status, content = self._make_request(
            "/controller/startLogsCollection", "POST",
            {"nodes": "*", "logRedactionLevel": "partial"})
        self._assert_blocked(status, content, "log redaction")

    # ------------------------------------------------------------------
    # Network / node-to-node encryption
    # testrunner: test_network_encryption, test_n2n_encryption
    # ------------------------------------------------------------------

    def test_network_encryption_blocked(self):
        """Setting cluster encryption level must be blocked on CE."""
        status, content = self._make_request(
            "/settings/security", "POST",
            {"clusterEncryptionLevel": "control",
             "tlsMinVersion": "tlsv1.2"})
        self._assert_blocked(status, content, "cluster network encryption")

    def test_n2n_encryption_blocked(self):
        """Node-to-node encryption must be blocked on CE."""
        status, content = self._make_request(
            "/settings/security", "POST",
            {"nodeEncryption": "on"})
        self._assert_blocked(status, content, "node-to-node encryption")

    # ------------------------------------------------------------------
    # Auto-failover EE-only settings
    # testrunner: test_setting_autofailover_enterprise_only (×4 REST variants)
    # ------------------------------------------------------------------

    def test_autofailover_disk_failover_blocked(self):
        """Auto-failover on data disk issues must be blocked on CE.

        Covers testrunner variants:
        - test_setting_autofailover_enterprise_only (default)
        - test_setting_autofailover_enterprise_only,failover_disk_period=True
        """
        status, content = self._make_request(
            "/settings/autoFailover", "POST",
            {"enabled": "true",
             "timeout": 120,
             "maxCount": 1,
             "failoverOnDataDiskIssues[enabled]": "true",
             "failoverOnDataDiskIssues[timePeriod]": 300})
        self._assert_blocked(status, content,
                             "auto-failover disk issues (EE-only)")

    def test_autofailover_server_group_failover_blocked(self):
        """Auto-failover of server groups must be blocked on CE.

        Covers testrunner variants:
        - test_setting_autofailover_enterprise_only,failover_server_group=True
        - test_setting_autofailover_enterprise_only,failover_disk_period=True,failover_server_group=True
        """
        status, content = self._make_request(
            "/settings/autoFailover", "POST",
            {"enabled": "true",
             "timeout": 120,
             "maxCount": 1,
             "failoverServerGroup": "true"})
        self._assert_blocked(status, content,
                             "auto-failover server group (EE-only)")

    # ------------------------------------------------------------------
    # CLI helpers
    # ------------------------------------------------------------------

    def _cli_on(self, node):
        shell = RemoteMachineShellConnection(node)
        cb_cli = CbCli(shell, username=node.rest_username,
                       password=node.rest_password)
        return shell, cb_cli

    @staticmethod
    def _combined(output, error):
        return "\n".join((output or []) + (error or []))

    def _assert_cli_blocked(self, combined, feature):
        self.assertIn(
            "enterprise edition", combined.lower(),
            "CE must block '%s' via CLI. Got: %s" % (feature, combined[:300]))
        self.log.info("CE blocked '%s' via CLI: %s", feature, combined[:200])

    # ------------------------------------------------------------------
    # Auto-failover EE settings — CLI variants
    # testrunner: test_setting_autofailover_enterprise_only,cli_test=True (×4)
    # ------------------------------------------------------------------

    def test_autofailover_ee_settings_blocked_cli(self):
        """CE must reject all EE-only autofailover setting combinations via CLI.

        Iterates all four disk/server-group combos in one pass:
        - disk_fo only
        - disk_fo + disk_fo_timeout
        - disk_fo + server_group
        - disk_fo + disk_fo_timeout + server_group
        """
        cases = [
            dict(disk_fo_timeout=None, failover_server_group=None),
            dict(disk_fo_timeout=300,  failover_server_group=None),
            dict(disk_fo_timeout=None, failover_server_group=1),
            dict(disk_fo_timeout=300,  failover_server_group=1),
        ]
        for case in cases:
            label = "disk_period=%s server_group=%s" % (
                case["disk_fo_timeout"], case["failover_server_group"])
            shell, cb_cli = self._cli_on(self.cluster.master)
            combined = ""
            try:
                output = cb_cli.auto_failover(
                    enable_auto_fo=1, disk_fo=1, **case)
                combined = "\n".join(output) if isinstance(output, list) \
                    else str(output)
            except Exception as exc:
                combined = str(exc)
            finally:
                shell.disconnect()

            self._assert_cli_blocked(
                combined, "autofailover EE settings (%s)" % label)

    # ------------------------------------------------------------------
    # Bucket compression — CLI variants
    # testrunner: test_set_bucket_compression,cli_test=True (×3)
    # ------------------------------------------------------------------

    def test_bucket_compression_blocked_cli(self):
        """CE must reject bucket compressionMode for all modes via couchbase-cli.

        Iterates off, passive, and active in one pass.
        """
        for mode in ("off", "passive", "active"):
            shell, cb_cli = self._cli_on(self.cluster.master)
            combined = ""
            try:
                cb_cli.create_bucket({
                    Bucket.name: "comp_test_bucket",
                    Bucket.bucketType: "couchbase",
                    Bucket.ramQuotaMB: 512,
                    Bucket.replicaNumber: 1,
                    Bucket.compressionMode: mode,
                })
                combined = "no error raised"
            except Exception as exc:
                combined = str(exc)
            finally:
                shell.disconnect()

            self._assert_cli_blocked(
                combined, "bucket compression mode=%s" % mode)

    # ------------------------------------------------------------------
    # Max TTL bucket — CLI variant
    # testrunner: test_max_ttl_bucket,cli_test=True
    # ------------------------------------------------------------------

    def test_max_ttl_bucket_blocked_cli(self):
        """CE must reject bucket with --max-ttl via couchbase-cli."""
        shell, cb_cli = self._cli_on(self.cluster.master)
        combined = ""
        try:
            cb_cli.create_bucket({
                Bucket.name: "ttl_test_bucket",
                Bucket.bucketType: "couchbase",
                Bucket.ramQuotaMB: 512,
                Bucket.replicaNumber: 1,
                Bucket.maxTTL: 200,
            })
            combined = "no error raised"
        except Exception as exc:
            combined = str(exc)
        finally:
            shell.disconnect()

        self._assert_cli_blocked(combined, "maxTTL bucket")

    # ------------------------------------------------------------------
    # Audit setting — CLI variant
    # testrunner: test_setting_audit,cli_test=True
    # ------------------------------------------------------------------

    def test_setting_audit_blocked_cli(self):
        """CE must reject enabling audit via couchbase-cli setting-audit."""
        shell, cb_cli = self._cli_on(self.cluster.master)
        output, error = cb_cli.setting_audit()
        shell.disconnect()
        combined = self._combined(output, error)
        self._assert_cli_blocked(combined, "setting-audit")

    # ------------------------------------------------------------------
    # LDAP groups — CLI variant
    # testrunner: test_ldap_groups,cli_test=True
    # ------------------------------------------------------------------

    def test_ldap_groups_blocked_cli(self):
        """CE must reject LDAP group creation via couchbase-cli user-manage."""
        shell, cb_cli = self._cli_on(self.cluster.master)
        output, error = cb_cli.user_manage_set_group(
            group_name="admins",
            roles="admin",
            description="Couchbase Server Administrators",
            ldap_ref="uid=cbadmins,ou=groups,dc=example,dc=com")
        shell.disconnect()
        combined = self._combined(output, error)
        self._assert_cli_blocked(combined, "LDAP group creation")

    # ------------------------------------------------------------------
    # Log redaction — CLI variant
    # testrunner: test_log_redaction,cli_test=True
    # ------------------------------------------------------------------

    def test_log_redaction_blocked_cli(self):
        """CE must reject log collection with redaction via couchbase-cli."""
        shell, cb_cli = self._cli_on(self.cluster.master)
        output, error = cb_cli.collect_logs_start(
            all_nodes=True, redaction_level="partial")
        shell.disconnect()
        combined = self._combined(output, error)
        self._assert_cli_blocked(combined, "log redaction")

    # ------------------------------------------------------------------
    # cbbackupmgr EE-only features
    # testrunner: test_ee_only_features (×5 variants)
    # ------------------------------------------------------------------

    def test_backup_ee_features_blocked_cli(self):
        """CE must reject EE-only cbbackupmgr features.

        Iterates five EE-only operations in one pass:
        - examine (document inspection)
        - merge --all
        - backup to S3 archive
        - backup with --consistency-check
        - restore with --include-data (collection-level restore)

        All five should produce "Enterprise Edition" in output.
        Replaces: test_ee_only_features (×5 variants).
        Requires nodes_init=1.
        """
        master = self.cluster.master
        shell = RemoteMachineShellConnection(master)
        mgr = CbBackupMgr(shell, username=master.rest_username,
                           password=master.rest_password)
        archive = "/tmp/ce_ee_backup_test"
        repo = "ce_ee_repo"

        mgr.create_repo(archive, repo)

        cases = [
            ("examine", lambda: mgr.examine(
                archive, repo, key="asdf",
                collection_string="asdf.asdf.asdf")),
            ("merge --all", lambda: mgr.merge(
                archive, repo, start=None, end=None)),
            ("backup s3://", lambda: mgr.backup(
                "s3://ce-ee-test-bucket", repo)),
            ("backup --consistency-check", lambda: mgr.backup(
                archive, repo, consistency_check=1)),
            ("restore --include-data", lambda: mgr.restore(
                archive, repo, include_data="asdf.asdf.asdf")),
        ]

        try:
            for label, fn in cases:
                output, error = fn()
                combined = self._combined(output, error)
                self.assertIn(
                    "enterprise edition", combined.lower(),
                    "CE must block cbbackupmgr %s. Got: %s"
                    % (label, combined[:300]))
                self.log.info("CE blocked cbbackupmgr %s: %s",
                              label, combined[:120])
        finally:
            shell.disconnect()

    # ------------------------------------------------------------------
    # LWW conflict resolution — CE block
    # testrunner: test_lww
    # ------------------------------------------------------------------

    def test_lww_conflict_resolution_blocked(self):
        """CE must reject bucket creation with LWW conflict resolution.

        Replaces: test_lww (CommunityXDCRTests).
        Requires nodes_init=1.
        """
        status, content = self._make_request(
            "/pools/default/buckets", "POST",
            {"name": "lww_test_bucket",
             "conflictResolutionType": "lww",
             "ramQuotaMB": 256})
        self._assert_blocked(status, content, "LWW conflict resolution")
        self.assertIn(
            "enterprise edition", content.lower(),
            "Expected enterprise edition message, got: %s" % content)

    # ------------------------------------------------------------------
    # cbbackupmgr binary presence check
    # testrunner: check_ent_backup
    # ------------------------------------------------------------------

    def test_cbbackupmgr_binary_present_on_ce(self):
        """cbbackupmgr binary must be present on CE nodes.

        Replaces: check_ent_backup.
        Requires nodes_init=1.
        """
        shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            exists = shell.file_exists("/opt/couchbase/bin/", "cbbackupmgr")
            self.assertTrue(
                exists,
                "cbbackupmgr binary not found on CE node at "
                "/opt/couchbase/bin/cbbackupmgr")
            self.log.info("cbbackupmgr binary confirmed present on CE node")
        finally:
            shell.disconnect()

    # ------------------------------------------------------------------
    # Indexer storage mode EE-only rejection
    # testrunner: check_memory_optimized_storage_mode + check_plasma_storage_mode
    # ------------------------------------------------------------------

    def test_indexer_storage_mode_blocked(self):
        """CE must reject EE-only indexer storage modes via REST.

        Iterates memory_optimized and plasma in one pass.
        Replaces: check_memory_optimized_storage_mode,
                  check_plasma_storage_mode.
        Requires nodes_init=1.
        """
        for mode in ("memory_optimized", "plasma"):
            status, content = self._make_request(
                "/settings/indexes", "POST", {"storageMode": mode})
            self._assert_blocked(
                status, content,
                "indexer storageMode=%s (EE-only)" % mode)

    # ------------------------------------------------------------------
    # cbbackup full-only enforcement on CE
    # testrunner: check_full_backup_only (diff, accu variants)
    # ------------------------------------------------------------------

    def test_cbbackup_full_only_on_ce(self):
        """CE forces full backup mode even when diff or accu is requested.

        Loads 1000 docs, runs a full backup, then a diff/accu backup.
        CE coerces both to full so cbtransfer counts 2000 items total
        (1000 per backup x 2 backups in archive).

        Iterates diff and accu in one pass.
        Replaces: check_full_backup_only,backup_option=diff
                  check_full_backup_only,backup_option=accu
        Requires nodes_init=1.
        """
        master = self.cluster.master
        bin_path = "/opt/couchbase/bin"
        host = "http://127.0.0.1:8091"
        creds = "-u %s -p %s" % (master.rest_username, master.rest_password)
        archive = "/tmp/ce_cbbackup_full_only_test"

        # Create default bucket for cbworkloadgen
        self._make_request(
            "/pools/default/buckets", "POST",
            {"name": "default", "ramQuotaMB": 256,
             "bucketType": "couchbase", "replicaNumber": 0})
        self.sleep(8, "wait for default bucket to be ready")

        shell = RemoteMachineShellConnection(master)
        try:
            # Load 1000 docs once; both backup iterations use same data
            shell.execute_command(
                "%s/cbworkloadgen -n %s %s -i 1000 -s 100 -j"
                % (bin_path, host, creds))

            for mode in ("diff", "accu"):
                # Fresh archive for each iteration
                shell.execute_command("rm -rf %s" % archive)

                # Baseline full backup — 1000 items
                shell.execute_command(
                    "%s/cbbackup %s %s %s -m full -b default"
                    % (bin_path, host, archive, creds))

                # diff/accu backup — CE must coerce to full (1000 more items)
                shell.execute_command(
                    "%s/cbbackup %s %s %s -m %s -b default"
                    % (bin_path, host, archive, creds, mode))

                # Count all SET operations across the entire archive
                count_out, _ = shell.execute_command(
                    "%s/cbtransfer %s/ stdout: 2>/dev/null "
                    "| grep set | uniq | wc -l" % (bin_path, archive))
                try:
                    count = int(count_out[0].strip()) if count_out else 0
                except (ValueError, IndexError):
                    count = 0

                self.assertEqual(
                    count, 2000,
                    "CE must coerce cbbackup -m %s to full. "
                    "Expected 2000 items (2 full backups x 1000), got %d"
                    % (mode, count))
                self.log.info(
                    "CE forced full backup for -m %s: %d items", mode, count)
        finally:
            shell.execute_command("rm -rf %s" % archive)
            shell.disconnect()
