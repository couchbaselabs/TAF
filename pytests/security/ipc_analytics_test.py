import threading
import time

import requests
from BucketLib.bucket import Bucket

from couchbase_utils.security_utils.ipc_analytics_utils import IPCAnalyticsUtils
from pytests.security.ipc_analytics_base import IPCAnalyticsBase


class IPCAnalyticsTest(IPCAnalyticsBase):
    """
    Internal-identity Password Check under mTLS, on the Analytics service.

    See IPCAnalyticsBase for which Analytics endpoints the internal
    certificate used to reach, and IPCBase for the feature itself.
    """

    def test_ipc_analytics_rest_cert_only_refused(self):
        """
        The internal certificate alone gets nothing from Analytics with the
        setting on -- on every Analytics node, directly on 18095 and through
        ns_server's /_p/cbas/ proxy -- and the refusals leak nothing.

        Steps:
            1. Enable client cert auth (client_cert_state, default enable).
            2. Forge an internal certificate (unrelated CN) and issue a
               genuine-looking one (CN=internal), both from the trusted CA.
            3. Positive control: certificate + Administrator reaches every
               endpoint on every node.
            4. Setting on: the certificate alone is refused (401) everywhere.
            5. Setting off: the cluster, link and active-request endpoints and
               the proxy answer it as an administrator, while the query
               servlet and node/config endpoints still refuse it. This is the
               "before" evidence: if it stops reproducing, step 4 proves
               nothing.
            6. Setting on again: refused everywhere again.
        """
        self._enable_client_cert_auth(
            state=self.input.param("client_cert_state", "enable"))
        certs = {
            "forged": self.generate_forged_internal_cert(),
            "genuine": self.generate_internal_client_cert(
                self.ca_cert, self.ca_key, name="internal"),
        }

        self.log.info("Step 3: positive control with Administrator")
        self.assert_analytics_admin_control(certs["forged"])

        for step, enabled in ((4, True), (5, False), (6, True)):
            self.log.info(f"Step {step}: {self.IPC_SETTING}={enabled}")
            self.set_ipc_setting(enabled)
            self.wait_for_analytics_cert_only(certs["forged"], enabled)
            for label, cert in certs.items():
                self.assert_analytics_cert_only(
                    cert, enabled, f"Step {step} {label} certificate")

        self.log.info("Test passed: the internal certificate reaches "
                      "Analytics only while the setting is off")

    def test_ipc_analytics_credentials_decide(self):
        """
        With the setting on, the credentials sent alongside an internal
        certificate decide who the caller is on Analytics, and grant exactly
        that account's permissions -- on the cluster controller and on a node
        that has to proxy to it.

        Steps:
            1. client_cert_state (hybrid or mandatory), setting on, a bucket
               and an analytics_reader user.
            2. On every Analytics node, directly and through /_p/cbas/:
               a. certificate + Administrator: CREATE and DROP a dataset.
               b. certificate + analytics_reader: SELECT allowed, CREATE
                  DATASET refused with 403. This is the discriminating case --
                  an administrator identity from the certificate would have
                  been allowed.
               c. certificate + wrong password, + unknown user: 401.
        """
        bucket = "ipcan"
        # Bucket, user and setting first: under mandatory the framework's
        # own HTTPS REST carries no client certificate and is refused.
        self.create_analytics_bucket(bucket)
        reader = self._create_rbac_test_user("ipcan_reader",
                                             "analytics_reader")
        self.set_ipc_setting(True)
        self._set_client_cert_auth_http(
            self.input.param("client_cert_state", "hybrid"))
        cert = self.generate_internal_client_cert(
            self.ca_cert, self.ca_key, name="internal")
        self.wait_for_analytics_cert_only(cert, True)
        cc_ip = self.cc_node_ip()
        self.log.info(f"Analytics cluster controller is {cc_ip}")

        failures = []
        for node in self.cbas_nodes:
            for proxied in (False, True):
                dataset = (f"ipcan_ds_{node.ip.replace('.', '_')}_"
                           f"{int(proxied)}")
                create = f"CREATE DATASET {dataset} ON `{bucket}`;"
                drop = f"DROP DATASET {dataset} IF EXISTS;"
                # Tracked before the checks run: the first one creates it, and
                # a later check raising would otherwise leave it untracked.
                self.track_dataset(dataset)
                for what, statement, auth, expected in (
                        ("Administrator CREATE DATASET", create,
                         self.admin_auth(), 200),
                        ("Administrator DROP DATASET", drop,
                         self.admin_auth(), 200),
                        ("analytics_reader SELECT", "SELECT 1;", reader, 200),
                        ("analytics_reader CREATE DATASET", create, reader,
                         403),
                        ("wrong password", "SELECT 1;",
                         (reader[0], "not-the-password"), 401),
                        ("unknown user", "SELECT 1;",
                         ("ipcan_nobody", "Couchbase@1234"), 401)):
                    failures += self.check_statement(
                        node, statement, cert, auth, expected, proxied, what,
                        cc_ip)
                IPCAnalyticsUtils.admin_query(node, drop)

        self.assertFalse(failures, "Credentials did not decide the outcome "
                         "over an internal certificate:\n  "
                         + "\n  ".join(failures))
        self.log.info("Test passed: credentials, not the certificate, "
                      "decided every Analytics outcome")

    def test_ipc_analytics_internode_traffic_healthy(self):
        """
        With the setting on, Analytics nodes keep talking to each other: a
        dataset spread over both nodes ingests and answers queries from
        either node.

        With node-to-node encryption on (n2n_level=all), each node's own
        internal certificate stops establishing its identity on the
        inter-node ports, so the nodes must have switched to sending
        credentials -- logged once per process start. With it off
        (n2n_level=off) the inter-node ports never use certificates, which
        makes that line the negative control.

        Steps:
            1. Bucket with docs; node-to-node encryption at n2n_level;
               setting on; client_cert_state.
            2. Dataset on the bucket, Local link connected.
            3. Every node counts every doc, and answers a grouped aggregate.
            4. More docs; every node counts them too, so ingestion kept going.
            5. The "no longer establishes our identity" line appeared on every
               node with encryption on, and on none with it off.
        """
        bucket, dataset = "ipcan", "ipcan_ds"
        level = self.input.param("n2n_level", "all")
        items = self.input.param("items", 5000)
        marker = "our client certificate no longer establishes our identity"

        self.create_analytics_bucket(bucket)
        self.load_docs(bucket, items)
        self.set_ipc_setting(True)
        # Snapshot the marker count BEFORE the encryption and clientCertAuth
        # changes below: those restart cbas, and only the fresh process logs
        # "no longer establishes our identity" (it is logged once per process,
        # when the state flips). Keep this snapshot ahead of those calls, or
        # the grew/not-grew check loses its signal.
        before = {node.ip: self.analytics_log_count(node, marker)
                  for node in self.cbas_nodes}
        if level == "off":
            self.disable_internode_tls()
        else:
            self.enable_internode_tls(level)
        self.assertEqual(self.internode_tls_level(), level,
                         "Node-to-node encryption is not at the level under "
                         "test")
        self._set_client_cert_auth_http(
            self.input.param("client_cert_state", "hybrid"))
        cert = self.generate_internal_client_cert(
            self.ca_cert, self.ca_key, name="internal")

        self.track_dataset(dataset)
        self.analytics_admin_exec(
            f"CREATE DATASET IF NOT EXISTS {dataset} ON `{bucket}`;")
        self.analytics_admin_exec("CONNECT LINK Local;")
        self.wait_for_dataset_count(dataset, items, cert=cert)

        failures = []
        for node in self.cbas_nodes:
            failures += self.check_statement(
                node, f"SELECT d.age, COUNT(*) AS c FROM {dataset} d "
                      f"GROUP BY d.age;",
                cert, self.admin_auth(), 200, False, "grouped aggregate",
                None)
        self.assertFalse(failures, "\n  ".join(failures))

        self.load_docs(bucket, items, prefix="ipcan_more")
        self.wait_for_dataset_count(dataset, 2 * items, cert=cert)

        after = {node.ip: self.analytics_log_count(node, marker)
                 for node in self.cbas_nodes}
        self.log.info(f"'{marker}' lines before/after: {before} {after}")
        for node in self.cbas_nodes:
            grew = after[node.ip] > before[node.ip]
            self.assertEqual(
                grew, level != "off",
                f"{node.ip}: with node-to-node encryption {level}, the "
                f"inter-node identity switch line should "
                f"{'appear' if level != 'off' else 'not appear'}; "
                f"before={before[node.ip]} after={after[node.ip]}")
        self.analytics_admin_exec(f"DROP DATASET {dataset} IF EXISTS;")
        self.log.info("Test passed: Analytics inter-node traffic healthy "
                      f"with the setting on (n2n {level})")

    def test_ipc_analytics_topology_and_restart(self):
        """
        With the setting on and node-to-node encryption on, Analytics
        survives the topology and process events that re-open every
        inter-node connection, and the setting still holds afterwards.

        Steps:
            1. Bucket, docs, dataset; encryption all; setting on; hybrid.
            2. Rebalance the second Analytics node out, then back in.
            3. Hard-fail it over, recover it (full), rebalance.
            4. Kill cbas on it; the babysitter restarts it.
            5. Restart couchbase-server on it.
            After every step every node counts every doc, and at the end the
            setting is still on and the certificate alone gets nothing.
        """
        bucket, dataset = "ipcan", "ipcan_ds"
        items = self.input.param("items", 2000)
        self.create_analytics_bucket(bucket)
        self.load_docs(bucket, items)
        self.set_ipc_setting(True)
        self.enable_internode_tls("all")
        self._set_client_cert_auth_http("hybrid")
        cert = self.generate_forged_internal_cert()
        self.track_dataset(dataset)
        self.analytics_admin_exec(
            f"CREATE DATASET IF NOT EXISTS {dataset} ON `{bucket}`;")
        self.analytics_admin_exec("CONNECT LINK Local;")
        self.wait_for_dataset_count(dataset, items, cert=cert)

        peer = self.cbas_only_node()
        self.pin_master(next(node for node in self.nodes_with_service("kv")
                             if node.ip != peer.ip))
        for step, action in (
                ("rebalance out", lambda: self.rebalance_out_in(peer)),
                ("failover and recovery",
                 lambda: self.failover_and_recover(peer)),
                ("cbas killed", lambda: self.kill_cbas(peer)),
                ("couchbase-server restarted",
                 lambda: self.restart_nodes([peer]))):
            self.log.info(f"Step: {step} on {peer.ip}")
            action()
            self.wait_for_analytics(self.cbas_nodes)
            self.wait_for_dataset_count(dataset, items, cert=cert)

        self.assertTrue(self.get_ipc_setting(),
                        f"{self.IPC_SETTING} did not survive the topology "
                        f"changes and restarts")
        self.wait_for_analytics_cert_only(cert, True)
        self.assert_analytics_cert_only(cert, True, "after the restarts")
        self.analytics_admin_exec(f"DROP DATASET {dataset} IF EXISTS;")
        self.log.info("Test passed: Analytics survived rebalance, failover, "
                      "a cbas kill and a node restart with the setting on")

    def test_ipc_analytics_logs_and_audit(self):
        """
        Refusing an internal certificate leaves no certificate, key or
        password material in any log on any node, and Analytics audit events
        name the account behind the credentials -- never the internal
        identity.

        Steps:
            1. Audit on with every event enabled; setting on; an
               analytics_reader user.
            2. Forged certificate alone against every endpoint; certificate +
               wrong password; certificate + analytics_reader SELECT.
            3. No PEM, private key or the wrong password in the Analytics,
               ns_server access or audit logs on any node. The forged CN is
               logged for review only.
            4. A SELECT audit event names analytics_reader, and none names an
               '@' internal identity.
        """
        reader = self._create_rbac_test_user("ipcan_reader",
                                             "analytics_reader")
        self.enable_all_audit()
        self.set_ipc_setting(True)
        self._set_client_cert_auth_http("hybrid")
        cert = self.generate_forged_internal_cert()
        bad_password = "ipcan-not-the-password"
        self.wait_for_analytics_cert_only(cert, True)
        audit_before = {node.ip: self.audit_line_count(node)
                        for node in self.cbas_nodes}
        files = ["analytics_*.log", "http_access*.log", "audit.log",
                 "current-audit.log"]
        log_before = {node.ip: self.log_baselines(node, files)
                      for node in self.cbas_nodes}

        self.assert_analytics_cert_only(cert, True, "audit run")
        failures = []
        for node in self.cbas_nodes:
            failures += self.check_statement(
                node, "SELECT 1;", cert, (reader[0], bad_password), 401,
                False, "wrong password", None)
            failures += self.check_statement(
                node, "SELECT 1;", cert, reader, 200, False,
                "analytics_reader SELECT", None)
        self.assertFalse(failures, "\n  ".join(failures))

        # Polled first: audit is written asynchronously, and by the time the
        # SELECT event lands the logs scanned below have caught up too.
        users = self.wait_for_audit_users(
            audit_before, self.ANALYTICS_SELECT_AUDIT_ID, reader[0])

        leaks = []
        for node in self.cbas_nodes:
            for needle in ("BEGIN CERTIFICATE", "PRIVATE KEY", bad_password):
                leaks += self.grep_node_logs(node, needle,
                                             log_before[node.ip])
            cn_lines = self.grep_node_logs(node, "ads-dashboard",
                                           log_before[node.ip])
            self.log.info(f"[observation] forged CN in logs on {node.ip}: "
                          f"{cn_lines}")
        self.assertFalse(leaks, "Certificate, key or password material in "
                         "the logs:\n  " + "\n  ".join(leaks))

        self.log.info(f"Analytics SELECT audit events by user: {users}")
        self.assertIn(reader[0], users,
                      "No Analytics SELECT audit event names "
                      f"{reader[0]}: {users}")
        internal = [user for user in users if user and user.startswith("@")]
        self.assertFalse(internal, "Analytics audit events name an internal "
                         f"identity: {internal}")
        self.log.info("Test passed: no secret material in logs, and audit "
                      "names the real user")

    def test_ipc_analytics_internode_forged_cert(self):
        """
        With node-to-node encryption on, the Analytics inter-node ports take
        a peer's certificate as its identity. The internal certificate must
        stop being enough once the setting is on: the peer then has to send
        full-admin credentials.

        Steps:
            1. Encryption all, client_cert_state hybrid, an analytics_admin
               user (Analytics rights, not a full admin).
            2. Setting on, on every inter-node port of every node:
               a. forged certificate, nothing sent: closed after the auth
                  timeout;
               b. forged certificate + Administrator credential frame: 200;
               c. forged certificate + analytics_admin frame: 401;
               d. a prefix-mapped user certificate, an untrusted-CA
                  certificate and no certificate: closed at once.
            3. Setting off: a silent forged connection is held open with no
               credentials asked for -- the "before" evidence. Nothing is
               sent on it.
            4. Setting on again: refused again.
        """
        non_admin = self._create_rbac_test_user("ipcan_admin",
                                                "analytics_admin")
        self.enable_internode_tls("all")
        self._set_client_cert_auth_http("hybrid")
        untrusted_ca, untrusted_key = self.crl_utils.generate_ca(
            "IPCAnalyticsUntrustedCA")
        certs = {
            "forged": self.generate_forged_internal_cert(),
            "user": self.generate_client_cert(non_admin[0])[:2],
            "untrusted": self.generate_forged_internal_cert(
                ca_cert=untrusted_ca, ca_key=untrusted_key),
        }
        admin_frame = IPCAnalyticsUtils.internode_credentials(
            *self.admin_auth())
        non_admin_frame = IPCAnalyticsUtils.internode_credentials(*non_admin)
        ports = {node.ip: self.internode_ports(node)
                 for node in self.cbas_nodes}
        self.log.info(f"Inter-node ports: {ports}")
        for node in self.cbas_nodes:
            self.assertTrue(ports[node.ip],
                            f"No inter-node port listening on {node.ip}")

        failures = []
        for step, enabled in ((2, True), (3, False), (4, True)):
            self.log.info(f"Step {step}: {self.IPC_SETTING}={enabled}")
            self.set_ipc_setting(enabled)
            for node in self.cbas_nodes:
                self.wait_for_internode_enforcement(
                    node, ports[node.ip][-1], certs["forged"], enabled)
                for port in ports[node.ip]:
                    failures += [f"Step {step}: {failure}" for failure in
                                 self.internode_outcomes(
                                     node, port, certs, admin_frame,
                                     non_admin_frame, enabled)]
        self.assertFalse(failures, "Inter-node ports did not enforce the "
                         "setting:\n  " + "\n  ".join(failures))
        self.log.info("Test passed: the internal certificate opens the "
                      "inter-node ports only while the setting is off")

    def test_ipc_analytics_runtime_toggle_under_load(self):
        """
        Flipping the setting while Analytics ingests and answers queries
        across both nodes neither fails queries nor stops ingestion: the
        inter-node connections already open stay up, and new ones pick the
        new rule up.

        Steps:
            1. Bucket, docs, dataset; encryption all; hybrid; setting on.
            2. Background: a COUNT query on every node every second, and
               batches of new docs.
            3. Flip the setting off and on ipc_toggle_cycles times,
               ipc_toggle_interval seconds apart.
            4. No query failed (more than max_query_failures), and every
               node counts every doc loaded.
        """
        bucket, dataset = "ipcan", "ipcan_ds"
        items = self.input.param("items", 2000)
        cycles = self.input.param("ipc_toggle_cycles", 4)
        interval = self.input.param("ipc_toggle_interval", 20)
        allowed = self.input.param("max_query_failures", 0)
        self.create_analytics_bucket(bucket)
        self.load_docs(bucket, items)
        self.set_ipc_setting(True)
        self.enable_internode_tls("all")
        self._set_client_cert_auth_http("hybrid")
        cert = self.generate_forged_internal_cert()
        self.track_dataset(dataset)
        self.analytics_admin_exec(
            f"CREATE DATASET IF NOT EXISTS {dataset} ON `{bucket}`;")
        self.analytics_admin_exec("CONNECT LINK Local;")
        self.wait_for_dataset_count(dataset, items, cert=cert)

        stop = threading.Event()
        outcomes = {"ok": 0, "failed": []}
        batches = [0]

        def query_loop():
            # A transport error is a failed query, not a reason to stop
            # counting: a dead thread would leave 'failed' empty.
            while not stop.is_set():
                for node in self.cbas_nodes:
                    stamp = f"{time.strftime('%H:%M:%S')} {node.ip}"
                    try:
                        resp = self.statement(
                            node, f"SELECT VALUE COUNT(*) FROM {dataset};",
                            cert=cert, auth=self.admin_auth())
                    except requests.exceptions.RequestException as exc:
                        outcomes["failed"].append(f"{stamp} {exc!r}")
                        continue
                    if resp.status_code == 200:
                        outcomes["ok"] += 1
                    else:
                        outcomes["failed"].append(
                            f"{stamp} {resp.status_code}: "
                            f"{resp.text[:150]}")
                time.sleep(1)

        def load_loop():
            while not stop.is_set():
                batches[0] += 1
                try:
                    self.load_docs(bucket, 200,
                                   prefix=f"ipcan_b{batches[0]}")
                except Exception as exc:
                    self.log.warning(f"Loader stopped at batch "
                                     f"{batches[0]}: {exc!r}")
                    return

        threads = [threading.Thread(target=query_loop, daemon=True),
                   threading.Thread(target=load_loop, daemon=True)]
        for thread in threads:
            thread.start()
        try:
            for cycle in range(1, cycles + 1):
                for enabled in (False, True):
                    self.sleep(interval, f"Load running; cycle {cycle}, "
                                         f"next {self.IPC_SETTING}={enabled}")
                    self.set_ipc_setting(enabled)
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=300)
        alive = [thread.name for thread in threads if thread.is_alive()]
        self.assertFalse(alive, f"Background threads still running: {alive}")
        self.assertGreater(outcomes["ok"], 0,
                           "No Analytics query succeeded during the toggles")

        expected = items + 200 * batches[0]
        self.log.info(f"Queries ok={outcomes['ok']} "
                      f"failed={len(outcomes['failed'])}; docs={expected}")
        kv_count = self.settled_kv_count(bucket)
        self.log.info(f"KV holds {kv_count} docs (loader reported "
                      f"{expected})")
        self.assertLessEqual(
            len(outcomes["failed"]), allowed,
            "Analytics queries failed while the setting was toggled:\n  "
            + "\n  ".join(outcomes["failed"][:20]))
        self.wait_for_dataset_count(dataset, kv_count, cert=cert)
        self.analytics_admin_exec(f"DROP DATASET {dataset} IF EXISTS;")
        self.log.info("Test passed: no Analytics query failed and ingestion "
                      f"kept up across {cycles} toggle cycles")

    def test_ipc_analytics_handshake_limits(self):
        """
        With the setting on, a certificate that names nobody makes the
        server wait for credentials. Many such connections at once must
        neither stay open past the auth timeout nor stop Analytics serving.

        Steps:
            1. Encryption all, hybrid, setting on.
            2. Open ipc_hold_connections silent forged-certificate
               connections to one inter-node port, all at once.
            3. While they are held, Analytics answers on every node.
            4. Within the auth timeout after its handshake every connection
               is closed. Whether the per-source pending cap fired is logged
               for review: it depends on how many handshakes cbas runs at
               once.
        """
        hold = self.input.param("ipc_hold_connections", 24)
        timeout = self.input.param("auth_handshake_timeout", 5)
        self.set_ipc_setting(True)
        self.enable_internode_tls("all")
        self._set_client_cert_auth_http("hybrid")
        cert = self.generate_forged_internal_cert()
        node = self.cbas_nodes[0]
        ports = self.internode_ports(node)
        self.assertTrue(ports, f"No inter-node port listening on {node.ip}")
        port = ports[-1]
        self.wait_for_internode_enforcement(node, port, cert, True)
        marker = "connection(s), latest"
        before = self.analytics_log_count(node, marker)

        held = IPCAnalyticsUtils.internode_hold(node.ip, port, cert, hold)
        try:
            failures = []
            for target in self.cbas_nodes:
                failures += self.check_statement(
                    target, "SELECT 1;", cert, self.admin_auth(), 200, False,
                    "query while connections are held", None)
            slowest = max(seconds for _, seconds in held)
            self.sleep(timeout + 5, "Wait out the auth handshake timeout")
            still_open = sum(
                not IPCAnalyticsUtils.socket_closed(sock) for sock, _ in held)
        finally:
            for sock, _ in held:
                if sock is not None:
                    sock.close()
        # The per-source cap rejects before the TLS handshake starts, so a cap
        # refusal is fast; a connection merely stuck in the accept backlog also
        # comes back as None, but only after the 15s client timeout. Count the
        # quick ones so this does not pass on backlog timeouts alone.
        cap_refused = sum(1 for sock, seconds in held
                          if sock is None and seconds < 3)
        refused = sum(sock is None for sock, _ in held)
        after = self.analytics_log_count(node, marker)
        self.log.info(f"{node.ip}:{port}: {hold} silent connections, "
                      f"{refused} refused at connect ({cap_refused} quickly), "
                      f"slowest handshake {slowest:.1f}s, still open after the "
                      f"timeout {still_open}; pending-cap warnings "
                      f"{before}->{after}")
        self.assertFalse(failures, "\n  ".join(failures))
        self.assertEqual(still_open, 0,
                         f"{still_open} of {hold} silent internal-certificate "
                         f"connections outlived the auth timeout")
        self.assertTrue(cap_refused > 0 or after > before,
                        f"{hold} silent connections from one source, but none "
                        f"was refused quickly and no pending-cap warning was "
                        f"logged")
        self.log.info("Test passed: silent connections past the per-source "
                      "cap are refused, the rest time out, and Analytics "
                      "keeps serving")

    def test_ipc_analytics_remote_link(self):
        """
        An Analytics remote link authenticates to the remote cluster with
        whatever the link holds. The remote cluster's setting decides
        whether an internal certificate on a link is worth anything.

        Runs on two single-node clusters: the first (local) holds the link,
        the second (remote) holds the data.

        Steps:
            1. Remote: bucket with docs, client cert auth enable, the
               test CA trusted, setting on.
            2. Link with username/password, encryption full: ingests.
            3. Link with a prefix-mapped user certificate: ingests.
            4. Link with an internal-SAN certificate from the trusted CA:
               refused with the remote setting on, and the error carries no
               internal domain or key material. With it off, the remote
               answers the certificate as the internal identity -- the
               "before" evidence.
        """
        remote = self.cb_clusters["C2"].master
        bucket, items = "ipcan_remote", self.input.param("items", 1000)
        self.bucket_util.create_default_bucket(
            self.cb_clusters["C2"], bucket_name=bucket, ram_quota=256,
            replica=0, storage=Bucket.StorageBackend.couchstore)
        self.load_docs(bucket, items, master=remote)
        self.prepare_remote(remote, self.ca_cert)
        self.set_remote_ipc(remote, True)
        link_user = "ipcan_link"
        self.create_remote_user(remote, link_user, "Couchbase@1234", "admin")
        remote_ca = self.remote_cluster_ca(remote)
        user_cert = self.generate_client_cert(link_user)[:2]
        forged = self.generate_forged_internal_cert()
        local = self.cbas_nodes[0]

        def read(path):
            with open(path) as handle:
                return handle.read()

        base = {"type": "couchbase", "hostname": f"{remote.ip}:8091",
                "encryption": "full", "certificate": remote_ca}
        links = (
            ("ipcan_l_pw", {**base, "username": remote.rest_username,
                            "password": remote.rest_password}),
            ("ipcan_l_user", {**base, "clientCertificate": read(user_cert[0]),
                              "clientKey": read(user_cert[1])}),
        )
        for name, params in links:
            self.log.info(f"Link {name}")
            self.track_link(name)
            resp = IPCAnalyticsUtils.link_request(local, name, params=params)
            self.assertEqual(resp.status_code, 200,
                             f"Creating link {name} failed: HTTP "
                             f"{resp.status_code}: {resp.text[:300]}")
            dataset = f"{name}_ds"
            self.track_dataset(dataset)
            self.analytics_admin_exec(
                f"CREATE DATASET {dataset} ON `{bucket}` AT {name};")
            self.analytics_admin_exec(f"CONNECT LINK {name};")
            self.wait_for_dataset_count(dataset, items, nodes=[local])
            self.analytics_admin_exec(f"DISCONNECT LINK {name};")
            self.analytics_admin_exec(f"DROP DATASET {dataset};")
            IPCAnalyticsUtils.link_request(local, name, method="DELETE")

        forged_params = {**base, "clientCertificate": read(forged[0]),
                         "clientKey": read(forged[1])}
        results = {}
        self.track_link("ipcan_l_forged")
        for enabled in (True, False, True):
            self.set_remote_ipc(remote, enabled)
            resp = IPCAnalyticsUtils.link_request(local, "ipcan_l_forged",
                                                  params=forged_params)
            results.setdefault(enabled, []).append(
                (resp.status_code, resp.text[:300]))
            self.log.info(f"Forged-certificate link, remote "
                          f"{self.IPC_SETTING}={enabled}: HTTP "
                          f"{resp.status_code}: {resp.text[:300]}")
            IPCAnalyticsUtils.link_request(local, "ipcan_l_forged",
                                           method="DELETE")
        for status, text in results[True]:
            self.assertNotEqual(status, 200, "A link holding only an "
                                "internal certificate was accepted with the "
                                f"remote setting on: {text}")
            leaked = [m for m in self.LEAK_MARKERS if m in text]
            self.assertFalse(leaked, f"Link error leaks {leaked}: {text}")
        self.assertEqual(results[False][0][0], 200,
                         "With the remote setting off the internal "
                         "certificate should be accepted as the internal "
                         f"identity: {results[False][0]}")
        self.log.info("Test passed: the remote cluster's setting decides "
                      "whether an internal certificate on a link counts")
