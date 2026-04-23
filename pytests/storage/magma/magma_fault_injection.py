import os
import subprocess
import threading

from shell_util.remote_connection import RemoteMachineShellConnection
from storage.magma.magma_base import MagmaBaseTest


class MagmaFaultInjection(MagmaBaseTest):

    FAULT_INJECT_DIR = "/root/fault_injection"

    def setUp(self):
        super(MagmaFaultInjection, self).setUp()

        self.script_path = os.path.abspath(__file__)
        self.local_test_path = os.path.dirname(self.script_path)
        split_path = self.local_test_path.split("/")
        self.local_fi_scripts_path = "/" + os.path.join("/".join(split_path[1:4]), "scripts", "io_fault_injection")
        self.log.info(f"Local fault injection scripts path: {self.local_fi_scripts_path}")

        threads = []
        for server in self.cluster.servers:
            t = threading.Thread(target=self._setup_fault_injection_on_server, args=[server])
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

    def tearDown(self):

        for server in self.cluster.servers:
            ssh = RemoteMachineShellConnection(server)
            self.log.info(f"Server: {server.ip}, Moving memcached binary to the original location")
            mv_cmd = "mv /opt/couchbase/bin/memcached.real /opt/couchbase/bin/memcached"
            o, e = ssh.execute_command(mv_cmd)
            self.log.info(f"Output = {o}, Error = {e}")
            ssh.disconnect()

        super(MagmaFaultInjection, self).tearDown()

    def _setup_fault_injection_on_server(self, server):
        self.log.info(f"Setting up fault injection on server: {server.ip}")

        ssh = RemoteMachineShellConnection(server)

        # Create destination directory
        o, e = ssh.execute_command(f"mkdir -p {self.FAULT_INJECT_DIR}")

        # Copy scripts from local machine to remote server
        for script in ["fault_injection.c", "mc_wrapper.sh"]:
            copy_cmd = (
                'sshpass -p "{password}" scp -o StrictHostKeyChecking=no'
                ' {src}/{script} root@{ip}:{dst}/{script}'
            ).format(
                password="couchbase",
                src=self.local_fi_scripts_path,
                script=script,
                ip=server.ip,
                dst=self.FAULT_INJECT_DIR,
            )
            self.log.info(f"Copying {script} to {server.ip}")
            subprocess.run(copy_cmd, shell=True, executable="/bin/bash")

        # chmod +x on both files
        o, e = ssh.execute_command(
            f"chmod +x {self.FAULT_INJECT_DIR}/fault_injection.c"
            f" {self.FAULT_INJECT_DIR}/mc_wrapper.sh"
        )

        # Step 1: Stop couchbase server
        self.log.info(f"Stopping couchbase-server on {server.ip}")
        o, e = ssh.execute_command("systemctl stop couchbase-server")
        self.log.info(f"Output = {o}, Error = {e}")

        self.sleep(30, "Wait after stopping couchbase-server")

        # Step 2: Compile fault_injection.c into a shared library and install it
        self.log.info(f"Compiling and installing libfaultinject.so on {server.ip}")
        compile_cmds = (
            f"gcc -shared -fPIC -o /root/libfaultinject.so"
            f" {self.FAULT_INJECT_DIR}/fault_injection.c -ldl"
            f" && mv /root/libfaultinject.so /opt/couchbase/lib/libfaultinject.so"
            f" && chown root:root /opt/couchbase/lib/libfaultinject.so"
            f" && chmod 755 /opt/couchbase/lib/libfaultinject.so"
        )
        o, e = ssh.execute_command(compile_cmds)
        self.log.info(f"Output = {o}, Error = {e}")

        # Step 3: Create config files (if not present)
        self.log.info(f"Creating fault injection config files on {server.ip}")
        config_cmds = (
            "mkdir -p /run/faultinject"
            " && ([ -f /run/faultinject/enable ]        || echo 0    > /run/faultinject/enable)"
            " && ([ -f /run/faultinject/delay_ms ]      || echo 200  > /run/faultinject/delay_ms)"
            " && ([ -f /run/faultinject/fail_rate ]     || echo 0    > /run/faultinject/fail_rate)"
            " && ([ -f /run/faultinject/target_path ]   || echo /data/dta > /run/faultinject/target_path)"
            " && ([ -f /run/faultinject/validate_mode ] || echo 0    > /run/faultinject/validate_mode)"
            " && ([ -f /run/faultinject/validate.log ]  || touch /run/faultinject/validate.log)"
        )
        o, e = ssh.execute_command(config_cmds)
        self.log.info(f"Output = {o}, Error = {e}")

        # Step 4: Create memcached wrapper
        self.log.info(f"Installing memcached wrapper on {server.ip}")
        mv_cmd = "mv /opt/couchbase/bin/memcached /opt/couchbase/bin/memcached.real"
        o, e = ssh.execute_command(mv_cmd)
        self.log.info(f"Output = {o}, Error = {e}")

        wrapper_cmd = f"bash {self.FAULT_INJECT_DIR}/mc_wrapper.sh"
        o, e = ssh.execute_command(wrapper_cmd)
        self.log.info(f"Output = {o}, Error = {e}")

        # Step 5: Start couchbase server
        self.log.info(f"Starting couchbase-server on {server.ip}")
        o, e = ssh.execute_command("systemctl start couchbase-server")
        self.log.info(f"Output = {o}, Error = {e}")

        self.sleep(30, "Wait after starting couchbase-server")

        ssh.disconnect()
        self.log.info(f"Fault injection setup complete on {server.ip}")
