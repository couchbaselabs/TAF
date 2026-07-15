import faulthandler
import os
import threading
import time
from pytests.bucket_collections.collections_base import CollectionBase
from shell_util.remote_connection import RemoteMachineShellConnection

"""
NFS setup requirements:
- Server: /data directory exported with appropriate permissions
- Client: Mount NFS export at /mnt/nfs_data
- Validation: Ensure server export and client mount are working
- Cleanup: Unique subdirectory created under mount point and removed after test
Scipts to setup NFS Server : https://github.com/couchbaselabs/test_infra_runner/tree/master/scripts/pitr_scripts
"""

# Exit code the shutdown guard reports; tearDown bumps it to 1 on failure so a
# failing suite still exits non-zero even when we bypass the normal exit path.
_suite_exit_code = 0
_guard_armed = False


def _arm_shutdown_guard():
    """
    PITR tests repeatedly delete/recreate buckets while SDK clients and remote
    shells are live, which can strand a worker thread blocked on I/O against a
    bucket that no longer exists. A stranded NON-daemon thread makes CPython
    hang forever during interpreter shutdown while joining it: testrunner's
    sys.exit() never returns and the run freezes right after "Killing Sirius
    pid". A stuck thread cannot be joined or made daemon after the fact, so we
    run just before the join phase (threading._register_atexit callbacks fire
    at the start of threading._shutdown, before the non-daemon join loop), dump
    whatever is still alive for a targeted follow-up fix, and hard-exit.

    Scoped to this suite: only armed when this module is imported, so no other
    test suite is affected.
    """
    global _guard_armed
    if _guard_armed or not hasattr(threading, "_register_atexit"):
        return
    _guard_armed = True

    def _on_shutdown():
        alive = [t for t in threading.enumerate()
                 if t is not threading.current_thread()
                 and t.is_alive() and not t.daemon]
        if alive:
            print("continuous_backup: %d non-daemon thread(s) still alive at "
                  "interpreter shutdown %s — dumping tracebacks and forcing "
                  "exit to avoid an indefinite join hang"
                  % (len(alive), [t.name for t in alive]))
            faulthandler.dump_traceback()
            # Bypass the hanging non-daemon join; reporting/cbcollect/Sirius
            # teardown have all already completed by this point.
            os._exit(_suite_exit_code)

    threading._register_atexit(_on_shutdown)


class ContinuousBackupBase(CollectionBase):
    def setUp(self):
        super(ContinuousBackupBase, self).setUp()

        _arm_shutdown_guard()
        self.bucket = self.cluster.buckets[0]
        self.bucket_name = self.bucket.name
        self.shell = RemoteMachineShellConnection(self.cluster.master)

    def tearDown(self):

        # Record failure so the shutdown guard still reports a non-zero exit
        # code if it has to bypass the normal exit path (see _arm_shutdown_guard).
        if self.is_test_failed():
            global _suite_exit_code
            _suite_exit_code = 1

        # Delete the shell connection if exists
        try:
            self.shell.disconnect()
        except Exception as e:
            self.log.error("Exception during removing shell: %s" % str(e))

        super(ContinuousBackupBase, self).tearDown()

    def _verify_doc_count(self, expected_count, bucket_name=None, timeout=300):
        if bucket_name is None:
            bucket_name = self.bucket.name
        self.log.info(f"Verifying document count for bucket '{bucket_name}'. Expected: {expected_count}")
        end_time = time.time() + timeout
        while time.time() < end_time:
            actual_items = self.bucket_util.get_buckets_item_count(self.cluster, bucket_name)
            if actual_items == expected_count:
                self.log.info(f"Document count for bucket '{bucket_name}' verified: {actual_items}")
                return
            self.log.info(f"Current doc counts for bucket '{bucket_name}'. Actual: {actual_items}, Expected: {expected_count}. Retrying in 10s...")
            self.sleep(10)
        self.fail(f"Document count mismatch for bucket '{bucket_name}'. Expected: {expected_count}, Actual: {actual_items}")
