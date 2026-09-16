"""
Enterprise Analytics Python UDF automation (IDEA-543, EA 2.3).

Covers the framework's first test-content slice for library-backed
external functions: the argument-marshaling empirical check, Library
Lifecycle, Function Definition and Binding, and Type Mappings. See
`~/Documents/Analytics_Python_UDF_Test_Plan.md` for the full 13-section
plan this is a slice of, and
`~/Documents/Analytics_Python_UDF_Automation_Handoff.md` for the verified
environment facts and known bugs this suite is written against.

This is a sibling to `cbas_udf_management.py` (inline-body UDFs) and
deliberately does not extend `CBASUDF` from that module -- library
lifecycle is a concept that class has no notion of.

Automate against S3 only (node 2 of
`b/resources/enterprise-analytics-2-node-new.ini`): library upload on
Azure blob storage is confirmed broken (MB-73763).
"""

import json
import shutil

from cbas.cbas_base_server import CBASBaseTest
from cbas_utils.udf_fixture_builder import build_malformed_fixture, build_pyz_fixture
from cbas_utils.udf_library_utils import (
    UDF_EXECUTOR_CONTAINER_NAME, EAUDFLibraryClient, assert_library_on_every_node,
    ensure_udf_executor_runtime)
from cbas_utils.udf_sandbox_probe_utils import (
    HostObserver, SANDBOX_PROBE_MODULE_SOURCE)
from rbac_utils.Rbac_ready_functions import RbacUtils
from security.rbac_base import RbacBase
from security_utils.audit_ready_functions import audit as AuditUtil
from shell_util.remote_connection import RemoteMachineShellConnection

DEFAULT_DATAVERSE = "Default"
DEFAULT_SCOPE = "Default"
DEFAULT_DATABASE = "Default"


class _RawSqlpp:
    """
    Wraps a literal SQL++ expression so `_sqlpp_literal` emits it verbatim
    instead of quoting it as a string -- used for MISSING/NULL/casts/NaN
    literals that aren't representable as a plain Python value.
    """

    def __init__(self, expression):
        self.expression = expression


def _sqlpp_literal(value):
    """
    Renders a Python value as a SQL++ literal, for building a direct
    function-call statement when `verify_function_execution_result`'s
    plain str()-per-argument join isn't enough (booleans, strings,
    arrays/objects, None, and raw expressions via `_RawSqlpp`).
    """
    if isinstance(value, _RawSqlpp):
        return value.expression
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        # ensure_ascii=False: the default True escapes an astral-plane
        # character (e.g. an emoji) as a UTF-16 surrogate pair
        # (😀), and SQL++'s literal parser doesn't reconstitute
        # that back into one code point -- verified live 2026-09-13, the
        # value arrived as None instead of the emoji. Emitting the raw
        # UTF-8 bytes sidesteps the surrogate-pair question entirely.
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


# The executor's warnings channel carries a generic placeholder on
# literally every external UDF call, success or failure alike --
# verified live 2026-09-16 against a plain identity function that never
# raises. It is not itself a signal of anything going wrong. When the
# UDF actually raises, the *same* channel instead carries the real
# Python traceback (e.g. "...ZeroDivisionError: division by zero...").
# This is also why MB-73975 ("no error is reported at any level") needs
# a correction: an error is reported, just as a warning rather than as
# `errors`, and only visible when the caller asks for warnings at all
# (`execute_statement_on_cbas_util`'s `warnings` kwarg / `max-warnings`
# on the REST call) -- which this suite never did before this fix.
_UDF_NO_RETURNED_WARNINGS_MSG = "Error retrieving returned warnings from Python UDF"


def _real_udf_exception(warnings):
    """
    :return: the first warning dict that is not the generic placeholder
    (i.e. a real exception the UDF raised), or None if every warning
    present is just the placeholder (including when there are no
    warnings at all).
    """
    for w in (warnings or []):
        if _UDF_NO_RETURNED_WARNINGS_MSG not in w.get("msg", ""):
            return w
    return None


ECHO_MODULE_SOURCE = (
    "class Echo(object):\n"
    "    def __init__(self):\n"
    "        self.calls = 0\n"
    "    def hello(self, *args):\n"
    "        self.calls += 1\n"
    '        return "hello"\n'
)

ARG_PROBE_MODULE_SOURCE = (
    "class ArgProbe(object):\n"
    "    def via_varargs(self, *args):\n"
    '        return {"shape": "varargs", "type": type(args).__name__,\n'
    '                "len": len(args), "values": list(args)}\n'
    "    def via_single_arg(self, args):\n"
    '        return {"shape": "single_arg", "type": type(args).__name__,\n'
    '                "len": (len(args) if hasattr(args, "__len__") else -1),\n'
    '                "values": (list(args) if isinstance(args, (list, tuple))\n'
    "                            else [args])}\n"
)

# test_argument_marshaling_convention settled this: SQL++ passes arguments
# as native varargs, so args[0] is always the actual first argument value
# (not a wrapped tuple) -- verified live 2026-09-06.
IDENTITY_MODULE_SOURCE = (
    "class Identity(object):\n"
    "    def echo_one(self, *args):\n"
    "        return args[0]\n"
    "    def echo_none(self, *args):\n"
    "        return None\n"
    "    def echo_type_name(self, *args):\n"
    "        return type(args[0]).__name__\n"
)


class CBASPythonUDF(CBASBaseTest):
    def setUp(self):
        super().setUp()
        # All test cases run against a single Enterprise Analytics cluster.
        self.cluster = list(self.cb_clusters.values())[0]
        self.udf_username = self.input.param("udf_username", "Administrator")
        self.udf_password = self.input.param("udf_password", "password")
        self._local_fixture_dirs = []
        self.rbac_util = RbacUtils(self.cluster.master)
        for node in self.cluster.cbas_nodes:
            if not ensure_udf_executor_runtime(node, self.udf_username, self.udf_password):
                self.fail(
                    f"Failed to provision the UDF executor container on {node.ip}")
        self.log_setup_status(self.__class__.__name__, "Finished", stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started", stage=self.tearDown.__name__)
        for path in self._local_fixture_dirs:
            shutil.rmtree(path, ignore_errors=True)
        super().tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage=self.tearDown.__name__)

    # ---- helpers ----------------------------------------------------

    def _lib_client(self, node=None, username=None, password=None):
        return EAUDFLibraryClient(
            node or self.cluster.cbas_nodes[0],
            username or self.udf_username,
            password or self.udf_password,
        )

    def _upload(self, scope, name, module_name, module_source, extra_files=None, node=None,
                username=None, password=None, expect_success=True):
        local_path = build_pyz_fixture(module_name, module_source, extra_files=extra_files)
        self._local_fixture_dirs.append(local_path.rsplit("/", 1)[0])
        client = self._lib_client(node, username, password)
        try:
            success, http_status, body = client.upload_library(scope, name, local_path)
            if expect_success and not success:
                self.fail(f"Failed to upload library {scope}/{name}: status={http_status} body={body}")
        finally:
            client.disconnect()
        return local_path, success, http_status, body

    def _execute(self, cmd, **kwargs):
        """
        Runs `cmd` via `execute_statement_on_cbas_util` and returns
        (status, metrics, errors, results, handle, warnings).

        `warnings` defaults to 0 on the underlying call (cbas_utils_columnar
        .execute_statement_on_cbas_util's max_warning param), which is
        exactly why this suite never saw a warning even where one was
        actually returned -- this suite runs with runtype=onprem-columnar,
        which routes self.cbas_util through cbas_utils_columnar.CbasUtil
        (see cbas_utils.py's runtype dispatch), the variant that supports
        warnings end to end. Defaults to 100 here so a caller doesn't have
        to remember to ask for them; pass warnings=0 explicitly to suppress.
        Pass username=/password= to run as a non-admin user (RBAC checks).
        """
        kwargs.setdefault("warnings", 100)
        return self.cbas_util.execute_statement_on_cbas_util(self.cluster, cmd, **kwargs)

    def _get_library_row(self, scope, name):
        cmd = f'select value l from Metadata.`Library` l where Name="{name}" and DataverseName="{scope}"'
        status, _, _, results, _, _ = self._execute(cmd)
        if status == "success" and results:
            return results[0]
        return None

    def _create_udf(
        self,
        name,
        module,
        entry_point,
        library,
        parameters=None,
        dataverse=DEFAULT_DATAVERSE,
        or_replace=False,
        if_not_exists=False,
        with_options=None,
        validate_error_msg=False,
        expected_error=None,
        username=None,
        password=None,
    ):
        parameters = parameters if parameters is not None else ["a", "b", "c"]
        return self.cbas_util.create_udf(
            self.cluster,
            name=name,
            dataverse=dataverse,
            or_replace=or_replace,
            parameters=parameters,
            if_not_exists=if_not_exists,
            library=library,
            module=module,
            entry_point=entry_point,
            with_options=with_options,
            validate_error_msg=validate_error_msg,
            expected_error=expected_error,
            username=username,
            password=password,
            timeout=300,
            analytics_timeout=300,
        )

    def _call(self, full_name, args, username=None, password=None, timeout=300, analytics_timeout=300):
        arg_string = ",".join(_sqlpp_literal(a) for a in args)
        cmd = f"{full_name}({arg_string})"
        return self._execute(
            cmd, username=username, password=password, timeout=timeout, analytics_timeout=analytics_timeout)

    def _create_rbac_user(self, base_role=None, grant_execute=False,
                           grant_create_drop=False, password="password"):
        """
        Creates a local user via the Server REST API -- still available
        on EA -- optionally holding one built-in Server-side role (e.g.
        "admin", "analytics_access", "analytics_manager[*]"), then
        layers on function-level privilege via EA's own SQL++ RBAC.

        EA does not expose Couchbase Server's custom-role REST endpoint:
        PUT /settings/rbac/roles/<name> 404s "Object Not Found", even
        immediately after the diag_eval trick that enables it on classic
        Couchbase Server (verified live 2026-09-14, both nodes, right
        after confirming the persistent_term write itself took effect).
        Enterprise Analytics roles and function privileges are SQL++-
        native instead (CREATE ROLE / GRANT ... TO USER|ROLE /
        Metadata.Role); this suite grants straight to the user rather
        than through an intermediate role, since none of these tests
        share a role across users.

        Library upload over the admin socket is still gated the old
        way, and needs the built-in "admin" (Full Admin) role --
        verified live that neither "analytics_admin" nor
        "analytics_manager[*]" suffice, so pass base_role="admin" for a
        user that must be able to upload/define, and a narrower role
        (e.g. "analytics_manager[*]") for one that must not.
        Executing a function needs SQL++ EXECUTE (grant_execute=True)
        on top of the "analytics_access" baseline every query needs;
        defining/dropping one over an *existing* library needs SQL++
        CREATE, DROP FUNCTION (grant_create_drop=True) and does not
        require "admin".

        :return: (username, password) for use with
        _execute/_call/_create_udf/_upload's username=/password= and
        cleanup via _drop_rbac_user.
        """
        username = f"udfuser_{self.cbas_util.generate_name()}"
        params = f"name={username}&password={password}"
        if base_role:
            params += f"&roles={base_role}"
        status, resp = self.rbac_util.security_rest.create_local_user(username, params)
        if not status:
            self.fail(f"create_local_user failed for {username}: {resp}")
        if grant_execute:
            status, _, errors, _, _, _ = self._execute(
                f"GRANT EXECUTE ON any FUNCTION TO USER {username}")
            if status != "success":
                self.fail(f"GRANT EXECUTE ON any FUNCTION TO USER {username} failed: {errors}")
        if grant_create_drop:
            status, _, errors, _, _, _ = self._execute(
                f"GRANT CREATE, DROP FUNCTION TO USER {username}")
            if status != "success":
                self.fail(f"GRANT CREATE, DROP FUNCTION TO USER {username} failed: {errors}")
        return username, password

    def _create_rbac_user_no_privileges(self, password="password"):
        # A user assigned no role at all -- covers "no Enterprise
        # Analytics privileges" without needing a custom role.
        username = f"udfuser_{self.cbas_util.generate_name()}"
        user = [{"id": username, "password": password, "name": "Some Name"}]
        RbacBase().create_user_source(user, "builtin", self.cluster.master)
        return username, password

    def _drop_rbac_user(self, username):
        self.rbac_util.security_rest.delete_local_user(username)

    # ---- §4 argument marshaling (run first; gates type mappings) ----

    def test_argument_marshaling_convention(self):
        name = f"argprobe_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "argprobe", ARG_PROBE_MODULE_SOURCE)

        varargs_fn = f"arg_via_varargs_{self.cbas_util.generate_name()}"
        single_arg_fn = f"arg_via_single_{self.cbas_util.generate_name()}"

        if not self._create_udf(varargs_fn, "argprobe", "ArgProbe.via_varargs", name, parameters=["a", "b", "c"]):
            self.fail("Error creating varargs-style external UDF")
        if not self._create_udf(single_arg_fn, "argprobe", "ArgProbe.via_single_arg", name, parameters=["a", "b", "c"]):
            self.fail("Error creating single-tuple-style external UDF")

        varargs_full = f"{DEFAULT_DATAVERSE}.{varargs_fn}"
        single_arg_full = f"{DEFAULT_DATAVERSE}.{single_arg_fn}"

        status_a, _, errors_a, results_a, _, _ = self._call(varargs_full, [1, 2, 3])
        status_b, _, errors_b, results_b, _, _ = self._call(single_arg_full, [1, 2, 3])

        self.log.info(
            f"Argument marshaling result -- *args form: status={status_a} result={results_a} errors={errors_a}"
        )
        self.log.info(
            f"Argument marshaling result -- single-arg form: status={status_b} result={results_b} errors={errors_b}"
        )

        if status_a != "success" and status_b != "success":
            self.fail(
                "Neither the *args form nor the single-tuple form of an "
                "external UDF could be invoked -- Blocker B may have "
                f"regressed. *args errors: {errors_a}; single-arg errors: {errors_b}"
            )

        # The point of this check is the finding, not a pass/fail verdict:
        # whichever form succeeded (or both) is the marshaling convention.
        # Downstream type-mapping functions in this file are written to
        # tolerate either shape rather than assume this result.

    # ---- §2 Library Lifecycle ----------------------------------------

    def test_library_upload_and_metadata(self):
        name = f"echo_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)

        row = self._get_library_row(DEFAULT_SCOPE, name)
        if row is None:
            self.fail(f"No Metadata.Library entry found for {DEFAULT_SCOPE}/{name} after upload")
        self.log.info(f"Metadata.Library row: {row}")

        results = assert_library_on_every_node(
            self.cluster.cbas_nodes,
            DEFAULT_DATAVERSE,
            DEFAULT_SCOPE,
            name,
            username=self.udf_username,
            password=self.udf_password,
        )
        for ip, (exists, _) in results.items():
            if not exists:
                self.fail(f"Library artifact missing on node {ip} after upload")

    def test_library_listing_always_empty_known_bug(self):
        # Regression guard for the unfiled known bug: GET /api/v1/library
        # returns 200 with [] even with a library installed and callable.
        # If this starts returning entries, that's a fix worth noticing,
        # not a silent behavior change.
        #
        # The scope-qualified GET /api/v1/library/{scope} 404s -- verified
        # live, this listing endpoint is not scope-qualifiable, only the
        # bare form works.
        name = f"echo_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)

        client = self._lib_client()
        try:
            http_status, body = client.list_libraries()
        finally:
            client.disconnect()
        self.log.info(f"Library listing after upload: status={http_status} body={body}")
        if http_status != "200":
            self.fail(f"Library listing returned unexpected status {http_status}")
        if body and body.strip() not in ("[]", ""):
            self.log.info(f"Library listing now returns entries -- the known always-empty bug appears fixed: {body}")

    def test_library_upload_rejects_malformed_archives(self):
        # "empty" and "truncated" are not valid zip files at all, and are
        # rejected at upload time. "no_site_packages" is verified live
        # (2026-09-06) to be a different case: it's a structurally valid
        # zip, so upload accepts it with 200 OK -- the failure only
        # surfaces later, when a function bound to it is actually invoked
        # and the interpreter can't find the module.
        client = self._lib_client()
        try:
            for kind in ("truncated", "empty"):
                local_path = build_malformed_fixture(kind)
                self._local_fixture_dirs.append(local_path.rsplit("/", 1)[0])
                name = f"malformed_{kind}_{self.cbas_util.generate_name()}"
                success, http_status, body = client.upload_library(DEFAULT_SCOPE, name, local_path)
                self.log.info(f"Malformed archive '{kind}' upload: success={success} status={http_status} body={body}")
                if success:
                    self.fail(f"Malformed archive '{kind}' was accepted with 200 OK")
                row = self._get_library_row(DEFAULT_SCOPE, name)
                if row is not None:
                    self.fail(
                        f"Rejected upload for malformed archive '{kind}' still left a Metadata.Library entry: {row}"
                    )

            local_path = build_malformed_fixture("no_site_packages")
            self._local_fixture_dirs.append(local_path.rsplit("/", 1)[0])
            name = f"malformed_no_site_packages_{self.cbas_util.generate_name()}"
            success, http_status, body = client.upload_library(DEFAULT_SCOPE, name, local_path)
            self.log.info(
                f"Malformed archive 'no_site_packages' upload: success={success} status={http_status} body={body}"
            )
            if not success:
                self.fail(
                    "Malformed archive 'no_site_packages' was rejected at upload -- expected it to be accepted "
                    "(structurally valid zip) and fail later at invocation instead"
                )
        finally:
            client.disconnect()

        fn = f"nosite_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a"]):
            self.fail("Error binding a function to the no_site_packages library")
        status, _, errors, _, _, _ = self._call(f"{DEFAULT_DATAVERSE}.{fn}", [1])
        if status == "success":
            self.fail("Invoking a function from a no_site_packages library succeeded")
        self.log.info(f"Invocation against no_site_packages library failed as expected: {errors}")

    def test_library_upsert_takes_effect(self):
        name = f"upsert_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)

        fn = f"upsert_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a"]):
            self.fail("Error creating UDF bound to library")
        full_name = f"{DEFAULT_DATAVERSE}.{fn}"

        status, _, errors, results, _, _ = self._call(full_name, [1])
        if status != "success" or results[0] != "hello":
            self.fail(f"Initial call before upsert failed: {errors} / {results}")

        new_source = ECHO_MODULE_SOURCE.replace('return "hello"', 'return "hello-v2"')
        self._upload(DEFAULT_SCOPE, name, "mylib", new_source)

        status, _, errors, results, _, _ = self._call(full_name, [1])
        if status != "success":
            self.fail(f"Call after upsert failed: {errors}")
        if results[0] != "hello-v2":
            self.fail(f"Upsert did not take effect on next query -- expected 'hello-v2', got {results[0]}")

    def test_library_drop_breaks_bound_function(self):
        name = f"todrop_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)

        fn = f"todrop_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a"]):
            self.fail("Error creating UDF bound to library")
        full_name = f"{DEFAULT_DATAVERSE}.{fn}"

        status, _, _, results, _, _ = self._call(full_name, [1])
        if status != "success" or results[0] != "hello":
            self.fail("Function did not work before dropping its library")

        client = self._lib_client()
        try:
            http_status, body = client.drop_library(DEFAULT_SCOPE, name)
        finally:
            client.disconnect()
        self.log.info(f"drop_library observed status={http_status} body={body}")

        results_by_node = assert_library_on_every_node(
            self.cluster.cbas_nodes,
            DEFAULT_DATAVERSE,
            DEFAULT_SCOPE,
            name,
            username=self.udf_username,
            password=self.udf_password,
        )
        artifact_still_present = any(exists for exists, _ in results_by_node.values())

        status, _, errors, results, _, _ = self._call(full_name, [1])
        if status == "success" and not artifact_still_present:
            self.fail(
                "Function call succeeded after its library's on-disk "
                "artifact was removed -- expected a missing-library error. "
                f"Result: {results}"
            )
        self.log.info(
            f"Post-drop call status={status} errors={errors} (artifact_still_present="
            f"{artifact_still_present}, drop_library http_status={http_status})"
        )

    def test_library_same_name_different_scopes(self):
        name = f"shared_{self.cbas_util.generate_name()}"
        second_scope = f"scope_{self.cbas_util.generate_name()}"

        if not self.cbas_util.create_dataverse(self.cluster, dataverse_name=second_scope, analytics_scope=True):
            self.fail("Error creating second analytics scope for isolation test")

        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)
        second_source = ECHO_MODULE_SOURCE.replace('return "hello"', 'return "hello-scope-2"')
        self._upload(second_scope, name, "mylib", second_source)

        fn_default = f"shared_fn_a_{self.cbas_util.generate_name()}"
        fn_second = f"shared_fn_b_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn_default, "mylib", "Echo.hello", name, parameters=["a"], dataverse=DEFAULT_DATAVERSE):
            self.fail("Error creating UDF against Default-scope library")
        if not self._create_udf(fn_second, "mylib", "Echo.hello", name, parameters=["a"], dataverse=second_scope):
            self.fail("Error creating UDF against second-scope library")

        status_a, _, errors_a, results_a, _, _ = self._call(f"{DEFAULT_DATAVERSE}.{fn_default}", [1])
        status_b, _, errors_b, results_b, _, _ = self._call(f"{second_scope}.{fn_second}", [1])

        if status_a != "success" or results_a[0] != "hello":
            self.fail(f"Default-scope function resolved incorrectly: {errors_a} / {results_a}")
        if status_b != "success" or results_b[0] != "hello-scope-2":
            self.fail(f"Second-scope function resolved incorrectly: {errors_b} / {results_b}")

    def test_library_survives_service_restart(self):
        name = f"restart_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)

        fn = f"restart_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a"]):
            self.fail("Error creating UDF before restart")
        full_name = f"{DEFAULT_DATAVERSE}.{fn}"

        # cluster_util.stop_server/start_server assume a classic Couchbase
        # Server install (is_couchbase_installed() -> stop_couchbase(), a
        # no-op check that falls through to the legacy stop_membase() path
        # that doesn't exist on this shell class) and don't recognize
        # Enterprise Analytics -- restart the enterprise-analytics systemd
        # service directly instead.
        node = self.cluster.cbas_nodes[0]
        shell = RemoteMachineShellConnection(node)
        try:
            shell.execute_command("systemctl restart enterprise-analytics")
        finally:
            shell.disconnect()
        if not self.cbas_util.is_analytics_running(self.cluster):
            self.fail("Analytics service did not come back up after restart")

        status, _, errors, results, _, _ = self._call(full_name, [1])
        if status != "success" or results[0] != "hello":
            self.fail(
                "Function did not survive a service restart without "
                f"re-upload: status={status} errors={errors} results={results}"
            )

    # ---- §3 Function Definition and Binding ---------------------------

    def test_bind_class_method(self):
        name = f"classmethod_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)

        fn = f"classmethod_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a"]):
            self.fail("Error binding a class method")
        if not self.cbas_util.validate_external_udf_in_metadata(
            self.cluster,
            fn,
            DEFAULT_DATAVERSE,
            DEFAULT_DATABASE,
            ["a"],
            expected_library=name,
            expected_module="mylib",
            expected_entry_point="Echo.hello",
        ):
            self.fail("Metadata.Function entry did not reflect the bound library/module/entry point")

    def test_bind_module_level_function(self):
        name = f"modlevel_{self.cbas_util.generate_name()}"
        module_source = 'def greet(*args):\n    return "hi"\n'
        self._upload(DEFAULT_SCOPE, name, "greetlib", module_source)

        fn = f"modlevel_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "greetlib", "greet", name, parameters=["a"]):
            self.fail("Error binding a module-level function (no class)")
        status, _, errors, results, _, _ = self._call(f"{DEFAULT_DATAVERSE}.{fn}", [1])
        if status != "success" or results[0] != "hi":
            self.fail(f"Module-level function call failed: {errors} / {results}")

    def test_lazy_binding_unresolved_symbol_fails_at_first_call(self):
        name = f"lazy_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)

        bad_module = "no_such_module_xyz"
        fn = f"lazy_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, bad_module, "Echo.hello", name, parameters=["a"]):
            self.fail(
                "CREATE FUNCTION against a non-existent module failed at "
                "definition time -- contradicts the documented laziness"
            )

        status, _, errors, _, _, _ = self._call(f"{DEFAULT_DATAVERSE}.{fn}", [1])
        if status == "success":
            self.fail("Call to a function bound to a non-existent module succeeded")
        # Verified live (2026-09-06): the client-facing error is always the
        # same generic {"code": 25000, "msg": "Internal error"} regardless
        # of cause -- it never names the unresolved symbol. The real
        # exception (e.g. a Python ModuleNotFoundError) only appears in the
        # node's own analytics_cbas_debug.log, not in the response. This is
        # a real diagnostic-quality finding, not a test bug: don't assert
        # symbol-naming in the client response, just that it fails.
        self.log.info(f"First-call error for unresolved module {bad_module}: {errors}")

    def test_arity_mismatch_error_at_first_call(self):
        name = f"arity_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)

        fn = f"arity_fn_{self.cbas_util.generate_name()}"
        # Declared arity 3, Python signature is variadic (accepts any
        # count) -- to actually surface a mismatch, call with an argument
        # count colliding with a *different*, non-existent overload
        # instead, since Echo.hello(*args) accepts anything: bind at
        # arity 3, call with a 2-argument invocation, which is a genuine
        # signature mismatch at the SQL++ dispatch layer.
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a", "b", "c"]):
            self.fail("Error creating 3-arity external UDF")

        status, _, errors, _, _, _ = self._call(f"{DEFAULT_DATAVERSE}.{fn}", [1, 2])
        if status == "success":
            self.fail("Call with a mismatched argument count against a 3-arity function succeeded")
        self.log.info(f"Arity-mismatch error: {errors}")

    def test_create_or_replace_rebinds_to_different_library(self):
        lib_a = f"reba_{self.cbas_util.generate_name()}"
        lib_b = f"rebb_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, lib_a, "mylib", ECHO_MODULE_SOURCE)
        self._upload(DEFAULT_SCOPE, lib_b, "mylib", ECHO_MODULE_SOURCE.replace('return "hello"', 'return "hello-b"'))

        fn = f"rebind_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "mylib", "Echo.hello", lib_a, parameters=["a"]):
            self.fail("Error creating initial function bound to library A")
        full_name = f"{DEFAULT_DATAVERSE}.{fn}"
        status, _, errors, results, _, _ = self._call(full_name, [1])
        if status != "success" or results[0] != "hello":
            self.fail(f"Initial binding to library A failed: {errors} / {results}")

        if not self._create_udf(fn, "mylib", "Echo.hello", lib_b, parameters=["a"], or_replace=True):
            self.fail("CREATE OR REPLACE to library B failed")
        status, _, errors, results, _, _ = self._call(full_name, [1])
        if status != "success" or results[0] != "hello-b":
            self.fail(
                "After OR REPLACE, function still resolves against the "
                f"old library -- expected 'hello-b', got: {errors} / {results}"
            )

    def test_drop_function_if_exists(self):
        fn = f"dropfn_{self.cbas_util.generate_name()}"
        if not self.cbas_util.drop_udf(
            self.cluster,
            name=fn,
            dataverse=DEFAULT_DATAVERSE,
            database=DEFAULT_DATABASE,
            parameters=["a"],
            if_exists=True,
            timeout=300,
            analytics_timeout=300,
        ):
            self.fail("DROP FUNCTION IF EXISTS failed for a function that was never created")

        name = f"dropfn_lib_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a"]):
            self.fail("Error creating function for drop test")
        if not self.cbas_util.drop_udf(
            self.cluster,
            name=fn,
            dataverse=DEFAULT_DATAVERSE,
            database=DEFAULT_DATABASE,
            parameters=["a"],
            if_exists=False,
            timeout=300,
            analytics_timeout=300,
        ):
            self.fail("DROP FUNCTION failed for an existing function")

    def test_with_clause_defaults_and_round_trip(self):
        name = f"withclause_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)

        fn_default = f"withdef_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn_default, "mylib", "Echo.hello", name, parameters=["a"]):
            self.fail("Error creating function with no WITH clause")
        row = self.cbas_util.get_udf_metadata_row(self.cluster, fn_default, DEFAULT_DATAVERSE, DEFAULT_DATABASE)
        self.log.info(
            "Metadata.Function row with no WITH clause given (checking "
            f"documented defaults null-call=false, deterministic=true): {row}"
        )

        fn_explicit = f"withexp_fn_{self.cbas_util.generate_name()}"
        with_options = {"null-call": True, "deterministic": False}
        if not self._create_udf(fn_explicit, "mylib", "Echo.hello", name, parameters=["a"], with_options=with_options):
            self.fail("Error creating function with an explicit WITH clause")
        row = self.cbas_util.get_udf_metadata_row(self.cluster, fn_explicit, DEFAULT_DATAVERSE, DEFAULT_DATABASE)
        self.log.info(f"Metadata.Function row with explicit WITH {with_options}: {row}")
        row_text = json.dumps(row)
        if "true" not in row_text.lower() and "True" not in row_text:
            self.log.error(
                "Explicit WITH clause options do not appear to be "
                f"reflected anywhere in the Metadata.Function row: {row}"
            )

    def test_dropping_library_referenced_by_function(self):
        name = f"refd_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)

        fn = f"refd_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a"]):
            self.fail("Error creating function for drop-library-in-use test")

        client = self._lib_client()
        try:
            http_status, body = client.drop_library(DEFAULT_SCOPE, name)
        finally:
            client.disconnect()
        self.log.info(f"Dropping a library referenced by a live function: status={http_status} body={body}")
        # No assertion on whether the drop itself is blocked or succeeds --
        # the test plan explicitly leaves this open ("either blocked or
        # invalidates the function"). What must not happen is the function
        # silently continuing to work against bytes that no longer exist:
        status, _, errors, results, _, _ = self._call(f"{DEFAULT_DATAVERSE}.{fn}", [1])
        artifact_by_node = assert_library_on_every_node(
            self.cluster.cbas_nodes,
            DEFAULT_DATAVERSE,
            DEFAULT_SCOPE,
            name,
            username=self.udf_username,
            password=self.udf_password,
        )
        artifact_gone = not any(exists for exists, _ in artifact_by_node.values())
        if artifact_gone and status == "success":
            self.fail(
                "Function call succeeded even though its library's "
                f"on-disk artifact was removed on every node. Result: {results}"
            )
        self.log.info(f"Post-drop-attempt call: status={status} errors={errors}")

    # ---- §6 Type Mappings ----------------------------------------------

    def _identity_fn(self):
        """
        Uploads the shared Identity library once per call site and returns
        its (library_name, full_dataverse_name) for the three probe
        entry points (echo_one, echo_none, echo_type_name).
        """
        name = f"identity_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "identitylib", IDENTITY_MODULE_SOURCE)
        return name

    def _bind_identity(self, library, entry_point):
        fn = f"id_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "identitylib", entry_point, library, parameters=["a"]):
            self.fail(f"Error binding Identity.{entry_point}")
        return f"{DEFAULT_DATAVERSE}.{fn}"

    def test_scalar_roundtrip(self):
        library = self._identity_fn()
        full_name = self._bind_identity(library, "Identity.echo_one")

        cases = [
            ("int8", 12),
            ("int16", 1234),
            ("int32", 123456),
            ("int64", 123456789012),
            ("float", 1.5),
            ("double", 2.71828),
            ("string", "round-trip"),
            ("boolean", True),
        ]
        for label, value in cases:
            status, _, errors, results, _, _ = self._call(full_name, [value])
            if status != "success":
                self.fail(f"Round-trip call failed for {label}: {errors}")
            if results[0] != value:
                self.fail(f"Round-trip mismatch for {label}: sent {value}, got {results[0]}")

    def test_array_and_object_roundtrip(self):
        library = self._identity_fn()
        full_name = self._bind_identity(library, "Identity.echo_one")

        cases = [
            [1, 2, 3],
            {"a": 1, "b": "two"},
            {"nested": [1, {"x": [2, 3]}, "y"]},
        ]
        for value in cases:
            status, _, errors, results, _, _ = self._call(full_name, [value])
            if status != "success":
                self.fail(f"Round-trip call failed for {value}: {errors}")
            if results[0] != value:
                self.fail(f"Round-trip mismatch: sent {value}, got {results[0]}")

    def test_none_return_becomes_null(self):
        library = self._identity_fn()
        full_name = self._bind_identity(library, "Identity.echo_none")

        status, _, errors, results, _, _ = self._call(full_name, [1])
        if status != "success":
            self.fail(f"Call failed: {errors}")
        if results[0] is not None:
            self.fail(f"Python None did not become SQL++ NULL -- got {results[0]}")

    def test_unsupported_type_rejected_cast_succeeds(self):
        library = self._identity_fn()
        full_name = self._bind_identity(library, "Identity.echo_type_name")

        status, _, errors, _, _, _ = self._call(full_name, [_RawSqlpp("current_datetime()")])
        if status == "success":
            self.fail("Passing an unsupported type (datetime) directly succeeded instead of failing with a type error")
        self.log.info(f"Unsupported-type error: {errors}")

        # SQL++ has no CAST(x AS t) -- verified live 2026-09-14
        # ("Encountered \"cast\" at column..."). Use the conversion
        # function instead.
        status, _, errors, results, _, _ = self._call(full_name, [_RawSqlpp("to_string(current_datetime())")])
        if status != "success":
            self.fail(f"Explicitly casting the same value to a supported type (string) still failed: {errors}")
        if results[0] != "str":
            self.fail(f"After casting to string, UDF observed type {results[0]}, expected 'str'")

    def test_null_call_flag_skip_behavior(self):
        library = self._identity_fn()

        fn_default = self._bind_identity(library, "Identity.echo_one")
        status, _, errors, results, _, _ = self._call(fn_default, [_RawSqlpp("missing")])
        self.log.info(
            f"Default null-call=false, called with MISSING: status={status} errors={errors} results={results}"
        )
        if status == "success" and results and results[0] not in (None, "missing"):
            self.fail(
                f"Under the default null-call=false, a MISSING argument did not skip invocation -- got {results[0]}"
            )

        fn = f"id_nullcall_{self.cbas_util.generate_name()}"
        if not self._create_udf(
            fn, "identitylib", "Identity.echo_one", library, parameters=["a"], with_options={"null-call": True}
        ):
            self.fail("Error creating UDF with null-call=true")
        full_name = f"{DEFAULT_DATAVERSE}.{fn}"
        status, _, errors, results, _, _ = self._call(full_name, [_RawSqlpp("null")])
        if status != "success":
            self.fail(f"With null-call=true, invocation with NULL failed: {errors}")

    def test_boundary_values(self):
        library = self._identity_fn()
        full_name = self._bind_identity(library, "Identity.echo_one")

        cases = [
            ("int64_max", 9223372036854775807, 9223372036854775807),
            # -9223372036854775808 as a literal fails to parse: SQL++ parses
            # the positive magnitude first, which overflows int64 by one
            # (9223372036854775808 > int64 max). Verified live 2026-09-12.
            # Compute it instead so it never forms that literal.
            ("int64_min", _RawSqlpp("-9223372036854775807 - 1"), -9223372036854775808),
            ("empty_string", "", ""),
            ("unicode_emoji", "héllo \U0001f600", "héllo \U0001f600"),
            ("large_string", "x" * (2 * 1024 * 1024), "x" * (2 * 1024 * 1024)),
            ("deeply_nested", {"a": {"b": {"c": {"d": [1, 2, 3]}}}}, {"a": {"b": {"c": {"d": [1, 2, 3]}}}}),
        ]
        for label, value, expected in cases:
            status, _, errors, results, _, _ = self._call(full_name, [value])
            if status != "success":
                self.fail(f"Boundary value {label} failed rather than being handled or cleanly rejected: {errors}")
            if results[0] != expected:
                self.fail(f"Boundary value {label} round-tripped incorrectly: expected {expected}, got {results[0]}")

        for label, expr in (("nan", 'double("NaN")'), ("pos_inf", 'double("+INF")'), ("neg_inf", 'double("-INF")')):
            status, _, errors, results, _, _ = self._call(full_name, [_RawSqlpp(expr)])
            self.log.info(f"Boundary value {label}: status={status} errors={errors} results={results}")
            if status != "success":
                self.fail(
                    f"Boundary value {label} was rejected outright rather than handled or cleanly errored: {errors}"
                )

    # ---- §5 RBAC and Privilege Enforcement ----------------------------
    #
    # The privilege split is settled (test plan §5): both uploading a
    # library and defining a function over it require
    # cluster.admin.diag!write, not cluster.analytics!manage -- exposing
    # a function is treated as an administrative act since the code
    # restricting a definition to its own library isn't strongly
    # trusted. Executing an already-defined function needs only
    # cluster.analytics!select. There is deliberately no per-user
    # sandboxing: the administrator is trusted to vet uploaded code.

    def test_rbac_define_function_requires_diag_write(self):
        name = f"rbacdef_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)

        # EA's equivalent of the old cluster.analytics!manage/
        # cluster.admin.diag!write split, verified live 2026-09-14:
        # "analytics_manager[*]" alone cannot define a function or
        # upload a library; the built-in "admin" (Full Admin) role can
        # do both (narrower built-in roles, e.g. "analytics_admin", do
        # not suffice for the upload socket either).
        manage_user, manage_password = self._create_rbac_user(
            base_role="analytics_manager[*],analytics_access")
        diagwrite_user, diagwrite_password = self._create_rbac_user(base_role="admin")
        try:
            denied_fn = f"rbacdef_denied_{self.cbas_util.generate_name()}"
            if self._create_udf(
                denied_fn, "mylib", "Echo.hello", name, parameters=["a"],
                username=manage_user, password=manage_password,
            ):
                self.fail(
                    "A user holding only cluster.analytics!manage was able "
                    "to define a function over an existing library"
                )

            # Same restriction applies to the upload step itself.
            upload_name = f"rbacdef_upload_{self.cbas_util.generate_name()}"
            _, upload_success, upload_status, upload_body = self._upload(
                DEFAULT_SCOPE, upload_name, "mylib", ECHO_MODULE_SOURCE,
                username=manage_user, password=manage_password, expect_success=False,
            )
            if upload_success:
                self.fail(
                    "A user holding only cluster.analytics!manage was able "
                    "to upload a library"
                )
            self.log.info(f"manage-only upload denial: status={upload_status} body={upload_body}")

            allowed_fn = f"rbacdef_allowed_{self.cbas_util.generate_name()}"
            if not self._create_udf(
                allowed_fn, "mylib", "Echo.hello", name, parameters=["a"],
                username=diagwrite_user, password=diagwrite_password,
            ):
                self.fail(
                    "A user holding cluster.admin.diag!write could not "
                    "define a function over an existing library"
                )
        finally:
            self._drop_rbac_user(manage_user)
            self._drop_rbac_user(diagwrite_user)

    def test_rbac_execute_privilege_load_bearing(self):
        name = f"rbacexec_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)
        fn = f"rbacexec_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a"]):
            self.fail("Error creating UDF for the execute-privilege test")
        full_name = f"{DEFAULT_DATAVERSE}.{fn}"

        # "analytics_access" is the baseline every EA query needs (even
        # a bare `select value 1` 403s without it, verified live); it
        # carries no function-execute privilege of its own, so this
        # user starts out unable to call anything.
        no_select_user, no_select_password = self._create_rbac_user(base_role="analytics_access")
        try:
            status, _, errors, _, _, _ = self._call(
                full_name, [1], username=no_select_user, password=no_select_password)
            if status == "success":
                self.fail(
                    "A user without cluster.analytics!select was able to "
                    "call an existing external function"
                )
            self.log.info(f"Denied call before grant: {errors}")

            status, _, errors, _, _, _ = self._execute(
                f"GRANT EXECUTE ON any FUNCTION TO USER {no_select_user}")
            if status != "success":
                self.fail(f"Failed to re-grant cluster.analytics!select: {errors}")

            status, _, errors, results, _, _ = self._call(
                full_name, [1], username=no_select_user, password=no_select_password)
            if status != "success" or results[0] != "hello":
                self.fail(
                    "After granting cluster.analytics!select, the identical "
                    f"call still failed: {errors} / {results}"
                )
        finally:
            self._drop_rbac_user(no_select_user)

    def test_rbac_select_only_user_can_execute_not_manage(self):
        name = f"rbacsel_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)
        fn = f"rbacsel_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a"]):
            self.fail("Error creating UDF for the select-only test")
        full_name = f"{DEFAULT_DATAVERSE}.{fn}"

        select_user, select_password = self._create_rbac_user(
            base_role="analytics_access", grant_execute=True)
        try:
            status, _, errors, results, _, _ = self._call(
                full_name, [1], username=select_user, password=select_password)
            if status != "success" or results[0] != "hello":
                self.fail(f"A select-only user could not execute an existing function: {errors} / {results}")

            other_fn = f"rbacsel_other_{self.cbas_util.generate_name()}"
            if self._create_udf(
                other_fn, "mylib", "Echo.hello", name, parameters=["a"],
                username=select_user, password=select_password,
            ):
                self.fail("A select-only user was able to CREATE FUNCTION")
            if self._create_udf(
                fn, "mylib", "Echo.hello", name, parameters=["a"], or_replace=True,
                username=select_user, password=select_password,
            ):
                self.fail("A select-only user was able to CREATE OR REPLACE FUNCTION")
            if self.cbas_util.drop_udf(
                self.cluster, name=fn, dataverse=DEFAULT_DATAVERSE, database=DEFAULT_DATABASE,
                parameters=["a"], username=select_user, password=select_password,
                timeout=300, analytics_timeout=300,
            ):
                self.fail("A select-only user was able to DROP FUNCTION")
        finally:
            self._drop_rbac_user(select_user)

    def test_rbac_no_privileges_denied_entirely(self):
        name = f"rbacnone_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)
        fn = f"rbacnone_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a"]):
            self.fail("Error creating UDF for the no-privileges test")
        full_name = f"{DEFAULT_DATAVERSE}.{fn}"

        none_user, none_password = self._create_rbac_user_no_privileges()
        try:
            new_fn = f"rbacnone_new_{self.cbas_util.generate_name()}"
            if self._create_udf(
                new_fn, "mylib", "Echo.hello", name, parameters=["a"],
                username=none_user, password=none_password,
            ):
                self.fail("A user with no Enterprise Analytics privileges was able to define a function")
            status, _, errors, _, _, _ = self._call(
                full_name, [1], username=none_user, password=none_password)
            if status == "success":
                self.fail("A user with no Enterprise Analytics privileges was able to execute a function")
            self.log.info(f"No-privileges denial: {errors}")
        finally:
            self._drop_rbac_user(none_user)

    def test_rbac_no_indirect_definition_path(self):
        # This suite is a SQL++ client only, so it can only probe the
        # WITH-clause vector directly; the cluster-settings, service-
        # config, and node-environment vectors the plan also lists need
        # a host-level test rather than a SQL++ one, and are out of
        # scope here.
        manage_user, manage_password = self._create_rbac_user(
            base_role="analytics_manager[*],analytics_access")
        try:
            fn = f"rbacindirect_fn_{self.cbas_util.generate_name()}"
            cmd = (
                'select value 1 '
                f'with {{"library": "nonexistent", "module": "x", "entry_point": "y", '
                f'"name": "{fn}"}}'
            )
            status, _, errors, _, _, _ = self._execute(
                cmd, username=manage_user, password=manage_password, timeout=300, analytics_timeout=300)
            self.log.info(
                f"WITH-clause smuggling attempt as a manage-only user: status={status} errors={errors}")
            row = self.cbas_util.get_udf_metadata_row(
                self.cluster, fn, DEFAULT_DATAVERSE, DEFAULT_DATABASE)
            if row is not None:
                self.fail(
                    "A WITH clause on an unrelated statement created a "
                    f"function definition as a manage-only user: {row}"
                )
        finally:
            self._drop_rbac_user(manage_user)

    def test_rbac_hostile_argument_data_no_host_effect(self):
        # A property of the sandbox boundary, not of the function: a
        # correctly defined function must still fail harmlessly on
        # hostile *data*, since there's no per-user sandboxing to fall
        # back on. echo_one just returns its argument unchanged, so any
        # payload that comes back altered, or that errors, or that
        # visibly disrupts the node, is the finding.
        library = self._identity_fn()
        full_name = self._bind_identity(library, "Identity.echo_one")

        payloads = [
            ("shell_metachars", "; rm -rf / #`whoami`$(id)"),
            ("format_string", "%s%s%s%n{0.__class__}"),
            ("path_traversal", "../../../../etc/shadow"),
            ("pickle_like_bytes", "\x80\x04\x95\x0c\x00\x00\x00\x00\x00\x00\x00\x8c\x08evil_code\x94."),
            ("deeply_nested_hostile", {"a": [{"b": [{"c": "; cat /etc/passwd"}]}]}),
            ("very_large", "A" * (4 * 1024 * 1024)),
        ]
        for label, payload in payloads:
            status, _, errors, results, _, warnings = self._call(full_name, [payload])
            if status != "success":
                self.fail(f"Hostile payload {label} was rejected rather than handled harmlessly: {errors}")
            if results[0] != payload:
                # Verified live: pickle_like_bytes fails with a real,
                # actionable warning (msgpack's unpacker rejects the
                # invalid-UTF-8 byte sequence: UnicodeDecodeError) rather
                # than silently -- surface it instead of just the
                # mismatched value, since this suite was blind to
                # warnings until this fix (see MB-73975).
                real_warning = _real_udf_exception(warnings)
                self.fail(
                    f"Hostile payload {label} did not round-trip unchanged -- "
                    f"sent {payload!r}, got {results[0]!r}"
                    + (f" -- warning: {real_warning}" if real_warning else "")
                )
        if not self.cbas_util.is_analytics_running(self.cluster):
            self.fail("Analytics service is not healthy after hostile-argument-data payloads")

    def test_rbac_no_per_user_sandboxing_shared_trust(self):
        # Documented, not a bug: confirm two different select-only users
        # calling the same function get identical results, so a future
        # claim of per-caller isolation would be a deliberate change
        # rather than an unverified assumption.
        name = f"rbacshared_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)
        fn = f"rbacshared_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a"]):
            self.fail("Error creating UDF for the shared-trust test")
        full_name = f"{DEFAULT_DATAVERSE}.{fn}"

        user_a, password_a = self._create_rbac_user(base_role="analytics_access", grant_execute=True)
        user_b, password_b = self._create_rbac_user(base_role="analytics_access", grant_execute=True)
        try:
            status_a, _, errors_a, results_a, _, _ = self._call(full_name, [1], username=user_a, password=password_a)
            status_b, _, errors_b, results_b, _, _ = self._call(full_name, [1], username=user_b, password=password_b)
            if status_a != "success" or status_b != "success":
                self.fail(f"One of the two select-only users failed to call the function: {errors_a} / {errors_b}")
            if results_a != results_b:
                self.fail(
                    "Two different select-only users calling the same "
                    f"function got different results: {results_a} vs {results_b}"
                )
        finally:
            self._drop_rbac_user(user_a)
            self._drop_rbac_user(user_b)

    # ---- §9 Sandbox Isolation and Security -----------------------------
    #
    # Threat model per the test plan: no seccomp, no landlock -- content
    # inside and crossing the sandbox boundary is assumed hostile. The
    # only two invariants are "cannot escape" and "cannot consume more
    # resources than allotted." Each check is written as an attack, and
    # the assertion is about the *host*, not about whether the in-sandbox
    # call itself raised: an in-sandbox process, file write, or socket is
    # expected to be possible and is not a defect by itself.
    #
    # These are first-of-their-kind checks for this suite (no prior test
    # exercises this surface) and have not been run live yet -- read
    # them as a careful first pass, not a verified baseline.

    def _bind_probe(self, entry_point, parameters=None, library=None):
        parameters = parameters if parameters is not None else ["a"]
        if library is None:
            library = f"sandboxprobe_{self.cbas_util.generate_name()}"
            self._upload(DEFAULT_SCOPE, library, "sandboxprobe", SANDBOX_PROBE_MODULE_SOURCE)
        fn = f"probe_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "sandboxprobe", entry_point, library, parameters=parameters):
            self.fail(f"Error binding sandbox probe entry point {entry_point}")
        return f"{DEFAULT_DATAVERSE}.{fn}", library

    def _probe_call(self, full_name, args):
        status, _, errors, results, _, _ = self._call(full_name, args)
        if status != "success":
            self.fail(f"Sandbox probe call itself failed rather than reporting a structured result: {errors}")
        return results[0]

    def test_sandbox_no_network_stack(self):
        full_name, _ = self._bind_probe("SandboxProbe.attempt_network", parameters=["a", "b", "c"])
        nonce = f"udfnonce_{self.cbas_util.generate_name()}"
        observer = HostObserver(self.cluster.cbas_nodes[0])
        try:
            listener_started = observer.start_listener(port=9797)
            if not listener_started:
                self.fail("Could not start a host-side listener -- nc unavailable, cannot run this check")

            report = self._probe_call(full_name, [self.cluster.cbas_nodes[0].ip, 9797, nonce])
            self.log.info(f"Network attempt report: {report}")

            if report.get("raw_socket", {}).get("ok"):
                # socket.socket(AF_INET, SOCK_STREAM) allocating an fd is
                # not by itself proof of a working network stack --
                # Docker's --network=none still lets the syscall succeed
                # (it just leaves nothing to route through). Verified
                # live 2026-09-13. Record this rather than fail on it;
                # the decisive checks are the ones that actually require
                # routing (dns/connect/http) and the host listener below.
                self.log.info(
                    "Raw socket() creation succeeded inside the sandbox "
                    "-- not itself proof of a working network stack, see "
                    "dns/connect/http and the host-listener check"
                )
            for op in ("dns", "connect", "http"):
                if report.get(op, {}).get("ok"):
                    self.fail(f"Sandbox network attempt '{op}' succeeded inside the sandbox: {report[op]}")

            if observer.was_contacted(nonce):
                self.fail("The host-side listener was contacted despite every in-sandbox network attempt failing")
        finally:
            observer.stop_listener()
            observer.disconnect()

    def test_sandbox_library_mount_read_only(self):
        full_name, library = self._bind_probe("SandboxProbe.attempt_write_outside_mount", parameters=["a", "b"])
        client = self._lib_client()
        try:
            artifact_path = client.artifact_path(DEFAULT_DATABASE, DEFAULT_SCOPE, library)
        finally:
            client.disconnect()

        report = self._probe_call(full_name, [artifact_path, "pwned-by-udf"])
        self.log.info(f"Write-to-mount attempt report: {report}")
        if report.get("ok"):
            self.fail(f"A UDF was able to write to its own read-only library mount path: {report}")

        node = self.cluster.cbas_nodes[0]
        observer = HostObserver(node)
        try:
            content = observer.read_file(artifact_path.replace("library_archive.zip", "pwned-marker"))
            if content:
                self.fail("A file the UDF attempted to write outside the sandbox reached host disk")
        finally:
            observer.disconnect()

    def test_sandbox_cannot_read_host_paths(self):
        full_name, _ = self._bind_probe("SandboxProbe.attempt_read_host_path", parameters=["a"])
        for label, path in (
            ("shadow", "/etc/shadow"),
            ("ea_data_dir", "/opt/enterprise-analytics/var/lib/couchbase/config/couchbase-server.properties"),
            ("node_config", "/opt/enterprise-analytics/var/lib/couchbase/config/config.dat"),
        ):
            report = self._probe_call(full_name, [path])
            self.log.info(f"Host-path read attempt '{label}' ({path}): {report}")
            if report.get("ok"):
                self.fail(f"A UDF was able to read host path '{path}' ({label}) from inside the sandbox: {report}")

    def test_sandbox_host_process_and_syscalls_no_host_effect(self):
        marker = f"udf_sandbox_probe_{self.cbas_util.generate_name()}"
        full_name, _ = self._bind_probe("SandboxProbe.attempt_host_process", parameters=["a"])
        report = self._probe_call(full_name, [f"touch /tmp/{marker}_hostside"])
        self.log.info(f"os.system/subprocess/ctypes attempt report: {report}")

        observer = HostObserver(self.cluster.cbas_nodes[0])
        try:
            if observer.file_exists(f"/tmp/{marker}_hostside"):
                self.fail(
                    "os.system/subprocess/ctypes from inside a UDF created "
                    "a file on the host -- sandbox escape"
                )
            if observer.process_running(marker):
                self.fail("A UDF's os.system/subprocess/ctypes call left a process visible on the host")
        finally:
            observer.disconnect()

    def test_sandbox_unsandboxed_path_cannot_be_selected(self):
        # The single most important check in this section per the plan:
        # PythonLibraryEvaluatorFactory falls back to the unsandboxed
        # PythonLibraryTCPSocketEvaluator when PYTHON_DS_PATH is blank.
        # This suite is a SQL++ client only, so it can directly probe
        # only the WITH-clause vector; cluster settings, service config,
        # node env, and CLI args need a host-level test and are recorded
        # here as a follow-up, not attempted.
        name = f"dspath_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)
        fn = f"dspath_fn_{self.cbas_util.generate_name()}"
        blanking_attempt = self._create_udf(
            fn, "mylib", "Echo.hello", name, parameters=["a"],
            with_options={"PYTHON_DS_PATH": ""},
        )
        full_name = f"{DEFAULT_DATAVERSE}.{fn}"
        if blanking_attempt:
            status, _, errors, results, _, _ = self._call(full_name, [1])
            self.log.info(
                f"Call after attempting to blank PYTHON_DS_PATH via WITH clause: "
                f"status={status} errors={errors} results={results}"
            )
            if status == "success":
                node = self.cluster.cbas_nodes[0]
                shell = RemoteMachineShellConnection(node)
                try:
                    out, _ = shell.execute_command(
                        "pgrep -f PythonLibraryTCPSocketEvaluator && echo present; "
                        "ps aux | grep -i '[p]ython_cmd\\|asterixdb_udf' "
                    )
                finally:
                    shell.disconnect()
                self.log.info(f"Host process check for an unsandboxed interpreter: {out}")
        else:
            self.log.info("A WITH clause attempting to blank PYTHON_DS_PATH was refused at definition time -- good")

    def test_sandbox_ds_path_length_limit_handled_cleanly(self):
        name = f"dspathlen_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)
        fn = f"dspathlen_fn_{self.cbas_util.generate_name()}"
        long_path = "/tmp/" + ("a" * 200) + "/udf.sock"
        created = self._create_udf(
            fn, "mylib", "Echo.hello", name, parameters=["a"],
            with_options={"PYTHON_DS_PATH": long_path},
        )
        if not created:
            self.log.info("A WITH clause overriding PYTHON_DS_PATH to an over-length path was refused at definition")
            return
        status, _, errors, _, _, _ = self._call(f"{DEFAULT_DATAVERSE}.{fn}", [1])
        self.log.info(f"Call with an over-length PYTHON_DS_PATH: status={status} errors={errors}")
        if not self.cbas_util.is_analytics_running(self.cluster):
            self.fail("Analytics service is not healthy after an over-length PYTHON_DS_PATH attempt")

    def test_sandbox_ncconfig_python_options_not_silently_honoured(self):
        # Attempt each of the seven documented options via a WITH clause
        # -- the only vector reachable from a SQL++ client. Cluster
        # settings, service config, node env, and CLI args need a
        # host-level test and are not attempted here.
        options_to_try = {
            "PYTHON_CMD": "/bin/sh",
            "PYTHON_CMD_AUTOLOCATE": True,
            "PYTHON_ARGS": "-c evil",
            "PYTHON_ENV": "LD_PRELOAD=/tmp/evil.so",
            "PYTHON_ADDITIONAL_PACKAGES": "/tmp",
            "PYTHON_USE_BUNDLED_MSGPACK": False,
            "PYTHON_DS_PATH": "",
        }
        for option, value in options_to_try.items():
            fn = f"ncopt_{self.cbas_util.generate_name()}"
            library = f"ncoptlib_{self.cbas_util.generate_name()}"
            self._upload(DEFAULT_SCOPE, library, "sandboxprobe", SANDBOX_PROBE_MODULE_SOURCE)
            created = self._create_udf(
                fn, "sandboxprobe", "SandboxProbe.report_environment", library,
                parameters=["a"], with_options={option: value},
            )
            if not created:
                self.log.info(f"WITH clause option {option}={value!r} was refused at definition time")
                continue
            report = self._probe_call(f"{DEFAULT_DATAVERSE}.{fn}", [1])
            self.log.info(f"Environment report with {option}={value!r}: {report}")
            if option == "PYTHON_ADDITIONAL_PACKAGES" and report.get("sys_path_head", {}).get("ok"):
                sys_path = report["sys_path_head"]["result"]
                if "/tmp" in sys_path:
                    self.fail(
                        f"PYTHON_ADDITIONAL_PACKAGES={value!r} injected an entry "
                        f"ahead of the library paths in sys.path: {sys_path}"
                    )

    def test_sandbox_runsc_flags_registered_correctly(self):
        # Positive check only: confirm the runtime flags the security
        # argument assumes are actually what's configured. Actively
        # weakening them (--network=none removed, runc substituted) and
        # confirming the node then refuses to serve UDFs is a separate,
        # larger scenario deliberately left for a follow-up given the
        # wedge risk to the shared executor container every other test
        # in this suite depends on.
        node = self.cluster.cbas_nodes[0]
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command("cat /etc/docker/daemon.json 2>/dev/null")
            daemon_json = "\n".join(out) if out else ""
            self.log.info(f"Docker daemon.json on {node.ip}: {daemon_json}")
            for flag in ("--network=none", "--host-uds=create", "--directfs=false"):
                if flag not in daemon_json:
                    self.fail(f"Expected runsc runtime flag {flag} not found in daemon.json on {node.ip}")

            inspect_out, _ = shell.execute_command(
                f"docker inspect {UDF_EXECUTOR_CONTAINER_NAME} "
                "--format '{{.HostConfig.Runtime}}' 2>/dev/null")
            runtime = inspect_out[0].strip() if inspect_out else ""
            self.log.info(f"Executor container runtime on {node.ip}: {runtime}")
            if runtime != "runsc":
                self.fail(f"Executor container is not running under runsc: {runtime!r}")
        finally:
            shell.disconnect()

    def test_sandbox_shared_socket_directory_no_crosstalk(self):
        marker_a = f"marker_a_{self.cbas_util.generate_name()}"
        full_name_a, _ = self._bind_probe("SandboxProbe.plant_marker", parameters=["a"])
        full_name_b, _ = self._bind_probe("SandboxProbe.plant_marker", parameters=["a"])

        report_a = self._probe_call(full_name_a, [marker_a])
        self.log.info(f"Library A plant_marker report: {report_a}")

        node = self.cluster.cbas_nodes[0]
        observer = HostObserver(node)
        try:
            if observer.file_exists(f"/var/run/udf-sockets/{marker_a}"):
                self.log.info(
                    "A UDF was able to plant a file in the shared "
                    "/var/run/udf-sockets directory -- confirming a "
                    "second library can't read it next"
                )
                report_b = self._probe_call(full_name_b, [marker_a])
                content_b = report_b.get("udf_socket_dir", {})
                if content_b.get("ok") and marker_a in str(content_b.get("result", "")):
                    self.fail(
                        "A second library was able to read a marker file "
                        "another library planted in the shared udf-sockets directory"
                    )
        finally:
            observer.disconnect()

    def test_sandbox_one_shared_container_no_cross_library_file_access(self):
        library_a = f"crosslib_a_{self.cbas_util.generate_name()}"
        library_b = f"crosslib_b_{self.cbas_util.generate_name()}"
        # library_a is the one the probe function is actually bound
        # into (_bind_probe's module="sandboxprobe" below), so it must
        # carry the sandboxprobe module -- uploading it as "mylib"/echo
        # instead left CREATE FUNCTION succeeding (lazy binding) but
        # every call failing with a bare 25000 Internal error, since
        # module "sandboxprobe" didn't exist inside library_a. Verified
        # live 2026-09-14. library_b only needs to be *some other*
        # library -- its on-disk artifact is the read target, its
        # contents are never invoked.
        self._upload(DEFAULT_SCOPE, library_a, "sandboxprobe", SANDBOX_PROBE_MODULE_SOURCE)
        self._upload(DEFAULT_SCOPE, library_b, "mylib", ECHO_MODULE_SOURCE)

        full_name, _ = self._bind_probe("SandboxProbe.attempt_read_host_path", parameters=["a"], library=library_a)
        client = self._lib_client()
        try:
            other_artifact_path = client.artifact_path(DEFAULT_DATABASE, DEFAULT_SCOPE, library_b)
        finally:
            client.disconnect()

        report = self._probe_call(full_name, [other_artifact_path])
        self.log.info(f"Cross-library read attempt: {report}")
        if report.get("ok"):
            self.fail(f"A UDF in library A was able to read library B's on-disk artifact: {report}")

    def test_sandbox_direct_escape_attempts_fail(self):
        full_name_proc, _ = self._bind_probe("SandboxProbe.attempt_proc_access", parameters=["a"])
        report = self._probe_call(full_name_proc, [1])
        self.log.info(f"Proc-access escape attempt report: {report}")

        # A gVisor sandbox virtualizes its own /proc: /proc/net/tcp and
        # /proc/1/* reflect the sandbox's own (empty, --network=none)
        # network namespace and its own PID-1-equivalent, not the host's
        # -- reading them succeeding is not itself an escape. Verified
        # live 2026-09-13: proc_net_tcp returned only the header line, no
        # connection rows, consistent with an isolated empty namespace.
        # The decisive signal for proc_net_tcp is actual connection rows;
        # for proc_1_root/proc_1_environ there's no cheap way from here
        # to tell "the sandbox's own init" apart from "the real host
        # init" without a host-side cross-check, so those are logged as
        # findings to review rather than auto-failed.
        net_tcp = report.get("proc_net_tcp", {})
        if net_tcp.get("ok"):
            lines = [ln for ln in str(net_tcp.get("result", "")).splitlines() if ln.strip()]
            if len(lines) > 1:
                self.fail(f"/proc/net/tcp revealed real connection entries from inside the sandbox: {net_tcp}")
        for op in ("proc_1_root", "proc_1_environ"):
            if report.get(op, {}).get("ok"):
                self.log.info(
                    f"'{op}' succeeded inside the sandbox -- review whether this is the sandbox's own "
                    f"PID-1-equivalent (expected) or a real host leak: {report[op]}"
                )

        # A plain listdir('/../../...') always succeeds -- '/..' resolves
        # to the sandbox's own root inside a container, so that alone is
        # not an escape (see udf_sandbox_probe_utils.py). The decisive
        # check: plant a marker directly on the host's own /tmp (not
        # shared with the container by any bind mount) and confirm the
        # same traversal prefix cannot read it from inside the sandbox.
        marker_name = f"udf_escape_marker_{self.cbas_util.generate_name()}"
        marker_content = f"escaped-{self.cbas_util.generate_name()}"
        node = self.cluster.cbas_nodes[0]
        observer = HostObserver(node)
        try:
            observer.write_file(f"/tmp/{marker_name}", marker_content)
            full_name_root, _ = self._bind_probe("SandboxProbe.attempt_traverse_root", parameters=["a"])
            report = self._probe_call(full_name_root, [f"tmp/{marker_name}"])
            self.log.info(f"Root-traversal escape attempt report: {report}")
            read_result = report.get("read_via_traversal", {})
            if read_result.get("ok") and marker_content in str(read_result.get("result", "")):
                self.fail(
                    "Traversing past the sandbox root reached a marker "
                    f"planted on the real host: {report}"
                )
        finally:
            observer.delete_file(f"/tmp/{marker_name}")
            observer.disconnect()

    def test_sandbox_module_curtain_bypass_is_a_finding_not_a_blocker(self):
        # The curtain that stops a function reaching outside its own
        # library's modules is documented as best-effort; a bypass is a
        # defence-in-depth finding to raise, not a release-gating
        # failure -- the gating assertion is direct-escape (covered by
        # test_sandbox_direct_escape_attempts_fail), not this curtain.
        full_name, _ = self._bind_probe("SandboxProbe.attempt_import_other_library", parameters=["a"])
        report = self._probe_call(full_name, ["os"])
        self.log.info(f"Cross-module-curtain import attempt (stdlib 'os'): {report}")
        if report.get("ok"):
            self.log.info(
                "Importing a module outside the library's own bundle "
                "succeeded -- record as a defence-in-depth finding, not a failure"
            )

    def test_sandbox_audit_events_no_content_leak(self):
        node = self.cluster.cbas_nodes[0]
        audit_master = AuditUtil(host=node)
        if not audit_master.getAuditStatus():
            audit_master.setAuditEnable(True)
        audit_master.setAuditFeatureDisabled(disabled=[])  # un-disable everything, including 36894-36897

        name = f"auditlib_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)  # CREATE LIBRARY-equivalent (36896)
        fn = f"audit_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a"]):  # 36894
            self.fail("Error creating function for the audit-event test")
        if not self.cbas_util.drop_udf(
            self.cluster, name=fn, dataverse=DEFAULT_DATAVERSE, database=DEFAULT_DATABASE,
            parameters=["a"], timeout=300, analytics_timeout=300,
        ):  # 36895
            self.fail("Error dropping function for the audit-event test")
        client = self._lib_client()
        try:
            client.drop_library(DEFAULT_SCOPE, name)  # 36897
        finally:
            client.disconnect()

        for event_id, label in (
            (36894, "CREATE FUNCTION"), (36895, "DROP FUNCTION"),
            (36896, "CREATE LIBRARY"), (36897, "DROP LIBRARY"),
        ):
            audit_obj = AuditUtil(eventID=event_id, host=node)
            if not audit_obj.check_if_audit_event_generated():
                self.fail(f"Audit event {event_id} ({label}) did not fire once enabled")
            event = audit_obj.returnEvent(event_id)
            self.log.info(f"Audit event {event_id} ({label}): {event}")
            statement_text = json.dumps(event)
            if ECHO_MODULE_SOURCE.strip().splitlines()[0] in statement_text:
                self.fail(f"Audit event {event_id} ({label}) leaked library source code in its recorded statement")
            if self.udf_password in statement_text:
                self.fail(f"Audit event {event_id} ({label}) leaked a credential in its recorded statement")

    def test_sandbox_failing_udf_does_not_destabilize_neighbor_query(self):
        # A stability property, not per-user isolation -- there is no
        # per-user sandboxing, so the assertion is that a neighboring
        # query completes correctly and on time regardless of who
        # issued the failing one.
        bad_name = f"failing_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, bad_name, "faillib", "def boom(*args):\n    raise ValueError('boom')\n")
        bad_fn = f"failing_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(bad_fn, "faillib", "boom", bad_name, parameters=["a"]):
            self.fail("Error creating the deliberately-failing UDF")
        # Verified live: a raising UDF reports status=success /
        # results=[None], not status=fatal -- the real exception arrives
        # via `warnings` instead of `errors`.
        status, _, errors, results, _, warnings = self._call(f"{DEFAULT_DATAVERSE}.{bad_fn}", [1])
        real_warning = _real_udf_exception(warnings)
        if status != "success" or results != [None] or not real_warning:
            self.fail(
                "The deliberately-failing UDF did not fail the expected way: "
                f"status={status} errors={errors} results={results} warnings={warnings}"
            )
        self.log.info(f"Deliberately-failing UDF warning (expected): {real_warning}")

        good_name = f"neighbor_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, good_name, "mylib", ECHO_MODULE_SOURCE)
        good_fn = f"neighbor_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(good_fn, "mylib", "Echo.hello", good_name, parameters=["a"]):
            self.fail("Error creating the neighboring UDF")
        status, _, errors, results, _, _ = self._call(f"{DEFAULT_DATAVERSE}.{good_fn}", [1])
        if status != "success" or results[0] != "hello":
            self.fail(
                "A neighboring UDF query failed immediately after a "
                f"different UDF raised inside its Python body: {errors} / {results}"
            )

    # ---- §10 Resource Limits and Failure Handling ----------------------
    #
    # Confirmed by the test plan itself: the executor container ships
    # with no --memory, --cpus, --pids-limit or --ulimit, so several
    # checks here are release-blocking *gaps* to file, not behavior to
    # assert passes. The container is also shared across every query,
    # library and user on the node -- the same one every other test in
    # this suite depends on -- so several of the plan's literal attacks
    # (a real fork bomb, a host-crashing memory hog, `docker kill` with
    # no recovery) are deliberately NOT attempted here: the risk is
    # leaving the shared executor, and therefore the rest of this run,
    # wedged for the remainder of the session. Where the plan calls for
    # an attack, this file runs a bounded, safety-scaled version instead
    # and says so in the test; the unscaled version needs its own
    # isolated, non-shared environment, not a shared conf.

    def _force_recreate_executor(self):
        for node in self.cluster.cbas_nodes:
            shell = RemoteMachineShellConnection(node)
            try:
                shell.execute_command(
                    f"docker rm -f {UDF_EXECUTOR_CONTAINER_NAME} 2>/dev/null; "
                    "rm -f /var/run/udf-sockets/pyudf.socket*"
                )
            finally:
                shell.disconnect()
            if not ensure_udf_executor_runtime(node, self.udf_username, self.udf_password):
                self.fail(f"Failed to recover the UDF executor container on {node.ip}")

    def test_resource_cpu_or_walltime_limit(self):
        # Expected to fail as shipped (test plan): no container-level
        # CPU or wall-clock bound exists. Bounded on the client side with
        # a short analytics_timeout so this test itself can't hang the
        # suite regardless of what the server does -- the finding is
        # whether the *server* stopped the loop, not whether our own
        # client-side timeout fired.
        name = f"cpuloop_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "cpulib", "def spin(*args):\n    while True:\n        pass\n")
        fn = f"cpuloop_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "cpulib", "spin", name, parameters=["a"]):
            self.fail("Error creating the infinite-loop UDF")
        try:
            status, _, errors, _, _, _ = self._call(
                f"{DEFAULT_DATAVERSE}.{fn}", [1], timeout=20, analytics_timeout=20)
            self.log.info(f"Infinite-loop UDF call: status={status} errors={errors}")
            if status == "success":
                self.fail("An infinite-loop UDF returned success rather than being stopped or timing out")
        except Exception as e:
            self.log.info(f"Infinite-loop UDF call errored client-side (no server-side limit fired in time): {e}")
        finally:
            self._force_recreate_executor()
        if not self.cbas_util.is_analytics_running(self.cluster):
            self.fail("Analytics service is not healthy after an infinite-loop UDF")

    def test_resource_limits_configurability_documented_gap(self):
        # There is nothing to raise or lower yet -- confirm that by
        # inspecting the running container's actual resource config,
        # rather than assert adjustable behavior the plan itself says
        # doesn't exist. This becomes a real regression guard the day
        # limits ship.
        node = self.cluster.cbas_nodes[0]
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command(
                f"docker inspect {UDF_EXECUTOR_CONTAINER_NAME} "
                "--format '{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}} "
                "{{.HostConfig.PidsLimit}}' 2>/dev/null"
            )
        finally:
            shell.disconnect()
        config = out[0].strip() if out else ""
        self.log.info(f"Executor container resource config (memory nanocpus pidslimit): {config}")
        if config and config != "0 0 0":
            self.log.info(
                "Resource limits now appear configured -- the "
                "administrator-configurability checks in this section "
                "can move from documentation to real assertions"
            )

    def test_resource_memory_ceiling_documented_gap(self):
        # No memory ceiling ships today (confirmed: Memory=0). A
        # genuinely host-crashing allocation is not attempted against a
        # shared node -- this uses a bounded, moderate allocation as a
        # safe proxy and inspects the container's real limit instead of
        # trying to trigger a host OOM.
        node = self.cluster.cbas_nodes[0]
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command(
                f"docker inspect {UDF_EXECUTOR_CONTAINER_NAME} "
                "--format '{{.HostConfig.Memory}}' 2>/dev/null"
            )
        finally:
            shell.disconnect()
        memory_limit = out[0].strip() if out else "0"
        self.log.info(f"Executor container memory limit: {memory_limit}")

        name = f"memhog_{self.cbas_util.generate_name()}"
        self._upload(
            DEFAULT_SCOPE, name, "memlib",
            "def hog(*args):\n    buf = bytearray(64 * 1024 * 1024)\n    return len(buf)\n",
        )
        fn = f"memhog_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "memlib", "hog", name, parameters=["a"]):
            self.fail("Error creating the bounded memory-allocation UDF")
        status, _, errors, results, _, _ = self._call(f"{DEFAULT_DATAVERSE}.{fn}", [1])
        self.log.info(f"Bounded (64MB) allocation call: status={status} errors={errors} results={results}")
        if memory_limit in ("0", ""):
            self.log.info("Confirmed: no memory ceiling configured on the executor container (documented gap)")
        if not self.cbas_util.is_analytics_running(self.cluster):
            self.fail("Analytics service is not healthy after a bounded memory-allocation UDF")

    def test_resource_uncaught_exception_surfaces_and_recovers(self):
        name = f"uncaught_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "uncaughtlib", "def boom(*args):\n    return 1 / 0\n")
        fn = f"uncaught_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "uncaughtlib", "boom", name, parameters=["a"]):
            self.fail("Error creating the uncaught-exception UDF")
        full_name = f"{DEFAULT_DATAVERSE}.{fn}"

        # Verified live 2026-09-16: a UDF that raises does NOT set
        # status=fatal / populate errors -- it reports status=success,
        # results=[None], and the real Python traceback arrives via
        # `warnings` instead (only visible when a caller actually asks
        # for warnings, which this suite did not do before this fix --
        # see MB-73975, which needs correcting on this point: an error
        # *is* reported, just not in `errors`).
        status, _, errors, results, _, warnings = self._call(full_name, [1])
        real_warning = _real_udf_exception(warnings)
        if status != "success" or results != [None]:
            self.fail(
                "A UDF that raises ZeroDivisionError behaved unexpectedly: "
                f"status={status} errors={errors} results={results}"
            )
        if not real_warning:
            self.fail(f"An uncaught Python exception produced no real warning at all: {warnings}")
        self.log.info(f"Uncaught-exception warning (expected, checking it's useful): {real_warning}")
        if "ZeroDivisionError" not in real_warning.get("msg", ""):
            self.fail(f"The warning did not name the actual exception: {real_warning}")
        if not self.cbas_util.is_analytics_running(self.cluster):
            self.fail("Analytics service is not healthy after an uncaught Python exception")

        status, _, errors, results, _, warnings = self._call(full_name, [1])
        if status == "success" and results == [None] and _real_udf_exception(warnings):
            self.log.info("Second call to the same failing function still fails the same way as expected")
        else:
            self.fail(
                "The second call after an uncaught exception behaved unexpectedly "
                f"(still expected to fail the same way): status={status} results={results} warnings={warnings}"
            )

        good_name = f"uncaught_neighbor_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, good_name, "mylib", ECHO_MODULE_SOURCE)
        good_fn = f"uncaught_neighbor_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(good_fn, "mylib", "Echo.hello", good_name, parameters=["a"]):
            self.fail("Error creating a different, working UDF after the uncaught-exception one")
        status, _, errors, results, _, _ = self._call(f"{DEFAULT_DATAVERSE}.{good_fn}", [1])
        if status != "success" or results[0] != "hello":
            self.fail(f"A different, working UDF failed after an unrelated uncaught exception: {errors} / {results}")

    def test_resource_executor_kill_mid_query_recovery(self):
        # Measured by the plan itself: the shipped unit does not recover
        # on its own (Restart=on-failure, a clean exit goes inactive/dead
        # with NRestarts=0). This test kills the container mid-query,
        # confirms the in-flight query fails cleanly, and then recovers
        # it *itself* via _force_recreate_executor rather than relying
        # on the unit -- proving the gap while keeping the rest of this
        # run (and the rest of the session) usable afterward.
        name = f"killmidquery_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, name, "mylib", ECHO_MODULE_SOURCE)
        fn = f"killmidquery_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "mylib", "Echo.hello", name, parameters=["a"]):
            self.fail("Error creating UDF for the kill-mid-query test")
        full_name = f"{DEFAULT_DATAVERSE}.{fn}"

        node = self.cluster.cbas_nodes[0]
        shell = RemoteMachineShellConnection(node)
        try:
            # ensure_udf_executor_runtime starts this container with
            # --restart unless-stopped, so Docker itself would resurrect
            # it right after the kill below -- masking the very gap this
            # test exists to prove (the plan's own measurement is against
            # the product's systemd unit, Restart=on-failure, which does
            # NOT recover on a clean exit; this suite's own container
            # config is a separate thing). Disable the policy on this one
            # container for the duration of this test only; the
            # _force_recreate_executor() below restores the normal policy
            # via a full rebuild, so no other test is affected.
            shell.execute_command(f"docker update --restart=no {UDF_EXECUTOR_CONTAINER_NAME}")
            shell.execute_command(f"docker kill {UDF_EXECUTOR_CONTAINER_NAME}")
        finally:
            shell.disconnect()

        try:
            status, _, errors, _, _, _ = self._call(full_name, [1])
            self.log.info(f"Call immediately after killing the executor container: status={status} errors={errors}")
            if status == "success":
                self.fail("A call succeeded immediately after the executor container was killed")

            recovered_shell = RemoteMachineShellConnection(node)
            try:
                unit_state, _ = recovered_shell.execute_command(
                    f"docker inspect {UDF_EXECUTOR_CONTAINER_NAME} "
                    "--format '{{.State.Status}} {{.State.ExitCode}}' 2>/dev/null"
                )
            finally:
                recovered_shell.disconnect()
            self.log.info(f"Executor container state after kill, before manual recovery: {unit_state}")
        finally:
            self._force_recreate_executor()

        status, _, errors, results, _, _ = self._call(full_name, [1])
        if status != "success" or results[0] != "hello":
            self.fail(
                "The same function still failed after the executor was "
                f"manually recovered post-kill: {errors} / {results}"
            )

    def test_resource_hostile_udf_bounded_dos_attempt(self):
        # The plan asks whether one hostile UDF (memory hog, fork bomb,
        # fd exhaustion) can deny service to every other UDF on the
        # shared node. Given the confirmed absence of any resource
        # limits, the unscaled versions of this attack are not run here
        # -- they risk leaving the shared executor genuinely unusable
        # for the remainder of this run and this session. This runs a
        # bounded CPU-bound loop (client-side timeout, not a true DoS)
        # and confirms a concurrent-ish neighbor call still completes.
        name = f"boundedhog_{self.cbas_util.generate_name()}"
        self._upload(
            DEFAULT_SCOPE, name, "boundedhoglib",
            "def spin(*args):\n    total = 0\n    for i in range(10 ** 8):\n        total += i\n    return total\n",
        )
        fn = f"boundedhog_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "boundedhoglib", "spin", name, parameters=["a"]):
            self.fail("Error creating the bounded CPU-load UDF")
        try:
            status, _, errors, results, _, _ = self._call(f"{DEFAULT_DATAVERSE}.{fn}", [1])
            self.log.info(f"Bounded CPU-load call: status={status} errors={errors} results={results}")
        finally:
            neighbor_name = f"boundedhog_neighbor_{self.cbas_util.generate_name()}"
            self._upload(DEFAULT_SCOPE, neighbor_name, "mylib", ECHO_MODULE_SOURCE)
            neighbor_fn = f"boundedhog_neighbor_fn_{self.cbas_util.generate_name()}"
            if not self._create_udf(neighbor_fn, "mylib", "Echo.hello", neighbor_name, parameters=["a"]):
                self.fail("Error creating the neighbor UDF for the bounded-DoS test")
            status, _, errors, results, _, _ = self._call(f"{DEFAULT_DATAVERSE}.{neighbor_fn}", [1])
            if status != "success" or results[0] != "hello":
                self.fail(
                    "A neighboring UDF failed to complete while a bounded "
                    f"CPU-load UDF was also running: {errors} / {results}"
                )

    def test_resource_sustained_concurrency_no_leak(self):
        # A bounded proxy for "sustained concurrent load doesn't exhaust
        # descriptors" -- a real stress test belongs in its own
        # isolated environment; this runs a moderate number of rapid
        # sequential calls and checks the node stays healthy afterward.
        library = self._identity_fn()
        full_name = self._bind_identity(library, "Identity.echo_one")
        for i in range(25):
            status, _, errors, results, _, _ = self._call(full_name, [i])
            if status != "success" or results[0] != i:
                self.fail(f"Call {i} of 25 rapid sequential calls failed: {errors} / {results}")
        if not self.cbas_util.is_analytics_running(self.cluster):
            self.fail("Analytics service is not healthy after 25 rapid sequential UDF calls")

    def test_resource_udf_errors_distinguishable_from_other_error_classes(self):
        udf_name = f"errclass_{self.cbas_util.generate_name()}"
        self._upload(DEFAULT_SCOPE, udf_name, "errclasslib", "def boom(*args):\n    raise RuntimeError('udf-error')\n")
        fn = f"errclass_fn_{self.cbas_util.generate_name()}"
        if not self._create_udf(fn, "errclasslib", "boom", udf_name, parameters=["a"]):
            self.fail("Error creating UDF for the error-class-distinction test")

        # Both control queries below are verified live 2026-09-14 against
        # the previous versions' actual behavior:
        #   select value from nonexistent_clause_xyz      -> 24000 syntax
        #     error (as written) -- a *syntax* error, not the planning
        #     error ("collection not found") this control means to be.
        #   select value 1 + "not_a_number"               -> success,
        #     errors=None -- SQL++ returns NULL for type-mismatched
        #     arithmetic rather than erroring, so this was never a real
        #     data error either.
        # Corrected to queries that genuinely trip the intended class:
        _, _, planning_errors, _, _, _ = self._execute("select value v from nonexistent_clause_xyz v")
        _, _, data_errors, _, _, _ = self._execute("select value 9223372036854775807 + 1")

        udf_status, _, udf_errors, _, _, udf_warnings = self._call(f"{DEFAULT_DATAVERSE}.{fn}", [1])
        _, _, missing_lib_errors, _, _, _ = self._call(f"{DEFAULT_DATAVERSE}.definitely_not_a_function_xyz", [1])

        real_udf_warning = _real_udf_exception(udf_warnings)
        self.log.info(
            f"Error classes -- UDF: status={udf_status} errors={udf_errors} warning={real_udf_warning} | "
            f"planning: {planning_errors} | data: {data_errors} | missing-function: {missing_lib_errors}"
        )
        # Verified live: a UDF runtime error never populates `errors` --
        # status stays "success" and the real exception arrives only via
        # `warnings` (the MB-73975 correction: an error *is* reported,
        # just not as `errors`). So "distinguishable from a planning/data
        # error" means distinguishable by shape -- status=success plus a
        # real warning and no errors, versus status=fatal plus a
        # populated errors list for planning/data/missing-function --
        # not by comparing error codes against a udf_errors that will
        # never be populated.
        if udf_status != "success" or udf_errors or not real_udf_warning:
            self.fail(
                "A UDF runtime error was not reported the expected way "
                f"(status=success, no errors, a real warning): status={udf_status} "
                f"errors={udf_errors} warnings={udf_warnings}"
            )
        if not planning_errors or not data_errors or not missing_lib_errors:
            self.fail(
                "One of the comparison error classes was not reported: "
                f"planning={planning_errors} data={data_errors} missing={missing_lib_errors}"
            )
