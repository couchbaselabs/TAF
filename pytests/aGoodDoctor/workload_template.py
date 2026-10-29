"""
Bucket workload templates for the on-prem volume tests in Hospital.py.

A template is a Python dict literal under pytests/aGoodDoctor/templates/,
keyed by bucket name. Each value has the shape of a workloads.py workload
dict. Passing the test param `workload_template=<name or path>` makes
Murphy:
  - create one bucket per key, named after the key;
  - name every collection after its collections_defn `collection_id`;
  - load, mutate and query each bucket with its own valType and rates.
Without the param, the single `val_type` workload is shared by every bucket
as before. Everything that differs between the two paths lives here.
Hospital.py, opd.py and n1ql.py only branch into it when a template is set.

Rates. A bucket or a collection can set either of these:
    "ops"            rate of the initial load
    "rebl_ops_rate"  rate of every load after it (mutations, steady state)
A bucket-level rate is split evenly across the bucket's collections. A
collection-level rate overrides that collection's share.

Template mode supports the file-driven vector datasets (siftBigANN and the
MSMARCO variants) with GSI query loads, in the _default scope only.
"""

import os
import re
import threading

from bucket_utils.bucket_ready_functions import CollectionUtils, JavaDocLoaderUtils
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader

from py_constants.cb_constants.CBServer import CbServer

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")

# Set on every template bucket's loadDefn, so code shared with the val_type
# path (n1ql.py) can tell, per bucket, which of the two it is serving.
TEMPLATE_KEY = "workload_template"

SIFT_VAL_TYPES = ("siftBigANN",)
MSMARCO_VAL_TYPES = ("MSMARCOEmbeddingProduct", "MSMARCOSiftEmbeddingProduct")
# Items per collection that DocLoader's dataset endpoints can serve. Past
# these it rejects the load request, and the collection stays empty.
MAX_ITEMS = {"siftBigANN": 1000000000, "MSMARCOEmbeddingProduct": 8841823, "MSMARCOSiftEmbeddingProduct": 8841823}

INITIAL_RATE = "ops"
MUTATION_RATE = "rebl_ops_rate"

DOC_OPS = ("create", "read", "update", "delete", "expiry")
CREATE_ONLY = {"create": 100, "read": 0, "update": 0, "delete": 0, "expiry": 0}
# Same fallback JavaDocLoaderUtils.load_sift_data / load_msmarco_data use
DEFAULT_PATTERN = {"create": 0, "update": 100, "delete": 0, "read": 0, "expiry": 0}
SIFT_URL = "ftp://ftp.irisa.fr/local/texmex/corpus/bigann_base.bvecs.gz"

# Bucket names and collection_ids end up in N1QL keyspaces, and both go into
# index names (<bucket>_idx_<collection>_<n>). These patterns keep every
# index name a plain identifier. n1ql.py escapes any keyspace part that is
# not one, e.g. a collection_id that starts with a digit like "8M".
BUCKET_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,99}$")
COLLECTION_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_]{0,250}$")


def resolve_template_path(name_or_path):
    """
    Accept a path to a template file, or the name of one in TEMPLATE_DIR,
    with or without the .py suffix.
    """
    candidates = [name_or_path]
    if not os.path.isabs(name_or_path):
        candidates.append(os.path.join(TEMPLATE_DIR, name_or_path))
        if not name_or_path.endswith(".py"):
            candidates.append(os.path.join(TEMPLATE_DIR, name_or_path + ".py"))
    for path in candidates:
        if os.path.isfile(path):
            return path
    raise ValueError(f"no template file at any of {candidates}")


def load_template(path):
    """
    The templates are dict literals that also use list comprehensions,
    e.g. "queryParams": [{} for _ in range(7)]. ast.literal_eval rejects
    those, so evaluate the file with nothing but range() reachable.
    """
    with open(path) as template_file:
        source = template_file.read()
    return eval(compile(source, path, "eval"), {"__builtins__": {"range": range}})


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def collection_rate(workload, defn, rate_key):
    """
    The rate (`rate_key`: INITIAL_RATE or MUTATION_RATE) one collection of a
    template bucket loads at. A collection-level value wins; otherwise the
    bucket-level value is split evenly across the bucket's collections.
    None when neither level sets it.
    """
    if defn.get(rate_key) is not None:
        return defn[rate_key]
    bucket_rate = workload.get(rate_key)
    if not _is_int(bucket_rate):
        # None (not set) or a bad value, which template_errors() reports
        return bucket_rate
    return bucket_rate // len(workload["collections_defn"])


def template_errors(template, process_concurrency):
    """
    Everything in `template` that would make the run fail later, or silently
    load or query less than it describes. Returns a list of messages; empty
    means the template is usable.
    """
    if not isinstance(template, dict) or not template:
        return ["a template is a non-empty dict of {bucket_name: workload}"]

    errors = []
    sift_base_paths = set()
    for bucket_name, workload in template.items():
        where = f"bucket {bucket_name}"
        if not isinstance(bucket_name, str) or not BUCKET_NAME_RE.match(bucket_name):
            errors.append(f"{where}: the name must match {BUCKET_NAME_RE.pattern}")
        if not isinstance(workload, dict):
            errors.append(f"{where}: the workload must be a dict")
            continue

        val_type = workload.get("valType")
        if val_type not in MAX_ITEMS:
            errors.append(f"{where}: valType {val_type!r} is not one of {sorted(MAX_ITEMS)}")
            continue

        for key in ("key_prefix", "key_type"):
            if not isinstance(workload.get(key), str):
                errors.append(f"{where}: {key} must be a string")
        if not _is_int(workload.get("key_size")) or workload["key_size"] < 1:
            errors.append(f"{where}: key_size must be a positive int")
        # create_template_collections() creates the collections in the
        # _default scope only
        if workload.get("scopes") != 1:
            errors.append(f"{where}: scopes must be 1, template collections live in the _default scope")
        pattern = workload.get("doc_op_percentages")
        if not isinstance(pattern, dict) or not all(_is_int(pattern.get(op)) for op in DOC_OPS):
            errors.append(f"{where}: doc_op_percentages must give an int for each of {list(DOC_OPS)}")
        path_key = "baseFilePath" if val_type in SIFT_VAL_TYPES else "vecFilePath"
        if not isinstance(workload.get(path_key), str):
            errors.append(f"{where}: {val_type} needs {path_key}")
        if val_type in SIFT_VAL_TYPES:
            sift_base_paths.add(workload.get("baseFilePath"))
        qps = workload.get("2iQPS", 0)
        if not _is_int(qps) or qps < 0:
            errors.append(f"{where}: 2iQPS must be an int >= 0")
            qps = 0

        defns = workload.get("collections_defn")
        if not isinstance(defns, list) or not defns:
            errors.append(f"{where}: collections_defn must be a non-empty list")
            continue
        if "collections" in workload and workload["collections"] != len(defns):
            errors.append(
                f"{where}: collections is {workload['collections']} but collections_defn has {len(defns)} entries"
            )
        for rate_key in (INITIAL_RATE, MUTATION_RATE):
            if workload.get(rate_key) is not None and not _is_int(workload[rate_key]):
                errors.append(f"{where}: {rate_key} must be an int")

        seen = set()
        num_queries_total = 0
        for idx, defn in enumerate(defns):
            if not isinstance(defn, dict):
                errors.append(f"{where}: collections_defn[{idx}] must be a dict")
                continue
            coll = defn.get("collection_id")
            c_where = f"{where}, collection " + (coll if isinstance(coll, str) else f"collections_defn[{idx}]")
            if not isinstance(coll, str) or not COLLECTION_NAME_RE.match(coll):
                errors.append(f"{c_where}: collection_id must match {COLLECTION_NAME_RE.pattern}")
            elif coll in seen:
                errors.append(f"{c_where}: collection_id is used twice")
            seen.add(coll)
            if defn.get("valType") != val_type:
                errors.append(f"{c_where}: valType {defn.get('valType')!r} differs from the bucket's {val_type!r}")
            items = defn.get("num_items")
            if not _is_int(items) or not 1 <= items <= MAX_ITEMS[val_type]:
                errors.append(f"{c_where}: num_items must be an int in [1, {MAX_ITEMS[val_type]}] for {val_type}")

            two_i = defn.get("2i")
            if (
                not isinstance(two_i, dict)
                or not _is_int(two_i.get("num_indexes"))
                or not _is_int(two_i.get("num_queries"))
                or two_i["num_indexes"] < 0
                or two_i["num_queries"] < 0
            ):
                errors.append(f"{c_where}: 2i must give int num_indexes and num_queries >= 0")
                num_indexes = num_queries = 0
            else:
                num_indexes = two_i["num_indexes"]
                num_queries = two_i["num_queries"]
            num_queries_total += num_queries
            for key, needed in (("indexes", num_indexes), ("queries", num_queries)):
                stmts = defn.get(key)
                if needed and (not isinstance(stmts, list) or not stmts or not all(isinstance(s, str) for s in stmts)):
                    errors.append(f"{c_where}: {key} must be a non-empty list of statements")
            vectors = defn.get("vector")
            if (
                not isinstance(vectors, list)
                or not vectors
                or not all(isinstance(v, dict) and _is_int(v.get("nProbe")) for v in vectors)
            ):
                errors.append(f"{c_where}: vector must be a non-empty list of dicts with an int nProbe")
            gts = defn.get("groundTruths")
            if gts is not None and (not isinstance(gts, list) or len(gts) < num_queries):
                errors.append(
                    f"{c_where}: groundTruths, when given, needs an entry for each of the {num_queries} queries"
                )
            query_params = defn.get("queryParams")
            if query_params is not None and (
                not isinstance(query_params, list) or not all(isinstance(p, dict) for p in query_params)
            ):
                errors.append(f"{c_where}: queryParams must be a list of dicts")

            for rate_key in (INITIAL_RATE, MUTATION_RATE):
                if defn.get(rate_key) is None and workload.get(rate_key) is None:
                    errors.append(f"{c_where}: no {rate_key} at bucket or collection level")
                    continue
                rate = collection_rate(workload, defn, rate_key)
                if not _is_int(rate):
                    # A bad bucket-level value is reported once, above
                    if defn.get(rate_key) is not None:
                        errors.append(f"{c_where}: {rate_key} must be an int")
                elif rate < process_concurrency:
                    # DocLoader paces each of its pc workers at ops // pc
                    # docs/s and loads nothing when that comes to 0
                    errors.append(
                        f"{c_where}: {rate_key} comes to {rate} ops/s for "
                        f"this collection, below pc={process_concurrency}; "
                        f"DocLoader would load nothing at that rate"
                    )

        if qps > 0 and num_queries_total == 0:
            errors.append(f"{where}: 2iQPS is {qps} but no collection has queries")

    # QueryLoad keeps one process-wide cache of the siftBigANN query vectors
    if len(sift_base_paths) > 1:
        errors.append(f"every siftBigANN bucket must use the same baseFilePath, got {sorted(sift_base_paths)}")
    return errors


def build_load_defns(template, template_name):
    """
    One loadDefn per bucket, in template order. Each is the template's own
    dict, so nothing is shared between buckets. Each is marked with its
    template name, given the bucket name that create_required_buckets() picks
    up, and has its collections in collection_id order.
    """
    load_defns = []
    for bucket_name, workload in template.items():
        workload["name"] = bucket_name
        workload[TEMPLATE_KEY] = template_name
        workload["collections_defn"] = sorted(workload["collections_defn"], key=lambda defn: defn["collection_id"])
        workload["collections"] = len(workload["collections_defn"])
        load_defns.append(workload)
    return load_defns


def template_collection_names(bucket):
    return [defn["collection_id"] for defn in bucket.loadDefn["collections_defn"]]


class WorkloadTemplateMixin:
    """
    Template-mode paths for Hospital.Murphy. Every method expects
    self.workload_template to be set, and the val_type paths never call
    them.
    """

    # Overridden in Murphy.setUp() when the test param is passed
    workload_template = None

    def load_workload_template(self, name_or_path):
        """
        Read and validate the template and return the per-bucket loadDefn
        list. Fails the test, listing every problem, rather than letting a
        bad template surface hours later as a missing load or a dead query
        thread.
        """
        try:
            path = resolve_template_path(name_or_path)
            template = load_template(path)
        except Exception as e:
            self.fail(f"Could not read workload_template={name_or_path}: {e}")
        errors = template_errors(template, self.input.param("pc", 20))
        if not errors:
            errors = self._template_param_errors(template)
        if errors:
            self.fail(f"workload_template {path} cannot be used:\n  " + "\n  ".join(errors))

        load_defns = build_load_defns(template, os.path.basename(path))
        self.log.info(f"Using workload template {path}")
        for workload in load_defns:
            defns = workload["collections_defn"]
            initial = sorted({collection_rate(workload, defn, INITIAL_RATE) for defn in defns})
            mutation = sorted({collection_rate(workload, defn, MUTATION_RATE) for defn in defns})
            self.log.info(
                f"  {workload['name']}: {workload['valType']}, "
                f"{len(defns)} collection(s), "
                f"{sum(defn['num_items'] for defn in defns)} items, "
                f"ops/collection initial {initial} mutation {mutation}, "
                f"2iQPS {workload.get('2iQPS', 0)}"
            )
        return load_defns

    def _template_param_errors(self, template):
        """Test params that a template run cannot honour."""
        errors = []
        for param in ("fts_nodes", "cbas_nodes"):
            if self.input.param(param, 0):
                errors.append(f"{param} is set, but templates only drive GSI query loads")
        if self.input.param("collection_id", None):
            errors.append("collection_id filters the val_type workloads; a template already lists its collections")
        if self.input.param("coll_order", 1) == -1:
            errors.append(
                "coll_order=-1 reverses which definition each "
                "collection gets, but a template collection is "
                "named after its definition"
            )
        # The same precedence DoctorN1QL.create_indexes() uses to pick its
        # statements; combinational falls back to indexes/queries.
        needed = []
        if not self.input.param("combinational", False):
            if self.input.param("base64", False):
                needed = ["indexes_base64", "queries_base64"]
            elif self.input.param("bhive", False):
                needed = ["bhive_indexes"]
        mock_vector = self.input.param("mockVector", False)
        for bucket_name, workload in template.items():
            for defn in workload["collections_defn"]:
                where = f"bucket {bucket_name}, collection {defn['collection_id']}"
                for key in needed:
                    if not defn.get(key):
                        errors.append(f"{where}: the base64/bhive params need {key}")
                # QueryLoad._run_query sends a mock dense $vector, which only
                # exists with mockVector=True. siftBigANN queries read
                # theirs from the query file instead.
                if mock_vector or workload["valType"] in SIFT_VAL_TYPES or not defn.get("dim"):
                    continue
                for query in defn.get("queries") or []:
                    if "$vector" in query and "SPARSE_VECTOR_DISTANCE" not in query:
                        errors.append(f"{where}: dense $vector queries need mockVector=True")
                        break
        return errors

    def create_template_collections(self, cluster, buckets=None):
        """
        Template counterpart of OPD.create_required_collections(): create
        each bucket's collections in the _default scope, one thread per
        bucket, named by their collection_id, in collection_id order.
        """
        buckets = buckets or cluster.buckets
        node = cluster.master

        def create_collections(bucket):
            for name in template_collection_names(bucket):
                self.bucket_util.create_collection(node, bucket, CbServer.default_scope, {"name": name})
                self.sleep(0.1)
            self.log.debug(f"Collections list == {bucket.scopes[CbServer.default_scope].collections.keys()}")

        threads = []
        for bucket in buckets:
            th = threading.Thread(target=create_collections, name=f"{bucket.name}_create_collection", args=(bucket,))
            threads.append(th)
            th.start()
        for th in threads:
            th.join()

    def bind_template_to_existing_buckets(self):
        """
        skip_init counterpart of the bucket loop in Murphy.setUp(): give the
        server's buckets their template loadDefn by name, rather than by
        position, and their template collection objects.
        """
        workloads = {workload["name"]: workload for workload in self.load_defn}
        on_server = [bucket.name for bucket in self.cluster.buckets]
        missing = sorted(set(workloads) - set(on_server))
        unexpected = sorted(set(on_server) - set(workloads))
        if missing or unexpected:
            self.fail(
                f"skip_init with workload_template needs exactly the "
                f"template's buckets on the cluster. Missing: "
                f"{missing}, not in the template: {unexpected}"
            )
        for bucket in self.cluster.buckets:
            bucket.loadDefn = workloads[bucket.name]
            num_clients = self.input.param("clients_per_db", min(5, bucket.loadDefn.get("collections")))
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.cluster.master,
                self.cluster.master.rest_username,
                self.cluster.master.rest_password,
                bucket.name,
                req_clients=num_clients,
            )
            self.create_sdk_client_pool(self.cluster, self.cluster.buckets, 1)
            for name in template_collection_names(bucket):
                CollectionUtils.create_collection_object(bucket, CbServer.default_scope, {"name": name})
            # After the template collections above, whose objects come out
            # with num_items=0
            self._sync_bucket_from_server(bucket)

    def _template_loader(
        self,
        cluster,
        bucket,
        scope,
        collection,
        defn,
        pattern,
        items,
        ops,
        mutate,
        suppress_error_table,
        track_failures,
    ):
        """
        One DocLoader task for one collection. It sends the same arguments
        JavaDocLoaderUtils.load_sift_data / load_msmarco_data send, except:
          - ops is this collection's own rate;
          - the definition was found by collection name;
          - the credentials come from the cluster.
        """
        workload = bucket.loadDefn
        val_type = defn["valType"]
        kwargs = {
            "server_ip": cluster.master.ip,
            "server_port": cluster.master.port,
            "generator": None,
            "op_type": None,
            "username": cluster.master.rest_username,
            "password": cluster.master.rest_password,
            "bucket": bucket,
            "scope_name": scope,
            "collection_name": collection,
            "key_prefix": workload.get("key_prefix"),
            "key_size": workload.get("key_size"),
            "doc_size": defn.get("doc_size", 256),
            "key_type": workload.get("key_type"),
            "value_type": val_type,
            "create_percent": pattern["create"],
            "read_percent": pattern["read"],
            "update_percent": pattern["update"],
            "delete_percent": pattern["delete"],
            "expiry_percent": pattern["expiry"],
            "create_start_index": 0,
            "create_end_index": items,
            "read_start_index": 0,
            "read_end_index": items,
            "update_start_index": 0,
            "update_end_index": items,
            "delete_start_index": 0,
            "delete_end_index": items,
            "touch_start_index": 0,
            "touch_end_index": 0,
            "replace_start_index": 0,
            "replace_end_index": 0,
            "expiry_start_index": 0,
            "expiry_end_index": items,
            "process_concurrency": JavaDocLoaderUtils.process_concurrency,
            "task_identifier": "",
            "ops": ops,
            "suppress_error_table": suppress_error_table,
            "track_failures": track_failures,
            "mutate": mutate,
            "elastic": False,
            "model": workload.get("model"),
            "mockVector": workload.get("mockVector"),
            "dim": defn.get("dim", 128),
            "base64": workload.get("base64"),
        }
        if val_type in SIFT_VAL_TYPES:
            kwargs.update(
                base_vectors_file_path=workload.get("baseFilePath", "/root/bigann_base.bvecs.gz"),
                sift_url=SIFT_URL,
                stall_timeout=-1,
            )
        else:
            kwargs.update(
                vec_file_path=workload.get("vecFilePath"), base_vectors_file_path=workload.get("baseFilePath")
            )
        return SiriusCouchbaseLoader(**kwargs)

    def submit_template_loads(
        self,
        rate_key,
        buckets=None,
        pattern=None,
        override_num_items=None,
        mutate=0,
        suppress_error_table=False,
        track_failures=True,
    ):
        """
        Start one DocLoader task per template collection of `buckets`, each
        with its bucket's valType and the collection's `rate_key` rate.
        Returns (tasks, failures): the started tasks, and a message for each
        collection whose task DocLoader refused. The JavaDocLoaderUtils
        loaders drop those without a word.
        """
        tasks = []
        failures = []
        for bucket in buckets or self.cluster.buckets:
            workload = bucket.loadDefn
            defns = {defn["collection_id"]: defn for defn in workload["collections_defn"]}
            load_pattern = pattern or workload.get("doc_op_percentages", DEFAULT_PATTERN)
            for scope in list(bucket.scopes.keys()):
                if scope == CbServer.system_scope:
                    continue
                for collection in sorted(bucket.scopes[scope].collections.keys()):
                    defn = defns.get(collection) if scope == CbServer.default_scope else None
                    if defn is None:
                        if collection != CbServer.default_collection:
                            self.log.warning(
                                f"{bucket.name}.{scope}.{collection} is not in the template; not loading it"
                            )
                        continue
                    loader = self._template_loader(
                        self.cluster,
                        bucket,
                        scope,
                        collection,
                        defn,
                        load_pattern,
                        override_num_items or defn["num_items"],
                        collection_rate(workload, defn, rate_key),
                        mutate,
                        suppress_error_table,
                        track_failures,
                    )
                    result, response = loader.create_doc_load_task()
                    if not result:
                        failures.append(f"{bucket.name}.{scope}.{collection}: {response}")
                        continue
                    JavaDocLoaderUtils.doc_loading_tm.add_new_task(loader)
                    tasks.append(loader)
        return tasks, failures

    def _run_template_loads(self, rate_key, what, **kwargs):
        """
        Submit, then wait for, one template load wave from the test's own
        thread. Fails the test, after stopping whatever did start, if
        DocLoader refused any collection.
        """
        tasks, failures = self.submit_template_loads(rate_key, **kwargs)
        if failures:
            self._stop_loader_tasks(tasks)
            self.fail(f"DocLoader refused the {what} for:\n  " + "\n  ".join(failures))
        JavaDocLoaderUtils.wait_for_doc_load_completion(self.cluster, tasks, wait_for_stats=False)

    def template_initial_load(self):
        """Create every template collection's num_items docs at its "ops"."""
        self._run_template_loads(INITIAL_RATE, "initial load", pattern=CREATE_ONLY)

    def template_rollback_load(self, num_items):
        """
        trigger_rollback()'s load of `num_items` new docs per collection. The
        rollback key prefix is already set on each loadDefn.
        """
        self._run_template_loads(INITIAL_RATE, "rollback load", pattern=CREATE_ONLY, override_num_items=num_items)

    def template_steady_state_wave(self):
        """
        One SteadyStateVolume iteration: a full pass of every bucket's own
        doc_op_percentages over all its docs, at "rebl_ops_rate", with a new
        mutate value, so every pass rewrites the vectors.
        """
        self.mutate += 1
        self._run_template_loads(
            MUTATION_RATE,
            "steady state load",
            mutate=self.mutate,
            suppress_error_table=self.suppress_error_table,
            track_failures=self.track_failures,
        )

    def record_template_collection_items(self, bucket):
        """
        Template counterpart of initial_setup()'s per-collection bookkeeping:
        each collection holds its own num_items, not the bucket-level one.
        """
        defns = {defn["collection_id"]: defn for defn in bucket.loadDefn["collections_defn"]}
        for coll_name, coll_obj in bucket.scopes[CbServer.default_scope].collections.items():
            defn = defns.get(coll_name)
            if defn is None:
                continue
            coll_obj.num_items = defn["num_items"]
            coll_obj.doc_index = (0, defn["num_items"])

    def template_vector_mutations(self):
        """
        Mutation thread for template runs. The counterpart of
        vector_file_mutations(), with these differences:
          - each bucket is loaded with its own valType's loader;
          - each collection runs at its own "rebl_ops_rate" share;
          - every task of a wave is awaited.
        It keeps the same pause handshake. self.loader_tasks holds the
        in-flight wave, so pause_data_workload() can stop it.
        """
        self.loader_tasks = []
        self.gtm = False

        while self.mutations:
            self.wait_if_workload_paused()
            if not self.mutations:
                break
            self.mutate += 1
            tasks, failures = self.submit_template_loads(
                MUTATION_RATE,
                mutate=self.mutate,
                suppress_error_table=self.suppress_error_table,
                track_failures=self.track_failures,
            )
            for failure in failures:
                self.log.critical(f"Mutation load not started for {failure}")
            self.loader_tasks = tasks
            for task in list(tasks):
                self.task_manager.get_task_result(task)
                self.loader_tasks.remove(task)
            self.check_index_pending_mutations(self.cluster)
