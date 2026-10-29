"""
Splits key-range doc loads into chunks and runs them on a set of DocLoader
REST endpoints (usually a DocLoaderFleet, but any list of URLs works).

Every endpoint gets `chunks_per_jvm` worker threads; each pulls the next chunk
when its current one finishes, so faster JVMs do more. By default every JVM
runs one chunk at a time with `num_workers` (40) DocLoader threads, and the
chunk size is chosen so all slots get equal work in full waves (see
auto_chunk_size). Creates are bulkUpsert
in DocLoader, so a failed chunk is simply re-run (up to max_chunk_retries). A
JVM that stops answering /check-online is dropped and its chunk is requeued
without consuming a retry.
"""

import copy
import math
import queue
import threading
import time
from dataclasses import dataclass

import requests

from bucket_utils.bucket_ready_functions import JavaDocLoaderUtils
from Jython_tasks.java_loader_tasks import BaseSiriusLoader, SiriusCouchbaseLoader

from .fleet import is_loader_online

# Upper bound for an automatically sized chunk: bounds the rework when a chunk
# fails or its JVM dies (~9 min of work for one JVM at ~180K docs/s).
MAX_AUTO_CHUNK = 100000000


def auto_chunk_size(total_items, slots, max_chunk=MAX_AUTO_CHUNK):
    """Equal chunks that fill every slot in whole waves, each <= max_chunk.

    With chunk = total / (slots * waves) every slot gets the same number of
    equal chunks, so all slots finish together instead of a partial last
    wave leaving most of the fleet idle."""
    slots = max(1, slots)
    waves = max(1, math.ceil(total_items / (slots * max_chunk)))
    return max(1, math.ceil(total_items / (slots * waves)))


@dataclass
class LoadChunk:
    cluster: object
    template: SiriusCouchbaseLoader
    start: int
    end: int
    attempts: int = 0

    @staticmethod
    def split(cluster, template, start, end, chunk_size):
        """Cut [start, end) of `template`'s collection into chunks."""
        return [LoadChunk(cluster, template, s, min(s + chunk_size, end))
                for s in range(start, end, chunk_size)]

    @staticmethod
    def split_even(cluster, template, start, end, target_size):
        """Cut [start, end) into equal chunks of about target_size."""
        pieces = max(1, round((end - start) / target_size))
        bounds = [start + (end - start) * i // pieces for i in range(pieces + 1)]
        return [LoadChunk(cluster, template, lo, hi)
                for lo, hi in zip(bounds, bounds[1:])]

    @property
    def label(self):
        return "%s:%s:%s:%s[%s,%s)" % (
            getattr(self.cluster, "id", self.cluster.master.ip),
            self.template.bucket.name, self.template.scope,
            self.template.collection, self.start, self.end)


class FleetLoadDispatcher(object):
    def __init__(self, log, loader_urls, server_name=None,
                 chunk_size=None, chunks_per_jvm=1, num_workers=40,
                 process_concurrency=None, ops_per_chunk=None,
                 max_chunk_retries=3, stall_timeout=1800,
                 progress_interval=300):
        self.log = log
        self.loader_urls = list(loader_urls)
        self.server_name = server_name
        # None -> auto_chunk_size() over the whole load
        self.chunk_size = int(chunk_size) if chunk_size else None
        self.chunks_per_jvm = int(chunks_per_jvm)
        # DocLoader threads per JVM, shared by its concurrent chunks
        self.num_workers = int(num_workers)
        # Threads per chunk; None -> num_workers / chunks_per_jvm
        self.process_concurrency = int(process_concurrency) \
            if process_concurrency else max(1, self.num_workers // self.chunks_per_jvm)
        self.ops_per_chunk = ops_per_chunk
        self.max_chunk_retries = int(max_chunk_retries)
        self.stall_timeout = stall_timeout
        self.progress_interval = progress_interval

        self._lock = threading.Lock()
        self._queue = queue.Queue()
        self._dead = set()
        self._done_items = 0
        self._done_chunks = 0
        self._total_chunks = 0
        self._total_items = 0
        self._failed = list()
        self._stop_progress = threading.Event()

    def load_initial_data(self, clusters):
        """Create every cluster's buckets' create range through the fleet.
        Same bucket bookkeeping as JavaDocLoaderUtils.load_data(); raises if
        any chunk is still failing after max_chunk_retries."""
        per_cluster_chunks = list()
        for cluster in clusters:
            for bucket in cluster.buckets:
                JavaDocLoaderUtils.generate_docs(
                    doc_ops=["create"], create_start=0,
                    create_end=bucket.loadDefn.get("num_items"), bucket=bucket)
            loader_map = JavaDocLoaderUtils._loader_dict(
                cluster, cluster.buckets,
                overRidePattern={"create": 100, "read": 0, "update": 0,
                                 "delete": 0, "expiry": 0},
                process_concurrency=self.process_concurrency,
                suppress_error_table=True)
            per_cluster_chunks.append(list(loader_map.values()))

        total_items = sum(t.create_end_index - t.create_start_index
                          for templates in per_cluster_chunks for t in templates)
        slots = len(self.loader_urls) * self.chunks_per_jvm
        chunk_size = self.chunk_size or auto_chunk_size(total_items, slots)
        self.log.info("Fleet load plan: %s items over %s slots (%s JVMs x %s), "
                      "%s chunk size %s, %s threads per chunk"
                      % (total_items, slots, len(self.loader_urls),
                         self.chunks_per_jvm,
                         "fixed" if self.chunk_size else "auto",
                         chunk_size, self.process_concurrency))
        for i, (cluster, templates) in enumerate(zip(clusters, per_cluster_chunks)):
            chunks = list()
            for template in templates:
                split = LoadChunk.split if self.chunk_size else LoadChunk.split_even
                chunks.extend(split(cluster, template,
                                    template.create_start_index,
                                    template.create_end_index, chunk_size))
            per_cluster_chunks[i] = chunks

        if not self.ops_per_chunk:
            self._split_ops_across_chunks(per_cluster_chunks)

        # Round-robin across clusters so every cluster loads concurrently
        # instead of the fleet draining one cluster after another.
        interleaved = list()
        for i in range(max(len(c) for c in per_cluster_chunks)):
            for chunks in per_cluster_chunks:
                if i < len(chunks):
                    interleaved.append(chunks[i])
        self.load_chunks(interleaved, clusters)

    def load_chunks(self, chunks, clusters):
        """Run `chunks` (in the given order) on the fleet and block until done.
        `clusters` are the ones the chunks target; their buckets get SDK
        clients on every JVM. Raises if any chunk still fails after
        max_chunk_retries or no JVM is left alive."""
        # Every slot of a JVM must get a thread per worker of its chunk, or
        # chunks sit queued behind each other inside the JVM.
        self.num_workers = max(self.num_workers, self.chunks_per_jvm * max(
            c.template.process_concurrency for c in chunks))
        for chunk in chunks:
            self._queue.put(chunk)
        self._total_chunks = self._queue.qsize()
        self._total_items = sum(c.end - c.start for c in self._queue.queue)
        self.log.info("Fleet load: %s chunks, %s items over %s DocLoader "
                      "JVMs x %s slots"
                      % (self._total_chunks, self._total_items,
                         len(self.loader_urls), self.chunks_per_jvm))

        self._prepare_loaders(clusters)
        progress = threading.Thread(target=self._log_progress, daemon=True)
        progress.start()
        try:
            self._run()
        finally:
            self._stop_progress.set()

        if self._failed:
            raise Exception("Fleet load failed for %s chunk(s): %s"
                            % (len(self._failed), self._failed[:20]))
        if not self._queue.empty():
            raise Exception("Fleet load stopped with %s chunk(s) unloaded: "
                            "no DocLoader JVMs left alive (dead: %s)"
                            % (self._queue.qsize(), sorted(self._dead)))

    def _split_ops_across_chunks(self, per_cluster_chunks):
        """Keep ops_rate meaning what it does for the local loader.

        _loader_dict() gives each collection's template its share of the
        bucket's ops_rate. A collection's chunks run in parallel on the
        fleet, so handing every chunk that full share would multiply the
        collection's rate by its concurrency. Divide it by the number of its
        chunks expected to run at once: interleaving gives every collection
        an equal share of the fleet's slots, capped by its chunk count."""
        templates = dict()
        for chunks in per_cluster_chunks:
            for chunk in chunks:
                templates.setdefault(id(chunk.template),
                                     [chunk.template, 0])[1] += 1
        if not templates:
            return
        total_slots = len(self.loader_urls) * self.chunks_per_jvm
        slots_per_collection = max(1, total_slots // len(templates))
        for template, num_chunks in templates.values():
            if template.gtm or not template.ops:
                continue
            concurrent = min(num_chunks, slots_per_collection)
            template.ops = max(1, template.ops // concurrent)
            self.log.info("%s:%s:%s - %s chunks, ~%s concurrent, %s ops/s each"
                          % (template.bucket.name, template.scope,
                             template.collection, num_chunks, concurrent,
                             template.ops))

    def _prepare_loaders(self, clusters):
        for url in self.loader_urls:
            resp = requests.post(
                url + "/reset_task_manager",
                '{"num_workers":%s}' % self.num_workers,
                headers=BaseSiriusLoader.get_headers(), timeout=60)
            if not resp.ok:
                raise Exception("reset_task_manager failed on %s: %s"
                                % (url, resp.text))
            for cluster in clusters:
                for bucket in cluster.buckets:
                    # /create_clients always answers HTTP 400; trust the body
                    _, body = self._create_clients(url, cluster, bucket)
                    if not body or not body.get("status"):
                        raise Exception("create_clients failed on %s for %s: "
                                        "%s" % (url, bucket.name, body))

    @staticmethod
    def _create_clients(url, cluster, bucket):
        resp = requests.post(
            url + "/create_clients",
            BaseSiriusLoader._flatten_param_to_str({
                "server_ip": cluster.master.ip,
                "server_port": cluster.master.memcached_port,
                "username": cluster.master.rest_username,
                "password": cluster.master.rest_password,
                "bucket_name": bucket.name,
                "req_clients": 1}),
            headers=BaseSiriusLoader.get_headers(), timeout=120)
        try:
            return resp.ok, resp.json()
        except ValueError:
            return resp.ok, None

    def _run(self):
        # A worker exits when the queue is momentarily empty, but a chunk that
        # fails on a dying JVM is requeued afterwards - so keep running rounds
        # until the queue is drained or no JVM is left.
        while not self._queue.empty():
            live = [u for u in self.loader_urls if u not in self._dead]
            if not live:
                return
            threads = [threading.Thread(target=self._worker, args=(url,),
                                        name="fleet-%s-%s" % (url, slot),
                                        daemon=True)
                       for url in live for slot in range(self.chunks_per_jvm)]
            for th in threads:
                th.start()
            for th in threads:
                th.join()

    def _worker(self, url):
        while url not in self._dead:
            try:
                chunk = self._queue.get_nowait()
            except queue.Empty:
                return
            try:
                ok, error = self._run_chunk(chunk, url)
            except Exception as e:
                ok, error = False, str(e)
            if ok:
                with self._lock:
                    self._done_items += chunk.end - chunk.start
                    self._done_chunks += 1
                continue
            if not is_loader_online(url, self.server_name):
                with self._lock:
                    self._dead.add(url)
                self.log.critical("DocLoader %s is unreachable, dropping it "
                                  "from the fleet; requeuing %s"
                                  % (url, chunk.label))
                self._queue.put(chunk)
                return
            chunk.attempts += 1
            if chunk.attempts > self.max_chunk_retries:
                self.log.critical("Chunk %s failed %s times, giving up: %s"
                                  % (chunk.label, chunk.attempts, error))
                with self._lock:
                    self._failed.append("%s: %s" % (chunk.label, error))
            else:
                self.log.warning("Chunk %s failed on %s (attempt %s), "
                                 "requeuing: %s"
                                 % (chunk.label, url, chunk.attempts, error))
                self._queue.put(chunk)

    def _run_chunk(self, chunk, url):
        loader = copy.copy(chunk.template)
        loader.sirius_url = url
        loader.task_ids = list()
        loader.thread_name = None
        loader.success = dict()
        loader.fail = dict()
        loader.fail_count = 0
        loader.completed = False
        loader.create_start_index = chunk.start
        loader.create_end_index = chunk.end
        # Progress polling must stay on: with it off, the blocking
        # /get_task_result would hit its 600s HTTP timeout on any real chunk.
        loader.stall_timeout = self.stall_timeout
        # A JVM sits idle from its chunk finishing until this poll notices,
        # so keep it short: a few cheap REST calls vs. minutes of idle JVMs.
        loader.progress_poll_interval = 5
        if self.ops_per_chunk:
            loader.ops = int(self.ops_per_chunk)
            loader.gtm = False

        ok, resp = loader.create_doc_load_task()
        if not ok or not resp or not resp.get("status", True):
            return False, "doc_load rejected: %s" % resp
        ok, resp = loader._make_task_request(loader.task_ids, "submit_task")
        if not ok or (resp and resp.get("status") is False):
            return False, "submit_task failed: %s" % resp
        if not loader.get_task_result():
            return False, "get_task_result reported failure"
        if loader.fail_count:
            sample = list(loader.fail.items())[:3]
            return False, "%s failed keys, e.g. %s" % (loader.fail_count,
                                                       sample)
        return True, None

    def _log_progress(self):
        start_time = time.time()
        total_items = self._total_items or 1
        while not self._stop_progress.wait(self.progress_interval):
            with self._lock:
                done_items, done_chunks = self._done_items, self._done_chunks
                dead = len(self._dead)
            elapsed = time.time() - start_time
            rate = done_items / elapsed if elapsed else 0
            eta = (total_items - done_items) / rate if rate else float("inf")
            self.log.info(
                "Fleet load: %s/%s chunks, %s/%s items (%.1f%%), %.0f "
                "items/s, ETA %.1fh, dead JVMs %s/%s"
                % (done_chunks, self._total_chunks, done_items, total_items,
                   100.0 * done_items / total_items, rate, eta / 3600, dead,
                   len(self.loader_urls)))
