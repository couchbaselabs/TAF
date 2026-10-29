"""
Test-facing entry point: a DocLoaderFleet plus a FleetLoadDispatcher, built
from `loader_fleet_*` test params.

    self.fleet_loader = FleetDataLoader.from_test_input(
        self.input, self.log, region="us-east-1", boto3_session=session)
    self.fleet_loader.start()                      # launches in background
    ...                                            # create clusters/buckets
    self.fleet_loader.load_initial_data(clusters)  # blocks until loaded
    ...
    self.fleet_loader.teardown()                   # in tearDown()
"""

import os
import sys
from concurrent.futures import ThreadPoolExecutor

import boto3

from .dispatcher import FleetLoadDispatcher
from .fleet import FLEET_TAG_KEY, DocLoaderFleet


class FleetDataLoader(object):
    PARAM_PREFIX = "loader_fleet_"

    def __init__(self, log, fleet, dispatcher_kwargs=None, keep_fleet=False):
        self.log = log
        self.fleet = fleet
        self.dispatcher_kwargs = dispatcher_kwargs or dict()
        self.keep_fleet = keep_fleet
        self._launch = None

    @classmethod
    def from_test_input(cls, test_input, log, region, boto3_session=None,
                        size=1):
        """Build from `loader_fleet_*` params; `size` is the instance count
        when loader_fleet_size is not given (e.g. one per cluster). AWS
        credentials, in order: loader_fleet_aws_profile, the given
        boto3_session, then boto3's default credential chain."""
        def param(name, default=None):
            return test_input.param(cls.PARAM_PREFIX + name, default)

        def csv(name):
            value = param(name)
            return value.split(",") if value else None

        region = param("region", region)
        if param("aws_profile"):
            session = boto3.Session(profile_name=param("aws_profile"),
                                    region_name=region)
        else:
            session = boto3_session or boto3.Session(region_name=region)

        fleet = DocLoaderFleet(
            log, session, region,
            taf_path=os.path.dirname(os.path.abspath(sys.argv[0])),
            size=param("size", size),
            instance_type=param("instance_type", "c8g.8xlarge"),
            jvms_per_instance=param("jvms_per_instance", 4),
            base_port=param("base_port", 8080),
            subnet_ids=csv("subnet_ids"),
            ami_id=param("ami"),
            key_name=param("key_name"),
            ingress_cidrs=csv("ingress_cidrs"),
            s3_bucket=param("s3_bucket"),
            launch_template=param("launch_template"))
        dispatcher_kwargs = dict(
            # None -> sized automatically (dispatcher.auto_chunk_size)
            chunk_size=param("chunk_size"),
            chunks_per_jvm=param("chunks_per_jvm", 1),
            num_workers=param("workers_per_jvm", 40),
            # None -> workers_per_jvm / chunks_per_jvm
            process_concurrency=param("process_concurrency"),
            ops_per_chunk=param("ops_per_chunk"),
            max_chunk_retries=param("chunk_retries", 3))
        return cls(log, fleet, dispatcher_kwargs,
                   keep_fleet=not param("teardown", True))

    def start(self):
        """Launch the fleet in the background so instance boot overlaps with
        whatever setup the test does next."""
        if self._launch is None:
            self._launch = ThreadPoolExecutor(max_workers=1).submit(
                self.fleet.launch)
        return self

    @property
    def loader_urls(self):
        """DocLoader URLs, one per JVM; blocks until the fleet is online."""
        return self.start()._launch.result()

    def dispatcher(self, **overrides):
        """A new FleetLoadDispatcher on this fleet (one per load)."""
        kwargs = dict(self.dispatcher_kwargs, **overrides)
        return FleetLoadDispatcher(self.log, self.loader_urls,
                                   server_name=self.fleet.server_name,
                                   **kwargs)

    def load_initial_data(self, clusters, **overrides):
        """Create each bucket's loadDefn num_items in every non-system
        collection of every cluster (same bookkeeping as
        JavaDocLoaderUtils.load_data)."""
        self.dispatcher(**overrides).load_initial_data(clusters)

    def load_chunks(self, chunks, clusters, **overrides):
        """Run caller-built LoadChunks, e.g. LoadChunk.split(...)."""
        self.dispatcher(**overrides).load_chunks(chunks, clusters)

    def teardown(self):
        """Best effort; never raises, so it is safe in tearDown()."""
        if self.keep_fleet:
            self.log.warning("Keeping DocLoader fleet %s (teardown disabled) "
                             "- delete its ASG and launch template version "
                             "by tag %s" % (self.fleet.run_id, FLEET_TAG_KEY))
            return
        try:
            if self._launch:
                # Never tear down underneath a launch still in flight
                self._launch.exception()
            self.fleet.teardown()
        except Exception as e:
            self.log.critical("DocLoader fleet teardown failed: %s" % e)
