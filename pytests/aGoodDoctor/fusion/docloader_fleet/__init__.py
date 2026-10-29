"""
Scale Java DocLoader out to an EC2 Auto Scaling Group and load key-range
chunks through it. Product-agnostic; lives under fusion/ for now.
See AGENTS.md in this directory.
"""

from .dispatcher import FleetLoadDispatcher, LoadChunk
from .fleet import DocLoaderFleet, is_loader_online
from .loader import FleetDataLoader

__all__ = ["DocLoaderFleet", "FleetDataLoader", "FleetLoadDispatcher",
           "LoadChunk", "is_loader_online"]
