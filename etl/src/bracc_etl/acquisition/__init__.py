"""Local acquisition primitives; callers own source-specific pagination and policy."""

from .catalog import RawArtifactRef, publish_partition, register_raw_artifact
from .client import Acquisition, AcquisitionError, Manifest

__all__ = [
    "Acquisition", "AcquisitionError", "Manifest", "RawArtifactRef",
    "publish_partition", "register_raw_artifact",
]
