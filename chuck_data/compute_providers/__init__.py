"""Compute Provider Abstractions.

Compute providers define where Stitch jobs execute (Databricks clusters, EMR clusters).
This is independent of where the data comes from (DataProvider).
"""

from chuck_data.compute_providers.provider import ComputeProvider
from chuck_data.compute_providers.databricks import DatabricksComputeProvider
from chuck_data.compute_providers.emr import EMRComputeProvider


__all__ = [
    "ComputeProvider",
    "DatabricksComputeProvider",
    "EMRComputeProvider",
]
