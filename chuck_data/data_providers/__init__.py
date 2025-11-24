"""Data Provider Module.

Factory pattern for creating data provider instances that abstract
different data platforms (Databricks, AWS Redshift, etc.)
"""

from chuck_data.data_providers.factory import DataProviderFactory
from chuck_data.data_providers.provider import DataProvider

__all__ = ["DataProviderFactory", "DataProvider"]
