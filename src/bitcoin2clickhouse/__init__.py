"""
Bitcoin to ClickHouse - A library for parsing Bitcoin blockchain data and storing it in ClickHouse.

This package provides tools for parsing Bitcoin blockchain data and efficiently
storing it in ClickHouse database.
"""

__version__ = "0.1.0"
__author__ = "hal9000cc"
__email__ = "hal9000cc@gmail.com"

from .bitcoin2clickhouse import BitcoinClickHouseLoader, setup_logging
