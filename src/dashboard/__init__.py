"""Dashboard serving package for CPC AdTech Lakehouse."""

from src.dashboard.db import (
    DatabaseConnectionError,
    create_root_connection,
    get_root_connection,
    run_query,
)
from src.dashboard.repository import (
    AlertThresholds,
    DashboardRepository,
    DataSourcePaths,
)

__all__ = [
    "AlertThresholds",
    "DashboardRepository",
    "DataSourcePaths",
    "DatabaseConnectionError",
    "create_root_connection",
    "get_root_connection",
    "run_query",
]
