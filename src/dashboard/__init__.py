"""Dashboard serving package for CPC AdTech Lakehouse."""

from src.dashboard.db import (
    DatabaseConnectionError,
    create_root_connection,
    get_root_connection,
    run_query,
)

__all__ = [
    "DatabaseConnectionError",
    "create_root_connection",
    "get_root_connection",
    "run_query",
]
