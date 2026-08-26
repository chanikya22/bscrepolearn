"""
excel_sync
==========

Minimal utility to replace the Airbyte Google Sheets connector with a direct
SharePoint Excel -> Postgres full-refresh pipeline, built for Airflow.

Public API:
    SheetSyncConfig, SyncMode     -- config.py   (the only thing most people touch)
    create_workbook_sync_dag      -- dag_factory.py -- STANDARD: one DAG per workbook, one task per sheet
    create_excel_sync_dag         -- dag_factory.py -- single-sheet convenience wrapper
    (both only usable where airflow is installed)

Everything else (auth, reading, schema inference, loading) is internal
plumbing shared by every sheet you onboard, and works with or without airflow
installed -- see test_local_sync.py for running the pipeline standalone.
"""
from .config import SheetSyncConfig, SyncMode

try:
    from .dag_factory import create_excel_sync_dag, create_workbook_sync_dag
except ImportError:
    # airflow isn't installed in this environment (e.g. local testing) --
    # config/auth/reader/schema/loader all still work fine without it.
    create_excel_sync_dag = None
    create_workbook_sync_dag = None

__all__ = ["SheetSyncConfig", "SyncMode", "create_excel_sync_dag", "create_workbook_sync_dag"]