"""
Given one or more SheetSyncConfig, produces a ready-to-schedule Airflow DAG
wrapped in your existing pipeline tracking (TrackedPythonOperator +
PipelineTracker), the same pattern used by combined_vinculum_dag.py.

Standard going forward: one DAG per WORKBOOK, not one DAG per sheet -- pass
a list of SheetSyncConfig (one per sheet you want synced out of that
workbook) to create_workbook_sync_dag() and get back a single DAG with one
task per sheet. create_excel_sync_dag() still exists for a single sheet and
is now just a thin wrapper around create_workbook_sync_dag() with a one-item
list, so existing single-sheet DAGs keep working unchanged.

Each sheet's task is independently tracked as its own complete pipeline run
(own run_id, own row in pipeline_runs) rather than sharing one run_id across
all of a workbook's tasks -- sheets don't depend on each other, so there's no
natural "first"/"last" task to hang a shared run on without adding explicit
task ordering (which would also cap concurrency). Independent tracking avoids
an XCom race entirely and keeps every sheet's data traceable to exactly the
run that loaded it, which is what actually matters here.

This is intentionally the only file with "magic" in it. Onboarding a new
sheet should never require touching this file -- only writing a config and
calling create_workbook_sync_dag() (see example_dags/sync_workbook_multi_sheet.py).
"""
from datetime import datetime
from typing import List

from airflow import DAG

from .config import SheetSyncConfig, SyncMode
from .auth import get_graph_token
from .sharepoint_reader import SharePointExcelReader
from .schema import clean_column_name
from .loader import full_refresh_load
from .file_sync import run_file_sync

# Point these at wherever they actually live in your project.
from utils.postgresconnector_v3 import PostgresConnector
from plugins.operators.tracked_python_operator import TrackedPythonOperator


def _run_sync(config: SheetSyncConfig, run_id=None, tracker=None, **context):
    # run_id and tracker are injected automatically by TrackedPythonOperator
    # (see plugins/operators/tracked_python_operator.py) -- not passed by us.
    if config.sync_mode == SyncMode.FILE_DOWNLOAD:
        return run_file_sync(config, run_id=run_id)

    reader = SharePointExcelReader(get_token_fn=get_graph_token)

    if config.sync_mode == SyncMode.FULL_SHEET:
        df = reader.get_full_sheet(
            site_id=config.site_id,
            file_path=config.file_path,
            sheet_name=config.sheet_name,
            header_row=config.header_row,
        )
    else:  # SyncMode.RANGE
        df = reader.get_range(
            site_id=config.site_id,
            file_path=config.file_path,
            sheet_name=config.sheet_name,
            address=config.range_address,
            header_row=config.header_row,
        )

    connector = PostgresConnector()  # pulls creds from Secrets Manager, same as your other DAGs
    return full_refresh_load(connector, df, config.target_schema, config.target_table, run_id=run_id)


def _task_id_for(target_table: str) -> str:
    """
    Airflow task_ids only allow alphanumeric characters, dashes, dots, and
    underscores -- stricter than Postgres identifiers, which can legally
    contain spaces, mixed case, etc. as long as they're double-quoted.
    target_table is free to be whatever valid (quotable) Postgres identifier
    you want; this always produces a safe task_id independent of that, reusing
    the same cleaning logic used to auto-derive target_table from sheet_name.
    """
    return f"sync_{clean_column_name(target_table, lowercase=True)}"


def create_workbook_sync_dag(
    sheets: List[SheetSyncConfig],
    dag_id: str,
    schedule: str = "@daily",
    start_date: datetime = datetime(2026, 1, 1),
    max_active_tasks: int = 3,
    **dag_kwargs,
) -> DAG:
    """
    One DAG for an entire workbook. Each item in `sheets` is a normal
    SheetSyncConfig -- for sheets in the same workbook, site_id and file_path
    will be identical across entries; that's expected, not redundant config
    to clean up (see example_dags/sync_workbook_multi_sheet.py).

    max_active_tasks caps how many sheets sync concurrently within this one
    DAG run. This matters for Microsoft Graph rate limiting: keeping bursts
    small reduces how often a 429 happens in the first place, on top of the
    retry/backoff handling already built into sharepoint_reader.py. Default
    of 3 is a conservative starting point for a workbook with many sheets;
    raise it if your tenant handles more concurrent Graph calls comfortably.
    """
    task_ids = [_task_id_for(s.target_table) for s in sheets]
    duplicates = {t for t in task_ids if task_ids.count(t) > 1}
    if duplicates:
        raise ValueError(
            f"Duplicate task_id(s) in this workbook's sheet list: {duplicates}. "
            f"Two sheets have target_table values that clean down to the same "
            f"task_id (e.g. \"SKU List\" and \"sku_list\" both become sync_sku_list) "
            f"-- give each sheet a distinct target_table."
        )

    with DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False,
        max_active_tasks=max_active_tasks,
        tags=["excel_sync", "full_refresh", "workbook"],
        **dag_kwargs,
    ) as dag:
        for sheet_config, task_id in zip(sheets, task_ids):
            TrackedPythonOperator(
                task_id=task_id,
                python_callable=_run_sync,
                op_kwargs={"config": sheet_config},
                pipeline_name=sheet_config.pipeline_name,
                client_id=sheet_config.client_id,
                data_type=sheet_config.data_type,
                is_first_task=True,
                is_last_task=True,
                failure_email_to=sheet_config.failure_email_to,
                success_email_to=sheet_config.success_email_to,
            )
    return dag


def create_excel_sync_dag(
    config: SheetSyncConfig,
    dag_id: str,
    schedule: str = "@daily",
    start_date: datetime = datetime(2026, 1, 1),
    **dag_kwargs,
) -> DAG:
    """
    Single-sheet convenience wrapper around create_workbook_sync_dag() with a
    one-item list. Kept for backward compatibility with existing single-sheet
    DAG files -- prefer create_workbook_sync_dag() directly for any workbook
    with more than one sheet to sync, since one DAG per workbook is the
    standard going forward, not one DAG per sheet.
    """
    return create_workbook_sync_dag([config], dag_id, schedule, start_date, **dag_kwargs)