"""
Loads a DataFrame into Postgres as a full-refresh overwrite -- the same
semantics as Airbyte's "Full Refresh | Overwrite" sync mode:

  1. create the target table if it doesn't exist yet, using the inferred schema
  2. TRUNCATE it
  3. bulk-insert the new data

Reuses your existing PostgresConnector (postgres_connector_v3.py) rather than
opening new connections or reimplementing insert logic.
"""
import logging
from typing import Optional
import pandas as pd

from .schema import infer_postgres_schema, coerce_dataframe_to_schema

logger = logging.getLogger(__name__)


def _ensure_table(connector, schema: str, table: str, column_types: dict):
    if connector.table_exists(table, schema=schema):
        return
    cols_sql = ",\n    ".join(f'"{col}" {sql_type}' for col, sql_type in column_types.items())
    ddl = f'CREATE TABLE {schema}."{table}" (\n    {cols_sql}\n);'
    logger.info(f"Table {schema}.{table} does not exist, creating it:\n{ddl}")
    connector.execute_query(ddl, autocommit=True)


def _get_existing_columns(connector, schema: str, table: str) -> list:
    query = """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = :schema_name AND table_name = :table_name
        ORDER BY ordinal_position
    """
    rows = connector.read_query(query, params={"schema_name": schema, "table_name": table}, as_dict=True)
    return [r["column_name"] for r in rows]


def _reconcile_columns(connector, schema: str, table: str, column_types: dict):
    """
    Adds any column present in the current sheet but missing from the table,
    and drops any table column no longer present in the current sheet -- so
    the table's column set always mirrors the sheet's current columns, the
    same way TRUNCATE + reload already mirrors the sheet's current rows.

    Note: dropping a column permanently deletes that column's data. This is
    intentional and consistent with full-refresh-overwrite semantics (which
    already destroys and rebuilds every row on each run) -- but it does mean
    an accidental column deletion in the sheet is not recoverable from the
    database side. Existing shared columns' types are left untouched, so a
    manual type fix (see README) survives future syncs.
    """
    existing = set(_get_existing_columns(connector, schema, table))
    current = set(column_types.keys())

    for col in current - existing:
        ddl = f'ALTER TABLE {schema}."{table}" ADD COLUMN "{col}" {column_types[col]};'
        logger.info(f"New column in sheet, adding to table: {ddl}")
        connector.execute_query(ddl, autocommit=True)

    for col in existing - current:
        ddl = f'ALTER TABLE {schema}."{table}" DROP COLUMN "{col}";'
        logger.warning(f"Column no longer in sheet, dropping from table (data loss): {ddl}")
        connector.execute_query(ddl, autocommit=True)


def full_refresh_load(
    connector,
    df: pd.DataFrame,
    target_schema: str,
    target_table: str,
    run_id: Optional[str] = None,
) -> dict:
    """
    connector: an instance of PostgresConnector (or TrackedPostgresConnector)
    run_id: if provided (e.g. from TrackedPythonOperator's pipeline tracking),
        stamped onto every row as a "run_id" TEXT column, so rows loaded by a
        given pipeline run can always be traced back to it. The column is
        added to an existing table automatically via the same reconciliation
        that handles the sheet's own column changes -- no manual ALTER needed.
    Returns a small stats dict, handy for logging or pushing to XCom.
    """
    if df.empty:
        logger.warning(f"No rows fetched for {target_schema}.{target_table}, skipping load")
        return {"rows_loaded": 0, "record_count": 0, "table": f"{target_schema}.{target_table}"}

    column_types = infer_postgres_schema(df)
    df = df.copy()
    df.columns = list(column_types.keys())  # apply cleaned + de-duped names, same order
    df = coerce_dataframe_to_schema(df, column_types)  # blanks -> real NULL, values -> matching dtype

    if run_id is not None:
        df = df.assign(run_id=run_id)
        column_types["run_id"] = "TEXT"

    _ensure_table(connector, target_schema, target_table, column_types)
    _reconcile_columns(connector, target_schema, target_table, column_types)

    connector.execute_query(f'TRUNCATE TABLE {target_schema}."{target_table}"', autocommit=True)
    inserted = connector.bulk_insert_df(df, target_table, if_exists="append", schema=target_schema)

    logger.info(f"Full-refresh loaded {inserted} rows into {target_schema}.{target_table}")
    return {
        "rows_loaded": inserted,
        "record_count": inserted,  # TrackedPythonOperator reads this key specifically
        "table": f"{target_schema}.{target_table}",
        "run_id": run_id,
    }