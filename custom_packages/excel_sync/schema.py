"""
Infers a Postgres schema from a pandas DataFrame the way Airbyte does when it
auto-creates a destination table: sample every column's values and pick the
narrowest safe type. Also normalizes column names into valid Postgres
identifiers (spaces/punctuation cleaned, original letter case preserved).
"""
import re
import warnings
import pandas as pd


def clean_column_name(name: str, *, lowercase: bool = False) -> str:
    name = str(name).strip()
    if lowercase:
        name = name.lower()
    name = re.sub(r"[()]", "", name)
    name = re.sub(r"[\s\-.]+", "_", name)
    name = re.sub(r"[^a-zA-Z0-9_]", "", name)
    if not name:
        name = "unnamed_col"
    if name[0].isdigit():
        name = f"col_{name}"
    return name


def _infer_column_type(series: pd.Series) -> str:
    non_null = series.dropna()
    non_null = non_null[non_null.astype(str).str.strip() != ""]

    if non_null.empty:
        return "TEXT"

    lowered = non_null.astype(str).str.strip().str.lower()
    if lowered.isin(["true", "false"]).all():
        return "BOOLEAN"

    numeric = pd.to_numeric(non_null, errors="coerce")
    if numeric.notna().all():
        return "BIGINT" if (numeric % 1 == 0).all() else "DOUBLE PRECISION"

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        parsed_dates = pd.to_datetime(non_null, errors="coerce")
    if parsed_dates.notna().all():
        return "TIMESTAMP"

    return "TEXT"


def infer_postgres_schema(df: pd.DataFrame) -> dict:
    """Returns an ordered {clean_column_name: postgres_type} dict for df."""
    schema = {}
    for col in df.columns:
        clean_name = clean_column_name(col)
        base_name, i = clean_name, 1
        while clean_name in schema:  # de-dupe columns that clean down to the same name
            i += 1
            clean_name = f"{base_name}_{i}"
        schema[clean_name] = _infer_column_type(df[col])
    return schema


def coerce_dataframe_to_schema(df: pd.DataFrame, column_types: dict) -> pd.DataFrame:
    """
    Casts each column to match its inferred Postgres type and converts blank
    cells to a true NULL, rather than leaving them as literal empty strings.

    This matters because Graph's usedRange API returns a blanked-out Excel
    cell as "" (empty string), not null/None. infer_postgres_schema already
    ignores blanks when detecting a column's type, but without this step
    those blanks would still reach Postgres as raw "" values, which Postgres
    correctly rejects for any non-text column (e.g. inserting "" into a
    DOUBLE PRECISION column raises InvalidTextRepresentation).
    """
    df = df.copy()
    for col, sql_type in column_types.items():
        if col not in df.columns:
            continue

        series = df[col]
        is_blank = series.astype(str).str.strip() == ""
        series = series.where(~is_blank, other=None)

        if sql_type in ("BIGINT", "DOUBLE PRECISION"):
            series = pd.to_numeric(series, errors="coerce")
        elif sql_type == "BOOLEAN":
            series = series.astype(str).str.strip().str.lower().map({"true": True, "false": False})
        elif sql_type == "TIMESTAMP":
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                series = pd.to_datetime(series, errors="coerce")
        # TEXT columns: already blank-normalized above, nothing further needed

        df[col] = series
    return df