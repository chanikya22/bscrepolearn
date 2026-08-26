import pandas as pd
import numpy as np
import os
import json
from typing import Dict, List, Union, Callable, Any


def transform_to_parquet(
        data_source: Union[str, pd.DataFrame],
        output_path: str,
        column_mappings: Dict[str, str] = None,
        data_types: Dict[str, Union[str, Callable]] = None,
        custom_transformations: Dict[str, Callable] = None,
        format_specs: Dict[str, Dict] = None,
        partition_cols: List[str] = None
) -> str:
    """
    Transform data from various sources to parquet with column and data type transformations.

    Args:
        data_source: Path to source file (json, csv, excel) or pandas DataFrame
        output_path: Path where parquet file will be saved
        column_mappings: Dict mapping source columns to target columns. If None, keep all columns
        data_types: Dict of column name to data type ('int', 'float', 'str', 'bool', 'date') or custom function
        custom_transformations: Dict of column name to custom transformation functions
        format_specs: Dict of column format specifications:
            - numeric_columns: Dict of column name to {"precision": int}
            - date_columns: Dict of column name to {"format": str}
        partition_cols: Columns to use for partitioning the parquet output

    Returns:
        Path to the created parquet file
    """
    # 1. Load data from source
    df = None

    if isinstance(data_source, pd.DataFrame):
        df = data_source.copy()
    elif isinstance(data_source, str):
        file_ext = os.path.splitext(data_source)[1].lower()

        if file_ext == '.json':
            # Handle JSON files
            with open(data_source, 'r', encoding='utf-8', errors='ignore') as f:
                data = json.load(f)

            # Handle different JSON structures - assuming flat or 1-level nesting
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # Try to find array in the main keys - common in API responses
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0:
                        df = pd.DataFrame(value)
                        break

                if df is None:
                    # Just use the dict as a single row
                    df = pd.DataFrame([data])

        elif file_ext == '.csv':
            df = pd.read_csv(data_source)

        elif file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(data_source)

        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

    if df is None or df.empty:
        raise ValueError("No valid data found in source")

    # 2. Apply column mappings if provided
    if column_mappings:
        # Keep only requested columns and rename them
        df = df[[col for col in column_mappings.keys() if col in df.columns]]
        df = df.rename(columns=column_mappings)

    # 3. Apply data type conversions
    if data_types:
        for col, dtype in data_types.items():
            if col not in df.columns:
                continue

            if callable(dtype):
                # Apply custom function
                df[col] = df[col].apply(dtype)
            else:
                try:
                    if dtype == 'int':
                        # Convert to float first to handle strings with decimals
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        # Round to nearest integer and convert
                        df[col] = df[col].round().astype('Int64')  # nullable integer type

                    elif dtype == 'float':
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                    elif dtype == 'bool':
                        # Handle various boolean representations
                        true_values = ['true', 'True', 't', 'T', '1', 1, 'yes', 'Yes', 'Y', 'y']
                        false_values = ['false', 'False', 'f', 'F', '0', 0, 'no', 'No', 'N', 'n']

                        df[col] = df[col].apply(
                            lambda x: True if x in true_values
                            else (False if x in false_values else None)
                        )

                    elif dtype == 'date':
                        df[col] = pd.to_datetime(df[col], errors='coerce')

                    elif dtype == 'str':
                        # Convert to string but handle NaN values
                        df[col] = df[col].astype(str).replace('nan', None)

                except Exception as e:
                    print(f"Error converting column {col} to {dtype}: {str(e)}")

    # 4. Apply format specifications for numeric precision and dates
    if format_specs:
        # Handle numeric precision
        if 'numeric_columns' in format_specs:
            for col, spec in format_specs['numeric_columns'].items():
                if col in df.columns:
                    precision = spec.get('precision', 2)
                    if pd.api.types.is_numeric_dtype(df[col]):
                        if precision == 0:
                            # Convert to integer if precision is 0
                            df[col] = df[col].round().astype('Int64')
                        else:
                            # Round to specified precision
                            df[col] = df[col].round(precision)

        # Handle date formatting (for to_parquet this may not be needed,
        # but useful if exporting to other formats)
        if 'date_columns' in format_specs:
            for col, spec in format_specs['date_columns'].items():
                if col in df.columns and pd.api.types.is_datetime64_dtype(df[col]):
                    # Store the datetime format in metadata or convert if needed
                    pass  # Parquet stores dates in a standard format

    # 5. Apply custom transformations
    if custom_transformations:
        for col, transform_func in custom_transformations.items():
            if col in df.columns:
                df[col] = df[col].apply(transform_func)

    # 6. Save to parquet
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Handle partitioning
    if partition_cols:
        # Make sure partition columns exist
        valid_partition_cols = [col for col in partition_cols if col in df.columns]
        if valid_partition_cols:
            # For simple partitioning (without additional libraries)
            for _, group_df in df.groupby(valid_partition_cols):
                # Create partition path
                partition_path = output_path.rstrip('.parquet')
                for col in valid_partition_cols:
                    val = group_df[col].iloc[0]
                    partition_path = f"{partition_path}/{col}={val}"

                # Save this partition
                partition_file = f"{partition_path}/data.parquet"
                os.makedirs(os.path.dirname(partition_file), exist_ok=True)
                group_df.to_parquet(partition_file, index=False)

            return os.path.dirname(output_path)

    # No partitioning, just save directly
    df.to_parquet(output_path, index=False)
    return output_path