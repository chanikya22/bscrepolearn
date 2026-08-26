import os
import yaml
import json
import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Union, List


class AutoSchemaGenerator:
    """
    Automatically generates schema definitions from parquet files or pandas DataFrames
    and saves them as YAML files for use in ETL pipelines.
    """

    def __init__(self, schema_dir="schemas"):
        """
        Initialize the schema generator.

        Args:
            schema_dir: Directory to store schema files
        """
        self.schema_dir = schema_dir
        self.logger = logging.getLogger('AutoSchemaGenerator')

        # Create schema directory if it doesn't exist
        if not os.path.exists(schema_dir):
            os.makedirs(schema_dir)

    def generate_from_parquet(self, parquet_path: str, table_name: str,
                              output_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate schema from a parquet file.

        Args:
            parquet_path: Path to the parquet file
            table_name: Name of the database table
            output_path: Optional custom path for the schema file
                        If None, will use schema_dir/table_name.yaml

        Returns:
            Schema definition dictionary
        """
        try:
            # Use pandas to read a sample of the data
            df = pd.read_parquet(parquet_path, engine='pyarrow')

            # If the file is large, take just a sample
            if len(df) > 1000:
                df = df.sample(n=1000)

            return self._generate_from_df(df, table_name, output_path)

        except Exception as e:
            self.logger.error(f"Error generating schema from parquet {parquet_path}: {str(e)}")
            raise

    def generate_from_json(self, json_path: str, table_name: str,
                           output_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate schema from a JSON file.

        Args:
            json_path: Path to the JSON file
            table_name: Name of the database table
            output_path: Optional custom path for the schema file
                        If None, will use schema_dir/table_name.yaml

        Returns:
            Schema definition dictionary
        """
        try:
            # Load JSON data
            with open(json_path, 'r') as f:
                data = json.load(f)

            # Convert to DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # Handle nested JSON with items
                if 'items' in data and isinstance(data['items'], list):
                    # Generate schema for main record
                    main_data = data.copy()
                    del main_data['items']
                    main_df = pd.DataFrame([main_data])
                    self._generate_from_df(main_df, table_name, output_path)

                    # Generate schema for items
                    items_df = pd.DataFrame(data['items'])
                    items_table = f"{table_name}_items"
                    items_output = None if output_path is None else f"{os.path.splitext(output_path)[0]}_items.yaml"
                    return self._generate_from_df(items_df, items_table, items_output)
                else:
                    df = pd.DataFrame([data])
            else:
                raise ValueError(f"Unsupported JSON structure in {json_path}")

            return self._generate_from_df(df, table_name, output_path)

        except Exception as e:
            self.logger.error(f"Error generating schema from JSON {json_path}: {str(e)}")
            raise

    def generate_from_df(self, df: pd.DataFrame, table_name: str,
                         output_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate schema from a pandas DataFrame.

        Args:
            df: pandas DataFrame
            table_name: Name of the database table
            output_path: Optional custom path for the schema file
                        If None, will use schema_dir/table_name.yaml

        Returns:
            Schema definition dictionary
        """
        return self._generate_from_df(df, table_name, output_path)

    def _generate_from_df(self, df: pd.DataFrame, table_name: str,
                          output_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Internal method to generate schema from a pandas DataFrame.

        Args:
            df: pandas DataFrame
            table_name: Name of the database table
            output_path: Optional custom path for the schema file

        Returns:
            Schema definition dictionary
        """
        # Map pandas dtypes to PostgreSQL and Python types
        schema = {}

        for col_name, dtype in df.dtypes.items():
            # Infer if nullable - convert numpy bool to Python bool
            is_nullable = bool(df[col_name].isna().any())

            # Get sample values for better type determination
            sample_values = df[col_name].dropna().head(10).tolist()

            # Convert numpy values to Python native types
            sample_values = [self._convert_numpy_to_python(val) for val in sample_values]

            # Get the schema entry for this column
            schema_entry = self._infer_column_types(dtype, col_name, sample_values, is_nullable)
            schema[col_name] = schema_entry

        # Add primary key if we can identify one
        self._add_primary_key(schema, df, table_name)

        # Add ETL tracking fields
        schema['etl_inserted_at'] = {
            'pg_type': 'timestamp with time zone',
            'py_type': 'datetime',
            'nullable': False,
            'default': 'CURRENT_TIMESTAMP'
        }

        schema['etl_updated_at'] = {
            'pg_type': 'timestamp with time zone',
            'py_type': 'datetime',
            'nullable': False,
            'default': 'CURRENT_TIMESTAMP'
        }

        schema['etl_batch_id'] = {
            'pg_type': 'character varying(50)',
            'py_type': 'str',
            'nullable': True
        }

        schema['etl_source'] = {
            'pg_type': 'character varying(50)',
            'py_type': 'str',
            'nullable': True
        }

        # Clean up the schema before saving
        self._clean_schema(schema)

        # Save schema to file if output_path is provided
        if output_path is None:
            output_path = os.path.join(self.schema_dir, f"{table_name}.yaml")

        # Custom YAML dumper that doesn't use anchors and references
        class NoAliasDumper(yaml.SafeDumper):
            def ignore_aliases(self, data):
                return True

        with open(output_path, 'w') as f:
            yaml.dump(schema, f, default_flow_style=False, Dumper=NoAliasDumper)

        self.logger.info(f"Generated schema for {table_name} saved to {output_path}")

        return schema

    def _convert_numpy_to_python(self, value: Any) -> Any:
        """
        Convert numpy types to standard Python types.

        Args:
            value: Value to convert

        Returns:
            Converted value
        """
        if value is None:
            return None

        # Handle numpy types
        if isinstance(value, np.integer):
            return int(value)
        elif isinstance(value, np.floating):
            return float(value)
        elif isinstance(value, np.bool_):
            return bool(value)
        elif isinstance(value, np.datetime64):
            return pd.Timestamp(value).to_pydatetime()
        elif isinstance(value, np.ndarray):
            return value.tolist()
        # Fixed: Use more specific type checks that work with NumPy 2.0
        elif hasattr(np, 'bytes_') and isinstance(value, np.bytes_):
            return value.decode('utf-8', errors='replace')
        elif hasattr(np, 'str_') and isinstance(value, np.str_):
            return str(value)
        # Handle scalar strings as bytes
        elif isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')

        # Fall back to string conversion for other NumPy types
        if hasattr(value, 'dtype') and hasattr(value, 'item'):
            return value.item()

        return value

    def _infer_column_types(self, dtype, col_name: str, sample_values: List, nullable: bool) -> Dict[str, Any]:
        """
        Infer PostgreSQL and Python types for a column based on its dtype and sample values.

        Args:
            dtype: pandas dtype
            col_name: Column name
            sample_values: List of sample values from the column
            nullable: Whether the column contains null values

        Returns:
            Dictionary with pg_type, py_type, nullable, and optional constraints
        """
        # Default schema entry
        schema_entry = {
            'nullable': nullable
        }

        # Basic dtype mapping
        dtype_str = str(dtype)

        # First check if string fields might actually be numeric
        if dtype_str == 'object' and sample_values:
            # Check if all non-empty values are numeric strings
            numeric_strings = True
            for val in sample_values:
                if val is not None and str(val).strip() != '':
                    try:
                        float(val)
                    except (ValueError, TypeError):
                        numeric_strings = False
                        break

            # If they're all numeric strings, treat as numeric
            if numeric_strings and sample_values:
                # Check if they're integers
                all_integers = True
                for val in sample_values:
                    if val is not None and str(val).strip() != '':
                        try:
                            float_val = float(val)
                            if float_val != int(float_val):
                                all_integers = False
                                break
                        except (ValueError, TypeError):
                            all_integers = False
                            break

                if all_integers:
                    # They're integer strings
                    return self._infer_column_types(np.dtype('int64'), col_name,
                                                    [int(float(v)) if v is not None and str(v).strip() != '' else None
                                                     for v in sample_values],
                                                    nullable)
                else:
                    # They're float strings
                    return self._infer_column_types(np.dtype('float64'), col_name,
                                                    [float(v) if v is not None and str(v).strip() != '' else None
                                                     for v in sample_values],
                                                    nullable)

        if 'int' in dtype_str:
            # Check the magnitude to determine the appropriate integer type
            if sample_values:
                max_val = max([abs(val) if val is not None else 0 for val in sample_values])
                if max_val < 32767:  # 2^15 - 1
                    schema_entry['pg_type'] = 'smallint'
                elif max_val < 2147483647:  # 2^31 - 1
                    schema_entry['pg_type'] = 'integer'
                else:
                    schema_entry['pg_type'] = 'bigint'
            else:
                schema_entry['pg_type'] = 'integer'

            schema_entry['py_type'] = 'int'

        elif 'float' in dtype_str:
            # Check if this might be a decimal/numeric field
            if sample_values:
                decimal_places = max([
                    len(str(val).split('.')[-1]) if val is not None and '.' in str(val) else 0
                    for val in sample_values
                ])

                non_null_values = [val for val in sample_values if val is not None]
                if non_null_values:
                    max_val = max([abs(val) for val in non_null_values])

                    if decimal_places > 0 and max_val < 1000000:
                        precision = len(str(int(max_val))) + decimal_places
                        precision = min(precision, 15)  # PostgreSQL max precision
                        schema_entry['pg_type'] = f'numeric({precision},{decimal_places})'
                    else:
                        schema_entry['pg_type'] = 'double precision'
                else:
                    schema_entry['pg_type'] = 'double precision'
            else:
                schema_entry['pg_type'] = 'double precision'

            schema_entry['py_type'] = 'float'

        elif dtype_str == 'bool':
            schema_entry['pg_type'] = 'boolean'
            schema_entry['py_type'] = 'bool'

        elif 'datetime' in dtype_str:
            # Check if there are time components
            if sample_values:
                has_time = any([
                    val.time().hour != 0 or val.time().minute != 0 or val.time().second != 0
                    for val in sample_values if val is not None and hasattr(val, 'time')
                ])
                if has_time:
                    schema_entry['pg_type'] = 'timestamp'
                else:
                    schema_entry['pg_type'] = 'date'
            else:
                schema_entry['pg_type'] = 'timestamp'

            schema_entry['py_type'] = 'datetime'

        elif dtype_str == 'object':
            # This could be string, JSON, or other types
            if sample_values:
                # Check if this looks like JSON
                json_like = True
                for val in sample_values:
                    if val is not None:
                        if isinstance(val, (dict, list)):
                            continue
                        elif isinstance(val, str) and (val.startswith('{') or val.startswith('[')):
                            try:
                                json.loads(val)
                            except:
                                json_like = False
                                break
                        else:
                            json_like = False
                            break

                if json_like and sample_values:
                    schema_entry['pg_type'] = 'jsonb'
                    schema_entry['py_type'] = 'json'
                else:
                    # Calculate max string length for non-empty values
                    non_empty_values = [str(val) for val in sample_values if val is not None and str(val).strip() != '']

                    if non_empty_values:
                        max_len = max([len(val) for val in non_empty_values])

                        # Determine if this should be text or varchar
                        if max_len > 1000:
                            schema_entry['pg_type'] = 'text'
                        else:
                            # Add some buffer to the max length, but ensure it's at least 1
                            varchar_len = max(1, min(int(max_len * 1.5), 1000))
                            schema_entry['pg_type'] = f'character varying({varchar_len})'
                    else:
                        # Default for empty strings
                        schema_entry['pg_type'] = 'character varying(255)'

                    schema_entry['py_type'] = 'str'
            else:
                schema_entry['pg_type'] = 'text'
                schema_entry['py_type'] = 'str'

        else:
            # Default to text for unknown types
            schema_entry['pg_type'] = 'text'
            schema_entry['py_type'] = 'str'

        return schema_entry

    def _add_primary_key(self, schema: Dict[str, Any], df: pd.DataFrame, table_name: str):
        """
        Try to identify primary key columns and add primary key constraint.

        Args:
            schema: Schema dictionary to update
            df: pandas DataFrame
            table_name: Name of the table
        """
        # List of common primary key column names
        pk_candidates = ['id', f'{table_name}_id', 'key', 'code', 'orderId', 'order_id']

        # Add more specific candidates based on the table name
        if '_' in table_name:
            base_name = table_name.split('_')[0]
            pk_candidates.extend([f'{base_name}_id', f'{base_name}Id'])

        # Try to identify primary key columns
        for col in pk_candidates:
            if col in df.columns and df[col].notna().all() and df[col].is_unique:
                # Found a primary key candidate
                if col in schema:
                    schema[col]['primary_key'] = True
                    schema[col]['nullable'] = False
                    break

        # If the table seems to be a child table (contains items, lines, etc.)
        if any(term in table_name.lower() for term in ['item', 'line', 'detail']):
            # Try to identify parent key + child key for composite primary key
            parent_candidates = ['order_id', 'orderId', 'parent_id', 'parentId', 'header_id', 'headerId']
            child_candidates = ['line_no', 'lineNo', 'item_id', 'itemId', 'detail_id', 'detailId', 'line_number',
                                'lineNumber', 'lineno']

            for parent_col in parent_candidates:
                if parent_col in df.columns and df[parent_col].notna().all():
                    for child_col in child_candidates:
                        if child_col in df.columns and df[child_col].notna().all():
                            # Check if combination is unique
                            if df.duplicated(subset=[parent_col, child_col]).sum() == 0:
                                # Found a composite primary key
                                if parent_col in schema:
                                    schema[parent_col]['composite_key'] = True
                                    schema[parent_col]['nullable'] = False
                                if child_col in schema:
                                    schema[child_col]['composite_key'] = True
                                    schema[child_col]['nullable'] = False
                                break
                    # Break out of outer loop if we found a composite key
                    if any('composite_key' in schema.get(col, {}) for col in df.columns):
                        break

    def _clean_schema(self, schema: Dict[str, Any]):
        """
        Clean up the schema to fix common issues.

        Args:
            schema: Schema dictionary to clean
        """
        for col_name, props in schema.items():
            # Fix character varying with length 0
            if 'pg_type' in props and 'character varying(0)' in props['pg_type']:
                props['pg_type'] = 'character varying(255)'

            # Fix string fields that look like they should be numeric
            if 'py_type' in props and props['py_type'] == 'str' and 'pg_type' in props:
                if col_name.lower() in ['quantity', 'qty', 'price', 'amount', 'cost',
                                        'rate', 'discount', 'tax', 'total', 'subtotal',
                                        'orderqty', 'shippedqty', 'returnqty', 'cancelledqty',
                                        'unitprice', 'discountamt', 'taxamount']:
                    # These field names suggest numeric types
                    if 'numeric' not in props['pg_type'] and 'int' not in props['pg_type'] and 'float' not in props[
                        'pg_type']:
                        props['pg_type'] = 'numeric(10,2)'
                        props['py_type'] = 'float'

            # Ensure nullable is a Python bool, not a numpy bool
            if 'nullable' in props:
                props['nullable'] = bool(props['nullable'])


# Example usage in a standalone script
if __name__ == "__main__":
    import argparse

    # Configure logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Generate schema from data files")
    parser.add_argument("file_path", help="Path to parquet, JSON, or other data file")
    parser.add_argument("table_name", help="Name of the target database table")
    parser.add_argument("--output", "-o", help="Output path for schema file")
    parser.add_argument("--schema-dir", "-d", default="schemas", help="Schema directory")

    args = parser.parse_args()

    # Create generator
    generator = AutoSchemaGenerator(schema_dir=args.schema_dir)

    # Generate schema based on file type
    file_ext = os.path.splitext(args.file_path)[1].lower()

    if file_ext == '.parquet':
        generator.generate_from_parquet(args.file_path, args.table_name, args.output)
    elif file_ext == '.json':
        generator.generate_from_json(args.file_path, args.table_name, args.output)
    else:
        raise ValueError(f"Unsupported file extension: {file_ext}")

    logging.info("Schema generation complete")