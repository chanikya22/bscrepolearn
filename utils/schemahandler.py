import os
import yaml
import logging
import pandas as pd
from typing import Dict, Any, Optional


class SchemaHandler:
    """
    A lightweight schema handler that loads schema definitions from YAML files
    stored within each pipeline folder.
    """

    def __init__(self, base_dir=None):
        """
        Initialize the schema handler.

        Args:
            base_dir: Optional base directory for schema files.
                     If None, uses the current directory.
        """
        self.base_dir = base_dir or os.getcwd()
        self.logger = logging.getLogger('SchemaHandler')

    def load_schema(self, schema_path: str) -> Optional[Dict[str, Any]]:
        """
        Load a schema from a YAML file.

        Args:
            schema_path: Path to the schema YAML file

        Returns:
            Schema definition dict or None if not found/invalid
        """
        try:
            full_path = schema_path
            if not os.path.isabs(schema_path):
                full_path = os.path.join(self.base_dir, schema_path)

            if not os.path.exists(full_path):
                self.logger.warning(f"Schema file not found: {full_path}")
                return None

            with open(full_path, 'r') as f:
                schema = yaml.safe_load(f)
                self.logger.info(f"Loaded schema from {full_path}")
                return schema
        except Exception as e:
            self.logger.error(f"Error loading schema from {schema_path}: {str(e)}")
            return None

    def apply_schema(self, df: pd.DataFrame, schema: Dict[str, Any]) -> pd.DataFrame:
        """
        Apply schema to a DataFrame by converting columns to the right types.

        Args:
            df: pandas DataFrame to process
            schema: Schema definition dictionary

        Returns:
            Processed DataFrame with correct data types
        """
        if not schema:
            self.logger.warning("No schema provided, returning original DataFrame")
            return df

        processed_df = df.copy()

        for column, type_info in schema.items():
            if column in processed_df.columns:
                pg_type = type_info.get('pg_type')
                py_type = type_info.get('py_type')

                try:
                    if py_type == 'int':
                        # First convert to float to handle strings like '123.45'
                        processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce')
                        # Fill NaN values with 0 for integer columns that don't allow nulls
                        if not type_info.get('nullable', True):
                            processed_df[column] = processed_df[column].fillna(0)
                        processed_df[column] = processed_df[column].astype('Int64')  # pandas nullable integer

                    elif py_type == 'float':
                        processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce')
                        if not type_info.get('nullable', True):
                            processed_df[column] = processed_df[column].fillna(0.0)

                    elif py_type == 'bool':
                        # Handle various boolean representations
                        if processed_df[column].dtype == 'object':
                            bool_map = {
                                'true': True, 'True': True, 't': True, 'T': True,
                                '1': True, 1: True, 'yes': True, 'Yes': True, 'Y': True, 'y': True,
                                'false': False, 'False': False, 'f': False, 'F': False,
                                '0': False, 0: False, 'no': False, 'No': False, 'N': False, 'n': False
                            }
                            processed_df[column] = processed_df[column].map(bool_map)
                            if not type_info.get('nullable', True):
                                processed_df[column] = processed_df[column].fillna(False)

                    elif py_type == 'datetime':
                        processed_df[column] = pd.to_datetime(processed_df[column], errors='coerce')

                    elif py_type == 'date':
                        processed_df[column] = pd.to_datetime(processed_df[column], errors='coerce').dt.date

                    elif py_type == 'str':
                        # Handle None values properly for strings
                        processed_df[column] = processed_df[column].astype(str).replace('nan', None)

                except Exception as e:
                    self.logger.error(f"Error converting column {column} to {py_type}: {str(e)}")
                    # Keep original values if conversion fails

        return processed_df

    def generate_create_table_sql(self, table_name: str, schema: Dict[str, Any],
                                  use_iceberg: bool = False, s3_location: str = None) -> str:
        """
        Generate a CREATE TABLE SQL statement from a schema definition.

        Args:
            table_name: Name of the table
            schema: Schema definition dictionary
            use_iceberg: Whether to create an Iceberg table (defaults to False)
            s3_location: S3 location for Iceberg table (required if use_iceberg is True)

        Returns:
            SQL statement for creating the table
        """
        if not schema:
            return ""

        sql = f"CREATE TABLE {table_name} (\n"
        columns = []

        for col_name, type_info in schema.items():
            pg_type = type_info.get('pg_type', 'text')
            nullable = type_info.get('nullable', True)

            column_def = f"    {col_name} {pg_type}"
            if not nullable:
                column_def += " NOT NULL"

            columns.append(column_def)

        sql += ",\n".join(columns)
        sql += "\n)"

        # Add Iceberg-specific syntax if requested
        if use_iceberg:
            sql += "\nUSING iceberg"
            if s3_location:
                sql += f" WITH (location = '{s3_location}')"

        sql += ";"

        return sql

    @staticmethod
    def infer_schema_from_df(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Infer a schema definition from a pandas DataFrame.

        Args:
            df: pandas DataFrame

        Returns:
            Schema definition dictionary
        """
        # Map pandas dtypes to PostgreSQL and Python types
        dtype_mapping = {
            'int64': ('bigint', 'int'),
            'int32': ('integer', 'int'),
            'int16': ('smallint', 'int'),
            'int8': ('smallint', 'int'),
            'float64': ('double precision', 'float'),
            'float32': ('real', 'float'),
            'bool': ('boolean', 'bool'),
            'datetime64[ns]': ('timestamp', 'datetime'),
            'object': ('text', 'str'),
            'category': ('text', 'str')
        }

        schema = {}

        for col_name, dtype in df.dtypes.items():
            pg_type, py_type = dtype_mapping.get(str(dtype), ('text', 'str'))

            schema[col_name] = {
                'pg_type': pg_type,
                'py_type': py_type,
                'nullable': df[col_name].isna().any()
            }

        return schema

    def save_schema(self, schema: Dict[str, Any], output_path: str) -> bool:
        """
        Save a schema definition to a YAML file.

        Args:
            schema: Schema definition dictionary
            output_path: Path to save the schema YAML file

        Returns:
            True if successful, False otherwise
        """
        try:
            full_path = output_path
            if not os.path.isabs(output_path):
                full_path = os.path.join(self.base_dir, output_path)

            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(full_path), exist_ok=True)

            with open(full_path, 'w') as f:
                yaml.dump(schema, f, default_flow_style=False)

            self.logger.info(f"Saved schema to {full_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving schema to {output_path}: {str(e)}")
            return False

    def create_table(self, connection, table_name: str, schema: Dict[str, Any],
                     use_iceberg: bool = False, s3_location: str = None) -> bool:
        """
        Create a table in the database using the schema definition.

        Args:
            connection: PostgresConnector instance
            table_name: Name of the table to create
            schema: Schema definition dictionary
            use_iceberg: Whether to create an Iceberg table
            s3_location: S3 location for Iceberg table (required if use_iceberg is True)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Generate the SQL statement
            sql = self.generate_create_table_sql(table_name, schema, use_iceberg, s3_location)

            # Execute the SQL
            connection.execute_query(sql)
            self.logger.info(f"Successfully created table: {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error creating table {table_name}: {str(e)}")
            return False