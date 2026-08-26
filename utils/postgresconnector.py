import os
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import logging
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import execute_values, execute_batch
from dotenv import load_dotenv, find_dotenv
from typing import Iterator
import numpy as np
import json
import uuid
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresConnector:
    """
    A utility class for handling PostgreSQL database operations with support for large-scale ETL.
    """

    def __init__(self,
                 host: str = None,
                 database: str = None,
                 user: str = None,
                 password: str = None,
                 port: str = "5432",
                 env_file: str = ".env",
                 db_prefix: str = ""):
        """
        Initialize database connection parameters.
        Can either pass parameters directly or use environment variables.

        Args:
            host: Database host address
            database: Database name
            user: Database username
            password: Database password
            port: Database port (default: "5432")
            env_file: Path to .env file with connection parameters
            db_prefix: Prefix for database environment variables (e.g., "SECONDARY_")
        """
        # Load environment variables if file exists
        dotenv_path = find_dotenv(env_file) if env_file else find_dotenv()
        if os.path.exists(dotenv_path):
            load_dotenv(dotenv_path)

        # Map environment variable names based on prefix
        host_var = f"{db_prefix}DB_HOST"
        name_var = f"{db_prefix}DB_NAME"
        user_var = f"{db_prefix}DB_USER"
        password_var = f"{db_prefix}DB_PASSWORD"
        port_var = f"{db_prefix}DB_PORT"

        self.db_params = {
            'host': host or os.getenv(host_var),
            'database': database or os.getenv(name_var),
            'user': user or os.getenv(user_var),
            'password': password or os.getenv(password_var),
            'port': port or os.getenv(port_var, '5432')
        }

        # Validate connection parameters
        missing_params = [k for k, v in self.db_params.items() if not v]
        if missing_params:
            raise ValueError(f"Missing required database parameters: {', '.join(missing_params)}")

        self._engine = None

        # Log connection info without exposing password
        safe_params = {k: v if k != 'password' else '****' for k, v in self.db_params.items()}
        logger.info(f"PostgresConnector initialized with parameters: {safe_params}")

    @property
    def engine(self) -> Engine:
        """Lazy loading of SQLAlchemy engine."""
        if self._engine is None:
            connection_string = (f"postgresql://{self.db_params['user']}:{self.db_params['password']}"
                                 f"@{self.db_params['host']}:{self.db_params['port']}"
                                 f"/{self.db_params['database']}")
            self._engine = create_engine(connection_string)
        return self._engine

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()

    @contextmanager
    def transaction(self):
        """Context manager for a single transaction with multiple operations."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                yield cur

    def get_table_schema(self, table_name):
        """
        Get the column schema information for a specific table.

        Args:
            table_name: The name of the table

        Returns:
            Dictionary mapping column names to their PostgreSQL data types
        """
        query = """
        SELECT column_name, data_type, udt_name
        FROM information_schema.columns
        WHERE table_name = :table_name
        """

        result = self.read_query(query, {"table_name": table_name})

        schema = {}
        for _, row in result.iterrows():
            schema[row['column_name']] = {
                'data_type': row['data_type'],
                'udt_name': row['udt_name']
            }

        return schema

    def create_temp_table_from_df(self, temp_table: str, df: pd.DataFrame, target_schema=None):
        """
        Create a temporary table based on DataFrame structure.
        If target_schema is provided, tries to match column types to target schema.

        Args:
            temp_table: Name for the temporary table
            df: DataFrame to base structure on
            target_schema: Optional schema from target table to match types
        """
        # Map pandas dtypes to PostgreSQL types
        type_mapping = {
            'int64': 'BIGINT',
            'int32': 'INTEGER',
            'int16': 'SMALLINT',
            'int8': 'SMALLINT',
            'float64': 'DOUBLE PRECISION',
            'float32': 'REAL',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'object': 'TEXT',
            'category': 'TEXT'
        }

        # Build column definitions
        columns = []
        for col_name, dtype in df.dtypes.items():
            # Use target schema type if available
            if target_schema and col_name in target_schema:
                pg_type = target_schema[col_name]['data_type'].upper()
                if pg_type == 'USER-DEFINED' and target_schema[col_name]['udt_name'] == 'hstore':
                    pg_type = 'hstore'
                elif pg_type == 'USER-DEFINED' and target_schema[col_name]['udt_name'].startswith('_'):
                    pg_type = 'TEXT[]'  # Handle arrays as text arrays for safety
                elif pg_type == 'CHARACTER VARYING':
                    pg_type = 'TEXT'
                # Add more mappings as needed
            else:
                pg_type = type_mapping.get(str(dtype), 'TEXT')

            columns.append(f"\"{col_name}\" {pg_type}")

        with self.transaction() as cur:
            # Create the temporary table
            create_query = f"""
            CREATE TEMPORARY TABLE {temp_table} (
                {','.join(columns)}
            ) ON COMMIT DROP;
            """
            cur.execute(create_query)
            logger.info(f"Created temporary table {temp_table} from DataFrame")

    def convert_df_types(self, df: pd.DataFrame, target_schema: Dict[str, dict]) -> pd.DataFrame:
        """
        Convert DataFrame column types to match target table schema.

        Args:
            df: Source DataFrame
            target_schema: Target table schema

        Returns:
            DataFrame with converted types
        """
        df_copy = df.copy()

        for col_name in df_copy.columns:
            if col_name in target_schema:
                data_type = target_schema[col_name]['data_type']

                try:
                    if data_type in ('timestamp without time zone', 'timestamp with time zone', 'timestamp'):
                        # Handle timestamp type conversion
                        df_copy[col_name] = pd.to_datetime(df_copy[col_name], errors='coerce')

                    elif data_type in ('integer', 'bigint', 'smallint'):
                        # Handle integer type conversion
                        df_copy[col_name] = pd.to_numeric(df_copy[col_name], errors='coerce').astype('Int64')

                    elif data_type in ('numeric', 'double precision', 'real'):
                        # Handle float type conversion
                        df_copy[col_name] = pd.to_numeric(df_copy[col_name], errors='coerce')

                    elif data_type == 'boolean':
                        # Handle boolean conversion
                        if df_copy[col_name].dtype == 'object':
                            bool_map = {
                                'true': True, 'True': True, 't': True, 'T': True,
                                '1': True, 1: True, 'yes': True, 'Yes': True, 'Y': True, 'y': True,
                                'false': False, 'False': False, 'f': False, 'F': False,
                                '0': False, 0: False, 'no': False, 'No': False, 'N': False, 'n': False
                            }
                            df_copy[col_name] = df_copy[col_name].map(bool_map)

                    # Add more conversions as needed

                except Exception as e:
                    logger.warning(f"Could not convert column {col_name} to {data_type}: {str(e)}")

        return df_copy

    # def merge_data_with_type_conversion(self, df, target_table, unique_key=None, chunk_size=1000):
    #     """
    #     Merge a DataFrame into a target table with proper type conversion.
    #     Keeps everything in a single transaction to avoid temp table visibility issues.
    #
    #     Args:
    #         df: DataFrame to merge
    #         target_table: Target table name
    #         unique_key: Primary key or unique column for merge operations
    #         chunk_size: Batch size for processing
    #
    #     Returns:
    #         Number of affected rows
    #     """
    #     try:
    #         # Get target table schema first
    #         target_schema = self.get_table_schema(target_table)
    #
    #         # Convert DataFrame types to match target schema
    #         converted_df = self.convert_df_types(df, target_schema)
    #
    #         # Generate a unique name for the temporary table
    #         temp_table = f"temp_{target_table.replace('.', '_')}_{uuid.uuid4().hex[:8]}"
    #         columns = list(converted_df.columns)
    #
    #         # Prepare records for insertion
    #         records = []
    #         for _, row in converted_df.iterrows():
    #             record = []
    #             for val in row:
    #                 # First check if it's an array-like object
    #                 if isinstance(val, (np.ndarray, pd.Series)):
    #                     if val.size == 0:
    #                         record.append(None)
    #                     else:
    #                         record.append(val.tolist())  # Convert array to list
    #                 # Then check if it's NaN
    #                 elif pd.isna(val):
    #                     record.append(None)
    #                 else:
    #                     record.append(val)
    #             records.append(tuple(record))
    #
    #         # All operations in a single connection and transaction
    #         with self.get_connection() as conn:
    #             with conn.cursor() as cur:
    #                 # Map pandas dtypes to PostgreSQL types
    #                 type_mapping = {
    #                     'int64': 'BIGINT',
    #                     'int32': 'INTEGER',
    #                     'int16': 'SMALLINT',
    #                     'int8': 'SMALLINT',
    #                     'float64': 'DOUBLE PRECISION',
    #                     'float32': 'REAL',
    #                     'bool': 'BOOLEAN',
    #                     'datetime64[ns]': 'TIMESTAMP',
    #                     'object': 'TEXT',
    #                     'category': 'TEXT'
    #                 }
    #
    #                 # Build column definitions for temporary table
    #                 columns_sql = []
    #                 for col_name, dtype in converted_df.dtypes.items():
    #                     # Use target schema type if available
    #                     if col_name in target_schema:
    #                         pg_type = target_schema[col_name]['data_type'].upper()
    #                         if pg_type == 'USER-DEFINED' and target_schema[col_name]['udt_name'] == 'hstore':
    #                             pg_type = 'hstore'
    #                         elif pg_type == 'USER-DEFINED' and target_schema[col_name]['udt_name'].startswith('_'):
    #                             pg_type = 'TEXT[]'  # Handle arrays as text arrays for safety
    #                         elif pg_type == 'CHARACTER VARYING':
    #                             pg_type = 'TEXT'
    #                         # Add more mappings as needed
    #                     else:
    #                         pg_type = type_mapping.get(str(dtype), 'TEXT')
    #
    #                     columns_sql.append(f"\"{col_name}\" {pg_type}")
    #
    #                 # Create the temporary table
    #                 create_query = f"""
    #                 CREATE TEMPORARY TABLE {temp_table} (
    #                     {','.join(columns_sql)}
    #                 );
    #                 """
    #                 cur.execute(create_query)
    #                 logger.info(f"Created temporary table {temp_table} from DataFrame")
    #
    #                 # Insert records into temp table
    #                 placeholders = ",".join(["%s"] * len(columns))
    #                 insert_sql = f"INSERT INTO {temp_table} ({','.join([f'"{col}"' for col in columns])}) VALUES ({placeholders})"
    #
    #                 # Execute batch insert
    #                 execute_batch(cur, insert_sql, records, page_size=chunk_size)
    #                 logger.info(f"Inserted {len(records)} records into temporary table")
    #
    #                 # Now merge from temp table to target table
    #                 affected_rows = 0
    #
    #                 if unique_key:
    #                     # Perform merge operation
    #                     merge_sql = f"""
    #                     WITH deleted AS (
    #                         DELETE FROM {target_table}
    #                         WHERE {unique_key} IN (SELECT {unique_key} FROM {temp_table})
    #                         RETURNING *
    #                     )
    #                     INSERT INTO {target_table} ({','.join([f'"{col}"' for col in columns])})
    #                     SELECT {','.join([f'"{col}"' for col in columns])} FROM {temp_table};
    #                     """
    #                     cur.execute(merge_sql)
    #                     affected_rows = cur.rowcount
    #                     logger.info(f"Merged data with {affected_rows} records affected using key: {unique_key}")
    #                 else:
    #                     # Simple append if no unique key provided
    #                     insert_sql = f"""
    #                     INSERT INTO {target_table} ({','.join([f'"{col}"' for col in columns])})
    #                     SELECT {','.join([f'"{col}"' for col in columns])} FROM {temp_table};
    #                     """
    #                     cur.execute(insert_sql)
    #                     affected_rows = cur.rowcount
    #                     logger.info(f"Appended {affected_rows} records to target table")
    #
    #         # Return the number of affected rows
    #         return affected_rows
    #
    #     except Exception as e:
    #         logger.error(f"Error in merge_data_with_type_conversion: {str(e)}")
    #         import traceback
    #         logger.error(f"Traceback: {traceback.format_exc()}")
    #         raise

    def merge_df_to_table(self, df, target_table, unique_key=None, chunk_size=1000):
        """
        Merge a DataFrame into a target table.

        Args:
            df: DataFrame to merge
            target_table: Target table name
            unique_key: Primary key or unique column for merge operations
            chunk_size: Batch size for processing

        Returns:
            Number of affected rows
        """
        import uuid
        import numpy as np
        import logging

        try:
            # Generate a unique name for the temporary table
            temp_table = f"temp_{target_table.replace('.', '_')}_{uuid.uuid4().hex[:8]}"
            columns = list(df.columns)

            # Create the temporary table directly
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Map pandas dtypes to PostgreSQL types
                    dtype_mapping = {
                        'int64': 'BIGINT',
                        'int32': 'INTEGER',
                        'int16': 'SMALLINT',
                        'int8': 'SMALLINT',
                        'float64': 'DOUBLE PRECISION',
                        'float32': 'REAL',
                        'bool': 'BOOLEAN',
                        'datetime64[ns]': 'TIMESTAMP',
                        'object': 'TEXT',
                        'category': 'TEXT'
                    }

                    # Create column definitions for temp table
                    column_defs = []
                    for col_name, dtype in df.dtypes.items():
                        pg_type = dtype_mapping.get(str(dtype), 'TEXT')
                        column_defs.append(f"\"{col_name}\" {pg_type}")

                    # Create temporary table
                    create_query = f"""
                    CREATE TEMPORARY TABLE {temp_table} (
                        {','.join(column_defs)}
                    ) ON COMMIT DROP;
                    """
                    cur.execute(create_query)
                    logging.info(f"Created temporary table {temp_table}")

                    # Convert DataFrame to records safely
                    records = []
                    for _, row in df.iterrows():
                        record = []
                        for val in row:
                            # First check if it's an array-like object
                            if isinstance(val, (np.ndarray, pd.Series)):
                                if val.size == 0:
                                    record.append(None)
                                else:
                                    record.append(val.tolist())  # Convert array to list
                            # Then check if it's NaN
                            elif pd.isna(val):
                                record.append(None)
                            else:
                                record.append(val)
                        records.append(tuple(record))

                    # Insert records into temp table
                    placeholders = ",".join(["%s"] * len(columns))
                    insert_sql = f"INSERT INTO {temp_table} ({','.join(columns)}) VALUES ({placeholders})"

                    # Execute batch insert
                    from psycopg2.extras import execute_batch
                    execute_batch(cur, insert_sql, records, page_size=chunk_size)
                    logging.info(f"Inserted {len(records)} records into temporary table")

                    # Now merge from temp table to target table
                    affected_rows = 0

                    if unique_key:
                        # Perform merge operation
                        merge_sql = f"""
                        WITH deleted AS (
                            DELETE FROM {target_table}
                            WHERE {unique_key} IN (SELECT {unique_key} FROM {temp_table})
                            RETURNING *
                        )
                        INSERT INTO {target_table}
                        SELECT * FROM {temp_table};
                        """
                        cur.execute(merge_sql)
                        affected_rows = cur.rowcount
                        logging.info(f"Merged data with {affected_rows} records affected using key: {unique_key}")
                    else:
                        # Simple append if no unique key provided
                        insert_sql = f"""
                        INSERT INTO {target_table}
                        SELECT * FROM {temp_table};
                        """
                        cur.execute(insert_sql)
                        affected_rows = cur.rowcount
                        logging.info(f"Appended {affected_rows} records to target table")

            # Return the number of affected rows
            return affected_rows

        except Exception as e:
            logging.error(f"Error in merge_df_to_table: {str(e)}")
            raise

    def merge_data_with_temp_table(self,
                                   df: pd.DataFrame,
                                   target_table: str,
                                   unique_key: str = None,
                                   temp_table_name: str = None) -> int:
        """
        Create a temporary table from DataFrame and merge data into target table in a single transaction.
        Performs data type conversion based on the target table schema.

        Args:
            df: DataFrame containing the data to transfer
            target_table: Target table to merge data into
            temp_table_name: Optional name for the temporary table. If not provided, a random name will be generated.
            unique_key: Optional primary key for more selective merging
                       If provided, only matching records will be replaced
                       If None, uses the CrunchyBridge pattern of transferring all records

        Returns:
            Number of records affected
        """
        try:
            if temp_table_name is None:
                import uuid
                temp_table_name = f"temp_{uuid.uuid4().hex[:16]}"

            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # First, get the target table schema to perform accurate type conversion
                    schema_query = f"""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns
                    WHERE table_name = '{target_table}'
                    ORDER BY ordinal_position;
                    """
                    cur.execute(schema_query)
                    target_schema = cur.fetchall()

                    # Create a mapping of column names to PostgreSQL data types
                    column_types = {col[0]: col[1] for col in target_schema}

                    # Create the temporary table like the target table
                    create_query = f"""
                    CREATE TEMPORARY TABLE {temp_table_name} 
                    ON COMMIT DROP 
                    AS SELECT * FROM {target_table} WHERE 1=0
                    """
                    cur.execute(create_query)
                    logger.info(f"Created temporary table {temp_table_name} based on {target_table}")

                    # Insert data into temporary table
                    from psycopg2.extras import execute_values

                    # Column names for the INSERT
                    cols = [f'"{col}"' for col in df.columns]

                    # Insert statement
                    insert_query = f"""
                    INSERT INTO {temp_table_name} ({', '.join(cols)})
                    VALUES %s
                    """

                    values = []
                    for _, row in df.iterrows():
                        # Convert each row to a tuple with proper type conversion
                        row_values = []
                        for col_name, val in row.items():
                            # Skip if value is None or NaN
                            if pd.isna(val):
                                row_values.append(None)
                                continue

                            # Get PostgreSQL data type for this column if available
                            pg_type = column_types.get(col_name.lower(), None)

                            # Perform type conversion based on PostgreSQL data type
                            if pg_type:
                                try:
                                    if 'int' in pg_type:
                                        val = int(float(val)) if val != '' else None
                                    elif 'float' in pg_type or 'numeric' in pg_type or 'double' in pg_type or 'real' in pg_type:
                                        val = float(val) if val != '' else None
                                    elif pg_type == 'boolean':
                                        if isinstance(val, str):
                                            val = val.lower() in ('true', 't', 'yes', 'y', '1')
                                        else:
                                            val = bool(val)
                                    elif 'timestamp' in pg_type or 'date' in pg_type:
                                        if isinstance(val, str) and val:
                                            val = pd.to_datetime(val)
                                        # pandas timestamps are already compatible
                                    elif 'json' in pg_type:
                                        if isinstance(val, str):
                                            # Ensure it's valid JSON
                                            val = json.loads(val)
                                        # If it's a dict or list, psycopg2 will handle conversion
                                    elif 'array' in pg_type:
                                        # Convert to list if it's not already
                                        if isinstance(val, str):
                                            # Simple array parsing for string representation
                                            val = val.strip('{}').split(',')
                                        elif not isinstance(val, (list, tuple, np.ndarray)):
                                            val = [val]
                                except (ValueError, TypeError) as e:
                                    logger.warning(
                                        f"Type conversion failed for column {col_name}, value '{val}': {str(e)}")
                                    # Keep original value if conversion fails

                            row_values.append(val)

                        values.append(tuple(row_values))

                    # Insert the data
                    execute_values(cur, insert_query, values)

                    # Perform the merge
                    if unique_key:
                        # Selective merge using unique key
                        merge_sql = f"""
                        WITH deleted_records AS (
                            DELETE FROM {target_table}
                            WHERE {unique_key} IN (SELECT {unique_key} FROM {temp_table_name})
                            RETURNING *
                        ),
                        inserted_records AS (
                            INSERT INTO {target_table}
                            SELECT * FROM {temp_table_name}
                            RETURNING {unique_key}
                        )
                        SELECT 
                            (SELECT COUNT(*) FROM deleted_records) AS records_deleted,
                            (SELECT COUNT(*) FROM inserted_records) AS records_inserted;
                        """

                        cur.execute(merge_sql)
                        result = cur.fetchone()

                        if result:
                            deleted, inserted = result
                            logger.info(
                                f"Merged data: {deleted} records deleted, {inserted} records inserted in {target_table}")
                            return inserted
                        return 0

                    else:
                        # CrunchyBridge pattern - complete transfer of all staging data
                        transfer_sql = f"""
                        WITH new_rows AS (
                            DELETE FROM {temp_table_name} RETURNING *
                        )
                        INSERT INTO {target_table} 
                        SELECT * FROM new_rows;
                        """

                        cur.execute(transfer_sql)
                        affected_rows = cur.rowcount

                        logger.info(f"Transferred {affected_rows} records from {temp_table_name} to {target_table}")
                        return affected_rows

        except Exception as e:
            logger.error(f"Error in merge_data_with_temp_table: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    def merge_data(self,
                   source_table: str,
                   target_table: str,
                   unique_key: str = None):
        """
        Transfer data from staging table to target table in a single transaction.

        This implementation follows the CrunchyBridge pattern of moving data
        from a staging table to a target table atomically.

        Args:
            source_table: Source/staging table containing the data to transfer
            target_table: Target table to merge data into
            unique_key: Optional primary key for more selective merging
                        If provided, only matching records will be replaced
                        If None, uses the CrunchyBridge pattern of transferring all records

        Returns:
            Number of records affected
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    if unique_key:
                        # Selective merge using unique key
                        merge_sql = f"""
                        WITH deleted_records AS (
                            DELETE FROM {target_table}
                            WHERE {unique_key} IN (SELECT {unique_key} FROM {source_table})
                            RETURNING *
                        ),
                        inserted_records AS (
                            INSERT INTO {target_table}
                            SELECT * FROM {source_table}
                            RETURNING {unique_key}
                        )
                        SELECT 
                            (SELECT COUNT(*) FROM deleted_records) AS records_deleted,
                            (SELECT COUNT(*) FROM inserted_records) AS records_inserted;
                        """

                        cur.execute(merge_sql)
                        result = cur.fetchone()

                        if result:
                            deleted, inserted = result
                            logger.info(
                                f"Merged data: {deleted} records deleted, {inserted} records inserted in {target_table}")
                            return inserted
                        return 0

                    else:
                        # CrunchyBridge pattern - complete transfer of all staging data
                        transfer_sql = f"""
                        WITH new_rows AS (
                            DELETE FROM {source_table} RETURNING *
                        )
                        INSERT INTO {target_table} 
                        SELECT * FROM new_rows;
                        """

                        cur.execute(transfer_sql)
                        affected_rows = cur.rowcount

                        logger.info(f"Transferred {affected_rows} records from {source_table} to {target_table}")
                        return affected_rows

        except Exception as e:
            logger.error(f"Error in merge_data: {str(e)}")
            raise

    def bulk_insert(self,
                    table_name: str,
                    data: Union[pd.DataFrame, List[Dict[str, Any]]],
                    chunk_size: int = 10000,
                    if_exists: str = 'append') -> int:
        """
        Efficiently insert large datasets into PostgreSQL using pandas' to_sql method.
        Handles numpy arrays and other special data types by converting them to Python native types.

        Args:
            table_name: Name of the target table
            data: DataFrame or list of dictionaries to insert
            chunk_size: Number of records to insert in each batch
            if_exists: How to behave if the table already exists ('fail', 'replace', or 'append')

        Returns:
            Number of records inserted
        """
        try:
            import numpy as np

            # Convert list of dicts to DataFrame if necessary
            if isinstance(data, list):
                data = pd.DataFrame(data)

            total_rows = len(data)

            # Create a deep copy to avoid modifying the original DataFrame
            df_copy = data.copy()

            # Function to convert numpy arrays and other problematic types to Python native types
            def convert_types(x):
                # Handle numpy arrays
                if isinstance(x, np.ndarray):
                    return x.tolist()  # Convert numpy array to Python list
                # Handle other numpy types
                elif isinstance(x, (np.integer, np.int64, np.int32, np.int16, np.int8)):
                    return int(x)
                elif isinstance(x, (np.float64, np.float32, np.float16)):
                    return float(x)
                elif isinstance(x, (np.bool_)):
                    return bool(x)
                elif isinstance(x, (np.datetime64, np.timedelta64)):
                    return pd.Timestamp(x).to_pydatetime()
                # Return original value for other types
                return x

            # Apply type conversion to all columns
            for col in df_copy.columns:
                df_copy[col] = df_copy[col].apply(convert_types)

            logger.info(f"Starting bulk insert of {total_rows} rows into {table_name}")

            # Use to_sql with method='multi' for better performance
            df_copy.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=chunk_size
            )

            logger.info(f"Successfully inserted {total_rows} records into {table_name}")
            return total_rows

        except Exception as e:
            logger.error(f"Error in bulk_insert: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    def table_exists(self, table_name: str, schema: str = 'public') -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Table name to check
            schema: Schema name (default: 'public')

        Returns:
            True if table exists, False otherwise
        """
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = :schema
            AND table_name = :table_name
        );
        """
        params = {
            "schema": schema,
            "table_name": table_name
        }

        result = self.read_query(query, params)
        return result.iloc[0, 0]

    def create_table_like(self, new_table: str, template_table: str):
        """
        Create a new table with the same structure as an existing table.

        Args:
            new_table: Name for the new table
            template_table: Existing table to copy structure from
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {new_table} 
        (LIKE {template_table} INCLUDING ALL);
        """
        self.execute_query(query)
        logger.info(f"Created table {new_table} based on {template_table}")

    def read_query(self,
                   query: str,
                   params: Optional[Dict[str, Any]] = None,
                   chunk_size: Optional[int] = None) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """
        Execute a SELECT query and return results as a DataFrame.
        For very large queries, can return an iterator of chunks.

        Args:
            query: SQL query string
            params: Dictionary of query parameters
            chunk_size: If provided, return an iterator of DataFrames of this size

        Returns:
            DataFrame or iterator of DataFrames
        """
        try:
            if chunk_size:
                # Return iterator for processing large datasets
                return pd.read_sql_query(
                    text(query),
                    self.engine,
                    params=params,
                    chunksize=chunk_size
                )
            else:
                # Return complete DataFrame
                return pd.read_sql_query(
                    text(query),
                    self.engine,
                    params=params
                )
        except SQLAlchemyError as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    def execute_query(self,
                      query: str,
                      params: Optional[Dict[str, Any]] = None) -> None:
        """
        Execute a non-SELECT query (INSERT, UPDATE, DELETE, etc.).

        Args:
            query: SQL query string
            params: Dictionary of query parameters
        """
        try:
            with self.engine.connect() as connection:
                with connection.begin():  # Start a transaction
                    connection.execute(text(query), params or {})
        except SQLAlchemyError as e:
            logger.error(f"Error executing query: {str(e)}")
            raise