import os
import uuid
import logging
import json
from typing import List, Dict, Any, Optional, Union, Tuple, Iterator
from contextlib import contextmanager
import time
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, Table, MetaData, Column, Integer, String, inspect
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from psycopg2.extras import execute_values, execute_batch
from psycopg2 import sql
from dotenv import load_dotenv, find_dotenv
from io import StringIO
import tempfile
import jinja2

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PostgresWarehouseConnector:
    """
    A high-performance connector for Postgres Data Warehouse using both SQLAlchemy and psycopg2.
    Optimized for Iceberg tables, heap tables, and efficient data operations.
    """

    def __init__(self,
                 host: str = None,
                 database: str = None,
                 user: str = None,
                 password: str = None,
                 port: str = "5432",
                 env_file: str = ".env",
                 db_prefix: str = "",
                 application_name: str = "PostgresWarehouseConnector",
                 pool_size: int = 5,
                 max_overflow: int = 10,
                 pool_timeout: int = 30,
                 pool_recycle: int = 1800):
        """
        Initialize database connection parameters with optimizations for Postgres Data Warehouse.

        Args:
            host: Database host address
            database: Database name
            user: Database username
            password: Database password
            port: Database port (default: "5432")
            env_file: Path to .env file with connection parameters
            db_prefix: Prefix for database environment variables (e.g., "SECONDARY_")
            application_name: Name to identify this application in pg_stat_activity
            pool_size: The size of the connection pool
            max_overflow: The maximum overflow size of the pool
            pool_timeout: Seconds to wait before giving up on getting a connection from pool
            pool_recycle: Seconds after which a connection is automatically recycled
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
            'port': port or os.getenv(port_var, '5432'),
            'application_name': application_name,
        }

        # Validate connection parameters
        missing_params = [k for k, v in self.db_params.items() if not v and k != 'application_name']
        if missing_params:
            raise ValueError(f"Missing required database parameters: {', '.join(missing_params)}")

        # SQLAlchemy pool settings
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle

        # Lazy initialization of SQLAlchemy engine
        self._engine = None
        self._metadata = None

        # Jinja2 Environment for SQL templating
        self.jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader("./sql_templates"),
            autoescape=jinja2.select_autoescape(['sql']),
            trim_blocks=True,
            lstrip_blocks=True
        )

        # Default Iceberg location settings
        self._default_iceberg_location = None

        # Log connection info without exposing password
        safe_params = {k: v if k != 'password' else '****' for k, v in self.db_params.items()}
        logger.info(f"PostgresWarehouseConnector initialized with parameters: {safe_params}")

    @property
    def engine(self) -> Engine:
        """Lazy loading of SQLAlchemy engine with connection pooling."""
        if self._engine is None:
            connection_string = (f"postgresql://{self.db_params['user']}:{self.db_params['password']}"
                                 f"@{self.db_params['host']}:{self.db_params['port']}"
                                 f"/{self.db_params['database']}")
            self._engine = create_engine(
                connection_string,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_timeout=self.pool_timeout,
                pool_recycle=self.pool_recycle,
                connect_args={'application_name': self.db_params['application_name']}
            )
        return self._engine

    @property
    def metadata(self) -> MetaData:
        """Lazy loading of SQLAlchemy metadata."""
        if self._metadata is None:
            self._metadata = MetaData()
        return self._metadata

    @property
    def default_iceberg_location(self) -> str:
        """Get the default Iceberg location from the database settings."""
        if self._default_iceberg_location is None:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW crunchy_iceberg.default_location_prefix;")
                    result = cur.fetchone()
                    if result:
                        self._default_iceberg_location = result[0]
        return self._default_iceberg_location

    @contextmanager
    def get_connection(self):
        """Context manager for psycopg2 database connections."""
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
    def get_sqlalchemy_connection(self) -> Connection:
        """Context manager for SQLAlchemy connections."""
        conn = None
        try:
            conn = self.engine.connect()
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

    def execute_sql_template(self, template_name: str, params: Dict[str, Any] = None,
                             template_dir: str = None, as_df: bool = False):
        """
        Execute SQL from a Jinja2 template with parameters.

        Args:
            template_name: Name of the template file
            params: Parameters to pass to the template
            template_dir: Optional custom directory for templates
            as_df: Whether to return results as a DataFrame

        Returns:
            DataFrame if as_df is True, else execution result
        """
        try:
            # Use custom loader if template_dir provided
            if template_dir:
                env = jinja2.Environment(
                    loader=jinja2.FileSystemLoader(template_dir),
                    autoescape=jinja2.select_autoescape(['sql']),
                    trim_blocks=True,
                    lstrip_blocks=True
                )
                template = env.get_template(template_name)
            else:
                # Try to get from the default loader
                template = self.jinja_env.get_template(template_name)

            # Render template with params
            sql_query = template.render(params or {})
            logger.debug(f"Generated SQL from template {template_name}:\n{sql_query}")

            # Execute rendered SQL
            if as_df:
                return self.read_query(sql_query)
            else:
                return self.execute_query(sql_query)
        except Exception as e:
            logger.error(f"Error executing SQL template {template_name}: {str(e)}")
            raise

    def read_query(self,
                   query: str,
                   params: Optional[Dict[str, Any]] = None,
                   chunk_size: Optional[int] = None,
                   stream_results: bool = False) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """
        Execute a SELECT query and return results as a DataFrame.
        For very large queries, can return an iterator of chunks.

        Args:
            query: SQL query string
            params: Dictionary of query parameters
            chunk_size: If provided, return an iterator of DataFrames of this size
            stream_results: If True, use server-side cursors for minimal memory usage

        Returns:
            DataFrame or iterator of DataFrames
        """
        try:
            if stream_results:
                # Use server-side cursor via psycopg2 for large result sets
                with self.get_connection() as conn:
                    # Use a server-side cursor with a unique name
                    cursor_name = f"stream_cursor_{uuid.uuid4().hex}"
                    with conn.cursor(name=cursor_name) as cur:
                        # Execute the query
                        if params:
                            params_tuple = tuple(params.values())
                            cur.execute(query, params_tuple)
                        else:
                            cur.execute(query)

                        # Fetch results in chunks
                        while True:
                            records = cur.fetchmany(chunk_size or 1000)
                            if not records:
                                break

                            # Convert to DataFrame
                            col_names = [desc[0] for desc in cur.description]
                            yield pd.DataFrame(records, columns=col_names)
            elif chunk_size:
                # Return iterator for processing large datasets with client-side batching
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
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    def execute_query(self,
                      query: str,
                      params: Optional[Dict[str, Any]] = None) -> int:
        """
        Execute a non-SELECT query (INSERT, UPDATE, DELETE, etc.).

        Args:
            query: SQL query string
            params: Dictionary of query parameters

        Returns:
            Number of affected rows
        """
        try:
            with self.engine.connect() as connection:
                with connection.begin():  # Start a transaction
                    result = connection.execute(text(query), params or {})
                    return result.rowcount
        except SQLAlchemyError as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    def get_table_schema(self, table_name: str, schema: str = 'public') -> Dict[str, Dict[str, Any]]:
        """
        Get the column schema information for a specific table.

        Args:
            table_name: The name of the table
            schema: The schema name (default: 'public')

        Returns:
            Dictionary mapping column names to their PostgreSQL data types
        """
        query = """
        SELECT column_name, data_type, udt_name, 
               character_maximum_length, 
               is_nullable,
               column_default
        FROM information_schema.columns
        WHERE table_schema = :schema
        AND table_name = :table_name
        ORDER BY ordinal_position
        """

        result = self.read_query(query, {"schema": schema, "table_name": table_name})

        if result.empty:
            logger.warning(f"No schema found for {schema}.{table_name}")
            return {}

        schema_info = {}
        for _, row in result.iterrows():
            schema_info[row['column_name']] = {
                'data_type': row['data_type'],
                'udt_name': row['udt_name'],
                'character_maximum_length': row['character_maximum_length'],
                'is_nullable': row['is_nullable'] == 'YES',
                'column_default': row['column_default']
            }

        return schema_info

    def is_iceberg_table(self, table_name: str, schema: str = 'public') -> bool:
        """
        Check if a table is an Iceberg table.

        Args:
            table_name: The name of the table
            schema: The schema name (default: 'public')

        Returns:
            True if the table is an Iceberg table, False otherwise
        """
        query = """
        SELECT 1
        FROM iceberg_tables
        WHERE table_namespace = :schema
        AND table_name = :table_name
        LIMIT 1
        """

        result = self.read_query(query, {"schema": schema, "table_name": table_name})
        return not result.empty

    def table_exists(self, table_name: str, schema: str = 'public') -> bool:
        """
        Check if a table exists in the database.
        """
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = :schema
            AND table_name = :table_name
        )
        """
        params = {
            "schema": schema,
            "table_name": table_name
        }

        # Make sure read_query returns a DataFrame, not a generator
        result = self.read_query(query, params, chunk_size=None)  # Ensure no chunking
        return result.iloc[0, 0]

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
                            df_copy[col_name] = df_copy[col_name].map(lambda x: bool_map.get(x, x))

                    # Handle arrays and JSON types
                    elif data_type == 'ARRAY' or target_schema[col_name]['udt_name'].startswith('_'):
                        # Convert lists or strings to proper PostgreSQL arrays
                        df_copy[col_name] = df_copy[col_name].apply(
                            lambda x: x if isinstance(x, list) else
                            json.loads(x) if isinstance(x, str) and x.startswith('[') else x
                        )

                    elif data_type in ('json', 'jsonb'):
                        # Ensure JSON columns are properly formatted
                        df_copy[col_name] = df_copy[col_name].apply(
                            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                        )

                except Exception as e:
                    logger.warning(f"Could not convert column {col_name} to {data_type}: {str(e)}")

        return df_copy

    def create_temp_table_like(self, temp_table: str, template_table: str, schema: str = 'public') -> None:
        """
        Create a temporary table with the same structure as an existing table.

        Args:
            temp_table: Name for the temporary table
            template_table: Existing table to copy structure from
            schema: Schema of the template table
        """
        qualified_template = f"{schema}.{template_table}" if schema else template_table
        query = f"""
        CREATE TEMPORARY TABLE {temp_table} 
        (LIKE {qualified_template} INCLUDING ALL)
        ON COMMIT DROP
        """
        self.execute_query(query)
        logger.info(f"Created temporary table {temp_table} based on {qualified_template}")

    def create_table_if_not_exists(self,
                                   table_name: str,
                                   df: pd.DataFrame,
                                   schema: str = 'public',
                                   use_iceberg: bool = False,
                                   iceberg_location: str = None,
                                   primary_key: str = None) -> None:
        """
        Create a table from DataFrame schema if it doesn't exist.

        Args:
            table_name: Name for the new table
            df: DataFrame to base structure on
            schema: Schema name (default: 'public')
            use_iceberg: Whether to create an Iceberg table
            iceberg_location: Optional S3 location for Iceberg table
            primary_key: Optional column(s) to set as primary key
        """
        # if self.table_exists(table_name, schema):
        #     logger.info(f"Table {schema}.{table_name} already exists, skipping creation")
        #     return

        # Map pandas dtypes to PostgreSQL types
        type_mapping = {
            'int64': 'BIGINT',
            'Int64': 'BIGINT',
            'int32': 'INTEGER',
            'Int32': 'INTEGER',
            'int16': 'SMALLINT',
            'int8': 'SMALLINT',
            'float64': 'DOUBLE PRECISION',
            'float32': 'REAL',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'object': 'TEXT',
            'category': 'TEXT',
            'complex128': 'TEXT'
        }

        # Build column definitions
        columns = []
        for col_name, dtype in df.dtypes.items():
            pg_type = type_mapping.get(str(dtype), 'TEXT')
            column_def = f"\"{col_name}\" {pg_type}"

            # Add NOT NULL if the column has no nulls
            if not df[col_name].isna().any():
                column_def += " NOT NULL"

            columns.append(column_def)

        # Add primary key constraint if specified
        if primary_key:
            pk_constraint = f", PRIMARY KEY ({primary_key})"
        else:
            pk_constraint = ""

        # Add Iceberg options if requested
        if use_iceberg:
            # Build Iceberg-specific clause
            iceberg_clause = "USING iceberg"
            if iceberg_location:
                iceberg_clause += f" WITH (location = '{iceberg_location}')"
        else:
            iceberg_clause = ""

        # Create the table
        qualified_table = f"{schema}.{table_name}" if schema else table_name
        create_query = f"""
        CREATE TABLE {qualified_table} (
            {','.join(columns)}
            {pk_constraint}
        ) {iceberg_clause}
        """

        self.execute_query(create_query)
        logger.info(f"Created {'Iceberg ' if use_iceberg else ''}table {qualified_table}")

    def copy_from_dataframe(self,
                            df: pd.DataFrame,
                            table_name: str,
                            schema: str = 'public',
                            chunk_size: int = 10000) -> int:
        """
        Efficiently copy data from DataFrame to Postgres table using COPY.

        Args:
            df: DataFrame to copy
            table_name: Target table name
            schema: Schema name (default: 'public')
            chunk_size: Number of rows per chunk for large dataframes

        Returns:
            Number of rows copied
        """
        qualified_table = f"{schema}.{table_name}" if schema else table_name
        rows_copied = 0

        # Get table schema for type conversion
        target_schema = self.get_table_schema(table_name, schema)

        # Convert DataFrame to match PostgreSQL types
        converted_df = self.convert_df_types(df, target_schema)

        # Process in chunks if needed
        total_rows = len(converted_df)
        for i in range(0, total_rows, chunk_size):
            chunk = converted_df.iloc[i:i + chunk_size]

            # Use StringIO for COPY FROM
            with StringIO() as csv_buffer:
                # Convert to CSV with correct formatting
                chunk.to_csv(csv_buffer, index=False, header=False, na_rep='\\N')
                csv_buffer.seek(0)

                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        # Build list of columns
                        columns = ", ".join([f'"{c}"' for c in chunk.columns])

                        # Execute COPY
                        cur.copy_expert(
                            f"COPY {qualified_table} ({columns}) FROM STDIN WITH CSV",
                            csv_buffer
                        )
                        rows_copied += cur.rowcount

            logger.info(f"Copied chunk of {len(chunk)} rows to {qualified_table} " +
                        f"({i + len(chunk)}/{total_rows})")

        logger.info(f"Total {rows_copied} rows copied to {qualified_table}")
        return rows_copied

    def copy_to_s3(self,
                   query_or_table: str,
                   s3_location: str,
                   format: str = 'parquet',
                   compression: str = 'snappy',
                   options: Dict[str, str] = None) -> str:
        """
        Export query results or table to S3 in specified format.

        Args:
            query_or_table: SQL query or table name to export
            s3_location: S3 bucket path (s3://bucket/path)
            format: Output format ('parquet', 'csv', 'json')
            compression: Compression algorithm ('snappy', 'gzip', 'zstd', 'none')
            options: Additional options for the COPY command

        Returns:
            S3 path where data was written
        """
        # Determine if input is a query or table name
        is_query = query_or_table.strip().lower().startswith(('select', 'with'))

        # Add format extension to S3 path if not present
        if not s3_location.endswith(f'.{format}'):
            if compression != 'none' and format in ('csv', 'json'):
                s3_location = f"{s3_location}.{format}.{compression}"
            else:
                s3_location = f"{s3_location}.{format}"

        # Build COPY command
        if is_query:
            copy_command = f"COPY ({query_or_table})"
        else:
            copy_command = f"COPY {query_or_table}"

        copy_command += f" TO '{s3_location}'"

        # Add format and compression options
        opt_list = [f"format '{format}'", f"compression '{compression}'"]

        # Add any additional options
        if options:
            for key, value in options.items():
                opt_list.append(f"{key} '{value}'")

        # Append options to command
        copy_command += f" WITH ({', '.join(opt_list)})"

        # Execute the COPY command
        self.execute_query(copy_command)
        logger.info(f"Exported data to {s3_location}")

        return s3_location

    def copy_from_s3(self,
                     s3_location: str,
                     table_name: str,
                     schema: str = 'public',
                     format: str = None,  # Auto-detected from extension if None
                     compression: str = None,  # Auto-detected from extension if None
                     options: Dict[str, str] = None) -> int:
        """
        Import data from S3 to a table.

        Args:
            s3_location: S3 bucket path (s3://bucket/path)
            table_name: Target table name
            schema: Schema name (default: 'public')
            format: File format (auto-detected if None)
            compression: Compression algorithm (auto-detected if None)
            options: Additional options for the COPY command

        Returns:
            Number of rows imported
        """
        qualified_table = f"{schema}.{table_name}" if schema else table_name

        # Build the COPY command
        copy_command = f"COPY {qualified_table} FROM '{s3_location}'"

        # Add format and compression options if specified
        opt_list = []
        if format:
            opt_list.append(f"format '{format}'")
        if compression:
            opt_list.append(f"compression '{compression}'")

        # Add any additional options
        if options:
            for key, value in options.items():
                opt_list.append(f"{key} '{value}'")

        # Append options to command if any
        if opt_list:
            copy_command += f" WITH ({', '.join(opt_list)})"

        # Execute the COPY command
        rows_imported = self.execute_query(copy_command)
        logger.info(f"Imported {rows_imported} rows from {s3_location} to {qualified_table}")

        return rows_imported

    def create_iceberg_table_from_df(self,
                                     df: pd.DataFrame,
                                     table_name: str,
                                     schema: str = 'public',
                                     location: str = None,
                                     if_exists: str = 'error') -> Tuple[str, int]:
        """
        Create an Iceberg table from a DataFrame and load data into it.

        Args:
            df: DataFrame to create table from and load
            table_name: Name for the new Iceberg table
            schema: Schema name (default: 'public')
            location: Optional S3 location for Iceberg table
            if_exists: Action if table exists ('error', 'replace', 'append')

        Returns:
            Tuple (table_name, row_count)
        """
        qualified_table = f"{schema}.{table_name}" if schema else table_name

        # Check if table exists
        table_exists = self.table_exists(table_name, schema)

        if table_exists:
            if if_exists == 'error':
                raise ValueError(f"Table {qualified_table} already exists")
            elif if_exists == 'replace':
                # Drop the existing table
                self.execute_query(f"DROP TABLE {qualified_table}")
                table_exists = False
            elif if_exists == 'append':
                # Keep the table, will append later
                pass
            else:
                raise ValueError(f"Invalid value for if_exists: {if_exists}")

        # Create table if needed
        if not table_exists:
            self.create_table_if_not_exists(
                table_name=table_name,
                df=df,
                schema=schema,
                use_iceberg=True,
                iceberg_location=location
            )

        # Load data into the table
        # For Iceberg tables, we'll use COPY with a temp file for better performance
        with tempfile.NamedTemporaryFile('w+t', suffix='.csv') as temp_file:
            # Write to CSV
            df.to_csv(temp_file.name, index=False, header=True)
            temp_file.flush()

            # Use psycopg2 for COPY operation
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Build column list
                    columns = ", ".join([f'"{c}"' for c in df.columns])

                    # Execute COPY
                    with open(temp_file.name, 'r') as f:
                        cur.copy_expert(
                            f"COPY {qualified_table} ({columns}) FROM STDIN WITH CSV HEADER",
                            f
                        )
                        rows_loaded = cur.rowcount

        logger.info(f"Created Iceberg table {qualified_table} with {rows_loaded} rows")

        # Run vacuum to optimize files for future query performance
        self.execute_query(f"VACUUM (ICEBERG) {qualified_table}")
        logger.info(f"Vacuumed Iceberg table {qualified_table}")

        return (qualified_table, rows_loaded)

    def upsert_to_table(self,
                        df: pd.DataFrame,
                        table_name: str,
                        schema: str = 'public',
                        unique_key: str = None,
                        use_merge: bool = False,
                        chunk_size: int = 10000) -> int:
        """
        Perform an UPSERT operation to a table using a temporary table.

        Args:
            df: DataFrame with the data to upsert
            table_name: Target table name
            schema: Schema name (default: 'public')
            unique_key: Column or columns that identify unique rows
            use_merge: Use MERGE instead of DELETE+INSERT for heap tables
            chunk_size: Number of rows per chunk for large dataframes

        Returns:
            Number of rows affected
        """
        qualified_table = f"{schema}.{table_name}" if schema else table_name
        is_iceberg = self.is_iceberg_table(table_name, schema)

        if not unique_key:
            raise ValueError("unique_key is required for upsert operations")

        # Create a unique name for the temporary table
        temp_table = f"tmp_{table_name.replace('.', '_')}_{uuid.uuid4().hex[:8]}"

        # Get the target table schema
        target_schema = self.get_table_schema(table_name, schema)

        # Convert DataFrame types to match target schema
        converted_df = self.convert_df_types(df, target_schema)

        # Create a temporary table with the same structure as the target
        self.create_temp_table_like(temp_table, qualified_table)

        # Insert data into the temporary table using COPY
        total_rows = len(converted_df)
        logger.info(f"Upserting {total_rows} rows to {qualified_table} using key {unique_key}")

        # Process in chunks if needed
        for i in range(0, total_rows, chunk_size):
            chunk = converted_df.iloc[i:i + chunk_size]

            # Use StringIO for COPY FROM
            with StringIO() as csv_buffer:
                chunk.to_csv(csv_buffer, index=False, header=False, na_rep='\\N')
                csv_buffer.seek(0)

                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        # Build list of columns
                        columns = ", ".join([f'"{c}"' for c in chunk.columns])

                        # Execute COPY
                        cur.copy_expert(
                            f"COPY {temp_table} ({columns}) FROM STDIN WITH CSV",
                            csv_buffer
                        )

            logger.debug(f"Loaded chunk {i // chunk_size + 1} ({i + len(chunk)}/{total_rows}) into temp table")

        # Now perform the upsert operation
        affected_rows = 0

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                if is_iceberg:
                    # For Iceberg tables we need to use DELETE+INSERT (MERGE not supported)
                    merge_sql = f"""
                    WITH deleted AS (
                        DELETE FROM {qualified_table}
                        WHERE {unique_key} IN (SELECT {unique_key} FROM {temp_table})
                        RETURNING *
                    )
                    INSERT INTO {qualified_table}
                    SELECT * FROM {temp_table};
                    """
                    cur.execute(merge_sql)
                    affected_rows = cur.rowcount

                elif use_merge and not is_iceberg:
                    # For heap tables, we can use MERGE if requested (PostgreSQL 15+)
                    # Build a list of columns for the SET clause
                    columns = [col for col in converted_df.columns if col != unique_key.split(',')[0].strip()]
                    set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])

                    merge_sql = f"""
                    INSERT INTO {qualified_table}
                    SELECT * FROM {temp_table}
                    ON CONFLICT ({unique_key}) 
                    DO UPDATE SET {set_clause}
                    """
                    cur.execute(merge_sql)
                    affected_rows = cur.rowcount

                else:
                    # Fallback to DELETE+INSERT for all table types
                    merge_sql = f"""
                    WITH deleted AS (
                        DELETE FROM {qualified_table}
                        WHERE {unique_key} IN (SELECT {unique_key} FROM {temp_table})
                        RETURNING *
                    )
                    INSERT INTO {qualified_table}
                    SELECT * FROM {temp_table};
                    """
                    cur.execute(merge_sql)
                    affected_rows = cur.rowcount

        logger.info(f"Upserted {affected_rows} rows to {qualified_table}")
        return affected_rows

    def merge_df_to_iceberg(self,
                            df: pd.DataFrame,
                            table_name: str,
                            schema: str = 'public',
                            unique_key: str = None,
                            chunk_size: int = 10000) -> int:
        """
        High-performance merge of DataFrame into an Iceberg table.

        Args:
            df: DataFrame with the data to merge
            table_name: Target Iceberg table name
            schema: Schema name (default: 'public')
            unique_key: Column or columns that identify unique rows
            chunk_size: Number of rows per chunk for large dataframes

        Returns:
            Number of rows affected
        """
        # Verify it's an Iceberg table
        if not self.is_iceberg_table(table_name, schema):
            raise ValueError(f"Table {schema}.{table_name} is not an Iceberg table")

        # Simply call upsert_to_table with appropriate settings for Iceberg
        return self.upsert_to_table(
            df=df,
            table_name=table_name,
            schema=schema,
            unique_key=unique_key,
            use_merge=False,  # Iceberg doesn't support MERGE
            chunk_size=chunk_size
        )

    def vacuum_iceberg_table(self, table_name: str, schema: str = 'public') -> None:
        """
        Vacuum an Iceberg table to optimize storage and improve query performance.

        Args:
            table_name: Iceberg table name
            schema: Schema name (default: 'public')
        """
        qualified_table = f"{schema}.{table_name}" if schema else table_name

        # Verify it's an Iceberg table
        if not self.is_iceberg_table(table_name, schema):
            raise ValueError(f"Table {qualified_table} is not an Iceberg table")

        self.execute_query(f"VACUUM (ICEBERG) {qualified_table}")
        logger.info(f"Vacuumed Iceberg table {qualified_table}")

    def create_foreign_table_from_parquet(self,
                                          s3_path: str,
                                          table_name: str,
                                          schema: str = 'public',
                                          columns: List[Dict[str, str]] = None) -> str:
        """
        Create a FOREIGN TABLE pointing to Parquet file(s) in S3.

        Args:
            s3_path: S3 path to Parquet file(s), can use wildcards
            table_name: Name for the foreign table
            schema: Schema name (default: 'public')
            columns: Optional list of column definitions, auto-detected if None

        Returns:
            The created table name
        """
        qualified_table = f"{schema}.{table_name}" if schema else table_name

        # Build CREATE FOREIGN TABLE statement
        if columns:
            # Use provided column definitions
            col_defs = []
            for col in columns:
                col_defs.append(f"\"{col['name']}\" {col['type']}")
            column_clause = f"({', '.join(col_defs)})"
        else:
            # Auto-detect columns
            column_clause = "()"

        create_sql = f"""
        CREATE FOREIGN TABLE {qualified_table} {column_clause}
        SERVER crunchy_lake_analytics
        OPTIONS (path '{s3_path}')
        """

        self.execute_query(create_sql)
        logger.info(f"Created foreign table {qualified_table} pointing to {s3_path}")

        return qualified_table

    def bulk_load_to_table(self,
                           df: pd.DataFrame,
                           table_name: str,
                           schema: str = 'public',
                           if_exists: str = 'append',
                           use_iceberg: bool = False,
                           s3_staging: bool = False,
                           s3_location: str = None,
                           chunk_size: int = 100000) -> int:
        """
        Highly optimized bulk loading of DataFrame to PostgreSQL/Iceberg.
        Automatically selects the best loading strategy based on size and table type.

        Args:
            df: DataFrame to load
            table_name: Target table name
            schema: Schema name (default: 'public')
            if_exists: Action if table exists ('error', 'replace', 'append')
            use_iceberg: Whether to use/create an Iceberg table
            s3_staging: Use S3 staging for very large datasets
            s3_location: S3 location to use for staging or Iceberg tables
            chunk_size: Number of rows per chunk for large dataframes

        Returns:
            Number of rows loaded
        """
        qualified_table = f"{schema}.{table_name}" if schema else table_name
        table_exists = self.table_exists(table_name, schema)
        total_rows = len(df)

        # Handle 'if_exists' for existing tables
        if table_exists:
            if if_exists == 'error':
                raise ValueError(f"Table {qualified_table} already exists")
            elif if_exists == 'replace':
                # Drop the existing table
                self.execute_query(f"DROP TABLE {qualified_table}")
                table_exists = False
            elif if_exists == 'append':
                # Will append later
                pass
            else:
                raise ValueError(f"Invalid value for if_exists: {if_exists}")

        # Create table if needed
        if not table_exists:
            self.create_table_if_not_exists(
                table_name=table_name,
                df=df,
                schema=schema,
                use_iceberg=use_iceberg,
                iceberg_location=s3_location
            )

        # For very large datasets and S3 staging is requested
        if s3_staging and total_rows > chunk_size:
            # Stage data in S3 and then COPY
            start_time = time.time()

            # Generate temporary S3 location if none provided
            if not s3_location:
                s3_location = self.default_iceberg_location
                if not s3_location:
                    raise ValueError("S3 location must be provided for S3 staging")

                # Add a random suffix to avoid collisions
                s3_staging_path = f"{s3_location}/staging/{table_name}_{uuid.uuid4().hex}"
            else:
                s3_staging_path = s3_location

            # Write DataFrame to S3 as Parquet
            temp_s3_path = f"{s3_staging_path}/{table_name}.parquet"

            # Write to S3 directly from memory
            with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_file:
                df.to_parquet(temp_file.name, index=False)
                temp_file.flush()

                # Use execute_query to run COPY command to write to S3
                copy_cmd = f"""
                COPY (SELECT * FROM {qualified_table} WHERE 1=0)
                TO '{temp_s3_path}'
                WITH (format 'parquet', compression 'snappy')
                """
                self.execute_query(copy_cmd)

                # Now load from temp file to S3
                # Use psycopg2 for COPY operation
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        # Build column list
                        columns = ", ".join([f'"{c}"' for c in df.columns])

                        # Execute COPY
                        with open(temp_file.name, 'rb') as f:
                            cur.copy_expert(
                                f"COPY {qualified_table} FROM '{temp_s3_path}' WITH (format 'parquet')",
                                f
                            )
                            rows_loaded = cur.rowcount

            load_time = time.time() - start_time
            logger.info(
                f"Loaded {rows_loaded} rows to {qualified_table} via S3 in {load_time:.2f}s " +
                f"({rows_loaded / load_time:.2f} rows/s)"
            )

            # Cleanup the temp S3 file
            # This might be left for manual cleanup if S3 permissions are complex

            return rows_loaded

        # For smaller datasets or regular loading
        else:
            # Get target schema for type conversion
            target_schema = self.get_table_schema(table_name, schema)

            # Convert DataFrame types to match target schema
            converted_df = self.convert_df_types(df, target_schema)

            # Use direct COPY for efficiency
            start_time = time.time()

            # Process in chunks if needed
            rows_loaded = 0
            for i in range(0, total_rows, chunk_size):
                chunk = converted_df.iloc[i:i + chunk_size]

                # Use StringIO for COPY FROM
                with StringIO() as csv_buffer:
                    chunk.to_csv(csv_buffer, index=False, header=False, na_rep='\\N')
                    csv_buffer.seek(0)

                    with self.get_connection() as conn:
                        with conn.cursor() as cur:
                            # Build list of columns
                            columns = ", ".join([f'"{c}"' for c in chunk.columns])

                            # Execute COPY
                            cur.copy_expert(
                                f"COPY {qualified_table} ({columns}) FROM STDIN WITH CSV",
                                csv_buffer
                            )
                            rows_loaded += cur.rowcount

                logger.debug(f"Loaded chunk {i // chunk_size + 1} ({i + len(chunk)}/{total_rows})")

            load_time = time.time() - start_time
            logger.info(
                f"Loaded {rows_loaded} rows to {qualified_table} in {load_time:.2f}s " +
                f"({rows_loaded / load_time:.2f} rows/s)"
            )

            # For Iceberg tables, run vacuum to optimize file size
            if use_iceberg or self.is_iceberg_table(table_name, schema):
                self.vacuum_iceberg_table(table_name, schema)

            return rows_loaded

    def analyze_table(self, table_name: str, schema: str = 'public', columns: List[str] = None) -> None:
        """
        Run ANALYZE on a table to update statistics for the query planner.

        Args:
            table_name: Table name to analyze
            schema: Schema name (default: 'public')
            columns: Optional list of columns to analyze
        """
        qualified_table = f"{schema}.{table_name}" if schema else table_name

        if columns:
            column_clause = f"({', '.join([f'\"{col}\"' for col in columns])})"
        else:
            column_clause = ""

        self.execute_query(f"ANALYZE {qualified_table} {column_clause}")
        logger.info(f"Analyzed table {qualified_table}")

    def execute_with_result(self, query: str, params: Dict[str, Any] = None) -> Tuple[int, List[Dict]]:
        """
        Execute a query and return affected rows and results if any.

        Args:
            query: SQL query string
            params: Dictionary of query parameters

        Returns:
            Tuple (affected_rows, results)
        """
        results = []
        affected_rows = 0

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)

                affected_rows = cur.rowcount

                # Check if the query returns results
                try:
                    if cur.description:
                        # Get column names
                        columns = [desc[0] for desc in cur.description]

                        # Fetch results
                        for row in cur.fetchall():
                            results.append(dict(zip(columns, row)))
                except:
                    pass  # No results to fetch

        return affected_rows, results

    def get_table_size(self, table_name: str, schema: str = 'public') -> Dict[str, int]:
        """
        Get the size of a table in bytes, rows, and pages.

        Args:
            table_name: Table name
            schema: Schema name (default: 'public')

        Returns:
            Dictionary with size information
        """
        qualified_table = f"{schema}.{table_name}" if schema else table_name

        # For Iceberg tables, use the specialized function
        is_iceberg = self.is_iceberg_table(table_name, schema)

        if is_iceberg:
            query = f"SELECT pg_table_size('{qualified_table}') AS size_bytes"
            result = self.read_query(query)

            # Get row count for Iceberg table
            count_query = f"SELECT COUNT(*) AS row_count FROM {qualified_table}"
            count_result = self.read_query(count_query)

            return {
                'size_bytes': result.iloc[0]['size_bytes'],
                'row_count': count_result.iloc[0]['row_count'],
                'is_iceberg': True
            }
        else:
            # For regular heap tables, use pg_total_relation_size and pg_stat_user_tables
            query = f"""
            SELECT 
                pg_total_relation_size('{qualified_table}') AS size_bytes,
                (SELECT n_live_tup FROM pg_stat_user_tables 
                 WHERE schemaname = '{schema}' AND relname = '{table_name}') AS row_count,
                (SELECT relpages FROM pg_class 
                 WHERE relnamespace = '{schema}'::regnamespace AND relname = '{table_name}') AS pages
            """

            result = self.read_query(query)

            return {
                'size_bytes': result.iloc[0]['size_bytes'],
                'row_count': result.iloc[0]['row_count'],
                'pages': result.iloc[0]['pages'],
                'is_iceberg': False
            }

    def close(self) -> None:
        """
        Explicitly close all connections in the connection pool.
        """
        if self._engine:
            self._engine.dispose()
            logger.info("Disposed SQLAlchemy connection pool")



