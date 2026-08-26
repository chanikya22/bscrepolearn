import os
from datetime import date, timedelta
import pandas as pd
import logging
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, execute_batch, RealDictCursor
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from typing import Dict, List, Any, Optional, Union, Tuple, Iterator
# from dotenv import load_dotenv, find_dotenv
import awswrangler as wr
import boto3
import re
import numpy as np
import time
import os
import json
import boto3
from botocore.exceptions import ClientError


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresConnector:
    """
    Enhanced PostgreSQL connector for ETL operations with support for both
    traditional PostgreSQL tables and Iceberg tables through Crunchy Data.
    """

    def __init__(
        self,
        host: str = None,
        database: str = None,
        user: str = None,
        password: str = None,
        port: str = "5432",
        db_prefix: str = "",
        query_dir: str = None,
        secret_name: str = "insyt-flow-env",
        region_name: str = "ap-south-1"
    ):

        # Fetch secret from AWS Secrets Manager
        secret_dict = self._get_secret(secret_name, region_name)

        # Map keys based on prefix
        host_var = f"{db_prefix}DB_HOST"
        name_var = f"{db_prefix}DB_NAME"
        user_var = f"{db_prefix}DB_USER"
        password_var = f"{db_prefix}DB_PASSWORD"
        port_var = f"{db_prefix}DB_PORT"
        sslmode_var = f"{db_prefix}DB_SSLMODE"
        sslrootcert_var = f"{db_prefix}DB_SSLROOTCERT"

        self.db_params = {
            'host': host or secret_dict.get(host_var),
            'database': database or secret_dict.get(name_var),
            'user': user or secret_dict.get(user_var),
            'password': password or secret_dict.get(password_var),
            'port': port or secret_dict.get(port_var, "5432")
        }

        if secret_dict.get(sslmode_var):
            self.db_params['sslmode'] = secret_dict.get(sslmode_var)

        if secret_dict.get(sslrootcert_var):
            self.db_params['sslrootcert'] = secret_dict.get(sslrootcert_var)

        # Validate connection parameters
        missing_params = [k for k, v in self.db_params.items() if not v]
        if missing_params:
            raise ValueError(f"Missing required database parameters: {missing_params}")

        self._engine = None
        self.query_dir = query_dir
        self._query_cache = {}

    def _get_secret(self, secret_name, region_name):
        """
        Fetch secret from AWS Secrets Manager
        """
        try:
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=region_name
            )

            response = client.get_secret_value(SecretId=secret_name)
            return json.loads(response['SecretString'])

        except ClientError as e:
            raise Exception(f"Unable to fetch secret from AWS: {e}")

    @property
    def engine(self):
        """Lazy loading of SQLAlchemy engine for pandas operations."""
        if self._engine is None:
            connection_string = (f"postgresql://{self.db_params['user']}:{self.db_params['password']}"
                                 f"@{self.db_params['host']}:{self.db_params['port']}"
                                 f"/{self.db_params['database']}")

            # Add SSL parameters for SQLAlchemy
            connect_args = {}
            if 'sslmode' in self.db_params:
                connect_args['sslmode'] = self.db_params['sslmode']
            if 'sslrootcert' in self.db_params:
                connect_args['sslrootcert'] = self.db_params['sslrootcert']

            self._engine = create_engine(connection_string, connect_args=connect_args)
        return self._engine

    @contextmanager
    def get_connection(self, autocommit=False):
        """
        Context manager for database connections.

        Args:
            autocommit: Whether to set autocommit mode for the connection

        Yields:
            Active database connection
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            if autocommit:
                conn.autocommit = True
            yield conn
            if not autocommit:
                conn.commit()
        except Exception as e:
            if conn and not autocommit:
                conn.rollback()
            logger.error(f"Database error: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    @contextmanager
    def get_cursor(self, cursor_factory=None, autocommit=False):
        """
        Context manager providing a database cursor.

        Args:
            cursor_factory: Optional cursor factory (e.g., RealDictCursor)
            autocommit: Whether to set autocommit mode for the connection

        Yields:
            Active database cursor
        """
        with self.get_connection(autocommit=autocommit) as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()

    def load_query(self, query_name: str) -> str:
        """
        Load SQL query from file or return directly if it's a SQL string.

        Args:
            query_name: Either a SQL string or the name of a query file without extension

        Returns:
            The SQL query as a string
        """
        # If this looks like SQL (contains SELECT, INSERT, etc.), return it as is
        if isinstance(query_name, str) and re.search(r'\b(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|WITH)\b',
                                                     query_name, re.IGNORECASE):
            return query_name

        # Check cache first
        if query_name in self._query_cache:
            return self._query_cache[query_name]

        # Not in cache, try to load from file
        if not self.query_dir:
            # If no query directory is set but this seems to be a query name rather than SQL text,
            # just return it as-is to maintain compatibility with existing code
            return query_name

        query_path = os.path.join(self.query_dir, f"{query_name}.sql")

        if not os.path.exists(query_path):
            # If file doesn't exist, return the query_name as is for backward compatibility
            return query_name

        with open(query_path, 'r') as file:
            query = file.read()

        # Cache for future use
        self._query_cache[query_name] = query
        return query

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None,
                      return_id: bool = False, id_column: str = 'id',
                      autocommit: bool = False) -> Union[int, None]:
        """
        Execute a non-SELECT query (INSERT, UPDATE, DELETE) with transaction support.

        Args:
            query: SQL query string or name of query file
            params: Dictionary of query parameters
            return_id: Whether to return the ID of the last inserted row
            id_column: Name of the ID column to return (if return_id is True)
            autocommit: Whether to execute in autocommit mode

        Returns:
            Number of affected rows or ID of inserted row if return_id is True
        """
        query_text = self.load_query(query)

        # Convert psycopg2 parameter style from named (:param) to positional (%s)
        if params:
            # Change from colon-based named params to pyformat (%s)
            query_text = self._convert_params_for_psycopg2(query_text)

        try:
            with self.get_cursor(autocommit=autocommit) as cursor:
                if params:
                    cursor.execute(query_text, params)
                else:
                    cursor.execute(query_text)

                if return_id:
                    # Fetch the last inserted ID if requested
                    cursor.execute(f"SELECT lastval() as {id_column}")
                    result = cursor.fetchone()
                    return result[0] if result else None
                else:
                    return cursor.rowcount
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    def execute_query_returning(self, query, params, returning_column="id"):
        """
        Execute a query with a RETURNING clause and return the value.

        Args:
            query: SQL query with RETURNING clause
            params: Query parameters
            returning_column: Name of the column being returned

        Returns:
            Value of the returned column
        """
        # Convert parameters if needed
        query_text = self.load_query(query)
        query_text = self._convert_params_for_psycopg2(query_text)

        with self.get_cursor() as cursor:
            cursor.execute(query_text, params)
            result = cursor.fetchone()
            return result[0] if result else None

    def _convert_params_for_psycopg2(self, query: str) -> str:
        """
        Convert named parameters with colon prefix (:param_name) to
        psycopg2's preferred style (%(param_name)s).

        Args:
            query: SQL query with :param_name style parameters

        Returns:
            SQL query with %(param_name)s style parameters
        """
        # Use regex to find parameters like :param_name and replace with %(param_name)s
        pattern = r'(?<!:):([a-zA-Z0-9_]+)'
        return re.sub(pattern, r'%(\1)s', query)

    def read_query(self, query: str, params: Optional[Dict[str, Any]] = None,
                   as_dict: bool = False,
                   chunk_size: Optional[int] = None) -> Union[
        pd.DataFrame, List[Dict[str, Any]], Iterator[pd.DataFrame]]:
        """
        Execute a SELECT query and return results.
        Compatible with SQLAlchemy < 2.0

        Args:
            query: SQL query string or name of query file
            params: Dictionary of query parameters
            as_dict: Whether to return results as list of dictionaries (True) or DataFrame (False)
            chunk_size: If provided, return an iterator of DataFrames of this size

        Returns:
            Query results as DataFrame or list of dictionaries or iterator of DataFrames
        """
        query_text = self.load_query(query)

        try:
            if as_dict:
                # Use raw psycopg2 connection for dictionary results
                if params:
                    query_text = self._convert_params_for_psycopg2(query_text)

                with self.get_cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query_text, params)
                    return cursor.fetchall()
            else:
                # For pandas DataFrame results
                if params:
                    # Method 1: Use raw psycopg2 connection with pandas
                    # Convert to psycopg2 format
                    psycopg2_query = self._convert_params_for_psycopg2(query_text)

                    # Use raw connection instead of SQLAlchemy engine
                    with self.get_connection() as conn:
                        if chunk_size:
                            return pd.read_sql_query(
                                psycopg2_query,
                                conn,
                                params=params,
                                chunksize=chunk_size
                            )
                        else:
                            return pd.read_sql_query(psycopg2_query, conn, params=params)
                else:
                    # No parameters - can use SQLAlchemy engine directly
                    if chunk_size:
                        return pd.read_sql_query(query_text, self.engine, chunksize=chunk_size)
                    else:
                        return pd.read_sql_query(query_text, self.engine)

        except Exception as e:
            logger.error(f"Error executing read query: {str(e)}")
            raise

    def batch_insert(self, table: str, data: List[Dict[str, Any]],
                     chunk_size: int = 1000, schema: str = 'public') -> int:
        """
        Efficiently insert a batch of records using execute_values.

        Args:
            table: Table name
            data: List of dictionaries with column values
            chunk_size: Number of records to insert in each batch
            schema: Schema name

        Returns:
            Number of inserted records
        """
        if not data:
            return 0

        # Get column names from the first record
        columns = list(data[0].keys())

        # Format table with schema
        table_reference = f"{schema}.{table}" if schema else table

        # Prepare the SQL query
        query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(schema, table) if schema != 'public' else sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )

        # Convert data to list of tuples in the same order as columns
        values = [[record.get(col) for col in columns] for record in data]

        # Execute in batches
        with self.get_cursor() as cursor:
            execute_values(cursor, query, values, page_size=chunk_size)
            return len(values)


    def bulk_upsert_df(self, df: pd.DataFrame, table: str,
                       conflict_columns: List[str], update_columns: List[str] = None,
                       schema: str = 'public', chunk_size: int = 1000) -> int:
        """
        Bulk upsert DataFrame with ON CONFLICT support.

        Args:
            df: DataFrame to upsert
            table: Target table name
            conflict_columns: Columns to use for conflict detection
            update_columns: Columns to update on conflict (if None, updates all except conflict columns)
            schema: Schema name
            chunk_size: Records per batch

        Returns:
            Number of processed records
        """
        if update_columns is None:
            update_columns = [col for col in df.columns if col not in conflict_columns]

        # Create temp table and use it for upsert
        temp_table = f"temp_{table}_{int(time.time())}"

        try:
            # Insert into temp table
            df.to_sql(temp_table, self.engine, schema=schema,
                      if_exists='replace', index=False, method='multi')

            # Build upsert query
            columns = ', '.join(df.columns)
            conflict_cols = ', '.join(conflict_columns)
            updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])

            upsert_query = f"""
                INSERT INTO {schema}.{table} ({columns})
                SELECT {columns} FROM {schema}.{temp_table}
                ON CONFLICT ({conflict_cols}) DO UPDATE SET {updates}
            """

            # Execute upsert
            affected_rows = self.execute_query(upsert_query, autocommit=True)

            return len(df)

        finally:
            # Clean up temp table
            self.execute_query(f"DROP TABLE IF EXISTS {schema}.{temp_table}", autocommit=True)

    def bulk_insert_df(self, df: pd.DataFrame, table: str,
                       if_exists: str = 'append', schema: str = 'public',
                       chunk_size: int = 10000) -> int:
        """
        Efficiently insert DataFrame using SQLAlchemy.

        Args:
            df: DataFrame to insert
            table: Target table name
            if_exists: How to behave if the table exists ('fail', 'replace', or 'append')
            schema: Schema name
            chunk_size: Records per batch

        Returns:
            Number of inserted records
        """
        try:
            # Make a copy to avoid modifying the original DataFrame
            df_copy = df.copy()

            # Handle numpy and pandas-specific types
            for col in df_copy.columns:
                # Handle numpy data types
                if pd.api.types.is_integer_dtype(df_copy[col]):
                    df_copy[col] = df_copy[col].astype('Int64')  # nullable integer
                elif pd.api.types.is_float_dtype(df_copy[col]):
                    df_copy[col] = df_copy[col].astype(float)
                elif pd.api.types.is_bool_dtype(df_copy[col]):
                    df_copy[col] = df_copy[col].astype(bool)

                # Further conversion for specific types that might cause issues
                df_copy[col] = df_copy[col].apply(self._convert_special_types)

            # Insert DataFrame
            df_copy.to_sql(
                name=table,
                con=self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=chunk_size
            )

            return len(df_copy)
        except Exception as e:
            logger.error(f"Error in bulk_insert_df: {str(e)}")
            raise

    # Add bulk_insert alias for compatibility with PipelineTracker
    def bulk_insert(self, table, data, chunk_size=1000, if_exists='append', schema='public'):
        """
        Compatibility method for PipelineTracker.
        Handles both DataFrame and list of dictionary inputs.

        Args:
            table: Table name to insert into
            data: DataFrame or list of dictionaries
            chunk_size: Records per batch
            if_exists: How to behave if the table exists
            schema: Database schema

        Returns:
            Number of inserted records
        """
        if isinstance(data, pd.DataFrame):
            return self.bulk_insert_df(data, table, if_exists, schema, chunk_size)
        elif isinstance(data, list):
            # Convert list of dicts to DataFrame and insert
            df = pd.DataFrame(data)
            return self.bulk_insert_df(df, table, if_exists, schema, chunk_size)
        else:
            raise TypeError("Data must be a DataFrame or list of dictionaries")

    def _convert_special_types(self, value):
        """Helper method to convert numpy types to Python native types."""
        if isinstance(value, np.integer):
            return int(value)
        elif isinstance(value, np.floating):
            return float(value)
        elif isinstance(value, np.ndarray):
            return value.tolist()
        elif isinstance(value, np.bool_):
            return bool(value)
        else:
            return value

    def create_staging_table(self,
                             s3_path: str,
                             staging_table: str,
                             schema: str = 'public') -> str:
        """
        Create a staging table from a Parquet file in S3 using Crunchy Data's special syntax.
        First removes the file from the crunchy_file_cache if it exists.

        Args:
            s3_path: S3 path to the Parquet file (s3://bucket/path/to/file.parquet)
            staging_table: Name for the staging table
            schema: Database schema

        Returns:
            Full table name (schema.table)
        """
        full_table_name = f"{schema}.{staging_table}"

        # First remove the file from cache if it exists
        cache_clear_query = f"""
        SELECT crunchy_file_cache.remove('{s3_path}');
        """

        # Create staging table with data loaded from S3
        create_table_query = f"""
        CREATE TABLE {full_table_name} () 
        WITH (load_from = '{s3_path}');
        """

        with self.get_cursor(autocommit=True) as cursor:
            # Clear the cache first
            cursor.execute(cache_clear_query)
            logger.info(f"Removed {s3_path} from crunchy_file_cache")

            # Then create the staging table
            cursor.execute(create_table_query)
            logger.info(f"Created staging table {full_table_name} from {s3_path}")

        return full_table_name

    def load_s3_file_to_table(self,
                              s3_path: str,
                              target_table: str,
                              key_columns: Union[str, List[str]],
                              schema: str = 'public',
                              staging_table: str = None,
                              run_id: str = None,
                              date_columns: List[str] = None , date_format: str = None) -> Dict[str, Any]:
        """
        Load method with explicit date column specification for CSV files.

        Args:
            s3_path: S3 path to the file
            target_table: Target table name
            key_columns: Primary key column(s) for upserting
            schema: Database schema
            staging_table: Optional staging table name
            date_columns: List of columns to treat as dates (CSV only)

        Returns:
            Dictionary with operation results
        """
        results = {'success': False}

        try:
            # Generate staging table name if not provided
            if not staging_table:
                staging_table = f"{target_table}_staging_{int(time.time())}"

                # Check file type and create staging table
                # Check file type and create staging table
                if s3_path.lower().endswith('.parquet'):
                    # Use existing Parquet method
                    self.create_staging_table(s3_path, staging_table, schema)
                    results['file_type'] = 'parquet'

                    # Add run_id column to parquet staging table if provided
                    if run_id:
                        self._add_run_id_to_staging_table(staging_table, run_id, schema)

                elif s3_path.lower().endswith('.csv'):
                    # Handle CSV with optional date columns and run_id
                    self._create_staging_table_from_csv(
                        s3_path, staging_table, schema, run_id, date_columns , date_format
                    )
                    results['file_type'] = 'csv'
                else:
                    self._create_staging_table_from_csv(s3_path, staging_table, schema)
                results['file_type'] = 'csv'

            else:
                raise ValueError(f"Unsupported file type. Use .parquet or .csv files. Got: {s3_path}")

            results['staging_table'] = f"{schema}.{staging_table}"

            # Use the improved upsert method
            upsert_stats = self.upsert_from_staging_v2(
                staging_table=staging_table,
                target_table=target_table,
                key_columns=key_columns,
                schema=schema
            )

            results.update(upsert_stats)
            results['success'] = True
            results['message'] = f"Successfully loaded {results['file_type']} from {s3_path} to {schema}.{target_table}"

            if date_columns:
                results['date_columns_processed'] = date_columns

            return results

        except Exception as e:
            logger.error(f"Error loading S3 file: {str(e)}")
            results['error'] = str(e)
            return results

        finally:
            # Clean up staging table
            try:
                if staging_table:
                    self.execute_query(f"DROP TABLE IF EXISTS {schema}.{staging_table}", autocommit=True)
            except Exception as e:
                logger.warning(f"Failed to drop staging table: {str(e)}")

    def _add_run_id_to_staging_table(self, staging_table: str, run_id: str, schema: str):
        """
        Add run_id column to an existing staging table (for Parquet files).
        """
        try:
            # Add run_id column and populate it
            alter_sql = f"ALTER TABLE {schema}.{staging_table} ADD COLUMN run_id TEXT;"
            update_sql = f"UPDATE {schema}.{staging_table} SET run_id = %s;"

            with self.get_cursor() as cursor:
                cursor.execute(alter_sql)
                cursor.execute(update_sql, (run_id,))

            logger.info(f"Added run_id column to {schema}.{staging_table} with value: {run_id}")

        except Exception as e:
            logger.error(f"Error adding run_id to staging table: {str(e)}")
            raise

    def _create_staging_table_from_csv(self,
                                       s3_path: str,
                                       staging_table: str,
                                       schema: str,
                                       run_id: str = None,
                                       date_columns: List[str] = None,date_format : str = None):
        """
        CSV handler with explicit date column specification.

        Args:
            s3_path: S3 path to CSV
            staging_table: Staging table name
            schema: Schema name
            date_columns: List of column names that should be treated as dates
        """
        import tempfile
        import os
        import boto3

        # Parse S3 path
        bucket = s3_path.split('/')[2]
        key = '/'.join(s3_path.split('/')[3:])

        # Download CSV
        s3_client = boto3.client('s3')
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            s3_client.download_file(bucket, key, temp_path)

            # Read CSV with explicit date parsing


            df = pd.read_csv(temp_path)

            # Clean column names

            df.columns = [re.sub(r'[\s\-]+', '_', re.sub(r'[()]', '', col.lower())) for col in df.columns]
            df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
            df.columns = [col.lower().replace('.', '') for col in df.columns]
            # If date_columns specified, ensure they're converted
            if date_columns:
                cleaned_date_cols = [col.lower().replace(' ', '_').replace('-', '_') for col in date_columns]
                for col in cleaned_date_cols:
                    if col in df.columns:
                        try:
                            df[col] = pd.to_datetime(df[col], format=f'{date_format}', errors='coerce')
                            logger.info(f"Converted column '{col}' to datetime")
                        except Exception as e:
                            logger.warning(f"Failed to convert column '{col}' to datetime: {e}")

            # Add run_id column if provided
            if run_id:
                df['run_id'] = run_id
                logger.info(f"Added run_id column with value: {run_id}")

            # Load into staging table
            self.bulk_insert_df(
                df=df,
                table=staging_table,
                if_exists='replace',
                schema=schema
            )

            logger.info(f"Created staging table {schema}.{staging_table} with {len(df)} rows")

        finally:
            # Clean up temp file
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def get_iceberg_schema(self, table_name: str, schema: str = 'public') -> Dict:
        """
        Get the schema of an Iceberg table.

        Args:
            table_name: Iceberg table name
            schema: Database schema

        Returns:
            Dictionary containing the Iceberg schema
        """
        query = f"""
        WITH ice AS (
            SELECT crunchy_iceberg.metadata(metadata_location) metadata 
            FROM iceberg_tables 
            WHERE table_name = '{table_name}'
        )
        SELECT metadata->'schemas'->(metadata->>'current-schema-id')::int AS schema_json
        FROM ice;
        """

        with self.get_cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            result = cursor.fetchone()

            if not result:
                raise ValueError(f"Iceberg table {schema}.{table_name} not found")

            return result['schema_json']

    def validate_parquet_schema(self,
                                s3_path: str,
                                table_name: str,
                                schema: str = 'public') -> Dict[str, Any]:
        """
        Validate that a Parquet file schema is compatible with an Iceberg table schema.

        Args:
            s3_path: S3 path to the Parquet file
            table_name: Iceberg table name
            schema: Database schema

        Returns:
            Dictionary with validation results
        """
        try:
            # Get Iceberg table schema
            iceberg_schema = self.get_iceberg_schema(table_name, schema)

            # Create a boto3 session
            aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            region = os.getenv('AWS_REGION', 'ap-south-1')

            session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region
            )

            # Read Parquet file schema
            parquet_schema = wr.s3.read_parquet_metadata(
                path=s3_path,
                boto3_session=session
            )

            # Extract field information from Iceberg schema
            iceberg_fields = {field['name'].lower(): field for field in iceberg_schema['fields']}

            # Validate columns exist in both schemas
            missing_in_parquet = []
            missing_in_iceberg = []
            type_mismatches = []

            # Map parquet to iceberg type conversions
            type_mapping = {
                'int32': ['int', 'integer', 'int32', 'long'],
                'int64': ['long', 'int64', 'int'],
                'float32': ['float', 'float32', 'double'],
                'float64': ['double', 'float64', 'float'],
                'boolean': ['boolean', 'bool'],
                'string': ['string', 'varchar', 'char', 'text'],
                'date32[day]': ['date'],
                'timestamp[ns]': ['timestamp', 'timestamp without time zone'],
                'timestamp[us]': ['timestamp', 'timestamp without time zone'],
            }

            # Check parquet columns against iceberg schema
            for col_name, dtype in parquet_schema['dtypes'].items():
                col_lower = col_name.lower()

                if col_lower not in iceberg_fields:
                    missing_in_iceberg.append(col_name)
                    continue

                # Type checking
                iceberg_type = iceberg_fields[col_lower]['type']
                if isinstance(iceberg_type, dict):
                    # Handle complex types - this is simplified
                    iceberg_type = iceberg_type['type']

                parquet_type = str(dtype)

                # Check type compatibility
                type_compatible = False
                for p_type, i_types in type_mapping.items():
                    if p_type in parquet_type and any(i_type in iceberg_type for i_type in i_types):
                        type_compatible = True
                        break

                if not type_compatible:
                    type_mismatches.append({
                        'column': col_name,
                        'parquet_type': parquet_type,
                        'iceberg_type': iceberg_type
                    })

            # Check for columns in iceberg but not in parquet
            for iceberg_col in iceberg_fields:
                if iceberg_col not in [col.lower() for col in parquet_schema['dtypes'].keys()]:
                    # Skip required check for partition columns (year, month, etc.)
                    if iceberg_col not in ['year', 'month', 'day', 'hour'] and iceberg_fields[iceberg_col].get(
                            'required', False):
                        missing_in_parquet.append(iceberg_col)

            # Validation result
            is_valid = (not missing_in_parquet and not type_mismatches)

            return {
                'is_valid': is_valid,
                'missing_in_parquet': missing_in_parquet,
                'missing_in_iceberg': missing_in_iceberg,
                'type_mismatches': type_mismatches,
                'parquet_columns': list(parquet_schema['dtypes'].keys()),
                'iceberg_columns': list(iceberg_fields.keys())
            }

        except Exception as e:
            logger.error(f"Error validating schemas: {str(e)}")
            return {
                'is_valid': False,
                'error': str(e)
            }



    def upsert_from_staging_v2(self,
                               staging_table: str,
                               target_table: str,
                               key_columns: Union[str, List[str]],
                               schema: str = 'public',
                               include_stats: bool = True) -> Dict[str, int]:
        """
        Enhanced upsert method that handles column matching and ordering properly.
        Works even when staging and target tables have different column orders.

        Args:
            staging_table: Source staging table name
            target_table: Target table name
            key_columns: Primary key column(s) for upserting (string or list of strings)
            schema: Database schema
            include_stats: Whether to return detailed statistics

        Returns:
            Dictionary with operation statistics
        """
        # Convert single key to list for consistent handling
        if isinstance(key_columns, str):
            key_columns = [key_columns]

        # Fully qualified table names
        staging_fq = f"{schema}.{staging_table}"
        target_fq = f"{schema}.{target_table}"

        try:
            # Get column information for both tables
            staging_columns = self._get_table_columns(staging_table, schema)
            target_columns = self._get_table_columns(target_table, schema)

            # Find matching columns (intersection)
            matching_columns = [col for col in target_columns if col in staging_columns]

            if not matching_columns:
                raise ValueError(f"No matching columns found between {staging_fq} and {target_fq}")

            logger.info(f"Found {len(matching_columns)} matching columns: {matching_columns}")

            # Validate that all key columns exist in both tables
            missing_keys = [key for key in key_columns if key not in matching_columns]
            if missing_keys:
                raise ValueError(f"Key columns {missing_keys} not found in both tables")

            # Build column lists for SELECT statements
            column_list = ", ".join(matching_columns)

            # Build the join condition for multiple keys
            join_condition = " AND ".join([f"target.{col} = staging.{col}" for col in key_columns])

            # SQL for upserting with proper column matching
            if include_stats:
                sql = f"""
                WITH deleted_rows AS (
                    DELETE FROM {staging_fq}
                    RETURNING {column_list}
                ),
                removed_existing AS (
                    DELETE FROM {target_fq} target
                    USING deleted_rows staging
                    WHERE {join_condition}
                    RETURNING target.*
                ),
                inserted AS (
                    INSERT INTO {target_fq} ({column_list})
                    SELECT {column_list} FROM deleted_rows
                    RETURNING *
                )
                SELECT 
                    (SELECT COUNT(*) FROM deleted_rows) AS staging_count,
                    (SELECT COUNT(*) FROM removed_existing) AS deleted_count,
                    (SELECT COUNT(*) FROM inserted) AS inserted_count;
                """
            else:
                sql = f"""
                WITH deleted_rows AS (
                    DELETE FROM {staging_fq}
                    RETURNING {column_list}
                ),
                removed_existing AS (
                    DELETE FROM {target_fq} target
                    USING deleted_rows staging
                    WHERE {join_condition}
                )
                INSERT INTO {target_fq} ({column_list})
                SELECT {column_list} FROM deleted_rows;
                """

            with self.get_cursor() as cursor:
                cursor.execute(sql)

                if include_stats:
                    result = cursor.fetchone()
                    return {
                        'staging_count': result[0],
                        'deleted_count': result[1],
                        'inserted_count': result[2],
                        'matching_columns': matching_columns
                    }
                else:
                    return {
                        'affected_rows': cursor.rowcount,
                        'matching_columns': matching_columns
                    }

        except Exception as e:
            logger.error(f"Error in upsert operation: {str(e)}")
            raise

    def _get_table_columns(self, table_name: str, schema: str = 'public') -> List[str]:
        """
        Get column names for a table in the correct order.

        Args:
            table_name: Table name
            schema: Schema name

        Returns:
            List of column names in order
        """
        with self.get_cursor() as cursor:
            # First check if it's an Iceberg table
            iceberg_check = """
                            SELECT COUNT(*) \
                            FROM iceberg_tables \
                            WHERE table_name = %s; \
                            """
            cursor.execute(iceberg_check, (table_name,))
            is_iceberg = cursor.fetchone()[0] > 0

            if is_iceberg:
                # For Iceberg tables, get columns from iceberg metadata
                iceberg_query = """
                                WITH ice AS (SELECT crunchy_iceberg.metadata(metadata_location) metadata \
                                             FROM iceberg_tables \
                                             WHERE table_name = %s \
                                               AND table_namespace = %s)
                                SELECT field ->> 'name' as column_name
                                FROM ice,
                                     jsonb_array_elements( \
                                             metadata -> 'schemas' -> (metadata ->> 'current-schema-id')::int -> \
                                             'fields') as field
                                ORDER BY (field ->> 'id')::int; \
                                """
                cursor.execute(iceberg_query, (table_name, schema))

            else:
                # For regular PostgreSQL tables
                regular_query = """
                                SELECT column_name
                                FROM information_schema.columns
                                WHERE table_schema = %s \
                                  AND table_name = %s
                                ORDER BY ordinal_position; \
                                """
                cursor.execute(regular_query, (schema, table_name))

            results = cursor.fetchall()
            return [row[0].lower() for row in results]

    def upsert_with_column_mapping(self,
                                   staging_table: str,
                                   target_table: str,
                                   key_columns: Union[str, List[str]],
                                   column_mapping: Dict[str, str] = None,
                                   schema: str = 'public',
                                   include_stats: bool = True) -> Dict[str, int]:
        """
        Advanced upsert with explicit column mapping support.

        Args:
            staging_table: Source staging table name
            target_table: Target table name
            key_columns: Primary key column(s) for upserting
            column_mapping: Dict mapping staging_column -> target_column
            schema: Database schema
            include_stats: Whether to return detailed statistics

        Returns:
            Dictionary with operation statistics
        """
        # Convert single key to list
        if isinstance(key_columns, str):
            key_columns = [key_columns]

        staging_fq = f"{schema}.{staging_table}"
        target_fq = f"{schema}.{target_table}"

        try:
            # Get columns for both tables
            staging_columns = self._get_table_columns(staging_table, schema)
            target_columns = self._get_table_columns(target_table, schema)

            # Build column mapping
            if column_mapping:
                # Use provided mapping
                mapped_columns = []
                staging_select_columns = []

                for staging_col, target_col in column_mapping.items():
                    if staging_col in staging_columns and target_col in target_columns:
                        mapped_columns.append(target_col)
                        staging_select_columns.append(f"{staging_col} as {target_col}")

                target_column_list = ", ".join(mapped_columns)
                staging_select_list = ", ".join(staging_select_columns)

            else:
                # Auto-match columns by name
                matching_columns = [col for col in target_columns if col in staging_columns]
                target_column_list = ", ".join(matching_columns)
                staging_select_list = target_column_list

            # Validate key columns
            key_check_columns = mapped_columns if column_mapping else matching_columns
            missing_keys = [key for key in key_columns if key not in key_check_columns]
            if missing_keys:
                raise ValueError(f"Key columns {missing_keys} not available after column mapping")

            # Build join condition
            join_condition = " AND ".join([f"target.{col} = staging.{col}" for col in key_columns])

            # Execute upsert
            if include_stats:
                sql = f"""
                WITH deleted_rows AS (
                    DELETE FROM {staging_fq}
                    RETURNING {staging_select_list}
                ),
                removed_existing AS (
                    DELETE FROM {target_fq} target
                    USING deleted_rows staging
                    WHERE {join_condition}
                    RETURNING target.*
                ),
                inserted AS (
                    INSERT INTO {target_fq} ({target_column_list})
                    SELECT {staging_select_list} FROM deleted_rows
                    RETURNING *
                )
                SELECT 
                    (SELECT COUNT(*) FROM deleted_rows) AS staging_count,
                    (SELECT COUNT(*) FROM removed_existing) AS deleted_count,
                    (SELECT COUNT(*) FROM inserted) AS inserted_count;
                """
            else:
                sql = f"""
                WITH deleted_rows AS (
                    DELETE FROM {staging_fq}
                    RETURNING {staging_select_list}
                ),
                removed_existing AS (
                    DELETE FROM {target_fq} target
                    USING deleted_rows staging
                    WHERE {join_condition}
                )
                INSERT INTO {target_fq} ({target_column_list})
                SELECT {staging_select_list} FROM deleted_rows;
                """

            with self.get_cursor() as cursor:
                cursor.execute(sql)

                if include_stats:
                    result = cursor.fetchone()
                    return {
                        'staging_count': result[0],
                        'deleted_count': result[1],
                        'inserted_count': result[2],
                        'column_mapping_used': column_mapping or 'auto-matched'
                    }
                else:
                    return {
                        'affected_rows': cursor.rowcount,
                        'column_mapping_used': column_mapping or 'auto-matched'
                    }

        except Exception as e:
            logger.error(f"Error in mapped upsert operation: {str(e)}")
            raise

    def load_df_to_iceberg_table(self,
                                 df: pd.DataFrame,
                                 target_table: str,
                                 key_columns: Union[str, List[str]],
                                 schema: str = 'public',
                                 business_key_columns: Union[str, List[str], None] = None,
                                 staging_table: str = None,
                                 use_transaction: bool = True,
                                 include_stats: bool = True) -> Dict[str, Any]:
        """
        Load DataFrame to Iceberg table with deduplication using staging table approach.

        Args:
            df: DataFrame to load
            target_table: Target Iceberg table name
            key_columns: Primary key column(s) for upserting (string or list of strings)
            schema: Database schema
            business_key_columns: Optional business key column(s) for complete replacement
            staging_table: Optional custom staging table name
            use_transaction: Whether to wrap operations in a single transaction
            include_stats: Whether to return detailed statistics

        Returns:
            Dictionary with operation results and statistics
        """
        results = {'success': False}
        staging_table_created = None

        if df.empty:
            results['message'] = "DataFrame is empty, no data to load"
            results['success'] = True
            return results

        try:
            # Generate staging table name if not provided
            if not staging_table:
                staging_table = f"{target_table}_staging_{int(time.time())}"

            if use_transaction:
                # Execute all operations within a single transaction
                with self.get_connection() as conn:
                    with conn.cursor() as cursor:
                        # 1. Create staging table and load DataFrame data
                        self.bulk_insert_df(
                            df=df,
                            table=staging_table,
                            if_exists='replace',
                            schema=schema
                        )
                        staging_table_created = staging_table
                        results['staging_table'] = f"{schema}.{staging_table}"
                        results['staging_rows_loaded'] = len(df)

                        # 2. Handle business key deletion if specified
                        if business_key_columns:
                            deletion_stats = self._delete_by_business_key_transactional(
                                staging_table=staging_table,
                                target_table=target_table,
                                business_key_columns=business_key_columns,
                                schema=schema,
                                cursor=cursor
                            )
                            results['business_key_deletion'] = deletion_stats

                        # 3. Perform upsert using existing method
                        upsert_stats = self.upsert_from_staging_v2(
                            staging_table=staging_table,
                            target_table=target_table,
                            key_columns=key_columns,
                            schema=schema,
                            include_stats=include_stats
                        )

                        results.update(upsert_stats)
                        results['success'] = True
                        results['message'] = f"Successfully loaded DataFrame to {schema}.{target_table}"
                        results['transaction_used'] = True
            else:
                # Non-transactional mode
                # Create staging table and load DataFrame data
                self.bulk_insert_df(
                    df=df,
                    table=staging_table,
                    if_exists='replace',
                    schema=schema
                )
                staging_table_created = staging_table
                results['staging_table'] = f"{schema}.{staging_table}"
                results['staging_rows_loaded'] = len(df)

                # Handle business key deletion if specified
                if business_key_columns:
                    deletion_stats = self._delete_by_business_key_transactional(
                        staging_table=staging_table,
                        target_table=target_table,
                        business_key_columns=business_key_columns,
                        schema=schema,
                        cursor=None  # Will create its own cursor
                    )
                    results['business_key_deletion'] = deletion_stats

                # Perform upsert using existing method
                upsert_stats = self.upsert_from_staging_v2(
                    staging_table=staging_table,
                    target_table=target_table,
                    key_columns=key_columns,
                    schema=schema,
                    include_stats=include_stats
                )

                results.update(upsert_stats)
                results['success'] = True
                results['message'] = f"Successfully loaded DataFrame to {schema}.{target_table}"
                results['transaction_used'] = False

            return results

        except Exception as e:
            logger.error(f"Error in load_df_to_iceberg_table: {str(e)}")
            results['error'] = str(e)
            results['success'] = False

            if use_transaction:
                logger.info("Transaction rolled back due to error")
                results['rollback_performed'] = True

            return results

        finally:
            # Clean up staging table if it exists
            if staging_table_created:
                try:
                    self.execute_query(f"DROP TABLE IF EXISTS {schema}.{staging_table_created}", autocommit=True)
                    logger.info(f"Cleaned up staging table: {schema}.{staging_table_created}")
                except Exception as e:
                    logger.warning(f"Failed to drop staging table: {str(e)}")

    def load_from_s3_to_iceberg(self,
                                s3_path: str,
                                target_table: str,
                                key_columns: Union[str, List[str]],
                                schema: str = 'public',
                                business_key_columns: Union[str, List[str], None] = None,
                                validate_schema: bool = False,
                                staging_table: str = None,
                                use_transaction: bool = True) -> Dict[str, Any]:
        """
        Complete workflow to load data from S3 parquet file to an Iceberg table.
        Supports both single key column and composite keys with full transaction support.

        Args:
            s3_path: S3 path to the Parquet file
            target_table: Target Iceberg table
            key_columns: Primary key column(s) for upserting (string or list of strings)
            schema: Database schema
            business_key_columns: Optional business key column(s) for complete replacement (string or list of strings)
            validate_schema: Whether to validate schema before loading
            staging_table: Optional custom staging table name
            use_transaction: Whether to wrap operations in a single transaction (default: True)

        Returns:
            Dictionary with operation results and statistics
        """
        results = {'success': False}
        staging_table_created = None

        try:
            # Generate staging table name if not provided
            if not staging_table:
                staging_table = f"{target_table}_staging_{int(time.time())}"

            # Validate schema if requested (this happens outside transaction)
            if validate_schema:
                validation = self.validate_parquet_schema(s3_path, target_table, schema)
                results['schema_validation'] = validation

                if not validation['is_valid']:
                    logger.warning(f"Schema validation failed: {validation}")
                    results['message'] = "Schema validation failed"
                    return results

            if use_transaction:
                # Execute all operations within a single transaction
                with self.get_connection() as conn:
                    with conn.cursor() as cursor:
                        # 1. Create staging table from S3
                        staging_table_created = self._create_staging_table_transactional(
                            s3_path, staging_table, schema, cursor
                        )
                        results['staging_table'] = f"{schema}.{staging_table}"

                        # 2. Handle business key deletion if specified
                        if business_key_columns:
                            deletion_stats = self._delete_by_business_key_transactional(
                                staging_table=staging_table,
                                target_table=target_table,
                                business_key_columns=business_key_columns,
                                schema=schema,
                                cursor=cursor
                            )
                            results['business_key_deletion'] = deletion_stats

                        # 3. Perform upsert
                        upsert_stats = self.upsert_from_staging(
                            staging_table=staging_table,
                            target_table=target_table,
                            key_columns=key_columns,
                            schema=schema,
                            cursor=cursor
                        )

                        results.update(upsert_stats)

                        # If we reach here, transaction commits automatically
                        results['success'] = True
                        results['message'] = f"Successfully loaded data from {s3_path} to {schema}.{target_table}"
                        results['transaction_used'] = True

            else:
                # Non-transactional mode (original behavior)
                # Create staging table from S3
                self.create_staging_table(s3_path, staging_table, schema)
                staging_table_created = staging_table
                results['staging_table'] = f"{schema}.{staging_table}"

                # Handle business key deletion if specified
                if business_key_columns:
                    deletion_stats = self.delete_by_business_key(
                        staging_table=staging_table,
                        target_table=target_table,
                        business_key_columns=business_key_columns,
                        schema=schema
                    )
                    results['business_key_deletion'] = deletion_stats

                # Perform upsert
                upsert_stats = self.upsert_from_staging(
                    staging_table=staging_table,
                    target_table=target_table,
                    key_columns=key_columns,
                    schema=schema
                )

                results.update(upsert_stats)
                results['success'] = True
                results['message'] = f"Successfully loaded data from {s3_path} to {schema}.{target_table}"
                results['transaction_used'] = False

            return results

        except Exception as e:
            logger.error(f"Error in load_from_s3_to_iceberg: {str(e)}")
            results['error'] = str(e)
            results['success'] = False

            if use_transaction:
                logger.info("Transaction rolled back due to error")
                results['rollback_performed'] = True

            return results

        finally:
            # Clean up staging table if it exists (always in separate transaction)
            if staging_table_created:
                try:
                    self.execute_query(f"DROP TABLE IF EXISTS {schema}.{staging_table_created}", autocommit=True)
                    logger.info(f"Cleaned up staging table: {schema}.{staging_table_created}")
                except Exception as e:
                    logger.warning(f"Failed to drop staging table: {str(e)}")

    def _create_staging_table_transactional(self,
                                            s3_path: str,
                                            staging_table: str,
                                            schema: str,
                                            cursor) -> str:
        """
        Create a staging table from S3 using an existing cursor/transaction.

        Args:
            s3_path: S3 path to the Parquet file
            staging_table: Name for the staging table
            schema: Database schema
            cursor: Database cursor within transaction

        Returns:
            Full table name (schema.table)
        """
        full_table_name = f"{schema}.{staging_table}"

        # First remove the file from cache if it exists
        cache_clear_query = f"SELECT crunchy_file_cache.remove('{s3_path}');"

        # Create staging table with data loaded from S3
        create_table_query = f"""
        CREATE TABLE {full_table_name} () 
        WITH (load_from = '{s3_path}');
        """

        # Execute within the provided transaction
        cursor.execute(cache_clear_query)
        logger.info(f"Removed {s3_path} from crunchy_file_cache")

        cursor.execute(create_table_query)
        logger.info(f"Created staging table {full_table_name} from {s3_path}")

        return staging_table

    def _delete_by_business_key_transactional(self,
                                              staging_table: str,
                                              target_table: str,
                                              business_key_columns: Union[str, List[str]],
                                              schema: str,
                                              cursor) -> Dict[str, int]:
        """
        Delete records from target table based on business key values using existing cursor/transaction.

        Args:
            staging_table: Staging table containing new data
            target_table: Target table to delete from
            business_key_columns: Business key column(s) for deletion
            schema: Database schema
            cursor: Database cursor within transaction

        Returns:
            Dictionary with deletion statistics
        """
        # Convert single key to list for consistent handling
        if isinstance(business_key_columns, str):
            business_key_columns = [business_key_columns]

        # Fully qualified table names
        staging_fq = f"{schema}.{staging_table}"
        target_fq = f"{schema}.{target_table}"

        # Build the join condition for business keys
        join_condition = " AND ".join([f"target.{col} = staging.{col}" for col in business_key_columns])

        # Get distinct business keys from staging table and delete matching records from target
        sql = f"""
        DELETE FROM {target_fq} target
        USING (
            SELECT DISTINCT {', '.join(business_key_columns)}
            FROM {staging_fq}
        ) staging
        WHERE {join_condition}
        """

        cursor.execute(sql)
        deleted_count = cursor.rowcount

        logger.info(f"Deleted {deleted_count} records from {target_fq} based on business key: {business_key_columns}")

        return {
            'deleted_by_business_key': deleted_count,
            'business_key_columns': business_key_columns
        }

    def upsert_from_staging(self,
                            staging_table: str,
                            target_table: str,
                            key_columns: Union[str, List[str]],  # Accept either a string or list
                            schema: str = 'public',
                            include_stats: bool = True,
                            cursor=None) -> Dict[str, int]:
        """
        Upsert data from a staging table to a target table in a single transaction.
        Supports both single key column and composite keys (multiple columns).

        Args:
            staging_table: Source staging table name
            target_table: Target Iceberg table name
            key_columns: Primary key column(s) for upserting (string or list of strings)
            schema: Database schema
            include_stats: Whether to return detailed statistics
            cursor: Optional cursor to use (for transaction control)

        Returns:
            Dictionary with operation statistics
        """
        # Convert single key to list for consistent handling
        if isinstance(key_columns, str):
            key_columns = [key_columns]

        # Fully qualified table names
        staging_fq = f"{schema}.{staging_table}"
        target_fq = f"{schema}.{target_table}"

        # Build the join condition for multiple keys
        join_condition = " AND ".join([f"target.{col} = staging.{col}" for col in key_columns])

        # SQL for upserting with stats
        if include_stats:
            sql = f"""
            WITH deleted_rows AS (
                DELETE FROM {staging_fq}
                RETURNING *
            ),
            removed_existing AS (
                DELETE FROM {target_fq} target
                USING deleted_rows staging
                WHERE {join_condition}
                RETURNING target.*
            ),
            inserted AS (
                INSERT INTO {target_fq}
                SELECT * FROM deleted_rows
                RETURNING *
            )
            SELECT 
                (SELECT COUNT(*) FROM deleted_rows) AS staging_count,
                (SELECT COUNT(*) FROM removed_existing) AS deleted_count,
                (SELECT COUNT(*) FROM inserted) AS inserted_count;
            """

            if cursor:
                # Use provided cursor (part of larger transaction)
                cursor.execute(sql)
                result = cursor.fetchone()
            else:
                # Create own cursor (standalone operation)
                with self.get_cursor() as cursor:
                    cursor.execute(sql)
                    result = cursor.fetchone()

            return {
                'staging_count': result[0],
                'deleted_count': result[1],
                'inserted_count': result[2]
            }
        else:
            # Simplified version without detailed stats for better performance
            sql = f"""
            WITH deleted_rows AS (
                DELETE FROM {staging_fq}
                RETURNING *
            ),
            removed_existing AS (
                DELETE FROM {target_fq} target
                USING deleted_rows staging
                WHERE {join_condition}
            )
            INSERT INTO {target_fq}
            SELECT * FROM deleted_rows;
            """

            if cursor:
                # Use provided cursor (part of larger transaction)
                cursor.execute(sql)
                return {'affected_rows': cursor.rowcount}
            else:
                # Create own cursor (standalone operation)
                with self.get_cursor() as cursor:
                    cursor.execute(sql)
                    return {'affected_rows': cursor.rowcount}

    def table_exists(self, table_name: str, schema: str = 'public') -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Table name to check
            schema: Schema name

        Returns:
            True if table exists, False otherwise
        """
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s
            AND table_name = %s
        );
        """

        with self.get_cursor() as cursor:
            cursor.execute(query, (schema, table_name))
            return cursor.fetchone()[0]

    def health_check(
            self,
            table_name: str,
            date_column: str,
            time_period: str,
            schema: str = "public",
            extra_where: Optional[str] = None,
            joins: Optional[list[dict]] = None  # NEW parameter
    ) -> bool:
        """
        Health check to validate data availability till a minimum expected date.

        Args:
            table_name: Table name
            date_column: Date column to validate
            time_period: Expected data availability (e.g. 't-0', 't-1', 't-2')
            schema: Schema name
            extra_where: Optional extra WHERE condition (without 'WHERE')
            joins: Optional list of join configs, each with keys:
                   - 'table': str (e.g. 'public.other_table' or 'other_table')
                   - 'on': str (e.g. 'main.id = other.id')
                   - 'type': str (optional, defaults to 'LEFT')

        Returns:
            True if data is available till expected date, False otherwise
        """

        # Parse time period (t-0, t-1, t-2 ...)
        days_offset = int(time_period.replace("t-", ""))
        expected_date = date.today() - timedelta(days=days_offset)

        query = f"""
            SELECT MAX({date_column})
            FROM {schema}.{table_name} AS main
        """

        # Build JOIN clauses
        if joins:
            for join in joins:
                join_type = join.get("type", "LEFT").upper()
                join_table = join["table"]
                join_on = join["on"]
                query += f"\n        {join_type} JOIN {join_table} ON {join_on}"

        # Add extra condition if provided
        if extra_where:
            query += f"\n        WHERE {extra_where}"

        with self.get_cursor() as cursor:
            cursor.execute(query)
            max_available_date = cursor.fetchone()[0]

        # If table is empty or date is insufficient
        if not max_available_date:
            return False

        return max_available_date >= expected_date

    def is_iceberg_table(self, table_name: str, schema: str = 'public') -> bool:
        """
        Check if a table is an Iceberg table.

        Args:
            table_name: Table name to check
            schema: Schema name

        Returns:
            True if it's an Iceberg table, False otherwise
        """
        query = """
        SELECT COUNT(*) 
        FROM iceberg_tables 
        WHERE table_name = %s;
        """

        with self.get_cursor() as cursor:
            cursor.execute(query, (table_name,))
            return cursor.fetchone()[0] > 0

    def execute_transaction(self, statements: List[str], params: List[Dict] = None) -> bool:
        """
        Execute multiple SQL statements in a single transaction.

        Args:
            statements: List of SQL statements to execute
            params: List of parameter dictionaries (one per statement)

        Returns:
            True if transaction succeeded, False otherwise
        """
        if params and len(statements) != len(params):
            raise ValueError("Number of statements must match number of parameter dictionaries")

        # Default empty params if None
        if params is None:
            params = [{} for _ in statements]

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for i, statement in enumerate(statements):
                    cursor.execute(statement, params[i])

        return True


# Helper class for working with pipeline tracker integration
class TrackedPostgresConnector(PostgresConnector):
    """
    Extended PostgreSQL connector with integrated pipeline tracking.
    """

    def __init__(self, tracker=None, **kwargs):
        """
        Initialize with pipeline tracker.

        Args:
            tracker: PipelineTracker instance
            **kwargs: Arguments to pass to PostgresConnector
        """
        super().__init__(**kwargs)
        self.tracker = tracker

    def load_from_s3_to_iceberg_tracked(self,
                                        s3_path: str,
                                        target_table: str,
                                        key_column: str,
                                        run_id: str,
                                        step_name: str = "load_to_iceberg",
                                        **kwargs) -> Dict[str, Any]:
        """
        Load from S3 to Iceberg with pipeline tracking.

        Args:
            s3_path: S3 path to the Parquet file
            target_table: Target Iceberg table
            key_column: Primary key column for upserting
            run_id: Pipeline run ID for tracking
            step_name: Name for this pipeline step
            **kwargs: Additional arguments for load_from_s3_to_iceberg

        Returns:
            Dictionary with operation results
        """
        if not self.tracker:
            raise ValueError("Pipeline tracker not provided")

        # Start tracking step
        step_id = self.tracker.start_pipeline_step(run_id, step_name)

        try:
            # Execute the load operation
            results = self.load_from_s3_to_iceberg(
                s3_path=s3_path,
                target_table=target_table,
                key_column=key_column,
                **kwargs
            )

            # Complete step with success or failure
            if results['success']:
                record_count = results.get('inserted_count', 0)
                self.tracker.complete_pipeline_step(
                    step_id=step_id,
                    status="success",
                    record_count=record_count,
                    output_location=f"s3 -> {kwargs.get('schema', 'public')}.{target_table}"
                )
            else:
                self.tracker.complete_pipeline_step(
                    step_id=step_id,
                    status="failed",
                    error_message=results.get('error', 'Unknown error')
                )

            return results

        except Exception as e:
            # Handle exceptions and track failure
            error_message = str(e)
            logger.error(f"Error in tracked load operation: {error_message}")

            self.tracker.complete_pipeline_step(
                step_id=step_id,
                status="failed",
                error_message=error_message
            )

            return {
                'success': False,
                'error': error_message
            }