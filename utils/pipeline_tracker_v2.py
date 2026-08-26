import uuid
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional, List, Union
import pandas as pd
import os
from dotenv import load_dotenv, find_dotenv
import environmentconfig
# Import the PostgresWarehouseConnector
from postgresconnector_v2 import PostgresWarehouseConnector


class PipelineTracker:
    """
    Enhanced PipelineTracker designed for Crunchy Data Warehouse.
    Provides tracking, monitoring, and management of data pipelines.
    """

    def __init__(self,
                 env_file: str = ".env",
                 db_prefix: str = "",
                 use_iceberg: bool = True,
                 application_name: str = "PipelineTracker"):
        """
        Initialize the PipelineTracker with a PostgresWarehouseConnector.

        Args:
            env_file: Path to .env file with connection parameters
            db_prefix: Prefix for database variables (e.g., "SECONDARY_" for second database)
            use_iceberg: Whether to use Iceberg tables for tracking (recommended for analytics)
            application_name: Name to identify this application in pg_stat_activity
        """
        # Create PostgresWarehouseConnector with the db_prefix feature
        dotenv_path = find_dotenv(env_file)
        self.connector = PostgresWarehouseConnector(
            env_file=dotenv_path,
            db_prefix=db_prefix,
            application_name=application_name
        )

        # Store configuration
        self.use_iceberg = use_iceberg

        # Get database name for logging
        if db_prefix:
            db_name = os.getenv(f"{db_prefix}DB_NAME")
        else:
            db_name = os.getenv("DB_NAME")

        # Ensure tracking tables exist
        self._ensure_tables_exist()

        logging.info(f"PipelineTracker initialized with PostgresWarehouseConnector for database: {db_name}")
        if use_iceberg:
            logging.info("Using Iceberg tables for pipeline tracking")

    def _ensure_tables_exist(self):
        """
        Ensure all required tracking tables exist in the database.
        Creates them if they don't exist, using Iceberg tables if specified.
        """
        # Check for pipeline_runs table
        if not self.connector.table_exists('pipeline_runs'):
            logging.info("Creating pipeline_runs table...")

            create_sql = """
            CREATE TABLE pipeline_runs (
                run_id UUID PRIMARY KEY,
                pipeline_name VARCHAR(255) NOT NULL,
                client_id VARCHAR(255) NOT NULL,
                data_type VARCHAR(255) NOT NULL,
                start_time TIMESTAMP WITH TIME ZONE NOT NULL,
                end_time TIMESTAMP WITH TIME ZONE,
                status VARCHAR(50) NOT NULL,
                record_count BIGINT,
                parameters JSONB,
                error_details TEXT,
                created_by VARCHAR(255) NOT NULL
            )
            """

            if self.use_iceberg:
                create_sql += " USING iceberg"

            self.connector.execute_query(create_sql)

            # Create indexes for faster lookups
            self.connector.execute_query("""
            CREATE INDEX idx_pipeline_runs_pipeline_name ON pipeline_runs(pipeline_name);
            CREATE INDEX idx_pipeline_runs_client_id ON pipeline_runs(client_id);
            CREATE INDEX idx_pipeline_runs_status ON pipeline_runs(status);
            CREATE INDEX idx_pipeline_runs_start_time ON pipeline_runs(start_time);
            """)

        # Check for pipeline_steps table
        if not self.connector.table_exists('pipeline_steps'):
            logging.info("Creating pipeline_steps table...")

            create_sql = """
            CREATE TABLE pipeline_steps (
                step_id SERIAL PRIMARY KEY,
                run_id UUID NOT NULL REFERENCES pipeline_runs(run_id),
                step_name VARCHAR(255) NOT NULL,
                start_time TIMESTAMP WITH TIME ZONE NOT NULL,
                end_time TIMESTAMP WITH TIME ZONE,
                status VARCHAR(50) NOT NULL,
                record_count BIGINT,
                error_message TEXT,
                output_location TEXT
            )
            """

            if self.use_iceberg:
                create_sql += " USING iceberg"

            self.connector.execute_query(create_sql)

            # Create indexes for faster lookups
            self.connector.execute_query("""
            CREATE INDEX idx_pipeline_steps_run_id ON pipeline_steps(run_id);
            CREATE INDEX idx_pipeline_steps_status ON pipeline_steps(status);
            CREATE INDEX idx_pipeline_steps_start_time ON pipeline_steps(start_time);
            """)

        # Check for data_registry table
        if not self.connector.table_exists('data_registry'):
            logging.info("Creating data_registry table...")

            create_sql = """
            CREATE TABLE data_registry (
                id SERIAL PRIMARY KEY,
                client_id VARCHAR(255) NOT NULL,
                data_type VARCHAR(255) NOT NULL,
                source_id VARCHAR(255) NOT NULL,
                run_id UUID NOT NULL REFERENCES pipeline_runs(run_id),
                processed_at TIMESTAMP WITH TIME ZONE NOT NULL,
                output_location TEXT NOT NULL,
                is_current BOOLEAN NOT NULL DEFAULT TRUE,
                source_updated_at TIMESTAMP WITH TIME ZONE,
                metadata JSONB
            )
            """

            if self.use_iceberg:
                create_sql += " USING iceberg"

            self.connector.execute_query(create_sql)

            # Create indexes for faster lookups
            self.connector.execute_query("""
            CREATE INDEX idx_data_registry_client_type ON data_registry(client_id, data_type);
            CREATE INDEX idx_data_registry_source_id ON data_registry(source_id);
            CREATE INDEX idx_data_registry_run_id ON data_registry(run_id);
            CREATE INDEX idx_current_data_registry ON data_registry(is_current) WHERE is_current = TRUE;
            """)

        # Check for data_registry_history table
        if not self.connector.table_exists('data_registry_history'):
            logging.info("Creating data_registry_history table...")

            create_sql = """
            CREATE TABLE data_registry_history (
                history_id SERIAL PRIMARY KEY,
                id INTEGER NOT NULL,
                client_id VARCHAR(255) NOT NULL,
                data_type VARCHAR(255) NOT NULL,
                source_id VARCHAR(255) NOT NULL,
                run_id UUID NOT NULL,
                processed_at TIMESTAMP WITH TIME ZONE NOT NULL,
                output_location TEXT NOT NULL,
                source_updated_at TIMESTAMP WITH TIME ZONE,
                metadata JSONB,
                archived_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                archived_by_run_id UUID NOT NULL
            )
            """

            if self.use_iceberg:
                create_sql += " USING iceberg"

            self.connector.execute_query(create_sql)

            # Create indexes for faster lookups
            self.connector.execute_query("""
            CREATE INDEX idx_data_registry_history_id ON data_registry_history(id);
            CREATE INDEX idx_data_registry_history_run_id ON data_registry_history(run_id);
            """)

        # If using Iceberg, vacuum the tables to optimize storage
        if self.use_iceberg:
            for table in ['pipeline_runs', 'pipeline_steps', 'data_registry', 'data_registry_history']:
                self.connector.vacuum_iceberg_table(table)

    def start_pipeline_run(self,
                           pipeline_name: str,
                           client_id: str,
                           data_type: str,
                           parameters: Dict[str, Any] = None,
                           created_by: str = "system") -> str:
        """
        Start tracking a new pipeline run.

        Args:
            pipeline_name: Name of the pipeline
            client_id: Client identifier
            data_type: Type of data being processed
            parameters: Optional parameters for the run
            created_by: User or system that started the run

        Returns:
            run_id: UUID for the new pipeline run
        """
        run_id = str(uuid.uuid4())

        try:
            query = """
                INSERT INTO pipeline_runs (
                    run_id, pipeline_name, client_id, data_type,
                    start_time, status, parameters, created_by
                ) VALUES (
                    :run_id, :pipeline_name, :client_id, :data_type,
                    :start_time, :status, :parameters, :created_by
                )
            """

            # Convert dict to JSON string if it exists
            parameters_json = json.dumps(parameters) if parameters else None

            params = {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "client_id": client_id,
                "data_type": data_type,
                "start_time": datetime.now(),
                "status": "running",
                "parameters": parameters_json,
                "created_by": created_by
            }

            # Using PostgresWarehouseConnector's execute_query method
            self.connector.execute_query(query, params)

            logging.info(f"Started pipeline run {run_id} for {pipeline_name}/{client_id}")
            return run_id

        except Exception as e:
            logging.error(f"Error starting pipeline run: {str(e)}")
            raise

    def start_pipeline_step(self, run_id: str, step_name: str) -> int:
        """
        Start tracking a pipeline step.

        Args:
            run_id: The pipeline run UUID
            step_name: Name of the step

        Returns:
            step_id: The ID of the step entry
        """
        try:
            # Using connector's execute_with_result for simplified operation
            query = """
                INSERT INTO pipeline_steps (
                    run_id, step_name, start_time, status
                ) VALUES (
                    :run_id, :step_name, :start_time, :status
                )
                RETURNING step_id
            """

            params = {
                "run_id": run_id,
                "step_name": step_name,
                "start_time": datetime.now(),
                "status": "running"
            }

            _, results = self.connector.execute_with_result(query, params)
            step_id = results[0]['step_id']

            logging.info(f"Started step {step_name} (ID: {step_id}) for run {run_id}")
            return step_id

        except Exception as e:
            logging.error(f"Error starting pipeline step: {str(e)}")
            raise

    def complete_pipeline_step(self,
                               step_id: int,
                               status: str,
                               record_count: int = None,
                               error_message: str = None,
                               output_location: str = None) -> None:
        """
        Complete a pipeline step.

        Args:
            step_id: The step ID
            status: Status of the step ('success', 'failed', etc.)
            record_count: Number of records processed
            error_message: Error message if failed
            output_location: S3 location where output is stored
        """
        try:
            query = """
                UPDATE pipeline_steps
                SET end_time = :end_time,
                    status = :status,
                    record_count = :record_count,
                    error_message = :error_message,
                    output_location = :output_location
                WHERE step_id = :step_id
            """

            params = {
                "step_id": step_id,
                "end_time": datetime.now(),
                "status": status,
                "record_count": record_count,
                "error_message": error_message,
                "output_location": output_location
            }

            # Using PostgresWarehouseConnector's execute_query method
            self.connector.execute_query(query, params)

            logging.info(f"Completed step {step_id} with status {status}")

        except Exception as e:
            logging.error(f"Error completing pipeline step: {str(e)}")
            raise

    def get_pipeline_step(self,
                          run_id: str,
                          step_name: str = None,
                          step_id: int = None,
                          status: str = None) -> pd.DataFrame:
        """
        Get details of pipeline steps for a specific run.

        Args:
            run_id: The pipeline run UUID
            step_name: Optional name of the step to filter by
            step_id: Optional step ID to get a specific step
            status: Optional status to filter by

        Returns:
            DataFrame with pipeline step details
        """
        try:
            query = """
                SELECT step_id, run_id, step_name, start_time, end_time, 
                       status, record_count, error_message, output_location
                FROM pipeline_steps
                WHERE run_id = :run_id
            """

            params = {"run_id": run_id}

            if step_name:
                query += " AND step_name = :step_name"
                params["step_name"] = step_name

            if step_id:
                query += " AND step_id = :step_id"
                params["step_id"] = step_id

            if status:
                query += " AND status = :status"
                params["status"] = status

            query += " ORDER BY start_time ASC"

            # Using PostgresWarehouseConnector's read_query method
            result = self.connector.read_query(query, params)

            logging.info(f"Retrieved {len(result)} pipeline steps for run {run_id}")
            return result

        except Exception as e:
            logging.error(f"Error getting pipeline step details: {str(e)}")
            raise

    def get_latest_pipeline_step(self,
                                 run_id: str,
                                 step_name: str = None,
                                 status: str = "success") -> Optional[pd.DataFrame]:
        """
        Get the latest successful pipeline step for a specific run.

        Args:
            run_id: The pipeline run UUID
            step_name: Optional name of the step to filter by
            status: Status to filter by (default 'success')

        Returns:
            First row of the result DataFrame, or None if no matching step found
        """
        try:
            query = """
                SELECT step_id, run_id, step_name, start_time, end_time, 
                       status, record_count, error_message, output_location
                FROM pipeline_steps
                WHERE run_id = :run_id AND status = :status
            """

            params = {"run_id": run_id, "status": status}

            if step_name:
                query += " AND step_name = :step_name"
                params["step_name"] = step_name

            query += " ORDER BY start_time DESC LIMIT 1"

            # Using PostgresWarehouseConnector's read_query method
            result = self.connector.read_query(query, params)

            if result.empty:
                return None

            return result.iloc[0]

        except Exception as e:
            logging.error(f"Error getting latest pipeline step: {str(e)}")
            raise

    def complete_pipeline_run(self,
                              run_id: str,
                              status: str,
                              record_count: int = None,
                              error_details: str = None) -> None:
        """
        Complete a pipeline run.

        Args:
            run_id: The pipeline run UUID
            status: Final status of the run
            record_count: Total records processed
            error_details: Error details if failed
        """
        try:
            query = """
                UPDATE pipeline_runs
                SET end_time = :end_time,
                    status = :status,
                    record_count = :record_count,
                    error_details = :error_details
                WHERE run_id = :run_id
            """

            params = {
                "run_id": run_id,
                "end_time": datetime.now(),
                "status": status,
                "record_count": record_count,
                "error_details": error_details
            }

            # Using PostgresWarehouseConnector's execute_query method
            self.connector.execute_query(query, params)

            logging.info(f"Completed pipeline run {run_id} with status {status}")

            # For Iceberg tables, we should vacuum after updates
            if self.use_iceberg:
                self.connector.vacuum_iceberg_table("pipeline_runs")
                self.connector.vacuum_iceberg_table("pipeline_steps")

        except Exception as e:
            logging.error(f"Error completing pipeline run: {str(e)}")
            raise

    def update_pipeline_status(self,
                               run_id: str,
                               status: str,
                               record_count: int = None,
                               error_details: str = None) -> None:
        """
        Update the status of a pipeline run without marking it as complete.

        Args:
            run_id: The pipeline run UUID
            status: Current status of the run
            record_count: Current record count if available
            error_details: Error details if relevant
        """
        try:
            query = """
                UPDATE pipeline_runs
                SET status = :status,
                    record_count = :record_count,
                    error_details = :error_details
                WHERE run_id = :run_id
            """

            params = {
                "run_id": run_id,
                "status": status,
                "record_count": record_count,
                "error_details": error_details
            }

            # Using PostgresWarehouseConnector's execute_query method
            self.connector.execute_query(query, params)

            logging.info(f"Updated pipeline run {run_id} with status {status}")

        except Exception as e:
            logging.error(f"Error updating pipeline run: {str(e)}")
            raise

    def get_pipeline_status(self,
                            client_id: str = None,
                            pipeline_name: str = None,
                            data_type: str = None,
                            days: int = 7,
                            status: str = None,
                            limit: int = 100) -> pd.DataFrame:
        """
        Get status of recent pipeline runs.

        Args:
            client_id: Optional client filter
            pipeline_name: Optional pipeline name filter
            data_type: Optional data type filter
            days: Number of days to look back
            status: Optional status filter
            limit: Maximum number of records to return

        Returns:
            DataFrame with pipeline run status
        """
        query = """
            SELECT run_id, pipeline_name, client_id, data_type,
                   start_time, end_time, status, record_count, 
                   parameters, error_details, created_by,
                   EXTRACT(EPOCH FROM (end_time - start_time)) as duration_seconds
            FROM pipeline_runs
            WHERE start_time > NOW() - INTERVAL ':days days'
        """

        params = {"days": days}

        if client_id:
            query += " AND client_id = :client_id"
            params["client_id"] = client_id

        if pipeline_name:
            query += " AND pipeline_name = :pipeline_name"
            params["pipeline_name"] = pipeline_name

        if data_type:
            query += " AND data_type = :data_type"
            params["data_type"] = data_type

        if status:
            query += " AND status = :status"
            params["status"] = status

        query += " ORDER BY start_time DESC LIMIT :limit"
        params["limit"] = limit

        try:
            # Using PostgresWarehouseConnector's read_query method
            result = self.connector.read_query(query, params)

            # Process JSON parameters column
            if 'parameters' in result.columns:
                result['parameters'] = result['parameters'].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else x
                )

            return result
        except Exception as e:
            logging.error(f"Error getting pipeline status: {str(e)}")
            raise

    def check_current_database(self) -> tuple:
        """
        Check which database the current connection is using.

        Returns:
            tuple: (database_name, schema_name)
        """
        try:
            query = "SELECT current_database(), current_schema()"
            result = self.connector.read_query(query)

            if not result.empty:
                current_db = result.iloc[0][0]
                current_schema = result.iloc[0][1]
                logging.info(f"Currently connected to database: {current_db}, schema: {current_schema}")
                return current_db, current_schema
            return None, None

        except Exception as e:
            logging.error(f"Error checking current database: {str(e)}")
            return None, None

    def register_data(self,
                      client_id: str,
                      data_type: str,
                      source_id: str,
                      run_id: str,
                      output_location: str,
                      source_updated_at: datetime = None,
                      metadata: Dict[str, Any] = None) -> int:
        """
        Register processed data in the data registry.

        Args:
            client_id: Client identifier
            data_type: Type of data
            source_id: ID from source system
            run_id: Pipeline run ID
            output_location: S3 location of processed data
            source_updated_at: When the data was updated in source
            metadata: Optional metadata for quick reference

        Returns:
            id: Registry entry ID
        """
        try:
            # Check if record exists
            check_query = """
                SELECT id FROM data_registry
                WHERE client_id = :client_id
                AND data_type = :data_type
                AND source_id = :source_id
                AND is_current = TRUE
            """

            check_params = {
                "client_id": client_id,
                "data_type": data_type,
                "source_id": source_id
            }

            # Using PostgresWarehouseConnector's read_query for the check
            existing_df = self.connector.read_query(check_query, check_params)

            if not existing_df.empty:
                # Archive current version
                existing_id = existing_df.iloc[0]['id']

                # Move current record to history
                archive_query = """
                    WITH source_record AS (
                        SELECT id, client_id, data_type, source_id, run_id,
                               processed_at, output_location, source_updated_at, metadata
                        FROM data_registry
                        WHERE id = :id
                    )
                    INSERT INTO data_registry_history (
                        id, client_id, data_type, source_id, run_id,
                        processed_at, output_location, source_updated_at, metadata,
                        archived_by_run_id
                    )
                    SELECT id, client_id, data_type, source_id, run_id,
                           processed_at, output_location, source_updated_at, metadata,
                           :archive_run_id
                    FROM source_record
                """

                archive_params = {
                    "id": existing_id,
                    "archive_run_id": run_id
                }

                self.connector.execute_query(archive_query, archive_params)

                # Update existing record
                update_query = """
                    UPDATE data_registry
                    SET run_id = :run_id,
                        processed_at = :processed_at,
                        output_location = :output_location,
                        source_updated_at = :source_updated_at,
                        metadata = :metadata
                    WHERE id = :id
                    RETURNING id
                """

                update_params = {
                    "id": existing_id,
                    "run_id": run_id,
                    "processed_at": datetime.now(),
                    "output_location": output_location,
                    "source_updated_at": source_updated_at,
                    "metadata": json.dumps(metadata) if metadata else None
                }

                _, results = self.connector.execute_with_result(update_query, update_params)

                return results[0]['id']
            else:
                # Insert new record
                insert_query = """
                    INSERT INTO data_registry (
                        client_id, data_type, source_id, run_id, 
                        processed_at, output_location, is_current,
                        source_updated_at, metadata
                    ) VALUES (
                        :client_id, :data_type, :source_id, :run_id, 
                        :processed_at, :output_location, TRUE,
                        :source_updated_at, :metadata
                    )
                    RETURNING id
                """

                insert_params = {
                    "client_id": client_id,
                    "data_type": data_type,
                    "source_id": source_id,
                    "run_id": run_id,
                    "processed_at": datetime.now(),
                    "output_location": output_location,
                    "source_updated_at": source_updated_at,
                    "metadata": json.dumps(metadata) if metadata else None
                }

                _, results = self.connector.execute_with_result(insert_query, insert_params)

                return results[0]['id']

        except Exception as e:
            logging.error(f"Error registering data: {str(e)}")
            raise

    def bulk_register_data(self, data_records: List[Dict[str, Any]], chunk_size: int = 1000) -> int:
        """
        Register multiple data records in bulk.

        Args:
            data_records: List of dictionaries containing data registry records
            chunk_size: Number of records to insert in each batch

        Returns:
            Number of records processed
        """
        try:
            # Convert to DataFrame for processing
            df = pd.DataFrame(data_records)

            # Ensure required columns
            required_cols = ['client_id', 'data_type', 'source_id', 'run_id', 'output_location']
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

            # Add processed_at and is_current if not present
            if 'processed_at' not in df.columns:
                df['processed_at'] = datetime.now()

            if 'is_current' not in df.columns:
                df['is_current'] = True

            # Convert metadata to JSON if present
            if 'metadata' in df.columns:
                df['metadata'] = df['metadata'].apply(
                    lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                )

            # Use the bulk_load method from PostgresWarehouseConnector
            records_inserted = self.connector.bulk_load_to_table(
                df=df,
                table_name='data_registry',
                chunk_size=chunk_size,
                if_exists='append',
                use_iceberg=self.use_iceberg
            )

            logging.info(f"Bulk registered {records_inserted} data records")
            return records_inserted

        except Exception as e:
            logging.error(f"Error in bulk register data: {str(e)}")
            raise

    def get_data_registry(self,
                          client_id: str = None,
                          data_type: str = None,
                          source_id: str = None,
                          run_id: str = None,
                          is_current: bool = None,
                          limit: int = 100,
                          offset: int = 0,
                          sort_by: str = 'processed_at',
                          sort_order: str = 'DESC') -> pd.DataFrame:
        """
        Query data registry with flexible filtering options.

        Args:
            client_id: Optional client identifier filter
            data_type: Optional data type filter
            source_id: Optional source ID filter
            run_id: Optional pipeline run UUID filter
            is_current: Optional boolean to filter for current/archived records
            limit: Maximum number of records to return (default 100)
            offset: Number of records to skip for pagination (default 0)
            sort_by: Column to sort results by (default 'processed_at')
            sort_order: Sort direction, 'ASC' or 'DESC' (default 'DESC')

        Returns:
            DataFrame with matching data registry entries
        """
        try:
            # Start building the query
            query = """
                SELECT id, client_id, data_type, source_id, run_id, 
                       processed_at, output_location, is_current, 
                       source_updated_at, metadata
                FROM data_registry
                WHERE 1=1
            """

            # Initialize params dictionary
            params = {}

            # Add filters based on provided parameters
            if client_id:
                query += " AND client_id = :client_id"
                params["client_id"] = client_id

            if data_type:
                query += " AND data_type = :data_type"
                params["data_type"] = data_type

            if source_id:
                query += " AND source_id = :source_id"
                params["source_id"] = source_id

            if run_id:
                query += " AND run_id = :run_id"
                params["run_id"] = run_id

            if is_current is not None:
                query += " AND is_current = :is_current"
                params["is_current"] = is_current

            # Add sorting
            valid_sort_columns = [
                'id', 'client_id', 'data_type', 'source_id', 'run_id',
                'processed_at', 'source_updated_at'
            ]

            if sort_by in valid_sort_columns:
                # Sanitize sort order to prevent SQL injection
                sort_direction = "DESC" if sort_order.upper() != "ASC" else "ASC"
                query += f" ORDER BY {sort_by} {sort_direction}"
            else:
                # Default sort if invalid column provided
                query += " ORDER BY processed_at DESC"

            # Add pagination
            query += " LIMIT :limit OFFSET :offset"
            params["limit"] = limit
            params["offset"] = offset

            # Execute query using PostgresWarehouseConnector's read_query method
            result_df = self.connector.read_query(query, params)

            # Process JSON metadata column if needed
            if 'metadata' in result_df.columns:
                result_df['metadata'] = result_df['metadata'].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else x
                )

            return result_df

        except Exception as e:
            logging.error(f"Error querying data registry: {str(e)}")
            raise

    def get_pipeline_metrics(self,
                             days: int = 30,
                             client_id: str = None,
                             pipeline_name: str = None) -> pd.DataFrame:
        """
        Get performance metrics for pipeline runs.

        Args:
            days: Number of days to analyze
            client_id: Optional client filter
            pipeline_name: Optional pipeline name filter

        Returns:
            DataFrame with pipeline metrics
        """
        query = """
        WITH pipeline_stats AS (
            SELECT 
                pipeline_name,
                client_id,
                data_type,
                COUNT(*) as run_count,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failure_count,
                AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration_seconds,
                MAX(EXTRACT(EPOCH FROM (end_time - start_time))) as max_duration_seconds,
                MIN(EXTRACT(EPOCH FROM (end_time - start_time))) as min_duration_seconds,
                SUM(record_count) as total_records,
                AVG(record_count) as avg_records
            FROM pipeline_runs
            WHERE 
                start_time > NOW() - INTERVAL ':days days'
                AND end_time IS NOT NULL
                {% if client_id %}AND client_id = :client_id{% endif %}
                {% if pipeline_name %}AND pipeline_name = :pipeline_name{% endif %}
            GROUP BY pipeline_name, client_id, data_type
        )
        SELECT 
            pipeline_name,
            client_id,
            data_type,
            run_count,
            success_count,
            failure_count,
            (success_count::float / NULLIF(run_count, 0) * 100) as success_rate,
            avg_duration_seconds,
            max_duration_seconds,
            min_duration_seconds,
            total_records,
            avg_records,
            (total_records::float / NULLIF(avg_duration_seconds, 0)) as avg_records_per_second
        FROM pipeline_stats
        ORDER BY run_count DESC
        """

        # Use Jinja2 templating to build the query
        from jinja2 import Template
        template = Template(query)
        rendered_query = template.render(
            client_id=client_id is not None,
            pipeline_name=pipeline_name is not None
        )

        params = {
            "days": days
        }

        if client_id:
            params["client_id"] = client_id

        if pipeline_name:
            params["pipeline_name"] = pipeline_name

        try:
            result = self.connector.read_query(rendered_query, params)
            return result
        except Exception as e:
            logging.error(f"Error getting pipeline metrics: {str(e)}")
            raise

    def close(self) -> None:
        """
        Close the database connections.
        """
        if hasattr(self, 'connector'):
            self.connector.close()
            logging.info("Closed PipelineTracker connections")
