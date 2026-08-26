import ksuid
import logging
from datetime import datetime
import json
import pandas as pd
import os
from dotenv import load_dotenv, find_dotenv

# Import the new enhanced PostgresConnector
from utils.postgresconnector_v3 import PostgresConnector


class PipelineTracker:
    def __init__(self,db_prefix="", query_dir=None):
        """
        Initialize the PipelineTracker with a PostgresConnector using environment variables.

        Args:
            env_file: Path to .env file with connection parameters
            db_prefix: Prefix for database variables (e.g., "SECONDARY_" for second database)
            query_dir: Optional directory for SQL query files
        """
        # Create PostgresConnector with the specified parameters

        self.connector = PostgresConnector(
            db_prefix=db_prefix,
            query_dir=query_dir
        )

        # Get database name for logging
        if db_prefix:
            db_name = os.getenv(f"{db_prefix}DB_NAME")
        else:
            db_name = os.getenv("DB_NAME")

        logging.info(f"PipelineTracker initialized with PostgresConnector for database: {db_name}")

    def start_pipeline_run(self, pipeline_name, client_id, data_type,
                           parameters=None, created_by="system"):
        """
        Start tracking a new pipeline run.

        Args:
            pipeline_name: Name of the pipeline
            client_id: Client identifier
            data_type: Type of data being processed
            parameters: Optional parameters for the run (including date ranges if needed)
            created_by: User or system that started the run

        Returns:
            run_id: UUID for the new pipeline run
        """
        run_id = str(ksuid.ksuid())

        try:
            # Insert pipeline run record
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

            # Using PostgresConnector's execute_query method
            self.connector.execute_query(query, params)

            logging.info(f"Started pipeline run {run_id} for {pipeline_name}/{client_id}")
            return run_id

        except Exception as e:
            logging.error(f"Error starting pipeline run: {str(e)}")
            raise

    def start_pipeline_step(self, run_id, step_name):
        try:
            query = """
                INSERT INTO pipeline_steps (
                    run_id, step_name, start_time, status
                ) VALUES (
                    :run_id, :step_name, :start_time, :status
                ) RETURNING step_id
            """

            params = {
                "run_id": run_id,
                "step_name": step_name,
                "start_time": datetime.now(),
                "status": "running"
            }

            # Use the dedicated method for RETURNING clauses
            step_id = self.connector.execute_query_returning(query, params, "step_id")

            logging.info(f"Started step {step_name} for run {run_id}")
            return step_id

        except Exception as e:
            logging.error(f"Error starting pipeline step: {str(e)}")
            raise

    def start_pipeline_step(self, run_id, step_name):
        """
        Start tracking a pipeline step.

        Args:
            run_id: The pipeline run UUID
            step_name: Name of the step

        Returns:
            step_id: The ID of the step entry
        """
        try:
            query = """
                INSERT INTO pipeline_steps (
                    run_id, step_name, start_time, status
                ) VALUES (
                    :run_id, :step_name, :start_time, :status
                ) RETURNING step_id
            """

            params = {
                "run_id": run_id,
                "step_name": step_name,
                "start_time": datetime.now(),
                "status": "running"
            }

            # Use the dedicated method for RETURNING clauses
            step_id = self.connector.execute_query_returning(query, params, "step_id")

            logging.info(f"Started step {step_name} for run {run_id}")
            return step_id

        except Exception as e:
            logging.error(f"Error starting pipeline step: {str(e)}")
            raise

    def complete_pipeline_step(self, step_id, status, record_count=None,
                               error_message=None, output_location=None):
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

            # Using PostgresConnector's execute_query method with parameter conversion
            self.connector.execute_query(query, params)

            logging.info(f"Completed step {step_id} with status {status}")

        except Exception as e:
            logging.error(f"Error completing pipeline step: {str(e)}")
            raise

    def get_pipeline_step(self, run_id, step_name=None, step_id=None):
        """
        Get details of pipeline steps for a specific run.

        Args:
            run_id: The pipeline run UUID
            step_name: Optional name of the step to filter by
            step_id: Optional step ID to get a specific step

        Returns:
            DataFrame with pipeline step details
        """
        try:
            query = """
                SELECT step_id, run_id, step_name, start_time, end_time, 
                       status, record_count, error_message, output_location
                FROM public.pipeline_steps
                WHERE run_id = :run_id AND status = 'success'
            """

            params = {"run_id": run_id}

            if step_name:
                query += " AND step_name = :step_name"
                params["step_name"] = step_name

            if step_id:
                query += " AND step_id = :step_id"
                params["step_id"] = step_id

            query += " ORDER BY start_time DESC LIMIT 1"

            # Using PostgresConnector's read_query method with parameter conversion
            result = self.connector.read_query(query, params)

            logging.info(f"Retrieved {len(result)} pipeline steps for run {run_id}")
            return result

        except Exception as e:
            logging.error(f"Error getting pipeline step details: {str(e)}")
            raise

    def complete_pipeline_run(self, run_id, status, record_count=None, error_details=None):
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

            # Using PostgresConnector's execute_query method with parameter conversion
            self.connector.execute_query(query, params)

            logging.info(f"Completed pipeline run {run_id} with status {status}")

        except Exception as e:
            logging.error(f"Error completing pipeline run: {str(e)}")
            raise

    def update_pipeline_status(self, run_id, status, record_count=None, error_details=None):
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

            # Using PostgresConnector's execute_query method with parameter conversion
            self.connector.execute_query(query, params)

            logging.info(f"Updated pipeline run {run_id} with status {status}")

        except Exception as e:
            logging.error(f"Error updating pipeline run: {str(e)}")
            raise

    def get_pipeline_end_time(self, run_id, step_name):
        try:
            query = """
                    SELECT end_time
                    FROM pipeline_steps
                    WHERE run_id = :run_id \
                      AND step_name = :step_name \
                    """

            params = {
                "run_id": run_id,
                "step_name": step_name,
            }

            # Execute the query once and get the end_time
            result = self.connector.execute_query_returning(query, params)

            if not result:
                logging.warning(f"No record found for run_id={run_id} and step_name={step_name}")
                return None

            end_time = result  # Assuming result is a list of dictionaries

            logging.info(f"End time for run_id {run_id} is {end_time}")
            return end_time

        except Exception as e:
            logging.error(f"Error retrieving pipeline end time: {str(e)}")
            raise

    def get_source_id(self, name, source_type):
        try:
            query = """
                    SELECT id
                    FROM data_sources
                    WHERE name = :name \
                      AND source_type = :source_type \
                    """

            params = {
                "name": name,
                "source_type": source_type,
            }

            # Execute the query once and get the end_time
            result = self.connector.execute_query_returning(query, params)

            if not result:
                logging.warning(f"No record found for source name {name} and source type={source_type}")
                return None

            logging.info(f"source id  for {name} is {result}")
            return result

        except Exception as e:
            logging.error(f"Error retrieving pipeline end time: {str(e)}")
            raise
    def get_pipeline_status(self, client_id=None, pipeline_name=None, data_type=None,
                            days=7, status=None, limit=100):
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
                   start_date, end_date, start_time, end_time, status, record_count
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
            # Using PostgresConnector's read_query method
            return self.connector.read_query(query, params)
        except Exception as e:
            logging.error(f"Error getting pipeline status: {str(e)}")
            raise

    def check_current_database(self):
        """
        Check which database the current connection is using.

        Returns:
            str: The name of the current database
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

    def register_data(self, client_id, data_type, source_id, run_id,
                      s3_location, processed_at, source_updated_at=None, is_current=False, metadata=None):
        """
        Register processed data in the data registry.
        Args:
            client_id: Client identifier
            data_type: Type of data
            source_id: ID from source system
            run_id: Pipeline run ID
            s3_location: S3 location of processed data
            processed_at: When the data was processed
            source_updated_at: When the data was updated in source (optional - keeps existing if None)
            is_current: Whether this is the current version
            metadata: Optional metadata for quick reference
        Returns:
            id: Registry entry ID
        """
        try:
            # First check if record exists
            check_query = """
                          SELECT id \
                          FROM data_registry
                          WHERE client_id = :client_id
                            AND run_id = :run_id \
                          """

            check_params = {
                "client_id": client_id,
                "run_id": run_id
            }

            # Using PostgresConnector's read_query for the check
            existing_df = self.connector.read_query(check_query, check_params)

            # Execute transaction
            if not existing_df.empty:
                # Update existing record
                existing_id = int(existing_df.iloc[0]['id'])

                with self.connector.get_connection() as conn:
                    with conn.cursor() as cursor:
                        # Build dynamic update query based on provided values
                        update_fields = []
                        update_params = {"id": existing_id}

                        # Always update these fields
                        update_fields.extend([
                            "processed_at = %(processed_at)s",
                            "s3_location = %(s3_location)s",
                            "is_current = %(is_current)s"
                        ])
                        update_params.update({
                            "processed_at": processed_at,
                            "s3_location": s3_location,
                            "is_current": is_current
                        })

                        # Only update source_updated_at if provided
                        if source_updated_at is not None:
                            update_fields.append("source_updated_at = %(source_updated_at)s")
                            update_params["source_updated_at"] = source_updated_at

                        # Only update metadata if provided
                        if metadata is not None:
                            update_fields.append("metadata = %(metadata)s")
                            update_params["metadata"] = json.dumps(metadata)

                        update_query = f"""
                            UPDATE data_registry
                            SET {', '.join(update_fields)}
                            WHERE id = %(id)s 
                            RETURNING id
                        """

                        cursor.execute(update_query, update_params)
                        result = cursor.fetchone()
                        return result[0] if result else None
            else:
                # Insert new record
                insert_query = """
                               INSERT INTO data_registry (client_id, data_type, source_id, run_id, \
                                                          processed_at, s3_location, is_current, \
                                                          source_updated_at, metadata) \
                               VALUES (%(client_id)s, %(data_type)s, %(source_id)s, %(run_id)s, \
                                       %(processed_at)s, %(s3_location)s, %(is_current)s, \
                                       %(source_updated_at)s, %(metadata)s) RETURNING id \
                               """

                params = {
                    "client_id": client_id,
                    "data_type": data_type,
                    "source_id": source_id,
                    "run_id": run_id,
                    "processed_at": processed_at,
                    "s3_location": s3_location,
                    "is_current": is_current,
                    "source_updated_at": source_updated_at,
                    "metadata": json.dumps(metadata) if metadata else None
                }

                with self.connector.get_cursor() as cursor:
                    cursor.execute(insert_query, params)
                    result = cursor.fetchone()
                    return result[0] if result else None

        except Exception as e:
            logging.error(f"Error registering data: {str(e)}")
            raise

    def bulk_register_data(self, data_records, chunk_size=1000):
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
                df['is_current'] = False

            # Convert any metadata dict to JSON string
            if 'metadata' in df.columns:
                df['metadata'] = df['metadata'].apply(lambda x: json.dumps(x) if x is not None else None)

            # Using bulk_insert_df from the enhanced connector
            records_inserted = self.connector.bulk_insert_df(
                df=df,
                table='data_registry',
                if_exists='append',
                chunk_size=chunk_size
            )

            logging.info(f"Bulk registered {records_inserted} data records")
            return records_inserted

        except Exception as e:
            logging.error(f"Error in bulk register data: {str(e)}")
            raise

    def get_data_registry(self, client_id=None, data_type=None, source_id=None,
                          run_id=None, is_current=None, limit=100, offset=0,
                          sort_by='processed_at', sort_order='DESC'):
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
                       processed_at, s3_location, is_current, 
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

            # Execute query using PostgresConnector's read_query method
            result_df = self.connector.read_query(query, params)

            # Process JSON metadata column if needed
            if 'metadata' in result_df.columns:
                result_df['metadata'] = result_df['metadata'].apply(
                    lambda x: json.loads(x) if isinstance(x, str) and x else x
                )

            return result_df

        except Exception as e:
            logging.error(f"Error querying data registry: {str(e)}")
            raise
# import uuid
# import logging
# from datetime import datetime
# import json
# import pandas as pd
# import os
# from dotenv import load_dotenv, find_dotenv
#
# # Import the PostgresConnector
# from utils.postgresconnector_v3 import PostgresConnector
#
#
# class PipelineTracker:
#     def __init__(self, env_file=".env", db_prefix=""):
#         """
#         Initialize the PipelineTracker with a PostgresConnector using environment variables.
#
#         Args:
#             env_file: Path to .env file with connection parameters
#             db_prefix: Prefix for database variables (e.g., "SECONDARY_" for second database)
#         """
#         # Create PostgresConnector with the db_prefix feature
#         dotenv_path = find_dotenv()
#         self.connector = PostgresConnector(env_file=dotenv_path, db_prefix=db_prefix)
#
#         # Get database name for logging
#         if db_prefix:
#             db_name = os.getenv(f"{db_prefix}DB_NAME")
#         else:
#             db_name = os.getenv("DB_NAME")
#
#         logging.info(f"PipelineTracker initialized with PostgresConnector for database: {db_name}")
#
#     def start_pipeline_run(self, pipeline_name, client_id, data_type,
#                            parameters=None, created_by="system"):
#         """
#         Start tracking a new pipeline run.
#
#         Args:
#             pipeline_name: Name of the pipeline
#             client_id: Client identifier
#             data_type: Type of data being processed
#             parameters: Optional parameters for the run (including date ranges if needed)
#             created_by: User or system that started the run
#
#         Returns:
#             run_id: UUID for the new pipeline run
#         """
#         run_id = str(uuid.uuid4())
#
#         try:
#             # Update the query to remove start_date and end_date fields
#             query = """
#                 INSERT INTO pipeline_runs (
#                     run_id, pipeline_name, client_id, data_type,
#                     start_time, status, parameters, created_by
#                 ) VALUES (
#                     :run_id, :pipeline_name, :client_id, :data_type,
#                     :start_time, :status, :parameters, :created_by
#                 )
#             """
#
#             # Convert dict to JSON string if it exists
#             parameters_json = json.dumps(parameters) if parameters else None
#
#             params = {
#                 "run_id": run_id,
#                 "pipeline_name": pipeline_name,
#                 "client_id": client_id,
#                 "data_type": data_type,
#                 "start_time": datetime.now(),
#                 "status": "running",
#                 "parameters": parameters_json,
#                 "created_by": created_by
#             }
#
#             # Using PostgresConnector's execute_query method
#             self.connector.execute_query(query, params)
#
#             logging.info(f"Started pipeline run {run_id} for {pipeline_name}/{client_id}")
#             return run_id
#
#         except Exception as e:
#             logging.error(f"Error starting pipeline run: {str(e)}")
#             raise
#
#
#     def start_pipeline_step(self, run_id, step_name):
#         """
#         Start tracking a pipeline step.
#
#         Args:
#             run_id: The pipeline run UUID
#             step_name: Name of the step
#
#         Returns:
#             step_id: The ID of the step entry
#         """
#         try:
#             # Using connector's context manager for connection
#             with self.connector.get_connection() as conn:
#                 with conn.cursor() as cur:
#                     query = """
#                         INSERT INTO pipeline_steps (
#                             run_id, step_name, start_time, status
#                         ) VALUES (
#                             %s, %s, %s, %s
#                         ) RETURNING step_id
#                     """
#
#                     cur.execute(query, (
#                         run_id,
#                         step_name,
#                         datetime.now(),
#                         "running"
#                     ))
#
#                     step_id = cur.fetchone()[0]
#
#             logging.info(f"Started step {step_name} for run {run_id}")
#             return step_id
#
#         except Exception as e:
#             logging.error(f"Error starting pipeline step: {str(e)}")
#             raise
#
#     def complete_pipeline_step(self, step_id, status, record_count=None,
#                                error_message=None, output_location=None):
#         """
#         Complete a pipeline step.
#
#         Args:
#             step_id: The step ID
#             status: Status of the step ('success', 'failed', etc.)
#             record_count: Number of records processed
#             error_message: Error message if failed
#             output_location: S3 location where output is stored
#         """
#         try:
#             query = """
#                 UPDATE pipeline_steps
#                 SET end_time = :end_time,
#                     status = :status,
#                     record_count = :record_count,
#                     error_message = :error_message,
#                     output_location = :output_location
#                 WHERE step_id = :step_id
#             """
#
#             params = {
#                 "step_id": step_id,
#                 "end_time": datetime.now(),
#                 "status": status,
#                 "record_count": record_count,
#                 "error_message": error_message,
#                 "output_location": output_location
#             }
#
#             # Using PostgresConnector's execute_query method
#             self.connector.execute_query(query, params)
#
#             logging.info(f"Completed step {step_id} with status {status}")
#
#         except Exception as e:
#             logging.error(f"Error completing pipeline step: {str(e)}")
#             raise
#
#     def get_pipeline_step(self, run_id, step_name=None, step_id=None):
#         """
#         Get details of pipeline steps for a specific run.
#
#         Args:
#             run_id: The pipeline run UUID
#             step_name: Optional name of the step to filter by
#             step_id: Optional step ID to get a specific step
#
#         Returns:
#             DataFrame with pipeline step details
#         """
#         try:
#             query = """
#                 SELECT step_id, run_id, step_name, start_time, end_time,
#                        status, record_count, error_message, output_location
#                 FROM public.pipeline_steps
#                 WHERE run_id = :run_id AND status = 'success'
#             """
#
#             params = {"run_id": run_id}
#
#             if step_name:
#                 query += " AND step_name = :step_name"
#                 params["step_name"] = step_name
#
#             if step_id:
#                 query += " AND step_id = :step_id"
#                 params["step_id"] = step_id
#
#             query += " ORDER BY start_time ASC LIMIT 1"
#
#             # Using PostgresConnector's read_query method
#             result = self.connector.read_query(query, params)
#
#             logging.info(f"Retrieved {len(result)} pipeline steps for run {run_id}")
#             return result
#
#         except Exception as e:
#             logging.error(f"Error getting pipeline step details: {str(e)}")
#             raise
#     def complete_pipeline_run(self, run_id, status, record_count=None, error_details=None):
#         """
#         Complete a pipeline run.
#
#         Args:
#             run_id: The pipeline run UUID
#             status: Final status of the run
#             record_count: Total records processed
#             error_details: Error details if failed
#         """
#         try:
#             query = """
#                 UPDATE pipeline_runs
#                 SET end_time = :end_time,
#                     status = :status,
#                     record_count = :record_count,
#                     error_details = :error_details
#                 WHERE run_id = :run_id
#             """
#
#             params = {
#                 "run_id": run_id,
#                 "end_time": datetime.now(),
#                 "status": status,
#                 "record_count": record_count,
#                 "error_details": error_details
#             }
#
#             # Using PostgresConnector's execute_query method
#             self.connector.execute_query(query, params)
#
#             logging.info(f"Completed pipeline run {run_id} with status {status}")
#
#         except Exception as e:
#             logging.error(f"Error completing pipeline run: {str(e)}")
#             raise
#
#     def update_pipeline_status(self, run_id, status, record_count=None, error_details=None):
#         """
#         Update the status of a pipeline run without marking it as complete.
#
#         Args:
#             run_id: The pipeline run UUID
#             status: Current status of the run
#             record_count: Current record count if available
#             error_details: Error details if relevant
#         """
#         try:
#             query = """
#                 UPDATE pipeline_runs
#                 SET status = :status,
#                     record_count = :record_count,
#                     error_details = :error_details
#                 WHERE run_id = :run_id
#             """
#
#             params = {
#                 "run_id": run_id,
#                 "status": status,
#                 "record_count": record_count,
#                 "error_details": error_details
#             }
#
#             # Using PostgresConnector's execute_query method
#             self.connector.execute_query(query, params)
#
#             logging.info(f"Updated pipeline run {run_id} with status {status}")
#
#         except Exception as e:
#             logging.error(f"Error updating pipeline run: {str(e)}")
#             raise
#
#     def get_pipeline_status(self, client_id=None, pipeline_name=None, data_type=None,
#                             days=7, status=None, limit=100):
#         """
#         Get status of recent pipeline runs.
#
#         Args:
#             client_id: Optional client filter
#             pipeline_name: Optional pipeline name filter
#             data_type: Optional data type filter
#             days: Number of days to look back
#             status: Optional status filter
#             limit: Maximum number of records to return
#
#         Returns:
#             DataFrame with pipeline run status
#         """
#         query = """
#             SELECT run_id, pipeline_name, client_id, data_type,
#                    start_date, end_date, start_time, end_time, status, record_count
#             FROM pipeline_runs
#             WHERE start_time > NOW() - INTERVAL ':days days'
#         """
#
#         params = {"days": days}
#
#         if client_id:
#             query += " AND client_id = :client_id"
#             params["client_id"] = client_id
#
#         if pipeline_name:
#             query += " AND pipeline_name = :pipeline_name"
#             params["pipeline_name"] = pipeline_name
#
#         if data_type:
#             query += " AND data_type = :data_type"
#             params["data_type"] = data_type
#
#         if status:
#             query += " AND status = :status"
#             params["status"] = status
#
#         query += " ORDER BY start_time DESC LIMIT :limit"
#         params["limit"] = limit
#
#         try:
#             # Using PostgresConnector's read_query method
#             return self.connector.read_query(query, params)
#         except Exception as e:
#             logging.error(f"Error getting pipeline status: {str(e)}")
#             raise
#
#     def check_current_database(self):
#         """
#         Check which database the current connection is using.
#
#         Returns:
#             str: The name of the current database
#         """
#         try:
#             query = "SELECT current_database(), current_schema()"
#             result = self.connector.read_query(query)
#
#             if not result.empty:
#                 current_db = result.iloc[0][0]
#                 current_schema = result.iloc[0][1]
#                 logging.info(f"Currently connected to database: {current_db}, schema: {current_schema}")
#                 return current_db, current_schema
#             return None, None
#
#         except Exception as e:
#             logging.error(f"Error checking current database: {str(e)}")
#             return None, None
#     def register_data(self, client_id, data_type, source_id, run_id,
#                       output_location, source_updated_at=None, metadata=None):
#         """
#         Register processed data in the data registry.
#
#         Args:
#             client_id: Client identifier
#             data_type: Type of data
#             source_id: ID from source system
#             run_id: Pipeline run ID
#             output_location: S3 location of processed data
#             source_updated_at: When the data was updated in source
#             metadata: Optional metadata for quick reference
#
#         Returns:
#             id: Registry entry ID
#         """
#         try:
#             # First check if record exists
#             check_query = """
#                 SELECT id FROM data_registry
#                 WHERE client_id = :client_id
#                 AND data_type = :data_type
#                 AND source_id = :source_id
#                 AND is_current = TRUE
#             """
#
#             check_params = {
#                 "client_id": client_id,
#                 "data_type": data_type,
#                 "source_id": source_id
#             }
#
#             # Using PostgresConnector's read_query for the check
#             existing_df = self.connector.read_query(check_query, check_params)
#
#             # Using context manager for transactions that need returning values
#             with self.connector.get_connection() as conn:
#                 with conn.cursor() as cur:
#                     if not existing_df.empty:
#                         # Archive current version
#                         existing_id = existing_df.iloc[0]['id']
#
#                         archive_query = """
#                             INSERT INTO data_registry_history (
#                                 id, client_id, data_type, source_id, run_id,
#                                 processed_at, output_location, source_updated_at, metadata,
#                                 archived_by_run_id
#                             )
#                             SELECT id, client_id, data_type, source_id, run_id,
#                                    processed_at, output_location, source_updated_at, metadata,
#                                    %s
#                             FROM data_registry
#                             WHERE id = %s
#                         """
#
#                         cur.execute(archive_query, (run_id, existing_id))
#
#                         # Update existing record
#                         update_query = """
#                             UPDATE data_registry
#                             SET run_id = %s,
#                                 processed_at = %s,
#                                 output_location = %s,
#                                 source_updated_at = %s,
#                                 metadata = %s
#                             WHERE id = %s
#                             RETURNING id
#                         """
#
#                         cur.execute(update_query, (
#                             run_id,
#                             datetime.now(),
#                             output_location,
#                             source_updated_at,
#                             json.dumps(metadata) if metadata else None,
#                             existing_id
#                         ))
#
#                         return cur.fetchone()[0]
#                     else:
#                         # Insert new record
#                         insert_query = """
#                             INSERT INTO data_registry (
#                                 client_id, data_type, source_id, run_id,
#                                 processed_at, output_location, is_current,
#                                 source_updated_at, metadata
#                             ) VALUES (
#                                 %s, %s, %s, %s, %s, %s, TRUE, %s, %s
#                             )
#                             RETURNING id
#                         """
#
#                         cur.execute(insert_query, (
#                             client_id,
#                             data_type,
#                             source_id,
#                             run_id,
#                             datetime.now(),
#                             output_location,
#                             source_updated_at,
#                             json.dumps(metadata) if metadata else None
#                         ))
#
#                         return cur.fetchone()[0]
#
#         except Exception as e:
#             logging.error(f"Error registering data: {str(e)}")
#             raise
#
#     def bulk_register_data(self, data_records, chunk_size=1000):
#         """
#         Register multiple data records in bulk.
#
#         Args:
#             data_records: List of dictionaries containing data registry records
#             chunk_size: Number of records to insert in each batch
#
#         Returns:
#             Number of records processed
#         """
#         try:
#             # Convert to DataFrame for processing
#             df = pd.DataFrame(data_records)
#
#             # Ensure required columns
#             required_cols = ['client_id', 'data_type', 'source_id', 'run_id', 'output_location']
#             missing_cols = [col for col in required_cols if col not in df.columns]
#
#             if missing_cols:
#                 raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")
#
#             # Add processed_at and is_current if not present
#             if 'processed_at' not in df.columns:
#                 df['processed_at'] = datetime.now()
#
#             if 'is_current' not in df.columns:
#                 df['is_current'] = True
#
#             # Using PostgresConnector's bulk_insert
#             records_inserted = self.connector.bulk_insert('data_registry', df, chunk_size)
#
#             logging.info(f"Bulk registered {records_inserted} data records")
#             return records_inserted
#
#         except Exception as e:
#             logging.error(f"Error in bulk register data: {str(e)}")
#             raise
#
#     def get_data_registry(self, client_id=None, data_type=None, source_id=None,
#                           run_id=None, is_current=None, limit=100, offset=0,
#                           sort_by='processed_at', sort_order='DESC'):
#         """
#         Query data registry with flexible filtering options.
#
#         This method utilizes the existing indexes (idx_current_data_registry,
#         idx_data_registry_client_type, idx_data_registry_run_id) for efficient queries.
#
#         Args:
#             client_id: Optional client identifier filter
#             data_type: Optional data type filter
#             source_id: Optional source ID filter
#             run_id: Optional pipeline run UUID filter
#             is_current: Optional boolean to filter for current/archived records
#             limit: Maximum number of records to return (default 100)
#             offset: Number of records to skip for pagination (default 0)
#             sort_by: Column to sort results by (default 'processed_at')
#             sort_order: Sort direction, 'ASC' or 'DESC' (default 'DESC')
#
#         Returns:
#             DataFrame with matching data registry entries
#         """
#         try:
#             # Start building the query
#             query = """
#                 SELECT id, client_id, data_type, source_id, run_id,
#                        processed_at, s3_location, is_current,
#                        source_updated_at, metadata
#                 FROM data_registry
#                 WHERE 1=1
#             """
#
#             # Initialize params dictionary
#             params = {}
#
#             # Add filters based on provided parameters
#             if client_id:
#                 query += " AND client_id = :client_id"
#                 params["client_id"] = client_id
#
#             if data_type:
#                 query += " AND data_type = :data_type"
#                 params["data_type"] = data_type
#
#             if source_id:
#                 query += " AND source_id = :source_id"
#                 params["source_id"] = source_id
#
#             if run_id:
#                 query += " AND run_id = :run_id"
#                 params["run_id"] = run_id
#
#             if is_current is not None:
#                 query += " AND is_current = :is_current"
#                 params["is_current"] = is_current
#
#             # Add sorting
#             valid_sort_columns = [
#                 'id', 'client_id', 'data_type', 'source_id', 'run_id',
#                 'processed_at', 'source_updated_at'
#             ]
#
#             if sort_by in valid_sort_columns:
#                 # Sanitize sort order to prevent SQL injection
#                 sort_direction = "DESC" if sort_order.upper() != "ASC" else "ASC"
#                 query += f" ORDER BY {sort_by} {sort_direction}"
#             else:
#                 # Default sort if invalid column provided
#                 query += " ORDER BY processed_at DESC"
#
#             # Add pagination
#             query += " LIMIT :limit OFFSET :offset"
#             params["limit"] = limit
#             params["offset"] = offset
#
#             # Log the query for debugging (with sensitive params redacted)
#             log_params = {k: '***' if k in ['password'] else v for k, v in params.items()}
#             logging.debug(f"Executing query with params: {log_params}")
#
#             # Execute query using PostgresConnector's read_query method
#             result_df = self.connector.read_query(query, params)
#
#             # Process JSON metadata column if needed
#             if 'metadata' in result_df.columns:
#                 result_df['metadata'] = result_df['metadata'].apply(
#                     lambda x: json.loads(x) if isinstance(x, str) else x
#                 )
#
#             return result_df
#
#         except Exception as e:
#             logging.error(f"Error querying data registry: {str(e)}")
#             raise