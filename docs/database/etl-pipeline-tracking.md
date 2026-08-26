# ETL Pipeline Tracking Tables: Purpose and Column Descriptions

## Table Purposes and Column Descriptions

This document provides a comprehensive overview of the database schema used for tracking ETL pipelines, including the purpose of each table and detailed descriptions of all columns.

## 1. pipeline_runs

**Purpose:** Tracks each execution of a pipeline from start to finish. This is the primary table for monitoring overall pipeline executions, capturing high-level metadata about each run.

| Column | Type | Description |
|--------|------|-------------|
| run_id | uuid | Primary key that uniquely identifies each pipeline run. Used as a reference in other tables to link pipeline steps and processed data. |
| client_id | varchar(50) | Identifies which client's data is being processed. Useful for multi-tenant systems. |
| pipeline_name | varchar(50) | Name of the pipeline that was executed (e.g., "orders_pipeline", "inventory_pipeline"). |
| data_type | varchar(50) | Type of data being processed (e.g., "orders", "customers", "products"). |
| start_time | timestamp | When the pipeline execution started. |
| end_time | timestamp | When the pipeline execution finished (null if still running). |
| status | pipeline_status | Current status of the pipeline run (e.g., "running", "success", "failed"). |
| record_count | integer | Number of records processed in this run. |
| error_details | text | Details of any errors encountered during execution. |
| parameters | jsonb | JSON containing additional parameters used for this run. |
| created_by | varchar(100) | User or system that initiated this pipeline run. |

## 2. pipeline_steps

**Purpose:** Tracks individual stages within a pipeline run. Provides granular visibility into the progress and performance of specific pipeline components.

| Column | Type | Description |
|--------|------|-------------|
| step_id | serial | Primary key that uniquely identifies each pipeline step. |
| run_id | uuid | Foreign key to pipeline_runs, linking this step to its parent run. |
| step_name | varchar(100) | Name of the step (e.g., "extract", "transform", "load"). |
| start_time | timestamp | When this step started executing. |
| end_time | timestamp | When this step finished (null if still running). |
| status | varchar(20) | Current status of this step (e.g., "running", "success", "failed"). |
| record_count | integer | Number of records processed in this step. |
| error_message | text | Details of any errors encountered during this step. |
| output_location | varchar(255) | S3 or other location where this step's output is stored. |

## 3. data_registry

**Purpose:** Records all processed data sets, their locations, and current status. Acts as a catalog of all data that has been processed, including where it's stored and which time periods it covers.

| Column | Type | Description |
|--------|------|-------------|
| id | serial | Primary key that uniquely identifies each data registry entry. |
| client_id | varchar(50) | Identifies which client this data belongs to. |
| data_type | varchar(50) | Type of data in this entry (e.g., "orders", "customers"). |
| source_id | varchar(100) | Identifier from the source system, often including time period info (e.g., "orders_2023-03"). |
| run_id | uuid | Foreign key to pipeline_runs, linking this data to the run that processed it. |
| processed_at | timestamp | When this data was processed. |
| s3_location | varchar(255) | S3 path where this processed data is stored. |
| is_current | boolean | Whether this is the current version of this data (true) or has been superseded (false). |
| source_updated_at | timestamp | When this data was last updated in the source system. |
| metadata | jsonb | Additional information about this data (e.g., record counts, file format). |
| data_currency_date | timestamp | The date up to which this data is current (optional addition). |
| data_currency_pattern | varchar(50) | The refresh pattern this data follows (e.g., "t-1", "daily") (optional addition). |

## 4. data_sources

**Purpose:** Catalogs all data sources with their refresh requirements and connection details. Serves as a configuration table for the ETL system.

| Column | Type | Description |
|--------|------|-------------|
| id | serial | Primary key that uniquely identifies each data source. |
| name | varchar(100) | Descriptive name of the data source. |
| source_type | varchar(50) | Type of the data source (e.g., "PostgreSQL", "S3", "API"). |
| connection_details | jsonb | JSON containing connection parameters for this source. |
| refresh_interval | interval | How frequently this data should be refreshed (e.g., '1 day', '4 hours'). |
| refresh_pattern | varchar(50) | Standard pattern for data currency ('t-1', 't-2', 't', 'real-time', etc.). |
| active | boolean | Whether this data source is currently active. |
| description | text | Detailed description of what this data source contains. |
| documentation_url | varchar(255) | Link to more detailed documentation. |
| created_at | timestamp | When this data source was added to the system. |
| updated_at | timestamp | When this data source was last modified. |
| created_by | varchar(100) | User who created this data source entry. |
| updated_by | varchar(100) | User who last updated this data source entry. |

## Index Purposes

### pipeline_runs Indexes
- **idx_pipeline_runs_client**: Improves queries filtering by client_id
- **idx_pipeline_runs_status**: Accelerates queries filtering by status (e.g., finding failed runs)

### pipeline_steps Indexes
- **idx_pipeline_steps_run_id**: Speeds up queries for steps belonging to a specific run

### data_registry Indexes
- **idx_current_data_registry**: Optimizes queries for current data by client, type, and source
- **idx_data_registry_client_type**: Improves performance when querying by client and data type
- **idx_data_registry_run_id**: Accelerates queries for data produced by a specific pipeline run

## Relationships

1. pipeline_steps.run_id → pipeline_runs.run_id
2. data_registry.run_id → pipeline_runs.run_id
3. data_registry.data_currency_pattern → data_sources.refresh_pattern (if added)

This schema provides a comprehensive framework for tracking and managing ETL pipelines, from high-level run tracking to detailed step monitoring and data cataloging.