#!/usr/bin/env python3
"""
Schema Generation Utility

This script provides a command-line interface to generate schema YAML files
from various data sources like parquet files, JSON files, or database tables.
"""

import os
import sys
import argparse
import logging
import json
import pandas as pd
from postgresconnector import PostgresConnector
from auto_schema_generator import AutoSchemaGenerator

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('schema_generator')


def generate_from_file(file_path, table_name, output_path=None, schema_dir='schemas'):
    """
    Generate schema from a data file.

    Args:
        file_path: Path to the data file (parquet, JSON, CSV)
        table_name: Name of the target table
        output_path: Optional custom output path
        schema_dir: Schema directory
    """
    generator = AutoSchemaGenerator(schema_dir=schema_dir)

    file_ext = os.path.splitext(file_path)[1].lower()

    if file_ext == '.parquet':
        generator.generate_from_parquet(file_path, table_name, output_path)
    elif file_ext == '.json':
        generator.generate_from_json(file_path, table_name, output_path)
    elif file_ext == '.csv':
        # Read CSV to DataFrame first
        df = pd.read_csv(file_path)
        generator.generate_from_df(df, table_name, output_path)
    elif file_ext == '.xlsx' or file_ext == '.xls':
        # Read Excel to DataFrame first
        df = pd.read_excel(file_path)
        generator.generate_from_df(df, table_name, output_path)
    else:
        logger.error(f"Unsupported file extension: {file_ext}")
        return False

    return True


def generate_from_s3(s3_path, table_name, output_path=None, schema_dir='schemas'):
    """
    Generate schema from a file in S3.

    Args:
        s3_path: S3 path (s3://bucket/key)
        table_name: Name of the target table
        output_path: Optional custom output path
        schema_dir: Schema directory
    """
    try:
        from utils.s3utility import read_s3_parquet

        if s3_path.endswith('.parquet'):
            # Read parquet file from S3
            df = read_s3_parquet(s3_path)

            # Generate schema from DataFrame
            generator = AutoSchemaGenerator(schema_dir=schema_dir)
            generator.generate_from_df(df, table_name, output_path)

            return True
        else:
            logger.error(f"Only parquet files are supported for S3 paths: {s3_path}")
            return False

    except Exception as e:
        logger.error(f"Error generating schema from S3 path {s3_path}: {str(e)}")
        return False


def generate_from_database(table_name, db_prefix="warehouse_", schema_name='public',
                           output_path=None, schema_dir='schemas'):
    """
    Generate schema by querying a database table.

    Args:
        table_name: Name of the database table
        db_prefix: Database connection prefix
        schema_name: Database schema name
        output_path: Optional custom output path
        schema_dir: Schema directory
    """
    try:
        # Connect to the database
        postgres = PostgresConnector(db_prefix=db_prefix)

        # Query table structure
        query = f"""
        SELECT 
            column_name, 
            data_type, 
            is_nullable,
            column_default,
            character_maximum_length
        FROM 
            information_schema.columns
        WHERE 
            table_schema = '{schema_name}'
            AND table_name = '{table_name}'
        ORDER BY 
            ordinal_position;
        """

        columns_df = postgres.read_query(query)

        if columns_df.empty:
            logger.error(f"Table not found: {schema_name}.{table_name}")
            return False

        # Get primary key information
        pk_query = f"""
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                           AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = '{schema_name}.{table_name}'::regclass
        AND    i.indisprimary;
        """

        try:
            pk_df = postgres.read_query(pk_query)
            primary_keys = pk_df['attname'].tolist() if not pk_df.empty else []
        except:
            primary_keys = []
            logger.warning("Could not determine primary key information")

        # Map PostgreSQL types to Python types
        pg_to_py_type = {
            'integer': 'int',
            'bigint': 'int',
            'smallint': 'int',
            'numeric': 'float',
            'double precision': 'float',
            'real': 'float',
            'boolean': 'bool',
            'character varying': 'str',
            'character': 'str',
            'text': 'str',
            'json': 'json',
            'jsonb': 'json',
            'timestamp': 'datetime',
            'timestamp with time zone': 'datetime',
            'date': 'date'
        }

        # Build schema dictionary
        schema = {}

        for _, row in columns_df.iterrows():
            col_name = row['column_name']
            data_type = row['data_type']
            is_nullable = row['is_nullable'] == 'YES'
            default_val = row['column_default']
            max_length = row['character_maximum_length']

            # Extract base type without length/precision specifications
            base_type = data_type.split('(')[0].lower()

            # Define the schema entry
            schema_entry = {
                'nullable': is_nullable
            }

            # Set PostgreSQL type
            if 'character varying' in data_type.lower() and max_length:
                schema_entry['pg_type'] = f'character varying({max_length})'
            else:
                schema_entry['pg_type'] = data_type

            # Set Python type
            schema_entry['py_type'] = pg_to_py_type.get(base_type, 'str')

            # Set default if present
            if default_val:
                schema_entry['default'] = default_val

            # Set primary key flag
            if col_name in primary_keys:
                schema_entry['primary_key'] = True

            schema[col_name] = schema_entry

        # Save schema to file
        if output_path is None:
            os.makedirs(schema_dir, exist_ok=True)
            output_path = os.path.join(schema_dir, f"{table_name}.yaml")

        with open(output_path, 'w') as f:
            import yaml
            yaml.dump(schema, f, default_flow_style=False)

        logger.info(f"Generated schema for {table_name} saved to {output_path}")
        return True

    except Exception as e:
        logger.error(f"Error generating schema from database table {table_name}: {str(e)}")
        return False


def generate_from_json_string(json_str, table_name, output_path=None, schema_dir='schemas'):
    """
    Generate schema from a JSON string.

    Args:
        json_str: JSON string
        table_name: Name of the target table
        output_path: Optional custom output path
        schema_dir: Schema directory
    """
    try:
        # Parse JSON
        data = json.loads(json_str)

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

                # Generate schema for main table
                generator = AutoSchemaGenerator(schema_dir=schema_dir)
                generator.generate_from_df(main_df, table_name, output_path)

                # Generate schema for items
                items_df = pd.DataFrame(data['items'])
                items_table = f"{table_name}_items"
                items_output = None if output_path is None else f"{os.path.splitext(output_path)[0]}_items.yaml"
                generator.generate_from_df(items_df, items_table, items_output)

                return True
            else:
                df = pd.DataFrame([data])
        else:
            raise ValueError(f"Unsupported JSON structure")

        # Generate schema from DataFrame
        generator = AutoSchemaGenerator(schema_dir=schema_dir)
        generator.generate_from_df(df, table_name, output_path)

        return True

    except Exception as e:
        logger.error(f"Error generating schema from JSON string: {str(e)}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Generate schema YAML files for ETL pipelines")
    subparsers = parser.add_subparsers(dest='command', help='Command')

    # File command
    file_parser = subparsers.add_parser('file', help='Generate schema from a file')
    file_parser.add_argument('file_path', help='Path to the data file')
    file_parser.add_argument('table_name', help='Target table name')
    file_parser.add_argument('--output', '-o', help='Output path for schema file')
    file_parser.add_argument('--schema-dir', '-d', default='schemas', help='Schema directory')

    # S3 command
    s3_parser = subparsers.add_parser('s3', help='Generate schema from a file in S3')
    s3_parser.add_argument('s3_path', help='S3 path (s3://bucket/key)')
    s3_parser.add_argument('table_name', help='Target table name')
    s3_parser.add_argument('--output', '-o', help='Output path for schema file')
    s3_parser.add_argument('--schema-dir', '-d', default='schemas', help='Schema directory')

    # Database command
    db_parser = subparsers.add_parser('db', help='Generate schema from a database table')
    db_parser.add_argument('table_name', help='Database table name')
    db_parser.add_argument('--db-prefix', default='warehouse_', help='Database connection prefix')
    db_parser.add_argument('--schema', default='public', help='Database schema name')
    db_parser.add_argument('--output', '-o', help='Output path for schema file')
    db_parser.add_argument('--schema-dir', '-d', default='schemas', help='Schema directory')

    # JSON command
    json_parser = subparsers.add_parser('json', help='Generate schema from a JSON string')
    json_parser.add_argument('json_input', help='JSON string or path to JSON file')
    json_parser.add_argument('table_name', help='Target table name')
    json_parser.add_argument('--output', '-o', help='Output path for schema file')
    json_parser.add_argument('--schema-dir', '-d', default='schemas', help='Schema directory')
    json_parser.add_argument('--is-file', action='store_true', help='Treat JSON input as a file path')

    args = parser.parse_args()

    if args.command == 'file':
        generate_from_file(args.file_path, args.table_name, args.output, args.schema_dir)
    elif args.command == 's3':
        generate_from_s3(args.s3_path, args.table_name, args.output, args.schema_dir)
    elif args.command == 'db':
        generate_from_database(args.table_name, args.db_prefix, args.schema, args.output, args.schema_dir)
    elif args.command == 'json':
        if args.is_file:
            with open(args.json_input, 'r') as f:
                json_str = f.read()
        else:
            json_str = args.json_input
        generate_from_json_string(json_str, args.table_name, args.output, args.schema_dir)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()