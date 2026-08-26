import pandas as pd
import logging
import argparse
import os
import time
from dotenv import load_dotenv

# Import the connector - note we've renamed it to PostgresWarehouseConnector
from postgresconnector_v2 import PostgresWarehouseConnector

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def test_iceberg_merge(parquet_path, table_name, schema="public", use_table=False):
    """
    Test reading a Parquet file and merging it into an Iceberg table.

    Args:
        parquet_path: Path to the Parquet file
        table_name: Name of the Iceberg table to create/merge into
        schema: Database schema (default: "public")
        use_table: Use an existing table if True, otherwise create a new one
    """
    # Initialize the connector
    connector = PostgresWarehouseConnector(
        application_name="TestConnector",
        db_prefix="warehouse_"
    )

    try:
        # 1. Check connection and environment
        print("Testing database connection...")
        db_info = connector.execute_query("SELECT current_database(), version()")
        print(f"Connected to database: {db_info}")

        # 2. Read the Parquet file into a DataFrame
        print(f"Reading Parquet file: {parquet_path}")
        start_time = time.time()

        # Check if file exists
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

        df = pd.read_parquet(parquet_path)
        read_time = time.time() - start_time

        # Log DataFrame info
        print(f"DataFrame loaded in {read_time:.2f} seconds")
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame columns: {df.columns.tolist()}")

        # Show first few rows
        print("DataFrame preview:")
        print(df.head())

        # 3. Create or validate Iceberg table
        qualified_table = f"{schema}.{table_name}" if schema else table_name

        if not use_table:
            # # Check if table exists and drop if needed
            # if connector.table_exists(table_name, schema):
            #     print(f"Dropping existing table {qualified_table}")
            #     connector.execute_query(f"DROP TABLE IF EXISTS {qualified_table}")

            # Create a new Iceberg table based on the DataFrame schema
            print(f"Creating new Iceberg table: {qualified_table}")
            connector.create_table_if_not_exists(
                table_name=table_name,
                df=df,
                schema=schema,
                use_iceberg=True
            )
            print("Iceberg table created successfully")
        else:
            # Verify the table exists and is an Iceberg table
            # if not connector.table_exists(table_name, schema):
            #     raise ValueError(f"Table {qualified_table} does not exist")

            if not connector.is_iceberg_table(table_name, schema):
                raise ValueError(f"Table {qualified_table} is not an Iceberg table")

            print(f"Using existing Iceberg table: {qualified_table}")

        # 4. Determine merge key (use first column if table has no primary key)
        merge_key = df.columns[0]
        print(f"Using merge key: {merge_key}")

        # 5. Merge data into the Iceberg table
        print(f"Merging {len(df)} rows into Iceberg table...")
        merge_start = time.time()

        # For the initial data load
        if not use_table:
            merged_rows = connector.bulk_load_to_table(
                df=df,
                table_name=table_name,
                schema=schema,
                use_iceberg=True
            )
        else:
            # For subsequent merges using a key
            merged_rows = connector.merge_df_to_iceberg(
                df=df,
                table_name=table_name,
                schema=schema,
                unique_key=merge_key
            )

        merge_time = time.time() - merge_start

        print(f"Merged {merged_rows} rows in {merge_time:.2f} seconds ({merged_rows / merge_time:.2f} rows/s)")

        # 6. Vacuum the Iceberg table to optimize storage
        print(f"Vacuuming Iceberg table: {qualified_table}")
        connector.vacuum_iceberg_table(table_name, schema)

        # 7. Verify the results
        verify_query = f"SELECT COUNT(*) AS row_count FROM {qualified_table}"
        result = connector.read_query(verify_query)
        row_count = result.iloc[0]['row_count']

        print(f"Table {qualified_table} now has {row_count} rows")

        # 8. Close the connection
        connector.close()
        print("Test completed successfully")

    except Exception as e:
        print(f"Test failed: {str(e)}")
        # Ensure connection is closed
        if 'connector' in locals():
            connector.close()
        raise


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Test PostgresWarehouseConnector with a Parquet file")
    # parser.add_argument("parquet_path", help="Path to the Parquet file")
    # parser.add_argument("--table", "-t", default="test_iceberg_table", help="Table name (default: test_iceberg_table)")
    # parser.add_argument("--schema", "-s", default="public", help="Schema name (default: public)")
    # parser.add_argument("--use-existing", "-e", action="store_true",
    #                     help="Use existing table instead of creating new one")
    #
    # args = parser.parse_args()

    test_iceberg_merge(
        parquet_path="C:\\Users\\shish\\Downloads\\orders (4).parquet",
        table_name="test_iceberg_table",
        schema="bsc"
    )