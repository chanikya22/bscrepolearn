"""
Onboarding a new sheet = copy this file, change the values below, done.
Mode: FULL_SHEET -- Graph usedRange. FILE_DOWNLOAD -- SharePoint file -> S3 -> Postgres.
"""
from datetime import timedelta
import sys
sys.path.append('/opt/airflow')

from custom_packages.excel_sync import SheetSyncConfig, SyncMode, create_excel_sync_dag
SITE_ID = '36c1362d-c807-4966-b80b-e8cfe7e05874,5e5d1942-a7c3-405c-b252-90cef59a085a'


##### PRICEMONITORING ##############
price_monitoring_config = SheetSyncConfig(
    site_id=f"vlpcpl.sharepoint.com,{SITE_ID}",
    file_path="/PriceMonitoring.xlsx",
    sheet_name="price_monitoring_sku_details",
    sync_mode=SyncMode.FULL_SHEET,
    target_table="price_monitoring_sku_details",
    target_schema="market_intelligence",
)

excel_price_monitoring = create_excel_sync_dag(
    price_monitoring_config, dag_id="excel_price_monitoring", schedule="0 0 * * *"
)



####### NeurogumProductMaster ###########

neurogum_productmaster_config = SheetSyncConfig(
    site_id=f"vlpcpl.sharepoint.com,{SITE_ID}",
    file_path="/100Days/Neurogum/NeurogumProductMaster.xlsx",
    sheet_name="productmaster",
    sync_mode=SyncMode.FULL_SHEET,
    target_table="productmaster",
    target_schema="neurogum",
)

excel_neurogum_productmaster = create_excel_sync_dag(
    neurogum_productmaster_config, dag_id="excel_neurogum_productmaster", schedule="10 0 * * *"
)

####### Item Master (FILE_DOWNLOAD: SharePoint file -> S3 -> Postgres) ###########
item_master_config = SheetSyncConfig(
    site_id=f"vlpcpl.sharepoint.com,{SITE_ID}",
    file_path="/ItemMaster.xlsx",
    sheet_name="item_master",
    sync_mode=SyncMode.FILE_DOWNLOAD,
    target_table="bsc_mrp_cogs",
    target_schema="bsc",
    s3_bucket="bsc-file-automation",
    s3_prefix="itemmaster",
    failure_email_to=["shashankbhushan@bombayshavingcompany.com", "shishank@bombayshavingcompany.com"],
)

excel_item_master = create_excel_sync_dag(
    item_master_config,
    dag_id="excel_item_master",
    schedule="10 0 * * *",
    dagrun_timeout=timedelta(hours=3),
    default_args={"execution_timeout": timedelta(hours=2)},
)