"""
Onboarding a new sheet = copy this file, change the values below, done.
Mode: RANGE -- syncs only a fixed A1-style block, e.g. when the sheet has
extra notes/scratch columns you don't want pulled into Postgres.
"""
from excel_sync import SheetSyncConfig, SyncMode, create_excel_sync_dag

config = SheetSyncConfig(
    site_id="bombayshavingcompany.sharepoint.com,xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx,yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy",
    file_path="/Shared Documents/Pricing/Price Monitor.xlsx",
    sheet_name="Blinkit",
    sync_mode=SyncMode.RANGE,
    range_address="A1:M500",
    target_table="price_monitor_blinkit",
    target_schema="public",
)

dag = create_excel_sync_dag(config, dag_id="sync_price_monitor_blinkit", schedule="0 */6 * * *")
