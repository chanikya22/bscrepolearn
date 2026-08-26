# Team Guide: Scheduling an Excel Sync from SharePoint

## 1. What You Need to Get Started

Before you begin, gather these details about your Excel file:

| Item | What to put | Example |
|------|-------------|---------|
| File Path | The full path to your file in SharePoint, starting with a `/`. | `/ItemMaster.xlsx` or `/100Days/Neurogum/NeurogumProductMaster.xlsx` |
| Sheet / Tab Name | The exact name of the tab within the Excel workbook. | `item_master` |
| Target Schema | The database schema where the table should live (usually `bsc`). | `bsc` |
| Target Table | The name for your new table in Postgres. | `bsc_mrp_cogs` |
| Schedule | When you want the sync to run, using a cron schedule. | `10 0 * * *` |

## 2. Choosing the Right Sync Mode

| Mode | When to Use It |
|------|----------------|
| `FULL_SHEET` | Use this for most normal workbooks. |
| `FILE_DOWNLOAD` | Use this for large files or files with complex formulas/external links that cause timeouts. The file is archived to S3 before loading. |
| `RANGE` | Use this if you only want to sync a specific block of cells (e.g., `A1:M500`). You will also need to set the `range_address`. |

**Tip:** Start with `FULL_SHEET`. If your sync times out, switch to `FILE_DOWNLOAD`.

## 3. How to Configure Your Sync

You will add your configuration to one of two files:

- Use `dags/excel_sync.py` for a **single sheet**.
- Use `dags/excel_multisheet_sync.py` for **multiple sheets** from the same workbook.

### A. Syncing a Single Sheet

1. Open the `dags/excel_sync.py` file.
2. Copy and paste an existing configuration block and change the values to match your file.

```python
my_sheet_config = SheetSyncConfig(
    site_id=f"vlpcpl.sharepoint.com,{SITE_ID}",  # This is pre-filled for you
    file_path="/Path/To/YourFile.xlsx",          # Replace with your file path
    sheet_name="the_tab_name",                   # Replace with your sheet name
    sync_mode=SyncMode.FULL_SHEET,               # Change to FILE_DOWNLOAD if needed
    target_schema="bsc",                         # Replace with your schema
    target_table="your_table_name",              # Replace with your desired table name
    failure_email_to=["you@company.com"],        # Optional: add your email
)

excel_your_table_name = create_excel_sync_dag(
    my_sheet_config,
    dag_id="excel_your_table_name",              # Must be unique
    schedule="10 0 * * *",                       # Your cron schedule
)
```

### B. Syncing Multiple Sheets from One Workbook

1. Open the `dags/excel_multisheet_sync.py` file.
2. Define the workbook path once, then create a list of configurations for each sheet you want to sync.

```python
WORKBOOK_FILE_PATH = "/Path/To/YourFile.xlsx"  # Set this once for all sheets

sheets = [
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="Sheet One",                  # First sheet
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="bsc",
        target_table="sheet_one_data",           # Table for this sheet
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="Sheet Two",                  # Second sheet
        sync_mode=SyncMode.FILE_DOWNLOAD,        # Mixing modes is fine
        target_schema="bsc",
        target_table="sheet_two_data",           # Table for this sheet
    ),
]

your_workbook_dag = create_workbook_sync_dag(
    sheets,
    dag_id="excel_your_workbook",                # Unique DAG name
    schedule="0 6 * * *",                        # Your cron schedule
    max_active_tasks=3,
)
```

For `FILE_DOWNLOAD` jobs, also check that the file was saved to S3.

## 4. Important Rules of Thumb

- **Don't Touch the Core Code:** Do not edit files in `custom_packages/excel_sync/`. Only change the config files (`excel_sync.py` and `excel_multisheet_sync.py`).
- **Sheet Names:** The `sheet_name` must match the tab name in Excel exactly; case and spaces matter.
- **Unique Tables:** In a multi-sheet DAG, each sheet must have a unique `target_table`.
- **New SharePoint Sites:** The default site (`vlpcpl.sharepoint.com`) is already set up. If you need to sync from a different SharePoint site, file a separate request with the data engineering team for app permissions.
