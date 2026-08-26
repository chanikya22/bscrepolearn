"""
Configuration for the Excel -> Postgres full-refresh sync utility.

This is the ONLY file a team member should need to write when onboarding a
new sheet. Everything else (auth, reading, schema inference, loading, DAG
wiring) lives in the shared excel_sync package and never changes per sheet.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List

from .schema import clean_column_name


class SyncMode(str, Enum):
    RANGE = "range"            # sync a fixed A1-style range, e.g. "A1:M500"
    FULL_SHEET = "full_sheet"  # Graph usedRange of the sheet, all columns
    FILE_DOWNLOAD = "file_download"  # download .xlsx (no Graph calc) -> S3 -> sheet -> Postgres


@dataclass
class SheetSyncConfig:
    # --- required: SharePoint source ---
    site_id: str          # SharePoint site id, see README for how to get this once
    file_path: str         # path to the .xlsx relative to the site's drive root
                           # e.g. "/Shared Documents/Reports/Sales Tracker.xlsx"
    sheet_name: str        # worksheet/tab name inside the workbook -- can be anything a
                           # real Excel tab is allowed to contain (spaces, mixed case, an
                           # apostrophe); reading it doesn't require this to be a clean identifier

    # --- required: sync behaviour ---
    sync_mode: SyncMode

    # --- optional ---
    target_table: Optional[str] = None    # defaults to a cleaned version of sheet_name if
                                           # omitted (e.g. "SKU List" -> "sku_list") -- handy
                                           # when a sheet can't be renamed to something Postgres-safe
    range_address: Optional[str] = None   # required only when sync_mode == SyncMode.RANGE
    header_row: bool = True               # first row of the range/sheet holds column headers
    target_schema: str = "public"

    # --- optional: FILE_DOWNLOAD only ---
    s3_bucket: str = "bsc-file-automation"
    s3_prefix: Optional[str] = None       # defaults to the workbook file name, e.g. ItemMaster -> itemmaster

    # --- optional: pipeline tracking (TrackedPythonOperator / PipelineTracker) ---
    # Defaults are filled in from target_table if left blank, so existing configs
    # written before this feature keep working with zero changes.
    pipeline_name: Optional[str] = None    # defaults to f"excel_sync_{target_table}"
    client_id: str = "bsc"
    data_type: Optional[str] = None        # defaults to target_table
    failure_email_to: List[str] = field(default_factory=list)
    success_email_to: List[str] = field(default_factory=list)

    def __post_init__(self):
        if isinstance(self.sync_mode, str):
            self.sync_mode = SyncMode(self.sync_mode)

        if not self.target_table:
            self.target_table = clean_column_name(self.sheet_name, lowercase=True)

        if self.sync_mode == SyncMode.RANGE and not self.range_address:
            raise ValueError(
                f"[{self.target_table}] sync_mode=RANGE requires range_address, "
                f"e.g. range_address='A1:M500'"
            )
        if self.sync_mode == SyncMode.FILE_DOWNLOAD and not self.s3_prefix:
            name = self.file_path.rsplit("/", 1)[-1]
            if "." in name:
                name = name.rsplit(".", 1)[0]
            self.s3_prefix = name.lower().replace(" ", "_")
        if not self.pipeline_name:
            self.pipeline_name = f"excel_sync_{self.target_table}"
        if not self.data_type:
            self.data_type = self.target_table