# excel_sync

Replaces the Airbyte Google Sheets connector with a direct SharePoint Excel -> Postgres
full-refresh pipeline for Airflow. Built so that onboarding a new sheet is: copy one
file, change a few values, done.

## Quick start (one-time project setup)

1. Drop the `excel_sync/` folder into your Airflow project. `pip install msal pandas requests`.
2. In `excel_sync/dag_factory.py`, fix the import: `from postgres_connector_v3 import PostgresConnector` -> point it at wherever that module actually lives.
3. Confirm your app registration has `Sites.Selected` and `Files.ReadWrite.All` (or `Files.Read.All`) granted as **Application** permissions, admin-consented (see screenshot-style check in Azure Portal -> App registrations -> your app -> API permissions -- Type column should say "Application", Status should be green "Granted").
4. Per SharePoint site you'll read from (once per site, not per sheet):
   - Get the `site_id`: `GET https://graph.microsoft.com/v1.0/sites/{hostname}:/sites/{site-name}`
   - Grant this app access to that site: `POST https://graph.microsoft.com/v1.0/sites/{site-id}/permissions` (see "One-time setup" below -- this step needs a temporary elevated permission, explained there).
5. Set three env vars on the Airflow worker/scheduler: `GRAPH_CLIENT_ID`, `GRAPH_CLIENT_SECRET`, `GRAPH_TENANT_ID`.
6. Copy `example_dags/sync_sales_raw.py`, fill in `site_id`, `file_path`, `sheet_name`, `sync_mode`, `target_table`, drop it in your DAGs folder.
7. Trigger it manually in the Airflow UI, confirm the target table and row count look right.
8. Every sheet after this is just repeating step 6 -- steps 1-5 don't repeat per sheet.

## Files

```
excel_sync/
  config.py            # SheetSyncConfig, SyncMode -- the only thing you edit per sheet
  auth.py               # app-only (client credentials) Graph auth, shared across all sheets
  sharepoint_reader.py  # Graph calls -> pandas DataFrame (range or full sheet), with throttling retry/backoff
  file_sync.py          # FILE_DOWNLOAD: SharePoint .xlsx -> S3 -> cached sheet values -> Postgres
  schema.py             # Airbyte-style type inference + column-name cleaning
  loader.py             # create-table-if-missing, TRUNCATE, bulk insert (uses your PostgresConnector)
  dag_factory.py        # SheetSyncConfig(s) -> Airflow DAG, one DAG per workbook

example_dags/
  sync_sales_raw.py               # single-sheet example
  sync_price_monitor_range.py     # single-sheet RANGE example
  sync_workbook_multi_sheet.py    # STANDARD: one DAG, multiple sheets from the same workbook
```

## Onboarding a new workbook (standard: one DAG per workbook)

Copy `example_dags/sync_workbook_multi_sheet.py`, list one `SheetSyncConfig`
per sheet you want synced out of that workbook (same `site_id`/`file_path`
repeated across entries -- expected, not redundant), drop it in your DAGs
folder:

```python
sheets = [
    SheetSyncConfig(site_id=SITE_ID, file_path=FILE_PATH, sheet_name="North",
                     sync_mode=SyncMode.FULL_SHEET, target_table="regional_north"),
    SheetSyncConfig(site_id=SITE_ID, file_path=FILE_PATH, sheet_name="South",
                     sync_mode=SyncMode.FULL_SHEET, target_table="regional_south"),
]
dag = create_workbook_sync_dag(sheets, dag_id="sync_regional_tracker_workbook", schedule="0 6 * * *")
```

This produces one DAG with one task per sheet. Each sheet's task is tracked
as its own independent pipeline run (own `run_id`), so every row stays
traceable to the exact run that loaded it, same as a single-sheet DAG.

**Large workbooks / external links (FILE_DOWNLOAD):** Graph `usedRange` opens
Excel Online and recalculates, which times out on big files. Use
`sync_mode=SyncMode.FILE_DOWNLOAD` instead -- same config, but it downloads
the `.xlsx` bytes, archives to S3, reads cached sheet values locally, then
full-refresh loads Postgres:

```python
SheetSyncConfig(
    site_id=SITE_ID,
    file_path="/Ixxxxxxxr.xlsx",
    sheet_name="abcr",
    sync_mode=SyncMode.FILE_DOWNLOAD,
    target_schema="bsc",
    target_table="bsc_dddp_xxx",
    s3_bucket="bsc-file-automation",   # optional, this is the default
    s3_prefix="ixxxxxxxxr",            # optional, defaults to the file name
)
```

**If a workbook only has one sheet to sync**, `create_excel_sync_dag()` is
still there as a convenience wrapper (see `sync_sales_raw.py` /
`sync_price_monitor_range.py`) -- it just calls `create_workbook_sync_dag()`
with a one-item list under the hood.

That's it -- no new Python logic, no manual `CREATE TABLE`.

**One easy-to-miss gotcha**: Airflow only scans `.py` files that literally
contain both the words `airflow` and `dag` (case-insensitive) somewhere in
the text, as a parsing speed optimization -- files without both are silently
skipped, with no error anywhere. Both example templates already include this
in their docstring; if you write a DAG file from scratch instead of copying
the template, make sure the word `airflow` appears somewhere in it (a comment
is enough), or it just won't show up in the UI.

## What happens on each run

1. `FULL_SHEET` / `RANGE`: `sharepoint_reader.py` pulls the used range or your
   fixed `range_address` from Graph into a DataFrame.
   `FILE_DOWNLOAD`: downloads the `.xlsx` bytes (no Excel Online recalc),
   archives to `s3://{s3_bucket}/{s3_prefix}/year=YYYY/month=MM/day=DD/{file}`,
   then reads cached sheet values with openpyxl.
2. `schema.py` cleans the column names (lowercase, snake_case, ascii) and infers a
   Postgres type per column by sampling values -- same idea as Airbyte's
   auto-create: `BIGINT` / `DOUBLE PRECISION` / `BOOLEAN` / `TIMESTAMP` / `TEXT`.
3. `loader.py` creates `target_schema.target_table` if it doesn't exist yet using
   that inferred schema, then does `TRUNCATE` + bulk insert -- i.e. Full Refresh |
   Overwrite semantics (same as the other excel_sync modes, not a SQL UPSERT).

## Rate limiting (Microsoft Graph throttling)

`sharepoint_reader.py` retries automatically on Graph throttling (`429 Too
Many Requests`) and transient upstream errors (`502`/`503`/`504`), following
[Microsoft's own throttling guidance](https://learn.microsoft.com/en-us/graph/throttling):
every throttled response almost always carries a `Retry-After` header telling
you exactly how long to wait, and that number is authoritative -- this never
guesses or shortens it. SharePoint specifically may also send a
`RateLimit-Reset` header alongside it; when both appear, the documented
advice is to honor whichever is larger, which is what this does. Only when
Graph gives no header at all (rare, but documented as possible for a few
resource types) does it fall back to exponential backoff with jitter.

Deliberately not retried: other 4xx errors (`400`, `401`, `403`, `404`) --
those are real problems (bad path, bad auth, missing permission grant, file
not found) that a retry won't fix, and retrying would just delay surfacing
the actual issue.

**Why this doesn't hard-code a specific requests-per-second limit**: Graph's
numeric throttling thresholds vary by tenant size and change over time --
Microsoft's own docs explicitly say not to rely on a fixed number. Reacting
correctly to what each response says is the durable strategy; the retry
logic above already does that automatically, no configuration needed.

**Complementary, proactive mitigation**: `create_workbook_sync_dag()`'s
`max_active_tasks` parameter (default `3`) caps how many sheets in one
workbook sync concurrently, which reduces how often a burst of parallel
Graph calls triggers throttling in the first place -- on top of, not instead
of, the retry handling above.

**Tuning the retry behavior**, if ever needed: `SharePointExcelReader(get_token_fn,
max_retries=5, base_backoff_seconds=2.0)` -- both have defaults that should
work for a normal sheet-sync workload without changing anything.

## Pipeline tracking (TrackedPythonOperator)

Every sync runs through `TrackedPythonOperator` instead of a plain
`PythonOperator`, so each run gets registered in your existing
`pipeline_runs` / `pipeline_steps` audit tables the same way your other
Vinculum pipelines do -- and the `run_id` that tracking generates is stamped
onto every row of the synced data as a `run_id` TEXT column, so any row can
be traced back to exactly which pipeline run loaded it.

Because each `excel_sync` DAG has exactly one task, that task is set as both
`is_first_task=True` and `is_last_task=True` -- it starts tracking, runs the
sync, and closes out tracking, all in one go.

**New optional config fields** on `SheetSyncConfig` (all have sensible
defaults derived from `target_table`, so existing configs written before this
feature keep working untouched):
```python
pipeline_name: str            # defaults to f"excel_sync_{target_table}"
client_id: str                 # defaults to "bsc"
data_type: str                  # defaults to target_table
failure_email_to: list[str]      # defaults to []
success_email_to: list[str]      # defaults to []
```

**Adding `run_id` to a table that predates this feature** requires no manual
step -- the same column-reconciliation logic that handles the sheet's own
column changes treats `run_id` as just another new column and adds it via
`ALTER TABLE ... ADD COLUMN "run_id" TEXT` automatically on the next run.

**One more import to adjust**, same idea as `postgres_connector_v3`:
`dag_factory.py` imports `TrackedPythonOperator` from
`plugins.operators.tracked_python_operator` -- point that at wherever it
actually lives in your project if the path differs.

## One-time setup

**Azure app registration**: an app registration with `Sites.Selected` and
`Files.ReadWrite.All` (or `Files.Read.All`) granted as **Application**
permissions (not Delegated), admin-consented. This is what makes the whole
thing fully unattended -- the app authenticates as itself, no human sign-in
involved anywhere, ever.

**`Sites.Selected` needs one extra one-time step per site.** Consenting to the
scope does NOT give the app access to any SharePoint site by default -- an
admin has to explicitly grant it access to each site individually:

```
POST https://graph.microsoft.com/v1.0/sites/{site-id}/permissions
Content-Type: application/json

{
  "roles": ["read"],
  "grantedToIdentities": [
    {
      "application": {
        "id": "<app registration client id>",
        "displayName": "<app registration name>"
      }
    }
  ]
}
```

This call needs an admin token with `Sites.FullControl.All` (run it once via
Graph Explorer, signed in as an admin). Do this once per SharePoint site the
DAGs will read from -- not per sheet, not per file, not per run. If a new
sheet lives on a site that's never been granted access, the DAG will fail with
a 403 until this is run for that `site-id`.

**If Graph Explorer gives you `accessDenied` on this call even as an admin**:
this isn't about your admin role -- Graph Explorer is itself a separate app
and needs `Sites.FullControl.All` consented specifically for it, plus your
signed-in account needs the actual SharePoint Administrator or Global
Administrator role (not just "an admin" of something else). Two ways through:
  - In Graph Explorer, click "Modify permissions", search `Sites.FullControl.All`,
    click Consent, sign in as an admin, retry.
  - Or, more reliably: temporarily add `Sites.FullControl.All` as an
    **Application** permission on your own app registration, admin-consent it,
    get an app-only token via `client_credentials`, and call the POST directly
    with curl/Postman instead of Graph Explorer. Remove the permission again
    once the grant succeeds -- it's only needed for this one provisioning call,
    never for ongoing sync runs (which only ever need `Sites.Selected`).

**Environment variables** (set once on the Airflow worker/scheduler):
```
GRAPH_CLIENT_ID=<app registration client id>
GRAPH_CLIENT_SECRET=<app registration client secret>   # pull from Secrets Manager in prod, same as your DB creds
GRAPH_TENANT_ID=<your tenant id>                        # required -- "common" does not work for app-only auth
```

**Auth model**: pure client-credentials, no login step at all. Every DAG run
calls `get_graph_token()`, which authenticates as the app itself using
`GRAPH_CLIENT_ID` + `GRAPH_CLIENT_SECRET` and gets a token back directly --
no device code, no cached refresh token, no bootstrap, nothing that can go
stale from inactivity or an MFA policy. As long as the secret is valid and the
site grant from above is in place, it works indefinitely with zero human
involvement.

**Getting `site_id`**: `GET https://graph.microsoft.com/v1.0/sites/{hostname}:/sites/{site-name}`
returns an `id` field, e.g. `bombayshavingcompany.sharepoint.com,<guid>,<guid>`. You
only need to look this up once per SharePoint site, not per sheet.

**Getting `file_path` and `sheet_name` for a config**: `file_path` is the path to
the `.xlsx` relative to the site's document library root, starting with `/`,
e.g. `/Shared Documents/Reports/Sales Tracker.xlsx`. If you're not sure of the
exact path/casing, browse it via Graph Explorer:
```
GET https://graph.microsoft.com/v1.0/sites/{site-id}/drive/root/children
```
then drill into folders by their `id`:
```
GET https://graph.microsoft.com/v1.0/sites/{site-id}/drive/items/{folder-id}/children
```
until you see the file, and use its exact `name` to build the path. `sheet_name`
is just the worksheet tab name at the bottom of the Excel file, case-sensitive.
If you're ever unsure exactly how Graph has a worksheet named (or a sync 404s
on one specific sheet while its siblings in the same workbook succeed), list
every worksheet's exact name directly:
```
GET https://graph.microsoft.com/v1.0/sites/{site-id}/drive/root:/YourFile.xlsx:/workbook/worksheets
```

**`sheet_name` can be anything a real Excel tab is allowed to contain** --
spaces, mixed case, an apostrophe -- with no cleanup needed on your end.
Spaces/case are handled by ordinary URL-encoding; an embedded apostrophe
(e.g. `"Prakash's Sheet"`) gets the OData-required doubling (`''`) before
encoding, since Graph's worksheet address is a quoted string literal and a
raw, undoubled apostrophe would otherwise get decoded back and prematurely
terminate the literal server-side, silently corrupting the rest of the name.
`target_table` is entirely independent of `sheet_name` -- and if you omit it,
it defaults to a cleaned version of `sheet_name` (e.g. `"SKU List"` ->
`sku_list`), so a sheet you can't rename to something Postgres-safe never
forces you to manually invent a separate name; just give an explicit
`target_table` whenever you want a different one.

## Notes / things you'll likely want to adjust

- **Auth reuse**: if insytflow_v2 already has a working MSAL auth module for
  Graph, swap it in for `excel_sync/auth.py` -- everything else only depends on
  a zero-arg `get_token_fn() -> str`, so it's a one-line change in
  `dag_factory.py`.
- **Postgres import path**: `dag_factory.py` imports `PostgresConnector` from
  `postgres_connector_v3`. Point that import at wherever that module actually lives
  in your Airflow DAGs folder / plugins path.
- **Schema inference is a starting point, not gospel**: if a column always needs to
  be `NUMERIC(12,2)` instead of `DOUBLE PRECISION`, or a specific column should be
  `DATE` not `TIMESTAMP`, just `ALTER TABLE` once after first creation -- shared
  columns' types are never touched automatically, so a manual type fix sticks
  across future syncs.
- **Column additions and removals in the sheet are handled automatically, every run**:
  if a column appears in the sheet that isn't in the table yet, it's added
  (`ALTER TABLE ... ADD COLUMN`); if a column that used to be in the sheet is
  gone, it's dropped from the table (`ALTER TABLE ... DROP COLUMN`) -- **this
  permanently deletes that column's data**, consistent with the fact that
  `TRUNCATE` + reload already discards and rebuilds every row on each run.
  This is logged clearly (a `WARNING`-level log line) whenever it happens, so
  an accidental column deletion in the sheet doesn't get silently mirrored
  into the database without a trace in the task logs.
- **Concurrent syncs**: if two sheets ever need to land in the same target table
  (unlikely given one DAG per sheet), don't run them concurrently -- `TRUNCATE` +
  insert isn't safe under concurrent writers to the same table.
- **Task IDs within a workbook DAG** are derived as `sync_{cleaned target_table}` --
  cleaned the same way `target_table` itself gets auto-derived from `sheet_name`,
  since Airflow's task_id rules (alphanumeric, dashes, dots, underscores only)
  are stricter than Postgres identifiers, which can contain spaces or mixed
  case if double-quoted. This means `target_table="SKU List"` is fine for the
  table itself but always produces a safe task_id (`sync_sku_list`) regardless.
  `create_workbook_sync_dag()` raises a clear `ValueError` up front if two
  sheets' `target_table` values clean down to the *same* task_id (e.g. `"SKU List"`
  and `"sku_list"` both becoming `sync_sku_list`), rather than letting Airflow
  fail later with a cryptic duplicate-task-id error.