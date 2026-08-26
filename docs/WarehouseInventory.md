# Warehouse Inventory Extract & Load System

## Overview
Two-phase inventory load process:
1. **Phase 1**: Fetch warehouse-level inventory from Vinculum API
2. **Phase 2**: Replace "In Process" rows with bin-level detail from Bin-Lot API

---

## Phase 1: Warehouse API Load

### 1. `extract(postgres)`
- Fetches inventory from warehouse API endpoint
- Calls with all SKU codes from `productmaster` table
- Retrieves buckets: `["good", "bad", "hold", "In Process"]`
- Returns DataFrame with raw API response
- **Timestamp issue here**: Uses `pd.Timestamp.now()` (UTC) → Should use `datetime.now(IST)` for IST time

### 2. `load(postgres, brand, tracker, run_id)` - Main Orchestrator

**Step 1**: Call `extract(postgres)` → Get warehouse inventory

**Step 2**: Deduplicate by `[skucode, location, bucket]` (keep last)

**Step 3**: Add timestamp `updated_on` in IST

**Step 4**: **Full Refresh** - Delete all existing rows + Insert new rows
```
DELETE FROM bsc.vinculum_inventory_v2
INSERT all rows from extract
```

---

## Phase 2: Bin-Lot API Replacement

### 1. `get_in_process_skus(postgres)`
- Query newly loaded table for SKUs with bucket = `"In Process"`
- Used to identify which products need bin-level detail

### 2. `request_stage_bin_inventory_file_upload(skucodes, run_id)`
- Call Bin-Lot API with `currentmode=1` (triggers async file generation)
- API uploads compressed JSON to S3 path: 
  ```
  vinculum/warehouseinventory/stagebin/raw/year={year}/month={month:02d}/{run_id}/
  ```
- Returns S3 path for polling

### 3. `download_stage_bin_from_s3(file_upload_path)`
- Poll S3 path up to 6 times (60 seconds total) to wait for file
- Download ZIP file
- Extract JSON inside
- Convert JSON array to DataFrame

### 4. `fetch_stage_bin_inventory(skucodes, run_id)` - Orchestrates bin-lot fetch
- Calls `request_stage_bin_inventory_file_upload()` → triggers API, gets S3 path
- Calls `download_stage_bin_from_s3()` → downloads & extracts
- **Transform**:
  - Normalize `bincode` column
  - Map: `"stagebin"` → `"Stage bin"`, else → `"In Process"`
  - Sum quantities by `[skucode, location, bucket]`
  - Add IST timestamp
- Returns cleaned DataFrame

### 5. Back in `load()` - Replace In Process rows
```
DELETE FROM bsc.vinculum_inventory_v2 WHERE bucket = "In Process"
INSERT bin-lot data (better quality, bin-level detail)
```

### 6. Refresh Materialized View
```sql
REFRESH MATERIALIZED VIEW bsc.vinculum_inventory_v2_mv
```

---

## Data Flow Summary

```
Vinculum Warehouse API
        ↓
extract() → DataFrame
        ↓
Deduplicate & Timestamp
        ↓
Full Delete + Insert
        ↓
Load Complete (first time)
        ↓
Query In Process SKUs
        ↓
Bin-Lot API (currentmode=1)
        ↓
S3 Upload + Poll
        ↓
Download & Extract JSON
        ↓
Transform (normalize, group, timestamp)
        ↓
Delete existing In Process rows
        ↓
Insert bin-lot rows
        ↓
Refresh MV
        ↓
Complete
```

---

## Key Points

| Aspect | Detail |
|--------|--------|
| **Timezone** | IST (UTC+5:30) for all timestamps |
| **Freshness** | Full refresh each run (no incremental) |
| **In Process** | Replaced with bin-lot detail for higher accuracy |
| **Deduplication** | By `(skucode, location, bucket)` |
| **Retry Logic** | S3 polling: 6 attempts, 10 seconds apart |
| **Failure Handling** | If bin-lot fails, retains original In Process rows |
| **Output** | `bsc.vinculum_inventory_v2` table + MV refresh |

---

## Known Issues

1. **Timestamp Bug in `extract()`**: Line 250 uses `pd.Timestamp.now()` (UTC) instead of `datetime.now(IST)` (IST)
   - **Fix**: Change to `df['updated_on'] = datetime.now(IST).replace(tzinfo=None)`

---

## Function Reference

| Function | Purpose | Input | Output |
|----------|---------|-------|--------|
| `get_sku_code()` | Fetch all SKUs from productmaster | postgres conn | List of SKU codes |
| `get_in_process_skus()` | Fetch In Process SKUs after load | postgres conn | List of SKU codes |
| `request_stage_bin_inventory_file_upload()` | Trigger API, get S3 path | skucodes, run_id | S3 path string |
| `download_stage_bin_from_s3()` | Download & extract from S3 | S3 path | DataFrame |
| `fetch_stage_bin_inventory()` | Orchestrate bin-lot (API + S3) | skucodes, run_id | Transformed DataFrame |
| `extract()` | Call warehouse API | postgres conn | Raw DataFrame |
| `load()` | Main orchestrator | brand, tracker, run_id | Summary dict |
