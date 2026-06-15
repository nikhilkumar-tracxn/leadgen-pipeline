"""
================================================================================
User Category & Status Sync Pipeline
================================================================================

PURPOSE
-------
User categories on the Tracxn platform are not static — they get updated over
time as users interact with the product and are reclassified. Similarly, a
user's `userStatus` (account/lead status) can change after signup. This script
re-fetches the latest category AND status from the Tracxn API for a date range
of users already in BigQuery and updates the `userCategory` and `userStatus`
columns in-place.

It is designed to run in two automatic patterns:

  Weekly sync  — runs on 7th, 14th, 21st, 28th of every month
                 Updates users created in the last 7 days
                 (catches recent signups whose category/status was not yet
                  assigned at the time of the daily pipeline run)

  Monthly sync — runs on 1st of every month
                 Updates all users created in the previous calendar month
                 (full refresh pass for the completed month)

Both workflows can also be triggered manually with a custom date range for
backfills or one-off corrections.

HOW THE UPDATE WORKS
--------------------
BigQuery does not support row-level UPDATE the same way a traditional SQL DB
does — DML UPDATE statements on large tables are slow and costly. Instead, the
script uses the "swap" pattern, which is more efficient and atomic:

  1. Read the rows in the target date range from BigQuery into memory
  2. For each user in that range, fetch their latest category AND status from
     the Tracxn API (single call per batch returns both fields)
  3. Apply the updated categories and statuses to the in-memory dataframe
  4. Write the updated rows to a temporary BigQuery table
  5. Reconstruct the full table using CREATE OR REPLACE TABLE:
       rows BEFORE the range  (unchanged, read from existing table)
     + rows IN the range      (updated, from temp table)
     + rows AFTER the range   (unchanged, read from existing table)
  6. Drop the temp table

This ensures all other dates are completely untouched and the operation is
atomic — the table is either fully updated or not updated at all.

The temp table is written with autodetect=True from a pandas DataFrame that
already carries the full schema of the source table (including `userStatus`,
since it was read via `SELECT *`), so all columns — including any added in
the future — pass through correctly as long as they exist in the source table.

RUN MODES
---------
  weekly_auto        Auto: last 7 days → Main table
  monthly_auto       Auto: last full calendar month → Main table
  manual             Custom date range you provide → Main table
  test_weekly_auto   Auto: last 7 days → Backup table
  test_monthly_auto  Auto: last full calendar month → Backup table
  test_manual        Custom date range you provide → Backup table

ENVIRONMENT VARIABLES
---------------------
  Required secrets:
    TRACXN_ACCESS_TOKEN   Tracxn API auth token
    GCP_PROJECT_ID        GCP project ID
    GCP_SA_KEY            Full GCP service account key JSON

  Required config variables:
    BQ_DATASET            BigQuery dataset name
    BQ_TABLE              Main production table
    BQ_TABLE_BACKUP       Backup table (for test modes)

  Set by workflow at runtime:
    SYNC_MODE             One of the six modes listed above
    SYNC_START_DATE       YYYY-MM-DD, required for *manual modes
    SYNC_END_DATE         YYYY-MM-DD, required for *manual modes

AUTHOR / HISTORY
----------------
Based on a one-off Google Colab script that was run manually to sync May 2026
categories. Converted to a scheduled GitHub Actions pipeline for fully
automatic recurring syncs. Extended to also refresh `userStatus` after the
`userStatus` column was added to the BigQuery table and backfilled via a
one-off Colab script.
================================================================================
"""

import os
import json
import time
import logging
import uuid
from datetime import datetime, timedelta, timezone, date
from typing import Tuple

import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config from environment ────────────────────────────────────────────────────
ACCESS_TOKEN = os.environ["TRACXN_ACCESS_TOKEN"]
PROJECT_ID   = os.environ["GCP_PROJECT_ID"]
DATASET      = os.environ.get("BQ_DATASET", "leadgen_dataset")
GCP_SA_JSON  = os.environ["GCP_SA_KEY"]

SYNC_MODE        = os.environ.get("SYNC_MODE", "weekly_auto").lower()
TABLE_PRODUCTION = os.environ.get("BQ_TABLE",        "leadgen_users_v2_no_partition")
TABLE_BACKUP     = os.environ.get("BQ_TABLE_BACKUP",  "leadgen_users_v2_no_partition_backup3")
SYNC_START_INPUT = os.environ.get("SYNC_START_DATE", "").strip()  # YYYY-MM-DD
SYNC_END_INPUT   = os.environ.get("SYNC_END_DATE",   "").strip()  # YYYY-MM-DD

API_USER_ENDPOINT = "https://platform.tracxn.com/api/2.2/user"
API_HEADERS = {
    "accessToken": ACCESS_TOKEN,
    "X-Request-Source": "GitHub-Actions-CategorySync",
    "Content-Type": "application/json",
}

BATCH_SIZE  = 20    # Users per API request (ID filter batch size)
SLEEP_S     = 0.4   # Seconds between API batches (rate-limit safety)
MAX_RETRIES = 3     # Retry attempts per failed API request


# ════════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════════
def main():
    log.info("=" * 60)
    log.info(f"CATEGORY & STATUS SYNC PIPELINE  |  mode={SYNC_MODE.upper()}")
    log.info("=" * 60)

    # ── Resolve date range and target table from SYNC_MODE ───────────────────
    #
    # SYNC_MODE              Date range              Table
    # ─────────────────────  ──────────────────────  ─────────────────────
    # weekly_auto            Last 7 days             Main
    # monthly_auto           Last full month         Main
    # manual                 SYNC_START / END input  Main
    # test_weekly_auto       Last 7 days             Backup
    # test_monthly_auto      Last full month         Backup
    # test_manual            SYNC_START / END input  Backup

    is_test = SYNC_MODE.startswith("test_")

    if SYNC_MODE in ("manual", "test_manual"):
        if not SYNC_START_INPUT or not SYNC_END_INPUT:
            raise ValueError(
                f"SYNC_MODE={SYNC_MODE} requires SYNC_START_DATE and SYNC_END_DATE (YYYY-MM-DD)"
            )
        start_date = _parse_date(SYNC_START_INPUT)
        end_date   = _parse_date(SYNC_END_INPUT)

    elif SYNC_MODE in ("monthly_auto", "test_monthly_auto"):
        start_date, end_date = _last_full_month()

    else:  # weekly_auto / test_weekly_auto (default)
        start_date, end_date = _last_n_days(7)

    table = TABLE_BACKUP if is_test else TABLE_PRODUCTION

    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")

    log.info(f"Date range  : {start_str} → {end_str}")
    log.info(f"Table       : {PROJECT_ID}.{DATASET}.{table}")
    log.info(f"Test mode   : {is_test}")
    log.info("-" * 60)

    # ── BigQuery client ───────────────────────────────────────────────────────
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GCP_SA_JSON),
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    table_ref = f"{PROJECT_ID}.{DATASET}.{table}"

    # ── Step 1: Load users in range from BigQuery ─────────────────────────────
    log.info(f"STEP 1: Loading users from BigQuery for {start_str} → {end_str}...")
    range_df = _load_range_from_bq(client, table_ref, start_str, end_str)

    if range_df.empty:
        log.warning(f"No users found in BigQuery for this date range. Nothing to sync.")
        return

    log.info(f"  → {len(range_df):,} users loaded")

    # ── Step 2: Fetch latest categories + statuses from Tracxn API ────────────
    log.info(f"STEP 2: Fetching latest categories and statuses from Tracxn API...")
    id_to_category, id_to_status = _fetch_categories_and_status_from_api(range_df["id"].tolist())
    log.info(f"  → Categories fetched for {len(id_to_category):,} users")
    log.info(f"  → Statuses fetched for {len(id_to_status):,} users")

    failed_count = len(range_df) - len(id_to_category)
    if failed_count > 0:
        log.warning(f"  → {failed_count} users had API fetch failures — "
                     f"their categories and statuses are unchanged")

    # ── Step 3: Apply updated categories and statuses ─────────────────────────
    log.info("STEP 3: Applying updated categories and statuses...")
    range_df["id_str"] = range_df["id"].astype(str)

    cat_mask = range_df["id_str"].isin(id_to_category)
    range_df.loc[cat_mask, "userCategory"] = range_df.loc[cat_mask, "id_str"].map(id_to_category)

    status_mask = range_df["id_str"].isin(id_to_status)
    range_df.loc[status_mask, "userStatus"] = range_df.loc[status_mask, "id_str"].map(id_to_status)

    range_df = range_df.drop(columns=["id_str"])

    categories_changed = cat_mask.sum()
    statuses_changed   = status_mask.sum()
    log.info(f"  → {categories_changed:,} user categories updated in memory")
    log.info(f"  → {statuses_changed:,} user statuses updated in memory")

    # ── Step 4: Write updated range to temp table ─────────────────────────────
    temp_table_id = f"{PROJECT_ID}.{DATASET}.temp_catsync_{uuid.uuid4().hex[:8]}"
    log.info(f"STEP 4: Writing updated rows to temp table: {temp_table_id}")
    _write_df_to_bq(client, range_df, temp_table_id)
    log.info(f"  → {len(range_df):,} rows written to temp table")

    # ── Step 5: Atomic table swap ─────────────────────────────────────────────
    log.info("STEP 5: Performing atomic table swap (other dates untouched)...")
    try:
        _atomic_swap(client, table_ref, temp_table_id, start_str, end_str)
        log.info("  → Table swap complete")
    finally:
        # Always clean up temp table, even if swap failed
        _drop_temp_table(client, temp_table_id)
        log.info(f"  → Temp table dropped")

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("CATEGORY & STATUS SYNC COMPLETE")
    log.info(f"  Date range    : {start_str} → {end_str}")
    log.info(f"  Users in range: {len(range_df):,}")
    log.info(f"  Categories updated: {categories_changed:,}")
    log.info(f"  Statuses updated  : {statuses_changed:,}")
    log.info(f"  API failures  : {failed_count}")
    log.info(f"  Table         : {table_ref}")
    log.info("=" * 60)


# ════════════════════════════════════════════════════════════════════════════════
# STEP IMPLEMENTATIONS
# ════════════════════════════════════════════════════════════════════════════════

def _load_range_from_bq(client: bigquery.Client, table_ref: str,
                         start_str: str, end_str: str) -> pd.DataFrame:
    """
    Loads all rows from BigQuery where createdDate is in [start_str, end_str].
    Returns a pandas DataFrame with all columns intact (including userStatus).
    """
    query = f"""
        SELECT *
        FROM `{table_ref}`
        WHERE createdDate BETWEEN DATE('{start_str}') AND DATE('{end_str}')
        ORDER BY createdDate, id
    """
    return client.query(query).to_dataframe()


def _fetch_categories_and_status_from_api(user_ids: list) -> Tuple[dict, dict]:
    """
    Fetches the latest userCategory AND userStatus for a list of user IDs from
    the Tracxn API.

    Uses ID-based filtering in batches of BATCH_SIZE. For each user:
      - userCategory: extracts all entries in categoryList[].userCategory and
        joins them with ", ". Falls back to "Not yet classified" if empty.
      - userStatus: extracted from the top-level "status" field. Falls back to
        "N/A" if absent.

    Both fields come from the same API response — no extra API calls needed.

    Returns:
      (id_to_category, id_to_status) — two dicts {str(user_id): value}.
      Only contains entries for users that were successfully fetched.
      Users with API failures are absent from both dicts (their rows are left
      unchanged in the downstream apply step).
    """
    id_to_category = {}
    id_to_status   = {}
    total_batches = (len(user_ids) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num, i in enumerate(range(0, len(user_ids), BATCH_SIZE), start=1):
        batch_ids = user_ids[i : i + BATCH_SIZE]
        int_ids = []
        for uid in batch_ids:
            try:
                int_ids.append(int(uid))
            except (ValueError, TypeError):
                log.warning(f"  Skipping non-integer user ID: {uid}")

        log.info(f"  Batch {batch_num}/{total_batches} — {len(int_ids)} users")

        users = _post_with_retry({"filter": {"id": int_ids}})
        if users is None:
            log.warning(f"  Batch {batch_num} failed after all retries — skipping")
            continue

        for user in users:
            uid = str(user.get("id", ""))

            cats = [
                c["userCategory"]
                for c in user.get("categoryList", [])
                if c.get("userCategory")
            ]
            id_to_category[uid] = ", ".join(cats) if cats else "Not yet classified"

            id_to_status[uid] = user.get("status") or "N/A"

        time.sleep(SLEEP_S)

    return id_to_category, id_to_status


def _write_df_to_bq(client: bigquery.Client, df: pd.DataFrame, temp_table_id: str):
    """
    Writes a pandas DataFrame to a new BigQuery temp table.

    Uses WRITE_TRUNCATE (creates fresh each time). The table is always
    dropped at the end of the pipeline regardless of success or failure.
    """
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,   # Schema matches the source table exactly
    )
    job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    job.result()


def _atomic_swap(client: bigquery.Client, table_ref: str,
                  temp_table_id: str, start_str: str, end_str: str):
    """
    Reconstructs the full table by unioning:
      - Rows before the sync range (from the existing table — untouched)
      - Rows in the sync range  (from the temp table — updated categories/statuses)
      - Rows after the sync range (from the existing table — untouched)

    Uses CREATE OR REPLACE TABLE so the operation is atomic. The table is
    either fully replaced or not replaced at all if the query fails.

    Preserves the CLUSTER BY registrationType, geography clustering spec
    from the original Colab script.
    """
    sql = f"""
        CREATE OR REPLACE TABLE `{table_ref}`
        CLUSTER BY registrationType, geography
        AS

        -- All rows BEFORE the sync range — completely unchanged
        SELECT * FROM `{table_ref}`
        WHERE createdDate < DATE('{start_str}')

        UNION ALL

        -- Rows IN the sync range — with refreshed categories/statuses from temp table
        SELECT * FROM `{temp_table_id}`

        UNION ALL

        -- All rows AFTER the sync range — completely unchanged
        SELECT * FROM `{table_ref}`
        WHERE createdDate > DATE('{end_str}')
    """
    client.query(sql).result()


def _drop_temp_table(client: bigquery.Client, temp_table_id: str):
    """Silently drops the temp table. not_found_ok=True prevents errors if already gone."""
    try:
        client.delete_table(temp_table_id, not_found_ok=True)
    except Exception as e:
        log.warning(f"Could not drop temp table {temp_table_id}: {e}")


# ════════════════════════════════════════════════════════════════════════════════
# API HELPER
# ════════════════════════════════════════════════════════════════════════════════

def _post_with_retry(payload: dict) -> list | None:
    """
    POSTs to the Tracxn User API with exponential backoff retry.

    Returns list of user records on success, None if all retries fail.
    A 204 (No Content) response is treated as an empty result, not a failure.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                API_USER_ENDPOINT,
                headers=API_HEADERS,
                json=payload,
                timeout=60,
            )
            if resp.status_code == 204:
                return []   # Empty result — valid, not a failure
            if resp.status_code == 200:
                return resp.json().get("result") or []
            log.warning(f"  API HTTP {resp.status_code} (attempt {attempt}): {resp.text[:200]}")
            if attempt < MAX_RETRIES:
                time.sleep(3 * attempt)
        except requests.RequestException as e:
            log.warning(f"  API request error (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(5 * attempt)
    return None   # All retries exhausted


# ════════════════════════════════════════════════════════════════════════════════
# DATE UTILITIES
# ════════════════════════════════════════════════════════════════════════════════

def _parse_date(date_str: str) -> date:
    """Parses YYYY-MM-DD string into a date object."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(
            f"Invalid date format '{date_str}' — expected YYYY-MM-DD (e.g. 2026-05-01)"
        )


def _last_n_days(n: int) -> Tuple[date, date]:
    """
    Returns (start, end) for the last n calendar days, not including today.
    e.g. on 2026-06-07 with n=7: returns (2026-05-31, 2026-06-06)
    """
    today = datetime.now(timezone.utc).date()
    end   = today - timedelta(days=1)
    start = today - timedelta(days=n)
    return start, end


def _last_full_month() -> Tuple[date, date]:
    """
    Returns (first_day, last_day) of the previous complete calendar month.
    e.g. called on any day in June 2026 → returns (2026-05-01, 2026-05-31)
    """
    today      = datetime.now(timezone.utc).date()
    first_this = today.replace(day=1)
    last_prev  = first_this - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    return first_prev, last_prev


if __name__ == "__main__":
    main()
