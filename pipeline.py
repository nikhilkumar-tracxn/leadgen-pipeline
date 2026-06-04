"""
================================================================================
Leadgen BigQuery Upload Pipeline
================================================================================

PURPOSE
-------
This pipeline runs daily to fetch all new user signups from the Tracxn platform
API and load them into a BigQuery table with enriched session/journey data.

It is triggered automatically every day at 6:00 AM IST via GitHub Actions,
and can also be triggered manually for testing or backfilling historical dates.

DATA FLOW
---------
  Tracxn API (platform logs)  ──┐
  Tracxn API (form logs)      ──┼──► in-memory maps ──► enriched records ──► BigQuery
  Tracxn API (users)          ──┘

RUN MODES
---------
  production        Auto: yesterday's date → Main production table
  production_manual Specific date you provide → Main production table (backfill)
  test_auto         Auto: yesterday's date → Backup table (safe testing)
  test_manual       Specific date you provide → Backup table (safe testing)

  Mode is set via the MODE environment variable.
  Date is set via the TEST_DATE environment variable (format: YYYY-MM-DD).
  All four modes are selectable from the GitHub Actions "Run workflow" UI.

ENVIRONMENT VARIABLES
---------------------
  Required secrets (GitHub Secrets):
    TRACXN_ACCESS_TOKEN   API token for Tracxn platform
    GCP_PROJECT_ID        Google Cloud project ID
    GCP_SA_KEY            Full JSON content of the GCP service account key

  Required config variables (GitHub Variables):
    BQ_DATASET            BigQuery dataset name
    BQ_TABLE              Main production table name
    BQ_TABLE_BACKUP       Backup table name (used for test modes)

  Set by the workflow at runtime:
    MODE                  One of: production | production_manual | test_auto | test_manual
    TEST_DATE             Date string YYYY-MM-DD (required for *_manual modes)

STEPS
-----
  Step 1  Fetch platform request logs  → builds {email: [sessionId, ts]} map
  Step 2  Fetch form submission logs   → builds {email: [sessionId, ts, path]} map
  Step 3  Fetch new users for the target date
  Step 4  Enrich each user with origin URL, trigger URL, session journey
  Step 5  Upload enriched records to BigQuery (WRITE_APPEND)

NO GOOGLE SHEETS DEPENDENCY
----------------------------
All intermediate data lives in Python dicts/lists in memory.
There is no connection to Google Sheets anywhere in this pipeline.

AUTHOR / HISTORY
----------------
Originally written as a Google Apps Script pipeline with 5 separate steps
and a Google Sheets-based progress tracker (required due to Apps Script's
6-minute execution limit). Migrated to Python + GitHub Actions to remove
all execution time constraints and Sheets dependencies.
================================================================================
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Secrets / config — all from environment variables, never hardcoded ─────────
ACCESS_TOKEN = os.environ["TRACXN_ACCESS_TOKEN"]
PROJECT_ID   = os.environ["GCP_PROJECT_ID"]
DATASET      = os.environ.get("BQ_DATASET", "leadgen_dataset")
GCP_SA_JSON  = os.environ["GCP_SA_KEY"]

# Run-mode resolution
MODE             = os.environ.get("MODE", "production").lower()
TABLE_PRODUCTION = os.environ.get("BQ_TABLE",        "leadgen_users_v2_no_partition")
TABLE_BACKUP     = os.environ.get("BQ_TABLE_BACKUP",  "leadgen_users_v2_no_partition_backup3")
TEST_DATE_INPUT  = os.environ.get("TEST_DATE", "").strip()   # YYYY-MM-DD

# ── API config ─────────────────────────────────────────────────────────────────
HEADERS = {
    "accessToken": ACCESS_TOKEN,
    "X-Request-Source": "GitHub-Actions-Pipeline",
    "Content-Type": "application/json",
}

API = {
    # Returns all users registered on a given date (filter: DD/MM/YYYY)
    "user":      "https://platform.tracxn.com/api/2.2/user",
    # Returns form submission events (signup/login). Filter uses ISO 8601 timestamps.
    "form":      "https://platform.tracxn.com/api/2.2/logs/frontend/formsubmit",
    # Returns all platform API requests. Filter uses epoch milliseconds.
    "platform":  "https://platform.tracxn.com/api/2.2/platformrequests",
    # Returns URL change events (page navigation) for a given sessionId.
    "urlchange": "https://platform.tracxn.com/api/2.2/logs/frontend/urlchange",
}

# Registration types that come through the web form (/signup path)
# vs. those that hit the platform API directly (used to determine
# which log source to match a session ID from)
FORM_TYPES = {
    "OTP_SIGNUP",
    "THIRD_PARTY_SIGNUP",
    "THIRD_PARTY_SIGNUP_GOOGLE",
    "THIRD_PARTY_SIGNUP_MICROSOFT",
    "THIRD_PARTY_SIGNUP_ENTRA_ID",
}

# Pagination and retry config
SLEEP_S     = 0.3   # seconds to sleep between API pages (rate-limit safety)
BATCH_SIZE  = 20    # records per API page (Tracxn API max)
MAX_RETRIES = 3     # retry attempts per failed API call


# ════════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════════
def main():
    log.info("=" * 60)
    log.info(f"LEADGEN PIPELINE  |  mode={MODE.upper()}")
    log.info("=" * 60)

    # ── Resolve target date and destination table from MODE ──────────────────
    #
    # MODE                 Date source          Table
    # ──────────────────   ──────────────────   ─────────────────────
    # production           auto (yesterday)     Main production table
    # production_manual    TEST_DATE env var    Main production table
    # test_auto            auto (yesterday)     Backup table
    # test_manual          TEST_DATE env var    Backup table

    if MODE == "production_manual":
        if not TEST_DATE_INPUT:
            raise ValueError("MODE=production_manual requires TEST_DATE (YYYY-MM-DD)")
        target_dt = _parse_date(TEST_DATE_INPUT)
        table = TABLE_PRODUCTION
        log.info("PRODUCTION MANUAL MODE — writing to MAIN table")

    elif MODE == "test_manual":
        if not TEST_DATE_INPUT:
            raise ValueError("MODE=test_manual requires TEST_DATE (YYYY-MM-DD)")
        target_dt = _parse_date(TEST_DATE_INPUT)
        table = TABLE_BACKUP
        log.info("TEST MANUAL MODE — writing to BACKUP table")

    elif MODE == "test_auto":
        target_dt = _yesterday()
        table = TABLE_BACKUP
        log.info("TEST AUTO MODE — writing to BACKUP table")

    else:  # production (default, used by scheduled runs)
        target_dt = _yesterday()
        table = TABLE_PRODUCTION
        log.info("PRODUCTION AUTO MODE — writing to MAIN table")

    # Log window is target ± 1 day to catch any timezone edge cases in the API
    log_start = target_dt - timedelta(days=1)
    log_end   = target_dt + timedelta(days=1)

    # Two date formats: Tracxn User API wants DD/MM/YYYY, BigQuery wants YYYY-MM-DD
    target_date_api = target_dt.strftime("%d/%m/%Y")
    target_date_bq  = target_dt.strftime("%Y-%m-%d")

    log.info(f"Target date : {target_date_api}  ({target_date_bq})")
    log.info(f"Log window  : {log_start.date()} → {log_end.date()}")
    log.info(f"Destination : {PROJECT_ID}.{DATASET}.{table}")
    log.info("-" * 60)

    # ── Execute pipeline steps ────────────────────────────────────────────────
    platform_map = step1_fetch_platform_logs(log_start, log_end)
    form_map     = step2_fetch_form_logs(log_start, log_end)
    users        = step3_fetch_users(target_date_api)

    log.info(f"Fetched {len(users)} users")

    records = step4_enrich_users(users, form_map, platform_map, target_date_api)
    log.info(f"Enriched {len(records)} records")

    if not records:
        log.warning("No records to upload. Pipeline complete with 0 rows.")
        return

    step5_upload_to_bigquery(records, table)

    log.info("=" * 60)
    log.info(f"PIPELINE COMPLETE  |  {target_date_api}  →  {table}")
    log.info("=" * 60)


# ════════════════════════════════════════════════════════════════════════════════
# STEP 1 — Fetch Platform Request Logs
# ════════════════════════════════════════════════════════════════════════════════
def step1_fetch_platform_logs(start: datetime, end: datetime) -> dict:
    """
    Fetches all platform API request logs in the given date window.

    Why we need this:
      For users who registered via the platform API (non-form registrations),
      the session ID is stored in the platform request log, not the form log.
      We need the session ID to later fetch their URL navigation journey.

    Filter format: epoch milliseconds (integers).

    Returns:
      dict keyed by email → list of {sessionId, ts}
      Example: {"user@example.com": [{"sessionId": "abc123", "ts": 1717200000000}]}
    """
    log.info("STEP 1: Fetching platform logs...")
    payload = {
        "filter": {
            "createdDate": {
                "min": int(start.timestamp() * 1000),
                "max": int(end.replace(hour=23, minute=59, second=59).timestamp() * 1000),
            }
        }
    }
    records = fetch_all(API["platform"], payload, "platform_logs")
    log.info(f"  → {len(records)} platform log entries")

    result: dict = {}
    for r in records:
        email = (r.get("requestor", {}).get("userEmail") or "").lower()
        sid   = r.get("requestor", {}).get("sessionId") or ""
        ts    = r.get("createdDate", {}).get("epochMillis", 0)
        if email:
            result.setdefault(email, []).append({"sessionId": sid, "ts": ts})

    log.info(f"  → {len(result)} unique emails in platform map")
    return result


# ════════════════════════════════════════════════════════════════════════════════
# STEP 2 — Fetch Form Submission Logs
# ════════════════════════════════════════════════════════════════════════════════
def step2_fetch_form_logs(start: datetime, end: datetime) -> dict:
    """
    Fetches all form submission events (signup, third-party auth) in the date window.

    Why we need this:
      For users who registered via the web signup form, the session ID is captured
      in the form submission log (not the platform log). We filter entries where
      path == "/signup" to find the correct session.

    Filter format: ISO 8601 string with +00:00 UTC offset.
      e.g. "2026-06-01T00:00:00+00:00"

    Returns:
      dict keyed by email → list of {sessionId, ts, path}
    """
    log.info("STEP 2: Fetching form logs...")

    def fmt(dt: datetime, end_of_day: bool) -> str:
        d = dt.replace(hour=23, minute=59, second=59) if end_of_day \
            else dt.replace(hour=0, minute=0, second=0)
        return d.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    payload = {
        "filter": {
            "createdDate": {"min": fmt(start, False), "max": fmt(end, True)}
        }
    }
    records = fetch_all(API["form"], payload, "form_logs")
    log.info(f"  → {len(records)} form log entries")

    result: dict = {}
    for r in records:
        email = (r.get("metrics", {}).get("customData", {}).get("userName") or "").lower()
        sid   = r.get("sessionId") or ""
        ts    = r.get("createdDate", {}).get("epochMillis", 0)
        path  = r.get("metrics", {}).get("page", {}).get("parsedUrl", {}).get("pathname") or ""
        if email:
            result.setdefault(email, []).append({"sessionId": sid, "ts": ts, "path": path})

    log.info(f"  → {len(result)} unique emails in form map")
    return result


# ════════════════════════════════════════════════════════════════════════════════
# STEP 3 — Fetch New Users
# ════════════════════════════════════════════════════════════════════════════════
def step3_fetch_users(target_date: str) -> list:
    """
    Fetches all users whose account was created on the target date.

    Filter format: DD/MM/YYYY (this specific format is required by the Tracxn
    User API — different from the log APIs which use epoch ms or ISO 8601).

    Returns:
      list of raw user dicts from the API
    """
    log.info(f"STEP 3: Fetching users for {target_date}...")
    payload = {"filter": {"createdDate": {"min": target_date, "max": target_date}}}
    users = fetch_all(API["user"], payload, "users")
    log.info(f"  → {len(users)} users fetched")
    return users


# ════════════════════════════════════════════════════════════════════════════════
# STEP 4 — Enrich Users
# ════════════════════════════════════════════════════════════════════════════════
def step4_enrich_users(users: list, form_map: dict, platform_map: dict, target_date: str) -> list:
    """
    Enriches each user record with session data and navigation journey.

    For each user:
      1. Determines which log source to use (form vs platform) based on registrationType
      2. Finds their session ID from the appropriate log map
      3. Fetches their URL change events using that session ID
      4. Derives: originUrl (first page), triggerUrl (page before signup), userJourney (full path)

    Returns:
      list of flat dicts ready for BigQuery insertion
    """
    log.info(f"STEP 4: Enriching {len(users)} users...")
    records = []
    for i, user in enumerate(users, 1):
        if i % 50 == 0:
            log.info(f"  Processed {i}/{len(users)}...")
        try:
            records.append(build_user_record(user, form_map, platform_map, target_date))
        except Exception as e:
            # Log and skip bad records rather than failing the entire pipeline
            log.warning(f"  Skipping user {user.get('id', '?')} due to error: {e}")
    log.info(f"  → {len(records)} records ready")
    return records


def build_user_record(user: dict, form_map: dict, platform_map: dict, target_date: str) -> dict:
    """
    Builds a single enriched record for one user.

    Session ID resolution logic:
      - If registrationType is in FORM_TYPES → look in form_map, filter path==/signup,
        take the earliest timestamp (first signup attempt)
      - Otherwise → look in platform_map, take the earliest timestamp

    Journey resolution logic:
      - Fetch URL change events for the session (up to 50 events)
      - origin  = first URL the user visited
      - journey = full navigation path, URLs joined by " > ", query strings stripped
      - trigger = the last non-auth page before the signup/login page
                  (walks backwards through tab chain using prevTab pointer)
    """
    email    = (user.get("email") or "").lower()
    reg_type = user.get("registrationType") or ""

    # Build category string from categoryList (more specific than userCategory)
    cats = [c.get("userCategory") for c in (user.get("categoryList") or []) if c.get("userCategory")]
    user_category = ", ".join(cats) if cats else (user.get("userCategory") or "N/A")

    # createdDate comes as {year, month, day} dict. Values may be int or str
    # depending on the API response — always cast to int to be safe.
    cd = user.get("createdDate") or {}
    created_date = "{}-{:02d}-{:02d}".format(
        int(cd.get("year") or 2025),
        int(cd.get("month") or 1),
        int(cd.get("day") or 1),
    )

    # ── Find session ID ────────────────────────────────────────────────────
    session_id = None
    if reg_type in FORM_TYPES:
        entries = sorted(
            [e for e in form_map.get(email, []) if e["path"] == "/signup"],
            key=lambda x: x["ts"]
        )
        if entries:
            session_id = entries[0]["sessionId"]
    else:
        entries = sorted(platform_map.get(email, []), key=lambda x: x["ts"])
        if entries:
            session_id = entries[0]["sessionId"]

    # ── Build journey from URL change events ───────────────────────────────
    origin = trigger = journey = "N/A"

    if session_id:
        url_logs = fetch_limited(API["urlchange"], {"filter": {"sessionId": session_id}}, 50)
        events = sorted(
            [
                {
                    "ts":      e.get("createdDate", {}).get("epochMillis", 0),
                    "url":     e.get("metrics", {}).get("page", {}).get("url") or "",
                    "path":    e.get("metrics", {}).get("page", {}).get("parsedUrl", {}).get("pathname") or "",
                    "tab":     e.get("tabId"),
                    "prevTab": e.get("previousTabId"),
                }
                for e in url_logs
                if e.get("metrics", {}).get("page", {}).get("url")
            ],
            key=lambda x: x["ts"]
        )

        if events:
            origin  = events[0]["url"]
            journey = " > ".join(e["url"].split("?")[0] for e in events)

            auth_events = [e for e in events if e["path"] in ("/signup", "/login")]
            if auth_events:
                # Walk backwards through the tab chain from the last auth event
                # to find the page that brought the user to signup
                tab = auth_events[-1]["prevTab"]
                while tab:
                    prev = next((e for e in events if e["tab"] == tab), None)
                    if not prev:
                        break
                    if prev["path"] not in ("/signup", "/login"):
                        trigger = prev["url"]
                        break
                    tab = prev["prevTab"]
                if trigger == "N/A":
                    trigger = origin   # fallback: origin was also the trigger
            else:
                trigger = origin

    return {
        "createdDate":      created_date,
        "id":               str(user.get("id") or ""),
        "email":            email,
        "userCategory":     clean(user_category),
        "originUrl":        clean(origin),
        "triggerUrl":       clean(trigger),
        "geography":        clean(user.get("primaryGeography") or "N/A"),
        "registrationType": clean(reg_type),
        "sessionId":        clean(session_id or "N/A"),
        "userJourney":      clean(journey, is_journey=True),
        "cta":              f"Auto_{target_date}",
    }


# ════════════════════════════════════════════════════════════════════════════════
# STEP 5 — Upload to BigQuery
# ════════════════════════════════════════════════════════════════════════════════
def step5_upload_to_bigquery(records: list, table: str):
    """
    Uploads enriched records to BigQuery using WRITE_APPEND disposition.

    Format: Newline-delimited JSON (NDJSON) — chosen over CSV because it
    handles commas, quotes, and newlines in field values without escaping issues.

    Authentication: Uses the GCP service account JSON from the GCP_SA_KEY
    environment variable (stored as a GitHub Secret).

    The schema is explicitly provided so BigQuery validates types on insert
    rather than inferring them, which prevents silent type mismatches.
    """
    log.info(f"STEP 5: Uploading {len(records)} rows → {PROJECT_ID}.{DATASET}.{table}")

    creds = service_account.Credentials.from_service_account_info(
        json.loads(GCP_SA_JSON),
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    client    = bigquery.Client(project=PROJECT_ID, credentials=creds)
    table_ref = f"{PROJECT_ID}.{DATASET}.{table}"

    schema = [
        bigquery.SchemaField("createdDate",       "DATE",   mode="NULLABLE"),
        bigquery.SchemaField("id",                "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email",             "STRING", mode="NULLABLE"),
        bigquery.SchemaField("userCategory",      "STRING", mode="NULLABLE"),
        bigquery.SchemaField("originUrl",         "STRING", mode="NULLABLE"),
        bigquery.SchemaField("triggerUrl",        "STRING", mode="NULLABLE"),
        bigquery.SchemaField("geography",         "STRING", mode="NULLABLE"),
        bigquery.SchemaField("registrationType",  "STRING", mode="NULLABLE"),
        bigquery.SchemaField("sessionId",         "STRING", mode="NULLABLE"),
        bigquery.SchemaField("userJourney",       "STRING", mode="NULLABLE"),
        bigquery.SchemaField("cta",               "STRING", mode="NULLABLE"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",          # Always appends, never overwrites
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    job = client.load_table_from_json(records, table_ref, job_config=job_config)
    job.result()   # Blocks until the BQ load job completes; raises on failure
    log.info(f"  ✓ {len(records)} rows uploaded successfully")


# ════════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ════════════════════════════════════════════════════════════════════════════════
def fetch_all(endpoint: str, payload: dict, name: str) -> list:
    """
    Paginates through all records from a Tracxn API endpoint.

    Tracxn APIs use offset-based pagination: each request includes
    `from` (start index) and `size` (page size). We keep incrementing
    `from` until we get an empty page, which signals the end.

    Retries are handled inside _post(). If all retries fail for a page,
    we stop pagination (safer than skipping and causing gaps).
    """
    results = []
    payload = {**payload, "size": BATCH_SIZE, "from": 0}
    log.info(f"  [{name}] Starting fetch — payload: {json.dumps(payload)}")

    while True:
        batch, ok = _post(endpoint, payload, name)
        if not ok or not batch:
            break   # Empty page = end of results; failed page = stop safely
        results.extend(batch)
        payload["from"] += len(batch)
        if len(results) % 150 == 0:
            log.info(f"  [{name}] {len(results)} records so far...")
        time.sleep(SLEEP_S)

    return results


def fetch_limited(endpoint: str, payload: dict, max_records: int) -> list:
    """
    Fetches a single page of up to max_records records.
    Used for per-user URL change lookups where we want a cap
    to prevent slow users with huge session histories from blocking the pipeline.
    """
    payload = {**payload, "size": min(BATCH_SIZE, max_records), "from": 0}
    batch, _ = _post(endpoint, payload, "urlchange")
    return (batch or [])[:max_records]


def _post(endpoint: str, payload: dict, name: str):
    """
    Makes a single POST request with exponential backoff retry.

    Returns:
      (list_of_records, success_bool)
      On failure after all retries: ([], False)
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(endpoint, headers=HEADERS, json=payload, timeout=30)
            if resp.status_code != 200:
                log.warning(f"  [{name}] HTTP {resp.status_code} (attempt {attempt}): {resp.text[:300]}")
                if attempt < MAX_RETRIES:
                    time.sleep(3 * attempt)
                    continue
                return [], False
            return resp.json().get("result") or [], True
        except requests.RequestException as e:
            log.warning(f"  [{name}] Request error (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(5 * attempt)
    return [], False


def clean(value: Optional[str], is_journey: bool = False) -> str:
    """
    Sanitises a value for safe storage in BigQuery.

    - Replaces all newline/tab variants with spaces
    - Collapses multiple spaces
    - Replaces Unicode arrows (→) with > in journey fields
    - Returns "N/A" for null/empty/None values
    - Truncates to 10,000 chars (well within BigQuery STRING limits)
    """
    if not value or str(value).lower() in ("none", "null", "undefined", ""):
        return "N/A"
    s = str(value).replace("\r\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")
    if is_journey:
        s = s.replace("→", ">").replace("==>", ">")
    s = " ".join(s.split()).strip()
    return s[:10000] if s else "N/A"


# ── Date utilities ─────────────────────────────────────────────────────────────
def _yesterday() -> datetime:
    """Returns start of yesterday in UTC."""
    return (datetime.now(timezone.utc) - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def _parse_date(date_str: str) -> datetime:
    """Parses a YYYY-MM-DD string into a UTC midnight datetime."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError(f"Invalid date format '{date_str}' — expected YYYY-MM-DD (e.g. 2026-05-01)")


if __name__ == "__main__":
    main()
