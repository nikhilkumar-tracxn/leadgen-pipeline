"""
================================================================================
Manual Reprocess Pipeline  —  reprocess_range.py
================================================================================

PURPOSE
-------
Reprocesses a date range of users already in BigQuery using the fixed pipeline
logic (wider log window + cross-source session ID fallback). Designed to be
run once to fix historical data — specifically May 2026 which was processed
with the old code that had ~25% session ID miss rate.

HOW IT WORKS (per-day loop)
----------------------------
For each day in the date range:

  1. Fetch fresh enriched records from the Tracxn API using the fixed logic
  2. Query BigQuery to count session ID misses in the EXISTING data for that day
  3. Count session ID misses in the NEWLY processed data for that day
  4. Compare:
       - If new data has FEWER misses (better) → replace that day's data in BQ
       - If new data has EQUAL OR MORE misses (worse/same) → skip, keep existing
  5. Log the decision and move to the next day

SAFETY
------
- Only touches rows for the specific day being processed
- All other dates in the table are completely untouched
- Uses the atomic swap pattern (CREATE OR REPLACE TABLE with UNION ALL) so
  the update is all-or-nothing per day — no partial states
- A dry-run mode logs decisions without writing anything to BigQuery
- Full audit log is printed at the end showing every day's outcome

ENVIRONMENT VARIABLES
---------------------
  Required secrets (same GitHub Secrets as pipeline.py):
    TRACXN_ACCESS_TOKEN
    GCP_PROJECT_ID
    GCP_SA_KEY

  Required config:
    BQ_DATASET          (or defaults to leadgen_dataset)
    BQ_TABLE            Target table (main production table)

  Required for this script:
    REPROCESS_START     Start date  YYYY-MM-DD  e.g. 2026-05-01
    REPROCESS_END       End date    YYYY-MM-DD  e.g. 2026-05-31

  Optional:
    DRY_RUN             Set to "true" to log decisions without writing to BQ
                        Useful to preview what would change before committing

HOW TO RUN (GitHub Actions manual trigger)
------------------------------------------
  Set the workflow inputs:
    reprocess_start: 2026-05-01
    reprocess_end:   2026-05-31
    dry_run:         false   (or true to preview first)

ALSO SAFE TO RUN MULTIPLE TIMES
---------------------------------
Running on the same date range twice is safe. The compare step ensures we
never replace good data with worse data.
================================================================================
"""

import os
import json
import time
import logging
import uuid
from datetime import datetime, timedelta, timezone, date
from typing import Optional

import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
ACCESS_TOKEN = os.environ["TRACXN_ACCESS_TOKEN"]
PROJECT_ID   = os.environ["GCP_PROJECT_ID"]
DATASET      = os.environ.get("BQ_DATASET", "leadgen_dataset")
GCP_SA_JSON  = os.environ["GCP_SA_KEY"]
TABLE        = os.environ.get("BQ_TABLE", "leadgen_users_v2_no_partition")

REPROCESS_START = os.environ.get("REPROCESS_START", "").strip()   # YYYY-MM-DD
REPROCESS_END   = os.environ.get("REPROCESS_END",   "").strip()   # YYYY-MM-DD
DRY_RUN         = os.environ.get("DRY_RUN", "false").lower() == "true"

HEADERS = {
    "accessToken": ACCESS_TOKEN,
    "X-Request-Source": "GitHub-Actions-Reprocess",
    "Content-Type": "application/json",
}

API = {
    "user":      "https://platform.tracxn.com/api/2.2/user",
    "form":      "https://platform.tracxn.com/api/2.2/logs/frontend/formsubmit",
    "platform":  "https://platform.tracxn.com/api/2.2/platformrequests",
    "urlchange": "https://platform.tracxn.com/api/2.2/logs/frontend/urlchange",
}

FORM_TYPES = {
    "OTP_SIGNUP", "THIRD_PARTY_SIGNUP", "THIRD_PARTY_SIGNUP_GOOGLE",
    "THIRD_PARTY_SIGNUP_MICROSOFT", "THIRD_PARTY_SIGNUP_ENTRA_ID",
}

# Wider log window: 3 days before target (matching Apps Script behaviour)
LOG_WINDOW_DAYS_BEFORE = 3
LOG_WINDOW_DAYS_AFTER  = 1

SLEEP_S     = 0.3
BATCH_SIZE  = 30
MAX_RETRIES = 3


# ════════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════════
def main():
    if not REPROCESS_START or not REPROCESS_END:
        raise ValueError("REPROCESS_START and REPROCESS_END must be set (YYYY-MM-DD)")

    start_dt = _parse_date(REPROCESS_START)
    end_dt   = _parse_date(REPROCESS_END)

    if start_dt > end_dt:
        raise ValueError(f"REPROCESS_START ({REPROCESS_START}) must be <= REPROCESS_END ({REPROCESS_END})")

    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    log.info("=" * 70)
    log.info("MANUAL REPROCESS PIPELINE")
    log.info("=" * 70)
    log.info(f"Date range  : {REPROCESS_START} → {REPROCESS_END}")
    log.info(f"Table       : {table_ref}")
    log.info(f"Dry run     : {DRY_RUN}")
    log.info(f"Log window  : {LOG_WINDOW_DAYS_BEFORE}d before + {LOG_WINDOW_DAYS_AFTER}d after each target date")
    log.info("=" * 70)

    # BigQuery client (reused across all days)
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GCP_SA_JSON),
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    bq = bigquery.Client(project=PROJECT_ID, credentials=creds)

    # Track outcomes for final audit summary
    audit = []

    # ── Per-day loop ──────────────────────────────────────────────────────────
    current = start_dt
    day_num = 0
    total_days = (end_dt - start_dt).days + 1

    while current <= end_dt:
        day_num += 1
        date_str     = current.strftime("%Y-%m-%d")
        date_api_str = current.strftime("%d/%m/%Y")

        log.info("")
        log.info(f"{'─'*70}")
        log.info(f"DAY {day_num}/{total_days}  —  {date_str}")
        log.info(f"{'─'*70}")

        outcome = _process_one_day(bq, table_ref, current, date_str, date_api_str)
        audit.append(outcome)

        current += timedelta(days=1)

    # ── Final audit summary ───────────────────────────────────────────────────
    _print_audit_summary(audit)


# ════════════════════════════════════════════════════════════════════════════════
# PER-DAY PROCESSING
# ════════════════════════════════════════════════════════════════════════════════
def _process_one_day(bq, table_ref, target_dt, date_str, date_api_str) -> dict:
    """
    Processes one calendar day end-to-end and returns an audit dict.
    """
    result = {
        "date":           date_str,
        "existing_total": 0,
        "existing_miss":  0,
        "new_total":      0,
        "new_miss":       0,
        "action":         "SKIPPED",
        "reason":         "",
    }

    # ── Step A: Check existing data in BigQuery ───────────────────────────────
    log.info(f"  A. Checking existing BigQuery data for {date_str}...")
    existing_stats = _query_existing_miss_rate(bq, table_ref, date_str)
    result["existing_total"] = existing_stats["total"]
    result["existing_miss"]  = existing_stats["missing"]

    if existing_stats["total"] == 0:
        log.info(f"  → No existing rows for {date_str} in BigQuery. Will insert fresh data.")
    else:
        log.info(f"  → Existing: {existing_stats['total']} rows, "
                 f"{existing_stats['missing']} missing session IDs "
                 f"({existing_stats['miss_pct']:.1f}%)")

    # ── Step B: Fetch fresh data from APIs ───────────────────────────────────
    log.info(f"  B. Fetching fresh data from Tracxn API...")
    log_start = target_dt - timedelta(days=LOG_WINDOW_DAYS_BEFORE)
    log_end   = target_dt + timedelta(days=LOG_WINDOW_DAYS_AFTER)

    platform_map = _fetch_platform_logs(log_start, log_end)
    form_map     = _fetch_form_logs(log_start, log_end)
    users        = _fetch_users(date_api_str)

    if not users:
        log.info(f"  → No users found for {date_str} in Tracxn API. Skipping.")
        result["action"] = "SKIPPED"
        result["reason"] = "No users returned from API"
        return result

    # ── Step C: Enrich users ──────────────────────────────────────────────────
    log.info(f"  C. Enriching {len(users)} users...")
    new_records, new_miss = _enrich_users(users, form_map, platform_map, date_api_str)
    new_total = len(new_records)
    result["new_total"] = new_total
    result["new_miss"]  = new_miss
    new_miss_pct = (new_miss / new_total * 100) if new_total else 0
    log.info(f"  → New data: {new_total} rows, {new_miss} missing session IDs ({new_miss_pct:.1f}%)")

    # ── Step D: Compare and decide ────────────────────────────────────────────
    log.info(f"  D. Comparing...")

    if existing_stats["total"] == 0:
        # No existing data — just insert
        decision = "INSERT"
        reason   = "No existing data for this date"
    elif new_miss < existing_stats["missing"]:
        # New data is strictly better
        decision = "REPLACE"
        reason   = (f"New miss count ({new_miss}) < existing ({existing_stats['missing']}) "
                    f"— improvement of {existing_stats['missing'] - new_miss} rows")
    elif new_miss == existing_stats["missing"]:
        decision = "SKIP"
        reason   = f"Same miss count ({new_miss}) — no improvement, keeping existing data"
    else:
        decision = "SKIP"
        reason   = (f"New miss count ({new_miss}) > existing ({existing_stats['missing']}) "
                    f"— new data is worse, keeping existing")

    log.info(f"  → Decision: {decision}  ({reason})")

    # ── Step E: Execute decision ──────────────────────────────────────────────
    if DRY_RUN:
        log.info(f"  → DRY RUN: would {decision} but not writing to BigQuery")
        result["action"] = f"DRY_{decision}"
        result["reason"] = reason
        return result

    if decision in ("INSERT", "REPLACE"):
        log.info(f"  E. Writing to BigQuery...")
        if decision == "REPLACE":
            _atomic_replace_day(bq, table_ref, new_records, date_str)
        else:
            _insert_day(bq, table_ref, new_records)
        log.info(f"  → {decision} complete for {date_str}")
        result["action"] = decision
    else:
        result["action"] = "SKIPPED"

    result["reason"] = reason
    return result


# ════════════════════════════════════════════════════════════════════════════════
# BIGQUERY OPERATIONS
# ════════════════════════════════════════════════════════════════════════════════
def _query_existing_miss_rate(bq, table_ref: str, date_str: str) -> dict:
    """
    Queries BigQuery for the current session ID miss rate for a specific date.
    Returns: {total, missing, miss_pct}
    """
    query = f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(sessionId = 'N/A' OR sessionId IS NULL) AS missing
        FROM `{table_ref}`
        WHERE createdDate = DATE('{date_str}')
    """
    row = list(bq.query(query).result())[0]
    total   = row.total or 0
    missing = row.missing or 0
    return {
        "total":    total,
        "missing":  missing,
        "miss_pct": (missing / total * 100) if total else 0.0,
    }


def _atomic_replace_day(bq, table_ref: str, records: list, date_str: str):
    """
    Replaces all rows for date_str in table_ref with the new records.
    Uses the atomic swap pattern — all other dates are untouched.

    We use load_table_from_json with an explicit schema (not autodetect)
    so that createdDate is correctly typed as DATE in the temp table.
    autodetect would infer "2026-05-01" as STRING, causing the UNION ALL
    to fail with "incompatible types: DATE, STRING".
    """
    temp_id = f"{PROJECT_ID}.{DATASET}.temp_reprocess_{uuid.uuid4().hex[:8]}"

    # Explicit schema — must match the main table exactly
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

    try:
        # Write to temp table via NDJSON with explicit schema
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        bq.load_table_from_json(records, temp_id, job_config=job_config).result()

        # Atomic swap: rebuild table excluding this date, then add new rows
        sql = f"""
            CREATE OR REPLACE TABLE `{table_ref}`
            CLUSTER BY registrationType, geography
            AS
            SELECT * FROM `{table_ref}`
            WHERE createdDate != DATE('{date_str}')

            UNION ALL

            SELECT * FROM `{temp_id}`
        """
        bq.query(sql).result()

    finally:
        try:
            bq.delete_table(temp_id, not_found_ok=True)
        except Exception:
            pass


def _insert_day(bq, table_ref: str, records: list):
    """
    Inserts records for a date that has no existing rows (simple append).
    """
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
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    bq.load_table_from_json(records, table_ref, job_config=job_config).result()


# ════════════════════════════════════════════════════════════════════════════════
# API FETCH FUNCTIONS  (same logic as fixed pipeline.py)
# ════════════════════════════════════════════════════════════════════════════════
def _fetch_platform_logs(start: datetime, end: datetime) -> dict:
    payload = {
        "filter": {
            "createdDate": {
                "min": int(start.timestamp() * 1000),
                "max": int(end.replace(hour=23, minute=59, second=59).timestamp() * 1000),
            }
        }
    }
    records = _fetch_all(API["platform"], payload, "platform")
    result: dict = {}
    for r in records:
        email = (r.get("requestor", {}).get("userEmail") or "").lower()
        sid   = r.get("requestor", {}).get("sessionId") or ""
        ts    = r.get("createdDate", {}).get("epochMillis", 0)
        if email and sid:
            result.setdefault(email, []).append({"sessionId": sid, "ts": ts})
    log.info(f"    platform map: {len(records)} entries, {len(result)} unique emails")
    return result


def _fetch_form_logs(start: datetime, end: datetime) -> dict:
    def fmt(dt, end_of_day):
        d = dt.replace(hour=23, minute=59, second=59) if end_of_day else dt.replace(hour=0, minute=0, second=0)
        return d.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    payload = {"filter": {"createdDate": {"min": fmt(start, False), "max": fmt(end, True)}}}
    records = _fetch_all(API["form"], payload, "form")
    result: dict = {}
    for r in records:
        email = (r.get("metrics", {}).get("customData", {}).get("userName") or "").lower()
        sid   = r.get("sessionId") or ""
        ts    = r.get("createdDate", {}).get("epochMillis", 0)
        path  = r.get("metrics", {}).get("page", {}).get("parsedUrl", {}).get("pathname") or ""
        if email and sid:
            result.setdefault(email, []).append({"sessionId": sid, "ts": ts, "path": path})
    log.info(f"    form map: {len(records)} entries, {len(result)} unique emails")
    return result


def _fetch_users(target_date: str) -> list:
    payload = {"filter": {"createdDate": {"min": target_date, "max": target_date}}}
    users = _fetch_all(API["user"], payload, "users")
    log.info(f"    {len(users)} users fetched for {target_date}")
    return users


def _enrich_users(users: list, form_map: dict, platform_map: dict, target_date: str):
    """Returns (records_list, miss_count)."""
    records = []
    miss_count = 0
    for user in users:
        try:
            record = _build_record(user, form_map, platform_map, target_date)
            records.append(record)
            if record["sessionId"] == "N/A":
                miss_count += 1
        except Exception as e:
            log.warning(f"    Skipping user {user.get('id','?')}: {e}")
            miss_count += 1
    return records, miss_count


def _build_record(user: dict, form_map: dict, platform_map: dict, target_date: str) -> dict:
    email    = (user.get("email") or "").lower()
    reg_type = user.get("registrationType") or ""

    cats = [c.get("userCategory") for c in (user.get("categoryList") or []) if c.get("userCategory")]
    user_category = ", ".join(cats) if cats else (user.get("userCategory") or "N/A")

    cd = user.get("createdDate") or {}
    created_date = "{}-{:02d}-{:02d}".format(
        int(cd.get("year") or 2025),
        int(cd.get("month") or 1),
        int(cd.get("day") or 1),
    )

    # ── Session ID resolution ─────────────────────────────────────────────────
    #
    # RULE: we want the session that existed BEFORE the user created their
    # account — i.e. the browsing session that led them to sign up.
    # After signup, a user gets new session IDs (post-activate, post-login)
    # which belong to their in-platform activity, not their acquisition journey.
    #
    # Step 1: Convert the user's createdDate to epoch ms for timestamp comparison.
    # Step 2: For FORM_TYPES users, look in form_map for path==/signup entries
    #         that occurred BEFORE createdDate.
    #         Drop /activate — those are post-signup email verification steps
    #         and always have a later session ID than we want.
    # Step 3: If no pre-signup /signup entry found, fall back to platform_map
    #         entries that occurred before createdDate.
    # Step 4: For non-FORM_TYPES, try platform_map first (pre-signup), then
    #         form_map as fallback (pre-signup).
    # Step 5: If absolutely nothing is found before createdDate, fall back to
    #         the earliest entry overall (rare edge case, e.g. same-millisecond
    #         timestamps or clock skew between API servers).

    # Convert createdDate {year, month, day} → epoch ms (midnight UTC)
    created_epoch = None
    try:
        created_epoch = int(datetime(
            int(cd.get("year") or 2025),
            int(cd.get("month") or 1),
            int(cd.get("day") or 1),
            tzinfo=timezone.utc
        ).timestamp() * 1000)
    except Exception:
        created_epoch = None   # if conversion fails, skip the timestamp filter

    def _pick_session(candidates: list) -> str | None:
        """
        Given a list of log entries [{sessionId, ts, ...}], returns the
        sessionId of the entry with the earliest ts that is strictly before
        created_epoch.  Falls back to the globally earliest entry if nothing
        qualifies (handles same-ms or missing timestamp edge cases).
        Returns None if the list is empty.
        """
        if not candidates:
            return None
        if created_epoch is not None:
            pre = [e for e in candidates if e["ts"] < created_epoch]
        else:
            pre = []
        pool = sorted(pre if pre else candidates, key=lambda x: x["ts"])
        return pool[0]["sessionId"] if pool else None

    session_id = None

    if reg_type in FORM_TYPES:
        # Primary: form map, /signup path ONLY (not /activate — those are post-signup)
        signup_entries = [
            e for e in form_map.get(email, [])
            if e["path"].startswith("/signup")
        ]
        session_id = _pick_session(signup_entries)

        # Fallback: platform map (pre-signup entries)
        if not session_id:
            session_id = _pick_session(platform_map.get(email, []))

    else:
        # Primary: platform map (pre-signup entries)
        session_id = _pick_session(platform_map.get(email, []))

        # Fallback: form map, any path (pre-signup)
        if not session_id:
            session_id = _pick_session(form_map.get(email, []))

    # Journey
    origin = trigger = journey = "N/A"
    if session_id:
        url_logs = _fetch_limited(API["urlchange"], {"filter": {"sessionId": session_id}}, 50)
        events = sorted(
            [{"ts": e.get("createdDate", {}).get("epochMillis", 0),
              "url": e.get("metrics", {}).get("page", {}).get("url") or "",
              "path": e.get("metrics", {}).get("page", {}).get("parsedUrl", {}).get("pathname") or "",
              "tab": e.get("tabId"), "prevTab": e.get("previousTabId")}
             for e in url_logs if e.get("metrics", {}).get("page", {}).get("url")],
            key=lambda x: x["ts"]
        )
        if events:
            origin  = events[0]["url"]
            journey = " > ".join(e["url"].split("?")[0] for e in events)
            auth_events = [e for e in events if e["path"].startswith(("/signup", "/login"))]
            if auth_events:
                tab = auth_events[-1]["prevTab"]
                while tab:
                    prev = next((e for e in events if e["tab"] == tab), None)
                    if not prev: break
                    if not prev["path"].startswith(("/signup", "/login")):
                        trigger = prev["url"]
                        break
                    tab = prev["prevTab"]
                if trigger == "N/A":
                    trigger = origin
            else:
                trigger = origin

    return {
        "createdDate":      created_date,
        "id":               str(user.get("id") or ""),
        "email":            email,
        "userCategory":     _clean(user_category),
        "originUrl":        _clean(origin),
        "triggerUrl":       _clean(trigger),
        "geography":        _clean(user.get("primaryGeography") or "N/A"),
        "registrationType": _clean(reg_type),
        "sessionId":        _clean(session_id or "N/A"),
        "userJourney":      _clean(journey, is_journey=True),
        "cta":              f"Auto_{target_date}",
    }


# ════════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ════════════════════════════════════════════════════════════════════════════════
def _fetch_all(endpoint: str, payload: dict, name: str) -> list:
    results = []
    payload = {**payload, "size": BATCH_SIZE, "from": 0}
    while True:
        batch, ok = _post(endpoint, payload, name)
        if not ok or not batch:
            break
        results.extend(batch)
        payload["from"] += len(batch)
        time.sleep(SLEEP_S)
    return results


def _fetch_limited(endpoint: str, payload: dict, max_records: int) -> list:
    payload = {**payload, "size": min(BATCH_SIZE, max_records), "from": 0}
    batch, _ = _post(endpoint, payload, "urlchange")
    return (batch or [])[:max_records]


def _post(endpoint: str, payload: dict, name: str):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(endpoint, headers=HEADERS, json=payload, timeout=30)
            if resp.status_code != 200:
                log.warning(f"  [{name}] HTTP {resp.status_code} (attempt {attempt}): {resp.text[:200]}")
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


def _clean(value: Optional[str], is_journey: bool = False) -> str:
    if not value or str(value).lower() in ("none", "null", "undefined", ""):
        return "N/A"
    s = str(value).replace("\r\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")
    if is_journey:
        s = s.replace("→", ">").replace("==>", ">")
    s = " ".join(s.split()).strip()
    return s[:10000] if s else "N/A"


def _parse_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError(f"Invalid date '{date_str}' — use YYYY-MM-DD")


# ════════════════════════════════════════════════════════════════════════════════
# AUDIT SUMMARY
# ════════════════════════════════════════════════════════════════════════════════
def _print_audit_summary(audit: list):
    log.info("")
    log.info("=" * 70)
    log.info("REPROCESS AUDIT SUMMARY")
    log.info("=" * 70)
    log.info(f"{'Date':<12} {'Action':<12} {'Old miss':<10} {'New miss':<10} {'Reason'}")
    log.info("-" * 70)

    replaced = skipped = inserted = dry = 0
    total_old_miss = 0
    total_new_miss = 0

    for r in audit:
        action = r["action"]
        log.info(
            f"{r['date']:<12} {action:<12} "
            f"{r['existing_miss']:<10} {r['new_miss']:<10} "
            f"{r['reason']}"
        )
        total_old_miss += r["existing_miss"]
        total_new_miss += r["new_miss"] if action in ("REPLACE", "INSERT") else r["existing_miss"]

        if action == "REPLACE":   replaced += 1
        elif action == "INSERT":  inserted += 1
        elif action == "SKIPPED": skipped += 1
        elif action.startswith("DRY_"): dry += 1

    log.info("=" * 70)
    log.info(f"Days replaced  : {replaced}")
    log.info(f"Days inserted  : {inserted}")
    log.info(f"Days skipped   : {skipped}")
    log.info(f"Days dry-run   : {dry}")
    log.info(f"Total session miss improvement: {total_old_miss - total_new_miss} fewer misses")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
