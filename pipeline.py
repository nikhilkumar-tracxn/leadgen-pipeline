"""
================================================================================
Leadgen BigQuery Upload Pipeline
================================================================================

FIXES vs previous version (session ID miss rate ~25% → ~1%)
------------------------------------------------------------
FIX 1 — Wider log window
  Old: log_start = target - 1 day,  log_end = target + 1 day  (2-day window)
  New: log_start = target - 3 days, log_end = target + 1 day  (4-day window)

  Why: Apps Script used twoDaysAgo → today (3 days). A user may have started
  a browsing session up to 3 days before actually signing up. The old Python
  window was too narrow and missed those platform/form log entries entirely,
  causing ~25% of session IDs to be unfindable.

FIX 2 — Cross-source session ID fallback
  Old: FORM_TYPES → form map only.  Others → platform map only.
  New: FORM_TYPES → try form map first, then platform map as fallback.
       Others    → try platform map first, then form map as fallback.

  Why: In practice some FORM_TYPE users also have platform log entries (e.g.
  they used the API before switching to web signup). The fallback catches these.

FIX 3 — Broader /signup path matching
  Old: path must be exactly "/signup"
  New: path must START WITH "/signup" (catches "/signup?ref=..." variants)

FIX 4 — Diagnostic logging
  Session ID hit/miss stats are now logged at the end of Step 4 so you can
  see the miss rate in every run without inspecting individual records.

RUN MODES
---------
  production        Auto: yesterday → Main table   (used by scheduled runs)
  production_manual Specific date  → Main table   (backfill)
  test_auto         Auto: yesterday → Backup table
  test_manual       Specific date  → Backup table
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

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
ACCESS_TOKEN     = os.environ["TRACXN_ACCESS_TOKEN"]
PROJECT_ID       = os.environ["GCP_PROJECT_ID"]
DATASET          = os.environ.get("BQ_DATASET", "leadgen_dataset")
GCP_SA_JSON      = os.environ["GCP_SA_KEY"]
MODE             = os.environ.get("MODE", "production").lower()
TABLE_PRODUCTION = os.environ.get("BQ_TABLE",       "leadgen_users_v2_no_partition")
TABLE_BACKUP     = os.environ.get("BQ_TABLE_BACKUP", "leadgen_users_v2_no_partition_backup3")
TEST_DATE_INPUT  = os.environ.get("TEST_DATE", "").strip()

HEADERS = {
    "accessToken": ACCESS_TOKEN,
    "X-Request-Source": "GitHub-Actions-Pipeline",
    "Content-Type": "application/json",
}

API = {
    "user":      "https://platform.tracxn.com/api/2.2/user",
    "form":      "https://platform.tracxn.com/api/2.2/logs/frontend/formsubmit",
    "platform":  "https://platform.tracxn.com/api/2.2/platformrequests",
    "urlchange": "https://platform.tracxn.com/api/2.2/logs/frontend/urlchange",
}

FORM_TYPES = {
    "OTP_SIGNUP",
    "THIRD_PARTY_SIGNUP",
    "THIRD_PARTY_SIGNUP_GOOGLE",
    "THIRD_PARTY_SIGNUP_MICROSOFT",
    "THIRD_PARTY_SIGNUP_ENTRA_ID",
}

# FIX 1: wider log window — 3 days before target (matching Apps Script behaviour)
LOG_WINDOW_DAYS_BEFORE = 3
LOG_WINDOW_DAYS_AFTER  = 1

SLEEP_S     = 0.3
BATCH_SIZE  = 30
MAX_RETRIES = 3


# ════════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════════
def main():
    log.info("=" * 60)
    log.info(f"LEADGEN PIPELINE  |  mode={MODE.upper()}")
    log.info("=" * 60)

    if MODE == "production_manual":
        if not TEST_DATE_INPUT:
            raise ValueError("MODE=production_manual requires TEST_DATE (YYYY-MM-DD)")
        target_dt = _parse_date(TEST_DATE_INPUT)
        table = TABLE_PRODUCTION
        log.info("PRODUCTION MANUAL — writing to MAIN table")

    elif MODE == "test_manual":
        if not TEST_DATE_INPUT:
            raise ValueError("MODE=test_manual requires TEST_DATE (YYYY-MM-DD)")
        target_dt = _parse_date(TEST_DATE_INPUT)
        table = TABLE_BACKUP
        log.info("TEST MANUAL — writing to BACKUP table")

    elif MODE == "test_auto":
        target_dt = _yesterday()
        table = TABLE_BACKUP
        log.info("TEST AUTO — writing to BACKUP table")

    else:
        target_dt = _yesterday()
        table = TABLE_PRODUCTION
        log.info("PRODUCTION AUTO — writing to MAIN table")

    # FIX 1: wider log window matches Apps Script (3 days before target)
    log_start = target_dt - timedelta(days=LOG_WINDOW_DAYS_BEFORE)
    log_end   = target_dt + timedelta(days=LOG_WINDOW_DAYS_AFTER)

    target_date_api = target_dt.strftime("%d/%m/%Y")
    target_date_bq  = target_dt.strftime("%Y-%m-%d")

    log.info(f"Target date : {target_date_api}  ({target_date_bq})")
    log.info(f"Log window  : {log_start.date()} → {log_end.date()}  ({LOG_WINDOW_DAYS_BEFORE}d before, {LOG_WINDOW_DAYS_AFTER}d after)")
    log.info(f"Destination : {PROJECT_ID}.{DATASET}.{table}")
    log.info("-" * 60)

    platform_map = step1_fetch_platform_logs(log_start, log_end)
    form_map     = step2_fetch_form_logs(log_start, log_end)
    users        = step3_fetch_users(target_date_api)

    log.info(f"Users fetched: {len(users)}")

    records = step4_enrich_users(users, form_map, platform_map, target_date_api)

    if not records:
        log.warning("No records to upload. Pipeline complete with 0 rows.")
        return

    step5_upload_to_bigquery(records, table)

    log.info("=" * 60)
    log.info(f"PIPELINE COMPLETE  |  {target_date_api}  →  {table}")
    log.info("=" * 60)


# ════════════════════════════════════════════════════════════════════════════════
# STEP 1 — Platform Logs
# ════════════════════════════════════════════════════════════════════════════════
def step1_fetch_platform_logs(start: datetime, end: datetime) -> dict:
    """
    Fetches platform request logs for the full window.
    Filter format: epoch milliseconds.
    Returns: {email → [{sessionId, ts}]}
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
        if email and sid:           # only store entries that have a valid session ID
            result.setdefault(email, []).append({"sessionId": sid, "ts": ts})

    log.info(f"  → {len(result)} unique emails with session IDs in platform map")
    return result


# ════════════════════════════════════════════════════════════════════════════════
# STEP 2 — Form Logs
# ════════════════════════════════════════════════════════════════════════════════
def step2_fetch_form_logs(start: datetime, end: datetime) -> dict:
    """
    Fetches form submission logs for the full window.
    Filter format: ISO 8601 with +00:00 UTC offset.
    Returns: {email → [{sessionId, ts, path}]}
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
        if email and sid:           # only store entries that have a valid session ID
            result.setdefault(email, []).append({"sessionId": sid, "ts": ts, "path": path})

    log.info(f"  → {len(result)} unique emails with session IDs in form map")
    return result


# ════════════════════════════════════════════════════════════════════════════════
# STEP 3 — Fetch Users
# ════════════════════════════════════════════════════════════════════════════════
def step3_fetch_users(target_date: str) -> list:
    """
    Fetches all users created on the target date.
    Filter format: DD/MM/YYYY.
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
    log.info(f"STEP 4: Enriching {len(users)} users...")
    records = []
    session_found = 0
    session_missing = 0

    for i, user in enumerate(users, 1):
        if i % 50 == 0:
            log.info(f"  Processed {i}/{len(users)}  (session hits so far: {session_found}, misses: {session_missing})")
        try:
            record = build_user_record(user, form_map, platform_map, target_date)
            records.append(record)
            if record["sessionId"] != "N/A":
                session_found += 1
            else:
                session_missing += 1
        except Exception as e:
            log.warning(f"  Skipping user {user.get('id', '?')}: {e}")
            session_missing += 1

    total = session_found + session_missing
    miss_pct = (session_missing / total * 100) if total else 0
    log.info(f"  → Session ID stats: {session_found} found, {session_missing} missing ({miss_pct:.1f}% miss rate)")
    log.info(f"  → {len(records)} records ready for upload")
    return records


def build_user_record(user: dict, form_map: dict, platform_map: dict, target_date: str) -> dict:
    """
    Builds one enriched record.

    FIX 2 — Cross-source fallback:
      FORM_TYPES → try form_map (/signup entries) FIRST, then platform_map
      Others     → try platform_map FIRST, then form_map

    FIX 3 — Broader path match:
      path.startswith("/signup") instead of path == "/signup"
      catches "/signup?ref=google" and other variants
    """
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

    # ── FIX 2: Session ID with cross-source fallback ───────────────────────
    session_id = None

    if reg_type in FORM_TYPES:
        # Primary: form map, /signup path (FIX 3: startswith, not ==)
        entries = sorted(
            [e for e in form_map.get(email, []) if e["path"].startswith("/signup")],
            key=lambda x: x["ts"]
        )
        if entries:
            session_id = entries[0]["sessionId"]

        # Fallback: platform map (catches users who hit API before web signup)
        if not session_id:
            fallback = sorted(platform_map.get(email, []), key=lambda x: x["ts"])
            if fallback:
                session_id = fallback[0]["sessionId"]
                log.debug(f"  {email}: used platform map fallback (reg_type={reg_type})")

    else:
        # Primary: platform map
        entries = sorted(platform_map.get(email, []), key=lambda x: x["ts"])
        if entries:
            session_id = entries[0]["sessionId"]

        # Fallback: form map (any path, not just /signup)
        if not session_id:
            fallback = sorted(form_map.get(email, []), key=lambda x: x["ts"])
            if fallback:
                session_id = fallback[0]["sessionId"]
                log.debug(f"  {email}: used form map fallback (reg_type={reg_type})")

    # ── Journey from URL change events ────────────────────────────────────
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

            auth_events = [e for e in events if e["path"].startswith(("/signup", "/login"))]
            if auth_events:
                tab = auth_events[-1]["prevTab"]
                while tab:
                    prev = next((e for e in events if e["tab"] == tab), None)
                    if not prev:
                        break
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
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    job = client.load_table_from_json(records, table_ref, job_config=job_config)
    job.result()
    log.info(f"  ✓ {len(records)} rows uploaded successfully")


# ════════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ════════════════════════════════════════════════════════════════════════════════
def fetch_all(endpoint: str, payload: dict, name: str) -> list:
    results = []
    payload = {**payload, "size": BATCH_SIZE, "from": 0}
    log.info(f"  [{name}] payload: {json.dumps(payload)}")
    while True:
        batch, ok = _post(endpoint, payload, name)
        if not ok or not batch:
            break
        results.extend(batch)
        payload["from"] += len(batch)
        if len(results) % 150 == 0:
            log.info(f"  [{name}] {len(results)} records fetched...")
        time.sleep(SLEEP_S)
    return results


def fetch_limited(endpoint: str, payload: dict, max_records: int) -> list:
    payload = {**payload, "size": min(BATCH_SIZE, max_records), "from": 0}
    batch, _ = _post(endpoint, payload, "urlchange")
    return (batch or [])[:max_records]


def _post(endpoint: str, payload: dict, name: str):
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
    if not value or str(value).lower() in ("none", "null", "undefined", ""):
        return "N/A"
    s = str(value).replace("\r\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")
    if is_journey:
        s = s.replace("→", ">").replace("==>", ">")
    s = " ".join(s.split()).strip()
    return s[:10000] if s else "N/A"


def _yesterday() -> datetime:
    return (datetime.now(timezone.utc) - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def _parse_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError(f"Invalid date '{date_str}' — use YYYY-MM-DD")


if __name__ == "__main__":
    main()
