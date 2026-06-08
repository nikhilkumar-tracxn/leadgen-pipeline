"""
================================================================================
Leadgen BigQuery Upload Pipeline  —  pipeline.py
================================================================================

PURPOSE
-------
Daily ingest pipeline. Runs automatically every day at 6:00 AM IST to fetch
all users who signed up the previous day from the Tracxn API, enrich them with
session ID and journey data, and append them to the main BigQuery table.

HOW IT WORKS
------------
For each run:

  Step 1 — Fetch platform request logs for the log window
           Builds: {email → [{sessionId, ts}]}

  Step 2 — Fetch form submission logs for the log window
           Builds: {email → [{sessionId, ts, path}]}

  Step 3 — Fetch all users created on the target date

  Step 4 — For each user, resolve session ID and build enriched record
           Session ID logic: see _pick_session / build_user_record below

  Step 5 — Upload enriched records to BigQuery (WRITE_APPEND)

SESSION ID LOGIC
----------------
We want the session ID that existed BEFORE the user created their account —
i.e. the browsing session that led them to sign up, not any post-signup
in-platform session.

  created_epoch: read from createdDate.epochMillis (exact ms from API).
                 Falls back to midnight UTC of that day if epochMillis absent.

  _pick_session(candidates):
    1. Filter to entries where ts < created_epoch (strictly before signup)
    2. From those, pick the one with the earliest timestamp
    3. If nothing qualifies (all entries are post-signup), fall back to the
       globally earliest entry across all candidates
    4. Return None if candidates is empty

  Registration type determines which source is checked first:

    FORM_TYPES (OTP_SIGNUP, THIRD_PARTY_SIGNUP, THIRD_PARTY_SIGNUP_GOOGLE,
                THIRD_PARTY_SIGNUP_MICROSOFT, THIRD_PARTY_SIGNUP_ENTRA_ID):
      Primary  → form map, /signup paths only (/activate excluded)
      Fallback → platform map (any path)

    All other types:
      Primary  → platform map
      Fallback → form map (any path)

  /activate is ALWAYS excluded from form map lookups for FORM_TYPES.
  /activate is a post-signup email verification step — its session ID
  belongs to in-platform activity, not the acquisition journey.

LOG WINDOW
----------
  log_start = target_date - 1 day
  log_end   = target_date + 1 day

  A ±1-day window is used to catch sessions that started just before midnight
  or log entries recorded slightly after midnight of the target date.

RUN MODES
---------
  production        Auto: yesterday → Main table   (scheduled runs)
  production_manual Specific date  → Main table   (backfill a missed date)
  test_auto         Auto: yesterday → Backup table (smoke test)
  test_manual       Specific date  → Backup table (test a historical date)

ENVIRONMENT VARIABLES
---------------------
  Required secrets:
    TRACXN_ACCESS_TOKEN
    GCP_PROJECT_ID
    GCP_SA_KEY              Full GCP service account key JSON

  Required config:
    BQ_DATASET              (defaults to leadgen_dataset)
    BQ_TABLE                Main production table name
    BQ_TABLE_BACKUP         Backup table name (test modes)

  Runtime:
    MODE                    One of the four run modes above
    TEST_DATE               YYYY-MM-DD, required for *_manual modes
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
TABLE_PRODUCTION = os.environ.get("BQ_TABLE",        "leadgen_users_v2_no_partition")
TABLE_BACKUP     = os.environ.get("BQ_TABLE_BACKUP",  "leadgen_users_v2_no_partition_backup3")
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

# Registration types that use the web signup form (checked against form map first)
FORM_TYPES = {
    "OTP_SIGNUP", "THIRD_PARTY_SIGNUP", "THIRD_PARTY_SIGNUP_GOOGLE",
    "THIRD_PARTY_SIGNUP_MICROSOFT", "THIRD_PARTY_SIGNUP_ENTRA_ID",
}

# Log window: ±1 day around the target date
LOG_WINDOW_DAYS_BEFORE = 1
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
    Fetches platform request logs for the full log window.
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
        if email and sid:
            result.setdefault(email, []).append({"sessionId": sid, "ts": ts})

    log.info(f"  → {len(result)} unique emails with session IDs in platform map")
    return result


# ════════════════════════════════════════════════════════════════════════════════
# STEP 2 — Form Logs
# ════════════════════════════════════════════════════════════════════════════════
def step2_fetch_form_logs(start: datetime, end: datetime) -> dict:
    """
    Fetches form submission logs for the full log window.
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
        if email and sid:
            result.setdefault(email, []).append({"sessionId": sid, "ts": ts, "path": path})

    log.info(f"  → {len(result)} unique emails with session IDs in form map")
    return result


# ════════════════════════════════════════════════════════════════════════════════
# STEP 3 — Fetch Users
# ════════════════════════════════════════════════════════════════════════════════
def step3_fetch_users(target_date: str) -> list:
    """
    Fetches all users created on the target date.
    Filter format: DD/MM/YYYY (Tracxn user API expects this format).
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
    records         = []
    session_found   = 0
    session_missing = 0

    for i, user in enumerate(users, 1):
        if i % 50 == 0:
            log.info(f"  Processed {i}/{len(users)}  (session hits: {session_found}, misses: {session_missing})")
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

    total    = session_found + session_missing
    miss_pct = (session_missing / total * 100) if total else 0
    log.info(f"  → Session ID stats: {session_found} found, {session_missing} missing ({miss_pct:.1f}% miss rate)")
    log.info(f"  → {len(records)} records ready for upload")
    return records


def build_user_record(user: dict, form_map: dict, platform_map: dict, target_date: str) -> dict:
    """
    Builds one enriched BigQuery record for a single user.

    Session ID resolution:
      - Use createdDate.epochMillis from the API for exact ms comparison.
        Falls back to midnight UTC of the created day if epochMillis absent.
      - _pick_session picks the earliest log entry whose ts < created_epoch.
        If nothing qualifies, falls back to the globally earliest entry.
      - FORM_TYPES: check form map (/signup paths only) first, then platform.
      - Other types: check platform map first, then form map (any path).
      - /activate paths are never used — post-signup email verification only.
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

    # ── created_epoch: exact ms timestamp of signup ───────────────────────────
    # Prefer epochMillis from the API (exact second of signup).
    # Fall back to midnight UTC of that day if not present.
    created_epoch = (
        cd.get("epochMillis")
        or int(datetime(
            int(cd.get("year") or 2025),
            int(cd.get("month") or 1),
            int(cd.get("day") or 1),
            tzinfo=timezone.utc,
        ).timestamp() * 1000)
    )

    def _pick_session(candidates: list) -> Optional[str]:
        """
        From a list of log entries [{sessionId, ts, ...}], returns the sessionId
        of the entry with the earliest ts that is strictly before created_epoch.
        If no entry qualifies (all post-signup), falls back to the globally
        earliest entry. Returns None if candidates is empty.
        """
        if not candidates:
            return None
        pre  = [e for e in candidates if e["ts"] < created_epoch]
        pool = sorted(pre if pre else candidates, key=lambda x: x["ts"])
        return pool[0]["sessionId"] if pool else None

    # ── Session ID lookup: branched by registration type ─────────────────────
    session_id = None

    if reg_type in FORM_TYPES:
        # Primary: form map, /signup paths only.
        # /activate is excluded — it is a post-signup email verification step.
        signup_entries = [
            e for e in form_map.get(email, [])
            if e["path"].startswith("/signup")
        ]
        session_id = _pick_session(signup_entries)

        # Fallback: platform map (any entry)
        if not session_id:
            session_id = _pick_session(platform_map.get(email, []))

    else:
        # Primary: platform map
        session_id = _pick_session(platform_map.get(email, []))

        # Fallback: form map (any path — not restricted to /signup)
        if not session_id:
            session_id = _pick_session(form_map.get(email, []))

    # ── Journey from URL change events ────────────────────────────────────────
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
            key=lambda x: x["ts"],
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
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
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
    """Paginates through all results from an endpoint using size+from."""
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
    """Fetches one page of up to max_records results (no pagination)."""
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
        hour=0, minute=0, second=0, microsecond=0,
    )


def _parse_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError(f"Invalid date '{date_str}' — use YYYY-MM-DD")


if __name__ == "__main__":
    main()
