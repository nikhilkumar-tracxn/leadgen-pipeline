"""
Leadgen BigQuery Upload Pipeline

Supports three run modes controlled by environment variables:

  MODE=production   (default)
    - Target date  = yesterday (auto)
    - Table        = BQ_TABLE env var (main table)

  MODE=test_auto
    - Target date  = yesterday (auto)
    - Table        = BQ_TABLE_BACKUP env var (backup table)

  MODE=test_manual
    - Target date  = TEST_DATE env var  e.g. "2026-06-01"  (YYYY-MM-DD)
    - Table        = BQ_TABLE_BACKUP env var (backup table)

In all modes, secrets come from environment variables (GitHub Secrets).
Zero dependency on Google Sheets.
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

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Secrets / config from environment ──────────────────────────────────────
ACCESS_TOKEN = os.environ["TRACXN_ACCESS_TOKEN"]
PROJECT_ID   = os.environ["GCP_PROJECT_ID"]
DATASET      = os.environ.get("BQ_DATASET", "leadgen_dataset")
GCP_SA_JSON  = os.environ["GCP_SA_KEY"]

# Run-mode config
MODE              = os.environ.get("MODE", "production").lower()
TABLE_PRODUCTION  = os.environ.get("BQ_TABLE",        "leadgen_users_v2_no_partition")
TABLE_BACKUP      = os.environ.get("BQ_TABLE_BACKUP",  "leadgen_users_v2_no_partition_backup3")
TEST_DATE_INPUT   = os.environ.get("TEST_DATE", "")    # YYYY-MM-DD, only used in test_manual

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
    "OTP_SIGNUP", "THIRD_PARTY_SIGNUP", "THIRD_PARTY_SIGNUP_GOOGLE",
    "THIRD_PARTY_SIGNUP_MICROSOFT", "THIRD_PARTY_SIGNUP_ENTRA_ID",
}

SLEEP_S    = 0.3
BATCH_SIZE = 30
MAX_RETRIES = 3


# ════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════
def main():
    log.info("=" * 60)
    log.info(f"LEADGEN PIPELINE  |  mode={MODE.upper()}")
    log.info("=" * 60)

    # ── Resolve target date and table based on mode ──────────────────────
    if MODE == "test_manual":
        if not TEST_DATE_INPUT:
            raise ValueError("MODE=test_manual requires TEST_DATE env var (YYYY-MM-DD)")
        target_dt = datetime.strptime(TEST_DATE_INPUT, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        table = TABLE_BACKUP
        log.info(f"TEST MODE (manual date) — writing to BACKUP table")

    elif MODE == "test_auto":
        target_dt = (datetime.now(timezone.utc) - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0)
        table = TABLE_BACKUP
        log.info(f"TEST MODE (yesterday auto) — writing to BACKUP table")

    else:  # production
        target_dt = (datetime.now(timezone.utc) - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0)
        table = TABLE_PRODUCTION
        log.info(f"PRODUCTION MODE — writing to MAIN table")

    # Log window: day-before-target → day-after-target (covers timezone edge cases)
    log_start = target_dt - timedelta(days=1)
    log_end   = target_dt + timedelta(days=1)

    target_date_api = target_dt.strftime("%d/%m/%Y")   # DD/MM/YYYY  — for Tracxn User API
    target_date_bq  = target_dt.strftime("%Y-%m-%d")   # YYYY-MM-DD  — for BigQuery

    log.info(f"Target date : {target_date_api}  ({target_date_bq})")
    log.info(f"Log window  : {log_start.date()} → {log_end.date()}")
    log.info(f"Destination : {PROJECT_ID}.{DATASET}.{table}")
    log.info("-" * 60)

    # ── Run pipeline ──────────────────────────────────────────────────────
    platform_map = step1_fetch_platform_logs(log_start, log_end)
    form_map     = step2_fetch_form_logs(log_start, log_end)
    users        = step3_fetch_users(target_date_api)

    log.info(f"Fetched {len(users)} users")

    records = step4_enrich_users(users, form_map, platform_map, target_date_api)
    log.info(f"Enriched {len(records)} records")

    step5_upload_to_bigquery(records, table)

    log.info("=" * 60)
    log.info(f"PIPELINE COMPLETE  |  {target_date_api}  →  {table}")
    log.info("=" * 60)


# ════════════════════════════════════════════════════════════════════════════
# STEP 1 — Platform Logs
# ════════════════════════════════════════════════════════════════════════════
def step1_fetch_platform_logs(start: datetime, end: datetime) -> dict:
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
    return result


# ════════════════════════════════════════════════════════════════════════════
# STEP 2 — Form Logs
# ════════════════════════════════════════════════════════════════════════════
def step2_fetch_form_logs(start: datetime, end: datetime) -> dict:
    log.info("STEP 2: Fetching form logs...")

    def fmt(dt: datetime, end_of_day: bool) -> str:
        d = dt.replace(hour=23, minute=59, second=59) if end_of_day else dt.replace(hour=0, minute=0, second=0)
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
    return result


# ════════════════════════════════════════════════════════════════════════════
# STEP 3 — Fetch Users
# ════════════════════════════════════════════════════════════════════════════
def step3_fetch_users(target_date: str) -> list:
    log.info(f"STEP 3: Fetching users for {target_date}...")
    payload = {"filter": {"createdDate": {"min": target_date, "max": target_date}}}
    users = fetch_all(API["user"], payload, "users")
    log.info(f"  → {len(users)} users fetched")
    return users


# ════════════════════════════════════════════════════════════════════════════
# STEP 4 — Enrich Users
# ════════════════════════════════════════════════════════════════════════════
def step4_enrich_users(users, form_map, platform_map, target_date) -> list:
    log.info(f"STEP 4: Enriching {len(users)} users...")
    records = []
    for i, user in enumerate(users, 1):
        if i % 50 == 0:
            log.info(f"  Processed {i}/{len(users)}...")
        records.append(build_user_record(user, form_map, platform_map, target_date))
    log.info(f"  → {len(records)} records ready")
    return records


def build_user_record(user, form_map, platform_map, target_date) -> dict:
    email    = (user.get("email") or "").lower()
    reg_type = user.get("registrationType") or ""

    cats = [c.get("userCategory") for c in (user.get("categoryList") or []) if c.get("userCategory")]
    user_category = ", ".join(cats) if cats else (user.get("userCategory") or "N/A")

    cd = user.get("createdDate") or {}
    created_date = "{}-{:02d}-{:02d}".format(cd.get("year", 2025), cd.get("month", 1), cd.get("day", 1))

    session_id = None
    if reg_type in FORM_TYPES:
        entries = sorted([e for e in form_map.get(email, []) if e["path"] == "/signup"], key=lambda x: x["ts"])
        if entries:
            session_id = entries[0]["sessionId"]
    else:
        entries = sorted(platform_map.get(email, []), key=lambda x: x["ts"])
        if entries:
            session_id = entries[0]["sessionId"]

    origin = trigger = journey = "N/A"
    if session_id:
        url_logs = fetch_limited(API["urlchange"], {"filter": {"sessionId": session_id}}, 50)
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
            auth_events = [e for e in events if e["path"] in ("/signup", "/login")]
            if auth_events:
                tab = auth_events[-1]["prevTab"]
                while tab:
                    prev = next((e for e in events if e["tab"] == tab), None)
                    if not prev: break
                    if prev["path"] not in ("/signup", "/login"):
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


# ════════════════════════════════════════════════════════════════════════════
# STEP 5 — Upload to BigQuery
# ════════════════════════════════════════════════════════════════════════════
def step5_upload_to_bigquery(records: list, table: str):
    log.info(f"STEP 5: Uploading {len(records)} rows → {PROJECT_ID}.{DATASET}.{table}")

    creds  = service_account.Credentials.from_service_account_info(
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


# ════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ════════════════════════════════════════════════════════════════════════════
def fetch_all(endpoint, payload, name) -> list:
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
            log.info(f"  [{name}] {len(results)} records so far...")
        time.sleep(SLEEP_S)
    return results


def fetch_limited(endpoint, payload, max_records) -> list:
    payload = {**payload, "size": min(BATCH_SIZE, max_records), "from": 0}
    batch, _ = _post(endpoint, payload, "urlchange")
    return (batch or [])[:max_records]


def _post(endpoint, payload, name):
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


def clean(value: Optional[str], is_journey: bool = False) -> str:
    if not value or str(value).lower() in ("none", "null", "undefined", ""):
        return "N/A"
    s = str(value).replace("\r\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")
    if is_journey:
        s = s.replace("→", ">").replace("==>", ">")
    s = " ".join(s.split()).strip()
    return s[:10000] if s else "N/A"


if __name__ == "__main__":
    main()
