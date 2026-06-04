"""
Leadgen BigQuery Upload Pipeline
Runs all 5 steps in sequence. No checkpointing needed — GitHub Actions
gives us a 6-hour window, so the full ~20 min job runs in one shot.
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

# ── Config (all sensitive values come from GitHub Secrets → env vars) ───────
ACCESS_TOKEN  = os.environ["TRACXN_ACCESS_TOKEN"]
PROJECT_ID    = os.environ["GCP_PROJECT_ID"]
DATASET       = os.environ.get("BQ_DATASET", "leadgen_dataset")
TABLE         = os.environ.get("BQ_TABLE",   "leadgen_users_v2_no_partition")
GCP_SA_JSON   = os.environ["GCP_SA_KEY"]       # full service-account JSON string

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

SLEEP_S       = 0.3   # between API pages
BATCH_SIZE    = 30    # records per API page
MAX_RETRIES   = 3


# ════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════
def main():
    log.info("=" * 60)
    log.info("LEADGEN PIPELINE STARTED")
    log.info("=" * 60)

    # Date setup: target = yesterday, log window = day-before-yesterday → today
    today         = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday     = today - timedelta(days=1)
    two_days_ago  = today - timedelta(days=2)

    target_date   = yesterday.strftime("%d/%m/%Y")         # for User API  e.g. 03/06/2026
    bq_date       = yesterday.strftime("%Y-%m-%d")          # for BigQuery  e.g. 2026-06-03

    log.info(f"Target date : {target_date}")
    log.info(f"Log window  : {two_days_ago.date()} → {today.date()}")

    # Step 1 & 2: fetch logs
    platform_map = step1_fetch_platform_logs(two_days_ago, today)
    form_map     = step2_fetch_form_logs(two_days_ago, today)

    # Step 3: fetch users
    users = step3_fetch_users(target_date)
    log.info(f"Fetched {len(users)} users")

    # Step 4: enrich users
    records = step4_enrich_users(users, form_map, platform_map, target_date)
    log.info(f"Enriched {len(records)} records")

    # Step 5: upload
    step5_upload_to_bigquery(records, bq_date)

    log.info("=" * 60)
    log.info(f"PIPELINE COMPLETE for {target_date}")
    log.info("=" * 60)


# ════════════════════════════════════════════════════════════════════════════
# STEP 1 — Platform Logs
# ════════════════════════════════════════════════════════════════════════════
def step1_fetch_platform_logs(start: datetime, end: datetime) -> dict:
    """Returns {email: [{"sessionId": ..., "ts": ...}]}"""
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
        if not email:
            continue
        result.setdefault(email, []).append({"sessionId": sid, "ts": ts})

    return result


# ════════════════════════════════════════════════════════════════════════════
# STEP 2 — Form Logs
# ════════════════════════════════════════════════════════════════════════════
def step2_fetch_form_logs(start: datetime, end: datetime) -> dict:
    """Returns {email: [{"sessionId": ..., "ts": ..., "path": ...}]}"""
    log.info("STEP 2: Fetching form logs...")

    def fmt(dt: datetime, end_of_day: bool) -> str:
        d = dt.replace(hour=23, minute=59, second=59) if end_of_day else dt.replace(hour=0, minute=0, second=0)
        return d.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    payload = {
        "filter": {
            "createdDate": {
                "min": fmt(start, False),
                "max": fmt(end, True),
            }
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
        if not email:
            continue
        result.setdefault(email, []).append({"sessionId": sid, "ts": ts, "path": path})

    return result


# ════════════════════════════════════════════════════════════════════════════
# STEP 3 — Fetch Users
# ════════════════════════════════════════════════════════════════════════════
def step3_fetch_users(target_date: str) -> list:
    """target_date: DD/MM/YYYY"""
    log.info(f"STEP 3: Fetching users for {target_date}...")

    payload = {
        "filter": {
            "createdDate": {"min": target_date, "max": target_date}
        }
    }

    users = fetch_all(API["user"], payload, "users")
    log.info(f"  → {len(users)} users fetched")
    return users


# ════════════════════════════════════════════════════════════════════════════
# STEP 4 — Enrich Users
# ════════════════════════════════════════════════════════════════════════════
def step4_enrich_users(users: list, form_map: dict, platform_map: dict, target_date: str) -> list:
    log.info(f"STEP 4: Enriching {len(users)} users...")
    records = []

    for i, user in enumerate(users, 1):
        if i % 50 == 0:
            log.info(f"  Processed {i}/{len(users)}...")

        record = build_user_record(user, form_map, platform_map, target_date)
        records.append(record)

    log.info(f"  → {len(records)} records ready for upload")
    return records


def build_user_record(user: dict, form_map: dict, platform_map: dict, target_date: str) -> dict:
    email    = (user.get("email") or "").lower()
    reg_type = user.get("registrationType") or ""

    # Category
    category_list = user.get("categoryList") or []
    cats = [c.get("userCategory") for c in category_list if c.get("userCategory")]
    user_category = ", ".join(cats) if cats else (user.get("userCategory") or "N/A")

    # Created date → YYYY-MM-DD
    cd = user.get("createdDate") or {}
    created_date = "{}-{:02d}-{:02d}".format(
        cd.get("year", 2025), cd.get("month", 1), cd.get("day", 1)
    )

    # Session ID
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

    # Journey
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
                    trigger = origin
            else:
                trigger = origin

    return {
        "createdDate":    created_date,
        "id":             str(user.get("id") or ""),
        "email":          email,
        "userCategory":   clean(user_category),
        "originUrl":      clean(origin),
        "triggerUrl":     clean(trigger),
        "geography":      clean(user.get("primaryGeography") or "N/A"),
        "registrationType": clean(reg_type),
        "sessionId":      clean(session_id or "N/A"),
        "userJourney":    clean(journey, is_journey=True),
        "cta":            f"Auto_{target_date}",
    }


# ════════════════════════════════════════════════════════════════════════════
# STEP 5 — Upload to BigQuery
# ════════════════════════════════════════════════════════════════════════════
def step5_upload_to_bigquery(records: list, bq_date: str):
    log.info(f"STEP 5: Uploading {len(records)} rows to BigQuery ({TABLE})...")

    sa_info = json.loads(GCP_SA_JSON)
    creds   = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)

    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

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

    # Use newline-delimited JSON — much safer than CSV (no escaping headaches)
    rows_json = "\n".join(json.dumps(r) for r in records)

    job = client.load_table_from_json(
        [json.loads(r) for r in rows_json.splitlines()],
        table_ref,
        job_config=job_config,
    )
    job.result()  # Waits for completion, raises on error

    log.info(f"  ✓ {len(records)} rows uploaded successfully")


# ════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ════════════════════════════════════════════════════════════════════════════
def fetch_all(endpoint: str, payload: dict, name: str) -> list:
    """Paginate through all records from a Tracxn API endpoint."""
    results = []
    payload = {**payload, "size": BATCH_SIZE, "from": 0}

    log.info(f"  [{name}] Starting fetch — payload: {json.dumps(payload)}")

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


def fetch_limited(endpoint: str, payload: dict, max_records: int) -> list:
    """Fetch up to max_records — used for per-user URL changes."""
    payload = {**payload, "size": min(BATCH_SIZE, max_records), "from": 0}
    batch, _ = _post(endpoint, payload, "urlchange")
    return (batch or [])[:max_records]


def _post(endpoint: str, payload: dict, name: str):
    """Single POST with retry. Returns (records_list, success_bool)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(endpoint, headers=HEADERS, json=payload, timeout=30)

            if resp.status_code != 200:
                log.warning(f"  [{name}] HTTP {resp.status_code} (attempt {attempt}): {resp.text[:200]}")
                if attempt < MAX_RETRIES:
                    time.sleep(3 * attempt)
                    continue
                return [], False

            data = resp.json()
            return data.get("result") or [], True

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
