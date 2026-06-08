"""
================================================================================
Manual Reprocess Pipeline  —  reprocess_range.py (Enriched Audit & Strict Pre-Signup)
================================================================================
"""

import os
import json
import time
import logging
import uuid
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
ACCESS_TOKEN = os.environ["TRACXN_ACCESS_TOKEN"]
PROJECT_ID   = os.environ["GCP_PROJECT_ID"]
DATASET      = os.environ.get("BQ_DATASET", "leadgen_dataset")
GCP_SA_JSON  = os.environ["GCP_SA_KEY"]
TABLE        = os.environ.get("BQ_TABLE", "leadgen_users_v2_no_partition")

REPROCESS_START = os.environ.get("REPROCESS_START", "2026-05-30").strip()
REPROCESS_END   = os.environ.get("REPROCESS_END",   "2026-05-30").strip()
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

# ── Target Audit Matrix ───────────────────────────────────────────────────────
WATCH_EMAILS = {
    "connect@amplior.com",
    "sachin@1729capital.com",
    "rakshit.gairola@gopeoplematters.com",
    "anurag@protectron.in",
    "kona.sripooja@bba.christuniversity.in",
    "m.sy@sylamtechgroup.com",
    "vishal@prudenttec.com",
    "lu@akool.com"
}

LOG_WINDOW_DAYS_BEFORE = 1
LOG_WINDOW_DAYS_AFTER  = 1
SLEEP_S     = 0.3
BATCH_SIZE  = 30
MAX_RETRIES = 3

# ── Timezone Setup (IST Native) ───────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))


def main():
    if not REPROCESS_START or not REPROCESS_END:
        raise ValueError("REPROCESS_START and REPROCESS_END must be set (YYYY-MM-DD)")

    start_dt = _parse_date(REPROCESS_START)
    end_dt   = _parse_date(REPROCESS_END)

    if start_dt > end_dt:
        raise ValueError(f"REPROCESS_START ({REPROCESS_START}) must be <= REPROCESS_END ({REPROCESS_END})")

    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    log.info("=" * 70)
    log.info("MANUAL REPROCESS PIPELINE  —  STRICT PRE-SIGNUP & TRACE ENABLED")
    log.info("=" * 70)
    log.info(f"Date range  : {REPROCESS_START} → {REPROCESS_END}")
    log.info(f"Table       : {table_ref}")
    log.info(f"Dry run     : {DRY_RUN}")
    log.info(f"Timezone    : IST (+05:30)")
    log.info("=" * 70)

    creds = service_account.Credentials.from_service_account_info(
        json.loads(GCP_SA_JSON),
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    bq = bigquery.Client(project=PROJECT_ID, credentials=creds)

    audit = []
    current = start_dt
    day_num = 0
    total_days = (end_dt - start_dt).days + 1

    while current <= end_dt:
        day_num += 1
        date_str     = current.strftime("%Y-%m-%d")
        date_api_str = current.strftime("%d/%m/%Y")

        log.info("")
        log.info(f"{'─' * 70}")
        log.info(f"DAY {day_num}/{total_days}  —  {date_str}")
        log.info(f"{'─' * 70}")

        outcome = _process_one_day(bq, table_ref, current, date_str, date_api_str)
        audit.append(outcome)
        current += timedelta(days=1)

    _print_audit_summary(audit)


def _process_one_day(bq, table_ref, target_dt, date_str, date_api_str) -> dict:
    result = {
        "date": date_str, "existing_total": 0, "existing_miss": 0,
        "new_total": 0, "new_miss": 0, "action": "SKIPPED", "reason": ""
    }

    log.info(f"  A. Checking existing BigQuery data for {date_str}...")
    existing_stats = _query_existing_miss_rate(bq, table_ref, date_str)
    result["existing_total"] = existing_stats["total"]
    result["existing_miss"]  = existing_stats["missing"]

    log.info(f"  B. Fetching data arrays from Tracxn endpoints...")
    log_start = target_dt - timedelta(days=LOG_WINDOW_DAYS_BEFORE)
    log_end   = target_dt + timedelta(days=LOG_WINDOW_DAYS_AFTER)

    platform_map = _fetch_platform_logs(log_start, log_end)
    form_map     = _fetch_form_logs(log_start, log_end)
    users        = _fetch_users(date_api_str)

    if not users:
        result["reason"] = "No user records returned via API"
        return result

    log.info(f"  C. Executing pipeline calculations and matching passes...")
    new_records, new_miss = _enrich_users(users, form_map, platform_map, date_api_str)
    
    result["new_total"] = len(new_records)
    result["new_miss"]  = new_miss

    if existing_stats["total"] == 0:
        decision = "INSERT"
        reason   = "Target segment is empty in destination table"
    else:
        decision = "REPLACE"
        reason   = f"Replacing rows to force updated calculation logic"

    result["action"] = f"DRY_{decision}" if DRY_RUN else decision
    result["reason"] = reason

    if not DRY_RUN:
        if decision == "REPLACE":
            _atomic_replace_day(bq, table_ref, new_records, date_str)
        else:
            _insert_day(bq, table_ref, new_records)
            
    return result


def _query_existing_miss_rate(bq, table_ref: str, date_str: str) -> dict:
    query = f"""
        SELECT COUNT(*) AS total, COUNTIF(sessionId = 'N/A' OR sessionId IS NULL) AS missing
        FROM `{table_ref}` WHERE createdDate = DATE('{date_str}')
    """
    row = list(bq.query(query).result())[0]
    return {"total": row.total or 0, "missing": row.missing or 0, "miss_pct": (row.missing / row.total * 100) if row.total else 0.0}


def _atomic_replace_day(bq, table_ref: str, records: list, date_str: str):
    temp_id = f"{PROJECT_ID}.{DATASET}.temp_reprocess_{uuid.uuid4().hex[:8]}"
    schema = [bigquery.SchemaField(f, "STRING" if f != "createdDate" else "DATE") for f in ["createdDate", "id", "email", "userCategory", "originUrl", "triggerUrl", "geography", "registrationType", "sessionId", "userJourney", "cta"]]
    
    try:
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE", source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
        bq.load_table_from_json(records, temp_id, job_config=job_config).result()

        sql = f"""
            CREATE OR REPLACE TABLE `{table_ref}` CLUSTER BY registrationType, geography AS
            SELECT * FROM `{table_ref}` WHERE createdDate != DATE('{date_str}')
            UNION ALL
            SELECT * FROM `{temp_id}`
        """
        bq.query(sql).result()
    finally:
        bq.delete_table(temp_id, not_found_ok=True)


def _insert_day(bq, table_ref: str, records: list):
    schema = [bigquery.SchemaField(f, "STRING" if f != "createdDate" else "DATE") for f in ["createdDate", "id", "email", "userCategory", "originUrl", "triggerUrl", "geography", "registrationType", "sessionId", "userJourney", "cta"]]
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND", source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
    bq.load_table_from_json(records, table_ref, job_config=job_config).result()


def _fetch_platform_logs(start: datetime, end: datetime) -> dict:
    payload = {"filter": {"createdDate": {"min": int(start.timestamp() * 1000), "max": int(end.replace(hour=23, minute=59, second=59).timestamp() * 1000)}}}
    records = _fetch_all(API["platform"], payload, "platform")
    result = {}
    for r in records:
        email = (r.get("requestor", {}).get("userEmail") or "").lower().strip()
        sid   = r.get("requestor", {}).get("sessionId") or ""
        ts    = r.get("createdDate", {}).get("epochMillis", 0)
        if email and sid:
            result.setdefault(email, []).append({"sessionId": sid, "ts": ts})
    return result


def _fetch_form_logs(start: datetime, end: datetime) -> dict:
    fmt = lambda dt, eod: (dt.replace(hour=23, minute=59, second=59) if eod else dt.replace(hour=0, minute=0, second=0)).strftime("%Y-%m-%dT%H:%M:%S+05:30")
    payload = {"filter": {"createdDate": {"min": fmt(start, False), "max": fmt(end, True)}}}
    records = _fetch_all(API["form"], payload, "form")
    result = {}
    for r in records:
        email = (r.get("metrics", {}).get("customData", {}).get("userName") or "").lower().strip()
        sid   = r.get("sessionId") or ""
        ts    = r.get("createdDate", {}).get("epochMillis", 0)
        path  = r.get("metrics", {}).get("page", {}).get("parsedUrl", {}).get("pathname") or ""
        if email and sid:
            result.setdefault(email, []).append({"sessionId": sid, "ts": ts, "path": path})
    return result


def _fetch_users(target_date: str) -> list:
    return _fetch_all(API["user"], {"filter": {"createdDate": {"min": target_date, "max": target_date}}}, "users")


def _enrich_users(users: list, form_map: dict, platform_map: dict, target_date: str):
    records = []
    miss_count = 0
    for user in users:
        record = _build_record(user, form_map, platform_map, target_date)
        records.append(record)
        if record["sessionId"] == "N/A":
            miss_count += 1
    return records, miss_count


def _build_record(user: dict, form_map: dict, platform_map: dict, target_date: str) -> dict:
    email    = (user.get("email") or "").lower().strip()
    reg_type = user.get("registrationType") or ""
    is_target_audit = email in WATCH_EMAILS

    cats = [c.get("userCategory") for c in (user.get("categoryList") or []) if c.get("userCategory")]
    user_category = ", ".join(cats) if cats else (user.get("userCategory") or "N/A")

    cd = user.get("createdDate") or {}
    created_date = f"{int(cd.get('year') or 2026)}-{int(cd.get('month') or 1):02d}-{int(cd.get('day') or 1):02d}"

    # Precise IST Evaluation mapping
    created_epoch = int(datetime(
        int(cd.get("year") or 2026), int(cd.get("month") or 5), int(cd.get("day") or 30),
        int(cd.get("hours") or 0), int(cd.get("minutes") or 0), int(cd.get("seconds") or 0),
        tzinfo=IST
    ).timestamp() * 1000)

    if is_target_audit:
        print(f"\n⚡ [TRACE AUDIT] Active processing verification for target: {email}")
        print(f" ├─ Registration Type : {reg_type}")
        print(f" └─ Calculated Sign-up: {created_date} {cd.get('hours')}:{cd.get('minutes')}:{cd.get('seconds')} IST (Epoch: {created_epoch})")

    def _pick_session(candidates: list, pool_label: str) -> Optional[str]:
        if not candidates:
            if is_target_audit: print(f" ├─ Pool [{pool_label}]: No raw tracking hits found.")
            return None
        
        # STRICT RULE: Only keep log entries strictly BEFORE the exact signup ms
        pre = [e for e in candidates if e["ts"] < created_epoch]
        
        # If no entries happened before signup, we discard the entire pool
        if not pre:
            if is_target_audit: print(f" ├─ Pool [{pool_label}]: All hits occurred AFTER signup. Discarding.")
            return None

        # Sort the valid pre-signup entries and pick the earliest one
        pool = sorted(pre, key=lambda x: x["ts"])
        selected = pool[0]["sessionId"]
        
        if is_target_audit:
            print(f" ├─ Pool [{pool_label}]: Matches evaluated. Total hits={len(candidates)}, Valid Pre-signup={len(pre)}.")
            print(f" │  └─ Resolved Session ID: {selected}")
        return selected

    session_id = None

    if reg_type in FORM_TYPES:
        signup_entries = [e for e in form_map.get(email, []) if e["path"].startswith("/signup")]
        if is_target_audit: print(f" ├─ Path Selection Rules: Form Log Priority initiated.")
        session_id = _pick_session(signup_entries, "Form Logs (/signup)")
        if not session_id:
            session_id = _pick_session(platform_map.get(email, []), "Platform Logs (Secondary)")
    else:
        if is_target_audit: print(f" ├─ Path Selection Rules: Platform Log Priority initiated.")
        session_id = _pick_session(platform_map.get(email, []), "Platform Logs")
        if not session_id:
            session_id = _pick_session(form_map.get(email, []), "Form Logs (Secondary)")

    if is_target_audit:
        print(f" └─ Final Resolution Output Strategy -> sessionId: '{session_id or 'N/A'}'")

    origin = trigger = journey = "N/A"
    if session_id:
        url_logs = _fetch_limited(API["urlchange"], {"filter": {"sessionId": session_id}}, 50)
        events = sorted([{"ts": e.get("createdDate", {}).get("epochMillis", 0), "url": e.get("metrics", {}).get("page", {}).get("url") or "", "path": e.get("metrics", {}).get("page", {}).get("parsedUrl", {}).get("pathname") or "", "tab": e.get("tabId"), "prevTab": e.get("previousTabId")} for e in url_logs if e.get("metrics", {}).get("page", {}).get("url")], key=lambda x: x["ts"])

        if events:
            origin = events[0]["url"]
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
                if trigger == "N/A": trigger = origin
            else:
                trigger = origin

    return {
        "createdDate": created_date, "id": str(user.get("id") or ""), "email": email,
        "userCategory": _clean(user_category), "originUrl": _clean(origin), "triggerUrl": _clean(trigger),
        "geography": _clean(user.get("primaryGeography") or "N/A"), "registrationType": _clean(reg_type),
        "sessionId": _clean(session_id or "N/A"), "userJourney": _clean(journey, is_journey=True),
        "cta": f"Auto_{target_date}"
    }


def _fetch_all(endpoint: str, payload: dict, name: str) -> list:
    results = []
    payload = {**payload, "size": BATCH_SIZE, "from": 0}
    while True:
        batch, ok = _post(endpoint, payload, name)
        if not ok or not batch: break
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
            if resp.status_code == 200:
                return resp.json().get("result") or [], True
            if attempt < MAX_RETRIES: time.sleep(3 * attempt)
        except requests.RequestException:
            if attempt < MAX_RETRIES: time.sleep(5 * attempt)
    return [], False


def _clean(value: Optional[str], is_journey: bool = False) -> str:
    if not value or str(value).lower() in ("none", "null", "undefined", ""): return "N/A"
    s = " ".join(str(value).replace("\n", " ").replace("\t", " ").split()).strip()
    if is_journey: s = s.replace("→", ">")
    return s[:10000] if s else "N/A"


def _parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=IST)


def _print_audit_summary(audit: list):
    log.info("")
    log.info("=" * 70)
    log.info("REPROCESS AUDIT SUMMARY")
    log.info("=" * 70)
    log.info(f"{'Date':<12} {'Action':<12} {'Old Miss':<10} {'New Miss':<10}")
    log.info("-" * 70)
    for r in audit:
        log.info(f"{r['date']:<12} {r['action']:<12} {r['existing_miss']:<10} {r['new_miss']:<10}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
