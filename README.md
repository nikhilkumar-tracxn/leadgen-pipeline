# Leadgen BigQuery Pipeline

Three pipelines that keep the Tracxn user dataset in BigQuery accurate and up-to-date.

| Pipeline | File | Schedule | What it does |
|---|---|---|---|
| **Daily Ingest** | `pipeline.py` | 6:00 AM IST daily | Fetches new signups from yesterday, enriches with session/journey data, loads to BigQuery |
| **Category Sync** | `sync_categories.py` | 7th/14th/21st/28th + 1st monthly | Re-fetches latest user categories from Tracxn API and updates them in BigQuery |
| **Reprocess Range** | `reprocess_range.py` | Manual only | Reprocesses a historical date range day-by-day; always replaces with freshly enriched data |

---

## Table of Contents

- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Pipeline 1 — Daily Ingest](#pipeline-1--daily-ingest)
- [Pipeline 2 — Category Sync](#pipeline-2--category-sync)
- [Pipeline 3 — Reprocess Range](#pipeline-3--reprocess-range)
- [Session ID Resolution Logic — Deep Dive](#session-id-resolution-logic--deep-dive)
- [Run Modes Reference](#run-modes-reference)
- [How to Trigger a Manual Run](#how-to-trigger-a-manual-run)
- [Initial Setup](#initial-setup)
- [BigQuery Schema](#bigquery-schema)
- [API Reference](#api-reference)
- [Configuration Reference](#configuration-reference)
- [Troubleshooting](#troubleshooting)
- [Design Decisions](#design-decisions)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          GitHub Actions                              │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Daily Ingest  (pipeline.py)          6:00 AM IST daily     │    │
│  │                                                             │    │
│  │  Tracxn API /platformrequests  ──────────────────────────►  │    │
│  │  Tracxn API /formsubmit        ──────────────────────────►  │    │──► BigQuery
│  │  Tracxn API /user              ──────────────────────────►  │    │    WRITE_APPEND
│  │  Tracxn API /urlchange (per user) ───────────────────────►  │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Category Sync  (sync_categories.py)  7th/14th/21st/28th   │    │
│  │                                       + 1st of month        │    │
│  │  BigQuery (read range)  ─────────────────────────────────►  │    │
│  │  Tracxn API /user (by ID list)  ─────────────────────────►  │    │──► BigQuery
│  │  Write to temp table, atomic swap  ──────────────────────►  │    │    atomic swap
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Reprocess Range  (reprocess_range.py)  Manual only         │    │
│  │                                                             │    │
│  │  For each day in range:                                     │    │
│  │    Query existing miss count from BigQuery  ─────────────►  │    │
│  │    Re-fetch and re-enrich from Tracxn API  ──────────────►  │    │──► BigQuery
│  │    Always replace with fresh data  ──────────────────────►  │    │    per-day swap
│  └─────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
```

**No Google Sheets.** All intermediate data lives in Python dicts/lists in memory.

---

## Repository Structure

```
leadgen-pipeline/
│
├── pipeline.py              Daily ingest pipeline
├── sync_categories.py       Category sync pipeline
├── reprocess_range.py       Manual historical reprocess tool
├── requirements.txt         Python dependencies (shared by all scripts)
├── README.md                This file
│
└── .github/
    └── workflows/
        ├── pipeline.yml           Daily ingest workflow
        ├── sync_categories.yml    Category sync workflow
        └── reprocess_range.yml    Reprocess range workflow (manual only)
```

---

## Pipeline 1 — Daily Ingest

### What it does

Every day at 6:00 AM IST, for yesterday's date:

1. Fetches all platform request logs for a window of `target - 3 days → target + 1 day`
2. Fetches all form submission logs for the same window
3. Fetches all user accounts created on the target date
4. For each user, finds their pre-signup session ID and builds their navigation journey
5. Uploads enriched records to BigQuery with `WRITE_APPEND`

### Schedule

`cron: "30 0 * * *"` = 00:30 UTC = **6:00 AM IST**, every day.

GitHub Actions scheduled runs are best-effort and can be delayed up to 15 minutes. If a run is missed, use `production_manual` mode to backfill it.

### Run Modes

| Mode | Date | Table | Use case |
|---|---|---|---|
| `production` | Yesterday (auto) | Main | Scheduled runs — fires automatically |
| `production_manual` | You specify | Main | Backfilling a missed or failed date |
| `test_auto` | Yesterday (auto) | Backup | Smoke test without touching main data |
| `test_manual` | You specify | Backup | Test a specific date safely |

### Step-by-step

**Step 1 — Platform Logs**
Endpoint: `POST /api/2.2/platformrequests`
Filter: epoch milliseconds. Window: target − 3 days to target + 1 day.
Builds: `{email → [{sessionId, ts}]}`

**Step 2 — Form Logs**
Endpoint: `POST /api/2.2/logs/frontend/formsubmit`
Filter: ISO 8601 timestamps (`YYYY-MM-DDTHH:MM:SS+00:00`). Same window.
Stores ALL paths including `/activate`. Path filtering to `/signup` only happens
later inside `_build_user_record` for FORM_TYPES users.
Builds: `{email → [{sessionId, ts, path}]}`

**Step 3 — Fetch Users**
Endpoint: `POST /api/2.2/user`
Filter: `DD/MM/YYYY` — this specific format is required by the User API.
Returns all users created on the exact target date.

**Step 4 — Enrich**
See [Session ID Resolution Logic](#session-id-resolution-logic--deep-dive) below.
Logs session ID hit/miss rate at the end of every run.

**Step 5 — Upload**
Format: Newline-delimited JSON (NDJSON). Disposition: `WRITE_APPEND`.
Explicit schema provided to prevent type mismatches on `createdDate` (DATE).

---

## Pipeline 2 — Category Sync

### What it does

User categories on the Tracxn platform are not always assigned at signup. They are filled in or updated later. This pipeline re-fetches the latest `userCategory` for users already in BigQuery and updates the column in place.

**Weekly sync** (7th, 14th, 21st, 28th at 7:00 AM IST): updates users from the last 7 days.
**Monthly sync** (1st of every month at 7:30 AM IST): updates all users from the previous full month.

### Atomic swap pattern

Rather than running slow DML UPDATEs, the sync uses a more efficient approach:
```
1. Read the date range rows from BigQuery → memory
2. Fetch latest categories from Tracxn API (by user ID, batches of 20)
3. Apply updated categories in memory
4. Write updated rows to a temp BigQuery table (explicit schema — never autodetect)
5. Reconstruct full table:
     SELECT * FROM main WHERE date < range_start   -- untouched
     UNION ALL
     SELECT * FROM temp_table                      -- updated
     UNION ALL
     SELECT * FROM main WHERE date > range_end     -- untouched
6. Drop temp table
```
The operation is atomic. All data outside the date range is untouched.

### Run Modes

| Mode | Date range | Table | Use case |
|---|---|---|---|
| `weekly_auto` | Last 7 days (auto) | Main | Scheduled weekly runs |
| `monthly_auto` | Last full month (auto) | Main | Scheduled monthly runs |
| `manual` | You specify start + end | Main | Custom backfill on main table |
| `test_weekly_auto` | Last 7 days (auto) | Backup | Test weekly logic safely |
| `test_monthly_auto` | Last full month (auto) | Backup | Test monthly logic safely |
| `test_manual` | You specify start + end | Backup | Test a custom range safely |

---

## Pipeline 3 — Reprocess Range

### What it does

Reprocesses historical data day-by-day using the current session ID logic. Designed as a correction tool — run it when the daily pipeline had a bug that caused incorrect session IDs or high miss rates for a date range.

For each day in the specified range:
1. Queries BigQuery for the existing session ID miss count for that day
2. Re-fetches and re-enriches all users from the Tracxn API using the current logic
3. Logs the comparison (old miss count vs new miss count)
4. **Always replaces** the existing data with the freshly processed data
5. Moves to the next day

### Safety features
- Only touches the specific day being processed — all other dates are untouched
- Atomic swap pattern per day — no partial states
- Dry-run mode: logs all decisions without writing to BigQuery
- Full per-day audit table printed at the end of every run

### Per-day atomic replace

```sql
CREATE OR REPLACE TABLE main_table
CLUSTER BY registrationType, geography
AS
SELECT * FROM main_table WHERE createdDate != DATE('YYYY-MM-DD')  -- all other dates
UNION ALL
SELECT * FROM temp_table                                           -- this day, updated
```

The temp table is written with an **explicit schema** (not autodetect) so `createdDate` is
typed as `DATE`, not `STRING`. Without this, the `UNION ALL` fails with
`incompatible types: DATE, STRING`.

### Log window

`reprocess_range.py` uses `LOG_WINDOW_DAYS_BEFORE = 1` (1 day before each target date).
`pipeline.py` uses `LOG_WINDOW_DAYS_BEFORE = 3` (3 days before).

This difference is intentional: the reprocess is run after the fact when logs are
already available and the window can be narrower. The daily pipeline uses 3 days to
catch pre-signup browsing sessions that may have started several days before the signup.

---

## Session ID Resolution Logic — Deep Dive

This is the most important and complex part of the codebase. The logic is **identical** in `pipeline.py` (`_build_user_record`) and `reprocess_range.py` (`_build_record`). Any change must be made in both files.

### Goal

Find the session ID from the user's browsing session **before** they signed up — the acquisition session. This is the session that came from an SEO page, ad, or referral. After signup, users get new session IDs for their in-platform activity. Those post-signup sessions must never be stored.

### created_epoch — the cutoff

```python
created_epoch = (
    cd.get("epochMillis")                          # exact ms if available
    or int(datetime(year, month, day, tzinfo=UTC)  # else midnight UTC of that day
           .timestamp() * 1000)
)
```

The User API returns `createdDate` as `{year, month, day, ...}`. When `epochMillis` is present in the response, it gives the exact creation timestamp in milliseconds — this is used directly. When it is absent (older API responses), we fall back to midnight UTC of the creation date as a conservative estimate.

All platform log and form log `ts` values are UTC epochMillis, so this comparison is consistent with no timezone conversion needed.

### _pick_session — the selection function

```python
def _pick_session(candidates):
    if not candidates:
        return None
    pre  = [e for e in candidates if e["ts"] < created_epoch]
    pool = sorted(pre if pre else candidates, key=lambda x: x["ts"])
    return pool[0]["sessionId"] if pool else None
```

- Filters candidates to those with `ts < created_epoch` (pre-signup only)
- Sorts by timestamp ascending, picks the earliest (furthest back — most likely the original acquisition session)
- Falls back to the globally earliest if nothing is found before `created_epoch` (handles clock-skew edge cases where the log timestamp and the user creation timestamp are within milliseconds of each other)

### Registration type branching

```
FORM_TYPES = {OTP_SIGNUP, THIRD_PARTY_SIGNUP, THIRD_PARTY_SIGNUP_GOOGLE,
              THIRD_PARTY_SIGNUP_MICROSOFT, THIRD_PARTY_SIGNUP_ENTRA_ID}

If reg_type IN FORM_TYPES:
    Primary:  form_map[email] filtered to path.startswith("/signup")
              /activate is EXCLUDED — it is the email verification step after
              signup. Its clientInfo.session.type is always LOGGED_IN.
              Its session ID belongs to in-platform activity, not acquisition.
    Fallback: platform_map[email]

If reg_type NOT IN FORM_TYPES:
    Primary:  platform_map[email]
    Fallback: form_map[email] (any path — no /signup filter)
```

### Why /activate is excluded

A user's form submission timeline looks like this:
```
ts: D-36s   formsubmit  /signup   session=X  ANONYMOUS   ← want this
ts: D-1s    formsubmit  /signup   session=Y  ANONYMOUS   ← or this (earlier wins)
    D       USER CREATED (created_epoch)
ts: D+12s   formsubmit  /activate session=Z  LOGGED_IN   ← never use this
ts: D+20s   urlchange   /a/dashboard  session=Z  LOGGED_IN
```

The `/activate` path is the email verification link clicked after the account is created. Its session (`Z`) is always `LOGGED_IN` — the user is already inside the platform. If we used session `Z`, the navigation journey would show in-platform pages (`/a/dashboard`, `/a/s/query/...`) rather than the SEO pages that led the user to sign up.

Note: For FORM_TYPES users, `/activate` entries in form_map often have `customData.userName` filled in (because the user is now logged in), which is why they appear in the map at all. The path filter and the `created_epoch` filter together ensure they are always excluded.

### Why there is still a FORM_TYPES branch

Despite the goal of having no registration-type branching, the FORM_TYPES branch exists because:
1. FORM_TYPES users go through the web signup form at `tracxn.com/signup`. Their pre-signup session is most reliably found in the form log with path `/signup`.
2. Non-FORM_TYPES users (Google/Microsoft/Entra OAuth) do not fill in `customData.userName` on the form log — so their entry there has an empty email and is dropped. Their pre-signup session is most reliably found in the platform log.

The branching is not about restricting which sources are checked — both sources are checked for both types via primary + fallback. It is only about which source is tried first to maximise the hit rate.

---

## Run Modes Reference

### Daily Ingest (`pipeline.yml`)

| Input | Options | Required for |
|---|---|---|
| **Mode** | `production`, `production_manual`, `test_auto`, `test_manual` | always |
| **Date** | `YYYY-MM-DD` | `*_manual` modes only |

### Category Sync (`sync_categories.yml`)

| Input | Options | Required for |
|---|---|---|
| **Sync mode** | `weekly_auto`, `monthly_auto`, `manual`, `test_weekly_auto`, `test_monthly_auto`, `test_manual` | always |
| **Start date** | `YYYY-MM-DD` | `*manual` modes only |
| **End date** | `YYYY-MM-DD` | `*manual` modes only |

### Reprocess Range (`reprocess_range.yml`)

| Input | Options | Required for |
|---|---|---|
| **Start date** | `YYYY-MM-DD` e.g. `2026-05-01` | always |
| **End date** | `YYYY-MM-DD` e.g. `2026-05-31` | always |
| **Dry run** | `true` / `false` | always — use `true` first |

---

## How to Trigger a Manual Run

All pipelines are triggered from the **Actions tab** in GitHub.

1. Go to **Actions** tab in your repository
2. Click the workflow name in the left sidebar
3. Click **Run workflow** (grey button, right side)
4. Fill in the inputs and click **Run workflow**

Each run shows a **Summary** panel at the bottom of the run page with mode, dates, status.

**Always dry-run first for reprocess.** Set `dry_run=true` to preview which days would be replaced and by how much before writing anything to BigQuery.

---

## Initial Setup

### 1. GCP Service Account (one-time)

No credit card required — this is just IAM.

1. Go to [console.cloud.google.com](https://console.cloud.google.com) → project `leadgen-474708`
2. **IAM & Admin → Service Accounts → Create Service Account** — name: `leadgen-pipeline`
3. Grant roles: `BigQuery Data Editor` + `BigQuery Job User`
4. **Keys → Add Key → JSON** → download the file

### 2. GitHub Secrets

**Settings → Secrets and variables → Actions → Secrets tab:**

| Secret | Value |
|---|---|
| `TRACXN_ACCESS_TOKEN` | Long alphanumeric token — used by daily ingest and reprocess |
| `GCP_PROJECT_ID` | `leadgen-474708` |
| `GCP_SA_KEY` | Full contents of the downloaded JSON service account key file |

**Variables tab:**

| Variable | Value |
|---|---|
| `BQ_DATASET` | `leadgen_dataset` |
| `BQ_TABLE` | `leadgen_users_v2_no_partition` |
| `BQ_TABLE_BACKUP` | `leadgen_users_v2_no_partition_backup3` |

### 3. Add files via GitHub UI

For each new file: **Add file → Create new file** → type the full path → paste contents → **Commit changes**.
For existing files: click the file → **pencil ✏️** → replace contents → **Commit changes**.

---

## BigQuery Schema

| Column | Type | Description |
|---|---|---|
| `createdDate` | DATE | Date the user account was created (YYYY-MM-DD) |
| `id` | STRING | Tracxn internal user ID |
| `email` | STRING | User email, lowercased |
| `userCategory` | STRING | Category/industry labels, comma-separated. Refreshed by category sync. |
| `originUrl` | STRING | First URL the user visited in their pre-signup session |
| `triggerUrl` | STRING | Last non-auth page visited before the signup/login page |
| `geography` | STRING | User's primary geography from their profile |
| `registrationType` | STRING | How they signed up (e.g. OTP_SIGNUP, THIRD_PARTY_SIGNUP_GOOGLE) |
| `sessionId` | STRING | Browser session ID of the pre-signup session. `N/A` if not found. |
| `userJourney` | STRING | Full navigation path, URLs joined by ` > `, query strings stripped |
| `cta` | STRING | `Auto_DD/MM/YYYY` — the date this record was generated by the pipeline |

---

## API Reference

All Tracxn API calls authenticate via the `accessToken` header.

| Endpoint | Filter format | Used by |
|---|---|---|
| `POST /api/2.2/platformrequests` | `createdDate`: epoch ms integers | Daily ingest, Reprocess |
| `POST /api/2.2/logs/frontend/formsubmit` | `createdDate`: ISO 8601 `YYYY-MM-DDTHH:MM:SS+00:00` | Daily ingest, Reprocess |
| `POST /api/2.2/user` | `createdDate`: `DD/MM/YYYY` **or** `id`: [int list] | All pipelines |
| `POST /api/2.2/logs/frontend/urlchange` | `sessionId`: string | Daily ingest, Reprocess |

Pagination: `size` (page size, max 30) + `from` (offset). Empty `result` array = end of data.

**Important:** The User API accepts two different filter types:
- Daily ingest and Reprocess filter by `createdDate` (DD/MM/YYYY) to get all users for a day
- Category sync filters by `id` (list of integers) to refresh specific users

---

## Configuration Reference

| Variable | Where | Used by | Description |
|---|---|---|---|
| `TRACXN_ACCESS_TOKEN` | GitHub Secret | Daily ingest, Reprocess | Long alphanumeric Tracxn API token |
| `GCP_PROJECT_ID` | GitHub Secret | All | GCP project ID |
| `GCP_SA_KEY` | GitHub Secret | All | Full GCP service account key JSON |
| `BQ_DATASET` | GitHub Variable | All | BigQuery dataset name |
| `BQ_TABLE` | GitHub Variable | All | Main production table name |
| `BQ_TABLE_BACKUP` | GitHub Variable | Daily ingest, Category sync | Backup table name (test modes) |
| `MODE` | Set by workflow | Daily ingest | Run mode |
| `TEST_DATE` | Set by workflow | Daily ingest | Manual date override (YYYY-MM-DD) |
| `SYNC_MODE` | Set by workflow | Category sync | Sync mode |
| `SYNC_START_DATE` | Set by workflow | Category sync | Range start (YYYY-MM-DD) |
| `SYNC_END_DATE` | Set by workflow | Category sync | Range end (YYYY-MM-DD) |
| `REPROCESS_START` | Set by workflow | Reprocess | Range start (YYYY-MM-DD) |
| `REPROCESS_END` | Set by workflow | Reprocess | Range end (YYYY-MM-DD) |
| `DRY_RUN` | Set by workflow | Reprocess | `true` = log only, `false` = write to BigQuery |

---

## Troubleshooting

**High session ID miss rate (> ~5%)**
Check Step 1 log: how many platform log entries were fetched? Check Step 2: how many form log entries? If either is unexpectedly low, the API may have returned fewer results than expected for that date window. Compare the `platform map: N entries` and `form map: N entries` log lines against a normal day. Also verify `TRACXN_ACCESS_TOKEN` in GitHub Secrets exactly matches the token in use.

**Session IDs found but they are post-signup (inside-platform sessions)**
The most common cause is that `created_epoch` is computed incorrectly. When `epochMillis` is missing from the User API response, the fallback is midnight UTC of the creation date. If many users were created late in the day (UTC), this means their same-day pre-signup sessions are accepted. Check the `_pick_session` fallback: `pre if pre else candidates` — the `else candidates` branch returns the earliest overall entry regardless of timestamp, which may be post-signup if no pre-signup entry exists. To debug: add a log line inside `_build_user_record` printing `email, created_epoch, len(pre), session_id` for a sample of users.

**`UNION ALL has incompatible types: DATE, STRING`**
Caused by writing the temp table with `autodetect=True` which infers `createdDate` as STRING. The reprocess pipeline uses explicit schema with `createdDate` as `DATE`. Ensure you are running the latest `reprocess_range.py`.

**Scheduled run at 6 AM IST did not fire**
GitHub pauses scheduled workflows after 60 days of repo inactivity. Go to Actions tab → Enable Workflows if shown. Also check if the run was just delayed — GitHub can delay scheduled runs by up to 15 minutes under load. If the run was missed entirely, use `production_manual` mode to backfill.

**BigQuery permission error**
The service account is missing `BigQuery Data Editor` or `BigQuery Job User`. Add the missing roles in GCP Console → IAM & Admin → IAM.

**`ValueError: Unknown format code 'd' for object of type 'str'`**
The User API returned `createdDate.year/month/day` as strings. Fixed in the current pipeline with `int(cd.get("year") or 2025)` casting. Ensure you have the latest `pipeline.py`.

---

## Design Decisions

**Why pipeline.py and reprocess_range.py have the same session ID logic**
Both files process users through the same enrichment function. The only differences are: (1) pipeline runs daily for one date, reprocess loops over a date range; (2) pipeline uses a wider log window (3 days before) while reprocess uses a narrower one (1 day before) since it runs after the fact; (3) pipeline appends to BigQuery, reprocess replaces per day. All session ID selection logic must be kept in sync between the two files manually — if you fix a bug in one, fix it in the other.

**Why FORM_TYPES branching still exists**
FORM_TYPES users go through the web form at `tracxn.com/signup`. Their pre-signup email is captured in `customData.userName` of the form log only after a successful submission — so the form map is the most reliable primary source. Non-FORM_TYPES (OAuth) users don't fill in this field, so their form map entry is usually empty and unusable. The platform map is their most reliable source. The branching determines which source is tried first, not which sources are available. Both sources are always checked via primary + fallback.

**Why /activate is excluded and not just filtered by created_epoch**
The `/activate` path is the email verification step. It always happens after account creation, so `ts > created_epoch` always. However, relying solely on the timestamp filter would require `created_epoch` to be perfectly accurate. The `/activate` path filter is a defence-in-depth measure — it explicitly excludes these entries at the source regardless of their timestamp, making the logic robust against any timestamp precision or timezone issues.

**Why the atomic swap pattern instead of DML UPDATE**
BigQuery DML UPDATE on large tables runs a full table scan and is billed by bytes processed. The `CREATE OR REPLACE TABLE ... UNION ALL` swap is faster and cheaper because BigQuery only reads the relevant rows. It also preserves the `CLUSTER BY registrationType, geography` definition and is atomic — no half-updated state is possible.

**Why NDJSON instead of CSV for uploads**
CSV requires escaping of commas, quotes, and newlines in field values. The `userJourney` field in particular contains URLs with commas and special characters. NDJSON handles all of this natively — each row is a self-contained JSON object and BigQuery's JSON loader parses correctly regardless of field content.

**Why GitHub Actions instead of Google Apps Script**
The original Apps Script pipeline had a 6-minute execution limit per function, requiring a complex progress-tracking system using Google Sheets as a checkpoint database. The daily ingest takes ~20 minutes, the monthly category sync ~45 minutes, and a full-month reprocess ~90 minutes. None of these fit in 6 minutes. GitHub Actions provides up to 6 hours per job, eliminating the need for checkpointing entirely. All intermediate data stays in Python memory — no Google Sheets dependency.
