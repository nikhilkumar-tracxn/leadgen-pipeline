# Leadgen BigQuery Pipeline

Two automated pipelines that keep the Tracxn user dataset in BigQuery accurate and up-to-date.

| Pipeline | File | Schedule | What it does |
|---|---|---|---|
| **Daily Ingest** | `pipeline.py` | 6:00 AM IST daily | Fetches yesterday's signups, enriches with session/journey data, appends to BigQuery |
| **Category Sync** | `sync_categories.py` | 7th/14th/21st/28th + 1st monthly | Re-fetches latest user categories and updates BigQuery in place |

Both run fully automatically via GitHub Actions. Zero manual steps required after initial setup.

---

## Table of Contents

- [Why Two Pipelines?](#why-two-pipelines)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Pipeline 1 — Daily Ingest](#pipeline-1--daily-ingest)
- [Pipeline 2 — Category Sync](#pipeline-2--category-sync)
- [Manual Reprocess Pipeline](#manual-reprocess-pipeline)
- [Run Modes Reference](#run-modes-reference)
- [How to Trigger a Manual Run](#how-to-trigger-a-manual-run)
- [Initial Setup](#initial-setup)
- [BigQuery Schema](#bigquery-schema)
- [API Reference](#api-reference)
- [Configuration Reference](#configuration-reference)
- [Troubleshooting](#troubleshooting)
- [Design Decisions](#design-decisions)

---

## Why Two Pipelines?

The **daily ingest** captures new users as they sign up. However, user categories on the Tracxn platform are not always assigned immediately at signup — they can be updated or filled in later. A user who signed up on Day 1 might not have an accurate `userCategory` until Day 3 or Day 7.

The **category sync** solves this by periodically going back over recent users and refreshing their category from the API. Weekly runs cover the past 7 days; the monthly run does a full pass of the entire previous calendar month.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          GitHub Actions                              │
│                                                                      │
│  ┌───────────────────────────────────────┐                          │
│  │  Daily Ingest  (pipeline.py)          │  6:00 AM IST daily       │
│  │                                       │                          │
│  │  Tracxn API                           │                          │
│  │    /platformrequests ────────────────►│                          │
│  │    /formsubmit       ────────────────►│──► in-memory dicts       │
│  │    /user             ────────────────►│         │                │
│  │    /urlchange (per user) ────────────►│         │ enrich         │
│  │                                       │         ▼                │
│  │                               BigQuery WRITE_APPEND              │
│  └───────────────────────────────────────┘                          │
│                                                                      │
│  ┌───────────────────────────────────────┐                          │
│  │  Category Sync  (sync_categories.py)  │  7th/14th/21st/28th      │
│  │                                       │  + 1st of month          │
│  │  BigQuery (read range) ──────────────►│                          │
│  │  Tracxn API /user (by ID) ───────────►│──► update categories     │
│  │  BigQuery (write temp) ◄─────────────│         │                │
│  │  BigQuery (atomic swap) ◄────────────│─────────┘                │
│  └───────────────────────────────────────┘                          │
└──────────────────────────────────────────────────────────────────────┘
```

No Google Sheets involved anywhere. All intermediate data lives in Python dicts/lists in memory.

---

## Repository Structure

```
leadgen-pipeline/
│
├── pipeline.py              Daily ingest pipeline
├── reprocess_range.py       Manual reprocess pipeline (historical fix)
├── sync_categories.py       Category sync pipeline
├── requirements.txt         Python dependencies (shared by all scripts)
├── README.md                This file
│
└── .github/
    └── workflows/
        ├── pipeline.yml              Daily ingest workflow
        ├── reprocess_range.yml       Manual reprocess workflow
        └── sync_categories.yml       Category sync workflow
```

---

## Pipeline 1 — Daily Ingest

### What it does

Every day at 6:00 AM IST, for yesterday's date:

1. Fetches all platform request logs for a ±1-day window (session ID source for API/platform users)
2. Fetches all form submission logs for a ±1-day window (session ID source for web signup users)
3. Fetches all user accounts created on the target date
4. For each user, resolves their pre-signup session ID and fetches their full URL navigation history
5. Derives: **originUrl** (first URL in session), **triggerUrl** (page that led to signup), **userJourney** (full navigation path)
6. Appends enriched records to BigQuery with `WRITE_APPEND`

### Schedule

`cron: "30 0 * * *"` → 00:30 UTC → **6:00 AM IST**, every day.

### Log Window

```
log_start = target_date - 1 day
log_end   = target_date + 1 day
```

A ±1-day window catches sessions that started just before midnight or log entries recorded slightly after midnight of the target date.

### Session ID Resolution — Full Detail

This is the most important logic in the pipeline. The goal is to find the **browser session ID that existed before the user created their account** — i.e. the session from their acquisition journey, not any post-signup in-platform session.

#### Step 1 — Build source maps (Steps 1 and 2 of the pipeline)

Two in-memory lookup maps are built, keyed by email:

**`platform_map`** → `{email: [{sessionId, ts}]}`
- Source: `/api/2.2/platformrequests`
- Used for: users who registered via the Tracxn platform API (non-form signups)
- Every entry that has a valid email + session ID is stored

**`form_map`** → `{email: [{sessionId, ts, path}]}`
- Source: `/api/2.2/logs/frontend/formsubmit`
- Used for: users who registered via the web signup form
- Every entry that has a valid email + session ID is stored, including the URL path

#### Step 2 — Determine `created_epoch` (per user)

```python
created_epoch = (
    cd.get("epochMillis")          # exact ms timestamp of signup if API provides it
    or int(datetime(year, month, day, tzinfo=utc).timestamp() * 1000)  # midnight UTC fallback
)
```

`epochMillis` from `createdDate` gives the exact millisecond the account was created. This is used as the cutoff — any log entry with `ts >= created_epoch` is considered post-signup and deprioritised.

#### Step 3 — `_pick_session(candidates)` function

```
Input:  list of log entries [{sessionId, ts, ...}]
Output: sessionId string, or None if list is empty

Logic:
  1. Filter to entries where ts < created_epoch  (strictly before signup)
  2. Sort filtered entries by ts ascending
  3. Return the sessionId of the first (earliest) entry
  4. If no entries pass the filter (all post-signup), fall back:
       sort ALL candidates by ts, return the earliest
  5. Return None only if candidates list is completely empty
```

The fallback in step 4 handles edge cases where the API timestamp and log timestamp have slight clock skew, or where `epochMillis` was unavailable and midnight UTC was used as a less-precise cutoff.

#### Step 4 — Registration type branching

The registration type determines which source map is tried first:

```
FORM_TYPES = {
    "OTP_SIGNUP",
    "THIRD_PARTY_SIGNUP",
    "THIRD_PARTY_SIGNUP_GOOGLE",
    "THIRD_PARTY_SIGNUP_MICROSOFT",
    "THIRD_PARTY_SIGNUP_ENTRA_ID",
}

if reg_type in FORM_TYPES:
    Primary  → form_map, filtered to paths starting with /signup
    Fallback → platform_map (any entry)

else (API/platform users):
    Primary  → platform_map (any entry)
    Fallback → form_map (any path — not restricted to /signup)
```

**Why `/activate` is always excluded for FORM_TYPES:**
`/activate` is the post-signup email verification URL. A user lands on `/activate` only after completing signup. Its session ID belongs to in-platform activity, not the acquisition journey. Including it would attribute the wrong session to the user's origin.

**Why form_map fallback for non-FORM_TYPES uses any path:**
Non-form users (API registrations, enterprise SSO) sometimes also have form log entries from earlier browsing. No path restriction is applied because there's no `/signup` path to expect from them — any log entry is potentially valid acquisition data.

### Journey Construction

Once a `session_id` is found, the pipeline fetches up to 50 URL change events for that session from `/api/2.2/logs/frontend/urlchange`.

```
events: sorted list of {ts, url, path, tab, prevTab}

originUrl:   events[0]["url"]  — first URL in the session
userJourney: all URLs joined by " > ", query strings stripped
triggerUrl:  the last non-auth page before the signup/login event
             (found by walking the prevTab chain backwards from the last
              /signup or /login event)
```

`triggerUrl` is the most meaningful field for attribution — it's the content page, search result, or referral link that directly caused the user to click signup.

### Run Modes

| Mode | Date used | Table | When to use |
|---|---|---|---|
| `production` | Yesterday (auto) | Main | Scheduled runs (automatic) |
| `production_manual` | You specify | Main | Backfilling a missed date |
| `test_auto` | Yesterday (auto) | Backup | Quick smoke test |
| `test_manual` | You specify | Backup | Test a specific historical date |

---

## Pipeline 2 — Category Sync

### What it does

User categories are not always assigned at signup time. This pipeline periodically re-fetches the latest `userCategory` for users already in BigQuery and updates it in place.

**Weekly sync** (7th, 14th, 21st, 28th at 7:00 AM IST): updates users created in the last 7 days.

**Monthly sync** (1st of every month at 7:30 AM IST): updates all users from the entire previous calendar month.

### How the update works — atomic swap pattern

BigQuery DML `UPDATE` on large tables is slow and expensive. Instead:

```
1. Read the date range rows from BigQuery → pandas DataFrame
2. Fetch latest categories from Tracxn API (by user ID, batches of 20)
3. Apply updated categories to the DataFrame in memory
4. Write the updated DataFrame to a temporary BigQuery table (temp_catsync_*)
5. Reconstruct the full table atomically:

     CREATE OR REPLACE TABLE main_table AS
       SELECT * FROM main_table WHERE date < range_start   ← untouched rows
       UNION ALL
       SELECT * FROM temp_table                            ← updated rows
       UNION ALL
       SELECT * FROM main_table WHERE date > range_end     ← untouched rows

6. Drop the temp table
```

This is atomic — either the full swap succeeds or nothing changes. All data outside the target date range is completely untouched. The `CLUSTER BY` definition on the main table is preserved.

### Schedule

| Cron | UTC | IST | What fires |
|---|---|---|---|
| `30 1 * * *` | 01:30 UTC daily | 7:00 AM IST | Script checks if today is day 7/14/21/28, proceeds if so |
| `0 2 1 * *` | 02:00 UTC on 1st | 7:30 AM IST | Monthly sync on 1st of every month |

The daily cron is used for the weekly triggers because GitHub Actions does not support day-of-month lists in a single cron expression (e.g. `30 1 7,14,21,28 * *` fires only once per month). The workaround: schedule daily and let the script check `$(date +%d)` at runtime.

### Run Modes

| Mode | Date range | Table | When to use |
|---|---|---|---|
| `weekly_auto` | Last 7 days (auto) | Main | Scheduled weekly runs |
| `monthly_auto` | Last full month (auto) | Main | Scheduled monthly runs |
| `manual` | You specify start + end | Main | Custom backfill on main table |
| `test_weekly_auto` | Last 7 days (auto) | Backup | Test weekly logic safely |
| `test_monthly_auto` | Last full month (auto) | Backup | Test monthly logic safely |
| `test_manual` | You specify start + end | Backup | Test a custom range safely |

---

## Manual Reprocess Pipeline

### Purpose

`reprocess_range.py` is a one-off tool for fixing historical data over a date range. It applies the same enrichment logic as `pipeline.py` but processes multiple days in sequence and always replaces existing data with freshly processed data.

### When to use it

- A bug was found in session ID logic and historical data needs to be corrected
- A date range was processed with old code and needs to be rerun with the current logic
- Any situation where you need to overwrite existing BigQuery rows for a past date range

### How it works (per-day loop)

For each day in `REPROCESS_START` → `REPROCESS_END`:

```
A. Query BigQuery — count session ID misses in existing data for that day
B. Fetch fresh data from Tracxn API using current pipeline logic
C. Enrich users (same session ID logic as pipeline.py)
D. Decide:
     - If no existing data → INSERT fresh records
     - If existing data exists → always REPLACE with freshly processed data
       (regardless of whether miss count improved, stayed the same, or worsened)
E. Write to BigQuery (or log-only if DRY_RUN=true)
```

### Session ID logic in reprocess_range.py (identical to pipeline.py)

Both files share the same session ID resolution logic:

- `created_epoch` from `createdDate.epochMillis` (exact ms), fallback to midnight UTC
- `_pick_session`: earliest `ts < created_epoch`, fallback to earliest overall
- FORM_TYPES → form `/signup` paths first, then platform fallback
- Non-FORM_TYPES → platform first, then form any path fallback
- `/activate` always excluded for FORM_TYPES

### Log window

```
log_start = target_date - 1 day
log_end   = target_date + 1 day
```

Same ±1-day window as `pipeline.py`.

### Always-replace behaviour

Unlike an earlier version that skipped days where new miss count was not lower than existing, this version always replaces. The reasoning: even if the miss count is identical or slightly higher, the session IDs that *are* found are now correct (no `/activate` contamination, exact epoch comparison). A correct N/A is better than an incorrect session ID.

### Dry run mode

Set `DRY_RUN=true` to log all decisions without writing anything to BigQuery. Always do a dry run first to preview which days will be replaced.

### Environment variables

| Variable | Required | Description |
|---|---|---|
| `TRACXN_ACCESS_TOKEN` | Yes | Tracxn API auth token |
| `GCP_PROJECT_ID` | Yes | GCP project ID |
| `GCP_SA_KEY` | Yes | Full GCP service account key JSON |
| `BQ_DATASET` | No | Defaults to `leadgen_dataset` |
| `BQ_TABLE` | No | Defaults to `leadgen_users_v2_no_partition` |
| `REPROCESS_START` | Yes | Start date `YYYY-MM-DD` |
| `REPROCESS_END` | Yes | End date `YYYY-MM-DD` |
| `DRY_RUN` | No | Set to `"true"` to preview without writing |

### Atomic replace — how it works

When replacing an existing day, `reprocess_range.py` uses the same atomic swap pattern as the category sync:

```
1. Write new records to a temp table (temp_reprocess_<uuid>)
   with explicit schema (not autodetect) — required so createdDate
   is typed DATE not STRING, which would break the UNION ALL
2. Rebuild the main table:
     CREATE OR REPLACE TABLE main_table AS
       SELECT * FROM main_table WHERE createdDate != 'YYYY-MM-DD'  ← all other days
       UNION ALL
       SELECT * FROM temp_table                                     ← new records
3. Drop the temp table
```

This is all-or-nothing. No partial states. All other dates in the table are completely untouched.

### Audit summary

At the end of every run, a full audit table is printed:

```
Date         Action       Old miss   New miss   Reason
2026-05-01   REPLACE      45         12         New miss count (12) < existing (45) — 33 fewer misses
2026-05-02   REPLACE      30         30         Same miss count (30) — replacing with latest processed data
2026-05-03   SKIPPED      0          0          No users returned from API
...
Days replaced  : 28
Days inserted  : 0
Days skipped   : 3
Total session miss improvement: 341 fewer misses
```

### How to trigger (GitHub Actions)

1. Go to **Actions** → **Manual Reprocess Pipeline** → **Run workflow**
2. Fill in:
   - `reprocess_start`: `YYYY-MM-DD`
   - `reprocess_end`: `YYYY-MM-DD`
   - `dry_run`: `true` (preview first) or `false` (actually write)
3. Click **Run workflow**

---

## Run Modes Reference

### Daily Ingest (`pipeline.yml`)

| Input | Options | Notes |
|---|---|---|
| **Mode** | `production`, `production_manual`, `test_auto`, `test_manual` | |
| **Date** | `YYYY-MM-DD` | Required only for `*_manual` modes |

### Category Sync (`sync_categories.yml`)

| Input | Options | Notes |
|---|---|---|
| **Sync mode** | `weekly_auto`, `monthly_auto`, `manual`, `test_weekly_auto`, `test_monthly_auto`, `test_manual` | |
| **Start date** | `YYYY-MM-DD` | Required only for `*manual` modes |
| **End date** | `YYYY-MM-DD` | Required only for `*manual` modes |

### Manual Reprocess (`reprocess_range.yml`)

| Input | Options | Notes |
|---|---|---|
| **reprocess_start** | `YYYY-MM-DD` | Required |
| **reprocess_end** | `YYYY-MM-DD` | Required |
| **dry_run** | `true` / `false` | Defaults to `false` |

---

## How to Trigger a Manual Run

### Daily Ingest

1. Go to **Actions** → **Leadgen BigQuery Pipeline** → **Run workflow**
2. Select **Mode** from the dropdown
3. For `*_manual` modes, enter the date in `YYYY-MM-DD` format
4. Click **Run workflow**

### Category Sync

1. Go to **Actions** → **Category Sync** → **Run workflow**
2. Select **Sync mode** from the dropdown
3. For `*manual` modes, enter **Start date** and **End date**
4. Click **Run workflow**

### Manual Reprocess

1. Go to **Actions** → **Manual Reprocess Pipeline** → **Run workflow**
2. Enter **reprocess_start** and **reprocess_end**
3. Set **dry_run** to `true` for a preview first, then `false` to actually write
4. Click **Run workflow**

> **Always test before writing to main.** Use `test_*` modes or `dry_run=true` before any run that writes to the production table.

---

## Initial Setup

### 1. GCP Service Account (one-time)

1. Go to [console.cloud.google.com](https://console.cloud.google.com) → select your project
2. Navigate to **IAM & Admin → Service Accounts → Create Service Account**
3. Grant two roles: `BigQuery Data Editor` + `BigQuery Job User`
4. Click the account → **Keys → Add Key → JSON** → download the file

### 2. GitHub Secrets

**Settings → Secrets and variables → Actions → Secrets tab:**

| Secret | Value |
|---|---|
| `TRACXN_ACCESS_TOKEN` | Your Tracxn API access token |
| `GCP_PROJECT_ID` | Your GCP project ID |
| `GCP_SA_KEY` | Paste the entire contents of the downloaded JSON key file |

**Variables tab** (same page):

| Variable | Value |
|---|---|
| `BQ_DATASET` | `leadgen_dataset` |
| `BQ_TABLE` | `leadgen_users_v2_no_partition` |
| `BQ_TABLE_BACKUP` | `leadgen_users_v2_no_partition_backup3` |

These are shared by all three pipelines — no duplication needed.

### 3. Add new files to your repository

For the category sync and reprocess pipelines, add files via the GitHub UI:

1. Click **Add file → Create new file**
2. Enter the filename (for nested paths like `.github/workflows/foo.yml`, type the `/` characters and GitHub creates the folders automatically)
3. Paste the file contents
4. Click **Commit changes**

Files needed: `sync_categories.py`, `reprocess_range.py`, `.github/workflows/sync_categories.yml`, `.github/workflows/reprocess_range.yml`

Also update `requirements.txt` to add `pandas` and `db-dtypes` (required by the category sync).

---

## BigQuery Schema

All pipelines read from and write to the same table structure.

**Main table:** `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
**Backup table:** `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_BACKUP}`

| Column | Type | Set by | Description |
|---|---|---|---|
| `createdDate` | DATE | Daily ingest, Reprocess | Date the user account was created |
| `id` | STRING | Daily ingest, Reprocess | Tracxn internal user ID |
| `email` | STRING | Daily ingest, Reprocess | User email, lowercased |
| `userCategory` | STRING | All pipelines | Category/industry labels, comma-separated. Updated by category sync. |
| `originUrl` | STRING | Daily ingest, Reprocess | First URL visited in the pre-signup session |
| `triggerUrl` | STRING | Daily ingest, Reprocess | The specific page that led the user to signup/login |
| `geography` | STRING | Daily ingest, Reprocess | User's primary geography from their profile |
| `registrationType` | STRING | Daily ingest, Reprocess | How they signed up (e.g. `OTP_SIGNUP`, `THIRD_PARTY_SIGNUP_GOOGLE`) |
| `sessionId` | STRING | Daily ingest, Reprocess | Pre-signup browser session ID. `N/A` if not found. |
| `userJourney` | STRING | Daily ingest, Reprocess | Full navigation path — all URLs in session joined by ` > `, query strings stripped |
| `cta` | STRING | Daily ingest, Reprocess | Always `Auto_DD/MM/YYYY` — records the pipeline run date |

---

## API Reference

All Tracxn API calls use `POST` and the `accessToken` header for authentication. Pagination uses `size` (page size, max 30) and `from` (offset). An empty or absent `result` array signals end of data.

| Endpoint | Used by | Filter format | Notes |
|---|---|---|---|
| `POST /api/2.2/platformrequests` | Daily ingest, Reprocess | `createdDate`: epoch ms | Builds platform_map |
| `POST /api/2.2/logs/frontend/formsubmit` | Daily ingest, Reprocess | `createdDate`: ISO 8601 (`YYYY-MM-DDTHH:MM:SS+00:00`) | Builds form_map |
| `POST /api/2.2/user` | All pipelines | `createdDate`: DD/MM/YYYY **or** `id`: [int list] | Daily ingest + Reprocess use date filter; category sync uses ID filter |
| `POST /api/2.2/logs/frontend/urlchange` | Daily ingest, Reprocess | `sessionId`: string | Max 50 results fetched per user; builds journey data |

**Important:** The User API accepts two different filter shapes depending on the caller:
- Daily ingest and reprocess filter by `createdDate` (DD/MM/YYYY) to get all users for a day
- Category sync filters by `id` (list of integers) to get current data for specific known users

---

## Configuration Reference

| Variable | Where stored | Used by | Description |
|---|---|---|---|
| `TRACXN_ACCESS_TOKEN` | GitHub Secret | All | Tracxn API auth token |
| `GCP_PROJECT_ID` | GitHub Secret | All | GCP project ID |
| `GCP_SA_KEY` | GitHub Secret | All | Full GCP service account key JSON |
| `BQ_DATASET` | GitHub Variable | All | BigQuery dataset name |
| `BQ_TABLE` | GitHub Variable | All | Main production table name |
| `BQ_TABLE_BACKUP` | GitHub Variable | Daily ingest | Backup table name (test modes) |
| `MODE` | Set by workflow | Daily ingest | Run mode |
| `TEST_DATE` | Set by workflow | Daily ingest | Manual date (YYYY-MM-DD) |
| `SYNC_MODE` | Set by workflow | Category sync | Sync mode |
| `SYNC_START_DATE` | Set by workflow | Category sync | Range start (YYYY-MM-DD) |
| `SYNC_END_DATE` | Set by workflow | Category sync | Range end (YYYY-MM-DD) |
| `REPROCESS_START` | Set by workflow | Reprocess | Range start (YYYY-MM-DD) |
| `REPROCESS_END` | Set by workflow | Reprocess | Range end (YYYY-MM-DD) |
| `DRY_RUN` | Set by workflow | Reprocess | `"true"` to log decisions only, no writes |

---

## Troubleshooting

### Daily Ingest

**0 rows uploaded**
Check Step 3 logs. If 0 users fetched, either there were genuinely no signups that day (valid) or the date format was wrong. Verify the input is `YYYY-MM-DD` for manual modes.

**High session ID miss rate in Step 4 logs**
The `miss rate` line at the end of Step 4 shows the percentage of users where no session ID was found. A rate above ~5% may indicate: (a) users who signed up via a channel that doesn't produce platform or form logs, (b) the log window was too narrow for that day's signup pattern, or (c) an API issue during log fetching.

**`ValueError: Unknown format code 'd' for object of type 'str'`**
The Tracxn API returned `createdDate.year/month/day` as strings instead of integers. Fixed with `int()` casting in the current version — ensure you have the latest `pipeline.py`.

**HTTP 401 from Tracxn API**
The `TRACXN_ACCESS_TOKEN` secret has expired. Update it under Settings → Secrets → Actions.

**BigQuery permission error**
The service account is missing `BigQuery Data Editor` or `BigQuery Job User`. Add the roles in GCP Console → IAM & Admin → IAM.

**Schedule stopped running**
GitHub pauses scheduled workflows after 60 days of repository inactivity. Go to Actions → Enable Workflows, or trigger a manual run to re-enable.

**Run cancelled after 60 minutes**
Unusually high user count for that day. Increase `timeout-minutes` in `pipeline.yml`.

### Category Sync

**Sync ran but categories look unchanged**
Check Step 2 logs for API failure warnings. If the Tracxn API returned empty `categoryList` for users, they are stored as "Not yet classified" — this may be correct if the platform hasn't classified them yet.

**Temp table not dropped after failure**
The cleanup call is in a `finally` block, so it runs even on errors. If you see orphaned `temp_catsync_*` tables in BigQuery, delete them manually — they contain no unique data.

**`SYNC_START_DATE` / `SYNC_END_DATE` not provided for manual mode**
The script raises `ValueError` immediately with a clear message. Fill in both date fields when choosing a `*manual` mode.

**Weekly sync ran on the wrong days**
The schedule fires daily but the script exits early unless today is day 7/14/21/28. Check the "Determine sync mode" step logs in the Actions run.

### Manual Reprocess

**Orphaned temp table `temp_reprocess_*` in BigQuery**
The cleanup is in a `finally` block but may fail if the BigQuery client lost its connection. Delete the table manually — it contains no unique data and can be safely removed.

**Reprocess completed but miss count did not improve as expected**
Check the per-day audit log. If "new miss" equals "existing miss", the session IDs that were found are still more correct (no `/activate` contamination) even if the count is the same. The count measures quantity of misses, not quality of hits.

**`DRY_RUN` showed REPLACE for every day but actual run skipped some**
Dry run and real run use the same decision logic. If skips occur, check whether the API returned users for those days — a "No users returned from API" skip is expected for dates with zero signups.

---

## Design Decisions

### Why two separate workflows instead of one?

The ingest and sync have different schedules, different failure modes, and different timeout requirements (~20 min vs ~60 min). Keeping them separate means a sync failure never blocks the daily ingest, and each can be triggered, monitored, and re-run independently.

### Why the atomic swap pattern for updates?

BigQuery DML `UPDATE` runs a full table scan and is billed by bytes processed. The swap pattern (read range → update in memory → write temp → reconstruct with UNION ALL) is faster and cheaper — it only touches the rows in the target date range. It also preserves the `CLUSTER BY registrationType, geography` definition on the main table.

### Why `created_epoch` uses `epochMillis` not just the date?

The user API returns `createdDate` as `{year, month, day, epochMillis}`. Using only the date would force a comparison against midnight UTC, which incorrectly treats all same-day log entries as pre-signup (a user who browsed at 2 PM and signed up at 8 PM on the same day would have their post-8PM session picked up). Using `epochMillis` gives an exact millisecond cutoff.

### Why is there a fallback in `_pick_session` to earliest-overall?

If `epochMillis` is unavailable and midnight UTC is used as the cutoff, platform or form log timestamps might all appear to be "after" midnight of that day simply due to timezone representation differences or slight API clock skew. The fallback prevents these edge cases from producing an unnecessary N/A. The priority is always pre-signup entries; the fallback only fires when the strict filter returns nothing.

### Why is `/activate` excluded for FORM_TYPES?

`/activate` is the email verification URL sent after signup. A user only visits `/activate` after they have already created their account. Its session ID belongs to in-platform activity, not the acquisition journey that caused them to sign up. Including it would attribute a post-signup session as the origin, which is incorrect.

### Why does the non-FORM_TYPES fallback use any path (not just `/signup`)?

Non-form users (API registrations, enterprise SSO via Entra ID, etc.) do not go through the standard web signup flow. They may not have any `/signup` path entries in the form log at all. Restricting to `/signup` would always miss them. Any form log entry for these users is potentially valid pre-signup browsing data.

### Why is batch size 20 for the category sync API calls?

The category sync uses `id: [int list]` filtering against the user API. Batches of 20 were found to work reliably in the original Colab script without hitting rate limits. Larger batches risk timeouts on the API side.

### Why GitHub Actions instead of Google Apps Script?

Apps Script has a hard 6-minute execution limit. The daily ingest takes ~20 minutes and the monthly category sync can take ~45 minutes. Neither would complete within Apps Script's limits. GitHub Actions provides up to 6 hours per job, making both pipelines feasible without checkpointing or resumption logic.

### Why does the reprocess pipeline always replace (never skip)?

An earlier version skipped days where the new miss count was not lower than the existing miss count. This was wrong: even if the miss count is the same or slightly higher, the session IDs that *are* found are more accurate under the current logic (no `/activate` contamination, exact epoch comparison). Skipping would preserve stale data that is known to be incorrect.

### Why `db-dtypes` in requirements.txt?

The `google-cloud-bigquery` library requires `db-dtypes` when converting BigQuery query results to pandas DataFrames (used in the category sync's `to_dataframe()` call). Without it, the call raises an `ImportError`.
