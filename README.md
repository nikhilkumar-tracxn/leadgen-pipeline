# Leadgen BigQuery Pipeline

Two automated pipelines that together keep the Tracxn user dataset in BigQuery accurate and up-to-date.

| Pipeline | File | Schedule | What it does |
|---|---|---|---|
| **Daily Ingest** | `pipeline.py` | 6:00 AM IST daily | Fetches new signups from yesterday and loads them into BigQuery with session/journey enrichment |
| **Category Sync** | `sync_categories.py` | 7th/14th/21st/28th + 1st monthly | Re-fetches the latest user categories from the Tracxn API and updates them in BigQuery |

Both run fully automatically via GitHub Actions — zero manual steps required after initial setup.

---

## Table of Contents

- [Why Two Pipelines?](#why-two-pipelines)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Pipeline 1 — Daily Ingest](#pipeline-1--daily-ingest)
- [Pipeline 2 — Category Sync](#pipeline-2--category-sync)
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

The **daily ingest** captures new users as they sign up. However, user categories on the Tracxn platform are not always assigned immediately at signup — they can be updated or filled in later. This means a user who signed up on Day 1 might not have an accurate `userCategory` until Day 3 or Day 7.

The **category sync** solves this by periodically going back over recent users and refreshing their category from the API. Weekly runs catch users from the past 7 days; the monthly run does a full pass of the entire previous month.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        GitHub Actions                            │
│                                                                  │
│  ┌─────────────────────────────────┐                            │
│  │  Daily Ingest (pipeline.py)     │  6:00 AM IST daily         │
│  │                                 │                            │
│  │  Tracxn API                     │                            │
│  │    /platformrequests ──────────►│                            │
│  │    /formsubmit       ──────────►│──► in-memory dicts         │
│  │    /user             ──────────►│         │                  │
│  │    /urlchange (per user) ──────►│         │ enrich           │
│  │                                 │         ▼                  │
│  │                          BigQuery WRITE_APPEND               │
│  └─────────────────────────────────┘                            │
│                                                                  │
│  ┌─────────────────────────────────┐                            │
│  │  Category Sync                  │  7th/14th/21st/28th        │
│  │  (sync_categories.py)           │  + 1st of month            │
│  │                                 │                            │
│  │  BigQuery (read range) ────────►│                            │
│  │  Tracxn API /user (by ID) ─────►│──► update categories       │
│  │  BigQuery (write temp) ◄────────│         │                  │
│  │  BigQuery (atomic swap) ◄───────│─────────┘                  │
│  └─────────────────────────────────┘                            │
└──────────────────────────────────────────────────────────────────┘
```

**No Google Sheets involved anywhere.** All intermediate data lives in Python dicts/lists/dataframes in memory.

---

## Repository Structure

```
leadgen-pipeline/
│
├── pipeline.py              Daily ingest pipeline
├── sync_categories.py       Category sync pipeline
├── requirements.txt         Python dependencies (shared by both scripts)
├── README.md                This file
│
└── .github/
    └── workflows/
        ├── pipeline.yml         Daily ingest workflow
        └── sync_categories.yml  Category sync workflow
```

---

## Pipeline 1 — Daily Ingest

### What it does

Every day at 6:00 AM IST, for yesterday's date:

1. Fetches all platform request logs (for session ID matching)
2. Fetches all form submission logs (for session ID matching)
3. Fetches all user accounts created on the target date
4. For each user, finds their session ID and fetches their full URL navigation history
5. Derives: **originUrl** (where they started), **triggerUrl** (page that led to signup), **userJourney** (full path)
6. Uploads enriched records to BigQuery with `WRITE_APPEND`

### Schedule

`cron: "30 0 * * *"` = **00:30 UTC = 6:00 AM IST**, every day.

### Run Modes

| Mode | Date used | Table | When to use |
|---|---|---|---|
| `production` | Yesterday (auto) | Main | Scheduled runs (automatic) |
| `production_manual` | You specify | Main | Backfilling a missed date |
| `test_auto` | Yesterday (auto) | Backup | Quick smoke test |
| `test_manual` | You specify | Backup | Test a specific historical date |

### Step-by-Step Detail

**Step 1 — Platform Logs**
- Endpoint: `POST /api/2.2/platformrequests`
- Filter: epoch milliseconds, window = target ± 1 day
- Builds: `{email → [{sessionId, ts}]}`
- Used for: users who registered via platform API (non-form signups)

**Step 2 — Form Logs**
- Endpoint: `POST /api/2.2/logs/frontend/formsubmit`
- Filter: ISO 8601 timestamps (`YYYY-MM-DDTHH:MM:SS+00:00`), window = target ± 1 day
- Builds: `{email → [{sessionId, ts, path}]}`
- Used for: users who registered via the web signup form

**Step 3 — Fetch Users**
- Endpoint: `POST /api/2.2/user`
- Filter: `DD/MM/YYYY` (note: different format from the log APIs)
- Returns: all users created on the exact target date

**Step 4 — Enrich**
- Session ID: chosen from form map (if `registrationType` in FORM_TYPES) or platform map
- Journey: fetches up to 50 URL change events per user, builds origin/trigger/journey
- Trigger URL: found by walking backwards through the `prevTab` chain from the last auth event
- Error handling: individual user failures are skipped with a warning; the rest continue

**Step 5 — Upload**
- Format: Newline-delimited JSON (NDJSON) — handles special characters natively
- Disposition: `WRITE_APPEND` — always adds rows, never overwrites

---

## Pipeline 2 — Category Sync

### What it does

User categories are not always assigned at signup time. This pipeline periodically re-fetches the latest `userCategory` for users already in BigQuery and updates it in place.

**Weekly sync** (7th, 14th, 21st, 28th at 7:00 AM IST): updates users created in the last 7 days.

**Monthly sync** (1st of every month at 7:30 AM IST): updates all users from the entire previous calendar month.

### How the update works — the "atomic swap" pattern

BigQuery DML UPDATE on large tables is slow and expensive. Instead, the sync uses a more efficient pattern:

```
1. Read the date range rows from BigQuery → pandas DataFrame
2. Fetch latest categories from Tracxn API (by user ID, batches of 20)
3. Apply updated categories to the DataFrame in memory
4. Write the updated DataFrame to a temporary BigQuery table
5. Reconstruct the full table:
     CREATE OR REPLACE TABLE main_table AS
       SELECT * FROM main_table WHERE date < range_start   ← untouched
       UNION ALL
       SELECT * FROM temp_table                            ← updated
       UNION ALL
       SELECT * FROM main_table WHERE date > range_end     ← untouched
6. Drop the temp table
```

This is atomic — either the full swap succeeds or nothing changes. All data outside the target date range is completely untouched.

### Schedule

The sync uses two cron entries:

| Cron | UTC time | IST time | What fires |
|---|---|---|---|
| `30 1 * * *` | 01:30 UTC daily | 7:00 AM IST | Runs daily, but script only proceeds on days 7/14/21/28 |
| `0 2 1 * *` | 02:00 UTC on 1st | 7:30 AM IST | Monthly sync on 1st of every month |

The daily cron trick: GitHub Actions does not support day-of-month lists in a single cron expression (e.g. `30 1 7,14,21,28 * *` fires only once per month, not four times). The workaround is to schedule daily and let the shell script check `$(date +%d)` at runtime to decide whether to proceed.

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

---

## How to Trigger a Manual Run

### Daily Ingest

1. Go to **Actions** tab → **Leadgen BigQuery Pipeline** → **Run workflow**
2. Select **Mode** from the dropdown
3. For `*_manual` modes, enter the date in `YYYY-MM-DD` format
4. Click **Run workflow**

### Category Sync

1. Go to **Actions** tab → **Category Sync** → **Run workflow**
2. Select **Sync mode** from the dropdown
3. For `*manual` modes, enter **Start date** and **End date** in `YYYY-MM-DD` format
4. Click **Run workflow**

> **Always test first.** Use a `test_*` mode before any `production` or `manual` mode that writes to the main table. The backup table has an expiry date — test data there won't persist forever.

Each run shows a **Summary** panel at the bottom of the Actions run page with mode, date range, trigger type, and status.

---

## Initial Setup

### 1. GCP Service Account (one-time)

No credit card required — this is just IAM configuration.

1. Go to [console.cloud.google.com](https://console.cloud.google.com) → select project `leadgen-474708`
2. Navigate to **IAM & Admin → Service Accounts → Create Service Account**
   - Name: `leadgen-pipeline`
3. Grant two roles: `BigQuery Data Editor` + `BigQuery Job User`
4. Click the account → **Keys → Add Key → JSON** → download the file

### 2. GitHub Secrets

**Settings → Secrets and variables → Actions → Secrets tab:**

| Secret | Value |
|---|---|
| `TRACXN_ACCESS_TOKEN` | Your Tracxn API access token |
| `GCP_PROJECT_ID` | `leadgen-474708` |
| `GCP_SA_KEY` | Paste the entire contents of the downloaded JSON key file |

**Variables tab** (same page):

| Variable | Value |
|---|---|
| `BQ_DATASET` | `leadgen_dataset` |
| `BQ_TABLE` | `leadgen_users_v2_no_partition` |
| `BQ_TABLE_BACKUP` | `leadgen_users_v2_no_partition_backup3` |

These are shared by both pipelines — no duplication needed.

### 3. Add the New Files to Your Repository

For the category sync, you need to add two new files via the GitHub UI:

**File 1 — `sync_categories.py`**
1. In your repo, click **Add file → Create new file**
2. Name: `sync_categories.py`
3. Paste the full contents of `sync_categories.py`
4. Click **Commit changes**

**File 2 — `.github/workflows/sync_categories.yml`**
1. Click **Add file → Create new file**
2. In the Name field, type: `.github/workflows/sync_categories.yml`
   (GitHub auto-creates the folder as you type the `/` characters)
3. Paste the full contents of `sync_categories.yml`
4. Click **Commit changes**

**File 3 — update `requirements.txt`**
1. Click `requirements.txt` in your repo → pencil ✏️ edit icon
2. Replace the contents with the updated `requirements.txt` (adds `pandas` and `db-dtypes`)
3. Click **Commit changes**

---

## BigQuery Schema

Both pipelines read from and write to the same table structure.

**Main table:** `leadgen-474708.leadgen_dataset.leadgen_users_v2_no_partition`
**Backup table:** `leadgen-474708.leadgen_dataset.leadgen_users_v2_no_partition_backup3`

| Column | Type | Set by | Description |
|---|---|---|---|
| `createdDate` | DATE | Daily ingest | Date the user account was created |
| `id` | STRING | Daily ingest | Tracxn internal user ID |
| `email` | STRING | Daily ingest | User email, lowercased |
| `userCategory` | STRING | Both pipelines | Category/industry labels, comma-separated. Updated by category sync. |
| `originUrl` | STRING | Daily ingest | First URL the user visited in their session |
| `triggerUrl` | STRING | Daily ingest | The page that led the user to signup/login |
| `geography` | STRING | Daily ingest | User's primary geography from their profile |
| `registrationType` | STRING | Daily ingest | How they signed up (e.g. OTP_SIGNUP, THIRD_PARTY_SIGNUP_GOOGLE) |
| `sessionId` | STRING | Daily ingest | Browser session ID |
| `userJourney` | STRING | Daily ingest | Full navigation path, URLs joined by ` > ` |
| `cta` | STRING | Daily ingest | Always `Auto_DD/MM/YYYY` — the pipeline run date |

---

## API Reference

All Tracxn API calls use the `accessToken` header for authentication.

| Endpoint | Used by | Filter format | Notes |
|---|---|---|---|
| `POST /api/2.2/platformrequests` | Daily ingest | `createdDate`: epoch ms | Pagination: size+from |
| `POST /api/2.2/logs/frontend/formsubmit` | Daily ingest | `createdDate`: ISO 8601 | Pagination: size+from |
| `POST /api/2.2/user` | Both pipelines | `createdDate`: DD/MM/YYYY **or** `id`: [int list] | Daily ingest uses date filter; sync uses ID filter |
| `POST /api/2.2/logs/frontend/urlchange` | Daily ingest | `sessionId`: string | Max 50 results fetched per user |

**Important:** The User API accepts two different filter types depending on the caller:
- Daily ingest filters by `createdDate` (DD/MM/YYYY) to get all users for a day
- Category sync filters by `id` (list of integers) to get current data for specific users

Pagination uses `size` (page size, max 30) and `from` (offset). An empty `result` array signals end of data.

---

## Configuration Reference

| Variable | Where stored | Used by | Description |
|---|---|---|---|
| `TRACXN_ACCESS_TOKEN` | GitHub Secret | Both | Tracxn API auth token |
| `GCP_PROJECT_ID` | GitHub Secret | Both | GCP project ID |
| `GCP_SA_KEY` | GitHub Secret | Both | Full GCP service account key JSON |
| `BQ_DATASET` | GitHub Variable | Both | BigQuery dataset name |
| `BQ_TABLE` | GitHub Variable | Both | Main production table name |
| `BQ_TABLE_BACKUP` | GitHub Variable | Both | Backup table name (test modes) |
| `MODE` | Set by workflow | Daily ingest | Run mode |
| `TEST_DATE` | Set by workflow | Daily ingest | Manual date (YYYY-MM-DD) |
| `SYNC_MODE` | Set by workflow | Category sync | Sync mode |
| `SYNC_START_DATE` | Set by workflow | Category sync | Range start (YYYY-MM-DD) |
| `SYNC_END_DATE` | Set by workflow | Category sync | Range end (YYYY-MM-DD) |

---

## Troubleshooting

### Daily Ingest

**0 rows uploaded**
Check Step 3 logs. If 0 users fetched, either there were no signups that day (valid) or the date format was wrong. Verify input is `YYYY-MM-DD`.

**`ValueError: Unknown format code 'd' for object of type 'str'`**
The Tracxn API returned `createdDate.year/month/day` as strings. Fixed with `int()` casting in the current version — ensure you have the latest `pipeline.py`.

**HTTP 401 from Tracxn API**
The `TRACXN_ACCESS_TOKEN` secret has expired. Update it under Settings → Secrets → Actions.

**BigQuery permission error**
The service account is missing `BigQuery Data Editor` or `BigQuery Job User`. Add the roles in GCP Console → IAM & Admin → IAM.

**Schedule stopped running**
GitHub pauses scheduled workflows after 60 days of repo inactivity. Go to Actions → Enable Workflows, or trigger a manual run to re-enable.

**Run cancelled after 60 minutes**
Unusually high user count for that day. Increase `timeout-minutes` in `pipeline.yml`.

### Category Sync

**Sync ran but categories look unchanged**
- Check Step 2 logs for API failure warnings. If the Tracxn API returned empty `categoryList` for users, they are stored as "Not yet classified" — this may be correct if the platform hasn't classified them yet.
- Verify the `SYNC_MODE` was not a test mode writing to the backup table when you expected main table.

**Temp table not dropped after failure**
The `_drop_temp_table` call is inside a `finally` block, so it runs even on errors. If you see orphaned temp tables in BigQuery (named `temp_catsync_*`), you can delete them manually — they contain no unique data.

**`SYNC_START_DATE` / `SYNC_END_DATE` not provided for manual mode**
The script raises `ValueError` immediately with a clear message. Fill in both date fields when choosing a `*manual` mode.

**Weekly sync ran on the wrong days**
The schedule fires daily but the shell script exits early unless today is day 7/14/21/28. Check the "Determine sync mode" step logs in the Actions run — it prints which day triggered.

**Sync took longer than expected**
The monthly sync reads the entire previous month from BigQuery and calls the API for every user. For months with 15,000+ users this can take 45–60 minutes. `timeout-minutes` is set to 120 to handle this. If it exceeds 120 min, increase the value in `sync_categories.yml`.

---

## Design Decisions

**Why two separate workflows instead of one?**
The ingest and sync have different schedules, different failure modes, and different timeout requirements (60 min vs 120 min). Keeping them separate means a sync failure doesn't block the daily ingest, and each can be triggered, monitored, and re-run independently.

**Why the atomic swap pattern for category updates?**
BigQuery DML UPDATE on large tables runs a full table scan and is billed by bytes processed. The swap pattern (read range → update in memory → write temp → reconstruct) is faster and cheaper because it only touches the rows in the date range, not the entire table. It also preserves the `CLUSTER BY` definition from the original table.

**Why batch size 20 for the category sync API calls?**
The daily ingest uses `size: 30` for pagination. The category sync uses `id: [int list]` filtering, and from the original Colab script, batches of 20 were found to work reliably without hitting rate limits. Larger batches risk timeouts on the API side.

**Why `db-dtypes` in requirements.txt?**
The `google-cloud-bigquery` library requires `db-dtypes` when converting BigQuery results to pandas DataFrames (used in the category sync). Without it, the `to_dataframe()` call raises an import error.

**Why GitHub Actions instead of Google Apps Script?**
Apps Script has a hard 6-minute execution limit. The daily ingest takes ~20 minutes and the monthly category sync can take ~45 minutes. Neither would complete within Apps Script's limits. GitHub Actions provides up to 6 hours per job, making both pipelines feasible without checkpointing or progress tracking.

**Why no Google Sheets?**
The original Apps Script pipeline used Sheets as a progress-tracking database to checkpoint across multiple 6-minute trigger firings. With GitHub Actions there is no execution limit to work around, so all intermediate data stays in Python memory — simpler, faster, and with no external dependencies.
