# Leadgen BigQuery Pipeline

Automated daily pipeline that fetches new user signups from the Tracxn platform, enriches them with session and navigation journey data, and loads them into BigQuery.

Runs every day at **6:00 AM IST** via GitHub Actions — fully automatic, zero manual steps required once set up.

---

## Table of Contents

- [What This Does](#what-this-does)
- [Architecture](#architecture)
- [Run Modes](#run-modes)
- [Repository Structure](#repository-structure)
- [How to Trigger a Manual Run](#how-to-trigger-a-manual-run)
- [Initial Setup](#initial-setup)
- [Pipeline Steps — Deep Dive](#pipeline-steps--deep-dive)
- [BigQuery Schema](#bigquery-schema)
- [API Reference](#api-reference)
- [Configuration Reference](#configuration-reference)
- [Troubleshooting](#troubleshooting)
- [Design Decisions](#design-decisions)

---

## What This Does

Every day, the pipeline:

1. Fetches all platform request logs and form submission logs for a 3-day window around the target date (to handle timezone edge cases)
2. Fetches all user accounts created on the target date from the Tracxn User API
3. For each user, finds their session ID from the log that matches their registration type
4. Uses that session ID to retrieve their full URL navigation history
5. Derives three enriched fields: **origin URL** (where they started), **trigger URL** (what page led them to sign up), and **user journey** (full navigation path)
6. Uploads all enriched records to BigQuery with `WRITE_APPEND` (adds rows, never overwrites)

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   GitHub Actions                        │
│                                                         │
│  Schedule (6 AM IST daily)  OR  Manual trigger          │
│            │                                            │
│            ▼                                            │
│       pipeline.py                                       │
│            │                                            │
│    ┌───────┴────────┐                                   │
│    │  Tracxn API    │                                   │
│    │                │                                   │
│    │  /platformrequests  → platform_map (in memory)     │
│    │  /formsubmit        → form_map     (in memory)     │
│    │  /user              → users list  (in memory)      │
│    │  /urlchange         → per-user navigation events   │
│    └───────┬────────┘                                   │
│            │                                            │
│     Enrich + merge                                      │
│            │                                            │
│    ┌───────┴────────┐                                   │
│    │   BigQuery     │                                   │
│    │   WRITE_APPEND │                                   │
│    └────────────────┘                                   │
└─────────────────────────────────────────────────────────┘
```

**No Google Sheets involved anywhere.** All intermediate data lives in Python dicts and lists in memory during the GitHub Actions run. When the job finishes, nothing is persisted except the rows in BigQuery.

---

## Run Modes

There are four modes, selectable from the **Run workflow** button in the Actions tab:

| Mode | Date used | BigQuery table | When to use |
|---|---|---|---|
| `production` | Yesterday (automatic) | Main table | Automatic scheduled runs only |
| `production_manual` | You specify | Main table | Backfilling a missed date in production |
| `test_auto` | Yesterday (automatic) | Backup table | Quick smoke test — safe, won't touch main data |
| `test_manual` | You specify | Backup table | Testing a specific historical date safely |

**Rule of thumb:** Always test with `test_manual` or `test_auto` before using any `production_*` mode manually. The scheduled nightly run always uses `production` automatically.

---

## Repository Structure

```
leadgen-pipeline/
│
├── pipeline.py              Main pipeline — all logic lives here
├── requirements.txt         Python package dependencies
├── README.md                This file
│
└── .github/
    └── workflows/
        └── pipeline.yml     GitHub Actions workflow definition
```

---

## How to Trigger a Manual Run

1. Go to your repository on GitHub
2. Click the **Actions** tab (top navigation)
3. Click **Leadgen BigQuery Pipeline** in the left sidebar
4. Click the **Run workflow** button (grey, right side)
5. A dropdown appears:

   **Mode** — pick one of the four modes described above

   **Date** — only fill this in for `test_manual` or `production_manual`. Format must be `YYYY-MM-DD` (e.g. `2026-05-01` for May 1st 2026). Leave blank for auto modes.

6. Click **Run workflow** (green button)
7. A new run appears in the list — click it to watch live logs

The **Summary** tab at the bottom of each run shows a quick table of what mode ran, what date was processed, and whether it succeeded.

---

## Initial Setup

### 1. GCP Service Account

The pipeline needs a GCP service account to write to BigQuery. No credit card required — this is just IAM.

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Select project `leadgen-474708`
3. Navigate to **IAM & Admin → Service Accounts**
4. Click **Create Service Account**
   - Name: `leadgen-pipeline`
   - Click **Create and Continue**
5. Add two roles:
   - `BigQuery Data Editor`
   - `BigQuery Job User`
6. Click **Done**
7. Click the new service account → **Keys tab → Add Key → JSON**
8. A JSON file downloads — keep it safe, you'll paste it as a secret

### 2. GitHub Secrets

Go to: **Repository → Settings → Secrets and variables → Actions**

**Secrets tab** (sensitive values — masked in logs forever after saving):

| Secret name | Value |
|---|---|
| `TRACXN_ACCESS_TOKEN` | Your Tracxn API access token |
| `GCP_PROJECT_ID` | `leadgen-474708` |
| `GCP_SA_KEY` | Open the downloaded JSON file in Notepad, select all, paste the entire contents |

**Variables tab** (non-sensitive config):

| Variable name | Value |
|---|---|
| `BQ_DATASET` | `leadgen_dataset` |
| `BQ_TABLE` | `leadgen_users_v2_no_partition` |
| `BQ_TABLE_BACKUP` | `leadgen_users_v2_no_partition_backup3` |

### 3. Verify the Schedule

The workflow runs automatically at `cron: "30 0 * * *"` — this is **00:30 UTC = 6:00 AM IST**.

GitHub Actions cron uses UTC always. IST is UTC+5:30, so to get 6:00 AM IST:
- 6:00 AM IST − 5h30m = 12:30 AM UTC = `30 0 * * *` ✓

You don't need to do anything for the schedule to work once the workflow file is in the repo. GitHub reads it automatically.

**One caveat:** GitHub may delay scheduled runs by up to 15 minutes during periods of high server load. This is normal — the pipeline is not time-sensitive to the minute.

---

## Pipeline Steps — Deep Dive

### Step 1 — Fetch Platform Logs

**Endpoint:** `POST /api/2.2/platformrequests`

**Filter format:** Epoch milliseconds (integer)
```json
{"filter": {"createdDate": {"min": 1748736000000, "max": 1749081599999}}}
```

**Why:** For users who registered via the platform API (not the web form), the session ID is embedded in the platform request log. We build an in-memory dict: `{email → [{sessionId, ts}]}`.

**Date window:** target_date ± 1 day. This is wider than strictly necessary to catch any events that may be timestamped slightly outside the target day due to timezone differences between the API server and UTC.

---

### Step 2 — Fetch Form Logs

**Endpoint:** `POST /api/2.2/logs/frontend/formsubmit`

**Filter format:** ISO 8601 with explicit UTC offset
```json
{"filter": {"createdDate": {"min": "2026-05-31T00:00:00+00:00", "max": "2026-06-02T23:59:59+00:00"}}}
```

**Why:** For users who registered via the web signup form (`/signup` path), the session ID is in the form submission log. We filter entries where `path == "/signup"` and take the earliest timestamp to get the first signup attempt.

**Output:** `{email → [{sessionId, ts, path}]}`

---

### Step 3 — Fetch Users

**Endpoint:** `POST /api/2.2/user`

**Filter format:** `DD/MM/YYYY` string — this specific format is required by this particular API. It differs from the other two endpoints.
```json
{"filter": {"createdDate": {"min": "01/06/2026", "max": "01/06/2026"}}}
```

**Pagination:** All three fetch steps use offset-based pagination with `size: 30` and incrementing `from` values until an empty page is returned.

---

### Step 4 — Enrich Users

For each user:

1. **Session ID resolution:**
   - If `registrationType` is in `FORM_TYPES` (web signup) → check `form_map`, filter `path == "/signup"`, use earliest
   - Otherwise (API/OAuth registration) → check `platform_map`, use earliest

2. **Navigation journey** (only if session ID found):
   - Fetches up to 50 URL change events for the session from `/logs/frontend/urlchange`
   - Sorts events by timestamp
   - `originUrl` = first URL visited
   - `userJourney` = all URLs joined by ` > `, query strings stripped
   - `triggerUrl` = last non-auth page before `/signup` or `/login`, found by walking backwards through the `prevTab` chain

3. **Category enrichment:**
   - Prefers `categoryList[].userCategory` (more granular) over `userCategory` (top-level)
   - Joins multiple categories with `, `

4. **Error handling:**
   - If a single user fails (bad data, unexpected field type), that user is **skipped with a warning** and the pipeline continues. This prevents one bad record from failing the entire day's upload.

---

### Step 5 — Upload to BigQuery

- **Format:** Newline-delimited JSON (NDJSON) — chosen over CSV because it handles commas, quotes, and newlines in field values natively with no escaping required
- **Disposition:** `WRITE_APPEND` — rows are always added, existing data is never touched
- **Schema:** Explicitly provided to ensure type validation (DATE for `createdDate`, STRING for everything else)
- **Auth:** Service account credentials from `GCP_SA_KEY` environment variable
- **Blocking:** `job.result()` waits for the BigQuery load job to complete and raises an exception if it fails, so pipeline failures are always visible in the Actions log

---

## BigQuery Schema

Table: `leadgen-474708.leadgen_dataset.leadgen_users_v2_no_partition`

| Column | Type | Description |
|---|---|---|
| `createdDate` | DATE | Date the user account was created (YYYY-MM-DD) |
| `id` | STRING | Tracxn internal user ID |
| `email` | STRING | User email, lowercased |
| `userCategory` | STRING | Category/industry label(s), comma-separated if multiple |
| `originUrl` | STRING | First URL the user visited in their session |
| `triggerUrl` | STRING | The page that led the user to the signup/login page |
| `geography` | STRING | User's primary geography from their profile |
| `registrationType` | STRING | How they signed up (e.g. OTP_SIGNUP, THIRD_PARTY_SIGNUP_GOOGLE) |
| `sessionId` | STRING | Browser session ID used to track navigation |
| `userJourney` | STRING | Full URL path through the session, joined by ` > ` |
| `cta` | STRING | Always `Auto_DD/MM/YYYY` — the date this record was generated |

---

## API Reference

All Tracxn API calls are authenticated via the `accessToken` header.

| API | Method | Filter key format | Used for |
|---|---|---|---|
| `/api/2.2/platformrequests` | POST | `createdDate`: epoch ms | Platform request logs |
| `/api/2.2/logs/frontend/formsubmit` | POST | `createdDate`: ISO 8601 | Form submission logs |
| `/api/2.2/user` | POST | `createdDate`: DD/MM/YYYY | Fetch users |
| `/api/2.2/logs/frontend/urlchange` | POST | `sessionId`: string | Navigation events per session |

**Note:** Each API uses a different date format. This is a quirk of the Tracxn API and is handled explicitly in each step.

Pagination: `size` (page size, max 30) + `from` (offset). Responses return a `result` array.

---

## Configuration Reference

| Name | Where | Type | Description |
|---|---|---|---|
| `TRACXN_ACCESS_TOKEN` | GitHub Secret | Secret | Tracxn API auth token |
| `GCP_PROJECT_ID` | GitHub Secret | Secret | GCP project ID |
| `GCP_SA_KEY` | GitHub Secret | Secret | Full GCP service account JSON |
| `BQ_DATASET` | GitHub Variable | Config | BigQuery dataset name |
| `BQ_TABLE` | GitHub Variable | Config | Main production table |
| `BQ_TABLE_BACKUP` | GitHub Variable | Config | Backup table for testing |
| `MODE` | Set by workflow | Runtime | Pipeline run mode |
| `TEST_DATE` | Set by workflow | Runtime | Manual date override (YYYY-MM-DD) |

---

## Troubleshooting

**Pipeline ran but 0 rows uploaded**
- Check Step 3 logs — if 0 users were fetched, the date may have had no signups, or the date format was wrong
- Verify the date input is `YYYY-MM-DD`, not `DD/MM/YYYY`

**`ValueError: Unknown format code 'd' for object of type 'str'`**
- The Tracxn API returned `createdDate.year/month/day` as strings instead of integers for some users
- Fixed in the current version with explicit `int()` casting — ensure you have the latest `pipeline.py`

**HTTP 401 from Tracxn API**
- The `TRACXN_ACCESS_TOKEN` secret has expired or is incorrect
- Update it in Settings → Secrets → Actions

**BigQuery permission error**
- The service account is missing the `BigQuery Data Editor` or `BigQuery Job User` role
- Go to GCP Console → IAM & Admin → IAM, find the service account, and add the missing roles

**Schedule not running**
- GitHub pauses scheduled workflows if the repo has no activity for 60 days
- Fix: go to Actions tab → click Enable Workflows (if shown), or trigger a manual run

**Run took longer than 60 minutes and was cancelled**
- The user count for that day was unusually high
- Increase `timeout-minutes` in `pipeline.yml` (safe up to 360 for public repos, 6h)

---

## Design Decisions

**Why GitHub Actions instead of Google Apps Script?**
Apps Script has a hard 6-minute execution limit per function call. The pipeline processes ~600 users/day and takes ~20 minutes. The original Apps Script version required a complex progress-tracker system using Google Sheets as a database to checkpoint across multiple trigger firings. GitHub Actions has a 60-minute limit, runs the full pipeline in one shot, and needs none of that complexity.

**Why no Google Sheets?**
The Sheets-based progress tracker was only needed because of the Apps Script execution limit. With GitHub Actions there is no limit to work around, so all intermediate data is held in Python dicts/lists in memory during the run.

**Why NDJSON instead of CSV for BigQuery upload?**
CSV requires careful escaping of commas, double-quotes, and newlines — especially in fields like `userJourney` which can contain any characters. NDJSON (newline-delimited JSON) handles all of this natively. Each row is a valid JSON object; BigQuery's JSON loader parses them correctly regardless of what characters appear in string values.

**Why is the log window ± 1 day around the target date?**
The Tracxn API timestamps events in the server's local timezone, which may differ from UTC. Fetching a ±1 day window ensures we don't miss events that were logged slightly before midnight or after midnight UTC relative to the target date. The user filter (Step 3) is still exact — only users created on the target date are processed.

**Why `WRITE_APPEND` and not `WRITE_TRUNCATE`?**
`WRITE_TRUNCATE` would delete all existing data in the table before inserting. Since each run only processes one day at a time, `WRITE_APPEND` is the correct choice — it adds the new day's rows without touching any previously loaded data.
