# Project 07: Open Source Activity Dashboard (GitHub Events)

## Overview

GitHub publishes a public event feed showing all public activity across millions of
repositories: commits, pull requests, stars, forks, releases, and more. Without
authentication you get 60 requests/hour; with a personal access token, 5000/hour.

Your pipeline ingests this event stream, filters bot accounts and private repo noise,
and builds an **open source developer activity dashboard** that tracks trends in
languages, repositories, and developer actions.

## Data Source

| Property | Value |
|----------|-------|
| URL | `https://api.github.com/events` |
| Protocol | REST (GET, JSON), polling |
| Auth | Optional — create a Personal Access Token for higher rate limits |
| Rate limit | 60/hour unauthenticated, 5000/hour with token |
| Format | JSON array of event objects |

**Tip:** Create a free PAT at https://github.com/settings/tokens (no scopes needed
for public events — just click "Generate token" with no boxes checked).

**Quick test:**
```bash
# Without auth
curl -s "https://api.github.com/events" | python3 -m json.tool | head -80

# With auth (recommended)
curl -s -H "Authorization: token YOUR_TOKEN" "https://api.github.com/events" | python3 -m json.tool | head -80
```

## Key schema fields

```json
{
  "id": "33456789012",
  "type": "PushEvent",
  "actor": {
    "id": 12345, "login": "some_user", "display_login": "some_user",
    "url": "https://api.github.com/users/some_user"
  },
  "repo": {
    "id": 67890, "name": "owner/repo-name",
    "url": "https://api.github.com/repos/owner/repo-name"
  },
  "payload": { "...": "type-specific content" },
  "public": true,
  "created_at": "2024-01-15T10:30:00Z",
  "org": { "login": "some-org" }
}
```

**Common event types and their payload:**

| Type | Payload highlights |
|------|--------------------|
| `PushEvent` | `commits[]`, `ref` (branch), `size` (commit count) |
| `WatchEvent` | `action = "started"` (someone starred the repo) |
| `ForkEvent` | `forkee.full_name` (new fork's name) |
| `PullRequestEvent` | `action`, `pull_request.title`, `pull_request.state` |
| `IssuesEvent` | `action`, `issue.title`, `issue.labels[]` |
| `CreateEvent` | `ref_type` (branch/tag/repository), `ref` |
| `ReleaseEvent` | `release.tag_name`, `release.name`, `release.prerelease` |

---

## Phase A: Data Discovery (30 min)

1. Fetch the events endpoint. How many events are returned per page?
   Is there a `Link` header for pagination?

2. Copy one example of each event type you see into `notes.txt`.

3. Answer these questions:
   - What event types appear? Tally them from 100 events.
     What fraction are `PushEvent`? What fraction are `WatchEvent`?
   - How do you identify bot accounts? Look at `actor.login` —
     names containing `[bot]`, `dependabot`, `renovate`, `-bot` are common.
     Can you find 3 bot accounts in your sample?
   - For `PushEvent`: the `payload.commits` array — does it always have commits?
     What does `payload.size = 0` mean?
   - For private repos: `public = false`. What is in the payload for private events?
   - What does `org` being null vs populated tell you?
   - How do you extract the programming language from the event?
     (Hint: you can't from the event alone — but you can infer from the repo name
     or make a secondary API call to the repo endpoint)
   - Events are returned newest-first. If you poll every 60 seconds, can you miss events?
     What does the `X-Poll-Interval` header tell you?

4. Note the `ETag` and `X-Poll-Interval` headers. How would you use them to avoid
   re-downloading events you already have?

**You are ready for Phase B when you understand the ETag-based polling pattern.**

---

## Phase B: Producer and Ingestion

### producer.py

Write a Python script that:
- Polls `https://api.github.com/events` every **60 seconds**
- Sends `If-None-Match` header with the last ETag — the API returns 304 Not Modified
  (with no body) if no new events since last poll, saving your rate limit quota
- On a 200 response: extract and flatten each event into:
  `event_id`, `event_type`, `actor_login`, `repo_name`, `org_login` (may be null),
  `is_public`, `created_at`, `polled_at`, `payload_size` (bytes of payload JSON)
- For `PushEvent` also extract: `push_ref`, `push_commit_count`
- For `WatchEvent`: `watch_action`
- For `ReleaseEvent`: `release_tag`, `release_is_prerelease`
- Writes new events to `.../raw/poll_<ts>.json`
- Keeps a `seen_ids` set to avoid duplicates across polls

### ingestion_job.py

Write a PySpark batch job that:
- Reads all landing JSON files
- Converts `created_at` to `TimestampType`
- Extracts `repo_owner` and `repo_name_only` from `repo_name` (split on `/`)
- Adds `ingested_at`
- Writes to `raw-delta` using `mode("append")`

---

## Phase C: Cleansing Job (45 min)

### Data quality rules

| # | Field | Rule | rejection_reason |
|---|-------|------|-----------------|
| 1 | `event_id` | Not null | `null_event_id` |
| 2 | `created_at` | Not null | `null_timestamp` |
| 3 | `actor_login` | Not null | `null_actor` |
| 4 | `repo_name` | Not null, contains exactly one `/` | `invalid_repo_name` |
| 5 | `is_public` | Must be `true` | `private_event` |
| 6 | `event_type` | In known types list (see README) | `unknown_event_type` |
| 7 | `actor_login` | Does not match bot patterns | `bot_account` |

**Bot detection patterns** (mark as `bot_account`):
- `actor_login` contains `[bot]`
- `actor_login` ends with `-bot` or `_bot`
- `actor_login` in: `dependabot`, `renovate`, `github-actions`, `snyk-bot`, `codecov`

Add an `is_bot` boolean column before routing to error-delta.

---

## Phase D: Business Analytics Job (90 min)

### business_job.py — Open Source Activity Dashboard

Read from `valid-delta`.

### Business rules

**Step 1 — Hourly event type breakdown:**

For each `event_type` and 1-hour window:

| Column | Description |
|--------|-------------|
| `event_type` | GitHub event type |
| `window_start` | Hour window |
| `event_count` | Total events |
| `unique_actors` | Distinct developer count |
| `unique_repos` | Distinct repository count |

**Step 2 — Repository leaderboard (daily):**

For each `repo_name` per day, compute:
- `star_count` — WatchEvent count
- `fork_count` — ForkEvent count
- `push_count` — PushEvent count
- `pr_count` — PullRequestEvent count
- `total_activity` — sum of all event counts
- `unique_contributors` — distinct actor_logins

Rank by `total_activity` descending and keep top 100.

**Step 3 — Developer activity score (daily):**

For each `actor_login` per day:
- Count events by type
- Compute a weighted activity score:
  `score = pushes × 3 + prs × 5 + releases × 10 + issues × 2 + stars × 1`

Identify top 50 most active developers.

**Step 4 — Organisation activity:**
For events with a non-null `org_login`, aggregate by org per day.

Write to:
- `business-delta/hourly_event_breakdown/`
- `business-delta/repo_leaderboard/`
- `business-delta/developer_scores/`

---

## Sample deliverables

```
13-github-events/
├── notes.txt          # event type inventory from Phase A
├── pyproject.toml
└── src/
    └── github_events/
        ├── __init__.py
        ├── config.py          # GITHUB_TOKEN (or empty string), API_URL
        ├── producer.py
        ├── ingestion_job.py
        ├── cleansing_job.py
        └── business_job.py
```
