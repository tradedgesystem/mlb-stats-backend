# MLB Stats Chrome Extension - Project Status

This project is a Chrome extension that lets users compare player stats (2-5
players) or view an individual player's stats. Users will be able to select from
an available list of stats in the UI.

This file is the living status doc for goals, completed work, and remaining
tasks. Update it as the project evolves.

## Project Goals

- Provide a simple Chrome extension UI for:
  - Individual player stats
  - 2-5 player comparisons
  - User-selected stat list
- Serve data from a local FastAPI backend in development.
- Keep scraping out of the extension. Ingestion only runs via `backend/ingest.py`.

## Completed (Verified)

### 1) End-to-end pipeline (local)

Working loop:

```
pybaseball -> SQLite -> FastAPI -> Chrome extension
```

- `backend/ingest.py`
  - Idempotent (safe to run repeatedly).
  - Writes via a temp DB and atomically replaces `backend/stats.db`.
- `backend/api.py`
  - FastAPI starts cleanly:
    ```
    python3 -m uvicorn backend.api:app --reload --host 127.0.0.1 --port 8000
    ```
  - `GET /players?year=YYYY` returns JSON.
- Chrome extension
  - Loads unpacked and can fetch data from the local API.
- CORS (local dev only)
  - Allows `http://127.0.0.1:8000`, `http://localhost:8000`,
    and `chrome-extension://*`.

### 2) Year selection in the extension

- Removed hardcoded year.
- Added a year selector (2024 / 2025 / 2026).
- Fetches:
  ```
  /players?year=${selectedYear}
  ```
- Empty years return an empty array without error.

### 3) Player identity, search, and compare

- Stable player IDs stored as `player_id` (FanGraphs ID via `idfg`).
- Search endpoint:
  ```
  GET /search?year=YYYY&q=QUERY
  ```
  Returns `{player_id, name, team}` matches.
- Compare endpoint:
  ```
  GET /compare?year=YYYY&player_ids=ID1,ID2,ID3
  ```
  Returns aligned stat rows for the requested IDs.
- Extension UI:
  - Search players by name.
  - Add 1-5 players to a selection list.
  - Compare button renders JSON in the popup.
  - View Player renders a single player's JSON.

## Remaining Work

### 4) Individual player view

- Added `/player?year=YYYY&player_id=ID`.
- Extension supports selecting 1 player and viewing stats.

### 5) Zero-cost scaling and IP-ban/rate-limit strategy

Goal: reach 10k-20k users with $0 spend while staying safe with upstream data
sources.

Current status:

- Local dev only (extension -> local FastAPI -> local SQLite).

Target production path (zero-cost + ban-safe):

- Daily scheduled ingestion only (GitHub Actions).
- Export static JSON snapshots to a public host (GitHub Pages or Releases).
- Serve data directly from the static host (no live API server).

Recommended approach (low cost, low risk):

- **Precompute data once per day** (or less) via a scheduled job.
  - Keep ingestion in `backend/ingest.py` only.
  - Never run ingestion from the extension.
- **Serve static JSON to the extension** instead of a live API for production.
  - Publish a daily snapshot (per season) to a static host.
  - The extension fetches static JSON directly.
  - This eliminates runtime compute cost and reduces failure points.

Practical $0 options for static hosting:

- **GitHub Pages** or **GitHub Releases** (public assets)
- **jsDelivr** (free CDN in front of GitHub repos)
- **Netlify/Vercel static** (free static hosting)

Rate-limit and IP-ban avoidance (ingestion):

- Use pybaseball only (no direct scraping from extension).
- Keep the ingestion cadence low (daily or weekly).
- Rely on pybaseball's built-in caching to avoid repeated pulls.
- Avoid parallel ingestion or rapid retries.
- Fail closed if upstream rate limits or errors appear.

Production safety rules:

- Never expose ingestion endpoints.
- Never let users trigger ingestion.
- Keep a last-known-good snapshot and serve it if ingestion fails.
- Log ingestion errors and stop rather than retry aggressively.

Paid scale-up plan (triggered by user growth):

- Move ingestion and API to a managed backend (Render, Fly.io, or similar).
- Use managed Postgres for durable storage and faster queries.
- Add a CDN layer for static snapshots (Cloudflare or Fastly).
- Add monitoring/alerts (Sentry, Logtail, or similar).
- Add rate limiting and API keys at the edge.

## Zero-cost data scope note (Statcast)

Keeping *all raw Statcast data* for free is not realistic due to large storage
and bandwidth needs. A $0 approach can still work if we:

- Store **only aggregated, per-player stats** (the exact fields the UI uses).
- Keep data split by year and compress JSON.
- Publish static snapshots to free hosting (GitHub Pages/Releases + jsDelivr).
- Avoid serving raw pitch-by-pitch data in production.

### 5) Ingestion expansion (controlled)

- Multi-year data (historical seasons).
- Optional pitching stats.
- Scheduled updates (ingestion not user-triggered).

### 6) Deployment (required to sell)

- Host the FastAPI API publicly (Render or similar).
- Automate daily ingestion (GitHub Actions).
- Use persistent storage (Postgres or durable SQLite volume).

### 7) Basic protection and scaling

- API keys per user or license.
- Rate limiting.
- Basic logging/monitoring.

### 8) Chrome Web Store readiness

- Clean UI and versioning.
- Privacy policy.
- Minimal permissions.
- Screenshots and description.

### 9) Product polish

Pick 1-2 of:

- Stat normalization (per PA/IP).
- Preset comparisons.
- Player similarity.
- Export/share.

## Minimum "Sellable" Checklist

1. Hosted API (public URL)
2. Scheduled daily ingestion
3. Player search + compare (done locally)
4. Stable player IDs (done locally)
5. Basic API access control

## Suggested Next Step

Define the stat list + labels + formatting, then wire a stats selector UI that
filters the compare output.
