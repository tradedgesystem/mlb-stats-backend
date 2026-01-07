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

Implement search + compare endpoints and wire them into the extension UI.
