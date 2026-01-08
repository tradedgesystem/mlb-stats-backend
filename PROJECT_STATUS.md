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

## Stat List (Provided)

These are the target stats to compute and expose. Mapping to available data
columns and derived formulas is pending.

### Hitter Stats

#### Outcome and Slash

- Plate Appearances (PA)
- At-Bats (AB)
- Hits (H)
- Singles (1B)
- Doubles (2B)
- Triples (3B)
- Home Runs (HR)
- Runs (R)
- Runs Batted In (RBI)
- Walks (BB)
- Intentional Walks (IBB)
- Hit By Pitch (HBP)
- Strikeouts (SO)
- Sacrifice Flies (SF)
- Sacrifice Hits (SH)
- Batting Average (AVG)
- On-Base Percentage (OBP)
- Slugging Percentage (SLG)
- On-Base Plus Slugging (OPS)
- Isolated Power (ISO)
- Batting Average on Balls in Play (BABIP)

#### Expected and Statcast Quality

- Expected Batting Average (xBA)
- Expected Slugging (xSLG)
- Expected On-Base Percentage (xOBP)
- Expected Weighted On-Base Average (xwOBA)
- Weighted On-Base Average (wOBA)
- Expected Home Runs (xHR)

#### Contact and Power

- Average Exit Velocity
- Max Exit Velocity
- Median Exit Velocity
- Exit Velocity Percentiles (10th, 50th, 90th)
- Hard-Hit %
- Barrel Count
- Barrel %
- Barrels per Plate Appearance
- Barrels per Ball in Play
- Sweet-Spot %
- Average Launch Angle
- Launch Angle Standard Deviation
- Average Home Run Distance
- True Distance
- Hang Time

#### Batted-Ball Type

- Ground Ball %
- Line Drive %
- Fly Ball %
- Pop-Up %
- Ground Balls per Plate Appearance
- Fly Balls per Plate Appearance
- Line Drives per Plate Appearance
- Infield Fly %

#### Direction and Spray

- Pull %
- Center %
- Oppo %
- Pull Air %
- Oppo Air %
- Pulled Ground Ball %
- Oppo Ground Ball %
- Straightaway %
- Shifted Plate Appearance %
- Non-Shifted Plate Appearance %

#### Plate Discipline

- Swing %
- Swing Outside Zone % (O-Swing%)
- Swing Inside Zone % (Z-Swing%)
- Contact %
- Contact Outside Zone % (O-Contact%)
- Contact Inside Zone % (Z-Contact%)
- Whiff %
- Called Strike %
- Swinging Strike %
- Foul %
- Foul Tip %
- In-Play %
- Take %
- Take in Zone %
- Take out of Zone %
- First-Pitch Swing %
- First-Pitch Take %
- Two-Strike Swing %
- Two-Strike Whiff %

#### Contact Quality Buckets

- Under %
- Topped %
- Flare/Burner %
- Solid Contact %
- Weak Contact %
- Poorly Hit %
- Poorly Under %
- Poorly Topped %
- Poorly Weak %

#### Count and Context

- Ahead-in-Count %
- Even-Count %
- Behind-in-Count %
- Two-Strike Plate Appearance %
- Three-Ball Plate Appearance %
- Late and Close Plate Appearances
- Leverage Index

### Pitcher Stats

#### Outcomes and Rates

- Games (G)
- Games Started (GS)
- Innings Pitched (IP)
- Batters Faced (BF)
- Hits Allowed (H)
- Runs Allowed (R)
- Earned Runs (ER)
- Home Runs Allowed (HR)
- Walks Allowed (BB)
- Hit Batters (HBP)
- Strikeouts (SO)
- Earned Run Average (ERA)
- Walks plus Hits per Inning Pitched (WHIP)
- Strikeouts per Nine (K/9)
- Walks per Nine (BB/9)
- Home Runs per Nine (HR/9)
- Strikeout Minus Walk Rate (K-BB%)

#### Expected and Contact Allowed

- Expected ERA (xERA)
- Expected Weighted On-Base Average Allowed (xwOBA)
- Weighted On-Base Average Allowed (wOBA)
- Batting Average Allowed (BAA)
- Slugging Allowed (SLG)
- Average Exit Velocity Allowed
- Max Exit Velocity Allowed
- Exit Velocity Percentiles Allowed (10th, 50th, 90th)
- Barrel % Allowed
- Sweet-Spot % Allowed

#### Pitch Arsenal

- Pitch Type
- Pitch Usage %
- Average Velocity
- Max Velocity
- Velocity Standard Deviation
- Spin Rate
- Spin Rate Standard Deviation
- Spin Axis
- Extension
- Release Height
- Release Side

#### Pitch Results

- Whiff %
- Chase %
- Called Strikes plus Whiffs (CSW%)
- Strike %
- Called Strike %
- Swinging Strike %
- Ground Ball %
- Fly Ball %
- Line Drive %
- Pop-Ups Forced

#### Usage and Sequencing

- Primary Pitch %
- Secondary Pitch %

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
