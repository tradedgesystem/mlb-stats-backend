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

### Mapping to current DB columns (batting_stats)

Notes:
- Mappings below use current `batting_stats` columns from pybaseball.
- "Not in current dataset" means we do not have that stat yet.
- "Derived" indicates a formula using existing columns.

#### Hitter Stats - Outcome and Slash

- Plate Appearances (PA) -> `pa`
- At-Bats (AB) -> `ab`
- Hits (H) -> `h`
- Singles (1B) -> `1b`
- Doubles (2B) -> `2b`
- Triples (3B) -> `3b`
- Home Runs (HR) -> `hr`
- Runs (R) -> `r`
- Runs Batted In (RBI) -> `rbi`
- Walks (BB) -> `bb`
- Intentional Walks (IBB) -> `ibb`
- Hit By Pitch (HBP) -> `hbp`
- Strikeouts (SO) -> `so`
- Sacrifice Flies (SF) -> `sf`
- Sacrifice Hits (SH) -> `sh`
- Batting Average (AVG) -> `avg`
- On-Base Percentage (OBP) -> `obp`
- Slugging Percentage (SLG) -> `slg`
- On-Base Plus Slugging (OPS) -> `ops`
- Isolated Power (ISO) -> `iso`
- Batting Average on Balls in Play (BABIP) -> `babip`

#### Hitter Stats - Expected and Statcast Quality

- Expected Batting Average (xBA) -> `xba`
- Expected Slugging (xSLG) -> `xslg`
- Expected On-Base Percentage (xOBP) -> not in current dataset
- Expected Weighted On-Base Average (xwOBA) -> `xwoba`
- Weighted On-Base Average (wOBA) -> `woba`
- Expected Home Runs (xHR) -> not in current dataset

#### Hitter Stats - Contact and Power

- Average Exit Velocity -> `ev`
- Max Exit Velocity -> `maxev`
- Median Exit Velocity -> not in current dataset
- Exit Velocity Percentiles (10th, 50th, 90th) -> not in current dataset
- Hard-Hit % -> `hardhitpct`
- Barrel Count -> `barrels`
- Barrel % -> `barrelpct`
- Barrels per Plate Appearance -> derived: `barrels / pa`
- Barrels per Ball in Play -> not in current dataset
- Sweet-Spot % -> not in current dataset
- Average Launch Angle -> `la`
- Launch Angle Standard Deviation -> not in current dataset

#### Hitter Stats - Batted-Ball Type

- Ground Ball % -> `gbpct`
- Line Drive % -> `ldpct`
- Fly Ball % -> `fbpct`
- Pop-Up % -> `iffbpct`
- Ground Balls per Plate Appearance -> derived: `gb / pa`
- Fly Balls per Plate Appearance -> derived: `fb / pa`
- Line Drives per Plate Appearance -> derived: `ld / pa`
- Infield Fly % -> `iffbpct`

#### Hitter Stats - Direction and Spray

- Pull % -> `pullpct`
- Center % -> `centpct`
- Oppo % -> `oppopct`
- Pull Air % -> not in current dataset
- Oppo Air % -> not in current dataset
- Pulled Ground Ball % -> not in current dataset
- Oppo Ground Ball % -> not in current dataset
- Straightaway % -> `centpct` (center%)
- Shifted Plate Appearance % -> not in current dataset
- Non-Shifted Plate Appearance % -> not in current dataset

#### Hitter Stats - Plate Discipline

- Swing % -> `swingpct`
- Swing Outside Zone % (O-Swing%) -> `o_swingpct`
- Swing Inside Zone % (Z-Swing%) -> `z_swingpct`
- Contact % -> `contactpct`
- Contact Outside Zone % (O-Contact%) -> `o_contactpct`
- Contact Inside Zone % (Z-Contact%) -> `z_contactpct`
- Whiff % -> `swstrpct` (closest available)
- Called Strike % -> `cstrpct`
- Swinging Strike % -> `swstrpct`
- Foul % -> not in current dataset
- Foul Tip % -> not in current dataset
- In-Play % -> not in current dataset
- Take % -> not in current dataset
- Take in Zone % -> not in current dataset
- Take out of Zone % -> not in current dataset
- First-Pitch Swing % -> not in current dataset
- First-Pitch Take % -> not in current dataset
- Two-Strike Swing % -> not in current dataset
- Two-Strike Whiff % -> not in current dataset

#### Hitter Stats - Contact Quality Buckets

- Under % -> not in current dataset
- Topped % -> not in current dataset
- Flare/Burner % -> not in current dataset
- Solid Contact % -> not in current dataset
- Weak Contact % -> not in current dataset
- Poorly Hit % -> not in current dataset
- Poorly Under % -> not in current dataset
- Poorly Topped % -> not in current dataset
- Poorly Weak % -> not in current dataset

#### Hitter Stats - Count and Context

- Ahead-in-Count % -> not in current dataset
- Even-Count % -> not in current dataset
- Behind-in-Count % -> not in current dataset
- Two-Strike Plate Appearance % -> not in current dataset
- Three-Ball Plate Appearance % -> not in current dataset
- Late and Close Plate Appearances -> not in current dataset
- Leverage Index -> `pli` (avg LI)

### Pitcher Stats

All pitcher stat mappings are pending because we do not ingest a pitching table
yet. A pitching dataset (e.g., `pitching_stats`) is required before mapping.

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

### 4) Stats config + selector UI

- Added `extension/stats_config.json` as the stats source of truth.
- Extension renders a stats checkbox list grouped by category.
- Compare/View output is filtered to selected stats only.
- Output formatting follows `stats_config.json` (percent/rate/float/integer).
- Popup renders a readable table instead of raw JSON.
- Snapshot freshness warning shows if older than 36 hours.
- Added a Stats tab with a glossary description for each stat.
- Selection limit enforces a max of 10 stats with a warning message.
- Stats tab shows a live counter for selected stats.
- Added separate Players and Compare tabs to split individual vs comparison flow.

### 5) Derived stats + snapshot export

- Ingestion computes:
  - `barrels_per_pa`
  - `gb_per_pa`
  - `fb_per_pa`
  - `ld_per_pa`
- Added `backend/export_snapshot.py` to write compact per-year JSON using the
  config list.
- Snapshots include `meta.generated_at` and `meta.player_count`.
- Extension loads `extension/snapshots/players_{year}.json`.
- Extension now pulls snapshots from the hosted URL (jsDelivr).

### 6) Snapshot hosting + automation

- Snapshots are hosted via jsDelivr (backed by the GitHub repo).
- Scheduled GitHub Actions workflow regenerates snapshots nightly.

## Remaining Work

### 7) Individual player view

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

- **Precompute data once per day** via a scheduled job (overnight).
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

### 8) Ingestion expansion (controlled)

- Multi-year data (historical seasons).
- Optional pitching stats.
- Scheduled updates (ingestion not user-triggered).

### 9) Deployment (required to sell)

- Host the FastAPI API publicly (Render or similar).
- Automate daily ingestion (GitHub Actions).
- Use persistent storage (Postgres or durable SQLite volume).

### 10) Basic protection and scaling

- API keys per user or license.
- Rate limiting.
- Basic logging/monitoring.

### 11) Chrome Web Store readiness

- Clean UI and versioning.
- Privacy policy.
- Minimal permissions.
- Screenshots and description.

### 12) Product polish

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

Replace the JSON output with a simple table view in the extension popup for
better readability.
