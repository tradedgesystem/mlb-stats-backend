# MLB Stats Backend

Minimal read-only API for MLB batting stats. Data is pulled by a one-off ingest script and served from SQLite.

## Quick Start

```bash
python3 -m pip install -r backend/requirements.txt
python3 backend/ingest.py
python3 -m uvicorn backend.api:app --host 127.0.0.1 --port 8000
```

```bash
curl "http://127.0.0.1:8000/players?year=2025"
```

## Notes

- Ingestion uses `pybaseball.batting_stats` for the 2025 season.
- The API is read-only: no auth, no caching, no writes.
- Scraping only happens in `backend/ingest.py`.
- Data is stored in `backend/stats.db`.

## Local Dev Loop

- Ingest (idempotent): `python3 backend/ingest.py`
- API (reload): `python3 -m uvicorn backend.api:app --reload --host 127.0.0.1 --port 8000`
- Extension: `chrome://extensions` → Developer mode → Load unpacked → select `extension/`

If the extension cannot fetch, confirm the API is running and check the browser
console for CORS errors. The API includes local-dev CORS allowances for
`http://127.0.0.1:8000`, `http://localhost:8000`, and `chrome-extension://*`.
