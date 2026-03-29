# Financial News Sentiment Platform

A full-stack project that ingests financial news from multiple sources, enriches it with NLP, computes impact-weighted sentiment trends, and visualizes cross-sector analytics in an interactive dashboard.

## What This Project Does

- Ingests financial content from RSS, NewsAPI, and Marketaux.
- Normalizes and stores raw records in Postgres.
- Runs NLP enrichment (sentiment, summary, entities, relevance).
- Computes impact-weighted timeline analytics.
- Computes cross-sector correlation and lead/lag insights.
- Exposes a FastAPI backend and a static dashboard UI.

## High-Level Architecture

1. Connectors collect raw articles/posts.
2. Raw data is stored in `raw_items`.
3. NLP pipeline processes raw data into `processed_items`.
4. Analytics aggregates into `time_series_sentiment` and `sector_correlations`.
5. Dashboard queries backend endpoints and renders charts/cards.

## Repository Structure

- `backend/`: FastAPI app, connectors, NLP, analytics, DB models.
- `dashboard/`: Static frontend (HTML/CSS/JS + Plotly charts).
- `workers/`: Utility scripts for ingestion triggers.
- `.github/workflows/`: CI and scheduled ingestion workflows.
- `infra/`: Deployment/infrastructure notes.
- `render.yaml`: Render service configuration.

## Prerequisites

- Python 3.11+
- A PostgreSQL database (Supabase is used in this project)
- Optional API keys for richer ingestion:
  - NewsAPI
  - Marketaux
- Windows PowerShell commands below (works similarly in bash/zsh)

## Quick Start (Local)

### 1) Clone and create virtual environment

```powershell
cd C:\path\to\parent
git clone <your-repo-url> financial_news
cd financial_news
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2) Install backend dependencies

```powershell
cd backend
pip install -r requirements.txt
cd ..
```

### 3) Configure environment

- Copy values from [.env.example](.env.example) into [backend/.env](backend/.env).
- Required values to run backend fully:
  - `DATABASE_URL`
  - `INGEST_TOKEN`
- Strongly recommended:
  - `NEWSAPI_KEY`
  - `MARKETAUX_API_KEY`

### 4) Start backend API

```powershell
Set-Location "C:\Users\Atharva Srivastava\Downloads\financial_news"
& "C:\Users\Atharva Srivastava\Downloads\financial_news\.venv\Scripts\python.exe" -m uvicorn app.main:app --app-dir backend --env-file backend/.env --host 127.0.0.1 --port 8000 --reload
```

### 5) Start dashboard

Open a second terminal:

```powershell
Set-Location "C:\Users\Atharva Srivastava\Downloads\financial_news\dashboard"
& "C:\Users\Atharva Srivastava\Downloads\financial_news\.venv\Scripts\python.exe" -m http.server 5500
```

Open:

- `http://127.0.0.1:5500/index.html`

Set API Base URL to:

- `http://127.0.0.1:8000`

## Core API Endpoints

- `GET /health`
- `GET /health/dependencies`
- `POST /ingest/run` (requires Bearer token)
- `POST /enrich/run` (requires Bearer token)
- `POST /query`
- `GET /items`
- `GET /timeline`
- `GET /sector-insights`

Backend reference: [backend/README.md](backend/README.md)

## Typical Data Flow Commands

### Trigger ingestion for one company

```powershell
$token = "<INGEST_TOKEN>"
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8000/ingest/run?company=Apple&sources=rss,newsapi,marketaux" -Headers @{ Authorization = "Bearer $token" }
```

### Trigger enrichment for one company

```powershell
$token = "<INGEST_TOKEN>"
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8000/enrich/run?company=Apple&limit=100&min_relevance=0.20" -Headers @{ Authorization = "Bearer $token" }
```

### Query analytics payload

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/query?company=Apple&bucket=hour&window_days=14&item_limit=20&recompute_timeline=true" -Method Post | ConvertTo-Json -Depth 5
```

## CI and Scheduled Jobs

- CI (lint + tests): [.github/workflows/ci.yml](.github/workflows/ci.yml)
- Scheduled ingestion trigger: [.github/workflows/scheduled-ingestion.yml](.github/workflows/scheduled-ingestion.yml)

## Deployment

### Render (current template)

- Config file: [render.yaml](render.yaml)
- Root dir: `backend`
- Start command runs Uvicorn
- Set `DATABASE_URL` and `INGEST_TOKEN` in Render secrets

### GCP (planned path)

- Cloud Run for backend service
- Secret Manager for API keys/tokens
- Cloud Scheduler for ingestion/enrichment triggers
- Cloud Logging/Monitoring for alerts

## Troubleshooting

### Dashboard stuck at "Checking API..."

1. Confirm backend is running on `127.0.0.1:8000`.
2. Confirm dashboard server is running from [dashboard](dashboard) on port `5500`.
3. Test API health:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/health/dependencies" | ConvertTo-Json -Depth 4
```

### `database not configured` or 503 from query

- Ensure `DATABASE_URL` exists in [backend/.env](backend/.env).
- Start backend with `--env-file backend/.env`.

### Autofetch says NewsAPI/Marketaux key not configured

- Add `NEWSAPI_KEY` and `MARKETAUX_API_KEY` to [backend/.env](backend/.env).
- Restart backend.

### Company casing mismatch (example: `HDFC bank` vs `HDFC Bank`)

- Backend now resolves companies case-insensitively in analytics queries.

## Security Notes

- Do not commit secrets or API keys.
- Rotate tokens if they were ever shared in plaintext.
- Keep [backend/.env](backend/.env) out of git.


