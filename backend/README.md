# Backend Service

## Run locally
1. Create and activate virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Copy root `.env.example` to `.env` and set values.
4. Start API:
   - `uvicorn app.main:app --reload`

## Endpoints
- `GET /health`
- `GET /health/dependencies`
- `POST /ingest/run` (requires `Authorization: Bearer <INGEST_TOKEN>`)
  - Query params: `company=Apple` (default), `sources=rss,newsapi,reddit,x` (comma-separated)

## Migration workflow (Alembic)
Generate a revision:
- `alembic revision -m "init"`

Apply migrations:
- `alembic upgrade head`
