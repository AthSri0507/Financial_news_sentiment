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
   - query params: `company=Apple`, `sources=rss,newsapi,marketaux,reddit`

## NLP Transition Mode
- Primary scorer: FinBERT (optional) with lexicon fallback
- Fallback trigger: FinBERT unavailable or inference error
- `NLP_FINBERT_MIN_CONFIDENCE` now marks low-confidence FinBERT predictions in metadata (`comparison.low_confidence`), but does not force lexicon fallback
- Comparison fields (FinBERT + lexicon) are stored in `processed_items.model_confidence`

Set in environment:
- `NLP_PREFER_FINBERT=true`
- `NLP_FINBERT_MIN_CONFIDENCE=0.62`
- `HUGGINGFACE_API_KEY=<token>` (optional, for hosted FinBERT inference)

## Evaluate On Labeled Set
Use a CSV with headers: `text,label` where label is `positive|negative|neutral`.

Run evaluation:
- `python scripts/evaluate_sentiment.py path/to/labeled.csv --prefer-finbert --finbert-threshold 0.62 --hf-api-key <token>`

## Migration workflow (Alembic)
Generate a revision:
- `alembic revision -m "init"`

Apply migrations:
- `alembic upgrade head`
