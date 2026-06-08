import logging

from sqlalchemy.orm import Session

from app.models import ProcessedItem, RawItem
from app.ner import NEREngine
from app.nlp import SentimentEngine, SummarizationEngine, enrich_text

log = logging.getLogger(__name__)


def run_enrichment_pipeline(
    db_session: Session,
    company: str,
    limit: int = 30,
    min_relevance: float = 0.25,
    max_text_chars: int = 6000,
    prefer_finbert: bool = False,
    finbert_min_confidence: float = 0.62,
    hf_api_key: str | None = None,
    prefer_hf_ner: bool = False,
    ner_model_id: str = "dslim/bert-base-NER",
    prefer_hf_summary: bool = False,
    summary_model_id: str = "sshleifer/distilbart-cnn-12-6",
    summary_min_chars: int = 200,
    summary_max_input_chars: int = 1500,
) -> dict[str, object]:
    """Enrich recent raw items and persist processed results."""

    limit = max(1, min(limit, 200))
    max_text_chars = max(500, min(max_text_chars, 20000))

    sentiment_engine = SentimentEngine(
        prefer_finbert=prefer_finbert,
        finbert_min_confidence=finbert_min_confidence,
        hf_api_key=hf_api_key,
    )
    ner_engine = NEREngine(
        prefer_hf=prefer_hf_ner,
        model_id=ner_model_id,
        hf_api_key=hf_api_key,
    )
    summarizer = SummarizationEngine(
        prefer_hf=prefer_hf_summary,
        model_id=summary_model_id,
        hf_api_key=hf_api_key,
        min_chars=summary_min_chars,
        max_input_chars=summary_max_input_chars,
    )

    existing_processed = {
        raw_id for (raw_id,) in db_session.query(ProcessedItem.raw_item_id).all()
    }

    # Over-fetch generously: in a multi-company world the most-recent raw items
    # belong to many companies, and we must NOT relabel another company's items
    # as this one (the candidate filter below enforces that).
    candidates = (
        db_session.query(RawItem)
        .order_by(RawItem.ingested_at.desc())
        .limit(limit * 20)
        .all()
    )

    target_company = (company or "").strip().lower()

    inserted = 0
    skipped = 0
    failed = 0

    for raw in candidates:
        if inserted >= limit:
            break

        if raw.id in existing_processed:
            skipped += 1
            continue

        # Only enrich raw items that actually belong to this company. Items with
        # no recorded candidates are allowed through (legacy/forward compatible).
        candidate_names = [str(c).strip().lower() for c in (raw.company_candidates or [])]
        if target_company and candidate_names and target_company not in candidate_names:
            continue

        try:
            raw_text = f"{raw.title or ''} {raw.content or ''}".strip()
            if len(raw_text) > max_text_chars:
                raw_text = raw_text[:max_text_chars]

            enriched = enrich_text(
                company=company,
                title=raw.title or "",
                content=raw_text,
                sentiment_engine=sentiment_engine,
                ner_engine=ner_engine,
                summarizer=summarizer,
            )

            if enriched.language != "en":
                skipped += 1
                continue

            if enriched.is_noise:
                skipped += 1
                continue

            if enriched.relevance_score < min_relevance:
                skipped += 1
                continue

            processed = ProcessedItem(
                raw_item_id=raw.id,
                company=company,
                cleaned_text=enriched.cleaned_text,
                language=enriched.language,
                is_noise="true" if enriched.is_noise else "false",
                summary=enriched.summary or (raw.title or ""),
                sentiment_label=enriched.sentiment_label,
                sentiment_score=enriched.sentiment_score,
                relevance_score=enriched.relevance_score,
                entities=enriched.entities,
                notable_people=enriched.notable_people,
                model_confidence=enriched.model_confidence,
                pipeline_flags=enriched.pipeline_flags,
            )

            db_session.add(processed)
            inserted += 1
        except Exception as exc:  # pragma: no cover - defensive runtime safeguard
            failed += 1
            log.warning("NLP enrichment failed for raw_item_id=%s: %s", raw.id, exc)

    if inserted > 0:
        db_session.commit()

    return {
        "status": "success",
        "company": company,
        "processed": inserted,
        "skipped": skipped,
        "failed": failed,
        "model": sentiment_engine.mode,
        "min_relevance": min_relevance,
    }
