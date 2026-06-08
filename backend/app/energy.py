"""Estimated model energy & carbon from inference counts.

This is an ESTIMATE, not a measurement: most inference runs remotely on Hugging
Face (FinBERT/NER/summaries), which cannot be metered from this host. We count
each model call from the metadata already stored on `processed_items` and
multiply by configurable per-inference energy factors (Wh) and a grid carbon
intensity (gCO2/kWh). All factors are config-driven so the methodology is
transparent and adjustable.
"""

from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import ProcessedItem


def _flag_source(blob: object, *path: str) -> str | None:
    cur = blob
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur if isinstance(cur, str) else None


def estimate_energy(db_session: Session) -> dict[str, object]:
    """Aggregate inference counts and estimate total kWh + gCO2e."""
    settings = get_settings()

    rows = db_session.query(
        ProcessedItem.model_confidence, ProcessedItem.pipeline_flags
    ).all()

    counts = {
        "total_items": 0,
        "finbert": 0,
        "lexicon": 0,
        "hf_summary": 0,
        "extractive_summary": 0,
        "hf_ner": 0,
    }
    for model_confidence, pipeline_flags in rows:
        counts["total_items"] += 1
        sentiment_source = _flag_source(model_confidence, "final_source")
        if sentiment_source == "finbert":
            counts["finbert"] += 1
        elif sentiment_source == "lexicon":
            counts["lexicon"] += 1

        summary_source = _flag_source(pipeline_flags, "summary", "source")
        if summary_source == "hf":
            counts["hf_summary"] += 1
        elif summary_source == "extractive":
            counts["extractive_summary"] += 1

        if _flag_source(pipeline_flags, "ner", "source") == "hf":
            counts["hf_ner"] += 1

    factors = {
        "wh_per_finbert": settings.energy_wh_per_finbert,
        "wh_per_hf_summary": settings.energy_wh_per_hf_summary,
        "wh_per_hf_ner": settings.energy_wh_per_hf_ner,
        "wh_per_lexicon_item": settings.energy_wh_per_lexicon_item,
        "wh_base_per_item": settings.energy_wh_base_per_item,
        "grid_gco2_per_kwh": settings.energy_grid_gco2_per_kwh,
    }

    energy_wh = (
        counts["finbert"] * factors["wh_per_finbert"]
        + counts["lexicon"] * factors["wh_per_lexicon_item"]
        + counts["hf_summary"] * factors["wh_per_hf_summary"]
        + counts["hf_ner"] * factors["wh_per_hf_ner"]
        + counts["total_items"] * factors["wh_base_per_item"]
    )
    energy_kwh = energy_wh / 1000.0
    co2_g = energy_kwh * factors["grid_gco2_per_kwh"]

    return {
        "status": "success",
        "counts": counts,
        "factors": factors,
        "energy_wh": round(energy_wh, 4),
        "energy_kwh": round(energy_kwh, 6),
        "co2_g": round(co2_g, 4),
        "estimated": True,
        "methodology": (
            "Estimate, not measured. Remote Hugging Face inference cannot be metered "
            "locally, so energy = Σ(inference_count × per-inference Wh factor) + "
            "base Wh/item; CO2e = kWh × grid intensity. Factors are configurable."
        ),
    }
