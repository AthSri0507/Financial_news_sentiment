import re
from dataclasses import dataclass
from typing import Any


DEFAULT_COMPANY_ALIASES: dict[str, list[str]] = {
    "apple": ["apple", "aapl", "apple inc", "iphone", "ipad", "mac"],
    "microsoft": ["microsoft", "msft", "azure", "xbox", "windows"],
    "amazon": ["amazon", "amzn", "aws", "prime video", "kindle"],
    "google": ["google", "alphabet", "goog", "googl", "youtube", "android"],
    "tesla": ["tesla", "tsla", "elon musk", "model 3", "model y"],
}

POSITIVE_WORDS = {
    "growth",
    "beat",
    "bullish",
    "surge",
    "gain",
    "profit",
    "strong",
    "outperform",
    "record",
    "upgrade",
}

NEGATIVE_WORDS = {
    "drop",
    "miss",
    "bearish",
    "fall",
    "loss",
    "weak",
    "downgrade",
    "lawsuit",
    "decline",
    "cut",
}

NOISE_PATTERNS = [
    "click here",
    "subscribe now",
    "buy now",
    "free crypto",
    "get rich quick",
]

ENGLISH_HINT_WORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "this",
    "that",
    "will",
    "company",
    "market",
}


@dataclass
class EnrichmentResult:
    cleaned_text: str
    language: str
    is_noise: bool
    summary: str
    sentiment_label: str
    sentiment_score: float
    relevance_score: float
    entities: list[str]
    model_confidence: dict[str, Any]
    pipeline_flags: dict[str, Any]


def clean_text(text: str) -> str:
    text = text or ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def detect_language(text: str) -> str:
    if not text:
        return "unknown"

    lowered = text.lower()
    tokens = re.findall(r"[a-zA-Z]+", lowered)
    if not tokens:
        return "unknown"

    english_hits = sum(1 for token in tokens if token in ENGLISH_HINT_WORDS)
    ascii_ratio = sum(1 for ch in text if ord(ch) < 128) / max(len(text), 1)

    if ascii_ratio > 0.9 and english_hits >= 2:
        return "en"
    if ascii_ratio > 0.85 and len(tokens) >= 8:
        return "en"
    return "unknown"


def is_noise_text(text: str) -> bool:
    lowered = (text or "").lower()
    if len(lowered) < 40:
        return True

    for pattern in NOISE_PATTERNS:
        if pattern in lowered:
            return True

    link_count = len(re.findall(r"https?://", lowered))
    if link_count >= 3:
        return True

    # Detect obviously repetitive spam blocks.
    if re.search(r"(.)\1{6,}", lowered):
        return True

    return False


def summarize_text(text: str, max_sentences: int = 2) -> str:
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
    if not sentences:
        return ""

    if len(sentences) <= max_sentences:
        return " ".join(sentences)

    return " ".join(sentences[:max_sentences])


def extract_entities(text: str) -> list[str]:
    entities: set[str] = set()

    ticker_hits = re.findall(r"\$[A-Z]{1,5}\b", text)
    for hit in ticker_hits:
        entities.add(hit)

    proper_noun_hits = re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\b", text)
    for hit in proper_noun_hits:
        if len(hit) > 2:
            entities.add(hit)

    return sorted(entities)[:20]


def get_company_aliases(company: str) -> list[str]:
    company_key = company.lower().strip()
    aliases = DEFAULT_COMPANY_ALIASES.get(company_key)
    if aliases:
        return aliases

    words = [w for w in re.split(r"\s+", company_key) if w]
    return list({company_key, *words})


def relevance_score(company: str, text: str, entities: list[str]) -> float:
    aliases = get_company_aliases(company)
    lowered = text.lower()

    score = 0.0
    alias_hits = 0

    for alias in aliases:
        if alias and alias in lowered:
            alias_hits += 1

    if alias_hits > 0:
        score += min(0.7, 0.25 + alias_hits * 0.15)

    company_lower = company.lower()
    entity_hits = sum(1 for entity in entities if company_lower in entity.lower())
    if entity_hits > 0:
        score += min(0.2, entity_hits * 0.1)

    finance_terms = ["stock", "shares", "earnings", "revenue", "market", "guidance"]
    finance_hits = sum(1 for term in finance_terms if term in lowered)
    score += min(0.15, finance_hits * 0.03)

    unrelated_penalties = ["sports", "celebrity", "movie trailer", "lottery"]
    if any(term in lowered for term in unrelated_penalties):
        score -= 0.2

    return round(max(0.0, min(1.0, score)), 4)


class SentimentEngine:
    """FinBERT-first sentiment analyzer with lightweight lexical fallback."""

    def __init__(self, prefer_finbert: bool = True):
        self.mode = "lexicon"
        self._classifier = None

        if prefer_finbert:
            try:
                from transformers import pipeline  # type: ignore

                self._classifier = pipeline(
                    "sentiment-analysis",
                    model="ProsusAI/finbert",
                    tokenizer="ProsusAI/finbert",
                )
                self.mode = "finbert"
            except Exception:
                self._classifier = None
                self.mode = "lexicon"

    def analyze(self, text: str) -> tuple[str, float, dict[str, Any]]:
        if self.mode == "finbert" and self._classifier is not None:
            try:
                result = self._classifier(text[:512])[0]
                label = (result.get("label") or "neutral").lower()
                score = float(result.get("score") or 0.5)

                mapped_label = label
                mapped_score = score
                if label == "positive":
                    mapped_score = score
                elif label == "negative":
                    mapped_score = -score
                else:
                    mapped_label = "neutral"
                    mapped_score = 0.0

                return mapped_label, round(mapped_score, 4), {
                    "model": "finbert",
                    "confidence": round(score, 4),
                }
            except Exception as exc:
                return self._lexical(text, error=str(exc))

        return self._lexical(text)

    def _lexical(self, text: str, error: str | None = None) -> tuple[str, float, dict[str, Any]]:
        tokens = re.findall(r"[a-zA-Z]+", text.lower())
        if not tokens:
            return "neutral", 0.0, {"model": "lexicon", "confidence": 0.0, "error": error}

        pos = sum(1 for token in tokens if token in POSITIVE_WORDS)
        neg = sum(1 for token in tokens if token in NEGATIVE_WORDS)

        denom = max(pos + neg, 1)
        raw_score = (pos - neg) / denom
        confidence = min(1.0, (pos + neg) / max(len(tokens) / 6, 1))

        if raw_score > 0.1:
            label = "positive"
        elif raw_score < -0.1:
            label = "negative"
        else:
            label = "neutral"

        payload: dict[str, Any] = {
            "model": "lexicon",
            "confidence": round(confidence, 4),
            "positive_hits": pos,
            "negative_hits": neg,
        }
        if error:
            payload["error"] = error

        return label, round(raw_score, 4), payload


def enrich_text(company: str, title: str, content: str, sentiment_engine: SentimentEngine) -> EnrichmentResult:
    combined = f"{title or ''}. {content or ''}".strip()
    cleaned = clean_text(combined)
    language = detect_language(cleaned)
    noise = is_noise_text(cleaned)
    summary = summarize_text(cleaned)
    entities = extract_entities(cleaned)
    relevance = relevance_score(company, cleaned, entities)

    sentiment_label = "neutral"
    sentiment_score = 0.0
    sentiment_meta: dict[str, Any] = {"model": "none", "confidence": 0.0}

    if cleaned and language == "en" and not noise:
        sentiment_label, sentiment_score, sentiment_meta = sentiment_engine.analyze(cleaned)

    return EnrichmentResult(
        cleaned_text=cleaned,
        language=language,
        is_noise=noise,
        summary=summary,
        sentiment_label=sentiment_label,
        sentiment_score=sentiment_score,
        relevance_score=relevance,
        entities=entities,
        model_confidence=sentiment_meta,
        pipeline_flags={
            "language_ok": language == "en",
            "noise_filtered": noise,
        },
    )
