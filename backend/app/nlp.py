import html
import logging
import re
from dataclasses import dataclass, field
from typing import Any

import requests

try:
    import ftfy
except Exception:  # pragma: no cover - optional dependency
    ftfy = None

from app import notable_people as notable_people_mod
from app.ner import NEREngine

log = logging.getLogger(__name__)

# Signals that a sentence carries financial substance (numbers, %, money words).
_FINANCE_SIGNAL = re.compile(
    r"\d|%|\b(stock|shares?|earnings|revenue|profit|loss(?:es)?|guidance|debt|"
    r"cent|rs|crore|lakh|billion|million|deal|order|fall|rise|surge|drop)\b",
    re.IGNORECASE,
)

# Company/result-level finance keywords that mark a sentence as on-topic for
# sentiment (per the desired sentiment-text selection).
_FINANCE_KEYWORDS = re.compile(
    r"\b(profit|revenue|guidance|earnings|upgrade|downgrade|target price|growth|"
    r"loss(?:es)?|acquisition|contract|expansion|bullish|bearish|gain|rally|"
    r"results?|margin|dividend|order book|stake)\b",
    re.IGNORECASE,
)

# Broad-market / macro sentences that are usually unrelated noise for a single
# stock's sentiment. Dropped unless the sentence also names the company.
_MACRO_NOISE = re.compile(
    r"\b(sensex|nifty|gift nifty|global markets?|world markets?|asian markets?|"
    r"european markets?|wall street|dow(?: jones)?|nasdaq|s&p|treasury yields?|"
    r"bond yields?|crude|brent|wti|oil prices?|dollar index|forex|"
    r"broader market|benchmark index)\b",
    re.IGNORECASE,
)

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
    notable_people: list[dict] = field(default_factory=list)


def clean_text(text: str) -> str:
    text = text or ""
    # Repair recoverable mojibake (â€™, Â£, …) before stripping. Pure U+FFFD (�)
    # was lost at fetch time and cannot be recovered here.
    if ftfy is not None:
        try:
            text = ftfy.fix_text(text)
        except Exception:  # pragma: no cover - defensive
            pass
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = html.unescape(text)  # &nbsp; &amp; &#39; → spaces/chars
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _split_sentences(text: str) -> list[str]:
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+", text or "") if s.strip()]


def _norm_sentence(sentence: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", sentence.lower()).strip()


def _sentence_signature(sentence: str) -> str:
    """Leading-token fingerprint so near-duplicates (title vs. 'title + more')
    collapse together, not just exact repeats."""
    return " ".join(_norm_sentence(sentence).split()[:8])


def dedupe_sentences(text: str) -> str:
    """Order-preserving removal of duplicate/near-duplicate sentences.

    News feeds commonly set `content` to the title, then the title again with a
    bit more text. Those share a leading-token signature, so we collapse them and
    keep the **longest** (most informative) variant for each signature — fixing
    the "summary = repeated title" pattern.
    """
    order: list[str] = []
    best: dict[str, str] = {}
    for sentence in _split_sentences(text):
        sig = _sentence_signature(sentence)
        if not sig:
            continue
        if sig not in best:
            best[sig] = sentence
            order.append(sig)
        elif len(sentence) > len(best[sig]):
            best[sig] = sentence
    return " ".join(best[sig] for sig in order)


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
    # De-duplicate (incl. near-duplicate title echoes) first so a repeated title
    # doesn't fill the summary.
    sentences = _split_sentences(dedupe_sentences(text))

    if not sentences:
        return ""
    if len(sentences) <= max_sentences:
        return " ".join(sentences)

    # Keep the lead sentence, then prefer the first finance-bearing sentence so
    # the extractive summary is informative (not just the headline).
    picked = [sentences[0]]
    for sentence in sentences[1:]:
        if _FINANCE_SIGNAL.search(sentence):
            picked.append(sentence)
            break
    for sentence in sentences[1:]:
        if len(picked) >= max_sentences:
            break
        if sentence not in picked:
            picked.append(sentence)
    return " ".join(picked[:max_sentences])


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


def build_sentiment_text(title: str, content: str, company: str) -> str:
    """Headline + subject/finance-relevant body sentences, minus macro noise.

    Sentiment should reflect the article's stance on its SUBJECT, not unrelated
    broad-market context (e.g. "Sensex slipped 117 points...") which can flip the
    label. We keep the headline plus the first few body sentences that mention the
    company or carry a finance signal, and drop macro/market-noise sentences unless
    they also name the company.
    """
    title = clean_text(title or "")
    body = clean_text(content or "")
    aliases = [a for a in get_company_aliases(company) if a]

    kept: list[str] = []
    for sentence in _split_sentences(body):
        low = sentence.lower()
        mentions_company = any(a in low for a in aliases)
        if not mentions_company and _MACRO_NOISE.search(sentence):
            continue
        if (
            mentions_company
            or _FINANCE_KEYWORDS.search(sentence)
            or _FINANCE_SIGNAL.search(sentence)
        ):
            kept.append(sentence)
        if len(kept) >= 3:
            break

    parts = ([title] if title else []) + kept
    text = dedupe_sentences(". ".join(p.rstrip(". ") for p in parts if p))
    return text or title or body[:300]


class SentimentEngine:
    """FinBERT-primary sentiment analyzer with lexicon fallback."""

    HF_INFERENCE_URLS = [
        "https://router.huggingface.co/hf-inference/models/ProsusAI/finbert",
        "https://api-inference.huggingface.co/models/ProsusAI/finbert",
    ]

    def __init__(
        self,
        prefer_finbert: bool = True,
        finbert_min_confidence: float = 0.62,
        hf_api_key: str | None = None,
    ):
        self.prefer_finbert = prefer_finbert
        self.finbert_min_confidence = max(0.0, min(finbert_min_confidence, 1.0))
        self.hf_api_key = hf_api_key
        self.mode = "lexicon"
        self._classifier = None

        if prefer_finbert:
            self.mode = "finbert_primary"
            if not hf_api_key:
                # Optional local fallback for environments where transformers is available.
                try:
                    from transformers import pipeline  # type: ignore

                    self._classifier = pipeline(
                        "sentiment-analysis",
                        model="ProsusAI/finbert",
                        tokenizer="ProsusAI/finbert",
                    )
                except Exception:
                    self._classifier = None

    def analyze(self, text: str) -> tuple[str, float, dict[str, Any]]:
        lex_label, lex_score, lex_meta = self._lexical(text)

        finbert_result = None
        finbert_error = None
        if self.prefer_finbert:
            try:
                finbert_result = self._analyze_finbert(text)
            except Exception as exc:
                finbert_error = str(exc)

        if finbert_result:
            fin_label, fin_score, fin_meta = finbert_result
            confidence = float(fin_meta.get("confidence", 0.0))
            # Policy: if FinBERT is available, trust FinBERT as final scorer.
            final_label = fin_label
            final_score = fin_score
            final_source = "finbert"
            fallback_reason = None
            low_confidence = confidence < self.finbert_min_confidence
        else:
            fin_label, fin_score, fin_meta = "neutral", 0.0, {
                "model": "finbert",
                "confidence": 0.0,
                "available": False,
            }
            final_label = lex_label
            final_score = lex_score
            final_source = "lexicon"
            fallback_reason = "finbert_unavailable"
            low_confidence = None
            if finbert_error:
                fin_meta["error"] = finbert_error

        agreement = (fin_label == lex_label) if finbert_result else None

        comparison_meta: dict[str, Any] = {
            "model": final_source,
            "final_source": final_source,
            "confidence": round(
                float(fin_meta.get("confidence", lex_meta.get("confidence", 0.0))),
                4,
            ),
            "finbert_threshold": self.finbert_min_confidence,
            "fallback_reason": fallback_reason,
            "finbert": {
                "label": fin_label,
                "score": round(fin_score, 4),
                **fin_meta,
            },
            "lexicon": {
                "label": lex_label,
                "score": round(lex_score, 4),
                **lex_meta,
            },
            "comparison": {
                "agreement": agreement,
                "score_gap": round(abs(fin_score - lex_score), 4) if finbert_result else None,
                "low_confidence": low_confidence,
            },
        }

        return final_label, round(final_score, 4), comparison_meta

    def _analyze_finbert(self, text: str) -> tuple[str, float, dict[str, Any]]:
        if self.hf_api_key:
            headers = {
                "Authorization": f"Bearer {self.hf_api_key}",
                "Content-Type": "application/json",
            }
            payload = {
                "inputs": text[:1500],
                "options": {"wait_for_model": True},
            }
            last_error: Exception | None = None
            data = None
            for url in self.HF_INFERENCE_URLS:
                try:
                    response = requests.post(
                        url,
                        headers=headers,
                        json=payload,
                        timeout=20,
                    )
                    response.raise_for_status()
                    data = response.json()
                    break
                except Exception as exc:
                    last_error = exc
                    continue

            if data is None:
                if last_error:
                    raise last_error
                raise RuntimeError("finbert_inference_unavailable")

            # Most responses are [[{label, score}, ...]] from text-classification models.
            if isinstance(data, list) and data and isinstance(data[0], list):
                candidates = data[0]
            elif isinstance(data, list):
                candidates = data
            else:
                raise RuntimeError("unexpected_finbert_response")

            if not candidates:
                raise RuntimeError("empty_finbert_response")

            top = max(candidates, key=lambda x: float(x.get("score", 0.0)))
            return self._normalize_finbert_label(top)

        if self._classifier is not None:
            top = self._classifier(text[:512])[0]
            return self._normalize_finbert_label(top)

        raise RuntimeError("finbert_not_configured")

    def _normalize_finbert_label(self, result: dict[str, Any]) -> tuple[str, float, dict[str, Any]]:
        raw_label = str(result.get("label") or "neutral").lower()
        confidence = float(result.get("score") or 0.0)

        # FinBERT labels are usually positive/neutral/negative.
        if raw_label.startswith("pos"):
            label = "positive"
            score = confidence
        elif raw_label.startswith("neg"):
            label = "negative"
            score = -confidence
        else:
            label = "neutral"
            score = 0.0

        return label, round(score, 4), {
            "model": "finbert",
            "confidence": round(confidence, 4),
            "available": True,
            "raw_label": raw_label,
        }

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


class SummarizationEngine:
    """HF abstractive summarizer with extractive fallback.

    Mirrors SentimentEngine's HF-Inference + graceful-fallback design: tries an
    abstractive model when enabled and the text is long enough; otherwise (or on
    any error / low-quality output) falls back to the extractive summarize_text.
    Never raises.
    """

    def __init__(
        self,
        prefer_hf: bool = False,
        model_id: str = "sshleifer/distilbart-cnn-12-6",
        hf_api_key: str | None = None,
        min_chars: int = 200,
        max_input_chars: int = 1500,
    ):
        self.prefer_hf = prefer_hf
        self.model_id = model_id
        self.hf_api_key = hf_api_key
        self.min_chars = min_chars
        self.max_input_chars = max_input_chars
        self.urls = [
            f"https://router.huggingface.co/hf-inference/models/{model_id}",
            f"https://api-inference.huggingface.co/models/{model_id}",
        ]

    def summarize(self, text: str, fallback_max_sentences: int = 2) -> tuple[str, dict[str, Any]]:
        extractive = summarize_text(text, max_sentences=fallback_max_sentences)
        if not self.prefer_hf or not self.hf_api_key or len(text or "") < self.min_chars:
            return extractive, {"source": "extractive", "available": False}

        try:
            hf_summary = self._summarize_hf(text)
            # Quality guard: non-empty, not a verbatim echo, sensibly shorter.
            if hf_summary and hf_summary.strip() and hf_summary.strip() != text.strip():
                return hf_summary.strip(), {
                    "source": "hf",
                    "available": True,
                    "model": self.model_id,
                }
            return extractive, {
                "source": "extractive",
                "available": True,
                "fallback_reason": "low_quality",
            }
        except Exception as exc:  # pragma: no cover - network/parse dependent
            log.warning("HF summarization failed (%s); using extractive", exc)
            return extractive, {
                "source": "extractive",
                "available": False,
                "fallback_reason": str(exc),
            }

    def _summarize_hf(self, text: str) -> str:
        headers = {
            "Authorization": f"Bearer {self.hf_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "inputs": text[: self.max_input_chars],
            "options": {"wait_for_model": True},
        }
        last_error: Exception | None = None
        for url in self.urls:
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=20)
                response.raise_for_status()
                data = response.json()
                return self._parse_summary(data)
            except Exception as exc:
                last_error = exc
                continue
        if last_error:
            raise last_error
        raise RuntimeError("summary_inference_unavailable")

    @staticmethod
    def _parse_summary(data: Any) -> str:
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return str(data[0].get("summary_text") or "")
        if isinstance(data, dict):
            return str(data.get("summary_text") or "")
        return ""


def enrich_text(
    company: str,
    title: str,
    content: str,
    sentiment_engine: SentimentEngine,
    ner_engine: NEREngine | None = None,
    summarizer: "SummarizationEngine | None" = None,
) -> EnrichmentResult:
    combined = f"{title or ''}. {content or ''}".strip()
    # Repair encoding, strip markup, THEN drop repeated sentences (feeds often
    # repeat the title in `content`) so summary + sentiment see clean, unique text.
    cleaned = dedupe_sentences(clean_text(combined))
    language = detect_language(cleaned)
    noise = is_noise_text(cleaned)
    if summarizer is not None:
        summary, summary_meta = summarizer.summarize(cleaned)
    else:
        summary = summarize_text(cleaned)
        summary_meta = {"source": "extractive", "available": False}
    entities = extract_entities(cleaned)
    relevance = relevance_score(company, cleaned, entities)

    sentiment_label = "neutral"
    sentiment_score = 0.0
    sentiment_meta: dict[str, Any] = {"model": "none", "confidence": 0.0}
    sentiment_input = "none"

    if cleaned and language == "en" and not noise:
        # Score sentiment on a subject-relevant slice (headline + company/finance
        # sentences, macro-noise dropped) rather than the whole body, so off-topic
        # market context can't flip the label.
        sent_text = build_sentiment_text(title, content, company)
        sentiment_input = "subject_relevant"
        sentiment_label, sentiment_score, sentiment_meta = sentiment_engine.analyze(
            sent_text or cleaned
        )

    # Notable people: HF NER proposes person candidates (best-effort); the curated
    # watchlist also scans the text directly, so detection works even if NER is off.
    ner_names: list[str] = []
    ner_meta: dict[str, Any] = {"source": "disabled", "available": False}
    if ner_engine is not None:
        ner_names, ner_meta = ner_engine.persons(cleaned)
    notable = notable_people_mod.detect(cleaned, ner_names)

    return EnrichmentResult(
        cleaned_text=cleaned,
        language=language,
        is_noise=noise,
        summary=summary,
        sentiment_label=sentiment_label,
        sentiment_score=sentiment_score,
        relevance_score=relevance,
        entities=entities,
        notable_people=notable,
        model_confidence=sentiment_meta,
        pipeline_flags={
            "language_ok": language == "en",
            "noise_filtered": noise,
            "ner": ner_meta,
            "summary": summary_meta,
            "sentiment_input": sentiment_input,
        },
    )
