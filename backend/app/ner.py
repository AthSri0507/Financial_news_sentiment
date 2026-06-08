"""HF named-entity recognition for PERSON detection (failure-safe).

Mirrors SentimentEngine's HF-Inference + graceful-fallback design. Proposes
candidate person names from article text; notable_people.py decides which are
market-relevant. On any error (no key, network, parse) it returns no names and a
`watchlist_only` meta so the caller falls back to pure string matching — it
never raises into a request path.
"""

import logging
from typing import Any

import requests

log = logging.getLogger(__name__)


class NEREngine:
    def __init__(
        self,
        prefer_hf: bool = False,
        model_id: str = "dslim/bert-base-NER",
        hf_api_key: str | None = None,
    ):
        self.prefer_hf = prefer_hf
        self.model_id = model_id
        self.hf_api_key = hf_api_key
        self.urls = [
            f"https://router.huggingface.co/hf-inference/models/{model_id}",
            f"https://api-inference.huggingface.co/models/{model_id}",
        ]

    def persons(self, text: str) -> tuple[list[str], dict[str, Any]]:
        """Return (person_names, meta). Never raises."""
        if not self.prefer_hf or not self.hf_api_key or not text:
            return [], {"source": "disabled", "available": False}

        try:
            data = self._infer(text)
            names = self._aggregate_persons(data)
            return names, {"source": "hf", "available": True, "model": self.model_id}
        except Exception as exc:  # pragma: no cover - network/parse dependent
            log.warning("NER inference failed (%s); falling back to watchlist", exc)
            return [], {"source": "watchlist_only", "available": False, "error": str(exc)}

    def _infer(self, text: str) -> list[dict]:
        headers = {
            "Authorization": f"Bearer {self.hf_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "inputs": text[:1500],
            "parameters": {"aggregation_strategy": "simple"},
            "options": {"wait_for_model": True},
        }
        last_error: Exception | None = None
        for url in self.urls:
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=20)
                response.raise_for_status()
                return response.json()
            except Exception as exc:
                last_error = exc
                continue
        if last_error:
            raise last_error
        raise RuntimeError("ner_inference_unavailable")

    @staticmethod
    def _aggregate_persons(data: Any) -> list[str]:
        if not isinstance(data, list):
            return []
        names: list[str] = []
        seen: set[str] = set()
        for token in data:
            if not isinstance(token, dict):
                continue
            group = str(token.get("entity_group") or token.get("entity") or "").upper()
            if "PER" not in group:
                continue
            word = str(token.get("word") or "").replace("##", "").strip()
            key = word.lower()
            if word and key not in seen:
                seen.add(key)
                names.append(word)
        return names
