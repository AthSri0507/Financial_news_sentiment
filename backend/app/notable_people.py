"""Curated market-mover watchlist for "who said it" detection.

Mirrors the curated-data pattern in sectors.py. A small list of people whose
statements move markets (politicians, central bankers, prominent CEOs/investors),
each with a role and a boost weight in [0,1]. Used to:
  - detect notable people in article text (string match), independent of NER,
  - filter HF-NER PERSON candidates down to market-relevant names,
  - produce a notable-person boost for the (causal) impact score.

This is the source of truth for *who is market-relevant and how much* — the NER
model (ner.py) only proposes candidate names.
"""

import re
from collections.abc import Iterable

# Curated market-movers. `weight` is the impact-boost magnitude (0..1).
_PEOPLE: list[dict] = [
    {"canonical": "Donald Trump", "role": "politician", "weight": 1.0,
     "aliases": ["donald trump", "president trump", "trump"]},
    {"canonical": "Jerome Powell", "role": "central_banker", "weight": 1.0,
     "aliases": ["jerome powell", "jay powell", "fed chair powell", "powell"]},
    {"canonical": "Janet Yellen", "role": "policymaker", "weight": 0.85,
     "aliases": ["janet yellen", "yellen"]},
    {"canonical": "Christine Lagarde", "role": "central_banker", "weight": 0.85,
     "aliases": ["christine lagarde", "lagarde"]},
    {"canonical": "Elon Musk", "role": "ceo", "weight": 0.85,
     "aliases": ["elon musk", "musk"]},
    {"canonical": "Warren Buffett", "role": "investor", "weight": 0.8,
     "aliases": ["warren buffett", "buffett"]},
    {"canonical": "Narendra Modi", "role": "politician", "weight": 0.8,
     "aliases": ["narendra modi", "pm modi", "modi"]},
    {"canonical": "Shaktikanta Das", "role": "central_banker", "weight": 0.8,
     "aliases": ["shaktikanta das"]},
    {"canonical": "Tim Cook", "role": "ceo", "weight": 0.6,
     "aliases": ["tim cook"]},
    {"canonical": "Sundar Pichai", "role": "ceo", "weight": 0.6,
     "aliases": ["sundar pichai", "pichai"]},
    {"canonical": "Satya Nadella", "role": "ceo", "weight": 0.6,
     "aliases": ["satya nadella", "nadella"]},
    {"canonical": "Jensen Huang", "role": "ceo", "weight": 0.6,
     "aliases": ["jensen huang"]},
    {"canonical": "Mukesh Ambani", "role": "ceo", "weight": 0.6,
     "aliases": ["mukesh ambani", "ambani"]},
    {"canonical": "Gautam Adani", "role": "ceo", "weight": 0.6,
     "aliases": ["gautam adani", "adani"]},
]

# alias (lowercased) -> entry
ALIAS_INDEX: dict[str, dict] = {
    alias: {"canonical": p["canonical"], "role": p["role"], "weight": p["weight"]}
    for p in _PEOPLE
    for alias in p["aliases"]
}

# Longest aliases first so multi-word names win over their single-token forms.
_ALIASES_BY_LEN = sorted(ALIAS_INDEX, key=len, reverse=True)


def _person(entry: dict) -> dict:
    return {"name": entry["canonical"], "role": entry["role"], "weight": entry["weight"]}


def match_watchlist(text: str) -> list[dict]:
    """Detect notable people by scanning text for watchlist aliases (word-boundary)."""
    if not text:
        return []
    lowered = text.lower()
    found: dict[str, dict] = {}
    for alias in _ALIASES_BY_LEN:
        if re.search(rf"\b{re.escape(alias)}\b", lowered):
            entry = ALIAS_INDEX[alias]
            found.setdefault(entry["canonical"], _person(entry))
    return list(found.values())


def score_people(names: Iterable[str]) -> list[dict]:
    """Keep only the supplied names (e.g. from NER) that are on the watchlist."""
    found: dict[str, dict] = {}
    for raw in names or []:
        key = (raw or "").strip().lower()
        entry = ALIAS_INDEX.get(key)
        if entry:
            found.setdefault(entry["canonical"], _person(entry))
    return list(found.values())


def detect(text: str, ner_names: Iterable[str] = ()) -> list[dict]:
    """Union of watchlist text matches and watchlist-filtered NER names."""
    found: dict[str, dict] = {}
    for person in match_watchlist(text) + score_people(ner_names):
        found.setdefault(person["name"], person)
    return list(found.values())


def notable_boost(people: list[dict]) -> float:
    """Boost signal in [0,1] — the strongest notable voice present (0 if none)."""
    if not people:
        return 0.0
    return max(0.0, min(1.0, max(float(p.get("weight", 0.0)) for p in people)))
