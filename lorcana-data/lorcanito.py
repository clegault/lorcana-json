"""Load and index lorcanito card data for ability resolution."""

import json
import re
from pathlib import Path
from typing import Optional

# Default path: alongside this file; falls back to the Kotlin project copy
_DEFAULT_PATH = Path(__file__).resolve().parent / "lorcanito.json"
_FALLBACK_PATH = Path(__file__).resolve().parent.parent / "library" / "merge_catalog" / "lorcanito.json"

# Maps lorcanito set codes → our internal set codes
_SET_CODE_MAP = {
    "TFC": "tfc", "ROF": "rotf", "ITI": "iti", "URR": "urr", "SSK": "ssk",
    "006": "azu", "007": "arc", "008": "roj", "009": "fab",
    "010": "whi", "011": "win", "012": "wun",
}

_LANG_TEMPLATE = {"en": None, "fr": None, "de": None, "it": None, "es": None, "zh": None, "ja": None}

# Module-level state populated by load()
_cards: list = []
_index: dict = {}  # (set_code, number) -> card dict


def load(path: Optional[Path] = None) -> bool:
    """Load lorcanito.json into memory. Returns True on success."""
    global _cards, _index

    if path is None:
        path = _DEFAULT_PATH if _DEFAULT_PATH.exists() else _FALLBACK_PATH

    if not path.exists():
        return False

    with open(path, encoding="utf-8") as f:
        _cards = json.load(f)

    _index = {}
    for card in _cards:
        sc = _SET_CODE_MAP.get(card.get("set", ""))
        n = card.get("number")
        if sc and n is not None:
            _index[(sc, n)] = card

    return True


def _resolve_ref(ref: str) -> Optional[dict]:
    """Resolve a $5:...:cards:IDX:abilities:N[:effects:M:customAbility] link."""
    m = re.search(
        r'cards:(\d+):abilities:(\d+)(?::effects:(\d+):customAbility)?$', ref
    )
    if not m:
        return None

    card_idx, ab_idx = int(m.group(1)), int(m.group(2))
    ef_idx = int(m.group(3)) if m.group(3) is not None else None

    if card_idx >= len(_cards):
        return None

    abilities = _cards[card_idx].get("abilities") or []
    if ab_idx >= len(abilities):
        return None

    ab = abilities[ab_idx]
    if isinstance(ab, str):
        return None  # nested ref — skip

    if ef_idx is not None:
        effects = ab.get("effects") or []
        if ef_idx >= len(effects):
            return None
        return effects[ef_idx].get("customAbility")

    return ab


def _map_ability(raw) -> Optional[dict]:
    if isinstance(raw, str):
        raw = _resolve_ref(raw)
    if not isinstance(raw, dict):
        return None

    title = raw.get("name") or raw.get("ability") or ""
    text = raw.get("text") or ""

    return {
        "type": raw.get("type") or "",
        "title": {**_LANG_TEMPLATE, "en": title},
        "text": {**_LANG_TEMPLATE, "en": text},
        "ability": raw.get("ability"),
    }


def abilities_for(set_code: str, number: int) -> list:
    """Return the mapped ability list for a card, or [] if not found."""
    card = _index.get((set_code, number))
    if not card:
        return []
    result = []
    for raw in card.get("abilities") or []:
        mapped = _map_ability(raw)
        if mapped is not None:
            result.append(mapped)
    return result
