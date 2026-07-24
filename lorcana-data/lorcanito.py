"""Load and index tcg.online card data for ability resolution.

Historically this consumed db.lorcanito.com's scraped payload; that source was
retired and the data now comes from the tcg.online JSON API (see fetch.py).
The file on disk (lorcanito.json) keeps its name for continuity but holds the
tcg.online card objects.
"""

import json
import re
from pathlib import Path
from typing import Optional

# Default path: alongside this file; falls back to the Kotlin project copy
_DEFAULT_PATH = Path(__file__).resolve().parent / "lorcanito.json"
_FALLBACK_PATH = Path(__file__).resolve().parent.parent / "library" / "merge_catalog" / "lorcanito.json"

# Maps tcg.online set codes → our internal set codes
_SET_CODE_MAP = {
    "TFC": "tfc", "ROF": "rotf", "ITI": "iti", "URR": "urr", "SSK": "ssk",
    "AZS": "azu", "ARC": "arc", "ROJ": "roj", "FAB": "fab",
    "WIW": "whi", "WSP": "win", "WUN": "wun", "013": "aov",
}

_LANG_TEMPLATE = {"en": None, "fr": None, "de": None, "it": None, "es": None, "zh": None, "ja": None}

# A leading run of ALL-CAPS tokens is the ability's name (e.g. "STAR PERFORMANCE
# Whenever ..."). Keyword abilities ("Shift 5", "Evasive") are Title-case and
# won't match, so they keep an empty title.
_NAME_TOKEN = re.compile(r"^[A-Z][A-Z0-9'’.\-]*[!?.,]?$")

# Module-level state populated by load()
_cards: list = []
_index: dict = {}  # (set_code, number) -> card dict
_set_names: dict = {}  # set_code -> display name (e.g. "Attack of the Vine!")


def load(path: Optional[Path] = None) -> bool:
    """Load lorcanito.json into memory. Returns True on success."""
    global _cards, _index, _set_names

    if path is None:
        path = _DEFAULT_PATH if _DEFAULT_PATH.exists() else _FALLBACK_PATH

    if not path.exists():
        return False

    with open(path, encoding="utf-8") as f:
        _cards = json.load(f)

    _index = {}
    _set_names = {}
    for card in _cards:
        set_info = card.get("set") or {}
        sc = _SET_CODE_MAP.get(set_info.get("code", ""))
        n = _card_number(card.get("number"))
        if sc and n is not None:
            _index[(sc, n)] = card
        if sc and set_info.get("name"):
            _set_names.setdefault(sc, set_info["name"])

    return True


def set_name_for(set_code: str) -> Optional[str]:
    """Display name for a set code, as published by tcg.online.

    Lets consumers show "Attack of the Vine!" instead of inventing a label from
    the short code, so a new set needs no client-side change.
    """
    return _set_names.get(set_code)


def _card_number(number) -> Optional[int]:
    """Parse the leading integer from a tcg.online number like "17/207"."""
    if number is None:
        return None
    m = re.match(r"^(\d+)", str(number))
    return int(m.group(1)) if m else None


def _name_word_count(text: str) -> int:
    """Number of leading tokens that form an ability name (ALL-CAPS run).

    The card-level ``text`` field uppercases ability names but leaves the body
    in normal case, so an ALL-CAPS run reliably marks the name — even for names
    that display in title case (e.g. "Heroic Intervention"). Keyword abilities
    ("Singer 5", "Evasive") are title case and yield 0.
    """
    tokens = text.strip().split(" ")
    n = 0
    for tok in tokens:
        if tok == "&" or _NAME_TOKEN.match(tok):
            n += 1
        else:
            break
    if n == 0 or n >= len(tokens):
        return 0
    # Guard against a single short caps token being mistaken for a name.
    if n == 1 and len(tokens[0].rstrip("!?.,")) < 3:
        return 0
    return n


def _map_abilities(card: dict) -> list:
    """Map one card's abilities to the merge output shape.

    Uses the card-level ``text`` (complete, names uppercased) to find name
    boundaries and to backfill abilities whose own ``text`` is empty, while
    taking the display casing from ``abilities[].text`` when the two align.
    """
    abilities = card.get("abilities") or []
    lines = [ln.strip() for ln in (card.get("text") or "").split("\n")]
    aligned = len(lines) == len(abilities)

    result = []
    for i, ab in enumerate(abilities):
        if not isinstance(ab, dict):
            continue
        display = (ab.get("text") or "").strip()  # original casing
        caps = lines[i] if aligned and i < len(lines) else ""  # names uppercased
        full = display or caps  # prefer display casing; fall back to card text
        if not full:
            continue

        # Determine the name boundary from whichever source has it uppercased.
        n = _name_word_count(caps or full)
        title, body = "", full
        if n:
            ftoks = full.split(" ")
            candidate_body = " ".join(ftoks[n:]).strip()
            if candidate_body:  # never let the name swallow the whole text
                title = " ".join(ftoks[:n])
                body = candidate_body

        result.append(
            {
                "type": ab.get("type") or "",
                "title": {**_LANG_TEMPLATE, "en": title},
                "text": {**_LANG_TEMPLATE, "en": body},
                "ability": None,
            }
        )
    return result


def abilities_for(set_code: str, number: int) -> list:
    """Return the mapped ability list for a card, or [] if not found."""
    card = _index.get((set_code, number))
    if not card:
        return []
    return _map_abilities(card)
