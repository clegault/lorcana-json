"""Merge per-language Lorcana catalogs into a single flat card list."""

import re
from typing import Optional

import lorcanito

# Languages the Ravensburger v3 API serves (same as the Kotlin pipeline)
DOWNLOAD_LANGUAGES = ["en", "de", "fr", "it"]

# All languages included in the merged output — zh/ja come from cached files
LANGUAGES = ["en", "de", "fr", "it", "zh", "ja"]

_SET_CODES = {
    1: "tfc",
    2: "rotf",
    3: "iti",
    4: "urr",
    5: "ssk",
    6: "azu",
    7: "arc",
    8: "roj",
    9: "fab",
    10: "whi",
    11: "win",
    12: "wun",
    13: "aov",
}

# Non-numeric set parts from card_identifiers (e.g. "1/31 EN Q1")
_NAMED_SET_NUMBERS = {"Q1": -1, "Q2": -2}
_NAMED_SET_CODES = {"Q1": "quest1", "Q2": "quest2"}

_TYPE_MAP = {
    "characters": "glimmer",
    "items": "item",
    "actions": "action",
    "locations": "location",
}

_RARITY_MAP = {
    "COMMON": "common",
    "UNCOMMON": "uncommon",
    "RARE": "rare",
    "SUPER": "super_rare",
    "LEGENDARY": "legendary",
    "ENCHANTED": "enchanted",
    "SPECIAL": "special",
    "EPIC": "epic",
    "ICONIC": "iconic",
}

_ROTATION_MAP = {
    "CoreConstructed": ["core_constructed", "infinity_constructed"],
    "InfinityConstructed": ["infinity_constructed"],
}

# Cards whose foil field is wrong in the source data and must be forced True.
# Key: (set_code, card_number)
_FOIL_OVERRIDES: set = {
    ("ssk", 223),  # Half Hexwell Crown — incorrectly marked foil:false
}

# ---------------------------------------------------------------------------
# Card identifier parsing
# ---------------------------------------------------------------------------


def _parse_card_id(card_id: str) -> Optional[tuple]:
    """Return (card_num, bucket, set_part) or None.

    Handles two formats:
      Standard:  "28/204 EN 12"  →  (28, "204", "12")
      Reversed:  "1TFC EN 2/P1"  →  (2, "P1", "1")
    """
    m = re.match(r"^(\d+)/(\S+) [A-Z]{2} (\S+)$", card_id)
    if m:
        return int(m.group(1)), m.group(2), m.group(3)
    m = re.match(r"^(\S+) [A-Z]{2} (\d+)/(\S+)$", card_id)
    if m:
        prefix, num, bucket = m.group(1), int(m.group(2)), m.group(3)
        set_m = re.match(r"^(\d+)", prefix)
        set_part = set_m.group(1) if set_m else prefix
        return num, bucket, set_part
    return None


def _card_number(card_id: str) -> Optional[int]:
    p = _parse_card_id(card_id)
    return p[0] if p else None


def _set_number(card_id: str) -> Optional[int]:
    p = _parse_card_id(card_id)
    if not p:
        return None
    try:
        return int(p[2])
    except (ValueError, TypeError):
        return _NAMED_SET_NUMBERS.get(p[2])


def _set_code(card_id: str) -> Optional[str]:
    p = _parse_card_id(card_id)
    if not p:
        return None
    try:
        return _SET_CODES.get(int(p[2]))
    except (ValueError, TypeError):
        return _NAMED_SET_CODES.get(p[2])


def _dreamborn(card_id: str) -> str:
    """Derive the dreamborn ID string from a card_identifier.

    Examples:
      "1/204 EN 1"   →  "001-001"
      "213/204 EN 7" →  "007-213"
      "1/207 EN 13"  →  "013-001"
      "21/P2 EN 7"   →  "007-P2-021"
      "1TFC EN 2/P1" →  "001-P1-002"
    """
    p = _parse_card_id(card_id)
    if not p:
        return ""
    num, bucket, set_part = p
    try:
        sn = int(set_part)
    except (ValueError, TypeError):
        return f"{set_part}-{num:03d}"
    # A numeric bucket is the base-set denominator (204 for sets 1-12, 207 for
    # set 13, etc.); non-numeric buckets (P1, C1, D23) mark promos/variants.
    if bucket.isdigit():
        return f"{sn:03d}-{num:03d}"
    return f"{sn:03d}-{bucket}-{num:03d}"


def _normalize_id(card_id: str) -> str:
    """Replace the language segment with EN so IDs match across languages."""
    return re.sub(r" [A-Z]{2} ", " EN ", card_id, count=1)


# ---------------------------------------------------------------------------
# Card field extraction
# ---------------------------------------------------------------------------


def _image_url(card: dict) -> Optional[str]:
    # Standard format (EN/DE/FR/IT): variants[].detail_image_url
    for v in card.get("variants") or []:
        if v.get("variant_id") == "Regular":
            return v.get("detail_image_url")
    variants = card.get("variants") or []
    if variants:
        return variants[0].get("detail_image_url")
    # ZH/JA format: image_urls[].url
    image_urls = card.get("image_urls") or []
    return image_urls[0].get("url") if image_urls else None


def _foil_mask_url(card: dict) -> Optional[str]:
    for v in card.get("variants") or []:
        url = v.get("foil_mask_url")
        if url:
            return url
    return None


def _has_foil(card: dict) -> bool:
    return any(v.get("foil_mask_url") for v in card.get("variants") or [])


def _is_placeholder(lang: str, card: dict, en_card: dict) -> bool:
    """True when a non-EN language card is just EN data served as a fallback.

    The API reuses the EN image URL verbatim for untranslated cards.
    Comparing image URLs is a reliable signal because real translations
    always have a lang-specific path (e.g. /it/ vs /en/).
    """
    if lang == "en":
        return False
    card_img = _image_url(card)
    en_img = _image_url(en_card)
    return bool(card_img) and card_img == en_img


def _flavour(card: dict) -> Optional[str]:
    # zh/ja static files use % as a newline substitute
    text = card.get("flavor_text")
    if not text:
        return text  # None or empty string — keep as-is
    return text.replace("%", "\n")


def _translation(card: dict) -> dict:
    return {
        "name": card.get("name", ""),
        "title": card.get("subtitle") or "",
        "flavour": _flavour(card) or "",
        "rules_text": card.get("rules_text"),
        "image": _image_url(card),
        "thumbnail": card.get("thumbnail_url"),
        "foil_mask": _foil_mask_url(card),
    }


def _ravensburger_ids(en_card: dict) -> dict:
    en_id = en_card["card_identifier"]

    def lang_id(lang: str) -> str:
        return re.sub(r" [A-Z]{2} ", f" {lang.upper()} ", en_id, count=1)

    return {
        "en": en_id,
        "fr": lang_id("fr"),
        "de": lang_id("de"),
        "it": lang_id("it"),
        "zh": lang_id("zh"),
        "ja": lang_id("ja"),
        "culture_invariant_id": en_card.get("culture_invariant_id"),
        "sort_number": en_card.get("sort_number"),
    }


def _classifications(en_card: dict, lang_cards: dict) -> list:
    """Build classification objects with per-language subtype translations.

    Subtypes are positional — the FR/DE/ZH lists for the same card are
    assumed to be in the same order as EN.
    """
    en_subtypes = en_card.get("subtypes") or []
    result = []
    for i, en_sub in enumerate(en_subtypes):
        entry = {
            "slug": en_sub.lower().replace(" ", "_"),
            "en": en_sub,
        }
        for lang in ("fr", "de", "zh"):
            lc = lang_cards.get(lang)
            if lc is None:
                entry[lang] = None
            else:
                subs = lc.get("subtypes") or []
                entry[lang] = subs[i] if i < len(subs) else None
        result.append(entry)
    return result


# ---------------------------------------------------------------------------
# Catalog iteration
# ---------------------------------------------------------------------------


def _iter_cards(catalog: dict):
    """Yield (card_type, card) pairs, deduplicating by culture_invariant_id.

    The API occasionally assigns the same card_identifier to two distinct cards
    (e.g. Moana/Vaiana regional variants). culture_invariant_id is the true
    unique key and is guaranteed present on every card.
    """
    seen: set = set()
    for card_type in ("characters", "items", "actions", "locations", "all"):
        for card in catalog.get("cards", {}).get(card_type, []):
            key = card.get("culture_invariant_id") or card["card_identifier"]
            if key not in seen:
                seen.add(key)
                yield card_type, card


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------


def merge(catalogs: dict) -> list:
    """Return a flat list of merged cards keyed from the EN catalog."""
    en_catalog = catalogs.get("en", {})

    # Index: culture_invariant_id -> {lang: card}
    lang_index: dict = {}
    for lang, catalog in catalogs.items():
        for _, card in _iter_cards(catalog):
            key = card.get("culture_invariant_id") or _normalize_id(
                card["card_identifier"]
            )
            lang_index.setdefault(key, {})[lang] = card

    merged = []
    seen_dreamborn: set = set()
    for card_type, en_card in _iter_cards(en_catalog):
        cii_key = en_card.get("culture_invariant_id") or _normalize_id(
            en_card["card_identifier"]
        )
        norm_key = _normalize_id(en_card["card_identifier"])

        # zh/ja cards have culture_invariant_id=null so they're indexed under the
        # normalized string key; merge both lookups so nothing is dropped.
        lang_cards = dict(lang_index.get(cii_key, {}))
        if cii_key != norm_key:
            for lang, card in lang_index.get(norm_key, {}).items():
                if lang not in lang_cards:
                    lang_cards[lang] = card

        colors = [c.lower() for c in (en_card.get("magic_ink_colors") or [])]

        languages = {}
        for lang in LANGUAGES:
            lc = lang_cards.get(lang)
            if lc is None:
                languages[lang] = None
            elif _is_placeholder(lang, lc, en_card):
                languages[lang] = None
            else:
                languages[lang] = _translation(lc)

        rarity_raw = en_card.get("rarity", "")
        card_id = en_card["card_identifier"]
        set_code = _set_code(card_id)
        number = _card_number(card_id)
        abilities = (
            lorcanito.abilities_for(set_code, number) if set_code and number else []
        )

        dreamborn = _dreamborn(card_id)
        if dreamborn in seen_dreamborn:
            dreamborn = dreamborn + "V"
        seen_dreamborn.add(dreamborn)

        merged.append(
            {
                "card_identifier": card_id,
                "number": number,
                "dreamborn": dreamborn,
                "deck_building_id": en_card.get("deck_building_id"),
                "set_code": set_code,
                "set": set_code,
                "set_number": _set_number(card_id),
                "card_sets": en_card.get("card_sets", []),
                "type": _TYPE_MAP.get(card_type, card_type),
                "rarity": _RARITY_MAP.get(rarity_raw, rarity_raw.lower()),
                "special_rarity_id": en_card.get("special_rarity_id"),
                "cost": en_card.get("ink_cost"),
                "inkwell": en_card.get("ink_convertible"),
                "attack": en_card.get("strength"),
                "defence": en_card.get("willpower"),
                "lore": en_card.get("quest_value"),
                "move_cost": en_card.get("move_cost"),
                "color": colors[0] if colors else None,
                "colors": colors,
                "foil": _has_foil(en_card),
                "illustrator": en_card.get("author"),
                "abilities": abilities,
                "actions": abilities,
                "additional_info": en_card.get("additional_info", []),
                "subtypes": en_card.get("subtypes", []),
                "searchable_keywords": en_card.get("searchable_keywords", []),
                "classifications": _classifications(en_card, lang_cards),
                "rotation_states": _ROTATION_MAP.get(
                    en_card.get("set_rotation_state", ""), []
                ),
                "set_rotation_state": en_card.get("set_rotation_state"),
                "ravensburger": _ravensburger_ids(en_card),
                "languages": languages,
                # EN display fields at top level for convenience
                "name": en_card.get("name", ""),
                "subtitle": en_card.get("subtitle"),
                "flavor_text": en_card.get("flavor_text"),
                "rules_text": en_card.get("rules_text"),
            }
        )

    return merged
