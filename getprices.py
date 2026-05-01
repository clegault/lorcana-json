#!/usr/bin/env python3
from __future__ import annotations
import csv
import json
import re
import urllib.request
import zipfile
from datetime import date
from io import StringIO
from pathlib import Path

SET_MAP = {
    "22937": 1,
    "23303": 2,
    "23367": 3,
    "23474": 4,
    "23536": 5,
    "23746": 6,
    "24011": 7,
    "24258": 8,
    "24348": 9,
    "24414": 10,
    "24500": 11,
    "24617": 12,
}

URLS = [
    "https://tcgcsv.com/tcgplayer/71/22937/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/23303/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/23367/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/23474/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/23536/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/23746/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/24011/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/24258/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/24348/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/24414/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/24500/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/24617/ProductsAndPrices.csv",
]

PROMO_URLS = [
    "https://tcgcsv.com/tcgplayer/71/17690/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/23234/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/23305/ProductsAndPrices.csv",
]

PROMO_RARITIES = {"challenge24", "special", "top1"}

KEEP_FIELDS = {"extNumber", "marketPrice", "url", "subTypeName"}


def get_set_id(url: str) -> int:
    match = re.search(r"/(\d+)/ProductsAndPrices\.csv", url)
    if match:
        return SET_MAP.get(match.group(1), 0)
    return 0


CARDS_FILE = Path(__file__).parent / "lorcana_cards_update.json.zip"


def clean_ext_number(value: str) -> str:
    return value.split("/")[0].strip() if "/" in value else value.strip()


def clean_card_name(name: str) -> str:
    return re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()


def _load_cards() -> list[dict]:
    with zipfile.ZipFile(CARDS_FILE) as zf:
        with zf.open("lorcana_cards_update.json") as f:
            data = json.load(f)
    return data["cards"] if isinstance(data, dict) else data


def _normalize(s: str) -> str:
    return s.replace("’", "'").replace("‘", "'").lower()


def _card_en_name(card: dict) -> tuple[str, str]:
    en = card.get("languages", {}).get("en", {})
    return _normalize(en.get("name", "")), _normalize(en.get("title", "") or "")


def _parse_ravensburger(card: dict) -> tuple[int | None, int]:
    """Return (promo_card_number, set_number) from ravensburger.en.

    '19/P2 EN 7'  -> (19, 7)
    '8/D23 EN 8'  -> (8, 8)
    '1TFC EN 2/P1' -> (None, 2)
    ''             -> (None, 0)
    """
    en_str = card.get("ravensburger", {}).get("en", "") or ""
    m = re.match(r"^(\d+)/\S+ EN (\d+)", en_str)
    if m:
        return int(m.group(1)), int(m.group(2))
    m2 = re.search(r"\bEN (\d+)", en_str)
    if m2:
        return None, int(m2.group(1))
    return None, 0


def _parse_dreamborn_promo(card: dict) -> tuple[int | None, int | None]:
    """Return (promo_card_number, set_number) for promo dreamborn entries.

    '007-P2-019' -> (19, 7)
    '001-P1-008' -> (8, 1)
    'C1-005'     -> (5, None)
    'D23-001'    -> (1, None)
    '001-023'    -> (None, None)   regular card, ignored
    """
    db = card.get("dreamborn", "") or ""
    m = re.match(r"^(\d{3})-[A-Z0-9]+-(\d+)$", db)
    if m:
        return int(m.group(2)), int(m.group(1))
    m2 = re.match(r"^[A-Z][A-Z0-9]*-(\d+)$", db)
    if m2:
        return int(m2.group(1)), None
    return None, None


def load_promo_lookup() -> dict[tuple, list[dict]]:
    cards = _load_cards()

    # One-pass base-set lookup: (name, title) -> lowest set_number among regular cards.
    # Used when a promo card has no set info in ravensburger.en or dreamborn.
    base_set: dict[tuple, int] = {}
    for card in cards:
        sc = card.get("set_code", "")
        rarity = card.get("rarity", "").lower()
        sn = card.get("set_number")
        if not sn or sc.startswith("p") or rarity in PROMO_RARITIES:
            continue
        key = _card_en_name(card)
        if key not in base_set or sn < base_set[key]:
            base_set[key] = sn

    lookup: dict[tuple, list[dict]] = {}
    for card in cards:
        set_code = card.get("set_code", "")
        rarity = card.get("rarity", "").lower()
        if not (set_code.startswith("p") or rarity in PROMO_RARITIES):
            continue

        promo_number, set_num = _parse_ravensburger(card)

        if not set_num or promo_number is None:
            db_promo, db_set = _parse_dreamborn_promo(card)
            if not set_num and db_set:
                set_num = db_set
            if promo_number is None and db_promo is not None:
                promo_number = db_promo

        if not set_num:
            name_key = _card_en_name(card)
            set_num = base_set.get(name_key, 0)

        if not set_num:
            continue

        if promo_number is None:
            promo_number = card.get("set_number") or card.get("number")
        if not promo_number:
            continue

        name, title = _card_en_name(card)
        if not name:
            continue

        key = (name, title, promo_number)
        entry = {
            "number": promo_number,
            "rarity": rarity,
            "setCode": set_num,
        }
        lookup.setdefault(key, []).append(entry)
    return lookup


def resolve_promo_ext(
    name: str,
    ext_number: str,
    promo_lookup: dict[tuple, list[dict]],
) -> tuple[str, int]:
    cleaned = clean_card_name(name).strip()
    parts = cleaned.split(" - ", 1)
    card_name = _normalize(parts[0].strip())
    card_title = _normalize(parts[1].strip()) if len(parts) > 1 else ""

    # Strip trailing letter suffix (e.g. '24A' -> 24, '24B' -> 24)
    ext_clean = ext_number.rstrip("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
    try:
        ext_int = int(ext_clean)
    except (ValueError, TypeError):
        print(f"  WARNING: unparseable ext for {cleaned!r} ext={ext_number!r}")
        return ext_number, 0

    matches = promo_lookup.get((card_name, card_title, ext_int), [])
    if matches:
        m = matches[0]
        return f"{m['number']}/{m['rarity']}", m["setCode"]

    print(f"  WARNING: no promo match for {cleaned!r} ext={ext_number}")
    return ext_number, 0


def clean_subtype(value: str) -> str:
    return re.sub(r"\bcold foil\b", "foil", value, flags=re.IGNORECASE).strip()


def fetch_csv(url: str) -> list[dict]:
    print(f"Downloading {url} ...")
    req = urllib.request.Request(url, headers={"User-Agent": "LorScana/1.0.0"})
    with urllib.request.urlopen(req) as response:
        content = response.read().decode("utf-8")
    reader = csv.DictReader(StringIO(content))
    return list(reader)


def process_url(url: str) -> list[dict]:
    set_num = get_set_id(url)
    rows = fetch_csv(url)
    results = []
    for row in rows:
        record = {
            "set": set_num,
            "extNumber": clean_ext_number(row.get("extNumber", "")),
            "marketPrice": row.get("marketPrice", ""),
            "url": row.get("url", ""),
            "subTypeName": clean_subtype(row.get("subTypeName", "")),
        }
        results.append(record)
    return results


def process_promo_url(
    url: str,
    promo_lookup: dict[tuple, list[dict]],
) -> list[dict]:
    rows = fetch_csv(url)
    results = []
    for row in rows:
        raw_ext = row.get("extNumber", "").strip()
        name = row.get("name", "")
        if not raw_ext:
            continue
        ext_number, set_num = resolve_promo_ext(name, raw_ext, promo_lookup)
        if set_num == 0:
            continue
        record = {
            "set": set_num,
            "extNumber": ext_number,
            "marketPrice": row.get("marketPrice", ""),
            "url": row.get("url", ""),
            "subTypeName": clean_subtype(row.get("subTypeName", "")),
        }
        results.append(record)
    return results


def main():
    all_records = []
    for url in URLS:
        records = process_url(url)
        all_records.extend(records)
        print(f"  -> {len(records)} rows")

    print("\nLoading promo card lookup...")
    promo_lookup = load_promo_lookup()
    print(f"  -> {sum(len(v) for v in promo_lookup.values())} promo cards indexed")

    for url in PROMO_URLS:
        records = process_promo_url(url, promo_lookup)
        all_records.extend(records)
        print(f"  -> {len(records)} rows")

    output_file = Path(__file__).parent / "prices.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2)

    print(f"\nDone! {len(all_records)} total records written to {output_file}")


if __name__ == "__main__":
    main()
