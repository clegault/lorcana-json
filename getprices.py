#!/usr/bin/env python3
import csv
import gzip
import json
import re
import urllib.request
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
]

PROMO_URLS = [
    "https://tcgcsv.com/tcgplayer/71/17690/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/23234/ProductsAndPrices.csv",
    "https://tcgcsv.com/tcgplayer/71/23305/ProductsAndPrices.csv",
]

KEEP_FIELDS = {"extNumber", "marketPrice", "url", "subTypeName"}

def get_set_id(url: str) -> int:
    match = re.search(r"/(\d+)/ProductsAndPrices\.csv", url)
    if match:
        return SET_MAP.get(match.group(1), 0)
    return 0

CARDS_FILE = Path(__file__).parent / "lorcana_cards_update.json.gz"

def clean_ext_number(value: str) -> str:
    return value.split("/")[0].strip() if "/" in value else value.strip()

def clean_card_name(name: str) -> str:
    return re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()

def load_promo_lookup() -> dict[str, list[dict]]:
    with gzip.open(CARDS_FILE, "rt", encoding="utf-8") as f:
        data = json.load(f)
    lookup: dict[str, list[dict]] = {}
    for card in data["cards"]:
        if "promoGrouping" not in card:
            continue
        full_name = card["fullName"].lower()
        entry = {
            "number": card["number"],
            "promoGrouping": card["promoGrouping"],
            "setCode": card.get("setCode", "0"),
        }
        lookup.setdefault(full_name, []).append(entry)
    return lookup

def resolve_promo_ext(name: str, ext_number: str, lookup: dict[str, list[dict]]) -> tuple[str, int]:
    cleaned = clean_card_name(name).lower()
    has_suffix = name.strip() != clean_card_name(name)
    matches = lookup.get(cleaned, [])
    if not matches:
        print(f"  WARNING: no promo match for {cleaned!r}")
        return ext_number, 0
    try:
        ext_int = int(ext_number)
    except (ValueError, TypeError):
        return ext_number, 0
    number_matches = [m for m in matches if m["number"] == ext_int]
    if not number_matches:
        print(f"  WARNING: no number match for {cleaned!r} ext={ext_number}")
        return ext_number, 0
    if len(number_matches) == 1:
        m = number_matches[0]
        return f"{m['number']}/{m['promoGrouping']}", int(m["setCode"])
    if has_suffix:
        m = next((m for m in number_matches if m["promoGrouping"] != "P1"), number_matches[0])
    else:
        m = next((m for m in number_matches if m["promoGrouping"] == "P1"), number_matches[0])
    return f"{m['number']}/{m['promoGrouping']}", int(m["setCode"])

def clean_subtype(value: str) -> str:
    return re.sub(r"\bcold foil\b", "foil", value, flags=re.IGNORECASE).strip()

def fetch_csv(url: str) -> list[dict]:
    print(f"Downloading {url} ...")
    with urllib.request.urlopen(url) as response:
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

def process_promo_url(url: str, lookup: dict[str, list[dict]]) -> list[dict]:
    rows = fetch_csv(url)
    results = []
    for row in rows:
        raw_ext = row.get("extNumber", "").strip()
        name = row.get("name", "")
        if not raw_ext:
            continue
        ext_number, set_num = resolve_promo_ext(name, raw_ext, lookup)
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
    lookup = load_promo_lookup()
    print(f"  -> {sum(len(v) for v in lookup.values())} promo cards indexed")

    for url in PROMO_URLS:
        records = process_promo_url(url, lookup)
        all_records.extend(records)
        print(f"  -> {len(records)} rows")

    output_file = Path(__file__).parent / "prices.json.gz"

    with gzip.open(output_file, "wt", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2)

    print(f"\nDone! {len(all_records)} total records written to {output_file}")

if __name__ == "__main__":
    main()