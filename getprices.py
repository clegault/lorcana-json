#!/usr/bin/env python3
import csv
import json
import re
import time
import zipfile
import requests
from io import StringIO
from pathlib import Path

LORCANA_CATEGORY = "71"

# Maps tcgcsv group IDs to Lorcana set numbers. Add new entries here when new
# sets appear — any group not listed will be auto-assigned the next set number.
SET_MAP = {
    22937: 1,
    23303: 2,
    23367: 3,
    23474: 4,
    23536: 5,
    23746: 6,
    24011: 7,
    24258: 8,
    24348: 9,
    24414: 10,
    24500: 11,
    24617: 12,
    24666: 13,
}

PROMO_GROUP_IDS = {17690, 23234, 23305}

# Board game expansions and other non-card-set groups to skip
EXCLUDED_GROUP_IDS = {23528, 24257}

session = requests.Session()
session.headers.update({"User-Agent": "LorScana/1.0.0"})


CARDS_FILE = Path(__file__).parent / "lorcana_cards_update.json.zip"

def clean_ext_number(value: str) -> str:
    return value.split("/")[0].strip() if "/" in value else value.strip()

def clean_card_name(name: str) -> str:
    return re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()

def load_promo_lookup() -> dict[str, list[dict]]:
    with zipfile.ZipFile(CARDS_FILE) as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            cards = json.load(f)
    lookup: dict[str, list[dict]] = {}
    for card in cards:
        en = (card.get("languages") or {}).get("en") or {}
        name_part = en.get("name", "")
        title_part = en.get("title", "")
        full_name = f"{name_part} - {title_part}".lower() if title_part else name_part.lower()
        if not full_name:
            continue
        entry = {"number": card["number"], "set_number": card["set_number"]}
        lookup.setdefault(full_name, []).append(entry)
    return lookup

def resolve_promo_ext(name: str, ext_number: str, lookup: dict[str, list[dict]]) -> tuple[str, int]:
    cleaned = clean_card_name(name).lower()
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
    return ext_number, number_matches[0]["set_number"]

def clean_subtype(value: str) -> str:
    return re.sub(r"\bcold foil\b", "foil", value, flags=re.IGNORECASE).strip()

def fetch_groups() -> list[dict]:
    r = session.get(f"https://tcgcsv.com/tcgplayer/{LORCANA_CATEGORY}/groups")
    r.raise_for_status()
    return r.json()["results"]

def fetch_csv(url: str) -> list[dict]:
    print(f"Fetching {url} ...")
    r = session.get(url)
    r.raise_for_status()
    reader = csv.DictReader(StringIO(r.text))
    return list(reader)

def process_url(url: str, set_num: int) -> list[dict]:
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
    print("Fetching Lorcana groups from tcgcsv.com...")
    groups = fetch_groups()
    print(f"  -> {len(groups)} groups found")

    # Sort by groupId so auto-assigned set numbers are stable across runs
    groups.sort(key=lambda g: g["groupId"])

    # Build a complete group_id -> set_num mapping, auto-assigning unknowns
    next_set_num = max(SET_MAP.values(), default=0) + 1
    group_set_map: dict[int, int] = {}
    for group in groups:
        gid = group["groupId"]
        if gid in PROMO_GROUP_IDS or gid in EXCLUDED_GROUP_IDS:
            continue
        if gid in SET_MAP:
            group_set_map[gid] = SET_MAP[gid]
        else:
            print(f"  New group detected: {gid} ({group.get('name', '?')}) -> set {next_set_num}")
            group_set_map[gid] = next_set_num
            next_set_num += 1

    all_records = []
    for group in groups:
        gid = group["groupId"]
        if gid not in group_set_map:
            continue
        url = f"https://tcgcsv.com/tcgplayer/{LORCANA_CATEGORY}/{gid}/ProductsAndPrices.csv"
        records = process_url(url, group_set_map[gid])
        all_records.extend(records)
        print(f"  -> {len(records)} rows")
        time.sleep(0.25)

    print("\nLoading promo card lookup...")
    lookup = load_promo_lookup()
    print(f"  -> {sum(len(v) for v in lookup.values())} promo cards indexed")

    for gid in PROMO_GROUP_IDS:
        url = f"https://tcgcsv.com/tcgplayer/{LORCANA_CATEGORY}/{gid}/ProductsAndPrices.csv"
        records = process_promo_url(url, lookup)
        all_records.extend(records)
        print(f"  -> {len(records)} rows")
        time.sleep(0.25)

    output_file = Path(__file__).parent / "prices.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2)

    print(f"\nDone! {len(all_records)} total records written to {output_file}")

if __name__ == "__main__":
    main()