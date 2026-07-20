#!/usr/bin/env python3
"""
Download the Ravensburger Lorcana catalog for every supported language and
produce a single merged flat JSON at data_existing/catalog/merged.json.

Usage:
    python main.py               # download all languages + merge
    python main.py --skip-download   # merge from already-cached per-lang files
"""

import argparse
import json
import sys
import zipfile
from pathlib import Path

import lorcanito
from fetch import download_catalog, fetch_lorcanito
from merge import DOWNLOAD_LANGUAGES, LANGUAGES, merge

ROOT = Path(__file__).resolve().parent.parent
CATALOG_DIR = ROOT

# zh/ja are not served by the API — their full.json files are kept here
STATIC_LANGS_DIR = Path(__file__).resolve().parent / "data_existing"


def save_raw(lang: str, data: dict) -> Path:
    out_dir = CATALOG_DIR / lang
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "full.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return path


def load_cached(lang: str) -> dict | None:
    # zh/ja live inside the Python project's data_existing/ directory
    if lang in ("zh", "ja"):
        path = STATIC_LANGS_DIR / lang / "full.json"
    else:
        path = CATALOG_DIR / lang / "full.json"
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main() -> int:
    parser = argparse.ArgumentParser(description="Lorcana catalog pipeline")
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip API calls and use cached per-language files",
    )
    parser.add_argument(
        "--refresh-lorcanito",
        action="store_true",
        help="Re-download lorcanito.json from the tcg.online API before merging",
    )
    args = parser.parse_args()

    lorcanito_path = Path(__file__).resolve().parent / "lorcanito.json"

    if args.refresh_lorcanito:
        print("Refreshing lorcanito.json from the tcg.online API...")
        try:
            cards = fetch_lorcanito()
            with open(lorcanito_path, "w", encoding="utf-8") as f:
                json.dump(cards, f, indent=2, ensure_ascii=False)
            print(f"  {len(cards)} cards written to {lorcanito_path.name}")
        except Exception as exc:
            print(f"  FAILED ({exc})")
            return 1

    # Pass explicit path only when the local copy exists (e.g. after --refresh-lorcanito);
    # otherwise let lorcanito.load() fall back to the Kotlin project copy.
    load_path = lorcanito_path if lorcanito_path.exists() else None
    ok = lorcanito.load(load_path)
    if ok:
        print(f"Loaded lorcanito data ({len(lorcanito._index)} cards indexed)")
    else:
        print("WARNING: lorcanito.json not found — abilities will be empty")

    catalogs: dict = {}

    if args.skip_download:
        print("Loading cached catalogs...")
        for lang in LANGUAGES:
            data = load_cached(lang)
            if data is None:
                print(f"  WARNING: no cached file for '{lang}', skipping")
                continue
            catalogs[lang] = data
            print(f"  Loaded {lang} ({len(data.get('cards', {}))} card buckets)")
    else:
        print("Fetching OAuth token...")
        for lang in DOWNLOAD_LANGUAGES:
            print(f"  Downloading '{lang}'...", end=" ", flush=True)
            try:
                data = download_catalog(lang)
                path = save_raw(lang, data)
                catalogs[lang] = data
                total = sum(len(v) for v in data.get("cards", {}).values())
                print(f"{total} cards  ->  {path.relative_to(ROOT)}")
            except Exception as exc:
                print(f"FAILED ({exc})")
                return 1

        # zh and ja are not served by the v3 API — use cached files only
        for lang in ("zh", "ja"):
            data = load_cached(lang)
            if data is not None:
                catalogs[lang] = data
                total = sum(len(v) for v in data.get("cards", {}).values())
                print(f"  Loaded cached '{lang}': {total} cards")

    if "en" not in catalogs:
        print(
            "ERROR: English catalog is required but could not be loaded.",
            file=sys.stderr,
        )
        return 1

    print("\nMerging catalogs...")
    cards = merge(catalogs)
    print(f"  {len(cards)} cards across {len(catalogs)} languages")

    out_path = CATALOG_DIR / "lorcana_cards_update.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(cards, f, indent=2, ensure_ascii=True)
    print(f"Wrote {out_path.relative_to(ROOT)}")

    zip_path = out_path.with_name(out_path.name + ".zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(out_path, out_path.name)
    print(f"Wrote {zip_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
