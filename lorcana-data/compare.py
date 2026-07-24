#!/usr/bin/env python3
"""
Compare lorcana_cards_update-orig.json with lorcana_cards_update.json.

Matches cards by dreamborn ID (most stable cross-version key) and reports:
  - cards present in one file but not the other
  - field-level differences on matched cards

Usage:
    python compare.py                  # uses build/ relative to project root
    python compare.py --orig path/a.json --new path/b.json
"""

import argparse
import json
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "build"

# Fields that exist only in the new output — skip when comparing shared data
NEW_ONLY_FIELDS = {
    "card_identifier", "deck_building_id", "card_sets", "special_rarity_id",
    "lore", "additional_info", "subtypes", "searchable_keywords",
    "set_rotation_state", "name", "subtitle", "flavor_text", "rules_text",
    "set_name",
}

# Fields that exist only in the original output
ORIG_ONLY_FIELDS = {"franchise", "third_party"}

# Language keys present in both outputs
LANGUAGE_KEYS = ("en", "fr", "de", "it", "zh", "ja")

# Translation sub-fields present in both outputs
TRANSLATION_FIELDS = ("name", "title", "flavour", "image", "thumbnail", "foil_mask")


def load(path: Path) -> list:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def index_by_dreamborn(cards: list) -> dict:
    idx = {}
    for card in cards:
        key = card.get("dreamborn")
        if key:
            idx[key] = card
    return idx


def fmt(v) -> str:
    if v is None:
        return "null"
    if isinstance(v, str) and len(v) > 80:
        return repr(v[:77] + "...")
    return repr(v)


def compare_translations(dreamborn: str, orig_card: dict, new_card: dict, diffs: list):
    orig_langs = orig_card.get("languages") or {}
    new_langs = new_card.get("languages") or {}

    for lang in LANGUAGE_KEYS:
        orig_t = orig_langs.get(lang)
        new_t = new_langs.get(lang)

        if orig_t is None and new_t is None:
            continue

        if (orig_t is None) != (new_t is None):
            diffs.append(
                f"  [{dreamborn}] languages.{lang}: "
                f"orig={fmt(orig_t)} new={fmt(new_t)}"
            )
            continue

        for field in TRANSLATION_FIELDS:
            ov = orig_t.get(field)
            nv = new_t.get(field)
            if ov != nv:
                diffs.append(
                    f"  [{dreamborn}] languages.{lang}.{field}: "
                    f"orig={fmt(ov)} new={fmt(nv)}"
                )


def compare_classifications(dreamborn: str, orig_card: dict, new_card: dict, diffs: list):
    orig_cls = orig_card.get("classifications") or []
    new_cls = new_card.get("classifications") or []

    if len(orig_cls) != len(new_cls):
        diffs.append(
            f"  [{dreamborn}] classifications length: orig={len(orig_cls)} new={len(new_cls)}"
        )
        return

    for i, (oc, nc) in enumerate(zip(orig_cls, new_cls)):
        for field in ("slug", "en", "fr", "de", "zh"):
            ov = oc.get(field)
            nv = nc.get(field)
            if ov != nv:
                diffs.append(
                    f"  [{dreamborn}] classifications[{i}].{field}: "
                    f"orig={fmt(ov)} new={fmt(nv)}"
                )


SHARED_SCALAR_FIELDS = [
    "number", "set_code", "set", "set_number", "type", "rarity",
    "cost", "inkwell", "attack", "defence", "move_cost",
    "color", "foil", "illustrator", "rotation_states",
]

RAVENSBURGER_FIELDS = ["en", "fr", "de", "it", "zh", "ja", "culture_invariant_id", "sort_number"]


def compare_card(dreamborn: str, orig_card: dict, new_card: dict) -> list:
    diffs = []

    for field in SHARED_SCALAR_FIELDS:
        ov = orig_card.get(field)
        nv = new_card.get(field)
        if ov != nv:
            diffs.append(f"  [{dreamborn}] {field}: orig={fmt(ov)} new={fmt(nv)}")

    # ravensburger sub-object
    orig_rv = orig_card.get("ravensburger") or {}
    new_rv = new_card.get("ravensburger") or {}
    for field in RAVENSBURGER_FIELDS:
        ov = orig_rv.get(field)
        nv = new_rv.get(field)
        if ov != nv:
            diffs.append(f"  [{dreamborn}] ravensburger.{field}: orig={fmt(ov)} new={fmt(nv)}")

    compare_translations(dreamborn, orig_card, new_card, diffs)
    compare_classifications(dreamborn, orig_card, new_card, diffs)

    return diffs


def summarize_by_set(cards: list) -> dict:
    counts = defaultdict(int)
    for c in cards:
        counts[c.get("set_code") or "unknown"] += 1
    return dict(sorted(counts.items()))


def main():
    parser = argparse.ArgumentParser(description="Compare two lorcana_cards JSON files")
    parser.add_argument("--orig", type=Path, default=BUILD / "lorcana_cards_update-orig.json")
    parser.add_argument("--new", type=Path, default=BUILD / "lorcana_cards_update.json")
    parser.add_argument(
        "--max-diffs", type=int, default=50,
        help="Stop reporting field diffs after this many (default 50, 0=unlimited)",
    )
    args = parser.parse_args()

    print(f"Loading {args.orig.name} ...")
    orig_cards = load(args.orig)
    print(f"Loading {args.new.name} ...")
    new_cards = load(args.new)

    print(f"\nCard counts:  orig={len(orig_cards)}  new={len(new_cards)}\n")

    orig_idx = index_by_dreamborn(orig_cards)
    new_idx = index_by_dreamborn(new_cards)

    only_in_orig = sorted(set(orig_idx) - set(new_idx))
    only_in_new = sorted(set(new_idx) - set(orig_idx))

    # -----------------------------------------------------------------------
    # Cards missing from new output
    # -----------------------------------------------------------------------
    if only_in_orig:
        print(f"=== {len(only_in_orig)} card(s) in ORIG but not in NEW ===")
        for db in only_in_orig:
            c = orig_idx[db]
            en = (c.get("languages") or {}).get("en") or {}
            name = en.get("name") or db
            print(f"  {db}  {name}  (set={c.get('set_code')} rarity={c.get('rarity')})")
        print()

    # -----------------------------------------------------------------------
    # Extra cards in new output
    # -----------------------------------------------------------------------
    if only_in_new:
        print(f"=== {len(only_in_new)} card(s) in NEW but not in ORIG ===")
        for db in only_in_new:
            c = new_idx[db]
            name = c.get("name") or db
            print(f"  {db}  {name}  (set={c.get('set_code')} rarity={c.get('rarity')})")
        print()

    # -----------------------------------------------------------------------
    # Field-level diffs on matched cards
    # -----------------------------------------------------------------------
    matched = sorted(set(orig_idx) & set(new_idx))
    print(f"Comparing {len(matched)} matched cards...")

    all_diffs = []
    for db in matched:
        card_diffs = compare_card(db, orig_idx[db], new_idx[db])
        all_diffs.extend(card_diffs)

    if not all_diffs:
        print("  No field differences found on matched cards.\n")
    else:
        limit = args.max_diffs
        shown = all_diffs if limit == 0 else all_diffs[:limit]
        print(f"\n=== {len(all_diffs)} field difference(s) on matched cards ===")
        for line in shown:
            print(line)
        if limit and len(all_diffs) > limit:
            print(f"  ... ({len(all_diffs) - limit} more — use --max-diffs 0 to see all)")
        print()

    # -----------------------------------------------------------------------
    # Summary by set
    # -----------------------------------------------------------------------
    print("=== Card counts by set ===")
    orig_by_set = summarize_by_set(orig_cards)
    new_by_set = summarize_by_set(new_cards)
    all_sets = sorted(set(orig_by_set) | set(new_by_set))
    header = f"  {'set':<8}  {'orig':>6}  {'new':>6}  {'diff':>6}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for s in all_sets:
        oc = orig_by_set.get(s, 0)
        nc = new_by_set.get(s, 0)
        marker = " <--" if oc != nc else ""
        print(f"  {s:<8}  {oc:>6}  {nc:>6}  {nc - oc:>+6}{marker}")

    # -----------------------------------------------------------------------
    # Exit code
    # -----------------------------------------------------------------------
    issues = len(only_in_orig) + len(only_in_new) + len(all_diffs)
    print(f"\n{'PASS' if issues == 0 else 'DIFFERENCES FOUND'}: "
          f"{len(only_in_orig)} missing, {len(only_in_new)} extra, "
          f"{len(all_diffs)} field diff(s)")
    return 0 if issues == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
