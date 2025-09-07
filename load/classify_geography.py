from __future__ import annotations

import sys
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
import json

import pycountry
from sqlmodel import Session, select

from load.db import engine
from load.schema import Publication, GeographicalData, GeoAggregate
try:
    from pycountry_convert import (
        country_alpha3_to_country_alpha2 as _a3_to_a2,
        country_alpha2_to_continent_code as _a2_to_continent,
    )
except Exception:  # pragma: no cover - dependency may not be installed
    _a3_to_a2 = None  # type: ignore
    _a2_to_continent = None  # type: ignore


def _normalize_text(value: str) -> str:
    """Lowercase, strip accents, replace non-alnum with single spaces.

    Process order preserves word boundaries from punctuation like curly quotes
    before accent removal, so "Côte d’Ivoire" -> "cote d ivoire" (not "cote divoire").
    """
    value = value.lower()
    decomposed = unicodedata.normalize("NFKD", value)
    cleaned_chars: List[str] = []
    prev_space = False
    for ch in decomposed:
        # Skip combining marks (diacritics)
        if unicodedata.category(ch) == "Mn":
            continue
        if ch.isalnum():
            # Keep ASCII alphanumerics; drop other scripts for simplicity
            if ord(ch) < 128:
                cleaned_chars.append(ch)
                prev_space = False
            else:
                # Non-ASCII alnum: drop but do not insert extra spaces
                continue
        else:
            if not prev_space:
                cleaned_chars.append(" ")
                prev_space = True
    cleaned = "".join(cleaned_chars).strip()
    return " ".join(cleaned.split())


@dataclass(frozen=True)
class CountryEntry:
    alpha3: str
    display_name: str


def _manual_aliases() -> Dict[str, str]:
    """Common alternate country names mapped to ISO3 codes.

    These supplement pycountry's built-in names/official/common names.
    All keys must be normalized via _normalize_text.
    """
    aliases: Dict[str, str] = {
        # Name changes / exonyms
        "ivory coast": "CIV",
        "cape verde": "CPV",
        "czech republic": "CZE",
        "eswatini": "SWZ",
        "swaziland": "SWZ",
        "macedonia": "MKD",
        "north macedonia": "MKD",
        "russia": "RUS",
        "south korea": "KOR",
        "north korea": "PRK",
        "laos": "LAO",
        "moldova": "MDA",
        "tanzania": "TZA",
        "venezuela": "VEN",
        "bolivia": "BOL",
        "brunei": "BRN",
        "micronesia": "FSM",
        "syria": "SYR",
        "iran": "IRN",
        "palestine": "PSE",
        "state of palestine": "PSE",
        "west bank and gaza": "PSE",
        "occupied palestinian territory": "PSE",
        "kyrgyz republic": "KGZ",
        "the gambia": "GMB",
        "gambia": "GMB",
        "congo brazzaville": "COG",
        "republic of the congo": "COG",
        "congo republic": "COG",
        "congo kinshasa": "COD",
        "democratic republic of the congo": "COD",
        "drc": "COD",
        "burma": "MMR",
        "myanmar": "MMR",
        "turkey": "TUR",
        "turkiye": "TUR",
        "cote d ivoire": "CIV",
        "sao tome and principe": "STP",
        "east timor": "TLS",
        "timor leste": "TLS",
        "united states": "USA",
        "u s a": "USA",
        "u s": "USA",  # cautious: only when appears as tokenized phrase
        "united kingdom": "GBR",
        "great britain": "GBR",
        "britain": "GBR",
        "cabo verde": "CPV",
        "hong kong": "HKG",
        "macao": "MAC",
    }
    # Normalize keys
    return { _normalize_text(k): v for k, v in aliases.items() }


def _aggregate_aliases() -> Dict[str, List[str]]:
    """Region/aggregate names mapped to lists of ISO3 codes.

    Keys must be normalized via _normalize_text. Keep conservative, widely agreed sets.
    """
    aggregates: Dict[str, List[str]] = {
        # Balkans (geographic/cultural; exclude Kosovo due to ISO3166 absence)
        "balkan states": [
            "ALB", "BIH", "BGR", "HRV", "GRC", "MNE", "MKD", "ROU", "SRB", "SVN",
        ],
        "balkans": [
            "ALB", "BIH", "BGR", "HRV", "GRC", "MNE", "MKD", "ROU", "SRB", "SVN",
        ],
        "western balkans": [
            "ALB", "BIH", "MNE", "MKD", "SRB",
        ],
        # G5 Sahel
        "g5 sahel": ["BFA", "TCD", "MLI", "MRT", "NER"],
        "g-5 sahel": ["BFA", "TCD", "MLI", "MRT", "NER"],
        "g 5 sahel": ["BFA", "TCD", "MLI", "MRT", "NER"],
        # MENA
        "mena": [
            "DZA", "BHR", "DJI", "EGY", "IRN", "IRQ", "ISR", "JOR", "KWT", "LBN",
            "LBY", "MLT", "MAR", "OMN", "QAT", "SAU", "SYR", "TUN", "ARE", "PSE", "YEM",
        ],
        "middle east and north africa": [
            "DZA", "BHR", "DJI", "EGY", "IRN", "IRQ", "ISR", "JOR", "KWT", "LBN",
            "LBY", "MLT", "MAR", "OMN", "QAT", "SAU", "SYR", "TUN", "ARE", "PSE", "YEM",
        ],
        "middle east & north africa": [
            "DZA", "BHR", "DJI", "EGY", "IRN", "IRQ", "ISR", "JOR", "KWT", "LBN",
            "LBY", "MLT", "MAR", "OMN", "QAT", "SAU", "SYR", "TUN", "ARE", "PSE", "YEM",
        ],
        # Pacific Atoll countries (conservative core set)
        "pacific atoll": ["KIR", "MHL", "TUV", "TKL"],
        "pacific atoll countries": ["KIR", "MHL", "TUV", "TKL"],
        "pacific atolls": ["KIR", "MHL", "TUV", "TKL"],
    }
    return { _normalize_text(k): v for k, v in aggregates.items() }


def _continent_code_to_aggregate(cont: str) -> Optional[str]:
    mapping: Dict[str, str] = {
        "AF": GeoAggregate.CONTINENT_AF.value,
        "AN": GeoAggregate.CONTINENT_AN.value,
        "AS": GeoAggregate.CONTINENT_AS.value,
        "EU": GeoAggregate.CONTINENT_EU.value,
        "NA": GeoAggregate.CONTINENT_NA.value,
        "OC": GeoAggregate.CONTINENT_OC.value,
        "SA": GeoAggregate.CONTINENT_SA.value,
    }
    return mapping.get(cont)


def compute_continent_aggregates(iso3_codes: Iterable[str]) -> List[str]:
    """Derive continent aggregates (e.g., "continent:AF") for given ISO3 codes.

    Requires optional pycountry_convert. On failure, returns an empty list.
    """
    if not (_a3_to_a2 and _a2_to_continent):
        return []
    seen: set[str] = set()
    out: List[str] = []
    for code in iso3_codes:
        try:
            a2 = _a3_to_a2(code)
            cont = _a2_to_continent(a2)
        except Exception:
            continue
        agg = _continent_code_to_aggregate(cont)
        if agg and agg not in seen:
            seen.add(agg)
            out.append(agg)
    return out


def build_country_index() -> List[Tuple[str, CountryEntry]]:
    """Build a list of (normalized_name_variant, CountryEntry) pairs.

    The list is sorted by variant length descending to prefer longer matches
    (e.g., "democratic republic of the congo" before "congo").
    """
    variants: List[Tuple[str, CountryEntry]] = []

    # From pycountry base data
    for c in pycountry.countries:
        alpha3 = c.alpha_3
        display = getattr(c, "common_name", None) or getattr(c, "official_name", None) or c.name
        entry = CountryEntry(alpha3=alpha3, display_name=display)

        names: List[str] = [c.name]
        common_name = getattr(c, "common_name", None)
        if common_name:
            names.append(common_name)
        official_name = getattr(c, "official_name", None)
        if official_name:
            names.append(official_name)

        # Deduplicate normalized variants
        seen: set[str] = set()
        for name in names:
            norm = _normalize_text(name)
            if norm and norm not in seen:
                variants.append((norm, entry))
                seen.add(norm)

    # Add manual aliases
    for alias, alpha3 in _manual_aliases().items():
        # Find a representative display name if available
        try:
            c = pycountry.countries.get(alpha_3=alpha3)
            display = getattr(c, "common_name", None) or getattr(c, "official_name", None) or c.name
        except Exception:
            display = alias
        entry = CountryEntry(alpha3=alpha3, display_name=display)
        variants.append((alias, entry))

    # Sort by length descending to prefer the longest variant match first
    variants.sort(key=lambda item: len(item[0]), reverse=True)
    return variants


def find_country_iso3_in_title(title: str, variants: Optional[List[Tuple[str, CountryEntry]]] = None) -> Optional[CountryEntry]:
    """Return the CountryEntry if a country is detected in the title.

    Matching uses normalized substring search over known country name variants.
    """
    norm_title = _normalize_text(title)
    if not norm_title:
        return None

    if variants is None:
        variants = build_country_index()

    # Simple space-delimited word search is enough because both title and
    # variants are punctuation-free. We look for variant as a whole-word substring.
    title_words = f" {norm_title} "
    for variant, entry in variants:
        needle = f" {variant} "
        if needle in title_words:
            # Special case: "u s" may appear due to normalization; avoid false positives
            if variant == "u s" and "united states" not in title_words and "u s a" not in title_words:
                continue
            return entry
    return None


def classify_title_to_iso3_list(title: str,
                                variants: Optional[List[Tuple[str, CountryEntry]]] = None,
                                aggregate_map: Optional[Dict[str, List[str]]] = None) -> List[str]:
    """Return list of ISO3 codes for a title.

    - If an aggregate term is found, return its ISO3 list.
    - Else if a single country is detected, return [ISO3].
    - Else return [].
    """
    norm_title = _normalize_text(title)
    if not norm_title:
        return []

    if aggregate_map is None:
        aggregate_map = _aggregate_aliases()

    # Disambiguate Congo cases early to avoid matching "republic of congo" inside DR Congo
    title_words = f" {norm_title} "
    if (
        " drc " in title_words
        or " democratic republic of congo " in title_words
        or " congo kinshasa " in title_words
        or " democratic republic of the congo " in title_words
    ):
        return ["COD"]
    if (
        " republic of the congo " in title_words
        or " congo brazzaville " in title_words
    ):
        return ["COG"]

    # Aggregate detection: longest-first matching
    for agg_name, codes in sorted(aggregate_map.items(), key=lambda kv: len(kv[0]), reverse=True):
        if f" {agg_name} " in f" {norm_title} ":
            # Deduplicate and return ordered result
            seen: set[str] = set()
            out: List[str] = []
            for c in codes:
                if c not in seen:
                    seen.add(c)
                    out.append(c)
            return out

    # Multi-country detection: collect all country variants present
    if variants is None:
        variants = build_country_index()
    title_words = f" {norm_title} "
    work = title_words  # mutable working copy to mask matched spans
    seen_codes: set[str] = set()
    ordered_codes: List[str] = []
    for variant, entry in variants:
        needle = f" {variant} "
        pos = work.find(needle)
        if pos != -1:
            # Guard against false-positive 'u s'
            if variant == "u s" and (" united states " not in title_words and " u s a " not in title_words):
                continue
            code = entry.alpha3
            if code not in seen_codes:
                seen_codes.add(code)
                ordered_codes.append(code)
            # Mask this occurrence to avoid overlapping shorter-name matches
            work = work[:pos] + (" " * len(needle)) + work[pos + len(needle):]
    return ordered_codes


def classify_titles(titles: Iterable[str]) -> List[Tuple[str, List[str]]]:
    variants = build_country_index()
    aggregates = _aggregate_aliases()
    results: List[Tuple[str, List[str]]] = []
    for t in titles:
        t = t.strip()
        if not t:
            continue
        codes = classify_title_to_iso3_list(t, variants, aggregates)
        results.append((t, codes))
    return results


def _iter_db_titles() -> Iterable[str]:
    try:
        with Session(engine) as session:
            rows = session.exec(select(Publication.title)).all()
            for title in rows:
                if title:
                    yield title
    except Exception as exc:
        print(f"Failed to read titles from database: {exc}", file=sys.stderr)
        return []


def main(argv: Optional[List[str]] = None) -> int:
    # Fetch publications and update publication_metadata.geographical.iso3_country_codes
    updates = 0
    total = 0
    variants = build_country_index()
    aggregates = _aggregate_aliases()
    with Session(engine) as session:
        pubs: List[Publication] = session.exec(select(Publication)).all()
        for pub in pubs:
            total += 1
            title = pub.title or ""
            codes = classify_title_to_iso3_list(title, variants, aggregates)
            existing: Optional[GeographicalData] = pub.geographical_data
            existing_codes: List[str] = existing.iso3_country_codes if existing else []
            existing_aggs: List[str] = existing.aggregates if existing else []

            # Derive continent aggregates and merge with existing aggregates
            continent_aggs = compute_continent_aggregates(codes)
            merged_aggs: List[str] = list(existing_aggs)
            for agg in continent_aggs:
                if agg not in merged_aggs:
                    merged_aggs.append(agg)

            # Only update when there is a change
            if codes != existing_codes or merged_aggs != existing_aggs:
                new_geo = GeographicalData(
                    iso3_country_codes=codes,
                    aggregates=merged_aggs,
                )
                pub.geographical_data = new_geo
                session.add(pub)
                updates += 1
                print(
                    f"UPDATED\t{pub.id}\t{title}\t"
                    f"codes:{json.dumps(existing_codes)}->{json.dumps(codes)}\t"
                    f"aggregates:{json.dumps(existing_aggs)}->{json.dumps(merged_aggs)}"
                )
        if updates:
            session.commit()
    print(f"Summary: updated {updates} of {total} publications.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
