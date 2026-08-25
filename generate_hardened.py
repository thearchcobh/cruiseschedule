#!/usr/bin/env python3
"""Generate and harden The Arch calendar feeds.

This wrapper keeps the existing source-specific generators simple while adding:
- source-collapse safeguards before generated feeds replace known-good feeds;
- stable DTSTAMP values so unchanged source data does not create Git commits;
- preservation of historical cruise calls when Port of Cork drops old calls;
- improved InCobh venue extraction/fallbacks;
- cross-source event deduplication;
- an explicit '?' spend marker for unclassified cruise ships/lines.
"""

from __future__ import annotations

import html as html_lib
import json
import os
import re
from copy import deepcopy
from datetime import date, datetime, timezone
from typing import Iterable

from icalendar import Calendar

import generate_cobh_events as events
import generate_cobh_ics as cruises

CRUISE_FILES = (cruises.OUTPUT_ALL, cruises.OUTPUT_COBH)
EVENT_FILE = events.OUTPUT_EVENTS
ALL_FILES = (*CRUISE_FILES, EVENT_FILE)


def _read_bytes(path: str) -> bytes | None:
    try:
        with open(path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        return None


def _restore(backups: dict[str, bytes | None]) -> None:
    for path, payload in backups.items():
        if payload is None:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        else:
            with open(path, "wb") as f:
                f.write(payload)


def _load_calendar(payload: bytes | None) -> Calendar | None:
    if not payload:
        return None
    try:
        return Calendar.from_ical(payload)
    except Exception:
        return None


def _events(cal: Calendar | None) -> list:
    if cal is None:
        return []
    return [component for component in cal.walk() if component.name == "VEVENT"]


def _uid(component) -> str:
    value = component.get("UID")
    return str(value) if value is not None else ""


def _decoded_start(component):
    value = component.get("DTSTART")
    if value is None:
        return None
    try:
        return component.decoded("DTSTART")
    except Exception:
        return getattr(value, "dt", None)


def _start_date(component) -> date | None:
    value = _decoded_start(component)
    if isinstance(value, datetime):
        return value.astimezone(cruises.TZ).date() if value.tzinfo else value.date()
    if isinstance(value, date):
        return value
    return None


def _is_future_or_today(component) -> bool:
    start = _start_date(component)
    return bool(start and start >= datetime.now(cruises.TZ).date())


def _event_signature(component) -> tuple[bytes, ...]:
    keys = ("SUMMARY", "DTSTART", "DTEND", "LOCATION", "DESCRIPTION")
    out: list[bytes] = []
    for key in keys:
        value = component.get(key)
        if value is None:
            out.append(b"")
        else:
            try:
                out.append(value.to_ical())
            except Exception:
                out.append(str(value).encode("utf-8"))
    return tuple(out)


def _stabilize_dtstamps(new_cal: Calendar, old_cal: Calendar | None) -> None:
    old_by_uid = {_uid(e): e for e in _events(old_cal) if _uid(e)}
    now = datetime.now(timezone.utc)

    for component in _events(new_cal):
        uid = _uid(component)
        old = old_by_uid.get(uid)
        if old is not None and _event_signature(old) == _event_signature(component):
            old_stamp = old.get("DTSTAMP")
            if old_stamp is not None:
                component["DTSTAMP"] = deepcopy(old_stamp)
                continue
        component["DTSTAMP"] = now


def _assert_not_collapsed(
    label: str,
    new_events: Iterable,
    old_cal: Calendar | None,
    *,
    ratio: float = 0.5,
    min_baseline: int = 10,
) -> None:
    new_list = list(new_events)
    if not new_list:
        raise RuntimeError(f"{label}: scraper produced zero events; refusing to replace the feed")

    old_future = [e for e in _events(old_cal) if _is_future_or_today(e)]
    new_future = [e for e in new_list if _is_future_or_today(e)]

    if len(old_future) >= min_baseline and len(new_future) < len(old_future) * ratio:
        raise RuntimeError(
            f"{label}: future event count collapsed from {len(old_future)} to {len(new_future)}; "
            "refusing to replace the known-good feed"
        )


def _preserve_historical_cruises(new_cal: Calendar, old_cal: Calendar | None) -> int:
    if old_cal is None:
        return 0

    today = datetime.now(cruises.TZ).date()
    new_uids = {_uid(e) for e in _events(new_cal)}
    preserved = 0

    for old_event in _events(old_cal):
        uid = _uid(old_event)
        start = _start_date(old_event)
        if not uid or not start or uid in new_uids or start >= today:
            continue
        new_cal.add_component(deepcopy(old_event))
        preserved += 1

    return preserved


def _strip_emoji_prefix(summary: str) -> str:
    return re.sub(r"^[^\w\d]+\s*", "", summary or "").strip().lower()


def _dedupe_events(cal: Calendar) -> int:
    seen: set[tuple[str, str]] = set()
    duplicates = []

    for component in _events(cal):
        summary = _strip_emoji_prefix(str(component.get("SUMMARY") or ""))
        start = _decoded_start(component)
        key = (summary, str(start))
        if key in seen:
            duplicates.append(component)
        else:
            seen.add(key)

    for component in duplicates:
        cal.subcomponents.remove(component)

    return len(duplicates)


def _normalize_text(value: str) -> str:
    value = html_lib.unescape(value or "")
    return re.sub(r"\s+", " ", value).strip()


def _normalized_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _normalize_text(value).lower()).strip()


def _jsonld_event_name(soup) -> str:
    for script in soup.find_all("script", type="application/ld+json"):
        raw = (script.string or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        for obj in events._flatten_jsonld(data):
            if isinstance(obj, dict) and obj.get("@type") == "Event":
                return _normalize_text(str(obj.get("name") or ""))
    return ""


VENUE_OVERRIDES = {
    "cobh farmers market": "The Promenade",
}


def _venue_from_title(title: str) -> str:
    title_clean = _normalize_text(title)
    key = _normalized_title(title_clean)
    for fragment, venue in VENUE_OVERRIDES.items():
        if fragment in key:
            return venue

    match = re.search(
        r"\bat\s+(.+?)(?:\s+-\s+(?:mon|tue|wed|thu|fri|sat|sun)\b|\s+-\s+\d|$)",
        title_clean,
        flags=re.IGNORECASE,
    )
    if match:
        return _normalize_text(match.group(1))

    return ""


def _visible_venue(soup, title: str) -> str:
    title_key = _normalized_title(title)

    for element in soup.find_all(True):
        classes = " ".join(element.get("class") or [])
        element_id = element.get("id") or ""
        marker = f"{classes} {element_id}".lower()
        if "venue" not in marker and "location" not in marker:
            continue
        text = _normalize_text(element.get_text(" ", strip=True))
        text = re.sub(r"^(?:venue|location)\s*[:\-]\s*", "", text, flags=re.IGNORECASE)
        if not text or len(text) > 100:
            continue
        if _normalized_title(text) in {title_key, "cobh", "cork"}:
            continue
        if re.search(r"\b20\d{2}\b", text):
            continue
        return text

    return _venue_from_title(title)


def _patch_event_helpers() -> None:
    original_clean = events.clean
    original_extract = events.extract_event_jsonld

    def clean_unescaped(value: str) -> str:
        return html_lib.unescape(original_clean(value))

    def extract_with_better_venue(soup):
        result = original_extract(soup)
        if not result:
            return result

        title = _jsonld_event_name(soup)
        venue = _normalize_text(result.get("venue") or "")
        suspicious = not venue or (title and _normalized_title(venue) == _normalized_title(title))

        if suspicious:
            result["venue"] = _visible_venue(soup, title) or "Cobh"
        else:
            result["venue"] = venue
        return result

    events.clean = clean_unescaped
    events.extract_event_jsonld = extract_with_better_venue


def _patch_cruise_spend_marker() -> None:
    original = cruises.dollars_from_eur_per_pax

    def classify(value):
        return "?" if value is None else original(value)

    cruises.dollars_from_eur_per_pax = classify


def _write_calendar(path: str, cal: Calendar) -> None:
    with open(path, "wb") as f:
        f.write(cal.to_ical())


def main() -> None:
    backups = {path: _read_bytes(path) for path in ALL_FILES}
    old = {path: _load_calendar(payload) for path, payload in backups.items()}

    _patch_cruise_spend_marker()
    _patch_event_helpers()

    try:
        cruises.main()
        events.main()

        generated = {path: Calendar.from_ical(_read_bytes(path) or b"") for path in ALL_FILES}

        _assert_not_collapsed("All-port cruise feed", _events(generated[cruises.OUTPUT_ALL]), old[cruises.OUTPUT_ALL])
        _assert_not_collapsed("Cobh cruise feed", _events(generated[cruises.OUTPUT_COBH]), old[cruises.OUTPUT_COBH])
        _assert_not_collapsed("Cobh events feed", _events(generated[EVENT_FILE]), old[EVENT_FILE], ratio=0.4)

        preserved_all = _preserve_historical_cruises(generated[cruises.OUTPUT_ALL], old[cruises.OUTPUT_ALL])
        preserved_cobh = _preserve_historical_cruises(generated[cruises.OUTPUT_COBH], old[cruises.OUTPUT_COBH])
        removed_duplicates = _dedupe_events(generated[EVENT_FILE])

        for path in ALL_FILES:
            _stabilize_dtstamps(generated[path], old[path])
            _write_calendar(path, generated[path])

        print(f"Preserved historical cruise calls: all ports={preserved_all}, Cobh={preserved_cobh}")
        print(f"Removed duplicate Cobh events: {removed_duplicates}")

    except Exception:
        _restore(backups)
        raise


if __name__ == "__main__":
    main()
