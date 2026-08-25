#!/usr/bin/env python3
"""Refresh cached MarineTraffic ship IDs for vessels in the cruise calendar.

The normal calendar build never depends on MarineTraffic. This helper runs on a
separate, low-frequency schedule and only updates the cache when it can resolve
a vessel confidently from its IMO number.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from icalendar import Calendar

CACHE_FILE = Path("marinetraffic_shipids.json")
FEED_FILE = Path("all-ports.ics")
STALE_DAYS = int(os.getenv("MT_STALE_DAYS", "180"))
MAX_LOOKUPS = int(os.getenv("MT_MAX_LOOKUPS", "15"))

DETAILS_URL = "https://www.marinetraffic.com/en/ais/details/ships/imo:{imo}"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_cache() -> dict:
    try:
        data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    if not isinstance(data, dict):
        data = {}
    data.setdefault("version", 1)
    data.setdefault("updated_at", None)
    data.setdefault("mappings", {})
    return data


def extract_line(description: str, prefix: str) -> str:
    for line in description.splitlines():
        line = line.strip()
        if line.startswith(prefix):
            return line[len(prefix) :].strip()
    return ""


def current_vessels() -> dict[str, str]:
    payload = FEED_FILE.read_bytes()
    cal = Calendar.from_ical(payload)
    found: dict[str, str] = {}

    for component in cal.walk():
        if component.name != "VEVENT":
            continue
        description = str(component.get("DESCRIPTION") or "")
        link = extract_line(description, "🔗")
        imo_match = re.search(r"imo[:/](\d{7})", link, flags=re.IGNORECASE)
        if not imo_match:
            uid = str(component.get("UID") or "")
            uid_match = re.match(r"(\d{7})-", uid)
            if uid_match:
                imo_match = uid_match
        if not imo_match:
            continue

        imo = imo_match.group(1)
        vessel_line = extract_line(description, "🛳")
        vessel = vessel_line.split(",", 1)[0].strip() if vessel_line else ""
        found.setdefault(imo, vessel)

    return found


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def needs_lookup(record: dict | None, now: datetime) -> bool:
    if not record or not str(record.get("shipid") or "").isdigit():
        return True
    checked = parse_timestamp(record.get("last_verified_at"))
    return checked is None or checked < now - timedelta(days=STALE_DAYS)


def resolve_shipid(imo: str) -> str | None:
    url = DETAILS_URL.format(imo=imo)
    response = requests.get(
        url,
        timeout=30,
        allow_redirects=True,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151 Safari/537.36"
            )
        },
    )
    response.raise_for_status()

    candidates = [response.url, response.text]
    patterns = (
        r"shipid[:/](\d+)",
        r"[\"']shipid[\"']\s*:\s*[\"']?(\d+)",
        r"[\"']shipId[\"']\s*:\s*[\"']?(\d+)",
        r"[\"']ship_id[\"']\s*:\s*[\"']?(\d+)",
    )
    for text in candidates:
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return match.group(1)
    return None


def main() -> None:
    cache = load_cache()
    mappings: dict[str, dict] = cache["mappings"]
    vessels = current_vessels()
    now = utc_now()

    candidates = [
        (imo, vessel)
        for imo, vessel in sorted(vessels.items())
        if needs_lookup(mappings.get(imo), now)
    ][:MAX_LOOKUPS]

    print(f"Known IMOs in calendar: {len(vessels)}")
    print(f"Mappings already cached: {sum(bool(v.get('shipid')) for v in mappings.values())}")
    print(f"Looking up this run: {len(candidates)}")

    changed = False
    for imo, vessel in candidates:
        try:
            shipid = resolve_shipid(imo)
        except Exception as exc:
            print(f"{imo} {vessel}: lookup failed: {exc}")
            continue

        if not shipid:
            print(f"{imo} {vessel}: no shipid found")
            continue

        previous = mappings.get(imo) or {}
        record = {
            "imo": imo,
            "vessel": vessel or previous.get("vessel") or None,
            "shipid": shipid,
            "last_verified_at": iso(now),
            "details_url": DETAILS_URL.format(imo=imo),
            "route_forecast_url": f"https://www.marinetraffic.com/en/ais/home/shipid:{shipid}/tracktype:6",
        }
        if previous != record:
            mappings[imo] = record
            changed = True
        print(f"{imo} {vessel}: shipid={shipid}")

    if changed:
        cache["updated_at"] = iso(now)
        CACHE_FILE.write_text(json.dumps(cache, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print("MarineTraffic cache updated")
    else:
        print("No MarineTraffic cache changes")


if __name__ == "__main__":
    main()
