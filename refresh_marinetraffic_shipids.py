#!/usr/bin/env python3
"""Refresh cached MarineTraffic ship IDs for vessels in the cruise calendar.

Calendar generation never depends on MarineTraffic. This low-frequency helper
uses Wikidata references first and the public MarineTraffic vessel page as a
best-effort fallback. Successful mappings are rechecked periodically; unresolved
IMOs are backed off before retrying so they cannot starve newly seen vessels.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from icalendar import Calendar

CACHE_FILE = Path("marinetraffic_shipids.json")
FEED_FILE = Path("all-ports.ics")
STALE_DAYS = int(os.getenv("MT_STALE_DAYS", "180"))
MISS_RETRY_DAYS = int(os.getenv("MT_MISS_RETRY_DAYS", "30"))
MAX_LOOKUPS = int(os.getenv("MT_MAX_LOOKUPS", "60"))

DETAILS_URL = "https://www.marinetraffic.com/en/ais/details/ships/imo:{imo}"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
USER_AGENT = "TheArchCobh-CruiseCalendar/1.0 (https://github.com/thearchcobh/cruiseschedule)"


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
    cal = Calendar.from_ical(FEED_FILE.read_bytes())
    found: dict[str, str] = {}
    for component in cal.walk():
        if component.name != "VEVENT":
            continue
        description = str(component.get("DESCRIPTION") or "")
        link = extract_line(description, "🔗")
        match = re.search(r"imo[:/](\d{7})", link, flags=re.IGNORECASE)
        if not match:
            match = re.match(r"(\d{7})-", str(component.get("UID") or ""))
        if not match:
            continue
        imo = match.group(1)
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
    if not record:
        return True
    shipid = str(record.get("shipid") or "")
    if shipid.isdigit():
        checked = parse_timestamp(record.get("last_verified_at"))
        return checked is None or checked < now - timedelta(days=STALE_DAYS)
    attempted = parse_timestamp(record.get("last_attempt_at"))
    return attempted is None or attempted < now - timedelta(days=MISS_RETRY_DAYS)


def shipid_from_text(text: str) -> str | None:
    for pattern in (
        r"shipid[:/](\d+)",
        r"[\"']shipid[\"']\s*:\s*[\"']?(\d+)",
        r"[\"']shipId[\"']\s*:\s*[\"']?(\d+)",
        r"[\"']ship_id[\"']\s*:\s*[\"']?(\d+)",
    ):
        match = re.search(pattern, text or "", flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def wikidata_candidates(imo: str) -> list[str]:
    query = f'SELECT ?item WHERE {{ ?item wdt:P458 "{imo}". }} LIMIT 10'
    response = requests.get(
        WIKIDATA_SPARQL,
        params={"query": query, "format": "json"},
        timeout=25,
        headers={"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"},
    )
    response.raise_for_status()
    qids: list[str] = []
    for row in response.json().get("results", {}).get("bindings", []):
        value = ((row.get("item") or {}).get("value") or "")
        qid = value.rsplit("/", 1)[-1]
        if re.fullmatch(r"Q\d+", qid):
            qids.append(qid)
    return qids


def resolve_shipid_wikidata(imo: str) -> str | None:
    for qid in wikidata_candidates(imo):
        response = requests.get(
            WIKIDATA_ENTITY.format(qid=qid), timeout=20, headers={"User-Agent": USER_AGENT}
        )
        response.raise_for_status()
        claims = response.json().get("entities", {}).get(qid, {}).get("claims", {})
        for property_claims in claims.values():
            for claim in property_claims:
                for ref in claim.get("references", []) or []:
                    for snaks in (ref.get("snaks") or {}).values():
                        for snak in snaks:
                            value = (snak.get("datavalue") or {}).get("value")
                            if not isinstance(value, str) or "marinetraffic.com" not in value.lower():
                                continue
                            host = (urlparse(value).hostname or "").lower()
                            if host.endswith("marinetraffic.com"):
                                shipid = shipid_from_text(value)
                                if shipid:
                                    return shipid
    return None


def resolve_shipid_marinetraffic(imo: str) -> str | None:
    response = requests.get(
        DETAILS_URL.format(imo=imo), timeout=30, allow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 AppleWebKit/537.36 Chrome/151 Safari/537.36"},
    )
    response.raise_for_status()
    return shipid_from_text(response.url) or shipid_from_text(response.text)


def resolve_shipid(imo: str) -> tuple[str | None, str | None]:
    try:
        shipid = resolve_shipid_wikidata(imo)
        if shipid:
            return shipid, "wikidata_marinetraffic_reference"
    except Exception as exc:
        print(f"{imo}: Wikidata lookup failed: {exc}")
    try:
        shipid = resolve_shipid_marinetraffic(imo)
        if shipid:
            return shipid, "marinetraffic_public_page"
    except Exception as exc:
        print(f"{imo}: MarineTraffic lookup failed: {exc}")
    return None, None


def main() -> None:
    cache = load_cache()
    mappings: dict[str, dict] = cache["mappings"]
    vessels = current_vessels()
    now = utc_now()
    candidates = [
        (imo, vessel) for imo, vessel in sorted(vessels.items())
        if needs_lookup(mappings.get(imo), now)
    ][:MAX_LOOKUPS]

    print(f"Known IMOs in calendar: {len(vessels)}")
    print(f"Mappings already cached: {sum(str(v.get('shipid') or '').isdigit() for v in mappings.values())}")
    print(f"Looking up this run: {len(candidates)}")

    changed = False
    for imo, vessel in candidates:
        previous = mappings.get(imo) or {}
        shipid, source = resolve_shipid(imo)
        if shipid:
            record = {
                "imo": imo,
                "vessel": vessel or previous.get("vessel") or None,
                "shipid": shipid,
                "last_attempt_at": iso(now),
                "last_verified_at": iso(now),
                "resolution_source": source,
                "details_url": DETAILS_URL.format(imo=imo),
                "route_forecast_url": f"https://www.marinetraffic.com/en/ais/home/shipid:{shipid}/tracktype:6",
            }
            print(f"{imo} {vessel}: shipid={shipid} via {source}")
        else:
            record = {
                **previous,
                "imo": imo,
                "vessel": vessel or previous.get("vessel") or None,
                "shipid": previous.get("shipid"),
                "last_attempt_at": iso(now),
                "details_url": DETAILS_URL.format(imo=imo),
            }
            print(f"{imo} {vessel}: no shipid found; retry after {MISS_RETRY_DAYS} days")
        if previous != record:
            mappings[imo] = record
            changed = True

    if changed:
        cache["updated_at"] = iso(now)
        CACHE_FILE.write_text(json.dumps(cache, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print("MarineTraffic cache updated")
    else:
        print("No MarineTraffic cache changes")


if __name__ == "__main__":
    main()
