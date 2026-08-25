#!/usr/bin/env python3
"""Apply cached MarineTraffic route-forecast links to generated cruise feeds.

This runs after the source scrapers. It never performs a MarineTraffic network
request; it only reads marinetraffic_shipids.json. DTSTAMP values are stabilized
against the committed feed so a cached-link rewrite does not create noisy
calendar commits on every run.
"""

from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from icalendar import Calendar

CACHE_FILE = Path("marinetraffic_shipids.json")
FEEDS = (Path("all-ports.ics"), Path("cobh-cruise.ics"))


def load_mappings() -> dict[str, dict]:
    try:
        data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        mappings = data.get("mappings") or {}
        return mappings if isinstance(mappings, dict) else {}
    except Exception:
        return {}


def committed_payload(path: Path) -> bytes | None:
    try:
        return subprocess.check_output(["git", "show", f"HEAD:{path.as_posix()}"], stderr=subprocess.DEVNULL)
    except Exception:
        return None


def events(cal: Calendar | None) -> list:
    if cal is None:
        return []
    return [component for component in cal.walk() if component.name == "VEVENT"]


def uid(component) -> str:
    return str(component.get("UID") or "")


def signature(component) -> tuple[bytes, ...]:
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


def replace_dtstamp(component, value: datetime) -> None:
    if "DTSTAMP" in component:
        del component["DTSTAMP"]
    component.add("dtstamp", value)


def stabilize_dtstamps(new_cal: Calendar, old_cal: Calendar | None) -> None:
    old_by_uid = {uid(e): e for e in events(old_cal) if uid(e)}
    now = datetime.now(timezone.utc)

    for component in events(new_cal):
        old = old_by_uid.get(uid(component))
        if old is not None and signature(old) == signature(component):
            try:
                old_stamp = old.decoded("DTSTAMP")
            except Exception:
                old_stamp = None
            if isinstance(old_stamp, datetime):
                replace_dtstamp(component, old_stamp)
                continue
        replace_dtstamp(component, now)


def imo_for_component(component, description: str) -> str | None:
    match = re.search(r"imo[:/](\d{7})", description, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.match(r"(\d{7})-", uid(component))
    return match.group(1) if match else None


def apply_links(cal: Calendar, mappings: dict[str, dict]) -> int:
    changed = 0
    for component in events(cal):
        description = str(component.get("DESCRIPTION") or "")
        imo = imo_for_component(component, description)
        if not imo:
            continue
        record = mappings.get(imo) or {}
        shipid = str(record.get("shipid") or "").strip()
        if not shipid.isdigit():
            continue

        route = f"https://www.marinetraffic.com/en/ais/home/shipid:{shipid}/tracktype:6"
        lines = description.splitlines()
        replaced = False
        new_lines: list[str] = []
        for line in lines:
            if line.strip().startswith("🔗"):
                prefix = line[: len(line) - len(line.lstrip())]
                new_lines.append(f"{prefix}🔗 {route}")
                replaced = True
            else:
                new_lines.append(line)
        if not replaced:
            new_lines.append(f"🔗 {route}")

        new_description = "\n".join(new_lines)
        if new_description != description:
            if "DESCRIPTION" in component:
                del component["DESCRIPTION"]
            component.add("description", new_description)
            changed += 1
    return changed


def main() -> None:
    mappings = load_mappings()
    mapped = sum(bool(str(v.get("shipid") or "").isdigit()) for v in mappings.values())
    print(f"Cached MarineTraffic ship IDs: {mapped}")

    total_changed = 0
    for path in FEEDS:
        cal = Calendar.from_ical(path.read_bytes())
        old_payload = committed_payload(path)
        old_cal = Calendar.from_ical(old_payload) if old_payload else None
        changed = apply_links(cal, mappings)
        stabilize_dtstamps(cal, old_cal)
        path.write_bytes(cal.to_ical())
        total_changed += changed
        print(f"{path}: route links applied to {changed} events")

    print(f"MarineTraffic route-link updates: {total_changed}")


if __name__ == "__main__":
    main()
