import re
import csv
from io import StringIO
from datetime import datetime, timedelta

import pytz
import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse
from icalendar import Calendar, Event

TZ = pytz.timezone("Europe/Dublin")

INCOBH_UPCOMING = "https://incobh.com/events/?etype=upcoming"

SHEET_ID = "1pYxu33TbILiM6KCfM1hFRjiqSYvQIWvDjULdq7iFkhI"
SHEET_TAB_NAME = "events"  # <- your tab name

OUTPUT_EVENTS = "cobh-events.ics"


def clean(s):
    return re.sub(r"\s+", " ", (s or "").strip())


def safe_get(url):
    r = requests.get(url, timeout=30, headers={"User-Agent": "thearchcobh"})
    r.raise_for_status()
    return r.text


def build_cal(name):
    cal = Calendar()
    cal.add("prodid", "-//The Arch Cobh//Cobh Events//EN")
    cal.add("version", "2.0")
    cal.add("x-wr-calname", name)
    cal.add("x-wr-timezone", "Europe/Dublin")
    return cal


def uid(prefix, title, start_dt):
    base = re.sub(r"[^a-z0-9]+", "-", (title or "").lower()).strip("-")[:50] or "event"
    return f"{prefix}-{base}-{start_dt.strftime('%Y%m%dT%H%M')}-thearchcobh"


def sheet_csv_url(sheet_id, tab_name):
    # Public sheet export as CSV
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={tab_name}"


def parse_sheet_events():
    """
    Sheet headings:
    Event, Name, Date, Start Time, End Time, Notes
    """
    url = sheet_csv_url(SHEET_ID, SHEET_TAB_NAME)
    csv_text = safe_get(url)

    f = StringIO(csv_text)
    reader = csv.DictReader(f)
    out = []

    for row in reader:
        # Normalize keys, keep original values
        r = {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}

        event_name = r.get("event", "")
        person_name = r.get("name", "")
        date_raw = r.get("date", "")
        start_time_raw = r.get("start time", "")
        end_time_raw = r.get("end time", "")
        notes = r.get("notes", "")

        if not event_name or not date_raw:
            continue

        title = event_name
        if person_name:
            title = f"{event_name} — {person_name}"

        # Build datetime strings
        # If no start time, treat as all-day (00:00) with 2h default
        start_time_raw = start_time_raw if start_time_raw else "00:00"

        try:
            start = TZ.localize(parse(f"{date_raw} {start_time_raw}", dayfirst=True))
        except Exception:
            # Skip unparseable rows
            continue

        if end_time_raw:
            try:
                end = TZ.localize(parse(f"{date_raw} {end_time_raw}", dayfirst=True))
            except Exception:
                end = start + timedelta(hours=2)
        else:
            end = start + timedelta(hours=2)

        out.append(
            {
                "title": title,
                "start": start,
                "end": end,
                "location": "Cobh",
                "url": "",
                "notes": notes,
                "source": "Google Sheet",
            }
        )

    return out


def parse_incobh_events():
    """
    Scrape incobh upcoming events and keep only those with location text 'Cobh'.
    The page lists location as a text line; emoji may not be present in the raw HTML.
    """
    html = safe_get(INCOBH_UPCOMING)
    soup = BeautifulSoup(html, "html.parser")

    events = []

    # Find event titles as links, then inspect nearby card/container text for location + date/time.
    for a in soup.find_all("a", href=True):
        title = clean(a.get_text())
        href = a.get("href")

        # Heuristic: event links usually have meaningful titles and are not navigation
        if not title or len(title) < 3:
            continue
        if href and href.startswith("#"):
            continue

        # Try to use the closest “card-like” container
        container = a.find_parent(["article", "div", "li", "section"])
        if not container:
            continue

        block_text = [clean(t) for t in container.stripped_strings]
        if not block_text:
            continue

        # Location filter: must contain 'Cobh' (and not just as part of another word)
        # Prefer exact token match when possible.
        has_cobh = any(t == "Cobh" for t in block_text)
        if not has_cobh:
            continue

        # Extract a date and time from the text block
        # (This is intentionally flexible because listings vary.)
        date_str = ""
        time_str = ""

        for t in block_text:
            # Examples might be like "Thu 29 January 2026" or "29 January 2026"
            if re.search(r"\b20\d{2}\b", t) and re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)", t, re.I):
                date_str = t
            if re.fullmatch(r"\d{1,2}:\d{2}", t):
                time_str = t

        if not date_str:
            continue
        if not time_str:
            time_str = "00:00"

        try:
            start = TZ.localize(parse(f"{date_str} {time_str}", dayfirst=True))
        except Exception:
            continue

        end = start + timedelta(hours=2)

        events.append(
            {
                "title": title,
                "start": start,
                "end": end,
                "location": "Cobh",
                "url": href,
                "notes": "",
                "source": "InCobh",
            }
        )

    # De-duplicate by (title, start)
    seen = set()
    deduped = []
    for e in events:
        key = (e["title"].lower(), e["start"].strftime("%Y%m%dT%H%M"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(e)

    return deduped


def main():
    cal = build_cal("Cobh Events (The Arch)")

    sheet_events = parse_sheet_events()
    incobh_events = parse_incobh_events()
    all_events = sheet_events + incobh_events

    if not all_events:
        raise RuntimeError(
            "No events generated. Check: (1) sheet sharing is public-readable, "
            "(2) the sheet tab name is correct, (3) InCobh layout hasn't changed."
        )

    for e in all_events:
        ev = Event()
        ev.add("uid", uid("cobh-events", e["title"], e["start"]))
        ev.add("dtstamp", datetime.utcnow())
        ev.add("summary", f"🎫 {e['title']}")
        ev.add("dtstart", e["start"])
        ev.add("dtend", e["end"])
        ev.add("location", e.get("location", "Cobh"))

        desc_lines = []
        if e.get("notes"):
            desc_lines.append(e["notes"].strip())
            desc_lines.append("")

        if e.get("url"):
            desc_lines.append(f"🔗 {e['url']}")

        # Footer
        desc_lines.append("")
        desc_lines.append("Created by The Arch, Cobh")
        desc_lines.append("Data from InCobh.com")

        ev.add("description", "\n".join([line for line in desc_lines if line is not None]))

        cal.add_component(ev)

    with open(OUTPUT_EVENTS, "wb") as f:
        f.write(cal.to_ical())

    print("Wrote", OUTPUT_EVENTS, "events:", len(all_events))
    print(" - from sheet:", len(sheet_events))
    print(" - from incobh:", len(incobh_events))


if __name__ == "__main__":
    main()
