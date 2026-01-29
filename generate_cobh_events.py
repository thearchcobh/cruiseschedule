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
SHEET_TAB_NAME = "events"

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
    base = re.sub(r"[^a-z0-9]+", "-", (title or "").lower()).strip("-")[:60] or "event"
    return f"{prefix}-{base}-{start_dt.strftime('%Y%m%dT%H%M')}-thearchcobh"


def sheet_csv_url(sheet_id, tab_name):
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={tab_name}"


def looks_like_html(text):
    head = (text or "").lstrip().lower()[:200]
    return head.startswith("<!doctype html") or head.startswith("<html") or "accounts.google.com" in head


def parse_sheet_events():
    """
    Headings:
    Event, Name, Date, Start Time, End Time, Notes
    """
    url = sheet_csv_url(SHEET_ID, SHEET_TAB_NAME)
    body = safe_get(url)

    # If sheet isn't public, Google returns HTML. Detect and warn.
    if looks_like_html(body):
        print("[WARN] Google Sheet did not return CSV (looks like HTML).")
        print("[WARN] Make the sheet public ('Anyone with link can view') or 'Publish to web'.")
        return []

    f = StringIO(body)
    reader = csv.DictReader(f)
    out = []

    for row in reader:
        r = {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}

        event_name = r.get("event", "")
        person_name = r.get("name", "")
        date_raw = r.get("date", "")
        start_time_raw = r.get("start time", "")
        end_time_raw = r.get("end time", "")
        notes = r.get("notes", "")

        if not event_name or not date_raw:
            continue

        title = f"{event_name} — {person_name}" if person_name else event_name

        # If Start Time missing, default to 00:00 and treat as 2h block
        start_time_raw = start_time_raw if start_time_raw else "00:00"

        try:
            start = TZ.localize(parse(f"{date_raw} {start_time_raw}", dayfirst=True))
        except Exception:
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
    Matches the InCobh listing structure visible on the events page:
    Each event appears as: ### <a>Title</a> then lines like:
    Cobh
    Thu 29 January 2026
    21:00
    Venue...
    """
    html = safe_get(INCOBH_UPCOMING)
    soup = BeautifulSoup(html, "html.parser")

    out = []

    # Titles are in h3 -> a
    for h3 in soup.find_all("h3"):
        a = h3.find("a", href=True)
        if not a:
            continue

        title = clean(a.get_text())
        url = a.get("href", "")

        # The container around the h3 holds the text lines we need
        container = h3.parent
        if not container:
            continue

        lines = [clean(t) for t in container.stripped_strings if clean(t)]
        if not lines:
            continue

        # Require location == "Cobh" somewhere after the title
        # On the page, "Cobh" appears as a standalone line for Cobh events. :contentReference[oaicite:3]{index=3}
        if "Cobh" not in lines:
            continue

        # Grab first plausible date line and time line
        date_str = ""
        time_str = ""

        for t in lines:
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

        out.append(
            {
                "title": title,
                "start": start,
                "end": end,
                "location": "Cobh",
                "url": url,
                "notes": "",
                "source": "InCobh",
            }
        )

    # Deduplicate by (title, start)
    seen = set()
    deduped = []
    for e in out:
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

    print("Sheet events:", len(sheet_events))
    print("InCobh events:", len(incobh_events))

    all_events = sheet_events + incobh_events
    if not all_events:
        raise RuntimeError(
            "No events generated from either source. "
            "Fix sheet sharing OR InCobh page structure has changed."
        )

    for e in all_events:
        ev = Event()
        ev.add("uid", uid("cobh-events", e["title"], e["start"]))
        ev.add("dtstamp", datetime.utcnow())
        ev.add("summary", f"🎫 {e['title']}")
        ev.add("dtstart", e["start"])
        ev.add("dtend", e["end"])
        ev.add("location", e.get("location", "Cobh"))

        desc = []
        if e.get("notes"):
            desc.append(e["notes"].strip())
            desc.append("")

        if e.get("url"):
            desc.append(f"🔗 {e['url']}")

        desc.append("")
        desc.append("Created by The Arch, Cobh")
        desc.append("Data from InCobh + Google Sheet")

        ev.add("description", "\n".join([d for d in desc if d is not None]))
        cal.add_component(ev)

    with open(OUTPUT_EVENTS, "wb") as f:
        f.write(cal.to_ical())

    print("Wrote", OUTPUT_EVENTS, "events:", len(all_events))


if __name__ == "__main__":
    main()
