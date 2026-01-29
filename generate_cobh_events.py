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


def parse_date_line(line):
    """
    Parse date lines like:
      Thu 29 January 2026
      Sun 1 February 2026
    We don't regex the month; we just try parsing.
    """
    try:
        dt = parse(line, dayfirst=True, fuzzy=True)
        if dt.year >= 2020:
            return dt.date()
    except Exception:
        pass
    return None


def parse_time_line(line):
    """
    Accept:
      21:00
      10:00
    Ignore ranges like '10:00 - 14:00' (those appear in opening-hours blocks)
    """
    m = re.fullmatch(r"(\d{1,2}:\d{2})", line.strip())
    return m.group(1) if m else None


def parse_sheet_events():
    """
    Headings:
    Event, Name, Date, Start Time, End Time, Notes
    """
    url = sheet_csv_url(SHEET_ID, SHEET_TAB_NAME)
    body = safe_get(url)

    if looks_like_html(body):
        print("[WARN] Google Sheet did not return CSV (looks like HTML).")
        print("[WARN] Confirm sharing / publish-to-web.")
        return []

    f = StringIO(body)
    reader = csv.DictReader(f)

    out = []
    row_count = 0

    for row in reader:
        row_count += 1
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

        # Start time optional; default 00:00
        start_time_raw = start_time_raw if start_time_raw else "00:00"

        try:
            start = TZ.localize(parse(f"{date_raw} {start_time_raw}", dayfirst=True, fuzzy=True))
        except Exception:
            continue

        if end_time_raw:
            try:
                end = TZ.localize(parse(f"{date_raw} {end_time_raw}", dayfirst=True, fuzzy=True))
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

    print(f"[DEBUG] Sheet rows read: {row_count}, events parsed: {len(out)}")
    return out


def parse_incobh_events():
    """
    InCobh listing structure (as rendered in the HTML):
      ### <a>Event title ...</a>
      Cobh
      Thu 29 January 2026
      21:00
      <venue link>
    There are also blocks like opening hours; we ignore those by only taking the first clean time line.
    """
    html = safe_get(INCOBH_UPCOMING)
    soup = BeautifulSoup(html, "html.parser")

    out = []

    for h3 in soup.find_all("h3"):
        a = h3.find("a", href=True)
        if not a:
            continue

        title = clean(a.get_text())
        url = a.get("href", "")

        container = h3.parent
        if not container:
            continue

        lines = [clean(t) for t in container.stripped_strings if clean(t)]
        if not lines:
            continue

        # Filter: must include a standalone "Cobh" line
        if "Cobh" not in lines:
            continue

        # Find date line (first parsable date with a year)
        event_date = None
        for t in lines:
            if re.search(r"\b20\d{2}\b", t):
                d = parse_date_line(t)
                if d:
                    event_date = d
                    break

        if not event_date:
            continue

        # Find first simple time line like "21:00"
        time_str = None
        for t in lines:
            ts = parse_time_line(t)
            if ts:
                time_str = ts
                break

        if not time_str:
            time_str = "00:00"

        start = TZ.localize(parse(f"{event_date.isoformat()} {time_str}", dayfirst=True, fuzzy=True))
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

    print(f"[DEBUG] InCobh events parsed: {len(deduped)}")
    return deduped


def main():
    cal = build_cal("Cobh Events (The Arch)")

    sheet_events = parse_sheet_events()
    incobh_events = parse_incobh_events()

    all_events = sheet_events + incobh_events
    if not all_events:
        raise RuntimeError(
            "No events generated from either source. "
            "InCobh may have changed layout OR the sheet isn't truly returning CSV to the runner."
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
    print(" - from sheet:", len(sheet_events))
    print(" - from incobh:", len(incobh_events))


if __name__ == "__main__":
    main()
