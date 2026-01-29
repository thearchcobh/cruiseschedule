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

INCOBH_PAGE1 = "https://incobh.com/events/?etype=upcoming"

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


def norm_key(k):
    # "Event Name" -> "event_name"
    k = (k or "").strip().lower()
    k = re.sub(r"[^a-z0-9]+", "_", k)
    return k.strip("_")


def parse_sheet_events():
    """
    Sheet headings you gave:
    Event Name, Date, Start Time, End Time, Notes
    (Optionally Name column may exist in other rows; we support it.)
    """
    url = sheet_csv_url(SHEET_ID, SHEET_TAB_NAME)
    body = safe_get(url)

    if looks_like_html(body):
        print("[WARN] Google Sheet did not return CSV (looks like HTML).")
        return []

    f = StringIO(body)
    reader = csv.DictReader(f)

    out = []
    row_count = 0

    for row in reader:
        row_count += 1
        # normalize keys
        r = {norm_key(k): (v or "").strip() for k, v in row.items()}

        event_name = r.get("event_name") or r.get("event") or ""
        person_name = r.get("name") or ""
        date_raw = r.get("date") or ""
        start_time_raw = r.get("start_time") or ""
        end_time_raw = r.get("end_time") or ""
        notes = r.get("notes") or ""

        if not event_name or not date_raw:
            continue

        title = f"{event_name} — {person_name}" if person_name else event_name

        # If start time missing, make it all-day-ish at midnight
        start_time_raw = start_time_raw if start_time_raw else "00:00"

        try:
            start = TZ.localize(parse(f"{date_raw} {start_time_raw}", dayfirst=True, fuzzy=True))
        except Exception as e:
            print(f"[WARN] Sheet parse failed for start: date='{date_raw}' time='{start_time_raw}': {e}")
            continue

        if end_time_raw:
            try:
                end = TZ.localize(parse(f"{date_raw} {end_time_raw}", dayfirst=True, fuzzy=True))
            except Exception as e:
                print(f"[WARN] Sheet parse failed for end: date='{date_raw}' time='{end_time_raw}': {e}")
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
    Robust InCobh parser with pagination.
    - Walk pages: /events/?etype=upcoming, /events/page/2/?etype=upcoming, ...
    - For each event h3, collect following text until next h3
    - Filter: must contain standalone 'Cobh'
    - Date: must be a standalone weekday+date+year line (NOT the title)
    - Time: first HH:MM
    """
    out = []

    def page_url(n):
        if n == 1:
            return INCOBH_PAGE1
        return f"https://incobh.com/events/page/{n}/?etype=upcoming"

    for page in range(1, 11):  # scan up to 10 pages; stops early when empty
        html = safe_get(page_url(page))
        soup = BeautifulSoup(html, "html.parser")

        h3s = soup.find_all("h3")
        if not h3s:
            break

        page_events = 0

        for h3 in h3s:
            a = h3.find("a", href=True)
            if not a:
                continue

            title = clean(a.get_text())
            url = a.get("href", "")

            # Collect text AFTER this h3 until the next h3
            lines = []
            for el in h3.next_elements:
                if el is h3:
                    continue
                if getattr(el, "name", None) == "h3":
                    break
                if hasattr(el, "get_text"):
                    t = clean(el.get_text(" ", strip=True))
                    if not t:
                        continue
                    for part in re.split(r"\s{2,}|\n+", t):
                        part = clean(part)
                        if part:
                            lines.append(part)

            if not lines:
                continue

            # Must contain a standalone "Cobh"
            if "Cobh" not in lines:
                continue

            # Only search for date/time AFTER the location line, so we don't
            # accidentally parse the date embedded in the title.
            try:
                loc_idx = lines.index("Cobh")
            except ValueError:
                continue
            lines_after_loc = lines[loc_idx + 1 :]

            # Date line must look like: "Thu 29 January 2026"
            date_line = ""
            for t in lines_after_loc:
                if re.search(r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\b", t) and re.search(r"\b20\d{2}\b", t):
                    date_line = t
                    break
            if not date_line:
                continue

            # Time line is first strict HH:MM (ignore ranges like 10:00 - 14:00)
            time_line = ""
            for t in lines_after_loc:
                if re.fullmatch(r"\d{1,2}:\d{2}", t):
                    time_line = t
                    break
            if not time_line:
                time_line = "00:00"

            try:
                start = TZ.localize(parse(f"{date_line} {time_line}", dayfirst=True, fuzzy=True))
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
            page_events += 1

        print(f"[DEBUG] InCobh page {page} events parsed: {page_events}")

        # Stop when a page returns no matching events (usually means we've reached the end)
        if page_events == 0:
            break

    # Deduplicate by (title, start)
    seen = set()
    deduped = []
    for e in out:
        key = (e["title"].lower(), e["start"].strftime("%Y%m%dT%H%M"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(e)

    print(f"[DEBUG] InCobh total events parsed: {len(deduped)}")
    return deduped


def main():
    cal = build_cal("Cobh Events (The Arch)")

    sheet_events = parse_sheet_events()
    incobh_events = parse_incobh_events()

    all_events = sheet_events + incobh_events
    if not all_events:
        raise RuntimeError("No events generated from either source.")

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
