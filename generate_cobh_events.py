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

from datetime import date  # add this import near your other imports

def is_midnight_time_str(t):
    return (t or "").strip() in ("00:00", "00:00:00", "12:00:00 AM", "12:00 AM")

def parse_date_only_line(line):
    # e.g. "Thu 9 April 2026" -> date(2026,4,9)
    try:
        d = parse(line, dayfirst=True, fuzzy=True).date()
        return d if d.year >= 2020 else None
    except Exception:
        return None

def safe_get_jsonish_text(soup, heading_text):
    # Find an H2/H3/LI/tab heading containing heading_text, then grab nearby text block
    # Works well on event pages that have sections: Profile / Event Dates / Location etc.
    h = soup.find(lambda tag: tag.name in ("h2", "h3", "h4") and heading_text.lower() in tag.get_text(" ", strip=True).lower())
    if not h:
        return []
    block = h.find_parent() or h
    lines = []
    for t in block.get_text("\n", strip=True).split("\n"):
        t = clean(t)
        if t and t.lower() != heading_text.lower():
            lines.append(t)
    return lines

def enrich_from_event_page(event_url):
    """
    Pull venue + date range + tags from the event detail page.
    Tags are inferred from links like /events/tags/... and common category blocks.
    """
    try:
        html = safe_get(event_url)
    except Exception as e:
        print(f"[WARN] Could not fetch event page {event_url}: {e}")
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Venue
    venue = ""
    loc_lines = safe_get_jsonish_text(soup, "Location")
    if loc_lines:
        venue = loc_lines[0]

    # Date range
    start_d = None
    end_d = None
    date_lines = safe_get_jsonish_text(soup, "Event Dates")
    parsed_dates = []
    for l in date_lines:
        d = parse_date_only_line(l)
        if d:
            parsed_dates.append(d)
    if parsed_dates:
        start_d = parsed_dates[0]
        end_d = parsed_dates[-1]

    # Tags / categories (best-effort)
    tags = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        txt = clean(a.get_text())
        if not txt:
            continue
        if "/events/tags/" in href or "/events/tag/" in href:
            tags.add(txt)
        # some WordPress themes use categories rather than tags
        if "/category/" in href and "events" in href:
            tags.add(txt)

    return {
        "venue": venue,
        "start_date": start_d,
        "end_date": end_d,
        "tags": sorted(tags),
    }


def clean(s):
    return re.sub(r"\s+", " ", (s or "").strip())


def safe_get(url):
    # Browser-like headers (helps with WAF/CDN blocks that hit GitHub runners)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IE,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://incobh.com/",
    }

    r = requests.get(url, timeout=30, headers=headers, allow_redirects=True)

    # If blocked on this URL, try the same URL with a trailing slash (some setups differ)
    if r.status_code == 415 and not url.endswith("/"):
        r = requests.get(url + "/", timeout=30, headers=headers, allow_redirects=True)

    # If still blocked, raise with a clearer message
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
                "source": "The Arch",
            }
        )

    print(f"[DEBUG] Sheet rows read: {row_count}, events parsed: {len(out)}")
    return out


def parse_incobh_events():
    out = []

    def page_url(n):
        if n == 1:
            return "https://incobh.com/events/?etype=upcoming"
        return f"https://incobh.com/events/page/{n}/?etype=upcoming"

    for page in range(1, 21):
        try:
            html = safe_get(page_url(page))
        except Exception as e:
            print(f"[WARN] InCobh page {page} fetch failed: {e}")
            break

        soup = BeautifulSoup(html, "html.parser")
        h3s = soup.find_all("h3")
        if not h3s:
            print(f"[DEBUG] InCobh page {page}: no <h3> found, stopping.")
            break

        page_added = 0

        for h3 in h3s:
            a = h3.find("a", href=True)
            if not a:
                continue

            title = clean(a.get_text())
            url = a.get("href", "")

            # Collect text after this h3 until next h3 (newline-preserving)
            lines = []
            for el in h3.next_elements:
                if getattr(el, "name", None) == "h3":
                    break
                if hasattr(el, "get_text"):
                    txt = el.get_text("\n", strip=True)
                    if not txt:
                        continue
                    for part in txt.splitlines():
                        part = clean(part)
                        if part:
                            lines.append(part)

            if not lines:
                continue

            # Determine the FIRST location token in the block
            # (prevents Cork events being included just because 'Cobh' appears later)
            first_loc = None
            first_loc_idx = None
            for i, t in enumerate(lines):
                if t in ("Cobh", "Cork"):
                    first_loc = t
                    first_loc_idx = i
                    break

            if first_loc != "Cobh":
                continue  # ONLY Cobh events

            after = lines[first_loc_idx + 1 :]

            # Date line: first line containing a year
            date_line = ""
            for t in after:
                if re.search(r"\b20\d{2}\b", t):
                    date_line = t
                    break
            if not date_line:
                continue

            # Time line: first strict HH:MM
            time_line = ""
            for t in after:
                if re.fullmatch(r"\d{1,2}:\d{2}", t):
                    time_line = t
                    break
            if not time_line:
                time_line = "00:00"

            all_day = is_midnight_time_str(time_line)

            # Parse start/end from listing
            if all_day:
                d0 = parse_date_only_line(date_line)
                if not d0:
                    continue
                start_val = d0
                end_val = d0 + timedelta(days=1)
            else:
                try:
                    start_val = TZ.localize(parse(f"{date_line} {time_line}", dayfirst=True, fuzzy=True))
                    end_val = start_val + timedelta(hours=2)
                except Exception:
                    continue

            # Enrich from event page (venue, multi-day range, tags)
            venue = ""
            tags = []
            if url:
                enrich = enrich_from_event_page(url)
                if enrich:
                    venue = enrich.get("venue") or ""
                    tags = enrich.get("tags") or []
                    if enrich.get("start_date") and enrich.get("end_date"):
                        # If listing is all-day (or unreliable), prefer event-page range
                        if all_day:
                            start_val = enrich["start_date"]
                            end_val = enrich["end_date"] + timedelta(days=1)

            out.append(
                {
                    "title": title,
                    "start": start_val,
                    "end": end_val,
                    "location": venue or "Cobh",
                    "url": url,
                    "notes": "",
                    "source": "InCobh",
                    "tags": tags,
                }
            )
            page_added += 1

        print(f"[DEBUG] InCobh page {page}: added {page_added}")

    # Dedup
    seen = set()
    deduped = []
    for e in out:
        key = (e["title"].lower(), str(e["start"]))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(e)

    print(f"[DEBUG] InCobh total events parsed: {len(deduped)}")
    return deduped

def event_emoji(e):
    title = (e.get("title") or "").lower()
    tags = " ".join([t.lower() for t in (e.get("tags") or [])])

    # Farmers markets
    if "market" in title or "market" in tags:
        return "👨‍🌾"

    # Music-ish
    if any(k in title for k in ["music", "gig", "concert", "trad", "session"]) or "music" in tags:
        return "🎵"

    return "🎫"



def main():
    cal = build_cal("Cobh Events (The Arch)")

    sheet_events = parse_sheet_events()
    try:
        incobh_events = parse_incobh_events()
    except Exception as e:
        print(f"[WARN] InCobh fetch/parse failed: {e}")
        incobh_events = []


    all_events = sheet_events + incobh_events
    if not all_events:
        raise RuntimeError("No events generated from either source.")

    for e in all_events:
        ev = Event()
        ev.add("uid", uid("cobh-events", e["title"], e["start"]))
        ev.add("dtstamp", datetime.utcnow())
        ev.add("summary", f"{event_emoji(e)} {e['title']}")
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
        if e.get("source") == "InCobh":
            desc.append("Data from InCobh.com")
        else:
            desc.append("Data from The Arch")


        ev.add("description", "\n".join([d for d in desc if d is not None]))
        cal.add_component(ev)

    with open(OUTPUT_EVENTS, "wb") as f:
        f.write(cal.to_ical())

    print("Wrote", OUTPUT_EVENTS, "events:", len(all_events))
    print(" - from sheet:", len(sheet_events))
    print(" - from incobh:", len(incobh_events))


if __name__ == "__main__":
    main()
