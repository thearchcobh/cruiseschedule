#!/usr/bin/env python3
"""
generate_cobh_events.py

Creates cobh-events.ics by combining:
1) Events from a Google Sheet (tab: 'events') with columns:
   Event Name, Date, Start Time, End Time, Notes
2) Events scraped from InCobh upcoming listings (paged), enriched from each event page via JSON-LD.

Key behaviors:
- ONLY include InCobh events that are truly in Cobh:
  * If JSON-LD provides addressLocality => must equal "Cobh" (case-insensitive).
  * If JSON-LD has no locality => fall back to listing "first location token must be Cobh".
- Location in calendar = venue name from JSON-LD when available; otherwise best-effort from listing; otherwise "Cobh".
- If an event has no meaningful time (missing / 00:00), create an ALL-DAY event (DTSTART;VALUE=DATE).
- Multi-day all-day ranges are normalized to iCal exclusive DTEND (end date + 1 day).
- Emoji in SUMMARY:
  🎵 for music, 👨‍🌾 for markets, 🎫 for everything else (uses tags/keywords + title heuristics).
- Footer source line:
  InCobh events: "Data from InCobh.com"
  Sheet events:  "Data from The Arch"
"""

import csv
import json
import re
from datetime import datetime, timedelta, date
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple, Union

import pytz
import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse
from icalendar import Calendar, Event

TZ = pytz.timezone("Europe/Dublin")

INCOBH_PAGE1 = "https://incobh.com/events/?etype=upcoming"
INCOBH_PAGED = "https://incobh.com/events/page/{page}/?etype=upcoming"

SHEET_ID = "1pYxu33TbILiM6KCfM1hFRjiqSYvQIWvDjULdq7iFkhI"
SHEET_TAB_NAME = "events"

OUTPUT_EVENTS = "cobh-events.ics"


# -------------------------
# Helpers
# -------------------------
def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def norm_key(k: str) -> str:
    k = (k or "").strip().lower()
    k = re.sub(r"[^a-z0-9]+", "_", k)
    return k.strip("_")


def sheet_csv_url(sheet_id: str, tab_name: str) -> str:
    # Public sheet export as CSV
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={tab_name}"


def looks_like_html(text: str) -> bool:
    head = (text or "").lstrip().lower()[:300]
    return head.startswith("<!doctype html") or head.startswith("<html") or "accounts.google.com" in head


def safe_get(url: str) -> str:
    # Browser-like headers (helps with WAF/CDN blocks in GitHub Actions)
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
    # Some setups behave differently with trailing slash
    if r.status_code == 415 and not url.endswith("/"):
        r = requests.get(url + "/", timeout=30, headers=headers, allow_redirects=True)

    r.raise_for_status()
    return r.text


def build_cal(name: str) -> Calendar:
    cal = Calendar()
    cal.add("prodid", "-//The Arch Cobh//Cobh Events//EN")
    cal.add("version", "2.0")
    cal.add("x-wr-calname", name)
    cal.add("x-wr-timezone", "Europe/Dublin")
    return cal


def uid(prefix: str, title: str, start_val: Union[datetime, date]) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", (title or "").lower()).strip("-")[:60] or "event"
    if isinstance(start_val, datetime):
        stamp = start_val.strftime("%Y%m%dT%H%M")
    else:
        stamp = start_val.strftime("%Y%m%dT0000")
    return f"{prefix}-{base}-{stamp}-thearchcobh"


def is_midnight_like_time_str(t: str) -> bool:
    t = (t or "").strip()
    return t in ("00:00", "00:00:00", "12:00 AM", "12:00:00 AM")


def parse_date_only_line(line: str) -> Optional[date]:
    try:
        d = parse(line, dayfirst=True, fuzzy=True).date()
        if d.year >= 2020:
            return d
    except Exception:
        pass
    return None


def pick_first_hhmm(lines: List[str]) -> Optional[str]:
    for t in lines:
        if re.fullmatch(r"\d{1,2}:\d{2}", t.strip()):
            return t.strip()
    return None


def pick_first_line_with_year(lines: List[str]) -> Optional[str]:
    for t in lines:
        if re.search(r"\b20\d{2}\b", t):
            return t
    return None


def event_emoji(title: str, tags: List[str]) -> str:
    title_l = (title or "").lower()
    tags_l = " ".join((t or "").lower() for t in (tags or []))

    # Markets
    if "market" in title_l or "market" in tags_l or "farmers" in title_l or "farmer" in tags_l:
        return "👨‍🌾"

    # Music-ish
    if any(k in title_l for k in ["music", "gig", "concert", "trad", "session", "folk", "band"]) or "music" in tags_l:
        return "🎵"

    return "🎫"


# -------------------------
# JSON-LD extraction from event pages
# -------------------------
def _flatten_jsonld(root: Any) -> List[Dict[str, Any]]:
    """Flatten JSON-LD payloads into a list of dict objects."""
    objs: List[Dict[str, Any]] = []

    def add_obj(x: Any):
        if isinstance(x, dict):
            objs.append(x)
            if "@graph" in x and isinstance(x["@graph"], list):
                for g in x["@graph"]:
                    add_obj(g)
        elif isinstance(x, list):
            for item in x:
                add_obj(item)

    add_obj(root)
    return objs


def extract_event_jsonld(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """
    Attempts to find schema.org Event JSON-LD and return:
      start (date|datetime), end (date|datetime), venue (str), locality (str), tags (list[str])
    """
    scripts = soup.find_all("script", type="application/ld+json")
    for sc in scripts:
        raw = (sc.string or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        for obj in _flatten_jsonld(data):
            if not isinstance(obj, dict):
                continue
            if obj.get("@type") != "Event":
                continue

            start_raw = obj.get("startDate")
            end_raw = obj.get("endDate")

            start_val: Optional[Union[datetime, date]] = None
            end_val: Optional[Union[datetime, date]] = None

            # Parse start
            if start_raw:
                try:
                    p = parse(start_raw, dayfirst=True, fuzzy=True)
                    # Heuristic: if the source string doesn't include 'T' or time info, treat as date
                    if isinstance(start_raw, str) and "T" not in start_raw and p.hour == 0 and p.minute == 0:
                        start_val = p.date()
                    else:
                        start_val = TZ.localize(p) if p.tzinfo is None else p.astimezone(TZ)
                except Exception:
                    pass

            # Parse end
            if end_raw:
                try:
                    p = parse(end_raw, dayfirst=True, fuzzy=True)
                    if isinstance(end_raw, str) and "T" not in end_raw and p.hour == 0 and p.minute == 0:
                        end_val = p.date()
                    else:
                        end_val = TZ.localize(p) if p.tzinfo is None else p.astimezone(TZ)
                except Exception:
                    pass

            # Venue + locality
            venue = ""
            locality = ""
            loc = obj.get("location") or {}
            if isinstance(loc, dict):
                venue = clean(loc.get("name") or "")
                addr = loc.get("address") or {}
                if isinstance(addr, dict):
                    locality = clean(addr.get("addressLocality") or "")

            # Keywords/tags
            tags: List[str] = []
            kw = obj.get("keywords")
            if isinstance(kw, str):
                tags = [clean(x) for x in kw.split(",") if clean(x)]
            elif isinstance(kw, list):
                tags = [clean(x) for x in kw if clean(x)]

            return {
                "start": start_val,
                "end": end_val,
                "venue": venue,
                "locality": locality,
                "tags": tags,
            }

    return None


def enrich_from_event_page(event_url: str) -> Dict[str, Any]:
    """
    Enrich an InCobh event from its detail page.

    Returns:
      venue: str
      start: date|datetime|None
      end:   date|datetime|None
      tags:  list[str]
      is_cobh: True/False/None  (None means unknown / no locality data)
    """
    try:
        html = safe_get(event_url)
    except Exception as e:
        print(f"[WARN] Could not fetch event page {event_url}: {e}")
        return {"venue": "", "start": None, "end": None, "tags": [], "is_cobh": None}

    soup = BeautifulSoup(html, "html.parser")

    js = extract_event_jsonld(soup)
    if js:
        locality = (js.get("locality") or "").strip()
        if locality:
            is_cobh = locality.lower() == "cobh"
        else:
            is_cobh = None
        return {
            "venue": js.get("venue") or "",
            "start": js.get("start"),
            "end": js.get("end"),
            "tags": js.get("tags") or [],
            "is_cobh": is_cobh,
        }

    # If no JSON-LD found, keep it non-fatal and don't filter aggressively.
    return {"venue": "", "start": None, "end": None, "tags": [], "is_cobh": None}


# -------------------------
# Google Sheet events
# -------------------------
def parse_sheet_events() -> List[Dict[str, Any]]:
    """
    Expected headings (case-insensitive):
      Event Name, Date, Start Time, End Time, Notes
    """
    url = sheet_csv_url(SHEET_ID, SHEET_TAB_NAME)
    body = safe_get(url)

    if looks_like_html(body):
        print("[WARN] Google Sheet did not return CSV (looks like HTML). Check sharing/publish-to-web.")
        return []

    f = StringIO(body)
    reader = csv.DictReader(f)

    out: List[Dict[str, Any]] = []
    row_count = 0

    for row in reader:
        row_count += 1
        r = {norm_key(k): (v or "").strip() for k, v in row.items()}

        event_name = r.get("event_name") or r.get("event") or ""
        date_raw = r.get("date") or ""
        start_time_raw = r.get("start_time") or ""
        end_time_raw = r.get("end_time") or ""
        notes = r.get("notes") or ""

        if not event_name or not date_raw:
            continue

        # If time missing => all-day
        all_day = (not start_time_raw) or is_midnight_like_time_str(start_time_raw)

        if all_day:
            d0 = parse_date_only_line(date_raw)
            if not d0:
                # Try full datetime parse fallback
                try:
                    d0 = parse(date_raw, dayfirst=True, fuzzy=True).date()
                except Exception:
                    continue
            start_val: Union[date, datetime] = d0
            end_val: Union[date, datetime] = d0 + timedelta(days=1)
        else:
            try:
                start_dt = parse(f"{date_raw} {start_time_raw}", dayfirst=True, fuzzy=True)
                start_val = TZ.localize(start_dt) if start_dt.tzinfo is None else start_dt.astimezone(TZ)
            except Exception:
                continue

            if end_time_raw:
                try:
                    end_dt = parse(f"{date_raw} {end_time_raw}", dayfirst=True, fuzzy=True)
                    end_val = TZ.localize(end_dt) if end_dt.tzinfo is None else end_dt.astimezone(TZ)
                except Exception:
                    end_val = start_val + timedelta(hours=2)
            else:
                end_val = start_val + timedelta(hours=2)

        out.append(
            {
                "title": event_name,
                "start": start_val,
                "end": end_val,
                "location": "Cobh",
                "url": "",
                "notes": notes,
                "source": "The Arch",
                "tags": [],
            }
        )

    print(f"[DEBUG] Sheet rows read: {row_count}, events parsed: {len(out)}")
    return out


# -------------------------
# InCobh listing crawl + enrich
# -------------------------
def parse_incobh_events() -> List[Dict[str, Any]]:
    """
    Crawl InCobh upcoming events pages and extract Cobh events.
    Strategy:
      - Listing page discovers event title + URL and provides a rough location token.
      - Event page JSON-LD is used as the source of truth for:
          * Cobh validation when locality exists
          * venue/location
          * start/end date/time
          * tags/keywords
      - If JSON-LD lacks locality, we keep only events whose FIRST location token
        in the listing block is "Cobh" (prevents Cork bleed-through).
    """
    out: List[Dict[str, Any]] = []

    for page in range(1, 21):
        url = INCOBH_PAGE1 if page == 1 else INCOBH_PAGED.format(page=page)
        try:
            html = safe_get(url)
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
            event_url = a.get("href", "")

            # Collect block lines between this h3 and the next h3 (newline-preserving)
            lines: List[str] = []
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

            # Determine first location token from listing (Cobh/Cork)
            first_loc = None
            for t in lines:
                if t in ("Cobh", "Cork"):
                    first_loc = t
                    break

            # Enrich from event page JSON-LD (authoritative when present)
            enrich = enrich_from_event_page(event_url) if event_url else {"venue": "", "start": None, "end": None, "tags": [], "is_cobh": None}

            # Location filter logic:
            # - If JSON-LD provides locality => must be Cobh (is_cobh True), else exclude.
            # - If JSON-LD has no locality => fall back to listing first_loc must be Cobh.
            if enrich.get("is_cobh") is False:
                continue
            if enrich.get("is_cobh") is None and first_loc != "Cobh":
                continue

            venue = enrich.get("venue") or ""
            tags = enrich.get("tags") or []

            start_val = enrich.get("start")
            end_val = enrich.get("end")

            # If JSON-LD didn't give dates, fall back to listing parse
            if start_val is None or end_val is None:
                # Use listing after location line if possible
                if "Cobh" in lines:
                    idx = lines.index("Cobh")
                    after = lines[idx + 1 :]
                else:
                    after = lines

                date_line = pick_first_line_with_year(after)
                time_line = pick_first_hhmm(after) or "00:00"

                if not date_line:
                    continue

                all_day = is_midnight_like_time_str(time_line)

                if all_day:
                    d0 = parse_date_only_line(date_line)
                    if not d0:
                        continue
                    start_val = d0
                    end_val = d0 + timedelta(days=1)
                else:
                    try:
                        sdt = parse(f"{date_line} {time_line}", dayfirst=True, fuzzy=True)
                        start_val = TZ.localize(sdt) if sdt.tzinfo is None else sdt.astimezone(TZ)
                        end_val = start_val + timedelta(hours=2)
                    except Exception:
                        continue

            # Normalize all-day end (exclusive DTEND)
            if isinstance(start_val, date) and not isinstance(start_val, datetime):
                # start_val is a date
                if isinstance(end_val, datetime):
                    # weird mixed case: convert to date
                    end_val = end_val.date()

                if isinstance(end_val, date):
                    # If end <= start -> make it 1 day
                    if end_val <= start_val:
                        end_val = start_val + timedelta(days=1)
                    else:
                        # Assume end date is inclusive in source; iCal DTEND is exclusive
                        end_val = end_val + timedelta(days=1)

            location_val = venue or "Cobh"

            out.append(
                {
                    "title": title,
                    "start": start_val,
                    "end": end_val,
                    "location": location_val,
                    "url": event_url,
                    "notes": "",
                    "source": "InCobh",
                    "tags": tags,
                }
            )
            page_added += 1

        print(f"[DEBUG] InCobh page {page}: added {page_added}")

        # Don't stop early based on page_added; some pages may have no Cobh items but later pages might.
        # Stop only when h3s disappear (handled above).

    # Deduplicate by (title, start)
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for e in out:
        key = (e["title"].lower(), str(e["start"]))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(e)

    print(f"[DEBUG] InCobh total events parsed: {len(deduped)}")
    return deduped


# -------------------------
# Main: create ICS
# -------------------------
def main() -> None:
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
        start_val = e["start"]
        end_val = e["end"]

        ev.add("uid", uid("cobh-events", e["title"], start_val))
        ev.add("dtstamp", datetime.utcnow())

        emoji = event_emoji(e.get("title", ""), e.get("tags", []))
        ev.add("summary", f"{emoji} {e['title']}")

        # All-day vs timed
        if isinstance(start_val, date) and not isinstance(start_val, datetime):
            # All-day uses VALUE=DATE
            ev.add("dtstart", start_val)
            # end_val should be exclusive date
            ev.add("dtend", end_val if isinstance(end_val, date) else (start_val + timedelta(days=1)))
        else:
            ev.add("dtstart", start_val)
            ev.add("dtend", end_val)

        ev.add("location", e.get("location", "Cobh"))

        # Description
        desc_lines: List[str] = []

        notes = (e.get("notes") or "").strip()
        if notes:
            desc_lines.append(notes)
            desc_lines.append("")

        url = (e.get("url") or "").strip()
        if url:
            desc_lines.append(f"🔗 {url}")

        desc_lines.append("")
        desc_lines.append("Created by The Arch, Cobh")

        if e.get("source") == "InCobh":
            desc_lines.append("Data from InCobh.com")
        else:
            desc_lines.append("Data from The Arch")

        ev.add("description", "\n".join([x for x in desc_lines if x is not None]))

        cal.add_component(ev)

    with open(OUTPUT_EVENTS, "wb") as f:
        f.write(cal.to_ical())

    print("Wrote", OUTPUT_EVENTS, "events:", len(all_events))
    print(" - from The Arch (sheet):", len(sheet_events))
    print(" - from InCobh:", len(incobh_events))


if __name__ == "__main__":
    main()
