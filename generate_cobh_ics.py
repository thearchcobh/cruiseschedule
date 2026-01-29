import re
from datetime import datetime

import pytz
import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse
from icalendar import Calendar, Event

SOURCE_URL = "https://www.portofcork.ie/print-cruise-schedule.php"
OUTPUT_ICS = "cobh-cruise.ics"
COBH_BERTH = "Cobh Cruise Terminal"
TZ = pytz.timezone("Europe/Dublin")


# ----------------------------
# Helpers
# ----------------------------
def clean(s):
    return re.sub(r"\s+", " ", (s or "").strip())


def is_month_row(cells):
    # e.g. ["April 2026"]
    return len(cells) == 1 and re.search(r"\b20\d{2}\b", cells[0])


def is_header_row(cells):
    text = " ".join(c.lower() for c in cells)
    return ("vessel" in text) and ("berth" in text) and ("arrival" in text) and ("departure" in text)


def find_col(header_cells, needle):
    """
    Find the index of the first header cell containing `needle` (case-insensitive).
    This handles headers like 'IMO No.' instead of exact 'IMO'.
    """
    needle = needle.lower()
    for i, h in enumerate(header_cells):
        if needle in (h or "").lower():
            return i
    return None


def extract_digits(raw):
    return re.sub(r"[^\d]", "", raw or "")


def marinetraffic_link(imo_raw):
    imo = extract_digits(imo_raw)
    if not imo:
        return ""
    # This is the format you confirmed works
    return f"https://www.marinetraffic.com/en/ais/details/ships/imo:{imo}/"


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:40] or "x"


def stable_uid(vessel, imo_raw, start_dt, berth):
    """
    Stable UID within a given arrival date so time changes update cleanly.
    Uses IMO if available, otherwise vessel slug.
    """
    day = start_dt.strftime("%Y%m%d")
    base = extract_digits(imo_raw) or slug(vessel)
    return f"{base}-{day}-{slug(berth)}-thearchcobh"


def pax_int(pax_str):
    if not pax_str:
        return None
    digits = re.sub(r"[^\d]", "", pax_str)
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def pax_signal(pax_value):
    # Tune thresholds any time
    if pax_value is None:
        return "⚪"
    if pax_value >= 3000:
        return "🔴"
    if pax_value >= 1000:
        return "🟠"
    return "🟢"


# ----------------------------
# Main
# ----------------------------
def main():
    resp = requests.get(
        SOURCE_URL,
        timeout=30,
        headers={"User-Agent": "thearchcobh-cruise-ical/1.0"},
    )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError("No <table> elements found on schedule page.")

    cal = Calendar()
    cal.add("prodid", "-//The Arch, Cobh//Cruise Schedule//EN")
    cal.add("version", "2.0")
    cal.add("x-wr-calname", "Cobh Cruise Calls (The Arch)")
    cal.add("x-wr-timezone", "Europe/Dublin")

    total_rows_seen = 0
    header_rows_seen = 0
    events_written = 0
    imo_found_count = 0
    imo_blank_count = 0

    for t_i, table in enumerate(tables):
        rows = table.find_all("tr")
        if not rows:
            continue

        idx = None
        header_cells = None

        for r_i, row in enumerate(rows):
            total_rows_seen += 1

            cells = [clean(c.get_text()) for c in row.find_all(["th", "td"])]
            if not cells:
                continue
            if is_month_row(cells):
                continue

            # Header row (repeated per section/table)
            if is_header_row(cells):
                header_rows_seen += 1
                header_cells = cells
                idx = {
                    "vessel": find_col(cells, "vessel"),
                    "berth": find_col(cells, "berth"),
                    "arrival": find_col(cells, "arrival"),
                    "departure": find_col(cells, "departure"),
                    "pax": find_col(cells, "pax"),
                    "imo": find_col(cells, "imo"),
                }

                # Debug: show headers we found and the mapping
                print(f"[DEBUG] Table {t_i} header row {r_i}: {cells}")
                print(f"[DEBUG] Column index mapping: {idx}")

                continue

            # Can't parse without header mapping
            if not idx or idx["vessel"] is None or idx["berth"] is None or idx["arrival"] is None or idx["departure"] is None:
                continue

            # Guard: row may be shorter than header
            required_max = max(idx["vessel"], idx["berth"], idx["arrival"], idx["departure"])
            if len(cells) <= required_max:
                continue

            berth = cells[idx["berth"]]
            if berth != COBH_BERTH:
                continue

            vessel = cells[idx["vessel"]]
            arrival = cells[idx["arrival"]]
            departure = cells[idx["departure"]]

            pax = ""
            if idx.get("pax") is not None and idx["pax"] < len(cells):
                pax = cells[idx["pax"]]

            imo_raw = ""
            if idx.get("imo") is not None and idx["imo"] < len(cells):
                imo_raw = cells[idx["imo"]]

            # Debug IMO presence
            if extract_digits(imo_raw):
                imo_found_count += 1
            else:
                imo_blank_count += 1

            if not vessel or not arrival or not departure:
                continue

            # Parse dd/mm/yyyy times
            try:
                start = TZ.localize(parse(arrival, dayfirst=True))
                end = TZ.localize(parse(departure, dayfirst=True))
            except Exception as e:
                print(f"[WARN] Failed to parse datetimes for vessel={vessel} arrival={arrival} departure={departure}: {e}")
                continue

            p_int = pax_int(pax)
            signal = pax_signal(p_int)
            title_pax = pax if pax else ("?" if p_int is None else str(p_int))

            summary = f"{signal} {vessel} — {title_pax} pax"
            mt = marinetraffic_link(imo_raw)

            # Notes exactly as requested
            notes_lines = [
                f"Pax: {pax}".rstrip(),
                f"Vessel: {vessel}",
                f"MarineTraffic: {mt}".rstrip(),
                "Created by The Arch, Cobh",
                "Data from PortofCork.ie",
            ]
            description = "\n".join(notes_lines)

            ev = Event()
            ev.add("uid", stable_uid(vessel, imo_raw, start, berth))
            ev.add("dtstamp", datetime.utcnow())
            ev.add("summary", summary)
            ev.add("dtstart", start)
            ev.add("dtend", end)
            ev.add("location", berth)
            ev.add("description", description)

            cal.add_component(ev)
            events_written += 1

    if events_written == 0:
        raise RuntimeError(
            "No events were written. This likely means the berth filter didn't match "
            "or the page layout changed."
        )

    # Summary debug
    print(f"[DEBUG] Total tables found: {len(tables)}")
    print(f"[DEBUG] Total rows seen: {total_rows_seen}")
    print(f"[DEBUG] Header rows seen: {header_rows_seen}")
    print(f"[DEBUG] Events written: {events_written}")
    print(f"[DEBUG] Events with IMO extracted: {imo_found_count}")
    print(f"[DEBUG] Events with IMO blank: {imo_blank_count}")

    with open(OUTPUT_ICS, "wb") as f:
        f.write(cal.to_ical())

    print("Wrote", OUTPUT_ICS)


if __name__ == "__main__":
    main()
