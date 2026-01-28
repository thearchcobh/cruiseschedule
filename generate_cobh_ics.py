import re
from datetime import datetime
from dateutil import tz
from dateutil.parser import parse as dtparse

import pytz
import requests
from bs4 import BeautifulSoup
from icalendar import Calendar, Event

SOURCE_URL = "https://www.portofcork.ie/print-cruise-schedule.php"
OUTPUT_ICS = "cobh-cruise.ics"

# Filter target (as it appears on the Port of Cork schedule)
COBH_BERTH_EXACT = "Cobh Cruise Terminal"
TZID = "Europe/Dublin"


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def to_localized_dt(value: str):
    """
    Port of Cork uses format like: '13/04/2026 09:00'
    We'll parse and localize to Europe/Dublin.
    """
    # dayfirst=True is important for dd/mm/yyyy
    naive = dtparse(value, dayfirst=True)
    local_tz = pytz.timezone(TZID)
    return local_tz.localize(naive)


def build_uid(imo: str, vessel: str, arrival_dt: datetime) -> str:
    # Stable UID so Google updates events instead of duplicating
    base = imo if imo and imo.isdigit() else re.sub(r"\W+", "", vessel.lower())[:32]
    return f"{base}-{arrival_dt.strftime('%Y%m%dT%H%M')}-cobh@portofcork"


def main():
    resp = requests.get(SOURCE_URL, timeout=30, headers={"User-Agent": "cobh-cruise-ical/1.0"})
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        raise RuntimeError("Could not find schedule table on the page.")

rows = table.find_all("tr")
if not rows or len(rows) < 2:
    raise RuntimeError("Schedule table seems empty.")

def looks_like_header(cells):
    joined = " ".join(c.lower() for c in cells)
    return ("vessel" in joined) and ("berth" in joined)

def make_col_index(header_cells):
    return {name.strip().lower(): idx for idx, name in enumerate(header_cells) if name.strip()}

def idx_of(col_index, *names):
    for n in names:
        key = n.strip().lower()
        if key in col_index:
            return col_index[key]
    return None

# --- Find the REAL header row (skip month label rows like "April 2026") ---
header_cells = None
col_index = None
header_row_i = None

for i, r in enumerate(rows):
    cells = [clean_text(c.get_text()) for c in r.find_all(["th", "td"])]
    # Skip empty / single-cell month title rows like "April 2026"
    if not cells or (len(cells) == 1 and re.search(r"\b20\d{2}\b", cells[0])):
        continue
    if looks_like_header(cells):
        header_cells = cells
        col_index = make_col_index(header_cells)
        header_row_i = i
        break

if not col_index:
    raise RuntimeError(f"Could not locate header row. First rows seen: "
                       f"{[[clean_text(c.get_text()) for c in r.find_all(['th','td'])] for r in rows[:5]]}")

# Expected columns on this print page
i_vessel = idx_of(col_index, "VESSEL", "Vessel")
i_berth = idx_of(col_index, "BERTH", "Berth")
i_arrival = idx_of(col_index, "Arrival")
i_departure = idx_of(col_index, "Departure")
i_line = idx_of(col_index, "LINE", "Line")
i_pax = idx_of(col_index, "PAX", "Pax")
i_agent = idx_of(col_index, "AGENT", "Agent")
i_imo = idx_of(col_index, "IMO", "Imo")

needed = {"vessel": i_vessel, "berth": i_berth, "arrival": i_arrival, "departure": i_departure, "pax": i_pax, "imo": i_imo}
missing = [k for k, v in needed.items() if v is None]
if missing:
    raise RuntimeError(f"Missing expected columns on schedule table: {missing}. Headers seen: {header_cells}")


    cal = Calendar()
    cal.add("prodid", "-//Cobh Cruise Schedule//Port of Cork Scrape//EN")
    cal.add("version", "2.0")
    cal.add("x-wr-calname", "Cobh Cruise Calls (Port of Cork)")
    cal.add("x-wr-timezone", TZID)

    now_utc = datetime.now(tz=tz.UTC)

    # Data rows
    for r in rows[header_row_i + 1:]:
        cells = [clean_text(td.get_text()) for td in r.find_all(["td", "th"])]

    if not cells:
        continue

    # Skip month label rows like "April 2026"
    if len(cells) == 1 and re.search(r"\b20\d{2}\b", cells[0]):
        continue

    # Skip repeated header rows inside the table
    if looks_like_header(cells):
        continue

    # Ignore malformed rows
    if len(cells) < len(header_cells):
        continue

    vessel = cells[i_vessel]
    berth = cells[i_berth]
    arrival_raw = cells[i_arrival]
    departure_raw = cells[i_departure]
    line = cells[i_line] if i_line is not None else ""
    pax = cells[i_pax] if i_pax is not None else ""
    agent = cells[i_agent] if i_agent is not None else ""
    imo = cells[i_imo] if i_imo is not None else ""

    if berth != COBH_BERTH_EXACT:
        continue

    if not arrival_raw or not departure_raw:
        continue

    try:
        dt_start = to_localized_dt(arrival_raw)
        dt_end = to_localized_dt(departure_raw)
    except Exception:
        continue

    pax_str = pax if pax else "pax"
    summary = f"{vessel} ({pax_str} pax)"

    ev = Event()
    ev.add("uid", build_uid(imo, vessel, dt_start))
    ev.add("dtstamp", now_utc)
    ev.add("summary", summary)
    ev.add("dtstart", dt_start)
    ev.add("dtend", dt_end)
    ev.add("location", berth)

    desc_lines = []
    if line:
        desc_lines.append(f"Cruise line: {line}")
    if agent:
        desc_lines.append(f"Agent: {agent}")
    if pax:
        desc_lines.append(f"Pax: {pax}")
    if imo:
        desc_lines.append(f"IMO: {imo}")
    desc_lines.append(f"Source: {SOURCE_URL}")

    ev.add("description", "\n".join(desc_lines))
    cal.add_component(ev)

    with open(OUTPUT_ICS, "wb") as f:
        f.write(cal.to_ical())

    print(f"Wrote {OUTPUT_ICS}")


if __name__ == "__main__":
    main()
