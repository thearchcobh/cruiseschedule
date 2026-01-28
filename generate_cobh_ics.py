import re
from datetime import datetime

import pytz
import requests
from bs4 import BeautifulSoup
from dateutil import tz
from dateutil.parser import parse as dtparse
from icalendar import Calendar, Event

SOURCE_URL = "https://www.portofcork.ie/print-cruise-schedule.php"
OUTPUT_ICS = "cobh-cruise.ics"

# Filter target (as it appears on the Port of Cork schedule)
COBH_BERTH_EXACT = "Cobh Cruise Terminal"
TZID = "Europe/Dublin"


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def to_localized_dt(value: str) -> datetime:
    """
    Port of Cork uses a day-first datetime format (e.g. '13/04/2026 09:00').
    Parse and localize to Europe/Dublin.
    """
    naive = dtparse(value, dayfirst=True)
    local_tz = pytz.timezone(TZID)
    return local_tz.localize(naive)


def build_uid(imo: str, vessel: str, arrival_dt: datetime) -> str:
    """
    Stable UID so Google updates events instead of duplicating them.
    Prefer IMO if available; otherwise use a cleaned vessel name.
    """
    imo = (imo or "").strip()
    if imo.isdigit():
        base = imo
    else:
        base = re.sub(r"\W+", "", (vessel or "").lower())[:32] or "unknownvessel"
    return f"{base}-{arrival_dt.strftime('%Y%m%dT%H%M')}-cobh@portofcork"


def looks_like_month_label(cells: list[str]) -> bool:
    """
    Month label rows often look like: ['April 2026'] (single cell, contains a year)
    """
    if len(cells) != 1:
        return False
    return bool(re.search(r"\b20\d{2}\b", cells[0]))


def looks_like_header_row(cells: list[str]) -> bool:
    """
    True if this row looks like the column header row.
    We look for key terms rather than relying on position.
    """
    joined = " ".join(c.lower() for c in cells)
    return ("vessel" in joined) and ("berth" in joined) and ("arrival" in joined)


def make_col_index(header_cells: list[str]) -> dict[str, int]:
    """
    Map normalized header text -> column index
    """
    out: dict[str, int] = {}
    for idx, name in enumerate(header_cells):
        key = (name or "").strip().lower()
        if key:
            out[key] = idx
    return out


def idx_of(col_index: dict[str, int], *names: str) -> int | None:
    for n in names:
        key = n.strip().lower()
        if key in col_index:
            return col_index[key]
    return None


def main() -> None:
    resp = requests.get(
        SOURCE_URL,
        timeout=30,
        headers={"User-Agent": "cobh-cruise-ical/1.0"},
    )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        raise RuntimeError("Could not find schedule table on the page.")

    rows = table.find_all("tr")
    if not rows or len(rows) < 2:
        raise RuntimeError("Schedule table seems empty.")

    # --- Find the first REAL header row (skip month labels like "April 2026") ---
    header_cells: list[str] | None = None
    col_index: dict[str, int] | None = None
    header_row_i: int | None = None

    for i, r in enumerate(rows):
        cells = [clean_text(c.get_text()) for c in r.find_all(["th", "td"])]
        if not cells:
            continue
        if looks_like_month_label(cells):
            continue
        if looks_like_header_row(cells):
            header_cells = cells
            col_index = make_col_index(header_cells)
            header_row_i = i
            break

    if not header_cells or col_index is None or header_row_i is None:
        first_rows = [
            [clean_text(c.get_text()) for c in r.find_all(["th", "td"])]
            for r in rows[:8]
        ]
        raise RuntimeError(f"Could not locate header row. First rows seen: {first_rows}")

    # Resolve columns robustly
    i_vessel = idx_of(col_index, "vessel")
    i_berth = idx_of(col_index, "berth")
    i_arrival = idx_of(col_index, "arrival")
    i_departure = idx_of(col_index, "departure")
    i_line = idx_of(col_index, "line")
    i_pax = idx_of(col_index, "pax")
    i_agent = idx_of(col_index, "agent")
    i_imo = idx_of(col_index, "imo")

    needed = {
        "vessel": i_vessel,
        "berth": i_berth,
        "arrival": i_arrival,
        "departure": i_departure,
        "pax": i_pax,
        "imo": i_imo,
    }
    missing = [k for k, v in needed.items() if v is None]
    if missing:
        raise RuntimeError(
            f"Missing expected columns on schedule table: {missing}. "
            f"Headers seen: {header_cells}"
        )

    cal = Calendar()
    cal.add("prodid", "-//Cobh Cruise Schedule//Port of Cork Scrape//EN")
    cal.add("version", "2.0")
    cal.add("x-wr-calname", "Cobh Cruise Calls (Port of Cork)")
    cal.add("x-wr-timezone", TZID)

    now_utc = datetime.now(tz=tz.UTC)

    # --- Data rows (start after the header row we found) ---
    for r in rows[header_row_i + 1 :]:
        cells = [clean_text(c.get_text()) for c in r.find_all(["td", "th"])]
        if not cells:
            continue

        # Skip month labels and repeated headers inside the table
        if looks_like_month_label(cells):
            continue
        if looks_like_header_row(cells):
            continue

        # Ignore malformed rows (some rows may be separators)
        if len(cells) < len(header_cells):
            continue

        vessel = cells[i_vessel] if i_vessel is not None else ""
