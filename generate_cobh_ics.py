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


def clean(s):
    return re.sub(r"\s+", " ", (s or "").strip())


def is_month_row(cells):
    # e.g. ["April 2026"]
    return len(cells) == 1 and re.search(r"\b20\d{2}\b", cells[0])


def is_header_row(cells):
    text = " ".join(c.lower() for c in cells)
    return ("vessel" in text) and ("berth" in text) and ("arrival" in text) and ("departure" in text)


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:40] or "x"


def pax_int(pax_str):
    if not pax_str:
        return None
    # handle "4,200" etc.
    digits = re.sub(r"[^\d]", "", pax_str)
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def pax_signal(pax_value):
    # Adjust thresholds any time you like
    if pax_value is None:
        return "⚪"
    if pax_value >= 3000:
        return "🔴"
    if pax_value >= 1000:
        return "🟠"
    return "🟢"


def marinetraffic_link(imo):
    imo = (imo or "").strip()
    if not imo or not imo.isdigit():
        return ""
    # Working format you confirmed
    return f"https://www.marinetraffic.com/en/ais/details/ships/imo:{imo}/"


def stable_uid(vessel, imo, start_dt, berth):
    """
    Stable UID within a given arrival date so time changes update cleanly.
    If the date changes, UID changes (old event may be removed by the client on refresh).
    """
    day = start_dt.strftime("%Y%m%d")
    base = (imo.strip() if (imo or "").strip().isdigit() else slug(vessel))
    return f"{base}-{day}-{slug(berth)}-thearchcobh"


def main():
    html = requests.get(
        SOURCE_URL,
        timeout=30,
        headers={"User-Agent": "thearchcobh-cruise-ical/1.0"},
    ).text

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError("No tables found on schedule page")

    cal = Calendar()
    cal.add("prodid", "-//The Arch Cobh//Cruise Schedule//EN")
    cal.add("version", "2.0")
    cal.add("x-wr-calname", "Cobh Cruise Calls (The Arch)")
    cal.add("x-wr-timezone", "Europe/Dublin")

    events_written = 0

    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue

        idx = None

        for row in rows:
            cells = [clean(c.get_text()) for c in row.find_all(["th", "td"])]
            if not cells or is_month_row(cells):
                continue

            if is_header_row(cells):
                idx = {name.lower(): i for i, name in enumerate(cells) if name}
                continue

            if not idx:
                continue

            # Required columns
            for key in ("vessel", "berth", "arrival", "departure"):
                if key not in idx:
                    idx = None
                    break
            if not idx:
                continue

            # Ensure row has enough columns
            if len(cells) <= max(idx["vessel"], idx["berth"], idx["arrival"], idx["departure"]):
                continue

            berth = cells[idx["berth"]]
            if berth != COBH_BERTH:
                continue

            vessel = cells[idx["vessel"]]
            arrival = cells[idx["arrival"]]
            departure = cells[idx["departure"]]

            pax = ""
            if "pax" in idx and idx["pax"] < len(cells):
                pax = cells[idx["pax"]]

            imo = ""
            if "imo" in idx and idx["imo"] < len(cells):
                imo = cells[idx["imo"]]

            if not arrival or not departure or not vessel:
                continue

            # Parse dd/mm/yyyy times
            try:
                start = TZ.localize(parse(arrival, dayfirst=True))
                end = TZ.localize(parse(departure, dayfirst=True))
            except Exception:
                continue

            p_int = pax_int(pax)
            signal = pax_signal(p_int)

            # Title with passenger load signal
            # Keep the pax string as-is for readability (e.g. "4,200")
            title_pax = pax if pax else ("?" if p_int is None else str(p_int))
            summary = f"{signal} {vessel} — {title_pax} pax"

            mt = marinetraffic_link(imo)

            # Notes exactly as requested
            notes_lines = [
                f"Pax: {pax if pax else ''}".rstrip(),
                f"Vessel: {vessel}",
                f"MarineTraffic: {mt}" if mt else "MarineTraffic: ",
                "Created by The Arch, Cobh",
                "Data from PortofCork.ie",
            ]
            description = "\n".join(notes_lines)

            ev = Event()
            ev.add("uid", stable_uid(vessel, imo, start, berth))
            ev.add("dtstamp", datetime.utcnow())
            ev.add("summary", summary)
            ev.add("dtstart", start)
            ev.add("dtend", end)
            ev.add("location", berth)
            ev.add("description", description)

            cal.add_component(ev)
            events_written += 1

    with open(OUTPUT_ICS, "wb") as f:
        f.write(cal.to_ical())

    print("Wrote", OUTPUT_ICS)
    print("Events written:", events_written)


if __name__ == "__main__":
    main()
