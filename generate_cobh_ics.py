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
    return len(cells) == 1 and re.search(r"\b20\d{2}\b", cells[0])


def is_header_row(cells):
    text = " ".join(c.lower() for c in cells)
    return ("vessel" in text) and ("berth" in text) and ("arrival" in text) and ("departure" in text)


def uid_for(vessel, start_dt):
    base = re.sub(r"\W+", "", (vessel or "").lower())[:32] or "unknown"
    return f"{base}-{start_dt.strftime('%Y%m%dT%H%M')}-cobh"


def main():
    html = requests.get(
        SOURCE_URL,
        timeout=30,
        headers={"User-Agent": "cobh-cruise-ical/1.0"},
    ).text

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError("No tables found on schedule page")

    cal = Calendar()
    cal.add("prodid", "-//Cobh Cruise Schedule//EN")
    cal.add("version", "2.0")
    cal.add("x-wr-calname", "Cobh Cruise Calls (Port of Cork)")
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

            # Header row (usually first real row in each table)
            if is_header_row(cells):
                idx = {name.lower(): i for i, name in enumerate(cells) if name}
                continue

            if not idx:
                continue

            # Must have core fields
            needed = ["vessel", "berth", "arrival", "departure"]
            if any(k not in idx for k in needed):
                continue

            if len(cells) <= max(idx[k] for k in needed):
                continue

            berth = cells[idx["berth"]]
            if berth != COBH_BERTH:
                continue

            vessel = cells[idx["vessel"]]
            arrival = cells[idx["arrival"]]
            departure = cells[idx["departure"]]
            pax = cells[idx["pax"]] if ("pax" in idx and idx["pax"] < len(cells)) else ""

            if not arrival or not departure:
                continue

            start = TZ.localize(parse(arrival, dayfirst=True))
            end = TZ.localize(parse(departure, dayfirst=True))

            event = Event()
            event.add("uid", uid_for(vessel, start))
            event.add("dtstamp", datetime.utcnow())
            event.add("summary", f"{vessel} ({pax} pax)")
            event.add("dtstart", start)
            event.add("dtend", end)
            event.add("location", berth)
            event.add("description", f"Source: {SOURCE_URL}")

            cal.add_component(event)
            events_written += 1

    with open(OUTPUT_ICS, "wb") as f:
        f.write(cal.to_ical())

    print("Wrote", OUTPUT_ICS)
    print("Events written:", events_written)


if __name__ == "__main__":
    main()
