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
    return "vessel" in text and "berth" in text and "arrival" in text


def main():
    html = requests.get(SOURCE_URL, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    rows = table.find_all("tr")

    header = None
    header_index = None

    for i, row in enumerate(rows):
        cells = [clean(c.get_text()) for c in row.find_all(["th", "td"])]
        if not cells or is_month_row(cells):
            continue
        if is_header_row(cells):
            header = cells
            header_index = i
            break

    if not header:
        raise RuntimeError("Header row not found")

    idx = {name.lower(): i for i, name in enumerate(header)}

    cal = Calendar()
    cal.add("prodid", "-//Cobh Cruise Schedule//EN")
    cal.add("version", "2.0")

    for row in rows[header_index + 1:]:
        cells = [clean(c.get_text()) for c in row.find_all(["td", "th"])]
        if not cells or is_month_row(cells) or is_header_row(cells):
            continue
        if len(cells) < len(header):
            continue

        berth = cells[idx["berth"]]
        if berth != COBH_BERTH:
            continue

        vessel = cells[idx["vessel"]]
        arrival = cells[idx["arrival"]]
        departure = cells[idx["departure"]]
        pax = cells[idx.get("pax", "")]

        start = TZ.localize(parse(arrival, dayfirst=True))
        end = TZ.localize(parse(departure, dayfirst=True))

        event = Event()
        event.add("uid", f"{vessel}-{start}")
        event.add("summary", f"{vessel} ({pax} pax)")
        event.add("dtstart", start)
        event.add("dtend", end)
        event.add("location", berth)

        cal.add_component(event)

    with open(OUTPUT_ICS, "wb") as f:
        f.write(cal.to_ical())


if __name__ == "__main__":
    main()
