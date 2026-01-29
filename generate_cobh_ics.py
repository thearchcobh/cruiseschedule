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
    return "vessel" in text and "berth" in text and "arrival" in text and "departure" in text


def find_col(headers, needle):
    needle = needle.lower()
    for i, h in enumerate(headers):
        if needle in (h or "").lower():
            return i
    return None


def pax_int(p):
    if not p:
        return None
    digits = re.sub(r"[^\d]", "", p)
    return int(digits) if digits else None


def pax_signal(n):
    if n is None:
        return "⚪"
    if n >= 3000:
        return "🔴"
    if n >= 1000:
        return "🟠"
    return "🟢"


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:40] or "x"


def extract_digits(s):
    return re.sub(r"[^\d]", "", s or "")


def stable_uid(vessel, line, mt_url, start, berth):
    day = start.strftime("%Y%m%d")
    imo = extract_digits(mt_url)
    base = imo if imo else slug(vessel)
    extra = slug(line) if line else "x"
    return f"{base}-{day}-{slug(berth)}-{extra}-thearchcobh"


def normalize_mt(url):
    if not url:
        return ""
    u = url.strip()
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return "https://www.marinetraffic.com" + u
    if u.startswith("http"):
        return u
    return "https://" + u


def main():
    r = requests.get(SOURCE_URL, timeout=30, headers={"User-Agent": "thearchcobh"})
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.find_all("table")

    cal = Calendar()
    cal.add("prodid", "-//The Arch Cobh//Cruise Schedule//EN")
    cal.add("version", "2.0")
    cal.add("x-wr-calname", "Cobh Cruise Calls (The Arch)")
    cal.add("x-wr-timezone", "Europe/Dublin")

    events_written = 0

    for table in tables:
        rows = table.find_all("tr")
        idx = None

        for row in rows:
            cells = row.find_all(["th", "td"])
            if not cells:
                continue

            text = [clean(c.get_text()) for c in cells]

            if is_month_row(text):
                continue

            if is_header_row(text):
                idx = {
                    "vessel": find_col(text, "vessel"),
                    "berth": find_col(text, "berth"),
                    "arrival": find_col(text, "arrival"),
                    "departure": find_col(text, "departure"),
                    "pax": find_col(text, "pax"),
                    "line": find_col(text, "line"),
                    "imo": find_col(text, "imo"),
                }
                continue

            if not idx:
                continue

            required = ["vessel", "berth", "arrival", "departure"]
            if any(idx[k] is None for k in required):
                continue

            if len(cells) <= max(idx[k] for k in required):
                continue

            berth = clean(cells[idx["berth"]].get_text())
            if berth != COBH_BERTH:
                continue

            vessel = clean(cells[idx["vessel"]].get_text())
            arrival = clean(cells[idx["arrival"]].get_text())
            departure = clean(cells[idx["departure"]].get_text())

            pax = ""
            if idx["pax"] is not None and idx["pax"] < len(cells):
                pax = clean(cells[idx["pax"]].get_text())

            line = ""
            if idx["line"] is not None and idx["line"] < len(cells):
                line = clean(cells[idx["line"]].get_text())

            mt = ""
            if idx["imo"] is not None and idx["imo"] < len(cells):
                link = cells[idx["imo"]].find("a", href=True)
                if link:
                    mt = normalize_mt(link.get("href"))

            if not vessel or not arrival or not departure:
                continue

            try:
                start = TZ.localize(parse(arrival, dayfirst=True))
                end = TZ.localize(parse(departure, dayfirst=True))
            except Exception:
                continue

            p = pax_int(pax)
            signal = pax_signal(p)
            title_pax = pax if pax else ("?" if p is None else str(p))
            summary = f"{signal} {vessel} — {title_pax} pax"

            vessel_line = f"{vessel}, {line}" if line else vessel

            notes = [
                f"👥 {pax}".rstrip(),
                f"🛳 {vessel_line}",
                f"🔗 {mt}".rstrip(),
                "",
                "Created by The Arch, Cobh",
                "Data from PortofCork.ie",
            ]

            ev = Event()
            ev.add("uid", stable_uid(vessel, line, mt, start, berth))
            ev.add("dtstamp", datetime.utcnow())
            ev.add("summary", summary)
            ev.add("dtstart", start)
            ev.add("dtend", end)
            ev.add("location", berth)
            ev.add("description", "\n".join(notes))

            cal.add_component(ev)
            events_written += 1

    if events_written == 0:
        raise RuntimeError("No events written")

    print("Events written:", events_written)

    with open(OUTPUT_ICS, "wb") as f:
        f.write(cal.to_ical())


if __name__ == "__main__":
    main()
