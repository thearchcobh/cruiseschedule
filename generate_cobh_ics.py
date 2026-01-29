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


def is_month_row(cells_text):
    return len(cells_text) == 1 and re.search(r"\b20\d{2}\b", cells_text[0])


def is_header_row(cells_text):
    text = " ".join(c.lower() for c in cells_text)
    return ("vessel" in text) and ("berth" in text) and ("arrival" in text) and ("departure" in text)


def find_col(header_cells_text, needle):
    needle = needle.lower()
    for i, h in enumerate(header_cells_text):
        if needle in (h or "").lower():
            return i
    return None


def pax_int(pax_str):
    if not pax_str:
        return None
    digits = re.sub(r"[^\d]", "", pax_str)
    if not digits:
        return None
    return int(digits)


def pax_signal(pax_value):
    if pax_value is None:
        return "⚪"
    if pax_value >= 3000:
        return "🔴"
    if pax_value >= 1000:
        return "🟠"
    return "🟢"


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:40] or "x"


def extract_digits(raw):
    return re.sub(r"[^\d]", "", raw or "")


def stable_uid(vessel, line, mt_url, start_dt, berth):
    day = start_dt.strftime("%Y%m%d")
    imo_digits = extract_digits(mt_url)
    base = imo_digits or slug(vessel)
    extra = slug(line) if line else "x"
    return f"{base}-{day}-{slug(berth)}-{extra}-thearchcobh"


def normalize_mt_url(url):
    if not url:
        return ""
    u = url.strip()
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return "https://www.marinetraffic.com" + u
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return "https://" + u


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
        raise RuntimeError("No tables found on schedule page.")

    cal = Calendar()
    cal.add("prodid", "-//The Arch, Cobh//Cruise Schedule//EN")
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
            cells = row.find_all(["th", "td"])
            if not cells:
                continue

            cells_text = [clean(c.get_text()) for c in cells]

            if is_month_row(cells_text):
                continue

            if is_header_row(cells_text):
                idx = {
                    "vessel": find_col(cells_text, "vessel"),
                    "berth": find_col(cells_text, "berth"),
                    "arrival": find_col(cells_text, "arrival"),
                    "departure": find_col(cells_text, "departure"),
                    "pax": find_col(cells_text, "pax"),
                    "line": find_col(cells_text, "line"),
                    "imo": find_col(cells_text, "imo"),
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
            if idx.get("pax") is not None and idx["pax"] < len(cells):
                pax = clean(cells[idx["pax"]].get_text())

            line = ""
            if idx.get("line") is not None and idx["line"] < len(cells):
                line = clean(cells[idx["line"]].get_text())

            mt_url = ""
            if idx.get("imo") is not None and idx["imo"] < len(cells):
                a = cells[idx["imo"]].find("a", href=True)
                if a and a.get("href"):
                    mt_url = normalize_mt_url(a.get("href"))

            if not vessel or not arrival or not departure:
                continue

            try:
                start = TZ.localize(parse(arrival, dayfirst=True))
                end = TZ.localize(parse(departure, dayfirst=True))
            except Exception:
                continue

            p_int = pax_int(pax)
            signal = pax_signal(p_int)
            title_pax = pax if pax else ("?" if p_int is None else str(p_int))

            summary = f"{signal} {vessel} — {title_pax} pax"

            vessel_line = vessel
            if line:
                vessel_line = f"{vessel}, {line}"

            notes_lines = [
                f"👥 {pax}".rstrip(),
                f"🛳 {vessel_line}",
                f"🔗 {mt_url}".rstrip(),
                "",
                "Created by The Arch, Cobh",
                "Data from PortofCork.ie",
            ]
            description = "\n".join(notes_lines)

            ev = Event()
            ev.add("uid", stable_uid(vessel, line, mt_url, start, berth))
            ev.add("dtstamp", datetime.utcnow())
            ev.add("summary", summary)
            ev.add("dtstart", start)
            ev.add("dtend", end)
            ev.add("location", berth)
            ev.add("description", description)

            cal.add_component(ev)
            events_written += 1

    if events_written == 0:
        raise RuntimeError("No events written — check page layout.")

    print("Events written:", events_written)

    with open(OUTPUT_ICS, "wb") as f:
        f.write(cal.to_ical())


if __name__ == "__main__":
    main()
