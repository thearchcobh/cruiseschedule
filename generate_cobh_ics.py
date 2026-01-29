import re
from datetime import datetime

import pytz
import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse
from icalendar import Calendar, Event

SOURCE_URL = "https://www.portofcork.ie/print-cruise-schedule.php"

OUTPUT_COBH = "cobh-cruise.ics"
OUTPUT_ALL = "all-ports.ics"

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


def normalize_berth_title(berth):
    if berth == "Cobh Cruise Terminal":
        return "Cobh"
    if berth == "Ringaskiddy DWB":
        return "Ringaskiddy"
    return berth


# ---- Spend tier thresholds (€/passenger) ----
TOP_TIER = 9.0        # > 9  => $$$$
TIER_A = 3.0          # >= 3 => $$$
TIER_B = 1.5          # >= 1.5 => $$


def dollars_from_eur_per_pax(x):
    if x is None:
        return "$"
    if x > TOP_TIER:
        return "$$$$"
    if x >= TIER_A:
        return "$$$"
    if x >= TIER_B:
        return "$$"
    return "$"


def norm_key(s):
    return clean(s).lower()


# ---- Spend map (seeded with the vessels we computed from your 2025 data) ----
# Vessel-level is most reliable.
VesselSpendEUR = {
    # Top tier (event-level expedition): > 9 €/pax
    "ng explorer": 14.215541,
    "island sky": 13.818509,
    "world traveller": 9.477050,

    # Luxury baseline examples (>= 3 €/pax)
    "deutschland": 3.683215,
    "azamara quest": 3.387697,
    "azamara journey": 3.342009,
    "seven seas voyager": 3.219788,
    "seven seas grandeur": 3.173613,
    "ilma": 3.147009,

    # Add more vessel entries here as you expand your 2025 mapping
}

# Line-level fallback (coarser, but helps for new vessels in same brand)
LineSpendEUR = {
    "azamara": 3.35,
    "regent seven seas": 3.2,
    "seabourn": 2.8,
    "silversea cruises": 2.6,
    "oceania cruises": 1.9,
    "holland america line": 1.8,
    "princess cruises": 1.6,
    "celebrity cruises": 1.4,
    "norwegian cruise line": 1.2,
    "msc cruises": 1.0,
    "ambassador cruise line": 1.0,
    "saga": 1.1,
    "aida cruises": 1.1,
}


def spend_dollars_for_call(vessel, line):
    v = norm_key(vessel)
    l = norm_key(line)
    eur = VesselSpendEUR.get(v)
    if eur is None and l:
        eur = LineSpendEUR.get(l)
    return dollars_from_eur_per_pax(eur)


def main():
    r = requests.get(SOURCE_URL, timeout=30, headers={"User-Agent": "thearchcobh"})
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.find_all("table")

    cal_cobh = Calendar()
    cal_all = Calendar()

    cal_cobh.add("prodid", "-//The Arch Cobh//Cruise//EN")
    cal_all.add("prodid", "-//The Arch Cobh//Cruise//EN")

    cal_cobh.add("version", "2.0")
    cal_all.add("version", "2.0")

    cal_cobh.add("x-wr-calname", "Cobh Cruise Calls (The Arch)")
    cal_all.add("x-wr-calname", "Cork Harbour Cruise Calls (All Ports)")

    cal_cobh.add("x-wr-timezone", "Europe/Dublin")
    cal_all.add("x-wr-timezone", "Europe/Dublin")

    cobh_count = 0
    all_count = 0

    unknown_vessels = set()

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

            berth_raw = clean(cells[idx["berth"]].get_text())
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

            vessel_line = f"{vessel}, {line}" if line else vessel
            berth_title = normalize_berth_title(berth_raw)

            # -------- ALL PORTS --------
            ev_all = Event()
            ev_all.add("uid", stable_uid(vessel, line, mt, start, berth_raw + "-all"))
            ev_all.add("dtstamp", datetime.utcnow())
            ev_all.add("summary", f"🚢 {vessel} — {berth_title}")
            ev_all.add("dtstart", start)
            ev_all.add("dtend", end)
            ev_all.add("location", berth_title)
            ev_all.add("description", "\n".join([
                f"👥 {pax}".rstrip(),
                f"🛳 {vessel_line}",
                f"⚓ {berth_title}",
                f"🔗 {mt}".rstrip(),
                "",
                "Created by The Arch, Cobh",
                "Data from PortofCork.ie",
            ]))

            cal_all.add_component(ev_all)
            all_count += 1

            # -------- COBH ONLY --------
            if berth_raw == COBH_BERTH:
                p = pax_int(pax)
                signal = pax_signal(p)
                title_pax = pax if pax else ("?" if p is None else str(p))

                spend_tag = spend_dollars_for_call(vessel, line)
                if norm_key(vessel) not in VesselSpendEUR and norm_key(line) not in LineSpendEUR:
                    unknown_vessels.add(f"{vessel} | {line}")

                ev_cobh = Event()
                ev_cobh.add("uid", stable_uid(vessel, line, mt, start, "cobh"))
                ev_cobh.add("dtstamp", datetime.utcnow())
                ev_cobh.add("summary", f"{signal} {spend_tag} {vessel} — {title_pax} pax")
                ev_cobh.add("dtstart", start)
                ev_cobh.add("dtend", end)
                ev_cobh.add("location", "Cobh")
                ev_cobh.add("description", "\n".join([
                    f"👥 {pax}".rstrip(),
                    f"🛳 {vessel_line}",
                    f"🔗 {mt}".rstrip(),
                    "",
                    "Created by The Arch, Cobh",
                    "Data from PortofCork.ie",
                ]))

                cal_cobh.add_component(ev_cobh)
                cobh_count += 1

    print("All ports events:", all_count)
    print("Cobh events:", cobh_count)

    if unknown_vessels:
        print("\nUnknown vessels/lines (consider adding to VesselSpendEUR/LineSpendEUR):")
        for x in sorted(unknown_vessels):
            print(" -", x)

    with open(OUTPUT_ALL, "wb") as f:
        f.write(cal_all.to_ical())

    with open(OUTPUT_COBH, "wb") as f:
        f.write(cal_cobh.to_ical())


if __name__ == "__main__":
    main()
