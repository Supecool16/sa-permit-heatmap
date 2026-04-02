import csv
import json
import requests
from datetime import datetime

URLS = [
    "https://data.sanantonio.gov/dataset/05012dcb-ba1b-4ade-b5f3-7403bc7f52eb/resource/c22b1ef2-dcf8-4d77-be1a-ee3638092aab/download/permits_issued_ending_12312024.csv",
    "https://data.sanantonio.gov/dataset/05012dcb-ba1b-4ade-b5f3-7403bc7f52eb/resource/c21106f9-3ef5-4f3a-8604-f992b4db7512/download/permits_issued.csv",
]

OUTPUT_FILE = "san-antonio-permits.json"

def clean_text(value):
    return (value or "").strip()

def to_float(value):
    try:
        return float(str(value).replace(",", "").strip())
    except:
        return None

def parse_location(location):
    loc = clean_text(location)
    if not loc:
        return None, None

    loc = loc.replace("POINT", "").replace("(", "").replace(")", "").strip()

    if "," in loc:
        parts = [p.strip() for p in loc.split(",")]
    else:
        parts = loc.split()

    if len(parts) != 2:
        return None, None

    a = to_float(parts[0])
    b = to_float(parts[1])
    if a is None or b is None:
        return None, None

    # lat,lng
    if -90 <= a <= 90 and -180 <= b <= 180:
        return b, a

    # lng,lat
    if -180 <= a <= 180 and -90 <= b <= 90:
        return a, b

    return None, None

def get_lng_lat(row):
    lng, lat = parse_location(row.get("LOCATION"))
    if lng is not None and lat is not None:
        return lng, lat

    x = to_float(row.get("X_COORD"))
    y = to_float(row.get("Y_COORD"))

    if x is not None and y is not None:
        if -180 <= x <= 180 and -90 <= y <= 90:
            return x, y
        if -90 <= x <= 90 and -180 <= y <= 180:
            return y, x

    return None, None

def classify_permit(row):
    permit_type = clean_text(row.get("PERMIT TYPE")).lower()
    work_type = clean_text(row.get("WORK TYPE")).lower()
    project_name = clean_text(row.get("PROJECT NAME")).lower()
    address = clean_text(row.get("ADDRESS")).lower()

    text = f"{permit_type} {work_type} {project_name} {address}"

    # throw out obvious non-building/trade-only noise
    exclude_terms = [
        "garage sale",
        "mechanical",
        "electrical",
        "plumbing",
        "solar photovoltaic",
        "mep"
    ]
    if any(term in text for term in exclude_terms):
        return None

    residential_terms = [
        "single family", "single-family", "residential", "residence",
        "house", "home", "duplex", "triplex", "townhome", "town house",
        "condo", "condominium", "apartment", "multi-family", "multifamily"
    ]

    commercial_terms = [
        "commercial", "comm new building permit", "office", "retail",
        "warehouse", "restaurant", "medical", "school", "hotel",
        "shell building", "tenant finish", "tenant improvement",
        "industrial", "church", "bank", "hospital", "storage"
    ]

    if any(term in text for term in commercial_terms):
        return "commercial"

    if any(term in text for term in residential_terms):
        return "residential"

    # fallback: if it is clearly a building permit, force a category
    if "building permit" in text or "building" in permit_type:
        # light heuristic
        if any(term in text for term in ["unit", "res", "residence", "home", "house"]):
            return "residential"
        return "commercial"

    return None

points = []
seen = set()

for url in URLS:
    r = requests.get(url, timeout=180)
    r.raise_for_status()

    reader = csv.DictReader(r.text.splitlines())

    for row in reader:
        category = classify_permit(row)
        if category is None:
            continue

        lng, lat = get_lng_lat(row)
        if lng is None or lat is None:
            continue

        permit_number = clean_text(row.get("PERMIT #"))
        date_issued = clean_text(row.get("DATE ISSUED"))[:10]
        dedupe_key = (permit_number, date_issued)

        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        valuation = to_float(row.get("DECLARED VALUATION")) or 0

        points.append({
            "lng": lng,
            "lat": lat,
            "category": category,
            "permit_type": clean_text(row.get("PERMIT TYPE")),
            "permit_number": permit_number,
            "project_name": clean_text(row.get("PROJECT NAME")),
            "work_type": clean_text(row.get("WORK TYPE")),
            "address": clean_text(row.get("ADDRESS")),
            "date_issued": date_issued,
            "valuation": valuation
        })

payload = {
    "updated_at": datetime.utcnow().isoformat() + "Z",
    "source": "City of San Antonio Open Data",
    "count": len(points),
    "points": points
}

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(payload, f)

print(f"Wrote {len(points)} points to {OUTPUT_FILE}")
