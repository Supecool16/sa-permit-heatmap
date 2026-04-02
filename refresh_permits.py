import csv
import json
import requests
from datetime import datetime

SOURCE_URL = "https://data.sanantonio.gov/dataset/05012dcb-ba1b-4ade-b5f3-7403bc7f52eb/resource/c21106f9-3ef5-4f3a-8604-f992b4db7512/download/permits_issued.csv"
OUTPUT_FILE = "san-antonio-permits.json"


def clean_text(value):
    return (value or "").strip()


def to_float(value):
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def normalize_date(value):
    s = clean_text(value)
    if not s:
        return ""
    # Handles values like 2025-03-29 or 2025-03-29 00:00:00
    return s[:10]


def parse_location(location):
    """
    Accepts common forms like:
    - "(29.4241, -98.4936)"
    - "29.4241,-98.4936"
    - "POINT (-98.4936 29.4241)"
    - "-98.4936 29.4241"
    """
    loc = clean_text(location)
    if not loc:
        return None, None

    loc = loc.replace("POINT", "").replace("(", " ").replace(")", " ").replace(",", " ")
    parts = [p for p in loc.split() if p]

    if len(parts) != 2:
        return None, None

    a = to_float(parts[0])
    b = to_float(parts[1])

    if a is None or b is None:
        return None, None

    # lat lng
    if -90 <= a <= 90 and -180 <= b <= 180:
        return b, a

    # lng lat
    if -180 <= a <= 180 and -90 <= b <= 90:
        return a, b

    return None, None


def get_lng_lat(row):
    # Best source first
    lng, lat = parse_location(row.get("LOCATION"))
    if lng is not None and lat is not None:
        return lng, lat

    x = to_float(row.get("X_COORD"))
    y = to_float(row.get("Y_COORD"))

    # Only accept direct lon/lat-looking values
    if x is not None and y is not None:
        if -180 <= x <= 180 and -90 <= y <= 90:
            return x, y
        if -90 <= x <= 90 and -180 <= y <= 180:
            return y, x

    return None, None


def is_building_permit(row):
    permit_type = clean_text(row.get("PERMIT TYPE")).lower()
    return "building" in permit_type


def classify_permit(row):
    """
    Force every kept BUILDING permit into either commercial or residential.
    No third bucket, so 'all' should equal commercial + residential.
    """
    permit_type = clean_text(row.get("PERMIT TYPE")).lower()
    work_type = clean_text(row.get("WORK TYPE")).lower()
    project_name = clean_text(row.get("PROJECT NAME")).lower()
    address = clean_text(row.get("ADDRESS")).lower()

    text = f"{permit_type} {work_type} {project_name} {address}"

    residential_terms = [
        "residential",
        "single family",
        "single-family",
        "residence",
        "house",
        "home",
        "duplex",
        "triplex",
        "quadplex",
        "townhome",
        "town house",
        "condo",
        "condominium",
        "apartment",
        "apartments",
        "multi-family",
        "multifamily",
        "mf",
    ]

    commercial_terms = [
        "commercial",
        "comm",
        "office",
        "retail",
        "warehouse",
        "restaurant",
        "medical",
        "school",
        "hotel",
        "motel",
        "shell",
        "tenant finish",
        "tenant improvement",
        "industrial",
        "church",
        "bank",
        "hospital",
        "storage",
        "clubhouse",
    ]

    if any(term in text for term in residential_terms):
        return "residential"

    if any(term in text for term in commercial_terms):
        return "commercial"

    # Fallback: building permits that are not explicitly residential
    # are treated as commercial so no building rows are orphaned.
    return "commercial"


response = requests.get(SOURCE_URL, timeout=180)
response.raise_for_status()

reader = csv.DictReader(response.text.splitlines())

points = []
seen = set()

category_counts = {"commercial": 0, "residential": 0}
year_counts = {}

for row in reader:
    if not is_building_permit(row):
        continue

    lng, lat = get_lng_lat(row)
    if lng is None or lat is None:
        continue

    permit_number = clean_text(row.get("PERMIT #"))
    date_issued = normalize_date(row.get("DATE ISSUED"))

    # Skip rows without a usable issued date
    if not date_issued:
        continue

    dedupe_key = (permit_number, date_issued)
    if dedupe_key in seen:
        continue
    seen.add(dedupe_key)

    category = classify_permit(row)
    valuation = to_float(row.get("DECLARED VALUATION")) or 0

    year = date_issued[:4]
    year_counts[year] = year_counts.get(year, 0) + 1
    category_counts[category] += 1

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
    "category_counts": category_counts,
    "year_counts": dict(sorted(year_counts.items())),
    "points": points
}

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(payload, f)

print(f"Wrote {len(points)} points to {OUTPUT_FILE}")
print("Category counts:", category_counts)
print("Year counts:", dict(sorted(year_counts.items())))
