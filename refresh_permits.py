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
    # Common CKAN geo format: "(29.4241, -98.4936)"
    loc = clean_text(location)
    if not loc:
        return None, None

    loc = loc.replace("POINT", "").replace("(", "").replace(")", "").strip()
    parts = [p.strip() for p in loc.split(",")]

    if len(parts) == 2:
        a = to_float(parts[0])
        b = to_float(parts[1])

        # Try lat,lng
        if a is not None and b is not None:
            if -90 <= a <= 90 and -180 <= b <= 180:
                return b, a
            if -90 <= b <= 90 and -180 <= a <= 180:
                return a, b

    # Also try space-separated POINT style
    parts = loc.split()
    if len(parts) == 2:
        a = to_float(parts[0])
        b = to_float(parts[1])
        if a is not None and b is not None:
            if -180 <= a <= 180 and -90 <= b <= 90:
                return a, b
            if -180 <= b <= 180 and -90 <= a <= 90:
                return b, a

    return None, None

def classify_permit(row):
    permit_type = clean_text(row.get("PERMIT TYPE")).lower()
    work_type = clean_text(row.get("WORK TYPE")).lower()
    project_name = clean_text(row.get("PROJECT NAME")).lower()

    text = f"{permit_type} {work_type} {project_name}"

    single_family_terms = [
        "single family", "single-family", "new residence",
        "new single family", "1-family", "one-family"
    ]
    commercial_terms = [
        "comm new building permit", "commercial", "office", "retail",
        "warehouse", "restaurant", "medical", "school", "hotel",
        "shell building", "apartments"
    ]

    if any(term in text for term in single_family_terms):
        return "single_family"
    if any(term in text for term in commercial_terms):
        return "commercial"
    return None

def get_lng_lat(row):
    # First try LOCATION
    lng, lat = parse_location(row.get("LOCATION"))
    if lng is not None and lat is not None:
        return lng, lat

    # Then try direct lat/lng-style X/Y only if they look valid
    x = to_float(row.get("X_COORD"))
    y = to_float(row.get("Y_COORD"))

    if x is not None and y is not None:
        if -180 <= x <= 180 and -90 <= y <= 90:
            return x, y
        if -180 <= y <= 180 and -90 <= x <= 90:
            return y, x

    return None, None

seen = set()
points = []

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
        date_issued = clean_text(row.get("DATE ISSUED"))
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
            "date_issued": date_issued[:10],
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
