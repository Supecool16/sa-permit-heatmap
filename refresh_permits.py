import csv
import json
import requests
from datetime import datetime

SOURCE_URL = "https://data.sanantonio.gov/dataset/05012dcb-ba1b-4ade-b5f3-7403bc7f52eb/resource/c21106f9-3ef5-4f3a-8604-f992b4db7512/download/permits_issued.csv"
OUTPUT_FILE = "san-antonio-permits.json"

def to_float(value):
    try:
        return float(value)
    except:
        return None

def clean_text(value):
    return (value or "").strip()

def classify_permit(row):
    permit_type = clean_text(row.get("PERMIT TYPE")).lower()
    work_type = clean_text(row.get("WORK TYPE")).lower()
    project_name = clean_text(row.get("PROJECT NAME")).lower()
    address = clean_text(row.get("ADDRESS")).lower()

    combined = f"{permit_type} {work_type} {project_name} {address}"

    # Single-family patterns
    single_family_terms = [
        "single family",
        "single-family",
        "sf residential",
        "residential new",
        "res new",
        "new residence",
        "new single family",
        "1-family",
        "one-family",
    ]

    # Commercial patterns
    commercial_terms = [
        "comm new building permit",
        "commercial",
        "office",
        "retail",
        "warehouse",
        "shell building",
        "restaurant",
        "medical",
        "school",
        "apartments",
        "hotel",
    ]

    if any(term in combined for term in single_family_terms):
        return "single_family"

    if any(term in combined for term in commercial_terms):
        return "commercial"

    return None

r = requests.get(SOURCE_URL, timeout=120)
r.raise_for_status()

lines = r.text.splitlines()
reader = csv.DictReader(lines)

points = []

for row in reader:
    lng = to_float(row.get("lng") or row.get("LNG") or row.get("LONGITUDE") or row.get("X_COORD"))
    lat = to_float(row.get("lat") or row.get("LAT") or row.get("LATITUDE") or row.get("Y_COORD"))

    # Keep only normal lat/lng values
    if lat is None or lng is None:
        continue

    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        continue

    category = classify_permit(row)
    if category is None:
        continue

    valuation = to_float(row.get("DECLARED VALUATION")) or 0

    points.append({
        "lng": lng,
        "lat": lat,
        "category": category,
        "permit_type": clean_text(row.get("PERMIT TYPE")),
        "permit_number": clean_text(row.get("PERMIT #")),
        "project_name": clean_text(row.get("PROJECT NAME")),
        "work_type": clean_text(row.get("WORK TYPE")),
        "address": clean_text(row.get("ADDRESS")),
        "date_issued": clean_text(row.get("DATE ISSUED")),
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
