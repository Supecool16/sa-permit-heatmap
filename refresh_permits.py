import csv
import json
import requests
from datetime import datetime

SOURCE_URL = "https://data.sanantonio.gov/dataset/05012dcb-ba1b-4ade-b5f3-7403bc7f52eb/resource/c21106f9-3ef5-4f3a-8604-f992b4db7512/download/permits_issued.csv"
OUTPUT_FILE = "san-antonio-permits.json"

def is_building_permit(row):
    permit_type = (row.get("PERMIT TYPE") or "").strip().lower()
    exclude_terms = ["garage sale", "mechanical", "electrical", "plumbing"]
    return permit_type and not any(term in permit_type for term in exclude_terms)

def to_float(value):
    try:
        return float(value)
    except:
        return None

r = requests.get(SOURCE_URL, timeout=120)
r.raise_for_status()

lines = r.text.splitlines()
reader = csv.DictReader(lines)

points = []
for row in reader:
    x = to_float(row.get("X_COORD"))
    y = to_float(row.get("Y_COORD"))

    if x is None or y is None:
        continue

    if not is_building_permit(row):
        continue

    points.append({
        "lng": x,
        "lat": y,
        "permit_type": row.get("PERMIT TYPE"),
        "permit_number": row.get("PERMIT #"),
        "project_name": row.get("PROJECT NAME"),
        "work_type": row.get("WORK TYPE"),
        "address": row.get("ADDRESS"),
        "date_issued": row.get("DATE ISSUED"),
        "valuation": row.get("DECLARED VALUATION"),
    })

payload = {
    "updated_at": datetime.utcnow().isoformat() + "Z",
    "source": "City of San Antonio Open Data",
    "count": len(points),
    "points": points,
}

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(payload, f)

print(f"Wrote {len(points)} points to {OUTPUT_FILE}")
