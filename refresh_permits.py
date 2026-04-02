import csv
import json
import requests
from datetime import datetime
from collections import defaultdict

SOURCE_URL = "https://data.sanantonio.gov/dataset/05012dcb-ba1b-4ade-b5f3-7403bc7f52eb/resource/c21106f9-3ef5-4f3a-8604-f992b4db7512/download/permits_issued.csv"
OUTPUT_FILE = "san-antonio-new-build-permits.json"


def clean_text(value):
    return (value or "").strip()


def to_float(value):
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def normalize_date(value):
    s = clean_text(value)
    return s[:10] if s else ""


def parse_location(location):
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


def get_text_fields(row):
    permit_type = clean_text(row.get("PERMIT TYPE")).lower()
    work_type = clean_text(row.get("WORK TYPE")).lower()
    project_name = clean_text(row.get("PROJECT NAME")).lower()
    address = clean_text(row.get("ADDRESS")).lower()
    return permit_type, work_type, project_name, address


def is_trade_only_permit(permit_type):
    trade_terms = [
        "mechanical",
        "electrical",
        "plumbing",
        "fire",
        "irrigation",
        "sign",
        "demolition",
        "garage sale"
    ]
    return any(term in permit_type for term in trade_terms)


def is_new_build_permit(row):
    permit_type, work_type, project_name, _ = get_text_fields(row)
    text = f"{permit_type} {work_type} {project_name}"

    if is_trade_only_permit(permit_type):
        return False

    exclude_terms = [
        "remodel",
        "renovation",
        "repair",
        "addition",
        "alteration",
        "finish out",
        "finish-out",
        "tenant finish",
        "tenant improvement",
        "interior finish",
        "conversion",
        "demo",
        "demolition",
        "roof",
        "re-roof",
        "reroof",
        "foundation repair",
        "fence",
        "pool",
        "carport",
        "garage",
        "solar"
    ]
    if any(term in text for term in exclude_terms):
        return False

    # Primary detection: WORK TYPE indicates new construction/new build
    new_work_terms = [
        "new",
        "new construction",
        "new building",
        "new build",
        "construction"
    ]
    if any(term in work_type for term in new_work_terms):
        return True

    # Secondary fallback: project or permit naming convention indicates new build
    new_project_terms = [
        "new single family",
        "new single-family",
        "new residence",
        "new home",
        "new duplex",
        "new townhome",
        "new apartment",
        "new commercial",
        "new office",
        "new retail",
        "new warehouse",
        "ground up",
        "ground-up",
        "shell building"
    ]
    if any(term in text for term in new_project_terms):
        return True

    return False


def classify_permit(row):
    permit_type, work_type, project_name, address = get_text_fields(row)
    text = f"{permit_type} {work_type} {project_name} {address}"

    residential_terms = [
        "residential",
        "single family",
        "single-family",
        "residence",
        "home",
        "house",
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
        "multifamily"
    ]

    commercial_terms = [
        "commercial",
        "office",
        "retail",
        "warehouse",
        "restaurant",
        "medical",
        "school",
        "hotel",
        "motel",
        "industrial",
        "church",
        "bank",
        "hospital",
        "storage",
        "shell"
    ]

    if any(term in text for term in residential_terms):
        return "residential"

    if any(term in text for term in commercial_terms):
        return "commercial"

    # fallback:
    # if permit/work text mentions residential permit family, keep residential,
    # otherwise default to commercial
    if "res" in permit_type or "res" in work_type:
        return "residential"

    return "commercial"


response = requests.get(SOURCE_URL, timeout=180)
response.raise_for_status()

reader = csv.DictReader(response.text.splitlines())

points = []
seen = set()

category_counts = {"commercial": 0, "residential": 0}
year_counts = defaultdict(int)
month_counts = defaultdict(lambda: {"commercial": 0, "residential": 0, "all": 0})

for row in reader:
    if not is_new_build_permit(row):
        continue

    lng, lat = get_lng_lat(row)
    if lng is None or lat is None:
        continue

    permit_number = clean_text(row.get("PERMIT #"))
    date_issued = normalize_date(row.get("DATE ISSUED"))
    if not date_issued:
        continue

    dedupe_key = (permit_number, date_issued)
    if dedupe_key in seen:
        continue
    seen.add(dedupe_key)

    category = classify_permit(row)
    valuation = to_float(row.get("DECLARED VALUATION")) or 0

    year = date_issued[:4]
    month = date_issued[:7]

    category_counts[category] += 1
    year_counts[year] += 1
    month_counts[month][category] += 1
    month_counts[month]["all"] += 1

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
    "scope": "new_builds_only",
    "count": len(points),
    "category_counts": category_counts,
    "year_counts": dict(sorted(year_counts.items())),
    "month_counts": {k: month_counts[k] for k in sorted(month_counts.keys())},
    "points": points
}

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False)

print(f"Wrote {len(points)} points to {OUTPUT_FILE}")
print("Category counts:", category_counts)
print("Year counts:", dict(sorted(year_counts.items())))
print("Month counts:", {k: month_counts[k] for k in sorted(month_counts.keys())})
