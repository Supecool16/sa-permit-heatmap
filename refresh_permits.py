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
    if not s:
        return ""
    return s[:10]


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

    # lat, lng
    if -90 <= a <= 90 and -180 <= b <= 180:
        return b, a

    # lng, lat
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


def joined_text(row):
    fields = [
        row.get("PERMIT TYPE"),
        row.get("WORK TYPE"),
        row.get("PROJECT NAME"),
        row.get("ADDRESS"),
        row.get("DESCRIPTION"),
        row.get("SCOPE OF WORK"),
        row.get("PERMIT DESCRIPTION"),
        row.get("PROJECT DESCRIPTION"),
    ]
    return " ".join(clean_text(v).lower() for v in fields if clean_text(v))


def is_building_permit(row):
    text = joined_text(row)
    return "building" in text


def is_new_build_permit(row):
    """
    Keep only true new construction / new building permits.
    Exclude remodels, repairs, additions, tenant finish-outs, etc.
    """
    text = joined_text(row)

    include_terms = [
        "new construction",
        "new building",
        "new commercial",
        "new residential",
        "construction of",
        "construct new",
        "ground up",
        "ground-up",
        "shell building",
        "new shell",
        "new single family",
        "new single-family",
        "new residence",
        "new home",
        "new duplex",
        "new triplex",
        "new townhome",
        "new apartment",
        "new multifamily",
        "new multi-family",
    ]

    exclude_terms = [
        "remodel",
        "renovation",
        "repair",
        "replace",
        "re-roof",
        "reroof",
        "roof",
        "addition",
        "alteration",
        "interior finish out",
        "interior finish-out",
        "finish out",
        "finish-out",
        "tenant finish",
        "tenant improvement",
        "t.i.",
        "fit out",
        "fit-out",
        "conversion",
        "demo",
        "demolition",
        "pool",
        "fence",
        "sign",
        "plumbing",
        "mechanical",
        "electrical",
        "foundation repair",
        "fire repair",
        "temporary",
        "solar",
        "carport",
        "garage conversion",
    ]

    if not is_building_permit(row):
        return False

    if any(term in text for term in exclude_terms):
        return False

    if any(term in text for term in include_terms):
        return True

    # fallback heuristics: allow obvious "new" phrasing
    if "new" in text and any(
        term in text for term in [
            "building", "construction", "residence", "home",
            "single family", "single-family", "duplex",
            "apartment", "multifamily", "multi-family", "office",
            "retail", "warehouse", "restaurant", "school", "hotel"
        ]
    ):
        return True

    return False


def classify_permit(row):
    """
    Force every kept NEW BUILD permit into either commercial or residential.
    """
    text = joined_text(row)

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
        "sf residential",
        "mf residential",
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
        "clubhouse",
        "shell building",
        "tenant space",
    ]

    if any(term in text for term in residential_terms):
        return "residential"

    if any(term in text for term in commercial_terms):
        return "commercial"

    # fallback: for new-build permits, default to commercial only if
    # nothing clearly signals residential
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
