import csv
import json
import re
import requests
from datetime import datetime
from collections import defaultdict, Counter

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
    return s[:10] if s else ""


def year_from_date(value):
    d = normalize_date(value)
    return d[:4] if len(d) >= 4 else ""


def parse_location(location):
    """
    Accepts forms like:
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


def text_parts(row):
    permit_type = clean_text(row.get("PERMIT TYPE")).lower()
    work_type = clean_text(row.get("WORK TYPE")).lower()
    project_name = clean_text(row.get("PROJECT NAME")).lower()
    address = clean_text(row.get("ADDRESS")).lower()
    return permit_type, work_type, project_name, address


def joined_text(row):
    return " | ".join(text_parts(row))


def matches_any(text, patterns):
    return any(re.search(p, text) for p in patterns)


# Hard excludes: these are not new principal structures
EXCLUDE_PATTERNS = [
    r"\bremodel\b",
    r"\brenovat",
    r"\brepair\b",
    r"\balter",
    r"\baddition\b",
    r"\bfinish[\s-]?out\b",
    r"\btenant\b",
    r"\binterior\b",
    r"\bconvert",
    r"\bdemo\b",
    r"\bdemolition\b",
    r"\broof\b",
    r"\bre-roof\b",
    r"\breroof\b",
    r"\bfoundation repair\b",
    r"\bfence\b",
    r"\bpool\b",
    r"\bspa\b",
    r"\bdeck\b",
    r"\bpatio\b",
    r"\bporch\b",
    r"\bcarport\b",
    r"\bgarage\b",
    r"\bsolar\b",
    r"\bsign\b",
    r"\bmechanical\b",
    r"\belectrical\b",
    r"\bplumbing\b",
    r"\bfire alarm\b",
    r"\bfire sprinkler\b",
    r"\birrigation\b",
    r"\bsewer\b",
    r"\bwater heater\b",
    r"\bwindow\b",
    r"\bdoor\b",
    r"\bsiding\b",
    r"\btraffic\b",
    r"\bgarage sale\b",
]

# Strong positive signals for actual new construction
NEW_BUILD_PATTERNS = [
    r"\bnew\b",
    r"\bnew construction\b",
    r"\bnew building\b",
    r"\bnew commercial\b",
    r"\bnew residential\b",
    r"\bconstruct(?:ion)?\b",
    r"\bground[\s-]?up\b",
    r"\bshell\b",
    r"\bcore and shell\b",
    r"\bnew single[\s-]?family\b",
    r"\bnew residence\b",
    r"\bnew home\b",
    r"\bnew duplex\b",
    r"\bnew triplex\b",
    r"\bnew townhome\b",
    r"\bnew apartment\b",
    r"\bnew multifamily\b",
    r"\bnew multi[\s-]?family\b",
]

# Broad principal-structure signals for older rows / inconsistent labels
STRUCTURE_PATTERNS = [
    r"\bsingle[\s-]?family\b",
    r"\bresidential\b",
    r"\bduplex\b",
    r"\btriplex\b",
    r"\bquadplex\b",
    r"\btownhome\b",
    r"\bcondo\b",
    r"\bcondominium\b",
    r"\bapartment\b",
    r"\bmultifamily\b",
    r"\bmulti[\s-]?family\b",
    r"\bcommercial\b",
    r"\boffice\b",
    r"\bretail\b",
    r"\bwarehouse\b",
    r"\brestaurant\b",
    r"\bmedical\b",
    r"\bclinic\b",
    r"\bschool\b",
    r"\bhotel\b",
    r"\bmotel\b",
    r"\bindustrial\b",
    r"\bchurch\b",
    r"\bbank\b",
    r"\bhospital\b",
    r"\bstorage\b",
]

RESIDENTIAL_PATTERNS = [
    r"\bresidential\b",
    r"\bsingle[\s-]?family\b",
    r"\bresidence\b",
    r"\bhome\b",
    r"\bhouse\b",
    r"\bduplex\b",
    r"\btriplex\b",
    r"\bquadplex\b",
    r"\btownhome\b",
    r"\bcondo\b",
    r"\bcondominium\b",
    r"\bapartment\b",
    r"\bmultifamily\b",
    r"\bmulti[\s-]?family\b",
]

COMMERCIAL_PATTERNS = [
    r"\bcommercial\b",
    r"\boffice\b",
    r"\bretail\b",
    r"\bwarehouse\b",
    r"\brestaurant\b",
    r"\bmedical\b",
    r"\bclinic\b",
    r"\bschool\b",
    r"\bhotel\b",
    r"\bmotel\b",
    r"\bindustrial\b",
    r"\bchurch\b",
    r"\bbank\b",
    r"\bhospital\b",
    r"\bstorage\b",
    r"\bshell\b",
]


def classify_permit(row):
    text = joined_text(row)

    if matches_any(text, RESIDENTIAL_PATTERNS):
        return "residential"

    if matches_any(text, COMMERCIAL_PATTERNS):
        return "commercial"

    # fallback:
    # if it doesn't look residential, treat as commercial
    return "commercial"


def is_new_build_permit(row):
    permit_type, work_type, project_name, _ = text_parts(row)
    text = " | ".join([permit_type, work_type, project_name])

    if not text.strip():
        return False

    # Never keep obvious non-new or trade-only permits
    if matches_any(text, EXCLUDE_PATTERNS):
        return False

    valuation = to_float(row.get("DECLARED VALUATION")) or 0
    area = to_float(row.get("AREA (SF)")) or 0

    # Strong direct signal: keep immediately
    if matches_any(text, NEW_BUILD_PATTERNS):
        return True

    # Older / inconsistent rows:
    # keep records that look like a principal structure and have meaningful size/value
    if matches_any(text, STRUCTURE_PATTERNS):
        if valuation >= 40000 or area >= 400:
            return True

    # Extra fallback for sparse labels:
    # if work type is blank or generic, but permit/project still looks like a principal structure
    if (
        (not work_type or work_type in {"building", "commercial", "residential"})
        and matches_any(f"{permit_type} | {project_name}", STRUCTURE_PATTERNS)
        and (valuation >= 40000 or area >= 400)
    ):
        return True

    return False


response = requests.get(SOURCE_URL, timeout=180)
response.raise_for_status()

reader = csv.DictReader(response.text.splitlines())

points = []
seen = set()

category_counts = {"commercial": 0, "residential": 0}
year_counts = defaultdict(int)
month_counts = defaultdict(lambda: {"commercial": 0, "residential": 0, "all": 0})

# Diagnostics
kept_work_types = Counter()
kept_permit_types = Counter()
kept_by_year_work = defaultdict(Counter)
kept_by_year_permit = defaultdict(Counter)

for row in reader:
    date_issued = normalize_date(row.get("DATE ISSUED"))
    if not date_issued:
        continue

    year = date_issued[:4]
    if not year or year < "2020":
        continue

    if not is_new_build_permit(row):
        continue

    lng, lat = get_lng_lat(row)
    if lng is None or lat is None:
        continue

    permit_number = clean_text(row.get("PERMIT #"))
    dedupe_key = (permit_number, date_issued)
    if dedupe_key in seen:
        continue
    seen.add(dedupe_key)

    category = classify_permit(row)
    valuation = to_float(row.get("DECLARED VALUATION")) or 0
    month = date_issued[:7]

    permit_type_raw = clean_text(row.get("PERMIT TYPE"))
    work_type_raw = clean_text(row.get("WORK TYPE"))

    category_counts[category] += 1
    year_counts[year] += 1
    month_counts[month][category] += 1
    month_counts[month]["all"] += 1

    kept_work_types[work_type_raw] += 1
    kept_permit_types[permit_type_raw] += 1
    kept_by_year_work[year][work_type_raw] += 1
    kept_by_year_permit[year][permit_type_raw] += 1

    points.append({
        "lng": lng,
        "lat": lat,
        "category": category,
        "permit_type": permit_type_raw,
        "permit_number": permit_number,
        "project_name": clean_text(row.get("PROJECT NAME")),
        "work_type": work_type_raw,
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

print("\nTop kept WORK TYPE values:")
for k, v in kept_work_types.most_common(30):
    print(f"{v:>6} | {k}")

print("\nTop kept PERMIT TYPE values:")
for k, v in kept_permit_types.most_common(30):
    print(f"{v:>6} | {k}")

for yr in sorted(kept_by_year_work.keys()):
    print(f"\nTop WORK TYPE values kept in {yr}:")
    for k, v in kept_by_year_work[yr].most_common(15):
        print(f"{v:>6} | {k}")

for yr in sorted(kept_by_year_permit.keys()):
    print(f"\nTop PERMIT TYPE values kept in {yr}:")
    for k, v in kept_by_year_permit[yr].most_common(15):
        print(f"{v:>6} | {k}")
