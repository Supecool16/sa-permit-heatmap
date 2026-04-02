import fs from "node:fs/promises";

const PERMITS_CSV_URL =
  "https://data.sanantonio.gov/dataset/05012dcb-ba1b-4ade-b5f3-7403bc7f52eb/resource/c21106f9-3ef5-4f3a-8604-f992b4db7512/download/permits_issued.csv";

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    const next = line[i + 1];

    if (ch === '"') {
      if (inQuotes && next === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }

  out.push(cur);
  return out;
}

function parseCsv(text) {
  const lines = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");
  if (!lines.length) return [];

  const header = parseCsvLine(lines[0]).map(h => h.trim());
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const values = parseCsvLine(lines[i]);
    const row = {};
    for (let j = 0; j < header.length; j++) {
      row[header[j]] = values[j] ?? "";
    }
    rows.push(row);
  }

  return rows;
}

function parseNumber(value) {
  if (value == null) return 0;
  const cleaned = String(value).replace(/[^0-9.-]/g, "");
  const num = parseFloat(cleaned);
  return Number.isFinite(num) ? num : 0;
}

function parseDateString(value) {
  if (!value) return null;
  const s = String(value).trim();

  const parsed = new Date(s);
  if (!Number.isNaN(parsed.getTime())) return parsed;

  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (m) {
    const dt = new Date(`${m[3]}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}T00:00:00`);
    if (!Number.isNaN(dt.getTime())) return dt;
  }

  return null;
}

function formatDateForUi(value) {
  const d = parseDateString(value);
  if (!d) return null;
  return d.toISOString().slice(0, 10);
}

function parseCoords(row) {
  const loc = String(row["LOCATION"] || "").trim();
  const pointMatch = loc.match(/POINT\s*\(\s*(-?\d+\.?\d*)\s+(-?\d+\.?\d*)\s*\)/i);
  if (pointMatch) {
    const lng = parseFloat(pointMatch[1]);
    const lat = parseFloat(pointMatch[2]);
    if (Number.isFinite(lat) && Number.isFinite(lng)) {
      return { lat, lng };
    }
  }

  const x = parseFloat(row["X_COORD"]);
  const y = parseFloat(row["Y_COORD"]);
  if (Number.isFinite(x) && Number.isFinite(y)) {
    return { lat: y, lng: x };
  }

  return null;
}

function isNewBuild(row) {
  const permitType = String(row["PERMIT TYPE"] || "").toUpperCase();
  const workType = String(row["WORK TYPE"] || "").toUpperCase();
  const projectName = String(row["PROJECT NAME"] || "").toUpperCase();
  const combined = `${permitType} | ${workType} | ${projectName}`;

  const looksNew =
    combined.includes("NEW") ||
    combined.includes("NEW CONSTRUCTION") ||
    combined.includes("NEW BUILD") ||
    combined.includes("NEW SINGLE FAMILY") ||
    combined.includes("NEW COMMERCIAL") ||
    combined.includes("NEW STRUCTURE");

  const excludeTerms = [
    "GARAGE SALE",
    "MECHANICAL",
    "ELECTRICAL",
    "PLUMBING",
    "ROOF",
    "RE-ROOF",
    "SIGN",
    "FENCE",
    "POOL",
    "DEMOLITION",
    "DEMO",
    "REMODEL",
    "RENOVATION",
    "ADDITION",
    "REPAIR",
    "SOLAR",
    "FOUNDATION REPAIR",
    "INTERIOR FINISH OUT",
    "CERTIFICATE OF OCCUPANCY"
  ];

  const excluded = excludeTerms.some(term => combined.includes(term));
  return looksNew && !excluded;
}

function getCategory(row) {
  const permitType = String(row["PERMIT TYPE"] || "").toUpperCase();
  const workType = String(row["WORK TYPE"] || "").toUpperCase();
  const projectName = String(row["PROJECT NAME"] || "").toUpperCase();
  const combined = `${permitType} | ${workType} | ${projectName}`;

  const residentialTerms = [
    "RESIDENTIAL",
    "SINGLE FAMILY",
    "SINGLE-FAMILY",
    "MULTI-FAMILY",
    "MULTIFAMILY",
    "DUPLEX",
    "TOWNHOME",
    "TOWNHOUSE",
    "APARTMENT",
    "CONDO"
  ];

  if (residentialTerms.some(term => combined.includes(term))) {
    return "residential";
  }

  return "commercial";
}

function normalizePermitRow(row) {
  const coords = parseCoords(row);
  if (!coords) return null;
  if (!isNewBuild(row)) return null;

  const dateIssued = formatDateForUi(row["DATE ISSUED"]);
  if (!dateIssued) return null;

  return {
    lat: coords.lat,
    lng: coords.lng,
    category: getCategory(row),
    date_issued: dateIssued,
    valuation: parseNumber(row["DECLARED VALUATION"]),
    permit_type: row["PERMIT TYPE"] || "",
    work_type: row["WORK TYPE"] || "",
    permit_number: row["PERMIT #"] || "",
    project_name: row["PROJECT NAME"] || "",
    address: row["ADDRESS"] || ""
  };
}

async function main() {
  const res = await fetch(PERMITS_CSV_URL, {
    headers: {
      "user-agent": "permit-refresh-script"
    }
  });

  if (!res.ok) {
    throw new Error(`Failed to fetch CSV: HTTP ${res.status}`);
  }

  const csvText = await res.text();
  const parsed = parseCsv(csvText);

  const points = parsed
    .map(normalizePermitRow)
    .filter(Boolean)
    .sort((a, b) => {
      const dateCompare = a.date_issued.localeCompare(b.date_issued);
      if (dateCompare !== 0) return dateCompare;

      const categoryCompare = a.category.localeCompare(b.category);
      if (categoryCompare !== 0) return categoryCompare;

      return (b.valuation || 0) - (a.valuation || 0);
    });

  const payload = {
    refreshed_at: new Date().toISOString(),
    source: PERMITS_CSV_URL,
    count: points.length,
    points
  };

  await fs.mkdir("data", { recursive: true });
  await fs.writeFile(
    "data/san-antonio-permits.json",
    JSON.stringify(payload, null, 2) + "\n",
    "utf8"
  );

  console.log(`Wrote ${points.length} permits to data/san-antonio-permits.json`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
