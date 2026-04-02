<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.heat/dist/leaflet-heat.js"></script>

<div style="font-family: Arial, sans-serif; max-width: 100%;">
  <div style="margin-bottom: 12px; display: flex; gap: 8px; flex-wrap: wrap; align-items: center;">
    <button id="showSingle" style="padding: 8px 12px; border: 1px solid #ccc; background: white; cursor: pointer;">Single-Family</button>
    <button id="showCommercial" style="padding: 8px 12px; border: 1px solid #ccc; background: white; cursor: pointer;">Commercial</button>
    <button id="showBoth" style="padding: 8px 12px; border: 1px solid #ccc; background: #222; color: white; cursor: pointer;">Both</button>

    <label style="margin-left: 10px; font-size: 14px;">Begin Date</label>
    <input id="beginDate" type="date" style="padding: 8px; border: 1px solid #ccc; border-radius: 6px;" />

    <label style="font-size: 14px;">End Date</label>
    <input id="endDate" type="date" style="padding: 8px; border: 1px solid #ccc; border-radius: 6px;" />

    <button id="applyDates" style="padding: 8px 12px; border: 1px solid #ccc; background: white; cursor: pointer;">Apply Dates</button>
    <button id="clearDates" style="padding: 8px 12px; border: 1px solid #ccc; background: white; cursor: pointer;">Clear Dates</button>
  </div>

  <div id="summary" style="margin-bottom: 10px; font-size: 14px; color: #333;"></div>

  <div id="map" style="height: 650px; width: 100%; border-radius: 12px;"></div>

  <div style="margin-top: 10px; background: rgba(255,255,255,0.92); padding: 10px 12px; border: 1px solid #ddd; border-radius: 10px; display: inline-block;">
    <div style="font-weight: 700; margin-bottom: 6px;">Heat Intensity</div>
    <div style="display: flex; align-items: center; gap: 8px;">
      <span style="font-size: 12px;">Lower</span>
      <div style="width: 180px; height: 14px; border-radius: 8px; background: linear-gradient(to right, rgba(59,130,246,0.35), rgba(34,197,94,0.45), rgba(234,179,8,0.55), rgba(249,115,22,0.65), rgba(220,38,38,0.75)); border: 1px solid #ccc;"></div>
      <span style="font-size: 12px;">Higher</span>
    </div>
    <div style="font-size: 12px; color: #555; margin-top: 6px;">
      Heat is based on permit concentration and valuation weighting.
    </div>
  </div>
</div>

<script>
const map = L.map('map').setView([29.4241, -98.4936], 10);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

let allPoints = [];
let heatLayer = null;
let currentCategory = 'both';

function getWeight(p) {
  const valuation = parseFloat(p.valuation || 0);
  return Math.max(0.15, Math.min(valuation / 1000000, 0.9));
}

function parsePermitDate(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  return isNaN(d.getTime()) ? null : d;
}

function getFilteredPoints() {
  let filtered = allPoints;

  if (currentCategory === 'single_family') {
    filtered = filtered.filter(p => p.category === 'single_family');
  } else if (currentCategory === 'commercial') {
    filtered = filtered.filter(p => p.category === 'commercial');
  }

  const beginValue = document.getElementById('beginDate').value;
  const endValue = document.getElementById('endDate').value;

  const beginDate = beginValue ? new Date(beginValue + 'T00:00:00') : null;
  const endDate = endValue ? new Date(endValue + 'T23:59:59') : null;

  if (beginDate) {
    filtered = filtered.filter(p => {
      const d = parsePermitDate(p.date_issued);
      return d && d >= beginDate;
    });
  }

  if (endDate) {
    filtered = filtered.filter(p => {
      const d = parsePermitDate(p.date_issued);
      return d && d <= endDate;
    });
  }

  return filtered;
}

function updateSummary(filtered) {
  const categoryLabel =
    currentCategory === 'single_family' ? 'Single-Family' :
    currentCategory === 'commercial' ? 'Commercial' : 'Both';

  const beginValue = document.getElementById('beginDate').value;
  const endValue = document.getElementById('endDate').value;

  let text = `<strong>${filtered.length.toLocaleString()}</strong> permits shown | Category: <strong>${categoryLabel}</strong>`;

  if (beginValue || endValue) {
    text += ` | Date Range: <strong>${beginValue || 'Any'}</strong> to <strong>${endValue || 'Any'}</strong>`;
  }

  document.getElementById('summary').innerHTML = text;
}

function drawHeat() {
  if (heatLayer) {
    map.removeLayer(heatLayer);
  }

  const filtered = getFilteredPoints();
  updateSummary(filtered);

  const heatPoints = filtered.map(p => [p.lat, p.lng, getWeight(p)]);

  heatLayer = L.heatLayer(heatPoints, {
    radius: 18,
    blur: 14,
    maxZoom: 15,
    minOpacity: 0.18,
    gradient: {
      0.20: '#3b82f6',
      0.40: '#22c55e',
      0.60: '#eab308',
      0.80: '#f97316',
      1.00: '#dc2626'
    }
  }).addTo(map);
}

fetch("https://supecool16.github.io/sa-permit-heatmap/san-antonio-permits.json")
  .then(res => res.json())
  .then(data => {
    allPoints = data.points.filter(p =>
      typeof p.lat === "number" &&
      typeof p.lng === "number" &&
      p.lat >= -90 && p.lat <= 90 &&
      p.lng >= -180 && p.lng <= 180
    );

    drawHeat();
  })
  .catch(err => console.error("Map failed:", err));

document.getElementById('showSingle').addEventListener('click', () => {
  currentCategory = 'single_family';
  drawHeat();
});

document.getElementById('showCommercial').addEventListener('click', () => {
  currentCategory = 'commercial';
  drawHeat();
});

document.getElementById('showBoth').addEventListener('click', () => {
  currentCategory = 'both';
  drawHeat();
});

document.getElementById('applyDates').addEventListener('click', drawHeat);

document.getElementById('clearDates').addEventListener('click', () => {
  document.getElementById('beginDate').value = '';
  document.getElementById('endDate').value = '';
  drawHeat();
});
</script>
