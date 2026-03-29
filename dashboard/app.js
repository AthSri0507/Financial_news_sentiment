const state = {
  sectors: [],
  selectedSectors: new Set(),
  sourceFilter: "all",
};

const elements = {
  apiBaseUrl: document.getElementById("apiBaseUrl"),
  company: document.getElementById("company"),
  windowDays: document.getElementById("windowDays"),
  bucket: document.getElementById("bucket"),
  method: document.getElementById("method"),
  sourceFilter: document.getElementById("sourceFilter"),
  runQuery: document.getElementById("runQuery"),
  forceRecompute: document.getElementById("forceRecompute"),
  timelineChart: document.getElementById("timelineChart"),
  heatmapChart: document.getElementById("heatmapChart"),
  feedCards: document.getElementById("feedCards"),
  leadLagTable: document.getElementById("leadLagTable"),
  sectorFilter: document.getElementById("sectorFilter"),
  healthBadge: document.getElementById("healthBadge"),
  lastRun: document.getElementById("lastRun"),
  failedJobs: document.getElementById("failedJobs"),
  latency: document.getElementById("latency"),
};

function defaultApiBase() {
  const qs = new URLSearchParams(window.location.search);
  const fromQuery = qs.get("apiBaseUrl");
  if (fromQuery) {
    return fromQuery;
  }
  return localStorage.getItem("dashboard.apiBaseUrl") || "http://127.0.0.1:8000";
}

function apiUrl(path, params = {}) {
  const base = elements.apiBaseUrl.value.replace(/\/$/, "");
  const url = new URL(`${base}${path}`);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value));
    }
  });
  return url.toString();
}

function renderSectorChips(sectors) {
  elements.sectorFilter.innerHTML = "";

  const allChip = document.createElement("button");
  allChip.className = `chip ${state.selectedSectors.size === 0 ? "active" : ""}`;
  allChip.textContent = "All";
  allChip.onclick = () => {
    state.selectedSectors.clear();
    renderSectorChips(sectors);
  };
  elements.sectorFilter.appendChild(allChip);

  sectors.forEach((sector) => {
    const chip = document.createElement("button");
    chip.className = `chip ${state.selectedSectors.has(sector) ? "active" : ""}`;
    chip.textContent = sector;
    chip.onclick = () => {
      if (state.selectedSectors.has(sector)) {
        state.selectedSectors.delete(sector);
      } else {
        state.selectedSectors.add(sector);
      }
      renderSectorChips(sectors);
      runAnalysis(false);
    };
    elements.sectorFilter.appendChild(chip);
  });
}

function normalizeSourceOptions(items) {
  const existing = new Set(["all"]);
  elements.sourceFilter.innerHTML = "";

  const addOpt = (value, label) => {
    const opt = document.createElement("option");
    opt.value = value;
    opt.textContent = label;
    elements.sourceFilter.appendChild(opt);
  };

  addOpt("all", "All Sources");

  items.forEach((item) => {
    const source = item.source_type || "unknown";
    if (!existing.has(source)) {
      existing.add(source);
      addOpt(source, source);
    }
  });
  elements.sourceFilter.value = state.sourceFilter;
}

function renderTimeline(timeline) {
  const x = timeline.map((p) => p.bucket_start);
  const y = timeline.map((p) => p.weighted_sentiment);
  const confidence = timeline.map((p) => p.confidence_score || 0);

  const traceSentiment = {
    x,
    y,
    mode: "lines+markers",
    name: "Weighted Sentiment",
    line: { color: "#49c6b4", width: 3 },
    marker: {
      size: confidence.map((c) => 8 + c * 9),
      color: confidence,
      colorscale: "YlGnBu",
      cmin: 0,
      cmax: 1,
      showscale: true,
      colorbar: { title: "Confidence" },
    },
  };

  const layout = {
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { l: 40, r: 20, b: 50, t: 20 },
    font: { color: "#eaf6ff" },
    yaxis: { title: "Sentiment", range: [-1, 1] },
    xaxis: { title: "Bucket" },
  };

  Plotly.newPlot(elements.timelineChart, [traceSentiment], layout, { responsive: true });
}

function renderHeatmap(matrix) {
  if (!matrix.length) {
    elements.heatmapChart.innerHTML = "<p>No sector correlation data yet.</p>";
    return;
  }

  const sectors = matrix.map((row) => row.sector);
  const z = matrix.map((row) => row.values);

  Plotly.newPlot(
    elements.heatmapChart,
    [
      {
        z,
        x: sectors,
        y: sectors,
        type: "heatmap",
        colorscale: "RdBu",
        zmin: -1,
        zmax: 1,
      },
    ],
    {
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      margin: { l: 80, r: 20, b: 80, t: 20 },
      font: { color: "#eaf6ff" },
    },
    { responsive: true }
  );
}

function sentimentColor(label) {
  if (label === "positive") {
    return "#95d46b";
  }
  if (label === "negative") {
    return "#ff6b6b";
  }
  return "#ffc857";
}

function renderFeedCards(items) {
  elements.feedCards.innerHTML = "";

  const filtered = items.filter((item) => {
    if (state.sourceFilter === "all") {
      return true;
    }
    return (item.source_type || "unknown") === state.sourceFilter;
  });

  filtered.slice(0, 20).forEach((item) => {
    const card = document.createElement("div");
    card.className = "feed-card";

    const factors = item.impact_factors || {};

    card.innerHTML = `
      <div class="feed-head">
        <strong>${item.title || "Untitled"}</strong>
        <span style="color:${sentimentColor(item.sentiment_label)}">${item.sentiment_label}</span>
      </div>
      <p>${item.summary || "No summary available."}</p>
      <div class="feed-head">
        <a href="${item.url}" target="_blank" rel="noopener noreferrer">Open source</a>
        <span>Impact ${Number(item.impact_score || 0).toFixed(3)}</span>
      </div>
      <div class="badges">
        <span class="badge">R ${(factors.reliability || 0).toFixed(2)}</span>
        <span class="badge">E ${(factors.engagement || 0).toFixed(2)}</span>
        <span class="badge">Q ${(factors.relevance || 0).toFixed(2)}</span>
        <span class="badge">${item.source_type || "unknown"}</span>
      </div>
    `;

    elements.feedCards.appendChild(card);
  });

  if (!filtered.length) {
    elements.feedCards.innerHTML = "<p>No items match current source filter.</p>";
  }
}

function renderLeadLag(rows) {
  elements.leadLagTable.innerHTML = "";

  rows.slice(0, 20).forEach((row) => {
    const tr = document.createElement("tr");
    const leaderText = row.leader === "none" ? "No lead" : `${row.leader} -> ${row.follower}`;
    tr.innerHTML = `
      <td>${row.sector_a} vs ${row.sector_b}</td>
      <td>${row.best_lag}</td>
      <td>${Number(row.correlation || 0).toFixed(3)}</td>
      <td>${leaderText}</td>
    `;
    elements.leadLagTable.appendChild(tr);
  });
}

function filteredSectorInsights(sectorPayload) {
  if (state.selectedSectors.size === 0) {
    return sectorPayload;
  }

  const keep = new Set(state.selectedSectors);
  const sectorSeries = (sectorPayload.sector_series || []).filter((row) => keep.has(row.sector));
  const sectorNames = sectorSeries.map((row) => row.sector);
  const originalNames = (sectorPayload.correlation_matrix || []).map((row) => row.sector);
  const originalIndex = new Map(originalNames.map((name, index) => [name, index]));

  const matrix = [];
  (sectorPayload.correlation_matrix || []).forEach((row) => {
    if (!keep.has(row.sector)) {
      return;
    }

    const values = [];
    sectorNames.forEach((name) => {
      const sourceIndex = originalIndex.get(name);
      values.push(sourceIndex === undefined ? 0 : row.values[sourceIndex]);
    });
    matrix.push({ sector: row.sector, values });
  });

  const leadLag = (sectorPayload.lead_lag || []).filter(
    (row) => keep.has(row.sector_a) && keep.has(row.sector_b)
  );

  return {
    ...sectorPayload,
    sector_series: sectorSeries,
    correlation_matrix: matrix,
    lead_lag: leadLag,
  };
}

async function checkHealth() {
  try {
    const started = performance.now();
    const resp = await fetch(apiUrl("/health/dependencies"));
    const duration = performance.now() - started;
    const data = await resp.json();

    elements.latency.textContent = `${duration.toFixed(1)} ms`;
    elements.healthBadge.textContent = data.status === "ok" ? "API healthy" : "API degraded";
    elements.healthBadge.style.borderColor = data.status === "ok" ? "#49c6b4" : "#ff6b6b";
  } catch (error) {
    elements.healthBadge.textContent = "API unreachable";
    elements.healthBadge.style.borderColor = "#ff6b6b";
    elements.latency.textContent = "n/a";
  }
}

async function runAnalysis(forceRecompute) {
  const company = elements.company.value.trim() || "Apple";
  const windowDays = Number(elements.windowDays.value || 14);
  const bucket = elements.bucket.value;
  const method = elements.method.value;

  localStorage.setItem("dashboard.apiBaseUrl", elements.apiBaseUrl.value);
  localStorage.setItem("dashboard.company", company);

  const started = performance.now();

  const [queryResp, sectorResp] = await Promise.all([
    fetch(
      apiUrl("/query", {
        company,
        bucket,
        window_days: windowDays,
        item_limit: 50,
        recompute_timeline: forceRecompute,
      }),
      { method: "POST" }
    ),
    fetch(
      apiUrl("/sector-insights", {
        bucket,
        window_days: windowDays,
        method,
        max_lag: 3,
        recompute: forceRecompute,
      })
    ),
  ]);

  if (!queryResp.ok || !sectorResp.ok) {
    throw new Error("API call failed. Check backend logs.");
  }

  const queryData = await queryResp.json();
  const sectorDataRaw = await sectorResp.json();
  const sectorData = filteredSectorInsights(sectorDataRaw);

  normalizeSourceOptions(queryData.items || []);
  renderTimeline(queryData.timeline || []);
  renderHeatmap(sectorData.correlation_matrix || []);
  renderFeedCards(queryData.items || []);
  renderLeadLag(sectorData.lead_lag || []);

  state.sectors = (sectorDataRaw.sector_series || []).map((s) => s.sector);
  renderSectorChips(state.sectors);

  const failedJobs = (queryData.timeline || []).filter((point) => Number(point.item_count || 0) === 0).length;
  elements.failedJobs.textContent = String(failedJobs);
  elements.lastRun.textContent = queryData.timestamp || new Date().toISOString();

  const duration = performance.now() - started;
  elements.latency.textContent = `${duration.toFixed(1)} ms`;
}

function setInitialValues() {
  elements.apiBaseUrl.value = defaultApiBase();
  elements.company.value = localStorage.getItem("dashboard.company") || "Apple";
}

elements.runQuery.addEventListener("click", async () => {
  try {
    await runAnalysis(false);
  } catch (error) {
    alert(error.message || "Failed to run analysis");
  }
});

elements.forceRecompute.addEventListener("click", async () => {
  try {
    await runAnalysis(true);
  } catch (error) {
    alert(error.message || "Failed to recompute analytics");
  }
});

elements.sourceFilter.addEventListener("change", () => {
  state.sourceFilter = elements.sourceFilter.value;
  runAnalysis(false).catch((error) => alert(error.message || "Failed to refresh source filter"));
});

(async function bootstrap() {
  setInitialValues();
  await checkHealth();
  try {
    await runAnalysis(false);
  } catch (error) {
    elements.feedCards.innerHTML = `<p>${error.message}</p>`;
  }
})();
