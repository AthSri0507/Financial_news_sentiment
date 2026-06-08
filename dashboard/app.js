const state = {
  sectors: [],
  selectedSectors: new Set(),
  sourceFilter: "all",
  lastTimeline: [],
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
  freshnessBadge: document.getElementById("freshnessBadge"),
  timelineChart: document.getElementById("timelineChart"),
  smoothToggle: document.getElementById("smoothToggle"),
  heatmapChart: document.getElementById("heatmapChart"),
  feedCards: document.getElementById("feedCards"),
  leadLagTable: document.getElementById("leadLagTable"),
  sectorFilter: document.getElementById("sectorFilter"),
  healthBadge: document.getElementById("healthBadge"),
  lastRun: document.getElementById("lastRun"),
  energyInferences: document.getElementById("energyInferences"),
  energyKwh: document.getElementById("energyKwh"),
  energyCo2: document.getElementById("energyCo2"),
  energyBreakdown: document.getElementById("energyBreakdown"),
  energyMethodology: document.getElementById("energyMethodology"),
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

function movingAverage(values, window) {
  return values.map((_, i) => {
    const start = Math.max(0, i - window + 1);
    const slice = values.slice(start, i + 1).filter((v) => v !== null && v !== undefined);
    if (!slice.length) return null;
    return slice.reduce((a, b) => a + b, 0) / slice.length;
  });
}

function renderTimeline(timeline) {
  const x = timeline.map((p) => p.bucket_start);
  const y = timeline.map((p) => p.weighted_sentiment);
  const counts = timeline.map((p) => p.item_count || 0);
  const confidence = timeline.map((p) => p.confidence_score || 0);

  // Volume context (item counts) as faint bars on a secondary axis.
  const traceVolume = {
    x,
    y: counts,
    type: "bar",
    name: "Items",
    yaxis: "y2",
    marker: { color: "rgba(73,198,180,0.25)" },
    hovertemplate: "%{y} items<extra></extra>",
  };

  const traceSentiment = {
    x,
    y,
    mode: "lines+markers",
    name: "Weighted Sentiment",
    line: { color: "#49c6b4", width: 3 },
    connectgaps: false,
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

  const traces = [traceVolume, traceSentiment];

  if (elements.smoothToggle && elements.smoothToggle.checked) {
    traces.push({
      x,
      y: movingAverage(y, 3),
      mode: "lines",
      name: "Trend (3-pt avg)",
      line: { color: "#ffc857", width: 2, dash: "dash" },
      connectgaps: false,
    });
  }

  // Dynamic, padded y-axis so real variation is visible (vs. a flat line in a
  // fixed [-1,1] window). Clamped to the valid sentiment range.
  const finite = y.filter((v) => typeof v === "number");
  let yRange = [-1, 1];
  if (finite.length) {
    const lo = Math.min(...finite);
    const hi = Math.max(...finite);
    const pad = Math.max(0.05, (hi - lo) * 0.2);
    yRange = [Math.max(-1, lo - pad), Math.min(1, hi + pad)];
  }

  const layout = {
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { l: 40, r: 50, b: 50, t: 20 },
    font: { color: "#eaf6ff" },
    showlegend: false,
    yaxis: { title: "Sentiment", range: yRange },
    yaxis2: {
      title: "Items",
      overlaying: "y",
      side: "right",
      showgrid: false,
      rangemode: "tozero",
    },
    xaxis: { title: "Bucket" },
  };

  Plotly.newPlot(elements.timelineChart, traces, layout, { responsive: true });
}

function renderHeatmap(matrix) {
  if (!matrix.length) {
    elements.heatmapChart.innerHTML = "<p>No sector correlation data yet.</p>";
    return;
  }

  const sectors = matrix.map((row) => row.sector);
  const z = matrix.map((row) =>
    row.values.map((v) => (v === null || v === undefined ? null : v))
  );
  const text = matrix.map((row) =>
    row.values.map((v, j) => {
      const meta = (row.value_meta && row.value_meta[j]) || {};
      const n = meta.n ?? 0;
      if (v === null || v === undefined) {
        return `${row.sector} vs ${sectors[j]}<br>insufficient data (n=${n})`;
      }
      const p =
        meta.p_value === null || meta.p_value === undefined
          ? "n/a"
          : Number(meta.p_value).toFixed(3);
      const sig = meta.significant ? "significant" : "not significant";
      return `${row.sector} vs ${sectors[j]}<br>r=${Number(v).toFixed(3)} (n=${n}, p=${p}, ${sig})`;
    })
  );

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
        text,
        hoverinfo: "text",
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

function notableVoicesHtml(people) {
  if (!people || !people.length) return "";
  const chips = people
    .map((p) => `<span class="badge person">🎙 ${p.name}${p.role ? " · " + p.role : ""}</span>`)
    .join("");
  return `<div class="badges">${chips}</div>`;
}

function renderFreshness(freshness) {
  if (!elements.freshnessBadge) return;
  if (!freshness) {
    elements.freshnessBadge.textContent = "";
    return;
  }
  const mins = freshness.minutes_since_fetch;
  let label;
  let cls = "fresh-cached";
  switch (freshness.view_state) {
    case "updated":
      label = `Updated just now (${freshness.new_items} new article${freshness.new_items === 1 ? "" : "s"})`;
      cls = "fresh-updated";
      break;
    case "checked":
      label = "Checked just now (no newer articles found)";
      cls = "fresh-checked";
      break;
    case "unavailable":
      label = "Using cached data (refresh unavailable)";
      cls = "fresh-unavailable";
      break;
    default:
      label =
        mins === null || mins === undefined
          ? "Using cached data"
          : `Updated ${mins < 1 ? "just now" : Math.round(mins) + " min ago"}`;
      cls = "fresh-cached";
  }
  const q = freshness.quota || {};
  const quotaHint =
    q.newsapi_remaining !== undefined
      ? ` · quota N:${q.newsapi_remaining} M:${q.marketaux_remaining}`
      : "";
  elements.freshnessBadge.className = `freshness ${cls}`;
  elements.freshnessBadge.textContent = label + quotaHint;
}

async function checkEnergy() {
  if (!elements.energyKwh) return;
  try {
    const resp = await fetch(apiUrl("/energy"));
    if (!resp.ok) return;
    const data = await resp.json();
    const c = data.counts || {};
    const f = data.factors || {};
    const inferences =
      (c.finbert || 0) + (c.lexicon || 0) + (c.hf_summary || 0) + (c.hf_ner || 0);
    elements.energyInferences.textContent = `${inferences} (${c.total_items || 0} items)`;
    elements.energyKwh.textContent = `${Number(data.energy_kwh || 0).toFixed(4)} kWh`;
    elements.energyCo2.textContent = `${Number(data.co2_g || 0).toFixed(1)} gCO₂e`;
    if (elements.energyBreakdown) {
      elements.energyBreakdown.innerHTML = `
        <span class="badge">FinBERT ${c.finbert || 0}</span>
        <span class="badge">lexicon ${c.lexicon || 0}</span>
        <span class="badge">HF summary ${c.hf_summary || 0}</span>
        <span class="badge">extractive ${c.extractive_summary || 0}</span>
        <span class="badge">HF NER ${c.hf_ner || 0}</span>`;
    }
    if (elements.energyMethodology) {
      elements.energyMethodology.textContent =
        `${data.methodology || ""} Factors (Wh): FinBERT ${f.wh_per_finbert}, ` +
        `HF summary ${f.wh_per_hf_summary}, HF NER ${f.wh_per_hf_ner}, ` +
        `base/item ${f.wh_base_per_item}; grid ${f.grid_gco2_per_kwh} gCO₂/kWh.`;
    }
  } catch (error) {
    // Energy panel is best-effort; never block the dashboard.
  }
}

function marketReactionHtml(reaction) {
  // Observational: post-publication price movement over the primary (1d) window.
  // NOT a causal effect of the article.
  const state = reaction && reaction.state;
  const pct = reaction && reaction.change_pct;
  const win = (reaction && reaction.window) || "1d";
  const after = win === "1d" ? "1 day after" : `${win} after`;
  const fmt = (p) => `${p >= 0 ? "+" : ""}${Number(p).toFixed(1)}%`;
  if (state === "positive") {
    return `<span class="reaction moved" title="Post-publication price movement; not causal">↑ Price ${fmt(pct)} (${after})</span>`;
  }
  if (state === "negative") {
    return `<span class="reaction negative" title="Post-publication price movement; not causal">↓ Price ${fmt(pct)} (${after})</span>`;
  }
  if (state === "none") {
    const detail = pct === null || pct === undefined ? "" : ` (${fmt(pct)})`;
    return `<span class="reaction flat">≈ Price flat${detail} (${after})</span>`;
  }
  if (state === "undetermined") {
    return `<span class="reaction pending">— Price data unavailable</span>`;
  }
  return `<span class="reaction pending">⏳ Window not elapsed</span>`;
}

function sentimentHtml(item) {
  const conf = Number(item.sentiment_confidence || 0).toFixed(2);
  const src =
    item.sentiment_source === "finbert"
      ? "FinBERT"
      : item.sentiment_source === "lexicon"
      ? "lexicon"
      : "";
  const low = item.sentiment_low_confidence ? ' <span class="low-tag">low conf</span>' : "";
  const cls = item.sentiment_low_confidence ? "sentiment low-conf" : "sentiment";
  const srcTxt = src ? ` · ${src}` : "";
  return `<span class="${cls}" style="color:${sentimentColor(item.sentiment_label)}">${item.sentiment_label} · ${conf}${srcTxt}${low}</span>`;
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
    const summarySource = item.summary_source === "hf" ? `<span class="badge">AI</span>` : "";

    card.innerHTML = `
      <div class="feed-head">
        <strong>${item.title || "Untitled"}</strong>
        ${sentimentHtml(item)}
      </div>
      <p>${item.summary || "No summary available."}</p>
      ${notableVoicesHtml(item.notable_people)}
      <div class="feed-head">
        <a href="${item.url}" target="_blank" rel="noopener noreferrer">Open source</a>
        <span>Predicted Impact ${Number(item.impact_score || 0).toFixed(3)}</span>
      </div>
      <div class="feed-head">
        <span class="muted-label">Price after publication</span>
        ${marketReactionHtml(item.market_reaction)}
      </div>
      <div class="badges">
        <span class="badge">R ${(factors.reliability || 0).toFixed(2)}</span>
        <span class="badge">E ${(factors.engagement || 0).toFixed(2)}</span>
        <span class="badge">Q ${(factors.relevance || 0).toFixed(2)}</span>
        <span class="badge">Rec ${(factors.recency || 0).toFixed(2)}</span>
        <span class="badge">Mag ${(factors.magnitude || 0).toFixed(2)}</span>
        ${(factors.notable || 0) > 0 ? `<span class="badge">★ ${(factors.notable).toFixed(2)}</span>` : ""}
        ${summarySource}
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
    const hasCorr = row.correlation !== null && row.correlation !== undefined;
    const leaderText =
      !hasCorr || row.leader === "none" ? "No lead" : `${row.leader} -> ${row.follower}`;
    const corrText = hasCorr
      ? `${Number(row.correlation).toFixed(3)}${row.significant ? " *" : ""}`
      : "insufficient";
    const lagText = hasCorr ? row.best_lag : "—";
    tr.innerHTML = `
      <td>${row.sector_a} vs ${row.sector_b}</td>
      <td>${lagText}</td>
      <td>${corrText}</td>
      <td>${row.n ?? 0}</td>
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
    const valueMeta = [];
    sectorNames.forEach((name) => {
      const sourceIndex = originalIndex.get(name);
      if (sourceIndex === undefined) {
        values.push(null);
        valueMeta.push({ n: 0, p_value: null, significant: false });
      } else {
        values.push(row.values[sourceIndex]);
        valueMeta.push(row.value_meta ? row.value_meta[sourceIndex] : {});
      }
    });
    matrix.push({ sector: row.sector, values, value_meta: valueMeta });
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
  state.lastTimeline = queryData.timeline || [];
  renderTimeline(state.lastTimeline);
  renderHeatmap(sectorData.correlation_matrix || []);
  renderFeedCards(queryData.items || []);
  renderLeadLag(sectorData.lead_lag || []);
  renderFreshness(queryData.freshness);

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

if (elements.smoothToggle) {
  elements.smoothToggle.addEventListener("change", () => {
    renderTimeline(state.lastTimeline || []);
  });
}

(async function bootstrap() {
  setInitialValues();
  await checkHealth();
  checkEnergy();
  try {
    await runAnalysis(false);
  } catch (error) {
    elements.feedCards.innerHTML = `<p>${error.message}</p>`;
  }
})();
