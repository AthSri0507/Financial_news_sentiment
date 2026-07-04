"use strict";

const state = {
  activeTab: "overview",
  query: null,
  sectorRaw: null,
  movers: null,
  agreement: null,
  agreementHistory: [],
  energy: null,
  selectedSectors: new Set(),
  sourceFilter: "all",
  feedFilter: "all",
  personFilter: null,
  lastTimeline: [],
};

const $ = (id) => document.getElementById(id);
const el = {
  apiBaseUrl: $("apiBaseUrl"),
  company: $("company"),
  windowDays: $("windowDays"),
  bucket: $("bucket"),
  method: $("method"),
  sourceFilter: $("sourceFilter"),
  sectorFilter: $("sectorFilter"),
  runQuery: $("runQuery"),
  forceRecompute: $("forceRecompute"),
  freshnessBadge: $("freshnessBadge"),
  statusIndicator: $("statusIndicator"),
  healthBadge: $("healthBadge"),
  tabNav: $("tabNav"),
  toasts: $("toasts"),
  // overview
  snapshot: $("snapshot"),
  kpiStrip: $("kpiStrip"),
  marketMovers: $("marketMovers"),
  topStories: $("topStories"),
  timelineChart: $("timelineChart"),
  smoothToggle: $("smoothToggle"),
  // feed
  feedFilters: $("feedFilters"),
  feedHeader: $("feedHeader"),
  feedCards: $("feedCards"),
  // correlations
  corrInsight: $("corrInsight"),
  heatmapChart: $("heatmapChart"),
  leadLagTable: $("leadLagTable"),
  sectorCoverage: $("sectorCoverage"),
  // validation
  validationHero: $("validationHero"),
  reactionDist: $("reactionDist"),
  agreementSpark: $("agreementSpark"),
  // observability
  obsInsight: $("obsInsight"),
  energyInferences: $("energyInferences"),
  energyKwh: $("energyKwh"),
  energyCo2: $("energyCo2"),
  energyBreakdown: $("energyBreakdown"),
  energyMethodology: $("energyMethodology"),
  lastRun: $("lastRun"),
  failedJobs: $("failedJobs"),
  latency: $("latency"),
  quotaNews: $("quotaNews"),
  quotaMarketaux: $("quotaMarketaux"),
};

const PLOT_LAYOUT = {
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "rgba(0,0,0,0)",
  font: { color: "#cfe0ee", family: "Space Grotesk" },
};

// ---------- helpers ----------
function defaultApiBase() {
  const qs = new URLSearchParams(window.location.search);
  return qs.get("apiBaseUrl") || localStorage.getItem("dashboard.apiBaseUrl") || "http://127.0.0.1:8000";
}

function apiUrl(path, params = {}) {
  const base = el.apiBaseUrl.value.replace(/\/$/, "");
  const url = new URL(`${base}${path}`);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, String(v));
  });
  return url.toString();
}

function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
  );
}

function sentimentColor(label) {
  if (label === "positive") return "#7ee08a";
  if (label === "negative") return "#ff7b7b";
  return "#ffc857";
}

function timeAgo(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (isNaN(d)) return "";
  const mins = Math.round((Date.now() - d.getTime()) / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.round(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.round(hrs / 24)}d ago`;
}

function toast(msg, kind = "info") {
  const t = document.createElement("div");
  t.className = `toast toast-${kind}`;
  t.textContent = msg;
  el.toasts.appendChild(t);
  setTimeout(() => t.classList.add("show"), 10);
  setTimeout(() => {
    t.classList.remove("show");
    setTimeout(() => t.remove(), 300);
  }, 3600);
}

function setStatus(text, kind = "idle") {
  el.statusIndicator.textContent = text;
  el.statusIndicator.className = `status-indicator ${kind}`;
}

function setBusy(on) {
  [el.runQuery, el.forceRecompute].forEach((b) => {
    b.disabled = on;
    b.classList.toggle("busy", on);
  });
}

// ---------- tabs ----------
function resizeTabCharts(name) {
  const map = {
    overview: [el.timelineChart],
    correlations: [el.heatmapChart],
    validation: [el.agreementSpark],
  };
  (map[name] || []).forEach((div) => {
    if (div && div.data) {
      try {
        Plotly.Plots.resize(div);
      } catch (e) {
        /* ignore */
      }
    }
  });
}

function setTab(name) {
  state.activeTab = name;
  document.querySelectorAll(".tab-view").forEach((v) => v.classList.remove("active"));
  const view = $(`view-${name}`);
  if (view) view.classList.add("active");
  document.querySelectorAll("#tabNav .tab").forEach((t) =>
    t.classList.toggle("active", t.dataset.tab === name)
  );
  if (history.replaceState) history.replaceState(null, "", `#${name}`);
  resizeTabCharts(name);
}

// ---------- timeline / heatmap / leadlag (charts) ----------
function movingAverage(values, w) {
  return values.map((_, i) => {
    const slice = values.slice(Math.max(0, i - w + 1), i + 1).filter((v) => v != null);
    return slice.length ? slice.reduce((a, b) => a + b, 0) / slice.length : null;
  });
}

function renderTimeline(timeline) {
  const x = timeline.map((p) => p.bucket_start);
  const y = timeline.map((p) => p.weighted_sentiment);
  const counts = timeline.map((p) => p.item_count || 0);
  const confidence = timeline.map((p) => p.confidence_score || 0);

  const traces = [
    {
      x, y: counts, type: "bar", name: "Items", yaxis: "y2",
      marker: { color: "rgba(73,198,180,0.22)" },
      hovertemplate: "%{y} items<extra></extra>",
    },
    {
      x, y, mode: "lines+markers", name: "Weighted Sentiment",
      line: { color: "#49c6b4", width: 3 }, connectgaps: false,
      marker: {
        size: confidence.map((c) => 8 + c * 9), color: confidence,
        colorscale: "YlGnBu", cmin: 0, cmax: 1, showscale: true,
        colorbar: { title: "Conf", thickness: 10 },
      },
    },
  ];
  if (el.smoothToggle && el.smoothToggle.checked) {
    traces.push({
      x, y: movingAverage(y, 3), mode: "lines", name: "Trend",
      line: { color: "#ffc857", width: 2, dash: "dash" }, connectgaps: false,
    });
  }
  const finite = y.filter((v) => typeof v === "number");
  let yr = [-1, 1];
  if (finite.length) {
    const lo = Math.min(...finite), hi = Math.max(...finite);
    const pad = Math.max(0.05, (hi - lo) * 0.2);
    yr = [Math.max(-1, lo - pad), Math.min(1, hi + pad)];
  }
  Plotly.newPlot(el.timelineChart, traces, {
    ...PLOT_LAYOUT, margin: { l: 42, r: 48, b: 44, t: 12 }, showlegend: false,
    yaxis: { title: "Sentiment", range: yr, gridcolor: "rgba(255,255,255,0.06)" },
    yaxis2: { title: "Items", overlaying: "y", side: "right", showgrid: false, rangemode: "tozero" },
    xaxis: { gridcolor: "rgba(255,255,255,0.06)" },
  }, { responsive: true, displayModeBar: false });
}

function renderHeatmap(matrix) {
  if (!matrix.length) {
    el.heatmapChart.innerHTML = "<p class='empty-note'>No sector data for this window yet.</p>";
    return;
  }
  const sectors = matrix.map((r) => r.sector);
  const z = matrix.map((r) => r.values.map((v) => (v == null ? null : v)));
  const text = matrix.map((r) =>
    r.values.map((v, j) => {
      const m = (r.value_meta && r.value_meta[j]) || {};
      if (v == null) return `${r.sector} vs ${sectors[j]}<br>insufficient data (n=${m.n ?? 0})`;
      const p = m.p_value == null ? "n/a" : Number(m.p_value).toFixed(3);
      return `${r.sector} vs ${sectors[j]}<br>r=${Number(v).toFixed(2)} (n=${m.n ?? 0}, p=${p})`;
    })
  );
  Plotly.newPlot(el.heatmapChart, [{
    z, x: sectors, y: sectors, type: "heatmap", colorscale: "RdBu",
    zmin: -1, zmax: 1, text, hoverinfo: "text", xgap: 2, ygap: 2,
  }], { ...PLOT_LAYOUT, margin: { l: 110, r: 16, b: 110, t: 12 } },
    { responsive: true, displayModeBar: false });
}

function renderLeadLag(rows) {
  el.leadLagTable.innerHTML = "";
  const sorted = rows.slice().sort((a, b) => Math.abs(b.correlation || 0) - Math.abs(a.correlation || 0));
  sorted.slice(0, 20).forEach((row) => {
    const has = row.correlation != null;
    const leader = !has || row.leader === "none" ? "—" : `${row.leader} → ${row.follower}`;
    const corr = has ? `${Number(row.correlation).toFixed(2)}${row.significant ? " *" : ""}` : "insufficient";
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${esc(row.sector_a)} vs ${esc(row.sector_b)}</td>
      <td>${has ? row.best_lag : "—"}</td>
      <td class="${has && row.correlation < 0 ? "neg" : "pos"}">${corr}</td>
      <td>${row.n ?? 0}</td><td>${esc(leader)}</td>`;
    el.leadLagTable.appendChild(tr);
  });
}

// ---------- feed ----------
function driverChips(item) {
  const f = item.impact_factors || {};
  const contribs = [
    ["reliability", "reliability", f.reliability_contribution],
    ["engagement", "engagement", f.engagement_contribution],
    ["relevance", "relevance", f.relevance_contribution],
    ["recency", "recency", f.recency_contribution],
    ["magnitude", "sentiment strength", f.magnitude_contribution],
  ].filter((c) => c[2] != null).sort((a, b) => b[2] - a[2]);
  const qual = (v) => (v >= 0.66 ? "High" : v >= 0.33 ? "Moderate" : "Low");
  const drivers = [];
  contribs.slice(0, 2).forEach(([key, label]) => {
    drivers.push(`${qual(f[key] ?? 0)} ${label}`);
  });
  if ((item.notable_people || []).length) {
    drivers.push(`Notable: ${item.notable_people[0].name}`);
  }
  if (Math.abs(item.sentiment_score || 0) >= 0.55) {
    drivers.push(`Strong ${item.sentiment_label} sentiment`);
  }
  return drivers;
}

function sentimentChip(item) {
  const conf = Number(item.sentiment_confidence || 0).toFixed(2);
  const src = item.sentiment_source === "finbert" ? "FinBERT" : item.sentiment_source === "lexicon" ? "lexicon" : "";
  const low = item.sentiment_low_confidence ? ' <span class="low-tag">low conf</span>' : "";
  const cls = item.sentiment_low_confidence ? "sentiment low-conf" : "sentiment";
  return `<span class="${cls}" style="color:${sentimentColor(item.sentiment_label)}">
    ●&nbsp;${esc(item.sentiment_label)} · ${conf}${src ? " · " + src : ""}${low}</span>`;
}

function marketReactionHtml(reaction) {
  const s = reaction && reaction.state;
  const pct = reaction && reaction.change_pct;
  const win = (reaction && reaction.window) || "1d";
  const after = win === "1d" ? "1 day after" : `${win} after`;
  const fmt = (p) => `${p >= 0 ? "+" : ""}${Number(p).toFixed(1)}%`;
  const t = "Post-publication price movement; not causal";
  if (s === "positive") return `<span class="reaction moved" title="${t}">↑ Price ${fmt(pct)} (${after})</span>`;
  if (s === "negative") return `<span class="reaction negative" title="${t}">↓ Price ${fmt(pct)} (${after})</span>`;
  if (s === "none") {
    const d = pct == null ? "" : ` (${fmt(pct)})`;
    return `<span class="reaction flat">≈ Price flat${d} (${after})</span>`;
  }
  if (s === "undetermined") return `<span class="reaction pending">— Price data unavailable</span>`;
  return `<span class="reaction pending">⏳ Window not elapsed</span>`;
}

function notableCallout(item) {
  const people = item.notable_people || [];
  if (!people.length) return "";
  const contrib = (item.impact_factors || {}).notable_contribution;
  const chips = people
    .map((p) => `<span class="mover-name">${esc(p.name)}</span>${p.role ? `<span class="mover-role">${esc(p.role)}</span>` : ""}`)
    .join('<span class="dot">·</span>');
  const c = contrib ? `<span class="mover-impact">impact +${Number(contrib).toFixed(2)}</span>` : "";
  return `<div class="callout"><span class="callout-tag">🎙 Market Mover</span>${chips}${c}</div>`;
}

function feedCardHtml(item, rank) {
  const impact = Number(item.impact_score || 0);
  const top = rank <= 3 ? " top" : "";
  const drivers = driverChips(item);
  const f = item.impact_factors || {};
  return `<article class="feed-card${top}">
    <div class="rank-col"><span class="rank-badge">#${rank}</span></div>
    <div class="feed-body">
      <div class="feed-top">
        <h3 class="headline">${esc(item.title || "Untitled")}</h3>
        ${sentimentChip(item)}
      </div>
      <p class="summary">${esc(item.summary || "No summary available.")}</p>
      ${notableCallout(item)}
      <div class="meta-row">
        <span class="src">${esc(item.source_type || "unknown")}</span>
        <span class="dot">·</span><span>${esc(timeAgo(item.published_at))}</span>
        <a href="${esc(item.url)}" target="_blank" rel="noopener noreferrer">Open source ↗</a>
      </div>
      <div class="impact-row">
        <div class="impact-label">Predicted Impact <strong>${impact.toFixed(2)}</strong></div>
        <div class="impact-bar"><span style="width:${Math.round(impact * 100)}%"></span></div>
      </div>
      <div class="drivers"><span class="why">Why ranked high:</span> ${drivers.map((d) => `<span class="driver">${esc(d)}</span>`).join("")}</div>
      ${marketReactionHtml(item.market_reaction)}
      <div class="badges">
        <span class="badge">R ${(f.reliability || 0).toFixed(2)}</span>
        <span class="badge">E ${(f.engagement || 0).toFixed(2)}</span>
        <span class="badge">Q ${(f.relevance || 0).toFixed(2)}</span>
        <span class="badge">Rec ${(f.recency || 0).toFixed(2)}</span>
        <span class="badge">Mag ${(f.magnitude || 0).toFixed(2)}</span>
        ${(f.notable || 0) > 0 ? `<span class="badge star">★ ${f.notable.toFixed(2)}</span>` : ""}
        ${item.summary_source === "hf" ? `<span class="badge">AI summary</span>` : ""}
      </div>
    </div>
  </article>`;
}

function visibleFeedItems() {
  let items = (state.query && state.query.items) || [];
  if (state.sourceFilter !== "all") {
    items = items.filter((it) => (it.source_type || "unknown") === state.sourceFilter);
  }
  if (state.feedFilter === "movers") {
    items = items.filter((it) => (it.notable_people || []).length > 0);
    if (state.personFilter) {
      items = items.filter((it) =>
        (it.notable_people || []).some((p) => p.name === state.personFilter)
      );
    }
  } else if (state.feedFilter !== "all") {
    items = items.filter((it) => (it.sentiment_label || "neutral") === state.feedFilter);
  }
  return items;
}

function renderFeed() {
  const items = visibleFeedItems();
  // counts on the filter tabs
  const all = (state.query && state.query.items) || [];
  const counts = {
    all: all.length,
    positive: all.filter((i) => i.sentiment_label === "positive").length,
    neutral: all.filter((i) => i.sentiment_label === "neutral").length,
    negative: all.filter((i) => i.sentiment_label === "negative").length,
    movers: all.filter((i) => (i.notable_people || []).length).length,
  };
  document.querySelectorAll("#feedFilters .ftab").forEach((b) => {
    const k = b.dataset.filter;
    b.classList.toggle("active", k === state.feedFilter);
    const base = b.textContent.replace(/\s*\(\d+\)$/, "");
    b.textContent = `${base} (${counts[k] ?? 0})`;
  });

  let header = "";
  if (state.personFilter) {
    header = `Showing <strong>${esc(items.length)}</strong> ${esc(state.personFilter)}-related ${items.length === 1 ? "story" : "stories"}, ranked by impact
      <button class="clear-person" id="clearPerson">clear</button>`;
  } else if (state.feedFilter !== "all") {
    header = `<strong>${items.length}</strong> ${esc(state.feedFilter)} ${items.length === 1 ? "story" : "stories"}, ranked by impact`;
  } else {
    header = `<strong>${items.length}</strong> stories, ranked by predicted impact`;
  }
  el.feedHeader.innerHTML = header;
  const cp = $("clearPerson");
  if (cp) cp.onclick = () => { state.personFilter = null; renderFeed(); };

  if (!items.length) {
    el.feedCards.innerHTML = "<p class='empty-note'>No stories match this filter.</p>";
    return;
  }
  // rank = position within the FULL impact-sorted list (so #s are global, not per-filter)
  const rankOf = new Map(all.map((it, i) => [it.processed_id, i + 1]));
  el.feedCards.innerHTML = items.map((it) => feedCardHtml(it, rankOf.get(it.processed_id) || 0)).join("");
}

// ---------- overview: KPIs, snapshot, movers, top stories ----------
function avg(arr) {
  const v = arr.filter((x) => typeof x === "number");
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}

function kpiCard(label, value, sub, tone) {
  return `<div class="kpi ${tone || ""}"><p>${esc(label)}</p><strong>${value}</strong>${sub ? `<span>${esc(sub)}</span>` : ""}</div>`;
}

function renderKpis() {
  const q = state.query || {};
  const items = q.items || [];
  const timeline = q.timeline || [];
  const articles = timeline.reduce((a, p) => a + (p.item_count || 0), 0) || items.length;
  const sentiment = q.average_weighted_sentiment;
  const impact = avg(items.map((i) => i.impact_score));
  const sectors = state.sectorRaw ? (state.sectorRaw.sector_count || (state.sectorRaw.sector_series || []).length) : 0;
  const fresh = q.freshness ? q.freshness.new_items : 0;
  const movers = state.movers ? state.movers.distinct_people : 0;
  const mins = q.freshness ? q.freshness.minutes_since_fetch : null;
  const lastRefresh = mins == null ? "—" : mins < 1 ? "just now" : `${Math.round(mins)}m ago`;
  const sTone = sentiment == null ? "" : sentiment > 0.05 ? "good" : sentiment < -0.05 ? "bad" : "";

  el.kpiStrip.innerHTML =
    kpiCard("Articles Analyzed", articles) +
    kpiCard("Average Sentiment", sentiment == null ? "—" : sentiment.toFixed(2),
      sentiment == null ? "" : sentiment > 0 ? "net positive" : sentiment < 0 ? "net negative" : "neutral", sTone) +
    kpiCard("Average Impact", impact == null ? "—" : impact.toFixed(2)) +
    kpiCard("Active Sectors", sectors) +
    kpiCard("Fresh Articles", fresh) +
    kpiCard("Market Movers", movers, "notable people", movers > 0 ? "good" : "") +
    kpiCard("Last Refresh", lastRefresh);
}

function strongestLeadLag() {
  const ll = ((state.sectorRaw && state.sectorRaw.lead_lag) || [])
    .filter((r) => r.correlation != null && (r.n || 0) >= 5 && r.leader && r.leader !== "none")
    .sort((a, b) => Math.abs(b.correlation) - Math.abs(a.correlation));
  return ll[0] || null;
}

function renderSnapshot() {
  const q = state.query || {};
  const items = q.items || [];
  const top = items[0];
  const mover = state.movers && state.movers.most_mentioned && state.movers.most_mentioned[0];
  const ll = strongestLeadLag();
  const ag = state.agreement;

  const line = (icon, label, value) =>
    `<div class="snap-item"><span class="snap-ic">${icon}</span><div><p>${label}</p><strong>${value}</strong></div></div>`;

  const topStory = top
    ? `${esc((top.title || "").slice(0, 70))}${(top.title || "").length > 70 ? "…" : ""} <span class="snap-pill">impact ${Number(top.impact_score || 0).toFixed(2)}</span>`
    : "No stories yet";
  const moverTxt = mover ? `${esc(mover.name)} <span class="snap-pill">${mover.article_count} articles</span>` : "None detected";
  const llTxt = ll
    ? `${esc(ll.leader)} leads ${esc(ll.leader === ll.sector_a ? ll.sector_b : ll.sector_a)} by ${Math.abs(ll.best_lag)}d <span class="snap-pill">r ${Number(ll.correlation).toFixed(2)}</span>`
    : "Gathering data…";
  const agTxt = ag && ag.agreement_pct != null
    ? `${ag.agreement_pct}% of high-impact stories moved the market`
    : "Not enough validated stories yet";

  el.snapshot.innerHTML = `
    <div class="snap-head"><span class="eyebrow">Market Snapshot</span><span class="snap-co">${esc(q.company || "")}</span></div>
    <div class="snap-grid">
      ${line("📰", "Top Story", topStory)}
      ${line("🎙", "Top Market Mover", moverTxt)}
      ${line("🔗", "Strongest Sector Link", llTxt)}
      ${line("✓", "Validation Agreement", agTxt)}
    </div>`;
}

function renderMarketMovers() {
  const mm = state.movers;
  if (!mm || !(mm.most_mentioned || []).length) {
    el.marketMovers.innerHTML = "<p class='empty-note'>No market movers in this company's stories.</p>";
    return;
  }
  const max = mm.most_mentioned[0].article_count || 1;
  el.marketMovers.innerHTML = mm.most_mentioned
    .map((p) => `<button class="mover" data-person="${esc(p.name)}">
        <div class="mover-line"><span class="mover-name">${esc(p.name)}</span>
          ${p.role ? `<span class="mover-role">${esc(p.role)}</span>` : ""}
          <span class="mover-count">${p.article_count} articles</span></div>
        <div class="mover-bar"><span style="width:${Math.round((p.article_count / max) * 100)}%"></span></div>
      </button>`)
    .join("");
  el.marketMovers.querySelectorAll(".mover").forEach((b) => {
    b.onclick = () => {
      state.personFilter = b.dataset.person;
      state.feedFilter = "movers";
      renderFeed();
      setTab("feed");
    };
  });
}

function renderTopStories() {
  const items = ((state.query && state.query.items) || []).slice(0, 5);
  if (!items.length) {
    el.topStories.innerHTML = "<p class='empty-note'>No stories yet.</p>";
    return;
  }
  el.topStories.innerHTML = items
    .map((it, i) => {
      const person = (it.notable_people || [])[0];
      return `<button class="story-mini${i === 0 ? " lead" : ""}" data-id="${esc(it.processed_id)}">
        <span class="story-rank">#${i + 1}</span>
        <span class="story-dot" style="background:${sentimentColor(it.sentiment_label)}"></span>
        <span class="story-title">${esc(it.title || "Untitled")}</span>
        ${person ? `<span class="story-person">🎙 ${esc(person.name)}</span>` : ""}
        <span class="story-impact">${Number(it.impact_score || 0).toFixed(2)}</span>
      </button>`;
    })
    .join("");
  el.topStories.querySelectorAll(".story-mini").forEach((b) => {
    b.onclick = () => {
      state.feedFilter = "all";
      state.personFilter = null;
      renderFeed();
      setTab("feed");
      const card = [...el.feedCards.querySelectorAll(".feed-card")].find((c) =>
        c.querySelector(".headline")
      );
      if (card) card.scrollIntoView({ behavior: "smooth", block: "center" });
    };
  });
}

// ---------- correlations: insight + coverage ----------
function renderCorrInsight() {
  const ll = strongestLeadLag();
  if (!ll) {
    el.corrInsight.innerHTML = "Not enough overlapping sector history yet — see coverage below.";
    return;
  }
  const follower = ll.leader === ll.sector_a ? ll.sector_b : ll.sector_a;
  el.corrInsight.innerHTML = `<strong>${esc(ll.leader)}</strong> currently leads <strong>${esc(follower)}</strong>
    by <strong>${Math.abs(ll.best_lag)} day${Math.abs(ll.best_lag) === 1 ? "" : "s"}</strong>
    (r ${Number(ll.correlation).toFixed(2)}).`;
}

function renderSectorCoverage() {
  const series = (state.sectorRaw && state.sectorRaw.sector_series) || [];
  if (!series.length) {
    el.sectorCoverage.innerHTML = "<p class='empty-note'>No sector data yet — ingest a few companies.</p>";
    return;
  }
  const MIN = 5;
  const rows = series
    .map((s) => {
      const days = (s.points || []).filter((p) => (p.item_count || 0) > 0).length;
      return { sector: s.sector, items: s.item_count || 0, days };
    })
    .sort((a, b) => b.items - a.items);
  const eligible = rows.filter((r) => r.days >= MIN).length;
  const maxItems = Math.max(1, ...rows.map((r) => r.items));
  el.sectorCoverage.innerHTML =
    `<p class="coverage-head"><strong>${eligible}/${rows.length}</strong> sectors have ≥${MIN} days of data (the bar for correlations).</p>` +
    rows
      .map((r) => `<div class="cov-row ${r.days >= MIN ? "ok" : ""}">
        <span class="cov-name">${esc(r.sector)}</span>
        <div class="cov-bar"><span style="width:${Math.round((r.items / maxItems) * 100)}%"></span></div>
        <span class="cov-stat">${r.items} articles · ${r.days}d</span></div>`)
      .join("");
}

// ---------- validation ----------
function renderValidation() {
  const ag = state.agreement;
  if (ag && ag.agreement_pct != null) {
    el.validationHero.innerHTML = `<div class="val-big">${ag.agreement_pct}%</div>
      <div class="val-sub">of high-impact stories (predicted impact &gt; ${ag.impact_threshold ?? 0.75})
      were followed by a <strong>significant 1-day price move</strong>
      <span class="val-n">${ag.n_moved}/${ag.n_high_impact} validated</span></div>`;
  } else {
    el.validationHero.innerHTML = `<div class="val-big muted">—</div>
      <div class="val-sub">Not enough validated high-impact stories yet. Run <code>/validate/run</code> after the 1-day windows elapse.</div>`;
  }

  // reaction distribution from current feed
  const items = (state.query && state.query.items) || [];
  const dist = { positive: 0, negative: 0, none: 0, undetermined: 0, pending: 0 };
  items.forEach((it) => {
    const s = (it.market_reaction && it.market_reaction.state) || "pending";
    dist[s] = (dist[s] || 0) + 1;
  });
  const total = Math.max(1, items.length);
  const seg = (key, label, cls) =>
    `<div class="rd-row"><span class="rd-label ${cls}">${label}</span>
      <div class="rd-bar"><span class="${cls}" style="width:${Math.round((dist[key] / total) * 100)}%"></span></div>
      <span class="rd-n">${dist[key]}</span></div>`;
  el.reactionDist.innerHTML =
    seg("positive", "↑ Price up", "pos") +
    seg("negative", "↓ Price down", "neg") +
    seg("none", "≈ Flat", "flat") +
    seg("undetermined", "— Unavailable", "muted") +
    seg("pending", "⏳ Not elapsed", "muted");

  // agreement sparkline
  const hist = state.agreementHistory || [];
  if (hist.length >= 2) {
    Plotly.newPlot(el.agreementSpark, [{
      x: hist.map((h) => h.day), y: hist.map((h) => h.agreement_pct),
      mode: "lines+markers", line: { color: "#7ee08a", width: 2 }, marker: { size: 5 },
    }], { ...PLOT_LAYOUT, margin: { l: 36, r: 12, b: 36, t: 8 },
      yaxis: { title: "%", range: [0, 100], gridcolor: "rgba(255,255,255,0.06)" },
      xaxis: { gridcolor: "rgba(255,255,255,0.06)" } },
      { responsive: true, displayModeBar: false });
  } else {
    el.agreementSpark.innerHTML = "<p class='empty-note'>Trend appears after a few daily snapshots.</p>";
  }
}

// ---------- observability ----------
function renderObservability() {
  const q = state.query || {};
  const fr = q.freshness || {};
  const quota = fr.quota || {};
  el.quotaNews.textContent = quota.newsapi_remaining != null ? quota.newsapi_remaining : "—";
  el.quotaMarketaux.textContent = quota.marketaux_remaining != null ? quota.marketaux_remaining : "—";
  el.lastRun.textContent = q.timestamp ? new Date(q.timestamp).toLocaleString() : "—";
  const failed = (q.timeline || []).filter((p) => Number(p.item_count || 0) === 0).length;
  el.failedJobs.textContent = String(failed);

  const e = state.energy;
  const a = e ? e.counts.total_items : "—";
  const k = e ? Number(e.energy_kwh || 0).toFixed(3) : "—";
  const c = e ? Number(e.co2_g || 0).toFixed(0) : "—";
  el.obsInsight.innerHTML = e
    ? `<strong>${a}</strong> articles processed · est. <strong>${k} kWh</strong> · <strong>${c} gCO₂e</strong> (estimated from inference counts).`
    : "Loading model footprint…";
}

async function checkEnergy() {
  try {
    const resp = await fetch(apiUrl("/energy"));
    if (!resp.ok) return;
    const data = await resp.json();
    state.energy = data;
    const c = data.counts || {}, f = data.factors || {};
    const inf = (c.finbert || 0) + (c.lexicon || 0) + (c.hf_summary || 0) + (c.hf_ner || 0);
    el.energyInferences.textContent = `${inf}`;
    el.energyKwh.textContent = `${Number(data.energy_kwh || 0).toFixed(4)} kWh`;
    el.energyCo2.textContent = `${Number(data.co2_g || 0).toFixed(1)} gCO₂e`;
    el.energyBreakdown.innerHTML = `
      <span class="badge">FinBERT ${c.finbert || 0}</span>
      <span class="badge">lexicon ${c.lexicon || 0}</span>
      <span class="badge">HF summary ${c.hf_summary || 0}</span>
      <span class="badge">extractive ${c.extractive_summary || 0}</span>
      <span class="badge">HF NER ${c.hf_ner || 0}</span>`;
    el.energyMethodology.textContent =
      `${data.methodology || ""} Factors (Wh): FinBERT ${f.wh_per_finbert}, HF summary ${f.wh_per_hf_summary}, ` +
      `HF NER ${f.wh_per_hf_ner}, base/item ${f.wh_base_per_item}; grid ${f.grid_gco2_per_kwh} gCO₂/kWh.`;
    renderObservability();
  } catch (e) {
    /* best-effort */
  }
}

// ---------- sector chip filter ----------
function renderSectorChips(sectors) {
  el.sectorFilter.innerHTML = "";
  const mk = (label, active, onclick) => {
    const b = document.createElement("button");
    b.className = `chip ${active ? "active" : ""}`;
    b.textContent = label;
    b.onclick = onclick;
    el.sectorFilter.appendChild(b);
  };
  mk("All", state.selectedSectors.size === 0, () => {
    state.selectedSectors.clear();
    applySectorFilter();
  });
  sectors.forEach((s) =>
    mk(s, state.selectedSectors.has(s), () => {
      state.selectedSectors.has(s) ? state.selectedSectors.delete(s) : state.selectedSectors.add(s);
      applySectorFilter();
    })
  );
}

function filteredSectorInsights(payload) {
  if (state.selectedSectors.size === 0) return payload;
  const keep = new Set(state.selectedSectors);
  const series = (payload.sector_series || []).filter((r) => keep.has(r.sector));
  const names = series.map((r) => r.sector);
  const origIdx = new Map((payload.correlation_matrix || []).map((r, i) => [r.sector, i]));
  const matrix = [];
  (payload.correlation_matrix || []).forEach((row) => {
    if (!keep.has(row.sector)) return;
    const values = [], meta = [];
    names.forEach((n) => {
      const si = origIdx.get(n);
      values.push(si === undefined ? null : row.values[si]);
      meta.push(si === undefined ? { n: 0 } : (row.value_meta ? row.value_meta[si] : {}));
    });
    matrix.push({ sector: row.sector, values, value_meta: meta });
  });
  const leadLag = (payload.lead_lag || []).filter((r) => keep.has(r.sector_a) && keep.has(r.sector_b));
  return { ...payload, sector_series: series, correlation_matrix: matrix, lead_lag: leadLag };
}

function applySectorFilter() {
  renderSectorChips(state.sectorRaw ? state.sectorRaw.sector_series.map((s) => s.sector) : []);
  const data = filteredSectorInsights(state.sectorRaw || {});
  renderHeatmap(data.correlation_matrix || []);
  renderLeadLag(data.lead_lag || []);
}

// ---------- freshness / health ----------
function renderFreshness(freshness) {
  if (!el.freshnessBadge) return;
  if (!freshness) { el.freshnessBadge.textContent = ""; return; }
  const mins = freshness.minutes_since_fetch;
  let label, cls = "fresh-cached";
  switch (freshness.view_state) {
    case "updated":
      label = `Updated just now (${freshness.new_items} new)`; cls = "fresh-updated"; break;
    case "checked":
      label = "Checked just now (nothing newer)"; cls = "fresh-checked"; break;
    case "unavailable":
      label = "Using cached data"; cls = "fresh-unavailable"; break;
    default:
      label = mins == null ? "Using cached data" : `Updated ${mins < 1 ? "just now" : Math.round(mins) + "m ago"}`;
  }
  el.freshnessBadge.className = `freshness ${cls}`;
  el.freshnessBadge.textContent = label;
}

async function checkHealth() {
  try {
    const t0 = performance.now();
    const resp = await fetch(apiUrl("/health/dependencies"));
    const ms = performance.now() - t0;
    const data = await resp.json();
    el.latency.textContent = `${ms.toFixed(0)} ms`;
    el.healthBadge.textContent = data.status === "ok" ? "API healthy" : "API degraded";
    el.healthBadge.className = `status-badge ${data.status === "ok" ? "ok" : "bad"}`;
  } catch (e) {
    el.healthBadge.textContent = "API unreachable";
    el.healthBadge.className = "status-badge bad";
    el.latency.textContent = "n/a";
  }
}

// ---------- side fetches ----------
// Market Movers are derived from THIS company's feed so the panel always matches
// the "Market Movers (N)" feed filter — a notable person only appears if they
// actually show up in the current company's stories.
function computeMoversFromItems(items) {
  const map = new Map();
  (items || []).forEach((it) => {
    (it.notable_people || []).forEach((p) => {
      if (!p || !p.name) return;
      let e = map.get(p.name);
      if (!e) { e = { name: p.name, role: p.role, article_count: 0, _imp: 0, headline: it.title }; map.set(p.name, e); }
      e.article_count += 1;
      e._imp += Number(it.impact_score || 0);
    });
  });
  const arr = [...map.values()].map((e) => ({
    name: e.name, role: e.role, article_count: e.article_count,
    avg_impact: e.article_count ? Number((e._imp / e.article_count).toFixed(4)) : 0,
    headline: e.headline,
  }));
  arr.sort((a, b) => b.article_count - a.article_count || b.avg_impact - a.avg_impact);
  return { distinct_people: arr.length, most_mentioned: arr };
}
async function fetchAgreement() {
  try {
    const r = await fetch(apiUrl("/validation/agreement", { window: "1d", window_days: 60 }));
    if (r.ok) state.agreement = await r.json();
    const h = await fetch(apiUrl("/validation/agreement/history", { window: "1d", days: 60 }));
    if (h.ok) state.agreementHistory = (await h.json()).series || [];
  } catch (e) { /* ignore */ }
}

// ---------- main ----------
async function runAnalysis(force) {
  const company = el.company.value.trim() || "Apple";
  const windowDays = Number(el.windowDays.value || 14);
  const bucket = el.bucket.value;
  const method = el.method.value;
  localStorage.setItem("dashboard.apiBaseUrl", el.apiBaseUrl.value);
  localStorage.setItem("dashboard.company", company);

  setBusy(true);
  setStatus(force ? "Recomputing…" : "Running analysis…", "running");
  const t0 = performance.now();
  try {
    const [queryResp, sectorResp] = await Promise.all([
      fetch(apiUrl("/query", { company, bucket, window_days: windowDays, item_limit: 50, recompute_timeline: force }), { method: "POST" }),
      fetch(apiUrl("/sector-insights", { bucket, window_days: windowDays, method, max_lag: 3, recompute: force })),
    ]);
    if (!queryResp.ok || !sectorResp.ok) throw new Error("API call failed — check backend logs.");

    state.query = await queryResp.json();
    state.sectorRaw = await sectorResp.json();
    state.movers = computeMoversFromItems(state.query.items);
    await fetchAgreement();

    // source options
    const sources = new Set(["all"]);
    el.sourceFilter.innerHTML = '<option value="all">All Sources</option>';
    (state.query.items || []).forEach((it) => {
      const s = it.source_type || "unknown";
      if (!sources.has(s)) { sources.add(s); const o = document.createElement("option"); o.value = s; o.textContent = s; el.sourceFilter.appendChild(o); }
    });
    el.sourceFilter.value = state.sourceFilter;

    // render everything (cached; tabs just show/hide)
    renderSnapshot();
    renderKpis();
    renderMarketMovers();
    renderTopStories();
    state.lastTimeline = state.query.timeline || [];
    renderTimeline(state.lastTimeline);
    renderFeed();
    renderSectorChips((state.sectorRaw.sector_series || []).map((s) => s.sector));
    applySectorFilter();
    renderCorrInsight();
    renderSectorCoverage();
    renderValidation();
    renderObservability();
    renderFreshness(state.query.freshness);
    resizeTabCharts(state.activeTab);

    const secs = ((performance.now() - t0) / 1000).toFixed(1);
    setStatus(`Done · ${secs}s`, "done");
    const fnew = state.query.freshness && state.query.freshness.new_items;
    toast(fnew ? `Analysis complete · ${fnew} new article${fnew === 1 ? "" : "s"}` : "Analysis complete", "success");
  } catch (err) {
    setStatus("Error", "error");
    toast(err.message || "Analysis failed", "error");
    throw err;
  } finally {
    setBusy(false);
  }
}

// ---------- wire up ----------
function bindEvents() {
  el.tabNav.querySelectorAll(".tab").forEach((t) => (t.onclick = () => setTab(t.dataset.tab)));
  el.runQuery.onclick = () => runAnalysis(false).catch(() => {});
  el.forceRecompute.onclick = () => runAnalysis(true).catch(() => {});
  el.company.addEventListener("keydown", (e) => { if (e.key === "Enter") runAnalysis(false).catch(() => {}); });
  el.sourceFilter.onchange = () => { state.sourceFilter = el.sourceFilter.value; renderFeed(); };
  el.feedFilters.querySelectorAll(".ftab").forEach((b) => {
    b.onclick = () => {
      state.feedFilter = b.dataset.filter;
      if (b.dataset.filter !== "movers") state.personFilter = null;
      renderFeed();
    };
  });
  if (el.smoothToggle) el.smoothToggle.onchange = () => renderTimeline(state.lastTimeline || []);
}

(async function bootstrap() {
  el.apiBaseUrl.value = defaultApiBase();
  el.company.value = localStorage.getItem("dashboard.company") || "Apple";
  bindEvents();
  const hash = (location.hash || "").replace("#", "");
  if (["overview", "feed", "correlations", "validation", "observability"].includes(hash)) setTab(hash);
  await checkHealth();
  checkEnergy();
  try {
    await runAnalysis(false);
  } catch (e) {
    el.feedCards.innerHTML = `<p class="empty-note">${esc(e.message)}</p>`;
  }
})();
