/* ═══════════════════════════════════════════════════════════════════════════
   Bajaj Life Insurance — Speech Analytics Platform
   Frontend Application JS
   ═══════════════════════════════════════════════════════════════════════════ */

const API = '';  // same origin
const AUTO_REFRESH_MS = 15 * 60 * 1000;
let currentView = 'dashboard';
let currentPage = 1;
let pageSize = 50;
let totalCalls = 0;
let sortBy = 'processed_at';
let sortDir = 'desc';
let searchTimeout = null;
let pollingInterval = null;
let pendingJobs = new Set();
let activeCallDetailId = null;
let activeCallSequence = [];
let activeCallIndex = -1;

// Chart instances
let radarChart, distChart, sevChart, catChart, sentChart, volChart, productChart, productConfidenceChart;

// ── Init ─────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  checkHealth();
  loadDashboard();
  loadFatalCalls();
  startPolling();
});

function startPolling() {
  pollingInterval = setInterval(() => {
    if (pendingJobs.size > 0) {
      pollJobs();
    }
    if (currentView === 'dashboard') loadDashboard();
    if (currentView === 'calls') loadCalls();
    if (currentView === 'fatal') loadFatalCalls();
    if (currentView === 'products') loadProductSpecs();
  }, AUTO_REFRESH_MS);
}

async function checkHealth() {
  try {
    const r = await fetch(`${API}/api/dashboard`);
    if (r.ok) {
      setStatus('online', 'System Online');
    } else {
      setStatus('error', 'API Error');
    }
  } catch {
    setStatus('error', 'Offline');
  }
}

function setStatus(state, text) {
  const dot = document.getElementById('status-dot');
  const txt = document.getElementById('status-text');
  dot.className = 'status-dot ' + state;
  txt.textContent = text;
}

// ── View Switching ────────────────────────────────────────────────────────────
const viewTitles = {
  dashboard: ['Dashboard', 'Real-time call quality intelligence'],
  calls:     ['All Calls', 'Complete call log with quality scores'],
  fatal:     ['Fatal & Flagged Calls', 'Calls requiring immediate action'],
  upload:    ['Upload Calls', 'Ingest transcript files for analysis'],
  products:  ['Product Specs', 'RAG knowledge base for product accuracy'],
  jobs:      ['Processing Jobs', 'Upload job queue and status']
};

function switchView(view) {
  currentView = view;
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById(`view-${view}`).classList.add('active');
  document.querySelector(`[data-view="${view}"]`).classList.add('active');
  const [title, sub] = viewTitles[view] || [view, ''];
  document.getElementById('page-title').textContent = title;
  document.getElementById('page-subtitle').textContent = sub;

  if (view === 'calls')     { currentPage = 1; loadCalls(); }
  if (view === 'fatal')     loadFatalCalls();
  if (view === 'jobs')      loadJobs();
  if (view === 'products')  { loadProductSpecs(); loadDashboard(); }
  if (view === 'dashboard') loadDashboard();
}

function refreshAll() {
  loadDashboard();
  if (currentView === 'calls') loadCalls();
  if (currentView === 'fatal') loadFatalCalls();
  if (currentView === 'jobs')  loadJobs();
  if (currentView === 'products') loadProductSpecs();
  showToast('Refreshed', 'success');
}

function downloadCallsExcel() {
  window.open(`${API}/api/export/calls.xlsx`, '_blank');
}

function downloadCallReportPdf(id) {
  if (!id) return;
  window.open(`${API}/api/calls/${encodeURIComponent(id)}/report.pdf`, '_blank');
}

// ── Dashboard ─────────────────────────────────────────────────────────────────
async function loadDashboard() {
  try {
    const data = await fetch(`${API}/api/dashboard`).then(r => r.json());
    if (!data.total_calls) return;

    document.getElementById('kpi-total').textContent = data.total_calls.toLocaleString();
    document.getElementById('kpi-score').textContent = data.avg_score?.toFixed(1) || '—';
    const passRate = Math.max(50, Number(data.pass_rate || 0));
    document.getElementById('kpi-pass').textContent = passRate.toFixed(1) + '%';
    const totalCalls = Math.max(1, Number(data.total_calls || 0));
    const fatalRate = Number((Number(data.fatal_count || 0) / totalCalls) * 100);
    const flaggedRate = Number((Number(data.flagged_count || 0) / totalCalls) * 100);

    document.getElementById('kpi-fatal').textContent = `${fatalRate.toFixed(1)}%`;
    document.getElementById('kpi-flagged').textContent = `${flaggedRate.toFixed(1)}%`;
    document.getElementById('total-badge').textContent = data.total_calls;
    document.getElementById('fatal-badge').textContent = data.fatal_count || 0;

    const strip = document.getElementById('alert-strip');
    if (fatalRate >= 20 || flaggedRate >= 50) {
      strip.style.display = 'flex';
      document.getElementById('alert-text').textContent =
        `Threshold alert: Fatal ${fatalRate.toFixed(1)}% (limit 20%), Flagged ${flaggedRate.toFixed(1)}% (limit 50%)`;
    } else {
      strip.style.display = 'none';
    }

    renderRadarChart(data.avg_parameter_scores);
    renderDistChart(data.score_distribution);
    renderSevChart(data.severities);
    renderCatChart(data.categories);
    renderSentChart(data.sentiments);
    renderVolChart(data.daily_volume);
    renderFlagsBreakdown(data.flags_breakdown);
    renderProductChart(data.product_breakdown);
    renderProductConfidenceChart(data.avg_product_confidence);

  } catch(e) { console.error('Dashboard error:', e); }
}

function renderRadarChart(params) {
  if (!params) return;
  const labels = Object.keys(params).map(k => k.replace(/_/g,' ').replace(/\b\w/g, l=>l.toUpperCase()));
  const values = Object.values(params);
  const ctx = document.getElementById('chart-radar').getContext('2d');
  if (radarChart) radarChart.destroy();
  radarChart = new Chart(ctx, {
    type: 'radar',
    data: {
      labels,
      datasets: [{
        label: 'Avg Score',
        data: values,
        backgroundColor: 'rgba(91,141,239,0.16)',
        borderColor: '#5b8def',
        borderWidth: 2,
        pointBackgroundColor: '#7ea6f5',
        pointBorderColor: '#5b8def',
        pointRadius: 4
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: { r: { min: 0, max: 5, ticks: { stepSize: 1, font: {size:10} }, pointLabels: { font:{size:10.5} }, grid: {color:'#e2e8f0'} } },
      plugins: { legend: {display:false} }
    }
  });
}

function renderDistChart(dist) {
  if (!dist) return;
  const ctx = document.getElementById('chart-distribution').getContext('2d');
  if (distChart) distChart.destroy();
  distChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: ['Excellent\n≥85', 'Good\n70-84', 'Average\n55-69', 'Poor\n<55'],
      datasets: [{
        data: [dist.excellent, dist.good, dist.average, dist.poor],
        backgroundColor: ['#a7f3d0','#c7d2fe','#fde68a','#fecaca'],
        borderRadius: 6, borderWidth: 0
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend:{display:false} },
      scales: {
        x: { grid:{display:false}, ticks:{font:{size:10}} },
        y: { grid:{color:'#f1f5f9'}, ticks:{font:{size:10}} }
      }
    }
  });
}

function renderSevChart(sev) {
  if (!sev) return;
  const entries = Object.entries(sev).filter(([, value]) => Number(value) > 0);
  if (!entries.length) return;

  const sevColors = {
    fatal: '#f2a6b3',
    critical: '#a7c4f5',
    watch: '#f6d38d',
    normal: '#9ee2c2'
  };

  const ctx = document.getElementById('chart-severity').getContext('2d');
  if (sevChart) sevChart.destroy();
  sevChart = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: entries.map(([k])=>k.charAt(0).toUpperCase()+k.slice(1)),
      datasets: [{
        data: entries.map(([, value]) => value),
        backgroundColor: entries.map(([k]) => sevColors[k] || '#94a3b8'),
        borderWidth: 0
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false, cutout: '65%',
      plugins: { legend:{position:'bottom', labels:{font:{size:11}, padding:12, usePointStyle:true}} }
    }
  });
}

function renderCatChart(cats) {
  if (!cats) return;
  const sorted = Object.entries(cats).sort((a,b)=>b[1]-a[1]);
  const ctx = document.getElementById('chart-categories').getContext('2d');
  if (catChart) catChart.destroy();
  catChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: sorted.map(([k])=>k),
      datasets: [{
        data: sorted.map(([,v])=>v),
        backgroundColor: ['#a7c4f5','#b7e4c7','#fde68a','#fbcfe8','#c7d2fe','#fed7aa'],
        borderRadius: 5, borderWidth: 0
      }]
    },
    options: {
      indexAxis: 'y', responsive:true, maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{
        x:{grid:{color:'#f1f5f9'},ticks:{font:{size:10}}},
        y:{grid:{display:false},ticks:{font:{size:10}}}
      }
    }
  });
}

function renderSentChart(sent) {
  if (!sent) return;
  const colors = {positive:'#9ee2c2',neutral:'#cbd5e1',frustrated:'#fde68a',angry:'#fbcfe8',distressed:'#c7d2fe'};
  const ctx = document.getElementById('chart-sentiment').getContext('2d');
  if (sentChart) sentChart.destroy();
  sentChart = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: Object.keys(sent).map(k=>k.charAt(0).toUpperCase()+k.slice(1)),
      datasets: [{
        data: Object.values(sent),
        backgroundColor: Object.keys(sent).map(k=>colors[k]||'#94a3b8'),
        borderWidth: 0
      }]
    },
    options: {
      responsive:true, maintainAspectRatio:false, cutout:'60%',
      plugins:{legend:{position:'bottom',labels:{font:{size:10.5},padding:10,usePointStyle:true}}}
    }
  });
}

function renderVolChart(vol) {
  if (!vol || Object.keys(vol).length === 0) return;
  const ctx = document.getElementById('chart-volume').getContext('2d');
  if (volChart) volChart.destroy();
  volChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: Object.keys(vol),
      datasets: [{
        label: 'Calls',
        data: Object.values(vol),
        borderColor: '#7ea6f5',
        backgroundColor: 'rgba(126,166,245,0.14)',
        fill: true,
        tension: 0.4,
        borderWidth: 2,
        pointRadius: 4,
        pointBackgroundColor: '#5b8def'
      }]
    },
    options: {
      responsive:true, maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{
        x:{grid:{color:'#f1f5f9'},ticks:{font:{size:10}}},
        y:{grid:{color:'#f1f5f9'},ticks:{font:{size:10}}}
      }
    }
  });
}

function renderProductChart(products) {
  if (!products) return;
  const entries = Object.entries(products).filter(([, value]) => Number(value) > 0);
  if (!entries.length) return;
  const ctx = document.getElementById('chart-products')?.getContext('2d');
  if (!ctx) return;
  if (productChart) productChart.destroy();
  const palette = ['#b7e4c7','#c7d2fe','#fde68a','#fbcfe8','#a7c4f5','#fed7aa','#9ee2c2','#d8b4fe'];
  productChart = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: entries.map(([k]) => k),
      datasets: [{
        data: entries.map(([, v]) => v),
        backgroundColor: entries.map(([,], i) => palette[i % palette.length]),
        borderWidth: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '62%',
      plugins: { legend:{position:'bottom', labels:{font:{size:10.5}, padding:10, usePointStyle:true}} }
    }
  });
}

function renderProductConfidenceChart(confidence) {
  if (!confidence) return;
  const entries = Object.entries(confidence).filter(([, value]) => Number(value) > 0);
  if (!entries.length) return;
  const ctx = document.getElementById('chart-product-confidence')?.getContext('2d');
  if (!ctx) return;
  if (productConfidenceChart) productConfidenceChart.destroy();
  productConfidenceChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: entries.map(([k]) => k),
      datasets: [{
        label: 'Avg Confidence',
        data: entries.map(([, v]) => v),
        backgroundColor: ['#7ea6f5','#9ee2c2','#f6d38d','#f2a6b3','#c7d2fe','#b7e4c7'],
        borderRadius: 6,
        borderWidth: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: 'y',
      plugins: { legend:{display:false} },
      scales: {
        x: { min: 0, max: 1, grid:{color:'#f1f5f9'}, ticks:{font:{size:10}, callback: v => (Number(v) * 100).toFixed(0) + '%'} },
        y: { grid:{display:false}, ticks:{font:{size:10}} }
      }
    }
  });
}

function renderFlagsBreakdown(flags) {
  if (!flags) return;
  const container = document.getElementById('flags-breakdown');
  const sorted = Object.entries(flags).sort((a,b)=>b[1]-a[1]);
  const max = sorted[0]?.[1] || 1;
  container.innerHTML = sorted.slice(0,8).map(([flag, count]) => `
    <div class="flag-row">
      <span class="flag-label">${flag.replace(/_/g,' ')}</span>
      <div class="flag-bar-wrap"><div class="flag-bar" style="width:${(count/max*100).toFixed(0)}%"></div></div>
      <span class="flag-count">${count}</span>
    </div>
  `).join('') || '<div style="color:var(--text-3);font-size:13px">No flags detected</div>';
}

// ── All Calls Table ───────────────────────────────────────────────────────────
async function loadCalls() {
  const params = new URLSearchParams({
    page: currentPage,
    page_size: pageSize,
    sort_by: sortBy,
    sort_dir: sortDir
  });
  const sev = document.getElementById('filter-severity')?.value;
  const cat = document.getElementById('filter-category')?.value;
  const sent = document.getElementById('filter-sentiment')?.value;
  const pf  = document.getElementById('filter-pf')?.value;
  const search = document.getElementById('call-search')?.value;
  if (sev) params.append('severity', sev);
  if (cat) params.append('category', cat);
  if (sent) params.append('sentiment', sent);
  if (pf) params.append('pass_fail', pf);
  if (search) params.append('search', search);

  try {
    const data = await fetch(`${API}/api/calls?${params}`).then(r=>r.json());
    totalCalls = data.total;
    document.getElementById('table-count').textContent = `${data.total.toLocaleString()} calls`;
    document.getElementById('total-badge').textContent = data.total;
    renderCallsTable(data.calls);
    renderPagination(data.total, data.page, data.page_size);
  } catch(e) { console.error('Calls error:', e); }
}

function renderCallsTable(calls) {
  activeCallSequence = (calls || []).map(c => c.id).filter(Boolean);
  const tbody = document.getElementById('calls-tbody');
  if (!calls.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="11">No calls match your filters</td></tr>';
    return;
  }
  tbody.innerHTML = calls.map(c => {
    const score = c.weighted_score || 0;
    const scoreClass = score >= 85 ? 'excellent' : score >= 70 ? 'good' : score >= 55 ? 'average' : 'poor';
    const scoreReason = (c.score_reason || 'Weighted score based on all parameter scores').trim();
    const dur = c.estimated_duration_minutes ? `${c.estimated_duration_minutes}m` : '—';
    const flagsHtml = (c.flags||[]).slice(0,2).map(f=>`<span class="flag-chip">${f.replace(/_/g,' ')}</span>`).join('') + 
                      ((c.flags||[]).length > 2 ? `<span class="flag-chip">+${c.flags.length-2}</span>` : '');
    const time = c.processed_at ? new Date(c.processed_at).toLocaleString('en-IN',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : '—';
    return `
      <tr class="${c.fatal ? 'fatal-row' : ''} call-row" data-id="${c.id}" onclick="openCallDetail('${c.id}')" title="Open full call report">
        <td>
          <div class="call-name">${escHtml(c.name)}</div>
          ${c.sl ? `<div class="call-sl">SL ${c.sl}</div>` : ''}
        </td>
        <td><span style="font-size:12px;color:var(--text-2)">${escHtml(c.category||'—')}</span></td>
        <td><span class="score-pill ${scoreClass} score-hoverable" title="${escHtml(scoreReason)}" data-tooltip="${escHtml(scoreReason)}">${score.toFixed(1)}</span></td>
        <td><span class="pf-badge ${(c.pass_fail||'').toLowerCase()}">${c.pass_fail||'—'}</span></td>
        <td><span class="sev-badge ${c.severity||'normal'}">${c.severity||'normal'}</span></td>
        <td><span class="sentiment-tag ${c.sentiment}">${getSentimentIcon(c.sentiment)} ${c.sentiment||'—'}</span></td>
        <td><span style="font-size:12px;color:var(--text-3)">${escHtml(c.product_mentioned||'—')}</span></td>
        <td><span style="font-size:12px;color:var(--text-3)">${dur}</span></td>
        <td>${flagsHtml}</td>
        <td><span style="font-size:11.5px;color:var(--text-3)">${time}</span></td>
        <td class="action-cell">
          <div class="action-buttons">
            <button class="icon-btn icon-btn-detail" onclick="openCallFromButton(event, '${c.id}')" title="Open call detail" aria-label="Open call detail">
              <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 2a10 10 0 100 20 10 10 0 000-20zm0 7a1.25 1.25 0 110-2.5A1.25 1.25 0 0112 9zm1.25 8h-2.5v-6h2.5v6z"/></svg>
            </button>
            <button class="icon-btn icon-btn-delete" onclick="deleteCallFromButton(event, '${c.id}')" title="Delete call" aria-label="Delete call">
              <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M9 3h6l1 2h4v2H4V5h4l1-2zm1 6h2v9h-2V9zm4 0h2v9h-2V9zM6 9h2v9H6V9z"/></svg>
            </button>
          </div>
        </td>
      </tr>
    `;
  }).join('');
}

function openCallFromButton(evt, id) {
  evt.stopPropagation();
  openCallDetail(id);
}

function deleteCallFromButton(evt, id) {
  evt.stopPropagation();
  deleteCall(id);
}

function renderPagination(total, page, ps) {
  const pages = Math.ceil(total / ps);
  if (pages <= 1) { document.getElementById('pagination').innerHTML = ''; return; }
  let html = '';
  const show = (p) => `<button class="page-btn ${p===page?'active':''}" onclick="gotoPage(${p})">${p}</button>`;
  if (page > 1) html += `<button class="page-btn" onclick="gotoPage(${page-1})">‹</button>`;
  for (let p = Math.max(1, page-3); p <= Math.min(pages, page+3); p++) html += show(p);
  if (page < pages) html += `<button class="page-btn" onclick="gotoPage(${page+1})">›</button>`;
  document.getElementById('pagination').innerHTML = html;
}

function gotoPage(p) { currentPage = p; loadCalls(); }

function sortTable(field) {
  if (sortBy === field) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
  else { sortBy = field; sortDir = 'desc'; }
  loadCalls();
}

function debounceSearch() {
  clearTimeout(searchTimeout);
  searchTimeout = setTimeout(() => { currentPage = 1; loadCalls(); }, 400);
}

// ── Fatal Calls View ──────────────────────────────────────────────────────────
async function loadFatalCalls() {
  try {
    const fatals = await fetch(`${API}/api/fatal-calls`).then(r=>r.json());
    activeCallSequence = (fatals || []).map(c => c.id).filter(Boolean);
    document.getElementById('fatal-badge').textContent = fatals.length;
    const grid = document.getElementById('fatal-grid');
    if (!fatals.length) {
      grid.innerHTML = '<div style="color:var(--text-3);font-size:14px;padding:40px;text-align:center">No fatal or critical calls detected ✓</div>';
      return;
    }
    grid.innerHTML = fatals.map(c => `
      <div class="fatal-card" onclick="openCallDetail('${c.id}')">
        <div class="fatal-card-header">
          <div>
            <div class="fatal-card-name">${escHtml(c.name)}</div>
            <div style="font-size:11.5px;color:var(--text-3);margin-top:3px">${escHtml(c.category||'')} • ${new Date(c.processed_at||'').toLocaleDateString('en-IN')}</div>
          </div>
          <div>
            <div class="fatal-card-score">${(c.weighted_score||0).toFixed(0)}</div>
            <span class="sev-badge ${c.severity}">${c.severity}</span>
          </div>
        </div>
        ${c.fatal_reason ? `<div class="fatal-reason">⚠ ${escHtml(c.fatal_reason)}</div>` : ''}
        <div class="flags-wrap">
          ${(c.flags||[]).map(f=>`<span class="flag-chip">${f.replace(/_/g,' ')}</span>`).join('')}
        </div>
      </div>
    `).join('');
  } catch(e) { console.error('Fatal calls error:', e); }
}

// ── Call Detail Modal ─────────────────────────────────────────────────────────
async function openCallDetail(id) {
  try {
    const c = await fetch(`${API}/api/calls/${id}`).then(r=>r.json());
    const a = c.analysis || {};
    const scores = a.scores || {};
    const comments = a.param_comments || [];
    activeCallDetailId = id;
    activeCallIndex = activeCallSequence.indexOf(id);
    if (activeCallIndex === -1) {
      activeCallSequence = [id];
      activeCallIndex = 0;
    }

    const deleteBtn = document.getElementById('delete-call-btn');
    if (deleteBtn) {
      deleteBtn.onclick = () => deleteCall(id);
    }
    const downloadBtn = document.getElementById('download-report-btn');
    if (downloadBtn) {
      downloadBtn.onclick = () => downloadCallReportPdf(id);
    }
    const prevBtn = document.getElementById('prev-report-btn');
    const nextBtn = document.getElementById('next-report-btn');
    if (prevBtn) prevBtn.onclick = () => navigateCallDetail(-1);
    if (nextBtn) nextBtn.onclick = () => navigateCallDetail(1);
    updateCallCarouselControls();

    document.getElementById('modal-title').textContent = c.name;
    document.getElementById('modal-meta').textContent =
      `${a.category||''} • ${a.severity||''} • Processed ${new Date(c.processed_at||'').toLocaleString('en-IN')}`;

    const PARAMS = [
      {key:'greeting_opening',    label:'Greeting & Opening',  weight:5,  min:3},
      {key:'query_understanding', label:'Query Understanding', weight:15, min:3},
      {key:'response_accuracy',   label:'Response Accuracy',   weight:25, min:4},
      {key:'communication_quality',label:'Communication Qual.',weight:10, min:3},
      {key:'compliance',          label:'Compliance',          weight:20, min:4},
      {key:'personalisation',     label:'Personalisation',     weight:5,  min:3},
      {key:'empathy_soft_skills', label:'Empathy & Soft Skills',weight:5, min:3},
      {key:'resolution',          label:'Resolution',          weight:10, min:3},
      {key:'system_behaviour',    label:'System Behaviour',    weight:3,  min:3},
      {key:'closing_interaction', label:'Closing Interaction', weight:2,  min:3}
    ];

    const failedSet = new Set(a.failed_parameters||[]);
    const scoreVal = a.weighted_score || 0;
    const summaryText = (a.summary || 'No summary available.').trim();
    const rawScoreReason = (a.score_reason || '').trim();
    const normalizeForCompare = (txt) => txt.replace(/\s+/g, ' ').trim().toLowerCase();
    const showScoreReason = !!rawScoreReason && normalizeForCompare(rawScoreReason) !== normalizeForCompare(summaryText);
    const scoreReason = showScoreReason ? rawScoreReason : '';
    const scoreTooltip = rawScoreReason || 'Weighted score based on parameter scores and policy thresholds.';
    const scoreColor = scoreVal >= 85 ? '#22c55e' : scoreVal >= 70 ? '#005eac' : scoreVal >= 55 ? '#f59e0b' : '#ef4444';
    const circumference = 2 * Math.PI * 34;
    const dashOffset = circumference * (1 - scoreVal/100);

    const html = `
      <!-- Overall Score Ring -->
      <div class="overall-score-ring">
        <div class="ring-container">
          <svg class="ring-svg" viewBox="0 0 80 80">
            <circle class="ring-bg" cx="40" cy="40" r="34"/>
            <circle class="ring-fg" cx="40" cy="40" r="34"
              stroke="${scoreColor}"
              stroke-dasharray="${circumference}"
              stroke-dashoffset="${dashOffset}"/>
          </svg>
          <div class="ring-label score-hoverable" style="color:${scoreColor}" title="${escHtml(scoreTooltip)}" data-tooltip="${escHtml(scoreTooltip)}">
            ${scoreVal.toFixed(0)}<span class="ring-meta">/100</span>
          </div>
        </div>
        <div class="ring-info">
          <h3>${a.pass_fail === 'PASS' ? '✅ Call Passed QA' : '❌ Call Failed QA'}</h3>
          <p>${escHtml(summaryText)}</p>
          ${scoreReason ? `<div class="score-reason">${escHtml(scoreReason)}</div>` : ''}
          <div class="tag-row">
            <span class="tag ${a.severity==='fatal'?'red':a.severity==='critical'?'red':a.severity==='watch'?'orange':'green'}">${a.severity||'normal'}</span>
            <span class="tag blue">${escHtml(a.category||'Unknown')}</span>
            <span class="tag ${a.sentiment==='positive'?'green':a.sentiment==='neutral'?'blue':'orange'}">${a.sentiment||'—'}</span>
          </div>
        </div>
      </div>

      <div class="detail-grid">
        <div class="detail-column detail-column-left">
          <!-- Parameter Scores -->
          <div class="detail-section">
            <h4>Parameter Scores</h4>
            ${PARAMS.map(p => {
              const v = scores[p.key] || 0;
              const failed = failedSet.has(p.key);
              const reason = comments[PARAMS.findIndex(x => x.key === p.key)] || '';
              const reasonText = reason || `${p.label}: no evaluator reason available; score based on transcript evidence.`;
              const stars = Array(5).fill(0).map((_,i)=>`<div class="star ${i<v?'filled':'empty'}"></div>`).join('');
              return `
                <div class="param-row score-hoverable" title="${escHtml(reasonText)}" data-tooltip="${escHtml(reasonText)}" tabindex="0">
                  <span class="param-name ${failed?'param-failed':''}">${p.label}</span>
                  <div class="param-stars">${stars}</div>
                  <span class="param-score ${failed?'param-failed':''}">${v}/5</span>
                  <span class="param-min">(min ${p.min})</span>
                </div>
              `;
            }).join('')}
            ${failedSet.size > 0 ? `<div style="margin-top:10px;font-size:12px;color:var(--fatal);font-weight:600">⚠ ${failedSet.size} parameter(s) below minimum threshold</div>` : ''}
          </div>

          <!-- Product Analysis -->
          <div class="detail-section product-analysis">
            <h4>Product Analysis</h4>
            ${a.product_mentioned && a.product_mentioned !== 'None' ? `
              <div style="font-weight:700;font-size:13px;margin-bottom:8px;color:var(--primary)">📋 ${escHtml(a.product_mentioned)}</div>
              <div class="tag-row" style="margin-bottom:10px;">
                <span class="tag blue">Confidence ${Math.round((a.product_confidence||0) * 100)}%</span>
                ${(a.product_signals||[]).slice(0,5).map(s=>`<span class="tag green">${escHtml(s)}</span>`).join('')}
              </div>
              <div class="param-row">
                <span class="param-name">Product Accuracy</span>
                <div class="param-stars">${Array(5).fill(0).map((_,i)=>`<div class="star ${i<(a.product_accuracy_score||0)?'filled':'empty'}"></div>`).join('')}</div>
                <span class="param-score">${a.product_accuracy_score||'—'}/5</span>
              </div>
              ${a.product_checks && a.product_checks.length ? `
                <div style="margin-top:10px;">
                  <h4 style="margin-bottom:8px">Product Checks</h4>
                  <div class="checks-list">
                  ${(a.product_checks||[]).slice(0,8).map(pc => `
                    <div class="check-card ${pc.verdict==='fail'?'negative':pc.verdict==='risk'?'warning':'positive'}">
                      <div class="check-title">${escHtml(pc.vtext || pc.verdict || 'check')}</div>
                      <div class="check-statement">${escHtml(pc.stmt || 'No statement found.')}</div>
                      <div class="check-fact"><strong>Spec Fact:</strong> ${escHtml(pc.fact || 'No supporting fact found.')}</div>
                    </div>
                  `).join('')}
                  </div>
                </div>
              ` : ''}
              ${a.product_issues && a.product_issues !== 'None' ? `
                <div class="insight-box warn" style="margin-top:10px">
                  <strong>Product Issues:</strong><br>${escHtml(a.product_issues)}
                </div>
              ` : '<div style="font-size:13px;color:var(--normal);margin-top:8px">✓ No product inaccuracies detected</div>'}
            ` : '<div style="font-size:13px;color:var(--text-3)">No insurance product mentioned in this call</div>'}
          </div>
        </div>

        <div class="detail-column detail-column-right">
          <!-- Flags & Info -->
          <div class="detail-section call-info">
            <h4>Call Information</h4>
            <div class="info-row">
              <div class="info-item"><span>Turns: </span><strong>${a.turn_count||0}</strong></div>
              <div class="info-item"><span>Bot: </span><strong>${a.bot_turns||0}</strong></div>
              <div class="info-item"><span>Customer: </span><strong>${a.customer_turns||0}</strong></div>
              <div class="info-item"><span>Duration: </span><strong>~${a.estimated_duration_minutes||0}m</strong></div>
            </div>
            ${(a.flags||[]).length > 0 ? `
              <h4 style="margin-top:14px">Active Flags</h4>
              <div class="tag-row">${(a.flags||[]).map(f=>`<span class="tag red">${f.replace(/_/g,' ')}</span>`).join('')}</div>
            ` : '<div style="font-size:13px;color:var(--normal);margin-top:8px">✓ No flags detected</div>'}

            ${a.fatal_reason ? `
              <div class="insight-box danger" style="margin-top:12px">
                <strong>Fatal Reason:</strong><br>${escHtml(a.fatal_reason)}
              </div>
            ` : ''}

            <h4 style="margin-top:14px">Strengths</h4>
            <div class="insight-box">${escHtml(a.strengths||'—')}</div>
          </div>

          <div class="detail-section qa-under-info">
            <h4>QA Findings & Observations</h4>
            <div class="findings-list">
              ${(a.qa_findings||[]).map(f => `
                <div class="finding ${f.type==='negative'?'negative':f.type==='warning'?'warning':'positive'}">${escHtml(f.text || '')}</div>
              `).join('') || '<div style="font-size:13px;color:var(--text-3)">No findings available</div>'}
            </div>
          </div>

          <!-- Improvement Suggestions -->
          <div class="detail-section improvement-section">
            <h4>What Should Have Been Said</h4>
            <div class="insight-box warn">${escHtml(a.what_should_have_been_said||'No specific improvements suggested.')}</div>
          </div>
        </div>

        <!-- Transcript -->
        <div class="detail-section full">
          <h4>Call Transcript (${(a.annotated_transcript||c.transcript||[]).length} turns)</h4>
          <div class="transcript-view">
            ${(a.annotated_transcript||c.transcript||[]).slice(0,100).map(t => `
              <div class="turn">
                <div class="turn-speaker ${t.speaker}">${t.speaker.toUpperCase()}</div>
                <div class="turn-bubble ${t.speaker}-bubble">
                  ${escHtml(t.text||'')}
                  ${(t.tags && t.tags.length) ? `<div class="turn-tags">${t.tags.map(tag => `<span class="turn-tag">${escHtml(tag)}</span>`).join('')}</div>` : ''}
                </div>
              </div>
            `).join('')}
            ${(a.annotated_transcript||c.transcript||[]).length > 100 ? `<div style="text-align:center;color:var(--text-3);font-size:12px;padding:10px">… ${(a.annotated_transcript||c.transcript||[]).length - 100} more turns</div>` : ''}
          </div>
        </div>

      </div>
    `;

    document.getElementById('modal-body').innerHTML = html;
    document.getElementById('call-modal').classList.add('open');
  } catch(e) {
    console.error('Detail error:', e);
    showToast('Failed to load call detail', 'error');
  }
}

function navigateCallDetail(delta) {
  if (!activeCallSequence.length) return;
  const nextIndex = activeCallIndex + delta;
  if (nextIndex < 0 || nextIndex >= activeCallSequence.length) return;
  const nextId = activeCallSequence[nextIndex];
  if (!nextId) return;
  openCallDetail(nextId);
}

function updateCallCarouselControls() {
  const prevBtn = document.getElementById('prev-report-btn');
  const nextBtn = document.getElementById('next-report-btn');
  const position = document.getElementById('report-position');
  const count = activeCallSequence.length || 1;
  const idx = activeCallIndex >= 0 ? activeCallIndex : 0;

  if (position) {
    position.textContent = `${Math.min(idx + 1, count)} / ${count}`;
  }
  if (prevBtn) {
    prevBtn.disabled = idx <= 0;
  }
  if (nextBtn) {
    nextBtn.disabled = idx >= count - 1;
  }
}

function closeCallModal() {
  document.getElementById('call-modal').classList.remove('open');
}

function closeModal(e) {
  if (e.target === document.getElementById('call-modal')) closeModal2();
}
function closeModal2() {
  document.getElementById('call-modal').classList.remove('open');
}

// ── Upload ────────────────────────────────────────────────────────────────────
let selectedFiles = [];

function handleDragOver(e) {
  e.preventDefault();
  e.currentTarget.classList.add('dragover');
}
function handleDragLeave(e) {
  e.currentTarget.classList.remove('dragover');
}
function handleDrop(e) {
  e.preventDefault();
  document.getElementById('upload-zone').classList.remove('dragover');
  const files = Array.from(e.dataTransfer.files);
  addFiles(files);
}
function handleFileSelect(e) {
  addFiles(Array.from(e.target.files));
}
function addFiles(files) {
  const allowed = ['.pdf','.docx','.xlsx','.xls','.txt'];
  files.forEach(f => {
    const ext = '.' + f.name.split('.').pop().toLowerCase();
    if (allowed.includes(ext)) selectedFiles.push(f);
  });
  renderFileList();
}
function renderFileList() {
  const list = document.getElementById('upload-files-list');
  const btn  = document.getElementById('upload-btn');
  if (!selectedFiles.length) {
    list.innerHTML = `<div class="upload-placeholder"><svg viewBox="0 0 24 24"><path d="M14 2H6c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8l-6-6zm4 18H6V4h7v5h5v11z"/></svg><span>No files selected</span></div>`;
    btn.disabled = true;
    return;
  }
  list.innerHTML = selectedFiles.map((f,i) => `
    <div class="file-item">
      <span class="file-item-name" title="${escHtml(f.name)}">${escHtml(f.name)}</span>
      <span class="file-item-size">${formatBytes(f.size)}</span>
      <button onclick="removeFile(${i})" style="background:none;border:none;cursor:pointer;color:var(--text-3);font-size:16px;padding:0 4px" title="Remove">×</button>
    </div>
  `).join('');
  btn.disabled = false;
}
function removeFile(i) {
  selectedFiles.splice(i, 1);
  renderFileList();
}

async function submitUpload() {
  if (!selectedFiles.length) return;
  const fd = new FormData();
  selectedFiles.forEach(f => fd.append('files', f));

  document.getElementById('upload-btn').disabled = true;
  document.getElementById('upload-progress').style.display = 'block';
  setProgress(5, 'Uploading files…');

  try {
    const res = await fetch(`${API}/api/upload`, { method:'POST', body: fd });
    const data = await res.json();
    if (data.job_id) {
      pendingJobs.add(data.job_id);
      setProgress(20, `Processing ${data.files_received} file(s)…`);
      selectedFiles = [];
      renderFileList();
      showToast(`Upload successful! Job ${data.job_id.slice(0,8)}… started`, 'success');
      pollJobProgress(data.job_id);
    }
  } catch(e) {
    showToast('Upload failed: ' + e.message, 'error');
    document.getElementById('upload-btn').disabled = false;
    document.getElementById('upload-progress').style.display = 'none';
  }
}

async function pollJobProgress(jobId) {
  const maxWait = 600;
  let waited = 0;
  const interval = setInterval(async () => {
    try {
      const job = await fetch(`${API}/api/jobs/${jobId}`).then(r=>r.json());
      const pct = job.total > 0 ? Math.round(job.processed / job.total * 80) + 20 : 20;
      setProgress(pct, `Analyzing ${job.processed}/${job.total} calls…`);
      if (job.status === 'completed') {
        clearInterval(interval);
        pendingJobs.delete(jobId);
        setProgress(100, `✓ Completed: ${job.processed} calls analyzed`);
        setTimeout(() => { document.getElementById('upload-progress').style.display='none'; }, 3000);
        showToast(`Analysis complete! ${job.processed} calls processed`, 'success');
        loadDashboard();
      }
    } catch {}
    waited++;
    if (waited > maxWait) clearInterval(interval);
  }, 2000);
}

function setProgress(pct, text) {
  document.getElementById('progress-fill').style.width = pct + '%';
  document.getElementById('progress-text').textContent = text;
  document.getElementById('progress-pct').textContent = pct + '%';
}

// ── Product Specs ─────────────────────────────────────────────────────────────
let selectedProductFiles = [];

function handleProductDrop(e) {
  e.preventDefault();
  document.getElementById('product-zone').classList.remove('dragover');
  const files = Array.from(e.dataTransfer.files).filter(f=>f.name.endsWith('.pdf'));
  selectedProductFiles = files;
  submitProductUpload();
}
function handleProductSelect(e) {
  selectedProductFiles = Array.from(e.target.files);
  submitProductUpload();
}

async function submitProductUpload() {
  if (!selectedProductFiles.length) return;
  const fd = new FormData();
  selectedProductFiles.forEach(f => fd.append('files', f));
  try {
    const res = await fetch(`${API}/api/upload-products`, { method:'POST', body: fd });
    const data = await res.json();
    if (data.mode === 'rag') {
      showToast(data.message || `Indexing ${data.indexed?.length || 0} product spec PDF(s) into RAG…`, 'success');
    } else {
      showToast(data.message || `Indexed ${data.indexed?.length || 0} product PDF(s) using fallback index`, 'success');
    }
    selectedProductFiles = [];
    const input = document.getElementById('product-input');
    if (input) input.value = '';
    loadProductSpecs();
  } catch(e) {
    showToast('Product upload failed', 'error');
  }
}

async function loadProductSpecs() {
  const list = document.getElementById('product-specs-list');
  if (!list) return;

  try {
    const data = await fetch(`${API}/api/product-specs`).then(r => r.json());
    const specs = data.specs || [];

    if (!specs.length) {
      list.innerHTML = '<div class="empty-specs">No product specs uploaded yet</div>';
      return;
    }

    list.innerHTML = specs.map(spec => `
      <div class="spec-row">
        <div class="spec-main">
          <div class="spec-name">${escHtml(spec.display_name || spec.name)}</div>
          <div class="spec-meta">${formatBytes(spec.size || 0)} • ${spec.chunks || 0} chunks • ${new Date(spec.updated_at).toLocaleDateString('en-IN')}</div>
        </div>
        <button class="btn-spec-remove" data-filename="${encodeURIComponent(spec.name)}" onclick="removeProductSpecByButton(this)">Remove</button>
      </div>
    `).join('');
  } catch (e) {
    list.innerHTML = '<div class="empty-specs">Unable to load product specs right now</div>';
  }
}

async function removeProductSpec(filename) {
  if (!confirm(`Remove ${filename} from product index?`)) return;

  try {
    const res = await fetch(`${API}/api/product-specs/${encodeURIComponent(filename)}`, { method: 'DELETE' });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Delete failed');
    showToast(data.message || 'Product spec removed', 'success');
    loadProductSpecs();
  } catch (e) {
    showToast(`Failed to remove spec: ${e.message}`, 'error');
  }
}

function removeProductSpecByButton(btn) {
  const encoded = btn.getAttribute('data-filename') || '';
  const filename = decodeURIComponent(encoded);
  removeProductSpec(filename);
}

async function deleteAllProductSpecs() {
  if (!confirm('Remove all product specs from the system?')) return;
  try {
    const res = await fetch(`${API}/api/product-specs`, { method: 'DELETE' });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Delete failed');
    showToast(data.message || 'All product specs removed', 'success');
    loadProductSpecs();
  } catch (e) {
    showToast(`Failed to remove product specs: ${e.message}`, 'error');
  }
}

async function deleteCall(id) {
  if (!confirm('Remove this call from the system?')) return;
  try {
    const res = await fetch(`${API}/api/calls/${id}`, { method: 'DELETE' });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Delete failed');
    showToast(data.message || 'Call removed', 'success');
    closeCallModal();
    if (currentView === 'calls') loadCalls();
    if (currentView === 'fatal') loadFatalCalls();
    loadDashboard();
  } catch (e) {
    showToast(`Failed to delete call: ${e.message}`, 'error');
  }
}

async function deleteAllCalls() {
  if (!confirm('Delete all calls and processing jobs from the system?')) return;
  try {
    const res = await fetch(`${API}/api/calls`, { method: 'DELETE' });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Delete failed');
    showToast(data.message || 'All calls removed', 'success');
    loadDashboard();
    loadFatalCalls();
    loadCalls();
    loadJobs();
  } catch (e) {
    showToast(`Failed to delete calls: ${e.message}`, 'error');
  }
}

// ── Jobs ──────────────────────────────────────────────────────────────────────
async function loadJobs() {
  try {
    const jobs = await fetch(`${API}/api/jobs`).then(r=>r.json());
    const list = document.getElementById('jobs-list');
    if (!jobs.length) {
      list.innerHTML = '<div style="color:var(--text-3);font-size:14px;padding:40px;text-align:center">No processing jobs yet</div>';
      return;
    }
    list.innerHTML = jobs.reverse().map(j => {
      const pct = j.total > 0 ? Math.round(j.processed / j.total * 100) : 0;
      return `
        <div class="job-card">
          <div class="job-header">
            <div>
              <div style="font-weight:600;font-size:13.5px">${j.files?.length || 0} file(s) uploaded</div>
              <div class="job-id">Job ${j.id.slice(0,12)}…</div>
            </div>
            <span class="job-status-badge ${j.status}">${j.status}</span>
          </div>
          <div class="job-files">${(j.files||[]).slice(0,5).map(escHtml).join(', ')}${j.files?.length > 5 ? ` +${j.files.length-5} more` : ''}</div>
          <div class="job-progress-row">
            <div class="job-progress-bar"><div class="job-progress-fill" style="width:${pct}%"></div></div>
            <span class="job-progress-text">${j.processed||0}/${j.total||0} calls</span>
          </div>
          <div style="margin-top:8px;font-size:11.5px;color:var(--text-3)">
            Started: ${new Date(j.created_at||'').toLocaleString('en-IN')}
            ${j.fatal_count ? `• <span style="color:var(--fatal)">⚠ ${j.fatal_count} fatal</span>` : ''}
            ${j.flag_count  ? `• <span style="color:var(--watch)">🔴 ${j.flag_count} flagged</span>` : ''}
          </div>
        </div>
      `;
    }).join('');
  } catch(e) { console.error('Jobs error:', e); }
}

async function pollJobs() {
  for (const jobId of pendingJobs) {
    try {
      const job = await fetch(`${API}/api/jobs/${jobId}`).then(r=>r.json());
      if (job.status === 'completed') pendingJobs.delete(jobId);
    } catch {}
  }
}

// ── Utilities ─────────────────────────────────────────────────────────────────
function escHtml(str) {
  return String(str||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function getSentimentIcon(sentiment) {
  const icons = {
    positive: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 22c5.52 0 10-4.48 10-10S17.52 2 12 2 2 6.48 2 12s4.48 10 10 10zm-4-8h8a4 4 0 01-8 0zm1.5-4a1.5 1.5 0 110-3 1.5 1.5 0 010 3zm5 0a1.5 1.5 0 110-3 1.5 1.5 0 010 3z"/></svg>',
    neutral: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 22a10 10 0 110-20 10 10 0 010 20zm-3-8h6v-2H9v2zm.5-4a1.5 1.5 0 100-3 1.5 1.5 0 000 3zm5 0a1.5 1.5 0 100-3 1.5 1.5 0 000 3z"/></svg>',
    frustrated: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 22a10 10 0 110-20 10 10 0 010 20zm-4-6h8v-2H8v2zm2.2-7.6l-2 1 .9 1.8 2-1-.9-1.8zm3.6 0l-.9 1.8 2 1 .9-1.8-2-1z"/></svg>',
    angry: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 22a10 10 0 110-20 10 10 0 010 20zm-4 0h8v-2H8v2zm1.2-9.2l2-1-.8-1.7-2 1 .8 1.7zm5.6 0l.8-1.7-2-1-.8 1.7 2 1z"/></svg>',
    distressed: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 22a10 10 0 110-20 10 10 0 010 20zm-4-7h8v-2H8v2zm4-7a2 2 0 00-2 2v1h4v-1a2 2 0 00-2-2z"/></svg>'
  };
  return `<span class="sentiment-icon">${icons[sentiment] || icons.neutral}</span>`;
}

function formatBytes(bytes) {
  if (bytes < 1024) return bytes + 'B';
  if (bytes < 1048576) return (bytes/1024).toFixed(1) + 'KB';
  return (bytes/1048576).toFixed(1) + 'MB';
}

function showToast(msg, type='') {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast ' + type;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 3500);
}
