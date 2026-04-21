/**
 * AgroClimaX — Sidebar lateral moderna (estilo Mapbox Studio / QGIS / Figma dark).
 *
 * Estructura:
 *   - Rail de íconos (izquierda, 56px): siempre visible, cada ícono es una sección.
 *   - Panel de contenido (312px expandido, 0px colapsado): lazy-render de la sección activa.
 *   - Footer: estado del sistema + diagnóstico.
 *
 * Reusa toda la lógica ya existente (window.toggleMapLayer, store.activeLayers,
 * window.startDrawing, profilePageUrl) sin duplicar estado.
 */
import { store, setStore } from './state.js?v=20260420-6';
import { profilePageUrl } from './api.js?v=20260420-6';
import { diagnostics } from './diagnostics.js?v=20260420-6';
import { currentScopeLabel, resetToNacional } from './scopeController.js?v=20260420-6';

const COLLAPSE_STORAGE_KEY = 'agroclimax.sidebarCollapsed';
const ACTIVE_SECTION_KEY = 'agroclimax.sidebarActive';
const DEFAULT_SECTION = 'layers';

const SECTIONS = [
  { id: 'layers', label: 'Capas', icon: iconLayers, render: renderLayersSection, dynamic: false },
  { id: 'alerts', label: 'Alerta actual', icon: iconAlert, render: renderAlertsSection, dynamic: true },
  { id: 'fields', label: 'Mis campos', icon: iconGrid, render: renderFieldsSection, dynamic: false },
  { id: 'timeline', label: 'Timeline', icon: iconClock, render: renderTimelineSection, dynamic: false },
  { id: 'diagnostics', label: 'Diagnóstico', icon: iconActivity, render: renderDiagnosticsSection, dynamic: true },
  { id: 'profile', label: 'Perfil', icon: iconUser, render: renderProfileSection, dynamic: true },
];

const LAYER_GROUPS = [
  {
    id: 'analiticas',
    title: 'Analíticas',
    hint: 'Rasteres Sentinel / ERA5',
    layers: [
      { id: 'alerta', label: 'Alerta', hint: 'Fusión multi-capa', recommended: true },
      { id: 'rgb', label: 'RGB', hint: 'Sentinel-2 natural' },
      { id: 'ndvi', label: 'NDVI', hint: 'Vegetación' },
      { id: 'ndmi', label: 'NDMI', hint: 'Humedad' },
      { id: 'ndwi', label: 'NDWI', hint: 'Agua' },
      { id: 'savi', label: 'SAVI', hint: 'Vegetación ajustado' },
      { id: 'sar', label: 'SAR VV', hint: 'Radar Sentinel-1' },
      { id: 'lst', label: 'Termal', hint: 'Temperatura superficie' },
    ],
  },
  {
    id: 'administrativas',
    title: 'Administrativas',
    hint: 'Límites y divisiones',
    layers: [
      { id: 'judicial', label: 'Secciones', hint: 'Secciones policiales' },
      { id: 'productiva', label: 'Predios', hint: 'Unidades productivas' },
      { id: 'hex', label: 'Hexágonos H3', hint: 'Grilla H3 res 6-8' },
    ],
  },
];

/* ───────────── localStorage helpers ───────────── */

function readLS(key, fallback = null) {
  try { return window.localStorage.getItem(key) ?? fallback; } catch (_) { return fallback; }
}
function writeLS(key, value) {
  try { window.localStorage.setItem(key, value); } catch (_) { /* noop */ }
}

function isLayerActive(layerId) {
  const active = store.activeLayers || [];
  return Array.isArray(active) && active.includes(layerId);
}

/* ───────────── Íconos SVG (stroke, 20px) ───────────── */

function svg(path) {
  return `<svg class="sb-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${path}</svg>`;
}
function iconLayers() { return svg('<path d="M12 2 2 7l10 5 10-5-10-5z"/><path d="m2 17 10 5 10-5"/><path d="m2 12 10 5 10-5"/>'); }
function iconAlert()  { return svg('<circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>'); }
function iconGrid()   { return svg('<rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/>'); }
function iconClock()  { return svg('<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>'); }
function iconActivity() { return svg('<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>'); }
function iconUser()   { return svg('<path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/>'); }
function iconClose()  { return svg('<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>'); }
function iconChevron() { return svg('<polyline points="15 18 9 12 15 6"/>'); }

/* ───────────── Alert summary ───────────── */

function currentAlertSummary() {
  const nivelNode = document.getElementById('alerta-nivel-text');
  const rawLevel = nivelNode ? nivelNode.textContent.replace(/^[^A-Za-zÁÉÍÓÚÑáéíóúñ]+/, '').trim() : '-';
  const nivelNombre = rawLevel || '-';
  const riskNode = document.getElementById('kpi-risk');
  const humNode = document.getElementById('kpi-humedad');
  const riskScore = riskNode ? riskNode.textContent.trim() : '-';
  const humedad = humNode ? humNode.textContent.trim() : '-';
  const scopeNode = document.getElementById('scope-badge-value');
  const departamento = scopeNode && scopeNode.textContent.trim() ? scopeNode.textContent.trim() : (store.selectedDepartment || 'Uruguay');
  return { nivelNombre, riskScore, humedad, departamento };
}

/* ───────────── Renderers de sección ───────────── */

function renderLayerRow(layer) {
  const active = isLayerActive(layer.id);
  const row = document.createElement('label');
  row.className = `sb-layer-row${active ? ' is-active' : ''}`;
  row.dataset.layerId = layer.id;
  const checkbox = document.createElement('input');
  checkbox.type = 'checkbox';
  checkbox.className = 'sb-layer-checkbox';
  checkbox.checked = active;
  checkbox.dataset.layerId = layer.id;
  checkbox.addEventListener('change', (event) => {
    const enabled = event.currentTarget.checked;
    diagnostics.track('layer_toggle', { layerId: layer.id, enabled });
    if (typeof window.toggleMapLayer === 'function') window.toggleMapLayer(layer.id, enabled);
    row.classList.toggle('is-active', enabled);
  });
  const copy = document.createElement('div');
  copy.className = 'sb-layer-copy';
  const label = document.createElement('div');
  label.className = 'sb-layer-label';
  label.textContent = layer.label + (layer.recommended ? ' ★' : '');
  const hint = document.createElement('div');
  hint.className = 'sb-layer-hint';
  hint.textContent = layer.hint || '';
  copy.appendChild(label);
  copy.appendChild(hint);
  row.appendChild(checkbox);
  row.appendChild(copy);
  return row;
}

function renderLayersSection(container) {
  container.innerHTML = '';
  LAYER_GROUPS.forEach((group) => {
    const groupEl = document.createElement('div');
    groupEl.className = 'sb-layer-group';
    const head = document.createElement('div');
    head.className = 'sb-layer-group-head';
    head.innerHTML = `<span class="sb-layer-group-title">${group.title}</span><span class="sb-layer-group-hint">${group.hint}</span>`;
    groupEl.appendChild(head);
    const body = document.createElement('div');
    body.className = 'sb-layer-group-body';
    group.layers.forEach((layer) => body.appendChild(renderLayerRow(layer)));
    groupEl.appendChild(body);
    container.appendChild(groupEl);
  });
}

function renderAlertsSection(container) {
  const { nivelNombre, riskScore, humedad, departamento } = currentAlertSummary();
  container.innerHTML = `
    <div class="sb-alerts-card">
      <div class="sb-alerts-scope">${escapeHtml(departamento)}</div>
      <div class="sb-alerts-level" data-level="${escapeHtml(nivelNombre)}">${escapeHtml(nivelNombre)}</div>
      <div class="sb-alerts-metrics">
        <div class="sb-alerts-metric"><span class="sb-alerts-metric-label">Riesgo</span><span class="sb-alerts-metric-value">${escapeHtml(riskScore)}</span></div>
        <div class="sb-alerts-metric"><span class="sb-alerts-metric-label">Humedad</span><span class="sb-alerts-metric-value">${escapeHtml(humedad)}</span></div>
      </div>
    </div>
  `;
}

function renderFieldsSection(container) {
  container.innerHTML = `
    <div class="sb-card">
      <div class="sb-card-copy">Dibujá o importá tus parcelas para monitorearlas individualmente.</div>
      <div class="sb-card-actions">
        <button class="sb-btn primary" type="button" data-sb-action="draw">Dibujar parcela</button>
        <button class="sb-btn" type="button" data-sb-action="import">Importar .geojson / .zip</button>
      </div>
    </div>
  `;
  container.querySelector('[data-sb-action="draw"]')?.addEventListener('click', () => {
    diagnostics.track('draw_started');
    if (typeof window.startDrawing === 'function') window.startDrawing();
  });
  container.querySelector('[data-sb-action="import"]')?.addEventListener('click', () => {
    diagnostics.track('import_clicked');
    document.getElementById('productivas-file-input')?.click();
  });
}

function renderTimelineSection(container) {
  container.innerHTML = `
    <div class="sb-card">
      <div class="sb-card-copy">Activá una capa temporal (NDVI, NDMI, Alerta) para navegar historia.</div>
      <div class="sb-card-actions">
        <button class="sb-btn" type="button" data-sb-action="timeline-focus">Ir al timeline</button>
      </div>
    </div>
  `;
  container.querySelector('[data-sb-action="timeline-focus"]')?.addEventListener('click', () => {
    const dock = document.querySelector('.timeline-dock');
    if (dock) dock.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  });
}

function renderProfileSection(container) {
  const user = store.authUser || {};
  const displayName = user.full_name || user.email || 'Invitado';
  container.innerHTML = `
    <div class="sb-card">
      <div class="sb-profile-name">${escapeHtml(displayName)}</div>
      <div class="sb-profile-sub">${escapeHtml(user.email || 'Sin sesión')}</div>
      <div class="sb-card-actions">
        <a class="sb-btn primary" href="${profilePageUrl()}">Abrir perfil</a>
      </div>
    </div>
  `;
}

function renderDiagnosticsSection(container) {
  const stats = diagnostics.stats();
  const since = stats.startedAt ? new Date(stats.startedAt).toLocaleTimeString() : '-';
  container.innerHTML = `
    <div class="sb-card">
      <div class="sb-diag-header">
        <span class="sb-diag-pill sb-diag-pill-total">${stats.total} total</span>
        <span class="sb-diag-pill sb-diag-pill-error">${stats.errors} errores</span>
        <span class="sb-diag-pill sb-diag-pill-fetch">${stats.fetches} requests</span>
        <span class="sb-diag-pill sb-diag-pill-action">${stats.actions} acciones</span>
      </div>
      <div class="sb-diag-copy">Buffer iniciado ${escapeHtml(since)}. Todos los logs, errores, requests y acciones se capturan localmente.</div>
      <div class="sb-card-actions">
        <button class="sb-btn primary" type="button" data-sb-action="diag-copy">Copiar al portapapeles</button>
        <button class="sb-btn" type="button" data-sb-action="diag-open">Ver detalle</button>
        <button class="sb-btn" type="button" data-sb-action="diag-download">Descargar JSON</button>
        <button class="sb-btn" type="button" data-sb-action="diag-send">Enviar al backend</button>
        <button class="sb-btn ghost" type="button" data-sb-action="diag-clear">Limpiar</button>
      </div>
      <div class="sb-diag-status" data-sb-diag-status></div>
    </div>
  `;
  const status = container.querySelector('[data-sb-diag-status]');
  const flash = (msg, tone = 'info') => {
    if (!status) return;
    status.textContent = msg;
    status.dataset.tone = tone;
    clearTimeout(flash._t);
    flash._t = setTimeout(() => {
      if (status.textContent === msg) { status.textContent = ''; status.dataset.tone = ''; }
    }, 3500);
  };
  container.querySelector('[data-sb-action="diag-copy"]')?.addEventListener('click', async () => {
    const r = await diagnostics.copy();
    flash(r.ok ? `Copiado (${Math.round(r.size / 1024)} KB)` : `Error: ${r.error}`, r.ok ? 'success' : 'error');
  });
  container.querySelector('[data-sb-action="diag-download"]')?.addEventListener('click', () => {
    diagnostics.download();
    flash('Archivo descargado.', 'success');
  });
  container.querySelector('[data-sb-action="diag-send"]')?.addEventListener('click', async () => {
    flash('Enviando...', 'info');
    const r = await diagnostics.sendToBackend();
    flash(r.ok ? `Enviado (HTTP ${r.status})` : `Error HTTP: ${r.error || r.status}`, r.ok ? 'success' : 'error');
  });
  container.querySelector('[data-sb-action="diag-clear"]')?.addEventListener('click', () => {
    diagnostics.clear();
    flash('Buffer limpiado.', 'info');
    renderDiagnosticsSection(container);
  });
  container.querySelector('[data-sb-action="diag-open"]')?.addEventListener('click', () => openDiagModal());
}

/* ───────────── Modal de diagnóstico detallado ───────────── */

function openDiagModal() {
  const existing = document.getElementById('sb-diag-modal');
  if (existing) existing.remove();
  const modal = document.createElement('div');
  modal.id = 'sb-diag-modal';
  modal.className = 'sb-diag-modal';
  modal.innerHTML = `
    <div class="sb-diag-backdrop" data-sb-close></div>
    <div class="sb-diag-dialog" role="dialog" aria-label="Diagnóstico AgroClimaX">
      <header class="sb-diag-dialog-head">
        <div class="sb-diag-dialog-title">Diagnóstico en vivo</div>
        <div class="sb-diag-filters">
          <button class="sb-diag-filter is-active" data-filter="all">Todos</button>
          <button class="sb-diag-filter" data-filter="error">Errores</button>
          <button class="sb-diag-filter" data-filter="warn">Warnings</button>
          <button class="sb-diag-filter" data-filter="fetch">Requests</button>
          <button class="sb-diag-filter" data-filter="user_action">Acciones</button>
        </div>
        <button class="sb-diag-close" type="button" data-sb-close aria-label="Cerrar">${iconClose()}</button>
      </header>
      <div class="sb-diag-table-wrap">
        <table class="sb-diag-table">
          <thead><tr><th>Hora</th><th>Nivel</th><th>Tipo</th><th>Mensaje</th><th>Meta</th></tr></thead>
          <tbody data-sb-diag-tbody></tbody>
        </table>
      </div>
      <footer class="sb-diag-dialog-foot">
        <span class="sb-diag-count" data-sb-diag-count></span>
        <button class="sb-btn primary" type="button" data-sb-action="diag-copy">Copiar</button>
      </footer>
    </div>
  `;
  document.body.appendChild(modal);
  const tbody = modal.querySelector('[data-sb-diag-tbody]');
  const count = modal.querySelector('[data-sb-diag-count]');
  let filter = 'all';
  const render = () => {
    const entries = diagnostics.entries();
    const filtered = entries.filter((e) => {
      if (filter === 'all') return true;
      if (filter === 'error' || filter === 'warn') return e.level === filter;
      return e.type === filter;
    });
    tbody.innerHTML = filtered.slice(-200).reverse().map((e) => {
      const t = new Date(e.t).toLocaleTimeString();
      return `<tr class="sb-diag-row sb-diag-row-${escapeHtml(e.level)}">
        <td class="sb-diag-t">${escapeHtml(t)}</td>
        <td class="sb-diag-level">${escapeHtml(e.level)}</td>
        <td class="sb-diag-type">${escapeHtml(e.type)}</td>
        <td class="sb-diag-msg">${escapeHtml(e.message || '')}</td>
        <td class="sb-diag-meta">${escapeHtml((e.meta || '').slice(0, 200))}</td>
      </tr>`;
    }).join('');
    if (count) count.textContent = `${filtered.length} / ${entries.length} entradas`;
  };
  modal.querySelectorAll('[data-filter]').forEach((btn) => {
    btn.addEventListener('click', (ev) => {
      filter = ev.currentTarget.dataset.filter;
      modal.querySelectorAll('[data-filter]').forEach((b) => b.classList.toggle('is-active', b === ev.currentTarget));
      render();
    });
  });
  modal.querySelector('[data-sb-action="diag-copy"]')?.addEventListener('click', async () => {
    await diagnostics.copy();
  });
  modal.querySelectorAll('[data-sb-close]').forEach((el) => el.addEventListener('click', () => modal.remove()));
  window.addEventListener('keydown', function esc(ev) {
    if (ev.key === 'Escape') { modal.remove(); window.removeEventListener('keydown', esc); }
  });
  const unsubscribe = diagnostics.subscribe(render);
  modal.addEventListener('remove', unsubscribe);
  render();
}

/* ───────────── Utils ───────────── */

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

/* ───────────── Shell + navegación ───────────── */

let activeSectionId = DEFAULT_SECTION;
let asideRef = null;

function buildSidebarShell() {
  const aside = document.getElementById('app-sidebar');
  if (!aside) return null;
  aside.innerHTML = `
    <div class="sb-rail">
      <div class="sb-rail-brand" title="AgroClimaX">
        <span class="sb-rail-brand-dot"></span>
      </div>
      <nav class="sb-rail-nav" data-sb-rail>
        ${SECTIONS.map((s) => `<button class="sb-rail-btn" type="button" data-section="${s.id}" title="${escapeHtml(s.label)}" aria-label="${escapeHtml(s.label)}">${s.icon()}</button>`).join('')}
      </nav>
      <div class="sb-rail-foot">
        <button class="sb-rail-btn sb-rail-collapse" type="button" data-sb-collapse title="Plegar panel" aria-label="Plegar panel">${iconChevron()}</button>
      </div>
    </div>
    <div class="sb-content" data-sb-content>
      <header class="sb-content-head">
        <div class="sb-content-title" data-sb-title>Capas</div>
      </header>
      <div class="sb-content-body" data-sb-body></div>
      <footer class="sb-content-foot" data-sb-foot></footer>
    </div>
  `;
  return aside;
}

function renderActiveSection() {
  if (!asideRef) return;
  const section = SECTIONS.find((s) => s.id === activeSectionId) || SECTIONS[0];
  const title = asideRef.querySelector('[data-sb-title]');
  const body = asideRef.querySelector('[data-sb-body]');
  if (title) title.textContent = section.label;
  if (body) {
    body.innerHTML = '';
    section.render(body);
  }
  asideRef.querySelectorAll('.sb-rail-btn[data-section]').forEach((btn) => {
    btn.classList.toggle('is-active', btn.dataset.section === section.id);
  });
  renderScopeFooter();
}

function renderScopeFooter() {
  if (!asideRef) return;
  const foot = asideRef.querySelector('[data-sb-foot]');
  if (!foot) return;
  const scope = store.clipScope || 'nacional';
  const label = currentScopeLabel();
  const isNacional = scope === 'nacional';
  foot.innerHTML = `
    <div class="sb-scope-chip" data-scope="${escapeHtml(scope)}">
      <span class="sb-scope-dot"></span>
      <span class="sb-scope-label">Clip: ${escapeHtml(label)}</span>
      ${isNacional ? '' : '<button type="button" class="sb-scope-reset" data-sb-action="reset-scope" title="Ampliar a Uruguay">↺</button>'}
    </div>
  `;
  foot.querySelector('[data-sb-action="reset-scope"]')?.addEventListener('click', () => {
    resetToNacional().catch(() => { /* noop */ });
  });
}

function setActiveSection(id) {
  const exists = SECTIONS.find((s) => s.id === id);
  if (!exists) return;
  activeSectionId = id;
  writeLS(ACTIVE_SECTION_KEY, id);
  if (asideRef?.classList.contains('is-collapsed')) {
    asideRef.classList.remove('is-collapsed');
    document.body.classList.remove('sidebar-collapsed');
    writeLS(COLLAPSE_STORAGE_KEY, '0');
  }
  renderActiveSection();
  diagnostics.track('sidebar_section', { section: id });
}

function wireRail() {
  if (!asideRef) return;
  const rail = asideRef.querySelector('[data-sb-rail]');
  if (rail) {
    rail.addEventListener('click', (event) => {
      const btn = event.target.closest('.sb-rail-btn[data-section]');
      if (!btn) return;
      setActiveSection(btn.dataset.section);
    });
  }
  const collapseBtn = asideRef.querySelector('[data-sb-collapse]');
  if (collapseBtn) {
    collapseBtn.addEventListener('click', () => {
      const next = !asideRef.classList.contains('is-collapsed');
      asideRef.classList.toggle('is-collapsed', next);
      document.body.classList.toggle('sidebar-collapsed', next);
      writeLS(COLLAPSE_STORAGE_KEY, next ? '1' : '0');
      collapseBtn.classList.toggle('is-expanded', !next);
    });
  }
}

export function syncSidebar() {
  if (!asideRef) return;
  // El chip de scope se actualiza siempre (es barato y refleja cambios de scope)
  renderScopeFooter();
  // Re-render solo la sección activa si es dinámica
  const section = SECTIONS.find((s) => s.id === activeSectionId);
  if (section?.dynamic) {
    const body = asideRef.querySelector('[data-sb-body]');
    if (body) {
      body.innerHTML = '';
      section.render(body);
    }
  }
  // Sincronizar checkboxes de capas si está activa la sección
  if (activeSectionId === 'layers') {
    asideRef.querySelectorAll('.sb-layer-row').forEach((row) => {
      const layerId = row.dataset.layerId;
      const active = isLayerActive(layerId);
      row.classList.toggle('is-active', active);
      const checkbox = row.querySelector('input[type="checkbox"]');
      if (checkbox && checkbox.checked !== active) checkbox.checked = active;
    });
  }
}

export function initSidebar() {
  asideRef = buildSidebarShell();
  if (!asideRef) return;
  // Estado inicial
  const collapsed = readLS(COLLAPSE_STORAGE_KEY) === '1';
  if (collapsed) {
    asideRef.classList.add('is-collapsed');
    document.body.classList.add('sidebar-collapsed');
  }
  const savedSection = readLS(ACTIVE_SECTION_KEY);
  if (savedSection && SECTIONS.some((s) => s.id === savedSection)) {
    activeSectionId = savedSection;
  }
  wireRail();
  renderActiveSection();
  // Subscribirse a cambios del buffer de diagnóstico para refrescar la sección
  diagnostics.subscribe(() => {
    if (activeSectionId === 'diagnostics') {
      const body = asideRef.querySelector('[data-sb-body]');
      if (body) { body.innerHTML = ''; renderDiagnosticsSection(body); }
    }
  });
  window.syncSidebar = syncSidebar;
}
