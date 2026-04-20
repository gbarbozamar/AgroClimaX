/**
 * AgroClimaX — Sidebar lateral colapsable.
 *
 * Expone toggles de capas (tiles + geojson) como controles siempre visibles,
 * agrupados por categoria, con shortcuts a Alertas, Campos, Timeline y Perfil.
 * Reusa la logica existente via window.toggleMapLayer / store.activeLayers.
 */
import { store, setStore } from './state.js?v=20260419-4';
import { profilePageUrl } from './api.js?v=20260419-4';

const COLLAPSE_STORAGE_KEY = 'agroclimax.sidebarCollapsed';

const LAYER_GROUPS = [
  {
    id: 'analiticas',
    title: 'Analiticas',
    hint: 'Rasteres Sentinel / ERA5',
    layers: [
      { id: 'alerta', label: 'Alerta', hint: 'Fusion multi-capa', recommended: true },
      { id: 'rgb', label: 'RGB', hint: 'Sentinel-2 natural' },
      { id: 'ndvi', label: 'NDVI', hint: 'Vegetacion' },
      { id: 'ndmi', label: 'NDMI', hint: 'Humedad' },
      { id: 'ndwi', label: 'NDWI', hint: 'Agua' },
      { id: 'savi', label: 'SAVI', hint: 'Vegetacion ajustado' },
      { id: 'sar', label: 'SAR VV', hint: 'Radar Sentinel-1' },
      { id: 'lst', label: 'Termal', hint: 'Temperatura superficie' },
    ],
  },
  {
    id: 'administrativas',
    title: 'Administrativas',
    hint: 'Limites y divisiones',
    layers: [
      { id: 'judicial', label: 'Secciones', hint: 'Secciones policiales' },
      { id: 'productiva', label: 'Predios', hint: 'Unidades productivas' },
      { id: 'hex', label: 'Hexagonos H3', hint: 'Grilla H3 res 6-8' },
    ],
  },
];

function readCollapsedPref() {
  try {
    return window.localStorage.getItem(COLLAPSE_STORAGE_KEY) === '1';
  } catch (_) {
    return false;
  }
}

function writeCollapsedPref(collapsed) {
  try {
    window.localStorage.setItem(COLLAPSE_STORAGE_KEY, collapsed ? '1' : '0');
  } catch (_) {
    // ignore
  }
}

function isLayerActive(layerId) {
  const active = store.activeLayers || [];
  return Array.isArray(active) && active.includes(layerId);
}

function currentAlertSummary() {
  // Leemos directo del DOM que renderDashboard ya pobló — evita duplicar estado.
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
    if (typeof window.toggleMapLayer === 'function') {
      window.toggleMapLayer(layer.id, enabled);
    }
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
      <div class="sb-alerts-scope">${departamento}</div>
      <div class="sb-alerts-level" data-level="${nivelNombre}">${nivelNombre}</div>
      <div class="sb-alerts-metrics">
        <div class="sb-alerts-metric"><span class="sb-alerts-metric-label">Riesgo</span><span class="sb-alerts-metric-value">${riskScore}</span></div>
        <div class="sb-alerts-metric"><span class="sb-alerts-metric-label">Humedad</span><span class="sb-alerts-metric-value">${humedad}</span></div>
      </div>
    </div>
  `;
}

function renderFieldsSection(container) {
  container.innerHTML = `
    <div class="sb-fields-card">
      <div class="sb-fields-copy">Dibuja o importa tus parcelas para monitorearlas individualmente.</div>
      <div class="sb-fields-actions">
        <button class="sb-btn primary" type="button" data-sb-action="draw">Dibujar parcela</button>
        <button class="sb-btn" type="button" data-sb-action="import">Importar .geojson / .zip</button>
      </div>
    </div>
  `;
  container.querySelector('[data-sb-action="draw"]')?.addEventListener('click', () => {
    if (typeof window.startDrawing === 'function') window.startDrawing();
  });
  container.querySelector('[data-sb-action="import"]')?.addEventListener('click', () => {
    document.getElementById('productivas-file-input')?.click();
  });
}

function renderTimelineSection(container) {
  container.innerHTML = `
    <div class="sb-fields-card">
      <div class="sb-fields-copy">Activa una capa temporal (NDVI, NDMI, Alerta) para navegar historia.</div>
      <div class="sb-fields-actions">
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
    <div class="sb-profile-card">
      <div class="sb-profile-name">${displayName}</div>
      <div class="sb-profile-sub">${user.email || 'Sin sesion'}</div>
      <div class="sb-fields-actions">
        <a class="sb-btn primary" href="${profilePageUrl()}">Abrir perfil</a>
      </div>
    </div>
  `;
}

function buildSidebarShell() {
  const aside = document.getElementById('app-sidebar');
  if (!aside) return null;
  aside.innerHTML = `
    <div class="sb-head">
      <div class="sb-brand">
        <span class="sb-brand-dot"></span>
        <span class="sb-brand-copy">Panel</span>
      </div>
      <button class="sb-collapse-btn" type="button" data-sb-collapse aria-label="Plegar panel" title="Plegar panel">«</button>
    </div>
    <nav class="sb-body">
      <section class="sb-section" data-section="layers">
        <header class="sb-section-head">
          <span class="sb-section-icon">◧</span>
          <span class="sb-section-title">Capas</span>
        </header>
        <div class="sb-section-body" data-sb-body="layers"></div>
      </section>
      <section class="sb-section" data-section="alerts">
        <header class="sb-section-head">
          <span class="sb-section-icon">◆</span>
          <span class="sb-section-title">Alerta actual</span>
        </header>
        <div class="sb-section-body" data-sb-body="alerts"></div>
      </section>
      <section class="sb-section" data-section="fields">
        <header class="sb-section-head">
          <span class="sb-section-icon">▦</span>
          <span class="sb-section-title">Mis campos</span>
        </header>
        <div class="sb-section-body" data-sb-body="fields"></div>
      </section>
      <section class="sb-section" data-section="timeline">
        <header class="sb-section-head">
          <span class="sb-section-icon">⏱</span>
          <span class="sb-section-title">Timeline</span>
        </header>
        <div class="sb-section-body" data-sb-body="timeline"></div>
      </section>
      <section class="sb-section" data-section="profile">
        <header class="sb-section-head">
          <span class="sb-section-icon">◉</span>
          <span class="sb-section-title">Perfil</span>
        </header>
        <div class="sb-section-body" data-sb-body="profile"></div>
      </section>
    </nav>
  `;
  return aside;
}

function wireCollapse(aside) {
  const btn = aside.querySelector('[data-sb-collapse]');
  if (!btn) return;
  btn.addEventListener('click', () => {
    const next = !aside.classList.contains('is-collapsed');
    aside.classList.toggle('is-collapsed', next);
    btn.textContent = next ? '»' : '«';
    btn.setAttribute('aria-label', next ? 'Expandir panel' : 'Plegar panel');
    btn.setAttribute('title', next ? 'Expandir panel' : 'Plegar panel');
    writeCollapsedPref(next);
    document.body.classList.toggle('sidebar-collapsed', next);
  });
}

function renderAllSections(aside) {
  const map = {
    layers: renderLayersSection,
    alerts: renderAlertsSection,
    fields: renderFieldsSection,
    timeline: renderTimelineSection,
    profile: renderProfileSection,
  };
  Object.entries(map).forEach(([key, fn]) => {
    const node = aside.querySelector(`[data-sb-body="${key}"]`);
    if (node) fn(node);
  });
}

/**
 * Refresca secciones dinamicas (alertas, perfil) y sincroniza el estado de los
 * checkboxes con store.activeLayers. Seguro de llamar repetidas veces.
 */
export function syncSidebar() {
  const aside = document.getElementById('app-sidebar');
  if (!aside) return;
  // Re-render secciones que dependen de store
  const alertsBody = aside.querySelector('[data-sb-body="alerts"]');
  if (alertsBody) renderAlertsSection(alertsBody);
  const profileBody = aside.querySelector('[data-sb-body="profile"]');
  if (profileBody) renderProfileSection(profileBody);
  // Sincronizar checkboxes
  aside.querySelectorAll('.sb-layer-row').forEach((row) => {
    const layerId = row.dataset.layerId;
    const active = isLayerActive(layerId);
    row.classList.toggle('is-active', active);
    const checkbox = row.querySelector('input[type="checkbox"]');
    if (checkbox && checkbox.checked !== active) checkbox.checked = active;
  });
}

export function initSidebar() {
  const aside = buildSidebarShell();
  if (!aside) return;
  wireCollapse(aside);
  renderAllSections(aside);
  const collapsed = readCollapsedPref();
  if (collapsed) {
    aside.classList.add('is-collapsed');
    document.body.classList.add('sidebar-collapsed');
    const btn = aside.querySelector('[data-sb-collapse]');
    if (btn) {
      btn.textContent = '»';
      btn.setAttribute('aria-label', 'Expandir panel');
    }
  }
  // Expose sync globally so map.js / app.js pueden llamarlo sin import.
  window.syncSidebar = syncSidebar;
}
