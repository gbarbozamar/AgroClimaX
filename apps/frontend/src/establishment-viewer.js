import { fetchFarmOptions, fetchFields } from './api.js';
import { setSidebarView, syncSidebarView } from './settings.js';
import { setStore, store } from './state.js';

function getNode(id) {
  return document.getElementById(id);
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function emptyOption(label) {
  return `<option value="">${escapeHtml(label)}</option>`;
}

function setStatus(message, tone = 'muted') {
  const node = getNode('establishment-viewer-status');
  if (!node) return;
  node.textContent = message;
  node.dataset.tone = tone;
}

function renderEstablishmentOptions() {
  const select = getNode('establishment-viewer-select');
  if (!select) return;
  const items = store.farmEstablishments || [];
  select.innerHTML = [
    emptyOption('Seleccionar establecimiento'),
    ...items.map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name)}</option>`),
  ].join('');
  select.value = store.estViewerSelectedEstablishmentId || '';
  select.disabled = Boolean(store.estViewerLoading) || !items.length;
}

function renderFieldOptions() {
  const select = getNode('establishment-viewer-field-select');
  if (!select) return;
  const items = store.estViewerFields || [];
  select.innerHTML = [
    emptyOption('Seleccionar campo'),
    ...items.map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name || 'Campo')}</option>`),
  ].join('');
  select.value = store.estViewerSelectedFieldId || '';
  select.disabled = Boolean(store.estViewerLoading) || !items.length;
}

function renderSummary() {
  const node = getNode('establishment-viewer-summary');
  if (!node) return;
  const field = store.estViewerFieldDetail || null;
  if (!field) {
    node.innerHTML = '<div class="fields-empty">Selecciona un campo para ver su AOI, métricas y timeline.</div>';
    return;
  }
  const area = Number.isFinite(Number(field.area_ha)) ? `${Number(field.area_ha).toFixed(2)} ha` : 'Area sin dato';
  const aoi = field.aoi_unit_id || 'AOI no disponible';
  const department = field.department || 'Departamento sin dato';
  node.innerHTML = `
    <div class="viewer-summary-grid">
      <div><strong>Campo</strong><span>${escapeHtml(field.name || 'Campo')}</span></div>
      <div><strong>Establecimiento</strong><span>${escapeHtml(field.establishment_name || '-')}</span></div>
      <div><strong>Departamento</strong><span>${escapeHtml(department)}</span></div>
      <div><strong>Superficie</strong><span>${escapeHtml(area)}</span></div>
      <div class="viewer-summary-wide"><strong>AOI</strong><span>${escapeHtml(aoi)}</span></div>
      <div class="viewer-summary-wide"><strong>Timeline</strong><span>La timeline inferior del mapa queda acotada al campo seleccionado.</span></div>
    </div>
  `;
}

function renderFieldsList(onSelectField) {
  const node = getNode('establishment-viewer-fields-list');
  if (!node) return;
  const items = store.estViewerFields || [];
  if (!store.estViewerSelectedEstablishmentId) {
    node.innerHTML = '<div class="fields-empty">Selecciona un establecimiento para cargar sus campos.</div>';
    return;
  }
  if (!items.length) {
    node.innerHTML = '<div class="fields-empty">No hay campos activos para el establecimiento seleccionado.</div>';
    return;
  }
  node.innerHTML = items.map((item) => {
    const active = item.id === store.estViewerSelectedFieldId ? ' active' : '';
    const analytics = item.field_analytics || null;
    const state = analytics?.state || 'Sin analitica';
    const risk = analytics?.risk_score != null ? ` · riesgo ${Number(analytics.risk_score).toFixed(1)}` : '';
    return `
      <button class="fields-list-item${active}" type="button" data-est-viewer-field-id="${escapeHtml(item.id)}">
        <span class="fields-list-title">${escapeHtml(item.name || 'Campo')}</span>
        <span class="fields-list-copy">${escapeHtml(item.department || '-')} · ${escapeHtml(state)}${escapeHtml(risk)}</span>
      </button>
    `;
  }).join('');
  node.querySelectorAll('[data-est-viewer-field-id]').forEach((button) => {
    button.addEventListener('click', async () => {
      const fieldId = button.dataset.estViewerFieldId;
      const field = (store.estViewerFields || []).find((item) => item.id === fieldId) || null;
      if (!field) return;
      await onSelectField?.(field, { source: 'list' });
    });
  });
}

let fieldsRequestSeq = 0;

async function ensureFarmOptionsLoaded() {
  if (store.farmOptions && (store.farmEstablishments || []).length) return;
  const options = await fetchFarmOptions();
  setStore({
    farmOptions: options,
    farmEstablishments: options?.establishments || [],
  });
}

async function resolveInitialEstablishmentId() {
  if (store.estViewerSelectedEstablishmentId) return store.estViewerSelectedEstablishmentId;
  const establishments = store.farmEstablishments || [];
  if (!establishments.length) return null;
  for (const establishment of establishments) {
    try {
      const payload = await fetchFields(establishment.id);
      const items = payload?.items || [];
      if (items.length) return establishment.id;
    } catch (error) {
      console.warn('No se pudo inspeccionar establecimiento para el viewer:', error);
    }
  }
  return establishments[0]?.id || null;
}

async function loadFieldsForEstablishment(establishmentId) {
  const requestSeq = (fieldsRequestSeq += 1);
  setStore({ estViewerLoading: true, estViewerError: null, estViewerSelectedFieldId: null, estViewerFieldDetail: null });
  renderEstablishmentOptions();
  renderFieldOptions();
  renderSummary();
  try {
    if (!establishmentId) {
      if (requestSeq !== fieldsRequestSeq) return [];
      setStore({ estViewerFields: [], estViewerLoading: false });
      renderEstablishmentOptions();
      renderFieldOptions();
      renderSummary();
      setStatus('Selecciona un establecimiento para ver sus campos.', 'muted');
      return [];
    }
    const payload = await fetchFields(establishmentId);
    if (requestSeq !== fieldsRequestSeq) return [];
    const items = payload?.items || [];
    setStore({ estViewerFields: items, estViewerLoading: false });
    renderEstablishmentOptions();
    renderFieldOptions();
    renderSummary();
    setStatus(`Campos cargados: ${items.length}. Selecciona uno para abrir el visor.`, 'success');
    return items;
  } catch (error) {
    if (requestSeq !== fieldsRequestSeq) return [];
    setStore({ estViewerFields: [], estViewerLoading: false, estViewerError: error?.message || String(error) });
    renderEstablishmentOptions();
    renderFieldOptions();
    renderSummary();
    setStatus(`No se pudieron cargar los campos: ${error?.message || error}`, 'error');
    return [];
  }
}

export function renderEstablishmentViewer({ onSelectField } = {}) {
  renderEstablishmentOptions();
  renderFieldOptions();
  renderFieldsList(onSelectField);
  renderSummary();
  if (store.estViewerError) {
    setStatus(store.estViewerError, 'error');
    return;
  }
  if (store.estViewerLoading) {
    setStatus('Cargando campos...', 'info');
    return;
  }
  if (!store.estViewerSelectedEstablishmentId) {
    setStatus('Selecciona un establecimiento para comenzar.', 'muted');
    return;
  }
  if (!store.estViewerSelectedFieldId) {
    setStatus('Selecciona un campo para acotar el mapa y la timeline.', 'info');
    return;
  }
  setStatus('Visor listo. El mapa y la timeline trabajan solo sobre este campo.', 'success');
}

export function initEstablishmentViewerPanel({ onSelectField, onSelectEstablishment } = {}) {
  const tab = getNode('sidebar-establishment-viewer-tab');
  const select = getNode('establishment-viewer-select');
  const fieldSelect = getNode('establishment-viewer-field-select');
  const refreshButton = getNode('establishment-viewer-refresh-btn');

  if (!tab) return;

  async function openViewer() {
    setSidebarView('establishment_viewer');
    try {
      await ensureFarmOptionsLoaded();
      if (!store.estViewerSelectedEstablishmentId) {
        const preferredId = await resolveInitialEstablishmentId();
        setStore({ estViewerSelectedEstablishmentId: preferredId });
      }
      renderEstablishmentViewer({ onSelectField });
      await loadFieldsForEstablishment(store.estViewerSelectedEstablishmentId);
      renderEstablishmentViewer({ onSelectField });
      await onSelectEstablishment?.(store.estViewerSelectedEstablishmentId);
    } catch (error) {
      console.warn('No se pudo inicializar el visor de establecimiento:', error);
      setStatus(`No se pudo abrir el visor: ${error?.message || error}`, 'error');
    }
  }

  tab.addEventListener('click', openViewer);

  select?.addEventListener('change', async (event) => {
    const nextId = event.target.value || null;
    setStore({ estViewerSelectedEstablishmentId: nextId, estViewerSelectedFieldId: null, estViewerFieldDetail: null });
    await loadFieldsForEstablishment(nextId);
    await onSelectEstablishment?.(nextId);
    renderEstablishmentViewer({ onSelectField });
  });

  fieldSelect?.addEventListener('change', async (event) => {
    const fieldId = event.target.value || null;
    const field = (store.estViewerFields || []).find((item) => item.id === fieldId) || null;
    setStore({ estViewerSelectedFieldId: fieldId });
    renderEstablishmentViewer({ onSelectField });
    if (field) await onSelectField?.(field, { source: 'select' });
  });

  refreshButton?.addEventListener('click', async () => {
    try {
      await ensureFarmOptionsLoaded();
      await loadFieldsForEstablishment(store.estViewerSelectedEstablishmentId);
      await onSelectEstablishment?.(store.estViewerSelectedEstablishmentId);
      renderEstablishmentViewer({ onSelectField });
    } catch (error) {
      console.warn('No se pudo refrescar el visor de establecimiento:', error);
      setStatus(`No se pudo refrescar: ${error?.message || error}`, 'error');
    }
  });

  window.addEventListener('agroclimax:open-establishment-viewer', openViewer);

  syncSidebarView();
  renderEstablishmentViewer({ onSelectField });
}
