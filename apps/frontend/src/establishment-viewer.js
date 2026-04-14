import { fetchFarmOptions, fetchField, fetchFields } from './api.js';
import { fitGeojsonBounds } from './map.js';
import { setSidebarView, syncSidebarView } from './settings.js';
import { setStore, store } from './state.js';

const viewerHandlers = {
  onSelectField: null,
  onSelectEstablishment: null,
};

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
      if ((store.map?.getZoom?.() || 0) < 10 || store.establishmentViewerInitialFitFieldId !== field.id) {
        await ensureViewerFieldFocus(field.id);
      }
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

async function ensureViewerFieldFocus(fieldId, { maxZoom = 15, retries = 4 } = {}) {
  if (!fieldId) return false;
  for (let attempt = 0; attempt < retries; attempt += 1) {
    const currentDetail = store.estViewerFieldDetail?.id === fieldId ? store.estViewerFieldDetail : null;
    const detail = currentDetail || await fetchField(fieldId);
    if (detail?.field_geometry_geojson) {
      fitGeojsonBounds(detail.field_geometry_geojson, maxZoom);
      setStore({
        estViewerFieldDetail: detail,
        establishmentViewerInitialFitFieldId: fieldId,
      });
      return true;
    }
    await new Promise((resolve) => window.setTimeout(resolve, 250));
  }
  return false;
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
  const effectiveOnSelectField = onSelectField || viewerHandlers.onSelectField;
  renderEstablishmentOptions();
  renderFieldOptions();
  renderFieldsList(effectiveOnSelectField);
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
  viewerHandlers.onSelectField = onSelectField || viewerHandlers.onSelectField;
  viewerHandlers.onSelectEstablishment = onSelectEstablishment || viewerHandlers.onSelectEstablishment;
  const effectiveOnSelectField = viewerHandlers.onSelectField;
  const effectiveOnSelectEstablishment = viewerHandlers.onSelectEstablishment;
  const tab = getNode('sidebar-establishment-viewer-tab');
  const select = getNode('establishment-viewer-select');
  const fieldSelect = getNode('establishment-viewer-field-select');
  const refreshButton = getNode('establishment-viewer-refresh-btn');

  if (!tab) return;

  async function openViewer() {
    setSidebarView('establishment_viewer');
    try {
      await ensureFarmOptionsLoaded();
      let preferredId = store.estViewerSelectedEstablishmentId || null;
      if (!preferredId) {
        preferredId = await resolveInitialEstablishmentId();
        setStore({ estViewerSelectedEstablishmentId: preferredId });
        await effectiveOnSelectEstablishment?.(preferredId);
      }
      renderEstablishmentViewer({ onSelectField: effectiveOnSelectField });
      if (!store.estViewerFields.length && preferredId) {
        await loadFieldsForEstablishment(preferredId);
      }
      renderEstablishmentViewer({ onSelectField: effectiveOnSelectField });
    } catch (error) {
      console.warn('No se pudo inicializar el visor de establecimiento:', error);
      setStatus(`No se pudo abrir el visor: ${error?.message || error}`, 'error');
    }
  }

  tab.addEventListener('click', openViewer);

  select?.addEventListener('change', async (event) => {
    const nextId = event.target.value || null;
    if (nextId === (store.estViewerSelectedEstablishmentId || null) && (store.estViewerFields || []).length) {
      renderEstablishmentViewer({ onSelectField: effectiveOnSelectField });
      return;
    }
    setStore({ estViewerSelectedEstablishmentId: nextId, estViewerSelectedFieldId: null, estViewerFieldDetail: null });
    await effectiveOnSelectEstablishment?.(nextId);
    await loadFieldsForEstablishment(nextId);
    renderEstablishmentViewer({ onSelectField: effectiveOnSelectField });
  });

  fieldSelect?.addEventListener('change', async (event) => {
    const fieldId = event.target.value || null;
    const field = (store.estViewerFields || []).find((item) => item.id === fieldId) || null;
    setStore({ estViewerSelectedFieldId: fieldId });
    renderEstablishmentViewer({ onSelectField: effectiveOnSelectField });
    if (field) await effectiveOnSelectField?.(field, { source: 'select' });
  });

  refreshButton?.addEventListener('click', async () => {
    try {
      const preserveFieldId = store.estViewerSelectedFieldId || null;
      await ensureFarmOptionsLoaded();
      await effectiveOnSelectEstablishment?.(store.estViewerSelectedEstablishmentId);
      const items = await loadFieldsForEstablishment(store.estViewerSelectedEstablishmentId);
      if (preserveFieldId) {
        const field = (items || []).find((item) => item.id === preserveFieldId) || null;
        if (field) {
          setStore({ estViewerSelectedFieldId: preserveFieldId });
          await effectiveOnSelectField?.(field, { source: 'refresh' });
        }
      }
      renderEstablishmentViewer({ onSelectField: effectiveOnSelectField });
    } catch (error) {
      console.warn('No se pudo refrescar el visor de establecimiento:', error);
      setStatus(`No se pudo refrescar: ${error?.message || error}`, 'error');
    }
  });

  window.addEventListener('agroclimax:open-establishment-viewer', openViewer);

  syncSidebarView();
  renderEstablishmentViewer({ onSelectField: effectiveOnSelectField });
}
